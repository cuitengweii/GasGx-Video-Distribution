from __future__ import annotations

import argparse
import json
from typing import Any

from .collect import collect
from .engagement import run_wechat_engagement
from .migrate import migrate_legacy_assets
from .publish import immediate, publish
from .session import capture_login_qr, login_status, open_login


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CyberCar standalone CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ["immediate", "collect", "publish"]:
        sub = subparsers.add_parser(name)
        sub.add_argument("--profile", default="")
        sub.add_argument("--platforms", default="")
        sub.add_argument("--limit", type=int, default=0)
        sub.add_argument("--keyword", default="")

    login_parser = subparsers.add_parser("login")
    login_sub = login_parser.add_subparsers(dest="login_command", required=True)
    for name in ["status", "open"]:
        sub = login_sub.add_parser(name)
        sub.add_argument("--platform", required=True)
    qr = login_sub.add_parser("qr")
    qr.add_argument("--platform", default="wechat")

    engage = subparsers.add_parser("engage")
    engage_sub = engage.add_subparsers(dest="engage_command", required=True)
    engage_wechat = engage_sub.add_parser("wechat")
    engage_wechat.add_argument("--max-posts", type=int, default=0)
    engage_wechat.add_argument("--max-replies", type=int, default=0)
    engage_wechat.add_argument("--like-only", action="store_true")
    engage_wechat.add_argument("--latest-only", action="store_true")
    engage_wechat.add_argument("--debug", action="store_true")

    subparsers.add_parser("migrate-legacy")
    return parser


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> int:
    parser = build_parser()
    args, passthrough = parser.parse_known_args()
    command = str(args.command)
    if command == "immediate":
        return immediate(
            profile=str(args.profile),
            platforms=str(args.platforms),
            limit=int(args.limit),
            keyword=str(args.keyword),
            passthrough=passthrough,
        )
    if command == "collect":
        return collect(
            profile=str(args.profile),
            limit=int(args.limit),
            keyword=str(args.keyword),
            passthrough=passthrough,
        )
    if command == "publish":
        return publish(
            profile=str(args.profile),
            platforms=str(args.platforms),
            limit=int(args.limit),
            keyword=str(args.keyword),
            passthrough=passthrough,
        )
    if command == "login":
        if args.login_command == "status":
            _print_json(login_status(str(args.platform)))
            return 0
        if args.login_command == "open":
            _print_json(open_login(str(args.platform)))
            return 0
        _print_json(capture_login_qr(str(args.platform)))
        return 0
    if command == "engage" and args.engage_command == "wechat":
        result = run_wechat_engagement(
            max_posts=int(args.max_posts),
            max_replies=int(args.max_replies),
            like_only=bool(args.like_only),
            latest_only=bool(args.latest_only),
            debug=bool(args.debug),
        )
        _print_json(result)
        return 0 if bool(result.get("ok")) else 1
    if command == "migrate-legacy":
        summary = migrate_legacy_assets()
        _print_json({"copied": summary.copied, "skipped": summary.skipped})
        return 0
    parser.error(f"unsupported command: {command}")
    return 2
