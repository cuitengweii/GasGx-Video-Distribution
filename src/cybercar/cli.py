from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from .cleanup import run_cleanup
from .collect import collect
from .engagement import (
    diagnose_platform_engagement,
    run_douyin_engagement,
    run_douyin_focused_engagement,
    run_kuaishou_focused_engagement,
    run_kuaishou_engagement,
    run_wechat_engagement,
)
from .migrate import migrate_legacy_assets
from .publish import immediate, publish
from .session import capture_login_qr, login_status, open_login
from .telegram.bootstrap import recover_bot_surface, refresh_home_surface, set_clickable_commands
from .telegram.supervisor import ensure_worker_running, resolve_supervisor_settings, run_supervisor
from .telegram.worker import main as telegram_worker_main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CyberCar standalone CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ["immediate", "collect", "publish"]:
        sub = subparsers.add_parser(name)
        sub.add_argument("--profile", default="")
        sub.add_argument("--platforms", default="")
        sub.add_argument("--limit", type=int, default=0)
        sub.add_argument("--keyword", default="")

    cleanup = subparsers.add_parser("cleanup")
    cleanup.add_argument("--apply", action="store_true", help="Delete expired files instead of dry-run preview.")
    cleanup.add_argument("--print-files", action="store_true", help="Include matched file paths in the JSON summary.")

    login_parser = subparsers.add_parser("login")
    login_sub = login_parser.add_subparsers(dest="login_command", required=True)
    for name in ["status", "open"]:
        sub = login_sub.add_parser(name)
        sub.add_argument("--platform", required=True)
    qr = login_sub.add_parser("qr")
    qr.add_argument("--platform", default="wechat")

    engage = subparsers.add_parser("engage")
    engage_sub = engage.add_subparsers(dest="engage_command", required=True)
    diagnose = engage_sub.add_parser("diagnose")
    diagnose.add_argument("--platform", required=True, choices=["douyin", "kuaishou"])
    for name in ["wechat", "douyin", "kuaishou"]:
        engage_platform = engage_sub.add_parser(name)
        engage_platform.add_argument("--max-posts", type=int, default=0)
        engage_platform.add_argument("--max-replies", type=int, default=0)
        engage_platform.add_argument("--like-only", action="store_true")
        engage_platform.add_argument("--latest-only", action="store_true")
        engage_platform.add_argument("--debug", action="store_true")
    douyin_focused = engage_sub.add_parser("douyin-focused")
    douyin_focused.add_argument("--reply-text", required=True)
    douyin_focused.add_argument("--debug", action="store_true")
    douyin_focused.add_argument("--ignore-state", action="store_true")
    kuaishou_focused = engage_sub.add_parser("kuaishou-focused")
    kuaishou_focused.add_argument("--reply-text", required=True)
    kuaishou_focused.add_argument("--debug", action="store_true")
    kuaishou_focused.add_argument("--ignore-state", action="store_true")

    telegram = subparsers.add_parser("telegram")
    telegram_sub = telegram.add_subparsers(dest="telegram_command", required=True)
    telegram_sub.add_parser("worker", add_help=False)
    telegram_sub.add_parser("set-commands")
    telegram_sub.add_parser("home-refresh")
    supervise_defaults = resolve_supervisor_settings()
    supervise = telegram_sub.add_parser("supervise")
    supervise.add_argument("--once", action="store_true")
    supervise.add_argument(
        "--check-interval-seconds",
        type=int,
        default=int(supervise_defaults["check_interval_seconds"]),
    )
    supervise.add_argument(
        "--stale-heartbeat-seconds",
        type=int,
        default=int(supervise_defaults["stale_heartbeat_seconds"]),
    )
    supervise.add_argument(
        "--startup-grace-seconds",
        type=int,
        default=int(supervise_defaults["startup_grace_seconds"]),
    )
    supervise.add_argument(
        "--recover-retries",
        type=int,
        default=int(supervise_defaults["recover_retries"]),
    )
    supervise.add_argument(
        "--max-recoveries-per-window",
        type=int,
        default=int(supervise_defaults["max_recoveries_per_window"]),
    )
    supervise.add_argument(
        "--recovery-window-seconds",
        type=int,
        default=int(supervise_defaults["recovery_window_seconds"]),
    )
    recover = telegram_sub.add_parser("recover")
    recover.add_argument("--retries", type=int, default=3)

    subparsers.add_parser("migrate-legacy")
    return parser


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _print_json_and_hard_exit(payload: Any, code: int = 0) -> int:
    _print_json(payload)
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os._exit(code)


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
    if command == "cleanup":
        _print_json(run_cleanup(apply=bool(args.apply), print_files=bool(args.print_files)))
        return 0
    if command == "login":
        if args.login_command == "status":
            return _print_json_and_hard_exit(login_status(str(args.platform)))
        if args.login_command == "open":
            return _print_json_and_hard_exit(open_login(str(args.platform)))
        return _print_json_and_hard_exit(capture_login_qr(str(args.platform)))
    if command == "engage" and args.engage_command in {"wechat", "douyin", "kuaishou"}:
        runner = {
            "wechat": run_wechat_engagement,
            "douyin": run_douyin_engagement,
            "kuaishou": run_kuaishou_engagement,
        }[str(args.engage_command)]
        result = runner(
            max_posts=int(args.max_posts),
            max_replies=int(args.max_replies),
            like_only=bool(args.like_only),
            latest_only=bool(args.latest_only),
            debug=bool(args.debug),
        )
        _print_json(result)
        return 0 if bool(result.get("ok")) else 1
    if command == "engage" and args.engage_command == "diagnose":
        result = diagnose_platform_engagement(str(args.platform))
        _print_json(result)
        return 0 if bool(result.get("ok")) else 1
    if command == "engage" and args.engage_command == "douyin-focused":
        result = run_douyin_focused_engagement(
            reply_text=str(args.reply_text),
            debug=bool(args.debug),
            ignore_state=bool(args.ignore_state),
        )
        _print_json(result)
        return 0 if bool(result.get("ok")) else 1
    if command == "engage" and args.engage_command == "kuaishou-focused":
        result = run_kuaishou_focused_engagement(
            reply_text=str(args.reply_text),
            debug=bool(args.debug),
            ignore_state=bool(args.ignore_state),
        )
        _print_json(result)
        return 0 if bool(result.get("ok")) else 1
    if command == "telegram":
        if args.telegram_command == "worker":
            return telegram_worker_main(passthrough)
        if args.telegram_command == "set-commands":
            _print_json(set_clickable_commands())
            return 0
        if args.telegram_command == "supervise":
            if bool(args.once):
                result = ensure_worker_running(
                    stale_heartbeat_seconds=int(args.stale_heartbeat_seconds),
                    startup_grace_seconds=int(args.startup_grace_seconds),
                    recover_retries=int(args.recover_retries),
                    max_recoveries_per_window=int(args.max_recoveries_per_window),
                    recovery_window_seconds=int(args.recovery_window_seconds),
                )
                _print_json(result)
                return 0 if bool(result.get("ok")) else 1
            return run_supervisor(
                check_interval_seconds=int(args.check_interval_seconds),
                stale_heartbeat_seconds=int(args.stale_heartbeat_seconds),
                startup_grace_seconds=int(args.startup_grace_seconds),
                recover_retries=int(args.recover_retries),
                max_recoveries_per_window=int(args.max_recoveries_per_window),
                recovery_window_seconds=int(args.recovery_window_seconds),
            )
        if args.telegram_command == "recover":
            _print_json(recover_bot_surface(retries=int(args.retries)))
            return 0
        _print_json(refresh_home_surface())
        return 0
    if command == "migrate-legacy":
        summary = migrate_legacy_assets()
        _print_json({"copied": summary.copied, "skipped": summary.skipped})
        return 0
    parser.error(f"unsupported command: {command}")
    return 2
