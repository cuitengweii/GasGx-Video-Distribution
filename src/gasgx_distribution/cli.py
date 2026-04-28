from __future__ import annotations

import argparse
import json
from pathlib import Path

import uvicorn

from . import control_plane
from .matrix_publish import run_wechat_publish
from .service import ensure_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GasGx Video Distribution local console.")
    sub = parser.add_subparsers(dest="command", required=True)
    web = sub.add_parser("web")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8765)
    sub.add_parser("init-db")
    sub.add_parser("init-control-db")
    supabase_sql = sub.add_parser("supabase-sql")
    supabase_sql.add_argument("target", choices=["control", "brand", "all"])
    matrix_publish = sub.add_parser("matrix-publish-wechat")
    matrix_publish.add_argument("--limit", type=int, default=0)
    matrix_publish.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init-db":
        ensure_database()
        return 0
    if args.command == "init-control-db":
        control_plane.ensure_control_database()
        return 0
    if args.command == "supabase-sql":
        root = Path(__file__).resolve().parents[2]
        files = {
            "control": root / "config" / "supabase" / "control_plane.sql",
            "brand": root / "config" / "supabase" / "brand_baseline.sql",
        }
        targets = files.keys() if args.target == "all" else [args.target]
        for target in targets:
            path = files[target]
            print(f"-- {target}: {path}")
            print(path.read_text(encoding="utf-8"))
        return 0
    if args.command == "matrix-publish-wechat":
        ensure_database()
        result = run_wechat_publish(limit=int(args.limit), dry_run=bool(args.dry_run))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if bool(result.get("ok")) else 1
    if args.command == "web":
        ensure_database()
        uvicorn.run("gasgx_distribution.web:app", host=str(args.host), port=int(args.port), reload=False)
        return 0
    parser.error(f"unsupported command: {args.command}")
    return 2
