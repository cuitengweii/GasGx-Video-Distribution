from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from . import pipeline
from .settings import apply_runtime_environment, load_app_config, load_profile_config


class ProfileMapping:
    def __init__(self, *, name: str, payload: dict[str, Any]) -> None:
        self.name = name
        self.keyword = str(payload.get("keyword") or "").strip()
        self.exclude_keywords = _normalize_list(payload.get("exclude_keywords"))
        self.require_any_keywords = _normalize_list(payload.get("require_any_keywords"))
        upload_platforms = payload.get("target_platforms") or payload.get("upload_platforms")
        if isinstance(upload_platforms, list):
            self.upload_platforms = ",".join(_normalize_list(upload_platforms))
        else:
            self.upload_platforms = str(upload_platforms or "").strip()
        source_platforms = payload.get("source_platforms")
        if isinstance(source_platforms, list):
            self.source_platforms = ",".join(_normalize_list(source_platforms))
        else:
            self.source_platforms = str(source_platforms or "").strip()
        self.source_keywords = _normalize_list(payload.get("source_keywords"))
        self.source_watch_accounts = _normalize_watch_accounts(payload.get("source_watch_accounts"))
        self.collect_limit = _to_int(payload.get("collect_limit"), 0)
        self.publish_limit = _to_int(payload.get("publish_limit"), 0)


def _normalize_list(raw: Any) -> list[str]:
    if isinstance(raw, str):
        values = [part.strip() for part in raw.replace("\n", ",").split(",")]
    elif isinstance(raw, list):
        values = [str(item).strip() for item in raw]
    else:
        return []
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _to_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _normalize_watch_accounts(raw: Any) -> dict[str, list[str]]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, list[str]] = {}
    for key, value in raw.items():
        platform = str(key or "").strip().lower()
        if not platform:
            continue
        entries = _normalize_list(value)
        if entries:
            result[platform] = entries
    return result


def _resolve_profile(profile_name: str) -> ProfileMapping:
    cfg = load_profile_config()
    profiles = cfg.get("profiles") if isinstance(cfg.get("profiles"), dict) else {}
    default_profile = str(cfg.get("default_profile") or "cybertruck").strip() or "cybertruck"
    chosen = str(profile_name or default_profile).strip() or default_profile
    payload = profiles.get(chosen)
    if not isinstance(payload, dict):
        payload = {}
    return ProfileMapping(name=chosen, payload=payload)


def _merge_runtime_config(base: dict[str, Any], profile: ProfileMapping) -> dict[str, Any]:
    merged = dict(base)
    if profile.exclude_keywords:
        merged["exclude_keywords"] = profile.exclude_keywords
    if profile.require_any_keywords:
        merged["require_any_keywords"] = profile.require_any_keywords
    if profile.source_platforms or profile.source_keywords or profile.source_watch_accounts:
        sources = merged.get("sources") if isinstance(merged.get("sources"), dict) else {}
        merged_sources = dict(sources)
        if profile.source_platforms:
            merged_sources["platforms"] = profile.source_platforms
        if profile.source_keywords:
            merged_sources["keywords"] = list(profile.source_keywords)
        if profile.source_watch_accounts:
            merged_sources["watch_accounts"] = dict(profile.source_watch_accounts)
        merged["sources"] = merged_sources
    return merged


def _write_temp_config(payload: dict[str, Any]) -> str:
    fd, temp_path = tempfile.mkstemp(prefix="cybercar_runtime_", suffix=".json")
    os.close(fd)
    path = Path(temp_path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def run_mode(
    *,
    mode: str,
    profile_name: str = "",
    platforms: str = "",
    limit: int = 0,
    keyword: str = "",
    passthrough: list[str] | None = None,
) -> int:
    paths = apply_runtime_environment()
    app_config = load_app_config()
    profile = _resolve_profile(profile_name)
    merged_config = _merge_runtime_config(app_config, profile)
    runtime_config_path = str(paths.app_config_path)
    temp_config_path = ""
    if merged_config != app_config:
        temp_config_path = _write_temp_config(merged_config)
        runtime_config_path = temp_config_path

    chrome_cfg = app_config.get("chrome") if isinstance(app_config.get("chrome"), dict) else {}
    publish_cfg = app_config.get("publish") if isinstance(app_config.get("publish"), dict) else {}
    platform_text = str(platforms or "").strip() or profile.upload_platforms or str(
        publish_cfg.get("default_platforms") or ""
    ).strip()
    active_limit = limit
    if active_limit <= 0:
        if mode == "collect":
            active_limit = profile.collect_limit
        elif mode in {"publish", "pipeline"}:
            active_limit = profile.publish_limit
    args = [
        "--workspace",
        str(paths.runtime_root),
        "--config",
        runtime_config_path,
        "--chrome-user-data-dir",
        str(paths.default_profile_dir),
        "--wechat-chrome-user-data-dir",
        str(paths.wechat_profile_dir),
        "--x-chrome-user-data-dir",
        str(paths.x_profile_dir),
        "--x-cookie-file",
        str(paths.x_cookie_file_path),
        "--debug-port",
        str(chrome_cfg.get("default_debug_port") or 9333),
        "--wechat-debug-port",
        str(chrome_cfg.get("wechat_debug_port") or 9334),
        "--x-debug-port",
        str(chrome_cfg.get("x_debug_port") or 9335),
    ]
    effective_keyword = str(keyword or "").strip() or profile.keyword
    if effective_keyword:
        args += ["--keyword", effective_keyword]
    if active_limit > 0:
        args += ["--limit", str(active_limit)]
        if mode in {"publish", "pipeline"}:
            args += ["--non-wechat-max-videos", str(active_limit)]
    if mode == "collect":
        args.append("--collect-only")
    elif mode == "publish":
        args.append("--publish-only")
    if mode in {"publish", "pipeline"}:
        args.append("--wechat-publish-now")
        if platform_text:
            args += ["--upload-platforms", platform_text]
    if passthrough:
        args += passthrough
    old_argv = sys.argv
    sys.argv = [old_argv[0], *args]
    try:
        return int(pipeline.main())
    finally:
        sys.argv = old_argv
        if temp_config_path:
            Path(temp_config_path).unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CyberCar pipeline orchestrator.")
    parser.add_argument("--mode", choices=["collect", "publish", "pipeline"], default="pipeline")
    parser.add_argument("--profile", default="")
    parser.add_argument("--platforms", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--keyword", default="")
    return parser


def main() -> int:
    parser = build_parser()
    args, passthrough = parser.parse_known_args()
    return run_mode(
        mode=str(args.mode),
        profile_name=str(args.profile),
        platforms=str(args.platforms),
        limit=int(args.limit),
        keyword=str(args.keyword),
        passthrough=passthrough,
    )
