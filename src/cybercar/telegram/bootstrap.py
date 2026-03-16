from __future__ import annotations

import sys
import time
from pathlib import Path

from ..common.bot_notify import resolve_telegram_bot_settings
from ..support.config import load_default_profile_name, load_notify_config, load_telegram_config
from ..support.paths import apply_runtime_environment, get_paths
from . import legacy_worker
from .home import refresh_home_surface as _refresh_home_surface
from .transport import set_clickable_commands as _set_clickable_commands


def _contains_flag(argv: list[str], flag: str) -> bool:
    return any(str(item).strip() == flag for item in argv)


def build_worker_argv(passthrough: list[str] | None = None) -> list[str]:
    apply_runtime_environment()
    paths = get_paths()
    telegram_cfg = load_telegram_config()
    target = _resolve_target()
    argv = list(passthrough or [])
    if not _contains_flag(argv, "--repo-root"):
        argv = ["--repo-root", str(paths.repo_root), *argv]
    if not _contains_flag(argv, "--workspace"):
        argv = ["--workspace", str(paths.runtime_root), *argv]
    if not _contains_flag(argv, "--default-profile"):
        argv = ["--default-profile", load_default_profile_name(), *argv]
    if not _contains_flag(argv, "--poll-timeout-seconds"):
        argv = ["--poll-timeout-seconds", str(int(telegram_cfg.get("poll_timeout_seconds") or 10)), *argv]
    if not _contains_flag(argv, "--poll-interval-seconds"):
        argv = ["--poll-interval-seconds", str(int(telegram_cfg.get("poll_interval_seconds") or 0)), *argv]
    if not _contains_flag(argv, "--poll-network-failure-restart-threshold"):
        argv = [
            "--poll-network-failure-restart-threshold",
            str(int(telegram_cfg.get("restart_threshold") or 6)),
            *argv,
        ]
    if not _contains_flag(argv, "--telegram-bot-token"):
        bot_token = str(target.get("bot_token") or "").strip()
        if bot_token:
            argv = ["--telegram-bot-token", bot_token, *argv]
    if not _contains_flag(argv, "--telegram-chat-id"):
        chat_id = str(target.get("chat_id") or "").strip()
        if chat_id:
            argv = ["--telegram-chat-id", chat_id, *argv]
    return argv


def _resolve_target() -> dict[str, object]:
    apply_runtime_environment()
    paths = get_paths()
    notify_cfg = load_notify_config()
    telegram_cfg = load_telegram_config()
    resolved = resolve_telegram_bot_settings(
        {
            "registry_file": str(telegram_cfg.get("registry_file") or "").strip(),
            "timeout_seconds": int(telegram_cfg.get("poll_timeout_seconds") or 10) + 15,
        },
        env_prefix=str(notify_cfg.get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )
    return {
        "paths": paths,
        "telegram_config": telegram_cfg,
        "bot_token": str(resolved.get("bot_token") or "").strip(),
        "chat_id": str(resolved.get("chat_id") or "").strip(),
        "default_profile": load_default_profile_name(),
    }


def worker_main(passthrough: list[str] | None = None) -> int:
    argv = build_worker_argv(passthrough)
    old_argv = sys.argv
    sys.argv = [old_argv[0], *argv]
    try:
        return int(legacy_worker.main())
    finally:
        sys.argv = old_argv


def set_clickable_commands() -> dict[str, object]:
    target = _resolve_target()
    bot_token = str(target.get("bot_token") or "")
    if not bot_token:
        raise RuntimeError("telegram bot token is not configured")
    paths = target["paths"]
    assert isinstance(paths, object)
    log_file = get_paths().log_dir / f"telegram_worker_{time.strftime('%Y%m%d')}.log"
    _set_clickable_commands(
        bot_token=bot_token,
        timeout_seconds=max(10, int(target["telegram_config"].get("poll_timeout_seconds") or 10) + 5),  # type: ignore[index]
        log_file=log_file,
    )
    return {"ok": True, "log_file": str(log_file)}


def refresh_home_surface() -> dict[str, object]:
    target = _resolve_target()
    bot_token = str(target.get("bot_token") or "")
    chat_id = str(target.get("chat_id") or "")
    if not bot_token or not chat_id:
        raise RuntimeError("telegram bot token/chat_id is not configured")
    paths = get_paths()
    log_file = paths.log_dir / f"telegram_worker_{time.strftime('%Y%m%d')}.log"
    telegram_cfg = target["telegram_config"]
    _refresh_home_surface(
        bot_token=bot_token,
        chat_id=chat_id,
        workspace=paths.runtime_root,
        timeout_seconds=max(30, int(telegram_cfg.get("poll_timeout_seconds") or 10) + 20),  # type: ignore[index]
        log_file=log_file,
        default_profile=str(target.get("default_profile") or "cybertruck"),
    )
    return {"ok": True, "log_file": str(log_file)}


def recover_bot_surface(*, retries: int = 3, retry_delay_seconds: float = 2.0) -> dict[str, object]:
    target = _resolve_target()
    bot_token = str(target.get("bot_token") or "")
    chat_id = str(target.get("chat_id") or "")
    if not bot_token or not chat_id:
        raise RuntimeError("telegram bot token/chat_id is not configured")
    paths = get_paths()
    log_file = paths.log_dir / f"telegram_worker_{time.strftime('%Y%m%d')}.log"
    telegram_cfg = target["telegram_config"]
    timeout_seconds = max(30, int(telegram_cfg.get("poll_timeout_seconds") or 10) + 20)  # type: ignore[index]
    attempts = max(1, int(retries))
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            _set_clickable_commands(
                bot_token=bot_token,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
            )
            _refresh_home_surface(
                bot_token=bot_token,
                chat_id=chat_id,
                workspace=paths.runtime_root,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
                default_profile=str(target.get("default_profile") or "cybertruck"),
            )
            return {
                "ok": True,
                "attempts": attempt,
                "log_file": str(log_file),
                "chat_id": chat_id,
            }
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(max(0.5, float(retry_delay_seconds)))

    raise RuntimeError(f"telegram recover failed after {attempts} attempts: {last_error}") from last_error
