from __future__ import annotations

from typing import Any

from ..settings import apply_runtime_environment, load_app_config, load_profile_config


def load_notify_config() -> dict[str, Any]:
    cfg = load_app_config().get("notify")
    return dict(cfg) if isinstance(cfg, dict) else {}


def load_telegram_config() -> dict[str, Any]:
    cfg = load_app_config().get("telegram")
    return dict(cfg) if isinstance(cfg, dict) else {}


def load_default_profile_name() -> str:
    cfg = load_profile_config()
    return str(cfg.get("default_profile") or "cybertruck").strip() or "cybertruck"


__all__ = [
    "apply_runtime_environment",
    "load_app_config",
    "load_default_profile_name",
    "load_notify_config",
    "load_profile_config",
    "load_telegram_config",
]
