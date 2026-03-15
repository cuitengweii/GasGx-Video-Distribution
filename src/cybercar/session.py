from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from . import engine
from .settings import apply_runtime_environment, load_app_config


def _chrome_settings() -> tuple[int, int]:
    app_config = load_app_config()
    chrome_cfg = app_config.get("chrome") if isinstance(app_config.get("chrome"), dict) else {}
    return int(chrome_cfg.get("default_debug_port") or 9333), int(chrome_cfg.get("wechat_debug_port") or 9334)


def _profile_dir(platform: str) -> Path:
    paths = apply_runtime_environment()
    return paths.wechat_profile_dir if str(platform or "").strip().lower() == "wechat" else paths.default_profile_dir


def _open_url(platform: str) -> str:
    token = str(platform or "").strip().lower()
    return str(engine.PLATFORM_LOGIN_ENTRY_URLS.get(token) or engine.CREATE_POST_URL)


def login_status(platform: str) -> dict[str, Any]:
    default_port, wechat_port = _chrome_settings()
    token = str(platform or "").strip().lower()
    debug_port = wechat_port if token == "wechat" else default_port
    return engine.probe_platform_session_via_debug_port(
        platform_name=token,
        open_url=_open_url(token),
        debug_port=debug_port,
        chrome_user_data_dir=str(_profile_dir(token)),
    )


def open_login(platform: str) -> dict[str, Any]:
    default_port, wechat_port = _chrome_settings()
    token = str(platform or "").strip().lower()
    debug_port = wechat_port if token == "wechat" else default_port
    open_url = _open_url(token)
    engine._ensure_chrome_debug_port(
        debug_port=debug_port,
        auto_open_chrome=True,
        chrome_user_data_dir=str(_profile_dir(token)),
        startup_url=open_url,
    )
    return {
        "ok": True,
        "platform": token,
        "debug_port": debug_port,
        "profile_dir": str(_profile_dir(token)),
        "open_url": open_url,
    }


def capture_login_qr(platform: str = "wechat") -> dict[str, Any]:
    token = str(platform or "wechat").strip().lower()
    if token != "wechat":
        raise RuntimeError("login qr is currently supported for wechat only")
    paths = apply_runtime_environment()
    _, wechat_port = _chrome_settings()
    page = engine._connect_chrome(
        debug_port=wechat_port,
        auto_open_chrome=True,
        chrome_user_data_dir=str(paths.wechat_profile_dir),
        startup_url=_open_url("wechat"),
    )
    page.get(_open_url("wechat"))
    engine._prepare_platform_login_qr_surface(page, platform_name="wechat")
    qr_path = paths.log_dir / f"wechat_login_qr_{time.strftime('%Y%m%d_%H%M%S')}.png"
    source = engine._extract_login_qr_source(page, platform_name="wechat")
    if source:
        _, payload = engine._load_qr_image_source(source)
        qr_path.write_bytes(payload)
        return {"ok": True, "source": "qr_image", "path": str(qr_path)}
    screenshot = engine._capture_login_qr_screenshot(page, platform_name="wechat")
    qr_path.write_bytes(screenshot)
    return {"ok": True, "source": "screenshot", "path": str(qr_path)}
