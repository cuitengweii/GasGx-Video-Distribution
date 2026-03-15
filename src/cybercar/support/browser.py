from __future__ import annotations

from typing import Any

from .. import engine


def ensure_debug_port(*, debug_port: int, chrome_user_data_dir: str, startup_url: str = "") -> dict[str, Any]:
    engine._ensure_chrome_debug_port(
        debug_port=debug_port,
        auto_open_chrome=True,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=startup_url,
    )
    return {
        "ok": True,
        "debug_port": int(debug_port),
        "chrome_user_data_dir": str(chrome_user_data_dir),
        "startup_url": str(startup_url or ""),
    }


def connect_browser(*, debug_port: int, chrome_user_data_dir: str, startup_url: str = "") -> Any:
    return engine._connect_chrome(
        debug_port=debug_port,
        auto_open_chrome=True,
        chrome_user_data_dir=chrome_user_data_dir,
        startup_url=startup_url,
    )
