from __future__ import annotations

from typing import Any

from ..common.telegram_ui import build_home_callback_data, parse_home_callback_data
from . import legacy_worker


def handle_command(*, update_id: int, text: str, chat_id: str, username: str, **kwargs: Any) -> dict[str, Any]:
    return legacy_worker._handle_command(
        update_id=update_id,
        text=text,
        chat_id=chat_id,
        username=username,
        **kwargs,
    )


def handle_home_callback(*, update_id: int, callback: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return legacy_worker._handle_home_callback(
        update_id=update_id,
        callback=callback,
        **kwargs,
    )


__all__ = ["build_home_callback_data", "handle_command", "handle_home_callback", "parse_home_callback_data"]
