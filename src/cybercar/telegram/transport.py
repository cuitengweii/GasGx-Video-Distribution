from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from . import legacy_worker


def call_api(*, bot_token: str, method: str, params: dict[str, Any] | None = None, timeout_seconds: int, use_post: bool = False) -> dict[str, Any]:
    return legacy_worker._telegram_api(
        bot_token=bot_token,
        method=method,
        params=params,
        timeout_seconds=timeout_seconds,
        use_post=use_post,
    )


def send_reply(*, bot_token: str, chat_id: str, text: str, timeout_seconds: int) -> dict[str, Any]:
    return legacy_worker._send_reply(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        timeout_seconds=timeout_seconds,
    )


def send_text_message(*, bot_token: str, chat_id: str, text: str, timeout_seconds: int) -> dict[str, Any]:
    return legacy_worker._send_text_message(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        timeout_seconds=timeout_seconds,
    )


def send_card_message(*, bot_token: str, chat_id: str, card: Mapping[str, Any], timeout_seconds: int) -> dict[str, Any]:
    return legacy_worker._send_card_message(
        bot_token=bot_token,
        chat_id=chat_id,
        card=dict(card),
        timeout_seconds=timeout_seconds,
    )


def send_loading_placeholder(*, bot_token: str, chat_id: str, text: str, timeout_seconds: int) -> dict[str, Any]:
    return legacy_worker._send_loading_placeholder(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        timeout_seconds=timeout_seconds,
    )


def set_clickable_commands(*, bot_token: str, timeout_seconds: int, log_file: str | Path) -> None:
    legacy_worker._set_clickable_commands(
        bot_token=bot_token,
        timeout_seconds=timeout_seconds,
        log_file=Path(log_file),
    )
