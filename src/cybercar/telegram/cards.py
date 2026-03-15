from __future__ import annotations

from typing import Any, Mapping, Sequence

from ..common.telegram_ui import (
    build_action_feedback,
    build_home_callback_data,
    build_telegram_card,
    build_telegram_home,
    parse_home_callback_data,
)
from . import legacy_worker


def build_home_card(*, default_profile: str, workspace: str, chat_id: str = "", status_note: str = "") -> dict[str, Any]:
    return legacy_worker._build_home_card(
        default_profile=default_profile,
        workspace=legacy_worker.Path(workspace),
        chat_id=chat_id,
        status_note=status_note,
    )


def build_prefilter_card(item: Mapping[str, Any], item_id: str) -> dict[str, Any]:
    return legacy_worker._build_immediate_publish_confirm_card(dict(item), item_id)


__all__ = [
    "build_action_feedback",
    "build_home_callback_data",
    "build_home_card",
    "build_prefilter_card",
    "build_telegram_card",
    "build_telegram_home",
    "parse_home_callback_data",
]
