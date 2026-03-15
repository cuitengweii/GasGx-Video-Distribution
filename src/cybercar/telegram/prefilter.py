from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .. import pipeline
from . import legacy_worker


def load_queue(path: str | Path) -> dict[str, Any]:
    return legacy_worker._load_prefilter_queue(Path(path))


def save_queue(path: str | Path, queue: Mapping[str, Any]) -> Path:
    target = Path(path)
    legacy_worker._save_prefilter_queue(target, dict(queue))
    return target


def build_confirm_card(item: Mapping[str, Any], item_id: str) -> dict[str, Any]:
    return legacy_worker._build_immediate_publish_confirm_card(dict(item), item_id)


def service(ctx: Any, args: Any, email_settings: Any) -> None:
    pipeline._run_telegram_prefilter(ctx, args, email_settings)
