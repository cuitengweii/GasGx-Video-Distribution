from __future__ import annotations

from pathlib import Path
from typing import Any

from ..support import get_paths
from . import legacy_worker


def workspace_root() -> Path:
    return get_paths().runtime_root


def home_state_path(workspace: str | Path | None = None) -> Path:
    return legacy_worker._home_state_path(Path(workspace) if workspace else workspace_root())


def action_queue_path(workspace: str | Path | None = None) -> Path:
    return legacy_worker._action_queue_path(Path(workspace) if workspace else workspace_root())


def prefilter_queue_path(workspace: str | Path | None = None) -> Path:
    return legacy_worker._prefilter_queue_path(Path(workspace) if workspace else workspace_root())


def pending_feedback_path(workspace: str | Path | None = None) -> Path:
    return legacy_worker._pending_background_feedback_path(Path(workspace) if workspace else workspace_root())


def load_state(path: str | Path) -> dict[str, Any]:
    return legacy_worker._load_state(Path(path))


def save_state(path: str | Path, state: dict[str, Any]) -> Path:
    target = Path(path)
    legacy_worker._save_state(target, dict(state))
    return target
