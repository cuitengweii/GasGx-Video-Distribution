"""Resolve the video matrix batch export root (same rules as the matrix UI / generate job)."""

from __future__ import annotations

from pathlib import Path

from .. import paths as _paths
from .settings import ProjectSettings
from .ui_state import load_ui_state


def resolve_video_matrix_output_root() -> Path:
    repo = _paths.get_paths().repo_root
    config_path = repo / "config" / "video_matrix" / "defaults.json"
    if not config_path.exists():
        return (repo / "runtime" / "materials" / "videos").resolve()
    settings = ProjectSettings.from_file(config_path)
    ui_path = repo / "config" / "video_matrix" / "ui_state.json"
    ui_state = load_ui_state(ui_path)
    output_root = Path(str(ui_state.get("output_root") or settings.output_root)).expanduser()
    if not output_root.is_absolute():
        output_root = repo / output_root
    return output_root.resolve()
