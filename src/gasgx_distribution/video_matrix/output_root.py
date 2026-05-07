"""Resolve the video matrix batch export root (same rules as the matrix UI / generate job)."""

from __future__ import annotations

from pathlib import Path

from .. import paths as _paths
from .config_store import runtime_config_path
from .settings import ProjectSettings
from .ui_state import load_ui_state


def resolve_video_matrix_output_root() -> Path:
    repo = _paths.get_paths().repo_root
    runtime_settings_path = runtime_config_path("defaults.json")
    repo_settings_path = repo / "config" / "video_matrix" / "defaults.json"
    config_path = runtime_settings_path if runtime_settings_path.exists() else repo_settings_path
    if not config_path.exists():
        return (repo / "runtime" / "materials" / "videos").resolve()
    settings = ProjectSettings.from_file(config_path)
    repo_ui_state_path = repo / "config" / "video_matrix" / "ui_state.json"
    ui_state = load_ui_state(
        runtime_config_path("ui_state.json"),
        fallback_path=repo_ui_state_path if repo_ui_state_path.exists() else None,
    )
    output_root = Path(str(ui_state.get("output_root") or settings.output_root)).expanduser()
    if not output_root.is_absolute():
        output_root = repo / output_root
    return output_root.resolve()
