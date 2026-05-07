from __future__ import annotations

from pathlib import Path

from .. import paths as _paths


def repo_root() -> Path:
    return _paths.get_paths().repo_root


def default_config_dir() -> Path:
    configured = repo_root() / "config" / "video_matrix"
    if configured.exists():
        return configured
    return Path(__file__).resolve().parents[3] / "config" / "video_matrix"


def runtime_config_dir() -> Path:
    return repo_root() / "runtime" / "video_matrix" / "config"


def default_config_path(filename: str) -> Path:
    return default_config_dir() / filename


def runtime_config_path(filename: str) -> Path:
    return runtime_config_dir() / filename


def active_config_path(filename: str) -> Path:
    runtime_path = runtime_config_path(filename)
    if runtime_path.exists():
        return runtime_path
    return default_config_path(filename)


def writable_config_path(filename: str) -> Path:
    return runtime_config_path(filename)
