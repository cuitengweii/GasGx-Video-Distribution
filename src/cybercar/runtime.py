from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .settings import AppPaths, get_paths, load_app_config


@dataclass(frozen=True)
class RuntimeContext:
    paths: AppPaths
    app_config: dict[str, Any]


def current_runtime() -> RuntimeContext:
    return RuntimeContext(paths=get_paths(), app_config=load_app_config())


def ensure_runtime_layout() -> RuntimeContext:
    ctx = current_runtime()
    ctx.paths.ensure()
    return ctx


def runtime_log_dir() -> Path:
    return ensure_runtime_layout().paths.log_dir
