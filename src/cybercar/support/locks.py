from __future__ import annotations

from pathlib import Path


def acquire_directory_lock(path: str | Path) -> Path | None:
    target = Path(path)
    try:
        target.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        return None
    return target


def release_directory_lock(path: str | Path) -> None:
    target = Path(path)
    if target.exists():
        target.rmdir()
