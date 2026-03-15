from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from . import legacy_worker


def acquire_poller_lock(*, workspace: str | Path, log_file: str | Path) -> Path | None:
    return legacy_worker._acquire_poller_lock(
        workspace=Path(workspace),
        log_file=Path(log_file),
    )


def with_prefilter_queue_lock(workspace: str | Path, mutate: Callable[[dict[str, Any]], Any]) -> Any:
    return legacy_worker._with_prefilter_queue_lock(Path(workspace), mutate)


def with_platform_lock(workspace: str | Path, platform: str, mutate: Callable[[], Any]) -> Any:
    return legacy_worker._with_platform_lock(
        workspace=Path(workspace),
        platform=platform,
        callback=mutate,
    )
