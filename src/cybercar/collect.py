from __future__ import annotations

from .orchestrator import run_mode


def collect(*, profile: str = "", limit: int = 0, keyword: str = "", passthrough: list[str] | None = None) -> int:
    return run_mode(mode="collect", profile_name=profile, limit=limit, keyword=keyword, passthrough=passthrough or [])
