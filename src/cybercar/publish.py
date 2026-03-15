from __future__ import annotations

from .orchestrator import run_mode


def publish(
    *,
    profile: str = "",
    platforms: str = "",
    limit: int = 0,
    keyword: str = "",
    passthrough: list[str] | None = None,
) -> int:
    return run_mode(
        mode="publish",
        profile_name=profile,
        platforms=platforms,
        limit=limit,
        keyword=keyword,
        passthrough=passthrough or [],
    )


def immediate(
    *,
    profile: str = "",
    platforms: str = "",
    limit: int = 0,
    keyword: str = "",
    passthrough: list[str] | None = None,
) -> int:
    return run_mode(
        mode="pipeline",
        profile_name=profile,
        platforms=platforms,
        limit=limit,
        keyword=keyword,
        passthrough=passthrough or [],
    )
