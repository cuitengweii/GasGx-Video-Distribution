from __future__ import annotations

from pathlib import Path

from . import legacy_worker


def refresh_home_surface(
    *,
    bot_token: str,
    chat_id: str,
    workspace: str | Path,
    timeout_seconds: int,
    log_file: str | Path,
    default_profile: str,
) -> None:
    legacy_worker._refresh_home_surface_on_startup(
        bot_token=bot_token,
        chat_id=chat_id,
        workspace=Path(workspace),
        timeout_seconds=timeout_seconds,
        log_file=Path(log_file),
        default_profile=default_profile,
        force_refresh=True,
    )
