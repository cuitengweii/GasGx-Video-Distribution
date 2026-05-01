from __future__ import annotations

from dataclasses import dataclass

from .settings import ProjectSettings


@dataclass(slots=True)
class HudPayload:
    lines: list[str]
    used_live_data: bool


def build_hud_payload(settings: ProjectSettings, timeout: float = 4.0) -> HudPayload:
    return HudPayload(lines=[], used_live_data=False)
