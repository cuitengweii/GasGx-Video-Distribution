from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ProjectSettings:
    project_name: str
    source_root: Path
    library_root: Path
    output_root: Path
    output_count: int
    target_width: int
    target_height: int
    target_fps: int
    recent_limits: dict[str, int]
    video_duration_min: float
    video_duration_max: float
    default_title_prefix: str
    website_url: str
    hud_enable_live_data: bool
    hud_fixed_formulas: list[str]
    slogans: list[str]
    titles: list[str]
    hud_sources: dict[str, str]

    @classmethod
    def from_file(cls, config_path: Path) -> "ProjectSettings":
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        base_dir = config_path.parent.parent
        return cls(
            project_name=payload["project_name"],
            source_root=(base_dir / payload["source_root"]).resolve(),
            library_root=(base_dir / payload["library_root"]).resolve(),
            output_root=(base_dir / payload["output_root"]).resolve(),
            output_count=int(payload["output_count"]),
            target_width=int(payload["target_width"]),
            target_height=int(payload["target_height"]),
            target_fps=int(payload["target_fps"]),
            recent_limits={key: int(value) for key, value in payload.get("recent_limits", {}).items()},
            video_duration_min=float(payload["video_duration_min"]),
            video_duration_max=float(payload["video_duration_max"]),
            default_title_prefix=str(payload["default_title_prefix"]),
            website_url=str(payload["website_url"]),
            hud_enable_live_data=bool(payload["hud_enable_live_data"]),
            hud_fixed_formulas=list(payload["hud_fixed_formulas"]),
            slogans=list(payload["slogans"]),
            titles=list(payload["titles"]),
            hud_sources=dict(payload["hud_sources"]),
        )
