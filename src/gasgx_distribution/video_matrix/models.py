from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class ClipMetadata:
    clip_id: str
    source_path: Path
    normalized_path: Path
    category: str
    duration: float
    width: int
    height: int
    fps: float
    brightness_score: float
    contrast_score: float
    used_in_batch: bool = False
    tags: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "clip_id": self.clip_id,
            "source_path": str(self.source_path),
            "normalized_path": str(self.normalized_path),
            "category": self.category,
            "duration": self.duration,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
            "brightness_score": self.brightness_score,
            "contrast_score": self.contrast_score,
            "used_in_batch": self.used_in_batch,
            "tags": self.tags,
        }


@dataclass(slots=True)
class SegmentPlan:
    category: str
    clip: ClipMetadata
    start_time: float
    duration: float
    index: int


@dataclass(slots=True)
class VideoVariant:
    sequence_number: int
    title: str
    slogan: str
    hud_lines: list[str]
    lut_strength: float
    zoom: float
    mirror: bool
    x_offset: int
    y_offset: int
    segments: list[SegmentPlan]
    signature: str


@dataclass(slots=True)
class RenderedAsset:
    variant: VideoVariant
    video_path: Path
    cover_path: Path | None
    copy_path: Path | None
    manifest_path: Path | None
