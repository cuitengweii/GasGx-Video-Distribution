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
    scene_tag: str = ""
    subject_tag: list[str] = field(default_factory=list)
    action_tag: list[str] = field(default_factory=list)
    shot_size: str = ""
    camera_angle: str = ""
    camera_move: str = ""
    audio_state: str = ""
    language_tag: str = ""
    usable_range: tuple[float, float] | None = None
    account_fit_tags: list[str] = field(default_factory=list)
    rights_status: str = ""
    hero_frame_candidates: list[float] = field(default_factory=list)

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
            "scene_tag": self.scene_tag,
            "subject_tag": self.subject_tag,
            "action_tag": self.action_tag,
            "shot_size": self.shot_size,
            "camera_angle": self.camera_angle,
            "camera_move": self.camera_move,
            "audio_state": self.audio_state,
            "language_tag": self.language_tag,
            "usable_range": list(self.usable_range) if self.usable_range is not None else None,
            "account_fit_tags": self.account_fit_tags,
            "rights_status": self.rights_status,
            "hero_frame_candidates": self.hero_frame_candidates,
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
    narrative_template_id: str = ""
    account_pool_id: str = ""
    cover_frame_offset: float = 0.0
    text_variant_id: str = ""
    visual_plan_key: str = ""
    structure_variant_id: str = ""
    bgm_start_offset: float = 0.0
    bgm_offset_bucket: str = ""
    bgm_name: str = ""
    bgm_path: Path | None = None
    text_signature: str = ""
    structure_signature: str = ""
    first_frame_hash: str = ""
    cover_frame_hash: str = ""
    content_fingerprint: str = ""
    bgm_fingerprint: str = ""
    ending_follow_text: str = ""
    publish_description: str = ""
    dedupe_result: "DedupeResult | None" = None


@dataclass(slots=True)
class SimilarityReport:
    visual_score: float = 0.0
    audio_score: float = 0.0
    text_score: float = 0.0
    structure_score: float = 0.0
    total_score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    strongest_match: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "visual_score": self.visual_score,
            "audio_score": self.audio_score,
            "text_score": self.text_score,
            "structure_score": self.structure_score,
            "total_score": self.total_score,
            "reasons": list(self.reasons),
            "strongest_match": dict(self.strongest_match),
        }


@dataclass(slots=True)
class DedupeResult:
    status: str = "pass"
    action: str = "pass"
    retry_count: int = 0
    mutation_history: list[str] = field(default_factory=list)
    report: SimilarityReport = field(default_factory=SimilarityReport)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "action": self.action,
            "retry_count": self.retry_count,
            "mutation_history": list(self.mutation_history),
            "report": self.report.as_dict(),
        }


@dataclass(slots=True)
class RenderedAsset:
    variant: VideoVariant
    video_path: Path
    cover_path: Path | None
    copy_path: Path | None
    manifest_path: Path | None
