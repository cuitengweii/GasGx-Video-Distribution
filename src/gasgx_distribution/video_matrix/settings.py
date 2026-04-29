from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


DEFAULT_COMPOSITION_SEQUENCE = [
    {"category_id": "category_A", "duration": 1.5},
    {"category_id": "category_B", "duration": 3.4},
    {"category_id": "category_A", "duration": 1.5},
    {"category_id": "category_C", "duration": 3.0},
]
DEFAULT_BEAT_DETECTION = {
    "mode": "auto",
    "target_bpm_min": 120,
    "target_bpm_max": 130,
    "fallback_spacing": 0.48,
}


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
    material_categories: list[dict[str, str]]
    video_duration_min: float
    video_duration_max: float
    default_title_prefix: str
    website_url: str
    hud_enable_live_data: bool
    hud_fixed_formulas: list[str]
    slogans: list[str]
    titles: list[str]
    hud_sources: dict[str, str]
    composition_sequence: list[dict[str, object]]
    beat_detection: dict[str, object]
    max_variant_attempts: int
    variant_history_enabled: bool
    variant_history_limit: int
    enhancement_modules: dict[str, object]
    copy_mode: str

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
            material_categories=_material_categories(payload.get("material_categories")),
            video_duration_min=float(payload["video_duration_min"]),
            video_duration_max=float(payload["video_duration_max"]),
            default_title_prefix=str(payload["default_title_prefix"]),
            website_url=str(payload["website_url"]),
            hud_enable_live_data=bool(payload["hud_enable_live_data"]),
            hud_fixed_formulas=list(payload["hud_fixed_formulas"]),
            slogans=list(payload["slogans"]),
            titles=list(payload["titles"]),
            hud_sources=dict(payload["hud_sources"]),
            composition_sequence=_composition_sequence(payload.get("composition_sequence")),
            beat_detection=_beat_detection(payload.get("beat_detection")),
            max_variant_attempts=max(1, int(payload.get("max_variant_attempts", 20) or 20)),
            variant_history_enabled=bool(payload.get("variant_history_enabled", True)),
            variant_history_limit=max(0, int(payload.get("variant_history_limit", 5000) or 5000)),
            enhancement_modules=dict(payload.get("enhancement_modules") or {"enabled": False, "modules": []}),
            copy_mode=str(payload.get("copy_mode") or "spark_then_template"),
        )


def _material_categories(raw: object) -> list[dict[str, str]]:
    fallback = [
        {"id": "category_A", "label": "A 类"},
        {"id": "category_B", "label": "B 类"},
        {"id": "category_C", "label": "C 类"},
    ]
    if not isinstance(raw, list):
        return fallback
    categories: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        category_id = str(item.get("id") or "").strip()
        if not category_id or category_id in seen or "/" in category_id or "\\" in category_id:
            continue
        seen.add(category_id)
        categories.append({"id": category_id, "label": str(item.get("label") or category_id).strip() or category_id})
    return categories or fallback


def _composition_sequence(raw: object) -> list[dict[str, object]]:
    if not isinstance(raw, list):
        return [dict(item) for item in DEFAULT_COMPOSITION_SEQUENCE]
    sequence: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        category_id = str(item.get("category_id") or "").strip()
        if not category_id:
            continue
        try:
            duration = float(item.get("duration", 0))
        except (TypeError, ValueError):
            continue
        if duration <= 0:
            continue
        sequence.append({"category_id": category_id, "duration": duration})
    return sequence or [dict(item) for item in DEFAULT_COMPOSITION_SEQUENCE]


def _beat_detection(raw: object) -> dict[str, object]:
    if not isinstance(raw, dict):
        return dict(DEFAULT_BEAT_DETECTION)
    config = dict(DEFAULT_BEAT_DETECTION)
    config.update(raw)
    try:
        config["target_bpm_min"] = int(config.get("target_bpm_min", 120) or 120)
        config["target_bpm_max"] = int(config.get("target_bpm_max", 130) or 130)
        config["fallback_spacing"] = float(config.get("fallback_spacing", 0.48) or 0.48)
    except (TypeError, ValueError):
        return dict(DEFAULT_BEAT_DETECTION)
    if config["target_bpm_min"] <= 0 or config["target_bpm_max"] < config["target_bpm_min"]:
        config["target_bpm_min"] = 120
        config["target_bpm_max"] = 130
    if config["fallback_spacing"] <= 0:
        config["fallback_spacing"] = 0.48
    config["mode"] = str(config.get("mode") or "auto")
    return config
