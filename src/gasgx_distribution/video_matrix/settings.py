from __future__ import annotations

import json
from dataclasses import dataclass, field
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
DEFAULT_DEDUPE_POLICY = {
    "same_account_strictness": 0.04,
    "max_mutation_retries": 1,
    "borderline_total": 0.80,
    "reject_total": 0.88,
    "single_dimension_retry": True,
    "visual_retry": 0.90,
    "text_retry": 0.96,
    "structure_retry": 0.86,
    "preflight_avoidance_enabled": True,
    "text_preflight_threshold": 0.86,
    "structure_preflight_threshold": 0.72,
    "hook_cooldown_enabled": True,
    "bgm_random_offset_enabled": True,
    "bgm_offset_bucket_seconds": 8,
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
    slogans: list[str]
    titles: list[str]
    composition_sequence: list[dict[str, object]]
    beat_detection: dict[str, object]
    max_variant_attempts: int
    variant_history_enabled: bool
    variant_history_limit: int
    enhancement_modules: dict[str, object]
    copy_mode: str
    daily_output_goal: int = 50
    narrative_templates: list[dict[str, object]] = field(default_factory=list)
    dedupe_policy: dict[str, object] = field(default_factory=lambda: dict(DEFAULT_DEDUPE_POLICY))

    @classmethod
    def from_file(cls, config_path: Path) -> "ProjectSettings":
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        return cls(
            project_name=payload["project_name"],
            source_root=_resolve_config_path(config_path, payload["source_root"]),
            library_root=_resolve_config_path(config_path, payload["library_root"]),
            output_root=_resolve_config_path(config_path, payload["output_root"]),
            output_count=int(payload["output_count"]),
            target_width=int(payload["target_width"]),
            target_height=int(payload["target_height"]),
            target_fps=int(payload["target_fps"]),
            recent_limits={key: int(value) for key, value in payload.get("recent_limits", {}).items()},
            material_categories=_material_categories(payload.get("material_categories")),
            video_duration_min=float(payload["video_duration_min"]),
            video_duration_max=float(payload["video_duration_max"]),
            default_title_prefix=str(payload["default_title_prefix"]),
            slogans=list(payload["slogans"]),
            titles=list(payload["titles"]),
            composition_sequence=_composition_sequence(payload.get("composition_sequence")),
            beat_detection=_beat_detection(payload.get("beat_detection")),
            max_variant_attempts=max(1, int(payload.get("max_variant_attempts", 20) or 20)),
            variant_history_enabled=bool(payload.get("variant_history_enabled", True)),
            variant_history_limit=max(0, int(payload.get("variant_history_limit", 5000) or 5000)),
            enhancement_modules=dict(payload.get("enhancement_modules") or {"enabled": False, "modules": []}),
            copy_mode=str(payload.get("copy_mode") or "spark_then_template"),
            daily_output_goal=max(1, int(payload.get("daily_output_goal", 50) or 50)),
            narrative_templates=_narrative_templates(payload.get("narrative_templates")),
            dedupe_policy=_dedupe_policy(payload.get("dedupe_policy")),
        )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_config_path(config_path: Path, raw: object) -> Path:
    token = str(raw or "").strip()
    path = Path(token)
    if path.is_absolute():
        return path.resolve()
    normalized = token.replace("\\", "/")
    repo = _repo_root()
    if normalized.startswith("../runtime/"):
        return (repo / normalized.removeprefix("../")).resolve()
    if normalized.startswith("runtime/"):
        return (repo / normalized).resolve()
    return (config_path.parent.parent / path).resolve()


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


def _narrative_templates(raw: object) -> list[dict[str, object]]:
    if not isinstance(raw, list):
        return []
    templates: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        template_id = str(item.get("id") or "").strip()
        if not template_id or template_id in seen:
            continue
        seen.add(template_id)
        sequence = _narrative_sequence(item.get("composition_sequence"))
        if not sequence:
            continue
        templates.append(
            {
                "id": template_id,
                "name": str(item.get("name") or template_id).strip() or template_id,
                "account_pool_id": str(item.get("account_pool_id") or template_id).strip() or template_id,
                "composition_sequence": sequence,
            }
        )
    return templates


def _narrative_sequence(raw: object) -> list[dict[str, object]]:
    if not isinstance(raw, list):
        return []
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
        if duration > 0:
            sequence.append({"category_id": category_id, "duration": duration})
    return sequence


def _dedupe_policy(raw: object) -> dict[str, object]:
    config = dict(DEFAULT_DEDUPE_POLICY)
    if isinstance(raw, dict):
        config.update(raw)
    try:
        config["same_account_strictness"] = float(config.get("same_account_strictness", 0.04) or 0.04)
        config["max_mutation_retries"] = max(0, int(config.get("max_mutation_retries", 1) or 1))
        config["borderline_total"] = float(config.get("borderline_total", 0.80) or 0.80)
        config["reject_total"] = float(config.get("reject_total", 0.88) or 0.88)
        config["single_dimension_retry"] = bool(config.get("single_dimension_retry", True))
        config["visual_retry"] = float(config.get("visual_retry", 0.90) or 0.90)
        config["text_retry"] = float(config.get("text_retry", 0.96) or 0.96)
        config["structure_retry"] = float(config.get("structure_retry", 0.86) or 0.86)
        config["preflight_avoidance_enabled"] = bool(config.get("preflight_avoidance_enabled", True))
        config["text_preflight_threshold"] = float(config.get("text_preflight_threshold", 0.86) or 0.86)
        config["structure_preflight_threshold"] = float(config.get("structure_preflight_threshold", 0.72) or 0.72)
        config["hook_cooldown_enabled"] = bool(config.get("hook_cooldown_enabled", True))
        config["bgm_random_offset_enabled"] = bool(config.get("bgm_random_offset_enabled", True))
        config["bgm_offset_bucket_seconds"] = max(1, int(config.get("bgm_offset_bucket_seconds", 8) or 8))
    except (TypeError, ValueError):
        return dict(DEFAULT_DEDUPE_POLICY)
    return config
