from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

from .ffmpeg_tools import normalize_clip, probe_media
from .models import ClipMetadata
from .settings import ProjectSettings


VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".avi", ".mkv"}
KNOWN_CATEGORIES = ("category_A", "category_B", "category_C")


def category_ids(categories: list[dict[str, str]] | None = None) -> tuple[str, ...]:
    if not categories:
        return KNOWN_CATEGORIES
    ids = tuple(str(item.get("id") or "").strip() for item in categories if str(item.get("id") or "").strip())
    return ids or KNOWN_CATEGORIES


def infer_category(path: Path, categories: list[dict[str, str]] | None = None) -> str | None:
    tokens = {token.lower() for token in path.parts}
    name = path.stem.lower()
    for category in category_ids(categories):
        if category.lower() in tokens:
            return category
    if any(keyword in name for keyword in ("industrial", "weld", "spark", "machine", "factoryline", "metal", "robot", "production", "assembly")):
        return "category_A"
    if any(keyword in name for keyword in ("office", "screen", "roi", "code", "hmi", "dashboard", "monitor", "laptop", "ui", "control", "analysis")):
        return "category_B"
    if any(keyword in name for keyword in ("logo", "factory", "brand", "hall", "shipping", "warehouse", "outdoor", "sign", "showroom", "campus")):
        return "category_C"
    if "category_a" in tokens:
        return "category_A"
    if "category_b" in tokens:
        return "category_B"
    if "category_c" in tokens:
        return "category_C"
    return None


def _compute_visual_scores(metadata: dict) -> tuple[float, float]:
    stream = next((item for item in metadata.get("streams", []) if item.get("codec_type") == "video"), {})
    width = int(stream.get("width", 0) or 0)
    height = int(stream.get("height", 0) or 0)
    brightness = min(1.0, (width * height) / (1080 * 1920))
    contrast = 0.7 + brightness * 0.3
    return round(brightness, 4), round(contrast, 4)


def _parse_video_info(metadata: dict) -> tuple[float, int, int, float]:
    stream = next((item for item in metadata.get("streams", []) if item.get("codec_type") == "video"), {})
    duration = float(stream.get("duration") or metadata.get("format", {}).get("duration") or 0.0)
    width = int(stream.get("width", 0) or 0)
    height = int(stream.get("height", 0) or 0)
    rate = str(stream.get("avg_frame_rate", "0/1"))
    numerator, _, denominator = rate.partition("/")
    fps = float(numerator) / max(float(denominator or "1"), 1.0)
    return duration, width, height, fps


def ingest_sources(
    settings: ProjectSettings,
    source_root: Path | None = None,
    recent_limits: dict[str, int] | None = None,
    active_category_ids: list[str] | None = None,
    telemetry: Any | None = None,
    speed_mode: str = "quality",
) -> list[ClipMetadata]:
    root = source_root or settings.source_root
    root.mkdir(parents=True, exist_ok=True)
    metadata_items: list[ClipMetadata] = []
    selected_files = _select_source_files(root, recent_limits, settings.material_categories, active_category_ids)
    if telemetry is not None:
        telemetry.event(
            "ingestion",
            "source_files_selected",
            {
                "source_root": root,
                "selected_count": len(selected_files),
                "active_category_ids": active_category_ids or [],
                "recent_limits": recent_limits or {},
            },
        )
    for source_path in selected_files:
        category = infer_category(source_path, settings.material_categories) or "uncategorized"
        clip_id = hashlib.sha1(str(source_path).encode("utf-8")).hexdigest()[:12]
        normalized_path = settings.library_root / category / f"{clip_id}.mp4"
        payload = {
            "source_path": source_path,
            "category": category,
            "clip_id": clip_id,
            "source_bytes": _file_size(source_path),
            "target_path": normalized_path,
        }
        if not _normalize_cache_hit(source_path, normalized_path, settings, speed_mode):
            if telemetry is not None:
                with telemetry.span("ingestion", "normalize_clip", payload):
                    normalize_clip(
                        source=source_path,
                        target=normalized_path,
                        width=settings.target_width,
                        height=settings.target_height,
                        fps=settings.target_fps,
                        speed_mode=speed_mode,
                    )
            else:
                normalize_clip(
                    source=source_path,
                    target=normalized_path,
                    width=settings.target_width,
                    height=settings.target_height,
                    fps=settings.target_fps,
                    speed_mode=speed_mode,
                )
            _write_normalize_cache(source_path, normalized_path, settings, speed_mode)
        if telemetry is not None:
            with telemetry.span("ingestion", "probe_normalized_clip", {"clip_id": clip_id, "normalized_path": normalized_path}):
                raw_metadata = probe_media(normalized_path)
        else:
            raw_metadata = probe_media(normalized_path)
        duration, width, height, fps = _parse_video_info(raw_metadata)
        brightness, contrast = _compute_visual_scores(raw_metadata)
        business_tags = _business_tags_for_source(source_path, category, duration)
        if telemetry is not None:
            telemetry.event(
                "ingestion",
                "clip_ready",
                {
                    **payload,
                    "normalized_bytes": _file_size(normalized_path),
                    "duration": duration,
                    "width": width,
                    "height": height,
                    "fps": fps,
                    "brightness_score": brightness,
                    "contrast_score": contrast,
                },
            )
        metadata_items.append(
            ClipMetadata(
                clip_id=clip_id,
                source_path=source_path.resolve(),
                normalized_path=normalized_path.resolve(),
                category=category,
                duration=duration,
                width=width,
                height=height,
                fps=fps,
                brightness_score=brightness,
                contrast_score=contrast,
                tags=[category],
                **business_tags,
            )
        )
    if telemetry is not None:
        with telemetry.span("ingestion", "rebalance_categories", {"clip_count": len(metadata_items)}):
            metadata_items = rebalance_categories(metadata_items, settings.material_categories)
    else:
        metadata_items = rebalance_categories(metadata_items, settings.material_categories)
    index_path = settings.library_root / "metadata_index.json"
    if telemetry is not None:
        telemetry.event("ingestion", "category_distribution", {"categories": dict(Counter(item.category for item in metadata_items))})
        with telemetry.span("ingestion", "write_metadata_index", {"index_path": index_path, "clip_count": len(metadata_items)}):
            index_path.parent.mkdir(parents=True, exist_ok=True)
            index_path.write_text(
                json.dumps([item.as_dict() for item in metadata_items], indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
    else:
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(
            json.dumps([item.as_dict() for item in metadata_items], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return metadata_items


def ensure_category_dirs(root: Path, categories: list[dict[str, str]] | None = None) -> None:
    for category in category_ids(categories):
        (root / category).mkdir(parents=True, exist_ok=True)


def _select_source_files(
    root: Path,
    recent_limits: dict[str, int] | None,
    categories: list[dict[str, str]] | None = None,
    active_category_ids: list[str] | None = None,
) -> list[Path]:
    files = [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS
    ]
    known_categories = category_ids(categories)
    active_categories = {str(item).strip() for item in active_category_ids or [] if str(item).strip()}
    if active_category_ids is not None:
        files = [path for path in files if infer_category(path, categories) in active_categories]
    if not recent_limits:
        return sorted(files)

    grouped: dict[str, list[Path]] = {category: [] for category in set(known_categories) | set(recent_limits.keys())}
    other: list[Path] = []
    for path in files:
        category = infer_category(path, categories)
        if category in grouped:
            grouped[category].append(path)
        else:
            other.append(path)

    selected: list[Path] = []
    for category in known_categories:
        limit = int(recent_limits.get(category, 0) or 0)
        candidates = sorted(grouped[category], key=lambda item: item.stat().st_mtime, reverse=True)
        selected.extend(candidates[:limit] if limit > 0 else candidates)
    selected.extend(sorted(other, key=lambda item: item.stat().st_mtime, reverse=True))
    return selected


def rebalance_categories(clips: list[ClipMetadata], categories: list[dict[str, str]] | None = None) -> list[ClipMetadata]:
    if not clips:
        return clips

    known_categories = category_ids(categories)
    assigned = [clip for clip in clips if clip.category in known_categories]
    unassigned = [clip for clip in clips if clip.category not in known_categories]
    counts = Counter(clip.category for clip in assigned)

    for category in known_categories:
        if counts.get(category, 0) > 0 or not unassigned:
            continue
        _move_clip_to_category(unassigned.pop(0), category)
        counts[category] += 1

    category_cycle = sorted(KNOWN_CATEGORIES, key=lambda item: counts.get(item, 0))
    for clip in unassigned:
        target = category_cycle[0]
        _move_clip_to_category(clip, target, distributed=True)
        counts[target] += 1
        category_cycle = sorted(KNOWN_CATEGORIES, key=lambda item: counts.get(item, 0))

    return clips


def _move_clip_to_category(clip: ClipMetadata, target: str, distributed: bool = False) -> None:
    old_path = clip.normalized_path
    clip.category = target
    clip.tags = [tag for tag in clip.tags if tag != "uncategorized"]
    clip.tags.append("auto_distributed" if distributed else "auto_assigned")
    clip.tags.append(target)
    clip.normalized_path = old_path.parent.parent / target / old_path.name
    clip.normalized_path.parent.mkdir(parents=True, exist_ok=True)
    if old_path.exists():
        old_path.replace(clip.normalized_path)


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _normalize_cache_hit(source_path: Path, normalized_path: Path, settings: ProjectSettings, speed_mode: str) -> bool:
    if not normalized_path.exists():
        return False
    cache_path = _normalize_cache_path(normalized_path)
    if not cache_path.exists():
        return False
    try:
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    stat = source_path.stat()
    return (
        int(cache.get("source_mtime_ns", -1)) == int(stat.st_mtime_ns)
        and int(cache.get("source_size", -1)) == int(stat.st_size)
        and int(cache.get("width", -1)) == int(settings.target_width)
        and int(cache.get("height", -1)) == int(settings.target_height)
        and int(cache.get("fps", -1)) == int(settings.target_fps)
        and str(cache.get("speed_mode", "")) == str(speed_mode)
    )


def _write_normalize_cache(source_path: Path, normalized_path: Path, settings: ProjectSettings, speed_mode: str) -> None:
    stat = source_path.stat()
    payload = {
        "source_mtime_ns": int(stat.st_mtime_ns),
        "source_size": int(stat.st_size),
        "width": int(settings.target_width),
        "height": int(settings.target_height),
        "fps": int(settings.target_fps),
        "speed_mode": str(speed_mode),
    }
    _normalize_cache_path(normalized_path).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _normalize_cache_path(normalized_path: Path) -> Path:
    return normalized_path.with_suffix(".norm.json")


def _business_tags_for_source(source_path: Path, category: str, duration: float) -> dict[str, Any]:
    payload = _load_source_sidecar(source_path)
    name = source_path.stem.lower()
    default_action = "展示"
    if any(token in name for token in ("inspect", "巡检", "test", "测试")):
        default_action = "巡检测试"
    elif any(token in name for token in ("screen", "monitor", "display", "仪表", "屏")):
        default_action = "数据观察"
    elif any(token in name for token in ("build", "install", "施工", "操作")):
        default_action = "施工操作"
    usable_range = payload.get("usable_range") if isinstance(payload.get("usable_range"), list) else None
    parsed_range = None
    if usable_range and len(usable_range) >= 2:
        try:
            start = max(0.0, float(usable_range[0]))
            end = min(float(duration or 0.0), float(usable_range[1]))
            parsed_range = (start, max(start, end))
        except (TypeError, ValueError):
            parsed_range = None
    return {
        "scene_tag": str(payload.get("scene_tag") or category).strip(),
        "subject_tag": _string_list(payload.get("subject_tag") or payload.get("subject_tags") or [category]),
        "action_tag": _string_list(payload.get("action_tag") or payload.get("action_tags") or [default_action]),
        "shot_size": str(payload.get("shot_size") or "unknown").strip(),
        "camera_angle": str(payload.get("camera_angle") or "unknown").strip(),
        "camera_move": str(payload.get("camera_move") or "unknown").strip(),
        "audio_state": str(payload.get("audio_state") or "unknown").strip(),
        "language_tag": str(payload.get("language_tag") or "zh-CN").strip(),
        "usable_range": parsed_range,
        "account_fit_tags": _string_list(payload.get("account_fit_tags") or [category]),
        "rights_status": str(payload.get("rights_status") or "owned").strip(),
        "hero_frame_candidates": _float_list(payload.get("hero_frame_candidates")),
    }


def _load_source_sidecar(source_path: Path) -> dict[str, Any]:
    candidates = [
        source_path.with_suffix(".json"),
        source_path.with_name(f"{source_path.stem}.metadata.json"),
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _string_list(raw: object) -> list[str]:
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]


def _float_list(raw: object) -> list[float]:
    if not isinstance(raw, list):
        return []
    values: list[float] = []
    for item in raw:
        try:
            values.append(float(item))
        except (TypeError, ValueError):
            continue
    return values
