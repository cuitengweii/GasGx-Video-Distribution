from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

from .ffmpeg_tools import normalize_clip, probe_media
from .models import ClipMetadata
from .settings import ProjectSettings


VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".avi", ".mkv"}
KNOWN_CATEGORIES = ("category_A", "category_B", "category_C")


def infer_category(path: Path) -> str | None:
    tokens = {token.lower() for token in path.parts}
    name = path.stem.lower()
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
) -> list[ClipMetadata]:
    root = source_root or settings.source_root
    root.mkdir(parents=True, exist_ok=True)
    metadata_items: list[ClipMetadata] = []
    for source_path in _select_source_files(root, recent_limits):
        category = infer_category(source_path) or "uncategorized"
        clip_id = hashlib.sha1(str(source_path).encode("utf-8")).hexdigest()[:12]
        normalized_path = settings.library_root / category / f"{clip_id}.mp4"
        normalize_clip(
            source=source_path,
            target=normalized_path,
            width=settings.target_width,
            height=settings.target_height,
            fps=settings.target_fps,
        )
        raw_metadata = probe_media(normalized_path)
        duration, width, height, fps = _parse_video_info(raw_metadata)
        brightness, contrast = _compute_visual_scores(raw_metadata)
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
            )
        )
    metadata_items = rebalance_categories(metadata_items)
    index_path = settings.library_root / "metadata_index.json"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(
        json.dumps([item.as_dict() for item in metadata_items], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return metadata_items


def ensure_category_dirs(root: Path) -> None:
    for category in KNOWN_CATEGORIES:
        (root / category).mkdir(parents=True, exist_ok=True)


def _select_source_files(root: Path, recent_limits: dict[str, int] | None) -> list[Path]:
    files = [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS
    ]
    if not recent_limits:
        return sorted(files)

    grouped: dict[str, list[Path]] = {category: [] for category in KNOWN_CATEGORIES}
    other: list[Path] = []
    for path in files:
        category = infer_category(path)
        if category in grouped:
            grouped[category].append(path)
        else:
            other.append(path)

    selected: list[Path] = []
    for category in KNOWN_CATEGORIES:
        limit = int(recent_limits.get(category, 0) or 0)
        candidates = sorted(grouped[category], key=lambda item: item.stat().st_mtime, reverse=True)
        selected.extend(candidates[:limit] if limit > 0 else candidates)
    selected.extend(sorted(other, key=lambda item: item.stat().st_mtime, reverse=True))
    return selected


def rebalance_categories(clips: list[ClipMetadata]) -> list[ClipMetadata]:
    if not clips:
        return clips

    assigned = [clip for clip in clips if clip.category in KNOWN_CATEGORIES]
    unassigned = [clip for clip in clips if clip.category not in KNOWN_CATEGORIES]
    counts = Counter(clip.category for clip in assigned)

    for category in KNOWN_CATEGORIES:
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
