from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .beat import detect_beat_grid
from .ffmpeg_tools import resolve_ffmpeg_threads
from .composition import plan_variants
from .cover_templates import DEFAULT_COVER_TEMPLATE_ID, default_cover_templates, require_cover_template
from .hud import build_hud_payload
from .ingestion import ingest_sources
from .models import RenderedAsset
from .render import render_variant
from .settings import ProjectSettings
from ..paths import get_paths


ProgressCallback = Callable[[str, float, str], None]
AssetCallback = Callable[[RenderedAsset, int, int], None]
VIDEO_ENDING_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}


def run_pipeline(
    settings: ProjectSettings,
    bgm_path: Path,
    output_count: int | None = None,
    source_root: Path | None = None,
    output_root: Path | None = None,
    progress_callback: ProgressCallback | None = None,
    output_types: set[str] | None = None,
    copy_language: str = "zh",
    max_workers: int | None = None,
    recent_limits: dict[str, int] | None = None,
    active_category_ids: list[str] | None = None,
    template_config: dict | None = None,
    cover_template_id: str = DEFAULT_COVER_TEMPLATE_ID,
    cover_template_config: dict | None = None,
    ending_cover_template_config: dict | None = None,
    cover_intro_seconds: float = 1.0,
    text_overrides: dict[str, str] | None = None,
    outro_seconds: float = 1.0,
    composition_sequence: list[dict[str, Any]] | None = None,
    existing_signatures: set[str] | None = None,
    recent_clip_ids: set[str] | None = None,
    recent_segment_keys: set[str] | None = None,
    ending_template_path: Path | None = None,
    telemetry: Any | None = None,
    speed_mode: str = "quality",
    asset_callback: AssetCallback | None = None,
) -> list[RenderedAsset]:
    _notify(progress_callback, "ingestion", 0.05, "Collecting and normalizing source clips")
    if telemetry is not None:
        with telemetry.span("ingestion", "ingest_sources"):
            clips = ingest_sources(
                settings,
                source_root=source_root,
                recent_limits=recent_limits,
                active_category_ids=active_category_ids,
                telemetry=telemetry,
                speed_mode=speed_mode,
            )
    else:
        clips = ingest_sources(
            settings,
            source_root=source_root,
            recent_limits=recent_limits,
            active_category_ids=active_category_ids,
            speed_mode=speed_mode,
        )
    if not clips:
        raise ValueError("No source videos were found for ingestion")
    _notify(progress_callback, "hud", 0.20, "Preparing GasGx data HUD")
    if telemetry is not None:
        with telemetry.span("hud", "build_hud_payload"):
            hud_payload = build_hud_payload(settings)
    else:
        hud_payload = build_hud_payload(settings)
    active_composition_sequence = _fit_composition_sequence_to_max_duration(composition_sequence or settings.composition_sequence, settings.video_duration_max)
    algorithm_outro_seconds = 0.0 if _is_video_ending(ending_template_path) else outro_seconds
    beat_duration_hint = _beat_duration_hint(settings, active_composition_sequence, cover_intro_seconds, algorithm_outro_seconds)
    _notify(progress_callback, "beat", 0.30, "Analyzing BGM beat grid")
    beat_mode = "fallback" if speed_mode == "fast_first" else str(settings.beat_detection.get("mode", "auto"))
    beat_payload = {
        "bgm_path": bgm_path,
        "duration_hint": beat_duration_hint,
        "fallback_spacing": float(settings.beat_detection.get("fallback_spacing", 0.48)),
        "mode": beat_mode,
    }
    cache_key = _beat_cache_key(bgm_path, beat_duration_hint, settings, beat_mode)
    cached_grid = _load_beat_grid_cache(settings, cache_key)
    beat_grid: list[float]
    if isinstance(cached_grid, list) and cached_grid:
        beat_grid = [float(item) for item in cached_grid]
    else:
        if telemetry is not None:
            with telemetry.span("beat", "detect_beat_grid", beat_payload):
                beat_grid = detect_beat_grid(
                    bgm_path,
                    duration_hint=beat_duration_hint,
                    target_bpm_min=int(settings.beat_detection.get("target_bpm_min", 120)),
                    target_bpm_max=int(settings.beat_detection.get("target_bpm_max", 130)),
                    fallback_spacing=float(settings.beat_detection.get("fallback_spacing", 0.48)),
                    mode=beat_mode,
                )
            telemetry.event("beat", "beat_grid_ready", {"beat_count": len(beat_grid), **beat_payload})
        else:
            beat_grid = detect_beat_grid(
                bgm_path,
                duration_hint=beat_duration_hint,
                target_bpm_min=int(settings.beat_detection.get("target_bpm_min", 120)),
                target_bpm_max=int(settings.beat_detection.get("target_bpm_max", 130)),
                fallback_spacing=float(settings.beat_detection.get("fallback_spacing", 0.48)),
                mode=beat_mode,
            )
        _save_beat_grid_cache(settings, cache_key, beat_grid)
    _notify(progress_callback, "planning", 0.42, "Planning de-duplicated video variants")
    planning_payload = {
        "clip_count": len(clips),
        "output_count": output_count or settings.output_count,
        "composition_segments": len(active_composition_sequence or []),
        "existing_signatures": len(existing_signatures or set()),
        "recent_clip_ids": len(recent_clip_ids or set()),
        "recent_segment_keys": len(recent_segment_keys or set()),
    }
    if telemetry is not None:
        with telemetry.span("planning", "plan_variants", planning_payload):
            variants = plan_variants(
                clips,
                settings,
                hud_payload,
                beat_grid,
                output_count=output_count,
                composition_sequence=active_composition_sequence,
                max_attempts=settings.max_variant_attempts,
                existing_signatures=existing_signatures,
                recent_clip_ids=recent_clip_ids,
                recent_segment_keys=recent_segment_keys,
            )
        telemetry.event("planning", "variants_ready", {"variant_count": len(variants), **planning_payload})
    else:
        variants = plan_variants(
            clips,
            settings,
            hud_payload,
            beat_grid,
            output_count=output_count,
            composition_sequence=active_composition_sequence,
            max_attempts=settings.max_variant_attempts,
            existing_signatures=existing_signatures,
            recent_clip_ids=recent_clip_ids,
            recent_segment_keys=recent_segment_keys,
        )
    _apply_text_overrides(variants, text_overrides)
    template_copy = _copy_template_path().read_text(encoding="utf-8")
    active_cover_template = _resolve_cover_template_config(cover_template_id, cover_template_config)
    active_ending_cover_template = ending_cover_template_config or active_cover_template
    active_output_root = _resolve_output_root(settings, output_root)
    batch_dir = active_output_root
    filename_prefix = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
    assets: list[RenderedAsset] = []
    render_start = 0.45
    render_span = 0.50
    total = max(len(variants), 1)
    worker_count = _resolve_worker_count(max_workers, total)
    ffmpeg_threads = resolve_ffmpeg_threads(worker_count=worker_count, concurrent_jobs=2)
    if telemetry is not None:
        telemetry.event(
            "render",
            "render_batch_ready",
            {
                "variant_count": total,
                "worker_count": worker_count,
                "cpu_count": os.cpu_count() or 0,
                "ffmpeg_threads": ffmpeg_threads,
                "output_types": sorted(output_types or {"mp4"}),
                "output_root": batch_dir,
            },
        )
    _notify(progress_callback, "render", render_start, f"Rendering {total} videos with {worker_count} workers")
    with (telemetry.span("render", "render_batch", {"variant_count": total, "worker_count": worker_count, "ffmpeg_threads": ffmpeg_threads}) if telemetry is not None else _nullcontext()):
        executor = ThreadPoolExecutor(max_workers=worker_count)
        try:
            futures = {
                executor.submit(
                    render_variant,
                    variant,
                    settings,
                    template_copy,
                    batch_dir,
                    filename_prefix,
                    bgm_path,
                    output_types or {"mp4"},
                    copy_language,
                    template_config,
                    cover_template_id,
                    active_cover_template,
                    active_ending_cover_template,
                    cover_intro_seconds,
                    (text_overrides or {}).get("follow_text", ""),
                    outro_seconds,
                    ending_template_path,
                    telemetry.variant(variant.sequence_number) if telemetry is not None else None,
                    speed_mode,
                    ffmpeg_threads,
                ): variant.sequence_number
                for variant in variants
            }
            completed = 0
            failed = 0
            for future in as_completed(futures):
                sequence_number = futures[future]
                try:
                    asset = future.result()
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    if telemetry is not None:
                        telemetry.event(
                            "render",
                            "variant_failed",
                            {
                                "sequence_number": sequence_number,
                                "completed": completed,
                                "failed": failed,
                                "total": total,
                                "error": str(exc),
                            },
                        )
                    continue
                assets.append(asset)
                completed += 1
                progress = render_start + (completed / total) * render_span
                if telemetry is not None:
                    telemetry.event("render", "variant_completed", {"sequence_number": sequence_number, "completed": completed, "total": total})
                if asset_callback is not None:
                    asset_callback(asset, completed, total)
                _notify(progress_callback, "render", progress, f"Rendered video {completed}/{total}")
        finally:
            executor.shutdown(wait=True)
    _notify(progress_callback, "finalizing", 0.97, "Finalizing preview assets and manifests")
    if telemetry is not None:
        telemetry.event("finalizing", "assets_ready", {"asset_count": len(assets)})
    _notify(progress_callback, "complete", 1.0, f"Completed {len(assets)} exports")
    return sorted(assets, key=lambda asset: asset.variant.sequence_number)


def _notify(callback: ProgressCallback | None, stage: str, progress: float, message: str) -> None:
    if callback is not None:
        callback(stage, max(0.0, min(progress, 1.0)), message)


def _resolve_worker_count(max_workers: int | None, total: int) -> int:
    if max_workers is not None:
        return max(1, min(max_workers, total))
    cpu_count = os.cpu_count() or 2
    default_workers = max(1, min(2, (cpu_count + 3) // 4))
    return max(1, min(total, default_workers))


def _is_video_ending(path: Path | None) -> bool:
    return path is not None and path.suffix.lower() in VIDEO_ENDING_EXTENSIONS


def _beat_duration_hint(
    settings: ProjectSettings,
    composition_sequence: list[dict[str, Any]] | None,
    cover_intro_seconds: float,
    outro_seconds: float,
) -> float:
    segment_total = 0.0
    for item in composition_sequence or []:
        try:
            segment_total += float(item.get("duration", 0))
        except (AttributeError, TypeError, ValueError):
            continue
    composition_total = segment_total + max(0.0, cover_intro_seconds)
    return min(float(settings.video_duration_max), max(float(settings.video_duration_min), composition_total))


def _fit_composition_sequence_to_max_duration(
    composition_sequence: list[dict[str, Any]] | None,
    max_duration: float,
) -> list[dict[str, Any]]:
    sequence = [dict(item) for item in composition_sequence or []]
    target = max(0.1, float(max_duration))
    segment_total = 0.0
    for item in sequence:
        try:
            segment_total += max(0.0, float(item.get("duration", 0)))
        except (TypeError, ValueError):
            continue
    if segment_total <= target or segment_total <= 0:
        return sequence
    scale = target / segment_total
    fitted: list[dict[str, Any]] = []
    for item in sequence:
        next_item = dict(item)
        try:
            duration = max(0.0, float(next_item.get("duration", 0)))
        except (TypeError, ValueError):
            fitted.append(next_item)
            continue
        next_item["duration"] = duration * scale
        fitted.append(next_item)
    return fitted


def _resolve_output_root(settings: ProjectSettings, output_root: Path | None) -> Path:
    if output_root is None:
        return settings.output_root
    return output_root.expanduser().resolve()


def _resolve_cover_template_config(template_id: str, template_config: dict | None) -> dict:
    if template_config is not None:
        return require_cover_template({template_id: template_config}, template_id)
    try:
        return require_cover_template(default_cover_templates(), template_id)
    except KeyError as exc:
        raise ValueError(str(exc)) from exc


def _apply_text_overrides(variants: list, text_overrides: dict[str, str] | None) -> None:
    if not text_overrides:
        return
    headline = text_overrides.get("headline", "").strip()
    subhead = text_overrides.get("subhead", "").strip()
    hud_text = text_overrides.get("hud_text", "").strip()
    hud_lines = [line.strip() for line in hud_text.splitlines() if line.strip()]
    for variant in variants:
        if headline:
            variant.slogan = headline
        if subhead:
            variant.title = subhead
        if hud_lines:
            variant.hud_lines = hud_lines


def _copy_template_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "video_matrix" / "copy_template.txt"


@contextmanager
def _nullcontext():
    yield


def _beat_cache_path(settings: ProjectSettings) -> Path:
    path = get_paths().runtime_root / "video_matrix" / "beat_cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _beat_cache_key(settings_bgm_path: Path, duration_hint: float, settings: ProjectSettings, mode: str) -> str:
    return "|".join(
        [
            str(settings_bgm_path.resolve()),
            f"{duration_hint:.3f}",
            str(int(settings.beat_detection.get("target_bpm_min", 120))),
            str(int(settings.beat_detection.get("target_bpm_max", 130))),
            f"{float(settings.beat_detection.get('fallback_spacing', 0.48)):.3f}",
            str(mode),
        ]
    )


def _load_beat_grid_cache(settings: ProjectSettings, cache_key: str) -> list[float] | None:
    path = _beat_cache_entry_path(settings, cache_key)
    if path.exists():
        return _load_beat_grid_cache_entry(path)
    legacy_path = _beat_cache_legacy_path(settings)
    if not legacy_path.exists():
        return None
    try:
        raw = json.loads(legacy_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    values = raw.get(cache_key)
    if not isinstance(values, list):
        return None
    try:
        return [float(item) for item in values]
    except (TypeError, ValueError):
        return None


def _save_beat_grid_cache(settings: ProjectSettings, cache_key: str, beat_grid: list[float]) -> None:
    path = _beat_cache_entry_path(settings, cache_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(path, json.dumps(beat_grid, ensure_ascii=False))


def _beat_cache_legacy_path(settings: ProjectSettings) -> Path:
    return get_paths().runtime_root / "video_matrix" / "beat_cache.json"


def _beat_cache_entry_path(settings: ProjectSettings, cache_key: str) -> Path:
    digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
    return _beat_cache_path(settings) / f"{digest}.json"


def _load_beat_grid_cache_entry(path: Path) -> list[float] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, list):
        return None
    try:
        return [float(item) for item in raw]
    except (TypeError, ValueError):
        return None


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
