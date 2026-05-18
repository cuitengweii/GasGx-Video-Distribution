from __future__ import annotations

import hashlib
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Callable

from .beat import detect_beat_grid
from .dedupe import dedupe_payload_for_variant, enrich_rendered_asset_dedupe
from .ffmpeg_tools import probe_media, resolve_ffmpeg_threads
from .composition import plan_variants
from .cover_templates import DEFAULT_COVER_TEMPLATE_ID, default_cover_templates, require_cover_template
from .hud import build_hud_payload
from .ingestion import ingest_sources
from .models import RenderedAsset
from .render import render_variant
from .settings import ProjectSettings
from .spark_text import (
    build_description_variants,
    build_follow_text_variants,
    build_headline_variants,
    build_hud_variants,
    normalize_hud_lines,
)
from ..paths import get_paths


ProgressCallback = Callable[[str, float, str], None]
AssetCallback = Callable[[RenderedAsset, int, int], None]
VIDEO_ENDING_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}
DAILY_SEQUENCE_STATE_FILE = "daily_publish_sequence.json"
_DAILY_SEQUENCE_LOCK = Lock()


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
    text_overrides: dict[str, Any] | None = None,
    outro_seconds: float = 1.0,
    composition_sequence: list[dict[str, Any]] | None = None,
    existing_signatures: set[str] | None = None,
    recent_clip_ids: set[str] | None = None,
    recent_segment_keys: set[str] | None = None,
    history_features: list[dict[str, Any]] | None = None,
    ending_template_path: Path | None = None,
    telemetry: Any | None = None,
    speed_mode: str = "quality",
    asset_callback: AssetCallback | None = None,
    mining_bgm_path: Path | None = None,
    mining_bgm_volume: float | None = None,
    library_bgm_volume: float | None = None,
) -> list[RenderedAsset]:
    ai_prompt_hint = str((text_overrides or {}).get("ai_prompt_hint") or "").strip()
    daily_texts = [str(item).strip() for item in (text_overrides or {}).get("daily_texts") or [] if str(item).strip()]
    daily_headlines = [str(item).strip() for item in (text_overrides or {}).get("daily_headlines") or [] if str(item).strip()]
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
        "history_features": len(history_features or []),
    }
    bgm_duration = _audio_duration_seconds(bgm_path)
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
                history_features=history_features,
                bgm_name=bgm_path.name,
                bgm_duration=bgm_duration,
                ai_prompt_hint=ai_prompt_hint,
                avoid_texts=daily_texts,
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
            history_features=history_features,
            bgm_name=bgm_path.name,
            bgm_duration=bgm_duration,
            ai_prompt_hint=ai_prompt_hint,
            avoid_texts=daily_texts,
        )
    _apply_text_overrides(
        variants,
        text_overrides,
        settings,
        language=copy_language,
        ai_prompt_hint=ai_prompt_hint,
        avoid_headlines=daily_headlines,
        avoid_texts=daily_texts,
    )
    template_copy = _copy_template_path().read_text(encoding="utf-8")
    active_cover_template = _resolve_cover_template_config(cover_template_id, cover_template_config)
    active_ending_cover_template = ending_cover_template_config or active_cover_template
    active_output_root = _resolve_output_root(settings, output_root)
    run_dir_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    batch_dir = active_output_root / run_dir_name
    batch_dir.mkdir(parents=True, exist_ok=True)
    publish_day_code = datetime.now().strftime("%m%d")
    publish_sequence_start = _reserve_daily_publish_sequence(publish_day_code, len(variants))
    filename_prefix = ""
    assets: list[RenderedAsset] = []
    render_start = 0.45
    render_span = 0.50
    total = max(len(variants), 1)
    worker_count = _resolve_worker_count(max_workers, total)
    ffmpeg_threads = resolve_ffmpeg_threads(worker_count=worker_count, concurrent_jobs=2)
    # FFmpeg 8 + x264 can crash with access violations on some Windows hosts under multi-thread encode.
    # Prefer stability over throughput for matrix batch rendering.
    ffmpeg_threads = 1
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
                "base_output_root": active_output_root,
            },
        )
    _notify(progress_callback, "render", render_start, f"Rendering {total} videos with {worker_count} workers")
    with (telemetry.span("render", "render_batch", {"variant_count": total, "worker_count": worker_count, "ffmpeg_threads": ffmpeg_threads}) if telemetry is not None else _nullcontext()):
        executor = ThreadPoolExecutor(max_workers=worker_count)
        try:
            default_follow_text = str((text_overrides or {}).get("follow_text") or "")
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
                    getattr(variant, "ending_follow_text", "") or default_follow_text,
                    outro_seconds,
                    ending_template_path,
                    telemetry.variant(variant.sequence_number) if telemetry is not None else None,
                    speed_mode,
                    ffmpeg_threads,
                    ai_prompt_hint=ai_prompt_hint,
                    bgm_start_offset=variant.bgm_start_offset,
                    mining_bgm_path=mining_bgm_path,
                    mining_bgm_volume=mining_bgm_volume,
                    library_bgm_volume=library_bgm_volume,
                    publish_day_code=publish_day_code,
                    publish_sequence_number=publish_sequence_start + variant.sequence_number - 1,
                ): variant.sequence_number
                for variant in variants
            }
            completed = 0
            failed = 0
            for future in as_completed(futures):
                sequence_number = futures[future]
                try:
                    asset = future.result()
                    enrich_rendered_asset_dedupe(asset, bgm_path)
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


def _apply_text_overrides(
    variants: list,
    text_overrides: dict[str, Any] | None,
    settings: ProjectSettings,
    *,
    language: str = "zh",
    ai_prompt_hint: str = "",
    avoid_headlines: list[str] | set[str] | None = None,
    avoid_texts: list[str] | set[str] | None = None,
) -> None:
    if not text_overrides:
        return
    headline = str(text_overrides.get("headline") or "").strip()
    subhead = str(text_overrides.get("subhead") or "").strip()
    description_text = str(text_overrides.get("description_text") or "").strip()
    hud_text = str(text_overrides.get("hud_text") or "").strip()
    follow_text = str(text_overrides.get("follow_text") or "").strip()
    headline_ai_enabled = bool(text_overrides.get("headline_ai_enabled"))
    description_ai_enabled = bool(text_overrides.get("description_ai_enabled"))
    follow_text_ai_enabled = bool(text_overrides.get("follow_text_ai_enabled"))
    hud_ai_enabled = bool(text_overrides.get("hud_ai_enabled"))
    description_ai_prompt_hint = str(text_overrides.get("description_ai_prompt_hint") or "").strip()
    follow_text_ai_prompt_hint = str(text_overrides.get("follow_text_ai_prompt_hint") or "").strip()
    hud_ai_prompt_hint = str(text_overrides.get("hud_ai_prompt_hint") or "").strip()
    hud_lines = normalize_hud_lines(hud_text.splitlines())

    headline_variants: list[str] = []
    if headline_ai_enabled and variants:
        seed_headline = headline or str(variants[0].slogan or "").strip()
        headline_variants = build_headline_variants(
            seed_headline,
            len(variants),
            language=language,
            settings=settings,
            extra_prompt=ai_prompt_hint,
            avoid_texts=avoid_headlines,
        )

    description_variants: list[str] = []
    if description_ai_enabled and variants:
        seed_description = description_text or f"{str(variants[0].title or '').strip()}\n{str(variants[0].slogan or '').strip()}".strip()
        description_variants = build_description_variants(
            seed_description,
            len(variants),
            language=language,
            settings=settings,
            extra_prompt=description_ai_prompt_hint or ai_prompt_hint,
            avoid_texts=avoid_texts,
        )

    follow_text_variants: list[str] = []
    if follow_text_ai_enabled and variants:
        seed_follow = follow_text or "Follow GasGx for more field power cases"
        follow_text_variants = build_follow_text_variants(
            seed_follow,
            len(variants),
            language=language,
            settings=settings,
            extra_prompt=follow_text_ai_prompt_hint or ai_prompt_hint,
            avoid_texts=avoid_texts,
        )

    hud_text_variants: list[list[str]] = []
    if hud_ai_enabled and variants:
        seed_hud = hud_lines or list(getattr(variants[0], "hud_lines", []) or [])
        hud_text_variants = build_hud_variants(
            seed_hud,
            len(variants),
            language=language,
            settings=settings,
            extra_prompt=hud_ai_prompt_hint or ai_prompt_hint,
            avoid_texts=avoid_texts,
        )

    for index, variant in enumerate(variants):
        if headline_ai_enabled and index < len(headline_variants):
            variant.slogan = headline_variants[index]
        elif not headline_ai_enabled and headline:
            variant.slogan = headline
        if subhead:
            variant.title = subhead
        if hud_ai_enabled and index < len(hud_text_variants):
            variant.hud_lines = hud_text_variants[index]
        elif hud_lines:
            variant.hud_lines = hud_lines
        if follow_text_ai_enabled and index < len(follow_text_variants):
            variant.ending_follow_text = follow_text_variants[index]
        elif follow_text:
            variant.ending_follow_text = follow_text
        if description_ai_enabled and index < len(description_variants):
            variant.publish_description = description_variants[index]
        elif description_text:
            variant.publish_description = description_text


def _copy_template_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "video_matrix" / "copy_template.txt"


def rendered_asset_payload(asset: RenderedAsset) -> dict[str, Any]:
    return {
        "video_path": str(asset.video_path),
        "cover_path": str(asset.cover_path) if asset.cover_path else "",
        "copy_path": str(asset.copy_path) if asset.copy_path else "",
        "manifest_path": str(asset.manifest_path) if asset.manifest_path else "",
        "narrative_template_id": asset.variant.narrative_template_id,
        "account_pool_id": asset.variant.account_pool_id,
        "bgm_start_offset": asset.variant.bgm_start_offset,
        "bgm_offset_bucket": asset.variant.bgm_offset_bucket,
        "dedupe": dedupe_payload_for_variant(asset.variant),
    }


def _audio_duration_seconds(path: Path) -> float:
    if not path.exists():
        return 0.0
    try:
        payload = probe_media(path)
    except Exception:
        return 0.0
    try:
        return max(0.0, float((payload.get("format") or {}).get("duration") or 0.0))
    except (AttributeError, TypeError, ValueError):
        return 0.0


@contextmanager
def _nullcontext():
    yield


def _beat_cache_path(settings: ProjectSettings) -> Path:
    path = get_paths().runtime_root / "video_matrix" / "beat_cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _daily_publish_sequence_state_path() -> Path:
    path = get_paths().runtime_root / "video_matrix" / DAILY_SEQUENCE_STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _reserve_daily_publish_sequence(day_code: str, count: int) -> int:
    reserve_count = max(0, int(count or 0))
    if reserve_count == 0:
        return 1
    normalized_day_code = str(day_code or "").strip()
    if len(normalized_day_code) != 4 or not normalized_day_code.isdigit():
        normalized_day_code = datetime.now().strftime("%m%d")
    with _DAILY_SEQUENCE_LOCK:
        path = _daily_publish_sequence_state_path()
        state = _load_daily_publish_sequence_state(path)
        if str(state.get("day_code") or "") != normalized_day_code:
            last_sequence = 0
        else:
            try:
                last_sequence = max(0, int(state.get("last_sequence") or 0))
            except (TypeError, ValueError):
                last_sequence = 0
        start = last_sequence + 1
        updated = {
            "day_code": normalized_day_code,
            "last_sequence": last_sequence + reserve_count,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        _atomic_write_text(path, json.dumps(updated, ensure_ascii=False))
        return start


def _load_daily_publish_sequence_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


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
