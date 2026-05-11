from __future__ import annotations

import hashlib
import random
from collections import defaultdict
from typing import Any

from .dedupe import (
    evaluate_variant_plan,
    hamming_similarity,
    mark_manual_review,
    mark_recut_passed,
    plan_feature_record,
    prepare_variant_dedupe_fields,
)
from .hud import HudPayload
from .models import ClipMetadata, DedupeResult, SegmentPlan, VideoVariant
from .settings import DEFAULT_COMPOSITION_SEQUENCE, ProjectSettings
from .spark_text import build_text_variants


def plan_variants(
    clips: list[ClipMetadata],
    settings: ProjectSettings,
    hud_payload: HudPayload,
    beat_grid: list[float],
    output_count: int | None = None,
    seed: int = 42,
    composition_sequence: list[dict[str, Any]] | None = None,
    max_attempts: int | None = None,
    existing_signatures: set[str] | None = None,
    recent_clip_ids: set[str] | None = None,
    recent_segment_keys: set[str] | None = None,
    history_features: list[dict[str, Any]] | None = None,
    bgm_name: str = "",
    bgm_duration: float = 0.0,
) -> list[VideoVariant]:
    count = output_count or settings.output_count
    sequence = _resolve_sequence(composition_sequence or settings.composition_sequence)
    attempts_limit = max(1, int(max_attempts or settings.max_variant_attempts))
    historical_signatures = set(existing_signatures or set())
    historical_clip_ids = set(recent_clip_ids or set())
    historical_segment_keys = set(recent_segment_keys or set())
    buckets = defaultdict(list)
    for clip in clips:
        buckets[clip.category].append(clip)
    narrative_templates = _resolve_narrative_templates(settings, sequence, set(buckets))
    for category, _target_window in sequence:
        if not buckets[category]:
            raise ValueError(
                "Missing required category clips for "
                f"{category}. Upload or enable source videos for this category before generating."
            )

    rng = random.Random(seed)
    variants: list[VideoVariant] = []
    seen_signatures: set[str] = set()
    accepted_features = [dict(item) for item in history_features or [] if isinstance(item, dict)]
    seen_visual_keys = {str(item.get("visual_plan_key") or "").strip() for item in accepted_features if str(item.get("visual_plan_key") or "").strip()}
    batch_hook_clip_ids: set[str] = set()
    preflight_enabled = _policy_bool(settings, "preflight_avoidance_enabled", True)
    hook_cooldown = _policy_bool(settings, "hook_cooldown_enabled", True)
    text_variants = build_text_variants(
        settings,
        list(getattr(hud_payload, "lines", []) or []),
        count * max(2, attempts_limit),
        narrative_templates=narrative_templates,
    )

    for sequence_number in range(1, count + 1):
        attempts = 0
        history_collision: VideoVariant | None = None
        collision_result: DedupeResult | None = None
        best_candidate: VideoVariant | None = None
        best_result: DedupeResult | None = None
        best_score: float | None = None
        while attempts < attempts_limit:
            attempts += 1
            narrative = narrative_templates[(sequence_number - 1) % len(narrative_templates)] if narrative_templates else None
            structure_variant_id = _structure_variant_id(narrative, attempts)
            active_sequence = _structure_sequence_variant(_sequence_for_narrative(narrative, sequence, set(buckets)), attempts if preflight_enabled else 1)
            excluded_clip_ids = historical_clip_ids | batch_hook_clip_ids if hook_cooldown else historical_clip_ids
            segments = _pick_segments(buckets, beat_grid, rng, active_sequence, excluded_clip_ids, float(settings.video_duration_max))
            text_variant = _text_variant_for(text_variants, sequence_number, attempts, attempts_limit)
            title = str(text_variant.get("title") or "").strip() or _choice_or(settings.titles, rng, settings.default_title_prefix or "GasGx")
            slogan = str(text_variant.get("slogan") or "").strip() or _choice_or(settings.slogans, rng, settings.project_name or "GasGx")
            hud_lines = [str(line).strip() for line in text_variant.get("hud_lines") or [] if str(line).strip()]
            if not hud_lines:
                hud_lines = list(getattr(hud_payload, "lines", []) or [])
            lut_strength = round(1.0 + rng.uniform(-0.03, 0.03), 4)
            zoom = round(1.04 + rng.uniform(-0.02, 0.03), 4)
            mirror = rng.choice([False, True])
            x_offset = rng.randint(-24, 24)
            y_offset = rng.randint(-42, 42)
            cover_frame_offset = round(rng.uniform(0.0, 0.35), 3)
            narrative_template_id = str((narrative or {}).get("id") or "")
            account_pool_id = str((narrative or {}).get("account_pool_id") or narrative_template_id)
            bgm_start_offset = _bgm_start_offset(settings, rng, bgm_duration, float(settings.video_duration_max))
            bgm_offset_bucket = _bgm_offset_bucket(settings, bgm_start_offset)
            signature = _signature_for(
                segments,
                slogan,
                title,
                lut_strength,
                zoom,
                mirror,
                x_offset,
                y_offset,
                hud_lines,
                narrative_template_id,
                cover_frame_offset,
                structure_variant_id,
                bgm_offset_bucket,
            )
            if signature in seen_signatures:
                continue
            candidate = VideoVariant(
                sequence_number=sequence_number,
                title=title,
                slogan=slogan,
                hud_lines=hud_lines,
                lut_strength=lut_strength,
                zoom=zoom,
                mirror=mirror,
                x_offset=x_offset,
                y_offset=y_offset,
                segments=segments,
                signature=signature,
                narrative_template_id=narrative_template_id,
                account_pool_id=account_pool_id,
                cover_frame_offset=cover_frame_offset,
                text_variant_id=str(text_variant.get("id") or ""),
                structure_variant_id=structure_variant_id,
                bgm_start_offset=bgm_start_offset,
                bgm_offset_bucket=bgm_offset_bucket,
            )
            candidate.visual_plan_key = _visual_plan_key(candidate)
            prepare_variant_dedupe_fields(candidate)
            result = evaluate_variant_plan(
                candidate,
                accepted_features,
                existing_signatures=historical_signatures,
                recent_segment_keys=historical_segment_keys,
                policy=settings.dedupe_policy,
                bgm_name=bgm_name,
            )
            if preflight_enabled:
                _apply_preflight_avoidance(candidate, result, accepted_features, seen_visual_keys, settings)
            candidate_score = _preflight_score(result)
            if best_score is None or candidate_score < best_score:
                best_candidate = candidate
                best_result = result
                best_score = candidate_score
            if preflight_enabled and result.action == "retry" and not _hard_duplicate(result):
                continue
            mutation_history: list[str] = []
            retry_count = 0
            while result.action == "retry" and _hard_duplicate(result) and retry_count < _max_mutation_retries(settings):
                retry_count += 1
                mutation = _mutate_candidate(candidate, buckets, rng, retry_count, historical_clip_ids, float(settings.video_duration_max))
                mutation_history.append(mutation)
                candidate.visual_plan_key = _visual_plan_key(candidate)
                candidate.signature = _signature_for(
                    candidate.segments,
                    candidate.slogan,
                    candidate.title,
                    candidate.lut_strength,
                    candidate.zoom,
                    candidate.mirror,
                    candidate.x_offset,
                    candidate.y_offset,
                    candidate.hud_lines,
                    candidate.narrative_template_id,
                    candidate.cover_frame_offset,
                    candidate.structure_variant_id,
                    candidate.bgm_offset_bucket,
                )
                if candidate.signature in seen_signatures:
                    result = DedupeResult(status="retry", action="retry", report=result.report)
                    continue
                prepare_variant_dedupe_fields(candidate)
                result = evaluate_variant_plan(
                    candidate,
                    accepted_features,
                    existing_signatures=historical_signatures,
                    recent_segment_keys=historical_segment_keys,
                    policy=settings.dedupe_policy,
                    bgm_name=bgm_name,
                )
                if preflight_enabled:
                    _apply_preflight_avoidance(candidate, result, accepted_features, seen_visual_keys, settings)
            if result.action == "pass":
                candidate.dedupe_result = mark_recut_passed(result, retry_count, mutation_history)
            elif candidate.signature in historical_signatures or _segments_touch_history(candidate.segments, historical_segment_keys):
                history_collision = candidate
                collision_result = mark_manual_review(result, retry_count, mutation_history)
                continue
            elif result.status in {"borderline", "retry"}:
                history_collision = candidate
                collision_result = mark_manual_review(result, retry_count, mutation_history)
                continue
            _accept_variant(candidate, variants, accepted_features, seen_signatures, seen_visual_keys, batch_hook_clip_ids, bgm_name)
            break
        else:
            if preflight_enabled and best_candidate is not None and best_result is not None and not _hard_duplicate(best_result):
                best_result.report.reasons = _unique([*best_result.report.reasons, "preflight_limited_pool"])
                best_result.action = "pass"
                best_result.status = "pass"
                best_candidate.dedupe_result = mark_recut_passed(best_result, 0, [])
                _accept_variant(best_candidate, variants, accepted_features, seen_signatures, seen_visual_keys, batch_hook_clip_ids, bgm_name)
                continue
            if history_collision is not None:
                history_collision.dedupe_result = collision_result or DedupeResult(status="manual_review", action="review")
                _accept_variant(history_collision, variants, accepted_features, seen_signatures, seen_visual_keys, batch_hook_clip_ids, bgm_name)
                continue
            raise RuntimeError("Unable to produce a unique variant signature")
    return variants


def _pick_segments(
    buckets: dict[str, list[ClipMetadata]],
    beat_grid: list[float],
    rng: random.Random,
    sequence: list[tuple[str, float]],
    recent_clip_ids: set[str],
    max_total_duration: float,
) -> list[SegmentPlan]:
    beat_pairs = list(zip(beat_grid, beat_grid[1:]))
    if not beat_pairs:
        raise ValueError("Beat grid is empty")
    selected: list[SegmentPlan] = []
    beat_index = 0
    for index, (category, target_window) in enumerate(sequence):
        used_duration = sum(segment.duration for segment in selected)
        remaining_duration = max(0.0, max_total_duration - used_duration)
        if remaining_duration <= 0.0:
            break
        clip = _pick_clip(buckets[category], rng, recent_clip_ids)
        target_duration = min(target_window, remaining_duration)
        usable_start, usable_end = _usable_window(clip)
        available_duration = max(0.0, usable_end - usable_start)
        target_duration = min(target_duration, available_duration or target_duration)
        duration = min(_align_duration(target_duration, beat_pairs, beat_index), remaining_duration)
        start_time = _pick_start_time(clip, rng, usable_start, usable_end, duration, prefer_hero=index == 0)
        beat_index = min(len(beat_pairs) - 1, beat_index + max(1, int(duration / 0.45)))
        selected.append(
            SegmentPlan(
                category=category,
                clip=clip,
                start_time=start_time,
                duration=duration,
                index=index,
            )
        )
    return selected


def _accept_variant(
    candidate: VideoVariant,
    variants: list[VideoVariant],
    accepted_features: list[dict[str, Any]],
    seen_signatures: set[str],
    seen_visual_keys: set[str],
    batch_hook_clip_ids: set[str],
    bgm_name: str,
) -> None:
    seen_signatures.add(candidate.signature)
    variants.append(candidate)
    accepted_features.append(plan_feature_record(candidate, bgm_name=bgm_name))
    if candidate.visual_plan_key:
        seen_visual_keys.add(candidate.visual_plan_key)
    if candidate.segments:
        batch_hook_clip_ids.add(candidate.segments[0].clip.clip_id)


def _apply_preflight_avoidance(
    candidate: VideoVariant,
    result: DedupeResult,
    accepted_features: list[dict[str, Any]],
    seen_visual_keys: set[str],
    settings: ProjectSettings,
) -> None:
    reasons: list[str] = []
    if candidate.visual_plan_key and candidate.visual_plan_key in seen_visual_keys:
        reasons.append("visual_preflight_avoided")
        result.report.visual_score = max(result.report.visual_score, 0.90)
    else:
        reasons.append("visual_preflight_ok")
    text_score = _max_text_similarity(candidate, accepted_features)
    if text_score >= _policy_float(settings, "text_preflight_threshold", 0.86):
        reasons.append("text_preflight_avoided")
        result.report.text_score = max(result.report.text_score, text_score)
    else:
        reasons.append("ai_text_variant" if candidate.text_variant_id.startswith("spark_") else "template_text_variant")
    structure_score = _max_structure_similarity(candidate, accepted_features)
    if structure_score >= _policy_float(settings, "structure_preflight_threshold", 0.72):
        reasons.append("structure_preflight_avoided")
        result.report.structure_score = max(result.report.structure_score, structure_score)
    else:
        reasons.append("structure_preflight_ok")
    if candidate.bgm_start_offset > 0:
        reasons.append("bgm_offset_used")
    result.report.total_score = round(
        min(
            1.0,
            0.30 * result.report.visual_score
            + 0.15 * result.report.audio_score
            + 0.25 * result.report.text_score
            + 0.30 * result.report.structure_score,
        ),
        4,
    )
    result.report.reasons = _unique([*result.report.reasons, *reasons])
    if any(reason.endswith("_avoided") for reason in reasons) and not _hard_duplicate(result):
        result.status = "retry"
        result.action = "retry"


def _max_text_similarity(candidate: VideoVariant, accepted_features: list[dict[str, Any]]) -> float:
    prepare_variant_dedupe_fields(candidate)
    scores = [
        hamming_similarity(candidate.text_signature, str(item.get("text_signature") or ""))
        for item in accepted_features
        if str(item.get("text_signature") or "")
    ]
    return max(scores or [0.0])


def _max_structure_similarity(candidate: VideoVariant, accepted_features: list[dict[str, Any]]) -> float:
    candidate_tokens = _structure_tokens(candidate)
    scores = [
        _token_similarity(candidate_tokens, item.get("structure_tokens") if isinstance(item.get("structure_tokens"), list) else [])
        for item in accepted_features
    ]
    return max(scores or [0.0])


def _structure_tokens(candidate: VideoVariant) -> list[str]:
    tokens = [f"narrative:{candidate.narrative_template_id or 'default'}", f"structure_variant:{candidate.structure_variant_id or 'v1'}"]
    for segment in candidate.segments:
        duration_bucket = max(1, int(round(float(segment.duration) * 2)))
        tokens.append(f"{segment.index}:{segment.category}:{segment.clip.clip_id}:{duration_bucket}")
    return tokens


def _token_similarity(candidate_tokens: list[str], history_tokens: list[Any]) -> float:
    candidate = {str(item) for item in candidate_tokens if str(item)}
    history = {str(item) for item in history_tokens if str(item)}
    if not candidate or not history:
        return 0.0
    return round(len(candidate & history) / max(1, len(candidate | history)), 4)


def _preflight_score(result: DedupeResult) -> float:
    score = float(result.report.total_score or 0.0)
    penalty = {
        "signature_exact": 2.0,
        "segment_exact": 1.8,
        "visual_preflight_avoided": 0.35,
        "text_preflight_avoided": 0.25,
        "structure_preflight_avoided": 0.25,
        "same_hook_clip": 0.20,
    }
    return score + sum(penalty.get(reason, 0.0) for reason in result.report.reasons)


def _hard_duplicate(result: DedupeResult) -> bool:
    return any(reason in {"signature_exact", "segment_exact"} for reason in result.report.reasons)


def _pick_start_time(
    clip: ClipMetadata,
    rng: random.Random,
    usable_start: float,
    usable_end: float,
    duration: float,
    *,
    prefer_hero: bool,
) -> float:
    max_start = max(usable_start, usable_end - max(0.0, duration))
    if prefer_hero:
        candidates: list[float] = []
        for item in clip.hero_frame_candidates:
            try:
                candidate = float(item)
            except (TypeError, ValueError):
                continue
            if usable_start <= candidate <= max_start:
                candidates.append(candidate)
        if candidates:
            return round(rng.choice(candidates), 3)
    return max(0.0, round(rng.uniform(usable_start, max_start), 3))


def _visual_plan_key(candidate: VideoVariant) -> str:
    if not candidate.segments:
        return ""
    first = candidate.segments[0]
    start_bucket = int(float(first.start_time or 0.0) / 0.5)
    cover_bucket = int(float(candidate.cover_frame_offset or 0.0) / 0.15)
    return f"{first.category}:{first.clip.clip_id}:s{start_bucket}:c{cover_bucket}"


def _text_variant_for(text_variants: list[dict[str, object]], sequence_number: int, attempt: int, attempts_limit: int) -> dict[str, object]:
    if not text_variants:
        return {}
    index = ((sequence_number - 1) * max(1, attempts_limit) + attempt - 1) % len(text_variants)
    return text_variants[index]


def _structure_variant_id(narrative: dict[str, Any] | None, attempt: int) -> str:
    base = str((narrative or {}).get("id") or "default").strip() or "default"
    return f"{base}:v{((attempt - 1) % 4) + 1}"


def _structure_sequence_variant(sequence: list[tuple[str, float]], attempt: int) -> list[tuple[str, float]]:
    if len(sequence) < 3:
        return list(sequence)
    mode = (attempt - 1) % 4
    if mode == 0:
        return list(sequence)
    head = sequence[:1]
    tail = list(sequence[1:])
    if mode == 1:
        tail = list(reversed(tail))
    elif mode == 2:
        tail = tail[1:] + tail[:1]
    else:
        tail = tail[:]
        tail[0], tail[-1] = tail[-1], tail[0]
    return head + tail


def _bgm_start_offset(settings: ProjectSettings, rng: random.Random, bgm_duration: float, target_duration: float) -> float:
    if not _policy_bool(settings, "bgm_random_offset_enabled", True):
        return 0.0
    safe_duration = max(0.0, float(bgm_duration or 0.0))
    safe_target = max(1.0, float(target_duration or 0.0))
    max_offset = max(0.0, safe_duration - safe_target)
    if max_offset < 2.0:
        return 0.0
    return round(rng.uniform(0.0, max_offset), 3)


def _bgm_offset_bucket(settings: ProjectSettings, offset: float) -> str:
    seconds = max(1, int(settings.dedupe_policy.get("bgm_offset_bucket_seconds", 8) or 8))
    return f"b{int(max(0.0, float(offset or 0.0)) // seconds)}"


def _policy_bool(settings: ProjectSettings, key: str, fallback: bool) -> bool:
    try:
        return bool(settings.dedupe_policy.get(key, fallback))
    except AttributeError:
        return fallback


def _policy_float(settings: ProjectSettings, key: str, fallback: float) -> float:
    try:
        value = float(settings.dedupe_policy.get(key, fallback))
    except (AttributeError, TypeError, ValueError):
        return fallback
    return value if 0 <= value <= 1 else fallback


def _unique(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        token = str(item or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _mutate_candidate(
    candidate: VideoVariant,
    buckets: dict[str, list[ClipMetadata]],
    rng: random.Random,
    step: int,
    recent_clip_ids: set[str],
    max_total_duration: float,
) -> str:
    mutation = ((step - 1) % 5) + 1
    if mutation == 1:
        _mutate_hook_segment(candidate, buckets, rng, recent_clip_ids, max_total_duration)
        return "replace_hook"
    if mutation == 2:
        _mutate_segment_order(candidate, rng)
        return "shuffle_order"
    if mutation == 3:
        candidate.title = f"{candidate.title} #{step + candidate.sequence_number}"
        candidate.slogan = f"{candidate.slogan} route {candidate.sequence_number:02d}"
        return "rewrite_opening_text"
    if mutation == 4:
        candidate.lut_strength = round(candidate.lut_strength + rng.uniform(-0.025, 0.025), 4)
        candidate.zoom = round(min(1.15, max(1.0, candidate.zoom + rng.uniform(-0.015, 0.02))), 4)
        candidate.mirror = not candidate.mirror
        candidate.x_offset = max(-36, min(36, candidate.x_offset + rng.randint(-12, 12)))
        candidate.y_offset = max(-54, min(54, candidate.y_offset + rng.randint(-18, 18)))
        return "adjust_visual_variant"
    candidate.cover_frame_offset = round((candidate.cover_frame_offset + rng.uniform(0.12, 0.45)) % 0.75, 3)
    return "change_cover_frame"


def _mutate_hook_segment(
    candidate: VideoVariant,
    buckets: dict[str, list[ClipMetadata]],
    rng: random.Random,
    recent_clip_ids: set[str],
    max_total_duration: float,
) -> None:
    if not candidate.segments:
        return
    first = candidate.segments[0]
    options = [clip for clip in buckets.get(first.category, []) if clip.clip_id != first.clip.clip_id]
    if not options:
        options = [clip for clips in buckets.values() for clip in clips if clip.clip_id != first.clip.clip_id]
    if not options:
        first.start_time = round((first.start_time + 0.217) % max(first.clip.duration - first.duration, 0.217), 3)
        return
    fresh = [clip for clip in options if clip.clip_id not in recent_clip_ids]
    clip = rng.choice(fresh or options)
    duration = min(first.duration, clip.duration, max_total_duration)
    usable_start, usable_end = _usable_window(clip)
    start_time = _pick_start_time(clip, rng, usable_start, usable_end, duration, prefer_hero=True)
    candidate.segments[0] = SegmentPlan(
        category=clip.category,
        clip=clip,
        start_time=start_time,
        duration=duration,
        index=first.index,
    )


def _mutate_segment_order(candidate: VideoVariant, rng: random.Random) -> None:
    if len(candidate.segments) < 3:
        candidate.segments = list(reversed(candidate.segments))
    else:
        head = candidate.segments[:1]
        tail = candidate.segments[1:]
        rng.shuffle(tail)
        candidate.segments = head + tail
    for index, segment in enumerate(candidate.segments):
        segment.index = index


def _pick_clip(clips: list[ClipMetadata], rng: random.Random, recent_clip_ids: set[str]) -> ClipMetadata:
    fresh = [clip for clip in clips if clip.clip_id not in recent_clip_ids]
    return rng.choice(fresh or clips)


def _usable_window(clip: ClipMetadata) -> tuple[float, float]:
    if clip.usable_range is None:
        return 0.0, max(0.0, float(clip.duration))
    start, end = clip.usable_range
    start = max(0.0, min(float(start), float(clip.duration)))
    end = max(start, min(float(end), float(clip.duration)))
    return start, end


def _segments_touch_history(segments: list[SegmentPlan], recent_segment_keys: set[str]) -> bool:
    return any(_segment_key(segment) in recent_segment_keys for segment in segments)


def _segment_key(segment: SegmentPlan) -> str:
    return f"{segment.clip.clip_id}:{segment.start_time}:{segment.duration}"


def _resolve_sequence(raw: list[dict[str, Any]] | None) -> list[tuple[str, float]]:
    sequence: list[tuple[str, float]] = []
    for item in raw or []:
        category_id = str(item.get("category_id") or "").strip()
        if not category_id:
            continue
        try:
            duration = float(item.get("duration", 0))
        except (TypeError, ValueError):
            continue
        if duration <= 0:
            continue
        sequence.append((category_id, duration))
    if not sequence:
        sequence = [(str(item["category_id"]), float(item["duration"])) for item in DEFAULT_COMPOSITION_SEQUENCE]
    return sequence


def _resolve_narrative_templates(
    settings: ProjectSettings,
    fallback_sequence: list[tuple[str, float]],
    available_categories: set[str],
) -> list[dict[str, Any]]:
    templates: list[dict[str, Any]] = []
    for item in settings.narrative_templates or []:
        if not isinstance(item, dict):
            continue
        template_sequence = _sequence_for_narrative(item, fallback_sequence, available_categories)
        if not template_sequence:
            continue
        templates.append(
            {
                **item,
                "id": str(item.get("id") or "narrative").strip(),
                "account_pool_id": str(item.get("account_pool_id") or item.get("id") or "narrative").strip(),
                "resolved_sequence": template_sequence,
            }
        )
    return templates


def _sequence_for_narrative(
    narrative: dict[str, Any] | None,
    fallback_sequence: list[tuple[str, float]],
    available_categories: set[str],
) -> list[tuple[str, float]]:
    if narrative and isinstance(narrative.get("resolved_sequence"), list):
        return list(narrative["resolved_sequence"])
    raw_sequence = narrative.get("composition_sequence") if narrative else None
    resolved = _resolve_sequence(raw_sequence) if raw_sequence else []
    if not resolved:
        return list(fallback_sequence)
    available = {str(item) for item in available_categories}
    fallback_categories = [category for category, _duration in fallback_sequence if category in available]
    if not fallback_categories:
        fallback_categories = sorted(available)
    if not fallback_categories:
        return []
    fixed: list[tuple[str, float]] = []
    for index, (category, duration) in enumerate(resolved):
        fixed_category = category if category in available else fallback_categories[index % len(fallback_categories)]
        fixed.append((fixed_category, duration))
    return fixed


def _choice_or(items: list[str], rng: random.Random, fallback: str) -> str:
    values = [str(item).strip() for item in items if str(item).strip()]
    return rng.choice(values) if values else fallback


def _max_mutation_retries(settings: ProjectSettings) -> int:
    try:
        return max(0, int(settings.dedupe_policy.get("max_mutation_retries", 1)))
    except (AttributeError, TypeError, ValueError):
        return 1


def _align_duration(target_window: float, beat_pairs: list[tuple[float, float]], start_index: int) -> float:
    total = 0.0
    index = min(start_index, len(beat_pairs) - 1)
    while total + 0.15 < target_window and index < len(beat_pairs):
        start, end = beat_pairs[index]
        total += max(0.1, end - start)
        index += 1
    return round(total, 3)


def _signature_for(
    segments: list[SegmentPlan],
    slogan: str,
    title: str,
    lut_strength: float,
    zoom: float,
    mirror: bool,
    x_offset: int,
    y_offset: int,
    hud_lines: list[str],
    narrative_template_id: str = "",
    cover_frame_offset: float = 0.0,
    structure_variant_id: str = "",
    bgm_offset_bucket: str = "",
) -> str:
    payload = "|".join(
        [
            *(f"{segment.clip.clip_id}:{segment.start_time}:{segment.duration}" for segment in segments),
            slogan,
            title,
            str(lut_strength),
            str(zoom),
            str(mirror),
            str(x_offset),
            str(y_offset),
            str(narrative_template_id),
            f"{float(cover_frame_offset):.3f}",
            str(structure_variant_id),
            str(bgm_offset_bucket),
            *hud_lines,
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()
