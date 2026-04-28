from __future__ import annotations

import hashlib
import random
from collections import defaultdict

from .hud import HudPayload
from .models import ClipMetadata, SegmentPlan, VideoVariant
from .settings import ProjectSettings


SEQUENCE_PATTERN = ["category_A", "category_B", "category_A", "category_C"]
SEGMENT_WINDOWS = [1.5, 3.4, 1.5, 3.0]


def plan_variants(
    clips: list[ClipMetadata],
    settings: ProjectSettings,
    hud_payload: HudPayload,
    beat_grid: list[float],
    output_count: int | None = None,
    seed: int = 42,
) -> list[VideoVariant]:
    count = output_count or settings.output_count
    buckets = defaultdict(list)
    for clip in clips:
        buckets[clip.category].append(clip)
    for category in SEQUENCE_PATTERN:
        if not buckets[category]:
            raise ValueError(
                "Missing required category clips for "
                f"{category}. Upload more varied clips or rename some files with keywords such as "
                "'office/screen/roi' for category_B and 'logo/factory/brand' for category_C."
            )

    rng = random.Random(seed)
    variants: list[VideoVariant] = []
    seen_signatures: set[str] = set()

    for sequence_number in range(1, count + 1):
        attempts = 0
        while attempts < 50:
            attempts += 1
            segments = _pick_segments(buckets, beat_grid, rng)
            title = rng.choice(settings.titles)
            slogan = rng.choice(settings.slogans)
            lut_strength = round(1.0 + rng.uniform(-0.03, 0.03), 4)
            zoom = round(1.04 + rng.uniform(-0.02, 0.03), 4)
            mirror = rng.choice([False, True])
            x_offset = rng.randint(-24, 24)
            y_offset = rng.randint(-42, 42)
            signature = _signature_for(segments, slogan, title, lut_strength, zoom, mirror, x_offset, y_offset, hud_payload.lines)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            variants.append(
                VideoVariant(
                    sequence_number=sequence_number,
                    title=title,
                    slogan=slogan,
                    hud_lines=list(hud_payload.lines),
                    lut_strength=lut_strength,
                    zoom=zoom,
                    mirror=mirror,
                    x_offset=x_offset,
                    y_offset=y_offset,
                    segments=segments,
                    signature=signature,
                )
            )
            break
        else:
            raise RuntimeError("Unable to produce a unique variant signature")
    return variants


def _pick_segments(buckets: dict[str, list[ClipMetadata]], beat_grid: list[float], rng: random.Random) -> list[SegmentPlan]:
    beat_pairs = list(zip(beat_grid, beat_grid[1:]))
    if not beat_pairs:
        raise ValueError("Beat grid is empty")
    selected: list[SegmentPlan] = []
    beat_index = 0
    for index, (category, target_window) in enumerate(zip(SEQUENCE_PATTERN, SEGMENT_WINDOWS, strict=True)):
        clip = rng.choice(buckets[category])
        start_time = max(0.0, round(rng.uniform(0.0, max(clip.duration - target_window, 0.0)), 3))
        duration = _align_duration(target_window, beat_pairs, beat_index)
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
            *hud_lines,
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()
