from __future__ import annotations

import random
from pathlib import Path

from gasgx_distribution.video_matrix import random_variation
from gasgx_distribution.video_matrix.models import ClipMetadata, SegmentPlan, VideoVariant
from gasgx_distribution.video_matrix.settings import ProjectSettings


def _settings(output_root: Path) -> ProjectSettings:
    return ProjectSettings(
        project_name="test",
        source_root=output_root / "source",
        library_root=output_root / "library",
        output_root=output_root,
        output_count=1,
        target_width=1080,
        target_height=1920,
        target_fps=30,
        recent_limits={},
        material_categories=[],
        video_duration_min=1,
        video_duration_max=3,
        default_title_prefix="GasGx",
        slogans=[],
        titles=[],
        composition_sequence=[],
        beat_detection={},
        max_variant_attempts=1,
        variant_history_enabled=False,
        variant_history_limit=0,
        enhancement_modules={"enabled": False, "modules": []},
        copy_mode="template",
    )


def _variant(source: Path, sequence_number: int = 1) -> VideoVariant:
    clip = ClipMetadata(
        clip_id=f"clip-{sequence_number}",
        source_path=source,
        normalized_path=source,
        category="category_A",
        duration=2,
        width=1080,
        height=1920,
        fps=30,
        brightness_score=1,
        contrast_score=1,
        tags=["category_A"],
    )
    segment = SegmentPlan(category="category_A", clip=clip, start_time=0, duration=1, index=0)
    return VideoVariant(
        sequence_number=sequence_number,
        title=f"Title {sequence_number}",
        slogan="Slogan",
        hud_lines=["HUD"],
        lut_strength=1,
        zoom=1,
        mirror=False,
        x_offset=0,
        y_offset=0,
        segments=[segment],
        signature=f"sig-{sequence_number}",
    )


def test_random_variation_plan_is_deterministic(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    variant = _variant(tmp_path / "source.mp4")

    first = random_variation.build_random_variation_plan(variant, settings, enabled=True)
    second = random_variation.build_random_variation_plan(variant, settings, enabled=True)

    assert first.enabled is True
    assert first.signature == second.signature
    assert first.family == second.family
    assert first.filter_suffix == second.filter_suffix


def test_random_variation_framing_shift_filters_include_scale_and_crop(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    variant = _variant(tmp_path / "source.mp4")

    filters = random_variation._build_framing_shift_filters(1.0, random.Random(7), variant, settings, 0, 0)

    assert any(item.startswith("scale=") for item in filters)
    assert any(item.startswith("crop=") for item in filters)


def test_random_variation_plan_skips_recent_history(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    variant = _variant(tmp_path / "source.mp4")

    first = random_variation.build_random_variation_plan(variant, settings, enabled=True)
    blocked = random_variation.build_random_variation_plan(
        variant,
        settings,
        blocked_signatures={first.signature},
        enabled=True,
    )

    assert blocked.enabled is True
    assert blocked.signature != first.signature


def test_random_variation_plan_degrades_to_noop_when_disabled_or_unavailable(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    variant = _variant(tmp_path / "source.mp4")

    disabled = random_variation.build_random_variation_plan(variant, settings, enabled=False)
    assert disabled.enabled is False
    assert disabled.filter_suffix == ""

    monkeypatch.setattr(random_variation, "_build_candidates", lambda *_args, **_kwargs: [])
    unavailable = random_variation.build_random_variation_plan(variant, settings, enabled=True)
    assert unavailable.enabled is False
    assert unavailable.fallback_reason == "no_candidates"


def test_random_variation_plan_degrades_to_noop_when_history_saturates(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    variant = _variant(tmp_path / "source.mp4")
    blocked_plan = random_variation.RandomVariationPlan(
        enabled=True,
        family="grain_light",
        signature="blocked-signature",
        seed=123,
        strength=1.0,
        filter_suffix=",noise=alls=3:allf=t+u",
        filters=["noise=alls=3:allf=t+u"],
    )

    monkeypatch.setattr(random_variation, "_build_candidates", lambda *_args, **_kwargs: [blocked_plan])

    saturated = random_variation.build_random_variation_plan(
        variant,
        settings,
        blocked_signatures={"blocked-signature"},
        enabled=True,
    )

    assert saturated.enabled is False
    assert saturated.fallback_reason == "history_saturated"
    assert saturated.history_hit is True


def test_apply_random_variation_profiles_populates_variant_metadata(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    variant = _variant(tmp_path / "source.mp4")

    plans = random_variation.apply_random_variation_profiles([variant], settings, enabled=True)

    assert len(plans) == 1
    assert variant.random_variation_enabled is True
    assert variant.random_variation_signature
    assert variant.random_variation_profile["signature"] == variant.random_variation_signature
