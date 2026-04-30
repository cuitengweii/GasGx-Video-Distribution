from __future__ import annotations

from pathlib import Path

import pytest

from gasgx_distribution.video_matrix.composition import plan_variants
from gasgx_distribution.video_matrix.hud import HudPayload
from gasgx_distribution.video_matrix.models import ClipMetadata
from gasgx_distribution.video_matrix.pipeline import _beat_duration_hint
from gasgx_distribution.video_matrix.render import _build_filter_complex
from gasgx_distribution.video_matrix.settings import ProjectSettings


def _settings(**overrides) -> ProjectSettings:
    payload = {
        "project_name": "test",
        "source_root": Path("incoming"),
        "library_root": Path("library"),
        "output_root": Path("output"),
        "output_count": 1,
        "target_width": 1080,
        "target_height": 1920,
        "target_fps": 60,
        "recent_limits": {},
        "material_categories": [{"id": item, "label": item} for item in ["category_A", "category_B", "category_C", "category_D", "category_E", "category_F"]],
        "video_duration_min": 8,
        "video_duration_max": 12,
        "default_title_prefix": "GasGx",
        "website_url": "https://www.gasgx.com/roi",
        "hud_enable_live_data": False,
        "hud_fixed_formulas": ["A", "B", "C"],
        "slogans": ["Stop Flaring. Start Hashing."],
        "titles": ["Gas To Compute"],
        "hud_sources": {},
        "composition_sequence": [
            {"category_id": "category_A", "duration": 1.5},
            {"category_id": "category_B", "duration": 3.4},
            {"category_id": "category_A", "duration": 1.5},
            {"category_id": "category_C", "duration": 3.0},
        ],
        "beat_detection": {"mode": "auto", "target_bpm_min": 120, "target_bpm_max": 130, "fallback_spacing": 0.48},
        "max_variant_attempts": 20,
        "variant_history_enabled": True,
        "variant_history_limit": 5000,
        "enhancement_modules": {"enabled": False, "modules": []},
        "copy_mode": "spark_then_template",
    }
    payload.update(overrides)
    return ProjectSettings(**payload)


def _clip(category: str, clip_id: str) -> ClipMetadata:
    return ClipMetadata(
        clip_id=clip_id,
        source_path=Path(f"{clip_id}.mp4"),
        normalized_path=Path(f"{clip_id}.mp4"),
        category=category,
        duration=10.0,
        width=1080,
        height=1920,
        fps=60,
        brightness_score=1,
        contrast_score=1,
        tags=[category],
    )


def test_plan_variants_uses_default_abac_sequence() -> None:
    clips = [_clip("category_A", "a1"), _clip("category_B", "b1"), _clip("category_C", "c1")]

    variants = plan_variants(clips, _settings(), HudPayload(["HUD"], False), [0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5])

    assert [segment.category for segment in variants[0].segments] == ["category_A", "category_B", "category_A", "category_C"]


def test_plan_variants_accepts_configured_non_default_categories() -> None:
    clips = [_clip("category_D", "d1"), _clip("category_E", "e1"), _clip("category_F", "f1")]
    sequence = [
        {"category_id": "category_D", "duration": 0.5},
        {"category_id": "category_E", "duration": 1.0},
        {"category_id": "category_F", "duration": 1.5},
    ]

    variants = plan_variants(
        clips,
        _settings(composition_sequence=sequence),
        HudPayload(["HUD"], False),
        [0, 0.5, 1, 1.5, 2, 2.5, 3],
    )

    assert [segment.category for segment in variants[0].segments] == ["category_D", "category_E", "category_F"]
    assert [segment.duration for segment in variants[0].segments] == [0.5, 1.0, 1.5]


def test_plan_variants_reports_missing_configured_category() -> None:
    with pytest.raises(ValueError, match="category_F"):
        plan_variants(
            [_clip("category_D", "d1")],
            _settings(composition_sequence=[{"category_id": "category_F", "duration": 1.0}]),
            HudPayload(["HUD"], False),
            [0, 0.5, 1],
        )


def test_plan_variants_prefers_new_signature_over_history() -> None:
    clips = [_clip("category_A", "a1"), _clip("category_A", "a2")]
    settings = _settings(composition_sequence=[{"category_id": "category_A", "duration": 0.5}], output_count=2)
    first_batch = plan_variants(clips, settings, HudPayload(["HUD"], False), [0, 0.5, 1, 1.5])
    history = {first_batch[0].signature}

    second_batch = plan_variants(clips, settings, HudPayload(["HUD"], False), [0, 0.5, 1, 1.5], existing_signatures=history)

    assert first_batch[0].signature not in {variant.signature for variant in second_batch}


def test_plan_variants_soft_excludes_recent_clip_ids() -> None:
    clips = [_clip("category_A", "a1"), _clip("category_A", "a2")]
    settings = _settings(composition_sequence=[{"category_id": "category_A", "duration": 0.5}])

    variant = plan_variants(
        clips,
        settings,
        HudPayload(["HUD"], False),
        [0, 0.5, 1, 1.5],
        recent_clip_ids={"a1"},
    )[0]

    assert variant.segments[0].clip.clip_id == "a2"


def test_plan_variants_reuses_recent_clip_when_pool_is_exhausted() -> None:
    clips = [_clip("category_A", "a1")]
    settings = _settings(composition_sequence=[{"category_id": "category_A", "duration": 0.5}])

    variant = plan_variants(
        clips,
        settings,
        HudPayload(["HUD"], False),
        [0, 0.5, 1, 1.5],
        recent_clip_ids={"a1"},
    )[0]

    assert variant.segments[0].clip.clip_id == "a1"


def test_beat_duration_hint_expands_to_composition_total() -> None:
    settings = _settings(video_duration_max=12)
    sequence = [{"category_id": f"category_{idx}", "duration": 2.0} for idx in range(9)]

    assert _beat_duration_hint(settings, sequence, cover_intro_seconds=1.0, outro_seconds=1.0) == 20.0


def test_beat_duration_hint_keeps_configured_max_when_larger() -> None:
    settings = _settings(video_duration_max=30)
    sequence = [{"category_id": "category_A", "duration": 2.0}]

    assert _beat_duration_hint(settings, sequence, cover_intro_seconds=1.0, outro_seconds=1.0) == 30.0


def test_center_aligned_video_template_wraps_drawtext_inside_canvas() -> None:
    variant = plan_variants(
        [_clip("category_A", "a1")],
        _settings(
            composition_sequence=[{"category_id": "category_A", "duration": 0.5}],
            titles=["Generator sets for onsite Bitcoin and industrial load"],
            slogans=["Gas Engines That Turn Field Gas Into Power"],
        ),
        HudPayload(["Gas Engine -> Generator Set -> Power Output", "Field Gas -> Stable Load -> Hashrate"], False),
        [0, 0.5, 1],
    )[0]
    template = {
        "show_hud": True,
        "show_slogan": True,
        "show_title": True,
        "hud_bar_y": 1714,
        "hud_bar_height": 162,
        "hud_x": 70,
        "hud_y": 1762,
        "hud_font_size": 30,
        "slogan_x": 92,
        "slogan_y": 820,
        "slogan_font_size": 64,
        "title_x": 116,
        "title_y": 627,
        "title_font_size": 30,
        "hud_bar_color": "#75b37b",
        "hud_bar_opacity": 0.44,
        "primary_color": "#5DD62C",
        "secondary_color": "#FFFFFF",
        "align": "center",
    }

    filter_complex, _inputs = _build_filter_complex(variant, _settings(), template_config=template)

    assert "Gas Engines That Turn" in filter_complex
    assert "Gas Engines That Turn Field Gas Into Power':x=92" not in filter_complex
    assert filter_complex.count("fontsize=64") >= 2
    assert filter_complex.count("x=(w-text_w)/2") >= 3
