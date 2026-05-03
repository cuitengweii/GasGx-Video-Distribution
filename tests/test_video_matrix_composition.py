from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from gasgx_distribution.video_matrix.composition import plan_variants
from gasgx_distribution.video_matrix.hud import HudPayload
from gasgx_distribution.video_matrix.models import ClipMetadata
from gasgx_distribution.video_matrix.pipeline import _beat_duration_hint, _fit_composition_sequence_to_max_duration
from gasgx_distribution.video_matrix import cover as cover_renderer
from gasgx_distribution.video_matrix import render as video_renderer
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
        "slogans": ["Stop Flaring. Start Hashing."],
        "titles": ["Gas To Compute"],
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

    assert _beat_duration_hint(settings, sequence, cover_intro_seconds=1.0, outro_seconds=1.0) == 12.0


def test_beat_duration_hint_keeps_configured_max_when_larger() -> None:
    settings = _settings(video_duration_max=30)
    sequence = [{"category_id": "category_A", "duration": 2.0}]

    assert _beat_duration_hint(settings, sequence, cover_intro_seconds=1.0, outro_seconds=1.0) == 8.0


def test_fit_composition_sequence_caps_material_duration_without_outro() -> None:
    sequence = [
        {"category_id": "category_A", "duration": 4.0},
        {"category_id": "category_B", "duration": 8.0},
        {"category_id": "category_C", "duration": 8.0},
    ]

    fitted = _fit_composition_sequence_to_max_duration(sequence, 10.0)

    assert sum(float(item["duration"]) for item in fitted) <= 10.0
    assert [item["category_id"] for item in fitted] == ["category_A", "category_B", "category_C"]
    assert sequence[0]["duration"] == 4.0


def test_plan_variants_caps_segments_at_max_duration() -> None:
    clips = [_clip("category_A", "a1"), _clip("category_B", "b1"), _clip("category_C", "c1")]
    settings = _settings(
        video_duration_max=3,
        composition_sequence=[
            {"category_id": "category_A", "duration": 2.0},
            {"category_id": "category_B", "duration": 2.0},
            {"category_id": "category_C", "duration": 2.0},
        ],
    )

    variant = plan_variants(clips, settings, HudPayload(["HUD"], False), [0, 1, 2, 3, 4, 5])[0]

    assert sum(segment.duration for segment in variant.segments) <= 3.0


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
        "hud_bar_x": 110,
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
        "slogan_color": "#33FF66",
        "title_color": "#FF3366",
        "hud_color": "#66CCFF",
        "align": "center",
    }

    filter_complex, _inputs = _build_filter_complex(variant, _settings(), template_config=template)

    assert "Gas Engines That Turn" in filter_complex
    assert "Gas Engines That Turn Field Gas Into Power':x=92" not in filter_complex
    assert "drawbox=x=110:y=1714" in filter_complex
    assert "drawbox=x=0:y=820:w=1080:h=80:color=#75b37b@0.62:t=fill" in filter_complex
    assert "drawbox=x=0:y=627:w=1080:h=92:color=#75b37b@0.62:t=fill" in filter_complex
    assert "fontcolor=#33FF66" in filter_complex
    assert "fontcolor=#FF3366" in filter_complex
    assert "fontcolor=#66CCFF" in filter_complex
    assert filter_complex.count("fontsize=64") >= 2
    assert filter_complex.count("x=(w-text_w)/2") >= 3


def test_video_template_text_background_and_color_defaults_match_preview() -> None:
    variant = plan_variants(
        [_clip("category_A", "a1")],
        _settings(
            composition_sequence=[{"category_id": "category_A", "duration": 0.5}],
            titles=["GasGx天然气发电机组 搞浅天然气首选"],
            slogans=["The World's leading engine for monetizing stranded natural gas computing power"],
        ),
        HudPayload(["Gas Engine -> Generator Set -> Power Output"], False),
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
        "primary_color": "#FF2FAC",
        "secondary_color": "#FFFFFF",
        "align": "center",
    }

    filter_complex, _inputs = _build_filter_complex(variant, _settings(), template_config=template)

    assert "drawbox=x=0:y=820:w=1080:h=80:color=#75b37b@0.62:t=fill" in filter_complex
    assert "drawbox=x=0:y=627:w=1080:h=92:color=#75b37b@0.62:t=fill" in filter_complex
    assert filter_complex.count("fontcolor=#FF2FAC") >= 2
    assert "fontcolor=#FFFFFF" in filter_complex


def test_video_template_text_effect_is_rendered_in_drawtext_filter() -> None:
    variant = plan_variants(
        [_clip("category_A", "a1")],
        _settings(
            composition_sequence=[{"category_id": "category_A", "duration": 0.5}],
            titles=["GasGx天然气发电机组 搞浅天然气首选"],
            slogans=["The World's leading engine for monetizing stranded natural gas computing power"],
        ),
        HudPayload(["Gas Engine -> Generator Set -> Power Output"], False),
        [0, 0.5, 1],
    )[0]
    template = {
        "show_hud": False,
        "show_slogan": True,
        "show_title": True,
        "slogan_x": 92,
        "slogan_y": 820,
        "slogan_font_size": 64,
        "slogan_text_effect": "slide-up",
        "title_x": 116,
        "title_y": 627,
        "title_font_size": 30,
        "title_text_effect": "shake",
        "hud_bar_y": 1714,
        "hud_bar_height": 162,
        "hud_bar_color": "#75b37b",
        "hud_bar_opacity": 0.44,
        "primary_color": "#FF2FAC",
        "secondary_color": "#FFFFFF",
        "align": "center",
    }

    filter_complex, _inputs = _build_filter_complex(variant, _settings(), template_config=template)

    assert ":y=820+44*exp(-4*(t-0.00))" in filter_complex
    assert ":x=(w-text_w)/2+8*sin(38*(t-0.00)):y=627" in filter_complex


def test_hud_text_defaults_to_primary_color_when_not_overridden() -> None:
    variant = plan_variants(
        [_clip("category_A", "a"), _clip("category_B", "b"), _clip("category_C", "c")],
        _settings(),
        HudPayload(["Gas Input -> Power"], False),
        [0, 0.5, 1, 1.5],
    )[0]
    template = {
        "show_hud": True,
        "show_slogan": False,
        "show_title": False,
        "hud_bar_x": 0,
        "hud_bar_y": 1714,
        "hud_bar_width": 1080,
        "hud_bar_height": 162,
        "hud_x": 70,
        "hud_y": 1762,
        "hud_font_size": 30,
        "hud_bar_color": "#75b37b",
        "hud_bar_opacity": 0.44,
        "primary_color": "#5DD62C",
        "secondary_color": "#FFFFFF",
    }

    filter_complex, _inputs = _build_filter_complex(variant, _settings(), template_config=template)

    assert "fontcolor=#5DD62C" in filter_complex
    assert "fontcolor=#FFFFFF" not in filter_complex


def test_hud_text_alignment_uses_hud_specific_template_value() -> None:
    variant = plan_variants(
        [_clip("category_A", "a"), _clip("category_B", "b"), _clip("category_C", "c")],
        _settings(),
        HudPayload(["Gas Input -> Power"], False),
        [0, 0.5, 1, 1.5],
    )[0]
    template = {
        "show_hud": True,
        "show_slogan": False,
        "show_title": False,
        "hud_bar_x": 0,
        "hud_bar_y": 1714,
        "hud_bar_width": 1080,
        "hud_bar_height": 162,
        "hud_x": 0,
        "hud_y": 1762,
        "hud_font_size": 30,
        "hud_text_align": "right",
        "hud_bar_color": "#75b37b",
        "hud_bar_opacity": 0.44,
        "primary_color": "#5DD62C",
        "align": "left",
    }

    filter_complex, _inputs = _build_filter_complex(variant, _settings(), template_config=template)

    assert ":x=w-text_w-0:y=1762" in filter_complex
    assert ":x=0:y=1762" not in filter_complex


def test_video_template_background_overlay_uses_rounded_image_in_render_path(tmp_path: Path) -> None:
    variant = plan_variants(
        [_clip("category_A", "a1")],
        _settings(
            composition_sequence=[{"category_id": "category_A", "duration": 0.5}],
            titles=["GasGx title"],
            slogans=["Slogan text"],
        ),
        HudPayload(["Gas Input -> Power"], False),
        [0, 0.5, 1],
    )[0]
    template = {
        "show_hud": True,
        "show_slogan": True,
        "show_title": True,
        "hud_bar_x": 252,
        "hud_bar_y": 1202,
        "hud_bar_width": 576,
        "hud_bar_height": 168,
        "hud_bar_radius": 37,
        "hud_bar_color": "#2c302d",
        "hud_bar_opacity": 0.53,
        "slogan_bg_x": 96,
        "slogan_bg_y": 906,
        "slogan_bg_width": 888,
        "slogan_bg_height": 207,
        "slogan_bg_radius": 37,
        "slogan_bg_color": "#363a37",
        "slogan_bg_opacity": 0.62,
        "title_bg_x": 108,
        "title_bg_y": 635,
        "title_bg_width": 864,
        "title_bg_height": 207,
        "title_bg_radius": 37,
        "title_bg_color": "#3e4740",
        "title_bg_opacity": 0.89,
        "hud_x": 0,
        "hud_y": 1260,
        "hud_font_size": 40,
        "slogan_x": 24,
        "slogan_y": 927,
        "slogan_font_size": 52,
        "title_x": 60,
        "title_y": 687,
        "title_font_size": 50,
        "primary_color": "#1e1f20",
        "secondary_color": "#6710ea",
    }

    filter_complex, inputs = _build_filter_complex(variant, _settings(), template_config=template, text_dir=tmp_path / "text_layers")

    overlay_path = tmp_path / "template_background_overlay.png"
    assert overlay_path in inputs
    assert overlay_path.exists()
    assert "template_background_overlay.png" not in filter_complex
    assert "overlay=0:0:format=auto" in filter_complex
    assert "drawbox=" not in filter_complex
    image = Image.open(overlay_path).convert("RGBA")
    assert image.getpixel((108, 635))[3] == 0
    assert image.getpixel((145, 672))[3] > 0


def test_video_template_render_uses_template_font_family_for_drawtext() -> None:
    variant = plan_variants(
        [_clip("category_A", "a1")],
        _settings(
            composition_sequence=[{"category_id": "category_A", "duration": 0.5}],
            titles=["Industrial gas power"],
            slogans=["Monetize stranded gas"],
        ),
        HudPayload(["Gas Input -> Power"], False),
        [0, 0.5, 1],
    )[0]
    template = {
        "show_hud": False,
        "show_slogan": True,
        "show_title": True,
        "slogan_x": 92,
        "slogan_y": 820,
        "slogan_font_size": 64,
        "slogan_font_family": "'Segoe UI Black', 'Arial Black', sans-serif",
        "title_x": 116,
        "title_y": 627,
        "title_font_size": 50,
        "title_font_family": "Impact, 'Arial Black', sans-serif",
        "hud_bar_y": 1714,
        "hud_bar_height": 162,
        "hud_bar_color": "#75b37b",
        "hud_bar_opacity": 0,
        "primary_color": "#FF2FAC",
        "secondary_color": "#FFFFFF",
    }

    filter_complex, _inputs = _build_filter_complex(variant, _settings(), template_config=template)

    assert "seguibl.ttf" in filter_complex or "segoeuib.ttf" in filter_complex or "ariblk.ttf" in filter_complex
    assert "impact.ttf" in filter_complex or "ariblk.ttf" in filter_complex
    assert "msyh.ttc" not in filter_complex


def test_video_renderer_maps_ad_font_families_to_ffmpeg_candidates() -> None:
    assert video_renderer._font_candidates_for_family("DINNextLTPro-Bold")[0].name == "DINNextLTPro-Bold.ttf"
    assert video_renderer._font_candidates_for_family("'Microsoft YaHei Bold', sans-serif")[0].name == "msyhbd.ttc"
    assert video_renderer._font_candidates_for_family("'Noto Sans SC Bold', sans-serif")[0].name == "Noto Sans SC Bold (TrueType).otf"
    assert video_renderer._font_candidates_for_family("'Alibaba PuHuiTi Heavy', sans-serif")[0].name == "AlibabaPuHuiTi-Heavy.ttf"
    assert video_renderer._font_candidates_for_family("YouSheBiaoTiHei, sans-serif")[0].name == "YouSheBiaoTiHei.ttf"
    assert video_renderer._font_candidates_for_family("English Serif Luxe")[0].name == "georgia.ttf"
    assert video_renderer._font_candidates_for_family("English Data Mono")[0].name == "lucon.ttf"
    assert video_renderer._font_candidates_for_family("English Pop Comic")[0].name == "comic.ttf"


def test_video_renderer_title_background_height_does_not_inherit_slogan_height() -> None:
    template = {
        "title_bg_y": 100,
        "title_y": 120,
        "title_bg_width": 900,
        "slogan_bg_height": 260,
        "title_bg_color": "#111111",
        "title_bg_opacity": 0.6,
    }

    spec = video_renderer._background_box_spec(template, "title")

    assert spec is not None
    assert spec[3] == 92


def test_video_renderer_prefers_cjk_fonts_for_chinese_text() -> None:
    font_names = [candidate.name for candidate in video_renderer.FONT_CANDIDATES[:6]]

    assert "msyh.ttc" in font_names
    assert "simhei.ttf" in font_names
    assert "simsun.ttc" in font_names
    assert font_names.index("msyh.ttc") < font_names.index("simhei.ttf")


def test_cover_renderer_prefers_cjk_fonts_for_chinese_text() -> None:
    regular_names = [candidate.name for candidate in cover_renderer.FONT_CANDIDATES[:6]]
    bold_names = [candidate.name for candidate in cover_renderer.BOLD_FONT_CANDIDATES[:6]]

    assert "msyh.ttc" in regular_names
    assert "simhei.ttf" in regular_names
    assert "msyhbd.ttc" in bold_names
    assert "simhei.ttf" in bold_names


def test_single_video_cover_renders_reference_layout_scale() -> None:
    settings = _settings()
    background = Image.new("RGB", (1080, 1920), "#223322")

    image = cover_renderer.render_cover_preview_image(
        settings,
        {
            "cover_layout": "single_video",
            "single_cover_logo_text": "GasGx",
            "single_cover_slogan_text": "终结废气 | 重塑能源 | 就地变现",
            "single_cover_title_text": "全球领先的搁浅天然气算力变现引擎",
            "single_cover_logo_font_size": 92,
            "single_cover_slogan_font_size": 64,
            "single_cover_title_font_size": 48,
        },
        background=background,
    )

    assert image.size == (1080, 1920)
    assert image.getpixel((90, 260)) != image.getpixel((540, 1500))


def test_single_video_cover_honors_visual_template_alignment_color_and_offset() -> None:
    settings = _settings()
    background = Image.new("RGB", (1080, 1920), "#111111")

    image = cover_renderer.render_cover_preview_image(
        settings,
        {
            "cover_layout": "single_video",
            "mask_mode": "none",
            "single_cover_logo_text": "TAIL",
            "single_cover_slogan_text": " ",
            "single_cover_title_text": " ",
            "single_cover_logo_font_size": 120,
            "singleLogo_color": "#5DD62C",
            "singleLogo_text_align": "center",
            "singleLogo_offset_y": 100,
        },
        background=background,
    )

    def green_pixels(box: tuple[int, int, int, int]) -> int:
        crop = image.crop(box)
        pixels = crop.load()
        return sum(
            1
            for x in range(crop.width)
            for y in range(crop.height)
            for red, green, blue in [pixels[x, y]]
            if green > 140 and red < 130 and blue < 130
        )

    assert green_pixels((360, 440, 720, 640)) > 500
    assert green_pixels((40, 440, 260, 640)) < 40
    assert green_pixels((360, 300, 720, 410)) < 40
