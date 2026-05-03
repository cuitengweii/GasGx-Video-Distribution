from __future__ import annotations

from pathlib import Path

from PIL import Image
import pytest

from gasgx_distribution.video_matrix import render
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


def _variant(source: Path) -> VideoVariant:
    clip = ClipMetadata(
        clip_id="clip-1",
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
        sequence_number=1,
        title="Title",
        slogan="Slogan",
        hud_lines=["HUD"],
        lut_strength=1,
        zoom=1,
        mirror=False,
        x_offset=0,
        y_offset=0,
        segments=[segment],
        signature="sig-1",
    )


def test_render_variant_cleans_temporary_covers_after_concat_failure(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")

    def fake_extract_frame(_video_path: Path, output_path: Path, timestamp: float) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (1080, 1920), "#111111").save(output_path)

    def fail_concat(*_args, **_kwargs) -> None:
        raise RuntimeError("concat failed")

    monkeypatch.setattr(render, "extract_frame", fake_extract_frame)
    monkeypatch.setattr(render, "concat_video", fail_concat)

    with pytest.raises(RuntimeError, match="concat failed"):
        render.render_variant(
            _variant(source),
            _settings(tmp_path),
            template_copy="",
            batch_dir=tmp_path,
            cover_template_config={"name": "Cover"},
            outro_text="Follow GasGx",
        )

    assert not list(tmp_path.glob("*_intro_cover.png"))
    assert not list(tmp_path.glob("*_outro_cover.png"))
    assert not (tmp_path / ".render_tmp").exists()


def test_render_variant_appends_prebuilt_ending_template(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    ending = tmp_path / "ending.mp4"
    source.write_bytes(b"video")
    ending.write_bytes(b"ending")
    captured = {}

    def fake_concat(filter_complex, inputs, output, bgm_path=None) -> None:
        captured["filter_complex"] = filter_complex
        captured["inputs"] = inputs
        output.write_bytes(b"mp4")

    def fail_extract_frame(*_args, **_kwargs) -> None:
        raise AssertionError("prebuilt ending should skip dynamic outro frame extraction")

    monkeypatch.setattr(render, "concat_video", fake_concat)
    monkeypatch.setattr(render, "extract_frame", fail_extract_frame)

    render.render_variant(
        _variant(source),
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        cover_template_config={"name": "Cover"},
        cover_intro_seconds=0,
        outro_text="Follow GasGx",
        ending_template_path=ending,
    )

    assert ending in captured["inputs"]
    assert "[ending]" in captured["filter_complex"]
    assert "concat=n=2" in captured["filter_complex"]


def test_render_variant_copy_uses_ending_follow_text_without_cta(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    variant = _variant(source)

    def fake_concat(_filter_complex, _inputs, output, bgm_path=None) -> None:
        output.write_bytes(b"mp4")

    monkeypatch.setattr(render, "concat_video", fake_concat)

    asset = render.render_variant(
        variant,
        _settings(tmp_path),
        template_copy="{title}\n\n片尾文案:\n{ending_follow_text}\n\nHUD:\n{hud_summary}\n",
        batch_dir=tmp_path,
        cover_template_config=None,
        output_types={"mp4", "txt"},
        outro_text="界面录入的片尾文案",
    )

    copy_text = asset.copy_path.read_text(encoding="utf-8")
    assert "界面录入的片尾文案" in copy_text
    assert "https://example.test" not in copy_text
    assert "CTA" not in copy_text


def test_render_variant_uses_independent_ending_cover_template(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    captured = {}

    def fake_extract_frame(_video_path: Path, output_path: Path, timestamp: float) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (1080, 1920), "#111111").save(output_path)

    def fake_outro_cover(_frame, output, _settings, template, outro_text, hud_lines) -> None:
        captured["template"] = template
        captured["outro_text"] = outro_text
        captured["hud_lines"] = hud_lines
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"outro")

    def fake_concat(filter_complex, inputs, output, bgm_path=None) -> None:
        captured["inputs"] = inputs
        output.write_bytes(b"mp4")

    ending_template = {"name": "Ending Only", "single_cover_logo_text": "Tail Logo", "single_cover_logo_y": 220}
    monkeypatch.setattr(render, "extract_frame", fake_extract_frame)
    monkeypatch.setattr(render, "render_outro_cover", fake_outro_cover)
    monkeypatch.setattr(render, "concat_video", fake_concat)

    render.render_variant(
        _variant(source),
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        cover_template_config={"name": "First Screen"},
        ending_cover_template_config=ending_template,
        cover_intro_seconds=0,
        outro_text="Follow GasGx",
    )

    assert captured["template"] == ending_template
    assert captured["outro_text"] == "Follow GasGx"
    assert any(str(path).endswith("_outro_cover.png") for path in captured["inputs"])


def test_render_variant_escapes_drawtext_apostrophe_before_following_lines(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    captured = {}
    variant = _variant(source)
    variant.title = "The World's leading engine for monetizing stranded natural gas"

    def fake_concat(filter_complex, inputs, output, bgm_path=None) -> None:
        captured["filter_complex"] = filter_complex
        output.write_bytes(b"mp4")

    monkeypatch.setattr(render, "concat_video", fake_concat)

    render.render_variant(
        variant,
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        cover_template_config=None,
        template_config={
            "show_hud": True,
            "show_slogan": True,
            "show_title": True,
            "title_font_size": 52,
            "title_x": 44,
            "title_y": 450,
        },
    )

    assert "textfile=" in captured["filter_complex"]
    assert "text=The World" not in captured["filter_complex"]
    assert ";x=" not in captured["filter_complex"]
    assert "drawtext=fontfile=" in captured["filter_complex"]


def test_overlay_filters_match_selected_template_layer_rules(tmp_path: Path) -> None:
    template = {
        "show_hud": True,
        "show_slogan": True,
        "show_title": True,
        "hud_bar_y": 1230,
        "hud_bar_height": 155,
        "hud_bar_color": "#0E1A10",
        "hud_bar_opacity": 0.38,
        "hud_x": 217,
        "hud_y": 1296,
        "hud_font_size": 40,
        "hud_color": "#d3168a",
        "slogan_x": 44,
        "slogan_y": 450,
        "slogan_font_size": 58,
        "slogan_color": "#d8228c",
        "slogan_bg_y": 420,
        "slogan_bg_height": 216,
        "slogan_bg_opacity": 0,
        "slogan_text_align": "center",
        "slogan_text_effect": "none",
        "title_x": 228,
        "title_y": 795,
        "title_font_size": 52,
        "title_color": "#d3168a",
        "title_bg_y": 772,
        "title_bg_height": 140,
        "title_bg_opacity": 0,
        "title_text_align": "center",
        "title_text_effect": "none",
    }

    filters = render._overlay_filters(
        render.coerce_template(template),
        "终结废气 | 重塑能源 | 就地变现",
        "The World's leading engine for monetizing stranded natural gas computing power",
        "GasGx天然气发电机组\n搁浅天然气首选",
        set(template),
        text_dir=tmp_path,
    )

    assert "drawbox=x=0:y=1230" in filters
    assert "drawbox=x=0:y=420" not in filters
    assert "drawbox=x=0:y=772" not in filters
    assert "textfile=" in filters
    assert "text=The World" not in filters
    assert filters.count(":x=(w-text_w)/2:y=") >= 2
    assert "alpha='0.80+0.20*sin" not in filters
    assert "+44*exp" not in filters
    assert (tmp_path / "slogan_0.txt").read_text(encoding="utf-8").startswith("The World's leading engine")
    assert (tmp_path / "title_0.txt").read_text(encoding="utf-8") == "GasGx天然气发电机组"
    assert (tmp_path / "title_1.txt").read_text(encoding="utf-8") == "搁浅天然气首选"

def test_text_effect_options_cover_template_dropdown_values() -> None:
    expected_fragments = {
        "fade-in": "enable='gte(t\\,0.00)'",
        "fade-out": "alpha='exp(-0.35*(t-0.00))'",
        "fade-in-out": "sin(1.7*(t-0.00))",
        "slide-down": "-44*exp",
        "slide-left": "+64*exp",
        "slide-right": "-64*exp",
        "blink": "gt(sin(10*(t-0.00))\\,0)",
        "wave": "+10*sin",
        "jitter": "+3*sin",
        "zoom-in": "fontsize='48*(1-0.16*exp",
        "shadow-pop": "shadowcolor=0x000000@0.80",
    }

    for effect, fragment in expected_fragments.items():
        assert fragment in render._text_effect_options(effect, "100", "200", line_index=0, font_size=48)


def test_text_style_options_generate_drawtext_fragments() -> None:
    expected_fragments = {
        "soft-shadow": "shadowcolor=0x000000@0.78",
        "hard-shadow": "shadowx=6:shadowy=6",
        "outline": "bordercolor=0x000000@0.92",
        "white-outline": "bordercolor=0xFFFFFF@0.92",
        "glow": "shadowcolor=0x5DD62C@0.76",
        "neon": "bordercolor=0x5DD62C@0.82",
        "gradient": "bordercolor=0x000000@0.44",
        "reflection": "shadowcolor=0x000000@0.62",
    }

    for style, fragment in expected_fragments.items():
        assert fragment in render._text_style_options(style)


def test_text_style_extra_filters_support_gradient_and_reflection_layers() -> None:
    gradient_filters = render._text_style_extra_filters(
        "gradient",
        "fontfile=/tmp/font.ttf:",
        "text=GasGx",
        "100",
        "200",
        font_size=48,
        line_index=0,
    )
    reflection_filters = render._text_style_extra_filters(
        "reflection",
        "fontfile=/tmp/font.ttf:",
        "text=GasGx",
        "100",
        "200",
        font_size=48,
        line_index=0,
    )

    assert "fontcolor=#5DD62C@0.90" in gradient_filters[0]
    assert "fontcolor=#1F8F23@0.72" in gradient_filters[1]
    assert "fontcolor=#FFFFFF@0.24" in reflection_filters[0]
    assert "alpha='0.24*exp" in reflection_filters[0]


def test_overlay_filters_apply_selected_text_style_to_render_layers(tmp_path: Path) -> None:
    template = render.coerce_template(
        {
            "show_hud": True,
            "show_slogan": True,
            "show_title": True,
            "hud_text_style": "glow",
            "slogan_text_style": "gradient",
            "title_text_style": "reflection",
            "hud_text_effect": "none",
            "slogan_text_effect": "none",
            "title_text_effect": "none",
            "slogan_text_align": "center",
            "title_text_align": "center",
        }
    )

    filters = render._overlay_filters(
        template,
        "HUD",
        "The World's leading engine for monetizing stranded natural gas computing power",
        "GasGx Natural Gas",
        set(template),
        text_dir=tmp_path,
    )

    assert filters.count("fontcolor=0x5DD62C@0.42") >= 5
    assert "fontcolor=#5DD62C@0.90" in filters
    assert "fontcolor=#1F8F23@0.72" in filters
    assert "fontcolor=#FFFFFF:" in filters
    assert "fontcolor=#FFFFFF@0.24" in filters
    assert "alpha='0.24*exp" in filters
