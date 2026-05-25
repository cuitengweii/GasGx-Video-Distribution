from __future__ import annotations

import base64
import json
from io import BytesIO
from pathlib import Path

from PIL import Image
import pytest

from gasgx_distribution.video_matrix import render
from gasgx_distribution.video_matrix.ffmpeg_tools import FFmpegError
from gasgx_distribution.video_matrix.models import ClipMetadata, SegmentPlan, VideoVariant
from gasgx_distribution.video_matrix.settings import ProjectSettings
from gasgx_distribution.video_matrix.watermark import build_watermark_overlay, resolve_watermark_text, sanitize_watermark_text


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

    def fake_concat(filter_complex, inputs, output, bgm_path=None, speed_mode=None, threads=None) -> None:
        captured["filter_complex"] = filter_complex
        captured["inputs"] = inputs
        captured["body_output"] = output
        output.write_bytes(b"mp4")

    def fake_append(main_video, ending_video, output, width, height, fps, threads=None) -> None:
        captured["append"] = {
            "main_video": main_video,
            "ending_video": ending_video,
            "output": output,
            "width": width,
            "height": height,
            "fps": fps,
        }
        output.write_bytes(b"mp4+ending")

    def fail_extract_frame(*_args, **_kwargs) -> None:
        raise AssertionError("prebuilt ending should skip dynamic outro frame extraction")

    monkeypatch.setattr(render, "concat_video", fake_concat)
    monkeypatch.setattr(render, "append_video_tail", fake_append)
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

    assert ending not in captured["inputs"]
    assert "[ending]" not in captured["filter_complex"]
    assert "concat=n=1" in captured["filter_complex"]
    assert captured["body_output"].name == ".vibe_01_main.mp4"
    assert captured["append"]["main_video"].name == ".vibe_01_main.mp4"
    assert captured["append"]["ending_video"] == ending
    assert captured["append"]["output"].name == "vibe_01.mp4"
    assert captured["append"]["width"] == 1080
    assert captured["append"]["height"] == 1920
    assert captured["append"]["fps"] == 30
    assert not (tmp_path / ".vibe_01_main.mp4").exists()


def test_render_variant_copy_uses_ending_follow_text_without_cta(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    variant = _variant(source)

    def fake_concat(_filter_complex, _inputs, output, bgm_path=None, speed_mode=None, threads=None) -> None:
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


def test_render_variant_passes_bgm_offset_and_writes_manifest(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    bgm = tmp_path / "bgm.mp3"
    source.write_bytes(b"video")
    bgm.write_bytes(b"audio")
    captured = {}

    def fake_concat(_filter_complex, _inputs, output, **kwargs) -> None:
        captured.update(kwargs)
        output.write_bytes(b"mp4")

    monkeypatch.setattr(render, "concat_video", fake_concat)

    asset = render.render_variant(
        _variant(source),
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        bgm_path=bgm,
        output_types={"mp4", "json"},
        bgm_start_offset=12.0,
    )

    manifest = json.loads(asset.manifest_path.read_text(encoding="utf-8"))
    assert captured["bgm_start_offset"] == 12.0
    assert manifest["bgm_start_offset"] == 12.0
    assert manifest["bgm_offset_bucket"] == "b1"


def test_render_variant_appends_random_variation_suffix_only_when_enabled(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    captured: dict[str, str] = {}

    def fake_concat(filter_complex, inputs, output, bgm_path=None, speed_mode=None, threads=None) -> None:
        captured["filter_complex"] = filter_complex
        output.write_bytes(b"mp4")

    monkeypatch.setattr(render, "concat_video", fake_concat)

    base_variant = _variant(source)
    render.render_variant(
        base_variant,
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        cover_intro_seconds=0,
        outro_seconds=0,
    )
    assert "noise=alls=3:allf=t+u" not in captured["filter_complex"]

    enabled_variant = _variant(source)
    enabled_variant.random_variation_enabled = True
    enabled_variant.random_variation_family = "grain_light"
    enabled_variant.random_variation_signature = "sig-variation"
    enabled_variant.random_variation_profile = {
        "enabled": True,
        "family": "grain_light",
        "signature": "sig-variation",
        "filter_suffix": ",noise=alls=3:allf=t+u",
        "filters": ["noise=alls=3:allf=t+u"],
    }
    render.render_variant(
        enabled_variant,
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        cover_intro_seconds=0,
        outro_seconds=0,
    )
    assert "noise=alls=3:allf=t+u" in captured["filter_complex"]


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

    def fake_concat(filter_complex, inputs, output, bgm_path=None, speed_mode=None, threads=None) -> None:
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

    def fake_concat(filter_complex, inputs, output, bgm_path=None, speed_mode=None, threads=None) -> None:
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


def test_render_variant_wraps_ffmpeg_errors_with_user_message(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")

    def fail_concat(*_args, **_kwargs) -> None:
        raise FFmpegError("ffmpeg not found")

    monkeypatch.setattr(render, "concat_video", fail_concat)

    with pytest.raises(render.VideoMatrixRenderError) as excinfo:
        render.render_variant(
            _variant(source),
            _settings(tmp_path),
            template_copy="",
            batch_dir=tmp_path,
            cover_template_config=None,
        )

    assert excinfo.value.error_code == "ffmpeg_failed"
    assert excinfo.value.user_message == "请确认已安装 FFmpeg 并加入 PATH"


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
        "电价低至0.01美元/度\n发电+矿箱一体化",
        "The World's leading engine for monetizing stranded natural gas computing power",
        "GasGx天然气发电机组\n搁浅天然气首选",
        set(template),
        text_dir=tmp_path,
    )

    assert "drawbox=x=80:y=1230" in filters
    assert "drawbox=x=120:y=420" not in filters
    assert "drawbox=x=144:y=772" not in filters
    assert "textfile=" in filters
    assert "text=The World" not in filters
    assert filters.count("textfile=") >= 5
    assert "alpha='0.80+0.20*sin" not in filters
    assert "+44*exp" not in filters
    assert (tmp_path / "hud_0.txt").read_text(encoding="utf-8") == "电价低至0.01美元/度"
    assert (tmp_path / "hud_1.txt").read_text(encoding="utf-8") == "发电+矿箱一体化"
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
        "shadow-pop": "shadowcolor=0x000000@0.80:shadowx=6:shadowy=6",
    }

    for effect, fragment in expected_fragments.items():
        assert fragment in render._text_effect_options(effect, "100", "200", line_index=0, font_size=48)
        assert "shadowx='" not in render._text_effect_options(effect, "100", "200", line_index=0, font_size=48)


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


def test_text_style_extra_filters_inherit_text_effects() -> None:
    glow_filters = render._text_style_extra_filters(
        "glow",
        "fontfile=/tmp/font.ttf:",
        "text=GasGx",
        "100",
        "200",
        font_size=48,
        line_index=0,
        effect="fade-in",
    )
    gradient_filters = render._text_style_extra_filters(
        "gradient",
        "fontfile=/tmp/font.ttf:",
        "text=GasGx",
        "100",
        "200",
        font_size=48,
        line_index=0,
        effect="slide-left",
    )
    reflection_filters = render._text_style_extra_filters(
        "reflection",
        "fontfile=/tmp/font.ttf:",
        "text=GasGx",
        "100",
        "200",
        font_size=48,
        line_index=0,
        effect="pulse",
    )

    assert all("alpha='1-exp" in item for item in glow_filters)
    assert all("+64*exp" in item for item in gradient_filters)
    assert "0.80+0.20*sin" in reflection_filters[0]


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


def test_overlay_filters_keep_text_style_and_effect_in_fast_first_mode(tmp_path: Path) -> None:
    template = render.coerce_template(
        {
            "show_hud": True,
            "show_slogan": True,
            "show_title": True,
            "hud_text_style": "soft-shadow",
            "slogan_text_style": "outline",
            "title_text_style": "white-outline",
            "hud_text_effect": "jitter",
            "slogan_text_effect": "fade-in-out",
            "title_text_effect": "slide-up",
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
        speed_mode="fast_first",
    )

    assert "shadowcolor=0x000000@0.78" in filters
    assert "bordercolor=0x000000@0.92" in filters
    assert "bordercolor=0xFFFFFF@0.92" in filters
    assert "+3*sin" in filters
    assert "sin(1.7*(t-0.00))" in filters
    assert "44*exp(-4*(t-0.00))" in filters


def test_overlay_filters_do_not_truncate_multiline_slogan_by_default(tmp_path: Path) -> None:
    template = render.coerce_template(
        {
            "show_hud": False,
            "show_slogan": True,
            "show_title": False,
            "slogan_font_size": 42,
            "slogan_x": 120,
            "slogan_y": 540,
            "slogan_text_width": 800,
            "slogan_text_align": "left",
        }
    )

    render._overlay_filters(
        template,
        hud_text="",
        slogan="L1\nL2\nL3\nL4",
        title="",
        explicit_template_keys=set(template),
        text_dir=tmp_path,
    )

    assert (tmp_path / "slogan_0.txt").read_text(encoding="utf-8") == "L1"
    assert (tmp_path / "slogan_1.txt").read_text(encoding="utf-8") == "L2"
    assert (tmp_path / "slogan_2.txt").read_text(encoding="utf-8") == "L3"
    assert (tmp_path / "slogan_3.txt").read_text(encoding="utf-8") == "L4"


def test_overlay_filters_include_publish_sequence_tag_in_bottom_right() -> None:
    template = render.coerce_template(
        {
            "show_hud": False,
            "show_slogan": False,
            "show_title": False,
        }
    )

    filters = render._overlay_filters(
        template,
        hud_text="",
        slogan="",
        title="",
        explicit_template_keys=set(template),
        sequence_tag="0516-2",
    )

    assert "text=0516-2" in filters
    assert ":x=w-text_w-24:y=h-text_h-30" in filters


def test_build_filter_complex_avoids_empty_filter_after_overlay_label(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    variant = _variant(source)
    settings = _settings(tmp_path)
    bg_overlay = tmp_path / "bg_overlay.png"

    monkeypatch.setattr(render, "_render_background_overlay", lambda *_args, **_kwargs: bg_overlay)
    monkeypatch.setattr(render, "_render_watermark_overlay", lambda *_args, **_kwargs: None)

    filter_complex, _inputs = render._build_filter_complex(
        variant,
        settings,
        text_dir=tmp_path / "text_layers",
        sequence_tag="0520-1",
    )

    assert "[seg0bg],drawtext=" not in filter_complex
    assert "[seg0bg]drawtext=" in filter_complex
    assert "[seg0text],format=" not in filter_complex
    assert "[seg0text]setsar=1,format=" in filter_complex


def test_render_variant_uses_publish_sequence_number_for_sequence_tag(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    captured = {}

    def fake_concat(filter_complex, _inputs, output, **_kwargs) -> None:
        captured["filter_complex"] = filter_complex
        output.write_bytes(b"mp4")

    monkeypatch.setattr(render, "concat_video", fake_concat)

    render.render_variant(
        _variant(source),
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        cover_template_config=None,
        publish_day_code="0516",
        publish_sequence_number=23,
    )

    assert "text=0516-23" in captured["filter_complex"]


def test_overlay_filters_skip_legacy_sequence_tag_when_watermark_mode_is_explicit() -> None:
    template = render.coerce_template(
        {
            "show_hud": False,
            "show_slogan": False,
            "show_title": False,
            "watermark_mode": "text",
            "watermark_text": "GasGx",
            "watermark_position": "top_left",
        }
    )

    filters = render._overlay_filters(
        template,
        hud_text="",
        slogan="",
        title="",
        explicit_template_keys=set(template),
        sequence_tag="0516-2",
    )

    assert filters == ""


def test_overlay_filters_compacts_blank_helper_items(monkeypatch) -> None:
    template = render.coerce_template(
        {
            "show_hud": True,
            "show_slogan": False,
            "show_title": False,
            "watermark_mode": "text",
        }
    )

    monkeypatch.setattr(render, "_drawbox_filter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        render,
        "_drawtext_lines",
        lambda *_args, **_kwargs: ["", "drawtext=fontfile=/tmp/font.ttf:text=GasGx", ""],
    )

    filters = render._overlay_filters(
        template,
        hud_text="GasGx",
        slogan="",
        title="",
        explicit_template_keys=set(template),
    )

    assert filters == ",drawtext=fontfile=/tmp/font.ttf:text=GasGx"
    assert ",," not in filters


def test_build_filter_complex_preserves_watermark_overlay_chain(tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    text_dir = tmp_path / "render" / "text_layers"
    template = {
        "show_hud": False,
        "show_slogan": False,
        "show_title": False,
        "watermark_mode": "text",
        "watermark_text": "GasGx",
        "watermark_position": "top_left",
    }

    filter_complex, inputs = render._build_filter_complex(
        _variant(source),
        _settings(tmp_path),
        template_config=template,
        text_dir=text_dir,
    )

    assert "template_watermark_overlay.png" not in filter_complex
    assert inputs[-1].name == "template_watermark_overlay.png"
    assert ";;" not in filter_complex
    assert ",,overlay" not in filter_complex
    assert "No such filter" not in filter_complex
    assert "[seg0base]" in filter_complex
    assert "[seg0wm]setsar=1,format=yuv420p[v0]" in filter_complex


def test_watermark_helpers_support_auto_text_and_image_overlay() -> None:
    assert sanitize_watermark_text("12345678901") == "1234567890"
    assert sanitize_watermark_text("中英\nWatermark") == "中英Watermar"
    assert resolve_watermark_text({"watermark_mode": "auto", "watermark_text": "ignored"}, "0516-01") == "0516-01"

    text_overlay = build_watermark_overlay(
        {
            "watermark_mode": "text",
            "watermark_text": "测试Watermark",
            "watermark_position": "top_left",
            "watermark_font_size": 72,
            "watermark_color": "#FF0000",
            "watermark_opacity": 0.78,
        },
        (1080, 1920),
    )
    assert text_overlay is not None
    assert text_overlay.getbbox() is not None

    image = Image.new("RGBA", (320, 240), (255, 0, 0, 255))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    image_overlay = build_watermark_overlay(
        {
            "watermark_mode": "image",
            "watermark_image_data_url": f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('ascii')}",
            "watermark_position": "bottom_right",
            "watermark_opacity": 0.6,
        },
        (1080, 1920),
    )
    assert image_overlay is not None
    bbox = image_overlay.getbbox()
    assert bbox is not None
    assert bbox[2] - bbox[0] <= 200
    assert bbox[3] - bbox[1] <= 200


def test_render_variant_builds_split_screen_filter_graph(monkeypatch, tmp_path: Path) -> None:
    source_a = tmp_path / "source-a.mp4"
    source_b = tmp_path / "source-b.mp4"
    source_a.write_bytes(b"video-a")
    source_b.write_bytes(b"video-b")
    variant = _variant(source_a)
    clip_b = ClipMetadata(
        clip_id="clip-2",
        source_path=source_b,
        normalized_path=source_b,
        category="category_A",
        duration=2,
        width=1080,
        height=1920,
        fps=30,
        brightness_score=1,
        contrast_score=1,
        tags=["category_A"],
    )
    variant.segments = [
        SegmentPlan(category="category_A", clip=variant.segments[0].clip, start_time=0, duration=1.4, index=0),
        SegmentPlan(category="category_A", clip=clip_b, start_time=0.2, duration=1.2, index=1),
    ]
    variant.split_screen_panels = [
        list(variant.segments),
        [variant.segments[1]],
    ]
    variant.split_screen_enabled = True
    variant.split_screen_mode = "fixed"
    variant.split_screen_layout = "heroDetailText"
    variant.split_screen_gap = 10
    captured = {}

    def fake_concat(filter_complex, inputs, output, bgm_path=None, speed_mode=None, threads=None, **kwargs) -> None:
        captured["filter_complex"] = filter_complex
        captured["inputs"] = inputs
        output.write_bytes(b"mp4")

    monkeypatch.setattr(render, "concat_video", fake_concat)

    asset = render.render_variant(
        variant,
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        template_config={
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
            "align": "center",
        },
        cover_intro_seconds=0,
        outro_seconds=0,
    )

    assert asset.video_path.exists()
    assert "color=c=black:s=1080x1920:d=2.600,format=rgba[base]" in captured["filter_complex"]
    assert "concat=n=2:v=1:a=0" in captured["filter_complex"]
    assert "overlay=x=0:y=0:format=auto" in captured["filter_complex"]
    assert "drawtext=" in captured["filter_complex"]
    assert "drawbox=" in captured["filter_complex"]
    assert "tpad=stop_mode=clone" in captured["filter_complex"]
    assert "format=yuv420p[vout]" in captured["filter_complex"]
    assert [path.name for path in captured["inputs"]] == ["source-a.mp4", "source-b.mp4", "source-b.mp4"]
    assert "hud_0.txt" in captured["filter_complex"]
    assert "title_0.txt" in captured["filter_complex"]

