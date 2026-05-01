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
        website_url="https://example.test",
        hud_enable_live_data=False,
        hud_fixed_formulas=[],
        slogans=[],
        titles=[],
        hud_sources={},
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

    assert "text=The World\\'s leading engine for" in captured["filter_complex"]
    assert "text='The World\\'s leading engine for'" not in captured["filter_complex"]
    assert "drawtext=fontfile=" in captured["filter_complex"]
