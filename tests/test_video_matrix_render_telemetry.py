from __future__ import annotations

from pathlib import Path

from gasgx_distribution.video_matrix import render
from gasgx_distribution.video_matrix.models import ClipMetadata, SegmentPlan, VideoVariant
from gasgx_distribution.video_matrix.settings import ProjectSettings
from gasgx_distribution.video_matrix.telemetry import GenerationTrace


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


def test_render_variant_records_telemetry_events(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    trace = GenerationTrace("render-job", tmp_path / "logs")

    def fake_concat(_filter_complex, _inputs, output, bgm_path=None) -> None:
        output.write_bytes(b"mp4")

    monkeypatch.setattr(render, "concat_video", fake_concat)

    render.render_variant(
        _variant(source),
        _settings(tmp_path),
        template_copy="",
        batch_dir=tmp_path,
        cover_template_config=None,
        template_config={"show_hud": True, "show_slogan": False, "show_title": True},
        telemetry=trace.variant(1),
    )

    trace.finish("complete")
    text = (tmp_path / "logs" / "generation_events.jsonl").read_text(encoding="utf-8")
    assert '"name": "filter_build"' in text
    assert '"name": "ffmpeg_concat"' in text
    assert '"drawtext_count": 2' in text
    assert '"show_slogan": false' in text
