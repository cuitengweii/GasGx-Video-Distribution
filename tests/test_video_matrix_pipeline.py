from __future__ import annotations

from pathlib import Path

import pytest

from gasgx_distribution.video_matrix import pipeline
from gasgx_distribution.video_matrix.models import ClipMetadata, RenderedAsset, SegmentPlan, VideoVariant
from gasgx_distribution.video_matrix.settings import ProjectSettings


def _settings(output_root: Path) -> ProjectSettings:
    return ProjectSettings(
        project_name="test",
        source_root=output_root / "source",
        library_root=output_root / "library",
        output_root=output_root,
        output_count=2,
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


def _variant(source: Path, sequence_number: int) -> VideoVariant:
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


def test_run_pipeline_keeps_successful_renders_when_one_variant_fails(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    settings = _settings(tmp_path)
    (tmp_path / "config" / "video_matrix").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "video_matrix" / "copy_template.txt").write_text("{title}", encoding="utf-8")
    variants = [_variant(source, 1), _variant(source, 2)]
    completed: list[int] = []

    def fake_ingest_sources(*_args, **_kwargs):
        return [variants[0].segments[0].clip]

    def fake_build_hud_payload(_settings):
        return {"hud": True}

    def fake_detect_beat_grid(*_args, **_kwargs):
        return [0.1, 0.2]

    def fake_plan_variants(*_args, **_kwargs):
        return variants

    def fake_render_variant(variant, *_args, **_kwargs):
        if variant.sequence_number == 1:
            return RenderedAsset(variant, tmp_path / "v1.mp4", None, None, None)
        raise RuntimeError("boom")

    class FakePaths:
        runtime_root = tmp_path / "runtime"

    monkeypatch.setattr(pipeline, "get_paths", lambda: FakePaths())
    monkeypatch.setattr(pipeline, "ingest_sources", fake_ingest_sources)
    monkeypatch.setattr(pipeline, "build_hud_payload", fake_build_hud_payload)
    monkeypatch.setattr(pipeline, "detect_beat_grid", fake_detect_beat_grid)
    monkeypatch.setattr(pipeline, "plan_variants", fake_plan_variants)
    monkeypatch.setattr(pipeline, "render_variant", fake_render_variant)

    assets = pipeline.run_pipeline(
        settings,
        bgm_path=tmp_path / "bgm.mp3",
        output_root=tmp_path,
        max_workers=2,
        asset_callback=lambda asset, *_rest: completed.append(asset.variant.sequence_number),
    )

    assert [asset.variant.sequence_number for asset in assets] == [1]
    assert completed == [1]


def test_run_pipeline_uses_conservative_default_worker_count(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    settings = _settings(tmp_path)
    (tmp_path / "config" / "video_matrix").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "video_matrix" / "copy_template.txt").write_text("{title}", encoding="utf-8")
    variants = [_variant(source, 1), _variant(source, 2), _variant(source, 3), _variant(source, 4)]
    captured: dict[str, object] = {}

    def fake_ingest_sources(*_args, **_kwargs):
        return [variants[0].segments[0].clip]

    def fake_build_hud_payload(_settings):
        return {"hud": True}

    def fake_detect_beat_grid(*_args, **_kwargs):
        return [0.1, 0.2]

    def fake_plan_variants(*_args, **_kwargs):
        return variants

    def fake_render_variant(variant, *_args, **_kwargs):
        captured["ffmpeg_threads"] = _args[-1] if _args else _kwargs.get("ffmpeg_threads")
        return RenderedAsset(variant, tmp_path / f"v{variant.sequence_number}.mp4", None, None, None)

    class FakePaths:
        runtime_root = tmp_path / "runtime"

    monkeypatch.setattr(pipeline, "get_paths", lambda: FakePaths())
    monkeypatch.setattr(pipeline, "ingest_sources", fake_ingest_sources)
    monkeypatch.setattr(pipeline, "build_hud_payload", fake_build_hud_payload)
    monkeypatch.setattr(pipeline, "detect_beat_grid", fake_detect_beat_grid)
    monkeypatch.setattr(pipeline, "plan_variants", fake_plan_variants)
    monkeypatch.setattr(pipeline, "render_variant", fake_render_variant)
    monkeypatch.setattr(pipeline.os, "cpu_count", lambda: 4)

    assets = pipeline.run_pipeline(
        settings,
        bgm_path=tmp_path / "bgm.mp3",
        output_root=tmp_path,
        asset_callback=lambda *_args, **_kwargs: None,
    )

    assert [asset.variant.sequence_number for asset in assets] == [1, 2, 3, 4]
    assert captured["ffmpeg_threads"] == 2
