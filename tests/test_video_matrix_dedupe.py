from __future__ import annotations

import random
from pathlib import Path

from PIL import Image

from gasgx_distribution.video_matrix.composition import _mutate_candidate
from gasgx_distribution.video_matrix.dedupe import (
    compute_file_fingerprint,
    compute_image_hash,
    dedupe_payload_for_variant,
    evaluate_variant_plan,
    hamming_similarity,
    plan_feature_record,
    simhash_text,
)
from gasgx_distribution.video_matrix.ingestion import _business_tags_for_source
from gasgx_distribution.video_matrix.models import ClipMetadata, SegmentPlan, VideoVariant


def _clip(category: str, clip_id: str, duration: float = 6.0) -> ClipMetadata:
    return ClipMetadata(
        clip_id=clip_id,
        source_path=Path(f"{clip_id}.mp4"),
        normalized_path=Path(f"{clip_id}.mp4"),
        category=category,
        duration=duration,
        width=1080,
        height=1920,
        fps=30,
        brightness_score=1,
        contrast_score=1,
        tags=[category],
    )


def _variant() -> VideoVariant:
    clips = [_clip("category_A", "a1"), _clip("category_B", "b1"), _clip("category_C", "c1")]
    segments = [
        SegmentPlan(category=clip.category, clip=clip, start_time=0.0, duration=1.0, index=index)
        for index, clip in enumerate(clips)
    ]
    return VideoVariant(
        sequence_number=1,
        title="GasGx onsite power",
        slogan="Stop flaring start hashing",
        hud_lines=["field gas to stable load"],
        lut_strength=1,
        zoom=1.04,
        mirror=False,
        x_offset=0,
        y_offset=0,
        segments=segments,
        signature="sig-1",
        narrative_template_id="quick_showcase",
        account_pool_id="result_showcase",
    )


def test_low_cost_hashes_are_stable(tmp_path: Path) -> None:
    assert hamming_similarity(simhash_text("燃气发电 现场负载"), simhash_text("燃气发电 现场负载")) == 1.0

    image_path = tmp_path / "cover.png"
    Image.new("RGB", (16, 16), (120, 160, 80)).save(image_path)
    assert hamming_similarity(compute_image_hash(image_path), compute_image_hash(image_path)) == 1.0

    bgm = tmp_path / "bgm.mp3"
    bgm.write_bytes(b"same bgm bytes")
    file_fingerprint = compute_file_fingerprint(bgm)
    assert file_fingerprint.startswith("bgm.mp3:")
    variant = _variant()
    variant.bgm_fingerprint = file_fingerprint
    assert dedupe_payload_for_variant(variant)["bgm_fingerprint"] == file_fingerprint.rsplit(":", 1)[-1]


def test_evaluate_variant_plan_flags_signature_and_structure_reuse() -> None:
    variant = _variant()
    history = [plan_feature_record(variant)]

    result = evaluate_variant_plan(variant, history, existing_signatures={"sig-1"})

    assert result.action == "retry"
    assert result.status == "retry"
    assert "signature_exact" in result.report.reasons
    assert result.report.structure_score == 1.0


def test_evaluate_variant_plan_uses_policy_thresholds() -> None:
    variant = _variant()
    history_feature = plan_feature_record(variant)
    history_feature["signature"] = "previous-signature"

    default_result = evaluate_variant_plan(variant, [history_feature])
    relaxed_result = evaluate_variant_plan(
        variant,
        [history_feature],
        policy={"borderline_total": 0.99, "reject_total": 1.0, "single_dimension_retry": False},
    )

    assert default_result.action == "retry"
    assert relaxed_result.action == "pass"


def test_evaluate_variant_plan_retries_single_dimension_high_risk() -> None:
    variant = _variant()
    history_feature = plan_feature_record(variant)
    history_feature["signature"] = "previous-signature"
    history_feature["structure_tokens"] = ["narrative:other", "0:category_Z:z9:1"]

    result = evaluate_variant_plan(
        variant,
        [history_feature],
        policy={"borderline_total": 0.99, "reject_total": 1.0},
    )

    assert result.action == "retry"
    assert "same_hook_clip" in result.report.reasons


def test_bgm_offset_bucket_reduces_same_file_audio_risk() -> None:
    variant = _variant()
    variant.bgm_start_offset = 18.0
    variant.bgm_offset_bucket = "b2"
    shifted_history = plan_feature_record(variant, bgm_name="track.mp3")
    shifted_history["signature"] = "previous-signature"
    shifted_history["bgm_offset_bucket"] = "b5"
    same_bucket_history = dict(shifted_history)
    same_bucket_history["bgm_offset_bucket"] = "b2"

    shifted = evaluate_variant_plan(
        variant,
        [shifted_history],
        policy={"single_dimension_retry": False, "borderline_total": 0.99, "reject_total": 1.0},
        bgm_name="track.mp3",
    )
    same_bucket = evaluate_variant_plan(
        variant,
        [same_bucket_history],
        policy={"single_dimension_retry": False, "borderline_total": 0.99, "reject_total": 1.0},
        bgm_name="track.mp3",
    )

    assert shifted.report.audio_score == 0.25
    assert same_bucket.report.audio_score == 0.78
    assert "same_bgm_offset_shifted" in shifted.report.reasons
    assert "same_bgm" in same_bucket.report.reasons


def test_recut_mutation_order_is_predictable() -> None:
    candidate = _variant()
    buckets = {
        "category_A": [_clip("category_A", "a1"), _clip("category_A", "a2")],
        "category_B": [_clip("category_B", "b1")],
        "category_C": [_clip("category_C", "c1")],
    }
    rng = random.Random(7)

    labels = [
        _mutate_candidate(candidate, buckets, rng, step, set(), 10.0)
        for step in range(1, 6)
    ]

    assert labels == ["replace_hook", "shuffle_order", "rewrite_opening_text", "adjust_visual_variant", "change_cover_frame"]


def test_business_sidecar_tags_are_parsed(tmp_path: Path) -> None:
    source = tmp_path / "inspect_clip.mp4"
    source.write_bytes(b"video")
    source.with_suffix(".json").write_text(
        """{
          "scene_tag": "field_site",
          "subject_tags": ["engine", "generator"],
          "action_tags": ["inspection"],
          "shot_size": "wide",
          "camera_angle": "eye_level",
          "camera_move": "pan",
          "audio_state": "clean_voice",
          "usable_range": [1.5, 4.5],
          "account_fit_tags": ["tutorial"],
          "hero_frame_candidates": [1.8, 3.2]
        }""",
        encoding="utf-8",
    )

    tags = _business_tags_for_source(source, "category_A", 5.0)

    assert tags["scene_tag"] == "field_site"
    assert tags["subject_tag"] == ["engine", "generator"]
    assert tags["action_tag"] == ["inspection"]
    assert tags["usable_range"] == (1.5, 4.5)
    assert tags["account_fit_tags"] == ["tutorial"]
    assert tags["hero_frame_candidates"] == [1.8, 3.2]
