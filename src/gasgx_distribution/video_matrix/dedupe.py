from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Iterable

from PIL import Image

from .ffmpeg_tools import FFmpegError, resolve_binary
from .models import DedupeResult, RenderedAsset, SimilarityReport, VideoVariant


PLAN_TOTAL_REJECT = 0.88
PLAN_TOTAL_RETRY = 0.80
MAX_MUTATION_RETRIES = 3
SIMHASH_BITS = 64


def text_signature_for_variant(variant: VideoVariant) -> str:
    return simhash_text(" ".join([variant.slogan, variant.title, *variant.hud_lines]))


def structure_signature_for_variant(variant: VideoVariant) -> str:
    tokens = structure_tokens_for_variant(variant)
    payload = "|".join(tokens)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:24]


def structure_tokens_for_variant(variant: VideoVariant) -> list[str]:
    tokens = [
        f"narrative:{variant.narrative_template_id or 'default'}",
        f"structure_variant:{variant.structure_variant_id or 'v1'}",
    ]
    for segment in variant.segments:
        duration_bucket = max(1, int(round(float(segment.duration) * 2)))
        tokens.append(f"{segment.index}:{segment.category}:{segment.clip.clip_id}:{duration_bucket}")
    return tokens


def prepare_variant_dedupe_fields(variant: VideoVariant) -> None:
    variant.text_signature = text_signature_for_variant(variant)
    variant.structure_signature = structure_signature_for_variant(variant)


def plan_feature_record(
    variant: VideoVariant,
    *,
    bgm_name: str = "",
    content_fingerprint: str = "",
    first_frame_hash: str = "",
    cover_frame_hash: str = "",
    bgm_fingerprint: str = "",
) -> dict[str, Any]:
    prepare_variant_dedupe_fields(variant)
    first_segment = variant.segments[0] if variant.segments else None
    return {
        "signature": variant.signature,
        "text_signature": variant.text_signature,
        "structure_signature": variant.structure_signature,
        "structure_tokens": structure_tokens_for_variant(variant),
        "narrative_template_id": variant.narrative_template_id,
        "account_pool_id": variant.account_pool_id,
        "text_variant_id": variant.text_variant_id,
        "visual_plan_key": variant.visual_plan_key,
        "structure_variant_id": variant.structure_variant_id,
        "bgm_start_offset": round(float(variant.bgm_start_offset or 0.0), 3),
        "bgm_offset_bucket": variant.bgm_offset_bucket,
        "first_clip_id": first_segment.clip.clip_id if first_segment else "",
        "first_category": first_segment.category if first_segment else "",
        "segment_keys": [segment_key(segment) for segment in variant.segments],
        "bgm_name": bgm_name,
        "bgm_fingerprint": bgm_fingerprint,
        "content_fingerprint": content_fingerprint,
        "first_frame_hash": first_frame_hash,
        "cover_frame_hash": cover_frame_hash,
    }


def segment_key(segment: Any) -> str:
    return f"{segment.clip.clip_id}:{segment.start_time}:{segment.duration}"


def evaluate_variant_plan(
    variant: VideoVariant,
    history_features: Iterable[dict[str, Any]] | None,
    *,
    existing_signatures: set[str] | None = None,
    recent_segment_keys: set[str] | None = None,
    same_account: bool = False,
    policy: dict[str, Any] | None = None,
    bgm_name: str = "",
) -> DedupeResult:
    prepare_variant_dedupe_fields(variant)
    existing_signatures = set(existing_signatures or set())
    recent_segment_keys = set(recent_segment_keys or set())
    reject_total = _policy_float(policy, "reject_total", PLAN_TOTAL_REJECT)
    retry_total = _policy_float(policy, "borderline_total", PLAN_TOTAL_RETRY)
    candidate_features = plan_feature_record(variant, bgm_name=bgm_name)
    best_report = SimilarityReport()
    reasons: list[str] = []

    if variant.signature in existing_signatures:
        reasons.append("signature_exact")
        best_report = SimilarityReport(
            visual_score=0.0,
            audio_score=0.0,
            text_score=1.0,
            structure_score=1.0,
            total_score=1.0,
            reasons=list(reasons),
            strongest_match={"signature": variant.signature},
        )

    candidate_segment_keys = set(candidate_features["segment_keys"])
    touched_segments = candidate_segment_keys & recent_segment_keys
    if touched_segments:
        reasons.append("segment_exact")
        if best_report.total_score < 0.95:
            best_report = SimilarityReport(
                visual_score=0.72,
                audio_score=0.0,
                text_score=0.0,
                structure_score=0.95,
                total_score=0.95,
                reasons=list(reasons),
                strongest_match={"segment_keys": sorted(touched_segments)},
            )

    for history in history_features or []:
        report = _compare_plan_features(candidate_features, history, same_account=same_account)
        if report.total_score > best_report.total_score:
            best_report = report
    if reasons:
        best_report.reasons = _unique([*best_report.reasons, *reasons])

    single_dimension_reasons = _single_dimension_retry_reasons(best_report, policy)
    if single_dimension_reasons:
        best_report.reasons = _unique([*best_report.reasons, *single_dimension_reasons])
    if (
        single_dimension_reasons
        or best_report.total_score >= reject_total
        or "signature_exact" in best_report.reasons
        or "segment_exact" in best_report.reasons
    ):
        action = "retry"
        status = "retry"
    elif best_report.total_score >= retry_total:
        action = "retry"
        status = "borderline"
    else:
        action = "pass"
        status = "pass"
    return DedupeResult(status=status, action=action, report=best_report)


def _policy_float(policy: dict[str, Any] | None, key: str, fallback: float) -> float:
    if not isinstance(policy, dict):
        return fallback
    try:
        value = float(policy.get(key, fallback))
    except (TypeError, ValueError):
        return fallback
    return value if 0 <= value <= 1 else fallback


def _policy_bool(policy: dict[str, Any] | None, key: str, fallback: bool) -> bool:
    if not isinstance(policy, dict):
        return fallback
    return bool(policy.get(key, fallback))


def _single_dimension_retry_reasons(report: SimilarityReport, policy: dict[str, Any] | None) -> list[str]:
    if not _policy_bool(policy, "single_dimension_retry", True):
        return []
    reasons: list[str] = []
    if "same_hook_clip" in report.reasons:
        reasons.append("same_hook_clip")
    if report.visual_score >= _policy_float(policy, "visual_retry", 0.90):
        reasons.append("visual_near")
    if report.text_score >= _policy_float(policy, "text_retry", 0.96):
        reasons.append("text_near")
    if report.structure_score >= _policy_float(policy, "structure_retry", 0.86):
        reasons.append("structure_near")
    return _unique(reasons)


def mark_recut_passed(result: DedupeResult, retry_count: int, mutation_history: list[str]) -> DedupeResult:
    if retry_count <= 0:
        result.status = "pass"
    else:
        result.status = "recut_passed"
    result.action = "pass"
    result.retry_count = retry_count
    result.mutation_history = list(mutation_history)
    return result


def mark_manual_review(result: DedupeResult, retry_count: int, mutation_history: list[str]) -> DedupeResult:
    result.status = "manual_review"
    result.action = "review"
    result.retry_count = retry_count
    result.mutation_history = list(mutation_history)
    return result


def enrich_rendered_asset_dedupe(asset: RenderedAsset, bgm_path: Path | None = None) -> None:
    variant = asset.variant
    prepare_variant_dedupe_fields(variant)
    variant.content_fingerprint = compute_video_content_fingerprint(asset.video_path) or variant.content_fingerprint
    if asset.cover_path is not None and asset.cover_path.exists():
        cover_hash = compute_image_hash(asset.cover_path)
        variant.cover_frame_hash = cover_hash or variant.cover_frame_hash
        variant.first_frame_hash = variant.first_frame_hash or cover_hash
    if bgm_path is not None:
        variant.bgm_fingerprint = compute_file_fingerprint(bgm_path)
    if variant.dedupe_result is None:
        variant.dedupe_result = DedupeResult(status="pass", action="pass", report=SimilarityReport())
    _merge_render_features_into_report(variant)
    _write_manifest_dedupe_fields(asset, bgm_path)


def dedupe_payload_for_variant(variant: VideoVariant) -> dict[str, Any]:
    result = variant.dedupe_result.as_dict() if variant.dedupe_result is not None else DedupeResult().as_dict()
    return {
        **result,
        "narrative_template_id": variant.narrative_template_id,
        "account_pool_id": variant.account_pool_id,
        "text_signature": variant.text_signature,
        "structure_signature": variant.structure_signature,
        "first_frame_hash": variant.first_frame_hash,
        "cover_frame_hash": variant.cover_frame_hash,
        "content_fingerprint": _fingerprint_digest(variant.content_fingerprint),
        "bgm_fingerprint": _fingerprint_digest(variant.bgm_fingerprint),
        "visual_plan_key": variant.visual_plan_key,
        "text_variant_id": variant.text_variant_id,
        "structure_variant_id": variant.structure_variant_id,
        "bgm_start_offset": round(float(variant.bgm_start_offset or 0.0), 3),
        "bgm_offset_bucket": variant.bgm_offset_bucket,
    }


def compute_video_content_fingerprint(video_path: Path) -> str:
    width = 12
    height = 12
    frame_size = width * height
    try:
        ffmpeg = resolve_binary("ffmpeg")
    except FFmpegError:
        return ""
    cmd = [
        ffmpeg,
        "-v",
        "error",
        "-i",
        str(video_path),
        "-t",
        "24",
        "-vf",
        f"fps=1/2,scale={width}:{height}:flags=bilinear,format=gray",
        "-an",
        "-sn",
        "-dn",
        "-f",
        "rawvideo",
        "-",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, check=False)
    except Exception:
        return ""
    if result.returncode != 0:
        return ""
    data = result.stdout or b""
    if len(data) < frame_size * 2:
        return ""
    frame_count = len(data) // frame_size
    votes = [0] * (height * (width - 1))
    for i in range(frame_count):
        frame = data[i * frame_size : (i + 1) * frame_size]
        vote_index = 0
        for y in range(height):
            row = y * width
            for x in range(width - 1):
                if frame[row + x] > frame[row + x + 1]:
                    votes[vote_index] += 1
                vote_index += 1
    threshold = frame_count / 2.0
    bits = "".join("1" if vote >= threshold else "0" for vote in votes)
    digest = hashlib.sha1(bits.encode("ascii")).hexdigest()[:24]
    return f"{len(bits)}:{digest}:{bits}"


def compute_image_hash(image_path: Path) -> str:
    try:
        image = Image.open(image_path).convert("L").resize((8, 8), Image.Resampling.LANCZOS)
    except Exception:
        return ""
    pixels = list(image.getdata())
    average = sum(pixels) / max(1, len(pixels))
    bits = "".join("1" if pixel >= average else "0" for pixel in pixels)
    digest = hashlib.sha1(bits.encode("ascii")).hexdigest()[:16]
    return f"{len(bits)}:{digest}:{bits}"


def compute_file_fingerprint(path: Path) -> str:
    try:
        stat = path.stat()
        hasher = hashlib.sha1()
        hasher.update(path.name.encode("utf-8", errors="ignore"))
        hasher.update(str(stat.st_size).encode("ascii"))
        with path.open("rb") as handle:
            hasher.update(handle.read(1024 * 1024))
        return f"{path.name}:{stat.st_size}:{hasher.hexdigest()[:24]}"
    except Exception:
        return ""


def simhash_text(text: str, bits: int = SIMHASH_BITS) -> str:
    tokens = _text_tokens(text)
    if not tokens:
        return f"{bits}:{'0' * 16}:{'0' * bits}"
    weights = [0] * bits
    for token in tokens:
        digest = int(hashlib.sha1(token.encode("utf-8")).hexdigest(), 16)
        for index in range(bits):
            weights[index] += 1 if digest & (1 << index) else -1
    bit_string = "".join("1" if weight >= 0 else "0" for weight in weights)
    digest = hashlib.sha1(bit_string.encode("ascii")).hexdigest()[:16]
    return f"{bits}:{digest}:{bit_string}"


def hamming_similarity(fingerprint_a: str, fingerprint_b: str) -> float:
    bits_a = _fingerprint_bits(fingerprint_a)
    bits_b = _fingerprint_bits(fingerprint_b)
    if not bits_a or not bits_b:
        return 0.0
    size = max(len(bits_a), len(bits_b), 1)
    distance = _hamming_distance(bits_a, bits_b)
    return round(max(0.0, 1.0 - distance / size), 4)


def _compare_plan_features(candidate: dict[str, Any], history: dict[str, Any], *, same_account: bool) -> SimilarityReport:
    reasons: list[str] = []
    visual_score = 0.0
    audio_score = 0.0
    text_score = hamming_similarity(str(candidate.get("text_signature") or ""), str(history.get("text_signature") or ""))
    structure_score = _structure_similarity(candidate.get("structure_tokens") or [], history.get("structure_tokens") or [])

    if candidate.get("signature") and candidate.get("signature") == history.get("signature"):
        reasons.append("signature_exact")
        text_score = max(text_score, 1.0)
        structure_score = max(structure_score, 1.0)
    candidate_visual_key = str(candidate.get("visual_plan_key") or "").strip()
    history_visual_key = str(history.get("visual_plan_key") or "").strip()
    same_visual_key = bool(candidate_visual_key and history_visual_key and candidate_visual_key == history_visual_key)
    if same_visual_key:
        reasons.append("visual_plan_key_reuse")
        visual_score = max(visual_score, 0.90)
    if candidate.get("first_clip_id") and candidate.get("first_clip_id") == history.get("first_clip_id"):
        if same_visual_key or not (candidate_visual_key and history_visual_key):
            reasons.append("same_hook_clip")
            visual_score = max(visual_score, 0.92)
        else:
            reasons.append("hook_offset_shifted")
            visual_score = max(visual_score, 0.64)
    if candidate.get("first_frame_hash") and history.get("first_frame_hash"):
        visual_score = max(visual_score, hamming_similarity(str(candidate["first_frame_hash"]), str(history["first_frame_hash"])))
    if candidate.get("cover_frame_hash") and history.get("cover_frame_hash"):
        visual_score = max(visual_score, hamming_similarity(str(candidate["cover_frame_hash"]), str(history["cover_frame_hash"])))
    if candidate.get("content_fingerprint") and history.get("content_fingerprint"):
        visual_score = max(visual_score, hamming_similarity(str(candidate["content_fingerprint"]), str(history["content_fingerprint"])))
    if candidate.get("structure_variant_id") and candidate.get("structure_variant_id") == history.get("structure_variant_id"):
        reasons.append("same_structure_variant")
        structure_score = max(structure_score, 0.80)
    same_bgm_name = bool(candidate.get("bgm_name") and candidate.get("bgm_name") == history.get("bgm_name"))
    same_bgm_offset_bucket = bool(candidate.get("bgm_offset_bucket") and candidate.get("bgm_offset_bucket") == history.get("bgm_offset_bucket"))
    if same_bgm_name:
        if same_bgm_offset_bucket:
            audio_score = max(audio_score, 0.78)
            reasons.append("same_bgm")
        else:
            audio_score = max(audio_score, 0.25)
            reasons.append("same_bgm_offset_shifted")
    if candidate.get("bgm_fingerprint") and history.get("bgm_fingerprint") and not (same_bgm_name and not same_bgm_offset_bucket):
        audio_score = max(audio_score, hamming_similarity(str(candidate["bgm_fingerprint"]), str(history["bgm_fingerprint"])))
    if visual_score >= 0.90:
        reasons.append("visual_near")
    if text_score >= 0.96:
        reasons.append("text_near")
    if structure_score >= 0.86:
        reasons.append("structure_near")

    total = 0.30 * visual_score + 0.15 * audio_score + 0.25 * text_score + 0.30 * structure_score
    if same_account:
        total += 0.04
    return SimilarityReport(
        visual_score=round(visual_score, 4),
        audio_score=round(audio_score, 4),
        text_score=round(text_score, 4),
        structure_score=round(structure_score, 4),
        total_score=round(min(1.0, total), 4),
        reasons=_unique(reasons),
        strongest_match={
            "signature": history.get("signature", ""),
            "narrative_template_id": history.get("narrative_template_id", ""),
            "first_clip_id": history.get("first_clip_id", ""),
        },
    )


def _structure_similarity(candidate_tokens: list[str], history_tokens: list[str]) -> float:
    candidate = {str(item) for item in candidate_tokens if str(item)}
    history = {str(item) for item in history_tokens if str(item)}
    if not candidate or not history:
        return 0.0
    overlap = len(candidate & history)
    union = len(candidate | history)
    return round(overlap / max(1, union), 4)


def _merge_render_features_into_report(variant: VideoVariant) -> None:
    if variant.dedupe_result is None:
        return
    report = variant.dedupe_result.report
    if not report.reasons and variant.dedupe_result.status in {"pass", "recut_passed"}:
        report.reasons = ["low_cost_dedupe_passed"]


def _write_manifest_dedupe_fields(asset: RenderedAsset, bgm_path: Path | None) -> None:
    if asset.manifest_path is None or not asset.manifest_path.exists():
        return
    try:
        payload = json.loads(asset.manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    payload["narrative_template_id"] = asset.variant.narrative_template_id
    payload["account_pool_id"] = asset.variant.account_pool_id
    payload["cover_frame_offset"] = asset.variant.cover_frame_offset
    payload["bgm_name"] = bgm_path.name if bgm_path is not None else ""
    payload["bgm_start_offset"] = round(float(asset.variant.bgm_start_offset or 0.0), 3)
    payload["bgm_offset_bucket"] = asset.variant.bgm_offset_bucket
    payload["dedupe"] = dedupe_payload_for_variant(asset.variant)
    try:
        asset.manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return


def _text_tokens(text: str) -> list[str]:
    normalized = str(text or "").lower()
    ascii_words = re.findall(r"[a-z0-9]+", normalized)
    cjk_chars = re.findall(r"[\u4e00-\u9fff]", normalized)
    return ascii_words + cjk_chars


def _fingerprint_bits(fingerprint: str) -> str:
    parts = str(fingerprint or "").split(":")
    if len(parts) >= 3 and set(parts[-1]) <= {"0", "1"}:
        return parts[-1]
    return ""


def _fingerprint_digest(fingerprint: str) -> str:
    parts = str(fingerprint or "").split(":")
    if len(parts) >= 3:
        return parts[1] if set(parts[-1]) <= {"0", "1"} else parts[-1]
    if len(parts) >= 2:
        return parts[1]
    return str(fingerprint or "")


def _hamming_distance(bits_a: str, bits_b: str) -> int:
    size = min(len(bits_a), len(bits_b))
    distance = sum(1 for index in range(size) if bits_a[index] != bits_b[index])
    return distance + abs(len(bits_a) - len(bits_b))


def _unique(items: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        token = str(item or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result
