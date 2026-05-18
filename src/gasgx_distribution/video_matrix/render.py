from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

from .ffmpeg_tools import FFmpegError, append_video_tail, concat_video, extract_frame
from .font_config import build_font_candidates_for_family
from .cover import render_intro_cover, render_outro_cover
from .models import RenderedAsset, VideoVariant
from .settings import ProjectSettings
from .spark_text import build_marketing_copy, clean_generated_text, sanitize_headline_text
from .templates import coerce_template
ENDING_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}
CJK_FONT_FAMILY = "'Microsoft YaHei Bold', 'Microsoft YaHei', SimHei, SimSun, 'Noto Sans SC', 'Source Han Sans SC Heavy', 'HarmonyOS Sans SC Bold', sans-serif"
CJK_CHAR_PATTERN = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uac00-\ud7af]")


class VideoMatrixRenderError(RuntimeError):
    def __init__(self, message: str, *, error_code: str, user_message: str) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.user_message = user_message


def render_variant(
    variant: VideoVariant,
    settings: ProjectSettings,
    template_copy: str,
    batch_dir: Path,
    filename_prefix: str = "",
    bgm_path: Path | None = None,
    output_types: set[str] | None = None,
    copy_language: str = "zh",
    template_config: dict | None = None,
    cover_template_id: str | None = None,
    cover_template_config: dict | None = None,
    ending_cover_template_config: dict | None = None,
    cover_intro_seconds: float = 1.0,
    outro_text: str = "",
    outro_seconds: float = 1.0,
    ending_template_path: Path | None = None,
    telemetry: Any | None = None,
    speed_mode: str = "quality",
    ffmpeg_threads: int | None = None,
    ai_prompt_hint: str = "",
    bgm_start_offset: float = 0.0,
    mining_bgm_path: Path | None = None,
    mining_bgm_volume: float | None = None,
    library_bgm_volume: float | None = None,
    publish_day_code: str | None = None,
    publish_sequence_number: int | None = None,
) -> RenderedAsset:
    batch_dir.mkdir(parents=True, exist_ok=True)
    output_types = output_types or {"mp4"}
    base_name = f"{filename_prefix}vibe_{variant.sequence_number:02d}"
    video_path = batch_dir / f"{base_name}.mp4"
    main_video_path = batch_dir / f".{base_name}_main.mp4"
    scratch_dir = batch_dir / ".render_tmp" / base_name
    cover_frame = scratch_dir / f"{base_name}_raw_cover.png"
    intro_frame = scratch_dir / f"{base_name}_intro_frame.png"
    intro_cover = scratch_dir / f"{base_name}_intro_cover.png"
    outro_frame = scratch_dir / f"{base_name}_outro_frame.png"
    outro_cover = scratch_dir / f"{base_name}_outro_cover.png"
    cover_path = batch_dir / f"{base_name}_cover.png" if "png" in output_types else None
    copy_path = batch_dir / f"{base_name}_copy.txt" if "txt" in output_types else None
    manifest_path = batch_dir / f"{base_name}_manifest.json" if "json" in output_types else None
    if bgm_start_offset:
        variant.bgm_start_offset = max(0.0, float(bgm_start_offset or 0.0))
        variant.bgm_offset_bucket = variant.bgm_offset_bucket or f"b{int(variant.bgm_start_offset // 8)}"

    day_code = _resolve_publish_day_code(publish_day_code)
    display_sequence_number = _resolve_publish_sequence_number(publish_sequence_number, variant.sequence_number)
    sequence_tag = f"{day_code}-{display_sequence_number}"
    resolved_outro_text = str(outro_text or getattr(variant, "ending_follow_text", "") or "").strip()

    try:
        if telemetry is not None:
            telemetry.event(
                "render",
                "variant_started",
                {
                    "base_name": base_name,
                    "segment_count": len(variant.segments),
                    "target_duration": sum(segment.duration for segment in variant.segments)
                    + max(0.0, cover_intro_seconds)
                    + (max(0.0, outro_seconds) if not _is_video_ending(ending_template_path) else 0.0),
                    "output_types": sorted(output_types),
                },
            )
        intro_cover_path = None
        if cover_template_config is not None and cover_intro_seconds > 0:
            first_segment = variant.segments[0]
            intro_timestamp = _segment_cover_timestamp(first_segment, variant.cover_frame_offset)
            with _span(telemetry, "render", "intro_extract_frame", {"source": first_segment.clip.normalized_path, "timestamp": intro_timestamp}):
                _extract_frame_or_fallback(first_segment.clip.normalized_path, intro_frame, timestamp=intro_timestamp)
            with _span(telemetry, "render", "intro_cover_render", {"cover_template": cover_template_config.get("name", "") if isinstance(cover_template_config, dict) else ""}):
                render_intro_cover(intro_frame, intro_cover, variant, settings, cover_template_config)
            intro_cover_path = intro_cover
        outro_cover_path = None
        if ending_template_path is None and ending_cover_template_config is not None and resolved_outro_text and outro_seconds > 0:
            last_segment = variant.segments[-1]
            timestamp = last_segment.start_time + max(0.0, last_segment.duration - 0.2)
            with _span(telemetry, "render", "outro_extract_frame", {"source": last_segment.clip.normalized_path, "timestamp": timestamp}):
                _extract_frame_or_fallback(last_segment.clip.normalized_path, outro_frame, timestamp=timestamp)
            with _span(telemetry, "render", "outro_cover_render", {"ending_template": ending_cover_template_config.get("name", "") if isinstance(ending_cover_template_config, dict) else ""}):
                render_outro_cover(outro_frame, outro_cover, settings, ending_cover_template_config, resolved_outro_text, variant.hud_lines)
            outro_cover_path = outro_cover

        video_ending_path = ending_template_path if _is_video_ending(ending_template_path) else None
        inline_ending_path = None if video_ending_path is not None else ending_template_path
        with _span(telemetry, "render", "filter_build", _overlay_complexity(template_config, variant)):
            filter_complex, inputs = _build_filter_complex(
                variant,
                settings,
                template_config=template_config,
                intro_cover_path=intro_cover_path,
                cover_intro_seconds=cover_intro_seconds,
                outro_cover_path=outro_cover_path,
                outro_seconds=outro_seconds,
                ending_template_path=inline_ending_path,
                text_dir=scratch_dir / "text_layers",
                speed_mode=speed_mode,
                sequence_tag=sequence_tag,
            )
        if telemetry is not None:
            telemetry.event(
                "render",
                "ffmpeg_filter_ready",
                {
                    "input_count": len(inputs),
                    "filter_hash": hashlib.sha1(filter_complex.encode("utf-8", errors="replace")).hexdigest()[:16],
                    "filter_preview": filter_complex[:800],
                    **_overlay_complexity(template_config, variant),
                },
            )
        with _span(
            telemetry,
            "render",
            "ffmpeg_concat",
            {
                "input_count": len(inputs),
                "bgm_path": bgm_path,
                "output_path": video_path,
                "segment_duration": sum(segment.duration for segment in variant.segments),
            },
        ):
            body_output_path = main_video_path if video_ending_path is not None else video_path
            concat_kwargs = {
                "bgm_path": bgm_path,
                "speed_mode": speed_mode,
                "threads": ffmpeg_threads,
            }
            if bgm_start_offset:
                concat_kwargs["bgm_start_offset"] = max(0.0, float(bgm_start_offset or 0.0))
            if mining_bgm_path is not None:
                concat_kwargs["mining_bgm_path"] = mining_bgm_path
            if mining_bgm_volume is not None:
                concat_kwargs["mining_bgm_volume"] = float(mining_bgm_volume)
            if library_bgm_volume is not None:
                concat_kwargs["library_bgm_volume"] = float(library_bgm_volume)
            concat_video(filter_complex, inputs, body_output_path, **concat_kwargs)
        if video_ending_path is not None:
            with _span(
                telemetry,
                "render",
                "ffmpeg_append_video_ending",
                {
                    "main_video_path": main_video_path,
                    "ending_template_path": video_ending_path,
                    "output_path": video_path,
                },
            ):
                append_video_tail(
                    main_video_path,
                    video_ending_path,
                    video_path,
                    settings.target_width,
                    settings.target_height,
                    settings.target_fps,
                    threads=ffmpeg_threads,
                )
        if telemetry is not None:
            telemetry.event("render", "video_output_ready", {"video_path": video_path, "video_bytes": _file_size(video_path)})
        if cover_path is not None:
            if intro_cover_path is not None:
                with _span(telemetry, "render", "cover_copy", {"cover_path": cover_path}):
                    shutil.copyfile(intro_cover_path, cover_path)
            else:
                with _span(telemetry, "render", "cover_extract_frame", {"video_path": video_path, "timestamp": 1.0}):
                    extract_frame(video_path, cover_frame, timestamp=1.0)
                with _span(telemetry, "render", "cover_decorate", {"cover_path": cover_path}):
                    _decorate_cover(cover_frame, cover_path, variant.title)

        if copy_path is not None:
            with _span(telemetry, "render", "copy_write", {"copy_path": copy_path, "copy_language": copy_language}):
                publish_description = str(getattr(variant, "publish_description", "") or "").strip()
                if not publish_description:
                    publish_description = build_marketing_copy(
                        variant,
                        settings,
                        copy_language,
                        template_copy,
                        resolved_outro_text,
                        extra_prompt=ai_prompt_hint,
                    )
                publish_description = clean_generated_text(publish_description)
                copy_path.write_text(
                    publish_description,
                    encoding="utf-8",
                )

        if manifest_path is not None:
            with _span(telemetry, "render", "manifest_write", {"manifest_path": manifest_path}):
                manifest_path.write_text(
                    json.dumps(
                        {
                            "sequence_number": variant.sequence_number,
                            "title": variant.title,
                            "slogan": variant.slogan,
                            "signature": variant.signature,
                            "narrative_template_id": variant.narrative_template_id,
                            "account_pool_id": variant.account_pool_id,
                            "cover_frame_offset": variant.cover_frame_offset,
                            "bgm_start_offset": variant.bgm_start_offset,
                            "bgm_offset_bucket": variant.bgm_offset_bucket,
                            "video_path": str(video_path),
                            "cover_path": str(cover_path) if cover_path else None,
                            "cover_template_id": cover_template_id,
                            "cover_intro_seconds": cover_intro_seconds if intro_cover_path is not None else 0,
                            "outro_text": resolved_outro_text,
                            "outro_seconds": outro_seconds if outro_cover_path is not None else 0,
                            "publish_description": str(getattr(variant, "publish_description", "") or ""),
                            "copy_path": str(copy_path) if copy_path else None,
                            "copy_language": copy_language,
                            "publish_sequence_tag": sequence_tag,
                            "hud_lines": variant.hud_lines,
                            "segments": [
                                {
                                    "clip_id": segment.clip.clip_id,
                                    "category": segment.category,
                                    "source_path": str(segment.clip.source_path),
                                    "normalized_path": str(segment.clip.normalized_path),
                                    "start_time": segment.start_time,
                                    "duration": segment.duration,
                                }
                                for segment in variant.segments
                            ],
                        },
                        indent=2,
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
        return RenderedAsset(variant, video_path, cover_path, copy_path, manifest_path)
    except FFmpegError as exc:
            raise VideoMatrixRenderError(
                str(exc),
                error_code="ffmpeg_failed",
                user_message="请确认已安装 FFmpeg 并加入 PATH",
            ) from exc
    finally:
        with _span(telemetry, "render", "cleanup", {"scratch_dir": scratch_dir}):
            for temp_path in (cover_frame, intro_frame, intro_cover, outro_frame, outro_cover, main_video_path):
                temp_path.unlink(missing_ok=True)
            if scratch_dir.exists():
                shutil.rmtree(scratch_dir, ignore_errors=True)
            try:
                scratch_dir.parent.rmdir()
            except OSError:
                pass


def _extract_frame_or_fallback(video_path: Path, output_path: Path, timestamp: float) -> None:
    extract_frame(video_path, output_path, timestamp=timestamp)
    if output_path.exists():
        return
    extract_frame(video_path, output_path, timestamp=0.0)
    if not output_path.exists():
        raise FileNotFoundError(f"Unable to extract frame from {video_path}")


def _segment_cover_timestamp(segment: Any, offset: float) -> float:
    safe_offset = max(0.0, min(float(offset or 0.0), max(0.0, float(segment.duration) - 0.05)))
    return round(float(segment.start_time) + safe_offset, 3)


def _build_filter_complex(
    variant: VideoVariant,
    settings: ProjectSettings,
    template_config: dict | None = None,
    intro_cover_path: Path | None = None,
    cover_intro_seconds: float = 1.0,
    outro_cover_path: Path | None = None,
    outro_seconds: float = 1.0,
    ending_template_path: Path | None = None,
    text_dir: Path | None = None,
    speed_mode: str = "quality",
    sequence_tag: str = "",
) -> tuple[str, list[Path]]:
    inputs = [segment.clip.normalized_path for segment in variant.segments]
    chains: list[str] = []
    labels: list[str] = []
    input_offset = 0
    if intro_cover_path is not None and cover_intro_seconds > 0:
        inputs = [intro_cover_path, *inputs]
        input_offset = 1
        intro_frames = max(1, int(settings.target_fps * cover_intro_seconds))
        chains.append(
            f"[0:v]loop=loop={intro_frames}:size=1:start=0,"
            f"fps={settings.target_fps},scale={settings.target_width}:{settings.target_height},"
            f"trim=duration={cover_intro_seconds:.3f},setpts=PTS-STARTPTS,setsar=1,format=yuv420p[intro]"
        )
        labels.append("[intro]")
    template = coerce_template(template_config)
    explicit_template_keys = set((template_config or {}).keys())
    hud_text = " | ".join(variant.hud_lines)
    slogan = sanitize_headline_text(variant.slogan)
    title = sanitize_headline_text(variant.title)
    variant.slogan = slogan
    variant.title = title
    background_overlay_index: int | None = None
    background_overlay_path = _render_background_overlay(template, text_dir.parent / "template_background_overlay.png" if text_dir else None)
    if background_overlay_path is not None:
        inputs = [*inputs, background_overlay_path]
        background_overlay_index = len(inputs) - 1
    for idx, segment in enumerate(variant.segments):
        crop_x = max(0, (settings.target_width * variant.zoom - settings.target_width) / 2 + variant.x_offset)
        crop_y = max(0, (settings.target_height * variant.zoom - settings.target_height) / 2 + variant.y_offset)
        base_chain = (
            f"[{idx + input_offset}:v]"
            f"trim=start={segment.start_time}:duration={segment.duration},setpts=PTS-STARTPTS,"
            f"scale={int(settings.target_width * variant.zoom)}:{int(settings.target_height * variant.zoom)},"
            f"crop={settings.target_width}:{settings.target_height}:{int(crop_x)}:{int(crop_y)},"
            f"{'hflip,' if variant.mirror and idx % 2 == 0 else ''}"
            f"colorbalance=rs=-0.05:gs=0.10:bs=-0.04:rh=0.02:gh=0.01:bh=0.03,"
            f"eq=contrast={round(1.18 * variant.lut_strength, 3)}:brightness=-0.02:saturation=1.12,"
            f"setsar=1"
        )
        text_filters = _overlay_filters(
            template,
            hud_text,
            slogan,
            title,
            explicit_template_keys,
            text_dir=text_dir,
            include_boxes=background_overlay_index is None,
            speed_mode=speed_mode,
            sequence_tag=sequence_tag,
        )
        if background_overlay_index is not None:
            chain = (
                f"{base_chain},format=rgba[seg{idx}base];"
                f"[seg{idx}base][{background_overlay_index}:v]overlay=0:0:format=auto"
                f"{text_filters},format=yuv420p[v{idx}]"
            )
        else:
            chain = f"{base_chain}{text_filters}[v{idx}]"
        chains.append(chain)
        labels.append(f"[v{idx}]")
    if outro_cover_path is not None and outro_seconds > 0:
        inputs = [*inputs, outro_cover_path]
        outro_index = len(inputs) - 1
        outro_frames = max(1, int(settings.target_fps * outro_seconds))
        chains.append(
            f"[{outro_index}:v]loop=loop={outro_frames}:size=1:start=0,"
            f"fps={settings.target_fps},scale={settings.target_width}:{settings.target_height},"
            f"trim=duration={outro_seconds:.3f},setpts=PTS-STARTPTS,setsar=1,format=yuv420p[outro]"
        )
        labels.append("[outro]")
    if ending_template_path is not None:
        inputs = [*inputs, ending_template_path]
        ending_index = len(inputs) - 1
        if ending_template_path.suffix.lower() in ENDING_IMAGE_EXTENSIONS:
            ending_frames = max(1, int(settings.target_fps * outro_seconds))
            chains.append(
                f"[{ending_index}:v]loop=loop={ending_frames}:size=1:start=0,"
                f"fps={settings.target_fps},scale={settings.target_width}:{settings.target_height}:force_original_aspect_ratio=increase,"
                f"crop={settings.target_width}:{settings.target_height},"
                f"trim=duration={outro_seconds:.3f},setpts=PTS-STARTPTS,setsar=1,format=yuv420p[ending]"
            )
        else:
            chains.append(
                f"[{ending_index}:v]"
                f"fps={settings.target_fps},scale={settings.target_width}:{settings.target_height}:force_original_aspect_ratio=increase,"
                f"crop={settings.target_width}:{settings.target_height},setpts=PTS-STARTPTS,setsar=1,format=yuv420p[ending]"
            )
        labels.append("[ending]")
    chains.append(f"{''.join(labels)}concat=n={len(labels)}:v=1:a=0[vout]")
    return ";".join(chains), inputs


def _is_video_ending(path: Path | None) -> bool:
    return path is not None and path.suffix.lower() in VIDEO_EXTENSIONS


def _decorate_cover(source_path: Path, target_path: Path, title: str) -> None:
    image = Image.open(source_path).convert("RGBA")
    overlay = Image.new("RGBA", image.size, (13, 31, 18, 0))
    draw = ImageDraw.Draw(overlay)
    width, height = image.size
    for step in range(height):
        alpha = int(90 + (step / max(height, 1)) * 120)
        color = (22, 255, 135, min(alpha, 180))
        draw.line([(0, step), (width, step)], fill=color)
    base = Image.alpha_composite(image, overlay)
    text_draw = ImageDraw.Draw(base)
    font = _load_drawtext_font(40, sample_text=title)
    text_draw.rounded_rectangle((120, height // 2 - 56, width - 120, height // 2 + 56), radius=24, fill=(6, 14, 8, 190))
    text_draw.text((160, height // 2 - 8), title, fill=(255, 255, 255, 255), font=font)
    base.convert("RGB").save(target_path)


def _resolve_drawtext_font_arg(font_family: str | None = None, *, sample_text: str = "") -> str:
    for candidate in _font_candidates_for_text(font_family, sample_text):
        if candidate.exists():
            escaped = str(candidate).replace("\\", "/").replace(":", "\\:")
            return f"fontfile='{escaped}':"
    return ""


def _font_candidates_for_family(font_family: str | None = None) -> tuple[Path, ...]:
    return build_font_candidates_for_family(font_family)


def _font_candidates_for_text(font_family: str | None, text: str) -> tuple[Path, ...]:
    base = _font_candidates_for_family(font_family)
    if not _contains_cjk_text(text):
        return base
    # Chinese/Japanese/Korean text should prefer CJK-capable fonts first to avoid tofu glyphs.
    cjk_first = _font_candidates_for_family(CJK_FONT_FAMILY)
    return _merge_font_candidates(cjk_first, base)


def _merge_font_candidates(*groups: tuple[Path, ...]) -> tuple[Path, ...]:
    merged: list[Path] = []
    seen: set[str] = set()
    for group in groups:
        for candidate in group:
            key = str(candidate).lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(candidate)
    return tuple(merged)


def _contains_cjk_text(text: str) -> bool:
    return bool(CJK_CHAR_PATTERN.search(str(text or "")))


def _overlay_filters(
    template: dict,
    hud_text: str,
    slogan: str,
    title: str,
    explicit_template_keys: set[str] | None = None,
    *,
    text_dir: Path | None = None,
    include_boxes: bool = True,
    speed_mode: str = "quality",
    sequence_tag: str = "",
) -> str:
    filters: list[str] = []
    explicit_template_keys = explicit_template_keys or set()
    if template.get("show_hud", True):
        if include_boxes:
            box = _drawbox_filter(template, "hud")
            if box:
                filters.append(box)
        filters.extend(
            _drawtext_lines(
                template,
                hud_text,
                text_key="hud",
                color_key="hud_color",
                max_lines=_template_max_lines(template, "hud", 6),
                explicit_template_keys=explicit_template_keys,
                text_dir=text_dir,
                speed_mode=speed_mode,
            )
        )
    if template.get("show_slogan", True):
        if include_boxes:
            box = _drawbox_filter(template, "slogan")
            if box:
                filters.append(box)
        filters.extend(
            _drawtext_lines(
                template,
                slogan,
                text_key="slogan",
                color_key="slogan_color",
                max_lines=_template_max_lines(template, "slogan", 12),
                explicit_template_keys=explicit_template_keys,
                text_dir=text_dir,
                speed_mode=speed_mode,
            )
        )
    if template.get("show_title", True):
        if include_boxes:
            box = _drawbox_filter(template, "title")
            if box:
                filters.append(box)
        filters.extend(
            _drawtext_lines(
                template,
                title,
                text_key="title",
                color_key="title_color",
                max_lines=_template_max_lines(template, "title", 12),
                explicit_template_keys=explicit_template_keys,
                text_dir=text_dir,
                speed_mode=speed_mode,
            )
        )
    if template.get("show_sequence_tag", True) and sequence_tag.strip():
        filters.append(_sequence_tag_filter(template, sequence_tag.strip()))
    return "," + ",".join(filters) if filters else ""


def _sequence_tag_filter(template: dict[str, Any], sequence_tag: str) -> str:
    font_family = str(template.get("sequence_tag_font_family") or template.get("hud_font_family") or "")
    font_arg = _resolve_drawtext_font_arg(font_family, sample_text=sequence_tag)
    font_size = _coerce_int(template.get("sequence_tag_font_size"), 28, minimum=12, maximum=120)
    margin_right = _coerce_int(template.get("sequence_tag_margin_right"), 24, minimum=0, maximum=200)
    margin_bottom = _coerce_int(template.get("sequence_tag_margin_bottom"), 30, minimum=0, maximum=240)
    font_color = str(template.get("sequence_tag_color") or "#FFFFFF")
    text_style = str(template.get("sequence_tag_text_style") or "soft-shadow").strip().lower()
    x_expr = f"w-text_w-{margin_right}"
    y_expr = f"h-text_h-{margin_bottom}"
    text_source = f"text={_escape_drawtext_text(sequence_tag)}"
    return _drawtext_filter(
        font_arg,
        font_color,
        text_source,
        x_expr,
        y_expr,
        font_size=font_size,
        effect="none",
        line_index=0,
        options=_text_style_options(text_style),
    )


def _drawbox_filter(template: dict[str, Any], target: str) -> str | None:
    spec = _background_box_spec(template, target)
    if spec is None:
        return None
    x, y, width, height, color, opacity, _radius = spec
    return f"drawbox=x={x}:y={y}:w={width}:h={height}:color={color}@{opacity:.2f}:t=fill"


def _background_box_spec(template: dict[str, Any], target: str) -> tuple[int, int, int, int, str, float, int] | None:
    if target == "hud":
        x = int(template.get("hud_bar_x", 0))
        y = int(template["hud_bar_y"])
        width = int(template.get("hud_bar_width", 1080))
        height = int(template["hud_bar_height"])
        color = str(template["hud_bar_color"])
        opacity = float(template["hud_bar_opacity"])
        radius = int(template.get("hud_bar_radius", 0))
    else:
        x = int(template.get(f"{target}_bg_x", 0))
        y = int(template.get(f"{target}_bg_y", template[f"{target}_y"]))
        width = int(template.get(f"{target}_bg_width", 1080))
        default_height = 80 if target == "slogan" else 92
        height = int(template.get(f"{target}_bg_height", default_height))
        color = str(template.get(f"{target}_bg_color") or template.get("hud_bar_color") or "#0E1A10")
        opacity = float(template.get(f"{target}_bg_opacity", template.get("slogan_bg_opacity", 0.62)))
        radius = int(template.get(f"{target}_bg_radius", template.get("hud_bar_radius", 0)))
    if opacity <= 0:
        return None
    return x, y, width, height, color, opacity, radius


def _render_background_overlay(template: dict[str, Any], target_path: Path | None) -> Path | None:
    if target_path is None:
        return None
    specs: list[tuple[int, int, int, int, str, float, int]] = []
    for key in ("hud", "slogan", "title"):
        if not template.get(f"show_{key}", True):
            continue
        spec = _background_box_spec(template, key)
        if spec is not None:
            specs.append(spec)
    if not specs:
        return None
    target_path.parent.mkdir(parents=True, exist_ok=True)
    overlay = Image.new("RGBA", (1080, 1920), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    for x, y, width, height, color, opacity, radius in specs:
        red, green, blue = _hex_to_rgb(color)
        alpha = max(0, min(255, int(round(opacity * 255))))
        box = (x, y, x + width, y + height)
        if radius > 0:
            draw.rounded_rectangle(box, radius=radius, fill=(red, green, blue, alpha))
        else:
            draw.rectangle(box, fill=(red, green, blue, alpha))
    overlay.save(target_path)
    return target_path


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    raw = str(value or "#000000").strip()
    if raw.startswith("#"):
        raw = raw[1:]
    if len(raw) == 3:
        raw = "".join(ch * 2 for ch in raw)
    try:
        return int(raw[0:2], 16), int(raw[2:4], 16), int(raw[4:6], 16)
    except (ValueError, IndexError):
        return 0, 0, 0


def _drawtext_lines(
    template: dict[str, Any],
    text: str,
    *,
    text_key: str,
    color_key: str,
    max_lines: int,
    explicit_template_keys: set[str] | None = None,
    text_dir: Path | None = None,
    speed_mode: str = "quality",
) -> list[str]:
    font_size = int(template[f"{text_key}_font_size"])
    anchor_x = int(template[f"{text_key}_x"])
    anchor_y = int(template[f"{text_key}_y"])
    max_width = _text_box_width(template, text_key, anchor_x)
    font_family = str(template.get(f"{text_key}_font_family") or "")
    lines = _wrap_text_for_drawtext(text, font_size, max_width, font_family=font_family)[:max_lines]
    align = _target_text_align(template, text_key)
    line_gap = max(1, int(font_size * 1.18))
    anchor_y = _centered_text_anchor_y(
        template,
        text_key,
        anchor_y,
        line_count=max(1, len(lines)),
        line_gap=line_gap,
        font_size=font_size,
    )
    effect = str(template.get(f"{text_key}_text_effect") or "none").strip().lower()
    style = str(template.get(f"{text_key}_text_style") or "none").strip().lower()
    if str(speed_mode).strip().lower() == "fast_first":
        effect = "none"
        style = "none"
    color = _template_text_color(template, text_key, color_key, explicit_template_keys or set())
    font_arg = _resolve_drawtext_font_arg(font_family, sample_text=text)
    filters: list[str] = []
    for index, line in enumerate(lines):
        if align == "center":
            x_expr = f"{anchor_x}+({max_width}-text_w)/2"
        elif align == "right":
            x_expr = f"{anchor_x}+{max_width}-text_w"
        else:
            x_expr = str(anchor_x)
        y_expr = str(anchor_y + index * line_gap)
        text_source = _drawtext_text_source(line, text_key, index, text_dir)
        filters.extend(
            _text_style_extra_filters(
                style,
                font_arg,
                text_source,
                x_expr,
                y_expr,
                font_size=font_size,
                line_index=index,
                effect=effect,
            )
        )
        filters.append(
            _drawtext_filter(
                font_arg,
                _text_style_base_color(style, color),
                text_source,
                x_expr,
                y_expr,
                font_size=font_size,
                effect=effect,
                line_index=index,
                options=_text_style_options(style),
            )
        )
    return filters


def _centered_text_anchor_y(
    template: dict[str, Any],
    text_key: str,
    default_anchor_y: int,
    *,
    line_count: int,
    line_gap: int,
    font_size: int,
) -> int:
    if not bool(template.get(f"{text_key}_auto_center", True)):
        return default_anchor_y

    target = "hud" if text_key == "hud" else text_key
    spec = _background_box_spec(template, target)
    if spec is None:
        return default_anchor_y

    _x, y, _w, height, _color, _opacity, _radius = spec
    text_block_height = font_size + max(0, line_count - 1) * line_gap
    centered = y + max(0, int((height - text_block_height) / 2))
    return centered


def _drawtext_filter(
    font_arg: str,
    fontcolor: str,
    text_source: str,
    x_expr: str,
    y_expr: str,
    *,
    font_size: int,
    effect: str,
    line_index: int,
    options: str = "",
) -> str:
    return (
        "drawtext="
        f"{font_arg}fontcolor={fontcolor}:"
        f"fontsize={font_size}:"
        f"{text_source}"
        f"{_text_effect_options(effect, x_expr, y_expr, line_index=line_index, font_size=font_size)}"
        f"{options}"
    )


def _text_style_extra_filters(
    style: str,
    font_arg: str,
    text_source: str,
    x_expr: str,
    y_expr: str,
    *,
    font_size: int,
    line_index: int,
    effect: str = "none",
) -> list[str]:
    if style == "glow":
        return _text_glow_layers(
            font_arg,
            text_source,
            x_expr,
            y_expr,
            font_size,
            color="0x5DD62C",
            alpha=0.42,
            effect=effect,
            line_index=line_index,
        )
    if style == "neon":
        return _text_glow_layers(
            font_arg,
            text_source,
            x_expr,
            y_expr,
            font_size,
            color="0x5DD62C",
            alpha=0.52,
            effect=effect,
            line_index=line_index,
        )
    if style == "gradient":
        low_offset = max(3, int(font_size * 0.18))
        high_offset = max(6, int(font_size * 0.34))
        return [
            _drawtext_filter(
                font_arg,
                "#5DD62C@0.90",
                text_source,
                x_expr,
                f"{y_expr}+{low_offset}",
                font_size=font_size,
                effect=effect,
                line_index=line_index,
            ),
            _drawtext_filter(
                font_arg,
                "#1F8F23@0.72",
                text_source,
                x_expr,
                f"{y_expr}+{high_offset}",
                font_size=font_size,
                effect=effect,
                line_index=line_index,
            ),
        ]
    if style == "reflection":
        reflected_y = f"{y_expr}+{max(18, int(font_size * 1.18))}"
        reflection_alpha = 0.24 * math.exp(-0.18 * line_index)
        return [
            _drawtext_filter(
                font_arg,
                f"#FFFFFF@{reflection_alpha:.2f}",
                text_source,
                x_expr,
                reflected_y,
                font_size=font_size,
                effect=effect,
                line_index=line_index,
            )
        ]
    return []


def _text_glow_layers(
    font_arg: str,
    text_source: str,
    x_expr: str,
    y_expr: str,
    font_size: int,
    *,
    color: str,
    alpha: float,
    effect: str = "none",
    line_index: int = 0,
) -> list[str]:
    offsets = ((0, 0), (2, 0), (-2, 0), (0, 2), (0, -2))
    return [
        _drawtext_filter(
            font_arg,
            f"{color}@{alpha:.2f}",
            text_source,
            f"{x_expr}{_signed_offset(dx)}",
            f"{y_expr}{_signed_offset(dy)}",
            font_size=font_size,
            effect=effect,
            line_index=line_index,
        )
        for dx, dy in offsets
    ]


def _signed_offset(value: int) -> str:
    if value == 0:
        return ""
    return f"+{value}" if value > 0 else str(value)


def _text_style_base_color(style: str, color: str) -> str:
    if style == "gradient":
        return "#FFFFFF"
    if style == "neon":
        return "#F4FFF0"
    return color


def _text_style_options(style: str) -> str:
    if style == "soft-shadow":
        return ":shadowcolor=0x000000@0.78:shadowx=2:shadowy=3"
    if style == "hard-shadow":
        return ":shadowcolor=0x000000@0.88:shadowx=6:shadowy=6"
    if style == "outline":
        return ":borderw=3:bordercolor=0x000000@0.92"
    if style == "white-outline":
        return ":borderw=3:bordercolor=0xFFFFFF@0.92"
    if style == "glow":
        return ":shadowcolor=0x5DD62C@0.76:shadowx=0:shadowy=0"
    if style == "neon":
        return ":borderw=1:bordercolor=0x5DD62C@0.82:shadowcolor=0x5DD62C@0.82:shadowx=0:shadowy=0"
    if style == "gradient":
        return ":borderw=1:bordercolor=0x000000@0.44"
    if style == "reflection":
        return ":shadowcolor=0x000000@0.62:shadowx=1:shadowy=2"
    return ""


def _text_effect_options(effect: str, x_expr: str, y_expr: str, *, line_index: int, font_size: int) -> str:
    delay = line_index * 0.08
    if effect == "fade-in":
        return f":x={x_expr}:y={y_expr}:enable='gte(t\\,{delay:.2f})':alpha='1-exp(-5*(t-{delay:.2f}))'"
    if effect == "fade-out":
        return f":x={x_expr}:y={y_expr}:enable='gte(t\\,{delay:.2f})':alpha='exp(-0.35*(t-{delay:.2f}))'"
    if effect == "fade-in-out":
        return f":x={x_expr}:y={y_expr}:enable='gte(t\\,{delay:.2f})':alpha='0.55+0.45*sin(1.7*(t-{delay:.2f}))'"
    if effect == "pulse":
        return f":x={x_expr}:y={y_expr}:alpha='0.80+0.20*sin(5*(t-{delay:.2f}))'"
    if effect == "glow":
        return f":x={x_expr}:y={y_expr}:alpha='0.70+0.30*sin(7*(t-{delay:.2f}))':shadowcolor=0x5dd62c@0.72:shadowx=0:shadowy=0"
    if effect == "slide-up":
        return f":x={x_expr}:y={y_expr}+44*exp(-4*(t-{delay:.2f}))"
    if effect == "slide-down":
        return f":x={x_expr}:y={y_expr}-44*exp(-4*(t-{delay:.2f}))"
    if effect == "slide-left":
        return f":x={x_expr}+64*exp(-4*(t-{delay:.2f})):y={y_expr}"
    if effect == "slide-right":
        return f":x={x_expr}-64*exp(-4*(t-{delay:.2f})):y={y_expr}"
    if effect == "shake":
        return f":x={x_expr}+8*sin(38*(t-{delay:.2f})):y={y_expr}"
    if effect == "typewriter":
        return f":x={x_expr}:y={y_expr}:enable='gte(t\\,{delay:.2f})'"
    if effect == "pop":
        return f":x={x_expr}:y={y_expr}:alpha='0.65+0.35*exp(-5*(t-{delay:.2f}))*sin(22*(t-{delay:.2f}))'"
    if effect == "blink":
        return f":x={x_expr}:y={y_expr}:alpha='0.35+0.65*gt(sin(10*(t-{delay:.2f}))\\,0)'"
    if effect == "wave":
        return f":x={x_expr}:y={y_expr}+10*sin(4*(t-{delay:.2f})+{line_index})"
    if effect == "jitter":
        return f":x={x_expr}+3*sin(90*(t-{delay:.2f})):y={y_expr}+2*sin(110*(t-{delay:.2f}))"
    if effect == "zoom-in":
        return f":x={x_expr}:y={y_expr}:fontsize='{font_size}*(1-0.16*exp(-5*(t-{delay:.2f})))'"
    if effect == "shadow-pop":
        return f":x={x_expr}:y={y_expr}:alpha='0.70+0.30*exp(-5*(t-{delay:.2f}))*sin(20*(t-{delay:.2f}))':shadowcolor=0x000000@0.80:shadowx='6*exp(-4*(t-{delay:.2f}))':shadowy='6*exp(-4*(t-{delay:.2f}))'"
    return f":x={x_expr}:y={y_expr}"


def _template_text_color(template: dict[str, Any], text_key: str, color_key: str, explicit_template_keys: set[str]) -> str:
    if color_key in explicit_template_keys and template.get(color_key):
        return str(template[color_key])
    if text_key in {"slogan", "hud"}:
        return str(template.get("primary_color") or "#ffffff")
    return str(template.get("secondary_color") or "#ffffff")


def _target_text_align(template: dict[str, Any], text_key: str) -> str:
    value = str(template.get(f"{text_key}_text_align") or template.get("align") or "left").strip().lower()
    return value if value in {"left", "center", "right"} else "left"


def _text_box_width(template: dict[str, Any], text_key: str, anchor_x: int) -> int:
    explicit_width = template.get(f"{text_key}_text_width")
    if explicit_width is not None:
        try:
            return max(120, min(1080 - anchor_x, int(float(explicit_width))))
        except (TypeError, ValueError):
            pass
    fallback = 760
    return max(120, min(1080 - anchor_x, fallback))


def _drawtext_text_source(line: str, text_key: str, line_index: int, text_dir: Path | None) -> str:
    if text_dir is None:
        return f"text={_escape_drawtext_text(line)}"
    text_dir.mkdir(parents=True, exist_ok=True)
    text_path = text_dir / f"{text_key}_{line_index}.txt"
    text_path.write_text(line, encoding="utf-8")
    return f"textfile='{_escape_filter_path(text_path)}'"


def _wrap_text_for_drawtext(text: str, font_size: int, max_width: int, *, font_family: str | None = None) -> list[str]:
    paragraphs = [line.strip() for line in str(text).replace("\\n", "\n").splitlines()]
    if not paragraphs:
        return [""]
    font = _load_drawtext_font(font_size, font_family=font_family, sample_text=text)
    draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    lines: list[str] = []
    for paragraph in paragraphs:
        if not paragraph:
            continue
        if _contains_cjk_text(paragraph) and not any(char.isspace() for char in paragraph):
            lines.extend(_wrap_unspaced_text(draw, paragraph, font, max_width))
            continue
        words = paragraph.split()
        if not words:
            continue
        current = words[0]
        for word in words[1:]:
            candidate = f"{current} {word}"
            if _measure_text_width(draw, candidate, font) <= max_width:
                current = candidate
            else:
                lines.append(current)
                current = word
        lines.append(current)
    return lines or [""]


def _wrap_unspaced_text(draw: ImageDraw.ImageDraw, paragraph: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    wrapped: list[str] = []
    current = ""
    for char in paragraph:
        candidate = f"{current}{char}"
        if current and _measure_text_width(draw, candidate, font) > max_width:
            wrapped.append(current)
            current = char
        else:
            current = candidate
    if current:
        wrapped.append(current)
    return wrapped


def _load_drawtext_font(size: int, font_family: str | None = None, *, sample_text: str = "") -> ImageFont.ImageFont:
    for candidate in _font_candidates_for_text(font_family, sample_text):
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def _resolve_publish_day_code(raw: str | None) -> str:
    token = re.sub(r"\D+", "", str(raw or ""))
    if len(token) >= 4:
        # Prefer MMDD from YYYYMMDD-like tokens.
        token = token[-4:]
    if len(token) != 4:
        token = datetime.now().strftime("%m%d")
    return token


def _resolve_publish_sequence_number(raw: int | None, fallback: int) -> int:
    try:
        value = int(raw) if raw is not None else int(fallback)
    except (TypeError, ValueError):
        value = int(fallback)
    return max(1, value)


def _coerce_int(raw: Any, fallback: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        value = fallback
    return max(minimum, min(maximum, value))


def _measure_text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0]


def _escape_drawtext_text(text: str) -> str:
    replacements = {
        "\\": "\\\\",
        ":": "\\:",
        ",": "\\,",
        ";": "\\;",
        "[": "\\[",
        "]": "\\]",
        "'": "\\'",
        "%": "\\%",
    }
    return "".join(replacements.get(char, char) for char in text)


def _escape_filter_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace(":", "\\:").replace("'", "\\'")


@contextmanager
def _span(telemetry: Any | None, stage: str, name: str, payload: dict[str, Any] | None = None):
    if telemetry is None:
        yield
        return
    with telemetry.span(stage, name, payload):
        yield


def _overlay_complexity(template_config: dict | None, variant: VideoVariant) -> dict[str, Any]:
    template = coerce_template(template_config)
    enabled = {
        "hud": bool(template.get("show_hud", True)),
        "slogan": bool(template.get("show_slogan", True)),
        "title": bool(template.get("show_title", True)),
    }
    drawbox_count = sum(1 for key, value in enabled.items() if value and _drawbox_filter(template, key) is not None)
    text_inputs = {
        "hud": " | ".join(variant.hud_lines),
        "slogan": variant.slogan,
        "title": variant.title,
    }
    drawtext_count = 0
    for key, value in enabled.items():
        if value:
            if key == "slogan":
                max_lines = _template_max_lines(template, "slogan", 12)
            elif key == "title":
                max_lines = _template_max_lines(template, "title", 12)
            else:
                max_lines = _template_max_lines(template, "hud", 6)
            wrapped_lines = _wrap_text_for_drawtext(
                text_inputs[key],
                int(template[f"{key}_font_size"]),
                _text_box_width(template, key, int(template[f"{key}_x"])),
                font_family=str(template.get(f"{key}_font_family") or ""),
            )
            drawtext_count += min(max_lines, len(wrapped_lines) or 1)
    if bool(template.get("show_sequence_tag", True)):
        drawtext_count += 1
    return {
        "show_hud": enabled["hud"],
        "show_slogan": enabled["slogan"],
        "show_title": enabled["title"],
        "drawbox_count": drawbox_count,
        "drawtext_count": drawtext_count,
        "hud_lines": len(variant.hud_lines),
        "slogan_effect": str(template.get("slogan_text_effect") or "none"),
        "title_effect": str(template.get("title_text_effect") or "none"),
        "hud_effect": str(template.get("hud_text_effect") or "none"),
    }


def _template_max_lines(template: dict[str, Any], text_key: str, fallback: int) -> int:
    raw_value = template.get(f"{text_key}_max_lines")
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        parsed = fallback
    return max(1, min(parsed, 12))


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0
