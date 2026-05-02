from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

from .ffmpeg_tools import concat_video, extract_frame
from .cover import render_intro_cover, render_outro_cover
from .models import RenderedAsset, VideoVariant
from .settings import ProjectSettings
from .spark_text import build_marketing_copy
from .templates import coerce_template

FONT_CANDIDATES = (
    Path(r"C:\Windows\Fonts\msyh.ttc"),
    Path(r"C:\Windows\Fonts\msyhbd.ttc"),
    Path(r"C:\Windows\Fonts\simhei.ttf"),
    Path(r"C:\Windows\Fonts\simsun.ttc"),
    Path(r"C:\Windows\Fonts\NotoSansSC-VF.ttf"),
    Path(r"C:\Windows\Fonts\Noto Sans SC (TrueType).otf"),
    Path(r"C:\Windows\Fonts\arial.ttf"),
    Path(r"C:\Windows\Fonts\segoeui.ttf"),
    Path(r"C:\Windows\Fonts\arialbd.ttf"),
)
ENDING_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}


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
) -> RenderedAsset:
    batch_dir.mkdir(parents=True, exist_ok=True)
    output_types = output_types or {"mp4"}
    base_name = f"{filename_prefix}vibe_{variant.sequence_number:02d}"
    video_path = batch_dir / f"{base_name}.mp4"
    scratch_dir = batch_dir / ".render_tmp" / base_name
    cover_frame = scratch_dir / f"{base_name}_raw_cover.png"
    intro_frame = scratch_dir / f"{base_name}_intro_frame.png"
    intro_cover = scratch_dir / f"{base_name}_intro_cover.png"
    outro_frame = scratch_dir / f"{base_name}_outro_frame.png"
    outro_cover = scratch_dir / f"{base_name}_outro_cover.png"
    cover_path = batch_dir / f"{base_name}_cover.png" if "png" in output_types else None
    copy_path = batch_dir / f"{base_name}_copy.txt" if "txt" in output_types else None
    manifest_path = batch_dir / f"{base_name}_manifest.json" if "json" in output_types else None

    try:
        intro_cover_path = None
        if cover_template_config is not None and cover_intro_seconds > 0:
            first_segment = variant.segments[0]
            _extract_frame_or_fallback(first_segment.clip.normalized_path, intro_frame, timestamp=first_segment.start_time)
            render_intro_cover(intro_frame, intro_cover, variant, settings, cover_template_config)
            intro_cover_path = intro_cover
        outro_cover_path = None
        if ending_template_path is None and ending_cover_template_config is not None and outro_text.strip() and outro_seconds > 0:
            last_segment = variant.segments[-1]
            timestamp = last_segment.start_time + max(0.0, last_segment.duration - 0.2)
            _extract_frame_or_fallback(last_segment.clip.normalized_path, outro_frame, timestamp=timestamp)
            render_outro_cover(outro_frame, outro_cover, settings, ending_cover_template_config, outro_text.strip(), variant.hud_lines)
            outro_cover_path = outro_cover

        filter_complex, inputs = _build_filter_complex(
            variant,
            settings,
            template_config=template_config,
            intro_cover_path=intro_cover_path,
            cover_intro_seconds=cover_intro_seconds,
            outro_cover_path=outro_cover_path,
            outro_seconds=outro_seconds,
            ending_template_path=ending_template_path,
        )
        concat_video(filter_complex, inputs, video_path, bgm_path=bgm_path)
        if cover_path is not None:
            if intro_cover_path is not None:
                shutil.copyfile(intro_cover_path, cover_path)
            else:
                extract_frame(video_path, cover_frame, timestamp=1.0)
                _decorate_cover(cover_frame, cover_path, variant.title)

        if copy_path is not None:
            copy_path.write_text(
                build_marketing_copy(variant, settings, copy_language, template_copy, outro_text),
                encoding="utf-8",
            )

        if manifest_path is not None:
            manifest_path.write_text(
                json.dumps(
                    {
                        "sequence_number": variant.sequence_number,
                        "title": variant.title,
                        "slogan": variant.slogan,
                        "signature": variant.signature,
                        "video_path": str(video_path),
                        "cover_path": str(cover_path) if cover_path else None,
                        "cover_template_id": cover_template_id,
                        "cover_intro_seconds": cover_intro_seconds if intro_cover_path is not None else 0,
                        "outro_text": outro_text,
                        "outro_seconds": outro_seconds if outro_cover_path is not None else 0,
                        "copy_path": str(copy_path) if copy_path else None,
                        "copy_language": copy_language,
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
    finally:
        for temp_path in (cover_frame, intro_frame, intro_cover, outro_frame, outro_cover):
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


def _build_filter_complex(
    variant: VideoVariant,
    settings: ProjectSettings,
    template_config: dict | None = None,
    intro_cover_path: Path | None = None,
    cover_intro_seconds: float = 1.0,
    outro_cover_path: Path | None = None,
    outro_seconds: float = 1.0,
    ending_template_path: Path | None = None,
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
    font_arg = _resolve_drawtext_font_arg()
    template = coerce_template(template_config)
    explicit_template_keys = set((template_config or {}).keys())
    hud_text = " | ".join(variant.hud_lines)
    slogan = variant.slogan
    title = variant.title
    for idx, segment in enumerate(variant.segments):
        crop_x = max(0, (settings.target_width * variant.zoom - settings.target_width) / 2 + variant.x_offset)
        crop_y = max(0, (settings.target_height * variant.zoom - settings.target_height) / 2 + variant.y_offset)
        chain = (
            f"[{idx + input_offset}:v]"
            f"trim=start={segment.start_time}:duration={segment.duration},setpts=PTS-STARTPTS,"
            f"scale={int(settings.target_width * variant.zoom)}:{int(settings.target_height * variant.zoom)},"
            f"crop={settings.target_width}:{settings.target_height}:{int(crop_x)}:{int(crop_y)},"
            f"{'hflip,' if variant.mirror and idx % 2 == 0 else ''}"
            f"colorbalance=rs=-0.05:gs=0.10:bs=-0.04:rh=0.02:gh=0.01:bh=0.03,"
            f"eq=contrast={round(1.18 * variant.lut_strength, 3)}:brightness=-0.02:saturation=1.12,"
            f"setsar=1"
            f"{_overlay_filters(template, font_arg, hud_text, slogan, title, explicit_template_keys)}"
            f"[v{idx}]"
        )
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
    font = _load_drawtext_font(40)
    text_draw.rounded_rectangle((120, height // 2 - 56, width - 120, height // 2 + 56), radius=24, fill=(6, 14, 8, 190))
    text_draw.text((160, height // 2 - 8), title, fill=(255, 255, 255, 255), font=font)
    base.convert("RGB").save(target_path)


def _resolve_drawtext_font_arg() -> str:
    for candidate in FONT_CANDIDATES:
        if candidate.exists():
            escaped = str(candidate).replace("\\", "/").replace(":", "\\:")
            return f"fontfile='{escaped}':"
    return ""


def _overlay_filters(template: dict, font_arg: str, hud_text: str, slogan: str, title: str, explicit_template_keys: set[str] | None = None) -> str:
    filters: list[str] = []
    explicit_template_keys = explicit_template_keys or set()
    if template.get("show_hud", True):
        filters.append(_drawbox_filter(template, "hud"))
        filters.extend(
            _drawtext_lines(
                template,
                font_arg,
                hud_text,
                text_key="hud",
                color_key="hud_color",
                max_lines=2,
                explicit_template_keys=explicit_template_keys,
            )
        )
    if template.get("show_slogan", True):
        filters.append(_drawbox_filter(template, "slogan"))
        filters.extend(
            _drawtext_lines(
                template,
                font_arg,
                slogan,
                text_key="slogan",
                color_key="slogan_color",
                max_lines=3,
                explicit_template_keys=explicit_template_keys,
            )
        )
    if template.get("show_title", True):
        filters.append(_drawbox_filter(template, "title"))
        filters.extend(
            _drawtext_lines(
                template,
                font_arg,
                title,
                text_key="title",
                color_key="title_color",
                max_lines=2,
                explicit_template_keys=explicit_template_keys,
            )
        )
    return "," + ",".join(filters) if filters else ""


def _drawbox_filter(template: dict[str, Any], target: str) -> str:
    if target == "hud":
        x = int(template.get("hud_bar_x", 0))
        y = int(template["hud_bar_y"])
        width = int(template.get("hud_bar_width", 1080))
        height = int(template["hud_bar_height"])
        color = str(template["hud_bar_color"])
        opacity = float(template["hud_bar_opacity"])
    else:
        x = int(template.get(f"{target}_bg_x", 0))
        y = int(template.get(f"{target}_bg_y", template[f"{target}_y"]))
        width = int(template.get(f"{target}_bg_width", 1080))
        default_height = 80 if target == "slogan" else int(template.get("slogan_bg_height", 92))
        height = int(template.get(f"{target}_bg_height", default_height))
        color = str(template.get(f"{target}_bg_color") or template.get("hud_bar_color") or "#0E1A10")
        opacity = float(template.get(f"{target}_bg_opacity", template.get("slogan_bg_opacity", 0.62)))
    return f"drawbox=x={x}:y={y}:w={width}:h={height}:color={color}@{opacity:.2f}:t=fill"


def _drawtext_lines(
    template: dict[str, Any],
    font_arg: str,
    text: str,
    *,
    text_key: str,
    color_key: str,
    max_lines: int,
    explicit_template_keys: set[str] | None = None,
) -> list[str]:
    font_size = int(template[f"{text_key}_font_size"])
    anchor_x = int(template[f"{text_key}_x"])
    anchor_y = int(template[f"{text_key}_y"])
    max_width = _text_box_width(template, text_key, anchor_x)
    lines = _wrap_text_for_drawtext(text, font_size, max_width)[:max_lines]
    align = str(template.get("align", "left")).lower()
    line_gap = max(1, int(font_size * 1.18))
    effect = str(template.get(f"{text_key}_text_effect") or "none").strip().lower()
    filters: list[str] = []
    for index, line in enumerate(lines):
        if align == "center":
            x_expr = "(w-text_w)/2"
        else:
            x_expr = str(anchor_x)
        y_expr = str(anchor_y + index * line_gap)
        filters.append(
            "drawtext="
            f"{font_arg}fontcolor={_template_text_color(template, text_key, color_key, explicit_template_keys or set())}:"
            f"fontsize={font_size}:"
            f"text={_escape_drawtext_text(line)}"
            f"{_text_effect_options(effect, x_expr, y_expr, line_index=index)}"
        )
    return filters


def _text_effect_options(effect: str, x_expr: str, y_expr: str, *, line_index: int) -> str:
    delay = line_index * 0.08
    if effect == "pulse":
        return f":x={x_expr}:y={y_expr}:alpha='0.80+0.20*sin(5*(t-{delay:.2f}))'"
    if effect == "glow":
        return f":x={x_expr}:y={y_expr}:alpha='0.70+0.30*sin(7*(t-{delay:.2f}))'"
    if effect == "slide-up":
        return f":x={x_expr}:y={y_expr}+44*exp(-4*(t-{delay:.2f}))"
    if effect == "shake":
        return f":x={x_expr}+8*sin(38*(t-{delay:.2f})):y={y_expr}"
    if effect == "typewriter":
        return f":x={x_expr}:y={y_expr}:enable='gte(t\\,{delay:.2f})'"
    if effect == "pop":
        return f":x={x_expr}:y={y_expr}:alpha='0.65+0.35*exp(-5*(t-{delay:.2f}))*sin(22*(t-{delay:.2f}))'"
    return f":x={x_expr}:y={y_expr}"


def _template_text_color(template: dict[str, Any], text_key: str, color_key: str, explicit_template_keys: set[str]) -> str:
    if color_key in explicit_template_keys and template.get(color_key):
        return str(template[color_key])
    if text_key in {"slogan", "hud"}:
        return str(template.get("primary_color") or "#ffffff")
    return str(template.get("secondary_color") or "#ffffff")


def _text_box_width(template: dict[str, Any], text_key: str, anchor_x: int) -> int:
    if text_key == "hud":
        return max(120, 1080 - anchor_x * 2 if str(template.get("align", "left")).lower() == "center" else 1080 - anchor_x - 42)
    return max(120, 1080 - anchor_x * 2 if str(template.get("align", "left")).lower() == "center" else 1080 - anchor_x - 42)


def _wrap_text_for_drawtext(text: str, font_size: int, max_width: int) -> list[str]:
    words = text.split()
    if not words:
        return [""]
    font = _load_drawtext_font(font_size)
    draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if _measure_text_width(draw, candidate, font) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def _load_drawtext_font(size: int) -> ImageFont.ImageFont:
    for candidate in FONT_CANDIDATES:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


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
