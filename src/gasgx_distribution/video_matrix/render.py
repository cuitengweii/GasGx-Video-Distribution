from __future__ import annotations

import hashlib
import json
import shutil
from contextlib import contextmanager
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
    telemetry: Any | None = None,
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
        if telemetry is not None:
            telemetry.event(
                "render",
                "variant_started",
                {
                    "base_name": base_name,
                    "segment_count": len(variant.segments),
                    "target_duration": sum(segment.duration for segment in variant.segments) + max(0.0, cover_intro_seconds) + max(0.0, outro_seconds),
                    "output_types": sorted(output_types),
                },
            )
        intro_cover_path = None
        if cover_template_config is not None and cover_intro_seconds > 0:
            first_segment = variant.segments[0]
            with _span(telemetry, "render", "intro_extract_frame", {"source": first_segment.clip.normalized_path, "timestamp": first_segment.start_time}):
                _extract_frame_or_fallback(first_segment.clip.normalized_path, intro_frame, timestamp=first_segment.start_time)
            with _span(telemetry, "render", "intro_cover_render", {"cover_template": cover_template_config.get("name", "") if isinstance(cover_template_config, dict) else ""}):
                render_intro_cover(intro_frame, intro_cover, variant, settings, cover_template_config)
            intro_cover_path = intro_cover
        outro_cover_path = None
        if ending_template_path is None and ending_cover_template_config is not None and outro_text.strip() and outro_seconds > 0:
            last_segment = variant.segments[-1]
            timestamp = last_segment.start_time + max(0.0, last_segment.duration - 0.2)
            with _span(telemetry, "render", "outro_extract_frame", {"source": last_segment.clip.normalized_path, "timestamp": timestamp}):
                _extract_frame_or_fallback(last_segment.clip.normalized_path, outro_frame, timestamp=timestamp)
            with _span(telemetry, "render", "outro_cover_render", {"ending_template": ending_cover_template_config.get("name", "") if isinstance(ending_cover_template_config, dict) else ""}):
                render_outro_cover(outro_frame, outro_cover, settings, ending_cover_template_config, outro_text.strip(), variant.hud_lines)
            outro_cover_path = outro_cover

        with _span(telemetry, "render", "filter_build", _overlay_complexity(template_config, variant)):
            filter_complex, inputs = _build_filter_complex(
                variant,
                settings,
                template_config=template_config,
                intro_cover_path=intro_cover_path,
                cover_intro_seconds=cover_intro_seconds,
                outro_cover_path=outro_cover_path,
                outro_seconds=outro_seconds,
                ending_template_path=ending_template_path,
                text_dir=scratch_dir / "text_layers",
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
            concat_video(filter_complex, inputs, video_path, bgm_path=bgm_path)
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
                copy_path.write_text(
                    build_marketing_copy(variant, settings, copy_language, template_copy, outro_text),
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
        with _span(telemetry, "render", "cleanup", {"scratch_dir": scratch_dir}):
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
    text_dir: Path | None = None,
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
            f"{_overlay_filters(template, hud_text, slogan, title, explicit_template_keys, text_dir=text_dir)}"
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


def _overlay_filters(
    template: dict,
    hud_text: str,
    slogan: str,
    title: str,
    explicit_template_keys: set[str] | None = None,
    *,
    text_dir: Path | None = None,
) -> str:
    filters: list[str] = []
    explicit_template_keys = explicit_template_keys or set()
    if template.get("show_hud", True):
        box = _drawbox_filter(template, "hud")
        if box:
            filters.append(box)
        filters.extend(
            _drawtext_lines(
                template,
                hud_text,
                text_key="hud",
                color_key="hud_color",
                max_lines=2,
                explicit_template_keys=explicit_template_keys,
                text_dir=text_dir,
            )
        )
    if template.get("show_slogan", True):
        box = _drawbox_filter(template, "slogan")
        if box:
            filters.append(box)
        filters.extend(
            _drawtext_lines(
                template,
                slogan,
                text_key="slogan",
                color_key="slogan_color",
                max_lines=3,
                explicit_template_keys=explicit_template_keys,
                text_dir=text_dir,
            )
        )
    if template.get("show_title", True):
        box = _drawbox_filter(template, "title")
        if box:
            filters.append(box)
        filters.extend(
            _drawtext_lines(
                template,
                title,
                text_key="title",
                color_key="title_color",
                max_lines=2,
                explicit_template_keys=explicit_template_keys,
                text_dir=text_dir,
            )
        )
    return "," + ",".join(filters) if filters else ""


def _drawbox_filter(template: dict[str, Any], target: str) -> str | None:
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
    if opacity <= 0:
        return None
    return f"drawbox=x={x}:y={y}:w={width}:h={height}:color={color}@{opacity:.2f}:t=fill"


def _drawtext_lines(
    template: dict[str, Any],
    text: str,
    *,
    text_key: str,
    color_key: str,
    max_lines: int,
    explicit_template_keys: set[str] | None = None,
    text_dir: Path | None = None,
) -> list[str]:
    font_size = int(template[f"{text_key}_font_size"])
    anchor_x = int(template[f"{text_key}_x"])
    anchor_y = int(template[f"{text_key}_y"])
    max_width = _text_box_width(template, text_key, anchor_x)
    lines = _wrap_text_for_drawtext(text, font_size, max_width)[:max_lines]
    align = _target_text_align(template, text_key)
    line_gap = max(1, int(font_size * 1.18))
    effect = str(template.get(f"{text_key}_text_effect") or "none").strip().lower()
    font_arg = _resolve_drawtext_font_arg()
    filters: list[str] = []
    for index, line in enumerate(lines):
        if align == "center":
            x_expr = "(w-text_w)/2"
        elif align == "right":
            x_expr = f"w-text_w-{anchor_x}"
        else:
            x_expr = str(anchor_x)
        y_expr = str(anchor_y + index * line_gap)
        text_source = _drawtext_text_source(line, text_key, index, text_dir)
        filters.append(
            "drawtext="
            f"{font_arg}fontcolor={_template_text_color(template, text_key, color_key, explicit_template_keys or set())}:"
            f"fontsize={font_size}:"
            f"{text_source}"
            f"{_text_effect_options(effect, x_expr, y_expr, line_index=index, font_size=font_size)}"
        )
    return filters


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
            return max(120, min(1080, int(float(explicit_width))))
        except (TypeError, ValueError):
            pass
    align = _target_text_align(template, text_key)
    if text_key == "hud":
        return max(120, 1080 - anchor_x * 2 if align == "center" else 1080 - anchor_x - 42)
    if align == "center":
        return max(120, min(1000, 1080 - 84))
    return max(120, 1080 - anchor_x - 42)


def _drawtext_text_source(line: str, text_key: str, line_index: int, text_dir: Path | None) -> str:
    if text_dir is None:
        return f"text={_escape_drawtext_text(line)}"
    text_dir.mkdir(parents=True, exist_ok=True)
    text_path = text_dir / f"{text_key}_{line_index}.txt"
    text_path.write_text(line, encoding="utf-8")
    return f"textfile='{_escape_filter_path(text_path)}'"


def _wrap_text_for_drawtext(text: str, font_size: int, max_width: int) -> list[str]:
    paragraphs = [line.strip() for line in str(text).replace("\\n", "\n").splitlines()]
    if not paragraphs:
        return [""]
    font = _load_drawtext_font(font_size)
    draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    lines: list[str] = []
    for paragraph in paragraphs:
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
            max_lines = 3 if key == "slogan" else 2
            wrapped_lines = _wrap_text_for_drawtext(
                text_inputs[key],
                int(template[f"{key}_font_size"]),
                _text_box_width(template, key, int(template[f"{key}_x"])),
            )
            drawtext_count += min(max_lines, len(wrapped_lines) or 1)
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


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0
