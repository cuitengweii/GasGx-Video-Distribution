from __future__ import annotations

import base64
import re
from io import BytesIO
from typing import Any

from PIL import Image, ImageDraw, ImageFont

from .font_config import build_font_candidates_for_family

WATERMARK_TEXT_MAX_CHARS = 10
WATERMARK_IMAGE_MAX_SIZE = 200

WATERMARK_MODE_OPTIONS = ("none", "text", "image", "auto")
WATERMARK_POSITION_OPTIONS = (
    ("top_center", "顶部居中"),
    ("bottom_center", "底部居中"),
    ("top_left", "顶部靠左"),
    ("top_right", "顶部靠右"),
    ("bottom_left", "底部靠左"),
    ("bottom_right", "底部靠右"),
)
WATERMARK_TILE_OPTIONS = (
    ("none", "不平铺"),
    ("straight", "正向平铺"),
    ("diagonal", "斜向平铺"),
)

CJK_FONT_FAMILY = (
    "'Microsoft YaHei', 'Microsoft YaHei UI', SimHei, SimSun, "
    "'Noto Sans SC', 'Source Han Sans SC', 'HarmonyOS Sans SC', sans-serif"
)


def sanitize_watermark_text(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").replace("\n", "")
    return text[:WATERMARK_TEXT_MAX_CHARS]


def normalize_watermark_mode(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    return value if value in WATERMARK_MODE_OPTIONS else "none"


def normalize_watermark_position(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    for option, _label in WATERMARK_POSITION_OPTIONS:
        if value == option:
            return option
    return "bottom_right"


def normalize_watermark_tile_mode(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    for option, _label in WATERMARK_TILE_OPTIONS:
        if value == option:
            return option
    return "none"


def resolve_watermark_text(template: dict[str, Any], sequence_tag: str = "") -> str:
    mode = normalize_watermark_mode(template.get("watermark_mode"))
    if mode == "auto":
        return sanitize_watermark_text(sequence_tag)
    return sanitize_watermark_text(template.get("watermark_text") or "")


def load_watermark_font(size: int, font_family: str | None = None, *, sample_text: str = "") -> ImageFont.ImageFont:
    for candidate in _font_candidates_for_text(font_family, sample_text):
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def build_watermark_overlay(
    template: dict[str, Any],
    canvas_size: tuple[int, int],
    *,
    sequence_tag: str = "",
) -> Image.Image | None:
    raw_mode = template.get("watermark_mode")
    if raw_mode is None:
        return None

    mode = normalize_watermark_mode(raw_mode)
    if mode == "none":
        return None

    width = max(1, int(canvas_size[0]))
    height = max(1, int(canvas_size[1]))
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    opacity = _coerce_float(template.get("watermark_opacity"), 0.58, minimum=0.0, maximum=1.0)

    if mode == "image":
        image = _decode_image_data_url(template.get("watermark_image_data_url"))
        if image is None:
            return None
        _draw_image_watermark(overlay, image, template, opacity)
        return overlay

    text = resolve_watermark_text(template, sequence_tag)
    if not text:
        return None

    font_family = str(
        template.get("watermark_font_family")
        or template.get("watermark_text_font_family")
        or template.get("hud_font_family")
        or template.get("sequence_tag_font_family")
        or ""
    )
    font_size = _coerce_int(template.get("watermark_font_size"), 32, minimum=12, maximum=180)
    font_size = _fit_font_size_to_width(
        font_size,
        text,
        max_width=max(120, min(width - 96, 480)),
        font_family=font_family,
    )
    font = load_watermark_font(font_size, font_family=font_family, sample_text=text)
    fill = _hex_to_rgba(str(template.get("watermark_color") or "#FFFFFF"), opacity)
    tile_mode = normalize_watermark_tile_mode(template.get("watermark_tile_mode"))
    if tile_mode == "diagonal":
        _draw_diagonal_tiled_text(overlay, text, font, fill)
    elif tile_mode == "straight":
        _draw_straight_tiled_text(overlay, text, font, fill)
    else:
        _draw_positioned_text(overlay, text, font, fill, template)
    return overlay


def _draw_positioned_text(
    overlay: Image.Image,
    text: str,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
    template: dict[str, Any],
) -> None:
    draw = ImageDraw.Draw(overlay)
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = max(1, bbox[2] - bbox[0])
    text_height = max(1, bbox[3] - bbox[1])
    x, y = _watermark_position(overlay.size, text_width, text_height, template)
    shadow_fill = (0, 0, 0, max(0, min(255, int(round(fill[3] * 0.72)))))
    draw.text((x + 2, y + 2), text, fill=shadow_fill, font=font)
    draw.text((x, y), text, fill=fill, font=font)


def _draw_straight_tiled_text(
    overlay: Image.Image,
    text: str,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
) -> None:
    draw = ImageDraw.Draw(overlay)
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = max(1, bbox[2] - bbox[0])
    text_height = max(1, bbox[3] - bbox[1])
    gap_x = max(180, text_width + 96)
    gap_y = max(140, text_height + 84)
    shadow_fill = (0, 0, 0, max(0, min(255, int(round(fill[3] * 0.55)))))
    for top in range(-gap_y, overlay.height + gap_y, gap_y):
        for left in range(-gap_x, overlay.width + gap_x, gap_x):
            draw.text((left + 2, top + 2), text, fill=shadow_fill, font=font)
            draw.text((left, top), text, fill=fill, font=font)


def _draw_diagonal_tiled_text(
    overlay: Image.Image,
    text: str,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
) -> None:
    draw = ImageDraw.Draw(overlay)
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = max(1, bbox[2] - bbox[0])
    text_height = max(1, bbox[3] - bbox[1])
    tile_width = max(260, text_width + 124)
    tile_height = max(180, text_height + 104)
    tile = Image.new("RGBA", (tile_width, tile_height), (0, 0, 0, 0))
    tile_draw = ImageDraw.Draw(tile)
    x = max(24, int((tile_width - text_width) / 2))
    y = max(24, int((tile_height - text_height) / 2))
    shadow_fill = (0, 0, 0, max(0, min(255, int(round(fill[3] * 0.55)))))
    tile_draw.text((x + 2, y + 2), text, fill=shadow_fill, font=font)
    tile_draw.text((x, y), text, fill=fill, font=font)
    rotated = tile.rotate(-28, expand=True, resample=Image.BICUBIC)
    step_x = max(220, rotated.width + 56)
    step_y = max(160, rotated.height + 44)
    for top in range(-rotated.height, overlay.height + rotated.height, step_y):
        for left in range(-rotated.width, overlay.width + rotated.width, step_x):
            overlay.alpha_composite(rotated, (left, top))


def _draw_image_watermark(
    overlay: Image.Image,
    image: Image.Image,
    template: dict[str, Any],
    opacity: float,
) -> None:
    source = image.convert("RGBA")
    source.thumbnail((WATERMARK_IMAGE_MAX_SIZE, WATERMARK_IMAGE_MAX_SIZE), Image.LANCZOS)
    alpha = source.getchannel("A")
    source.putalpha(alpha.point(lambda value: max(0, min(255, int(round(value * opacity))))))
    x, y = _watermark_position(overlay.size, source.width, source.height, template)
    overlay.alpha_composite(source, (x, y))


def _watermark_position(
    canvas_size: tuple[int, int],
    item_width: int,
    item_height: int,
    template: dict[str, Any],
) -> tuple[int, int]:
    position = normalize_watermark_position(template.get("watermark_position"))
    width = max(1, int(canvas_size[0]))
    height = max(1, int(canvas_size[1]))
    item_width = max(1, int(item_width))
    item_height = max(1, int(item_height))
    margin_x = 56
    margin_y = 68
    if position == "top_center":
        return max(0, int((width - item_width) / 2)), margin_y
    if position == "bottom_center":
        return max(0, int((width - item_width) / 2)), max(0, height - item_height - margin_y)
    if position == "top_left":
        return margin_x, margin_y
    if position == "top_right":
        return max(0, width - item_width - margin_x), margin_y
    if position == "bottom_left":
        return margin_x, max(0, height - item_height - margin_y)
    return max(0, width - item_width - margin_x), max(0, height - item_height - margin_y)


def _fit_font_size_to_width(
    font_size: int,
    text: str,
    *,
    max_width: int,
    font_family: str | None = None,
) -> int:
    size = max(12, int(font_size))
    while size > 12:
        font = load_watermark_font(size, font_family=font_family, sample_text=text)
        draw = ImageDraw.Draw(Image.new("RGBA", (8, 8)))
        bbox = draw.textbbox((0, 0), text, font=font)
        if bbox[2] - bbox[0] <= max_width:
            break
        size -= 2
    return size


def _font_candidates_for_text(font_family: str | None, text: str) -> tuple[Any, ...]:
    base = build_font_candidates_for_family(font_family)
    if not _contains_cjk_text(text):
        return base
    cjk_first = build_font_candidates_for_family(CJK_FONT_FAMILY)
    return _merge_font_candidates(cjk_first, base)


def _merge_font_candidates(*groups: tuple[Any, ...]) -> tuple[Any, ...]:
    merged: list[Any] = []
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
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uac00-\ud7af]", str(text or "")))


def _decode_image_data_url(raw: Any) -> Image.Image | None:
    value = str(raw or "").strip()
    if not value:
        return None
    payload = value
    if value.startswith("data:"):
        try:
            _header, payload = value.split(",", 1)
        except ValueError:
            return None
    try:
        data = base64.b64decode(payload, validate=False)
    except Exception:
        return None
    try:
        return Image.open(BytesIO(data))
    except Exception:
        return None


def _hex_to_rgba(value: str, opacity: float) -> tuple[int, int, int, int]:
    raw = str(value or "#ffffff").strip().lstrip("#")
    if len(raw) == 3:
        raw = "".join(ch * 2 for ch in raw)
    try:
        red = int(raw[0:2], 16)
        green = int(raw[2:4], 16)
        blue = int(raw[4:6], 16)
    except (TypeError, ValueError):
        red, green, blue = 255, 255, 255
    alpha = max(0, min(255, int(round(255 * max(0.0, min(1.0, float(opacity)))))))
    return red, green, blue, alpha


def _coerce_int(raw: Any, fallback: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        value = fallback
    return max(minimum, min(maximum, value))


def _coerce_float(raw: Any, fallback: float, *, minimum: float, maximum: float) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = fallback
    return max(minimum, min(maximum, value))
