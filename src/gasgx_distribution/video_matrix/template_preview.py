from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from .font_config import build_font_candidates
from .settings import ProjectSettings
from .templates import coerce_template
from .watermark import build_watermark_overlay


def render_video_template_preview_image(
    settings: ProjectSettings,
    template_config: dict | None,
    hud_text: str = "",
    slogan: str = "",
    title: str = "",
    background: Image.Image | None = None,
    sequence_tag: str = "",
) -> Image.Image:
    template = coerce_template(template_config)
    explicit_template_keys = set((template_config or {}).keys())
    width = int(settings.target_width)
    height = int(settings.target_height)
    base = _fit_background(background, width, height).convert("RGBA") if background else _placeholder_background(width, height).convert("RGBA")
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    hud_lines = [line.strip() for line in hud_text.splitlines() if line.strip()]
    hud = " | ".join(hud_lines[:3]) or "BTC/USD -> ONSITE VALUE | GAS INPUT -> HASH OUTPUT"
    slogan_text = slogan.strip() or "Stop Flaring. Start Hashing."
    title_text = title.strip() or "Gas To Compute"

    if template.get("show_hud", True):
        y = int(template["hud_bar_y"])
        bar_height = int(template["hud_bar_height"])
        bar_width = int(template.get("hud_bar_width", width))
        draw.rectangle(
            (0, y, bar_width, y + bar_height),
            fill=_hex_to_rgba(str(template["hud_bar_color"]), float(template["hud_bar_opacity"])),
        )
        _draw_wrapped(
            draw,
            hud,
            (int(template["hud_x"]), int(template["hud_y"])),
            _load_font(int(template["hud_font_size"])),
            _template_text_color(template, "hud", "hud_color", explicit_template_keys),
            max_width=width - int(template["hud_x"]) - 42,
            max_lines=_template_max_lines(template, "hud", 2),
        )

    if template.get("show_slogan", True):
        _draw_text_background(draw, template, "slogan", width)
        _draw_wrapped(
            draw,
            slogan_text,
            (int(template["slogan_x"]), int(template["slogan_y"])),
            _load_font(int(template["slogan_font_size"]), bold=True),
            _template_text_color(template, "slogan", "slogan_color", explicit_template_keys),
            max_width=width - int(template["slogan_x"]) - 42,
            max_lines=_template_max_lines(template, "slogan", 3),
        )

    if template.get("show_title", True):
        _draw_text_background(draw, template, "title", width)
        _draw_wrapped(
            draw,
            title_text,
            (int(template["title_x"]), int(template["title_y"])),
            _load_font(int(template["title_font_size"])),
            _template_text_color(template, "title", "title_color", explicit_template_keys),
            max_width=width - int(template["title_x"]) - 42,
            max_lines=_template_max_lines(template, "title", 12),
        )

    watermark_overlay = build_watermark_overlay(template, base.size, sequence_tag=sequence_tag)
    if watermark_overlay is not None:
        base = Image.alpha_composite(base, watermark_overlay)
    return Image.alpha_composite(base, overlay).convert("RGB")


def _draw_text_background(draw: ImageDraw.ImageDraw, template: dict, target: str, width: int) -> None:
    x = int(template.get(f"{target}_bg_x", 0))
    y = int(template.get(f"{target}_bg_y", template[f"{target}_y"]))
    box_width = int(template.get(f"{target}_bg_width", width))
    default_height = 80 if target == "slogan" else int(template.get("slogan_bg_height", 92))
    height = int(template.get(f"{target}_bg_height", default_height))
    color = str(template.get(f"{target}_bg_color") or template.get("hud_bar_color") or "#0E1A10")
    opacity = float(template.get(f"{target}_bg_opacity", template.get("slogan_bg_opacity", 0.62)))
    draw.rectangle((x, y, x + box_width, y + height), fill=_hex_to_rgba(color, opacity))


def _fit_background(background: Image.Image, width: int, height: int) -> Image.Image:
    source = background.convert("RGB")
    src_width, src_height = source.size
    scale = max(width / max(src_width, 1), height / max(src_height, 1))
    resized = source.resize((max(1, int(src_width * scale)), max(1, int(src_height * scale))), Image.LANCZOS)
    left = max(0, (resized.width - width) // 2)
    top = max(0, (resized.height - height) // 2)
    return resized.crop((left, top, left + width, top + height))


def _placeholder_background(width: int, height: int) -> Image.Image:
    image = Image.new("RGB", (width, height), "#111611")
    draw = ImageDraw.Draw(image)
    for y in range(height):
        green = 18 + int(32 * y / max(height - 1, 1))
        draw.line([(0, y), (width, y)], fill=(8, green, 18))
    for idx, x in enumerate(range(-width // 5, width, width // 4)):
        color = (35, 64 + idx * 10, 42)
        draw.polygon([(x, height), (x + width // 2, height), (x + width // 3, height // 3)], fill=color)
    draw.rectangle((0, 0, width, height), outline=(93, 214, 44), width=8)
    return image


def _draw_wrapped(
    draw: ImageDraw.ImageDraw,
    text: str,
    point: tuple[int, int],
    font: ImageFont.ImageFont,
    fill: str,
    max_width: int,
    max_lines: int,
) -> None:
    y = point[1]
    for line in _wrap_text(draw, text, font, max_width)[:max_lines]:
        draw.text((point[0], y), line, fill=fill, font=font)
        y += int(getattr(font, "size", 24) * 1.18)


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = text.split()
    if not words:
        return [""]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if _text_width(draw, candidate, font) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0]


def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = build_font_candidates()
    if bold:
        candidates = tuple(path for path in candidates if "bd" in path.name.lower() or "bold" in path.name.lower()) + candidates
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def _template_text_color(template: dict, text_key: str, color_key: str, explicit_template_keys: set[str]) -> str:
    if color_key in explicit_template_keys and template.get(color_key):
        return str(template[color_key])
    if text_key in {"slogan", "hud"}:
        return str(template.get("primary_color") or "#ffffff")
    return str(template.get("secondary_color") or "#ffffff")


def _hex_to_rgba(value: str, opacity: float) -> tuple[int, int, int, int]:
    value = value.strip().lstrip("#")
    if len(value) == 3:
        value = "".join(ch * 2 for ch in value)
    red = int(value[0:2], 16)
    green = int(value[2:4], 16)
    blue = int(value[4:6], 16)
    return red, green, blue, max(0, min(255, int(255 * opacity)))


def _template_max_lines(template: dict, text_key: str, fallback: int) -> int:
    raw_value = template.get(f"{text_key}_max_lines")
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        parsed = fallback
    return max(1, min(parsed, 12))
