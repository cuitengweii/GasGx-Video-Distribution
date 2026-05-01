from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from .settings import ProjectSettings
from .templates import coerce_template


FONT_CANDIDATES = (
    Path(r"C:\Windows\Fonts\arial.ttf"),
    Path(r"C:\Windows\Fonts\segoeui.ttf"),
)
BOLD_FONT_CANDIDATES = (
    Path(r"C:\Windows\Fonts\arialbd.ttf"),
    Path(r"C:\Windows\Fonts\segoeuib.ttf"),
)


def render_video_template_preview_image(
    settings: ProjectSettings,
    template_config: dict | None,
    hud_text: str = "",
    slogan: str = "",
    title: str = "",
    background: Image.Image | None = None,
) -> Image.Image:
    template = coerce_template(template_config)
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
            str(template.get("hud_color") or template["primary_color"]),
            max_width=width - int(template["hud_x"]) - 42,
            max_lines=2,
        )

    if template.get("show_slogan", True):
        _draw_wrapped(
            draw,
            slogan_text,
            (int(template["slogan_x"]), int(template["slogan_y"])),
            _load_font(int(template["slogan_font_size"]), bold=True),
            str(template["primary_color"]),
            max_width=width - int(template["slogan_x"]) - 42,
            max_lines=3,
        )

    if template.get("show_title", True):
        _draw_wrapped(
            draw,
            title_text,
            (int(template["title_x"]), int(template["title_y"])),
            _load_font(int(template["title_font_size"])),
            str(template["secondary_color"]),
            max_width=width - int(template["title_x"]) - 42,
            max_lines=2,
        )

    return Image.alpha_composite(base, overlay).convert("RGB")


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
    candidates = BOLD_FONT_CANDIDATES if bold else FONT_CANDIDATES
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def _hex_to_rgba(value: str, opacity: float) -> tuple[int, int, int, int]:
    value = value.strip().lstrip("#")
    if len(value) == 3:
        value = "".join(ch * 2 for ch in value)
    red = int(value[0:2], 16)
    green = int(value[2:4], 16)
    blue = int(value[4:6], 16)
    return red, green, blue, max(0, min(255, int(255 * opacity)))
