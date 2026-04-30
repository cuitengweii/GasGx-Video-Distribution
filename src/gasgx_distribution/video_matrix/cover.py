from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from .cover_templates import coerce_cover_template
from .models import VideoVariant
from .settings import ProjectSettings


FONT_CANDIDATES = (
    Path(r"C:\Windows\Fonts\arial.ttf"),
    Path(r"C:\Windows\Fonts\segoeui.ttf"),
)
BOLD_FONT_CANDIDATES = (
    Path(r"C:\Windows\Fonts\arialbd.ttf"),
    Path(r"C:\Windows\Fonts\segoeuib.ttf"),
)


def render_intro_cover(
    source_frame_path: Path,
    target_path: Path,
    variant: VideoVariant,
    settings: ProjectSettings,
    template_config: dict | None,
) -> None:
    image = Image.open(source_frame_path).convert("RGB")
    cover = build_intro_cover_image(image, settings, template_config, variant.slogan, variant.title, variant.hud_lines)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    cover.save(target_path)


def render_outro_cover(
    source_frame_path: Path,
    target_path: Path,
    settings: ProjectSettings,
    template_config: dict | None,
    follow_text: str,
    hud_lines: list[str],
) -> None:
    image = Image.open(source_frame_path).convert("RGB")
    cover = build_intro_cover_image(
        image,
        settings,
        template_config,
        follow_text,
        "Follow GasGx for more gas engine and generator set cases",
        hud_lines,
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    cover.save(target_path)


def render_cover_preview_image(
    settings: ProjectSettings,
    template_config: dict | None,
    headline: str = "Stop Flaring. Start Hashing.",
    subhead: str = "Gas To Compute",
    hud_lines: list[str] | None = None,
) -> Image.Image:
    return build_intro_cover_image(
        background=_placeholder_background(settings.target_width, settings.target_height),
        settings=settings,
        template_config=template_config,
        headline=headline,
        subhead=subhead,
        hud_lines=hud_lines or ["BTC/USD -> ONSITE VALUE", "GAS INPUT -> HASH OUTPUT"],
    )


def build_intro_cover_image(
    background: Image.Image,
    settings: ProjectSettings,
    template_config: dict | None,
    headline: str,
    subhead: str,
    hud_lines: list[str],
) -> Image.Image:
    template = coerce_cover_template(template_config)
    base = _cover_crop(background, settings.target_width, settings.target_height).convert("RGBA")
    base = _apply_tint_and_gradient(base, template)
    draw = ImageDraw.Draw(base)
    width, height = base.size
    align = str(template.get("align", "left"))
    margin = 72

    brand_font = _load_font(52, bold=True)
    eyebrow_font = _load_font(24)
    headline_font = _load_font(84, bold=True)
    subhead_font = _load_font(38)
    hud_font = _load_font(28)
    cta_font = _load_font(28, bold=True)

    brand = str(template.get("brand", "GasGx"))
    eyebrow = str(template.get("eyebrow", ""))
    cta = str(template.get("cta", settings.website_url))
    primary = str(template["primary_color"])
    secondary = str(template["secondary_color"])
    accent = str(template["accent_color"])

    if align == "center":
        _draw_centered(draw, (0, int(template["brand_y"])), width, brand, brand_font, primary)
        if eyebrow:
            _draw_centered(draw, (0, int(template["brand_y"]) + 70), width, eyebrow, eyebrow_font, accent)
        headline_lines = _wrap_text(draw, headline, headline_font, width - margin * 2)
        y = int(template["headline_y"])
        for line in headline_lines[:3]:
            _draw_centered(draw, (0, y), width, line, headline_font, primary)
            y += int(headline_font.size * 1.05)
        y = int(template["subhead_y"])
        for line in _wrap_text(draw, subhead, subhead_font, width - margin * 2)[:2]:
            _draw_centered(draw, (0, y), width, line, subhead_font, secondary)
            y += int(subhead_font.size * 1.2)
    else:
        draw.text((margin, int(template["brand_y"])), brand, fill=primary, font=brand_font)
        if eyebrow:
            draw.text((margin, int(template["brand_y"]) + 70), eyebrow, fill=accent, font=eyebrow_font)
        y = int(template["headline_y"])
        for line in _wrap_text(draw, headline, headline_font, width - margin * 2)[:3]:
            draw.text((margin, y), line, fill=primary, font=headline_font)
            y += int(headline_font.size * 1.05)
        draw.text((margin, int(template["subhead_y"])), subhead, fill=secondary, font=subhead_font)

    _draw_panel(draw, width, int(template["hud_y"]), hud_lines, hud_font, template)
    _draw_cta(draw, width, int(template["cta_y"]), cta, cta_font, template)
    return base.convert("RGB")


def _cover_crop(image: Image.Image, width: int, height: int) -> Image.Image:
    image = image.convert("RGB")
    source_width, source_height = image.size
    scale = max(width / source_width, height / source_height)
    resized = image.resize((int(source_width * scale), int(source_height * scale)), Image.Resampling.LANCZOS)
    left = max(0, (resized.width - width) // 2)
    top = max(0, (resized.height - height) // 2)
    return resized.crop((left, top, left + width, top + height))


def _apply_tint_and_gradient(image: Image.Image, template: dict) -> Image.Image:
    width, height = image.size
    tint = Image.new("RGBA", image.size, _hex_to_rgba(str(template["tint_color"]), float(template["tint_opacity"])))
    base = Image.alpha_composite(image, tint)
    gradient = Image.new("RGBA", image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(gradient)
    color = _hex_to_rgb(str(template["gradient_color"]))
    max_alpha = int(255 * float(template["gradient_opacity"]))
    for y in range(height):
        alpha = int(max_alpha * (y / max(height - 1, 1)))
        draw.line([(0, y), (width, y)], fill=(*color, alpha))
    return Image.alpha_composite(base, gradient)


def _draw_panel(draw: ImageDraw.ImageDraw, width: int, y: int, hud_lines: list[str], font: ImageFont.ImageFont, template: dict) -> None:
    x0 = 64
    x1 = width - 64
    y1 = y + 150
    panel = _hex_to_rgba(str(template["panel_color"]), float(template["panel_opacity"]))
    draw.rounded_rectangle((x0, y, x1, y1), radius=20, fill=panel)
    hud_text = "  |  ".join(hud_lines[:3])
    wrapped = _wrap_text(draw, hud_text, font, x1 - x0 - 56)
    text_y = y + 38
    align = str(template.get("align", "left")).lower()
    for line in wrapped[:2]:
        if align == "center":
            text_width = _text_size(draw, line, font)[0]
            text_x = x0 + max(28, (x1 - x0 - text_width) // 2)
        else:
            text_x = x0 + 28
        draw.text((text_x, text_y), line, fill=str(template["secondary_color"]), font=font)
        text_y += int(font.size * 1.25)


def _draw_cta(draw: ImageDraw.ImageDraw, width: int, y: int, cta: str, font: ImageFont.ImageFont, template: dict) -> None:
    text_width = _text_size(draw, cta, font)[0]
    x0 = max(64, (width - text_width) // 2 - 42)
    x1 = min(width - 64, x0 + text_width + 84)
    draw.rounded_rectangle((x0, y, x1, y + 74), radius=18, fill=str(template["accent_color"]))
    draw.text((x0 + 42, y + 20), cta, fill="#071108", font=font)


def _draw_centered(
    draw: ImageDraw.ImageDraw,
    point: tuple[int, int],
    width: int,
    text: str,
    font: ImageFont.ImageFont,
    fill: str,
) -> None:
    text_width = _text_size(draw, text, font)[0]
    draw.text(((width - text_width) // 2, point[1]), text, fill=fill, font=font)


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = text.split()
    if not words:
        return [""]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if _text_size(draw, candidate, font)[0] <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def _text_size(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> tuple[int, int]:
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0], box[3] - box[1]


def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = BOLD_FONT_CANDIDATES if bold else FONT_CANDIDATES
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def _hex_to_rgba(value: str, opacity: float) -> tuple[int, int, int, int]:
    red, green, blue = _hex_to_rgb(value)
    return red, green, blue, max(0, min(255, int(255 * opacity)))


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    clean = value.strip().lstrip("#")
    if len(clean) != 6:
        return 255, 255, 255
    return int(clean[0:2], 16), int(clean[2:4], 16), int(clean[4:6], 16)


def _placeholder_background(width: int, height: int) -> Image.Image:
    image = Image.new("RGB", (width, height), "#111611")
    draw = ImageDraw.Draw(image)
    for y in range(height):
        green = 18 + int(44 * y / max(height - 1, 1))
        draw.line([(0, y), (width, y)], fill=(8, green, 18))
    for index in range(8):
        x = int(width * (0.12 + index * 0.11))
        draw.line([(x, 0), (x - 180, height)], fill=(40, 95, 55), width=6)
    return image
