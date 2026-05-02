from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from .cover_templates import coerce_cover_template
from .models import VideoVariant
from .settings import ProjectSettings


FONT_CANDIDATES = (
    Path(r"C:\Windows\Fonts\msyh.ttc"),
    Path(r"C:\Windows\Fonts\simhei.ttf"),
    Path(r"C:\Windows\Fonts\simsun.ttc"),
    Path(r"C:\Windows\Fonts\NotoSansSC-VF.ttf"),
    Path(r"C:\Windows\Fonts\Noto Sans SC (TrueType).otf"),
    Path(r"C:\Windows\Fonts\arial.ttf"),
    Path(r"C:\Windows\Fonts\segoeui.ttf"),
)
BOLD_FONT_CANDIDATES = (
    Path(r"C:\Windows\Fonts\msyhbd.ttc"),
    Path(r"C:\Windows\Fonts\simhei.ttf"),
    Path(r"C:\Windows\Fonts\simsunb.ttf"),
    Path(r"C:\Windows\Fonts\Noto Sans SC Bold (TrueType).otf"),
    Path(r"C:\Windows\Fonts\Noto Sans SC Medium (TrueType).otf"),
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
    background: Image.Image | None = None,
) -> Image.Image:
    template = coerce_cover_template(template_config)
    if str(template.get("cover_layout") or "profile") == "single_video":
        return build_single_video_cover_image(settings, template, background, headline, subhead, hud_lines or [])
    return build_cover_tile_preview_image(settings, template_config, background=background)


def build_cover_tile_preview_image(
    settings: ProjectSettings,
    template_config: dict | None,
    background: Image.Image | None = None,
) -> Image.Image:
    return build_single_video_cover_image(settings, template_config, background=background)


def build_legacy_cover_tile_preview_image(
    settings: ProjectSettings,
    template_config: dict | None,
    background: Image.Image | None = None,
) -> Image.Image:
    template = coerce_cover_template(template_config)
    width = int(settings.target_width)
    height = int(settings.target_height)
    source = background if background is not None else _placeholder_background(width, height)
    base = _cover_crop(source, width, height).convert("RGBA")
    base = _apply_tile_mask(base, template)
    draw = ImageDraw.Draw(base)

    accent = str(template["accent_color"])
    align = str(template.get("tile_text_align", "left")).lower()
    x = 56 if align != "right" else width - 56
    anchor = "ra" if align == "right" else "la"
    brand_font = _load_font(48, bold=True)
    tagline_font = _load_font(26, bold=True)
    title_font = _load_font(int(template.get("tile_title_font_size", 14)) * 5, bold=True)
    titles = _tile_titles(str(template.get("tile_titles_text") or ""))

    copy_y = int(template.get("tile_copy_y", 8)) * 8
    draw.text((x, copy_y), str(template.get("tile_brand_text") or "GasGx"), fill=accent, font=brand_font, anchor=anchor)
    draw.text((x, copy_y + 62), str(template.get("tile_tagline_text") or "终结废气 | 重塑能源 | 就地变现"), fill=accent, font=tagline_font, anchor=anchor)
    text_y = copy_y + 150
    for line in _wrap_text(draw, titles[0], title_font, width - 96)[:4]:
        draw.text((x, text_y), line, fill="#ffffff", font=title_font, anchor=anchor)
        text_y += int(title_font.size * 1.15)

    like_font = _load_font(42, bold=True)
    draw.text((width - 220, height - 92), "♡ 128", fill="#ffffff", font=like_font)

    return base.convert("RGB")


def build_intro_cover_image(
    background: Image.Image,
    settings: ProjectSettings,
    template_config: dict | None,
    headline: str,
    subhead: str,
    hud_lines: list[str],
) -> Image.Image:
    template = coerce_cover_template(template_config)
    if str(template.get("cover_layout") or "profile") == "single_video":
        return build_single_video_cover_image(settings, template, background, headline, subhead, hud_lines)
    base = _cover_crop(background, settings.target_width, settings.target_height).convert("RGBA")
    base = _apply_tint_and_gradient(base, template)
    draw = ImageDraw.Draw(base)
    width, height = base.size
    align = str(template.get("align", "left"))
    margin = 72

    brand_font = _load_font(int(template.get("brand_font_size", 52)), bold=True)
    headline_font = _load_font(int(template.get("headline_font_size", 84)), bold=True)
    subhead_font = _load_font(int(template.get("subhead_font_size", 38)))

    brand = str(template.get("profile_brand_text") or template.get("brand", "GasGx"))
    subhead = str(template.get("profile_subhead_text") or subhead)
    primary = str(template["primary_color"])
    secondary = str(template["secondary_color"])

    if align == "center":
        _draw_centered(draw, (0, int(template["brand_y"]) + int(template.get("profile_brand_offset_y", 0))), width, brand, brand_font, primary)
        headline_lines = _wrap_text(draw, headline, headline_font, width - margin * 2)
        y = int(template["headline_y"]) + int(template.get("profile_headline_offset_y", 0))
        for line in headline_lines[:3]:
            _draw_centered(draw, (0, y), width, line, headline_font, primary)
            y += int(headline_font.size * 1.05)
        y = int(template["subhead_y"]) + int(template.get("profile_subhead_offset_y", 0))
        for line in _wrap_text(draw, subhead, subhead_font, width - margin * 2)[:2]:
            _draw_centered(draw, (0, y), width, line, subhead_font, secondary)
            y += int(subhead_font.size * 1.2)
    else:
        draw.text((margin, int(template["brand_y"]) + int(template.get("profile_brand_offset_y", 0))), brand, fill=primary, font=brand_font)
        y = int(template["headline_y"]) + int(template.get("profile_headline_offset_y", 0))
        for line in _wrap_text(draw, headline, headline_font, width - margin * 2)[:3]:
            draw.text((margin, y), line, fill=primary, font=headline_font)
            y += int(headline_font.size * 1.05)
        draw.text((margin, int(template["subhead_y"]) + int(template.get("profile_subhead_offset_y", 0))), subhead, fill=secondary, font=subhead_font)

    return base.convert("RGB")


def build_single_video_cover_image(
    settings: ProjectSettings,
    template_config: dict | None,
    background: Image.Image | None = None,
    headline: str = "",
    subhead: str = "",
    hud_lines: list[str] | None = None,
) -> Image.Image:
    template = coerce_cover_template(template_config)
    width = int(settings.target_width)
    height = int(settings.target_height)
    source = background if background is not None else _placeholder_background(width, height)
    base = _cover_crop(source, width, height).convert("RGBA")
    base = _apply_single_cover_mask(base, template)
    draw = ImageDraw.Draw(base)

    logo_text = str(template.get("single_cover_logo_text") or "GasGx")
    slogan = str(template.get("single_cover_slogan_text") or "终结废气 | 重塑能源 | 就地变现")
    title = str(template.get("single_cover_title_text") or headline or subhead or "全球领先的搁浅天然气算力变现引擎")
    title = title.replace("\\n", "\n")

    logo_font = _load_font(max(24, int(float(template.get("single_cover_logo_font_size", 84)))), bold=True)
    slogan_font = _load_font(max(20, int(float(template.get("single_cover_slogan_font_size", 60)))), bold=True)
    title_font = _load_font(max(18, int(float(template.get("single_cover_title_font_size", 54)))), bold=True)
    _draw_single_cover_text(
        draw,
        template,
        "singleLogo",
        logo_text,
        logo_font,
        width,
        height,
        y_ratio=0.13,
        left_ratio=0.07,
        max_width_ratio=0.86,
        line_limit=1,
    )
    _draw_single_cover_text(
        draw,
        template,
        "singleSlogan",
        slogan,
        slogan_font,
        width,
        height,
        y_ratio=0.22,
        left_ratio=0.068,
        max_width_ratio=0.86,
        line_limit=2,
    )
    _draw_single_cover_text(
        draw,
        template,
        "singleTitle",
        title,
        title_font,
        width,
        height,
        y_ratio=0.266,
        left_ratio=0.068,
        max_width_ratio=0.88,
        line_limit=4,
    )

    return base.convert("RGB")


def _draw_single_cover_text(
    draw: ImageDraw.ImageDraw,
    template: dict,
    target: str,
    text: str,
    font: ImageFont.ImageFont,
    width: int,
    height: int,
    *,
    y_ratio: float,
    left_ratio: float,
    max_width_ratio: float,
    line_limit: int,
) -> None:
    align = _template_text_align(template, target)
    max_width = int(width * max_width_ratio)
    anchor_x = _single_cover_anchor_x(width, align, left_ratio)
    y = _single_cover_y(template, target, height, y_ratio)
    fill = str(template.get(f"{target}_color") or "#ffffff")
    lines = _wrap_multiline_text(draw, str(text or ""), font, max_width)[:line_limit]
    for line in lines:
        x = _aligned_text_x(draw, line, font, anchor_x, align)
        _draw_text_shadow(draw, (x, y), line, font, fill)
        y += int(font.size * 1.16)


def _template_text_align(template: dict, target: str) -> str:
    align = str(template.get(f"{target}_text_align") or "left").lower()
    return align if align in {"left", "center", "right"} else "left"


def _single_cover_anchor_x(width: int, align: str, left_ratio: float) -> int:
    if align == "center":
        return width // 2
    if align == "right":
        return width - int(width * 0.068)
    return int(width * left_ratio)


def _single_cover_y(template: dict, target: str, height: int, y_ratio: float) -> int:
    legacy_y_keys = {
        "singleLogo": "single_cover_logo_y",
        "singleSlogan": "single_cover_slogan_y",
        "singleTitle": "single_cover_title_y",
    }
    base_y = _coerce_float(template.get(legacy_y_keys[target]), height * y_ratio)
    offset_y = _coerce_float(template.get(f"{target}_offset_y"), 0)
    return int(base_y + offset_y * height / 852)


def _aligned_text_x(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, anchor_x: int, align: str) -> int:
    text_width = _text_size(draw, text, font)[0]
    if align == "center":
        return int(anchor_x - text_width / 2)
    if align == "right":
        return int(anchor_x - text_width)
    return anchor_x


def _coerce_float(value: object, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


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
    mode = str(template.get("mask_mode") or "bottom_gradient")
    if mode == "none":
        return image
    color = _hex_to_rgb(str(template.get("mask_color") or template.get("gradient_color") or template.get("tint_color")))
    max_alpha = int(255 * float(template.get("mask_opacity", template.get("gradient_opacity", template.get("tint_opacity", 0.35)))))
    max_alpha = max(0, min(255, max_alpha))
    mask = Image.new("RGBA", image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(mask)
    if mode == "full":
        draw.rectangle((0, 0, width, height), fill=(*color, max_alpha))
    else:
        for y in range(height):
            ratio = y / max(height - 1, 1)
            if mode == "top_gradient":
                ratio = 1 - ratio
            elif mode == "dual_gradient":
                ratio = abs(ratio - 0.5) * 2
            alpha = int(max_alpha * ratio)
            draw.line([(0, y), (width, y)], fill=(*color, alpha))
    return Image.alpha_composite(image, mask)


def _apply_tile_mask(image: Image.Image, template: dict) -> Image.Image:
    mode = str(template.get("mask_mode") or "bottom_gradient")
    if mode == "none":
        return image
    width, height = image.size
    color = _hex_to_rgb(str(template.get("mask_color") or template.get("gradient_color") or template.get("tint_color")))
    max_alpha = int(255 * float(template.get("mask_opacity", template.get("gradient_opacity", template.get("tint_opacity", 0.35)))))
    max_alpha = max(0, min(255, max_alpha))
    mask = Image.new("RGBA", image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(mask)
    if mode == "full":
        draw.rectangle((0, 0, width, height), fill=(*color, max_alpha))
    else:
        for y in range(height):
            ratio = y / max(height - 1, 1)
            if mode == "top_gradient":
                ratio = 1 - ratio
            elif mode == "dual_gradient":
                ratio = abs(ratio - 0.5) * 2
            alpha = int(max_alpha * ratio)
            draw.line([(0, y), (width, y)], fill=(*color, alpha))
    return Image.alpha_composite(image, mask)


def _apply_single_cover_mask(image: Image.Image, template: dict) -> Image.Image:
    width, height = image.size
    mask = Image.new("RGBA", image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(mask)
    draw.rectangle((0, 0, width, int(height * 0.18)), fill=(0, 0, 0, 220))
    for y in range(int(height * 0.18), int(height * 0.52)):
        ratio = (y - height * 0.18) / max(height * 0.34, 1)
        alpha = int(220 * (1 - ratio))
        draw.line([(0, y), (width, y)], fill=(0, 0, 0, max(0, alpha)))
    for x in range(width):
        ratio = x / max(width - 1, 1)
        alpha = int(72 * (1 - ratio))
        draw.line([(x, 0), (x, height)], fill=(0, 0, 0, max(0, alpha)))
    mode = str(template.get("mask_mode") or "bottom_gradient")
    if mode != "none":
        color = _hex_to_rgb(str(template.get("mask_color") or template.get("gradient_color") or template.get("tint_color")))
        max_alpha = int(255 * float(template.get("mask_opacity", template.get("gradient_opacity", template.get("tint_opacity", 0.35)))))
        max_alpha = max(0, min(255, max_alpha))
        if mode == "full":
            draw.rectangle((0, 0, width, height), fill=(*color, max_alpha))
        else:
            for y in range(height):
                ratio = y / max(height - 1, 1)
                if mode == "top_gradient":
                    ratio = 1 - ratio
                elif mode == "dual_gradient":
                    ratio = abs(ratio - 0.5) * 2
                alpha = int(max_alpha * ratio)
                draw.line([(0, y), (width, y)], fill=(*color, alpha))
    return Image.alpha_composite(image, mask)


def _tile_titles(value: str) -> list[str]:
    titles = [line.strip() for line in value.splitlines() if line.strip()]
    return titles or [
        "燃气发电机组并网测试",
        "油田伴生气资源再利用",
        "移动式算力中心部署",
        "野外发电设备日常维护",
        "零燃除：变废为宝",
        "集装箱数据中心内景",
        "高效燃气轮机运行状态",
        "夜间井场持续发电作业",
        "极寒环境设备启动测试",
    ]


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


def _draw_text_shadow(
    draw: ImageDraw.ImageDraw,
    point: tuple[int, int],
    text: str,
    font: ImageFont.ImageFont,
    fill: str,
) -> None:
    x, y = point
    draw.text((x + 4, y + 5), text, fill=(0, 0, 0, 170), font=font)
    draw.text((x, y), text, fill=fill, font=font)


def _wrap_multiline_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lines.extend(_wrap_text(draw, line, font, max_width))
    return lines or [""]


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
