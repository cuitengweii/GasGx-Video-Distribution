from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_TEMPLATE_ID = "impact_hud"


def load_templates(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {DEFAULT_TEMPLATE_ID: default_template()}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(key): dict(value) for key, value in payload.items()}


def save_templates(path: Path, templates: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(templates, indent=2, ensure_ascii=False), encoding="utf-8")


def default_template() -> dict[str, Any]:
    return {
        "name": "视频叠层模板 01 · 深绿玻璃",
        "show_hud": True,
        "show_slogan": True,
        "show_title": True,
        "hud_bar_y": 1306,
        "hud_bar_height": 136,
        "hud_bar_width": 920,
        "hud_bar_x": 80,
        "hud_x": 114,
        "hud_y": 1363,
        "hud_font_size": 30,
        "hud_text_width": 852,
        "hud_text_align": "center",
        "slogan_x": 152,
        "slogan_y": 761,
        "slogan_font_size": 56,
        "slogan_text_width": 776,
        "slogan_text_align": "center",
        "title_x": 168,
        "title_y": 528,
        "title_font_size": 38,
        "title_text_width": 744,
        "title_text_align": "center",
        "hud_bar_color": "#07100B",
        "hud_bar_opacity": 0.62,
        "hud_bar_radius": 24,
        "slogan_bg_x": 120,
        "slogan_bg_y": 700,
        "slogan_bg_width": 840,
        "slogan_bg_height": 160,
        "slogan_bg_color": "#07100B",
        "slogan_bg_opacity": 0.58,
        "slogan_bg_radius": 26,
        "title_bg_x": 144,
        "title_bg_y": 486,
        "title_bg_width": 792,
        "title_bg_height": 112,
        "title_bg_color": "#111811",
        "title_bg_opacity": 0.54,
        "title_bg_radius": 24,
        "primary_color": "#F5FAF3",
        "secondary_color": "#E6EEE3",
        "slogan_color": "#F5FAF3",
        "title_color": "#E6EEE3",
        "hud_color": "#E7EFE5",
        "slogan_text_style": "white-outline",
        "title_text_style": "outline",
        "hud_text_style": "soft-shadow",
    }


def coerce_template(template: dict[str, Any] | None) -> dict[str, Any]:
    merged = default_template()
    explicit = template or {}
    if explicit:
        merged.update(explicit)
    if "hud_color" not in explicit:
        merged["hud_color"] = merged.get("primary_color") or "#ffffff"
    return merged
