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
        "name": "Impact HUD",
        "show_hud": True,
        "show_slogan": True,
        "show_title": True,
        "hud_bar_y": 1804,
        "hud_bar_height": 116,
        "hud_x": 42,
        "hud_y": 1842,
        "hud_font_size": 32,
        "slogan_x": 42,
        "slogan_y": 48,
        "slogan_font_size": 58,
        "title_x": 42,
        "title_y": 118,
        "title_font_size": 26,
        "hud_bar_color": "#0E1A10",
        "hud_bar_opacity": 0.38,
        "primary_color": "#5DD62C",
        "secondary_color": "#FFFFFF",
    }


def coerce_template(template: dict[str, Any] | None) -> dict[str, Any]:
    merged = default_template()
    if template:
        merged.update(template)
    return merged
