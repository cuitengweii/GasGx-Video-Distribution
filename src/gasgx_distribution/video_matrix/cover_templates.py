from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_COVER_TEMPLATE_ID = "industrial_engine_hook"


def default_cover_templates() -> dict[str, dict[str, Any]]:
    return {
        "industrial_engine_hook": {
            "name": "Gas Engine Hook",
            "brand": "GasGx",
            "eyebrow": "GAS ENGINE / GENERATOR SET",
        "cta": "",
            "tint_color": "#08120A",
            "tint_opacity": 0.58,
            "gradient_color": "#5DD62C",
            "gradient_opacity": 0.24,
            "accent_color": "#5DD62C",
            "primary_color": "#F4FFF0",
            "secondary_color": "#C8D4C4",
            "panel_color": "#071108",
            "panel_opacity": 0.76,
            "brand_y": 118,
            "headline_y": 300,
            "subhead_y": 560,
            "hud_y": 1568,
            "cta_y": 1740,
            "align": "left",
        },
        "generator_set_roi": {
            "name": "Generator Set ROI",
            "brand": "GasGx Matrix",
            "eyebrow": "ONSITE POWER ROI",
            "cta": "CALCULATE FIELD POWER VALUE",
            "tint_color": "#071015",
            "tint_opacity": 0.62,
            "gradient_color": "#00A3FF",
            "gradient_opacity": 0.22,
            "accent_color": "#5DD62C",
            "primary_color": "#FFFFFF",
            "secondary_color": "#B9C8D2",
            "panel_color": "#071108",
            "panel_opacity": 0.70,
            "brand_y": 140,
            "headline_y": 620,
            "subhead_y": 860,
            "hud_y": 1320,
            "cta_y": 1660,
            "align": "center",
        },
        "roi_proof": {
            "name": "ROI Proof",
            "brand": "GasGx Matrix",
            "eyebrow": "ROI SIGNAL",
            "cta": "CALCULATE ONSITE VALUE",
            "tint_color": "#071015",
            "tint_opacity": 0.62,
            "gradient_color": "#00A3FF",
            "gradient_opacity": 0.22,
            "accent_color": "#5DD62C",
            "primary_color": "#FFFFFF",
            "secondary_color": "#B9C8D2",
            "panel_color": "#071108",
            "panel_opacity": 0.70,
            "brand_y": 140,
            "headline_y": 660,
            "subhead_y": 870,
            "hud_y": 1320,
            "cta_y": 1660,
            "align": "center",
        },
        "field_deployment": {
            "name": "Field Deployment",
            "brand": "GasGx",
            "eyebrow": "REMOTE SITE / SKID-MOUNTED POWER",
            "cta": "SEE FIELD DEPLOYMENT CASES",
            "tint_color": "#0B1110",
            "tint_opacity": 0.54,
            "gradient_color": "#5DD62C",
            "gradient_opacity": 0.20,
            "accent_color": "#5DD62C",
            "primary_color": "#F4FFF0",
            "secondary_color": "#D6DED2",
            "panel_color": "#071108",
            "panel_opacity": 0.72,
            "brand_y": 126,
            "headline_y": 430,
            "subhead_y": 700,
            "hud_y": 1460,
            "cta_y": 1700,
            "align": "left",
        },
        "top_banner_engine": {
            "name": "Top Banner Engine",
            "brand": "GasGx",
            "eyebrow": "GAS ENGINE POWER MODULE",
            "cta": "BOOK A FIELD POWER REVIEW",
            "tint_color": "#071108",
            "tint_opacity": 0.60,
            "gradient_color": "#5DD62C",
            "gradient_opacity": 0.12,
            "accent_color": "#5DD62C",
            "primary_color": "#FFFFFF",
            "secondary_color": "#D6DED2",
            "panel_color": "#0A140B",
            "panel_opacity": 0.82,
            "brand_y": 76,
            "headline_y": 210,
            "subhead_y": 430,
            "hud_y": 1520,
            "cta_y": 1720,
            "align": "center",
        },
        "lower_third_generator": {
            "name": "Lower Third Generator",
            "brand": "GasGx",
            "eyebrow": "GENERATOR SET / ONSITE LOAD",
            "cta": "FOLLOW FOR GENERATOR SET CASES",
            "tint_color": "#10130D",
            "tint_opacity": 0.46,
            "gradient_color": "#FF9900",
            "gradient_opacity": 0.14,
            "accent_color": "#5DD62C",
            "primary_color": "#FFFFFF",
            "secondary_color": "#E0E0E0",
            "panel_color": "#071108",
            "panel_opacity": 0.84,
            "brand_y": 126,
            "headline_y": 980,
            "subhead_y": 1230,
            "hud_y": 1420,
            "cta_y": 1690,
            "align": "left",
        },
        "manufacturing_floor": {
            "name": "Manufacturing Floor",
            "brand": "GasGx",
            "eyebrow": "ENGINEERED FOR MANUFACTURING SITES",
            "cta": "FOLLOW GASGX",
            "tint_color": "#10130D",
            "tint_opacity": 0.56,
            "gradient_color": "#FF9900",
            "gradient_opacity": 0.16,
            "accent_color": "#5DD62C",
            "primary_color": "#FFFFFF",
            "secondary_color": "#E0E0E0",
            "panel_color": "#071108",
            "panel_opacity": 0.78,
            "brand_y": 160,
            "headline_y": 520,
            "subhead_y": 780,
            "hud_y": 1500,
            "cta_y": 1710,
            "align": "left",
        },
        "center_spec_card": {
            "name": "Center Spec Card",
            "brand": "GasGx",
            "eyebrow": "MANUFACTURING SITE POWER",
            "cta": "CHECK GAS ENGINE ROI",
            "tint_color": "#061114",
            "tint_opacity": 0.64,
            "gradient_color": "#00A3FF",
            "gradient_opacity": 0.16,
            "accent_color": "#5DD62C",
            "primary_color": "#FFFFFF",
            "secondary_color": "#C8D4C4",
            "panel_color": "#06100A",
            "panel_opacity": 0.80,
            "brand_y": 220,
            "headline_y": 560,
            "subhead_y": 820,
            "hud_y": 1380,
            "cta_y": 1620,
            "align": "center",
        },
        "data_wall_power": {
            "name": "Data Wall Power",
            "brand": "GasGx Matrix",
            "eyebrow": "ENGINE DATA / POWER OUTPUT / HASHRATE",
            "cta": "SAVE THIS POWER MODEL",
            "tint_color": "#080D09",
            "tint_opacity": 0.58,
            "gradient_color": "#5DD62C",
            "gradient_opacity": 0.28,
            "accent_color": "#5DD62C",
            "primary_color": "#EFFFF0",
            "secondary_color": "#B9C8D2",
            "panel_color": "#071108",
            "panel_opacity": 0.88,
            "brand_y": 96,
            "headline_y": 700,
            "subhead_y": 920,
            "hud_y": 1180,
            "cta_y": 1710,
            "align": "left",
        },
        "brand_close": {
            "name": "Brand Close",
            "brand": "GasGx",
            "eyebrow": "MODULAR GAS-TO-COMPUTE",
            "cta": "DEPLOY POWER WHERE GAS LIVES",
            "tint_color": "#0E1A10",
            "tint_opacity": 0.50,
            "gradient_color": "#5DD62C",
            "gradient_opacity": 0.18,
            "accent_color": "#5DD62C",
            "primary_color": "#F4FFF0",
            "secondary_color": "#FFFFFF",
            "panel_color": "#071108",
            "panel_opacity": 0.82,
            "brand_y": 180,
            "headline_y": 760,
            "subhead_y": 940,
            "hud_y": 1500,
            "cta_y": 1700,
            "align": "center",
        },
    }


def load_cover_templates(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return default_cover_templates()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(key): dict(value) for key, value in payload.items()}


def require_cover_template(templates: dict[str, dict[str, Any]], template_id: str) -> dict[str, Any]:
    if template_id not in templates:
        choices = ", ".join(sorted(templates)) or "<none>"
        raise KeyError(f"Unknown cover template '{template_id}'. Available cover templates: {choices}")
    return coerce_cover_template(templates[template_id])


def coerce_cover_template(template: dict[str, Any] | None) -> dict[str, Any]:
    merged = default_cover_templates()[DEFAULT_COVER_TEMPLATE_ID].copy()
    if template:
        merged.update(template)
    merged.setdefault("mask_mode", "bottom_gradient")
    merged.setdefault("mask_color", merged.get("gradient_color") or merged.get("tint_color") or "#071015")
    merged.setdefault("mask_opacity", merged.get("gradient_opacity", merged.get("tint_opacity", 0.35)))
    merged.setdefault("cover_layout", "profile")
    merged.setdefault("single_cover_logo_text", "GasGx")
    merged.setdefault("single_cover_slogan_text", "终结废气 | 重塑能源 | 就地变现")
    merged.setdefault("single_cover_title_text", "全球领先的搁浅天然气算力变现引擎")
    merged.setdefault("single_cover_logo_font_size", 84)
    merged.setdefault("single_cover_slogan_font_size", 60)
    merged.setdefault("single_cover_title_font_size", 54)
    merged.setdefault("tile_brand_text", "GasGx")
    merged.setdefault("tile_tagline_text", "终结废气 | 重塑能源 | 就地变现")
    merged.setdefault(
        "tile_titles_text",
        "\n".join(
            [
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
        ),
    )
    return merged
