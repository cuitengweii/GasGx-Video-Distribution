from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_UI_STATE: dict[str, Any] = {
    "output_count": 30,
    "max_workers": 3,
    "output_options": ["mp4"],
    "template_id": "impact_hud",
    "cover_template_id": "industrial_engine_hook",
    "copy_language": "zh",
    "source_mode": "Category folders",
    "active_category_ids": None,
    "video_duration_min": 8.0,
    "video_duration_max": 12.0,
    "headline": "Gas Engines That Turn Field Gas Into Power",
    "subhead": "Generator sets for onsite Bitcoin and industrial load",
    "hud_text": "Gas Engine -> Generator Set -> Power Output\nField Gas -> Stable Load -> Hashrate",
    "follow_text": "Follow GasGx for more field power cases",
    "bgm_source": "Local library",
    "bgm_library_id": "",
    "composition_customized": False,
    "composition_sequence": [
        {"category_id": "category_A", "duration": 1.5},
        {"category_id": "category_B", "duration": 3.4},
        {"category_id": "category_A", "duration": 1.5},
        {"category_id": "category_C", "duration": 3.0},
    ],
}


def load_ui_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return DEFAULT_UI_STATE.copy()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return DEFAULT_UI_STATE.copy()
    state = DEFAULT_UI_STATE.copy()
    state.update(payload)
    return state


def save_ui_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
