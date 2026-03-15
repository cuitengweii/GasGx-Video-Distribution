from __future__ import annotations

import time
from typing import Any


def normalize_csv_list(value: Any) -> list[str]:
    text = str(value or "").replace("\n", ",")
    parts = [item.strip() for item in text.split(",") if item.strip()]
    output: list[str] = []
    seen: set[str] = set()
    for item in parts:
        token = item.lower()
        if token in seen:
            continue
        seen.add(token)
        output.append(item)
    return output


def now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")
