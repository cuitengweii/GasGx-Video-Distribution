from __future__ import annotations

import html
from typing import Any, Iterable, Mapping


def _render_item(item: Any) -> str:
    if isinstance(item, str):
        return f"- {html.escape(item)}"
    if isinstance(item, Mapping):
        label = html.escape(str(item.get("label") or "").strip())
        value = item.get("value")
        text = item.get("text")
        url = str(item.get("url") or "").strip()
        if value is not None and str(value).strip():
            body = html.escape(str(value))
        elif text is not None and str(text).strip():
            body = html.escape(str(text))
        elif url:
            body = html.escape(url)
        else:
            body = ""
        if label and body:
            return f"- <b>{label}</b>: {body}"
        if body:
            return f"- {body}"
    return ""


def build_telegram_card(kind: str, payload: Mapping[str, Any], actions: Iterable[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    title = str(payload.get("title") or kind).strip()
    subtitle = str(payload.get("subtitle") or "").strip()
    lines = [f"<b>{html.escape(title)}</b>"]
    if subtitle:
        lines.append(html.escape(subtitle))

    for section in payload.get("sections") or []:
        if not isinstance(section, Mapping):
            continue
        section_title = str(section.get("title") or "").strip()
        if section_title:
            lines.append("")
            lines.append(f"<b>{html.escape(section_title)}</b>")
        for item in section.get("items") or []:
            rendered = _render_item(item)
            if rendered:
                lines.append(rendered)

    inline_keyboard: list[list[dict[str, str]]] = []
    for action in actions or []:
        if not isinstance(action, Mapping):
            continue
        text = str(action.get("text") or "").strip()
        url = str(action.get("url") or "").strip()
        if text and url:
            inline_keyboard.append([{"text": text, "url": url}])

    return {
        "text": "\n".join(lines).strip(),
        "parse_mode": "HTML",
        "reply_markup": {"inline_keyboard": inline_keyboard} if inline_keyboard else {},
    }
