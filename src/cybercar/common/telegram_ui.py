from __future__ import annotations

import html
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .telegram_api import call_telegram_api


HOME_CALLBACK_PREFIX = "tgux"
_HOME_STATE_HISTORY_LIMIT = 8
_VARIATION_SELECTOR = "\ufe0f"
_CUSTOM_EMOJI_TAG_RE = re.compile(r"<tg-emoji\b[^>]*>(.*?)</tg-emoji>", flags=re.IGNORECASE | re.DOTALL)
_CUSTOM_EMOJI_ENABLED = False

_BOT_META: dict[str, dict[str, str]] = {
    "cybercar": {"name": "CyberCar", "home_title": "控制台"},
    "gasgx": {"name": "GasGx", "home_title": "控制台"},
    "manager": {"name": "Bot Manager", "home_title": "管理台"},
}

_KIND_META: dict[str, dict[str, str]] = {
    "collect_start": {"title": "采集开始", "emoji": "📲"},
    "collect_result": {"title": "采集结果", "emoji": "📋"},
    "collect_summary": {"title": "采集完成", "emoji": "✅"},
    "login_qr": {"title": "登录提醒", "emoji": "🔐"},
    "publish_result": {"title": "发布结果", "emoji": "🚀"},
    "alert": {"title": "异常提醒", "emoji": "❌"},
}

_STATUS_EMOJI: dict[str, str] = {
    "running": "⏳",
    "queued": "🕓",
    "success": "✅",
    "draft": "📝",
    "failed": "❌",
    "blocked": "❌",
    "login_required": "🔐",
    "alert": "❌",
}

_CUSTOM_EMOJI_BY_TOKEN: dict[str, str] = {
    "⏳": "5312016608254762256",
    "❌": "5379748062124056162",
    "🏠": "5312486108309757006",
    "📲": "5309965701241379366",
    "🕓": "5309984423003823246",
    "📝": "5373251851074415873",
    "📋": "5368653135101310687",
    "✅": "5357315181649076022",
    "🔐": "5409357944619802453",
    "🚀": "5310228579009699834",
}

_CUSTOM_EMOJI_TOKENS: tuple[str, ...] = tuple(
    sorted({str(token or "") for token in _CUSTOM_EMOJI_BY_TOKEN.keys() if str(token or "")}, key=len, reverse=True)
)
_HUMAN_FIRST_SECTION_TITLES = {
    "人工关注": 0,
    "失败原因": 1,
    "处理建议": 2,
    "结果说明": 3,
    "执行摘要": 4,
    "执行结果": 5,
}
_MACHINE_LAST_SECTION_TITLES = {
    "机器信息": 100,
    "运行上下文": 101,
    "任务日志": 102,
    "任务标识": 103,
    "菜单链路": 104,
}
_FAILURE_DETAIL_SECTION_TITLES = {"失败原因", "结果说明", "处理建议"}


def _escape_text(value: Any) -> str:
    return html.escape(str(value or "").strip())


def _normalize_emoji_token(value: str) -> str:
    return str(value or "").replace(_VARIATION_SELECTOR, "").strip()


def _strip_custom_emoji_markup(text: str) -> str:
    raw = str(text or "")
    if "<tg-emoji" not in raw.lower():
        return raw
    return _CUSTOM_EMOJI_TAG_RE.sub(lambda match: html.unescape(str(match.group(1) or "")), raw)


def _render_emoji(value: Any) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    if "<tg-emoji" in token.lower():
        return _strip_custom_emoji_markup(token)
    emoji_id = _CUSTOM_EMOJI_BY_TOKEN.get(token) or _CUSTOM_EMOJI_BY_TOKEN.get(_normalize_emoji_token(token))
    if not emoji_id or not _CUSTOM_EMOJI_ENABLED:
        return token
    return f'<tg-emoji emoji-id="{emoji_id}">{html.escape(token)}</tg-emoji>'


def _render_inline_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "<tg-emoji" in text.lower():
        return html.escape(_strip_custom_emoji_markup(text))
    if not _CUSTOM_EMOJI_ENABLED:
        return html.escape(text)
    placeholders: dict[str, str] = {}
    work = text
    for idx, token in enumerate(_CUSTOM_EMOJI_TOKENS):
        if not token or token not in work:
            continue
        marker = f"@@TGEMOJI{idx}@@"
        work = work.replace(token, marker)
        placeholders[marker] = _render_emoji(token)
    rendered = html.escape(work)
    for marker, replacement in placeholders.items():
        rendered = rendered.replace(html.escape(marker), replacement)
    return rendered


def _call_telegram_api_with_emoji_fallback(
    *,
    bot_token: str,
    method: str,
    params: Mapping[str, Any],
    timeout_seconds: int,
    use_post: bool,
) -> Mapping[str, Any]:
    payload = dict(params or {})
    text = str(payload.get("text") or "")
    try:
        return call_telegram_api(
            bot_token=bot_token,
            method=method,
            params=payload,
            timeout_seconds=timeout_seconds,
            use_post=use_post,
        )
    except Exception:
        if "<tg-emoji" not in text.lower():
            raise
        fallback_payload = dict(payload)
        fallback_payload["text"] = _strip_custom_emoji_markup(text)
        if str(fallback_payload.get("text") or "") == text:
            raise
        return call_telegram_api(
            bot_token=bot_token,
            method=method,
            params=fallback_payload,
            timeout_seconds=timeout_seconds,
            use_post=use_post,
        )


def _bot_name(bot_kind: str, fallback: str = "CyberCar") -> str:
    token = str(bot_kind or "").strip().lower()
    meta = _BOT_META.get(token)
    if isinstance(meta, dict):
        return str(meta.get("name") or fallback)
    return fallback


def _bot_home_title(bot_kind: str) -> str:
    token = str(bot_kind or "").strip().lower()
    meta = _BOT_META.get(token, {})
    return str(meta.get("home_title") or "控制台")


def _format_value(item: Mapping[str, Any]) -> str:
    raw_value = str(item.get("value", "") or "").strip()
    link = str(item.get("url", "") or "").strip()
    text = str(item.get("text", "") or "").strip() or raw_value
    style = str(item.get("style", "") or "").strip().lower()
    rendered_text = _render_inline_text(text)
    rendered_value = _render_inline_text(raw_value)
    escaped_value = _escape_text(raw_value)
    if link:
        label = rendered_text or rendered_value or escaped_value or "点击查看"
        return f'<a href="{html.escape(link, quote=True)}">{label}</a>'
    if style == "code":
        return f"<code>{escaped_value}</code>"
    if style == "bold":
        return f"<b>{rendered_value or escaped_value}</b>"
    if style == "italic":
        return f"<i>{rendered_value or escaped_value}</i>"
    return rendered_value or escaped_value


def _render_section_items(items: Sequence[Any]) -> list[str]:
    lines: list[str] = []
    for item in items:
        if isinstance(item, Mapping):
            label = str(item.get("label", "") or "").strip()
            formatted = _format_value(item)
            if label:
                lines.append(f"• <b>{html.escape(label)}</b>：{formatted}")
            elif formatted:
                lines.append(f"• {formatted}")
            continue
        text = _render_inline_text(item)
        if text:
            lines.append(f"• {text}")
    return lines


def _render_sections(sections: Sequence[Mapping[str, Any]]) -> list[str]:
    lines: list[str] = []
    for section in sections:
        title = str(section.get("title", "") or "").strip()
        if not title:
            continue
        icon = _render_emoji(section.get("emoji", ""))
        header = f"<b>{icon} {html.escape(title)}</b>" if icon else f"<b>{html.escape(title)}</b>"
        if lines:
            lines.append("")
        lines.append(header)
        lines.extend(_render_section_items(section.get("items", [])))
    return lines


def _failure_marker_for_text(text: str, *, fallback: str = "⚠️") -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return fallback
    if any(token in lowered for token in ("timeout", "network", "transport", "连接", "代理", "connection", "proxy")):
        return "🌐"
    if any(token in lowered for token in ("登录", "扫码", "未登录", "login", "sign in", "qr")):
        return "🔐"
    if any(token in lowered for token in ("telegram", "bot_token", "chat_id", "notify", "消息发送", "卡片发送")):
        return "📨"
    if any(token in lowered for token in ("素材", "候选", "下载", "文件", "链接", "source", "candidate", "download", "upload")):
        return "📦"
    if any(
        token in lowered
        for token in (
            "平台",
            "发布",
            "抖音",
            "小红书",
            "快手",
            "微信",
            "视频号",
            "bilibili",
            "wechat",
            "douyin",
            "xiaohongshu",
            "kuaishou",
        )
    ):
        return "📣"
    if any(token in lowered for token in ("跳过", "skip", "duplicate", "已发布")):
        return "⏭️"
    return fallback


def _decorate_failure_sections(status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    status_token = str(status or "").strip().lower()
    normalized = [dict(section) for section in sections if isinstance(section, Mapping)]
    if status_token not in {"failed", "blocked", "alert", "login_required"}:
        return normalized
    decorated: list[dict[str, Any]] = []
    for section in normalized:
        title = str(section.get("title") or "").strip()
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        if title not in _FAILURE_DETAIL_SECTION_TITLES:
            decorated.append(section)
            continue
        new_items: list[Any] = []
        default_marker = "🛠️" if title == "处理建议" else "⚠️"
        for item in items:
            if isinstance(item, Mapping):
                label = str(item.get("label") or "").strip()
                value = str(item.get("value") or item.get("text") or "").strip()
                marker = default_marker if title == "处理建议" else _failure_marker_for_text(f"{label} {value}", fallback=default_marker)
                updated = dict(item)
                updated["label"] = f"{marker} {label or '详情'}".strip()
                new_items.append(updated)
            else:
                marker = default_marker if title == "处理建议" else _failure_marker_for_text(str(item or ""), fallback=default_marker)
                new_items.append(f"{marker} {str(item or '').strip()}".strip())
        decorated.append({**section, "items": new_items})
    return decorated


def _pick_failure_header_emoji(status: str, sections: Sequence[Mapping[str, Any]], fallback: str) -> str:
    status_token = str(status or "").strip().lower()
    if status_token not in {"failed", "blocked", "alert", "login_required"}:
        return str(fallback or "").strip()
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        title = str(section.get("title") or "").strip()
        if title not in _FAILURE_DETAIL_SECTION_TITLES:
            continue
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        for item in items:
            if isinstance(item, Mapping):
                label = str(item.get("label") or "").strip()
                marker = label.split(" ", 1)[0].strip()
                if marker:
                    return marker
            else:
                marker = str(item or "").strip().split(" ", 1)[0].strip()
                if marker:
                    return marker
    return str(fallback or "").strip()


def _extract_primary_result_signal(sections: Sequence[Mapping[str, Any]]) -> str:
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        title = str(section.get("title") or "").strip()
        if title not in {"人工关注", "执行摘要"}:
            continue
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            label = str(item.get("label") or "").strip()
            if label not in {"执行结果", "结果", "平台摘要"}:
                continue
            value = str(item.get("value") or item.get("text") or "").strip()
            if value:
                return value
    return ""


def _extract_platform_summary_signal(sections: Sequence[Mapping[str, Any]]) -> str:
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            label = str(item.get("label") or "").strip()
            if label != "平台摘要":
                continue
            value = str(item.get("value") or item.get("text") or "").strip()
            if value:
                return value
    return ""


def _summarize_platform_subtitle(signal: str) -> str:
    text = str(signal or "").strip()
    if not text:
        return ""
    counts = {
        "success": text.count("✅"),
        "failed": text.count("📣"),
        "login": text.count("🔐"),
        "skipped": text.count("⏭️"),
        "unknown": text.count("⚠️"),
    }
    parts: list[str] = []
    if counts["success"] > 0:
        parts.append(f"{counts['success']}个平台成功")
    if counts["failed"] > 0:
        parts.append(f"{counts['failed']}个平台失败")
    if counts["login"] > 0:
        parts.append(f"{counts['login']}个平台需登录")
    if counts["skipped"] > 0:
        parts.append(f"{counts['skipped']}个平台跳过")
    if counts["unknown"] > 0:
        parts.append(f"{counts['unknown']}个平台待确认")
    return " / ".join(parts[:2])


def _compact_subtitle_text(subtitle: str) -> str:
    text = re.sub(r"\s+", " ", str(subtitle or "").strip())
    if not text:
        return ""
    text = text.replace("当前配置：", "配置：")
    text = text.replace("当前配置:", "配置：")
    if len(text) <= 28:
        return text
    return text[:25].rstrip() + "..."


def _decorate_card_subtitle(status: str, subtitle: str, sections: Sequence[Mapping[str, Any]]) -> str:
    platform_signal = _extract_platform_summary_signal(sections)
    platform_subtitle = _summarize_platform_subtitle(platform_signal)
    if platform_subtitle:
        return platform_subtitle
    return _compact_subtitle_text(subtitle)


def _decorate_positive_header(status: str, title: str, sections: Sequence[Mapping[str, Any]], fallback: str) -> tuple[str, str]:
    status_token = str(status or "").strip().lower()
    if status_token not in {"success", "done", "queued", "running"}:
        return str(fallback or "").strip(), str(title or "").strip()
    signal = _extract_primary_result_signal(sections)
    title_text = str(title or "").strip()
    signal_lower = signal.lower()
    if any(token in signal for token in ("部分", "待确认", "需处理")) or ("✅" in signal and any(token in signal for token in ("📣", "🔐", "⚠️", "⏭️"))):
        if "部分" not in title_text and "跳过" not in title_text:
            title_text = f"{title_text}（部分）"
        return "🟡", title_text
    if any(token in signal for token in ("跳过", "⏭️")):
        if "跳过" not in title_text:
            title_text = f"{title_text}（跳过）"
        return "⏭️", title_text
    if status_token == "queued":
        return "🕓", title_text
    if status_token == "running":
        return "⏳", title_text
    return str(fallback or "").strip(), title_text


def _prioritize_card_sections(status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = [dict(section) for section in sections if isinstance(section, Mapping)]
    if not normalized:
        return []
    status_token = str(status or "").strip().lower()

    def _section_rank(section: Mapping[str, Any], index: int) -> tuple[int, int]:
        title = str(section.get("title") or "").strip()
        if title in _MACHINE_LAST_SECTION_TITLES:
            return (_MACHINE_LAST_SECTION_TITLES[title], index)
        if status_token in {"failed", "blocked", "alert", "login_required"}:
            return (_HUMAN_FIRST_SECTION_TITLES.get(title, 50), index)
        if status_token in {"success", "done"}:
            if title in {"人工关注", "执行摘要"}:
                return (_HUMAN_FIRST_SECTION_TITLES.get(title, 10), index)
            if title == "执行结果":
                return (120, index)
        return (40, index)

    return [section for _, section in sorted(enumerate(normalized), key=lambda pair: _section_rank(pair[1], pair[0]))]


def _compact_sections_for_status(status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [dict(section) for section in sections if isinstance(section, Mapping)]
    status_token = str(status or "").strip().lower()
    if status_token not in {"success", "done"}:
        return normalized
    compacted: list[dict[str, Any]] = []
    deferred_machine_items: list[Any] = []
    for section in normalized:
        title = str(section.get("title") or "").strip()
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        if title == "人工关注" and len(items) > 3:
            compacted.append({**section, "items": items[:3]})
            deferred_machine_items.extend(items[3:])
            continue
        compacted.append(section)
    if deferred_machine_items:
        merged = False
        for section in compacted:
            if str(section.get("title") or "").strip() == "机器信息":
                current_items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
                section["items"] = [*current_items, *deferred_machine_items]
                merged = True
                break
        if not merged:
            compacted.append({"title": "机器信息", "emoji": "🤖", "items": deferred_machine_items})
    return compacted


def build_reply_markup(actions: Sequence[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    keyboard: list[list[dict[str, str]]] = []
    if not actions:
        return {"inline_keyboard": keyboard}
    grouped: dict[int, list[dict[str, str]]] = {}
    for action in actions:
        text = str(action.get("text", "") or "").strip()
        callback_data = str(action.get("callback_data", "") or "").strip()
        url = str(action.get("url", "") or "").strip()
        if not text:
            continue
        button: dict[str, str] = {"text": text}
        if url:
            button["url"] = url
        elif callback_data:
            button["callback_data"] = callback_data
        else:
            continue
        row = int(action.get("row", 0) or 0)
        grouped.setdefault(max(0, row), []).append(button)
    for row in sorted(grouped):
        keyboard.append(grouped[row])
    return {"inline_keyboard": keyboard}


def build_telegram_card(
    kind: str,
    payload: Mapping[str, Any],
    actions: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    token = str(kind or "").strip().lower()
    meta = _KIND_META.get(token, _KIND_META["alert"])
    status = str(payload.get("status", "") or "").strip().lower()
    emoji = str(payload.get("emoji", "") or "").strip() or _STATUS_EMOJI.get(status) or meta["emoji"]
    title = str(payload.get("title", "") or "").strip() or meta["title"]
    subtitle = str(payload.get("subtitle", "") or "").strip()
    bot_name = str(payload.get("bot_name", "") or "").strip() or "CyberCar"
    sections = payload.get("sections", [])
    if not isinstance(sections, Iterable):
        sections = []
    sections = _prioritize_card_sections(
        status,
        _compact_sections_for_status(
            status,
            _decorate_failure_sections(status, _prioritize_card_sections(status, list(sections))),
        ),
    )
    emoji = _pick_failure_header_emoji(status, sections, emoji)
    emoji, title = _decorate_positive_header(status, title, sections, emoji)
    subtitle = _decorate_card_subtitle(status, subtitle, sections)
    header = [f"<b>{_render_emoji(emoji)} {html.escape(bot_name)}｜{_render_inline_text(title)}</b>"]
    if subtitle:
        header.append(f"<i>{_render_inline_text(subtitle)}</i>")
    text_lines = list(header)
    rendered_sections = _render_sections(list(sections))
    if rendered_sections:
        text_lines.append("")
        text_lines.extend(rendered_sections)
    render_mode = str(payload.get("mode", "") or "").strip().lower()
    if render_mode not in {"photo", "text"}:
        render_mode = "photo" if payload.get("cover_image") or payload.get("qr_image") else "text"
    return {
        "kind": token,
        "mode": render_mode,
        "text": "\n".join(text_lines).strip(),
        "reply_markup": build_reply_markup(actions),
        "parse_mode": "HTML",
        "image": payload.get("cover_image") or payload.get("qr_image"),
    }


def build_telegram_home(
    bot_kind: str,
    payload: Mapping[str, Any],
    actions: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    token = str(bot_kind or "").strip().lower() or "cybercar"
    bot_name = _bot_name(token)
    title = str(payload.get("title", "") or "").strip() or _bot_home_title(token)
    subtitle = str(payload.get("subtitle", "") or "").strip()
    sections = payload.get("sections", [])
    if not isinstance(sections, Iterable):
        sections = []
    header = [f"<b>{_render_emoji('🏠')} {html.escape(bot_name)}｜{_render_inline_text(title)}</b>"]
    if subtitle:
        header.append(f"<i>{_render_inline_text(subtitle)}</i>")
    body = list(header)
    rendered_sections = _render_sections(list(sections))
    if rendered_sections:
        body.append("")
        body.extend(rendered_sections)
    return {
        "kind": "home",
        "mode": "text",
        "text": "\n".join(body).strip(),
        "reply_markup": build_reply_markup(actions),
        "parse_mode": "HTML",
        "image": None,
        "bot_kind": token,
    }


def build_action_feedback(
    status: str,
    title: str,
    subtitle: str = "",
    sections: Sequence[Mapping[str, Any]] | None = None,
    *,
    bot_name: str = "CyberCar",
) -> dict[str, Any]:
    token = str(status or "").strip().lower() or "success"
    return build_telegram_card(
        "alert" if token in {"failed", "blocked", "alert"} else "publish_result",
        {
            "status": token,
            "title": title,
            "subtitle": subtitle,
            "sections": list(sections or []),
            "bot_name": bot_name,
            "mode": "text",
        },
    )


def build_callback_toast(action: str, status: str, context: Any = None) -> str:
    act = str(action or "").strip().lower()
    state = str(status or "").strip().lower()
    ctx = str(context or "").strip()
    success_map = {
        "home": "已返回首页",
        "refresh_home": "首页已刷新",
        "process_status": "进度已刷新",
        "collect_now": "已开始采集",
        "publish_now": "已开始发布",
        "content_collect_now": "已开始内容采集",
        "view_result": "结果已刷新",
        "collect_log": "采集日志已刷新",
        "publish_log": "发布日志已刷新",
        "schedule_status": "定时状态已刷新",
        "worker_status": "系统状态已刷新",
        "wechat_login_qr": "二维码已发送",
        "refresh_qr": "二维码已刷新",
        "list": "列表已刷新",
        "listhandler": "路由状态已刷新",
        "switch_menu": "请选择目标机器人",
        "switchbot": f"已切换到 {ctx}" if ctx else "已切换机器人",
        "addbot": "已进入新增模式",
    }
    queued_map = {
        "collect_now": "采集任务已受理",
        "publish_now": "发布任务已受理",
        "content_collect_now": "内容采集已受理",
    }
    failed_map = {
        "refresh_qr": "二维码刷新失败",
        "switchbot": "切换失败",
    }
    if state in {"queued", "running"}:
        return queued_map.get(act, "请求已受理")
    if state in {"failed", "blocked", "alert"}:
        return failed_map.get(act, "执行失败，请查看消息")
    if state == "login_required":
        return "需要重新登录"
    return success_map.get(act, "操作已完成")


def build_home_callback_data(bot_kind: str, action: str, value: str = "") -> str:
    token = str(bot_kind or "").strip().lower() or "cybercar"
    act = str(action or "").strip().lower()
    extra = str(value or "").strip()
    return f"{HOME_CALLBACK_PREFIX}|{token}|{act}|{extra or '-'}"


def parse_home_callback_data(data: str) -> dict[str, str] | None:
    token = str(data or "").strip()
    if not token:
        return None
    parts = token.split("|", 3)
    if len(parts) != 4 or parts[0] != HOME_CALLBACK_PREFIX:
        return None
    bot_kind = str(parts[1] or "").strip().lower()
    action = str(parts[2] or "").strip().lower()
    value = str(parts[3] or "").strip()
    if not bot_kind or not action:
        return None
    return {
        "bot_kind": bot_kind,
        "action": action,
        "value": "" if value == "-" else value,
    }


def _load_home_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_home_state(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_result_message_id(payload: Mapping[str, Any]) -> int:
    result = payload.get("result")
    if isinstance(result, Mapping):
        try:
            return int(result.get("message_id") or 0)
        except Exception:
            return 0
    return 0


def _coerce_message_id(value: Any) -> int:
    try:
        message_id = int(value or 0)
    except Exception:
        return 0
    return message_id if message_id > 0 else 0


def _known_home_message_ids(state: Mapping[str, Any]) -> list[int]:
    seen: set[int] = set()
    values: list[int] = []
    for raw in state.get("recent_message_ids", []):
        message_id = _coerce_message_id(raw)
        if message_id <= 0 or message_id in seen:
            continue
        seen.add(message_id)
        values.append(message_id)
    current_message_id = _coerce_message_id(state.get("message_id"))
    if current_message_id > 0 and current_message_id not in seen:
        values.insert(0, current_message_id)
    return values[:_HOME_STATE_HISTORY_LIMIT]


def _message_params(chat_id: str, card: Mapping[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {
        "chat_id": str(chat_id or "").strip(),
        "text": str(card.get("text") or "").strip() or "(empty)",
        "parse_mode": str(card.get("parse_mode") or "HTML"),
        "disable_web_page_preview": True,
    }
    reply_markup = card.get("reply_markup")
    if isinstance(reply_markup, Mapping) and reply_markup:
        params["reply_markup"] = json.dumps(dict(reply_markup), ensure_ascii=True)
    return params


def _reply_markup_is_empty(reply_markup: Any) -> bool:
    if not isinstance(reply_markup, Mapping):
        return False
    inline_keyboard = reply_markup.get("inline_keyboard")
    if inline_keyboard is None:
        return len(dict(reply_markup)) == 0
    if not isinstance(inline_keyboard, Sequence):
        return False
    return len(list(inline_keyboard)) == 0


def _delete_message(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    timeout_seconds: int,
) -> None:
    if not str(chat_id or "").strip() or int(message_id or 0) <= 0:
        return
    call_telegram_api(
        bot_token=bot_token,
        method="deleteMessage",
        params={"chat_id": str(chat_id or "").strip(), "message_id": int(message_id)},
        timeout_seconds=timeout_seconds,
        use_post=True,
    )


def send_or_update_home_message(
    *,
    bot_token: str,
    chat_id: str,
    state_file: str | Path,
    bot_kind: str,
    card: Mapping[str, Any],
    timeout_seconds: int,
    force_new: bool = False,
) -> dict[str, Any]:
    api_timeout = max(8, min(int(timeout_seconds), 15))
    state_path = Path(state_file)
    state = _load_home_state(state_path)
    stored_chat_id = str(state.get("chat_id") or "").strip()
    stored_message_id = int(state.get("message_id") or 0) if str(state.get("message_id") or "").strip() else 0
    known_message_ids = _known_home_message_ids(state) if stored_chat_id == str(chat_id or "").strip() else []
    params = _message_params(chat_id, card)
    reply_markup = card.get("reply_markup") if isinstance(card, Mapping) else None
    if (not force_new) and stored_chat_id == str(chat_id or "").strip() and stored_message_id > 0:
        edit_params = dict(params)
        edit_params.pop("chat_id", None)
        edit_params["chat_id"] = stored_chat_id
        edit_params["message_id"] = stored_message_id
        try:
            _call_telegram_api_with_emoji_fallback(
                bot_token=bot_token,
                method="editMessageText",
                params=edit_params,
                timeout_seconds=api_timeout,
                use_post=True,
            )
            if _reply_markup_is_empty(reply_markup):
                _call_telegram_api_with_emoji_fallback(
                    bot_token=bot_token,
                    method="editMessageReplyMarkup",
                    params={
                        "chat_id": stored_chat_id,
                        "message_id": stored_message_id,
                        "reply_markup": json.dumps({}, ensure_ascii=True),
                    },
                    timeout_seconds=api_timeout,
                    use_post=True,
                )
            for stale_message_id in known_message_ids:
                if stale_message_id == stored_message_id:
                    continue
                try:
                    _delete_message(
                        bot_token=bot_token,
                        chat_id=stored_chat_id,
                        message_id=stale_message_id,
                        timeout_seconds=api_timeout,
                    )
                except Exception:
                    pass
            state.update(
                {
                    "chat_id": stored_chat_id,
                    "message_id": stored_message_id,
                    "recent_message_ids": [stored_message_id],
                    "bot_kind": str(bot_kind or "").strip().lower(),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            _save_home_state(state_path, state)
            return {"ok": True, "action": "edited", "message_id": stored_message_id}
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                state = {}
    payload = _call_telegram_api_with_emoji_fallback(
        bot_token=bot_token,
        method="sendMessage",
        params=params,
        timeout_seconds=api_timeout,
        use_post=True,
    )
    message_id = _extract_result_message_id(payload)
    current_chat_id = str(chat_id or "").strip()
    updated_state = dict(state)
    updated_state.update(
        {
            "chat_id": current_chat_id,
            "message_id": message_id,
            "recent_message_ids": [message_id] if message_id > 0 else [],
            "bot_kind": str(bot_kind or "").strip().lower(),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    _save_home_state(state_path, updated_state)
    for stale_message_id in known_message_ids:
        if stale_message_id == message_id:
            continue
        try:
            _delete_message(
                bot_token=bot_token,
                chat_id=current_chat_id,
                message_id=stale_message_id,
                timeout_seconds=api_timeout,
            )
        except Exception:
            pass
    return {"ok": True, "action": "sent", "message_id": message_id}


def send_interaction_result(
    *,
    bot_token: str,
    chat_id: str,
    card: Mapping[str, Any],
    timeout_seconds: int,
    message_id: int = 0,
    inline_message_id: str = "",
) -> dict[str, Any]:
    api_timeout = max(8, min(int(timeout_seconds), 15))
    params = _message_params(chat_id, card)
    if inline_message_id or int(message_id) > 0:
        edit_params = dict(params)
        if inline_message_id:
            edit_params.pop("chat_id", None)
            edit_params["inline_message_id"] = inline_message_id
        else:
            edit_params["message_id"] = int(message_id)
        try:
            _call_telegram_api_with_emoji_fallback(
                bot_token=bot_token,
                method="editMessageText",
                params=edit_params,
                timeout_seconds=api_timeout,
                use_post=True,
            )
            return {"ok": True, "action": "edited", "message_id": int(message_id)}
        except Exception as exc:
            if "message is not modified" in str(exc).lower():
                return {"ok": True, "action": "unchanged", "message_id": int(message_id)}
    payload = _call_telegram_api_with_emoji_fallback(
        bot_token=bot_token,
        method="sendMessage",
        params=params,
        timeout_seconds=api_timeout,
        use_post=True,
    )
    return {"ok": True, "action": "sent", "message_id": _extract_result_message_id(payload)}


def answer_interaction_toast(
    *,
    bot_token: str,
    query_id: str,
    action: str,
    status: str,
    context: Any = None,
    timeout_seconds: int,
) -> None:
    if not str(query_id or "").strip():
        return
    api_timeout = max(8, min(int(timeout_seconds), 12))
    call_telegram_api(
        bot_token=bot_token,
        method="answerCallbackQuery",
        params={
            "callback_query_id": str(query_id or "").strip(),
            "text": build_callback_toast(action, status, context=context),
        },
        timeout_seconds=api_timeout,
        use_post=True,
    )
