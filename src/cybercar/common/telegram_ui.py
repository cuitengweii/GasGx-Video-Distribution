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
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_STALE_CALLBACK_QUERY_ERROR_MARKERS = (
    "query is too old",
    "response timeout expired",
    "query id is invalid",
)

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
_SECTION_TITLE_ALIASES = {
    "执行状态": "执行摘要",
    "执行汇总": "执行摘要",
}
_SECTION_EMOJI_BY_TITLE = {
    "人工关注": "🎯",
    "失败原因": "⚠️",
    "处理建议": "🔧",
    "结果说明": "📝",
    "执行摘要": "📌",
    "执行结果": "📝",
    "候选信息": "🧾",
    "任务概览": "📦",
    "平台状态": "🧩",
    "发布选项": "🛠️",
    "下一步": "🛠️",
    "原帖摘要": "📝",
    "机器信息": "🤖",
    "运行上下文": "🧭",
    "任务日志": "🧾",
    "任务标识": "🏷️",
    "菜单链路": "🧭",
}

_PLATFORM_META: dict[str, dict[str, str]] = {
    "wechat": {"emoji": "📱", "name": "视频号"},
    "douyin": {"emoji": "🎵", "name": "抖音"},
    "xiaohongshu": {"emoji": "📝", "name": "小红书"},
    "kuaishou": {"emoji": "⚡", "name": "快手"},
    "bilibili": {"emoji": "📺", "name": "B站"},
}
_PLATFORM_ALIASES: tuple[tuple[str, str], ...] = (
    ("视频号", "wechat"),
    ("微信视频号", "wechat"),
    ("wechat", "wechat"),
    ("shipinhao", "wechat"),
    ("weixin", "wechat"),
    ("抖音", "douyin"),
    ("douyin", "douyin"),
    ("dy", "douyin"),
    ("小红书", "xiaohongshu"),
    ("xiaohongshu", "xiaohongshu"),
    ("xhs", "xiaohongshu"),
    ("快手", "kuaishou"),
    ("kuaishou", "kuaishou"),
    ("ks", "kuaishou"),
    ("B站", "bilibili"),
    ("哔哩哔哩", "bilibili"),
    ("bilibili", "bilibili"),
    ("bili", "bilibili"),
)


def _escape_text(value: Any) -> str:
    return html.escape(str(value or "").strip())


def _normalize_emoji_token(value: str) -> str:
    return str(value or "").replace(_VARIATION_SELECTOR, "").strip()


def _strip_custom_emoji_markup(text: str) -> str:
    raw = str(text or "")
    if "<tg-emoji" not in raw.lower():
        return raw
    return _CUSTOM_EMOJI_TAG_RE.sub(lambda match: html.unescape(str(match.group(1) or "")), raw)


def _strip_html_like_markup(text: str) -> str:
    raw = html.unescape(str(text or "").strip())
    if not raw:
        return ""
    raw = _strip_custom_emoji_markup(raw)
    return _HTML_TAG_RE.sub("", raw).strip()


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


def _is_stale_callback_query_error(exc: BaseException) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    return any(marker in text for marker in _STALE_CALLBACK_QUERY_ERROR_MARKERS)


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
    raw_value = _strip_html_like_markup(item.get("value", ""))
    link = str(item.get("url", "") or "").strip()
    text = _strip_html_like_markup(item.get("text", "")) or raw_value
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
            label = _strip_html_like_markup(item.get("label", ""))
            formatted = _format_value(item)
            if label:
                lines.append(f"• <b>{html.escape(label)}</b>：{formatted}")
            elif formatted:
                lines.append(f"• {formatted}")
            continue
        text = _render_inline_text(_strip_html_like_markup(item))
        if text:
            lines.append(f"• {text}")
    return lines


def _render_sections(sections: Sequence[Mapping[str, Any]]) -> list[str]:
    lines: list[str] = []
    for section in sections:
        title = _strip_html_like_markup(section.get("title", ""))
        if not title:
            continue
        icon = _render_emoji(section.get("emoji", ""))
        header = f"<b>{icon} {html.escape(title)}</b>" if icon else f"<b>{html.escape(title)}</b>"
        if lines:
            lines.append("")
        lines.append(header)
        lines.extend(_render_section_items(section.get("items", [])))
    return lines


def _canonical_section_title(title: str) -> str:
    token = _strip_html_like_markup(title)
    return _SECTION_TITLE_ALIASES.get(token, token)


def _detect_platform_token(text: str) -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return ""
    for keyword, token in _PLATFORM_ALIASES:
        if keyword.lower() in lowered:
            return token
    return ""


def _platform_meta(text: str) -> dict[str, str]:
    token = _detect_platform_token(text)
    if token:
        return _PLATFORM_META.get(token, {"emoji": "📣", "name": str(text or "").strip() or "平台"})
    label = str(text or "").strip() or "平台"
    return {"emoji": "📣", "name": label}


def _extract_platform_reason(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    for pattern in (r"原因[:：]\s*([^；;]+)", r"reason[:=]\s*([^;；]+)"):
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _compact_platform_status_value(text: str) -> str:
    raw = str(text or "").strip()
    lowered = raw.lower()
    if not raw:
        return "⚠️ 待确认"
    if any(token in lowered for token in ("登录", "扫码", "未登录", "login", "sign in", "qr")):
        return "🔐 需要登录"
    if any(token in lowered for token in ("失败", "异常", "未启动", "failed", "error")):
        reason = _extract_platform_reason(raw)
        return f"📣 发布失败｜{reason}" if reason else "📣 发布失败"
    if any(token in lowered for token in ("发布中", "处理中", "running", "processing")):
        return "⏳ 发布中"
    if any(token in lowered for token in ("排队", "queued", "queue")):
        return "🕓 已排队"
    if any(token in lowered for token in ("跳过", "duplicate", "历史发布记录", "已发布")):
        return "⏭️ 已跳过"
    if any(token in lowered for token in ("成功", "确认发布成功", "模拟发布成功", "success")):
        return "✅ 已确认"
    if any(token in lowered for token in ("待确认", "待核实", "pending")):
        return "⚠️ 待确认"
    return raw


def _platform_status_label_suffix(value: str) -> str:
    compact = str(value or "").strip()
    if compact.startswith("🔐") or compact.startswith("📣"):
        return " ❌"
    if compact.startswith("⚠️"):
        return " ⚠️"
    return ""


def _platform_status_priority(item: Any) -> tuple[int, str]:
    text = ""
    if isinstance(item, Mapping):
        text = f"{item.get('label') or ''} {item.get('value') or item.get('text') or ''}"
    else:
        text = str(item or "")
    lowered = str(text).strip().lower()
    platform_text = str(item.get("label") or "").strip() if isinstance(item, Mapping) else str(item or "").strip()
    if any(token in lowered for token in ("登录", "扫码", "未登录", "login", "sign in", "qr")):
        return (0, platform_text)
    if any(token in lowered for token in ("失败", "异常", "未启动", "failed", "error")):
        return (1, platform_text)
    if any(token in lowered for token in ("发布中", "处理中", "running", "processing")):
        return (2, platform_text)
    if any(token in lowered for token in ("排队", "queued", "queue")):
        return (3, platform_text)
    if any(token in lowered for token in ("跳过", "duplicate", "历史发布记录", "已发布")):
        return (4, platform_text)
    if any(token in lowered for token in ("成功", "确认发布成功", "模拟发布成功", "success")):
        return (5, platform_text)
    return (6, platform_text)


def _normalize_platform_status_items(items: Sequence[Any]) -> list[Any]:
    normalized: list[Any] = []
    for item in items:
        if not isinstance(item, Mapping):
            normalized.append(item)
            continue
        label = str(item.get("label") or "").strip()
        value = str(item.get("value") or item.get("text") or "").strip()
        meta = _platform_meta(label or value)
        updated = dict(item)
        compact_value = _compact_platform_status_value(value or label)
        updated["label"] = f"{meta['emoji']} {meta['name']}{_platform_status_label_suffix(compact_value)}"
        updated["value"] = compact_value
        normalized.append(updated)
    return sorted(normalized, key=_platform_status_priority)


def _is_positive_platform_status_item(item: Any) -> bool:
    text = ""
    if isinstance(item, Mapping):
        text = f"{item.get('label') or ''} {item.get('value') or item.get('text') or ''}"
    else:
        text = str(item or "")
    lowered = str(text).strip().lower()
    if not lowered:
        return False
    positive_tokens = ("成功", "确认发布成功", "模拟发布成功", "自动跳过", "历史发布记录", "已自动跳过")
    negative_tokens = ("失败", "登录", "待核实", "待确认", "建议", "原因", "分类", "平台未启动")
    return any(token in lowered for token in positive_tokens) and not any(token in lowered for token in negative_tokens)


def _normalize_card_sections(status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    status_token = str(status or "").strip().lower()
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        raw_title = str(section.get("title") or "").strip()
        title = _canonical_section_title(raw_title)
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        items = _normalize_summary_section_items(title, items)
        if title == "平台状态":
            items = _normalize_platform_status_items(items)
        if title == "平台状态" and status_token in {"success", "done"} and items and all(
            _is_positive_platform_status_item(item) for item in items
        ):
            continue
        emoji = _SECTION_EMOJI_BY_TITLE.get(title) or str(section.get("emoji") or "").strip()
        normalized.append({**section, "title": title, "emoji": emoji, "items": items})
    return normalized


def _failure_marker_for_text(text: str, *, fallback: str = "⚠️") -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return fallback
    if any(token in lowered for token in ("timeout", "network", "transport", "连接", "代理", "connection", "proxy")):
        return "🌐"
    if any(token in lowered for token in ("登录", "扫码", "未登录", "login", "sign in", "qr")):
        return "🔐"
    if any(token in lowered for token in ("跳过", "skip", "duplicate", "已发布")):
        return "⏭️"
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
                if value:
                    updated["value"] = _short_failure_text(title, label, value)
                new_items.append(updated)
            else:
                marker = default_marker if title == "处理建议" else _failure_marker_for_text(str(item or ""), fallback=default_marker)
                short_text = _short_failure_text(title, "", str(item or "").strip())
                new_items.append(f"{marker} {short_text}".strip())
        decorated.append({**section, "items": new_items})
    return decorated


def _short_failure_text(title: str, label: str, value: str) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    title_text = str(title or "").strip()
    label_text = str(label or "").strip()
    if not text:
        return text
    if title_text == "处理建议":
        if any(token in lowered for token in ("登录", "未登录", "需要登录", "login", "sign in", "扫码", "qr")):
            return "去登录"
        if any(token in lowered for token in ("刷新", "retry", "重试", "timeout", "network", "upload", "上传")):
            return "刷新后看进度"
        if any(token in lowered for token in ("无需", "跳过", "duplicate", "历史发布记录")):
            return "无需处理"
        return "查看进度"
    if any(token in lowered for token in ("未登录", "需要登录", "login", "sign in", "扫码", "qr")):
        return "登录失效"
    if any(token in lowered for token in ("timeout", "network", "连接", "超时", "upload", "上传")):
        return "上传失败" if any(token in lowered for token in ("upload", "上传")) else "网络超时"
    if any(token in lowered for token in ("duplicate", "历史发布记录", "已自动跳过", "跳过", "无需重复")):
        return "重复发布｜已跳过"
    if title_text == "结果说明" and label_text in {"说明", "详情"} and len(text) > 18:
        return text[:18].rstrip() + "..."
    return text


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
    direct_map = {
        "请直接选择普通发布、原创发布或跳过本条": "请选择发布方式",
        "平台已返回最新处理结果": "已返回平台结果",
        "平台已确认发布成功": "平台发布成功",
        "所有目标平台都已进入终态": "全部平台已完成",
        "部分平台成功，部分平台需要继续处理": "部分平台待处理",
        "平台处理失败，请查看原因后重试": "平台处理失败",
        "后台已接管处理，最终结果以后续平台通知为准": "后台处理中",
        "当前卡片已锁定，等待后台下载素材并分平台执行": "后台处理中",
        "当前卡片已锁定，等待后台下载素材": "后台处理中",
        "当前卡片已锁定，不再重复处理": "卡片已锁定",
        "当前卡片已锁定，不再进入后续发布": "卡片已锁定",
    }
    mapped = direct_map.get(text)
    if mapped:
        return mapped
    candidate_match = re.fullmatch(r"候选来源：X 搜索结果最近 (\d+) 条", text)
    if candidate_match:
        return f"X最近 {candidate_match.group(1)} 条"
    ordered_match = re.fullmatch(r"候选来源：X 搜索结果时间倒序｜目标 (\d+) 条", text)
    if ordered_match:
        return f"X倒序｜{ordered_match.group(1)}条"
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


def _split_header_title(title: str) -> tuple[str, str]:
    clean_title = _strip_html_like_markup(title)
    if not clean_title:
        return "", ""
    compact_title = re.sub(r"^[^A-Za-z0-9\u4e00-\u9fff【]+", "", clean_title).strip()
    match = re.match(r"^【([^】]+)】\s*(.+)$", compact_title)
    if match:
        context = str(match.group(1) or "").strip().replace("/", " / ")
        main = str(match.group(2) or "").strip()
        return main, context
    return clean_title, ""


def _build_platform_header_line(title: str) -> str:
    title_text = re.sub(r"^[^A-Za-z0-9\u4e00-\u9fff]+", "", _strip_html_like_markup(title)).strip()
    token = _detect_platform_token(title_text)
    if not token:
        return ""
    meta = _PLATFORM_META.get(token, {})
    platform_name = str(meta.get("name") or "").strip()
    platform_emoji = str(meta.get("emoji") or "📣").strip()
    if platform_name and title_text.startswith(platform_name):
        return f"{platform_emoji} {title_text}"
    return f"{platform_emoji} {title_text}"


def _decorate_platform_subtitle_line(status: str, subtitle: str) -> str:
    text = str(subtitle or "").strip()
    if not text:
        return ""
    status_token = str(status or "").strip().lower()
    if any(token in text for token in ("成功", "已确认")) or status_token in {"success", "done"}:
        return f"✅ {text}"
    if any(token in text for token in ("失败", "异常")) or status_token in {"failed", "blocked", "alert"}:
        return f"📣 {text}"
    if any(token in text for token in ("登录", "扫码")) or status_token == "login_required":
        return f"🔐 {text}"
    if any(token in text for token in ("处理中", "发布中")) or status_token == "running":
        return f"⏳ {text}"
    if any(token in text for token in ("排队",)) or status_token == "queued":
        return f"🕓 {text}"
    return text


def _should_hide_platform_subtitle_line(status: str, platform_header: str, subtitle: str) -> bool:
    header_text = str(platform_header or "").strip()
    subtitle_text = str(subtitle or "").strip()
    if not header_text or not subtitle_text:
        return False
    status_token = str(status or "").strip().lower()
    if _detect_platform_token(header_text) and status_token in {"success", "done"}:
        normalized = subtitle_text.replace("✅ ", "").strip()
        if normalized in {"平台发布成功", "平台已确认发布成功", "已返回平台结果"}:
            return True
    return False


def _decorate_context_line(text: str) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    return f"· {clean}"


def _dedupe_bot_title_prefix(title: str, bot_name: str) -> str:
    clean_title = _strip_html_like_markup(title)
    clean_bot_name = str(bot_name or "").strip()
    if not clean_title or not clean_bot_name:
        return clean_title
    pattern = rf"^[^A-Za-z0-9\u4e00-\u9fff]*\s*{re.escape(clean_bot_name)}\s*[｜|]\s*"
    return re.sub(pattern, "", clean_title, count=1).strip() or clean_title


def _compact_card_title(kind: str, title: str) -> str:
    token = str(kind or "").strip().lower()
    text = str(title or "").strip()
    if not text:
        return ""
    if token == "collect_start":
        text = text.replace("即采即发候选", "候选")
        text = text.replace("即采即发预审", "预审")
    if token == "publish_result":
        text = text.replace("发布状态更新", "结果")
        text = text.replace("发布已确认", "已确认")
    replacements = {
        "即采即发已全部完成": "全部完成",
        "即采即发部分平台已完成": "部分完成",
        "即采即发发布失败": "发布失败",
    }
    return replacements.get(text, text)


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


def _decorate_overview_header(status: str, title: str, sections: Sequence[Mapping[str, Any]], fallback: str) -> tuple[str, str]:
    title_text = str(title or "").strip()
    titles = {str(section.get("title") or "").strip() for section in sections if isinstance(section, Mapping)}
    has_platform_layout = "平台状态" in titles and "执行摘要" in titles
    if not has_platform_layout:
        return str(fallback or "").strip(), title_text
    if "即采即发" in title_text:
        return "📌", "即采即发平台概览"
    if any(token in title_text for token in ("发布完成", "发布失败", "部分完成", "全部完成")):
        return "📌", "平台概览"
    return str(fallback or "").strip(), title_text


def _prioritize_card_sections(status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = [dict(section) for section in sections if isinstance(section, Mapping)]
    if not normalized:
        return []
    status_token = str(status or "").strip().lower()
    titles = {str(section.get("title") or "").strip() for section in normalized}
    has_platform_result_layout = "平台状态" in titles and "执行摘要" in titles

    def _section_rank(section: Mapping[str, Any], index: int) -> tuple[int, int]:
        title = str(section.get("title") or "").strip()
        if has_platform_result_layout and title == "平台状态":
            return (0, index)
        if has_platform_result_layout and title == "执行摘要":
            return (1, index)
        if has_platform_result_layout and title == "机器信息":
            return (2, index)
        if has_platform_result_layout and title == "候选信息":
            return (3, index)
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


def _rank_machine_item_for_success(item: Any, index: int) -> tuple[int, int]:
    if isinstance(item, Mapping):
        label = str(item.get("label") or "").strip().lower()
        value = str(item.get("value") or "").strip().lower()
    else:
        label = ""
        value = str(item or "").strip().lower()
    haystack = f"{label} {value}"
    keyword_groups = (
        (0, ("日志", "log", ".log", "trace")),
        (1, ("标识", "task_id", "job_id", "message_id", "trace_id", "flag", "status", "state")),
        (2, ("任务", "task", "job", "platform", "pipeline")),
        (30, ("当前任务", "当前链路", "menu", "breadcrumb", "source_url", "原帖链接", "link")),
        (20, ()),
    )
    for priority, keywords in keyword_groups:
        if any(keyword in haystack for keyword in keywords):
            return (priority, index)
    return (20, index)


def _trim_success_section_items(title: str, items: Sequence[Any]) -> list[Any]:
    normalized_title = str(title or "").strip()
    normalized_items = list(items or [])
    if normalized_title != "候选信息":
        return normalized_items
    trimmed: list[Any] = []
    for item in normalized_items:
        if not isinstance(item, Mapping):
            trimmed.append(item)
            continue
        label = str(item.get("label") or "").strip()
        if label == "原帖链接":
            continue
        trimmed.append(dict(item))
    return trimmed or normalized_items[:1]


def _rank_summary_item(item: Any, index: int) -> tuple[int, int]:
    if not isinstance(item, Mapping):
        return (50, index)
    label = str(item.get("label") or "").strip()
    priority_map = {
        "成功平台": 0,
        "失败平台": 1,
        "目标平台": 2,
        "执行结果": 3,
        "结果": 3,
        "平台摘要": 4,
    }
    return (priority_map.get(label, 20), index)


def _normalize_summary_section_items(title: str, items: Sequence[Any]) -> list[Any]:
    if str(title or "").strip() != "执行摘要":
        return list(items or [])
    normalized_items = list(items or [])
    return [item for _, item in sorted(enumerate(normalized_items), key=lambda pair: _rank_summary_item(pair[1], pair[0]))]


def _prune_sections_for_kind(kind: str, status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    token = str(kind or "").strip().lower()
    status_token = str(status or "").strip().lower()
    normalized = [dict(section) for section in sections if isinstance(section, Mapping)]
    if not normalized:
        return []
    if token == "collect_start":
        allowed_titles = {"任务概览", "候选信息", "发布选项"}
        pruned = [section for section in normalized if str(section.get("title") or "").strip() in allowed_titles]
        compacted: list[dict[str, Any]] = []
        for section in pruned:
            title = str(section.get("title") or "").strip()
            items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
            if title == "候选信息":
                items = [item for item in items if not isinstance(item, Mapping) or str(item.get("label") or "").strip() != "原帖链接"]
            compacted.append({**section, "items": items[:3] if title != "任务概览" else items[:5]})
        return compacted
    if status_token in {"success", "done"}:
        allowed_titles = {"人工关注", "执行摘要", "候选信息", "机器信息", "任务日志"}
        return [
            section
            for section in normalized
            if (
                str(section.get("title") or "").strip() in allowed_titles
                or (
                    token == "publish_result"
                    and (
                        str(section.get("title") or "").strip() == "\u7ed3\u679c"
                        or str(section.get("title") or "").strip().startswith("\u56de\u590d ")
                    )
                )
            )
        ]
    if status_token in {"failed", "blocked", "alert", "login_required"}:
        allowed_titles = {
            "人工关注",
            "失败原因",
            "处理建议",
            "结果说明",
            "执行摘要",
            "候选信息",
            "机器信息",
            "平台状态",
            "运行上下文",
            "任务日志",
            "下一步",
        }
        return [section for section in normalized if str(section.get("title") or "").strip() in allowed_titles]
    return normalized


def _suppress_low_priority_success_sections(status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [dict(section) for section in sections if isinstance(section, Mapping)]
    status_token = str(status or "").strip().lower()
    if status_token not in {"success", "done"}:
        return normalized
    has_focus = any(str(section.get("title") or "").strip() == "人工关注" for section in normalized)
    compacted: list[dict[str, Any]] = []
    for section in normalized:
        title = str(section.get("title") or "").strip()
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        items = _trim_success_section_items(title, items)
        if has_focus and title == "执行结果":
            continue
        if title == "机器信息" and len(items) > 2:
            ranked_items = [
                item for _, item in sorted(enumerate(items), key=lambda pair: _rank_machine_item_for_success(pair[1], pair[0]))
            ]
            compacted.append({**section, "items": ranked_items[:2]})
            continue
        compacted.append({**section, "items": items})
    return compacted


def _hide_redundant_publish_success_sections(
    kind: str,
    status: str,
    sections: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    token = str(kind or "").strip().lower()
    status_token = str(status or "").strip().lower()
    normalized = [dict(section) for section in sections if isinstance(section, Mapping)]
    if token != "publish_result" or status_token not in {"success", "done"}:
        return normalized
    summary_title = "\u6267\u884c\u6458\u8981"
    if not normalized:
        return normalized
    summary_sections = [section for section in normalized if str(section.get("title") or "").strip() == summary_title]
    if not summary_sections:
        return normalized
    non_summary_sections = [section for section in normalized if str(section.get("title") or "").strip() != summary_title]
    has_focus_section = any(str(section.get("title") or "").strip() == "\u4eba\u5de5\u5173\u6ce8" for section in normalized)
    if has_focus_section or not non_summary_sections:
        return non_summary_sections
    return normalized


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
    title = _compact_card_title(token, str(payload.get("title", "") or "").strip() or meta["title"])
    subtitle = _strip_html_like_markup(payload.get("subtitle", ""))
    bot_name = str(payload.get("bot_name", "") or "").strip() or "CyberCar"
    sections = payload.get("sections", [])
    if not isinstance(sections, Iterable):
        sections = []
    sections = _normalize_card_sections(status, list(sections))
    sections = _prune_sections_for_kind(token, status, sections)
    emoji, title = _decorate_overview_header(status, title, sections, emoji)
    sections = _prioritize_card_sections(
        status,
        _suppress_low_priority_success_sections(
            status,
            _compact_sections_for_status(
                status,
                _decorate_failure_sections(status, _prioritize_card_sections(status, list(sections))),
            ),
        ),
    )
    sections = _hide_redundant_publish_success_sections(token, status, sections)
    if str(emoji or "").strip() != "📌":
        emoji = _pick_failure_header_emoji(status, sections, emoji)
        emoji, title = _decorate_positive_header(status, title, sections, emoji)
    title = _dedupe_bot_title_prefix(title, bot_name)
    title_main, title_context = _split_header_title(title)
    platform_header = _build_platform_header_line(title_main or title)
    subtitle = _decorate_card_subtitle(status, subtitle, sections)
    hide_success_context_line = token == "publish_result" and status in {"success", "done"}
    if platform_header:
        header = [f"<b>{_render_emoji(emoji)} {_render_inline_text(platform_header)}</b>"]
        if title_context and not hide_success_context_line:
            header.append(f"<i>{_render_inline_text(_decorate_context_line(title_context))}</i>")
        if subtitle and not _should_hide_platform_subtitle_line(status, platform_header, subtitle):
            header.append(f"<i>{_render_inline_text(_decorate_platform_subtitle_line(status, subtitle))}</i>")
    else:
        header = [f"<b>{_render_emoji(emoji)} {_render_inline_text(title_main or title)}</b>"]
        if hide_success_context_line:
            title_context = ""
        header_subtitle_parts = [part for part in (title_context, subtitle) if str(part or "").strip()]
        if header_subtitle_parts:
            header.append(f"<i>{_render_inline_text(_decorate_context_line('｜'.join(header_subtitle_parts)))}</i>")
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
    title = _dedupe_bot_title_prefix(title, bot_name)
    title_main, title_context = _split_header_title(title)
    subtitle = _strip_html_like_markup(payload.get("subtitle", ""))
    sections = payload.get("sections", [])
    if not isinstance(sections, Iterable):
        sections = []
    header = [f"<b>{_render_emoji('🏠')} {_render_inline_text(title_main or title)}</b>"]
    header_subtitle_parts = [part for part in (title_context, subtitle) if str(part or "").strip()]
    if header_subtitle_parts:
        header.append(f"<i>{_render_inline_text(_decorate_context_line('｜'.join(header_subtitle_parts)))}</i>")
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
    actions: Sequence[Mapping[str, Any]] | None = None,
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
        actions=actions,
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
    try:
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
    except Exception as exc:
        if _is_stale_callback_query_error(exc):
            return
        raise
