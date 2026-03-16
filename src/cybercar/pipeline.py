from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:  # pragma: no cover - optional dependency fallback
    Image = None  # type: ignore[assignment]
    ImageDraw = None  # type: ignore[assignment]
    ImageFont = None  # type: ignore[assignment]

# Ensure `Collection.*` imports work even when this file is executed by absolute path.
_THIS_FILE = Path(__file__).resolve()
for _parent in [_THIS_FILE.parent, *_THIS_FILE.parents]:
    _pkg_root = _parent / "Collection"
    if _pkg_root.is_dir() and (_pkg_root / "shared" / "common" / "notify.py").is_file():
        _parent_str = str(_parent)
        if _parent_str not in sys.path:
            sys.path.insert(0, _parent_str)
        break

from Collection.shared.common.bot_notify import (
    resolve_telegram_bot_settings as _resolve_telegram_bot_settings,
    send_notification as _notify_send_notification,
)
from Collection.shared.common.telegram_api import call_telegram_api as _telegram_call_api
from Collection.shared.common.telegram_ui import build_telegram_card

try:
    from . import main as core
except Exception:
    import main as core  # type: ignore


DEFAULT_NOTIFY_EMAIL_TO = ""
BLOCKED_NOTIFY_EMAIL_RECIPIENTS = {"aamecc@163.com"}
DEFAULT_NOTIFY_ENV_PREFIX = "CYBERCAR_NOTIFY_"
DEFAULT_SORTED_OUTPUT_SUBDIR = "4_Sorted_By_Time"
DEFAULT_COLLECT_LIMIT = 3
DEFAULT_RECYCLE_BIN_SUBDIR = "5_Recycle_Bin"
DEFAULT_MONITOR_URL = "http://127.0.0.1:8787/"
DEFAULT_NON_WECHAT_RANDOM_WINDOW_MINUTES = 0
DEFAULT_NON_WECHAT_MAX_VIDEOS = 3
DEFAULT_XIAOHONGSHU_EXTRA_IMAGES_PER_RUN = 3
DEFAULT_TELEGRAM_PREFILTER_QUEUE_FILE = Path("runtime") / "telegram_prefilter_queue.json"
TELEGRAM_PREFILTER_CALLBACK_PREFIX = "ctpf"
TELEGRAM_PREFILTER_PRUNE_DAYS = 14
BILIBILI_RANDOM_SCHEDULE_MIN_LEAD_MINUTES = max(
    121,
    int(getattr(core, "BILIBILI_RANDOM_SCHEDULE_MIN_LEAD_MINUTES", 121)),
)
HOURLY_COLLECT_REQUIRE_TEXT_KEYWORD_MATCH = False
HOURLY_COLLECT_ENFORCE_REQUIRED_TOPIC_KEYWORDS = False
DEFAULT_BILIBILI_RANDOM_SCHEDULE_MAX_MINUTES = max(
    BILIBILI_RANDOM_SCHEDULE_MIN_LEAD_MINUTES,
    int(getattr(core, "BILIBILI_RANDOM_SCHEDULE_MAX_MINUTES_DEFAULT", 240)),
)
PLATFORM_CN = {
    "collect": "采集",
    "wechat": "视频号",
    "douyin": "抖音",
    "xiaohongshu": "小红书",
    "kuaishou": "快手",
    "bilibili": "B站",
}
PLATFORM_LOGO = {
    "collect": "🔎",
    "wechat": "📱",
    "douyin": "🎵",
    "xiaohongshu": "📝",
    "kuaishou": "⚡",
    "bilibili": "📺",
}
PLATFORM_LOGO_FETCH_URL = {
    "wechat": "https://www.google.com/s2/favicons?domain=channels.weixin.qq.com&sz=128",
    "douyin": "https://www.google.com/s2/favicons?domain=creator.douyin.com&sz=128",
    "xiaohongshu": "https://www.google.com/s2/favicons?domain=creator.xiaohongshu.com&sz=128",
    "kuaishou": "https://www.google.com/s2/favicons?domain=cp.kuaishou.com&sz=128",
    "bilibili": "https://www.google.com/s2/favicons?domain=member.bilibili.com&sz=128",
}
PLATFORM_BANNER_LABEL = {
    "wechat": "WECHAT",
    "douyin": "DOUYIN",
    "xiaohongshu": "XHS",
    "kuaishou": "KUAISHOU",
    "bilibili": "BILIBILI",
}
PLATFORM_BANNER_ACCENT = {
    "wechat": (7, 193, 96),
    "douyin": (17, 17, 17),
    "xiaohongshu": (255, 36, 66),
    "kuaishou": (255, 106, 0),
    "bilibili": (0, 174, 236),
}
PLATFORM_LOGIN_URL = {
    "collect": "https://x.com/search?q=Cybertruck%20filter%3Avideos&src=typed_query&f=live",
    "wechat": "https://channels.weixin.qq.com/platform/post/create",
    "douyin": "https://creator.douyin.com/creator-micro/content/upload",
    "xiaohongshu": "https://creator.xiaohongshu.com/publish/publish",
    "kuaishou": "https://cp.kuaishou.com/article/publish/video",
    "bilibili": "https://member.bilibili.com/platform/upload/video/frame",
}


@dataclass(frozen=True)
class EmailSettings:
    enabled: bool
    provider: str
    env_prefix: str
    resend_api_key: str
    resend_from_email: str
    resend_endpoint: str
    resend_timeout_seconds: int
    recipients: list[str]
    telegram_bot_token: str
    telegram_chat_id: str
    telegram_timeout_seconds: int
    telegram_api_base: str


@dataclass(frozen=True)
class CycleContext:
    workspace: core.Workspace
    processed_outputs: list[Path]
    collected_x_urls: list[str]
    exclude_keywords: list[str]
    require_any_keywords: list[str]
    collection_name: str
    chrome_path: Optional[str]
    chrome_user_data_dir: str
    proxy: Optional[str]
    use_system_proxy: bool
    sorted_batch_dir: Optional[Path]
    collected_at: str
    keyword: str
    requested_limit: int
    extra_url_count: int
    auto_discover_x: bool


@dataclass
class PublishEvent:
    platform: str
    stage: str
    success: bool
    result: str
    published_at: str
    video_name: str
    publish_id: str
    desc_prefix10: str
    source_url: str
    error: str = ""


@dataclass(frozen=True)
class PlatformPublishMode:
    save_draft: bool
    publish_now: bool
    kuaishou_auto_publish_random_schedule: bool = False
    bilibili_auto_publish_random_schedule: bool = False


def _is_chrome_debug_not_ready_error(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "自动启动 chrome 后仍未就绪",
        "未检测到 chrome 调试端口",
        "chrome debug port",
        "debug port",
    )
    return any(marker in text for marker in markers)


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        token = str(name or "").strip()
        if not token:
            continue
        value = str(os.getenv(token, "") or "").strip()
        if value:
            return value
    return str(default or "").strip()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _file_lock_dir(path: Path) -> Path:
    return path.with_name(f"{path.name}.lock")


def _acquire_file_lock(path: Path, timeout_seconds: float = 30.0) -> Path:
    lock_dir = _file_lock_dir(path)
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + max(5.0, float(timeout_seconds))
    stale_after_seconds = max(120.0, float(timeout_seconds) * 4.0)
    while True:
        try:
            lock_dir.mkdir(parents=False, exist_ok=False)
            marker = lock_dir / "owner.txt"
            marker.write_text(
                f"pid={os.getpid()}\nts={time.time():.6f}\npath={path}",
                encoding="utf-8",
            )
            return lock_dir
        except FileExistsError:
            try:
                age_seconds = max(0.0, time.time() - lock_dir.stat().st_mtime)
            except Exception:
                age_seconds = 0.0
            if age_seconds > stale_after_seconds:
                try:
                    shutil.rmtree(lock_dir, ignore_errors=True)
                    continue
                except Exception:
                    pass
            if time.time() >= deadline:
                raise TimeoutError(f"Timed out waiting for lock: {path}")
            time.sleep(0.1)


def _release_file_lock(lock_dir: Optional[Path]) -> None:
    if not isinstance(lock_dir, Path):
        return
    try:
        shutil.rmtree(lock_dir, ignore_errors=True)
    except Exception:
        pass


def _workspace_root_path(workspace_or_root: Any) -> Path:
    root = getattr(workspace_or_root, "root", workspace_or_root)
    return Path(root).resolve()


def _normalize_platform_tokens_for_banner(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item or "").strip() for item in raw]
    else:
        parts = [x.strip() for x in re.split(r"[,/|、\s]+", str(raw or "")) if x.strip()]
    tokens: list[str] = []
    seen: set[str] = set()
    alias_map = {
        "wechat": "wechat",
        "weixin": "wechat",
        "wx": "wechat",
        "视频号": "wechat",
        "douyin": "douyin",
        "dy": "douyin",
        "抖音": "douyin",
        "xiaohongshu": "xiaohongshu",
        "xhs": "xiaohongshu",
        "hongshu": "xiaohongshu",
        "小红书": "xiaohongshu",
        "kuaishou": "kuaishou",
        "ks": "kuaishou",
        "快手": "kuaishou",
        "bilibili": "bilibili",
        "bili": "bilibili",
        "b站": "bilibili",
        "哔哩哔哩": "bilibili",
    }
    for part in parts:
        token = alias_map.get(str(part).lower(), alias_map.get(part, str(part).lower()))
        if token not in PLATFORM_BANNER_LABEL or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _resolve_platform_logo_cache_dir(workspace_or_root: Any) -> Path:
    return _workspace_root_path(workspace_or_root) / "runtime" / "platform_logos"


def _resolve_platform_logo_banner_dir(workspace_or_root: Any) -> Path:
    return _workspace_root_path(workspace_or_root) / "runtime" / "telegram_previews" / "platform_logo_banners"


def _resolve_cached_platform_logo(workspace_or_root: Any, platform: str) -> Optional[Path]:
    token = str(platform or "").strip().lower()
    if token not in PLATFORM_LOGO_FETCH_URL:
        return None
    cache_dir = _resolve_platform_logo_cache_dir(workspace_or_root)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{token}.png"
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path
    url = str(PLATFORM_LOGO_FETCH_URL.get(token) or "").strip()
    if not url:
        return None
    try:
        resp = requests.get(url, timeout=20)
        if not (200 <= resp.status_code < 300) or not resp.content:
            return None
        cache_path.write_bytes(resp.content)
    except Exception:
        return None
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path
    return None


def _render_platform_logo_banner(
    workspace_or_root: Any,
    platforms: list[str],
    *,
    variant: str,
) -> Optional[Path]:
    tokens = _normalize_platform_tokens_for_banner(platforms)
    if not tokens or Image is None or ImageDraw is None or ImageFont is None:
        return None
    banner_dir = _resolve_platform_logo_banner_dir(workspace_or_root)
    banner_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha1("|".join([variant, *tokens]).encode("utf-8", errors="ignore")).hexdigest()[:12]
    output_path = banner_dir / f"{variant}-{digest}.png"
    if output_path.exists() and output_path.stat().st_size > 0:
        return output_path

    tiles: list[tuple[str, Path]] = []
    for token in tokens:
        logo_path = _resolve_cached_platform_logo(workspace_or_root, token)
        if isinstance(logo_path, Path) and logo_path.exists():
            tiles.append((token, logo_path))
    if not tiles:
        return None

    tile_width = 180
    padding = 28
    width = max(420, padding + len(tiles) * tile_width + padding)
    height = 220
    surface = Image.new("RGBA", (width, height), (244, 246, 249, 255))
    draw = ImageDraw.Draw(surface)
    font = ImageFont.load_default()
    draw.rounded_rectangle((12, 12, width - 12, height - 12), radius=28, fill=(255, 255, 255, 255))
    draw.text((28, 22), "CyberCar Platforms", fill=(38, 43, 52, 255), font=font)

    x = padding
    for token, logo_path in tiles:
        accent = PLATFORM_BANNER_ACCENT.get(token, (40, 40, 40))
        draw.rounded_rectangle((x, 58, x + tile_width - 20, 184), radius=24, fill=accent + (24,), outline=accent + (90,), width=2)
        try:
            logo = Image.open(logo_path).convert("RGBA")
            logo.thumbnail((72, 72), Image.LANCZOS)
            logo_x = x + (tile_width - 20 - logo.width) // 2
            surface.alpha_composite(logo, (logo_x, 76))
        except Exception:
            pass
        label = str(PLATFORM_BANNER_LABEL.get(token) or token.upper())
        label_width = draw.textlength(label, font=font)
        draw.text((x + (tile_width - 20 - label_width) / 2.0, 156), label, fill=accent + (255,), font=font)
        x += tile_width

    surface.save(output_path, format="PNG")
    return output_path


def _parse_time_text(raw: str) -> Optional[datetime]:
    token = str(raw or "").strip()
    if not token:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(token, fmt)
        except Exception:
            continue
    return None


def _resolve_telegram_prefilter_queue_path(workspace: core.Workspace) -> Path:
    return workspace.root / DEFAULT_TELEGRAM_PREFILTER_QUEUE_FILE


def _load_telegram_prefilter_queue(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": "", "items": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "updated_at": "", "items": {}}
    if not isinstance(payload, dict):
        return {"version": 1, "updated_at": "", "items": {}}
    raw_items = payload.get("items", {})
    normalized_items: dict[str, dict[str, Any]] = {}
    if isinstance(raw_items, dict):
        for item_id, raw_entry in raw_items.items():
            key = str(item_id or "").strip()
            if not key or not isinstance(raw_entry, dict):
                continue
            row = dict(raw_entry)
            row["id"] = key
            normalized_items[key] = row
    elif isinstance(raw_items, list):
        for raw_entry in raw_items:
            if not isinstance(raw_entry, dict):
                continue
            key = str(raw_entry.get("id", "") or "").strip()
            if not key:
                continue
            row = dict(raw_entry)
            row["id"] = key
            normalized_items[key] = row
    return {
        "version": int(payload.get("version", 1) or 1),
        "updated_at": str(payload.get("updated_at", "") or ""),
        "items": normalized_items,
    }


def _save_telegram_prefilter_queue(path: Path, state: dict[str, Any]) -> None:
    payload = dict(state if isinstance(state, dict) else {})
    items = payload.get("items", {})
    if not isinstance(items, dict):
        items = {}
    payload["items"] = items
    payload["version"] = int(payload.get("version", 1) or 1)
    payload["updated_at"] = _now_text()
    _atomic_write_json(path, payload)


def _prune_telegram_prefilter_queue(state: dict[str, Any], days: int = TELEGRAM_PREFILTER_PRUNE_DAYS) -> int:
    items = state.get("items", {})
    if not isinstance(items, dict) or not items:
        return 0
    cutoff = datetime.now() - timedelta(days=max(1, int(days)))
    terminal = {"up_confirmed", "down_confirmed", "send_failed"}
    removed = 0
    for item_id, raw in list(items.items()):
        if not isinstance(raw, dict):
            continue
        status = str(raw.get("status", "") or "").strip().lower()
        if status not in terminal:
            continue
        ts = _parse_time_text(str(raw.get("updated_at", "") or "")) or _parse_time_text(
            str(raw.get("created_at", "") or "")
        )
        if ts and ts < cutoff:
            del items[item_id]
            removed += 1
    return removed


def _build_telegram_prefilter_id(video_name: str, collected_at: str) -> str:
    seed = f"{video_name}|{collected_at}"
    return hashlib.sha1(seed.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _resolve_prefilter_platform_hint(upload_platforms: str) -> str:
    platforms = core._normalize_upload_platforms(upload_platforms)
    if not platforms:
        return "-"
    return "/".join(_platform_display_name(p) for p in platforms)


def _resolve_platform_publish_mode(args: argparse.Namespace, platform: str) -> PlatformPublishMode:
    platform_name = str(platform or "").strip().lower()
    if platform_name == "wechat":
        publish_now = bool(getattr(args, "wechat_publish_now", False))
        if bool(getattr(args, "publish_only", False)) and not bool(getattr(args, "wechat_save_draft_only", False)):
            publish_now = True
        save_draft = bool(not publish_now and not getattr(args, "no_save_draft", False))
        return PlatformPublishMode(save_draft=save_draft, publish_now=publish_now)
    if platform_name == "bilibili":
        return PlatformPublishMode(
            save_draft=False,
            publish_now=True,
            bilibili_auto_publish_random_schedule=bool(
                getattr(args, "bilibili_auto_publish_random_schedule", False)
            ),
        )
    if platform_name == "kuaishou":
        return PlatformPublishMode(
            save_draft=False,
            publish_now=True,
            kuaishou_auto_publish_random_schedule=bool(
                getattr(args, "kuaishou_auto_publish_random_schedule", False)
            ),
        )
    return PlatformPublishMode(save_draft=False, publish_now=True)


def _build_telegram_prefilter_message(
    *,
    video: Path,
    source_url: str,
    platform_hint: str,
    idx: int,
    total: int,
    mode: str = "",
) -> str:
    card = _build_telegram_prefilter_video_card(
        workspace_root=video.parent.parent if video.parent.name == "2_Processed" else video.parent,
        video=video,
        source_url=source_url,
        platform_hint=platform_hint,
        idx=idx,
        total=total,
        mode=mode,
    )
    return str(card.get("text") or "")


def _override_card_header(card: dict[str, Any], title: str, emoji: str) -> dict[str, Any]:
    text = str(card.get("text") or "").strip()
    lines = text.splitlines() if text else []
    header = f"<b>{html.escape(str(emoji or '').strip())} {html.escape(str(title or '').strip())}</b>".strip()
    if lines:
        lines[0] = header
    else:
        lines = [header]
    card["text"] = "\n".join(lines).strip()
    card["mode"] = "text"
    card["image"] = None
    return card


def _add_card_header_spacing(card: dict[str, Any]) -> dict[str, Any]:
    text = str(card.get("text") or "").strip()
    if not text:
        return card
    lines = text.splitlines()
    if len(lines) >= 2 and lines[1].strip() and (len(lines) == 2 or lines[2].strip()):
        lines.insert(2, "")
    card["text"] = "\n".join(lines).strip()
    return card


def _build_telegram_prefilter_video_card(
    *,
    workspace_root: Any,
    video: Path,
    source_url: str,
    platform_hint: str,
    idx: int,
    total: int,
    mode: str = "",
) -> dict[str, Any]:
    try:
        workspace = core.init_workspace(str(workspace_root))
        video_meta = _resolve_video_index_item(workspace, video)
    except Exception:
        video_meta = {}
    video_title = _resolve_video_title(video, manual_caption=None, video_meta=video_meta)
    platform_text = _single_line_preview(str(platform_hint or "").strip(), limit=80) or "\u5168\u5e73\u53f0"
    if idx > 0 and total > 0:
        subtitle = f"\u7b2c {idx}/{total} \u6761 \u00b7 \u5df2\u52a0\u5165\u672c\u8f6e\u5019\u9009"
    elif idx > 0:
        subtitle = f"\u7b2c {idx} \u6761 \u00b7 \u5df2\u52a0\u5165\u672c\u8f6e\u5019\u9009"
    else:
        subtitle = "\u672c\u8f6e\u91c7\u96c6\u7ed3\u679c"
    card = build_telegram_card(
        "collect_result",
        {
            "subtitle": subtitle,
            "sections": [
                {
                    "title": "\u53d1\u5e03\u4fe1\u606f",
                    "emoji": "\U0001f3af",
                    "items": [
                        {"label": "\u5e73\u53f0", "value": platform_text},
                        {"label": "\u6807\u9898", "value": video_title},
                    ],
                },
                {
                    "title": "\u89c6\u9891\u94fe\u63a5",
                    "emoji": "\U0001f517",
                    "items": ([source_url] if source_url else ["\u672a\u8bb0\u5f55\u89c6\u9891\u94fe\u63a5"]),
                },
            ],
        },
    )
    card = _override_card_header(card, "CyberCar采集结果", "📮")
    return _add_card_header_spacing(card)


def _build_telegram_prefilter_candidate_card(
    *,
    workspace_root: Any,
    source_url: str,
    platform_hint: str,
    idx: int,
    total: int,
    mode: str = "",
    tweet_text: str = "",
    published_at: str = "",
    display_time: str = "",
    target_platforms: str = "",
) -> dict[str, Any]:
    if idx > 0 and total > 0:
        subtitle = f"\u7b2c {idx}/{total} \u6761 \u00b7 \u7b49\u5f85\u4f60\u5ba1\u6838"
    elif idx > 0:
        subtitle = f"\u7b2c {idx} \u6761 \u00b7 \u7b49\u5f85\u4f60\u5ba1\u6838"
    else:
        subtitle = "\u672c\u8f6e\u91c7\u96c6\u9884\u5ba1"
    banner_platforms = _normalize_platform_tokens_for_banner(target_platforms or platform_hint)
    target_names = "\u3001".join(_platform_display_name(token) for token in banner_platforms) or (platform_hint or "\u5168\u5e73\u53f0")
    video_title = _resolve_video_title(
        Path("candidate.mp4"),
        manual_caption=_single_line_preview(tweet_text, limit=180),
        video_meta={},
    )
    card = build_telegram_card(
        "collect_result",
        {
            "subtitle": subtitle,
            "sections": [
                {
                    "title": "\u53d1\u5e03\u4fe1\u606f",
                    "emoji": "\U0001f3af",
                    "items": [
                        {"label": "\u5e73\u53f0", "value": target_names},
                        {"label": "\u6807\u9898", "value": video_title},
                    ],
                },
                {
                    "title": "\u89c6\u9891\u94fe\u63a5",
                    "emoji": "\U0001f517",
                    "items": ([source_url] if source_url else ["\u672a\u8bb0\u5f55\u89c6\u9891\u94fe\u63a5"]),
                },
            ],
        },
    )
    card["mode"] = "text"
    card["image"] = None
    card = _override_card_header(card, "【即采即发】CyberCar采集审核卡片", "🧾")
    return _add_card_header_spacing(card)

def _build_telegram_prefilter_reply_markup(
    source_url: str,
    item_id: str,
    *,
    skip_only: bool = False,
    mode: str = "",
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    link = str(source_url or "").strip()
    mode_token = str(mode or "").strip().lower()
    if link:
        actions.append({"text": "🔗 查看原帖", "url": link, "row": 0})
    if mode_token == "immediate_manual_publish":
        actions.extend(
            [
                {"text": "⚡ 普通发布", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|publish_normal|{item_id}", "row": 1},
                {"text": "📝 原创发布", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|publish_original|{item_id}", "row": 1},
                {"text": "⏭ 跳过本条", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|skip|{item_id}", "row": 2},
            ]
        )
    elif skip_only:
        actions.append({"text": "⏭ 跳过本条", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|down|{item_id}", "row": 1})
    else:
        actions.extend(
            [
                {"text": "✅ 保留本条", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|up|{item_id}", "row": 1},
                {"text": "⏭ 跳过本条", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|down|{item_id}", "row": 1},
            ]
        )
    return build_telegram_card("collect_result", {"sections": []}, actions)["reply_markup"]

def _resolve_telegram_preview_dir(workspace: core.Workspace) -> Path:
    return workspace.root / "runtime" / "telegram_previews"


def _render_video_preview_image(video: Path, output_path: Path) -> Optional[Path]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    candidates = [
        ["ffmpeg", "-y", "-ss", "00:00:01.000", "-i", str(video), "-frames:v", "1", str(output_path)],
        ["ffmpeg", "-y", "-ss", "00:00:00.000", "-i", str(video), "-frames:v", "1", str(output_path)],
    ]
    for cmd in candidates:
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=45,
            )
        except Exception:
            continue
        if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
            return output_path
    return None


def _resolve_telegram_prefilter_preview_image(workspace: core.Workspace, video: Path) -> Optional[Path]:
    if not video.exists() or not video.is_file():
        return None
    suffix = str(video.suffix or "").lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        return video
    if suffix not in {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}:
        return None
    preview_dir = _resolve_telegram_preview_dir(workspace)
    preview_path = preview_dir / f"{video.stem}.jpg"
    if preview_path.exists() and preview_path.stat().st_size > 0:
        return preview_path
    return _render_video_preview_image(video, preview_path)


def _send_telegram_prefilter_for_video(
    *,
    workspace: core.Workspace,
    args: argparse.Namespace,
    email_settings: EmailSettings,
    video: Path,
    collected_at: str,
    platform_hint: str,
    source_url: str,
    idx: int,
    total: int,
    prune_queue: bool = False,
) -> bool:
    if bool(getattr(args, "no_telegram_prefilter", False)):
        return False
    if not (video.exists() and video.is_file() and core._is_video_file(video)):
        return False

    queue_path = _resolve_telegram_prefilter_queue_path(workspace)

    item_id = _build_telegram_prefilter_id(video.name, collected_at)
    now_ts = _now_text()
    lock_dir = _acquire_file_lock(queue_path)
    try:
        queue = _load_telegram_prefilter_queue(queue_path)
        if prune_queue:
            pruned = _prune_telegram_prefilter_queue(queue, days=TELEGRAM_PREFILTER_PRUNE_DAYS)
            if pruned > 0:
                core._log(f"[Prefilter] Queue cleanup: removed {pruned} expired terminal item(s).")

        queue_items = queue.get("items", {})
        if not isinstance(queue_items, dict):
            queue_items = {}
            queue["items"] = queue_items

        existing = queue_items.get(item_id, {})
        if not isinstance(existing, dict):
            existing = {}
        existing_status = str(existing.get("status", "") or "").strip().lower()
        existing_message_id = int(existing.get("message_id") or 0)
        already_sent = existing_message_id > 0 and existing_status in {"pending", "up_confirmed", "down_confirmed"}

        row = dict(existing)
        row["id"] = item_id
        row["video_name"] = video.name
        row["created_at"] = str(row.get("created_at", "") or now_ts)
        row["updated_at"] = now_ts
        row["collected_at"] = str(row.get("collected_at", "") or collected_at)
        row["workflow"] = str(getattr(args, "telegram_prefilter_mode", "") or row.get("workflow", "") or "default").strip().lower()
        row["source_url"] = str(source_url or row.get("source_url", "") or "").strip()
        row["platform_hint"] = str(platform_hint or row.get("platform_hint", "") or "").strip()
        row["target_platforms"] = str(getattr(args, "upload_platforms", "") or row.get("target_platforms", "") or "").strip()
        row["profile"] = str(getattr(args, "profile", "") or row.get("profile", "") or "").strip()
        if not str(row.get("chat_id", "") or "").strip():
            row["chat_id"] = str(email_settings.telegram_chat_id or "")
        if existing_status in {"up_confirmed", "down_confirmed"}:
            row["status"] = existing_status
        elif not already_sent:
            row["status"] = "pending"
        queue_items[item_id] = row
        _save_telegram_prefilter_queue(queue_path, queue)
    finally:
        _release_file_lock(lock_dir)

    if already_sent:
        core._log(f"[Prefilter] Skip resend (already sent): {video.name} (id={item_id}).")
        return True

    bot_token = str(email_settings.telegram_bot_token or "").strip()
    chat_id = str(email_settings.telegram_chat_id or "").strip()
    if not bot_token or not chat_id:
        lock_dir = _acquire_file_lock(queue_path)
        try:
            latest_queue = _load_telegram_prefilter_queue(queue_path)
            latest_items = latest_queue.get("items", {})
            if not isinstance(latest_items, dict):
                latest_items = {}
                latest_queue["items"] = latest_items
            latest_row = latest_items.get(item_id, {})
            if not isinstance(latest_row, dict):
                latest_row = {"id": item_id, "video_name": video.name, "created_at": now_ts}
                latest_items[item_id] = latest_row
            current_status = str(latest_row.get("status", "") or "").strip().lower()
            if current_status not in {"up_confirmed", "down_confirmed"}:
                latest_row["status"] = "send_failed"
                latest_row["updated_at"] = _now_text()
                latest_row["action"] = "send_failed_missing_config"
            _save_telegram_prefilter_queue(queue_path, latest_queue)
        finally:
            _release_file_lock(lock_dir)
        core._log("[Prefilter] Telegram prefilter skipped: missing bot token or chat id (fail-open).")
        return False

    card = _build_telegram_prefilter_video_card(
        workspace_root=workspace.root,
        video=video,
        source_url=source_url,
        platform_hint=platform_hint,
        idx=idx,
        total=total,
        mode=str(getattr(args, "telegram_prefilter_mode", "") or "").strip(),
    )
    reply_markup = _build_telegram_prefilter_reply_markup(
        source_url,
        item_id,
        skip_only=bool(getattr(args, "telegram_prefilter_skip_only", False)),
        mode=str(getattr(args, "telegram_prefilter_mode", "") or "").strip(),
    )
    card["reply_markup"] = reply_markup

    try:
        resp = _send_telegram_card_message(
            email_settings,
            card,
            disable_web_page_preview=(not source_url),
        )
        result = resp.get("result") if isinstance(resp, dict) else {}
        if not isinstance(result, dict):
            result = {}
        msg_id = int(result.get("message_id") or 0)
        msg_chat = result.get("chat") if isinstance(result.get("chat"), dict) else {}

        lock_dir = _acquire_file_lock(queue_path)
        try:
            latest_queue = _load_telegram_prefilter_queue(queue_path)
            latest_items = latest_queue.get("items", {})
            if not isinstance(latest_items, dict):
                latest_items = {}
                latest_queue["items"] = latest_items
            latest_row = latest_items.get(item_id, {})
            if not isinstance(latest_row, dict):
                latest_row = {"id": item_id, "video_name": video.name, "created_at": now_ts}
                latest_items[item_id] = latest_row
            current_status = str(latest_row.get("status", "") or "").strip().lower()
            if current_status not in {"up_confirmed", "down_confirmed"}:
                latest_row["status"] = "pending"
                latest_row["updated_at"] = _now_text()
                latest_row["action"] = "sent"
                latest_row["actor"] = ""
            latest_row["video_name"] = str(latest_row.get("video_name", "") or video.name)
            latest_row["collected_at"] = str(latest_row.get("collected_at", "") or collected_at)
            latest_row["message_id"] = msg_id
            latest_row["chat_id"] = str(msg_chat.get("id") or chat_id or "")
            _save_telegram_prefilter_queue(queue_path, latest_queue)
        finally:
            _release_file_lock(lock_dir)
        if total > 0:
            core._log(f"[Prefilter] Sent review message {idx}/{total}: {video.name} (id={item_id}).")
        else:
            core._log(f"[Prefilter] Sent immediate review message #{idx}: {video.name} (id={item_id}).")
        return True
    except Exception as exc:
        lock_dir = _acquire_file_lock(queue_path)
        try:
            latest_queue = _load_telegram_prefilter_queue(queue_path)
            latest_items = latest_queue.get("items", {})
            if not isinstance(latest_items, dict):
                latest_items = {}
                latest_queue["items"] = latest_items
            latest_row = latest_items.get(item_id, {})
            if not isinstance(latest_row, dict):
                latest_row = {"id": item_id, "video_name": video.name, "created_at": now_ts}
                latest_items[item_id] = latest_row
            current_status = str(latest_row.get("status", "") or "").strip().lower()
            if current_status not in {"up_confirmed", "down_confirmed"}:
                latest_row["status"] = "send_failed"
                latest_row["updated_at"] = _now_text()
                latest_row["action"] = f"send_failed:{_single_line_preview(str(exc), limit=120)}"
            latest_row["last_error"] = str(exc)
            _save_telegram_prefilter_queue(queue_path, latest_queue)
        finally:
            _release_file_lock(lock_dir)
        core._log(f"[Prefilter] Telegram send failed for {video.name} (id={item_id}): {exc} (fail-open)")
        return False


def _send_telegram_prefilter_for_candidate(
    *,
    workspace: core.Workspace,
    email_settings: EmailSettings,
    source_url: str,
    item_id: str,
    idx: int,
    total: int,
    platform_hint: str,
    mode: str,
    tweet_text: str = "",
    published_at: str = "",
    display_time: str = "",
    target_platforms: str = "",
    fast_send: bool = False,
) -> dict[str, Any]:
    card = _build_telegram_prefilter_candidate_card(
        workspace_root=workspace.root,
        source_url=source_url,
        platform_hint=platform_hint,
        idx=idx,
        total=total,
        mode=mode,
        tweet_text=tweet_text,
        published_at=published_at,
        display_time=display_time,
        target_platforms=target_platforms,
    )
    card["reply_markup"] = _build_telegram_prefilter_reply_markup(
        source_url,
        item_id,
        skip_only=False,
        mode=mode,
    )
    try:
        return _send_telegram_card_message(
            email_settings,
            card,
            disable_web_page_preview=(not source_url),
            max_attempts=1 if fast_send else 3,
            api_retries=0 if fast_send else 2,
            timeout_seconds_override=8 if fast_send else None,
        )
    except Exception as exc:
        preview = _single_line_preview(tweet_text, limit=180) or "-"
        lines = [
            f"图片采集审核候选 {idx}/{total}" if total > 0 else "图片采集审核候选",
            f"平台：{platform_hint or '-'}",
            f"时间：{display_time or published_at or '-'}",
            f"内容：{preview}",
        ]
        if source_url:
            lines.append(f"链接：{source_url}")
        fallback_card = {
            "text": "\n".join(lines),
            "reply_markup": card.get("reply_markup"),
            "parse_mode": "",
        }
        fallback_response = _send_telegram_card_message(
            email_settings,
            fallback_card,
            disable_web_page_preview=False,
            max_attempts=1,
            api_retries=0,
            timeout_seconds_override=8 if fast_send else None,
        )
        core._log(f"[Prefilter] Candidate card send failed, fallback text sent: {item_id} ({exc})")
        return fallback_response


def _run_telegram_prefilter(ctx: CycleContext, args: argparse.Namespace, email_settings: EmailSettings) -> None:
    if bool(getattr(args, "no_telegram_prefilter", False)):
        core._log("[Prefilter] Telegram prefilter disabled by --no-telegram-prefilter.")
        return

    videos = [
        p
        for p in ctx.processed_outputs
        if p.exists() and p.is_file() and core._is_video_file(p)
    ]
    if not videos:
        core._log("[Prefilter] No new video in current cycle; skip Telegram prefilter.")
        return

    platform_hint = _resolve_prefilter_platform_hint(args.upload_platforms)
    total = len(videos)
    for idx, video in enumerate(videos, 1):
        meta = _resolve_video_index_item(ctx.workspace, video)
        source_url = _single_line_preview(_resolve_source_url(meta), limit=180)
        _send_telegram_prefilter_for_video(
            workspace=ctx.workspace,
            args=args,
            email_settings=email_settings,
            video=video,
            collected_at=ctx.collected_at,
            platform_hint=platform_hint,
            source_url=source_url,
            idx=idx,
            total=total,
            prune_queue=(idx == 1),
        )


def _is_rejected_by_review_state(ctx: CycleContext, args: argparse.Namespace, target: Path) -> bool:
    review_state_path = core._resolve_review_state_path(
        ctx.workspace,
        review_state_file=(args.review_state_file or "").strip(),
    )
    entries = core._load_review_state_entries(review_state_path)
    if not entries:
        return False
    row = core._get_review_state_entry(entries, target.name, media_kind=core._media_kind_from_path(target))
    if not isinstance(row, dict):
        return False
    status = str(row.get("status", "") or "").strip().lower()
    return status in {"rejected", "blocked"}


def _normalize_env_prefix(raw: str, default: str = DEFAULT_NOTIFY_ENV_PREFIX) -> str:
    token = str(raw or "").strip()
    if not token:
        return default
    return token if token.endswith("_") else f"{token}_"


def _split_recipients(raw: str) -> list[str]:
    parts = [x.strip() for x in re.split(r"[,\s;]+", str(raw or "")) if x.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for item in parts:
        key = item.lower()
        if key in BLOCKED_NOTIFY_EMAIL_RECIPIENTS:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _normalize_notify_provider(raw: str) -> str:
    token = str(raw or "").strip().lower()
    if token in {"wecom", "wecom_bot", "wecom-robot", "qywx", "wework"}:
        return "telegram_bot"
    if token in {"telegram", "telegram_bot", "telegram-bot", "tg", "tg_bot", "tg-bot"}:
        return "telegram_bot"
    return token


def _send_email(settings: EmailSettings, subject: str, body: str) -> None:
    if not settings.enabled:
        return

    # Email channel has been removed; keep Telegram notification only.
    if settings.telegram_bot_token and settings.telegram_chat_id:
        telegram_result = _notify_send_notification(
            provider="telegram_bot",
            subject=subject,
            text_body=body,
            telegram={
                "bot_token": settings.telegram_bot_token,
                "chat_id": settings.telegram_chat_id,
                "timeout_seconds": settings.telegram_timeout_seconds,
                "api_base": settings.telegram_api_base,
            },
            env_prefix=settings.env_prefix,
        )
        if telegram_result.get("ok"):
            core._log(f"[Notify] Message sent via telegram_bot: {subject}")
        else:
            core._log(
                f"[Notify] Message send failed via telegram_bot: "
                f"{telegram_result.get('error') or 'unknown'}"
            )
    else:
        core._log("[Notify] Telegram send skipped: missing bot_token/chat_id.")


def _send_telegram_text(
    settings: EmailSettings,
    text: str,
    *,
    disable_web_page_preview: bool = True,
    reply_markup: Optional[dict[str, Any]] = None,
    parse_mode: str = "",
) -> bool:
    if not settings.enabled:
        return False
    bot_token = str(settings.telegram_bot_token or "").strip()
    chat_id = str(settings.telegram_chat_id or "").strip()
    if not bot_token or not chat_id:
        core._log("[Notify] Telegram ????????? bot_token ? chat_id?")
        return False
    params: dict[str, Any] = {
        "chat_id": chat_id,
        "text": str(text or "").strip(),
        "disable_web_page_preview": "true" if disable_web_page_preview else "false",
    }
    if str(parse_mode or "").strip():
        params["parse_mode"] = str(parse_mode or "").strip()
    if reply_markup:
        params["reply_markup"] = json.dumps(reply_markup, ensure_ascii=True)
    try:
        _telegram_call_api(
            bot_token=bot_token,
            method="sendMessage",
            params=params,
            timeout_seconds=max(8, int(settings.telegram_timeout_seconds or 20)),
            api_base=str(settings.telegram_api_base or "").strip(),
            use_post=True,
            max_retries=2,
        )
        return True
    except Exception as exc:
        core._log(f"[Notify] Telegram ?????????{exc}")
        return False


def _send_telegram_card_message(
    settings: EmailSettings,
    card: dict[str, Any],
    *,
    disable_web_page_preview: bool = True,
    max_attempts: int = 3,
    api_retries: int = 2,
    timeout_seconds_override: Optional[int] = None,
) -> dict[str, Any]:
    if not settings.enabled:
        raise RuntimeError("telegram settings not enabled")
    bot_token = str(settings.telegram_bot_token or "").strip()
    chat_id = str(settings.telegram_chat_id or "").strip()
    if not bot_token or not chat_id:
        raise RuntimeError("telegram bot token/chat id missing")
    text = str(card.get("text") or "").strip()
    reply_markup = card.get("reply_markup") if isinstance(card.get("reply_markup"), dict) else None
    parse_mode = str(card.get("parse_mode") or "").strip() or "HTML"
    image_value = str(card.get("image") or "").strip()
    image_path = Path(image_value) if image_value else None
    timeout_seconds = max(3, int(timeout_seconds_override or settings.telegram_timeout_seconds or 20))
    if isinstance(image_path, Path) and image_path.exists() and image_path.is_file():
        return core._send_telegram_photo(
            bot_token=bot_token,
            chat_id=chat_id,
            photo_bytes=image_path.read_bytes(),
            filename=image_path.name,
            caption=text,
            timeout_seconds=timeout_seconds,
            api_base=str(settings.telegram_api_base or "").strip(),
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
    params: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true" if disable_web_page_preview else "false",
    }
    if parse_mode:
        params["parse_mode"] = parse_mode
    if reply_markup:
        params["reply_markup"] = json.dumps(reply_markup, ensure_ascii=True)
    last_exc: Optional[Exception] = None
    total_attempts = max(1, int(max_attempts))
    request_retries = max(0, int(api_retries))
    for attempt in range(1, total_attempts + 1):
        try:
            return _telegram_call_api(
                bot_token=bot_token,
                method="sendMessage",
                params=params,
                timeout_seconds=timeout_seconds,
                api_base=str(settings.telegram_api_base or "").strip(),
                use_post=True,
                max_retries=request_retries,
            )
        except Exception as exc:
            last_exc = exc
            if attempt >= total_attempts:
                break
            core._log(f"[Notify] Telegram card send retry {attempt}/{max(0, total_attempts - 1)} failed: {exc}")
            time.sleep(float(attempt))
    raise RuntimeError(str(last_exc) if last_exc else "telegram card send failed")


def _send_publish_card_notification_fail_open(
    settings: EmailSettings,
    card: dict[str, Any],
    *,
    platform: str,
    video_name: str,
) -> bool:
    try:
        _send_telegram_card_message(settings, card, disable_web_page_preview=False)
        core._log(f"[Notify] Per-publish Telegram card sent: platform={platform} video={video_name}")
        return True
    except Exception as exc:
        core._log(
            f"[Notify] Message send failed via telegram_bot: "
            f"per_publish_card fail-open platform={platform} video={video_name} error={exc}"
        )
        return False


def _single_line_preview(text: str, limit: int = 180) -> str:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return ""
    if len(clean) <= limit:
        return clean
    return clean[: limit - 3].rstrip() + "..."


def _platform_display_name(platform: str) -> str:
    return PLATFORM_CN.get(str(platform or "").strip().lower(), platform)


def _platform_display_with_logo(platform: str) -> str:
    token = str(platform or "").strip().lower()
    name = _platform_display_name(token) or token or "????"
    logo = PLATFORM_LOGO.get(token, "??")
    return f"{logo} {name}"

def _stage_display_name(stage: str) -> str:
    raw = str(stage or "").strip()
    if raw == "immediate":
        return "\u7acb\u5373\u53d1\u5e03"
    if raw.startswith("followup_"):
        tail = raw.split("_", 1)[-1].strip()
        return f"\u8ddf\u53d1\u5e03\u7b2c {tail} \u6761" if tail.isdigit() else "\u8ddf\u53d1\u5e03"
    if raw == "draft":
        return "\u4fdd\u5b58\u8349\u7a3f"
    return raw or "\u672a\u77e5\u9636\u6bb5"

def _get_login_alert_sent_set(args: argparse.Namespace) -> set[str]:
    sent = getattr(args, "_login_alert_sent_keys", None)
    if isinstance(sent, set):
        return sent
    created: set[str] = set()
    setattr(args, "_login_alert_sent_keys", created)
    return created


def _looks_like_login_required(error_text: str) -> bool:
    raw = str(error_text or "")
    if not raw.strip():
        return False
    text = raw.lower()
    hints = [
        "requires login",
        "login gate",
        "please login",
        "not logged",
        "need login",
        "login qr not found",
        "already logged in",
        "cannot connect to chrome",
        "failed to connect chrome",
        "chrome devtools",
        "debug port",
        "/i/flow/login",
        "sign in",
        "log in",
        "未登录",
        "登录失效",
        "扫码登录",
        "请先完成扫码登录",
        "账号登录",
        "二维码登录",
        "微信扫码",
        "请先登录",
        "登录后继续",
    ]
    return any(token in text for token in hints)


def _resolve_login_assist_url(platform: str) -> str:
    token = str(platform or "").strip().lower()
    return PLATFORM_LOGIN_URL.get(token, PLATFORM_LOGIN_URL.get("collect", "https://x.com/search?q=Cybertruck%20filter%3Avideos&src=typed_query&f=live"))


def _resolve_monitor_url(args: argparse.Namespace) -> str:
    return str(getattr(args, "monitor_url", "") or "").strip() or DEFAULT_MONITOR_URL


def _send_login_required_alert(
    *,
    args: argparse.Namespace,
    settings: EmailSettings,
    platform: str,
    stage: str,
    error_text: str,
    debug_port: int,
    chrome_user_data_dir: str,
) -> None:
    if not _looks_like_login_required(error_text):
        return

    key = f"{str(platform or '').strip().lower()}::{str(stage or '').strip().lower()}"
    sent = _get_login_alert_sent_set(args)
    if key in sent:
        return
    sent.add(key)

    platform_display = _platform_display_name(platform) if platform else "\u91c7\u96c6\u94fe\u8def"
    stage_display = _stage_display_name(stage) if stage else "\u6267\u884c\u9636\u6bb5"
    monitor_url = _resolve_monitor_url(args)
    assist_url = _resolve_login_assist_url(platform)
    profile_dir = str(Path(chrome_user_data_dir or "").expanduser()) if chrome_user_data_dir else "\uff08\u672a\u63d0\u4f9b\uff09"
    subject = f"[CyberCar][\u767b\u5f55\u63d0\u9192] {platform_display} \u9700\u8981\u626b\u7801"
    qr_result: dict[str, Any] = {}
    try:
        qr_result = core.send_platform_login_qr_notification(
            platform_name=str(platform or "").strip().lower() or "wechat",
            open_url=assist_url,
            debug_port=debug_port,
            chrome_user_data_dir=chrome_user_data_dir,
            auto_open_chrome=True,
            refresh_page=True,
            allow_duplicate=False,
            telegram_bot_token=settings.telegram_bot_token,
            telegram_chat_id=settings.telegram_chat_id,
            telegram_timeout_seconds=settings.telegram_timeout_seconds,
            telegram_api_base=settings.telegram_api_base,
            notify_env_prefix=settings.env_prefix,
        )
    except Exception as exc:
        qr_result = {"ok": False, "error": str(exc)}
    card = build_telegram_card(
        "alert",
        {
            "status": "login_required",
            "title": f"{platform_display}\u767b\u5f55\u63d0\u9192",
            "subtitle": (
                f"{stage_display} \u68c0\u6d4b\u5230\u767b\u5f55\u5931\u6548\uff0c\u5df2"
                + ("\u540c\u6b65\u53d1\u9001\u767b\u5f55\u4e8c\u7ef4\u7801" if bool(qr_result.get("sent")) else "\u5c1d\u8bd5\u63a8\u9001\u767b\u5f55\u4e8c\u7ef4\u7801")
            ),
            "sections": [
                {
                    "title": "\u5904\u7406\u5efa\u8bae",
                    "emoji": "\U0001f510",
                    "items": [
                        "\u8bf7\u4f18\u5148\u5b8c\u6210\u626b\u7801\u767b\u5f55\uff0c\u518d\u91cd\u8bd5\u53d1\u5e03\u3002",
                        {"label": "\u4e8c\u7ef4\u7801\u63a8\u9001", "value": "\u5df2\u53d1\u9001\u5230 Telegram" if bool(qr_result.get("sent")) else "\u672a\u6210\u529f\u53d1\u9001\uff0c\u8bf7\u6253\u5f00\u767b\u5f55\u8f85\u52a9\u9875"},
                        {"label": "\u767b\u5f55\u8f85\u52a9\u9875", "text": "\u6253\u5f00\u8f85\u52a9\u9875", "url": assist_url},
                        {"label": "\u76d1\u63a7\u9875", "text": "\u6253\u5f00\u76d1\u63a7\u9875", "url": monitor_url},
                    ],
                },
                {
                    "title": "\u4f1a\u8bdd\u4fe1\u606f",
                    "emoji": "\U0001f9ed",
                    "items": [
                        {"label": "\u6267\u884c\u9636\u6bb5", "value": stage_display},
                        {"label": "\u8c03\u8bd5\u7aef\u53e3", "value": f"127.0.0.1:{int(debug_port)}"},
                        {"label": "\u4f1a\u8bdd\u76ee\u5f55", "value": profile_dir, "style": "code"},
                    ],
                },
                {
                    "title": "\u89e6\u53d1\u539f\u56e0",
                    "emoji": "\u26a0\ufe0f",
                    "items": [
                        {"label": "\u9519\u8bef\u4fe1\u606f", "value": _single_line_preview(error_text, limit=220)},
                        *(
                            [{"label": "\u4e8c\u7ef4\u7801\u7ed3\u679c", "value": _single_line_preview(str(qr_result.get("error") or ""), limit=180)}]
                            if qr_result and qr_result.get("error")
                            else []
                        ),
                    ],
                },
            ],
        },
    )
    _send_telegram_text(
        settings,
        str(card.get("text") or ""),
        disable_web_page_preview=False,
        reply_markup=card.get("reply_markup") if isinstance(card.get("reply_markup"), dict) else None,
        parse_mode=str(card.get("parse_mode") or "HTML"),
    )
    core._log(f"[Notify] \u767b\u5f55\u63d0\u9192\u5df2\u53d1\u9001\uff1a{subject}")

def _build_collect_start_message(args: argparse.Namespace) -> tuple[str, str]:
    keyword = str(getattr(args, "keyword", "") or "").strip() or core.DEFAULT_KEYWORD
    limit = max(1, int(getattr(args, "limit", DEFAULT_COLLECT_LIMIT) or DEFAULT_COLLECT_LIMIT))
    auto_discover_x = not bool(getattr(args, "no_x_auto_discover", False))
    collect_media_kind = str(getattr(args, "collect_media_kind", "video") or "video").strip().lower()
    upload_platforms = "/".join(core._normalize_upload_platforms(getattr(args, "upload_platforms", "")))
    media_label = "图片" if collect_media_kind == "image" else "视频"
    subject = f"[CyberCar][开始{media_label}采集] {keyword}"
    card = build_telegram_card(
        "collect_start",
        {
            "subtitle": f"本轮{media_label}采集任务已启动，开始进入 X 实时发现与素材处理",
            "sections": [
                {
                    "title": "\u4efb\u52a1\u53c2\u6570",
                    "emoji": "\U0001f9e9",
                    "items": [
                        {"label": "\u5173\u952e\u8bcd", "value": keyword},
                        {"label": "采集类型", "value": media_label},
                        {"label": "\u76ee\u6807\u6570\u91cf", "value": f"{limit} \u6761"},
                        {"label": "\u5b9e\u65f6\u9875\u53d1\u73b0", "value": "\u5f00\u542f" if auto_discover_x else "\u5173\u95ed"},
                    ],
                },
                {
                    "title": "\u540e\u7eed\u53bb\u5411",
                    "emoji": "\U0001f69a",
                    "items": [
                        {"label": "\u8ba1\u5212\u53d1\u5e03", "value": upload_platforms or "\u672a\u8bbe\u7f6e"},
                    ],
                },
            ],
        },
    )
    return subject, str(card.get("text") or "")

def _resolve_caption_details(video: Path, manual_caption: Optional[str]) -> tuple[str, str]:
    manual = str(manual_caption or "").strip()
    if manual:
        return manual, "manual_arg"
    caption_path = core._processed_caption_path(video)
    if caption_path.exists():
        text = caption_path.read_text(encoding="utf-8", errors="ignore").strip()
        if text:
            return text, str(caption_path)
    return "", "none"


def _resolve_video_index_item(workspace: core.Workspace, video: Path) -> dict[str, Any]:
    try:
        lookup = core._processed_index_lookup(workspace)
    except Exception:
        lookup = {}
    item = lookup.get(video.name, {}) if isinstance(lookup, dict) else {}
    return item if isinstance(item, dict) else {}


def _build_publish_identifier(video: Path, target_fp: str, video_meta: dict[str, Any]) -> str:
    status_id = str(video_meta.get("status_id", "") or "").strip()
    media_id = str(video_meta.get("media_id", "") or "").strip()
    source_name = str(video_meta.get("source_name", "") or "").strip()
    fp_text = str(target_fp or video_meta.get("fingerprint", "") or "").strip()
    raw = "|".join([video.name, status_id, media_id, source_name, fp_text[:64]])
    digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:12].upper()
    return f"CT-{digest}"


def _build_video_meta_lines(video_meta: dict[str, Any]) -> list[str]:
    if not video_meta:
        return ["meta=none"]

    preferred_keys = [
        "source_name",
        "processed_name",
        "status_url",
        "status_id",
        "media_id",
        "uploader_text",
        "title_text",
        "duration_seconds",
        "description_signature",
        "uploader_signature",
        "title_signature",
        "created_at",
        "fingerprint",
    ]
    lines: list[str] = []
    used: set[str] = set()
    for key in preferred_keys:
        if key not in video_meta:
            continue
        used.add(key)
        value = _single_line_preview(str(video_meta.get(key, "") or ""), limit=500)
        lines.append(f"{key}={value}")

    for key in sorted(str(k) for k in video_meta.keys()):
        if key in used:
            continue
        value = _single_line_preview(str(video_meta.get(key, "") or ""), limit=500)
        lines.append(f"{key}={value}")
    return lines


def _description_prefix10(
    video: Path,
    *,
    manual_caption: Optional[str],
    video_meta: dict[str, Any],
) -> str:
    caption_text, _ = _resolve_caption_details(video, manual_caption)
    candidates = [
        str(caption_text or "").strip(),
        str(video_meta.get("description_text", "") or "").strip(),
        str(video_meta.get("title_text", "") or "").strip(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        compact = re.sub(r"\s+", "", candidate)
        if not compact:
            continue
        chinese_chars = re.findall(r"[\u4e00-\u9fff]", compact)
        if chinese_chars:
            return "".join(chinese_chars)[:10]
    return "鏆傛棤涓枃鎻忚堪"


def _resolve_video_title(
    video: Path,
    *,
    manual_caption: Optional[str],
    video_meta: dict[str, Any],
) -> str:
    caption_text, _ = _resolve_caption_details(video, manual_caption)
    candidates = [
        str(video_meta.get("title_text", "") or "").strip(),
        str(video_meta.get("description_text", "") or "").strip(),
        str(caption_text or "").strip(),
    ]
    seen: set[str] = set()
    for candidate in candidates:
        value = _single_line_preview(candidate, limit=160)
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        return normalized
    return "未识别标题"


def _resolve_source_url(video_meta: dict[str, Any]) -> str:
    for key in ("status_url", "webpage_url", "original_url"):
        value = str(video_meta.get(key, "") or "").strip()
        if value:
            return value

    source_name = str(video_meta.get("source_name", "") or "").strip()
    try:
        media_id, uploader, _ = core._split_source_name_tokens(source_name) if source_name else ("", "", "")
    except Exception:
        media_id = ""
        uploader = ""
    media_id = str(video_meta.get("media_id", "") or media_id or "").strip()
    uploader = re.sub(
        r"[^A-Za-z0-9_]+",
        "",
        str(video_meta.get("uploader_text", "") or uploader or "").strip(),
    )
    if media_id.isdigit() and uploader:
        return f"https://x.com/{uploader}/status/{media_id}"
    return ""


def _all_summary_platforms() -> list[str]:
    base = ["wechat", "douyin", "xiaohongshu", "kuaishou"]
    supported = list(getattr(core, "SUPPORTED_UPLOAD_PLATFORMS", []))
    result: list[str] = []
    seen: set[str] = set()
    for token in [*base, *supported]:
        name = str(token or "").strip().lower()
        if not name or name in seen:
            continue
        seen.add(name)
        result.append(name)
    return result


def _count_pending_for_platform(
    workspace: core.Workspace,
    platform: str,
    *,
    exclude_keywords: list[str],
    require_any_keywords: list[str],
) -> int:
    del exclude_keywords, require_any_keywords
    platform_token = str(platform or "").strip().lower()
    try:
        core._backfill_uploaded_fingerprint_index(workspace, platform=platform_token)
    except Exception:
        pass

    total_db = _database_video_total_count(workspace)
    try:
        uploaded_items = core._load_uploaded_fingerprint_index(workspace, platform=platform_token)
    except Exception:
        uploaded_items = []
    uploaded_count = len(
        {
            str(item.get("processed_name", "")).strip()
            for item in uploaded_items
            if isinstance(item, dict) and str(item.get("processed_name", "")).strip()
        }
    )
    return max(0, int(total_db) - int(uploaded_count))


def _database_video_total_count(workspace: core.Workspace) -> int:
    try:
        library_items = core._load_fingerprint_index(workspace)
    except Exception:
        library_items = []
    total_db = len(
        {
            str(item.get("processed_name", "")).strip()
            for item in library_items
            if isinstance(item, dict) and str(item.get("processed_name", "")).strip()
        }
    )
    if total_db > 0:
        return total_db
    try:
        return len([p for p in workspace.processed.iterdir() if p.is_file() and core._is_video_file(p)])
    except Exception:
        return 0


def _build_pending_counts(
    ctx: CycleContext,
    platforms: list[str],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for platform in platforms:
        counts[platform] = _count_pending_for_platform(
            ctx.workspace,
            platform,
            exclude_keywords=ctx.exclude_keywords,
            require_any_keywords=ctx.require_any_keywords,
        )
    return counts


def _split_summary_chunks(lines: list[str], max_chars: int = 1400) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        token = str(line or "")
        token_len = len(token) + 1
        if current and (current_len + token_len > max_chars):
            chunks.append("\n".join(current))
            current = [token]
            current_len = token_len
            continue
        current.append(token)
        current_len += token_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def _send_publish_summary_notification(
    settings: EmailSettings,
    subject: str,
    lines: list[str],
) -> None:
    chunks = _split_summary_chunks(lines, max_chars=1400)
    total = len(chunks)
    for idx, body in enumerate(chunks, 1):
        part_subject = subject if total <= 1 else f"{subject} ({idx}/{total})"
        _send_email(settings, part_subject, body)


def _build_publish_summary(
    *,
    ctx: CycleContext,
    events: list[PublishEvent],
    pending_before: dict[str, int],
    pending_after: dict[str, int],
    platforms_for_count: list[str],
) -> tuple[str, list[str]]:
    success_total = sum(1 for e in events if e.result == "published")
    skipped_total = sum(1 for e in events if e.result == "skipped_duplicate")
    failed_total = sum(1 for e in events if not e.success)
    total = len(events)
    ts_text = time.strftime("%Y-%m-%d %H:%M:%S")
    subject = (
        f"[CyberCar][\u53d1\u5e03\u6c47\u603b] "
        f"\u6210\u529f{success_total} \u8df3\u8fc7{skipped_total} \u5931\u8d25{failed_total} \u603b\u8ba1{total}"
    )

    lines: list[str] = [
        "\u3010CyberCar \u53d1\u5e03\u6c47\u603b\u3011",
        f"\u65f6\u95f4\uff1a{ts_text}",
        "",
        "\u53d1\u5e03\u7ed3\u679c\uff1a",
        f"\u6210\u529f\u53d1\u5e03\uff1a{success_total} \u6761",
        f"\u81ea\u52a8\u8df3\u8fc7\uff1a{skipped_total} \u6761",
        f"\u53d1\u5e03\u5931\u8d25\uff1a{failed_total} \u6761",
        f"\u603b\u8ba1\uff1a{total} \u6761",
        "",
        "\u5f85\u53d1\u5e03\u5e93\u5b58\uff1a",
    ]
    lines.append(f"\u6570\u636e\u5e93\u603b\u91cf\uff1a{_database_video_total_count(ctx.workspace)} \u6761")
    for platform in platforms_for_count:
        before = int(pending_before.get(platform, 0))
        after = int(pending_after.get(platform, 0))
        lines.append(f"{_platform_display_with_logo(platform)}\uff1a{after} \u6761\uff08\u53d1\u5e03\u524d\uff1a{before} \u6761\uff09")

    lines.append("")
    lines.append("\u53d1\u5e03\u660e\u7ec6\uff1a")
    if not events:
        lines.append("\u672c\u6b21\u6ca1\u6709\u5b9e\u9645\u53d1\u5e03\u52a8\u4f5c\u3002")
        return subject, lines

    for idx, item in enumerate(events, 1):
        if item.result == "skipped_duplicate":
            status_text = "\u5e73\u53f0\u5df2\u6709\u53d1\u5e03\u8bb0\u5f55\uff0c\u5df2\u81ea\u52a8\u8df3\u8fc7"
        elif item.success:
            status_text = "\u6210\u529f\u653e\u5230\u8349\u7a3f\u7bb1" if item.platform == "wechat" else "\u6210\u529f\u53d1\u5e03"
        else:
            status_text = "\u53d1\u5e03\u5931\u8d25"

        failure_info = _describe_publish_failure(item.platform, item.error)
        reason = str(failure_info.get("reason") or "")
        category = str(failure_info.get("category") or "")
        suggestion = str(failure_info.get("suggestion") or "")
        link = _single_line_preview(item.source_url, limit=180) if item.source_url else "-"
        lines.extend(
            [
                f"{idx}. \u5e73\u53f0\uff1a{_platform_display_with_logo(item.platform)}",
                f"\u72b6\u6001\uff1a{status_text}",
                f"\u65f6\u95f4\uff1a{item.published_at}",
                f"\u63cf\u8ff0\uff08\u4e2d\u6587\u524d10\u5b57\uff09\uff1a{item.desc_prefix10 or '鏆傛棤涓枃鎻忚堪'}",
                f"\u6765\u6e90\u94fe\u63a5\uff1a{link}",
                f"\u53d1\u5e03ID\uff1a{item.publish_id}",
            ]
        )
        if reason:
            lines.append(f"\u5931\u8d25\u539f\u56e0\uff1a{reason}")
        if category:
            lines.append(f"\u5931\u8d25\u7c7b\u578b\uff1a{category}")
        if suggestion:
            lines.append(f"\u5efa\u8bae\u5904\u7406\uff1a{suggestion}")
        if idx < len(events):
            lines.append("")
    return subject, lines


def _build_notification_message(
    *,
    success: bool,
    platform: str,
    stage: str,
    video: Path,
    workspace_root: Path,
    collection_name: str,
    publish_id: str,
    target_fp: str,
    video_meta: dict[str, Any],
    manual_caption: Optional[str],
    save_draft: bool,
    kuaishou_auto_publish_random_schedule: bool,
    error_text: str = "",
) -> tuple[str, str]:
    subject, card = _build_publish_notification_card(
        success=success,
        platform=platform,
        stage=stage,
        video=video,
        workspace_root=workspace_root,
        collection_name=collection_name,
        publish_id=publish_id,
        target_fp=target_fp,
        video_meta=video_meta,
        manual_caption=manual_caption,
        save_draft=save_draft,
        kuaishou_auto_publish_random_schedule=kuaishou_auto_publish_random_schedule,
        error_text=error_text,
    )
    return subject, str(card.get("text") or "")


def _describe_publish_failure(platform: str, error_text: str) -> dict[str, str]:
    raw = str(error_text or "").strip()
    details = core.describe_publish_failure(platform, raw)
    friendly_reason = str(details.get("reason") or "").strip()
    if not friendly_reason:
        return {"reason": "", "category": "", "suggestion": "", "raw_signal": ""}

    raw_signal = ""
    if raw and raw != friendly_reason and not friendly_reason.startswith("未登录"):
        raw_signal = _single_line_preview(raw, limit=180)

    return {
        "reason": friendly_reason,
        "category": str(details.get("category") or "").strip(),
        "suggestion": str(details.get("suggestion") or "").strip(),
        "raw_signal": raw_signal,
    }


def _build_publish_notification_card(
    *,
    success: bool,
    platform: str,
    stage: str,
    video: Path,
    workspace_root: Path,
    collection_name: str,
    publish_id: str,
    target_fp: str,
    video_meta: dict[str, Any],
    manual_caption: Optional[str],
    save_draft: bool,
    kuaishou_auto_publish_random_schedule: bool,
    error_text: str = "",
) -> tuple[str, dict[str, Any]]:
    _ = workspace_root, collection_name, publish_id, target_fp, kuaishou_auto_publish_random_schedule

    failure_info = _describe_publish_failure(platform, error_text)
    friendly_reason = str(failure_info.get("reason") or "")
    is_login_required = (not success) and friendly_reason.startswith("\u672a\u767b\u5f55")

    if success:
        status = "draft" if save_draft else "success"
        status_text = "\u5df2\u4fdd\u5b58\u8349\u7a3f" if save_draft else "\u5df2\u6210\u529f\u53d1\u5e03"
    elif is_login_required:
        status = "login_required"
        status_text = "\u672a\u767b\u5f55"
    else:
        status = "failed"
        status_text = "\u53d1\u5e03\u5931\u8d25"

    video_title = _resolve_video_title(video, manual_caption=manual_caption, video_meta=video_meta)
    stage_display = _stage_display_name(stage)
    platform_display = _platform_display_name(platform)
    source_url = _single_line_preview(_resolve_source_url(video_meta), limit=180)
    subject = f"[CyberCar][{platform_display}] {status_text}"
    title = (
        f"{platform_display}\u9700\u8981\u767b\u5f55"
        if is_login_required
        else f"{platform_display}\u53d1\u5e03\u7ed3\u679c"
    )
    subtitle = (
        f"{stage_display} \u00b7 \u9700\u8981\u767b\u5f55"
        if is_login_required
        else f"{stage_display} \u00b7 {status_text}"
    )
    card = build_telegram_card(
        "publish_result",
        {
            "status": status,
            "title": title,
            "subtitle": subtitle,
            "sections": [
                {
                    "title": "\u53d1\u5e03\u4fe1\u606f",
                    "emoji": "\U0001f3af",
                    "items": [
                        {"label": "\u5e73\u53f0", "value": platform_display},
                        {"label": "\u6807\u9898", "value": video_title},
                    ],
                },
                {
                    "title": "\u89c6\u9891\u94fe\u63a5",
                    "emoji": "\U0001f517",
                    "items": ([source_url] if source_url else ["\u672a\u8bb0\u5f55\u89c6\u9891\u94fe\u63a5"]),
                },
            ],
        },
        actions=([{"text": "🔗 查看原帖", "url": source_url, "row": 0}] if source_url else None),
    )
    card["mode"] = "text"
    card["image"] = None
    card = _override_card_header(card, f"CyberCar{platform_display}", PLATFORM_LOGO.get(str(platform or "").strip().lower(), "📣"))
    card = _add_card_header_spacing(card)
    if friendly_reason:
        text = str(card.get("text") or "")
        text += "\n\n<b>\u26a0\ufe0f \u5931\u8d25\u539f\u56e0</b>\n\u2022 " + html.escape(friendly_reason)
        category = str(failure_info.get("category") or "").strip()
        suggestion = str(failure_info.get("suggestion") or "").strip()
        raw_signal = str(failure_info.get("raw_signal") or "").strip()
        if category:
            text += "\n<b>失败类型</b>\n\u2022 " + html.escape(category)
        if suggestion:
            text += "\n<b>建议处理</b>\n\u2022 " + html.escape(suggestion)
        if raw_signal:
            text += "\n<b>触发信号</b>\n\u2022 " + html.escape(raw_signal)
        card["text"] = text
    return subject, card

def _build_collect_summary_message(ctx: CycleContext) -> tuple[str, str]:
    count = len(ctx.processed_outputs)
    x_link_count = len([x for x in ctx.collected_x_urls if str(x or "").strip()])
    subject = f"[CyberCar][\u91c7\u96c6\u5b8c\u6210] {ctx.collected_at} \u5171{count}\u6761"
    output_dir = str(ctx.sorted_batch_dir) if ctx.sorted_batch_dir else "\uff08\u672c\u8f6e\u672a\u5bfc\u51fa\uff09"
    next_step = "\u4e0b\u65b9\u5c06\u9010\u6761\u53d1\u9001\u6765\u6e90\u94fe\u63a5\u5361\u7247\uff0c\u53ef\u76f4\u63a5\u5728 Telegram \u4e2d\u6253\u5f00\u9884\u89c8\u3002" if ctx.processed_outputs else "\u672c\u8f6e\u6ca1\u6709\u65b0\u589e\u53ef\u53d1\u5e03\u7d20\u6750\u3002"
    card = build_telegram_card(
        "collect_summary",
        {
            "subtitle": "\u672c\u8f6e\u91c7\u96c6\u4e0e\u5904\u7406\u5df2\u7ed3\u675f",
            "sections": [
                {
                    "title": "\u7ed3\u679c\u6c47\u603b",
                    "emoji": "\U0001f4e6",
                    "items": [
                        {"label": "\u5173\u952e\u8bcd", "value": ctx.keyword},
                        {"label": "\u6709\u6548\u7d20\u6750", "value": f"{count} \u6761"},
                        {"label": "\u6765\u6e90\u94fe\u63a5", "value": f"{x_link_count} \u6761"},
                        {"label": "\u91c7\u96c6\u4e0a\u9650", "value": f"{ctx.requested_limit} \u6761"},
                    ],
                },
                {
                    "title": "\u53d1\u73b0\u6765\u6e90",
                    "emoji": "\U0001f9ed",
                    "items": [
                        {"label": "\u5b9e\u65f6\u9875\u53d1\u73b0", "value": "\u5f00\u542f" if ctx.auto_discover_x else "\u5173\u95ed"},
                        {"label": "\u624b\u52a8\u8865\u5145", "value": f"{ctx.extra_url_count} \u6761"},
                    ],
                },
                {
                    "title": "\u8f93\u51fa\u4f4d\u7f6e",
                    "emoji": "\U0001f5c2\ufe0f",
                    "items": [
                        {"label": "\u76ee\u5f55", "value": output_dir, "style": "code"},
                        next_step,
                    ],
                },
            ],
        },
    )
    return subject, str(card.get("text") or "")


def _send_publish_skipped_notification(
    settings: EmailSettings,
    *,
    title: str,
    subtitle: str,
    reason: str,
    workspace_root: Path,
    collected_at: str,
    platforms: list[str],
    review_state_file: str = "",
) -> None:
    card = build_telegram_card(
        "alert",
        {
            "status": "blocked",
            "title": title,
            "subtitle": subtitle,
            "sections": [
                {
                    "title": "结果说明",
                    "emoji": "⚠️",
                    "items": [
                        {"label": "原因", "value": reason},
                        {"label": "时间", "value": str(collected_at or "").strip() or "-"},
                    ],
                },
                {
                    "title": "运行上下文",
                    "emoji": "🧭",
                    "items": [
                        {"label": "工作区", "value": str(workspace_root), "style": "code"},
                        {"label": "目标平台", "value": "、".join(_platform_display_name(p) for p in platforms) or "全平台"},
                    ]
                    + (
                        [{"label": "审核状态文件", "value": str(review_state_file).strip() or "default", "style": "code"}]
                        if str(review_state_file or "").strip()
                        else []
                    ),
                },
            ],
        },
    )
    _send_telegram_text(
        settings,
        str(card.get("text") or ""),
        disable_web_page_preview=True,
        reply_markup=card.get("reply_markup") if isinstance(card.get("reply_markup"), dict) else None,
        parse_mode=str(card.get("parse_mode") or "HTML"),
    )

def _collect_x_urls_for_outputs(workspace: core.Workspace, processed_outputs: list[Path]) -> list[str]:
    if not processed_outputs:
        return []

    try:
        processed_lookup = core._processed_index_lookup(workspace)
    except Exception:
        processed_lookup = {}

    urls: list[str] = []
    for video in processed_outputs:
        item = processed_lookup.get(video.name, {}) if isinstance(processed_lookup, dict) else {}
        if not isinstance(item, dict):
            item = {}

        normalized = ""
        for raw in (
            str(item.get("status_url", "") or ""),
            str(item.get("webpage_url", "") or ""),
            str(item.get("original_url", "") or ""),
        ):
            normalized = core._normalize_x_status_url(raw) or ""
            if normalized:
                break

        if not normalized:
            source_name = str(item.get("source_name", "") or "")
            source_media_id = ""
            source_uploader = ""
            if source_name:
                try:
                    source_media_id, source_uploader, _ = core._split_source_name_tokens(source_name)
                except Exception:
                    source_media_id = ""
                    source_uploader = ""
            media_id = str(item.get("media_id", "") or source_media_id or "").strip()
            uploader = re.sub(r"[^A-Za-z0-9_]+", "", str(source_uploader or "").strip())
            if media_id.isdigit() and uploader:
                normalized = core._normalize_x_status_url(f"https://x.com/{uploader}/status/{media_id}") or ""

        if normalized:
            urls.append(normalized)

    try:
        return core._dedupe_x_status_urls(urls)
    except Exception:
        deduped: list[str] = []
        seen: set[str] = set()
        for url in urls:
            token = str(url or "").strip()
            if not token or token in seen:
                continue
            seen.add(token)
            deduped.append(token)
        return deduped


def _build_collect_x_links_message(ctx: CycleContext) -> tuple[str, str]:
    url_count = len(ctx.collected_x_urls)
    subject = f"[CyberCar][X\u94fe\u63a5\u6e05\u5355] {ctx.collected_at} \u5171{url_count}\u6761"
    lines = [
        "\u3010CyberCar X\u94fe\u63a5\u6e05\u5355\u3011",
        f"\u91c7\u96c6\u65f6\u95f4\uff1a{ctx.collected_at}",
        f"\u94fe\u63a5\u6570\u91cf\uff1a{url_count}",
        "",
        "\u94fe\u63a5\u5217\u8868\uff1a",
    ]
    if not ctx.collected_x_urls:
        lines.append("\u672c\u8f6e\u672a\u63d0\u53d6\u5230\u53ef\u7528X\u94fe\u63a5\u3002")
    else:
        lines.extend(ctx.collected_x_urls)
    return subject, "\n".join(lines)


def _build_email_settings(args: argparse.Namespace) -> EmailSettings:
    recipients = _split_recipients(args.notify_email_to)
    if recipients:
        core._log("[Notify] Email notifications removed: --notify-email-to will be ignored.")
    raw_provider = str(getattr(args, "notify_provider", "") or "").strip()
    if not raw_provider:
        raw_provider = "telegram_bot"
    provider = _normalize_notify_provider(raw_provider) or "telegram_bot"
    env_prefix = _normalize_env_prefix(str(args.notify_env_prefix or DEFAULT_NOTIFY_ENV_PREFIX), DEFAULT_NOTIFY_ENV_PREFIX)
    resend_api_key = str(args.resend_api_key or "").strip()
    resend_from_email = str(args.resend_from_email or "").strip()
    resend_endpoint = str(args.resend_endpoint or "").strip()
    resend_timeout_seconds = max(5, int(args.resend_timeout_seconds))
    telegram_registry_file = str(getattr(args, "telegram_registry_file", "") or "").strip()
    if not telegram_registry_file:
        telegram_registry_file = _env_first(
            f"{env_prefix}TELEGRAM_REGISTRY_FILE",
            "CYBERCAR_NOTIFY_TELEGRAM_REGISTRY_FILE",
            "NOTIFY_TELEGRAM_REGISTRY_FILE",
            default="",
        )
    telegram_bot_token = str(getattr(args, "telegram_bot_token", "") or "").strip()
    telegram_chat_id = str(getattr(args, "telegram_chat_id", "") or "").strip()
    telegram_timeout_seconds = max(5, int(getattr(args, "telegram_timeout_seconds", 20)))
    telegram_api_base = str(getattr(args, "telegram_api_base", "") or "").strip()
    if not telegram_api_base:
        telegram_api_base = _env_first(
            f"{env_prefix}TELEGRAM_API_BASE",
            "CYBERCAR_NOTIFY_TELEGRAM_API_BASE",
            "NOTIFY_TELEGRAM_API_BASE",
            default="",
        )
    enabled = not bool(getattr(args, "disable_notify", False))
    if bool(getattr(args, "disable_email_notify", False)):
        core._log("[Notify] --disable-email-notify is deprecated: email channel is already removed.")
    if any(
        str(x or "").strip()
        for x in [
            getattr(args, "smtp_host", ""),
            getattr(args, "smtp_user", ""),
            getattr(args, "smtp_password", ""),
            getattr(args, "smtp_from", ""),
        ]
    ):
        core._log("[Notify] Legacy SMTP args detected and ignored.")

    if provider != "telegram_bot":
        core._log(f"[Notify] Email/legacy provider '{provider}' is disabled; fallback to telegram_bot.")
        provider = "telegram_bot"
    if any([resend_api_key, resend_from_email, resend_endpoint]):
        core._log("[Notify] Resend/email settings detected but ignored (email channel removed).")
    resend_api_key = ""
    resend_from_email = ""
    resend_endpoint = ""
    recipients = []

    resolved_telegram = _resolve_telegram_bot_settings(
        {
            # 鏍囪瘑浼樺厛浠嶄繚鐣欙紱浣嗗厑璁告樉寮忓叆鍙備綔涓哄厹搴曪紙渚嬪 Telegram 鑿滃崟瑙﹀彂鏃堕€忎紶 chat_id锛夈€?
            "bot_token": telegram_bot_token,
            "chat_id": telegram_chat_id,
            "registry_file": telegram_registry_file,
            "timeout_seconds": telegram_timeout_seconds,
            "api_base": telegram_api_base,
        },
        env_prefix="",
    )
    telegram_bot_token = str(resolved_telegram.get("bot_token") or "").strip()
    telegram_chat_id = str(resolved_telegram.get("chat_id") or "").strip()
    telegram_timeout_seconds = max(5, int(resolved_telegram.get("timeout_seconds") or telegram_timeout_seconds))
    telegram_api_base = str(resolved_telegram.get("api_base") or "").strip() or telegram_api_base

    has_telegram = bool(telegram_bot_token and telegram_chat_id)
    has_resend = False
    if enabled and provider == "telegram_bot" and not has_telegram:
        core._log(
            "[Notify] Telegram channel not configured: "
            "missing single-bot registry match or explicit --telegram-bot-token / --telegram-chat-id."
        )
    if enabled and (not has_telegram) and (not has_resend):
        core._log("[Notify] No notification channel configured: all channels will be skipped.")

    return EmailSettings(
        enabled=enabled,
        provider=provider,
        env_prefix=env_prefix,
        resend_api_key=resend_api_key,
        resend_from_email=resend_from_email,
        resend_endpoint=resend_endpoint,
        resend_timeout_seconds=resend_timeout_seconds,
        recipients=recipients,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        telegram_timeout_seconds=telegram_timeout_seconds,
        telegram_api_base=telegram_api_base,
    )


def _select_targets_for_platform(
    workspace: core.Workspace,
    platform: str,
    candidates: list[Path],
    exclude_keywords: list[str],
    require_any_keywords: list[str],
) -> list[Path]:
    core._backfill_uploaded_fingerprint_index(workspace, platform=platform)
    deduped = core._dedupe_targets_by_content(
        workspace,
        [p for p in candidates if p.exists() and p.is_file()],
        platform=platform,
        exclude_keywords=exclude_keywords,
        require_any_keywords=require_any_keywords,
    )
    return [p for p in deduped if p.exists() and p.is_file()]


def _review_status_allows_publish(review_status: str, require_approved: bool) -> bool:
    status = str(review_status or "").strip().lower()
    if require_approved:
        return status == "approved"
    return status not in {"rejected", "blocked"}


def _build_target_coordination_snapshot(
    ctx: CycleContext,
    args: argparse.Namespace,
    target: Path,
    platforms: list[str],
) -> dict[str, Any]:
    return core.build_content_coordination_snapshot(
        ctx.workspace,
        processed_name=target.name,
        media_kind=core._media_kind_from_path(target),
        review_state_file=(args.review_state_file or "").strip(),
        platforms=platforms,
    )


def _coordination_eligible_platforms(
    *,
    ctx: CycleContext,
    args: argparse.Namespace,
    target: Path,
    platforms: list[str],
) -> tuple[dict[str, Any], list[str]]:
    snapshot = _build_target_coordination_snapshot(ctx, args, target, platforms)
    if not _review_status_allows_publish(
        str(snapshot.get("review_status") or "").strip().lower(),
        bool(getattr(args, "upload_only_approved", False)),
    ):
        return snapshot, []
    unpublished = set(core._normalize_upload_platforms(",".join(snapshot.get("unpublished_platforms") or [])))
    eligible = [platform for platform in platforms if platform in unpublished]
    return snapshot, eligible


def _resolve_sorted_output_root(args: argparse.Namespace, workspace: core.Workspace) -> Path:
    if str(args.sorted_output_dir or "").strip():
        return Path(args.sorted_output_dir).expanduser()
    return workspace.root / DEFAULT_SORTED_OUTPUT_SUBDIR


def _export_sorted_batch(processed_outputs: list[Path], output_root: Path) -> Optional[Path]:
    valid = [p for p in processed_outputs if p.exists() and p.is_file()]
    if not valid:
        return None

    ordered = sorted(valid, key=lambda p: p.stat().st_mtime, reverse=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    batch_dir = output_root / stamp
    batch_dir.mkdir(parents=True, exist_ok=True)

    manifest_items: list[dict[str, Any]] = []
    for idx, src in enumerate(ordered, 1):
        mtime_text = time.strftime("%Y%m%d_%H%M%S", time.localtime(src.stat().st_mtime))
        prefix = f"{idx:03d}_{mtime_text}__{src.stem}"
        dst_video = batch_dir / f"{prefix}{src.suffix}"
        if dst_video.exists():
            dst_video.unlink()
        shutil.copy2(src, dst_video)

        src_caption = core._processed_caption_path(src)
        dst_caption = batch_dir / f"{prefix}.caption.txt"
        if src_caption.exists():
            shutil.copy2(src_caption, dst_caption)
        else:
            dst_caption.write_text("", encoding="utf-8")

        manifest_items.append(
            {
                "order": idx,
                "source_video": str(src),
                "video": str(dst_video),
                "caption": str(dst_caption),
                "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(src.stat().st_mtime)),
            }
        )

    manifest = {
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(manifest_items),
        "items": manifest_items,
    }
    manifest_path = batch_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    core._log(f"[Collector] Sorted batch exported: {batch_dir} ({len(manifest_items)} videos)")
    return batch_dir


def _resolve_recycle_bin_root(args: argparse.Namespace, workspace: core.Workspace) -> Path:
    raw = str(getattr(args, "recycle_bin_subdir", "") or "").strip() or DEFAULT_RECYCLE_BIN_SUBDIR
    root = Path(raw).expanduser()
    if not root.is_absolute():
        root = workspace.root / raw
    return root


def _move_file_to_dir(src: Path, dst_dir: Path) -> Optional[Path]:
    if not src.exists() or (not src.is_file()):
        return None
    dst_dir.mkdir(parents=True, exist_ok=True)
    candidate = dst_dir / src.name
    if candidate.exists():
        stamp = time.strftime("%Y%m%d_%H%M%S")
        candidate = dst_dir / f"{src.stem}__{stamp}{src.suffix}"
    shutil.move(str(src), str(candidate))
    return candidate


def _move_video_bundle_to_recycle(video: Path, recycle_dir: Path) -> int:
    moved_count = 0
    for src in [video, core._processed_caption_path(video)]:
        moved = _move_file_to_dir(src, recycle_dir)
        if moved:
            moved_count += 1
    return moved_count


def _recycle_fully_published_videos(
    ctx: CycleContext,
    args: argparse.Namespace,
    target_by_name: dict[str, Path],
    planned_platforms_by_video: dict[str, set[str]],
    publish_events: list[PublishEvent],
) -> int:
    if not planned_platforms_by_video:
        return 0
    success_platforms_by_video: dict[str, set[str]] = {}
    for item in publish_events:
        if not item.success and item.result != "skipped_duplicate":
            continue
        success_platforms_by_video.setdefault(item.video_name, set()).add(item.platform)

    recycle_root = _resolve_recycle_bin_root(args, ctx.workspace)
    recycle_batch = recycle_root / time.strftime("%Y%m%d_%H%M%S")
    recycled_videos = 0

    for video_name, required_platforms in planned_platforms_by_video.items():
        if not required_platforms:
            continue
        succeeded = success_platforms_by_video.get(video_name, set())
        missing = sorted(required_platforms - succeeded)
        if missing:
            core._log(
                f"[Recycle] Keep in 2_Processed: {video_name} "
                f"(missing success on: {','.join(missing)})"
            )
            continue
        target = target_by_name.get(video_name)
        if not target:
            continue
        moved_files = _move_video_bundle_to_recycle(target, recycle_batch)
        if moved_files <= 0:
            continue
        recycled_videos += 1
        core._log(f"[Recycle] Moved published video bundle: {video_name} -> {recycle_batch}")

    if recycled_videos > 0:
        core._log(
            f"[Recycle] Completed: recycled_videos={recycled_videos}, "
            f"folder={recycle_batch}"
        )
    else:
        core._log("[Recycle] No fully-published videos moved this cycle.")
    return recycled_videos


def _load_extra_urls(args: argparse.Namespace) -> list[str]:
    urls: list[str] = []
    if args.tweet_url_file:
        urls.extend(core._load_urls_file(args.tweet_url_file))
    if args.tweet_url:
        urls.extend(args.tweet_url)
    return core._dedupe_urls(urls)


def _run_collect_once(
    args: argparse.Namespace,
    email_settings: Optional[EmailSettings] = None,
) -> CycleContext:
    workspace = core.init_workspace(args.workspace)
    proxy = str(args.proxy or "").strip() or None
    use_system_proxy = bool(getattr(args, "use_system_proxy", False))
    proxy, use_system_proxy = core._resolve_network_proxy(proxy, use_system_proxy=use_system_proxy)
    network_mode = "explicit_proxy" if proxy else ("system_proxy" if use_system_proxy else "direct_tun")
    chrome_path = str(args.chrome_path or "").strip() or None
    collect_media_kind = str(getattr(args, "collect_media_kind", "video") or "video").strip().lower()
    if collect_media_kind not in {"video", "image"}:
        collect_media_kind = "video"
    require_text_keyword_match = bool(
        getattr(args, "require_text_keyword_match", False)
    ) or bool(HOURLY_COLLECT_REQUIRE_TEXT_KEYWORD_MATCH)
    xiaohongshu_allow_image = bool(getattr(args, "xiaohongshu_allow_image", True)) or collect_media_kind == "image"
    runtime_config = core._load_runtime_config(args.config or core.DEFAULT_CONFIG_PATH)
    x_download_policy = core.resolve_x_download_policy(runtime_config=runtime_config, args=args)
    spark_ai_config = runtime_config.get("spark_ai") if isinstance(runtime_config.get("spark_ai"), dict) else {}
    exclude_keywords = core._normalize_keyword_list(runtime_config.get("exclude_keywords"), core.DEFAULT_EXCLUDE_KEYWORDS)
    require_any_keywords = core._normalize_keyword_list(
        runtime_config.get("require_any_keywords"),
        core.DEFAULT_REQUIRE_ANY_KEYWORDS,
    )
    if not HOURLY_COLLECT_ENFORCE_REQUIRED_TOPIC_KEYWORDS:
        if require_any_keywords:
            core._log(
                "[Collector] Required-topic keyword filter disabled for hourly collect; "
                "use latest X search results for manual review."
            )
        require_any_keywords = []

    if args.auto_delete_source_files:
        auto_delete_source_files = True
    elif args.keep_source_files:
        auto_delete_source_files = False
    else:
        auto_delete_source_files = bool(runtime_config.get("auto_delete_source_files", core.DEFAULT_AUTO_DELETE_SOURCE_FILES))

    configured_collection_name = str(runtime_config.get("collection_name", "") or "").strip()
    resolved_collection_name = (args.collection_name or "").strip() or configured_collection_name or core.DEFAULT_COLLECTION_NAME
    chrome_user_data_dir = (args.chrome_user_data_dir or "").strip() or core.DEFAULT_CHROME_USER_DATA_DIR

    core._log(
        "[Collector] Config "
        f"collection_name={resolved_collection_name}, "
        f"auto_delete_source_files={auto_delete_source_files}, "
        f"spark_ai_ready={core._spark_config_ready(spark_ai_config)}, "
        f"exclude_keywords={len(exclude_keywords)}, "
        f"require_any_keywords={len(require_any_keywords)}, "
        f"require_text_keyword_match={require_text_keyword_match}, "
        f"collect_media_kind={collect_media_kind}, "
        f"xiaohongshu_allow_image={xiaohongshu_allow_image}, "
        f"x_download_socket_timeout={x_download_policy.socket_timeout_seconds}, "
        f"x_download_retries={x_download_policy.download_retries}, "
        f"x_download_fail_fast={x_download_policy.fail_fast}, "
        f"network_mode={network_mode}"
    )

    collected_at = time.strftime("%Y-%m-%d %H:%M:%S")
    extra_urls = _load_extra_urls(args)
    auto_discover_x = not args.no_x_auto_discover
    collect_limit = max(1, int(args.limit))
    non_wechat_video_plan = max(1, int(getattr(args, "non_wechat_max_videos", DEFAULT_NON_WECHAT_MAX_VIDEOS)))
    xhs_extra_images_per_run = max(
        0,
        int(getattr(args, "xiaohongshu_extra_images_per_run", DEFAULT_XIAOHONGSHU_EXTRA_IMAGES_PER_RUN)),
    )

    if collect_media_kind == "image":
        target_image_count = max(collect_limit, xhs_extra_images_per_run, 1)
        image_discovery_url_limit = max(int(args.x_discovery_url_limit), max(90, xhs_extra_images_per_run * 30))
        image_discovery_scroll_rounds = max(int(args.x_discovery_scroll_rounds), 16)
        core._log(
            "[Collector] Image-only collect enabled: "
            f"target_images={target_image_count}, "
            f"collect_limit={collect_limit}, "
            f"discover_url_limit={image_discovery_url_limit}, "
            f"scroll_rounds={image_discovery_scroll_rounds}"
        )
        core.download_from_x(
            workspace,
            keyword=args.keyword,
            limit=collect_limit,
            tweet_urls=extra_urls,
            proxy=proxy,
            use_system_proxy=use_system_proxy,
            include_images=True,
            image_min_target=target_image_count,
            auto_discover_x=auto_discover_x,
            x_discovery_url_limit=max(1, int(image_discovery_url_limit)),
            x_discovery_scroll_rounds=max(2, int(image_discovery_scroll_rounds)),
            x_discovery_scroll_wait=max(0.3, float(args.x_discovery_scroll_wait)),
            debug_port=args.debug_port,
            auto_open_chrome=not args.no_auto_open_chrome,
            chrome_path=chrome_path,
            chrome_user_data_dir=chrome_user_data_dir,
            require_x_live_discovery=bool(getattr(args, "require_x_live_discovery", False)),
            require_text_keyword_match=require_text_keyword_match,
            x_download_socket_timeout=x_download_policy.socket_timeout_seconds,
            x_download_extractor_retries=x_download_policy.extractor_retries,
            x_download_retries=x_download_policy.download_retries,
            x_download_fragment_retries=x_download_policy.fragment_retries,
            x_download_retry_sleep=x_download_policy.retry_sleep_seconds,
            x_download_batch_retry_sleep=x_download_policy.batch_retry_sleep_seconds,
            x_download_fail_fast=x_download_policy.fail_fast,
        )
    else:
        # First collect video sources to satisfy the planned per-slot video quota.
        if x_download_policy.fail_fast:
            # In latest-first fail-fast mode, keep X discovery/download fan-out
            # aligned to the explicit request instead of slot-planning defaults.
            video_collect_limit = collect_limit
        else:
            video_collect_limit = max(collect_limit, non_wechat_video_plan)
        core.download_from_x(
            workspace,
            keyword=args.keyword,
            limit=video_collect_limit,
            tweet_urls=extra_urls,
            proxy=proxy,
            use_system_proxy=use_system_proxy,
            include_images=False,
            auto_discover_x=auto_discover_x,
            x_discovery_url_limit=max(1, int(args.x_discovery_url_limit)),
            x_discovery_scroll_rounds=max(2, int(args.x_discovery_scroll_rounds)),
            x_discovery_scroll_wait=max(0.3, float(args.x_discovery_scroll_wait)),
            debug_port=args.debug_port,
            auto_open_chrome=not args.no_auto_open_chrome,
            chrome_path=chrome_path,
            chrome_user_data_dir=chrome_user_data_dir,
            require_x_live_discovery=bool(getattr(args, "require_x_live_discovery", False)),
            require_text_keyword_match=require_text_keyword_match,
            x_download_socket_timeout=x_download_policy.socket_timeout_seconds,
            x_download_extractor_retries=x_download_policy.extractor_retries,
            x_download_retries=x_download_policy.download_retries,
            x_download_fragment_retries=x_download_policy.fragment_retries,
            x_download_retry_sleep=x_download_policy.retry_sleep_seconds,
            x_download_batch_retry_sleep=x_download_policy.batch_retry_sleep_seconds,
            x_download_fail_fast=x_download_policy.fail_fast,
        )

        # Then top up media pool for Xiaohongshu image posts in this slot.
        if xiaohongshu_allow_image and xhs_extra_images_per_run > 0:
            if x_download_policy.fail_fast:
                image_collect_limit = collect_limit
                image_min_target = min(collect_limit, max(1, xhs_extra_images_per_run))
            else:
                image_collect_limit = max(collect_limit, xhs_extra_images_per_run * 4)
                image_min_target = xhs_extra_images_per_run
            image_discovery_url_limit = max(int(args.x_discovery_url_limit), max(90, xhs_extra_images_per_run * 30))
            image_discovery_scroll_rounds = max(int(args.x_discovery_scroll_rounds), 16)
            core._log(
                "[Collector] Xiaohongshu image-source top-up "
                "enabled: "
                f"extra_images={xhs_extra_images_per_run}, "
                f"collect_limit={image_collect_limit}, "
                f"discover_url_limit={image_discovery_url_limit}, "
                f"scroll_rounds={image_discovery_scroll_rounds}"
            )
            try:
                core.download_from_x(
                    workspace,
                    keyword=args.keyword,
                    limit=image_collect_limit,
                    tweet_urls=extra_urls,
                    proxy=proxy,
                    use_system_proxy=use_system_proxy,
                    include_images=True,
                    image_min_target=image_min_target,
                    auto_discover_x=auto_discover_x,
                    x_discovery_url_limit=max(1, int(image_discovery_url_limit)),
                    x_discovery_scroll_rounds=max(2, int(image_discovery_scroll_rounds)),
                    x_discovery_scroll_wait=max(0.3, float(args.x_discovery_scroll_wait)),
                    debug_port=args.debug_port,
                    auto_open_chrome=not args.no_auto_open_chrome,
                    chrome_path=chrome_path,
                    chrome_user_data_dir=chrome_user_data_dir,
                    require_x_live_discovery=bool(getattr(args, "require_x_live_discovery", False)),
                    require_text_keyword_match=require_text_keyword_match,
                    x_download_socket_timeout=x_download_policy.socket_timeout_seconds,
                    x_download_extractor_retries=x_download_policy.extractor_retries,
                    x_download_retries=x_download_policy.download_retries,
                    x_download_fragment_retries=x_download_policy.fragment_retries,
                    x_download_retry_sleep=x_download_policy.retry_sleep_seconds,
                    x_download_batch_retry_sleep=x_download_policy.batch_retry_sleep_seconds,
                    x_download_fail_fast=x_download_policy.fail_fast,
                )
            except Exception as exc:
                core._log(
                    "[Collector] Xiaohongshu image-source top-up skipped after failure: "
                    f"{exc}"
                )
    processed_outputs = core.process_video_fingerprint(
        workspace,
        proxy=proxy,
        use_system_proxy=use_system_proxy,
        auto_delete_source_files=auto_delete_source_files,
        spark_ai=spark_ai_config,
        exclude_keywords=exclude_keywords,
        require_any_keywords=require_any_keywords,
        include_images=xiaohongshu_allow_image,
        on_output_ready=None,
    )

    # Use newest-first for "latest first" publishing and sorted exports.
    processed_outputs = sorted(
        [p for p in processed_outputs if p.exists() and p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    export_root = _resolve_sorted_output_root(args, workspace)
    sorted_batch_dir = _export_sorted_batch(processed_outputs, export_root)

    return CycleContext(
        workspace=workspace,
        processed_outputs=processed_outputs,
        collected_x_urls=_collect_x_urls_for_outputs(workspace, processed_outputs),
        exclude_keywords=exclude_keywords,
        require_any_keywords=require_any_keywords,
        collection_name=resolved_collection_name,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        proxy=proxy,
        use_system_proxy=use_system_proxy,
        sorted_batch_dir=sorted_batch_dir,
        collected_at=collected_at,
        keyword=str(args.keyword),
        requested_limit=max(1, int(args.limit)),
        extra_url_count=len(extra_urls),
        auto_discover_x=auto_discover_x,
    )


def _list_existing_processed_outputs(workspace: core.Workspace, media_kind: str = "video") -> list[Path]:
    normalized_media_kind = str(media_kind or "video").strip().lower()
    base_dir = workspace.processed
    if normalized_media_kind == "image":
        patterns = ("*.jpg", "*.jpeg", "*.png", "*.webp", "*.bmp")
        image_dir = getattr(workspace, "image_processed", None)
        if image_dir:
            try:
                candidate_dir = Path(str(image_dir)).resolve()
                if candidate_dir.exists():
                    base_dir = candidate_dir
            except Exception:
                pass
    else:
        patterns = ("*.mp4",)
    files: list[Path] = []
    for pattern in patterns:
        files.extend([p for p in base_dir.glob(pattern) if p.exists() and p.is_file()])
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def _build_publish_only_context(args: argparse.Namespace) -> CycleContext:
    workspace = core.init_workspace(args.workspace)
    collect_media_kind = str(getattr(args, "collect_media_kind", "video") or "video").strip().lower()
    if collect_media_kind not in {"video", "image"}:
        collect_media_kind = "video"
    proxy = str(args.proxy or "").strip() or None
    use_system_proxy = bool(getattr(args, "use_system_proxy", False))
    proxy, use_system_proxy = core._resolve_network_proxy(proxy, use_system_proxy=use_system_proxy)
    chrome_path = str(args.chrome_path or "").strip() or None
    runtime_config = core._load_runtime_config(args.config or core.DEFAULT_CONFIG_PATH)
    exclude_keywords = core._normalize_keyword_list(runtime_config.get("exclude_keywords"), core.DEFAULT_EXCLUDE_KEYWORDS)
    require_any_keywords = core._normalize_keyword_list(
        runtime_config.get("require_any_keywords"),
        core.DEFAULT_REQUIRE_ANY_KEYWORDS,
    )
    configured_collection_name = str(runtime_config.get("collection_name", "") or "").strip()
    resolved_collection_name = (args.collection_name or "").strip() or configured_collection_name or core.DEFAULT_COLLECTION_NAME
    chrome_user_data_dir = (args.chrome_user_data_dir or "").strip() or core.DEFAULT_CHROME_USER_DATA_DIR
    processed_outputs = _list_existing_processed_outputs(workspace, media_kind=collect_media_kind)
    collected_at = time.strftime("%Y-%m-%d %H:%M:%S")
    media_label = "image(s)" if collect_media_kind == "image" else "video(s)"

    processed_root = workspace.processed
    if collect_media_kind == "image":
        image_dir = getattr(workspace, "image_processed", None)
        if image_dir:
            try:
                processed_root = Path(str(image_dir)).resolve()
            except Exception:
                processed_root = workspace.processed
    core._log(
        "[Runner] Publish-only mode enabled: "
        f"loaded {len(processed_outputs)} {media_label} from {processed_root}."
    )
    return CycleContext(
        workspace=workspace,
        processed_outputs=processed_outputs,
        collected_x_urls=[],
        exclude_keywords=exclude_keywords,
        require_any_keywords=require_any_keywords,
        collection_name=resolved_collection_name,
        chrome_path=chrome_path,
        chrome_user_data_dir=chrome_user_data_dir,
        proxy=proxy,
        use_system_proxy=use_system_proxy,
        sorted_batch_dir=None,
        collected_at=collected_at,
        keyword=str(args.keyword or "").strip(),
        requested_limit=max(0, int(args.limit)),
        extra_url_count=0,
        auto_discover_x=not args.no_x_auto_discover,
    )


def _publish_once(
    ctx: CycleContext,
    args: argparse.Namespace,
    email_settings: EmailSettings,
    platform: str,
    target: Path,
    stage: str,
    events: list[PublishEvent],
) -> bool:
    runtime_debug_port = int(args.debug_port)
    runtime_chrome_user_data_dir = ctx.chrome_user_data_dir
    if platform == "wechat":
        runtime_debug_port = int(getattr(args, "wechat_debug_port", args.debug_port))
        runtime_chrome_user_data_dir = (
            str(getattr(args, "wechat_chrome_user_data_dir", "") or "").strip() or ctx.chrome_user_data_dir
        )

    caption = (args.caption or "").strip() or None
    snapshot, eligible_platforms = _coordination_eligible_platforms(
        ctx=ctx,
        args=args,
        target=target,
        platforms=[platform],
    )
    review_status = str(snapshot.get("review_status") or "").strip().lower()
    target_fp = str(
        dict((snapshot.get("platform_status") or {}).get(platform, {}) or {}).get("fingerprint", "") or ""
    ).strip()
    if platform not in eligible_platforms:
        if review_status in {"rejected", "blocked"}:
            core._log(f"[Scheduler:{platform}] Skip by coordination review status: {target.name} ({review_status})")
            return False

        platform_state = dict((snapshot.get("platform_status") or {}).get(platform, {}) or {})
        reason = str(platform_state.get("reason") or "fingerprint")
        match_name = str(platform_state.get("matched_processed_name") or "unknown")
        dup_meta = _resolve_video_index_item(ctx.workspace, target)
        dup_fp = str(target_fp or dup_meta.get("fingerprint", "") or "").strip()
        publish_id = _build_publish_identifier(target, dup_fp, dup_meta)
        events.append(
            PublishEvent(
                platform=platform,
                stage=stage,
                success=True,
                result="skipped_duplicate",
                published_at=time.strftime("%H:%M:%S"),
                video_name=target.name,
                publish_id=publish_id,
                desc_prefix10=_description_prefix10(
                    target,
                    manual_caption=caption,
                    video_meta=dup_meta,
                ),
                source_url=_resolve_source_url(dup_meta),
                error=f"duplicate target blocked: {reason}",
            )
        )
        core._log(
            f"[Scheduler:{platform}] Skip duplicate before publish: {target.name} "
            f"(match={match_name}, reason={reason})"
        )
        return True

    publish_mode = _resolve_platform_publish_mode(args, platform)
    try:
        if platform == "wechat":
            used_target = core.fill_draft_wechat(
                ctx.workspace,
                caption=caption,
                target_video=target,
                collection_name=ctx.collection_name,
                debug_port=runtime_debug_port,
                save_draft=publish_mode.save_draft,
                publish_now=publish_mode.publish_now,
                declare_original=bool(getattr(args, "wechat_declare_original", False)),
                upload_timeout=max(30, int(args.upload_timeout)),
                auto_open_chrome=not args.no_auto_open_chrome,
                chrome_path=ctx.chrome_path,
                chrome_user_data_dir=runtime_chrome_user_data_dir,
                telegram_bot_token=email_settings.telegram_bot_token,
                telegram_chat_id=email_settings.telegram_chat_id,
                telegram_timeout_seconds=email_settings.telegram_timeout_seconds,
                telegram_api_base=email_settings.telegram_api_base,
                notify_env_prefix=email_settings.env_prefix,
            )
        elif platform == "douyin":
            used_target = core.fill_draft_douyin(
                ctx.workspace,
                caption=caption,
                target_video=target,
                debug_port=runtime_debug_port,
                save_draft=publish_mode.save_draft,
                publish_now=publish_mode.publish_now,
                upload_timeout=max(30, int(args.upload_timeout)),
                auto_open_chrome=not args.no_auto_open_chrome,
                chrome_path=ctx.chrome_path,
                chrome_user_data_dir=runtime_chrome_user_data_dir,
                telegram_bot_token=email_settings.telegram_bot_token,
                telegram_chat_id=email_settings.telegram_chat_id,
                telegram_timeout_seconds=email_settings.telegram_timeout_seconds,
                telegram_api_base=email_settings.telegram_api_base,
                notify_env_prefix=email_settings.env_prefix,
            )
        elif platform == "xiaohongshu":
            used_target = core.fill_draft_xiaohongshu(
                ctx.workspace,
                caption=caption,
                target_video=target,
                debug_port=runtime_debug_port,
                save_draft=publish_mode.save_draft,
                publish_now=publish_mode.publish_now,
                upload_timeout=max(30, int(args.upload_timeout)),
                auto_open_chrome=not args.no_auto_open_chrome,
                chrome_path=ctx.chrome_path,
                chrome_user_data_dir=runtime_chrome_user_data_dir,
                telegram_bot_token=email_settings.telegram_bot_token,
                telegram_chat_id=email_settings.telegram_chat_id,
                telegram_timeout_seconds=email_settings.telegram_timeout_seconds,
                telegram_api_base=email_settings.telegram_api_base,
                notify_env_prefix=email_settings.env_prefix,
            )
        elif platform == "kuaishou":
            used_target = core.fill_draft_kuaishou(
                ctx.workspace,
                caption=caption,
                target_video=target,
                debug_port=runtime_debug_port,
                save_draft=publish_mode.save_draft,
                publish_now=publish_mode.publish_now,
                auto_publish_random_schedule=publish_mode.kuaishou_auto_publish_random_schedule,
                random_schedule_max_minutes=max(1, int(args.kuaishou_random_schedule_max_minutes)),
                upload_timeout=max(30, int(args.upload_timeout)),
                auto_open_chrome=not args.no_auto_open_chrome,
                chrome_path=ctx.chrome_path,
                chrome_user_data_dir=runtime_chrome_user_data_dir,
                telegram_bot_token=email_settings.telegram_bot_token,
                telegram_chat_id=email_settings.telegram_chat_id,
                telegram_timeout_seconds=email_settings.telegram_timeout_seconds,
                telegram_api_base=email_settings.telegram_api_base,
                notify_env_prefix=email_settings.env_prefix,
            )
        elif platform == "bilibili":
            used_target = core.fill_draft_bilibili(
                ctx.workspace,
                caption=caption,
                target_video=target,
                debug_port=runtime_debug_port,
                save_draft=publish_mode.save_draft,
                publish_now=publish_mode.publish_now,
                auto_publish_random_schedule=publish_mode.bilibili_auto_publish_random_schedule,
                random_schedule_max_minutes=max(
                    BILIBILI_RANDOM_SCHEDULE_MIN_LEAD_MINUTES,
                    int(getattr(args, "bilibili_random_schedule_max_minutes", DEFAULT_BILIBILI_RANDOM_SCHEDULE_MAX_MINUTES)),
                ),
                upload_timeout=max(600, int(args.upload_timeout)),
                auto_open_chrome=not args.no_auto_open_chrome,
                chrome_path=ctx.chrome_path,
                chrome_user_data_dir=runtime_chrome_user_data_dir,
                telegram_bot_token=email_settings.telegram_bot_token,
                telegram_chat_id=email_settings.telegram_chat_id,
                telegram_timeout_seconds=email_settings.telegram_timeout_seconds,
                telegram_api_base=email_settings.telegram_api_base,
                notify_env_prefix=email_settings.env_prefix,
            )
        else:
            raise RuntimeError(f"Unsupported platform: {platform}")

        should_record = (
            publish_mode.save_draft
            or publish_mode.publish_now
            or (platform == "kuaishou" and publish_mode.kuaishou_auto_publish_random_schedule)
            or (platform == "bilibili" and publish_mode.bilibili_auto_publish_random_schedule)
        )
        should_record_fingerprint = bool(
            core._should_record_publish_fingerprint(
                platform,
                used_target,
                save_draft=bool(publish_mode.save_draft),
                publish_now=bool(publish_mode.publish_now),
            )
        )
        if should_record:
            core._append_draft_upload_history(ctx.workspace, used_target.name, platform=platform)
            if should_record_fingerprint:
                core._record_uploaded_content_fingerprint(
                    ctx.workspace,
                    used_target,
                    fingerprint=target_fp,
                    platform=platform,
                )

        success_meta = _resolve_video_index_item(ctx.workspace, used_target)
        success_fp = str(target_fp or success_meta.get("fingerprint", "") or "").strip()
        publish_id = _build_publish_identifier(used_target, success_fp, success_meta)
        event = PublishEvent(
            platform=platform,
            stage=stage,
            success=True,
            result="published",
            published_at=time.strftime("%H:%M:%S"),
            video_name=used_target.name,
            publish_id=publish_id,
            desc_prefix10=_description_prefix10(
                used_target,
                manual_caption=caption,
                video_meta=success_meta,
            ),
            source_url=_resolve_source_url(success_meta),
        )
        events.append(event)
        subject, card = _build_publish_notification_card(
            success=True,
            platform=platform,
            stage=stage,
            video=used_target,
            workspace_root=ctx.workspace.root,
            collection_name=ctx.collection_name,
            publish_id=publish_id,
            target_fp=success_fp,
            video_meta=success_meta,
            manual_caption=caption,
            save_draft=publish_mode.save_draft,
            kuaishou_auto_publish_random_schedule=publish_mode.kuaishou_auto_publish_random_schedule,
        )
        if bool(getattr(args, "notify_per_publish", False)):
            _send_publish_card_notification_fail_open(
                email_settings,
                card,
                platform=platform,
                video_name=used_target.name,
            )
        return True
    except Exception as exc:
        core._log(f"[Scheduler:{platform}] Publish failed: {target.name} ({exc})")
        _send_login_required_alert(
            args=args,
            settings=email_settings,
            platform=platform,
            stage=stage,
            error_text=str(exc),
            debug_port=runtime_debug_port,
            chrome_user_data_dir=runtime_chrome_user_data_dir,
        )
        failed_meta = _resolve_video_index_item(ctx.workspace, target)
        failed_fp = str(target_fp or failed_meta.get("fingerprint", "") or "").strip()
        publish_id = _build_publish_identifier(target, failed_fp, failed_meta)
        event = PublishEvent(
            platform=platform,
            stage=stage,
            success=False,
            result="failed",
            published_at=time.strftime("%H:%M:%S"),
            video_name=target.name,
            publish_id=publish_id,
            desc_prefix10=_description_prefix10(
                target,
                manual_caption=caption,
                video_meta=failed_meta,
            ),
            source_url=_resolve_source_url(failed_meta),
            error=str(exc),
        )
        events.append(event)
        subject, card = _build_publish_notification_card(
            success=False,
            platform=platform,
            stage=stage,
            video=target,
            workspace_root=ctx.workspace.root,
            collection_name=ctx.collection_name,
            publish_id=publish_id,
            target_fp=failed_fp,
            video_meta=failed_meta,
            manual_caption=caption,
            save_draft=publish_mode.save_draft,
            kuaishou_auto_publish_random_schedule=publish_mode.kuaishou_auto_publish_random_schedule,
            error_text=str(exc),
        )
        if bool(getattr(args, "notify_per_publish", False)):
            _send_publish_card_notification_fail_open(
                email_settings,
                card,
                platform=platform,
                video_name=target.name,
            )
        return False


def _pick_next_platform(available: list[str], previous: str) -> str:
    if not available:
        raise RuntimeError("No available platform to pick.")
    preferred = [p for p in available if p != previous]
    choices = preferred if preferred else available
    return random.choice(choices)


def _compute_followup_delay_minutes(args: argparse.Namespace) -> int:
    low = min(int(args.followup_delay_minutes_min), int(args.followup_delay_minutes_max))
    high = max(int(args.followup_delay_minutes_min), int(args.followup_delay_minutes_max))
    sampled = random.randint(low, high)
    return max(int(args.min_gap_minutes), sampled)


def _random_offsets_within_window(total: int, window_minutes: int) -> list[int]:
    count = max(0, int(total))
    if count <= 0:
        return []
    if int(window_minutes) <= 0:
        return [0 for _ in range(count)]
    window_seconds = max(60, int(window_minutes) * 60)
    # 10 鏉′互鍐呴噰鏍风绾ч殢鏈猴紝瓒冲閬垮厤鍐茬獊銆?
    if count < window_seconds:
        offsets = random.sample(range(1, window_seconds + 1), count)
        offsets.sort()
        return offsets
    # 鍏滃簳锛氬潎鍖€閾烘弧绐楀彛
    step = max(1, window_seconds // count)
    return [min(window_seconds, (idx + 1) * step) for idx in range(count)]


def _run_publish_schedule(ctx: CycleContext, args: argparse.Namespace, email_settings: EmailSettings) -> None:
    platforms = core._normalize_upload_platforms(args.upload_platforms)
    current_media_kind = str(getattr(args, "collect_media_kind", "video") or "video").strip().lower()
    if current_media_kind not in {"video", "image"}:
        current_media_kind = "video"
    image_only_mode = current_media_kind == "image"
    if not ctx.processed_outputs:
        core._log("[Scheduler] No new processed outputs from this crawl, skip publish schedule.")
        if not bool(getattr(args, "no_publish_skip_notify", False)):
            _send_publish_skipped_notification(
                email_settings,
                title="发布已跳过",
                subtitle="本轮没有进入发布阶段",
                reason="本轮没有可发布的新素材。",
                workspace_root=ctx.workspace.root,
                collected_at=ctx.collected_at,
                platforms=platforms,
            )
        return
    publish_events: list[PublishEvent] = []
    count_platforms = _all_summary_platforms()
    pending_before = _build_pending_counts(ctx, count_platforms)
    candidates = ctx.processed_outputs
    if bool(args.upload_only_approved):
        approved_candidates: list[Path] = []
        rejected_count = 0
        unknown_count = 0
        for target in candidates:
            snapshot = _build_target_coordination_snapshot(ctx, args, target, platforms)
            review_status = str(snapshot.get("review_status") or "").strip().lower()
            if review_status == "approved":
                approved_candidates.append(target)
            elif review_status in {"rejected", "blocked"}:
                rejected_count += 1
            else:
                unknown_count += 1
        candidates = approved_candidates
        core._log(
            "[Review] Approval filter via coordination snapshot: "
            f"approved={len(candidates)}, rejected={rejected_count}, unreviewed={unknown_count}"
        )
        if not candidates:
            core._log("[Scheduler] No approved videos available, skip publish schedule.")
            if not bool(getattr(args, "no_publish_skip_notify", False)):
                _send_publish_skipped_notification(
                    email_settings,
                    title="发布已跳过",
                    subtitle="审核过滤后没有进入发布阶段",
                    reason="审核过滤后没有可发布素材。",
                    workspace_root=ctx.workspace.root,
                    collected_at=ctx.collected_at,
                    platforms=platforms,
                    review_state_file=(args.review_state_file or "").strip(),
                )
            return

    non_wechat_platforms = [p for p in platforms if p in {"douyin", "xiaohongshu", "kuaishou", "bilibili"}]
    wechat_enabled = "wechat" in platforms
    planned_platforms_by_video: dict[str, set[str]] = {}
    target_by_name: dict[str, Path] = {}

    def _mark_planned(target: Path, platform: str) -> None:
        if not target or (not target.exists()) or (not target.is_file()):
            return
        target_by_name.setdefault(target.name, target)
        planned_platforms_by_video.setdefault(target.name, set()).add(platform)

    non_wechat_max_videos = max(1, int(args.non_wechat_max_videos))
    xiaohongshu_allow_image = bool(getattr(args, "xiaohongshu_allow_image", True))
    xiaohongshu_extra_images_per_run = max(
        0,
        int(getattr(args, "xiaohongshu_extra_images_per_run", DEFAULT_XIAOHONGSHU_EXTRA_IMAGES_PER_RUN)),
    )
    non_wechat_queues: dict[str, list[Path]] = {p: [] for p in non_wechat_platforms}
    shared_non_wechat_candidates: list[Path] = []
    shared_non_wechat_platform_map: dict[str, list[str]] = {}
    xhs_video_topup_queue: list[Path] = []
    xhs_image_extra_queue: list[Path] = []

    # 1) Build one shared candidate set for douyin/xiaohongshu/kuaishou/bilibili:
    #    same videos across platforms, while keeping per-platform dedupe records.
    for platform in non_wechat_platforms:
        core._backfill_uploaded_fingerprint_index(ctx.workspace, platform=platform)
    if wechat_enabled:
        core._backfill_uploaded_fingerprint_index(ctx.workspace, platform="wechat")

    if non_wechat_platforms:
        shared_pool = core._build_shared_source_targets(
            ctx.workspace,
            candidates,
            pool_size=max(non_wechat_max_videos * 12, 24),
            exclude_keywords=ctx.exclude_keywords,
            require_any_keywords=ctx.require_any_keywords,
            include_images=image_only_mode,
        )
        strict_candidates: list[Path] = []
        strict_platform_map: dict[str, list[str]] = {}
        partial_candidates: list[Path] = []
        partial_platform_map: dict[str, list[str]] = {}
        for target in shared_pool:
            if not target.exists() or (not target.is_file()):
                continue
            if image_only_mode and not core._is_image_file(target):
                continue
            if (not image_only_mode) and not core._is_video_file(target):
                continue
            snapshot, eligible_platforms = _coordination_eligible_platforms(
                ctx=ctx,
                args=args,
                target=target,
                platforms=non_wechat_platforms,
            )
            if not eligible_platforms:
                review_status = str(snapshot.get("review_status") or "").strip().lower()
                if review_status and review_status != "approved":
                    core._log(
                        f"[Scheduler] Skip in shared queue by review status: "
                        f"{target.name} (status={review_status})"
                    )
            for platform in non_wechat_platforms:
                if platform in eligible_platforms:
                    continue
                platform_state = dict((snapshot.get("platform_status") or {}).get(platform, {}) or {})
                if platform_state.get("published"):
                    reason = str(platform_state.get("reason") or "fingerprint")
                    core._log(
                        f"[Scheduler:{platform}] Skip in shared queue (duplicate): "
                        f"{target.name} (reason={reason})"
                    )

            if not eligible_platforms:
                continue
            if len(eligible_platforms) == len(non_wechat_platforms):
                strict_candidates.append(target)
                strict_platform_map[target.name] = eligible_platforms
            else:
                partial_candidates.append(target)
                partial_platform_map[target.name] = eligible_platforms

        for target in strict_candidates:
            if len(shared_non_wechat_candidates) >= non_wechat_max_videos:
                break
            shared_non_wechat_candidates.append(target)
            shared_non_wechat_platform_map[target.name] = strict_platform_map.get(target.name, [])

        if (not shared_non_wechat_candidates) and partial_candidates:
            core._log(
                "[Scheduler] No fully-shared non-wechat candidate found; "
                "fallback to partial-shared mode."
            )
            for target in partial_candidates:
                if len(shared_non_wechat_candidates) >= non_wechat_max_videos:
                    break
                shared_non_wechat_candidates.append(target)
                shared_non_wechat_platform_map[target.name] = partial_platform_map.get(target.name, [])

        for target in shared_non_wechat_candidates:
            for platform in shared_non_wechat_platform_map.get(target.name, []):
                non_wechat_queues.setdefault(platform, []).append(target)

    # 1.5) Xiaohongshu-only top-up:
    # keep video quota first, then append fixed-size image extras.
    if "xiaohongshu" in non_wechat_platforms and not image_only_mode:
        queued_xhs_names = {p.name for p in non_wechat_queues.get("xiaohongshu", [])}
        queued_xhs_video_names = {
            p.name
            for p in non_wechat_queues.get("xiaohongshu", [])
            if p.exists() and p.is_file() and core._is_video_file(p)
        }
        need_xhs_video = max(0, non_wechat_max_videos - len(queued_xhs_video_names))
        if need_xhs_video > 0:
            xhs_video_pool = core._build_shared_source_targets(
                ctx.workspace,
                candidates,
                pool_size=max(non_wechat_max_videos * 12, 24),
                exclude_keywords=ctx.exclude_keywords,
                require_any_keywords=ctx.require_any_keywords,
                include_images=False,
            )
            for target in xhs_video_pool:
                if len(xhs_video_topup_queue) >= need_xhs_video:
                    break
                if not target.exists() or (not target.is_file()):
                    continue
                if target.name in queued_xhs_names:
                    continue
                if not core._is_video_file(target):
                    continue
                snapshot, eligible_platforms = _coordination_eligible_platforms(
                    ctx=ctx,
                    args=args,
                    target=target,
                    platforms=["xiaohongshu"],
                )
                if "xiaohongshu" not in eligible_platforms:
                    review_status = str(snapshot.get("review_status") or "").strip().lower()
                    if review_status and review_status != "approved":
                        core._log(
                            f"[Scheduler:xiaohongshu] Skip xhs video top-up by review status: "
                            f"{target.name} (status={review_status})"
                        )
                        continue
                    platform_state = dict((snapshot.get("platform_status") or {}).get("xiaohongshu", {}) or {})
                    reason = str(platform_state.get("reason") or "fingerprint")
                    core._log(
                        f"[Scheduler:xiaohongshu] Skip xhs video top-up candidate (duplicate): "
                        f"{target.name} (reason={reason})"
                    )
                    continue
                xhs_video_topup_queue.append(target)
                non_wechat_queues.setdefault("xiaohongshu", []).append(target)
                queued_xhs_names.add(target.name)
                queued_xhs_video_names.add(target.name)
            if xhs_video_topup_queue:
                core._log(
                    f"[Scheduler:xiaohongshu] Added xhs video top-up: +{len(xhs_video_topup_queue)} "
                    f"(video_plan={non_wechat_max_videos})"
                )

        if xiaohongshu_allow_image and xiaohongshu_extra_images_per_run > 0:
            xhs_image_pool = core._build_shared_source_targets(
                ctx.workspace,
                candidates,
                pool_size=max(xiaohongshu_extra_images_per_run * 12, 24),
                exclude_keywords=ctx.exclude_keywords,
                require_any_keywords=ctx.require_any_keywords,
                include_images=True,
            )
            for target in xhs_image_pool:
                if len(xhs_image_extra_queue) >= xiaohongshu_extra_images_per_run:
                    break
                if not target.exists() or (not target.is_file()):
                    continue
                if target.name in queued_xhs_names:
                    continue
                if not core._is_image_file(target):
                    continue
                snapshot, eligible_platforms = _coordination_eligible_platforms(
                    ctx=ctx,
                    args=args,
                    target=target,
                    platforms=["xiaohongshu"],
                )
                if "xiaohongshu" not in eligible_platforms:
                    review_status = str(snapshot.get("review_status") or "").strip().lower()
                    if review_status and review_status != "approved":
                        core._log(
                            f"[Scheduler:xiaohongshu] Skip xhs image extra by review status: "
                            f"{target.name} (status={review_status})"
                        )
                        continue
                    platform_state = dict((snapshot.get("platform_status") or {}).get("xiaohongshu", {}) or {})
                    reason = str(platform_state.get("reason") or "fingerprint")
                    core._log(
                        f"[Scheduler:xiaohongshu] Skip xhs image extra candidate (duplicate): "
                        f"{target.name} (reason={reason})"
                    )
                    continue
                xhs_image_extra_queue.append(target)
                queued_xhs_names.add(target.name)
            if xhs_image_extra_queue:
                core._log(
                    f"[Scheduler:xiaohongshu] Added xhs image extras: +{len(xhs_image_extra_queue)} "
                    f"(target={xiaohongshu_extra_images_per_run})"
                )

    for platform in non_wechat_platforms:
        core._log(
            f"[Scheduler:{platform}] Candidate queue(shared-first): {len(non_wechat_queues.get(platform, []))}"
        )

    # 2) 瑙嗛鍙凤細浠呰崏绋匡紝浼樺厛鎵ц锛岄伩鍏嶈鍚庣画骞冲彴闀胯€楁椂闃诲銆?
    if wechat_enabled:
        wechat_fallback_used = False
        wechat_candidates = [
            p
            for p in candidates
            if p.exists() and p.is_file() and core._is_video_file(p)
        ]
        if not wechat_candidates:
            fallback_pool = core._build_shared_source_targets(
                ctx.workspace,
                [],
                pool_size=max(non_wechat_max_videos * 12, 24),
                exclude_keywords=ctx.exclude_keywords,
                require_any_keywords=ctx.require_any_keywords,
                include_images=False,
            )
            wechat_candidates = [
                p
                for p in fallback_pool
                if p.exists() and p.is_file() and core._is_video_file(p)
            ]
            if wechat_candidates:
                wechat_fallback_used = True
                core._log(
                    "[Scheduler:wechat] No current-cycle video; "
                    f"fallback to 2_Processed video pool: {len(wechat_candidates)} candidates."
                )
            else:
                core._log("[Scheduler:wechat] No current-cycle video and no fallback video in 2_Processed.")
        wechat_queue = _select_targets_for_platform(
            ctx.workspace,
            platform="wechat",
            candidates=wechat_candidates,
            exclude_keywords=ctx.exclude_keywords,
            require_any_keywords=ctx.require_any_keywords,
        )
        wechat_queue = [
            target
            for target in wechat_queue
            if "wechat"
            in _coordination_eligible_platforms(
                ctx=ctx,
                args=args,
                target=target,
                platforms=["wechat"],
            )[1]
        ]
        if args.publish_only and ctx.requested_limit > 0 and len(wechat_queue) > ctx.requested_limit:
            wechat_queue = wechat_queue[: ctx.requested_limit]
            core._log(
                "[Scheduler:wechat] Publish-only queue truncated: "
                f"{len(wechat_queue)} (limit={ctx.requested_limit})."
            )
        if wechat_fallback_used and len(wechat_queue) > non_wechat_max_videos:
            wechat_queue = wechat_queue[:non_wechat_max_videos]
            core._log(
                "[Scheduler:wechat] Fallback queue truncated: "
                f"{len(wechat_queue)} (limit={non_wechat_max_videos})."
            )
        core._log(f"[Scheduler:wechat] Candidate queue: {len(wechat_queue)}")
        wechat_publish_mode = _resolve_platform_publish_mode(args, "wechat")
        if wechat_publish_mode.publish_now:
            wechat_stage_label = "Immediate publish"
            wechat_stage_prefix = "immediate"
        elif wechat_publish_mode.save_draft:
            wechat_stage_label = "Draft-only"
            wechat_stage_prefix = "draft"
        else:
            wechat_stage_label = "Upload-only"
            wechat_stage_prefix = "upload_only"
        for idx, target in enumerate(wechat_queue, 1):
            _mark_planned(target, "wechat")
            core._log(f"[Scheduler:wechat] {wechat_stage_label} {idx}/{len(wechat_queue)}: {target.name}")
            publish_ok = _publish_once(
                ctx,
                args,
                email_settings,
                "wechat",
                target,
                stage=f"{wechat_stage_prefix}_{idx}",
                events=publish_events,
            )
            if not publish_ok:
                last_error = ""
                if publish_events:
                    last_event = publish_events[-1]
                    if last_event.platform == "wechat" and last_event.video_name == target.name:
                        last_error = str(last_event.error or "")
                if _is_chrome_debug_not_ready_error(last_error):
                    core._log(
                        "[Scheduler:wechat] Chrome debug port not ready; "
                        "abort remaining wechat queue in this run."
                    )
                    break

    # 3) Douyin/Xiaohongshu/Kuaishou/Bilibili:
    #    share the same video slots across platforms.
    #    window>0 uses random slot scheduling; window<=0 publishes immediately.
    raw_window_minutes = int(args.non_wechat_random_window_minutes)
    immediate_dispatch = raw_window_minutes <= 0
    window_minutes = max(1, raw_window_minutes)
    if immediate_dispatch:
        core._log("[Scheduler] Immediate dispatch enabled for non-wechat platforms.")
    if shared_non_wechat_candidates:
        if immediate_dispatch:
            total = len(shared_non_wechat_candidates)
            for slot_idx, target in enumerate(shared_non_wechat_candidates, 1):
                slot_platforms = shared_non_wechat_platform_map.get(target.name, [])
                if not slot_platforms:
                    continue
                ordered_platforms = (
                    random.sample(slot_platforms, len(slot_platforms))
                    if len(slot_platforms) > 1
                    else list(slot_platforms)
                )
                order_total = len(ordered_platforms)
                for order_idx, platform in enumerate(ordered_platforms, 1):
                    _mark_planned(target, platform)
                    core._log(
                        f"[Scheduler:{platform}] Immediate publish {slot_idx}/{total} "
                        f"(order {order_idx}/{order_total}): {target.name}"
                    )
                    _publish_once(
                        ctx,
                        args,
                        email_settings,
                        platform,
                        target,
                        stage=f"immediate_{slot_idx}_of_{total}_p{order_idx}_of_{order_total}",
                        events=publish_events,
                    )
        else:
            cycle_start = time.time()
            events: list[tuple[float, str, int, int, Path, int, int]] = []
            slot_offsets = _random_offsets_within_window(len(shared_non_wechat_candidates), window_minutes)
            for slot_idx, (target, offset_seconds) in enumerate(zip(shared_non_wechat_candidates, slot_offsets), 1):
                slot_platforms = shared_non_wechat_platform_map.get(target.name, [])
                if not slot_platforms:
                    continue
                ordered_platforms = (
                    random.sample(slot_platforms, len(slot_platforms))
                    if len(slot_platforms) > 1
                    else list(slot_platforms)
                )
                drift_seconds = 0
                for order_idx, platform in enumerate(ordered_platforms, 1):
                    if order_idx > 1:
                        drift_seconds += random.randint(8, 45)
                    due_ts = cycle_start + float(offset_seconds) + float(drift_seconds)
                    _mark_planned(target, platform)
                    events.append(
                        (
                            due_ts,
                            platform,
                            slot_idx,
                            len(shared_non_wechat_candidates),
                            target,
                            order_idx,
                            len(ordered_platforms),
                        )
                    )

            events.sort(key=lambda item: item[0])
            for due_ts, platform, idx, total, target, order_idx, order_total in events:
                wait_seconds = max(0.0, due_ts - time.time())
                if wait_seconds > 0:
                    core._log(
                        f"[Scheduler:{platform}] Waiting {int(wait_seconds)}s "
                        f"for random publish slot {idx}/{total} (order {order_idx}/{order_total})."
                    )
                    time.sleep(wait_seconds)
                core._log(
                    f"[Scheduler:{platform}] Random publish slot {idx}/{total} "
                    f"(order {order_idx}/{order_total}): {target.name}"
                )
                _publish_once(
                    ctx,
                    args,
                    email_settings,
                    platform,
                    target,
                    stage=f"random_{idx}_of_{total}_p{order_idx}_of_{order_total}",
                    events=publish_events,
                )
    elif non_wechat_platforms:
        core._log("[Scheduler] No shared non-wechat candidate found for this cycle.")

    # 2.5) Xiaohongshu video top-up queue (ensures video quota first).
    if xhs_video_topup_queue:
        if immediate_dispatch:
            total = len(xhs_video_topup_queue)
            for idx, target in enumerate(xhs_video_topup_queue, 1):
                _mark_planned(target, "xiaohongshu")
                core._log(f"[Scheduler:xiaohongshu] Immediate xhs-video-topup {idx}/{total}: {target.name}")
                _publish_once(
                    ctx,
                    args,
                    email_settings,
                    "xiaohongshu",
                    target,
                    stage=f"immediate_xhs_video_topup_{idx}_of_{total}",
                    events=publish_events,
                )
        else:
            offsets = _random_offsets_within_window(len(xhs_video_topup_queue), window_minutes)
            cycle_start = time.time()
            for idx, (target, offset_seconds) in enumerate(zip(xhs_video_topup_queue, offsets), 1):
                due_ts = cycle_start + float(offset_seconds)
                wait_seconds = max(0.0, due_ts - time.time())
                if wait_seconds > 0:
                    core._log(
                        f"[Scheduler:xiaohongshu] Waiting {int(wait_seconds)}s "
                        f"for xhs-video-topup random slot {idx}/{len(xhs_video_topup_queue)}."
                    )
                    time.sleep(wait_seconds)
                _mark_planned(target, "xiaohongshu")
                core._log(
                    f"[Scheduler:xiaohongshu] Random xhs-video-topup "
                    f"{idx}/{len(xhs_video_topup_queue)}: {target.name}"
                )
                _publish_once(
                    ctx,
                    args,
                    email_settings,
                    "xiaohongshu",
                    target,
                    stage=f"random_xhs_video_topup_{idx}_of_{len(xhs_video_topup_queue)}",
                    events=publish_events,
                )

    # 2.6) Xiaohongshu image extra queue (fixed per time slot).
    if xhs_image_extra_queue:
        if immediate_dispatch:
            total = len(xhs_image_extra_queue)
            for idx, target in enumerate(xhs_image_extra_queue, 1):
                _mark_planned(target, "xiaohongshu")
                core._log(f"[Scheduler:xiaohongshu] Immediate xhs-image-extra {idx}/{total}: {target.name}")
                _publish_once(
                    ctx,
                    args,
                    email_settings,
                    "xiaohongshu",
                    target,
                    stage=f"immediate_xhs_image_extra_{idx}_of_{total}",
                    events=publish_events,
                )
        else:
            offsets = _random_offsets_within_window(len(xhs_image_extra_queue), window_minutes)
            cycle_start = time.time()
            for idx, (target, offset_seconds) in enumerate(zip(xhs_image_extra_queue, offsets), 1):
                due_ts = cycle_start + float(offset_seconds)
                wait_seconds = max(0.0, due_ts - time.time())
                if wait_seconds > 0:
                    core._log(
                        f"[Scheduler:xiaohongshu] Waiting {int(wait_seconds)}s "
                        f"for xhs-image-extra random slot {idx}/{len(xhs_image_extra_queue)}."
                    )
                    time.sleep(wait_seconds)
                _mark_planned(target, "xiaohongshu")
                core._log(
                    f"[Scheduler:xiaohongshu] Random xhs-image-extra "
                    f"{idx}/{len(xhs_image_extra_queue)}: {target.name}"
                )
                _publish_once(
                    ctx,
                    args,
                    email_settings,
                    "xiaohongshu",
                    target,
                    stage=f"random_xhs_image_extra_{idx}_of_{len(xhs_image_extra_queue)}",
                    events=publish_events,
                )

    core._log("[Scheduler] Publish schedule completed for this crawl.")
    pending_after = _build_pending_counts(ctx, count_platforms)
    if not bool(getattr(args, "disable_publish_summary_notify", False)):
        subject, lines = _build_publish_summary(
            ctx=ctx,
            events=publish_events,
            pending_before=pending_before,
            pending_after=pending_after,
            platforms_for_count=count_platforms,
        )
        _send_publish_summary_notification(email_settings, subject, lines)

    _recycle_fully_published_videos(
        ctx=ctx,
        args=args,
        target_by_name=target_by_name,
        planned_platforms_by_video=planned_platforms_by_video,
        publish_events=publish_events,
    )


def _run_one_cycle(args: argparse.Namespace, email_settings: EmailSettings) -> int:
    _proxy = str(getattr(args, "proxy", "") or "").strip() or None
    _use_system_proxy = bool(getattr(args, "use_system_proxy", False))
    _proxy, _use_system_proxy = core._resolve_network_proxy(_proxy, use_system_proxy=_use_system_proxy)
    core._apply_runtime_network_env(proxy=_proxy, use_system_proxy=_use_system_proxy)

    if args.publish_only:
        ctx = _build_publish_only_context(args)
    else:
        if not bool(getattr(args, "no_telegram_collect_notify", False)):
            _collect_subject, collect_body = _build_collect_start_message(args)
            _send_telegram_text(email_settings, collect_body, disable_web_page_preview=True, parse_mode="HTML")
        try:
            ctx = _run_collect_once(args, email_settings=email_settings)
        except Exception as exc:
            _send_login_required_alert(
                args=args,
                settings=email_settings,
                platform="collect",
                stage="collect",
                error_text=str(exc),
                debug_port=int(args.debug_port),
                chrome_user_data_dir=str(args.chrome_user_data_dir or ""),
            )
            raise
        if not bool(getattr(args, "no_telegram_collect_notify", False)):
            _collect_subject, collect_body = _build_collect_summary_message(ctx)
            _send_telegram_text(email_settings, collect_body, disable_web_page_preview=True, parse_mode="HTML")
    if not args.publish_only:
        _run_telegram_prefilter(ctx, args, email_settings)
    else:
        core._log("[Prefilter] publish-only mode: skip Telegram prefilter (new-collection only).")
    if args.collect_only:
        core._log("[Runner] collect-only mode enabled, skip publish schedule (prefilter done).")
        return 0
    _run_publish_schedule(ctx, args, email_settings)
    return 0


def _mask_value(key: str, value: Any) -> Any:
    token = str(key or "").lower()
    if any(x in token for x in ("token", "key", "secret", "password", "webhook", "chat_id")):
        text = str(value or "").strip()
        if not text:
            return ""
        if len(text) <= 6:
            return "***"
        return f"{text[:3]}***{text[-2:]}"
    return value


def _mask_payload(payload: Any, key_hint: str = "") -> Any:
    if isinstance(payload, dict):
        return {str(k): _mask_payload(v, key_hint=str(k)) for k, v in payload.items()}
    if isinstance(payload, list):
        return [_mask_payload(x, key_hint=key_hint) for x in payload]
    return _mask_value(key_hint, payload)


def _build_effective_runtime_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    workspace = core.init_workspace(str(args.workspace))
    config_path = str(args.config or core.DEFAULT_CONFIG_PATH)
    loaded_cfg: dict[str, Any] = {}
    try:
        loaded_cfg = core.load_config(config_path)
    except Exception as exc:
        loaded_cfg = {"_config_load_error": str(exc)}
    return {
        "entry": {
            "cli_module": "Collection.cybercar.cybercar_video_capture_and_publishing_module.cli",
            "main_module": "Collection.cybercar.cybercar_video_capture_and_publishing_module.hourly_distribution",
            "windows_runners": [
                "Collection/scripts/windows/run_cybercar_hourly.ps1",
                "Collection/scripts/windows/run_cybercar_distribution_hourly.ps1",
            ],
        },
        "config_sources": {
            "priority": ["cli_args", "environment_variables", "config_file", "code_defaults"],
            "config_path": config_path,
            "workspace": str(args.workspace or core.DEFAULT_WORKSPACE),
        },
        "runtime_paths": {
            "workspace_root": str(workspace.root),
            "downloads": str(workspace.downloads),
            "processed": str(workspace.processed),
            "archive": str(workspace.archive),
            "logs": str(workspace.root / "runtime" / "logs"),
        },
        "effective_args_masked": _mask_payload(vars(args)),
        "effective_config_masked": _mask_payload(loaded_cfg),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Hourly CyberCar pipeline: collect from X, process, export sorted outputs, "
            "then distribute by platform with delayed follow-up scheduling."
        )
    )
    parser.add_argument("--config", default=core.DEFAULT_CONFIG_PATH)
    parser.add_argument("--workspace", default=core.DEFAULT_WORKSPACE)
    parser.add_argument("--keyword", default=core.DEFAULT_KEYWORD)
    parser.add_argument("--limit", type=int, default=DEFAULT_COLLECT_LIMIT)
    parser.add_argument("--collect-media-kind", default="video", help=argparse.SUPPRESS)
    parser.add_argument(
        "--require-text-keyword-match",
        action="store_true",
        help="Require collected X candidates to contain the keyword in tweet text.",
    )
    parser.add_argument("--tweet-url", action="append", default=[], help="Specific X post URL(s).")
    parser.add_argument("--tweet-url-file", default="", help="Text file with one X URL per line.")
    parser.add_argument("--no-x-auto-discover", action="store_true")
    parser.add_argument(
        "--require-x-live-discovery",
        action="store_true",
        help="Fail the run if X keyword live-page discovery is unavailable; do not fall back to seed accounts.",
    )
    parser.add_argument("--x-discovery-url-limit", type=int, default=core.X_DISCOVERY_URL_LIMIT)
    parser.add_argument("--x-discovery-scroll-rounds", type=int, default=core.X_DISCOVERY_SCROLL_ROUNDS)
    parser.add_argument("--x-discovery-scroll-wait", type=float, default=core.X_DISCOVERY_SCROLL_WAIT_SECONDS)
    parser.add_argument("--x-download-socket-timeout", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--x-download-extractor-retries", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--x-download-retries", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--x-download-fragment-retries", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--x-download-retry-sleep", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--x-download-batch-retry-sleep",
        type=float,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--x-download-fail-fast",
        action="store_true",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--proxy", default="", help="HTTP proxy, e.g. http://127.0.0.1:PORT")
    parser.add_argument(
        "--use-system-proxy",
        action="store_true",
        default=bool(core._env_bool_first(["CYBERCAR_USE_SYSTEM_PROXY"], default=False)),
        help="Use system/env proxy when --proxy is empty. Default false (TUN/direct mode).",
    )
    parser.add_argument("--debug-port", type=int, default=core.DEFAULT_PORT)
    parser.add_argument("--chrome-path", default="")
    parser.add_argument("--chrome-user-data-dir", default=core.DEFAULT_CHROME_USER_DATA_DIR)
    parser.add_argument(
        "--wechat-debug-port",
        type=int,
        default=_env_int("CYBERCAR_WECHAT_CHROME_DEBUG_PORT", core.DEFAULT_PORT + 1),
        help="WeChat upload debug port; use independent port to keep login stable.",
    )
    parser.add_argument(
        "--wechat-chrome-user-data-dir",
        default=os.getenv("CYBERCAR_WECHAT_CHROME_USER_DATA_DIR", core.DEFAULT_CHROME_USER_DATA_DIR + "_WeChat"),
        help="WeChat upload Chrome user data dir; use independent profile to reduce relogin.",
    )
    parser.add_argument("--no-auto-open-chrome", action="store_true")
    parser.add_argument(
        "--monitor-url",
        default=_env_first("CYBERCAR_MONITOR_URL", default=DEFAULT_MONITOR_URL),
        help="Monitor URL shown in login-required alerts.",
    )
    parser.add_argument("--sorted-output-dir", default="", help="Optional root dir for time-sorted export batches.")
    parser.add_argument(
        "--recycle-bin-subdir",
        default=DEFAULT_RECYCLE_BIN_SUBDIR,
        help=f"Published-video recycle folder (absolute path or workspace-relative). Default: {DEFAULT_RECYCLE_BIN_SUBDIR}",
    )
    parser.add_argument("--collect-only", action="store_true", help="Only collect/process/export, do not publish.")
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Skip collecting and publish directly from existing 2_Processed candidate pool.",
    )
    parser.add_argument("--loop-forever", action="store_true", help="Run cycles forever on a fixed interval.")
    parser.add_argument("--crawl-interval-minutes", type=int, default=60, help="Interval between cycle starts.")
    align_group = parser.add_mutually_exclusive_group()
    align_group.add_argument("--align-to-hour", dest="align_to_hour", action="store_true", default=True)
    align_group.add_argument("--no-align-to-hour", dest="align_to_hour", action="store_false")

    parser.add_argument(
        "--upload-platforms",
        default="wechat,douyin,xiaohongshu,kuaishou",
        help="Comma-separated platforms: wechat,douyin,xiaohongshu,kuaishou,bilibili",
    )
    parser.set_defaults(xiaohongshu_allow_image=True)
    parser.add_argument(
        "--xiaohongshu-allow-image",
        dest="xiaohongshu_allow_image",
        action="store_true",
        help="Allow Xiaohongshu to collect/upload image media in addition to videos.",
    )
    parser.add_argument(
        "--xiaohongshu-video-only",
        dest="xiaohongshu_allow_image",
        action="store_false",
        help="Disable Xiaohongshu image media and keep video-only behavior.",
    )
    parser.add_argument(
        "--xiaohongshu-extra-images-per-run",
        type=int,
        default=DEFAULT_XIAOHONGSHU_EXTRA_IMAGES_PER_RUN,
        help=(
            "Extra Xiaohongshu image posts per run after video quota is filled. "
            f"Default: {DEFAULT_XIAOHONGSHU_EXTRA_IMAGES_PER_RUN}."
        ),
    )
    parser.add_argument("--upload-only-approved", action="store_true")
    parser.add_argument("--review-state-file", default="")
    parser.add_argument(
        "--no-telegram-prefilter",
        action="store_true",
        help="Disable Telegram prefilter messages before publish schedule.",
    )
    parser.add_argument(
        "--telegram-prefilter-skip-only",
        action="store_true",
        help="Use skip-only Telegram prefilter buttons; default is continue if untouched.",
    )
    parser.add_argument(
        "--telegram-prefilter-mode",
        default="default",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-telegram-collect-notify",
        action="store_true",
        help="Disable Telegram collect-start and collect-summary notifications for this run.",
    )
    parser.add_argument(
        "--no-publish-skip-notify",
        action="store_true",
        help="Suppress generic publish-skipped notifications for this run.",
    )
    parser.add_argument("--collection-name", default="")
    parser.add_argument("--caption", default="")
    parser.add_argument("--upload-timeout", type=int, default=core.UPLOAD_TIMEOUT_SECONDS)
    parser.add_argument("--no-save-draft", action="store_true")
    parser.add_argument(
        "--wechat-publish-now",
        action="store_true",
        help="Publish WeChat immediately after upload instead of saving draft.",
    )
    parser.add_argument(
        "--wechat-save-draft-only",
        action="store_true",
        help="Keep WeChat in draft-only mode even when running publish-only.",
    )
    parser.add_argument(
        "--wechat-declare-original",
        action="store_true",
        help="Enable WeChat original declaration before saving draft or publishing.",
    )
    parser.add_argument("--kuaishou-auto-publish-random-schedule", action="store_true")
    parser.add_argument("--kuaishou-random-schedule-max-minutes", type=int, default=45)
    parser.add_argument(
        "--bilibili-auto-publish-random-schedule",
        action="store_true",
        help="B绔欎笂浼犲悗鑷姩璁剧疆闅忔満瀹氭椂骞剁偣鍑诲彂甯冿紙闈炶崏绋匡級",
    )
    parser.add_argument(
        "--bilibili-random-schedule-max-minutes",
        type=int,
        default=DEFAULT_BILIBILI_RANDOM_SCHEDULE_MAX_MINUTES,
        help=(
            f"B站随机定时发布窗口上限（分钟），"
            f"会自动保证至少 {BILIBILI_RANDOM_SCHEDULE_MIN_LEAD_MINUTES} 分钟后发布，"
            f"默认 {DEFAULT_BILIBILI_RANDOM_SCHEDULE_MAX_MINUTES}。"
        ),
    )
    parser.add_argument("--followup-delay-minutes-min", type=int, default=3)
    parser.add_argument("--followup-delay-minutes-max", type=int, default=20)
    parser.add_argument("--min-gap-minutes", type=int, default=5)
    parser.add_argument(
        "--non-wechat-random-window-minutes",
        type=int,
        default=DEFAULT_NON_WECHAT_RANDOM_WINDOW_MINUTES,
        help=(
            "Random publish time window (minutes) for douyin/xiaohongshu/kuaishou/bilibili. "
            "Set 0 for immediate dispatch. Default: 0."
        ),
    )
    parser.add_argument(
        "--non-wechat-max-videos",
        type=int,
        default=DEFAULT_NON_WECHAT_MAX_VIDEOS,
        help="Per-platform max videos per run for douyin/xiaohongshu/kuaishou/bilibili. Default: 3.",
    )
    parser.add_argument(
        "--max-followup-posts",
        type=int,
        default=0,
        help="0 means no cap; otherwise stop after this many delayed follow-up publishes.",
    )

    source_cleanup_group = parser.add_mutually_exclusive_group()
    source_cleanup_group.add_argument("--auto-delete-source-files", action="store_true")
    source_cleanup_group.add_argument("--keep-source-files", action="store_true")

    parser.add_argument(
        "--notify-provider",
        default=_env_first("CYBERCAR_NOTIFY_PROVIDER", "NOTIFY_PROVIDER", default="telegram_bot"),
        help="Notification provider; default telegram_bot.",
    )
    parser.add_argument("--disable-notify", action="store_true")
    parser.add_argument(
        "--wecom-webhook-url",
        default=_env_first("CYBERCAR_NOTIFY_WECOM_WEBHOOK_URL", "NOTIFY_WECOM_WEBHOOK_URL", default=""),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--wecom-timeout-seconds",
        type=int,
        default=_env_int("CYBERCAR_NOTIFY_WECOM_TIMEOUT_SECONDS", _env_int("NOTIFY_WECOM_TIMEOUT_SECONDS", 20)),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--wecom-mentioned-list",
        default=_env_first("CYBERCAR_NOTIFY_WECOM_MENTIONED_LIST", "NOTIFY_WECOM_MENTIONED_LIST", default=""),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--wecom-mentioned-mobile-list",
        default=_env_first(
            "CYBERCAR_NOTIFY_WECOM_MENTIONED_MOBILE_LIST",
            "NOTIFY_WECOM_MENTIONED_MOBILE_LIST",
            default="",
        ),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--telegram-registry-file",
        default=_env_first(
            "CYBERCAR_NOTIFY_TELEGRAM_REGISTRY_FILE",
            "NOTIFY_TELEGRAM_REGISTRY_FILE",
            default="",
        ),
        help="Optional telegram bot registry file for identifier resolution.",
    )
    parser.add_argument(
        "--telegram-bot-token",
        default="",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default="",
    )
    parser.add_argument(
        "--telegram-timeout-seconds",
        type=int,
        default=_env_int("CYBERCAR_NOTIFY_TELEGRAM_TIMEOUT_SECONDS", _env_int("NOTIFY_TELEGRAM_TIMEOUT_SECONDS", 20)),
    )
    parser.add_argument(
        "--telegram-api-base",
        default=_env_first("CYBERCAR_NOTIFY_TELEGRAM_API_BASE", "NOTIFY_TELEGRAM_API_BASE", default=""),
    )
    parser.add_argument(
        "--notify-per-publish",
        action="store_true",
        help="Send notification for each publish item. Default is disabled; use summary-only notification.",
    )
    parser.add_argument(
        "--disable-publish-summary-notify",
        action="store_true",
        help="Disable consolidated publish summary notification.",
    )

    # Backward-compatible email args.
    parser.add_argument("--notify-email-to", default=DEFAULT_NOTIFY_EMAIL_TO)
    parser.add_argument("--disable-email-notify", action="store_true")
    parser.add_argument(
        "--email-provider",
        default=_env_first("CYBERCAR_NOTIFY_EMAIL_PROVIDER", "NOTIFY_EMAIL_PROVIDER", default="resend"),
    )
    parser.add_argument(
        "--notify-env-prefix",
        default=_normalize_env_prefix(
            _env_first("CYBERCAR_NOTIFY_ENV_PREFIX", "NOTIFY_ENV_PREFIX", default=DEFAULT_NOTIFY_ENV_PREFIX),
            DEFAULT_NOTIFY_ENV_PREFIX,
        ),
    )
    parser.add_argument(
        "--resend-api-key",
        default=_env_first("CYBERCAR_NOTIFY_RESEND_API_KEY", "NOTIFY_RESEND_API_KEY", default=""),
    )
    parser.add_argument(
        "--resend-from-email",
        default=_env_first("CYBERCAR_NOTIFY_RESEND_FROM_EMAIL", "NOTIFY_RESEND_FROM_EMAIL", default=""),
    )
    parser.add_argument(
        "--resend-endpoint",
        default=_env_first("CYBERCAR_NOTIFY_RESEND_ENDPOINT", "NOTIFY_RESEND_ENDPOINT", default=""),
    )
    parser.add_argument(
        "--resend-timeout-seconds",
        type=int,
        default=_env_int("CYBERCAR_NOTIFY_RESEND_TIMEOUT_SECONDS", _env_int("NOTIFY_RESEND_TIMEOUT_SECONDS", 20)),
    )
    # Legacy args kept for compatibility with existing scheduled tasks.
    parser.add_argument("--smtp-host", default="", help=argparse.SUPPRESS)
    parser.add_argument("--smtp-port", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--smtp-user", default="", help=argparse.SUPPRESS)
    parser.add_argument("--smtp-password", default="", help=argparse.SUPPRESS)
    parser.add_argument("--smtp-from", default="", help=argparse.SUPPRESS)
    parser.add_argument("--smtp-use-ssl", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--print-effective-config",
        action="store_true",
        help="Print effective args/config (masked) and exit.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if bool(getattr(args, "print_effective_config", False)):
        print(json.dumps(_build_effective_runtime_snapshot(args), ensure_ascii=False, indent=2))
        return 0
    email_settings = _build_email_settings(args)

    interval_minutes = max(1, int(args.crawl_interval_minutes))
    if not args.loop_forever:
        return _run_one_cycle(args, email_settings)

    cycle_no = 0
    while True:
        cycle_no += 1
        started_at = time.time()
        core._log(f"[Runner] Cycle #{cycle_no} started.")
        try:
            _run_one_cycle(args, email_settings)
            core._log(f"[Runner] Cycle #{cycle_no} finished.")
        except Exception as exc:
            core._log(f"[Runner] Cycle #{cycle_no} failed: {exc}")

        if bool(args.align_to_hour):
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            sleep_seconds = int((next_hour - now).total_seconds())
            if sleep_seconds > 0:
                core._log(
                    f"[Runner] Sleeping {sleep_seconds}s until next full hour "
                    f"({next_hour.strftime('%Y-%m-%d %H:%M:%S')})."
                )
                time.sleep(sleep_seconds)
            else:
                core._log("[Runner] Already at boundary; starting next cycle immediately.")
        else:
            elapsed = time.time() - started_at
            sleep_seconds = int(interval_minutes * 60 - elapsed)
            if sleep_seconds > 0:
                core._log(f"[Runner] Sleeping {sleep_seconds}s before next cycle.")
                time.sleep(sleep_seconds)
            else:
                core._log("[Runner] Cycle ran longer than interval; starting next cycle immediately.")


if __name__ == "__main__":
    raise SystemExit(main())
