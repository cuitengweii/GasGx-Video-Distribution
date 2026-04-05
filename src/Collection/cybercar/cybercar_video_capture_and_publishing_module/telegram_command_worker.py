from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence
from urllib.parse import quote, urlparse

try:
    import winreg  # type: ignore
except Exception:  # pragma: no cover
    winreg = None  # type: ignore
try:
    import ctypes
except Exception:  # pragma: no cover
    ctypes = None  # type: ignore

from Collection.shared.common.bot_notify import resolve_telegram_bot_settings as _resolve_telegram_bot_settings
from Collection.shared.common.telegram_api import call_telegram_api as _shared_call_telegram_api
from Collection.shared.common.telegram_ui import (
    answer_interaction_toast,
    build_action_feedback,
    build_home_callback_data,
    build_telegram_home,
    parse_home_callback_data,
    send_interaction_result,
    send_or_update_home_message,
)

try:
    from . import main as core
except Exception:
    import main as core  # type: ignore

try:
    from cybercar.settings import get_paths as _get_cybercar_paths
except Exception:
    _get_cybercar_paths = None  # type: ignore


def _load_engagement_module() -> Any:
    try:
        from cybercar import engagement as engagement_module
    except Exception:
        import importlib

        engagement_module = importlib.import_module("cybercar.engagement")
    return engagement_module


def _default_repo_root_path() -> Path:
    if _get_cybercar_paths is not None:
        try:
            return _get_cybercar_paths().repo_root
        except Exception:
            pass
    return Path(__file__).resolve().parents[4]


def _default_workspace_path() -> Path:
    if _get_cybercar_paths is not None:
        try:
            return _get_cybercar_paths().runtime_root
        except Exception:
            pass
    return (_default_repo_root_path() / "runtime").resolve()


def _default_runtime_config_path(repo_root: Path) -> Path:
    return (Path(repo_root) / "config" / "app.json").resolve()


_PREFERRED_WORKSPACE = _default_workspace_path()
DEFAULT_WORKSPACE = str(_PREFERRED_WORKSPACE)
DEFAULT_REPO_ROOT = str(_default_repo_root_path())
DEFAULT_PROFILE = "cybertruck"
DEFAULT_DOMESTIC_COLLECT_PUBLISH_PROFILE = "x_to_cn"
DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE = "cn_to_global"
COLLECT_PUBLISH_ACTION = "collect_publish_latest"
COLLECT_PUBLISH_ACTION_DOMESTIC = "collect_publish_latest_domestic"
COLLECT_PUBLISH_ACTION_GLOBAL = "collect_publish_latest_global"
COLLECT_PUBLISH_MENU_ACTION = "collect_publish_latest_menu"
COLLECT_PUBLISH_MENU_ACTION_DOMESTIC = "collect_publish_latest_menu_domestic"
COLLECT_PUBLISH_MENU_ACTION_GLOBAL = "collect_publish_latest_menu_global"
DEFAULT_UNIFIED_RUNNER_REL = Path("scripts") / "telegram_unified_runner.ps1"
DEFAULT_PROFILE_CONFIG_REL = Path("config") / "profiles.json"
DEFAULT_OFFSET_FILE = Path("runtime") / "telegram_command_worker_offset.txt"
DEFAULT_STATE_FILE = Path("runtime") / "telegram_command_worker_state.json"
DEFAULT_HOME_STATE_FILE = Path("runtime") / "telegram_command_worker_home.json"
DEFAULT_HOME_SHORTCUT_STATE_FILE = Path("runtime") / "telegram_command_worker_home_shortcut.json"
DEFAULT_ACTION_QUEUE_FILE = Path("runtime") / "telegram_command_worker_action_queue.json"
DEFAULT_AUDIT_FILE = Path("runtime") / "telegram_command_worker_audit.jsonl"
DEFAULT_ERROR_EVENT_FILE = Path("runtime") / "logs" / "telegram_command_worker_errors.jsonl"
DEFAULT_LOG_SUBDIR = Path("runtime") / "logs"
DEFAULT_POLLER_LOCK_DIR = Path("runtime") / "telegram_command_worker.poller.lock"
DEFAULT_POLLER_LOCK_STARTUP_GRACE_SECONDS = 15
DEFAULT_POLLER_LOCK_STALE_SECONDS = 300
DEFAULT_WECHAT_COMMENT_REPLY_STATE_FILE = Path("runtime") / "wechat_comment_reply_state.json"
DEFAULT_POLL_INTERVAL_SECONDS = 0
DEFAULT_POLL_TIMEOUT_SECONDS = 10
MAX_BLOCKING_WAIT_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS = MAX_BLOCKING_WAIT_SECONDS
DEFAULT_POLL_NETWORK_FAILURE_RESTART_THRESHOLD = 6
DEFAULT_POLL_NETWORK_FAILURE_RESTART_MIN_SPAN_SECONDS = 600
DEFAULT_POLL_NETWORK_ERROR_BACKOFF_MAX_SECONDS = 30
DEFAULT_TELEGRAM_POST_RETRY_COUNT = 3
DEFAULT_SET_COMMANDS_REFRESH_SECONDS = 6 * 3600
DEFAULT_SET_COMMANDS_FAILURE_BACKOFF_SECONDS = 10 * 60
DEFAULT_ATOMIC_WRITE_REPLACE_MAX_ATTEMPTS = 12
DEFAULT_ATOMIC_WRITE_REPLACE_BACKOFF_BASE_SECONDS = 0.05
DEFAULT_BOOTSTRAP_GETUPDATES_LIMIT = 100
DEFAULT_BOOTSTRAP_GETUPDATES_MAX_PAGES = 20
DEFAULT_HOME_FORCE_NEW_DEBOUNCE_SECONDS = 6
DEFAULT_HOME_SHORTCUT_DEBOUNCE_SECONDS = 3600
DEFAULT_HOME_SHORTCUT_KEYBOARD_VERSION = 6
DEFAULT_HOME_SURFACE_VERSION = 3
DEFAULT_ACTION_QUEUE_STALE_SECONDS = 1800
DEFAULT_ACTION_QUEUE_TERMINAL_RETENTION_SECONDS = 86400
DEFAULT_ACTION_QUEUE_MAX_TASKS = 50
DEFAULT_HOME_VISIBLE_TASK_LIMIT = 5
DEFAULT_PROCESS_STATUS_TASK_LIMIT = 4
DEFAULT_PROCESS_STATUS_PREFILTER_LIMIT = 3
DEFAULT_PROCESS_STATUS_LOG_TAIL_LINES = 8
DEFAULT_PROCESS_STATUS_LOG_SCAN_LINES = 40
DEFAULT_IMMEDIATE_REVIEW_WAIT_SECONDS = 18
DEFAULT_IMMEDIATE_CANDIDATE_LIMIT = 10
DEFAULT_IMMEDIATE_COLLECT_LOCK_RETRY_SECONDS = 12
DEFAULT_IMMEDIATE_COLLECT_LOCK_MAX_WAIT_SECONDS = MAX_BLOCKING_WAIT_SECONDS
DEFAULT_IMMEDIATE_PUBLISH_LOCK_RETRY_SECONDS = 15
DEFAULT_IMMEDIATE_PUBLISH_LOCK_MAX_WAIT_SECONDS = MAX_BLOCKING_WAIT_SECONDS
COLLECT_PUBLISH_CANDIDATE_OPTIONS = [1, 3, 5, 10, 15, 30]
COMMENT_REPLY_POST_OPTIONS = [3, 5, 7, 10]
COMMENT_REPLY_PLATFORM_ORDER = ["wechat", "douyin", "kuaishou"]
COLLECT_PUBLISH_DISCOVERY_MULTIPLIER = 4
COLLECT_PUBLISH_MAX_DISCOVERY_CANDIDATES = 120
COLLECT_PUBLISH_DISCOVERY_ROUND_MULTIPLIERS = (2, 4, 6)
DEFAULT_REVIEW_STATE_FILE = "review_state.json"
DEFAULT_TELEGRAM_PREFILTER_QUEUE_FILE = Path("runtime") / "telegram_prefilter_queue.json"
DEFAULT_TELEGRAM_PREFILTER_FEEDBACK_HISTORY_FILE = Path("runtime") / "telegram_prefilter_feedback_history.jsonl"
DEFAULT_PENDING_BACKGROUND_FEEDBACK_FILE = Path("runtime") / "telegram_pending_background_feedback.json"
DEFAULT_PLATFORM_RESULT_EVENT_DIR = Path("runtime") / "prefilter_platform_result_events"
DEFAULT_PIPELINE_PRIORITY_REQUEST_DIR = Path("runtime") / "pipeline_priority_requests"
DEFAULT_PREFILTER_QUEUE_LOCK_TIMEOUT_SECONDS = 30
DEFAULT_PREFILTER_QUEUE_TERMINAL_RETENTION_SECONDS = 3 * 86400
DEFAULT_PREFILTER_QUEUE_STATUS_WINDOW_SECONDS = 24 * 3600
DEFAULT_PREFILTER_QUEUE_ACTIVE_WINDOW_SECONDS = 30 * 60
DEFAULT_IMMEDIATE_PREFILTER_PENDING_EXPIRY_SECONDS = 10 * 60
DEFAULT_QUEUE_MAINTENANCE_INTERVAL_SECONDS = 5 * 60
DEFAULT_PLATFORM_LOCK_TIMEOUT_SECONDS = MAX_BLOCKING_WAIT_SECONDS
TELEGRAM_PREFILTER_CALLBACK_PREFIX = "ctpf"
TELEGRAM_MENU_CALLBACK_PREFIX = "ctm"
TELEGRAM_WECHAT_QR_CALLBACK_PREFIX = "ctqr"
TELEGRAM_ALLOWED_UPDATES = ["message", "edited_message", "callback_query"]
IMMEDIATE_COLLECT_REVIEW_WORKFLOW = "immediate_collect_review"
MAX_REPLY_CHARS = 3500
BOT_NAME = "CyberCar"
DEFAULT_CRAWL_TASK_NAME = "CyberCar_Crawl_Hourly"
DEFAULT_DISTRIBUTION_TASK_NAME = "CyberCar_Distribution_Hourly"
SCHEDULE_WINDOW_OPTIONS = [15, 30, 60]
PUBLISH_PLATFORM_ORDER = ["wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili"]
IMMEDIATE_COLLECT_MEDIA_KIND_ORDER = ["video", "image"]
IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY = {
    "video": "视频",
    "image": "图片",
}
IMMEDIATE_CANDIDATE_REUSE_STATUSES = {
    "down_confirmed",
    "link_pending",
    "download_running",
    "publish_partial",
    "publish_requested",
    "publish_confirm_pending",
    "publish_running",
}
IMMEDIATE_CANDIDATE_REISSUE_STATUSES = {
    "link_pending",
    "publish_requested",
    "publish_confirm_pending",
    "publish_running",
    "publish_partial",
    "publish_failed",
}
IMMEDIATE_CANDIDATE_TERMINAL_SKIP_STATUSES = {
    "down_confirmed",
    "expired_pending",
    "publish_done",
}
IMMEDIATE_STORY_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "after",
    "around",
    "be",
    "been",
    "but",
    "by",
    "caught",
    "for",
    "from",
    "got",
    "had",
    "has",
    "have",
    "he",
    "her",
    "hers",
    "him",
    "his",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "like",
    "live",
    "me",
    "my",
    "new",
    "no",
    "not",
    "of",
    "on",
    "or",
    "our",
    "she",
    "that",
    "the",
    "their",
    "them",
    "they",
    "this",
    "to",
    "using",
    "was",
    "were",
    "who",
    "with",
    "woman",
    "you",
    "your",
}
IMMEDIATE_PUBLISH_APPROVED_STATUSES = {
    "publish_requested",
    "download_running",
    "publish_confirm_pending",
    "publish_running",
    "publish_partial",
}
IMMEDIATE_COLLECT_MEDIA_KIND_PLATFORMS = {
    "video": PUBLISH_PLATFORM_ORDER.copy(),
    "image": ["douyin", "xiaohongshu", "kuaishou"],
}
COLLECT_SOURCE_PLATFORM_ORDER = ["x", "douyin", "xiaohongshu"]
COLLECT_SOURCE_PLATFORM_DISPLAY = {
    "x": "X",
    "douyin": "抖音",
    "xiaohongshu": "小红书",
}
COLLECT_SOURCE_PLATFORM_ALIAS_MAP = {
    "x": "x",
    "twitter": "x",
    "推特": "x",
    "douyin": "douyin",
    "dy": "douyin",
    "抖音": "douyin",
    "xiaohongshu": "xiaohongshu",
    "xhs": "xiaohongshu",
    "hongshu": "xiaohongshu",
    "xiaohongshu_note": "xiaohongshu",
    "小红书": "xiaohongshu",
}
IMAGE_PUBLISH_CARD_PLATFORM_ORDER = ["douyin", "xiaohongshu", "kuaishou"]
PUBLISH_PLATFORM_DISPLAY = {
    "wechat": "视频号",
    "douyin": "抖音",
    "xiaohongshu": "小红书",
    "kuaishou": "快手",
    "bilibili": "B站",
}
PUBLISH_PLATFORM_LOGO = {
    "collect": "🔎",
    "wechat": "📱",
    "douyin": "🎵",
    "xiaohongshu": "📝",
    "kuaishou": "⚡",
    "bilibili": "📺",
}
PUBLISH_PLATFORM_ALIAS_MAP = {
    "wechat": "wechat",
    "wx": "wechat",
    "weixin": "wechat",
    "shipinhao": "wechat",
    "视频号": "wechat",
    "douyin": "douyin",
    "dy": "douyin",
    "抖音": "douyin",
    "xiaohongshu": "xiaohongshu",
    "xhs": "xiaohongshu",
    "小红书": "xiaohongshu",
    "hongshu": "xiaohongshu",
    "kuaishou": "kuaishou",
    "ks": "kuaishou",
    "快手": "kuaishou",
    "bilibili": "bilibili",
    "bili": "bilibili",
    "b站": "bilibili",
    "哔哩哔哩": "bilibili",
}
ALL_PLATFORM_ALIAS_SET = {
    "all",
    "全部",
    "所有",
    "全平台",
    "全部平台",
}
TELEGRAM_COMMAND_SPECS = [
    {"command": "start", "description": "打开首页", "usage": "/start", "help_text": "打开固定首页卡片"},
    {"command": "help", "description": "查看入口说明", "usage": "/help", "help_text": "查看当前首页入口与兜底命令"},
    {"command": "wechat_login_qr", "description": "获取登录二维码", "usage": "/wechat_login_qr", "help_text": "首页不可用时手动获取视频号二维码"},
]
TELEGRAM_CLICKABLE_COMMANDS = [
    {"command": str(item.get("command") or "").strip(), "description": str(item.get("description") or "").strip()}
    for item in TELEGRAM_COMMAND_SPECS
    if str(item.get("command") or "").strip() and str(item.get("description") or "").strip()
]
HOME_ACTION_ASYNC_ACTIONS = {
    "login_qr",
    "collect_publish_latest",
    "comment_reply_run",
}
HOME_ACTION_LOADING_PLACEHOLDER_ACTIONS = {
    "login_qr",
    "collect_publish_latest",
}
HOME_ACTION_PIPELINE_GUARDED_ACTIONS = {
    "collect_publish_latest",
    "comment_reply_run",
}
HOME_ACTION_ACTIVE_STATUSES = {"queued", "running"}
HOME_ACTION_TERMINAL_STATUSES = {"done", "failed", "blocked"}

# Conservative allowlist for /run and "閹笛嗩攽 <cmd>".
DEFAULT_SHELL_ALLOW_PREFIXES = [
    "Get-Date",
    "Get-Location",
    "Get-ChildItem",
    "dir",
    "ls",
    "pwd",
    "whoami",
    "hostname",
    "python -V",
    "python --version",
    "git status",
]


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        key = str(name or "").strip()
        if not key:
            continue
        value = str(os.getenv(key, "") or "").strip()
        if value:
            return value
    return str(default or "").strip()


def _detect_windows_manual_proxy() -> tuple[str, bool]:
    if os.name != "nt" or winreg is None:
        return "", False
    key_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:  # type: ignore[arg-type]
            try:
                enabled = int(winreg.QueryValueEx(key, "ProxyEnable")[0] or 0) > 0
            except Exception:
                enabled = False
            try:
                server = str(winreg.QueryValueEx(key, "ProxyServer")[0] or "").strip()
            except Exception:
                server = ""
            try:
                pac_url = str(winreg.QueryValueEx(key, "AutoConfigURL")[0] or "").strip()
            except Exception:
                pac_url = ""
    except Exception:
        return "", False
    if enabled and server:
        return server, True
    if pac_url:
        return "", True
    return "", False


def _resolve_worker_network_mode() -> tuple[str, bool]:
    explicit_proxy = _env_first(
        "CYBERCAR_PROXY",
        "CYBERCAR_HTTP_PROXY",
        default="",
    )
    if explicit_proxy:
        return explicit_proxy, False
    use_system_proxy = str(
        _env_first("CYBERCAR_USE_SYSTEM_PROXY", default="")
    ).strip().lower() in {"1", "true", "yes", "on"}
    if use_system_proxy:
        return "", True
    _proxy_server, system_enabled = _detect_windows_manual_proxy()
    if system_enabled:
        return "", True
    return "", False


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_worker_time_text(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _append_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    line = f"[{_now_text()}] {message}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        try:
            safe_line = line.encode(getattr(sys.stdout, "encoding", None) or "utf-8", errors="replace").decode(
                getattr(sys.stdout, "encoding", None) or "utf-8",
                errors="replace",
            )
        except Exception:
            safe_line = line.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
        try:
            print(safe_line, flush=True)
        except Exception:
            pass
    try:
        with log_file.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def _normalize_blocking_timeout(value: Any, default: float, *, minimum: float = 1.0) -> float:
    normalized_minimum = max(1.0, min(float(MAX_BLOCKING_WAIT_SECONDS), float(minimum)))
    fallback = max(normalized_minimum, min(float(MAX_BLOCKING_WAIT_SECONDS), float(default)))
    try:
        parsed = float(value)
    except Exception:
        parsed = fallback
    return max(normalized_minimum, min(float(MAX_BLOCKING_WAIT_SECONDS), parsed))


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _error_event_file_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_ERROR_EVENT_FILE).resolve()


def _sanitize_error_context_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 4:
        return str(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        text = value.strip()
        return text if len(text) <= 4000 else (text[:4000] + "...(truncated)")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for idx, (key, inner) in enumerate(value.items()):
            if idx >= 64:
                normalized["__truncated__"] = f"+{len(value) - idx} keys"
                break
            normalized[str(key)] = _sanitize_error_context_value(inner, depth=depth + 1)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        normalized_list: list[Any] = []
        for idx, inner in enumerate(value):
            if idx >= 64:
                normalized_list.append(f"...(+{len(value) - idx} items)")
                break
            normalized_list.append(_sanitize_error_context_value(inner, depth=depth + 1))
        return normalized_list
    return str(value)


def _record_error_event(
    *,
    workspace: Optional[Path],
    log_file: Optional[Path],
    category: str,
    message: str,
    error_text: str = "",
    exc: Optional[BaseException] = None,
    context: Optional[Mapping[str, Any]] = None,
    retryable: Optional[bool] = None,
    severity: str = "error",
) -> None:
    event_file: Optional[Path] = None
    if isinstance(workspace, Path):
        event_file = _error_event_file_path(workspace)
    elif isinstance(log_file, Path):
        event_file = (log_file.parent / "telegram_command_worker_errors.jsonl").resolve()
    if event_file is None:
        return
    payload: dict[str, Any] = {
        "ts": _now_text(),
        "pid": int(os.getpid()),
        "category": str(category or "").strip() or "unknown",
        "severity": str(severity or "").strip().lower() or "error",
        "message": str(message or "").strip(),
    }
    effective_error = str(error_text or "").strip()
    if not effective_error and exc is not None:
        effective_error = str(exc or "").strip()
    if effective_error:
        payload["error"] = effective_error
    if exc is not None:
        payload["error_type"] = type(exc).__name__
        try:
            stack_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).strip()
        except Exception:
            stack_text = ""
        if stack_text:
            payload["stack"] = stack_text[:12000] + ("...(truncated)" if len(stack_text) > 12000 else "")
    if retryable is not None:
        payload["retryable"] = bool(retryable)
    if isinstance(context, Mapping) and context:
        payload["context"] = _sanitize_error_context_value(context)
    try:
        _append_jsonl(event_file, payload)
    except Exception as write_exc:
        if isinstance(log_file, Path):
            _append_log(log_file, f"[Worker] error event write failed category={payload.get('category')}: {write_exc}")


def _load_offset(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        raw = path.read_text(encoding="utf-8").strip()
        return int(raw) if raw else 0
    except Exception:
        return 0


def _save_offset(path: Path, value: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(int(value)), encoding="utf-8")


_TELEGRAM_TRANSPORT_ERROR_MARKERS = (
    "httpsconnectionpool",
    "connection aborted",
    "connection reset",
    "connectionreseterror",
    "connecttimeout",
    "connect time out",
    "read timeout",
    "readtimeout",
    "max retries exceeded",
    "temporary failure",
    "name or service not known",
    "nodename nor servname",
    "failed to establish a new connection",
    "remote end closed connection",
    "proxyerror",
    "sslerror",
    "connection refused",
    "bad gateway",
    "gateway timeout",
    "service unavailable",
    "upstream connect error",
)

_TELEGRAM_TIMEOUT_CONTEXT_MARKERS = (
    "api.telegram.org",
    "httpsconnectionpool",
    "getupdates",
    "answercallbackquery",
    "setmycommands",
    "sendmessage",
    "editmessagetext",
)


def _is_telegram_transport_error_text(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if any(marker in text for marker in _TELEGRAM_TRANSPORT_ERROR_MARKERS):
        return True
    if "timed out" in text:
        return any(marker in text for marker in _TELEGRAM_TIMEOUT_CONTEXT_MARKERS)
    return False


def _is_telegram_rate_limit_error_text(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "too many requests" in text:
        return True
    return "retry after" in text and "telegram" in text


def _extract_telegram_retry_after_seconds(value: str, *, fallback_seconds: int = 0, cap_seconds: int = 120) -> int:
    text = str(value or "").strip()
    if not text:
        return max(0, int(fallback_seconds))
    match = re.search(r"retry\s+after\s+(\d+)", text, flags=re.IGNORECASE)
    if not match:
        return max(0, int(fallback_seconds))
    seconds = max(0, int(match.group(1) or 0))
    capped = max(1, int(cap_seconds or 0))
    return min(seconds, capped)


def _is_telegram_poll_conflict_error_text(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return "terminated by other getupdates request" in text


def _is_prefilter_queue_lock_timeout_error_text(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "timed out waiting for lock" not in text:
        return False
    return "telegram_prefilter_queue.json" in text


def _is_prefilter_queue_io_contention_error_text(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "telegram_prefilter_queue.json" not in text:
        return False
    if "winerror 5" in text or "permissionerror" in text or "access is denied" in text:
        return True
    return ".tmp-" in text and "->" in text


def _is_stale_callback_query_error_text(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return (
        "query is too old" in text
        or "response timeout expired" in text
        or "query id is invalid" in text
    )


_URL_PATTERN = re.compile(r"https?://[^\s<>\"]+")
_SHARED_LINK_ACTIVE_STATUSES = {
    "collect_requested",
    "download_running",
    "publish_requested",
    "publish_confirm_pending",
    "publish_running",
    "publish_partial",
}


def _iter_message_urls(text: str) -> Iterable[str]:
    raw_text = str(text or "")
    if not raw_text:
        return []
    urls: list[str] = []
    for match in _URL_PATTERN.finditer(raw_text):
        token = str(match.group(0) or "").strip().rstrip(".,!?)]}>\"'")
        if token:
            urls.append(token)
    return urls


def _normalize_shared_x_status_url(raw_url: str) -> str:
    candidate = str(raw_url or "").strip()
    if not candidate:
        return ""
    try:
        parsed = urlparse(candidate)
    except Exception:
        return ""
    scheme = str(parsed.scheme or "").strip().lower()
    host = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if scheme not in {"http", "https"}:
        return ""
    if host.startswith("www."):
        host = host[4:]
    if host not in {"x.com", "twitter.com", "mobile.x.com", "mobile.twitter.com"}:
        return ""
    match = re.search(
        r"/(?:(?:i/(?:web/)?)|([A-Za-z0-9_]{1,32})/)?(?:status|statuses)/(\d+)(?:/|$)",
        path,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    username = str(match.group(1) or "").strip()
    status_id = str(match.group(2) or "").strip()
    if username:
        return f"https://{host}/{username}/status/{status_id}"
    return f"https://{host}/i/status/{status_id}"


def _extract_shared_immediate_source_url(text: str) -> str:
    for raw_url in _iter_message_urls(text):
        normalized = _normalize_shared_x_status_url(raw_url)
        if normalized:
            return normalized
    return ""


def _looks_like_shared_link_message(text: str) -> bool:
    return any(_iter_message_urls(text))


def _should_try_shared_link_message(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    if raw.startswith("/"):
        return False
    if _parse_status_command(raw) is not None:
        return False
    if _parse_run_command(raw) is not None:
        return False
    if bool(_parse_slash_command_request(raw).get("matched")):
        return False
    if lowered.startswith("执行 ") or lowered.startswith("cmd "):
        return False
    return True


def _shared_link_actor(username: str) -> str:
    token = str(username or "").strip()
    if not token:
        return "@telegram"
    return token if token.startswith("@") else f"@{token}"


def _build_shared_link_candidate_text(source_url: str, text: str) -> str:
    compact = re.sub(r"\s+", " ", str(text or "").strip())
    if not compact or compact == source_url:
        return "Telegram 分享链接"
    if len(compact) > 160:
        return compact[:157].rstrip() + "..."
    return compact


def _build_shared_link_status_card(
    *,
    item: Dict[str, Any],
    title: str,
    subtitle: str,
    status: str,
    result_items: Sequence[Any],
) -> Dict[str, Any]:
    source_url = str(item.get("source_url") or "").strip()
    sections = [
        {
            "title": "执行摘要",
            "emoji": "📌",
            "items": list(result_items[:1]),
        },
        {
            "title": "候选信息",
            "emoji": "🎯",
            "items": [
                {"label": "平台", "value": _resolve_immediate_item_platform_text(item, with_logo=True)},
                {"label": "标题", "value": _resolve_immediate_item_title(item)},
            ],
        },
    ]
    return _build_prefilter_action_card(
        status=status,
        title=title,
        subtitle=f"{_menu_breadcrumb_for_item(item)}｜{subtitle}",
        sections=sections,
        source_url=source_url,
        include_source_button=False,
        action_rows=[[{"text": "📍 进度", "callback_data": build_home_callback_data("cybercar", "process_status")}]],
        menu_label="",
        task_identifier="",
    )


def _probe_shared_link_media_kind(source_url: str) -> str:
    normalized_url = str(source_url or "").strip()
    if not normalized_url:
        return "video"
    match = re.search(r"/status(?:es)?/(\d+)(?:/|$)", normalized_url, flags=re.IGNORECASE)
    if not match:
        return "video"
    status_id = str(match.group(1) or "").strip()
    if not status_id:
        return "video"
    proxy, use_system_proxy = _resolve_worker_network_mode()
    try:
        payload = core._fetch_x_syndication_payload(
            status_id,
            proxy=(proxy or None),
            use_system_proxy=use_system_proxy,
        )
    except Exception:
        payload = None
    if not isinstance(payload, dict) or not payload:
        return "video"
    try:
        video_variants = core._extract_x_video_variants_from_payload(payload)
    except Exception:
        video_variants = []
    if isinstance(video_variants, list) and video_variants:
        return "video"
    try:
        photo_urls = core._extract_x_photo_urls_from_payload(payload)
    except Exception:
        photo_urls = []
    if isinstance(photo_urls, list) and photo_urls:
        return "image"
    return "video"


def _handle_shared_immediate_link_message(
    *,
    text: str,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    chat_id: str,
    username: str,
    bot_token: str,
    telegram_bot_identifier: str,
    default_profile: str,
    log_file: Path,
    immediate_test_mode: bool = False,
) -> Optional[Dict[str, Any]]:
    source_url = _extract_shared_immediate_source_url(text)
    if not source_url:
        if _looks_like_shared_link_message(text):
            return _normalize_reply_payload(
                {
                    "text": "当前只支持直接分享 X/Twitter 单条帖子链接，收到后会按视频即采即发链路处理。",
                    "reply_markup": _with_process_status_button(),
                }
            )
        return None

    profile = _normalize_profile_name(default_profile)
    actor = _shared_link_actor(username)
    media_kind = _probe_shared_link_media_kind(source_url)
    target_platforms = ",".join(_collect_publish_target_platforms(media_kind))
    candidate = {
        "url": source_url,
        "published_at": "",
        "display_time": "Telegram 分享",
        "tweet_text": _build_shared_link_candidate_text(source_url, text),
        "match_mode": "telegram_share_link",
        "matched_keyword": "telegram_share_link",
    }
    upserted = _upsert_immediate_candidate_item(
        workspace=workspace,
        candidate=candidate,
        profile=profile,
        media_kind=media_kind,
        target_platforms=target_platforms,
        chat_id=chat_id,
        item_index=1,
        total_count=1,
        allow_reuse=True,
    )
    item_id = str(upserted.get("item_id") or "").strip()
    existing_item = upserted.get("item") if isinstance(upserted.get("item"), dict) else {}
    item = dict(existing_item) if isinstance(existing_item, dict) else {}
    if not item_id or not item:
        raise RuntimeError("shared link immediate item create failed")

    status = str(item.get("status") or "").strip().lower()
    if status in _SHARED_LINK_ACTIVE_STATUSES:
        _append_log(log_file, f"[Worker] shared link deduped source={source_url} item={item_id} status={status}")
        return _normalize_reply_payload(
            _build_shared_link_status_card(
                item=item,
                title="分享链接已在处理中",
                subtitle="同一条链接已有即采即发任务在运行",
                status="running",
                result_items=[
                    "当前链接对应的任务已经在后台执行，无需重复分享。",
                    "进度和最终结果会继续通过 Telegram 回传。",
                ],
            )
        )

    updated_item = _update_prefilter_item(
        workspace,
        item_id,
        updates={
            "status": "publish_requested",
            "updated_at": _now_text(),
            "actor": actor,
            "action": "shared_link",
            "chat_id": str(chat_id or "").strip(),
            "wechat_declare_original": False,
            "immediate_test_mode": bool(immediate_test_mode),
            "shared_via": "telegram_link",
        },
    )
    try:
        _spawn_immediate_collect_item_job(
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=bot_token,
            telegram_chat_id=chat_id,
            item_id=item_id,
            immediate_test_mode=immediate_test_mode,
        )
    except Exception as exc:
        _append_log(log_file, f"[Worker] shared link immediate spawn failed: {exc}")
        failed_item = _update_prefilter_item(
            workspace,
            item_id,
            updates={
                "status": "link_pending",
                "last_error": str(exc),
                "action": "shared_link_spawn_failed",
            },
        )
        return _normalize_reply_payload(
            _build_shared_link_status_card(
                item=failed_item,
                title="分享链接启动失败",
                subtitle="后台即采即发任务未成功启动",
                status="failed",
                result_items=[
                    "分享链接已识别，但后台任务启动失败，请稍后重试。",
                ],
            )
        )

    _append_log(log_file, f"[Worker] shared link queued source={source_url} item={item_id}")
    media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
    start_note = (
        "测试模式已放宽前置过滤，后台仍会继续真实采集和平台排队。"
        if immediate_test_mode
        else f"已按{media_label}即采即发链路排队，后台会先采集素材，再继续进入平台发布。"
    )
    return _normalize_reply_payload(
        _build_shared_link_status_card(
            item=updated_item,
            title="分享链接已接收",
            subtitle=f"后台{media_label}即采即发任务已排队",
            status="running",
            result_items=[
                start_note,
                "如需查看当前进度，可直接点“进度”。",
            ],
        )
    )


def _post_telegram_multipart_with_retries(
    *,
    bot_token: str,
    method: str,
    data: Dict[str, Any],
    files: Dict[str, Any],
    timeout_seconds: int,
    api_base: str = "",
    max_retries: int = 2,
) -> Dict[str, Any]:
    return _shared_call_telegram_api(
        bot_token=bot_token,
        method=method,
        params=data,
        files=files,
        timeout_seconds=max(8, int(timeout_seconds or 20)),
        api_base=str(api_base or "").strip(),
        use_post=True,
        max_retries=max(0, int(max_retries or 0)),
    )


def _exception_text(exc: BaseException) -> str:
    text = str(exc or "").strip()
    if text:
        return text
    name = str(getattr(type(exc), "__name__", "") or "").strip()
    return name or "unknown error"


def _is_telegram_poll_network_error(exc: Exception) -> bool:
    return _is_telegram_transport_error_text(_exception_text(exc))


def _should_log_poll_transport_warning(
    *,
    consecutive_failures: int,
    last_logged_epoch: float,
    now_epoch: float,
    min_interval_seconds: int = 60,
) -> bool:
    failures = max(0, int(consecutive_failures))
    if failures <= 3:
        return True
    if float(last_logged_epoch or 0.0) <= 0:
        return True
    interval = max(1, int(min_interval_seconds))
    return (float(now_epoch) - float(last_logged_epoch)) >= float(interval)


def _compute_poll_transport_backoff_seconds(
    *,
    consecutive_failures: int,
    base_interval_seconds: int,
    max_backoff_seconds: int = DEFAULT_POLL_NETWORK_ERROR_BACKOFF_MAX_SECONDS,
) -> int:
    base_interval = max(0, int(base_interval_seconds))
    failures = max(0, int(consecutive_failures))
    capped_max_backoff = max(base_interval, int(max_backoff_seconds))
    if failures <= 0:
        return base_interval
    # Use bounded exponential backoff for transport jitter to avoid rapid restart loops.
    exponential = 2 ** min(failures - 1, 6)
    return max(base_interval, min(capped_max_backoff, exponential))


def _should_restart_after_poll_error(
    exc: Exception,
    consecutive_failures: int,
    threshold: int,
    *,
    failure_span_seconds: float = 0.0,
    min_failure_span_seconds: int = DEFAULT_POLL_NETWORK_FAILURE_RESTART_MIN_SPAN_SECONDS,
) -> bool:
    if threshold <= 0:
        return False
    if consecutive_failures < threshold:
        return False
    if not _is_telegram_poll_network_error(exc):
        return False
    min_span_seconds = max(0, int(min_failure_span_seconds))
    if min_span_seconds <= 0:
        return True
    return float(failure_span_seconds or 0.0) >= float(min_span_seconds)


def _telegram_api(
    *,
    bot_token: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: int,
    use_post: bool = False,
) -> Dict[str, Any]:
    return _shared_call_telegram_api(
        bot_token=bot_token,
        method=method,
        params=params,
        timeout_seconds=timeout_seconds,
        use_post=use_post,
        max_retries=(DEFAULT_TELEGRAM_POST_RETRY_COUNT if use_post else None),
    )


def _set_clickable_commands(*, bot_token: str, timeout_seconds: int, log_file: Path) -> None:
    payload = _telegram_api(
        bot_token=bot_token,
        method="setMyCommands",
        params={
            "commands": json.dumps(
                [{"command": "start", "description": "打开首页"}],
                ensure_ascii=False,
            )
        },
        timeout_seconds=timeout_seconds,
        use_post=True,
    )
    if not bool(payload.get("ok")):
        raise RuntimeError(str(payload.get("description") or "setMyCommands failed"))
    _append_log(log_file, "[Worker] setMyCommands updated: /start -> 打开首页")


def _parse_worker_timestamp(text: str) -> Optional[float]:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return float(datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").timestamp())
    except Exception:
        return None


def _should_attempt_set_commands(
    state: Mapping[str, Any],
    *,
    now_epoch: Optional[float] = None,
    refresh_seconds: int = DEFAULT_SET_COMMANDS_REFRESH_SECONDS,
    failure_backoff_seconds: int = DEFAULT_SET_COMMANDS_FAILURE_BACKOFF_SECONDS,
) -> bool:
    now_value = float(now_epoch if now_epoch is not None else time.time())
    success_ts = _parse_worker_timestamp(str(state.get("set_commands_updated_at") or ""))
    if success_ts is not None and (now_value - success_ts) < float(max(0, int(refresh_seconds or 0))):
        return False
    failure_text = str(state.get("set_commands_last_error") or "").strip()
    if not failure_text:
        return True
    attempt_ts = _parse_worker_timestamp(str(state.get("set_commands_last_attempt_at") or ""))
    if attempt_ts is None:
        return True
    return (now_value - attempt_ts) >= float(max(0, int(failure_backoff_seconds or 0)))


def _max_update_id_from_updates(updates: Sequence[Any]) -> int:
    max_update_id = 0
    for row in updates:
        if not isinstance(row, dict):
            continue
        max_update_id = max(max_update_id, int(row.get("update_id") or 0))
    return max_update_id


def _bootstrap_latest_update_id(
    *,
    bot_token: str,
    timeout_seconds: int,
    start_after_update_id: int = 0,
) -> int:
    cursor_update_id = max(0, int(start_after_update_id or 0))
    max_update_id = cursor_update_id
    page_limit = max(1, int(DEFAULT_BOOTSTRAP_GETUPDATES_LIMIT))
    max_pages = max(1, int(DEFAULT_BOOTSTRAP_GETUPDATES_MAX_PAGES))
    for _ in range(max_pages):
        params: Dict[str, Any] = {
            "timeout": 0,
            "limit": page_limit,
            "allowed_updates": json.dumps(TELEGRAM_ALLOWED_UPDATES, ensure_ascii=True),
        }
        if cursor_update_id > 0:
            params["offset"] = cursor_update_id + 1
        response = _telegram_api(
            bot_token=bot_token,
            method="getUpdates",
            params=params,
            timeout_seconds=max(5, int(timeout_seconds or 10)),
            use_post=False,
        )
        rows = response.get("result") if isinstance(response, dict) else []
        if not isinstance(rows, list) or not rows:
            break
        page_max_update_id = _max_update_id_from_updates(rows)
        if page_max_update_id <= cursor_update_id:
            break
        cursor_update_id = page_max_update_id
        max_update_id = max(max_update_id, page_max_update_id)
        if len(rows) < page_limit:
            break
    return max_update_id


def _extract_message(update: Dict[str, Any]) -> Optional[Dict[str, str]]:
    msg = update.get("message")
    if not isinstance(msg, dict):
        msg = update.get("edited_message")
    if not isinstance(msg, dict):
        return None
    chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
    from_user = msg.get("from") if isinstance(msg.get("from"), dict) else {}
    text = str(msg.get("text") or "").strip()
    if not text:
        return None
    return {
        "chat_id": str(chat.get("id") or "").strip(),
        "chat_type": str(chat.get("type") or "").strip(),
        "text": text,
        "username": str(from_user.get("username") or "").strip(),
    }


def _extract_callback_query(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cb = update.get("callback_query")
    if not isinstance(cb, dict):
        return None
    query_id = str(cb.get("id") or "").strip()
    data = str(cb.get("data") or "").strip()
    from_user = cb.get("from") if isinstance(cb.get("from"), dict) else {}
    msg = cb.get("message") if isinstance(cb.get("message"), dict) else {}
    chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
    chat_id = str(chat.get("id") or "").strip()
    chat_type = str(chat.get("type") or "").strip().lower()
    username = str(from_user.get("username") or "").strip()
    if not username:
        username = str(from_user.get("first_name") or from_user.get("id") or "").strip()
    return {
        "query_id": query_id,
        "data": data,
        "chat_id": chat_id,
        "chat_type": chat_type,
        "username": username,
        "message_id": int(msg.get("message_id") or 0),
        "inline_message_id": str(cb.get("inline_message_id") or "").strip(),
    }


def _split_chunks(text: str, max_chars: int = MAX_REPLY_CHARS) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    chunks: list[str] = []
    current = ""
    for line in raw.splitlines():
        candidate = (current + "\n" + line).strip() if current else line
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(line) <= max_chars:
            current = line
            continue
        for i in range(0, len(line), max_chars):
            segment = line[i : i + max_chars]
            if len(segment) == max_chars:
                chunks.append(segment)
            else:
                current = segment
    if current:
        chunks.append(current)
    return chunks


_DISABLE_CARD_INLINE_BUTTONS = True


def _outgoing_reply_markup(reply_markup: Optional[Dict[str, Any]], *, for_edit: bool = False) -> Optional[Dict[str, Any]]:
    if not isinstance(reply_markup, dict):
        return None
    if "inline_keyboard" in reply_markup and _DISABLE_CARD_INLINE_BUTTONS:
        return {"inline_keyboard": []} if for_edit else None
    return reply_markup if reply_markup else None


def _send_reply(
    *,
    bot_token: str,
    chat_id: str,
    text: str,
    timeout_seconds: int,
    reply_markup: Optional[Dict[str, Any]] = None,
    parse_mode: str = "",
) -> None:
    chunks = _split_chunks(text, MAX_REPLY_CHARS) or ["(empty)"]
    outgoing_reply_markup = _outgoing_reply_markup(reply_markup, for_edit=False)
    for idx, chunk in enumerate(chunks):
        params: Dict[str, Any] = {"chat_id": chat_id, "text": chunk}
        if str(parse_mode or "").strip():
            params["parse_mode"] = str(parse_mode or "").strip()
        if idx == 0 and isinstance(outgoing_reply_markup, dict):
            params["reply_markup"] = json.dumps(outgoing_reply_markup, ensure_ascii=True)
        _telegram_api(
            bot_token=bot_token,
            method="sendMessage",
            params=params,
            timeout_seconds=timeout_seconds,
            use_post=True,
        )


def _looks_like_telegram_bot_token(value: str) -> bool:
    token = str(value or "").strip()
    if ":" not in token:
        return False
    head, tail = token.split(":", 1)
    if not head.isdigit():
        return False
    return len(tail) >= 20


def _send_text_message(
    *,
    bot_token: str,
    chat_id: str,
    text: str,
    timeout_seconds: int,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> int:
    params: Dict[str, Any] = {"chat_id": chat_id, "text": str(text or "").strip() or "(empty)"}
    outgoing_reply_markup = _outgoing_reply_markup(reply_markup, for_edit=False)
    if isinstance(outgoing_reply_markup, dict):
        params["reply_markup"] = json.dumps(outgoing_reply_markup, ensure_ascii=True)
    payload = _telegram_api(
        bot_token=bot_token,
        method="sendMessage",
        params=params,
        timeout_seconds=timeout_seconds,
        use_post=True,
    )
    result = payload.get("result") if isinstance(payload, dict) else {}
    if not isinstance(result, dict):
        return 0
    return int(result.get("message_id") or 0)


def _delete_telegram_message(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    timeout_seconds: int,
) -> None:
    if not str(chat_id or "").strip() or int(message_id or 0) <= 0:
        return
    _telegram_api(
        bot_token=bot_token,
        method="deleteMessage",
        params={"chat_id": chat_id, "message_id": int(message_id)},
        timeout_seconds=timeout_seconds,
        use_post=True,
    )


def _send_loading_placeholder(
    *,
    bot_token: str,
    chat_id: str,
    text: str,
    timeout_seconds: int,
) -> int:
    if not _looks_like_telegram_bot_token(bot_token):
        return 0
    try:
        return _send_text_message(
            bot_token=bot_token,
            chat_id=chat_id,
            text=text,
            timeout_seconds=max(8, int(timeout_seconds)),
        )
    except Exception:
        return 0


def _try_delete_telegram_message(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    timeout_seconds: int,
    log_file: Optional[Path] = None,
) -> bool:
    if not _looks_like_telegram_bot_token(bot_token):
        return False
    try:
        _delete_telegram_message(
            bot_token=bot_token,
            chat_id=chat_id,
            message_id=message_id,
            timeout_seconds=max(8, int(timeout_seconds)),
        )
        return True
    except Exception as exc:
        if isinstance(log_file, Path):
            _append_log(log_file, f"[Worker] deleteMessage warning: {exc}")
        return False


def _send_card_message(
    *,
    bot_token: str,
    chat_id: str,
    card: Dict[str, Any],
    timeout_seconds: int,
) -> None:
    if isinstance(card, dict):
        card = _ensure_card_has_home_button(card)
    params: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": str(card.get("text") or "").strip() or "(empty)",
        "disable_web_page_preview": "false",
    }
    parse_mode = str(card.get("parse_mode") or "").strip()
    if parse_mode:
        params["parse_mode"] = parse_mode
    reply_markup = _outgoing_reply_markup(
        card.get("reply_markup") if isinstance(card.get("reply_markup"), dict) else None,
        for_edit=False,
    )
    if isinstance(reply_markup, dict):
        params["reply_markup"] = json.dumps(reply_markup, ensure_ascii=True)
    _telegram_api(
        bot_token=bot_token,
        method="sendMessage",
        params=params,
        timeout_seconds=timeout_seconds,
        use_post=True,
    )


def _reply_payload(
    text: str,
    reply_markup: Optional[Dict[str, Any]] = None,
    *,
    parse_mode: str = "",
    mode: str = "",
    kind: str = "",
    image: Any = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"text": str(text or "").strip() or "(empty)"}
    payload["reply_markup"] = _with_home_button(reply_markup if isinstance(reply_markup, dict) else None)
    if str(parse_mode or "").strip():
        payload["parse_mode"] = str(parse_mode or "").strip()
    if str(mode or "").strip():
        payload["mode"] = str(mode or "").strip()
    if str(kind or "").strip():
        payload["kind"] = str(kind or "").strip()
    if image:
        payload["image"] = image
    return payload


def _normalize_reply_payload(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict) and "text" in value:
        reply_markup = value.get("reply_markup")
        normalized_reply_markup = dict(reply_markup) if isinstance(reply_markup, dict) else None
        return _reply_payload(
            str(value.get("text") or ""),
            normalized_reply_markup,
            parse_mode=str(value.get("parse_mode") or ""),
            mode=str(value.get("mode") or ""),
            kind=str(value.get("kind") or ""),
            image=value.get("image"),
        )
    return _reply_payload(str(value or ""))


def _build_inline_keyboard(rows: list[list[Dict[str, str]]]) -> Dict[str, Any]:
    keyboard = [row for row in rows if row]
    return {"inline_keyboard": keyboard}


def _with_home_button(reply_markup: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    rows: list[list[Dict[str, str]]] = []
    inline_keyboard = reply_markup.get("inline_keyboard") if isinstance(reply_markup, dict) else None
    if isinstance(inline_keyboard, list):
        for row in inline_keyboard:
            if isinstance(row, list) and row:
                rows.append([dict(button) for button in row if isinstance(button, dict) and button])
    home_callback = build_home_callback_data("cybercar", "home")
    for row in rows:
        for button in row:
            if str(button.get("callback_data") or "").strip() == home_callback:
                return _build_inline_keyboard(rows)
    rows.append([{"text": "🏠 首页", "callback_data": home_callback}])
    return _build_inline_keyboard(rows)


def _with_process_status_button(reply_markup: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    rows: list[list[Dict[str, str]]] = []
    inline_keyboard = reply_markup.get("inline_keyboard") if isinstance(reply_markup, dict) else None
    if isinstance(inline_keyboard, list):
        for row in inline_keyboard:
            if isinstance(row, list) and row:
                rows.append([dict(button) for button in row if isinstance(button, dict) and button])
    home_callback = build_home_callback_data("cybercar", "home")
    process_callback = build_home_callback_data("cybercar", "process_status")
    has_home = False
    has_process = False
    for row in rows:
        for button in row:
            callback_data = str(button.get("callback_data") or "").strip()
            if callback_data == home_callback:
                has_home = True
            elif callback_data == process_callback:
                has_process = True
    append_row: list[Dict[str, str]] = []
    if not has_process:
        append_row.append({"text": "📍 进度", "callback_data": process_callback})
    if not has_home:
        append_row.append({"text": "🏠 首页", "callback_data": home_callback})
    if append_row:
        rows.append(append_row)
    return _build_inline_keyboard(rows)


def _extract_collect_publish_item_id_from_task_identifier(task_identifier: str) -> str:
    token = str(task_identifier or "").strip()
    if not token:
        return ""
    parts = token.split("|", 1)
    if len(parts) != 2:
        return ""
    if str(parts[0] or "").strip().lower() != "collect_publish_latest":
        return ""
    return str(parts[1] or "").strip()


def _resolve_failure_feedback_retry_item_id(sections: Sequence[Mapping[str, Any]]) -> str:
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        section_title = str(section.get("title") or "").strip()
        for item in items:
            if isinstance(item, Mapping):
                label = str(item.get("label") or "").strip()
                value = str(item.get("value") or item.get("text") or "")
                if section_title not in {"任务标识", "机器信息"} and label != "当前任务":
                    continue
                item_id = _extract_collect_publish_item_id_from_task_identifier(value)
            else:
                if section_title not in {"任务标识", "机器信息"}:
                    continue
                item_id = _extract_collect_publish_item_id_from_task_identifier(str(item or ""))
            if item_id:
                return item_id
    return ""


def _build_failure_feedback_actions(*, status: str, sections: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    status_token = str(status or "").strip().lower()
    if status_token not in {"failed", "blocked", "alert", "login_required"}:
        return []
    login_signal_parts: list[str] = []
    merged_text_parts: list[str] = []
    platform_status_values: list[str] = []
    retry_item_id = _resolve_failure_feedback_retry_item_id(sections)
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        section_title = str(section.get("title") or "").strip()
        merged_text_parts.append(section_title)
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        for item in items:
            if isinstance(item, Mapping):
                label = str(item.get("label") or "").strip()
                value = str(item.get("value") or item.get("text") or "").strip()
                merged_text_parts.append(label)
                merged_text_parts.append(value)
                if section_title == "平台状态":
                    platform_status_values.append(value)
                if section_title in {"失败原因", "平台状态", "处理建议"}:
                    login_signal_parts.append(label)
                    login_signal_parts.append(value)
            else:
                text = str(item or "").strip()
                merged_text_parts.append(text)
                if section_title == "平台状态":
                    platform_status_values.append(text)
                if section_title in {"失败原因", "平台状态", "处理建议"}:
                    login_signal_parts.append(text)
    merged_text = " ".join(part for part in merged_text_parts if part).lower()
    login_text = " ".join(part for part in login_signal_parts if part).lower()
    has_non_login_platform_progress = any(
        any(token in str(value or "") for token in ("✅", "⏳", "🕓", "⏭️", "已确认", "发布中", "已排队", "已跳过"))
        for value in platform_status_values
    )
    needs_login = status_token == "login_required" or any(
        token in login_text for token in ("登录", "未登录", "扫码", "qr", "login", "sign in")
    )
    if needs_login and has_non_login_platform_progress and status_token != "login_required":
        needs_login = False
    is_retryable_transport = any(
        token in merged_text
        for token in ("timeout", "network", "连接", "超时", "上传失败", "upload", "transport", "proxy", "代理")
    )
    is_skip_like = any(
        token in merged_text
        for token in ("跳过", "重复", "duplicate", "已自动跳过", "历史发布记录", "无需重复")
    )
    actions: list[dict[str, Any]] = []
    row = 0
    if needs_login:
        actions.append({"text": "🔐 登录", "callback_data": build_home_callback_data("cybercar", "login_menu"), "row": row})
        actions.append({"text": "📍 进度", "callback_data": build_home_callback_data("cybercar", "process_status"), "row": row})
        return actions
    if is_skip_like:
        actions.append({"text": "🏠 首页", "callback_data": build_home_callback_data("cybercar", "home"), "row": row})
        return actions
    if is_retryable_transport:
        actions.append({"text": "🔄 刷新", "callback_data": build_home_callback_data("cybercar", "process_status"), "row": row})
        actions.append({"text": "📍 进度", "callback_data": build_home_callback_data("cybercar", "process_status"), "row": row})
        return actions
    if retry_item_id:
        actions.append(
            {
                "text": "🔁 补发",
                "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|retry_failed_publish|{retry_item_id}",
                "row": row,
            }
        )
        actions.append({"text": "📍 进度", "callback_data": build_home_callback_data("cybercar", "process_status"), "row": row})
        return actions
    actions.append({"text": "📍 进度", "callback_data": build_home_callback_data("cybercar", "process_status"), "row": row})
    return actions


def _ensure_card_has_home_button(card: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(card) if isinstance(card, dict) else {}
    normalized["reply_markup"] = _with_home_button(
        normalized.get("reply_markup") if isinstance(normalized.get("reply_markup"), dict) else None
    )
    return normalized


def _build_text_notice(
    title: str,
    sections: list[dict[str, Any]],
    *,
    title_emoji: str = "📣",
) -> str:
    lines = [f"{str(title_emoji or '').strip()} {str(title or '').strip()}".strip()]
    for section in sections:
        header = str(section.get("title") or "").strip()
        emoji = str(section.get("emoji") or "").strip()
        items = section.get("items") or []
        if not header:
            continue
        lines.append("")
        lines.append(f"{emoji} {header}".strip())
        for item in items:
            if isinstance(item, dict):
                label = str(item.get("label") or "").strip()
                value = str(item.get("value") or "").strip()
                if label and value:
                    lines.append(f"• {label}：{value}")
                elif value:
                    lines.append(f"• {value}")
                continue
            text = str(item or "").strip()
            if text:
                lines.append(f"• {text}")
    return "\n".join(lines).strip()


def _build_prefilter_action_card(
    *,
    status: str,
    title: str,
    subtitle: str,
    sections: list[dict[str, Any]],
    source_url: str = "",
    include_source_button: bool = True,
    action_rows: Optional[list[list[Dict[str, str]]]] = None,
    menu_label: str = "",
    task_identifier: str = "",
) -> Dict[str, Any]:
    normalized_sections = _normalize_task_log_sections(sections)
    menu_section = _build_menu_path_section(menu_label)
    task_identifier_section = _build_task_identifier_section(task_identifier)
    if menu_section is not None:
        normalized_sections = [menu_section, *normalized_sections]
    if task_identifier_section is not None:
        normalized_sections = [task_identifier_section, *normalized_sections]
    card = build_action_feedback(
        status=status,
        title=_prefix_menu_title(title, menu_label),
        subtitle=subtitle,
        sections=normalized_sections,
        bot_name=BOT_NAME,
    )
    rows: list[list[Dict[str, str]]] = []
    link = str(source_url or "").strip()
    if include_source_button and link:
        rows.append([{"text": "🔗 原帖", "url": link}])
    for row in action_rows or []:
        if row:
            rows.append(row)
    card["reply_markup"] = _with_home_button(_build_inline_keyboard(rows))
    return card


def _update_callback_message_card(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    inline_message_id: str,
    timeout_seconds: int,
    card: Dict[str, Any],
) -> None:
    card = _ensure_card_has_home_button(card)
    last_error: Optional[Exception] = None
    for attempt in range(2):
        try:
            send_interaction_result(
                bot_token=bot_token,
                chat_id=chat_id,
                card=card,
                timeout_seconds=max(20, int(timeout_seconds)),
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt >= 1:
                raise
            time.sleep(0.25)
    if last_error is not None:
        raise last_error


def _home_state_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_HOME_STATE_FILE).resolve()


def _home_shortcut_state_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_HOME_SHORTCUT_STATE_FILE).resolve()


def _wechat_comment_reply_state_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_WECHAT_COMMENT_REPLY_STATE_FILE).resolve()


def _worker_state_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_STATE_FILE).resolve()


def _build_home_reply_keyboard() -> Dict[str, Any]:
    return {
        "keyboard": [
            [{"text": "🔐 登录"}, {"text": "📍 进度"}],
            [{"text": "🇨🇳 国内即采即发"}, {"text": "🌐 海外即采即发"}],
            [{"text": "💬 点赞评论"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
        "input_field_placeholder": "使用底部快捷键直达国内/海外入口",
    }


def _coerce_positive_message_id(value: Any) -> int:
    try:
        message_id = int(value or 0)
    except Exception:
        return 0
    return message_id if message_id > 0 else 0


def _known_state_message_ids(state: Dict[str, Any]) -> list[int]:
    seen: set[int] = set()
    values: list[int] = []
    for raw in state.get("recent_message_ids", []):
        message_id = _coerce_positive_message_id(raw)
        if message_id <= 0 or message_id in seen:
            continue
        seen.add(message_id)
        values.append(message_id)
    current_message_id = _coerce_positive_message_id(state.get("message_id"))
    if current_message_id > 0 and current_message_id not in seen:
        values.insert(0, current_message_id)
    return values[:8]


def _mark_home_surface_state_version(path: Path, surface_version: int) -> None:
    if int(surface_version or 0) <= 0:
        return
    state = _load_state(path)
    state["surface_version"] = int(surface_version)
    message_id = _coerce_positive_message_id(state.get("message_id"))
    state["recent_message_ids"] = [message_id] if message_id > 0 else []
    _save_state(path, state)


def _ensure_home_shortcut_keyboard(
    *,
    bot_token: str,
    chat_id: str,
    workspace: Path,
    timeout_seconds: int,
    log_file: Path,
    force_refresh: bool = False,
    surface_version: int = 0,
) -> None:
    clean_chat_id = str(chat_id or "").strip()
    if not clean_chat_id:
        return
    state_path = _home_shortcut_state_path(workspace)
    state = _load_state(state_path)
    same_chat = str(state.get("chat_id") or "").strip() == clean_chat_id
    same_version = int(state.get("keyboard_version") or 0) == int(DEFAULT_HOME_SHORTCUT_KEYBOARD_VERSION)
    known_message_ids = _known_state_message_ids(state) if same_chat else []
    updated_text = str(state.get("updated_at") or "").strip()
    recently_sent = False
    if (not force_refresh) and same_chat and same_version and updated_text:
        try:
            updated_dt = datetime.strptime(updated_text, "%Y-%m-%d %H:%M:%S")
            recently_sent = (time.time() - updated_dt.timestamp()) <= float(DEFAULT_HOME_SHORTCUT_DEBOUNCE_SECONDS)
        except Exception:
            recently_sent = False
    if recently_sent:
        return
    try:
        message_id = _send_text_message(
            bot_token=bot_token,
            chat_id=clean_chat_id,
            text="已启用底部快捷键：可直接使用国内即采即发、海外即采即发、进度、登录、点赞评论。",
            timeout_seconds=max(8, int(timeout_seconds)),
            reply_markup=_build_home_reply_keyboard(),
        )
        _save_state(
            state_path,
            {
                "chat_id": clean_chat_id,
                "message_id": int(message_id or 0),
                "recent_message_ids": [int(message_id or 0)] if int(message_id or 0) > 0 else [],
                "keyboard_version": int(DEFAULT_HOME_SHORTCUT_KEYBOARD_VERSION),
                "surface_version": int(surface_version or state.get("surface_version") or 0),
                "updated_at": _now_text(),
            },
        )
        for stale_message_id in known_message_ids:
            if stale_message_id == int(message_id or 0):
                continue
            _try_delete_telegram_message(
                bot_token=bot_token,
                chat_id=clean_chat_id,
                message_id=stale_message_id,
                timeout_seconds=max(8, int(timeout_seconds)),
                log_file=log_file,
            )
    except Exception as exc:
        _append_log(log_file, f"[Worker] home shortcut keyboard failed: {exc}")


def _refresh_home_surface_on_startup(
    *,
    bot_token: str,
    chat_id: str,
    workspace: Path,
    timeout_seconds: int,
    log_file: Path,
    default_profile: str,
    force_refresh: bool = False,
) -> None:
    clean_chat_id = str(chat_id or "").strip()
    if not clean_chat_id:
        return
    home_state_path = _home_state_path(workspace)
    shortcut_state_path = _home_shortcut_state_path(workspace)
    home_state = _load_state(home_state_path)
    shortcut_state = _load_state(shortcut_state_path)
    home_chat_id = str(home_state.get("chat_id") or "").strip()
    shortcut_chat_id = str(shortcut_state.get("chat_id") or "").strip()
    home_needs_refresh = force_refresh or (not home_chat_id) or (
        home_chat_id == clean_chat_id and int(home_state.get("surface_version") or 0) < int(DEFAULT_HOME_SURFACE_VERSION)
    )
    shortcut_needs_refresh = force_refresh or (not shortcut_chat_id) or (
        shortcut_chat_id == clean_chat_id
        and (
            int(shortcut_state.get("surface_version") or 0) < int(DEFAULT_HOME_SURFACE_VERSION)
            or int(shortcut_state.get("keyboard_version") or 0) != int(DEFAULT_HOME_SHORTCUT_KEYBOARD_VERSION)
        )
    )
    if not home_needs_refresh and not shortcut_needs_refresh:
        return
    if home_needs_refresh:
        send_or_update_home_message(
            bot_token=bot_token,
            chat_id=clean_chat_id,
            state_file=home_state_path,
            bot_kind="cybercar",
            card=_build_home_card(
                default_profile=default_profile,
                status_note="已刷新首页状态" if force_refresh else "已同步新版首页",
                workspace=workspace,
                chat_id=clean_chat_id,
            ),
            timeout_seconds=int(_normalize_blocking_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1)),
            force_new=True,
        )
        _mark_home_surface_state_version(home_state_path, DEFAULT_HOME_SURFACE_VERSION)
    if shortcut_needs_refresh:
        _ensure_home_shortcut_keyboard(
            bot_token=bot_token,
            chat_id=clean_chat_id,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            log_file=log_file,
            force_refresh=True,
            surface_version=DEFAULT_HOME_SURFACE_VERSION,
        )
    _append_log(
        log_file,
        f"[Worker] startup home surface refresh completed. force={force_refresh}, home={home_needs_refresh}, shortcut={shortcut_needs_refresh}",
    )


def _preview_text(text: Any, limit: int = 60) -> str:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    if len(clean) <= limit:
        return clean
    return clean[: max(8, int(limit) - 3)] + "..."


def _read_process_log_lines(path: Path) -> list[str]:
    raw = path.read_bytes()
    candidates: list[str] = []
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "cp936", "latin-1"):
        try:
            candidates.append(raw.decode(encoding))
        except Exception:
            continue
    if not candidates:
        candidates.append(raw.decode("utf-8", errors="replace"))
    return max(candidates, key=_score_log_decode_candidate).splitlines()


def _score_log_decode_candidate(text: str) -> int:
    score = 0
    for char in str(text or ""):
        code = ord(char)
        if char in "\r\n\t":
            continue
        if "0" <= char <= "9" or "A" <= char <= "Z" or "a" <= char <= "z":
            score += 2
            continue
        if 0x4E00 <= code <= 0x9FFF:
            score += 4
            continue
        if char in " []():/._-+|,<>@#%=&":
            score += 1
            continue
        if code < 32:
            score -= 4
            continue
        if 0x370 <= code <= 0x52F or 0x0300 <= code <= 0x036F:
            score -= 4
            continue
        score -= 1
    lowered = str(text or "").lower()
    for marker in ("鎵", "鍙", "馃", "鈥", "锛", "寮", "缁", "璇", "ͼ", "ѷ"):
        if marker in text:
            score -= 12
    if "\ufffd" in text:
        score -= 20
    if "[init]" in lowered or "[worker]" in lowered or "[notify]" in lowered:
        score += 6
    return score


def _repair_process_log_text(text: str) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    candidates = [clean]
    for source_encoding, target_encoding in (("gb18030", "utf-8"), ("latin-1", "utf-8"), ("cp1252", "utf-8")):
        try:
            repaired = clean.encode(source_encoding, errors="ignore").decode(target_encoding, errors="ignore").strip()
        except Exception:
            continue
        if repaired:
            candidates.append(repaired)
    return max(candidates, key=_score_log_decode_candidate)


def _split_process_log_prefix(line: str) -> tuple[str, str]:
    match = re.match(r"^(\[[^\]]+\]\s+\[[^\]]+\]\s*)(.*)$", str(line or "").strip())
    if match:
        return str(match.group(1) or ""), str(match.group(2) or "").strip()
    return "", str(line or "").strip()


def _looks_like_garbled_log_body(text: str) -> bool:
    body = str(text or "").strip()
    if not body:
        return False
    suspicious = sum(1 for char in body if 0x370 <= ord(char) <= 0x52F or 0x0300 <= ord(char) <= 0x036F)
    if suspicious >= 2:
        return True
    markers = ("鎵", "鍙", "馃", "鈥", "锛", "ͼ", "ѷ", "ɼ")
    if sum(body.count(marker) for marker in markers) >= 2:
        return True
    if body.count("?") >= 4:
        question_ratio = body.count("?") / max(1, len(body))
        readable_ascii = sum(1 for char in body if char.isascii() and (char.isalnum() or char in " /:._-[]()"))
        if question_ratio >= 0.2 or readable_ascii <= max(6, len(body) // 3):
            return True
    return False


def _normalize_process_log_line(line: str) -> str:
    prefix, body = _split_process_log_prefix(line)
    repaired_body = _repair_process_log_text(body)
    if _looks_like_garbled_log_body(repaired_body):
        return (prefix + "日志文本存在编码异常，请查看原始日志文件。").strip()
    return (prefix + repaired_body).strip()


def _compact_process_log_lines(lines: Sequence[str], *, limit: int) -> tuple[list[str], int]:
    window = [str(line or "").strip() for line in lines if str(line or "").strip()]
    if not window:
        return [], 0
    selected = window[-max(int(limit), DEFAULT_PROCESS_STATUS_LOG_SCAN_LINES) :]
    compacted: list[str] = []
    folded = 0
    previous_key = ""
    for raw in selected:
        normalized = _normalize_process_log_line(raw)
        _, body = _split_process_log_prefix(normalized)
        key = re.sub(r"\s+", " ", body).strip().lower()
        if not key:
            continue
        if key == previous_key and "workspace ready" in key:
            folded += 1
            continue
        compacted.append(normalized)
        previous_key = key
    return compacted[-max(1, int(limit)) :], folded


def _normalize_shortcut_text(text: str) -> str:
    clean = re.sub(r"\s+", " ", str(text or "").replace("\ufe0f", "").strip())
    if not clean:
        return ""
    shortcut_map = {
        "🇨🇳 国内即采即发": "国内即采即发",
        "国内即采即发": "国内即采即发",
        "🌐 海外即采即发": "海外即采即发",
        "海外即采即发": "海外即采即发",
        "⚡ 即采即发": "即采即发",
        "✨ 即采即发": "即采即发",
        "即采即发": "即采即发",
        "📍 进度": "进程查看",
        "📍 进程查看": "进程查看",
        "进程查看": "进程查看",
        "🔐 登录": "平台登录",
        "🔐 平台登录": "平台登录",
        "平台登录": "平台登录",
        "💬 点赞评论": "点赞评论",
        "点赞评论": "点赞评论",
    }
    if clean in shortcut_map:
        return shortcut_map[clean]
    return clean


def _load_recent_comment_reply_items(workspace: Path, limit: int = 5) -> list[dict[str, Any]]:
    path = _wechat_comment_reply_state_path(workspace)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    items = payload.get("items")
    if not isinstance(items, dict):
        return []

    rows: list[dict[str, Any]] = []
    for fingerprint, raw in items.items():
        if not isinstance(raw, dict):
            continue
        replied_at = str(raw.get("replied_at") or "").strip()
        rows.append(
            {
                "fingerprint": str(fingerprint or "").strip(),
                "replied_at": replied_at,
                "post_title": str(raw.get("post_title") or "").strip(),
                "post_published_text": str(raw.get("post_published_text") or "").strip(),
                "comment_author": str(raw.get("comment_author") or "").strip(),
                "comment_time": str(raw.get("comment_time") or "").strip(),
                "comment_preview": str(raw.get("comment_preview") or "").strip(),
                "reply_text": str(raw.get("reply_text") or "").strip(),
            }
        )

    def _sort_key(item: dict[str, Any]) -> tuple[int, str]:
        raw_time = str(item.get("replied_at") or "").strip()
        if not raw_time:
            return (0, "")
        try:
            parsed = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
            return (1, parsed.strftime("%Y%m%d%H%M%S"))
        except Exception:
            return (0, raw_time)

    rows.sort(key=_sort_key, reverse=True)
    return rows[: max(1, int(limit))]


def _build_recent_comment_reply_detail(workspace: Path, limit: int = 5) -> str:
    items = _load_recent_comment_reply_items(workspace, limit=limit)
    state_path = _wechat_comment_reply_state_path(workspace)
    if not items:
        return f"暂无评论回复记录。\n状态文件：{state_path}"

    lines = [
        f"最近评论回复记录：{len(items)} 条",
        f"状态文件：{state_path}",
        "",
    ]
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. 时间：{str(item.get('replied_at') or '-').strip() or '-'}")
        lines.append(f"短视频：{_preview_text(item.get('post_title'), limit=80) or '-'}")
        published_text = str(item.get("post_published_text") or "").strip()
        if published_text:
            lines.append(f"发布时间：{published_text}")
        comment_author = str(item.get("comment_author") or "").strip()
        comment_time = str(item.get("comment_time") or "").strip()
        if comment_author or comment_time:
            author_line = comment_author or '-'
            if comment_time:
                author_line = f"{author_line}｜{comment_time}"
            lines.append(f"评论用户：{author_line}")
        lines.append(f"原评论：{_preview_text(item.get('comment_preview'), limit=80) or '-'}")
        lines.append(f"自动回复：{_preview_text(item.get('reply_text'), limit=80) or '-'}")
        if idx < len(items):
            lines.append("")
    return "\n".join(lines).strip()


def _format_platform_text(platforms: Iterable[str]) -> str:
    tokens = [PUBLISH_PLATFORM_DISPLAY.get(str(item or "").strip().lower(), str(item or "").strip()) for item in platforms]
    clean = [token for token in tokens if token]
    return " / ".join(clean) if clean else "全部平台"


def _normalize_platform_tokens(platforms: Optional[Iterable[str]]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in list(platforms or []):
        token = str(raw or "").strip().lower()
        if not token:
            continue
        if token in ALL_PLATFORM_ALIAS_SET:
            for platform in PUBLISH_PLATFORM_ORDER:
                if platform not in seen:
                    seen.add(platform)
                    result.append(platform)
            continue
        mapped = PUBLISH_PLATFORM_ALIAS_MAP.get(token, token)
        if mapped not in PUBLISH_PLATFORM_DISPLAY and mapped != "collect":
            continue
        if mapped in seen:
            continue
        seen.add(mapped)
        result.append(mapped)
    return result


def _platform_display_with_logo(platform: str) -> str:
    token = str(platform or "").strip().lower()
    name = PUBLISH_PLATFORM_DISPLAY.get(token, token or "平台")
    logo = PUBLISH_PLATFORM_LOGO.get(token, "📣")
    return f"{logo}{name}"


def _platform_button_text(platform: str) -> str:
    return _platform_display_with_logo(platform)


def _platforms_to_logo_text(platforms: Iterable[str]) -> str:
    tokens = _normalize_platform_tokens(platforms)
    if not tokens:
        return "全部平台"
    return " / ".join(_platform_display_with_logo(token) for token in tokens)


def _platforms_to_logo_badge(platforms: Optional[Iterable[str]]) -> str:
    tokens = _normalize_platform_tokens(platforms)
    badges = "".join(PUBLISH_PLATFORM_LOGO.get(token, "") for token in tokens)
    return badges.strip()


def _decorate_feedback_title(title: str, platforms: Optional[Iterable[str]]) -> str:
    badge = _platforms_to_logo_badge(platforms)
    clean_title = str(title or "").strip()
    if not badge:
        return clean_title
    return f"{badge} {clean_title}".strip()


def _menu_label_for_action(action: str) -> str:
    mapping = {
        "login_qr": "平台登录",
        "collect_publish_latest": "即采即发",
        "collect_publish_latest_domestic": "国内即采即发",
        "collect_publish_latest_global": "海外即采即发",
        "comment_reply_run": "点赞评论",
        "process_status": "进程查看",
    }
    return mapping.get(str(action or "").strip().lower(), "")


def _menu_media_label(action: str, media_kind: str) -> str:
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    if str(action or "").strip().lower() == "collect_now":
        return "视频采集学习" if normalized_media_kind == "video" else "图片采集"
    return IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(normalized_media_kind, "媒体")


def _menu_platform_label(platform_value: str) -> str:
    token = str(platform_value or "").strip().lower()
    if not token or token == "all":
        return "全部平台"
    return PUBLISH_PLATFORM_DISPLAY.get(token, token)


def _menu_count_label(count: int) -> str:
    return f"{max(1, int(count))}条"


def _menu_breadcrumb_for_item(item: Dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""
    media_kind = _normalize_immediate_collect_media_kind(str(item.get("media_kind") or "video"))
    platforms = _resolve_item_target_platforms(item)
    parts = ["即采即发", IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")]
    if platforms:
        parts.append(_menu_platform_label(platforms[0] if len(platforms) == 1 else "all"))
    return " / ".join(part for part in parts if str(part or "").strip())


def _menu_breadcrumb_for_action(action: str, value: str = "") -> str:
    action_token = str(action or "").strip().lower()
    value_token = str(value or "").strip().lower()
    root = _menu_label_for_action(action_token)
    if not root:
        return ""

    parts = [root]
    if action_token == "collect_now":
        media_kind, count = _parse_collect_request_value(value_token)
        parts.append(_menu_media_label(action_token, media_kind))
        if count > 0:
            parts.append(_menu_count_label(count))
    elif action_token == "publish_run":
        media_kind, platform_value = _parse_publish_request_value(value_token)
        parts.append(_menu_media_label(action_token, media_kind))
        parts.append(_menu_platform_label(platform_value))
    elif action_token == "schedule_run":
        media_kind, minutes, platform_value = _parse_schedule_callback_value(value_token)
        parts.append(_menu_media_label(action_token, media_kind))
        platform_label = _menu_platform_label(platform_value)
        if minutes > 0:
            parts.append(f"{minutes}分钟·{platform_label}")
        else:
            parts.append(platform_label)
    elif action_token == "login_qr":
        if value_token:
            parts.append(_menu_platform_label(value_token))
    elif _is_collect_publish_action(action_token):
        media_kind, count = _parse_collect_publish_request_value(value_token)
        parts.append(_menu_media_label(action_token, media_kind))
        parts.append(_menu_count_label(count))
    elif action_token == "comment_reply_run":
        platform_token, limit = _parse_comment_reply_request_value(value_token)
        parts.append(_menu_platform_label(platform_token))
        parts.append(f"{max(1, int(limit))}条")

    return " / ".join(part for part in parts if str(part or "").strip())


def _menu_label_from_title(title: str) -> str:
    clean_title = str(title or "").strip()
    if not clean_title:
        return ""
    if clean_title.startswith("【"):
        match = re.match(r"^【([^】]+)】", clean_title)
        if match:
            return str(match.group(1) or "").strip()
    keyword_mapping = [
        ("进程查看", "进程查看"),
        ("点赞评论", "点赞评论"),
        ("平台登录", "平台登录"),
        ("二维码", "平台登录"),
        ("即采即发", "即采即发"),
    ]
    for keyword, label in keyword_mapping:
        if keyword in clean_title:
            return label
    return ""


def _prefix_menu_title(title: str, menu_label: str = "") -> str:
    clean_title = str(title or "").strip()
    resolved_label = str(menu_label or "").strip() or _menu_label_from_title(clean_title)
    if not clean_title or not resolved_label:
        return clean_title
    if clean_title.startswith(f"【{resolved_label}】"):
        return clean_title
    return f"【{resolved_label}】{clean_title}"


def _log_display_name(log_path: str) -> str:
    token = str(log_path or "").strip()
    if not token:
        return ""
    try:
        return Path(token).name or token
    except Exception:
        return token.split("\\")[-1].split("/")[-1].strip()


_ERROR_CODE_PREFIX_PATTERN = re.compile(r"\b((?:E|ERR)_[A-Z0-9_]{3,})\b")
_ERROR_CODE_INLINE_PATTERN = re.compile(
    r"(?:error\s*code|err(?:or)?\s*code|code|错误码)\s*[:=：]\s*([A-Za-z0-9._-]{3,})",
    flags=re.IGNORECASE,
)
_ERROR_CODE_INVALID_TOKENS = {"error", "failed", "login", "unknown", "none", "null", "code"}


def _normalize_error_code_token(raw: str) -> str:
    token = str(raw or "").strip().strip("`'\".,;:()[]{}<>")
    if not token:
        return ""
    compact = token.replace(" ", "")
    if not compact:
        return ""
    upper = compact.upper()
    if upper.lower() in _ERROR_CODE_INVALID_TOKENS:
        return ""
    if not (any(ch.isdigit() for ch in upper) or "_" in upper or "-" in upper):
        return ""
    return upper


def _extract_error_code(*values: str) -> str:
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        upper_text = text.upper()
        prefixed = _ERROR_CODE_PREFIX_PATTERN.search(upper_text)
        if prefixed:
            token = _normalize_error_code_token(prefixed.group(1))
            if token:
                return token
        inline = _ERROR_CODE_INLINE_PATTERN.search(text)
        if inline:
            token = _normalize_error_code_token(inline.group(1))
            if token:
                return token
    return ""


def _strip_error_code_text(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = re.sub(
        r"(?:error\s*code|err(?:or)?\s*code|code|错误码)\s*[:=：]\s*[A-Za-z0-9._-]{3,}",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\b(?:E|ERR)_[A-Z0-9_]{3,}\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" \t\r\n,，;；:：|/-")
    return cleaned


def _build_task_log_section(log_paths: Iterable[str]) -> Optional[Dict[str, Any]]:
    normalized = [_log_display_name(path) for path in log_paths if str(path or "").strip()]
    if not normalized:
        return None
    return {
        "title": "任务日志",
        "emoji": "🧾",
        "items": [
            {"label": "日志 1" if len(normalized) > 1 else "日志名", "value": normalized[0]},
            *(
                {"label": f"日志 {idx}", "value": path}
                for idx, path in enumerate(normalized[1:], start=2)
            ),
        ],
    }


def _build_menu_path_section(menu_label: str) -> Optional[Dict[str, Any]]:
    clean_label = str(menu_label or "").strip()
    if not clean_label:
        return None
    return {
        "title": "菜单链路",
        "emoji": "🧭",
        "items": [{"label": "当前链路", "value": clean_label.replace(" / ", " > ")}],
    }


def _task_identifier_timestamp(*, log_path: str = "", updated_at: str = "") -> str:
    log_token = str(log_path or "").strip()
    if log_token:
        match = re.search(r"(\d{8}_\d{6})", _log_display_name(log_token))
        if match:
            return str(match.group(1) or "").strip()
    updated_token = str(updated_at or "").strip()
    if updated_token:
        try:
            return datetime.strptime(updated_token, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d_%H%M%S")
        except Exception:
            pass
    return ""


def _task_identifier_slug(text: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "_", str(text or "").strip().lower()).strip("_")
    return token


def _build_task_identifier(
    *,
    action: str = "",
    value: str = "",
    menu_label: str = "",
    log_path: str = "",
    updated_at: str = "",
    item_id: str = "",
) -> str:
    if str(item_id or "").strip():
        base = _task_identifier_slug(str(action or "").strip() or "prefilter")
        return f"{base}|{str(item_id or '').strip()}"
    action_token = str(action or "").strip().lower()
    if action_token:
        parts = [action_token]
        value_token = str(value or "").strip().lower()
        if value_token:
            if action_token == "collect_now":
                media_kind, count = _parse_collect_request_value(value_token)
                parts.extend([media_kind, str(count or 0)])
            elif action_token == "publish_run":
                media_kind, platform_value = _parse_publish_request_value(value_token)
                parts.extend([media_kind, platform_value or "all"])
            elif action_token == "schedule_run":
                media_kind, minutes, platform_value = _parse_schedule_callback_value(value_token)
                parts.extend([media_kind, str(minutes or 0), platform_value or "all"])
            elif action_token == "collect_publish_latest":
                media_kind, count = _parse_collect_publish_request_value(value_token)
                parts.extend([media_kind, str(count or 0)])
            elif action_token == "login_qr":
                parts.append(value_token)
            elif action_token == "comment_reply_run":
                parts.append(_normalize_comment_reply_request_value(value_token))
            else:
                parts.append(_task_identifier_slug(value_token))
        timestamp = _task_identifier_timestamp(log_path=log_path, updated_at=updated_at)
        if timestamp:
            parts.append(timestamp)
        return "|".join(part for part in parts if str(part or "").strip())
    menu_token = _task_identifier_slug(str(menu_label or "").replace(" / ", "|"))
    timestamp = _task_identifier_timestamp(log_path=log_path, updated_at=updated_at)
    if not menu_token and timestamp:
        return f"task|{timestamp}"
    if timestamp:
        return f"{menu_token}|{timestamp}"
    return menu_token


def _build_task_identifier_section(task_identifier: str) -> Optional[Dict[str, Any]]:
    clean_identifier = str(task_identifier or "").strip()
    if not clean_identifier:
        return None
    return {
        "title": "任务标识",
        "emoji": "🏷️",
        "items": [{"label": "当前任务", "value": clean_identifier}],
    }


_OPERATOR_PRIORITY_LABELS = {
    "状态",
    "目标平台",
    "平台摘要",
    "采集媒体",
    "候选数量",
    "时间窗口",
    "结果",
    "平台",
}
_MACHINE_SECTION_TITLES = {"任务标识", "菜单链路", "任务日志"}
_MACHINE_DETAIL_LABELS = {"耗时", "状态文件", "工作区", "运行上下文"}


def _optimize_feedback_sections_for_operator(
    sections: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    operator_items: list[Any] = []
    machine_items: list[Any] = []
    remaining_sections: list[dict[str, Any]] = []

    for section in sections:
        if not isinstance(section, Mapping):
            continue
        title = str(section.get("title") or "").strip()
        emoji = str(section.get("emoji") or "").strip()
        items = list(section.get("items") or []) if isinstance(section.get("items"), Sequence) else []
        if title in _MACHINE_SECTION_TITLES:
            machine_items.extend(items)
            continue
        if title == "执行摘要":
            for item in items:
                if not isinstance(item, Mapping):
                    machine_items.append(item)
                    continue
                label = str(item.get("label") or "").strip()
                normalized_item = dict(item)
                if label == "执行结果":
                    label = "状态"
                    normalized_item["label"] = "状态"
                if label in _OPERATOR_PRIORITY_LABELS:
                    operator_items.append(normalized_item)
                else:
                    machine_items.append(normalized_item)
            continue
        if title == "执行结果":
            continue
        remaining_sections.append({"title": title, "emoji": emoji, "items": items})

    optimized: list[dict[str, Any]] = []
    if operator_items:
        optimized.append({"title": "人工关注", "emoji": "🎯", "items": operator_items})
    optimized.extend(remaining_sections)
    if machine_items:
        normalized_machine_items: list[Any] = []
        for item in machine_items:
            if isinstance(item, Mapping):
                label = str(item.get("label") or "").strip()
                if label in _MACHINE_DETAIL_LABELS:
                    normalized_machine_items.append(dict(item))
                else:
                    normalized_machine_items.append(dict(item))
            else:
                normalized_machine_items.append(item)
        optimized.append({"title": "机器信息", "emoji": "🤖", "items": normalized_machine_items})
    return optimized


def _platform_status_marker(platform: str, merged_text: str, *, effective_status: str) -> str:
    token = str(platform or "").strip().lower()
    lowered = str(merged_text or "").strip().lower()
    related_lines = [line for line in lowered.splitlines() if token and token in line]
    if not token:
        return "⚠️"
    if any(
        marker in line
        for line in related_lines
        for marker in ("publish success", "发布成功", "已成功发布", "成功放到草稿箱")
    ):
        return "✅"
    if any(marker in line for line in related_lines for marker in ("login", "未登录", "登录", "扫码", "qr")):
        return "🔐"
    if (
        any(
            marker in line
            for line in related_lines
            for marker in ("publish failed", "发布失败", "执行失败")
        )
        or f"[scheduler:{token}] publish failed:" in lowered
        or f"{token} publish failed" in lowered
    ):
        return "📣"
    if any(marker in line for line in related_lines for marker in ("skipped", "跳过", "duplicate target blocked")):
        return "⏭️"
    if str(effective_status or "").strip().lower() == "done":
        return "✅"
    if str(effective_status or "").strip().lower() in {"failed", "blocked"}:
        return "📣"
    return "⚠️"


def _build_platform_status_summary(platforms: Sequence[str], merged_text: str, *, effective_status: str) -> str:
    normalized = _normalize_platform_tokens(platforms)
    if not normalized:
        return ""
    parts: list[str] = []
    for platform in normalized:
        marker = _platform_status_marker(platform, merged_text, effective_status=effective_status)
        platform_name = PUBLISH_PLATFORM_DISPLAY.get(platform, platform or "平台")
        if marker == "✅":
            suffix = "成功"
        elif marker == "🔐":
            suffix = "登录"
        elif marker == "⏭️":
            suffix = "跳过"
        elif marker == "📣":
            suffix = "失败"
        else:
            suffix = "待确认"
        parts.append(f"{marker} {platform_name}{suffix}")
    return " / ".join(parts)


def _format_task_log_header(
    *,
    task_identifier: str = "",
    menu_label: str = "",
    log_path: str = "",
) -> str:
    lines: list[str] = []
    clean_identifier = str(task_identifier or "").strip()
    clean_menu_label = str(menu_label or "").strip()
    clean_log_name = _log_display_name(log_path)
    if clean_identifier:
        lines.append(f"任务标识: {clean_identifier}")
    if clean_menu_label:
        lines.append(f"菜单链路: {clean_menu_label.replace(' / ', ' > ')}")
    if clean_log_name:
        lines.append(f"任务日志: {clean_log_name}")
    if not lines:
        return ""
    return "\n".join(lines) + "\n" + ("-" * 48) + "\n"


def _write_task_log_header(
    stream: Any,
    *,
    task_identifier: str = "",
    menu_label: str = "",
    log_path: str = "",
) -> None:
    header = _format_task_log_header(
        task_identifier=task_identifier,
        menu_label=menu_label,
        log_path=log_path,
    )
    if not header:
        return
    try:
        stream.write(header)
        stream.flush()
    except Exception:
        pass


def _prepend_task_log_header(
    text: str,
    *,
    task_identifier: str = "",
    menu_label: str = "",
    log_path: str = "",
) -> str:
    header = _format_task_log_header(
        task_identifier=task_identifier,
        menu_label=menu_label,
        log_path=log_path,
    )
    raw = str(text or "")
    if not header:
        return raw
    if raw.startswith(header) or raw.startswith("任务标识:"):
        return raw
    return header + raw


def _build_home_task_identifier(task: Dict[str, Any]) -> str:
    if not isinstance(task, dict):
        return ""
    existing = str(task.get("task_identifier") or "").strip()
    if existing:
        return existing
    return _build_task_identifier(
        action=str(task.get("action") or ""),
        value=str(task.get("value") or ""),
        log_path=str(task.get("log_path") or ""),
        updated_at=str(task.get("updated_at") or task.get("created_at") or ""),
        item_id=str(task.get("item_id") or ""),
    )


def _build_audit_task_identifier(action: str, command: str, ts: str) -> str:
    action_token = str(action or "").strip().lower()
    command_token = str(command or "").strip()
    timestamp = _task_identifier_timestamp(updated_at=ts)
    if action_token in {"allow_shell", "deny_shell"}:
        suffix = _task_identifier_slug(command_token)[:48]
        return "|".join(part for part in [action_token, suffix, timestamp] if part)
    if command_token.startswith("/"):
        body = command_token[1:].strip()
        cmd_name, _, cmd_rest = body.partition(" ")
        if cmd_name:
            return "|".join(
                part
                for part in [
                    _task_identifier_slug(cmd_name),
                    _task_identifier_slug(cmd_rest) if cmd_rest else "",
                    timestamp,
                ]
                if part
            )
    return "|".join(part for part in [_task_identifier_slug(action_token or "audit"), timestamp] if part)


def _normalize_task_log_sections(sections: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_sections: list[dict[str, Any]] = []
    for section in sections or []:
        if not isinstance(section, dict):
            continue
        if str(section.get("title") or "").strip() == "任务日志":
            continue
        normalized_sections.append(dict(section))
    return normalized_sections


def _compact_log_mentions(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    def _replace(match: re.Match[str]) -> str:
        prefix = str(match.group(1) or "")
        value = _log_display_name(str(match.group(2) or ""))
        return f"{prefix}{value}" if value else match.group(0)

    return re.sub(r"((?:日志(?:文件)?|Log file)\s*[:：]\s*)([^\s\r\n]+)", _replace, raw, flags=re.IGNORECASE)


_NON_FINAL_BACKGROUND_FEEDBACK_STATUSES = {"running", "queued"}
DEFAULT_IMMEDIATE_COLLECT_TRANSIENT_RETRY_LIMIT = 2
DEFAULT_IMMEDIATE_COLLECT_TRANSIENT_RETRY_SECONDS = 3.0
DEFAULT_PENDING_PREFILTER_RETRY_COOLDOWN_SECONDS = 45.0
DEFAULT_PENDING_PREFILTER_RETRY_BATCH_SIZE = 2
DEFAULT_PENDING_PREFILTER_RETRY_MAX_ATTEMPTS = 6


def _should_send_background_feedback(status: str) -> bool:
    clean_status = str(status or "").strip().lower()
    if not clean_status:
        return True
    return clean_status not in _NON_FINAL_BACKGROUND_FEEDBACK_STATUSES


def _build_home_sections(profile: str) -> list[dict[str, Any]]:
    return [
        {
            "title": "核心入口",
            "emoji": "🚀",
            "items": [
                {"label": "默认 profile", "value": profile},
                "国内即采即发：X -> 视频号/抖音/小红书/快手/B站。",
                "海外即采即发：抖音/小红书 -> TikTok/X。",
                "平台登录：按平台返回登录二维码。",
                "点赞评论：处理近期有评论的视频并回传结果。",
            ],
        },
    ]


def _pipeline_lock_path(workspace: Path) -> Path:
    return (workspace / "runtime" / "cybercar_pipeline.lock").resolve()


def _safe_float_minutes(value: float) -> int:
    try:
        numeric = float(value)
    except Exception:
        numeric = 0.0
    return max(0, int(numeric // 60))


def _load_pipeline_lock_state(workspace: Path) -> Dict[str, Any]:
    path = _pipeline_lock_path(workspace)
    payload = _load_state(path)
    pid = 0
    try:
        pid = int(payload.get("pid") or 0)
    except Exception:
        pid = 0
    started_at = str(payload.get("started_at") or "").strip()
    age_seconds = 0.0
    if started_at:
        try:
            age_seconds = max(0.0, time.time() - datetime.fromisoformat(started_at.replace("Z", "+00:00")).timestamp())
        except Exception:
            age_seconds = 0.0
    return {
        "path": str(path),
        "exists": path.exists(),
        "pid": pid,
        "alive": bool(pid > 0 and _pid_is_running(pid)),
        "mode": str(payload.get("mode") or "").strip().lower(),
        "priority": str(payload.get("priority") or "").strip().lower(),
        "started_at": started_at,
        "age_seconds": age_seconds,
        "age_minutes": _safe_float_minutes(age_seconds),
    }


def _list_active_home_action_tasks(
    *,
    workspace: Path,
    guarded_only: bool = False,
) -> list[Dict[str, Any]]:
    queue = _load_action_queue(_action_queue_path(workspace))
    raw_tasks = queue.get("tasks", {})
    if not isinstance(raw_tasks, dict):
        return []
    tasks: list[Dict[str, Any]] = []
    for row in raw_tasks.values():
        if not isinstance(row, dict):
            continue
        normalized = _normalize_home_action_task_record(row)
        status = str(normalized.get("status") or "").strip().lower()
        if status not in HOME_ACTION_ACTIVE_STATUSES:
            continue
        action = str(normalized.get("action") or "").strip().lower()
        if guarded_only and action not in HOME_ACTION_PIPELINE_GUARDED_ACTIONS:
            continue
        tasks.append(normalized)
    tasks.sort(key=_home_task_sort_epoch, reverse=True)
    return tasks


def _summarize_waiting_prefilter_items(workspace: Path, *, limit: int = 3) -> Dict[str, Any]:
    queue = _load_prefilter_queue(_prefilter_queue_path(workspace))
    items = queue.get("items", {})
    if not isinstance(items, dict):
        return {"count": 0, "ids": []}
    matched: list[str] = []
    for item_id, row in items.items():
        if not isinstance(row, dict):
            continue
        if not _is_prefilter_waiting_for_runtime_lock(row):
            continue
        if _is_stale_waiting_prefilter_item(row):
            continue
        matched.append(str(item_id or "").strip())
    return {"count": len(matched), "ids": matched[: max(1, int(limit))]}


def _inspect_runtime_execution_state(workspace: Path) -> Dict[str, Any]:
    return {
        "lock": _load_pipeline_lock_state(workspace),
        "active_tasks": _list_active_home_action_tasks(workspace=workspace, guarded_only=True),
        "waiting_prefilter": _summarize_waiting_prefilter_items(workspace),
    }


def _summarize_process_queue_status(workspace: Path) -> Dict[str, Any]:
    queue = _load_prefilter_queue(_prefilter_queue_path(workspace))
    raw_items = queue.get("items", {})
    if not isinstance(raw_items, dict) or not raw_items:
        return {"idle": True, "value": "空闲"}

    live_items = [dict(row) for row in raw_items.values() if isinstance(row, dict) and _is_prefilter_live_for_process_status(row)]
    if not live_items:
        return {"idle": True, "value": "空闲"}

    publish_running = [
        row for row in live_items if str(row.get("status") or "").strip().lower() == "publish_running"
    ]
    if publish_running:
        return {"idle": False, "value": f"发布中 {len(publish_running)} 条"}
    return {"idle": False, "value": f"活跃候选 {len(live_items)} 条"}


def _build_runtime_status_section(workspace: Optional[Path]) -> dict[str, Any]:
    if workspace is None:
        return {
            "title": "执行状态",
            "emoji": "🧯",
            "items": ["当前未挂载工作区，暂时无法检查锁与后台任务。"],
        }

    runtime_state = _inspect_runtime_execution_state(workspace)
    lock_state = runtime_state.get("lock", {})
    active_tasks = list(runtime_state.get("active_tasks") or [])
    waiting_prefilter = runtime_state.get("waiting_prefilter", {})
    queue_state = _summarize_process_queue_status(workspace)
    items: list[Any] = []
    all_idle = False

    if bool(lock_state.get("alive")):
        mode = str(lock_state.get("mode") or "pipeline").strip() or "pipeline"
        pid = int(lock_state.get("pid") or 0)
        age_minutes = int(lock_state.get("age_minutes") or 0)
        items.append({"label": "⚠️ 全局流水线锁", "value": f"占用中｜{mode}｜PID {pid}｜约 {age_minutes} 分钟"})
    elif bool(lock_state.get("exists")):
        items.append({"label": "⚠️ 全局流水线锁", "value": "发现残留锁文件，但占锁进程已不在。"})
    else:
        items.append({"label": "✅ 全局流水线锁", "value": "空闲"})

    if active_tasks:
        preview = active_tasks[0]
        title = _home_action_title(str(preview.get("action") or ""))
        updated_at = str(preview.get("updated_at") or "").strip() or "-"
        items.append({"label": "⚠️ 后台任务", "value": f"{title} 正在执行｜最近更新 {updated_at}｜共 {len(active_tasks)} 条"})
    else:
        items.append({"label": "✅ 后台任务", "value": "无互斥任务在运行"})

    queue_label = "✅ 即采即发队列" if bool(queue_state.get("idle")) else "⚠️ 即采即发队列"
    items.append({"label": queue_label, "value": str(queue_state.get("value") or "空闲")})

    waiting_count = int(waiting_prefilter.get("count") or 0)
    if waiting_count > 0:
        ids = ", ".join(str(token or "").strip() for token in waiting_prefilter.get("ids", []) if str(token or "").strip())
        suffix = f"｜示例 {ids}" if ids else ""
        items.append({"label": "⚠️ 即采即发等待锁", "value": f"{waiting_count} 条{suffix}"})

    all_idle = (
        not bool(lock_state.get("alive"))
        and not bool(lock_state.get("exists"))
        and not active_tasks
        and int(waiting_prefilter.get("count") or 0) <= 0
        and bool(queue_state.get("idle"))
    )

    return {
        "title": "可继续操作" if all_idle else "执行状态",
        "emoji": "✅" if all_idle else "🧯",
        "items": items,
    }


def _home_task_status_summary(status: str) -> tuple[str, str]:
    token = str(status or "").strip().lower()
    mapping = {
        "queued": ("📣", "等待执行"),
        "running": ("⚡", "执行中"),
        "done": ("✅", "已完成"),
        "failed": ("❌", "执行失败"),
        "blocked": ("⚠️", "待确认"),
    }
    return mapping.get(token, ("🧾", "状态未知"))


def _home_task_sort_epoch(task: Dict[str, Any]) -> float:
    if not isinstance(task, dict):
        return 0.0
    for key in ("updated_epoch", "created_epoch"):
        try:
            value = float(task.get(key) or 0.0)
        except Exception:
            value = 0.0
        if value > 0:
            return value
    return 0.0


def _preview_home_task_detail(task: Dict[str, Any]) -> str:
    detail = str(task.get("detail") or "").strip()
    if not detail:
        return ""
    first_line = detail.splitlines()[0].strip()
    if not first_line:
        return ""
    return _preview_text(first_line, limit=72)


def _list_home_action_tasks_for_display(
    *,
    workspace: Path,
    chat_id: str = "",
    limit: int = DEFAULT_HOME_VISIBLE_TASK_LIMIT,
) -> list[Dict[str, Any]]:
    _prune_home_action_tasks(workspace)
    queue = _load_action_queue(_action_queue_path(workspace))
    raw_tasks = queue.get("tasks", {})
    if not isinstance(raw_tasks, dict):
        return []

    normalized_chat_id = str(chat_id or "").strip()
    tasks: list[Dict[str, Any]] = []
    for task in raw_tasks.values():
        if not isinstance(task, dict):
            continue
        normalized = _normalize_home_action_task_record(task)
        if normalized_chat_id and str(normalized.get("chat_id") or "").strip() != normalized_chat_id:
            continue
        tasks.append(normalized)

    tasks.sort(key=_home_task_sort_epoch, reverse=True)
    active_tasks = [
        task for task in tasks if str(task.get("status") or "").strip().lower() in HOME_ACTION_ACTIVE_STATUSES
    ]
    terminal_tasks: list[Dict[str, Any]] = []
    seen_terminal_signatures: set[str] = set()
    for task in tasks:
        status = str(task.get("status") or "").strip().lower()
        if status not in HOME_ACTION_TERMINAL_STATUSES:
            continue
        signature = "|".join(
            [
                str(task.get("action") or "").strip().lower(),
                str(task.get("value") or "").strip().lower(),
                str(task.get("profile") or "").strip().lower(),
            ]
        )
        if signature in seen_terminal_signatures:
            continue
        seen_terminal_signatures.add(signature)
        terminal_tasks.append(task)
    return (active_tasks + terminal_tasks)[: max(1, int(limit))]


def _build_home_task_queue_section(
    *,
    workspace: Optional[Path],
    chat_id: str = "",
    limit: int = DEFAULT_HOME_VISIBLE_TASK_LIMIT,
) -> dict[str, Any]:
    if workspace is None:
        return {
            "title": "最近有效任务",
            "emoji": "🧾",
            "items": ["当前没有任务摘要；后续有新任务后会自动展示最近有效任务。"],
        }

    tasks = _list_home_action_tasks_for_display(workspace=workspace, chat_id=chat_id, limit=limit)
    if not tasks:
        return {
            "title": "最近有效任务",
            "emoji": "🧾",
            "items": ["当前没有进行中的任务；最近结果会在这里显示。"],
        }

    items: list[Any] = []
    for task in tasks:
        status_emoji, status_text = _home_task_status_summary(str(task.get("status") or ""))
        title = _home_action_title(str(task.get("action") or ""))
        updated_at = str(task.get("updated_at") or task.get("created_at") or "").strip() or "-"
        detail_preview = _preview_home_task_detail(task)
        value = f"{status_text}｜{updated_at}"
        if detail_preview:
            value = f"{value}\n{detail_preview}"
        items.append({"label": f"{status_emoji} {title}", "value": value})

    items.append(f"仅展示最近 {len(tasks)} 条有效任务；更早终态记录继续保留在队列文件中。")
    return {
        "title": "最近有效任务",
        "emoji": "🧾",
        "items": items,
    }


def _prefilter_progress_status_label(status: str, row: Optional[Mapping[str, Any]] = None) -> str:
    mapping = {
        "link_pending": "待人工确认",
        "expired_pending": "已过期",
        "up_confirmed": "待采集",
        "down_confirmed": "待发布",
        "download_running": "下载中",
        "publish_requested": "待平台发布",
        "publish_running": "发布中",
        "publish_partial": "部分完成",
        "publish_done": "全部完成",
        "publish_failed": "发布失败",
        "send_failed": "卡片发送失败",
    }
    token = str(status or "").strip().lower()
    if token == "down_confirmed" and _is_prefilter_skipped_terminal(row or {}):
        return "已跳过"
    return mapping.get(token, token or "未知状态")


def _is_prefilter_recent_for_process_status(row: Dict[str, Any]) -> bool:
    ts = _prefilter_item_timestamp(row)
    if ts is None:
        return False
    cutoff = datetime.now() - timedelta(seconds=max(3600, int(DEFAULT_PREFILTER_QUEUE_STATUS_WINDOW_SECONDS)))
    return ts >= cutoff


def _is_prefilter_live_for_process_status(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    status = str(row.get("status") or "").strip().lower()
    action = str(row.get("action") or "").strip().lower()
    if status not in {
        "link_pending",
        "up_confirmed",
        "down_confirmed",
        "download_running",
        "publish_requested",
        "publish_running",
    }:
        return False
    ts = _prefilter_item_timestamp(row)
    if ts is None:
        return False
    cutoff = datetime.now() - timedelta(seconds=max(300, int(DEFAULT_PREFILTER_QUEUE_ACTIVE_WINDOW_SECONDS)))
    if ts < cutoff:
        return False
    if _is_prefilter_overflow_review_item(row):
        return False
    if status == "down_confirmed" and action == "skip":
        return False
    if status in {"link_pending", "up_confirmed", "down_confirmed"}:
        try:
            return int(row.get("message_id") or 0) > 0
        except Exception:
            return False
    return True


def _is_prefilter_skipped_terminal(row: Mapping[str, Any]) -> bool:
    if not isinstance(row, Mapping):
        return False
    status = str(row.get("status") or "").strip().lower()
    action = str(row.get("action") or "").strip().lower()
    return status == "down_confirmed" and action == "skip"


def _is_prefilter_expired_terminal(row: Mapping[str, Any]) -> bool:
    if not isinstance(row, Mapping):
        return False
    status = str(row.get("status") or "").strip().lower()
    action = str(row.get("action") or "").strip().lower()
    return status == "expired_pending" and action == "expired"


def _is_prefilter_filtered_terminal(row: Mapping[str, Any]) -> bool:
    return _is_prefilter_skipped_terminal(row) or _is_prefilter_expired_terminal(row)


def _is_stale_link_pending_prefilter_item(row: Mapping[str, Any]) -> bool:
    if not isinstance(row, Mapping):
        return False
    status = str(row.get("status") or "").strip().lower()
    if status != "link_pending":
        return False
    ts = _prefilter_item_timestamp(dict(row))
    if ts is None:
        return False
    cutoff = datetime.now() - timedelta(seconds=max(60, int(DEFAULT_IMMEDIATE_PREFILTER_PENDING_EXPIRY_SECONDS)))
    return ts < cutoff


def _is_prefilter_overflow_review_item(row: Mapping[str, Any]) -> bool:
    if not isinstance(row, Mapping):
        return False
    status = str(row.get("status") or "").strip().lower()
    if status not in {"link_pending", "up_confirmed", "down_confirmed"}:
        return False
    try:
        candidate_index = int(row.get("candidate_index") or 0)
    except Exception:
        candidate_index = 0
    try:
        candidate_limit = int(row.get("candidate_limit") or 0)
    except Exception:
        candidate_limit = 0
    return candidate_index > 0 and candidate_limit > 0 and candidate_index > candidate_limit


def _is_prefilter_waiting_for_runtime_lock(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    status = str(row.get("status") or "").strip().lower()
    action = str(row.get("action") or "").strip().lower()
    return action == "collect_waiting_lock" or status == "download_running"


def _is_stale_waiting_prefilter_item(row: Dict[str, Any]) -> bool:
    if not _is_prefilter_waiting_for_runtime_lock(row):
        return False
    ts = _prefilter_item_timestamp(row)
    if ts is None:
        return True
    cutoff = datetime.now() - timedelta(seconds=max(300, int(DEFAULT_PREFILTER_QUEUE_ACTIVE_WINDOW_SECONDS)))
    return ts < cutoff


def _build_process_worker_section(workspace: Path) -> dict[str, Any]:
    state = _load_state(_worker_state_path(workspace))
    if not state:
        return {
            "title": "Bot 心跳",
            "emoji": "🤖",
            "items": ["尚未生成 worker 心跳文件；如果 Bot 刚启动，可以稍后再刷新一次。"],
        }

    status_token = str(state.get("status") or "").strip().lower()
    status_text = {
        "polling": "轮询中",
        "starting": "启动中",
        "idle": "空闲",
        "stopped": "已停止",
        "failed": "异常",
    }.get(status_token, status_token or "未知")
    heartbeat_at = str(state.get("worker_heartbeat_at") or state.get("updated_at") or "").strip()
    last_error = _preview_text(state.get("last_error") or "", limit=96)
    items: list[Any] = [
        {"label": "Worker 状态", "value": status_text},
        {"label": "最近心跳", "value": heartbeat_at or "-"},
    ]
    startup_stage = str(state.get("startup_stage") or "").strip()
    if startup_stage:
        items.append({"label": "启动阶段", "value": startup_stage})
    try:
        update_id = int(state.get("last_processed_update_id") or state.get("offset") or 0)
    except Exception:
        update_id = 0
    if update_id > 0:
        items.append({"label": "最近 UpdateId", "value": str(update_id)})
    try:
        failures = int(state.get("consecutive_poll_failures") or 0)
    except Exception:
        failures = 0
    if failures > 0:
        items.append({"label": "连续轮询失败", "value": str(failures)})
    items.append({"label": "最近错误", "value": last_error or "无"})
    return {
        "title": "Bot 心跳",
        "emoji": "🤖",
        "items": items,
    }


def _build_process_task_section(
    workspace: Path,
    *,
    limit: int = DEFAULT_PROCESS_STATUS_TASK_LIMIT,
) -> dict[str, Any]:
    active_tasks = _list_active_home_action_tasks(workspace=workspace, guarded_only=False)
    if active_tasks:
        items: list[Any] = []
        for task in active_tasks[: max(1, int(limit))]:
            status_emoji, status_text = _home_task_status_summary(str(task.get("status") or ""))
            title = _home_action_title(str(task.get("action") or ""))
            updated_at = str(task.get("updated_at") or task.get("created_at") or "").strip() or "-"
            lines = [f"{status_text}｜{updated_at}"]
            detail = _preview_home_task_detail(task)
            if detail:
                lines.append(detail)
            log_name = _log_display_name(str(task.get("log_path") or "").strip())
            if log_name:
                lines.append(f"日志：{log_name}")
            items.append({"label": f"{status_emoji} {title}", "value": "\n".join(lines)})
        remaining = len(active_tasks) - max(1, int(limit))
        if remaining > 0:
            items.append(f"还有 {remaining} 条活跃任务未展开。")
        return {
            "title": "当前活跃任务",
            "emoji": "⚡",
            "items": items,
        }

    recent_tasks = _list_home_action_tasks_for_display(workspace=workspace, limit=max(1, int(limit)))
    if not recent_tasks:
        return {
            "title": "当前活跃任务",
            "emoji": "⚡",
            "items": ["当前没有活跃任务。"],
        }

    items = []
    for task in recent_tasks[: max(1, int(limit))]:
        status_emoji, status_text = _home_task_status_summary(str(task.get("status") or ""))
        title = _home_action_title(str(task.get("action") or ""))
        updated_at = str(task.get("updated_at") or task.get("created_at") or "").strip() or "-"
        detail = _preview_home_task_detail(task)
        value = f"{status_text}｜{updated_at}"
        if detail:
            value = f"{value}\n{detail}"
        items.append({"label": f"{status_emoji} {title}", "value": value})
    return {
        "title": "最近任务摘要",
        "emoji": "🧾",
        "items": items,
    }


def _build_process_prefilter_section(
    workspace: Path,
    *,
    limit: int = DEFAULT_PROCESS_STATUS_PREFILTER_LIMIT,
) -> dict[str, Any]:
    queue = _load_prefilter_queue(_prefilter_queue_path(workspace))
    raw_items = queue.get("items", {})
    if not isinstance(raw_items, dict) or not raw_items:
        return {
            "title": "即采即发队列",
            "emoji": "🪄",
            "items": ["当前没有即采即发候选队列。"],
        }

    items_list = [dict(row) for row in raw_items.values() if isinstance(row, dict)]
    live_items = [row for row in items_list if _is_prefilter_live_for_process_status(row)]
    if not live_items:
        hidden_count = len([row for row in items_list if _is_prefilter_recent_for_process_status(row)])
        return {
            "title": "即采即发队列",
            "emoji": "🪄",
            "items": [
                (
                    f"当前没有活跃的即采即发积压。"
                    + (f" 已隐藏 {hidden_count} 条近期历史记录。" if hidden_count > 0 else "")
                )
            ],
        }

    status_counts: dict[str, int] = {}
    for row in live_items:
        status = str(row.get("status") or "").strip().lower() or "unknown"
        status_counts[status] = int(status_counts.get(status, 0)) + 1

    ordered_statuses = [
        "link_pending",
        "up_confirmed",
        "down_confirmed",
        "download_running",
        "publish_requested",
        "publish_running",
    ]
    status_emoji_map = {
        "link_pending": "📝",
        "up_confirmed": "📥",
        "down_confirmed": "📦",
        "download_running": "📥",
        "publish_requested": "🕓",
        "publish_running": "✅",
    }
    items: list[Any] = [
        {"label": "📊 当前积压数", "value": str(len(live_items))},
        {"label": "🕒 活跃窗口", "value": "最近30分钟（仅统计当前活跃项）"},
    ]
    queue_updated_at = str(queue.get("updated_at") or "").strip()
    if queue_updated_at:
        items.append({"label": "🕒 队列更新时间", "value": queue_updated_at})
    for status in ordered_statuses:
        count = int(status_counts.get(status) or 0)
        if count <= 0:
            continue
        status_label = _prefilter_progress_status_label(status)
        status_emoji = status_emoji_map.get(status, "•")
        items.append({"label": f"{status_emoji} {status_label}", "value": str(count)})

    active_like_statuses = {
        "link_pending",
        "up_confirmed",
        "down_confirmed",
        "download_running",
        "publish_requested",
        "publish_running",
    }
    highlighted = [
        row
        for row in sorted(
            live_items,
            key=lambda row: _prefilter_item_timestamp(row) or datetime.min,
            reverse=True,
        )
        if str(row.get("status") or "").strip().lower() in active_like_statuses
    ]
    if not highlighted:
        highlighted = sorted(
            live_items,
            key=lambda row: _prefilter_item_timestamp(row) or datetime.min,
            reverse=True,
        )

    for row in highlighted[: max(1, int(limit))]:
        media_kind = _normalize_immediate_collect_media_kind(str(row.get("media_kind") or "video"))
        media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
        status_label = _prefilter_progress_status_label(str(row.get("status") or ""), row)
        updated_at = str(row.get("updated_at") or row.get("created_at") or "").strip() or "-"
        preview = _preview_text(
            row.get("processed_name")
            or row.get("video_name")
            or row.get("tweet_text")
            or row.get("source_url")
            or row.get("id")
            or "",
            limit=72,
        )
        items.append({"label": f"{media_label}｜{status_label}", "value": f"{updated_at}\n{preview}"})
    hidden_count = max(0, len(items_list) - len(live_items))
    if hidden_count > 0:
        items.append(f"已隐藏 {hidden_count} 条非活跃历史记录。")

    return {
        "title": "即采即发队列",
        "emoji": "🪄",
        "items": items,
    }


def _build_process_active_publish_section(
    workspace: Path,
    *,
    limit: int = DEFAULT_PROCESS_STATUS_PREFILTER_LIMIT,
) -> Optional[dict[str, Any]]:
    queue = _load_prefilter_queue(_prefilter_queue_path(workspace))
    raw_items = queue.get("items", {})
    if not isinstance(raw_items, dict) or not raw_items:
        return None

    live_items = [
        dict(row)
        for row in raw_items.values()
        if isinstance(row, dict) and _is_prefilter_live_for_process_status(row)
    ]
    highlighted = [
        row
        for row in sorted(
            live_items,
            key=lambda row: _prefilter_item_timestamp(row) or datetime.min,
            reverse=True,
        )
        if str(row.get("status") or "").strip().lower() == "publish_running"
    ]
    if not highlighted:
        return None

    items: list[Any] = []
    queue_updated_at = str(queue.get("updated_at") or "").strip()
    if queue_updated_at:
        items.append({"label": "✅ 队列更新时间", "value": queue_updated_at})

    for row in highlighted[: max(1, int(limit))]:
        media_kind = _normalize_immediate_collect_media_kind(str(row.get("media_kind") or "video"))
        media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
        updated_at = str(row.get("updated_at") or row.get("created_at") or "").strip() or "-"
        preview = _preview_text(
            row.get("processed_name")
            or row.get("video_name")
            or row.get("tweet_text")
            or row.get("source_url")
            or row.get("id")
            or "",
            limit=72,
        )
        items.append({"label": f"✅ {media_label}｜发布中", "value": f"{updated_at}\n{preview}"})

    if not items:
        return None
    return {
        "title": "当前发布中",
        "emoji": "✅",
        "items": items,
    }


def _resolve_process_log_path(workspace: Path, log_path: str) -> Optional[Path]:
    token = str(log_path or "").strip()
    if not token:
        return None
    candidate = Path(token)
    if candidate.is_absolute():
        return candidate if candidate.exists() else None
    resolved = (workspace / DEFAULT_LOG_SUBDIR / candidate.name).resolve()
    return resolved if resolved.exists() else None


def _pick_process_log_target(workspace: Path) -> tuple[Optional[Path], str]:
    active_tasks = _list_active_home_action_tasks(workspace=workspace, guarded_only=False)
    for task in active_tasks:
        resolved = _resolve_process_log_path(workspace, str(task.get("log_path") or ""))
        if resolved is not None:
            return resolved, _home_action_title(str(task.get("action") or ""))

    recent_tasks = _list_home_action_tasks_for_display(workspace=workspace, limit=3)
    for task in recent_tasks:
        resolved = _resolve_process_log_path(workspace, str(task.get("log_path") or ""))
        if resolved is not None:
            return resolved, _home_action_title(str(task.get("action") or ""))

    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    if not log_dir.exists():
        return None, ""
    patterns = [
        "home_action_*.log",
        "collect_publish_latest_job_*.log",
        "immediate_collect_item_job_*.log",
        "immediate_publish_item_job_*.log",
        "immediate_publish_*.log",
        "comment_reply_job_*.log",
    ]
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(log_dir.glob(pattern))
    if not candidates:
        return None, ""
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0], ""


def _build_process_log_section(
    workspace: Path,
    *,
    lines: int = DEFAULT_PROCESS_STATUS_LOG_TAIL_LINES,
) -> dict[str, Any]:
    target, owner = _pick_process_log_target(workspace)
    if target is None:
        return {
            "title": "最新日志尾部",
            "emoji": "🧾",
            "items": ["当前没有可读取的进度日志。"],
        }

    try:
        raw_lines = _read_process_log_lines(target)
    except Exception as exc:
        return {
            "title": "最新日志尾部",
            "emoji": "🧾",
            "items": [f"读取日志失败：{exc}"],
        }

    tail_lines, folded = _compact_process_log_lines(raw_lines, limit=lines)
    items: list[Any] = [{"label": "日志文件", "value": target.name}]
    if owner:
        items.append({"label": "关联任务", "value": owner})
    if folded > 0:
        items.append({"label": "日志折叠", "value": f"已折叠 {folded} 条重复初始化日志"})
    if tail_lines:
        for line in tail_lines[-max(1, int(lines)) :]:
            items.append(_preview_text(line, limit=120))
    else:
        items.append("(empty log)")
    return {
        "title": "最新日志尾部",
        "emoji": "🧾",
        "items": items,
    }


def _build_process_status_card(*, default_profile: str, workspace: Path, status_note: str = "") -> Dict[str, Any]:
    profile = _normalize_profile_name(default_profile)
    subtitle = f"当前配置：{profile}｜随时查看整个流程是否在推进"
    if status_note:
        subtitle = f"{subtitle}｜{status_note}"
    sections: list[dict[str, Any]] = []
    active_publish_section = _build_process_active_publish_section(workspace)
    if isinstance(active_publish_section, dict):
        sections.append(active_publish_section)
    sections.extend(
        [
            _build_runtime_status_section(workspace),
            _build_process_task_section(workspace),
            _build_process_worker_section(workspace),
            _build_process_prefilter_section(workspace),
            _build_process_log_section(workspace),
        ]
    )
    return _build_submenu_card(
        title="即采即发进程查看",
        subtitle=subtitle,
        sections=sections,
        actions=[
            {
                "text": "🔄 刷新",
                "callback_data": build_home_callback_data("cybercar", "process_status_refresh"),
                "row": 0,
            },
            {
                "text": "🧹 队列清理",
                "callback_data": build_home_callback_data("cybercar", "process_status_cleanup_queue"),
                "row": 0,
            },
            {
                "text": "🏠 首页",
                "callback_data": build_home_callback_data("cybercar", "home"),
                "row": 1,
            },
        ],
    )


def _build_submenu_card(
    *,
    title: str,
    subtitle: str,
    sections: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> Dict[str, Any]:
    return build_telegram_home(
        "cybercar",
        {
            "title": title,
            "subtitle": subtitle,
            "sections": sections,
        },
        actions=actions,
    )


def _profile_label_text(profile: str) -> str:
    return f"当前配置：{str(profile or '').strip() or DEFAULT_PROFILE}"


def _parse_collect_publish_candidate_limit(raw: str) -> int:
    token = str(raw or "").strip()
    if not token:
        return DEFAULT_IMMEDIATE_CANDIDATE_LIMIT
    try:
        value = int(token)
    except Exception:
        return DEFAULT_IMMEDIATE_CANDIDATE_LIMIT
    allowed = {int(item) for item in COLLECT_PUBLISH_CANDIDATE_OPTIONS}
    return value if value in allowed else DEFAULT_IMMEDIATE_CANDIDATE_LIMIT


def _normalize_immediate_collect_media_kind(raw: str) -> str:
    token = str(raw or "").strip().lower()
    return token if token in IMMEDIATE_COLLECT_MEDIA_KIND_ORDER else "video"


def _parse_collect_publish_request_value(raw: str) -> tuple[str, int]:
    token = str(raw or "").strip().lower()
    if ":" in token:
        media_kind_raw, count_raw = token.split(":", 1)
        return _normalize_immediate_collect_media_kind(media_kind_raw), _parse_collect_publish_candidate_limit(count_raw)
    return "video", _parse_collect_publish_candidate_limit(token)


def _collect_publish_route_key(action: str) -> str:
    token = str(action or "").strip().lower()
    if token in {COLLECT_PUBLISH_ACTION_DOMESTIC, COLLECT_PUBLISH_MENU_ACTION_DOMESTIC}:
        return "domestic"
    if token in {COLLECT_PUBLISH_ACTION_GLOBAL, COLLECT_PUBLISH_MENU_ACTION_GLOBAL}:
        return "global"
    return "default"


def _collect_publish_callback_action(route_key: str) -> str:
    token = str(route_key or "").strip().lower()
    if token == "domestic":
        return COLLECT_PUBLISH_ACTION_DOMESTIC
    if token == "global":
        return COLLECT_PUBLISH_ACTION_GLOBAL
    return COLLECT_PUBLISH_ACTION


def _collect_publish_profile_for_route(route_key: str, default_profile: str) -> str:
    token = str(route_key or "").strip().lower()
    if token == "domestic":
        return _normalize_profile_name(DEFAULT_DOMESTIC_COLLECT_PUBLISH_PROFILE)
    if token == "global":
        return _normalize_profile_name(DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE)
    return _normalize_profile_name(default_profile)


def _collect_publish_route_label(route_key: str) -> str:
    token = str(route_key or "").strip().lower()
    if token == "domestic":
        return "国内链路（X→国内）"
    if token == "global":
        return "海外链路（国内→海外）"
    return "标准链路"


def _collect_publish_source_hint_for_route(route_key: str) -> str:
    token = str(route_key or "").strip().lower()
    if token == "global":
        return "抖音/小红书关键词搜索"
    if token == "domestic":
        return "X 搜索结果时间倒序"
    return "按当前配置源站搜索"


def _is_collect_publish_action(action: str) -> bool:
    token = str(action or "").strip().lower()
    return token in {COLLECT_PUBLISH_ACTION, COLLECT_PUBLISH_ACTION_DOMESTIC, COLLECT_PUBLISH_ACTION_GLOBAL}


def _parse_collect_request_value(raw: str) -> tuple[str, int]:
    token = str(raw or "").strip().lower()
    if not token:
        return "video", 0
    parts = [part.strip() for part in token.split(":") if str(part).strip()]
    if not parts:
        return "video", 0
    media_kind = _normalize_immediate_collect_media_kind(parts[0])
    if len(parts) < 2:
        return media_kind, 0
    try:
        return media_kind, max(1, int(parts[1]))
    except Exception:
        return media_kind, 0


def _parse_media_kind_value(raw: str) -> str:
    return _normalize_immediate_collect_media_kind(raw or "video")


def _parse_publish_request_value(raw: str) -> tuple[str, str]:
    token = str(raw or "").strip().lower()
    if not token:
        return "video", "all"
    parts = [part.strip().lower() for part in token.split(":") if str(part).strip()]
    if len(parts) == 1:
        if parts[0] in IMMEDIATE_COLLECT_MEDIA_KIND_ORDER:
            return parts[0], "all"
        return "video", parts[0]
    media_kind = _normalize_immediate_collect_media_kind(parts[0])
    platform_value = parts[1] if len(parts) > 1 else "all"
    return media_kind, platform_value or "all"


def _collect_publish_target_platforms(media_kind: str) -> list[str]:
    normalized = _normalize_immediate_collect_media_kind(media_kind)
    return list(IMMEDIATE_COLLECT_MEDIA_KIND_PLATFORMS.get(normalized, PUBLISH_PLATFORM_ORDER))


def _load_profile_config_payload(repo_root: Path) -> dict[str, Any]:
    profile_config_path = (repo_root / DEFAULT_PROFILE_CONFIG_REL).resolve()
    if not profile_config_path.exists():
        return {}
    try:
        payload = json.loads(profile_config_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_profile_payload(
    *,
    repo_root: Path,
    profile: str,
    profile_config: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload = profile_config if isinstance(profile_config, dict) else _load_profile_config_payload(repo_root)
    profiles = payload.get("profiles", {}) if isinstance(payload.get("profiles"), dict) else {}
    active_profile = _normalize_profile_name(profile)
    default_profile = str(payload.get("default_profile", "") or DEFAULT_PROFILE).strip() or DEFAULT_PROFILE
    profile_payload = profiles.get(active_profile)
    if not isinstance(profile_payload, dict):
        profile_payload = profiles.get(default_profile)
    return dict(profile_payload) if isinstance(profile_payload, dict) else {}


def _normalize_collect_source_platform(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if not token:
        return ""
    return str(COLLECT_SOURCE_PLATFORM_ALIAS_MAP.get(token) or "")


def _normalize_collect_source_platforms(raw: Any) -> list[str]:
    raw_items: list[Any] = []
    if isinstance(raw, str):
        token = str(raw or "").strip()
        if token:
            raw_items = [part for part in re.split(r"[,\s/|]+", token) if str(part or "").strip()]
    elif isinstance(raw, Iterable):
        raw_items = [item for item in raw]
    resolved: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        normalized = _normalize_collect_source_platform(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        resolved.append(normalized)
    return resolved


def _resolve_collect_publish_source_platforms(
    *,
    repo_root: Path,
    profile: str,
    profile_payload: Optional[dict[str, Any]] = None,
    profile_config: Optional[dict[str, Any]] = None,
) -> list[str]:
    resolved_profile = _normalize_profile_name(profile).lower()
    payload = dict(profile_payload) if isinstance(profile_payload, dict) else _resolve_profile_payload(
        repo_root=repo_root,
        profile=profile,
        profile_config=profile_config,
    )
    configured = _normalize_collect_source_platforms(payload.get("source_platforms"))
    if configured:
        return configured
    if resolved_profile == DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE.lower():
        return ["douyin", "xiaohongshu"]
    if resolved_profile == DEFAULT_DOMESTIC_COLLECT_PUBLISH_PROFILE.lower():
        return ["x"]
    return ["x"]


def _resolve_collect_source_platform_from_url(source_url: str) -> str:
    host = str(urlparse(str(source_url or "").strip()).netloc or "").strip().lower()
    if not host:
        return ""
    if any(token in host for token in ("x.com", "twitter.com", "t.co")):
        return "x"
    if any(token in host for token in ("douyin.com", "iesdouyin.com", "douyinv.com")):
        return "douyin"
    if any(token in host for token in ("xiaohongshu.com", "xhslink.com", "rednote.com")):
        return "xiaohongshu"
    return ""


def _resolve_candidate_collect_source_platform(candidate: Mapping[str, Any]) -> str:
    source_platform = _normalize_collect_source_platform(candidate.get("source_platform"))
    if source_platform:
        return source_platform
    return _resolve_collect_source_platform_from_url(str(candidate.get("url") or ""))


def _collect_publish_source_platforms_text(source_platforms: Sequence[str]) -> str:
    normalized = _normalize_collect_source_platforms(source_platforms)
    if not normalized:
        normalized = ["x"]
    labels = [COLLECT_SOURCE_PLATFORM_DISPLAY.get(item, item.upper()) for item in normalized]
    return "/".join(labels)


def _collect_publish_source_scan_label(source_platforms: Sequence[str]) -> str:
    normalized = _normalize_collect_source_platforms(source_platforms)
    if normalized == ["x"]:
        return "X 搜索结果时间倒序"
    return f"{_collect_publish_source_platforms_text(normalized)}关键词搜索"


def _collect_publish_source_target_subtitle(source_platforms: Sequence[str], requested_limit: int) -> str:
    return f"候选来源：{_collect_publish_source_scan_label(source_platforms)}｜目标 {max(1, int(requested_limit))} 条"


def _collect_publish_source_recent_subtitle(source_platforms: Sequence[str], requested_limit: int) -> str:
    return f"候选来源：{_collect_publish_source_scan_label(source_platforms)}最近 {max(1, int(requested_limit))} 条"


def _collect_publish_scan_detail(source_platforms: Sequence[str], media_label: str) -> str:
    normalized = _normalize_collect_source_platforms(source_platforms)
    if normalized == ["x"]:
        return f"浏览器正在按时间倒序扫描 X {media_label}搜索结果，并会直接返回候选预审卡片。"
    return (
        f"浏览器正在按关键词扫描{_collect_publish_source_platforms_text(normalized)} {media_label}内容，"
        "并会直接返回候选预审卡片。"
    )


def _collect_publish_no_candidate_detail(source_platforms: Sequence[str], media_label: str) -> str:
    normalized = _normalize_collect_source_platforms(source_platforms)
    if normalized == ["x"]:
        return f"按 X 搜索结果时间倒序扫描后，未发现可用 X {media_label}帖子。"
    return f"按{_collect_publish_source_platforms_text(normalized)}关键词搜索扫描后，未发现可用{media_label}内容。"


def _parse_comment_reply_post_limit(raw: str) -> int:
    token = str(raw or "").strip()
    if not token:
        return int(COMMENT_REPLY_POST_OPTIONS[0])
    try:
        value = int(token)
    except Exception:
        return int(COMMENT_REPLY_POST_OPTIONS[0])
    allowed = {int(item) for item in COMMENT_REPLY_POST_OPTIONS}
    return value if value in allowed else int(COMMENT_REPLY_POST_OPTIONS[0])


def _parse_comment_reply_request_value(raw: str) -> tuple[str, int]:
    token = str(raw or "").strip().lower()
    default_limit = int(COMMENT_REPLY_POST_OPTIONS[0])
    if not token:
        return "all", default_limit
    parts = [part.strip().lower() for part in token.split(":") if str(part).strip()]
    if not parts:
        return "all", default_limit
    if len(parts) == 1 and parts[0].isdigit():
        return "all", _parse_comment_reply_post_limit(parts[0])
    if parts[0] in ALL_PLATFORM_ALIAS_SET:
        platform_token = "all"
    else:
        platform = _normalize_platform_tokens([parts[0]])
        platform_token = platform[0] if platform else "wechat"
    limit_token = parts[1] if len(parts) > 1 else ""
    return platform_token, _parse_comment_reply_post_limit(limit_token)


def _normalize_comment_reply_request_value(raw: str) -> str:
    platform, post_limit = _parse_comment_reply_request_value(raw)
    return f"{platform}:{max(1, int(post_limit))}"


def _resolve_comment_reply_platforms(platform: str) -> list[str]:
    token = str(platform or "").strip().lower()
    if token in {"", "all"}:
        return COMMENT_REPLY_PLATFORM_ORDER.copy()
    normalized = _normalize_platform_tokens([token])
    return normalized if normalized else COMMENT_REPLY_PLATFORM_ORDER.copy()


def _resolve_collect_publish_discovery_limit(candidate_limit: int) -> int:
    requested = max(1, int(candidate_limit))
    return min(
        COLLECT_PUBLISH_MAX_DISCOVERY_CANDIDATES,
        max(requested, requested * COLLECT_PUBLISH_DISCOVERY_MULTIPLIER),
    )


def _resolve_collect_publish_round_limits(candidate_limit: int, discovery_limit: Optional[int] = None) -> list[int]:
    requested = max(1, int(candidate_limit))
    baseline = _resolve_collect_publish_discovery_limit(requested)
    if discovery_limit is not None:
        try:
            baseline = max(requested, int(discovery_limit))
        except Exception:
            baseline = _resolve_collect_publish_discovery_limit(requested)
    values: list[int] = []
    for multiplier in COLLECT_PUBLISH_DISCOVERY_ROUND_MULTIPLIERS:
        value = min(COLLECT_PUBLISH_MAX_DISCOVERY_CANDIDATES, max(requested, requested * int(multiplier)))
        values.append(value)
    values.append(min(COLLECT_PUBLISH_MAX_DISCOVERY_CANDIDATES, baseline))
    values = sorted({max(requested, int(value)) for value in values})
    return values[:3]


def _candidate_source_url(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("url") or "").strip()


def _source_url_explicitly_points_to_video(source_url: str) -> bool:
    normalized_url = str(source_url or "").strip()
    if not normalized_url:
        return False
    try:
        parsed = urlparse(normalized_url)
        normalized_path = str(parsed.path or "").strip().lower()
    except Exception:
        normalized_path = normalized_url.lower()
    if not normalized_path:
        return False
    return any(token in normalized_path for token in ("/video/", "/videos/", "/reel/", "/reels/", "/shorts/"))


def _candidate_matches_collect_publish_media_kind(candidate: Mapping[str, Any], media_kind: str) -> bool:
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    if normalized_media_kind != "image":
        return True

    source_url = _candidate_source_url(candidate)
    if _source_url_explicitly_points_to_video(source_url):
        return False

    raw_candidate_media_kind = str(candidate.get("media_kind") or "").strip().lower()
    if raw_candidate_media_kind in {"image", "photo"}:
        return True
    if raw_candidate_media_kind in {"video", "animated_gif", "gif"}:
        return False

    if not _extract_x_status_id_from_url(source_url):
        return True
    try:
        return _probe_shared_link_media_kind(source_url) == "image"
    except Exception:
        return False


def _normalize_candidate_story_text(raw_text: str) -> str:
    text = str(raw_text or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"https?://\S+", " ", text)
    text = text.replace("’", "'").replace("“", '"').replace("”", '"')
    text = re.sub(r"[@#]", " ", text)
    text = re.sub(r"[^0-9a-z\u00c0-\u024f\u4e00-\u9fff]+", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _candidate_story_tokens(candidate: Mapping[str, Any]) -> list[str]:
    normalized = _normalize_candidate_story_text(str(candidate.get("tweet_text") or ""))
    if not normalized:
        return []
    tokens: list[str] = []
    seen: set[str] = set()
    for token in re.findall(r"[0-9a-z\u00c0-\u024f\u4e00-\u9fff]+", normalized, flags=re.IGNORECASE):
        clean = str(token or "").strip().lower()
        if not clean or clean in seen:
            continue
        if clean in IMMEDIATE_STORY_STOPWORDS:
            continue
        if clean.isdigit() or len(clean) >= 3 or re.search(r"[\u4e00-\u9fff]", clean):
            seen.add(clean)
            tokens.append(clean)
    return tokens[:16]


def _same_story_tokens(left_tokens: Sequence[str], right_tokens: Sequence[str]) -> bool:
    left_set = {str(token or "").strip().lower() for token in left_tokens if str(token or "").strip()}
    right_set = {str(token or "").strip().lower() for token in right_tokens if str(token or "").strip()}
    if not left_set or not right_set:
        return False
    common = left_set & right_set
    if not common:
        return False
    smaller = min(len(left_set), len(right_set))
    union = left_set | right_set
    overlap = len(common) / max(1, smaller)
    jaccard = len(common) / max(1, len(union))
    if len(common) >= 5 and overlap >= 0.6:
        return True
    if len(common) >= 4 and jaccard >= 0.5:
        return True
    if len(common) >= 3 and overlap >= 0.8:
        return True
    return False


def _collapse_collect_publish_same_story_candidates(
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    collapsed: list[dict[str, Any]] = []
    kept_story_texts: list[str] = []
    kept_story_tokens: list[list[str]] = []
    collapsed_count = 0
    for candidate in candidates:
        row = dict(candidate)
        story_text = _normalize_candidate_story_text(str(row.get("tweet_text") or ""))
        story_tokens = _candidate_story_tokens(row)
        is_same_story = False
        for existing_text, existing_tokens in zip(kept_story_texts, kept_story_tokens):
            if story_text and existing_text and story_text == existing_text:
                is_same_story = True
                break
            if _same_story_tokens(story_tokens, existing_tokens):
                is_same_story = True
                break
        if is_same_story:
            collapsed_count += 1
            continue
        kept_story_texts.append(story_text)
        kept_story_tokens.append(story_tokens)
        collapsed.append(row)
    return collapsed, collapsed_count


def _build_media_pick_card(*, title: str, subtitle: str, media_action: str, default_profile: str) -> Dict[str, Any]:
    profile = _normalize_profile_name(default_profile)
    return _build_submenu_card(
        title=title,
        subtitle=f"当前配置：{profile}｜{subtitle}",
        sections=[
            {
                "title": "媒体类型",
                "emoji": "🗂️",
                "items": [
                    "视频：走现有视频采集/发布链路。",
                    "图片：走图片采集与抖音/小红书图文链路。",
                ],
            }
        ],
        actions=[
            {"text": "🎬 视频", "callback_data": build_home_callback_data("cybercar", media_action, "video"), "row": 0},
            {"text": "🖼 图片", "callback_data": build_home_callback_data("cybercar", media_action, "image"), "row": 0},
            {"text": "🏠 返回首页", "callback_data": build_home_callback_data("cybercar", "home"), "row": 1},
        ],
    )


def _build_login_menu_card(*, default_profile: str) -> Dict[str, Any]:
    profile = _normalize_profile_name(default_profile)
    actions = []
    for idx, platform in enumerate(PUBLISH_PLATFORM_ORDER):
        row = idx // 2
        actions.append(
            {
                "text": _platform_button_text(platform),
                "callback_data": build_home_callback_data("cybercar", "login_qr", platform),
                "row": row,
            }
        )
    actions.append({"text": "🏠 首页", "callback_data": build_home_callback_data("cybercar", "home"), "row": 3})
    return _build_submenu_card(
        title="平台登录",
        subtitle=f"当前配置：{profile}｜选择平台返回登录二维码",
        sections=[
            {
                "title": "登录说明",
                "emoji": "🔐",
                "items": [
                    "点击平台后会直接返回对应二维码消息。",
                    "如果当前会话已登录，会返回无需扫码提示。",
                ],
            }
        ],
        actions=actions,
    )


def _build_collect_publish_latest_menu_card(
    *,
    default_profile: str,
    callback_action: str = COLLECT_PUBLISH_ACTION,
    route_label: str = "",
) -> Dict[str, Any]:
    profile = _normalize_profile_name(default_profile)
    actions = []
    normalized_callback_action = (
        callback_action
        if callback_action in {COLLECT_PUBLISH_ACTION, COLLECT_PUBLISH_ACTION_DOMESTIC, COLLECT_PUBLISH_ACTION_GLOBAL}
        else COLLECT_PUBLISH_ACTION
    )
    normalized_route_label = str(route_label or "").strip()
    subtitle_prefix = f"{normalized_route_label}｜" if normalized_route_label else ""
    route_key = _collect_publish_route_key(normalized_callback_action)
    source_hint = _collect_publish_source_hint_for_route(route_key)
    for idx, count in enumerate(COLLECT_PUBLISH_CANDIDATE_OPTIONS):
        row = idx
        actions.append(
            {
                "text": f"🎬 视频 {count} 条",
                "callback_data": build_home_callback_data("cybercar", normalized_callback_action, f"video:{count}"),
                "row": row,
            }
        )
        actions.append(
            {
                "text": f"🖼 图片 {count} 条",
                "callback_data": build_home_callback_data("cybercar", normalized_callback_action, f"image:{count}"),
                "row": row,
            }
        )
    actions.append(
        {
            "text": "📍 进度",
            "callback_data": build_home_callback_data("cybercar", "process_status"),
            "row": len(COLLECT_PUBLISH_CANDIDATE_OPTIONS),
        }
    )
    actions.append(
        {
            "text": "🏠 首页",
            "callback_data": build_home_callback_data("cybercar", "home"),
            "row": len(COLLECT_PUBLISH_CANDIDATE_OPTIONS),
        }
    )
    return _build_submenu_card(
        title="\u5373\u91c7\u5373\u53d1",
        subtitle=f"\u5f53\u524d\u914d\u7f6e\uff1a{profile}\uff5c{subtitle_prefix}\u89c6\u9891/\u56fe\u7247\u53cc\u6d41\u7a0b",
        sections=[
            {
                "title": "\u6267\u884c\u8bf4\u660e",
                "emoji": "\u26a1",
                "items": [
                    f"\u89c6\u9891\u5373\u91c7\u5373\u53d1\uff1a\u4ece{source_hint}\u626b\u63cf\u5019\u9009\uff0c\u540e\u7eed\u8fdb\u5165\u89c6\u9891\u53d1\u5e03\u6d41\u7a0b\u3002",
                    f"\u56fe\u7247\u5373\u91c7\u5373\u53d1\uff1a\u4ece{source_hint}\u626b\u63cf\u5019\u9009\uff0c\u786e\u8ba4\u540e\u6309\u5b9e\u9645\u6d41\u7a0b\u8fdb\u5165\u6296\u97f3 / \u5c0f\u7ea2\u4e66 / \u5feb\u624b\u53d1\u5e03\u3002",
                    "\u4e24\u6761\u6d41\u7a0b\u7684\u5019\u9009\u961f\u5217\u3001\u5904\u7406\u548c\u53d1\u5e03\u8bb0\u5f55\u76f8\u4e92\u72ec\u7acb\u3002",
                ],
            }
        ],
        actions=actions,
    )


def _build_comment_reply_menu_card(*, default_profile: str) -> Dict[str, Any]:
    profile = _normalize_profile_name(default_profile)
    actions = []
    for idx, count in enumerate(COMMENT_REPLY_POST_OPTIONS):
        actions.append(
            {
                "text": f"💬 {count}个",
                "callback_data": build_home_callback_data("cybercar", "comment_reply_run", f"all:{count}"),
                "row": idx // 2,
            }
        )
    actions.append({"text": "🏠 首页", "callback_data": build_home_callback_data("cybercar", "home"), "row": 2})
    return _build_submenu_card(
        title="点赞评论",
        subtitle=f"当前配置：{profile}｜同一数量会依次执行视频号、抖音、快手",
        sections=[
            {
                "title": "执行说明",
                "emoji": "⚙️",
                "items": [
                    "点击一个数量后，会按同样的数量依次执行视频号、抖音、快手三个平台。",
                    "三个平台都走自动选作品或视频的评论任务，不要求手工提前聚焦输入框。",
                    "执行完成后会把短视频标题、原评论、自动回复结果发回 Telegram。",
                ],
            }
        ],
        actions=actions,
    )


def _build_home_actions() -> list[dict[str, Any]]:
    return [
        {"text": "🔐 登录", "callback_data": build_home_callback_data("cybercar", "login_menu"), "row": 0},
        {"text": "📍 进度", "callback_data": build_home_callback_data("cybercar", "process_status"), "row": 0},
        {
            "text": "🇨🇳 国内即采即发",
            "callback_data": build_home_callback_data("cybercar", COLLECT_PUBLISH_MENU_ACTION_DOMESTIC),
            "row": 1,
        },
        {
            "text": "🌐 海外即采即发",
            "callback_data": build_home_callback_data("cybercar", COLLECT_PUBLISH_MENU_ACTION_GLOBAL),
            "row": 1,
        },
        {"text": "💬 点赞评论", "callback_data": build_home_callback_data("cybercar", "comment_reply_menu"), "row": 2},
    ]


def _describe_runtime_conflict(workspace: Path, *, action: str, value: str) -> str:
    runtime_state = _inspect_runtime_execution_state(workspace)
    lock_state = runtime_state.get("lock", {})
    active_tasks = list(runtime_state.get("active_tasks") or [])
    waiting_prefilter = runtime_state.get("waiting_prefilter", {})
    lines = ["当前已有互斥任务在执行，本次操作未启动。"]
    if bool(lock_state.get("alive")):
        mode = str(lock_state.get("mode") or "pipeline").strip() or "pipeline"
        pid = int(lock_state.get("pid") or 0)
        age_minutes = int(lock_state.get("age_minutes") or 0)
        lines.append(f"全局流水线锁：{mode}｜PID {pid}｜约 {age_minutes} 分钟。")
    elif bool(lock_state.get("exists")):
        lines.append("检测到残留流水线锁文件，但占锁进程已不在。")
    if active_tasks:
        preview = active_tasks[0]
        task_title = _home_action_title(str(preview.get("action") or ""))
        updated_at = str(preview.get("updated_at") or "").strip() or "-"
        lines.append(f"最近活跃任务：{task_title}｜状态 {preview.get('status') or '-'}｜最近更新 {updated_at}。")
    waiting_count = int(waiting_prefilter.get("count") or 0)
    if waiting_count > 0:
        lines.append(f"即采即发等待锁：{waiting_count} 条。")
    lines.append("请稍后重试。")
    return "\n".join(lines)


def _has_runtime_conflict_for_action(workspace: Path, *, action: str, value: str) -> bool:
    del value
    action_token = str(action or "").strip().lower()
    if action_token not in HOME_ACTION_PIPELINE_GUARDED_ACTIONS:
        return False
    runtime_state = _inspect_runtime_execution_state(workspace)
    lock_state = runtime_state.get("lock", {})
    active_tasks = list(runtime_state.get("active_tasks") or [])
    return bool(lock_state.get("alive") or lock_state.get("exists") or active_tasks)


def _build_home_card(
    *,
    default_profile: str,
    status_note: str = "",
    workspace: Optional[Path] = None,
    chat_id: str = "",
) -> Dict[str, Any]:
    profile = _normalize_profile_name(default_profile)
    subtitle = f"当前配置：{profile}｜固定首页消息"
    if status_note:
        subtitle = f"{subtitle}｜{status_note}"
    sections = _build_home_sections(profile)
    sections.append(_build_runtime_status_section(workspace))
    sections.append(_build_home_task_queue_section(workspace=workspace, chat_id=chat_id))
    return build_telegram_home(
        "cybercar",
        {
            "subtitle": subtitle,
            "sections": sections,
        },
        actions=_build_home_actions(),
    )


def _home_response(
    default_profile: str,
    status_note: str = "",
    *,
    force_new: bool = False,
    workspace: Optional[Path] = None,
    chat_id: str = "",
) -> Dict[str, Any]:
    return {
        "home_card": _build_home_card(
            default_profile=default_profile,
            status_note=status_note,
            workspace=workspace,
            chat_id=chat_id,
        ),
        "force_new_home": bool(force_new),
    }


def _home_feedback_response(
    *,
    status: str,
    title: str,
    subtitle: str,
    detail: str,
    menu_label: str = "",
    task_identifier: str = "",
) -> Dict[str, Any]:
    sections = []
    task_identifier_section = _build_task_identifier_section(task_identifier)
    if task_identifier_section is not None:
        sections.append(task_identifier_section)
    menu_section = _build_menu_path_section(menu_label)
    if menu_section is not None:
        sections.append(menu_section)
    detail_text = str(detail or "").strip()
    if detail_text:
        sections.append({"title": "处理状态", "emoji": "📌", "items": [detail_text[:1200]]})
    card = build_action_feedback(
        status=status,
        title=title,
        subtitle=subtitle,
        sections=sections,
        bot_name=BOT_NAME,
    )
    card["reply_markup"] = _with_process_status_button()
    return card


def _extract_published_video_names(output: str, limit: int = 5) -> list[str]:
    text = str(output or "")
    names: list[str] = []
    seen: set[str] = set()
    patterns = [
        r"Moved published video bundle:\s*(.+?)\s*->",
        r"Immediate publish\s+\d+/\d+:\s*(.+)$",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE | re.MULTILINE):
            name = str(match.group(1) or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            names.append(name)
            if len(names) >= max(1, int(limit)):
                return names
    return names


def _collect_result_log_paths(raw_result: Dict[str, Any], merged_output: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        token = str(raw or "").strip().strip("\"'")
        if not token:
            return
        key = token.lower()
        if key in seen:
            return
        seen.add(key)
        values.append(token)

    base_log_path = str(raw_result.get("log_path") or "").strip()
    if base_log_path:
        _add(base_log_path)
    extracted = _extract_log_path(merged_output)
    if extracted:
        _add(extracted)

    base_dir = ""
    for candidate in (base_log_path, extracted):
        token = str(candidate or "").strip()
        if token and (":" in token or token.startswith("/") or token.startswith("\\")):
            try:
                base_dir = str(Path(token).expanduser().resolve().parent)
            except Exception:
                base_dir = str(Path(token).parent)
            if base_dir:
                break

    patterns = [
        r"([A-Za-z]:[\\/][^\r\n]*?\.log)\b",
        r"((?:home_action|cybercar_publish_runner|comment_reply_job|collect_publish_latest_job|immediate_publish_item_job|immediate_collect_item_job|immediate_publish_[^\\/\r\n]*|direct_publish_[^\\/\r\n]*?)_\d{8}_\d{6}\.log)\b",
    ]
    text = str(merged_output or "")
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            token = str(match.group(1) or "").strip()
            if not token:
                continue
            if base_dir and not (":" in token or token.startswith("/") or token.startswith("\\")):
                token = str(Path(base_dir) / token)
            _add(token)
    return values


def _has_publish_failure_output(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    return any(
        marker in lowered
        for marker in (
            "[scheduler:douyin] publish failed:",
            "[scheduler:xiaohongshu] publish failed:",
            "[scheduler:kuaishou] publish failed:",
            "[scheduler:bilibili] publish failed:",
            "[scheduler:wechat] publish failed:",
            "publish failed:",
        )
    )


def _build_distribution_result_card(
    *,
    action_token: str,
    result_status: str,
    resolved_profile: str,
    platforms: list[str],
    raw_result: Dict[str, Any],
    minutes: Optional[int] = None,
    media_kind: str = "video",
) -> Dict[str, Any]:
    title = "定时发布已完成" if action_token == "schedule_run" else "立即发布已完成"
    merged = "\n".join(
        part.strip()
        for part in (
            str(raw_result.get("stdout") or ""),
            str(raw_result.get("stderr") or ""),
        )
        if str(part or "").strip()
    )
    effective_status = str(result_status or "").strip().lower()
    if _has_publish_failure_output(merged):
        effective_status = "failed"
    if effective_status != "done":
        title = "定时发布需处理" if action_token == "schedule_run" else "立即发布需处理"
    platform_value = platforms[0] if len(platforms) == 1 else "all"
    breadcrumb_value = (
        f"{_normalize_immediate_collect_media_kind(media_kind)}:{int(minutes or 0)}:{platform_value}"
        if action_token == "schedule_run"
        else f"{_normalize_immediate_collect_media_kind(media_kind)}:{platform_value}"
    )
    title = _prefix_menu_title(title, _menu_breadcrumb_for_action(action_token, breadcrumb_value))
    video_names = _extract_published_video_names(merged)
    menu_label = _menu_breadcrumb_for_action(action_token, breadcrumb_value)
    menu_section = _build_menu_path_section(menu_label)
    task_identifier_section = _build_task_identifier_section(
        _build_task_identifier(
            action=action_token,
            value=breadcrumb_value,
            menu_label=menu_label,
        )
    )
    sections: list[Dict[str, Any]] = [
        *([task_identifier_section] if task_identifier_section is not None else []),
        *([menu_section] if menu_section is not None else []),
        {
            "title": "执行摘要",
            "emoji": "📌",
            "items": [
                {"label": "目标平台", "value": _format_platform_text(platforms)},
                {"label": "状态", "value": "成功" if effective_status == "done" else "需处理"},
                {"label": "耗时", "value": f"{float(raw_result.get('elapsed') or 0.0):.1f}s"},
            ],
        }
    ]
    platform_summary = _build_platform_status_summary(platforms, merged, effective_status=effective_status)
    if platform_summary:
        sections[-1]["items"].insert(1, {"label": "平台摘要", "value": platform_summary})
    if action_token == "schedule_run" and minutes is not None and minutes > 0:
        sections[-1]["items"].append({"label": "时间窗口", "value": f"{int(minutes)} 分钟"})
    if video_names:
        sections.append(
            {
                "title": "视频信息",
                "emoji": "🎬",
                "items": [
                    {"label": f"视频 {idx}", "value": _preview_text(name, limit=120) or "-"}
                    for idx, name in enumerate(video_names, start=1)
                ],
            }
        )
    sections = _optimize_feedback_sections_for_operator(sections)
    card = build_action_feedback(
        status="success" if effective_status == "done" else "failed",
        title=title,
        subtitle=f"当前配置：{resolved_profile}",
        sections=sections,
        bot_name=BOT_NAME,
    )
    card["reply_markup"] = _with_home_button()
    return card


def _extract_collect_candidate_total(output: str) -> int:
    text = str(output or "")
    patterns = [
        r"候选(?:总数|数量)\s*[:：]\s*(\d+)",
        r"candidate(?:s)?\s*(?:total|count)?\s*[:=]\s*(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            try:
                return max(0, int(match.group(1) or 0))
            except Exception:
                return 0
    return 0


def _build_collect_result_card(
    *,
    result_status: str,
    resolved_profile: str,
    media_kind: str,
    raw_result: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(normalized_media_kind, "媒体")
    title = f"{media_label}立即采集已完成"
    if str(result_status or "").strip().lower() != "done":
        title = f"{media_label}立即采集需处理"
    merged = "\n".join(
        part.strip()
        for part in (
            str(raw_result.get("stdout") or ""),
            str(raw_result.get("stderr") or ""),
        )
        if str(part or "").strip()
    )
    title = _prefix_menu_title(
        title,
        _menu_breadcrumb_for_action("collect_now", f"{normalized_media_kind}:{_extract_collect_candidate_total(merged) or 0}"),
    )
    menu_label = _menu_breadcrumb_for_action("collect_now", f"{normalized_media_kind}:{_extract_collect_candidate_total(merged) or 0}")
    menu_section = _build_menu_path_section(menu_label)
    task_identifier_section = _build_task_identifier_section(
        _build_task_identifier(
            action="collect_now",
            value=f"{normalized_media_kind}:{_extract_collect_candidate_total(merged) or 0}",
            menu_label=menu_label,
        )
    )
    sections: list[Dict[str, Any]] = [
        *([task_identifier_section] if task_identifier_section is not None else []),
        *([menu_section] if menu_section is not None else []),
        {
            "title": "执行摘要",
            "emoji": "📌",
            "items": [
                {"label": "采集媒体", "value": media_label},
                {"label": "状态", "value": "成功" if result_status == "done" else "需处理"},
                {"label": "耗时", "value": f"{float(raw_result.get('elapsed') or 0.0):.1f}s"},
            ],
        }
    ]
    candidate_total = _extract_collect_candidate_total(merged)
    if candidate_total > 0:
        sections[0]["items"].append({"label": "候选数量", "value": str(candidate_total)})
    sections = _optimize_feedback_sections_for_operator(sections)
    card = build_action_feedback(
        status="success" if result_status == "done" else "failed",
        title=title,
        subtitle=f"当前配置：{resolved_profile}",
        sections=sections,
        bot_name=BOT_NAME,
    )
    card["reply_markup"] = _with_home_button()
    return card


def _wechat_qr_callback_data(action: str, platform: str = "wechat", wait_token: str = "") -> str:
    payload = (
        f"{TELEGRAM_WECHAT_QR_CALLBACK_PREFIX}|"
        f"{str(action or '').strip().lower()}|"
        f"{str(platform or 'wechat').strip().lower()}"
    )
    token = str(wait_token or "").strip()
    if token:
        payload += f"|{token}"
    return payload



def _parse_wechat_qr_callback_data(data: str) -> Optional[Dict[str, str]]:
    token = str(data or "").strip()
    if not token:
        return None
    parts = token.split("|")
    if len(parts) not in {2, 3, 4} or parts[0] != TELEGRAM_WECHAT_QR_CALLBACK_PREFIX:
        return None
    action = str(parts[1] or "").strip().lower()
    if action not in {"refresh", "done"}:
        return None
    platform = str(parts[2] or "wechat").strip().lower() if len(parts) == 3 else "wechat"
    if len(parts) == 4:
        platform = str(parts[2] or "wechat").strip().lower() or "wechat"
    if platform not in {"wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili"}:
        return None
    wait_token = str(parts[3] or "").strip() if len(parts) == 4 else ""
    return {"action": action, "platform": platform, "wait_token": wait_token}


def _guess_feedback_status(detail: str) -> str:
    text = str(detail or "").strip().lower()
    if not text:
        return "success"
    first_line = text.splitlines()[0].strip() if text.splitlines() else text
    if _has_publish_failure_output(text):
        return "failed"
    if "已有发布/采集 pipeline 正在运行，本次任务未实际执行" in text:
        return "failed"
    if ("退出码: 0" in text or "exit code: 0" in text) and any(
        marker in first_line for marker in (": 成功", ": 跳过", ": success", ": skipped")
    ):
        return "success"
    if any(
        token in text
        for token in (
            "message send failed via telegram_bot",
            "deletemessage failed",
            "telegram card send retry",
        )
    ) and ("退出码: 0" in text or "exit code: 0" in text):
        return "success"
    failure_tokens = ["失败", "未确认", "denied", "异常", "error", "bad password", "disabled"]
    login_tokens = ["无需扫码", "已登录"]
    if any(token in text for token in failure_tokens):
        return "failed"
    if any(token in text for token in login_tokens):
        return "success"
    return "success"


def _home_action_title(action: str) -> str:
    mapping = {
        "login_menu": "平台登录",
        "login_qr": "平台登录",
        "process_status": "进程查看",
        "view_result": "查看结果",
        "collect_log": "采集日志",
        "worker_status": "系统状态",
        "wechat_login_qr": "获取二维码",
        "collect_publish_latest": "即采即发",
        "collect_publish_latest_domestic": "国内即采即发",
        "collect_publish_latest_global": "海外即采即发",
        "collect_publish_latest_menu_domestic": "国内即采即发",
        "collect_publish_latest_menu_global": "海外即采即发",
        "comment_reply_menu": "点赞评论",
        "comment_reply_run": "点赞评论",
        "home": "返回首页",
    }
    return mapping.get(str(action or "").strip().lower(), "执行操作")


def _home_action_command(action: str) -> str:
    mapping = {
        "view_result": "/publish_log",
        "collect_log": "/collect_log",
        "worker_status": "/worker_status",
        "wechat_login_qr": "/wechat_login_qr",
    }
    return mapping.get(str(action or "").strip().lower(), "")


def _home_action_loading_text(action: str, value: str) -> str:
    action_token = str(action or "").strip().lower()
    value_token = str(value or "").strip().lower()
    if action_token == "collect_now":
        media_kind, count = _parse_collect_request_value(value_token)
        media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
        count_text = f"，目标 {count} 条" if count > 0 else ""
        return f"🚀 已开始{media_label}采集{count_text}，正在拉起浏览器并进入采集流程..."
    if action_token == "login_qr":
        platform_label = PUBLISH_PLATFORM_DISPLAY.get(value_token, value_token or "目标平台")
        return f"⏳ 正在获取{platform_label}登录二维码..."
    if _is_collect_publish_action(action_token):
        media_kind, count = _parse_collect_publish_request_value(value_token)
        media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
        return f"⏳ 正在整理{media_label}即采即发候选，目标 {count} 条..."
    if action_token == "publish_run":
        media_kind, platform_value = _parse_publish_request_value(value_token)
        media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
        platform_label = "全部平台" if platform_value in {"", "all"} else PUBLISH_PLATFORM_DISPLAY.get(platform_value, platform_value)
        return f"⏳ 正在提交{media_label}立即发布请求：{platform_label}..."
    if action_token == "schedule_run":
        media_kind, minutes, platform_value = _parse_schedule_callback_value(value_token)
        media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
        platform_label = "全部平台" if platform_value in {"", "all"} else PUBLISH_PLATFORM_DISPLAY.get(platform_value, platform_value)
        return f"⏳ 正在提交{media_label}定时发布请求：{minutes} 分钟 / {platform_label}..."
    return "⏳ 正在处理，请稍候..."


def _home_action_feedback_suffix(status: str) -> str:
    token = str(status or "").strip().lower()
    mapping = {
        "queued": "已受理",
        "running": "进行中",
        "done": "已完成",
        "failed": "执行失败",
        "blocked": "状态待确认",
    }
    return mapping.get(token, "状态更新")


def _home_action_feedback_title(action: str, status: str, value: str = "") -> str:
    base_title = f"{_home_action_title(action)}{_home_action_feedback_suffix(status)}"
    return _prefix_menu_title(base_title, _menu_breadcrumb_for_action(action, value))


def _home_action_result_title(action: str, status: str, value: str = "", detail: str = "") -> str:
    action_token = str(action or "").strip().lower()
    status_token = str(status or "").strip().lower()
    detail_text = str(detail or "").strip()
    if action_token == "login_qr" and status_token == "done":
        if "无需扫码" in detail_text or "当前已登录" in detail_text:
            base_title = "平台当前已登录"
        else:
            base_title = "平台登录二维码已发送"
        return _prefix_menu_title(base_title, _menu_breadcrumb_for_action(action_token, value))
    return _home_action_feedback_title(action_token, status_token, value)


def _parse_schedule_callback_value(raw: str) -> tuple[str, int, str]:
    token = str(raw or "").strip().lower()
    if not token:
        return "video", 0, ""
    parts = [part.strip() for part in token.split(":")]
    if len(parts) == 1:
        try:
            return "video", int(parts[0]), ""
        except Exception:
            return "video", 0, ""
    if len(parts) >= 3 and parts[0] in IMMEDIATE_COLLECT_MEDIA_KIND_ORDER:
        media_kind = _normalize_immediate_collect_media_kind(parts[0])
        minutes_text = parts[1]
        platform_value = parts[2]
    else:
        media_kind = "video"
        minutes_text = parts[0]
        platform_value = parts[1]
    try:
        minutes = int(str(minutes_text or "").strip())
    except Exception:
        return media_kind, 0, ""
    return media_kind, minutes, str(platform_value or "").strip().lower()

def _handle_home_callback(
    *,
    callback: Dict[str, Any],
    parsed: Dict[str, str],
    bot_token: str,
    command_password: str,
    started_at: float,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    allow_shell: bool,
    allow_prefixes: list[str],
    audit_file: Path,
    update_id: int,
    default_profile: str,
    immediate_test_mode: bool = False,
) -> Dict[str, Any]:
    action = str(parsed.get("action") or "").strip().lower()
    value = str(parsed.get("value") or "").strip()
    chat_id = str(callback.get("chat_id") or "").strip()
    query_id = str(callback.get("query_id") or "").strip()
    username = str(callback.get("username") or "").strip()
    message_id = int(callback.get("message_id") or 0)
    inline_message_id = str(callback.get("inline_message_id") or "").strip()
    resolved_default_profile = _normalize_profile_name(default_profile)

    if action == "home":
        answer_interaction_toast(
            bot_token=bot_token,
            query_id=query_id,
            action="home",
            status="success",
            timeout_seconds=timeout_seconds,
        )
        _send_interaction_result_async(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_build_home_card(
                default_profile=resolved_default_profile,
                status_note="已返回首页",
                workspace=workspace,
                chat_id=chat_id,
            ),
            timeout_seconds=timeout_seconds,
            message_id=message_id,
            inline_message_id=inline_message_id,
        )
        return {"handled": True, "update_id": update_id}

    if action == "process_status":
        answer_interaction_toast(
            bot_token=bot_token,
            query_id=query_id,
            action=action,
            status="success",
            timeout_seconds=timeout_seconds,
        )
        _send_interaction_result_async(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_build_process_status_card(
                default_profile=resolved_default_profile,
                workspace=workspace,
            ),
            timeout_seconds=timeout_seconds,
        )
        return {"handled": True, "update_id": update_id}

    if action == "process_status_refresh":
        answer_interaction_toast(
            bot_token=bot_token,
            query_id=query_id,
            action="process_status",
            status="success",
            timeout_seconds=timeout_seconds,
        )
        _send_interaction_result_async(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_build_process_status_card(
                default_profile=resolved_default_profile,
                workspace=workspace,
            ),
            timeout_seconds=timeout_seconds,
            message_id=message_id,
            inline_message_id=inline_message_id,
        )
        return {"handled": True, "update_id": update_id}

    if action == "process_status_cleanup_queue":
        cleanup_summary = _cleanup_inactive_prefilter_items(
            workspace,
            log_file=workspace / "runtime" / "logs" / "telegram_command_worker.log",
        )
        removed_inactive = int(cleanup_summary.get("removed_inactive") or 0)
        filter_synced = int(cleanup_summary.get("filter_synced") or 0)
        if removed_inactive > 0:
            answer_interaction_toast(
                bot_token=bot_token,
                query_id=query_id,
                action=action,
                status="success",
                timeout_seconds=timeout_seconds,
            )
            status_note = f"已清理 {removed_inactive} 条非活跃队列项，其中 {filter_synced} 条已纳入后续采集过滤"
        else:
            answer_interaction_toast(
                bot_token=bot_token,
                query_id=query_id,
                action=action,
                status="success",
                timeout_seconds=timeout_seconds,
            )
            status_note = "没有发现可清理的非活跃队列项"
        _send_interaction_result_async(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_build_process_status_card(
                default_profile=resolved_default_profile,
                workspace=workspace,
                status_note=status_note,
            ),
            timeout_seconds=timeout_seconds,
            message_id=message_id,
            inline_message_id=inline_message_id,
        )
        return {"handled": True, "update_id": update_id}

    if action in {COLLECT_PUBLISH_MENU_ACTION, COLLECT_PUBLISH_MENU_ACTION_DOMESTIC, COLLECT_PUBLISH_MENU_ACTION_GLOBAL}:
        route_key = _collect_publish_route_key(action)
        callback_action = _collect_publish_callback_action(route_key)
        route_profile = _collect_publish_profile_for_route(route_key, resolved_default_profile)
        route_label = _collect_publish_route_label(route_key) if route_key != "default" else ""
        if value:
            answer_interaction_toast(
                bot_token=bot_token,
                query_id=query_id,
                action=action,
                status="success",
                timeout_seconds=timeout_seconds,
            )
            card = _build_collect_publish_latest_menu_card(
                default_profile=route_profile,
                callback_action=callback_action,
                route_label=route_label,
            )
            _send_interaction_result_async(
                bot_token=bot_token,
                chat_id=chat_id,
                card=card,
                timeout_seconds=timeout_seconds,
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
            return {"handled": True, "update_id": update_id}
        if action in {COLLECT_PUBLISH_MENU_ACTION_DOMESTIC, COLLECT_PUBLISH_MENU_ACTION_GLOBAL}:
            answer_interaction_toast(
                bot_token=bot_token,
                query_id=query_id,
                action=action,
                status="success",
                timeout_seconds=timeout_seconds,
            )
            _send_interaction_result_async(
                bot_token=bot_token,
                chat_id=chat_id,
                card=_build_collect_publish_latest_menu_card(
                    default_profile=route_profile,
                    callback_action=callback_action,
                    route_label=route_label,
                ),
                timeout_seconds=timeout_seconds,
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
            return {"handled": True, "update_id": update_id}
        answer_interaction_toast(
            bot_token=bot_token,
            query_id=query_id,
            action=action,
            status="success",
            timeout_seconds=timeout_seconds,
        )
        _send_interaction_result_async(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_build_media_pick_card(
                title="即采即发",
                subtitle="先选媒体类型，再选候选数量",
                media_action="collect_publish_latest_menu",
                default_profile=resolved_default_profile,
            ),
            timeout_seconds=timeout_seconds,
            message_id=message_id,
            inline_message_id=inline_message_id,
        )
        return {"handled": True, "update_id": update_id}

    action_profile = resolved_default_profile
    execution_action = action
    if _is_collect_publish_action(action):
        route_key = _collect_publish_route_key(action)
        action_profile = _collect_publish_profile_for_route(route_key, resolved_default_profile)
        execution_action = COLLECT_PUBLISH_ACTION

    if action == "comment_reply_menu":
        answer_interaction_toast(
            bot_token=bot_token,
            query_id=query_id,
            action=action,
            status="success",
            timeout_seconds=timeout_seconds,
        )
        _send_interaction_result_async(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_build_comment_reply_menu_card(default_profile=resolved_default_profile),
            timeout_seconds=timeout_seconds,
            message_id=message_id,
            inline_message_id=inline_message_id,
        )
        return {"handled": True, "update_id": update_id}

    if action == "login_menu":
        answer_interaction_toast(
            bot_token=bot_token,
            query_id=query_id,
            action=action,
            status="success",
            timeout_seconds=timeout_seconds,
        )
        _send_interaction_result_async(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_build_login_menu_card(default_profile=resolved_default_profile),
            timeout_seconds=timeout_seconds,
            message_id=message_id,
            inline_message_id=inline_message_id,
        )
        return {"handled": True, "update_id": update_id}

    if action == "login_qr":
        platform_value = value.strip().lower()
        if platform_value not in PUBLISH_PLATFORM_ORDER:
            answer_interaction_toast(
                bot_token=bot_token,
                query_id=query_id,
                action=action,
                status="failed",
                timeout_seconds=timeout_seconds,
            )
            return {"handled": True, "update_id": update_id}
        value = platform_value

    title = _home_action_title(action)
    if execution_action in HOME_ACTION_ASYNC_ACTIONS:
        if _has_runtime_conflict_for_action(workspace, action=execution_action, value=value):
            _answer_callback_query(
                bot_token=bot_token,
                query_id=query_id,
                text="当前已有任务占锁，请稍后或先强制解锁。",
                timeout_seconds=timeout_seconds,
            )
            send_interaction_result(
                bot_token=bot_token,
                chat_id=chat_id,
                card=_ensure_card_has_home_button(
                    _home_feedback_response(
                        status="blocked",
                        title=_home_action_feedback_title(action, "blocked", value),
                        subtitle=f"当前配置：{action_profile}",
                        detail=_describe_runtime_conflict(workspace, action=execution_action, value=value),
                        menu_label=_menu_breadcrumb_for_action(action, value),
                        task_identifier=_build_task_identifier(action=action, value=value),
                    )
                ),
                timeout_seconds=timeout_seconds,
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
            return {"handled": True, "update_id": update_id}

        runtime_bot_identifier = str(
            _env_first(
                "CYBERCAR_NOTIFY_TELEGRAM_BOT_IDENTIFIER",
                "CYBERCAR_NOTIFY_TELEGRAM_KEYWORD",
                "NOTIFY_TELEGRAM_BOT_IDENTIFIER",
                "NOTIFY_TELEGRAM_KEYWORD",
                default="cybercar",
            )
            or ""
        ).strip()
        claim = _claim_home_action_task(
            workspace=workspace,
            chat_id=chat_id,
            action=execution_action,
            value=value,
            profile=action_profile,
            username=username,
        )
        task = claim.get("task") if isinstance(claim.get("task"), dict) else {}
        task_key = str(claim.get("task_key") or "").strip()
        if not bool(claim.get("accepted")):
            _answer_callback_query(
                bot_token=bot_token,
                query_id=query_id,
                text="任务已在执行，请稍候。",
                timeout_seconds=timeout_seconds,
            )
            send_interaction_result(
                bot_token=bot_token,
                chat_id=chat_id,
                card=_ensure_card_has_home_button(
                    _home_feedback_response(
                        status="running",
                        title=_home_action_feedback_title(action, "running", value),
                        subtitle=f"当前配置：{action_profile}",
                        detail=_describe_home_action_task(task),
                        menu_label=_menu_breadcrumb_for_action(action, value),
                        task_identifier=_build_task_identifier(
                            action=action,
                            value=value,
                            log_path=str(task.get("log_path") or ""),
                            updated_at=str(task.get("updated_at") or ""),
                        ),
                    )
                ),
                timeout_seconds=timeout_seconds,
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
            return {"handled": True, "update_id": update_id}

        loading_message_id = 0
        _answer_callback_query(
            bot_token=bot_token,
            query_id=query_id,
            text="任务已受理，正在排队执行。",
            timeout_seconds=timeout_seconds,
        )
        if execution_action in HOME_ACTION_LOADING_PLACEHOLDER_ACTIONS and chat_id:
            loading_message_id = _send_loading_placeholder(
                bot_token=bot_token,
                chat_id=chat_id,
                text=_home_action_loading_text(action, value),
                timeout_seconds=timeout_seconds,
            )
            if loading_message_id > 0:
                _update_home_action_task(
                    workspace,
                    task_key,
                    extra={"loading_message_id": int(loading_message_id)},
                )
        send_interaction_result(
            bot_token=bot_token,
            chat_id=chat_id,
            card=_ensure_card_has_home_button(
                _home_feedback_response(
                    status="running",
                    title=_home_action_feedback_title(action, "queued", value),
                    subtitle=f"当前配置：{action_profile}",
                    detail="请求已进入后台队列。你可以稍后刷新首页查看最近有效任务，系统也会继续回传最终结果。",
                    menu_label=_menu_breadcrumb_for_action(action, value),
                    task_identifier=_build_task_identifier(action=action, value=value),
                )
            ),
            timeout_seconds=timeout_seconds,
            message_id=message_id,
            inline_message_id=inline_message_id,
        )
        queued_value = str(task.get("value") or value).strip() or str(value or "").strip()
        try:
            raw_result = _spawn_home_action_job(
                repo_root=repo_root,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                profile=action_profile,
                telegram_bot_identifier=runtime_bot_identifier,
                telegram_bot_token=bot_token,
                telegram_chat_id=chat_id,
                action=execution_action,
                value=queued_value,
                task_key=task_key,
                immediate_test_mode=immediate_test_mode,
            )
            detail = "后台任务已启动。"
            if execution_action == COLLECT_PUBLISH_ACTION:
                media_kind, _count = _parse_collect_publish_request_value(queued_value)
                media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
                target_platforms = _collect_publish_target_platforms(media_kind)
                detail = (
                    f"{media_label}即采即发后台任务已启动。\n"
                    "后续会先扫描候选，再逐条发送预审卡片；"
                    f"只有你明确点击“普通发布”或“原创发布”的候选，才会进入{_format_platform_text(target_platforms)}发布。"
                )
            elif execution_action == "comment_reply_run":
                comment_platform, comment_limit = _parse_comment_reply_request_value(queued_value)
                if comment_platform == "all":
                    detail = f"三平台点赞评论后台任务已启动，本轮会按相同数量检查最近 {max(1, int(comment_limit))} 个有评论视频。"
                else:
                    detail = f"{_menu_platform_label(comment_platform)}点赞评论后台任务已启动，本轮会检查最近 {max(1, int(comment_limit))} 个有评论视频。"
            elif execution_action == "login_qr":
                detail = "正在检查登录会话并准备二维码消息。"
            elif execution_action == "publish_run":
                detail = "发布任务已进入后台队列，完成后会继续回传平台结果。"
            elif execution_action == "schedule_run":
                detail = "定时发布任务已进入后台队列，完成后会继续回传结果。"
            elif execution_action == "collect_now":
                detail = "采集任务已进入后台队列，完成后会回传采集结果。"
            updated_task = _update_home_action_task(
                workspace,
                task_key,
                status="running",
                detail=detail,
                log_path=str(raw_result.get("log_path") or "").strip(),
                pid=int(raw_result.get("pid") or 0),
            )
            send_interaction_result(
                bot_token=bot_token,
                chat_id=chat_id,
                card=_ensure_card_has_home_button(
                    _home_feedback_response(
                        status="running",
                        title=_home_action_feedback_title(action, "running", queued_value),
                        subtitle=f"当前配置：{action_profile}",
                        detail=_describe_home_action_task(updated_task),
                        menu_label=_menu_breadcrumb_for_action(action, queued_value),
                        task_identifier=_build_task_identifier(
                            action=action,
                            value=queued_value,
                            log_path=str(updated_task.get("log_path") or ""),
                            updated_at=str(updated_task.get("updated_at") or ""),
                        ),
                    )
                ),
                timeout_seconds=timeout_seconds,
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
        except Exception as exc:
            if loading_message_id > 0:
                _try_delete_telegram_message(
                    bot_token=bot_token,
                    chat_id=chat_id,
                    message_id=loading_message_id,
                    timeout_seconds=timeout_seconds,
                    log_file=audit_file.parent / "telegram_command_worker.log",
                )
            failed_task = _update_home_action_task(
                workspace,
                task_key,
                status="failed",
                detail=f"后台任务启动失败：{exc}",
            )
            send_interaction_result(
                bot_token=bot_token,
                chat_id=chat_id,
                card=_ensure_card_has_home_button(
                    _home_feedback_response(
                        status="failed",
                        title=_home_action_feedback_title(action, "failed", value),
                        subtitle=f"当前配置：{action_profile}",
                        detail=_describe_home_action_task(failed_task),
                        menu_label=_menu_breadcrumb_for_action(action, value),
                        task_identifier=_build_task_identifier(
                            action=action,
                            value=value,
                            log_path=str(failed_task.get("log_path") or ""),
                            updated_at=str(failed_task.get("updated_at") or ""),
                        ),
                    )
                ),
                timeout_seconds=timeout_seconds,
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
        return {"handled": True, "update_id": update_id}

    answer_interaction_toast(
        bot_token=bot_token,
        query_id=query_id,
        action=action,
        status="queued",
        timeout_seconds=timeout_seconds,
    )

    if action == "unknown_fallback":
        raw_result = {}
        detail = ""
    else:
        command_text = _home_action_command(action)
        if not command_text:
            answer_interaction_toast(
                bot_token=bot_token,
                query_id=query_id,
                action=action,
                status="failed",
                timeout_seconds=timeout_seconds,
            )
            send_interaction_result(
                bot_token=bot_token,
                chat_id=chat_id,
                card=_ensure_card_has_home_button(
                    _home_feedback_response(
                        status="failed",
                        title=f"{title}不可用",
                        subtitle="当前按钮已失效，请返回首页重试",
                        detail="该入口当前不可执行。",
                        menu_label=_menu_breadcrumb_for_action(action, value),
                        task_identifier=_build_task_identifier(action=action, value=value),
                    )
                ),
                timeout_seconds=timeout_seconds,
                message_id=message_id,
                inline_message_id=inline_message_id,
            )
            return {"handled": True, "update_id": update_id}
        raw_result = _handle_command(
            text=command_text,
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            allow_shell=allow_shell,
            allow_prefixes=allow_prefixes,
            command_password=command_password,
            started_at=started_at,
            last_processed=update_id,
            update_id=update_id,
            chat_id=chat_id,
            username=username,
            audit_file=audit_file,
            default_profile=resolved_default_profile,
        )
        detail = str(raw_result.get("text") or "") if isinstance(raw_result, dict) and "text" in raw_result else str(raw_result or "")
    status = _guess_feedback_status(detail)
    result_title = _home_action_result_title(
        action,
        "done" if status == "success" else "failed",
        value,
        detail,
    )
    if _is_collect_publish_action(action) or action == "comment_reply_run":
        result_title = f"{title}已启动"
    send_interaction_result(
        bot_token=bot_token,
        chat_id=chat_id,
        card=_ensure_card_has_home_button(
            _home_feedback_response(
                status=status,
                title=result_title,
                subtitle=f"当前配置：{resolved_default_profile}",
                detail=detail,
                menu_label=_menu_breadcrumb_for_action(action, value),
                task_identifier=_build_task_identifier(action=action, value=value),
            )
        ),
        timeout_seconds=timeout_seconds,
        message_id=message_id,
        inline_message_id=inline_message_id,
    )
    return {"handled": True, "update_id": update_id}


def _edit_reply(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    inline_message_id: str,
    text: str,
    timeout_seconds: int,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> None:
    params: Dict[str, Any] = {"text": str(text or "").strip() or "(empty)"}
    if inline_message_id:
        params["inline_message_id"] = inline_message_id
    else:
        if not chat_id or int(message_id) <= 0:
            raise RuntimeError("missing target message for edit")
        params["chat_id"] = chat_id
        params["message_id"] = int(message_id)
    outgoing_reply_markup = _outgoing_reply_markup(reply_markup, for_edit=True)
    if isinstance(outgoing_reply_markup, dict):
        params["reply_markup"] = json.dumps(outgoing_reply_markup, ensure_ascii=True)
    _telegram_api(
        bot_token=bot_token,
        method="editMessageText",
        params=params,
        timeout_seconds=max(8, int(timeout_seconds)),
        use_post=True,
    )


def _present_callback_view(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    inline_message_id: str,
    view: Dict[str, Any],
    timeout_seconds: int,
    log_file: Path,
    fallback_to_send: bool = True,
) -> None:
    normalized = _normalize_reply_payload(view)
    reply_markup = normalized.get("reply_markup") if isinstance(normalized.get("reply_markup"), dict) else None
    try:
        _edit_reply(
            bot_token=bot_token,
            chat_id=chat_id,
            message_id=message_id,
            inline_message_id=inline_message_id,
            text=str(normalized.get("text") or ""),
            timeout_seconds=timeout_seconds,
            reply_markup=reply_markup,
        )
        return
    except Exception as exc:
        desc = str(exc).lower()
        if "message is not modified" in desc:
            return
        _append_log(log_file, f"[Worker] editMessageText failed, fallback_to_send={fallback_to_send}: {exc}")
    if fallback_to_send and chat_id:
        _send_reply(
            bot_token=bot_token,
            chat_id=chat_id,
            text=str(normalized.get("text") or ""),
            timeout_seconds=max(20, int(timeout_seconds)),
            reply_markup=reply_markup,
            parse_mode=str(normalized.get("parse_mode") or ""),
        )


def _present_callback_view_async(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    inline_message_id: str,
    view: Dict[str, Any],
    timeout_seconds: int,
    log_file: Path,
    fallback_to_send: bool = True,
) -> None:
    # Pure menu navigation does not need to block the polling loop on Telegram UI updates.
    thread = threading.Thread(
        target=_present_callback_view,
        kwargs={
            "bot_token": bot_token,
            "chat_id": chat_id,
            "message_id": message_id,
            "inline_message_id": inline_message_id,
            "view": view,
            "timeout_seconds": timeout_seconds,
            "log_file": log_file,
            "fallback_to_send": fallback_to_send,
        },
        daemon=True,
        name="cybercar-telegram-view",
    )
    thread.start()


def _send_interaction_result_async(
    *,
    bot_token: str,
    chat_id: str,
    card: Dict[str, Any],
    timeout_seconds: int,
    message_id: int = 0,
    inline_message_id: str = "",
) -> None:
    card = _ensure_card_has_home_button(card)
    thread = threading.Thread(
        target=send_interaction_result,
        kwargs={
            "bot_token": bot_token,
            "chat_id": chat_id,
            "card": card,
            "timeout_seconds": timeout_seconds,
            "message_id": message_id,
            "inline_message_id": inline_message_id,
        },
        daemon=True,
        name="cybercar-telegram-card",
    )
    thread.start()

def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = path.read_text(encoding="utf-8-sig")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        backup_path = path.with_name(f"{path.name}.invalid-{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak")
        try:
            shutil.copy2(path, backup_path)
        except Exception:
            pass
        return {}


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _update_worker_state(path: Path, **updates: Any) -> Dict[str, Any]:
    state = _load_state(path)
    state.update({k: v for k, v in updates.items()})
    state["updated_at"] = _now_text()
    _save_state(path, state)
    return state


def _safe_update_worker_state(path: Path, log_file: Path, **updates: Any) -> Dict[str, Any]:
    try:
        return _update_worker_state(path, **updates)
    except Exception as exc:
        _append_log(log_file, f"[Worker] state update skipped: {exc}")
        return {}


def _safe_save_offset(path: Path, offset: int, log_file: Path) -> None:
    try:
        _save_offset(path, offset)
    except Exception as exc:
        _append_log(log_file, f"[Worker] offset save skipped: {exc}")


def _safe_save_state(path: Path, state: Dict[str, Any], log_file: Path) -> None:
    try:
        _save_state(path, state)
    except Exception as exc:
        _append_log(log_file, f"[Worker] state save skipped: {exc}")


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}")
    try:
        with tmp.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
            handle.flush()
            os.fsync(handle.fileno())
        last_error: Optional[BaseException] = None
        max_attempts = max(1, int(DEFAULT_ATOMIC_WRITE_REPLACE_MAX_ATTEMPTS))
        base_backoff = max(0.01, float(DEFAULT_ATOMIC_WRITE_REPLACE_BACKOFF_BASE_SECONDS))
        for attempt in range(max_attempts):
            try:
                os.replace(tmp, path)
                last_error = None
                break
            except OSError as exc:
                last_error = exc
                winerror = int(getattr(exc, "winerror", 0) or 0)
                retryable_windows_lock_error = os.name == "nt" and (
                    winerror in {5, 32}
                    or isinstance(exc, PermissionError)
                )
                if not retryable_windows_lock_error or attempt >= (max_attempts - 1):
                    raise
                time.sleep(min(1.0, base_backoff * float(2 ** min(attempt, 5))))
        if last_error is not None:
            raise last_error
    finally:
        if tmp.exists():
            with contextlib.suppress(Exception):
                tmp.unlink()


def _file_lock_dir(path: Path) -> Path:
    return path.with_name(f"{path.name}.lock")


def _file_lock_owner_path(lock_dir: Path) -> Path:
    return lock_dir / "owner.json"


def _build_file_lock_owner_payload(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "path": str(path),
        "created_at": _now_text(),
        "acquired_epoch": time.time(),
    }
    try:
        payload["ppid"] = os.getppid()
    except Exception:
        payload["ppid"] = 0
    return payload


def _load_file_lock_owner_payload(lock_dir: Path) -> dict[str, Any]:
    owner_path = _file_lock_owner_path(lock_dir)
    if not owner_path.exists():
        return {}
    try:
        payload = json.loads(owner_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_known_dead_lock_owner(lock_dir: Path) -> bool:
    payload = _load_file_lock_owner_payload(lock_dir)
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        return False
    host = str(payload.get("host") or "").strip()
    if host and host.lower() != socket.gethostname().lower():
        return False
    return not _pid_is_running(pid)


def _pid_is_running(pid: int) -> bool:
    clean_pid = int(pid or 0)
    if clean_pid <= 0:
        return False
    if os.name == "nt" and ctypes is not None:
        process_query_limited_information = 0x1000
        synchronize = 0x00100000
        access = process_query_limited_information | synchronize
        handle = ctypes.windll.kernel32.OpenProcess(access, False, clean_pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(clean_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _poller_lock_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_POLLER_LOCK_DIR).resolve()


def _build_poller_owner_payload() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "lock_version": 2,
        "pid": os.getpid(),
        "acquired_epoch": time.time(),
        "created_at": _now_text(),
        "host": socket.gethostname(),
        "cwd": str(Path.cwd()),
        "python_executable": sys.executable,
        "argv": list(sys.argv),
    }
    try:
        payload["ppid"] = os.getppid()
    except Exception:
        payload["ppid"] = 0
    return payload


def _lock_dir_age_seconds(lock_dir: Path) -> float:
    try:
        return max(0.0, time.time() - float(lock_dir.stat().st_mtime))
    except Exception:
        return 0.0


def _load_poller_owner_payload(owner_file: Path) -> dict[str, Any]:
    if not owner_file.exists():
        return {}
    try:
        payload = json.loads(owner_file.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _cleanup_stale_poller_lock(lock_dir: Path, *, log_file: Path, reason: str) -> bool:
    try:
        shutil.rmtree(lock_dir)
    except FileNotFoundError:
        return True
    except Exception as exc:
        _append_log(log_file, f"[Worker] poller lock cleanup failed ({reason}): {exc}")
        return False
    _append_log(log_file, f"[Worker] removed stale poller lock: {reason}")
    return True


def _acquire_poller_lock(*, workspace: Path, log_file: Path) -> Optional[Path]:
    lock_dir = _poller_lock_path(workspace)
    owner_file = lock_dir / "owner.json"
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            lock_dir.mkdir()
            owner_file.write_text(
                json.dumps(_build_poller_owner_payload(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return lock_dir
        except FileExistsError:
            payload = _load_poller_owner_payload(owner_file)
            owner_pid = int(payload.get("pid") or 0)
            owner_created_at = str(payload.get("created_at") or "").strip()
            owner_host = str(payload.get("host") or "").strip()
            raw_argv = payload.get("argv")
            owner_argv = [str(item) for item in raw_argv if str(item).strip()] if isinstance(raw_argv, list) else []
            lock_age_seconds = _lock_dir_age_seconds(lock_dir)
            if owner_pid > 0 and _pid_is_running(owner_pid):
                owner_bits = [f"existing_pid={owner_pid}"]
                if owner_created_at:
                    owner_bits.append(f"created_at={owner_created_at}")
                if owner_host:
                    owner_bits.append(f"host={owner_host}")
                owner_bits.append(f"lock_age={int(lock_age_seconds)}s")
                if owner_argv:
                    owner_bits.append(f"argv={' '.join(owner_argv[:8])}")
                _append_log(log_file, f"[Worker] poller lock busy; {', '.join(owner_bits)}, skip duplicate start.")
                return None
            if not payload and lock_age_seconds < float(DEFAULT_POLLER_LOCK_STARTUP_GRACE_SECONDS):
                time.sleep(0.2)
                continue
            if _cleanup_stale_poller_lock(
                lock_dir,
                log_file=log_file,
                reason=(
                    f"owner_pid={owner_pid or 0} inactive, "
                    f"owner_file={'present' if owner_file.exists() else 'missing'}, "
                    f"lock_age={int(lock_age_seconds)}s"
                ),
            ):
                continue
            if lock_age_seconds < float(DEFAULT_POLLER_LOCK_STALE_SECONDS):
                time.sleep(0.5)
                continue
            _append_log(log_file, "[Worker] poller lock remains unusable after stale cleanup attempt; abort start.")
            return None


def _release_poller_lock(lock_dir: Optional[Path]) -> None:
    if not isinstance(lock_dir, Path):
        return
    try:
        shutil.rmtree(lock_dir)
    except FileNotFoundError:
        return
    except Exception:
        return


def _acquire_file_lock(path: Path, timeout_seconds: float = DEFAULT_PREFILTER_QUEUE_LOCK_TIMEOUT_SECONDS) -> Path:
    lock_dir = _file_lock_dir(path)
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + max(5.0, float(timeout_seconds))
    stale_after_seconds = max(120.0, float(timeout_seconds) * 4.0)
    while True:
        try:
            lock_dir.mkdir(parents=False, exist_ok=False)
            _atomic_write_json(_file_lock_owner_path(lock_dir), _build_file_lock_owner_payload(path))
            return lock_dir
        except FileExistsError:
            try:
                age_seconds = max(0.0, time.time() - lock_dir.stat().st_mtime)
            except Exception:
                age_seconds = 0.0
            if _is_known_dead_lock_owner(lock_dir):
                try:
                    shutil.rmtree(lock_dir, ignore_errors=True)
                    continue
                except Exception:
                    pass
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


def _action_queue_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_ACTION_QUEUE_FILE).resolve()


def _load_action_queue(path: Path) -> Dict[str, Any]:
    payload = _load_state(path)
    repaired = False
    if path.exists():
        try:
            repaired = path.stat().st_size > 0 and not bool(payload)
        except Exception:
            repaired = False
    tasks = payload.get("tasks")
    if not isinstance(tasks, dict):
        tasks = {}
        repaired = True
    normalized_tasks: Dict[str, Any] = {}
    for task_key, task in tasks.items():
        if not isinstance(task, dict):
            repaired = True
            continue
        normalized_key = str(task_key or "").strip()
        if not normalized_key:
            repaired = True
            continue
        normalized_task = _normalize_home_action_task_record(task)
        if normalized_task != task or normalized_key != str(task_key):
            repaired = True
        normalized_tasks[normalized_key] = normalized_task
    queue = {
        "version": int(payload.get("version") or 1),
        "tasks": normalized_tasks,
    }
    if repaired:
        _atomic_write_json(path, queue)
    return queue


def _save_action_queue(path: Path, queue: Dict[str, Any]) -> None:
    raw_tasks = queue.get("tasks", {})
    tasks: Dict[str, Any] = {}
    if isinstance(raw_tasks, dict):
        for task_key, task in raw_tasks.items():
            if not isinstance(task, dict):
                continue
            tasks[str(task_key or "").strip()] = _normalize_home_action_task_record(task)
    _atomic_write_json(
        path,
        {
            "version": int(queue.get("version") or 1),
            "tasks": tasks,
        },
    )


def _normalize_home_action_value(action: str, value: str) -> str:
    action_token = str(action or "").strip().lower()
    raw = str(value or "").strip()
    if action_token == "collect_now":
        media_kind, count = _parse_collect_request_value(raw)
        return f"{media_kind}:{count}" if count > 0 else media_kind
    if action_token == "publish_run":
        media_kind, platform = _parse_publish_request_value(raw)
        valid_platforms = _collect_publish_target_platforms(media_kind)
        platform_value = platform if platform == "all" or platform in valid_platforms else "all"
        return f"{media_kind}:{platform_value}"
    if action_token == "schedule_run":
        media_kind, minutes, platform = _parse_schedule_callback_value(raw)
        valid_platforms = _collect_publish_target_platforms(media_kind)
        platform_value = platform if platform in valid_platforms else "all"
        return f"{media_kind}:{max(0, int(minutes))}:{platform_value}"
    if _is_collect_publish_action(action_token):
        media_kind, count = _parse_collect_publish_request_value(raw)
        return f"{media_kind}:{count}"
    if action_token == "comment_reply_run":
        _, post_limit = _parse_comment_reply_request_value(raw)
        # Telegram comment-reply menu now uses a merged count-only workflow.
        # Force any legacy per-platform callback (for example "wechat:5")
        # onto the new all-platform route so stale inline keyboards still
        # trigger the expected three-platform execution.
        return f"all:{max(1, int(post_limit))}"
    return raw.lower()


def _build_home_action_key(*, chat_id: str, action: str, value: str, profile: str) -> str:
    payload = {
        "chat_id": str(chat_id or "").strip(),
        "action": str(action or "").strip().lower(),
        "value": _normalize_home_action_value(action, value),
        "profile": _normalize_profile_name(profile),
    }
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _looks_like_mojibake_text(text: str) -> bool:
    token = str(text or "").strip()
    if not token:
        return False
    suspicious_markers = ["鍚", "鍗", "璇", "浠", "诲", "绐", "銆", "€", "�", "瀹", "氭", "椂", "戝", "竷", "锛"]
    score = sum(token.count(marker) for marker in suspicious_markers)
    return score >= 2


def _fallback_home_action_detail(action: str, status: str) -> str:
    title = _home_action_title(action)
    status_token = str(status or "").strip().lower()
    if status_token == "done":
        return f"{title}已完成，请查看最新结果消息。"
    if status_token == "failed":
        return f"{title}执行失败，请重新发起或查看最新结果消息。"
    if status_token == "blocked":
        return f"{title}状态待确认，请查看最新结果消息。"
    if status_token == "running":
        return f"{title}正在后台执行，请稍候。"
    return f"{title}已进入队列，等待后台执行。"


def _compact_log_path(log_path: str) -> str:
    return _log_display_name(log_path)


def _normalize_home_action_task_record(task: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(task) if isinstance(task, dict) else {}
    action = str(normalized.get("action") or "").strip().lower()
    status = str(normalized.get("status") or "").strip().lower()
    detail = str(normalized.get("detail") or "").strip()
    if not detail or _looks_like_mojibake_text(detail):
        normalized["detail"] = _fallback_home_action_detail(action, status)
    else:
        normalized["detail"] = detail
    normalized["log_path"] = _compact_log_path(str(normalized.get("log_path") or "").strip())
    normalized["task_identifier"] = _build_home_task_identifier(normalized)
    return normalized


def _describe_home_action_task(task: Dict[str, Any]) -> str:
    if not isinstance(task, dict):
        return "任务状态未知。"
    status = str(task.get("status") or "").strip().lower()
    status_text = {
        "queued": "任务已进入队列，等待后台执行。",
        "running": "任务正在后台执行，请稍候。",
        "done": "任务已完成，请查看最新结果消息。",
        "failed": "任务执行失败，请查看最新结果消息。",
        "blocked": "任务已阻塞，请查看最新结果消息。",
    }.get(status, "任务状态未知。")
    parts = [status_text]
    detail = str(task.get("detail") or "").strip()
    updated_at = str(task.get("updated_at") or "").strip()
    task_identifier = str(task.get("task_identifier") or "").strip() or _build_home_task_identifier(task)
    if detail and detail != status_text:
        parts.append(detail)
    if updated_at:
        parts.append(f"最近更新：{updated_at}")
    if task_identifier:
        parts.append(f"任务标识：{task_identifier}")
    return "\n".join(parts)


def _claim_home_action_task(
    *,
    workspace: Path,
    chat_id: str,
    action: str,
    value: str,
    profile: str,
    username: str,
) -> Dict[str, Any]:
    path = _action_queue_path(workspace)
    lock_dir = _acquire_file_lock(path, timeout_seconds=10)
    try:
        queue = _load_action_queue(path)
        tasks = queue.setdefault("tasks", {})
        now = time.time()
        normalized_value = _normalize_home_action_value(action, value)
        created_at_text = _now_text()
        task_key = _build_home_action_key(chat_id=chat_id, action=action, value=value, profile=profile)
        existing = tasks.get(task_key)
        if isinstance(existing, dict):
            existing_status = str(existing.get("status") or "").strip().lower()
            updated_epoch = float(existing.get("updated_epoch") or 0.0)
            is_stale = existing_status in HOME_ACTION_ACTIVE_STATUSES and updated_epoch > 0 and (
                now - updated_epoch
            ) > float(DEFAULT_ACTION_QUEUE_STALE_SECONDS)
            if is_stale:
                existing["status"] = "failed"
                existing["detail"] = "历史任务超过等待阈值，已自动失效。"
                existing["updated_at"] = _now_text()
                existing["updated_epoch"] = now
            elif existing_status in HOME_ACTION_ACTIVE_STATUSES:
                _save_action_queue(path, queue)
                return {"accepted": False, "task_key": task_key, "task": dict(existing)}

        task = {
            "task_key": task_key,
            "chat_id": str(chat_id or "").strip(),
            "action": str(action or "").strip().lower(),
            "value": normalized_value,
            "profile": _normalize_profile_name(profile),
            "username": str(username or "").strip(),
            "status": "queued",
            "created_at": created_at_text,
            "updated_at": created_at_text,
            "created_epoch": now,
            "updated_epoch": now,
            "pid": 0,
            "log_path": "",
            "detail": "任务已进入队列，等待后台执行。",
        }
        task["task_identifier"] = _build_task_identifier(
            action=str(task.get("action") or ""),
            value=str(task.get("value") or ""),
            updated_at=created_at_text,
        )
        tasks[task_key] = task
        _save_action_queue(path, queue)
        return {"accepted": True, "task_key": task_key, "task": dict(task)}
    finally:
        _release_file_lock(lock_dir)


def _update_home_action_task(
    workspace: Path,
    task_key: str,
    *,
    status: Optional[str] = None,
    detail: Optional[str] = None,
    log_path: Optional[str] = None,
    pid: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    path = _action_queue_path(workspace)
    lock_dir = _acquire_file_lock(path, timeout_seconds=10)
    try:
        queue = _load_action_queue(path)
        tasks = queue.setdefault("tasks", {})
        task = tasks.get(task_key)
        if not isinstance(task, dict):
            task = {"task_key": str(task_key or "").strip()}
            tasks[task_key] = task
        if status is not None:
            task["status"] = str(status or "").strip().lower()
        if detail is not None:
            task["detail"] = str(detail or "").strip()
        if log_path is not None:
            task["log_path"] = str(log_path or "").strip()
        if pid is not None:
            try:
                task["pid"] = int(pid)
            except Exception:
                task["pid"] = 0
        if isinstance(extra, dict):
            task.update(extra)
        task["updated_at"] = _now_text()
        task["updated_epoch"] = time.time()
        task["task_identifier"] = _build_home_task_identifier(task)
        _save_action_queue(path, queue)
        return dict(task)
    finally:
        _release_file_lock(lock_dir)


def _prune_home_action_tasks(workspace: Path) -> int:
    path = _action_queue_path(workspace)
    lock_dir = _acquire_file_lock(path, timeout_seconds=10)
    removed = 0
    try:
        queue = _load_action_queue(path)
        tasks = queue.setdefault("tasks", {})
        now = time.time()
        removable: list[str] = []
        for task_key, task in list(tasks.items()):
            if not isinstance(task, dict):
                removable.append(str(task_key or "").strip())
                continue
            status = str(task.get("status") or "").strip().lower()
            updated_epoch = float(task.get("updated_epoch") or 0.0)
            age_seconds = (now - updated_epoch) if updated_epoch > 0 else float("inf")
            pid = int(task.get("pid") or 0)
            if status in HOME_ACTION_TERMINAL_STATUSES and age_seconds > float(DEFAULT_ACTION_QUEUE_TERMINAL_RETENTION_SECONDS):
                removable.append(str(task_key or "").strip())
                continue
            if status in HOME_ACTION_ACTIVE_STATUSES and age_seconds > float(DEFAULT_ACTION_QUEUE_STALE_SECONDS):
                if pid <= 0 or not _pid_is_running(pid):
                    removable.append(str(task_key or "").strip())
                    continue
        if len(tasks) - len(removable) > int(DEFAULT_ACTION_QUEUE_MAX_TASKS):
            terminal_candidates: list[tuple[float, str]] = []
            for task_key, task in list(tasks.items()):
                if not isinstance(task, dict):
                    continue
                if str(task.get("status") or "").strip().lower() not in HOME_ACTION_TERMINAL_STATUSES:
                    continue
                updated_epoch = float(task.get("updated_epoch") or 0.0)
                terminal_candidates.append((updated_epoch, str(task_key or "").strip()))
            terminal_candidates.sort(key=lambda item: item[0])
            overflow = max(0, (len(tasks) - len(removable)) - int(DEFAULT_ACTION_QUEUE_MAX_TASKS))
            removable.extend(task_key for _, task_key in terminal_candidates[:overflow])
        for task_key in dict.fromkeys(removable):
            if task_key in tasks:
                tasks.pop(task_key, None)
                removed += 1
        if removed > 0:
            _save_action_queue(path, queue)
    finally:
        _release_file_lock(lock_dir)
    return removed


def _home_action_log_stale(workspace: Path, log_path: str, *, stale_seconds: float) -> bool:
    resolved = _resolve_process_log_path(workspace, log_path)
    if resolved is None or (not resolved.exists()):
        return False
    try:
        age_seconds = max(0.0, time.time() - float(resolved.stat().st_mtime))
    except Exception:
        return False
    return age_seconds > float(stale_seconds)


def _terminate_pid_best_effort(pid: int, *, log_file: Optional[Path] = None) -> bool:
    clean_pid = int(pid or 0)
    if clean_pid <= 0:
        return False
    try:
        os.kill(clean_pid, 9)
        return True
    except Exception as exc:
        if isinstance(log_file, Path):
            _append_log(log_file, f"[Worker] terminate pid warning pid={clean_pid} error={exc}")
        return False


def _recover_orphaned_home_action_tasks(
    *,
    workspace: Path,
    bot_token: str,
    timeout_seconds: int,
    log_file: Path,
) -> int:
    path = _action_queue_path(workspace)
    lock_dir = _acquire_file_lock(path, timeout_seconds=10)
    recovered: list[Dict[str, Any]] = []
    try:
        queue = _load_action_queue(path)
        tasks = queue.setdefault("tasks", {})
        now_text = _now_text()
        now_epoch = time.time()
        changed = False
        for task_key, task in list(tasks.items()):
            if not isinstance(task, dict):
                continue
            status = str(task.get("status") or "").strip().lower()
            if status not in HOME_ACTION_ACTIVE_STATUSES:
                continue
            pid = int(task.get("pid") or 0)
            updated_epoch = float(task.get("updated_epoch") or 0.0)
            age_seconds = (now_epoch - updated_epoch) if updated_epoch > 0 else float("inf")
            process_alive = pid > 0 and _pid_is_running(pid)
            stale_timeout = float(DEFAULT_ACTION_QUEUE_STALE_SECONDS)
            log_stale = _home_action_log_stale(
                workspace,
                str(task.get("log_path") or "").strip(),
                stale_seconds=stale_timeout,
            )
            orphaned = pid <= 0 or not process_alive
            zombie = process_alive and age_seconds > stale_timeout and log_stale
            if not orphaned and not zombie:
                continue
            action = str(task.get("action") or "").strip().lower()
            if zombie:
                _terminate_pid_best_effort(pid, log_file=log_file)
            task["status"] = "blocked"
            task["detail"] = (
                "后台进程已退出，但当前卡片回传中断。请查看最近结果消息；如仍无结果，请重新发起一次。"
                if orphaned
                else "后台任务超过30分钟无进展，已按失活回收。如需继续，请重新发起一次。"
            )
            task["updated_at"] = now_text
            task["updated_epoch"] = now_epoch
            changed = True
            recovered.append(
                {
                    "task_key": str(task_key or "").strip(),
                    "action": action,
                    "value": str(task.get("value") or "").strip(),
                    "chat_id": str(task.get("chat_id") or "").strip(),
                    "profile": str(task.get("profile") or "").strip(),
                    "loading_message_id": int(task.get("loading_message_id") or 0),
                    "detail": str(task.get("detail") or "").strip(),
                    "log_path": str(task.get("log_path") or "").strip(),
                    "updated_at": str(task.get("updated_at") or "").strip(),
                    "recovery_reason": "zombie" if zombie else "orphaned",
                }
            )
        if changed:
            _save_action_queue(path, queue)
    finally:
        _release_file_lock(lock_dir)

    for task in recovered:
        chat_id = str(task.get("chat_id") or "").strip()
        loading_message_id = int(task.get("loading_message_id") or 0)
        should_notify = loading_message_id > 0
        if bot_token and chat_id and loading_message_id > 0:
            deleted = _try_delete_telegram_message(
                bot_token=bot_token,
                chat_id=chat_id,
                message_id=loading_message_id,
                timeout_seconds=max(10, int(timeout_seconds)),
                log_file=log_file,
            )
            if deleted:
                _update_home_action_task(workspace, str(task.get("task_key") or "").strip(), extra={"loading_message_id": 0})
        if bot_token and chat_id and should_notify:
            try:
                _send_card_message(
                    bot_token=bot_token,
                    chat_id=chat_id,
                    card=_home_feedback_response(
                        status="blocked",
                        title=_home_action_feedback_title(
                            str(task.get("action") or ""),
                            "blocked",
                            str(task.get("value") or ""),
                        ),
                        subtitle=f"当前配置：{_normalize_profile_name(str(task.get('profile') or DEFAULT_PROFILE))}",
                        detail=str(task.get("detail") or "").strip(),
                        menu_label=_menu_breadcrumb_for_action(
                            str(task.get("action") or ""),
                            str(task.get("value") or ""),
                        ),
                        task_identifier=_build_task_identifier(
                            action=str(task.get("action") or ""),
                            value=str(task.get("value") or ""),
                            log_path=str(task.get("log_path") or ""),
                            updated_at=str(task.get("updated_at") or ""),
                        ),
                    ),
                    timeout_seconds=max(10, int(timeout_seconds)),
                )
            except Exception as exc:
                _append_log(log_file, f"[Worker] orphaned home action notify failed: {exc}")
        _append_log(log_file, f"[Worker] orphaned home action recovered: task_key={task.get('task_key') or '-'}")
    return len(recovered)


def _should_reuse_home_message(*, workspace: Path, chat_id: str, force_new: bool) -> bool:
    if not bool(force_new):
        return True
    state = _load_state(_home_state_path(workspace))
    stored_chat_id = str(state.get("chat_id") or "").strip()
    updated_at = str(state.get("updated_at") or "").strip()
    if stored_chat_id != str(chat_id or "").strip() or not updated_at:
        return False
    try:
        updated_ts = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return False
    return (time.time() - updated_ts) <= float(DEFAULT_HOME_FORCE_NEW_DEBOUNCE_SECONDS)


def _prefilter_queue_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_TELEGRAM_PREFILTER_QUEUE_FILE).resolve()


def _review_state_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_REVIEW_STATE_FILE).resolve()


def _prefilter_feedback_history_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_TELEGRAM_PREFILTER_FEEDBACK_HISTORY_FILE).resolve()


def _pending_background_feedback_path(workspace: Path) -> Path:
    return (workspace / DEFAULT_PENDING_BACKGROUND_FEEDBACK_FILE).resolve()


def _platform_result_event_dir(workspace: Path) -> Path:
    return (workspace / DEFAULT_PLATFORM_RESULT_EVENT_DIR).resolve()


def _pipeline_priority_request_dir(workspace: Path) -> Path:
    return (workspace / DEFAULT_PIPELINE_PRIORITY_REQUEST_DIR).resolve()


def _register_pipeline_priority_request(*, workspace: Path, item_id: str, source: str) -> Path:
    request_dir = _pipeline_priority_request_dir(workspace)
    request_dir.mkdir(parents=True, exist_ok=True)
    token = hashlib.sha1(f"{source}:{item_id}:{os.getpid()}".encode("utf-8")).hexdigest()[:20]
    path = request_dir / f"{token}.json"
    _atomic_write_json(
        path,
        {
            "version": 1,
            "pid": int(os.getpid()),
            "item_id": str(item_id or "").strip(),
            "source": str(source or "").strip(),
            "priority": "high",
            "created_at": _now_text(),
            "updated_at": _now_text(),
        },
    )
    return path


def _clear_pipeline_priority_request(path: Optional[Path]) -> None:
    if not isinstance(path, Path):
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _load_pending_background_feedback(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": "", "items": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "updated_at": "", "items": []}
    if not isinstance(payload, dict):
        return {"version": 1, "updated_at": "", "items": []}
    items = payload.get("items", [])
    if not isinstance(items, list):
        items = []
    payload["items"] = [row for row in items if isinstance(row, dict)]
    return payload


def _save_pending_background_feedback(path: Path, payload: Dict[str, Any]) -> None:
    data = dict(payload if isinstance(payload, dict) else {})
    items = data.get("items", [])
    if not isinstance(items, list):
        items = []
    data["items"] = [row for row in items if isinstance(row, dict)]
    data["version"] = int(data.get("version", 1) or 1)
    data["updated_at"] = _now_text()
    _atomic_write_json(path, data)


def _enqueue_pending_background_feedback(
    *,
    workspace: Path,
    title: str,
    subtitle: str,
    sections: list[dict[str, Any]],
    status: str,
) -> None:
    path = _pending_background_feedback_path(workspace)
    payload = _load_pending_background_feedback(path)
    items = payload.get("items", [])
    if not isinstance(items, list):
        items = []
    items.append(
        {
            "id": f"bgfb-{int(time.time() * 1000)}-{os.getpid()}",
            "title": str(title or "").strip(),
            "subtitle": str(subtitle or "").strip(),
            "sections": sections if isinstance(sections, list) else [],
            "status": str(status or "").strip(),
            "created_at": _now_text(),
        }
    )
    payload["items"] = items[-50:]
    _save_pending_background_feedback(path, payload)


def _enqueue_platform_result_event(
    *,
    workspace: Path,
    item_id: str,
    platform: str,
    updates: Dict[str, Any],
) -> Path:
    event_dir = _platform_result_event_dir(workspace)
    event_dir.mkdir(parents=True, exist_ok=True)
    platform_token = str(platform or "").strip().lower()
    event_id = f"pfrevt-{time.time_ns()}-{os.getpid()}-{platform_token}"
    token = hashlib.sha1(f"{event_id}:{item_id}".encode("utf-8")).hexdigest()[:12]
    path = event_dir / f"{time.time_ns():020d}-{os.getpid()}-{platform_token}-{token}.json"
    payload = {
        "version": 1,
        "id": event_id,
        "item_id": str(item_id or "").strip(),
        "platform": platform_token,
        "updates": dict(updates if isinstance(updates, dict) else {}),
        "pid": int(os.getpid()),
        "created_at": _now_text(),
    }
    _atomic_write_json(path, payload)
    return path


def _load_platform_result_event(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    updates = payload.get("updates", {})
    if not isinstance(updates, dict):
        updates = {}
    payload["updates"] = updates
    payload["item_id"] = str(payload.get("item_id") or "").strip()
    payload["platform"] = str(payload.get("platform") or "").strip().lower()
    return payload


def _flush_pending_platform_result_events(
    *,
    workspace: Path,
    bot_token: str,
    chat_id: str,
    timeout_seconds: int,
    log_file: Path,
    runner: Any = None,
    email_settings: Any = None,
) -> int:
    event_dir = _platform_result_event_dir(workspace)
    if not event_dir.exists():
        return 0
    event_paths = sorted(path for path in event_dir.glob("*.json") if path.is_file())
    if not event_paths:
        return 0

    loaded_runner = runner
    loaded_email_settings = email_settings
    processed = 0
    for path in event_paths:
        payload = _load_platform_result_event(path)
        item_id = str(payload.get("item_id") or "").strip()
        platform = str(payload.get("platform") or "").strip().lower()
        updates = payload.get("updates", {})
        if not item_id or not platform or not isinstance(updates, dict):
            _append_log(log_file, f"[Worker] invalid platform result event dropped: {path}")
            with contextlib.suppress(Exception):
                path.unlink()
            continue
        current_item = _get_prefilter_item(workspace, item_id)
        if not current_item:
            _append_log(log_file, f"[Worker] stale platform result event dropped: item={item_id} platform={platform}")
            with contextlib.suppress(Exception):
                path.unlink()
            continue
        status_token = str(updates.get("status") or "").strip().lower()
        if status_token == "failed" and _should_promote_wechat_unconfirmed_publish_to_success(
            workspace=workspace,
            item=current_item,
            platform=platform,
            error_text=str(updates.get("error") or ""),
        ):
            updates = dict(updates)
            updates.update(
                {
                    "status": "success",
                    "published_at": str(updates.get("published_at") or _now_text()),
                    "publish_id": str(updates.get("publish_id") or "").strip() or "wechat-unconfirmed-auto-confirm",
                    "error": "",
                    "failure_reason": "",
                    "failure_category": "",
                    "failure_suggestion": "",
                }
            )
            _append_log(log_file, f"[Worker] promoted unconfirmed wechat publish to success item={item_id}")
        try:
            merged = _merge_platform_result(
                workspace=workspace,
                item_id=item_id,
                platform=platform,
                updates=updates,
            )
            result = _normalize_platform_results(merged.get("platform_results")).get(platform, {})
            status = str((result if isinstance(result, dict) else {}).get("status") or updates.get("status") or "").strip().lower()
            if _platform_result_is_terminal(status) and str(bot_token or "").strip() and str(chat_id or "").strip():
                if loaded_runner is None or loaded_email_settings is None:
                    loaded_runner, _ = _load_runtime_modules()
                    args = _build_immediate_publish_args(
                        runner=loaded_runner,
                        workspace=workspace,
                        telegram_bot_token=bot_token,
                        telegram_chat_id=chat_id,
                    )
                    loaded_email_settings = loaded_runner._build_email_settings(args)
                _send_immediate_platform_feedback(
                    runner=loaded_runner,
                    email_settings=loaded_email_settings,
                    workspace=workspace,
                    item_id=item_id,
                    platform=platform,
                )
            with contextlib.suppress(Exception):
                path.unlink()
            processed += 1
        except Exception as exc:
            _append_log(
                log_file,
                f"[Worker] platform result event flush deferred item={item_id} platform={platform} error={exc}",
            )
    return processed


def _flush_pending_background_feedback(
    *,
    workspace: Path,
    bot_token: str,
    chat_id: str,
    timeout_seconds: int,
    log_file: Path,
) -> None:
    path = _pending_background_feedback_path(workspace)
    payload = _load_pending_background_feedback(path)
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return
    if not str(bot_token or "").strip() or not str(chat_id or "").strip():
        return

    remaining: list[dict[str, Any]] = []
    for row in items:
        title = _prefix_menu_title(str(row.get("title") or "").strip())
        subtitle = str(row.get("subtitle") or "").strip()
        sections = row.get("sections", [])
        if not isinstance(sections, list):
            sections = []
        sections = _normalize_task_log_sections(sections)
        status = str(row.get("status") or "").strip() or "success"
        if not _should_send_background_feedback(status):
            _append_log(log_file, f"[Worker] pending background feedback skipped: {title or '-'}")
            continue
        card = build_action_feedback(
            status=status,
            title=title,
            subtitle=subtitle,
            sections=sections,
            bot_name="CyberCar",
        )
        card["reply_markup"] = _with_home_button(card.get("reply_markup") if isinstance(card, dict) else None)
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "text": str(card.get("text") or "").strip(),
            "disable_web_page_preview": "false",
        }
        parse_mode = str(card.get("parse_mode") or "").strip()
        if parse_mode:
            params["parse_mode"] = parse_mode
        reply_markup = _outgoing_reply_markup(
            card.get("reply_markup") if isinstance(card.get("reply_markup"), dict) else None,
            for_edit=False,
        )
        if isinstance(reply_markup, dict):
            params["reply_markup"] = json.dumps(reply_markup, ensure_ascii=True)
        sent = False
        last_exc: Optional[Exception] = None
        for use_post in (True, False):
            try:
                _shared_call_telegram_api(
                    bot_token=bot_token,
                    method="sendMessage",
                    params=params,
                    timeout_seconds=max(8, int(timeout_seconds)),
                    use_post=use_post,
                    max_retries=4 if use_post else 2,
                )
                sent = True
                break
            except Exception as exc:  # pragma: no cover - network path
                last_exc = exc
        if not sent:
            remaining.append(row)
            if last_exc is not None:
                _append_log(log_file, f"[Worker] pending background feedback retry failed: {last_exc}")
        else:
            _append_log(log_file, f"[Worker] pending background feedback delivered: {title or '-'}")

    if remaining:
        payload["items"] = remaining
        _save_pending_background_feedback(path, payload)
    elif path.exists():
        try:
            path.unlink()
        except Exception:
            payload["items"] = []
            _save_pending_background_feedback(path, payload)


def _pending_prefilter_retry_candidates(workspace: Path) -> list[dict[str, Any]]:
    queue = _load_prefilter_queue(_prefilter_queue_path(workspace))
    items = queue.get("items", {})
    if not isinstance(items, dict):
        return []
    now = time.time()
    candidates: list[dict[str, Any]] = []
    for item_id, row in items.items():
        if not isinstance(row, dict):
            continue
        workflow = str(row.get("workflow") or "").strip().lower()
        if workflow not in {"immediate_manual_publish", IMMEDIATE_COLLECT_REVIEW_WORKFLOW}:
            continue
        if int(row.get("message_id") or 0) > 0:
            continue
        if not bool(row.get("prefilter_retry_pending")) and str(row.get("status") or "").strip().lower() != "send_failed":
            continue
        try:
            retry_count = int(row.get("prefilter_retry_count") or 0)
        except Exception:
            retry_count = 0
        if retry_count >= int(DEFAULT_PENDING_PREFILTER_RETRY_MAX_ATTEMPTS):
            _update_prefilter_item(
                workspace,
                str(item_id),
                updates={
                    "prefilter_retry_pending": False,
                    "action": "send_retry_exhausted",
                },
            )
            continue
        try:
            last_retry_epoch = float(row.get("prefilter_last_retry_epoch") or 0.0)
        except Exception:
            last_retry_epoch = 0.0
        if last_retry_epoch > 0 and (now - last_retry_epoch) < float(DEFAULT_PENDING_PREFILTER_RETRY_COOLDOWN_SECONDS):
            continue
        payload = dict(row)
        payload["id"] = str(item_id)
        candidates.append(payload)
    candidates.sort(
        key=lambda row: (
            float(row.get("prefilter_last_retry_epoch") or 0.0),
            str(row.get("updated_at") or row.get("created_at") or ""),
            str(row.get("id") or ""),
        )
    )
    return candidates[: int(DEFAULT_PENDING_PREFILTER_RETRY_BATCH_SIZE)]


def _flush_pending_prefilter_retries(
    *,
    workspace: Path,
    bot_token: str,
    chat_id: str,
    timeout_seconds: int,
    log_file: Path,
) -> None:
    if not str(bot_token or "").strip() or not str(chat_id or "").strip():
        return
    _cleanup_prefilter_queue(workspace, log_file=log_file)
    candidates = _pending_prefilter_retry_candidates(workspace)
    if not candidates:
        return
    runner, core = _load_runtime_modules()
    args = _build_immediate_publish_args(
        runner=runner,
        workspace=workspace,
        telegram_bot_token=bot_token,
        telegram_chat_id=chat_id,
    )
    email_settings = runner._build_email_settings(args)
    workspace_ctx = core.init_workspace(str(workspace))
    for item in candidates:
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        try:
            retry_count = int(item.get("prefilter_retry_count") or 0) + 1
        except Exception:
            retry_count = 1
        now_epoch = time.time()
        _update_prefilter_item(
            workspace,
            item_id,
            updates={
                "prefilter_retry_pending": True,
                "prefilter_retry_count": retry_count,
                "prefilter_last_retry_epoch": now_epoch,
                "prefilter_last_retry_at": _now_text(),
                "action": "send_retry_running",
            },
        )
        try:
            response = _send_immediate_candidate_prefilter_card(
                runner=runner,
                email_settings=email_settings,
                workspace=workspace,
                item=item,
                workspace_ctx=workspace_ctx,
                fast_send=False,
            )
            result_payload = response.get("result") if isinstance(response, dict) else {}
            if not isinstance(result_payload, dict):
                result_payload = {}
            chat_payload = result_payload.get("chat") if isinstance(result_payload.get("chat"), dict) else {}
            message_id = int(result_payload.get("message_id") or 0)
            if message_id <= 0:
                raise RuntimeError("telegram candidate prefilter message_id missing")
            _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "link_pending",
                    "message_id": message_id,
                    "chat_id": str(chat_payload.get("id") or chat_id or ""),
                    "action": "resent",
                    "last_error": "",
                    "prefilter_retry_pending": False,
                    "prefilter_last_retry_epoch": now_epoch,
                    "prefilter_last_retry_at": _now_text(),
                },
            )
            _append_log(log_file, f"[Worker] pending prefilter delivered: {item_id}")
        except Exception as exc:
            retryable = retry_count < int(DEFAULT_PENDING_PREFILTER_RETRY_MAX_ATTEMPTS)
            _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "send_failed",
                    "action": "send_retry_failed" if retryable else "send_retry_exhausted",
                    "last_error": str(exc),
                    "prefilter_retry_pending": retryable,
                    "prefilter_last_retry_epoch": now_epoch,
                    "prefilter_last_retry_at": _now_text(),
                },
            )
            _append_log(
                log_file,
                f"[Worker] pending prefilter retry failed: {item_id} ({exc})",
            )


def _load_prefilter_queue(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": "", "items": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "updated_at": "", "items": {}}
    if not isinstance(payload, dict):
        return {"version": 1, "updated_at": "", "items": {}}
    raw_items = payload.get("items", {})
    items: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_items, dict):
        for item_id, row in raw_items.items():
            key = str(item_id or "").strip()
            if not key or not isinstance(row, dict):
                continue
            normalized = dict(row)
            normalized["id"] = key
            items[key] = normalized
    elif isinstance(raw_items, list):
        for row in raw_items:
            if not isinstance(row, dict):
                continue
            key = str(row.get("id", "") or "").strip()
            if not key:
                continue
            normalized = dict(row)
            normalized["id"] = key
            items[key] = normalized
    return {
        "version": int(payload.get("version", 1) or 1),
        "updated_at": str(payload.get("updated_at", "") or ""),
        "items": items,
    }


def _save_prefilter_queue(path: Path, queue: Dict[str, Any]) -> None:
    payload = dict(queue if isinstance(queue, dict) else {})
    items = payload.get("items", {})
    if not isinstance(items, dict):
        items = {}
    payload["items"] = items
    payload["version"] = int(payload.get("version", 1) or 1)
    payload["updated_at"] = _now_text()
    _atomic_write_json(path, payload)


def _prefilter_item_timestamp(row: Dict[str, Any]) -> Optional[datetime]:
    if not isinstance(row, dict):
        return None
    for key in ("updated_at", "created_at", "prefilter_last_retry_at"):
        parsed = _parse_worker_time_text(str(row.get(key) or ""))
        if parsed is not None:
            return parsed
    return None


def _is_prefilter_item_polluted(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return True
    workflow = str(row.get("workflow") or "").strip().lower()
    if workflow not in {"immediate_manual_publish", IMMEDIATE_COLLECT_REVIEW_WORKFLOW}:
        return False
    media_kind = str(row.get("media_kind") or "").strip().lower()
    if media_kind and media_kind not in {"video", "image"}:
        return True
    source_url = str(row.get("source_url") or "").strip()
    video_name = str(row.get("video_name") or "").strip()
    processed_name = str(row.get("processed_name") or "").strip()
    if source_url or video_name or processed_name:
        return False
    if _normalize_platform_results(row.get("platform_results")):
        return False
    try:
        message_id = int(row.get("message_id") or 0)
    except Exception:
        message_id = 0
    if message_id > 0:
        return False
    return True


def _prune_prefilter_queue(queue: Dict[str, Any]) -> Dict[str, int]:
    items = queue.get("items", {})
    if not isinstance(items, dict) or not items:
        return {"removed_terminal": 0, "removed_polluted": 0}
    cutoff = datetime.now() - timedelta(seconds=max(3600, int(DEFAULT_PREFILTER_QUEUE_TERMINAL_RETENTION_SECONDS)))
    removed_terminal = 0
    removed_polluted = 0
    for item_id, row in list(items.items()):
        if not isinstance(row, dict):
            del items[item_id]
            removed_polluted += 1
            continue
        if _is_prefilter_item_polluted(row):
            del items[item_id]
            removed_polluted += 1
            continue
        status = str(row.get("status") or "").strip().lower()
        if status not in {
            "up_confirmed",
            "down_confirmed",
            "expired_pending",
            "publish_partial",
            "publish_done",
            "publish_failed",
            "send_failed",
        }:
            continue
        ts = _prefilter_item_timestamp(row)
        if ts is not None and ts < cutoff:
            del items[item_id]
            removed_terminal += 1
    return {"removed_terminal": removed_terminal, "removed_polluted": removed_polluted}


def _cleanup_prefilter_queue(workspace: Path, *, log_file: Optional[Path] = None) -> Dict[str, int]:
    summary = _with_prefilter_queue_lock(workspace, _prune_prefilter_queue)
    if not isinstance(summary, dict):
        return {"removed_terminal": 0, "removed_polluted": 0}
    removed_terminal = int(summary.get("removed_terminal") or 0)
    removed_polluted = int(summary.get("removed_polluted") or 0)
    if (removed_terminal > 0 or removed_polluted > 0) and isinstance(log_file, Path):
        _append_log(
            log_file,
            (
                "[Worker] prefilter queue cleanup: "
                f"removed_terminal={removed_terminal}, removed_polluted={removed_polluted}"
            ),
        )
    return {"removed_terminal": removed_terminal, "removed_polluted": removed_polluted}


def _prune_inactive_prefilter_items_for_manual_cleanup(queue: Dict[str, Any]) -> Dict[str, Any]:
    items = queue.get("items", {})
    if not isinstance(items, dict) or not items:
        return {"removed_inactive": 0, "removed_ids": [], "filter_synced": 0}
    removed_ids: list[str] = []
    filter_synced = 0
    for item_id, row in list(items.items()):
        if not isinstance(row, dict):
            continue
        if bool(row.get("prefilter_retry_pending")):
            continue
        status = str(row.get("status") or "").strip().lower()
        should_sync_filter = False
        if _is_prefilter_overflow_review_item(row):
            should_sync_filter = True
            del items[item_id]
            removed_ids.append(str(item_id or "").strip())
            if _record_prefilter_source_in_collect_ledger(
                workspace=Path(queue.get("_workspace_path") or ""),
                source_url=str(row.get("source_url") or "").strip(),
                media_kind=str(row.get("media_kind") or "video"),
                processed_name=str(row.get("processed_name") or row.get("video_name") or "").strip(),
                state="review_skipped",
            ):
                filter_synced += 1
            continue
        if status in {"link_pending", "up_confirmed", "down_confirmed", "expired_pending", "send_failed"}:
            should_sync_filter = status in {"link_pending", "up_confirmed", "down_confirmed", "expired_pending"}
            del items[item_id]
            removed_ids.append(str(item_id or "").strip())
            if should_sync_filter and _record_prefilter_source_in_collect_ledger(
                workspace=Path(queue.get("_workspace_path") or ""),
                source_url=str(row.get("source_url") or "").strip(),
                media_kind=str(row.get("media_kind") or "video"),
                processed_name=str(row.get("processed_name") or row.get("video_name") or "").strip(),
                state="review_skipped",
            ):
                filter_synced += 1
            continue
        if _is_prefilter_live_for_process_status(row):
            continue
        del items[item_id]
        removed_ids.append(str(item_id or "").strip())
    return {
        "removed_inactive": len(removed_ids),
        "removed_ids": removed_ids[: max(1, DEFAULT_PROCESS_STATUS_PREFILTER_LIMIT)],
        "filter_synced": filter_synced,
    }


def _cleanup_inactive_prefilter_items(workspace: Path, *, log_file: Optional[Path] = None) -> Dict[str, Any]:
    def _mutate(queue: Dict[str, Any]) -> Dict[str, Any]:
        queue["_workspace_path"] = str(workspace)
        try:
            return _prune_inactive_prefilter_items_for_manual_cleanup(queue)
        finally:
            queue.pop("_workspace_path", None)

    summary = _with_prefilter_queue_lock(workspace, _mutate)
    if not isinstance(summary, dict):
        return {"removed_inactive": 0, "removed_ids": [], "filter_synced": 0}
    removed_inactive = int(summary.get("removed_inactive") or 0)
    filter_synced = int(summary.get("filter_synced") or 0)
    removed_ids = [
        str(token or "").strip()
        for token in list(summary.get("removed_ids") or [])
        if str(token or "").strip()
    ]
    if removed_inactive > 0 and isinstance(log_file, Path):
        preview = ", ".join(removed_ids)
        suffix = f" ids={preview}" if preview else ""
        _append_log(
            log_file,
            f"[Worker] manual prefilter cleanup: removed_inactive={removed_inactive}, filter_synced={filter_synced}{suffix}",
        )
    return {"removed_inactive": removed_inactive, "removed_ids": removed_ids, "filter_synced": filter_synced}


def _run_periodic_queue_maintenance(
    workspace: Path,
    *,
    log_file: Path,
    last_run_epoch: float = 0.0,
    interval_seconds: int = DEFAULT_QUEUE_MAINTENANCE_INTERVAL_SECONDS,
    force: bool = False,
) -> Dict[str, Any]:
    now_epoch = time.time()
    interval = max(30, int(interval_seconds or DEFAULT_QUEUE_MAINTENANCE_INTERVAL_SECONDS))
    if not force and last_run_epoch > 0 and (now_epoch - float(last_run_epoch)) < interval:
        return {"ran": False, "last_run_epoch": float(last_run_epoch)}

    pruned_tasks = _prune_home_action_tasks(workspace)
    if pruned_tasks > 0:
        _append_log(log_file, f"[Worker] home action tasks pruned: {pruned_tasks}")
    prefilter_summary = _cleanup_prefilter_queue(workspace, log_file=log_file)
    return {
        "ran": True,
        "last_run_epoch": now_epoch,
        "pruned_tasks": int(pruned_tasks or 0),
        "prefilter_removed_terminal": int(prefilter_summary.get("removed_terminal") or 0),
        "prefilter_removed_polluted": int(prefilter_summary.get("removed_polluted") or 0),
        "ran_at": _now_text(),
    }


def _expire_stale_link_pending_prefilter_items(
    workspace: Path,
    queue: Dict[str, Any],
) -> Dict[str, int]:
    items = queue.get("items", {})
    if not isinstance(items, dict) or not items:
        return {"expired_pending": 0, "ledger_synced": 0}
    expired_pending = 0
    ledger_synced = 0
    for item_id, row in list(items.items()):
        if not isinstance(row, dict):
            continue
        if not _is_stale_link_pending_prefilter_item(row):
            continue
        updated_row = dict(row)
        updated_row["status"] = "expired_pending"
        updated_row["action"] = "expired"
        updated_row["expired_at"] = _now_text()
        updated_row["prefilter_warning"] = "预审卡片超过 10 分钟未处理，已自动过期并纳入后续过滤。"
        updated_row["updated_at"] = _now_text()
        items[item_id] = updated_row
        expired_pending += 1
        if _record_prefilter_source_in_collect_ledger(
            workspace=workspace,
            source_url=str(updated_row.get("source_url") or "").strip(),
            media_kind=str(updated_row.get("media_kind") or "video"),
            processed_name=str(updated_row.get("processed_name") or updated_row.get("video_name") or "").strip(),
            state="review_skipped",
        ):
            ledger_synced += 1
    return {"expired_pending": expired_pending, "ledger_synced": ledger_synced}


def _with_prefilter_queue_lock(
    workspace: Path,
    callback: Callable[[Dict[str, Any]], Any],
    *,
    timeout_seconds: float = DEFAULT_PREFILTER_QUEUE_LOCK_TIMEOUT_SECONDS,
) -> Any:
    queue_path = _prefilter_queue_path(workspace)
    lock_dir = _acquire_file_lock(queue_path, timeout_seconds=timeout_seconds)
    try:
        queue = _load_prefilter_queue(queue_path)
        _expire_stale_link_pending_prefilter_items(workspace, queue)
        result = callback(queue)
        _save_prefilter_queue(queue_path, queue)
        return result
    finally:
        _release_file_lock(lock_dir)


def _platform_lock_path(workspace: Path, platform: str) -> Path:
    token = str(platform or "").strip().lower() or "unknown"
    return (workspace / "runtime" / "platform_publish_locks" / f"{token}.lockdir").resolve()


def _with_platform_lock(
    workspace: Path,
    platform: str,
    callback: Callable[[], Any],
    *,
    timeout_seconds: float = DEFAULT_PLATFORM_LOCK_TIMEOUT_SECONDS,
) -> Any:
    lock_path = _platform_lock_path(workspace, platform)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_dir = _acquire_file_lock(lock_path, timeout_seconds=timeout_seconds)
    try:
        return callback()
    finally:
        _release_file_lock(lock_dir)


def _load_review_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": "", "items": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "updated_at": "", "items": {}}
    if not isinstance(payload, dict):
        return {"version": 1, "updated_at": "", "items": {}}
    items = payload.get("items", {})
    if not isinstance(items, dict):
        items = {}
    payload["items"] = items
    return payload


def _save_review_state(path: Path, state: Dict[str, Any]) -> None:
    payload = dict(state if isinstance(state, dict) else {})
    items = payload.get("items", {})
    if not isinstance(items, dict):
        items = {}
    payload["items"] = items
    payload["version"] = int(payload.get("version", 1) or 1)
    payload["updated_at"] = _now_text()
    _atomic_write_json(path, payload)


def _normalize_review_media_kind(media_kind: str) -> str:
    return _normalize_immediate_collect_media_kind(media_kind or "video")


def _build_immediate_candidate_item_id(source_url: str, published_at: str, media_kind: str = "video") -> str:
    raw = "|".join(
        [
            str(source_url or "").strip(),
            str(published_at or "").strip(),
            "immediate_manual_publish",
            _normalize_immediate_collect_media_kind(media_kind),
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"ctim-{digest}"


def _resolve_item_target_platforms(item: Dict[str, Any]) -> list[str]:
    token = str(item.get("target_platforms") or "").strip()
    if token:
        return _resolve_platforms_expr(token) or _collect_publish_target_platforms(str(item.get("media_kind") or "video"))
    return _collect_publish_target_platforms(str(item.get("media_kind") or "video"))


def _normalize_platform_results(raw: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, value in raw.items():
        platform = str(key or "").strip().lower()
        if not platform:
            continue
        normalized[platform] = dict(value) if isinstance(value, dict) else {"status": str(value or "").strip().lower()}
    return normalized


def _summarize_platform_results(item: Dict[str, Any]) -> Dict[str, Any]:
    results = _normalize_platform_results(item.get("platform_results"))
    statuses = [str(row.get("status") or "").strip().lower() for row in results.values()]
    success_count = len([status for status in statuses if status in {"success", "skipped_duplicate"}])
    failed_count = len([status for status in statuses if status in {"failed", "login_required"}])
    running = any(status in {"queued", "running"} for status in statuses)
    if statuses:
        if running:
            status = "publish_running"
        elif success_count == len(statuses):
            status = "publish_done"
        elif success_count > 0:
            status = "publish_partial"
        else:
            status = "publish_failed"
    else:
        status = str(item.get("status") or "").strip().lower() or "publish_requested"
    return {
        "status": status,
        "publish_success_count": success_count,
        "publish_failed_count": failed_count,
        "platform_results": results,
    }


def _platform_result_is_active(status: str) -> bool:
    return str(status or "").strip().lower() in {"queued", "running"}


def _recover_platform_result_from_log(workspace: Path, platform: str, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    log_path = _resolve_process_log_path(workspace, str(result.get("log_path") or "").strip())
    if log_path is None or not log_path.exists():
        return None
    try:
        tail = log_path.read_text(encoding="utf-8", errors="ignore")[-16000:]
    except Exception:
        return None
    platform_token = str(platform or "").strip().lower()
    success_line = ""
    for line in reversed(tail.splitlines()):
        text = str(line or "").strip()
        if not text:
            continue
        if f"[Success:{platform_token}]" in text:
            success_line = text
            break
    if not success_line:
        return None
    published_at = ""
    matched = re.search(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", success_line)
    if matched:
        published_at = str(matched.group(1) or "").strip()[-8:]
    updates = dict(result)
    updates["status"] = "success"
    updates["updated_at"] = _now_text()
    updates["published_at"] = published_at or str(updates.get("published_at") or "").strip() or _now_text()[-8:]
    updates["failure_reason"] = ""
    updates["failure_category"] = ""
    updates["failure_suggestion"] = ""
    updates.pop("error", None)
    return updates


def _recover_orphaned_immediate_candidate(
    *,
    workspace: Path,
    item_id: str,
    item: Dict[str, Any],
) -> Dict[str, Any]:
    row = dict(item) if isinstance(item, dict) else {}
    if not row:
        return {}
    status = str(row.get("status") or "").strip().lower()
    if status not in {"publish_requested", "publish_running"}:
        return row
    results = _normalize_platform_results(row.get("platform_results"))
    if not results:
        return row

    changed = False
    for platform, result in list(results.items()):
        platform_status = str(result.get("status") or "").strip().lower()
        if not _platform_result_is_active(platform_status):
            continue
        pid = int(result.get("pid") or 0)
        if pid > 0 and _pid_is_running(pid):
            continue
        recovered = _recover_platform_result_from_log(workspace, platform, result)
        if isinstance(recovered, dict):
            results[platform] = recovered
            changed = True
            continue
        updated = dict(result)
        updated["status"] = "failed"
        updated["updated_at"] = _now_text()
        updated["error"] = "后台发布进程已退出，状态回传中断。请重新发起发布。"
        updated["failure_reason"] = str(updated.get("failure_reason") or updated["error"]).strip()
        updated["failure_category"] = str(updated.get("failure_category") or "worker_exit").strip()
        updated["failure_suggestion"] = str(updated.get("failure_suggestion") or "重新发起发布，若重复出现请继续排查。").strip()
        results[platform] = updated
        changed = True

    if not changed:
        return row

    row["platform_results"] = results
    row.update(_summarize_platform_results(row))
    row["updated_at"] = _now_text()
    return _update_prefilter_item(
        workspace,
        item_id,
        updates={
            "platform_results": row.get("platform_results"),
            "status": row.get("status"),
            "publish_success_count": row.get("publish_success_count"),
            "publish_failed_count": row.get("publish_failed_count"),
            "updated_at": row.get("updated_at"),
        },
    )


def _merge_platform_result(
    *,
    workspace: Path,
    item_id: str,
    platform: str,
    updates: Dict[str, Any],
) -> Dict[str, Any]:
    def _mutate(queue: Dict[str, Any]) -> Dict[str, Any]:
        items = queue.get("items", {})
        if not isinstance(items, dict):
            items = {}
            queue["items"] = items
        row = items.get(item_id, {})
        if not isinstance(row, dict):
            row = {"id": item_id}
            items[item_id] = row
        results = _normalize_platform_results(row.get("platform_results"))
        base = results.get(platform, {})
        if not isinstance(base, dict):
            base = {}
        merged = dict(base)
        merged.update({k: v for k, v in updates.items()})
        merged["updated_at"] = _now_text()
        results[platform] = merged
        row["platform_results"] = results
        row.update(_summarize_platform_results(row))
        row["updated_at"] = _now_text()
        return dict(row)

    return _with_prefilter_queue_lock(workspace, _mutate)


def _merge_platform_result_resilient(
    *,
    workspace: Path,
    item_id: str,
    platform: str,
    updates: Dict[str, Any],
    log_file: Optional[Path] = None,
) -> Dict[str, Any]:
    try:
        row = _merge_platform_result(
            workspace=workspace,
            item_id=item_id,
            platform=platform,
            updates=updates,
        )
        return {
            "item": dict(row) if isinstance(row, dict) else {},
            "queue_write_failed": False,
            "queue_write_error": "",
        }
    except Exception as exc:
        if isinstance(log_file, Path):
            _append_log(
                log_file,
                f"[Worker] platform result queue write failed platform={platform} item={item_id} error={exc}",
            )
        row = _get_prefilter_item(workspace, item_id)
        if not isinstance(row, dict):
            row = {}
        row = dict(row)
        if not row:
            row["id"] = item_id
        results = _normalize_platform_results(row.get("platform_results"))
        base = results.get(platform, {})
        if not isinstance(base, dict):
            base = {}
        merged = dict(base)
        merged.update({k: v for k, v in updates.items()})
        merged["updated_at"] = _now_text()
        results[platform] = merged
        row["platform_results"] = results
        row.update(_summarize_platform_results(row))
        row["updated_at"] = _now_text()
        return {
            "item": row,
            "queue_write_failed": True,
            "queue_write_error": str(exc),
        }


def _describe_platform_failure(platform: str, error_text: str) -> Dict[str, str]:
    raw = str(error_text or "").strip()
    if not raw:
        return {"reason": "", "category": "", "suggestion": "", "raw_signal": ""}
    try:
        _, core = _load_publish_runtime_modules()
        describer = getattr(core, "describe_publish_failure", None)
        if callable(describer):
            payload = describer(platform, raw)
            if isinstance(payload, dict) and any(payload.values()):
                return {
                    "reason": str(payload.get("reason") or "").strip(),
                    "category": str(payload.get("category") or "").strip(),
                    "suggestion": str(payload.get("suggestion") or "").strip(),
                    "raw_signal": str(payload.get("raw_signal") or raw).strip(),
                }
        classifier = getattr(core, "classify_publish_failure_reason", None)
        if callable(classifier):
            reason = str(classifier(platform, raw) or "").strip()
            if reason:
                return {"reason": reason, "category": "", "suggestion": "", "raw_signal": raw}
    except Exception:
        pass
    return {"reason": raw, "category": "", "suggestion": "", "raw_signal": raw}


def _failure_requires_login(failure: Dict[str, str], error_text: str) -> bool:
    reason = str((failure or {}).get("reason") or "").strip()
    category = str((failure or {}).get("category") or "").strip()
    raw = str(error_text or "").strip()
    if reason.startswith("未登录"):
        return True
    if "登录" in category:
        return True
    return "登录" in raw and "无需登录" not in raw


def _looks_like_explicit_login_gate_error(error_text: str) -> bool:
    raw = str(error_text or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    markers = (
        "publish blocked by login page",
        "blocked by login page",
        "login page",
        "login gate",
        "login.html",
        "needs_login",
        "scan qr",
        "scan code",
        "二维码",
        "扫码登录",
        "未登录",
        "重新登录",
        "登录失效",
    )
    return any(marker in lowered or marker in raw for marker in markers)


def _should_probe_platform_login_after_publish_failure(platform: str, error_text: str) -> bool:
    platform_token = str(platform or "").strip().lower()
    raw = str(error_text or "").strip().lower()
    if platform_token != "wechat":
        return False
    if not raw:
        return False
    if "timed out waiting for lock" in raw and "wechat.lockdir" in raw:
        return False
    return True


def _looks_like_wechat_unconfirmed_publish_failure(error_text: str) -> bool:
    raw = str(error_text or "").strip().lower()
    if not raw:
        return False
    if "e_publish_unconfirmed_draft_saved" in raw:
        return True
    return "wechat" in raw and "publish was not confirmed" in raw


def _resolve_platform_result_log_path_from_item(
    *,
    workspace: Path,
    item: Mapping[str, Any],
    platform: str,
) -> Optional[Path]:
    results = _normalize_platform_results(item.get("platform_results"))
    result = results.get(str(platform or "").strip().lower(), {})
    if not isinstance(result, dict):
        return None
    log_path = _resolve_process_log_path(workspace, str(result.get("log_path") or "").strip())
    if log_path is None or not log_path.exists():
        return None
    return log_path


def _should_promote_wechat_unconfirmed_publish_to_success(
    *,
    workspace: Path,
    item: Mapping[str, Any],
    platform: str,
    error_text: str,
) -> bool:
    platform_token = str(platform or "").strip().lower()
    if platform_token != "wechat":
        return False
    if not _looks_like_wechat_unconfirmed_publish_failure(error_text):
        return False
    if _looks_like_explicit_login_gate_error(error_text):
        return False
    log_path = _resolve_platform_result_log_path_from_item(workspace=workspace, item=item, platform=platform_token)
    if log_path is None:
        return False
    try:
        tail = log_path.read_text(encoding="utf-8", errors="ignore")[-24000:].lower()
    except Exception:
        return False
    submit_markers = (
        "clicked publish button by primary selector",
        "publish button click accepted",
        "clicked publish button",
    )
    if not any(marker in tail for marker in submit_markers):
        return False
    hard_failure_markers = (
        "publish button was not located",
        "login required persisted",
        "wechat page shows publish failure marker",
    )
    return not any(marker in tail for marker in hard_failure_markers)


def _upsert_immediate_candidate_item(
    *,
    workspace: Path,
    candidate: Dict[str, Any],
    profile: str,
    media_kind: str,
    target_platforms: str,
    chat_id: str,
    item_index: int,
    total_count: int,
    allow_reuse: bool = True,
) -> Dict[str, Any]:
    item_id = _build_immediate_candidate_item_id(
        str(candidate.get("url") or "").strip(),
        str(candidate.get("published_at") or "").strip(),
        media_kind,
    )

    def _mutate(queue: Dict[str, Any]) -> Dict[str, Any]:
        items = queue.get("items", {})
        if not isinstance(items, dict):
            items = {}
            queue["items"] = items
        existing = items.get(item_id, {})
        row = dict(existing) if isinstance(existing, dict) else {}
        existing_status = str(row.get("status") or "").strip().lower()
        existing_message_id = int(row.get("message_id") or 0)
        row["id"] = item_id
        row["workflow"] = "immediate_manual_publish"
        row["media_kind"] = _normalize_immediate_collect_media_kind(media_kind or row.get("media_kind") or "video")
        row["created_at"] = str(row.get("created_at") or _now_text())
        row["updated_at"] = _now_text()
        row["source_url"] = str(candidate.get("url") or row.get("source_url") or "").strip()
        row["source_platform"] = (
            _normalize_collect_source_platform(candidate.get("source_platform"))
            or _resolve_collect_source_platform_from_url(str(candidate.get("url") or ""))
            or _normalize_collect_source_platform(row.get("source_platform"))
        )
        row["published_at"] = str(candidate.get("published_at") or row.get("published_at") or "").strip()
        row["display_time"] = str(candidate.get("display_time") or row.get("display_time") or "").strip()
        row["tweet_text"] = str(candidate.get("tweet_text") or row.get("tweet_text") or "").strip()
        row["match_mode"] = str(candidate.get("match_mode") or row.get("match_mode") or "").strip()
        row["matched_keyword"] = str(candidate.get("matched_keyword") or row.get("matched_keyword") or "").strip()
        row["profile"] = str(profile or row.get("profile") or "").strip()
        row["target_platforms"] = str(target_platforms or row.get("target_platforms") or "").strip()
        row["candidate_index"] = int(item_index or 0)
        row["candidate_limit"] = int(total_count or 0)
        row["chat_id"] = str(chat_id or row.get("chat_id") or "").strip()
        existing_is_filtered_terminal = _is_prefilter_filtered_terminal(row)
        if existing_is_filtered_terminal:
            if _is_prefilter_expired_terminal(row):
                row["status"] = "expired_pending"
                row["action"] = "expired"
            else:
                row["status"] = "down_confirmed"
                row["action"] = "skip"
        elif existing_status == "publish_done":
            row["status"] = "publish_done"
            row["action"] = str(row.get("action") or "publish").strip() or "publish"
        elif not allow_reuse and existing_status in IMMEDIATE_CANDIDATE_REUSE_STATUSES:
            row["status"] = "link_pending"
            row["action"] = "resent_in_test_mode"
            row["message_id"] = 0
            row["platform_results"] = {}
            row["publish_success_count"] = 0
            row["publish_failed_count"] = 0
            row["prefilter_retry_pending"] = False
            row["prefilter_retry_count"] = 0
            row["prefilter_last_retry_epoch"] = 0.0
        elif existing_status not in IMMEDIATE_CANDIDATE_REUSE_STATUSES:
            row["status"] = "link_pending"
            row["prefilter_retry_pending"] = False
            row["prefilter_retry_count"] = 0
            row["prefilter_last_retry_epoch"] = 0.0
        items[item_id] = row
        return {
            "item": dict(row),
            "already_sent": bool(
                allow_reuse
                and
                existing_message_id > 0
                and existing_status in IMMEDIATE_CANDIDATE_REUSE_STATUSES
            ),
        }

    payload = _with_prefilter_queue_lock(workspace, _mutate)
    if not isinstance(payload, dict):
        return {"item_id": item_id, "item": {}, "already_sent": False}
    item = payload.get("item")
    if isinstance(item, dict):
        payload["item"] = _refresh_prefilter_item_coordination_snapshot(workspace, item_id, item)
    payload["item_id"] = item_id
    return payload


def _chat_allowed(
    *,
    chat_id: str,
    chat_type: str,
    allowed_chat_id: str,
    allow_private_chat_commands: bool,
) -> bool:
    if allowed_chat_id and chat_id != allowed_chat_id:
        if not (allow_private_chat_commands and chat_type == "private"):
            return False
    if (not allowed_chat_id) and allow_private_chat_commands and chat_type != "private":
        return False
    return True


def _parse_prefilter_callback_data(data: str) -> Optional[tuple[str, str]]:
    token = str(data or "").strip()
    if not token:
        return None
    parts = token.split("|")
    if len(parts) != 3:
        return None
    if parts[0] != TELEGRAM_PREFILTER_CALLBACK_PREFIX:
        return None
    action = str(parts[1] or "").strip().lower()
    item_id = str(parts[2] or "").strip()
    legacy_aliases = {
        "publish": "publish_normal",
        "publish_skip": "skip",
        "down": "skip",
    }
    action = legacy_aliases.get(action, action)
    if action not in {"up", "skip", "publish_normal", "publish_original", "retry_failed_publish"} or not item_id:
        return None
    return action, item_id


def _normalize_optional_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    token = str(value or "").strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _apply_coordination_snapshot(item: Dict[str, Any], workspace: Path) -> Dict[str, Any]:
    row = dict(item) if isinstance(item, dict) else {}
    source_url = str(row.get("source_url") or "").strip()
    processed_name = str(row.get("video_name") or row.get("processed_name") or "").strip()
    if not source_url and not processed_name:
        return row
    try:
        workspace_ctx = core.init_workspace(str(workspace))
        snapshot = core.build_content_coordination_snapshot(
            workspace_ctx,
            source_url=source_url,
            processed_name=processed_name,
            media_kind=str(row.get("media_kind") or "video"),
            platforms=_resolve_item_target_platforms(row),
        )
    except Exception:
        return row

    row["processed_name"] = str(snapshot.get("processed_name") or row.get("processed_name") or "").strip()
    row["media_kind"] = str(snapshot.get("media_kind") or row.get("media_kind") or "video").strip().lower()
    row["review_status"] = str(snapshot.get("review_status") or "").strip().lower()
    row["coordination_summary"] = str(snapshot.get("summary") or "").strip()
    row["published_platforms"] = list(snapshot.get("published_platforms") or [])
    row["platform_coordination"] = dict(snapshot.get("platform_status") or {})
    return row


def _refresh_prefilter_item_coordination_snapshot(
    workspace: Path,
    item_id: str,
    item: Dict[str, Any],
) -> Dict[str, Any]:
    row = dict(item) if isinstance(item, dict) else {}
    if not row:
        return {}
    refreshed = _apply_coordination_snapshot(row, workspace)
    if not isinstance(refreshed, dict):
        return row
    patch: Dict[str, Any] = {}
    for key in (
        "processed_name",
        "media_kind",
        "review_status",
        "coordination_summary",
        "published_platforms",
        "platform_coordination",
    ):
        value = refreshed.get(key)
        current = row.get(key)
        if value != current:
            if isinstance(value, dict):
                patch[key] = dict(value)
            elif isinstance(value, list):
                patch[key] = list(value)
            else:
                patch[key] = value
    if not patch:
        return refreshed

    def _mutate(queue: Dict[str, Any]) -> Dict[str, Any]:
        items = queue.get("items", {})
        if not isinstance(items, dict):
            items = {}
            queue["items"] = items
        current = items.get(item_id, {})
        merged = dict(current) if isinstance(current, dict) else {"id": item_id}
        merged.update(patch)
        merged["updated_at"] = _now_text()
        items[item_id] = merged
        return dict(merged)

    updated = _with_prefilter_queue_lock(workspace, _mutate)
    return dict(updated) if isinstance(updated, dict) else refreshed


def _item_targets_wechat(item: Dict[str, Any]) -> bool:
    return "wechat" in _resolve_item_target_platforms(item)


def _needs_wechat_original_confirmation(item: Dict[str, Any]) -> bool:
    return False


def _build_immediate_publish_confirm_reply_markup(item_id: str, source_url: str) -> Dict[str, Any]:
    rows: list[list[Dict[str, str]]] = []
    rows.append(
        [
            {"text": "⚡ 发布", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|publish_normal|{item_id}"},
            {"text": "📝 原创", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|publish_original|{item_id}"},
        ]
    )
    rows.append(
        [
            {"text": "⏭ 跳过", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|skip|{item_id}"},
        ]
    )
    return _build_inline_keyboard(rows)


def _build_immediate_publish_confirm_text(item: Dict[str, Any]) -> str:
    platforms = _platforms_to_logo_text(_resolve_item_target_platforms(item))
    title = _resolve_immediate_item_title(item)
    tweet_text = re.sub(r"\s+", " ", str(item.get("tweet_text") or "").strip())
    if len(tweet_text) > 90:
        tweet_text = tweet_text[:87].rstrip() + "..."
    if tweet_text:
        summary_items: list[Any] = [tweet_text]
    else:
        summary_items = ["本条未附带原帖摘要，请通过卡片预览链接确认。"]
    source_url = str(item.get('source_url') or "").strip()
    coordination_items: list[Any] = []
    coordination_summary = str(item.get("coordination_summary") or "").strip()
    processed_name = str(item.get("processed_name") or "").strip()
    review_status = str(item.get("review_status") or "").strip().lower()
    published_platforms = _normalize_platform_tokens(item.get("published_platforms") or [])
    if coordination_summary:
        coordination_items.append(coordination_summary)
    if processed_name:
        coordination_items.append({"label": "已采素材", "value": processed_name})
    if review_status:
        review_map = {
            "approved": "已审核通过",
            "rejected": "已审核拒绝",
            "blocked": "待审核",
        }
        coordination_items.append({"label": "审核状态", "value": review_map.get(review_status, review_status)})
    if published_platforms:
        coordination_items.append({"label": "已发平台", "value": _platforms_to_logo_text(published_platforms)})

    sections = [
        {
            "title": "候选信息",
            "emoji": "🎯",
            "items": [
                {"label": "平台", "value": platforms},
                {"label": "标题", "value": title},
            ],
        },
        _build_card_preview_link_section(source_url),
    ]
    if coordination_items:
        sections.append(
            {
                "title": "协作状态",
                "emoji": "🧭",
                "items": coordination_items,
            }
        )
    sections.extend(
        [
            {
                "title": "发布选项",
                "emoji": "✍️",
                "items": [
                    "普通发布：直接按当前平台配置进入发布",
                    "原创发布：仅视频号带原创声明，其它平台正常发布",
                    "跳过本条：本条候选不进入任何平台发布",
                ],
            },
            {
                "title": "原帖摘要",
                "emoji": "📝",
                "items": summary_items,
            },
        ]
    )
    return _build_text_notice(
        "即采即发候选确认",
        sections,
        title_emoji="🚀",
    )


def _build_immediate_publish_confirm_card(item: Dict[str, Any], item_id: str) -> Dict[str, Any]:
    source_url = str(item.get("source_url") or "").strip()
    return _build_prefilter_action_card(
        status="queued",
        title="即采即发候选确认",
        subtitle="请直接选择普通发布、原创发布或跳过本条",
        sections=[
            {
                "title": "候选信息",
                "emoji": "🎯",
                "items": [
                    {"label": "平台", "value": _platforms_to_logo_text(_resolve_item_target_platforms(item))},
                    {"label": "标题", "value": _resolve_immediate_item_title(item)},
                ],
            },
            _build_card_preview_link_section(source_url),
            {
                "title": "发布选项",
                "emoji": "✍️",
                "items": [
                    "声明原创：视频号发布时勾选原创，其它平台正常发布",
                    "跳过：不声明原创，直接进入发布",
                ],
            },
        ],
        source_url=source_url,
        include_source_button=False,
        action_rows=[
            [
                {"text": "⚡ 发布", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|publish_normal|{item_id}"},
                {"text": "📝 原创", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|publish_original|{item_id}"},
            ],
            [
                {"text": "⏭ 跳过", "callback_data": f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|skip|{item_id}"},
            ],
        ],
        menu_label=_menu_breadcrumb_for_item(item),
        task_identifier=_build_task_identifier(action="collect_publish_latest", item_id=item_id),
    )


def _answer_callback_query(
    *,
    bot_token: str,
    query_id: str,
    text: str,
    timeout_seconds: int,
) -> None:
    if not query_id:
        return
    params: Dict[str, Any] = {"callback_query_id": query_id}
    if text:
        params["text"] = text
    try:
        _telegram_api(
            bot_token=bot_token,
            method="answerCallbackQuery",
            params=params,
            timeout_seconds=max(8, int(timeout_seconds)),
            use_post=True,
        )
    except Exception as exc:
        if _is_stale_callback_query_error_text(_exception_text(exc)):
            return
        raise


def _try_clear_callback_buttons(
    *,
    bot_token: str,
    chat_id: str,
    message_id: int,
    inline_message_id: str,
    timeout_seconds: int,
) -> None:
    params: Dict[str, Any] = {}
    if inline_message_id:
        params["inline_message_id"] = inline_message_id
    else:
        if not chat_id or int(message_id) <= 0:
            return
        params["chat_id"] = chat_id
        params["message_id"] = int(message_id)
    params["reply_markup"] = json.dumps({}, ensure_ascii=True)
    _telegram_api(
        bot_token=bot_token,
        method="editMessageReplyMarkup",
        params=params,
        timeout_seconds=max(8, int(timeout_seconds)),
        use_post=True,
    )


def _request_platform_login_qr(
    *,
    platform_name: str = "wechat",
    bot_token: str,
    chat_id: str,
    timeout_seconds: int,
    log_file: Path,
    refresh_page: bool = False,
    prefer_login_entry: bool = False,
) -> Dict[str, Any]:
    try:
        from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as core
    except Exception:
        import main as core  # type: ignore

    try:
        runtime_ctx = _resolve_platform_login_runtime_context(
            core,
            platform_name,
            prefer_login_entry=bool(prefer_login_entry),
        )
        platform = runtime_ctx["platform"]
        result = core.send_platform_login_qr_notification(
            platform_name=platform,
            open_url=runtime_ctx["open_url"],
            debug_port=runtime_ctx["debug_port"],
            chrome_user_data_dir=runtime_ctx["chrome_user_data_dir"],
            auto_open_chrome=True,
            refresh_page=bool(refresh_page),
            allow_duplicate=bool(refresh_page),
            telegram_bot_token=bot_token,
            telegram_chat_id=chat_id,
            telegram_timeout_seconds=timeout_seconds,
        )
        _append_log(
            log_file,
            f"[Worker] platform_login_qr platform={platform} refresh={bool(refresh_page)} sent={bool(result.get('sent'))} "
            f"needs_login={bool(result.get('needs_login', True))} error={result.get('error') or '-'}",
        )
        return result if isinstance(result, dict) else {"ok": False, "error": "invalid platform_login_qr response"}
    except Exception as exc:
        _append_log(log_file, f"[Worker] platform_login_qr failed: {exc}")
        return {
            "ok": False,
            "needs_login": True,
            "transport_error": _is_telegram_transport_error_text(str(exc)),
            "error": str(exc),
        }


def _send_platform_login_text_notice(
    *,
    platform_name: str = "wechat",
    bot_token: str,
    chat_id: str,
    timeout_seconds: int,
    log_file: Path,
    login_reason: str = "",
    qr_error: str = "",
    wait_token: str = "",
    telegram_bot_identifier: str = "",
) -> Dict[str, Any]:
    platform = str(platform_name or "wechat").strip().lower() or "wechat"
    runtime_ctx = _resolve_platform_login_runtime_context(core, platform)
    helper = getattr(core, "_send_platform_login_text_notification", None)
    helper_error = ""
    if callable(helper):
        try:
            result = helper(
                platform_name=platform,
                open_url=str(runtime_ctx.get("open_url") or "").strip(),
                chrome_user_data_dir=str(runtime_ctx.get("chrome_user_data_dir") or "").strip(),
                login_reason=str(login_reason or "").strip(),
                qr_error=str(qr_error or "").strip(),
                wait_token=str(wait_token or "").strip(),
                telegram_bot_token=str(bot_token or "").strip(),
                telegram_chat_id=str(chat_id or "").strip(),
                telegram_bot_identifier=str(telegram_bot_identifier or "").strip(),
                telegram_timeout_seconds=max(10, int(timeout_seconds or 20)),
            )
            if isinstance(result, dict) and bool(result.get("sent")):
                _append_log(log_file, f"[Worker] platform_login_text_notice platform={platform} sent=True helper=core")
                return result
            if isinstance(result, dict):
                helper_error = str(result.get("error") or "").strip()
        except Exception as exc:
            helper_error = str(exc)
            _append_log(log_file, f"[Worker] platform_login_text_notice core helper failed: {exc}")

    display_name = str(PUBLISH_PLATFORM_DISPLAY.get(platform, platform) or platform)
    sections: list[dict[str, Any]] = [
        {
            "title": "登录状态",
            "emoji": "🔐",
            "items": [f"{display_name}当前未登录，需要重新登录。"],
        },
        {
            "title": "处理建议",
            "emoji": "🧭",
            "items": [
                {"label": "平台", "value": display_name},
                {"label": "登录页", "value": str(runtime_ctx.get('open_url') or '').strip() or "-"},
            ],
        },
    ]
    reason_text = str(login_reason or "").strip()
    qr_error_text = str(qr_error or "").strip()
    if reason_text or qr_error_text or helper_error:
        details: list[Any] = []
        if reason_text:
            details.append({"label": "原因", "value": reason_text})
        if qr_error_text:
            details.append({"label": "二维码状态", "value": qr_error_text})
        if helper_error:
            details.append({"label": "补充信息", "value": helper_error})
        sections.append({"title": "诊断信息", "emoji": "📝", "items": details})

    try:
        message_id = _send_text_message(
            bot_token=str(bot_token or "").strip(),
            chat_id=str(chat_id or "").strip(),
            text=_build_text_notice("平台登录提醒", sections, title_emoji="🔐"),
            timeout_seconds=max(10, int(timeout_seconds or 20)),
        )
        sent = int(message_id) > 0
        _append_log(log_file, f"[Worker] platform_login_text_notice platform={platform} sent={sent} helper=fallback")
        return {
            "ok": sent,
            "sent": sent,
            "message_id": int(message_id),
            "fallback": True,
            "error": "" if sent else (helper_error or "text notice send returned empty message id"),
        }
    except Exception as exc:
        error_text = str(exc or "").strip() or helper_error or "text notice send failed"
        _append_log(log_file, f"[Worker] platform_login_text_notice failed: {error_text}")
        return {
            "ok": False,
            "sent": False,
            "fallback": True,
            "transport_error": _is_telegram_transport_error_text(error_text),
            "error": error_text,
        }


def _refresh_platform_login_qr_message(
    *,
    platform_name: str,
    bot_token: str,
    chat_id: str,
    message_id: int,
    timeout_seconds: int,
    log_file: Path,
    telegram_bot_identifier: str = "",
    wait_token: str = "",
) -> Dict[str, Any]:
    try:
        from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as core
    except Exception:
        import main as core  # type: ignore

    if int(message_id) <= 0:
        return {"ok": False, "needs_login": True, "error": "invalid qr message id"}

    try:
        runtime_ctx = _resolve_platform_login_runtime_context(core, platform_name, prefer_login_entry=True)
        platform = runtime_ctx["platform"]
        cache_key = ""
        previous_fingerprint = ""
        cache_key_builder = getattr(core, "_platform_login_qr_cache_key", None)
        qr_notice_cache = getattr(core, "WECHAT_LOGIN_QR_NOTICE_CACHE", None)
        if callable(cache_key_builder):
            try:
                cache_key = str(
                    cache_key_builder(
                        platform,
                        runtime_ctx["debug_port"],
                        runtime_ctx["chrome_user_data_dir"],
                    )
                    or ""
                ).strip()
            except Exception:
                cache_key = ""
        if cache_key and isinstance(qr_notice_cache, dict):
            cached = qr_notice_cache.get(cache_key.lower())
            if not cached:
                cached = qr_notice_cache.get(cache_key)
            if isinstance(cached, tuple) and cached:
                previous_fingerprint = str(cached[0] or "").strip()

        prepared: Dict[str, Any] | None = None
        attempt_count = 2 if platform == "wechat" else 1
        for attempt_index in range(attempt_count):
            prepared = core._prepare_platform_login_qr_notice(
                platform_name=platform,
                open_url=runtime_ctx["open_url"],
                debug_port=runtime_ctx["debug_port"],
                chrome_user_data_dir=runtime_ctx["chrome_user_data_dir"],
                auto_open_chrome=True,
                refresh_page=True,
                wait_token=wait_token,
            )
            if not isinstance(prepared, dict):
                return {"ok": False, "needs_login": True, "error": "invalid qr payload"}
            if not bool(prepared.get("ok")):
                return prepared
            current_fingerprint = str(prepared.get("fingerprint") or "").strip()
            if (
                platform == "wechat"
                and attempt_index == 0
                and previous_fingerprint
                and current_fingerprint
                and current_fingerprint == previous_fingerprint
            ):
                _append_log(
                    log_file,
                    f"[Worker] platform_login_qr fingerprint unchanged after refresh; retry once platform={platform}",
                )
                time.sleep(1.0)
                continue
            break
        if not isinstance(prepared, dict):
            return {"ok": False, "needs_login": True, "error": "invalid qr payload"}
        if not bool(prepared.get("ok")):
            return prepared
        notify_settings = core._resolve_runtime_telegram_notify_settings(
            telegram_bot_token=bot_token,
            telegram_chat_id=chat_id,
            telegram_timeout_seconds=timeout_seconds,
        )
        api_base = str(getattr(notify_settings, "telegram_api_base", "") or "").strip()
        filename = str(prepared.get("filename") or "platform_login_qr.png")
        mime = str(prepared.get("mime") or "image/png")
        caption = str(prepared.get("caption") or "")
        reply_markup = _outgoing_reply_markup(
            prepared.get("reply_markup") if isinstance(prepared.get("reply_markup"), dict) else None,
            for_edit=True,
        )
        if not isinstance(reply_markup, dict):
            reply_markup = {"inline_keyboard": []}
        media = {
            "type": "photo",
            "media": "attach://photo",
            "caption": caption,
            "parse_mode": "HTML",
        }
        payload = _post_telegram_multipart_with_retries(
            bot_token=bot_token,
            method="editMessageMedia",
            data={
                "chat_id": str(chat_id or "").strip(),
                "message_id": int(message_id),
                "media": json.dumps(media, ensure_ascii=True),
                "reply_markup": json.dumps(reply_markup, ensure_ascii=True),
            },
            files={
                "photo": (
                    filename,
                    bytes(prepared.get("photo_bytes") or b""),
                    mime,
                )
            },
            timeout_seconds=max(8, int(timeout_seconds or 20)),
            api_base=api_base,
        )
        core._remember_wechat_qr_notice(
            str(prepared.get("cache_key") or ""),
            str(prepared.get("fingerprint") or ""),
        )
        result = dict(prepared)
        result.update({"sent": True, "edited": True, "response": payload})
        _append_log(log_file, f"[Worker] platform_login_qr refreshed in-place platform={platform} message_id={int(message_id)}")
        return result
    except Exception as exc:
        _append_log(log_file, f"[Worker] platform_login_qr in-place refresh failed: {exc}")
        return {
            "ok": False,
            "needs_login": True,
            "transport_error": _is_telegram_transport_error_text(str(exc)),
            "error": str(exc),
        }


def _resolve_platform_login_runtime_context(
    core: Any,
    platform_name: str,
    *,
    prefer_login_entry: bool = False,
) -> Dict[str, Any]:
    platform = str(platform_name or "wechat").strip().lower() or "wechat"
    if platform == "wechat":
        debug_port = int(
            os.getenv(
                "CYBERCAR_WECHAT_CHROME_DEBUG_PORT",
                os.getenv(
                    "CYBERCAR_CHROME_DEBUG_PORT",
                    str(getattr(core, "DEFAULT_WECHAT_DEBUG_PORT", getattr(core, "DEFAULT_PORT", 9333))),
                ),
            )
        )
        default_wechat_profile_dir = str(getattr(core, "DEFAULT_WECHAT_CHROME_USER_DATA_DIR", "")).strip()
        if not default_wechat_profile_dir and _get_cybercar_paths is not None:
            try:
                default_wechat_profile_dir = str(_get_cybercar_paths().wechat_profile_dir)
            except Exception:
                default_wechat_profile_dir = ""
        chrome_user_data_dir = str(
            os.getenv(
                "CYBERCAR_WECHAT_CHROME_USER_DATA_DIR",
                os.getenv(
                    "CYBERCAR_CHROME_USER_DATA_DIR",
                    default_wechat_profile_dir,
                ),
            )
            or ""
        ).strip() or default_wechat_profile_dir
    else:
        debug_port = int(os.getenv("CYBERCAR_CHROME_DEBUG_PORT", str(getattr(core, "DEFAULT_PORT", 9333))))
        chrome_user_data_dir = str(
            os.getenv(
                "CYBERCAR_CHROME_USER_DATA_DIR",
                str(getattr(core, "DEFAULT_CHROME_USER_DATA_DIR", "")),
            )
        ).strip()
    create_url = str((getattr(core, "PLATFORM_CREATE_POST_URLS", {}) or {}).get(platform) or "").strip()
    login_entry_url = str((getattr(core, "PLATFORM_LOGIN_ENTRY_URLS", {}) or {}).get(platform) or "").strip()
    # For publish-capable platforms, probe the business page first so a stale
    # login helper URL does not force the active tab back to login.html.
    # Manual login requests can opt into the dedicated login entry directly
    # so the user gets a QR code immediately.
    open_url = login_entry_url if prefer_login_entry and login_entry_url else (create_url or login_entry_url)
    return {
        "platform": platform,
        "debug_port": debug_port,
        "chrome_user_data_dir": chrome_user_data_dir,
        "open_url": open_url,
    }


def _confirm_platform_login_done(
    *,
    platform_name: str,
    bot_token: str,
    chat_id: str,
    timeout_seconds: int,
    log_file: Path,
    telegram_bot_identifier: str = "",
    wait_token: str = "",
) -> Dict[str, Any]:
    try:
        from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as core
    except Exception:
        import main as core  # type: ignore

    try:
        runtime_ctx = _resolve_platform_login_runtime_context(core, platform_name)
        core.confirm_platform_login_signal(
            platform_name=runtime_ctx["platform"],
            profile_dir=runtime_ctx["chrome_user_data_dir"],
            wait_token=wait_token,
        )
        status = core.check_platform_login_status(
            platform_name=runtime_ctx["platform"],
            open_url=runtime_ctx["open_url"],
            debug_port=runtime_ctx["debug_port"],
            chrome_user_data_dir=runtime_ctx["chrome_user_data_dir"],
            auto_open_chrome=True,
            refresh_page=True,
        )
        if not bool(status.get("needs_login", True)):
            _append_log(log_file, f"[Worker] platform_login_done platform={runtime_ctx['platform']} confirmed=True")
            return {"ok": True, "needs_login": False, "confirmed": True, "status": status}
        _append_log(
            log_file,
            f"[Worker] platform_login_done platform={runtime_ctx['platform']} confirmed=False waiting_auto_poll=True",
        )
        return {"ok": True, "needs_login": True, "confirmed": False, "status": status, "sent": False}
    except Exception as exc:
        _append_log(log_file, f"[Worker] platform_login_done failed: {exc}")
        return {"ok": False, "error": str(exc), "needs_login": True, "confirmed": False}


def _append_note_line(note: str, line: str, marker: str) -> str:
    raw_note = str(note or "").strip()
    if marker and marker in raw_note:
        return raw_note
    if not raw_note:
        return line
    return raw_note + "\n" + line


def _build_prefilter_down_result_message(
    *,
    video_name: str,
    actor: str,
    now_text: str,
    item_id: str,
    changed: bool,
) -> str:
    result = "已写入拒绝状态" if changed else "已经是拒绝状态（重复点击）"
    return _build_text_notice(
        "预过滤处理通知",
        [
            {
                "title": "处理结果",
                "emoji": "⚠️",
                "items": [result],
            },
            {
                "title": "操作信息",
                "emoji": "👤",
                "items": [
                    {"label": "操作人", "value": actor or "-"},
                    {"label": "时间", "value": now_text},
                    {"label": "编号", "value": item_id},
                ],
            },
            {
                "title": "后续影响",
                "emoji": "🧭",
                "items": [
                    "本条视频后续平台将自动跳过",
                    "后续轮次也会继续跳过，不回滚已发布结果",
                ],
            },
        ],
        title_emoji="🗂️",
    )


def _build_prefilter_status_card(
    *,
    item: Dict[str, Any],
    title: str,
    subtitle: str,
    status: str,
    result_section_title: str,
    result_items: list[Any],
) -> Dict[str, Any]:
    source_url = str(item.get("source_url") or "").strip()
    sections = [
        {
            "title": result_section_title,
            "emoji": "📌",
            "items": result_items,
        },
        {
            "title": "候选信息",
            "emoji": "🎯",
            "items": [
                {"label": "平台", "value": _resolve_immediate_item_platform_text(item, with_logo=True)},
                {"label": "标题", "value": _resolve_immediate_item_title(item)},
            ],
        },
        _build_card_preview_link_section(source_url),
    ]
    actor = str(item.get("actor") or "").strip()
    updated_at = str(item.get("updated_at") or "").strip()
    if actor or updated_at:
        details: list[Any] = []
        if actor:
            details.append({"label": "操作人", "value": actor})
        if updated_at:
            details.append({"label": "时间", "value": updated_at})
        sections.append({"title": "操作记录", "emoji": "👤", "items": details})
    return _build_prefilter_action_card(
        status=status,
        title=title,
        subtitle=subtitle,
        sections=sections,
        source_url=source_url,
        include_source_button=False,
        menu_label=_menu_breadcrumb_for_item(item),
        task_identifier=_build_task_identifier(
            action="collect_publish_latest",
            item_id=str(item.get("item_id") or item.get("id") or ""),
        ),
    )


def _build_platform_launch_result_section(platform_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    def _status_text_for_failure(status: str, details: dict[str, str], pid: int) -> str:
        reason = str(details.get("reason") or "").strip()
        category = str(details.get("category") or "").strip()
        if status == "login_required":
            return "需要登录"
        if "结果未确认" in category or "结果未明确" in reason:
            return "发布结果待核实"
        if "回退草稿" in category or "草稿" in reason:
            return "发布已回退草稿"
        if status == "failed" and pid <= 0:
            return "平台未启动"
        return "平台处理失败"

    items: list[Any] = []
    for platform in _normalize_platform_tokens(platform_results.keys()):
        result = platform_results.get(platform, {})
        if not isinstance(result, dict):
            result = {}
        status = str(result.get("status") or "").strip().lower()
        simulated = bool(result.get("simulated"))
        label = _platform_display_with_logo(platform)
        if status == "queued":
            pid = int(result.get("pid") or 0)
            value = "测试模式：平台任务已模拟排队" if simulated else "后台任务已排队"
            if pid > 0:
                value = f"{value} (PID {pid})"
        elif status == "running":
            value = "测试模式：平台发布模拟中" if simulated else "平台发布中"
        elif status == "success":
            value = "测试模式：平台已模拟发布成功" if simulated else "平台已确认发布成功"
        elif status == "skipped_duplicate":
            reason = str(result.get("error") or "").strip()
            value = "平台已有发布记录，已自动跳过"
            if reason:
                value = f"{value}；原因：{reason}"
        else:
            pid = int(result.get("pid") or 0)
            details = {
                "reason": str(result.get("failure_reason") or "").strip(),
                "category": str(result.get("failure_category") or "").strip(),
                "suggestion": str(result.get("failure_suggestion") or "").strip(),
                "raw_signal": str(result.get("error") or "").strip(),
            }
            if not any(
                (
                    str(details.get("reason") or "").strip(),
                    str(details.get("category") or "").strip(),
                    str(details.get("suggestion") or "").strip(),
                )
            ):
                details = _describe_platform_failure(platform, str(result.get("error") or "").strip())
            status_text = _status_text_for_failure(status, details, pid)
            reason = _strip_error_code_text(
                str(details.get("reason") or "").strip() or str(result.get("error") or "").strip()
            )
            parts = [status_text]
            if reason:
                parts.append(f"原因：{_preview_text(reason, limit=80)}")
            if status == "failed":
                parts.append("请修复后重试")
            value = "；".join(parts) if parts else "后台任务启动失败"
        items.append({"label": label, "value": value})
    if not items:
        items.append("未记录任何平台任务。")
    return {
        "title": "平台状态",
        "emoji": "🧾",
        "items": items,
    }


def _platform_result_is_terminal(status: str) -> bool:
    return str(status or "").strip().lower() in {"success", "failed", "login_required", "skipped_duplicate"}


def _claim_immediate_platform_feedback(
    *,
    workspace: Path,
    item_id: str,
    platform: str,
) -> Dict[str, Any]:
    platform_token = str(platform or "").strip().lower()

    def _mutate(queue: Dict[str, Any]) -> Dict[str, Any]:
        items = queue.get("items", {})
        if not isinstance(items, dict):
            items = {}
            queue["items"] = items
        row = items.get(item_id, {})
        if not isinstance(row, dict):
            return {"send_platform": False, "send_summary": False, "item": {}, "platform_result": {}}
        results = _normalize_platform_results(row.get("platform_results"))
        result = results.get(platform_token, {})
        if not isinstance(result, dict):
            result = {}
        status = str(result.get("status") or "").strip().lower()
        send_platform = _platform_result_is_terminal(status) and not str(result.get("feedback_sent_at") or "").strip()
        if send_platform:
            result = dict(result)
            result["feedback_sent_at"] = _now_text()
            results[platform_token] = result
            row["platform_results"] = results
            row.update(_summarize_platform_results(row))
        send_summary = False
        if (
            row.get("platform_results")
            and not any(
                str(payload.get("status") or "").strip().lower() in {"queued", "running"}
                for payload in _normalize_platform_results(row.get("platform_results")).values()
                if isinstance(payload, dict)
            )
            and not str(row.get("publish_summary_sent_at") or "").strip()
        ):
            row["publish_summary_sent_at"] = _now_text()
            send_summary = True
        row["updated_at"] = _now_text()
        items[item_id] = row
        return {
            "send_platform": send_platform,
            "send_summary": send_summary,
            "item": dict(row),
            "platform_result": dict(result) if isinstance(result, dict) else {},
        }

    payload = _with_prefilter_queue_lock(workspace, _mutate)
    return payload if isinstance(payload, dict) else {"send_platform": False, "send_summary": False, "item": {}, "platform_result": {}}


def _build_immediate_platform_feedback_payload(
    *,
    item: Dict[str, Any],
    platform: str,
    result: Dict[str, Any],
) -> Dict[str, Any]:
    platform_token = str(platform or "").strip().lower()
    label = _platform_display_with_logo(platform_token)
    status = str(result.get("status") or "").strip().lower()
    publish_id = str(result.get("publish_id") or "").strip()
    details = {
        "reason": str(result.get("failure_reason") or "").strip(),
        "category": str(result.get("failure_category") or "").strip(),
        "suggestion": str(result.get("failure_suggestion") or "").strip(),
        "raw_signal": str(result.get("error") or "").strip(),
    }
    if not any((details["reason"], details["category"], details["suggestion"])):
        details = _describe_platform_failure(platform_token, str(result.get("error") or "").strip())

    title = f"{label}发布状态更新"
    subtitle = "平台已返回最新处理结果"
    feedback_status = "success"
    status_items: list[Any]
    if status == "success":
        title = f"{label}发布已确认"
        subtitle = "平台已确认发布成功"
        status_items = ["平台已确认发布成功。"]
        if publish_id:
            status_items.append({"label": "发布ID", "value": publish_id})
    elif status == "skipped_duplicate":
        title = f"{label}已跳过重复发布"
        subtitle = "检测到历史发布记录，本轮未重复提交"
        status_items = ["平台已有历史发布记录，本轮已自动跳过。"]
        reason = str(result.get("error") or "").strip()
        if reason:
            status_items.append({"label": "原因", "value": reason})
    elif status == "login_required":
        title = f"{label}需要重新登录"
        subtitle = "平台登录态失效，请先完成登录"
        feedback_status = "failed"
        status_items = ["检测到平台当前需要重新登录。"]
        reason = _strip_error_code_text(
            str(details.get("reason") or "").strip() or str(result.get("error") or "").strip()
        )
        if reason:
            status_items.append({"label": "原因", "value": reason})
        status_items.append("登录后如仍失败，请继续修复。")
    else:
        title = f"{label}发布失败"
        subtitle = "平台处理失败，请查看原因后重试"
        feedback_status = "failed"
        status_items = ["平台处理失败，本次未确认发布成功。"]
        reason = _strip_error_code_text(
            str(details.get("reason") or "").strip() or str(result.get("error") or "").strip()
        )
        if reason:
            status_items.append({"label": "原因", "value": _preview_text(reason, limit=120)})
        status_items.append("请修复后重试。")

    return {
        "title": title,
        "subtitle": subtitle,
        "status": feedback_status,
        "sections": [
            _build_immediate_candidate_info_section(item, include_platform=False),
            {
                "title": "执行状态",
                "emoji": "📌",
                "items": status_items,
            },
            _build_platform_launch_result_section({platform_token: result}),
        ],
    }


def _build_immediate_publish_summary_feedback_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    platform_results = _normalize_platform_results(item.get("platform_results"))
    item_status = str(item.get("status") or "").strip().lower()
    success_count = int(item.get("publish_success_count") or 0)
    failed_count = int(item.get("publish_failed_count") or 0)
    total_count = len(platform_results)
    status_items: list[Any] = [
        {"label": "成功平台", "value": str(success_count)},
        {"label": "失败平台", "value": str(failed_count)},
        {"label": "目标平台", "value": str(total_count)},
    ]
    if item_status == "publish_done":
        title = "即采即发已全部完成"
        subtitle = "所有目标平台都已进入终态"
        feedback_status = "success"
        status_items.insert(0, "全部目标平台已完成发布或按去重策略跳过。")
    elif item_status == "publish_partial":
        title = "即采即发部分平台已完成"
        subtitle = "部分平台成功，部分平台需要继续处理"
        feedback_status = "failed"
        status_items.insert(0, "本轮存在部分平台成功、部分平台失败或需要登录。")
    else:
        title = "即采即发发布失败"
        subtitle = "所有目标平台都未成功完成"
        feedback_status = "failed"
        status_items.insert(0, "本轮所有目标平台均未确认发布成功。")
    return {
        "title": title,
        "subtitle": subtitle,
        "status": feedback_status,
        "sections": [
            _build_immediate_candidate_info_section(item, include_platform=False),
            {
                "title": "执行汇总",
                "emoji": "📦",
                "items": status_items,
            },
            _build_platform_launch_result_section(platform_results),
        ],
    }


def _send_immediate_platform_feedback(
    *,
    runner: Any,
    email_settings: Any,
    workspace: Path,
    item_id: str,
    platform: str,
    item_override: Optional[Dict[str, Any]] = None,
    result_override: Optional[Dict[str, Any]] = None,
    send_summary: bool = True,
) -> None:
    if isinstance(item_override, dict) and isinstance(result_override, dict):
        claimed = {
            "send_platform": True,
            "send_summary": False,
            "item": dict(item_override),
            "platform_result": dict(result_override),
        }
    else:
        claimed = _claim_immediate_platform_feedback(workspace=workspace, item_id=item_id, platform=platform)
    item = claimed.get("item") if isinstance(claimed.get("item"), dict) else {}
    result = claimed.get("platform_result") if isinstance(claimed.get("platform_result"), dict) else {}
    if bool(claimed.get("send_platform")) and item and result:
        payload = _build_immediate_platform_feedback_payload(item=item, platform=platform, result=result)
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title=str(payload.get("title") or "").strip(),
            subtitle=str(payload.get("subtitle") or "").strip(),
            sections=list(payload.get("sections") or []),
            status=str(payload.get("status") or "success").strip(),
            platforms=[platform],
            menu_label=_menu_breadcrumb_for_item(item),
            task_identifier=_build_task_identifier(
                action="collect_publish_latest",
                value=str(item.get("target_platforms") or ""),
                item_id=item_id,
            ),
        )
    if send_summary and bool(claimed.get("send_summary")) and item:
        payload = _build_immediate_publish_summary_feedback_payload(item)
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title=str(payload.get("title") or "").strip(),
            subtitle=str(payload.get("subtitle") or "").strip(),
            sections=list(payload.get("sections") or []),
            status=str(payload.get("status") or "success").strip(),
            platforms=_resolve_item_target_platforms(item),
            menu_label=_menu_breadcrumb_for_item(item),
            task_identifier=_build_task_identifier(
                action="collect_publish_latest",
                value=str(item.get("target_platforms") or ""),
                item_id=item_id,
            ),
        )


def _probe_platform_login_after_publish_failure(
    *,
    workspace: Path,
    item_id: str,
    platform: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    timeout_seconds: int,
    log_file: Path,
    error_text: str,
) -> tuple[str, str]:
    platform_token = str(platform or "").strip().lower()
    if platform_token != "wechat":
        return "failed", str(error_text or "").strip()
    explicit_login_signal = _looks_like_explicit_login_gate_error(error_text)
    try:
        from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as core
    except Exception:
        import main as core  # type: ignore

    runtime_ctx = _resolve_platform_login_runtime_context(core, platform_token)
    try:
        login_status = core.check_platform_login_status(
            platform_name=runtime_ctx["platform"],
            open_url=runtime_ctx["open_url"],
            debug_port=runtime_ctx["debug_port"],
            chrome_user_data_dir=runtime_ctx["chrome_user_data_dir"],
            auto_open_chrome=True,
            refresh_page=True,
        )
    except Exception as exc:
        _append_log(log_file, f"[Worker] immediate publish login recheck status probe failed platform={platform_token} item={item_id} error={exc}")
        _record_error_event(
            workspace=workspace,
            log_file=log_file,
            category="wechat_login_probe_status_check_failed",
            message="immediate publish login recheck status probe failed",
            exc=exc,
            context={
                "item_id": item_id,
                "platform": platform_token,
                "open_url": str(runtime_ctx.get("open_url") or "").strip(),
            },
            retryable=True,
            severity="warning",
        )
        login_status = {"needs_login": False, "probe_error": str(exc)}
    if not isinstance(login_status, dict):
        login_status = {"needs_login": False}
    probe_inconclusive = bool(login_status.get("probe_error")) or not bool(login_status.get("ok", True))
    if not bool(login_status.get("needs_login")) and not (explicit_login_signal and probe_inconclusive):
        _append_log(
            log_file,
            f"[Worker] immediate publish login recheck kept original failure platform={platform_token} item={item_id} "
            f"needs_login={bool((login_status or {}).get('needs_login'))}",
        )
        return "failed", str(error_text or "").strip()
    if not bool(login_status.get("needs_login")) and explicit_login_signal and probe_inconclusive:
        _append_log(
            log_file,
            f"[Worker] immediate publish login recheck falling back to explicit login signal "
            f"platform={platform_token} item={item_id} error={str(error_text or '').strip() or '-'}",
        )
    text_result = _send_platform_login_text_notice(
        platform_name=platform_token,
        bot_token=telegram_bot_token,
        chat_id=telegram_chat_id,
        timeout_seconds=max(10, int(timeout_seconds or 20)),
        log_file=log_file,
        login_reason=str((login_status or {}).get("reason") or "").strip() or str(error_text or "").strip(),
    )
    result = _request_platform_login_qr(
        platform_name=platform_token,
        bot_token=telegram_bot_token,
        chat_id=telegram_chat_id,
        timeout_seconds=max(10, int(timeout_seconds or 20)),
        log_file=log_file,
        refresh_page=True,
        prefer_login_entry=True,
    )
    if not isinstance(result, dict) or not bool(result.get("needs_login", True)):
        _record_error_event(
            workspace=workspace,
            log_file=log_file,
            category="wechat_login_probe_qr_unavailable",
            message="immediate publish login recheck qr result did not confirm needs_login",
            error_text=str(result if result is not None else ""),
            context={"item_id": item_id, "platform": platform_token},
            retryable=True,
            severity="warning",
        )
        return "failed", str(error_text or "").strip()
    qr_chain_confirmed = bool(result.get("sent")) or bool(result.get("skipped")) or bool(result.get("transport_error"))
    if not qr_chain_confirmed:
        _append_log(
            log_file,
            f"[Worker] immediate publish login recheck kept original failure platform={platform_token} item={item_id} "
            f"text_sent={bool(text_result.get('sent'))} qr_sent={bool(result.get('sent'))} "
            f"qr_skipped={bool(result.get('skipped'))} qr_transport_error={bool(result.get('transport_error'))}",
        )
        _record_error_event(
            workspace=workspace,
            log_file=log_file,
            category="wechat_login_probe_qr_unconfirmed",
            message="immediate publish login recheck qr chain not confirmed; keep original failure",
            error_text=str(error_text or "").strip(),
            context={
                "item_id": item_id,
                "platform": platform_token,
                "text_sent": bool(text_result.get("sent")),
                "qr_sent": bool(result.get("sent")),
                "qr_skipped": bool(result.get("skipped")),
                "qr_transport_error": bool(result.get("transport_error")),
            },
            retryable=True,
            severity="warning",
        )
        return "failed", str(error_text or "").strip()
    notices: list[str] = []
    if bool(text_result.get("sent")):
        notices.append("已向 Telegram 发送登录提醒")
    elif str(text_result.get("error") or "").strip():
        notices.append(f"登录提醒发送失败：{str(text_result.get('error') or '').strip()}")
    if bool(result.get("sent")):
        notices.append("视频号登录二维码已发送到 Telegram")
    elif bool(result.get("skipped")):
        notices.append("视频号登录二维码近期已发送，可直接查看最近一条二维码消息")
    elif bool(result.get("transport_error")):
        notices.append("Telegram 网络抖动导致二维码暂未送达，可稍后重试")
    elif str(result.get("error") or "").strip():
        notices.append(f"视频号登录二维码发送失败：{str(result.get('error') or '').strip()}")
    notice = "；".join(list(dict.fromkeys([str(part or "").strip() for part in notices if str(part or "").strip()]))).strip()
    if bool(result.get("transport_error")):
        _append_log(
            log_file,
            f"[Worker] immediate publish login recheck transport degraded platform={platform_token} item={item_id}",
        )
    merged_error = str(error_text or "").strip()
    if notice and notice not in merged_error:
        merged_error = f"{merged_error}；{notice}" if merged_error else notice
    _append_log(log_file, f"[Worker] immediate publish login recheck platform={platform_token} status=login_required notice={notice or '-'} item={item_id}")
    return "login_required", merged_error


def _append_prefilter_feedback_event(
    *,
    workspace: Path,
    action: str,
    item_id: str,
    video_name: str,
    actor: str,
    chat_id: str,
    message_id: int,
    queue_status: str,
    changed: Optional[bool] = None,
) -> None:
    payload: Dict[str, Any] = {
        "ts": _now_text(),
        "action": str(action or "").strip().lower(),
        "item_id": str(item_id or "").strip(),
        "video_name": str(video_name or "").strip(),
        "actor": str(actor or "").strip(),
        "chat_id": str(chat_id or "").strip(),
        "message_id": int(message_id or 0),
        "queue_status": str(queue_status or "").strip().lower(),
        # Record-only for future optimization pipeline; no runtime behavior depends on this file yet.
        "search_optimization_ready": False,
    }
    if changed is not None:
        payload["changed"] = bool(changed)
    _append_jsonl(_prefilter_feedback_history_path(workspace), payload)


def _apply_review_reject(
    *,
    workspace: Path,
    video_name: str,
    actor: str,
    item_id: str,
    media_kind: str = "video",
) -> bool:
    state_path = _review_state_path(workspace)
    state = _load_review_state(state_path)
    items = state.get("items", {})
    if not isinstance(items, dict):
        items = {}
        state["items"] = items
    clean_video_name = str(video_name or "").strip()
    if not clean_video_name:
        return False

    normalized_media_kind = _normalize_review_media_kind(media_kind)
    review_key = core._make_review_state_key(clean_video_name, normalized_media_kind)
    existing = items.get(review_key, {})
    base = dict(existing) if isinstance(existing, dict) else {}
    old_status = str(base.get("status", "") or "").strip().lower()
    old_note = str(base.get("note", "") or "")
    now_text = _now_text()
    marker = f"[prefilter:{item_id}]"
    actor_text = actor if actor.startswith("@") else f"@{actor}"
    new_line = f"telegram downvote by {actor_text} {marker} at {now_text}"
    new_note = _append_note_line(old_note, new_line, marker)

    changed = (old_status != "rejected") or (new_note != old_note)
    if not changed:
        return False

    base["status"] = "rejected"
    base["note"] = new_note
    base["updated_at"] = now_text
    base["media_kind"] = normalized_media_kind
    base["processed_name"] = clean_video_name
    items[review_key] = base
    _save_review_state(state_path, state)
    return True


def _record_prefilter_source_in_collect_ledger(
    *,
    workspace: Path,
    source_url: str,
    media_kind: str = "video",
    processed_name: str = "",
    state: str = "review_skipped",
) -> bool:
    clean_source_url = str(source_url or "").strip()
    if not clean_source_url:
        return False
    try:
        workspace_ctx = core.init_workspace(str(workspace))
        status_id = str(core._extract_status_id_from_url(clean_source_url) or "").strip()
        if not status_id:
            return False
        payload = core._load_candidate_ledger_payload(workspace_ctx)
        items = payload.get("items", {})
        if not isinstance(items, dict):
            items = {}
            payload["items"] = items
        changed = core._upsert_candidate_ledger_entry(
            items,
            candidate_id=core._make_x_candidate_id(status_id, status_id),
            status_id=status_id,
            media_key=status_id,
            media_kind=str(media_kind or "video"),
            state=str(state or "review_skipped").strip() or "review_skipped",
            status_url=clean_source_url,
            processed_name=str(processed_name or "").strip(),
        )
        if not changed:
            return False
        payload["version"] = int(payload.get("version") or 1)
        payload["updated_at"] = _now_text()
        core._save_candidate_ledger_payload(workspace_ctx, payload)
        return True
    except Exception:
        return False


def _record_prefilter_skip_source_in_collect_ledger(
    *,
    workspace: Path,
    source_url: str,
    media_kind: str = "video",
    processed_name: str = "",
) -> bool:
    return _record_prefilter_source_in_collect_ledger(
        workspace=workspace,
        source_url=source_url,
        media_kind=media_kind,
        processed_name=processed_name,
        state="review_skipped",
    )


def _apply_review_pending_block(*, workspace: Path, video_name: str, item_id: str, media_kind: str = "video") -> bool:
    state_path = _review_state_path(workspace)
    state = _load_review_state(state_path)
    items = state.get("items", {})
    if not isinstance(items, dict):
        items = {}
        state["items"] = items
    clean_video_name = str(video_name or "").strip()
    if not clean_video_name:
        return False

    normalized_media_kind = _normalize_review_media_kind(media_kind)
    review_key = core._make_review_state_key(clean_video_name, normalized_media_kind)
    existing = items.get(review_key, {})
    base = dict(existing) if isinstance(existing, dict) else {}
    old_status = str(base.get("status", "") or "").strip().lower()
    old_note = str(base.get("note", "") or "")
    now_text = _now_text()
    marker = f"[immediate-pending:{item_id}]"
    new_line = f"immediate publish candidate pending manual approval {marker} at {now_text}"
    new_note = _append_note_line(old_note, new_line, marker)

    changed = (old_status != "blocked") or (new_note != old_note)
    if not changed:
        return False

    base["status"] = "blocked"
    base["note"] = new_note
    base["updated_at"] = now_text
    base["media_kind"] = normalized_media_kind
    base["processed_name"] = clean_video_name
    items[review_key] = base
    _save_review_state(state_path, state)
    return True


def _apply_review_approve(
    *,
    workspace: Path,
    video_name: str,
    actor: str,
    item_id: str,
    media_kind: str = "video",
) -> bool:
    state_path = _review_state_path(workspace)
    state = _load_review_state(state_path)
    items = state.get("items", {})
    if not isinstance(items, dict):
        items = {}
        state["items"] = items
    clean_video_name = str(video_name or "").strip()
    if not clean_video_name:
        return False

    normalized_media_kind = _normalize_review_media_kind(media_kind)
    review_key = core._make_review_state_key(clean_video_name, normalized_media_kind)
    existing = items.get(review_key, {})
    base = dict(existing) if isinstance(existing, dict) else {}
    old_status = str(base.get("status", "") or "").strip().lower()
    old_note = str(base.get("note", "") or "")
    now_text = _now_text()
    marker = f"[immediate-approve:{item_id}]"
    actor_text = actor if actor.startswith("@") else f"@{actor}"
    new_line = f"immediate publish approved by {actor_text} {marker} at {now_text}"
    new_note = _append_note_line(old_note, new_line, marker)

    changed = (old_status != "approved") or (new_note != old_note)
    if not changed:
        return False

    base["status"] = "approved"
    base["note"] = new_note
    base["updated_at"] = now_text
    base["media_kind"] = normalized_media_kind
    base["processed_name"] = clean_video_name
    items[review_key] = base
    _save_review_state(state_path, state)
    return True

def _latest_log_tail(workspace: Path, prefix: str, lines: int = 25) -> str:
    log_dir = workspace / DEFAULT_LOG_SUBDIR
    if not log_dir.exists():
        return f"Log directory not found: {log_dir}"
    files = sorted(log_dir.glob(f"{prefix}_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return f"No logs found: {prefix}_*.log"
    target = files[0]
    content = target.read_text(encoding="utf-8", errors="ignore").splitlines()
    tail = "\n".join(content[-lines:]) if content else "(empty log)"
    return f"Log file: {target}\n\n{tail}"


def _run_cmd(cmd: Iterable[str], timeout_seconds: int, workdir: Optional[Path] = None) -> Dict[str, Any]:
    started = time.time()
    try:
        proc = subprocess.run(
            [str(x) for x in cmd],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(5, int(timeout_seconds)),
            cwd=str(workdir) if workdir else None,
            check=False,
        )
        elapsed = time.time() - started
        return {
            "ok": proc.returncode == 0,
            "code": proc.returncode,
            "stdout": proc.stdout or "",
            "stderr": proc.stderr or "",
            "elapsed": elapsed,
        }
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - started
        return {
            "ok": False,
            "code": -1,
            "stdout": str(exc.stdout or ""),
            "stderr": f"timeout after {elapsed:.1f}s",
            "elapsed": elapsed,
        }


def _extract_log_path(output: str) -> str:
    text = str(output or "")
    patterns = [
        r"(?:日志文件|日志)\s*[:：]\s*([^\r\n]+)",
        r"Log file\s*[:：]\s*([^\r\n]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return str(m.group(1) or "").strip()
    return ""


def _summarize_run(result: Dict[str, Any], title: str) -> str:
    out = str(result.get("stdout") or "")
    err = str(result.get("stderr") or "")
    code = int(result.get("code") or 0)
    elapsed = float(result.get("elapsed") or 0.0)
    merged = (out + "\n" + err).strip()
    merged_lower = merged.lower()
    log_paths = _collect_result_log_paths(result, merged)

    skip_reason = ""
    if "璺宠繃鏈鎵ц锛氫笂涓€娆℃祦姘寸嚎浠诲姟浠嶅湪杩愯" in merged:
        skip_reason = "已有同任务在运行（命中锁保护）"
    elif "[runner][skipped] previous pipeline still running" in merged_lower:
        skip_reason = "已有发布/采集 pipeline 正在运行，本次任务未实际执行"
    elif "[runner][skipped] high-priority request pending" in merged_lower:
        skip_reason = "存在高优先级即时候选任务，本次常规发布未执行"
    elif "no new processed outputs from this crawl, skip publish schedule" in merged_lower:
        skip_reason = "本轮没有可发布视频，已跳过发布"
    elif "[publish][skipped]" in merged_lower:
        if "duplicate target blocked" in merged_lower:
            skip_reason = "检测到素材，但已存在发布记录，当前没有可用新素材"
        else:
            skip_reason = "未检测到可用素材，发布流程已跳过"
    elif "collect-only mode enabled, skip publish schedule" in merged_lower:
        skip_reason = "当前为仅采集模式，发布阶段已跳过"

    lines: list[str] = []
    publish_like_title = "发布" in str(title or "")
    has_platform_log = bool(log_paths)
    has_meaningful_output = bool(merged.strip())
    unconfirmed_publish = bool(
        publish_like_title
        and result.get("ok")
        and not skip_reason
        and (not has_platform_log)
        and (not has_meaningful_output or elapsed <= 5.0)
    )
    if result.get("ok") and skip_reason:
        status = "跳过"
    elif _has_publish_failure_output(merged):
        status = "失败"
    elif unconfirmed_publish:
        status = "未确认"
    else:
        status = "成功" if result.get("ok") else "失败"
    lines.append(f"{title}: {status}")
    lines.append(f"退出码: {code}")
    lines.append(f"耗时: {elapsed:.1f}s")
    if skip_reason:
        lines.append(f"结果说明: {skip_reason}")
    elif unconfirmed_publish:
        lines.append("结果说明: 未检测到有效执行输出，当前只能确认任务结束，不能确认平台已实际发布。")

    key_lines: list[str] = []
    benign_notify_markers = (
        "[notify] message send failed via telegram_bot",
        "[notify] message send warning via telegram_bot",
        "[worker] deletemessage failed:",
        "[worker] deletemessage warning:",
    )
    for ln in merged.splitlines():
        s = ln.strip()
        if not s:
            continue
        lowered = s.lower()
        if code == 0 and any(marker in lowered for marker in benign_notify_markers):
            continue
        if any(
            token in s
            for token in [
                "鎵ц鍧?#3",
                "浠诲姟鎵ц鎴愬姛",
                "浠诲姟閫€鍑虹爜寮傚父",
                "failed",
                "閿欒",
                "Error",
                "璺宠繃鏈鎵ц",
                "skip publish schedule",
                "[Publish][Skipped]",
            ]
        ):
            key_lines.append(s)
    if key_lines:
        lines.append("")
        lines.append("关键输出:")
        lines.extend(key_lines[-8:])
    return "\n".join(lines)


def _normalize_prefixes(raw_values: list[str]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        for part in re.split(r"[,\n\r;]+", str(raw or "")):
            token = str(part or "").strip()
            if not token:
                continue
            key = token.lower()
            if key in seen:
                continue
            seen.add(key)
            values.append(token)
    return values


def _normalize_command_key(text: str) -> str:
    token = str(text or "").replace("\u3000", " ").strip()
    if not token:
        return ""
    token = re.sub(r"\s+", " ", token)
    lowered = token.lower().replace("\ufe0f", "")
    shortcut_map = {
        "首页": "首页",
        "🇨🇳 国内即采即发": "国内即采即发",
        "国内即采即发": "国内即采即发",
        "🌐 海外即采即发": "海外即采即发",
        "海外即采即发": "海外即采即发",
        "⚡ 即采即发": "即采即发",
        "✨ 即采即发": "即采即发",
        "📍 进度": "进程查看",
        "📍 进程查看": "进程查看",
        "🔐 登录": "平台登录",
        "🔐 平台登录": "平台登录",
        "💬 点赞评论": "点赞评论",
    }
    for raw, normalized in shortcut_map.items():
        if lowered == raw.lower().replace("\ufe0f", ""):
            return normalized.lower()
    return lowered


def _resolve_platforms_expr(raw: str) -> Optional[list[str]]:
    text = str(raw or "").strip()
    if not text:
        return PUBLISH_PLATFORM_ORDER.copy()
    normalized = _normalize_command_key(text).replace(" ", "")
    if normalized in ALL_PLATFORM_ALIAS_SET:
        return PUBLISH_PLATFORM_ORDER.copy()
    tokenized = re.sub(r"[锛屻€亅/]+", ",", text)
    items = [x.strip() for x in re.split(r"[\s,]+", tokenized) if x.strip()]
    if not items:
        items = [text]
    resolved: list[str] = []
    seen: set[str] = set()
    for item in items:
        key = _normalize_command_key(item).replace(" ", "")
        if not key:
            continue
        if key in ALL_PLATFORM_ALIAS_SET:
            return PUBLISH_PLATFORM_ORDER.copy()
        mapped = PUBLISH_PLATFORM_ALIAS_MAP.get(key)
        if not mapped:
            return None
        if mapped in seen:
            continue
        seen.add(mapped)
        resolved.append(mapped)
    return resolved or None


def _parse_optional_count(raw: str) -> Optional[int]:
    tail = str(raw or "").strip()
    if not tail:
        return None
    m = re.fullmatch(r"(?is)(\d+)\s*(?:个|条|个视频|条视频)?", tail)
    if not m:
        return None
    return max(1, int(m.group(1)))


def _normalize_profile_name(raw: str) -> str:
    token = str(raw or "").strip()
    return token or DEFAULT_PROFILE


def _resolve_default_profile_name(
    *,
    repo_root: Path,
    cli_default_profile: str = "",
    profile_config_path: Optional[Path] = None,
) -> str:
    cli_profile = str(cli_default_profile or "").strip()
    if cli_profile:
        return cli_profile
    resolved_profile_config_path = (
        profile_config_path.resolve()
        if profile_config_path is not None
        else (repo_root / DEFAULT_PROFILE_CONFIG_REL).resolve()
    )
    if not resolved_profile_config_path.exists():
        return DEFAULT_PROFILE
    try:
        payload = json.loads(resolved_profile_config_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return DEFAULT_PROFILE
    if not isinstance(payload, dict):
        return DEFAULT_PROFILE
    default_profile = str(payload.get("default_profile") or "").strip()
    return default_profile or DEFAULT_PROFILE


def _parse_count_and_profile(raw: str) -> tuple[Optional[int], str, str]:
    tail = str(raw or "").strip()
    if not tail:
        return None, "", ""
    parts = [x for x in tail.split() if x]
    if not parts:
        return None, "", ""

    count_head = _parse_optional_count(parts[0])
    if count_head is not None:
        if len(parts) == 1:
            return count_head, "", ""
        if len(parts) == 2:
            return count_head, parts[1], ""
        return None, "", "too_many_args"

    if len(parts) == 1:
        return None, parts[0], ""
    return None, "", "bad_count_or_profile"


def _parse_schedule_platform_and_profile(raw: str) -> tuple[str, str, str]:
    tail = str(raw or "").strip()
    if not tail:
        return "", "", ""
    parts = [x for x in tail.split() if x]
    if len(parts) == 1:
        maybe_platform = _resolve_platforms_expr(parts[0])
        if maybe_platform:
            return parts[0], "", ""
        return "", parts[0], ""
    if len(parts) == 2:
        return parts[0], parts[1], ""
    return "", "", "too_many_args"


def _parse_immediate_publish_request(raw: str) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {"matched": False}

    m_all = re.fullmatch(
        r"(?is)(立即分发|分发一轮|开始分发|分发任务|分发|立即发布|所有平台发布|全平台发布|全部平台发布|所有平台分发|全平台分发|全部平台分发)\s*(.*)",
        text,
    )
    if m_all:
        count_tail = str(m_all.group(2) or "").strip()
        count = _parse_optional_count(count_tail)
        if count_tail and count is None:
            return {"matched": True, "error": "发布数量格式错误，应为正整数。示例：立即分发 3"}
        return {"matched": True, "platforms": PUBLISH_PLATFORM_ORDER.copy(), "count": count}

    m_front = re.fullmatch(r"(?is)(?:绔嬪嵆)?鍙戝竷\s*([^\d\s]+)\s*(.*)", text)
    m_back = re.fullmatch(r"(?is)([^\d\s]+)\s*(?:绔嬪嵆)?鍙戝竷\s*(.*)", text)
    for m in [m_front, m_back]:
        if not m:
            continue
        platform_expr = str(m.group(1) or "").strip()
        count_tail = str(m.group(2) or "").strip()
        platforms = _resolve_platforms_expr(platform_expr)
        if not platforms:
            return {"matched": True, "error": f"不支持的平台：{platform_expr}"}
        count = _parse_optional_count(count_tail)
        if count_tail and count is None:
            return {"matched": True, "error": "发布数量格式错误，应为正整数。示例：发布抖音 2"}
        return {"matched": True, "platforms": platforms, "count": count}

    return {"matched": False}


def _parse_scheduled_publish_request(raw: str) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text or ("定时发布" not in text):
        return {"matched": False}
    tail = re.sub(r"定时发布", " ", text, count=1).strip()
    if not tail:
        return {
            "matched": True,
            "error": "定时发布需要分钟参数。示例：定时发布 30 所有平台 / 定时发布 20 抖音",
        }
    m_minutes = re.search(r"(\d+)\s*(?:分|分钟)?", tail)
    if not m_minutes:
        return {"matched": True, "error": "未识别到分钟参数。示例：定时发布 30 抖音"}
    minutes = int(m_minutes.group(1))
    if minutes <= 0:
        return {"matched": True, "error": "分钟必须大于 0。"}
    platform_expr = (tail[: m_minutes.start()] + " " + tail[m_minutes.end() :]).strip()
    platform_expr = platform_expr.replace("分钟", " ").replace("分", " ").strip()
    platforms = _resolve_platforms_expr(platform_expr) if platform_expr else PUBLISH_PLATFORM_ORDER.copy()
    if not platforms:
        return {"matched": True, "error": f"不支持的平台：{platform_expr}"}
    return {"matched": True, "platforms": platforms, "minutes": minutes}


def _platforms_to_text(platforms: list[str]) -> str:
    return "/".join(PUBLISH_PLATFORM_DISPLAY.get(p, p) for p in platforms)


def _run_collect_once(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str = DEFAULT_PROFILE,
    telegram_chat_id: str = "",
    media_kind: str = "video",
    count: Optional[int] = None,
) -> Dict[str, Any]:
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    extra_args: list[str] = ["--require-text-keyword-match"]
    if normalized_media_kind == "image":
        extra_args += [
            "--collect-media-kind",
            "image",
            "--xiaohongshu-extra-images-per-run",
            str(max(1, DEFAULT_XIAOHONGSHU_EXTRA_IMAGES_PER_RUN if "DEFAULT_XIAOHONGSHU_EXTRA_IMAGES_PER_RUN" in globals() else 3)),
        ]
    return _run_unified_once(
        repo_root=repo_root,
        workspace=workspace,
        timeout_seconds=timeout_seconds,
        mode="collect",
        profile=profile,
        telegram_chat_id=telegram_chat_id,
        count=count,
        extra_args=extra_args,
    )


def _run_unified_once(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    mode: str,
    profile: str = DEFAULT_PROFILE,
    platforms: Optional[list[str]] = None,
    telegram_chat_id: str = "",
    count: Optional[int] = None,
    schedule_minutes: Optional[int] = None,
    extra_args: Optional[list[str]] = None,
    pipeline_priority: str = "normal",
    proxy_override: Optional[str] = None,
    use_system_proxy_override: Optional[bool] = None,
) -> Dict[str, Any]:
    script = repo_root / DEFAULT_UNIFIED_RUNNER_REL
    if proxy_override is not None or use_system_proxy_override is not None:
        resolved_proxy = str(proxy_override or "").strip()
        resolved_use_system_proxy = bool(use_system_proxy_override) and not resolved_proxy
    else:
        resolved_proxy, resolved_use_system_proxy = _resolve_worker_network_mode()
    python_exe = str(sys.executable or "").strip() or "python"
    cmd: list[str] = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script),
        "-PythonExe",
        python_exe,
        "-Mode",
        str(mode),
        "-RepoRoot",
        str(repo_root),
        "-Workspace",
        str(workspace),
        "-Profile",
        _normalize_profile_name(profile),
    ]
    priority_value = str(pipeline_priority or "").strip().lower()
    if priority_value in {"high"}:
        cmd += ["-Priority", priority_value]
    if resolved_proxy:
        cmd += ["-Proxy", resolved_proxy]
    elif resolved_use_system_proxy:
        cmd += ["-UseSystemProxy"]
    notify_chat_id = str(telegram_chat_id or "").strip()
    if notify_chat_id:
        cmd += ["-TelegramChatId", notify_chat_id]
    if count is not None:
        cmd += ["-Limit", str(max(1, int(count)))]
    if platforms:
        cmd += ["-UploadPlatforms", ",".join(platforms)]
    merged_extra_args: list[str] = []
    if (mode == "publish") and (count is not None):
        merged_extra_args += ["--non-wechat-max-videos", str(max(1, int(count)))]
    if schedule_minutes is not None:
        merged_extra_args += ["--non-wechat-random-window-minutes", str(max(1, int(schedule_minutes)))]
    if extra_args:
        merged_extra_args += [str(item) for item in extra_args if str(item or "").strip()]
    if merged_extra_args:
        cmd += ["-ExtraArgsJson", json.dumps(merged_extra_args, ensure_ascii=True)]
    return _run_cmd(cmd, timeout_seconds=timeout_seconds)


def _build_immediate_fast_x_download_args(repo_root: Optional[Path] = None) -> list[str]:
    resolved_repo_root = Path(repo_root).resolve() if repo_root is not None else Path(DEFAULT_REPO_ROOT).resolve()
    load_runtime_config = getattr(core, "_load_runtime_config", None)
    resolve_policy = getattr(core, "resolve_x_download_policy", None)
    def _normalize_fast_args(args: list[str]) -> list[str]:
        normalized = [arg for arg in args if str(arg) != "--x-download-fail-fast"]
        normalized.append("--no-x-download-fail-fast")
        return normalized
    if callable(load_runtime_config) and callable(resolve_policy):
        runtime_config = load_runtime_config(str(_default_runtime_config_path(resolved_repo_root)))
        args = resolve_policy(runtime_config=runtime_config).to_cli_args()
        return _normalize_fast_args(args)
    from cybercar import engine as cybercar_engine

    runtime_config = cybercar_engine._load_runtime_config(str(_default_runtime_config_path(resolved_repo_root)))
    args = cybercar_engine.resolve_x_download_policy(runtime_config=runtime_config).to_cli_args()
    return _normalize_fast_args(args)


def _override_cli_arg(args: list[str], option: str, value: str) -> list[str]:
    normalized_option = str(option or "").strip()
    if not normalized_option:
        return [str(item) for item in args]
    rendered = [str(item) for item in args]
    filtered: list[str] = []
    skip_next = False
    removed = False
    for idx, item in enumerate(rendered):
        if skip_next:
            skip_next = False
            continue
        if item == normalized_option:
            removed = True
            if idx + 1 < len(rendered):
                skip_next = True
            continue
        filtered.append(item)
    filtered += [normalized_option, str(value)]
    return filtered


def _worker_system_proxy_available() -> bool:
    use_system_proxy = str(_env_first("CYBERCAR_USE_SYSTEM_PROXY", default="")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if use_system_proxy:
        return True
    _proxy_server, system_enabled = _detect_windows_manual_proxy()
    return bool(system_enabled)


def _build_child_worker_env(repo_root: Optional[Path] = None) -> dict[str, str]:
    env = os.environ.copy()
    resolved_proxy, resolved_use_system_proxy = _resolve_worker_network_mode()
    if resolved_proxy:
        env["CYBERCAR_PROXY"] = resolved_proxy
        env.pop("CYBERCAR_USE_SYSTEM_PROXY", None)
    elif resolved_use_system_proxy:
        env["CYBERCAR_USE_SYSTEM_PROXY"] = "1"
        env.pop("CYBERCAR_PROXY", None)
    if repo_root is not None:
        repo_root_path = Path(repo_root).resolve()
        src_path = repo_root_path / "src"
        existing = str(env.get("PYTHONPATH") or "").strip()
        path_parts = [str(src_path)]
        if existing:
            path_parts.append(existing)
        env["PYTHONPATH"] = os.pathsep.join(path_parts)
    return env


def _spawn_home_action_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    action: str,
    value: str,
    task_key: str,
    immediate_test_mode: bool = False,
) -> Dict[str, Any]:
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    action_token = str(action or "").strip().lower() or "job"
    log_path = log_dir / f"home_action_{action_token}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    repo_root_path = Path(repo_root).resolve()
    module_root = repo_root_path
    explicit_bot_token = str(telegram_bot_token or "").strip()
    explicit_chat_id = str(telegram_chat_id or "").strip()
    task_identifier = _build_task_identifier(action=action_token, value=value, log_path=str(log_path))
    menu_label = _menu_breadcrumb_for_action(action_token, value)
    cmd = [
        sys.executable,
        "-m",
        "Collection.cybercar.cybercar_video_capture_and_publishing_module.telegram_command_worker",
        "--repo-root",
        str(repo_root_path),
        "--workspace",
        str(workspace),
        "--timeout-seconds",
        str(max(60, int(timeout_seconds))),
        "--default-profile",
        _normalize_profile_name(profile),
        "--run-home-action-job",
        "--home-action",
        action_token,
        "--home-action-value",
        str(value or "").strip(),
        "--home-action-task-key",
        str(task_key or "").strip(),
    ]
    if immediate_test_mode:
        cmd.append("--immediate-test-mode")
    if explicit_bot_token:
        cmd += ["--telegram-bot-token", explicit_bot_token]
    if explicit_chat_id:
        cmd += ["--telegram-chat-id", explicit_chat_id]
    child_env = _build_child_worker_env(repo_root_path)
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    with log_path.open("a", encoding="utf-8") as stream:
        _write_task_log_header(
            stream,
            task_identifier=task_identifier,
            menu_label=menu_label,
            log_path=str(log_path),
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(module_root),
            env=child_env,
            stdout=stream,
            stderr=stream,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    return {
        "ok": True,
        "pid": int(proc.pid),
        "log_path": str(log_path),
        "action": action_token,
    }


def _spawn_comment_reply_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    platform: str,
    post_limit: int,
) -> Dict[str, Any]:
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"comment_reply_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    repo_root_path = Path(repo_root).resolve()
    module_root = repo_root_path
    explicit_bot_token = str(telegram_bot_token or "").strip()
    explicit_chat_id = str(telegram_chat_id or "").strip()
    normalized_platform, normalized_limit = _parse_comment_reply_request_value(f"{platform}:{post_limit}")
    action_value = _normalize_comment_reply_request_value(
        f"{normalized_platform}:{normalized_limit}" if normalized_platform == "wechat" else normalized_platform
    )
    task_identifier = _build_task_identifier(action="comment_reply_run", value=action_value, log_path=str(log_path))
    menu_label = _menu_breadcrumb_for_action("comment_reply_run", action_value)
    cmd = [
        sys.executable,
        "-m",
        "Collection.cybercar.cybercar_video_capture_and_publishing_module.telegram_command_worker",
        "--repo-root",
        str(repo_root_path),
        "--workspace",
        str(workspace),
        "--timeout-seconds",
        str(max(60, int(timeout_seconds))),
        "--default-profile",
        _normalize_profile_name(profile),
        "--run-comment-reply-job",
        "--comment-reply-post-limit",
        str(max(1, int(post_limit))),
        "--comment-reply-platform",
        normalized_platform,
    ]
    if explicit_bot_token:
        cmd += ["--telegram-bot-token", explicit_bot_token]
    if explicit_chat_id:
        cmd += ["--telegram-chat-id", explicit_chat_id]
    child_env = _build_child_worker_env(repo_root_path)
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    with log_path.open("a", encoding="utf-8") as stream:
        _write_task_log_header(
            stream,
            task_identifier=task_identifier,
            menu_label=menu_label,
            log_path=str(log_path),
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(module_root),
            env=child_env,
            stdout=stream,
            stderr=stream,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    return {
        "ok": True,
        "pid": int(proc.pid),
        "log_path": str(log_path),
        "post_limit": int(post_limit),
    }


def _spawn_collect_publish_latest_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    candidate_limit: int,
    media_kind: str = "video",
    immediate_test_mode: bool = False,
) -> Dict[str, Any]:
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"collect_publish_latest_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    repo_root_path = Path(repo_root).resolve()
    module_root = repo_root_path
    explicit_bot_token = str(telegram_bot_token or "").strip()
    explicit_chat_id = str(telegram_chat_id or "").strip()
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    task_identifier = _build_task_identifier(
        action="collect_publish_latest",
        value=f"{normalized_media_kind}:{max(1, int(candidate_limit))}",
        log_path=str(log_path),
    )
    menu_label = _menu_breadcrumb_for_action("collect_publish_latest", f"{normalized_media_kind}:{max(1, int(candidate_limit))}")
    cmd = [
        sys.executable,
        "-m",
        "Collection.cybercar.cybercar_video_capture_and_publishing_module.telegram_command_worker",
        "--repo-root",
        str(repo_root_path),
        "--workspace",
        str(workspace),
        "--timeout-seconds",
        str(max(60, int(timeout_seconds))),
        "--default-profile",
        _normalize_profile_name(profile),
        "--run-collect-publish-latest-job",
        "--job-candidate-limit",
        str(max(1, int(candidate_limit))),
        "--collect-publish-media-kind",
        normalized_media_kind,
    ]
    if immediate_test_mode:
        cmd.append("--immediate-test-mode")
    if explicit_bot_token:
        cmd += ["--telegram-bot-token", explicit_bot_token]
    if explicit_chat_id:
        cmd += ["--telegram-chat-id", explicit_chat_id]
    child_env = _build_child_worker_env(repo_root_path)
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    with log_path.open("a", encoding="utf-8") as stream:
        _write_task_log_header(
            stream,
            task_identifier=task_identifier,
            menu_label=menu_label,
            log_path=str(log_path),
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(module_root),
            env=child_env,
            stdout=stream,
            stderr=stream,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    return {
        "ok": True,
        "pid": int(proc.pid),
        "log_path": str(log_path),
        "candidate_limit": int(candidate_limit),
        "media_kind": normalized_media_kind,
    }


def _spawn_immediate_publish_item_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    immediate_test_mode: bool = False,
) -> Dict[str, Any]:
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"immediate_publish_item_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    repo_root_path = Path(repo_root).resolve()
    module_root = repo_root_path
    explicit_bot_token = str(telegram_bot_token or "").strip()
    explicit_chat_id = str(telegram_chat_id or "").strip()
    task_identifier = _build_task_identifier(action="collect_publish_latest", item_id=str(item_id or "").strip())
    cmd = [
        sys.executable,
        "-m",
        "Collection.cybercar.cybercar_video_capture_and_publishing_module.telegram_command_worker",
        "--repo-root",
        str(repo_root_path),
        "--workspace",
        str(workspace),
        "--timeout-seconds",
        str(max(60, int(timeout_seconds))),
        "--default-profile",
        _normalize_profile_name(profile),
        "--run-immediate-publish-item-job",
        "--publish-item-id",
        str(item_id or "").strip(),
    ]
    if immediate_test_mode:
        cmd.append("--immediate-test-mode")
    if explicit_bot_token:
        cmd += ["--telegram-bot-token", explicit_bot_token]
    if explicit_chat_id:
        cmd += ["--telegram-chat-id", explicit_chat_id]
    child_env = _build_child_worker_env(repo_root_path)
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    with log_path.open("a", encoding="utf-8") as stream:
        _write_task_log_header(
            stream,
            task_identifier=task_identifier,
            menu_label="即采即发",
            log_path=str(log_path),
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(module_root),
            env=child_env,
            stdout=stream,
            stderr=stream,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    return {
        "ok": True,
        "pid": int(proc.pid),
        "log_path": str(log_path),
        "item_id": str(item_id or "").strip(),
    }


def _spawn_immediate_collect_item_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    immediate_test_mode: bool = False,
) -> Dict[str, Any]:
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"immediate_collect_item_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    repo_root_path = Path(repo_root).resolve()
    module_root = repo_root_path
    explicit_bot_token = str(telegram_bot_token or "").strip()
    explicit_chat_id = str(telegram_chat_id or "").strip()
    task_identifier = _build_task_identifier(action="collect_publish_latest", item_id=str(item_id or "").strip())
    cmd = [
        sys.executable,
        "-m",
        "Collection.cybercar.cybercar_video_capture_and_publishing_module.telegram_command_worker",
        "--repo-root",
        str(repo_root_path),
        "--workspace",
        str(workspace),
        "--timeout-seconds",
        str(max(60, int(timeout_seconds))),
        "--default-profile",
        _normalize_profile_name(profile),
        "--run-immediate-collect-item-job",
        "--publish-item-id",
        str(item_id or "").strip(),
    ]
    if immediate_test_mode:
        cmd.append("--immediate-test-mode")
    if explicit_bot_token:
        cmd += ["--telegram-bot-token", explicit_bot_token]
    if explicit_chat_id:
        cmd += ["--telegram-chat-id", explicit_chat_id]
    child_env = _build_child_worker_env(repo_root_path)
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    with log_path.open("a", encoding="utf-8") as stream:
        _write_task_log_header(
            stream,
            task_identifier=task_identifier,
            menu_label="即采即发",
            log_path=str(log_path),
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(module_root),
            env=child_env,
            stdout=stream,
            stderr=stream,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    return {"ok": True, "pid": int(proc.pid), "log_path": str(log_path), "item_id": str(item_id or "").strip()}


def _spawn_immediate_publish_platform_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    platform: str,
) -> Dict[str, Any]:
    platform_token = str(platform or "").strip().lower()
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"immediate_publish_{platform_token}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    repo_root_path = Path(repo_root).resolve()
    module_root = repo_root_path
    explicit_bot_token = str(telegram_bot_token or "").strip()
    explicit_chat_id = str(telegram_chat_id or "").strip()
    task_identifier = _build_task_identifier(action="collect_publish_latest", item_id=str(item_id or "").strip())
    menu_label = "即采即发 / 视频 / " + _menu_platform_label(platform_token)
    cmd = [
        sys.executable,
        "-m",
        "Collection.cybercar.cybercar_video_capture_and_publishing_module.telegram_command_worker",
        "--repo-root",
        str(repo_root_path),
        "--workspace",
        str(workspace),
        "--timeout-seconds",
        str(max(60, int(timeout_seconds))),
        "--default-profile",
        _normalize_profile_name(profile),
        "--run-immediate-publish-platform-job",
        "--publish-item-id",
        str(item_id or "").strip(),
        "--publish-platform",
        platform_token,
    ]
    if explicit_bot_token:
        cmd += ["--telegram-bot-token", explicit_bot_token]
    if explicit_chat_id:
        cmd += ["--telegram-chat-id", explicit_chat_id]
    child_env = _build_child_worker_env(repo_root_path)
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    with log_path.open("a", encoding="utf-8") as stream:
        _write_task_log_header(
            stream,
            task_identifier=task_identifier,
            menu_label=menu_label,
            log_path=str(log_path),
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(module_root),
            env=child_env,
            stdout=stream,
            stderr=stream,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    return {
        "ok": True,
        "pid": int(proc.pid),
        "log_path": str(log_path),
        "item_id": str(item_id or "").strip(),
        "platform": platform_token,
    }


def _queue_collect_home_action_from_command(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    chat_id: str,
    username: str,
    media_kind: str = "video",
) -> str:
    normalized_profile = _normalize_profile_name(profile)
    normalized_media_kind = _parse_media_kind_value(media_kind)
    claim = _claim_home_action_task(
        workspace=workspace,
        chat_id=chat_id,
        action="collect_now",
        value=normalized_media_kind,
        profile=normalized_profile,
        username=username,
    )
    task = claim.get("task") if isinstance(claim.get("task"), dict) else {}
    if not bool(claim.get("accepted")):
        return _describe_home_action_task(task) or "采集任务已在执行，请稍候。"

    task_key = str(claim.get("task_key") or "").strip()
    raw_result = _spawn_home_action_job(
        repo_root=repo_root,
        workspace=workspace,
        timeout_seconds=timeout_seconds,
        profile=normalized_profile,
        telegram_bot_identifier=telegram_bot_identifier,
        telegram_bot_token="",
        telegram_chat_id=chat_id,
        action="collect_now",
        value=normalized_media_kind,
        task_key=task_key,
    )
    updated_task = _update_home_action_task(
        workspace,
        task_key,
        status="running",
        detail="采集任务已进入后台队列，完成后会回传采集结果。",
        log_path=str(raw_result.get("log_path") or "").strip(),
        pid=int(raw_result.get("pid") or 0),
    )
    return _describe_home_action_task(updated_task) or "采集任务已进入后台队列。"


def _run_distribution_once(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    platforms: list[str],
    profile: str = DEFAULT_PROFILE,
    telegram_chat_id: str = "",
    count: Optional[int] = None,
    schedule_minutes: Optional[int] = None,
    collect_only: bool = False,
    publish_only: bool = False,
    extra_args: Optional[list[str]] = None,
    media_kind: str = "video",
) -> Dict[str, Any]:
    mode = "pipeline"
    if collect_only:
        mode = "collect"
    elif publish_only:
        mode = "publish"
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    target_platforms = list(platforms or [])
    merged_extra_args = [str(item) for item in (extra_args or []) if str(item or "").strip()]
    if normalized_media_kind == "image":
        target_platforms = list(platforms or _collect_publish_target_platforms("image"))
        merged_extra_args += [
            "--collect-media-kind",
            "image",
            "--xiaohongshu-extra-images-per-run",
            "6",
        ]
    lock_wait_deadline = _lock_wait_deadline(
        max_wait_seconds=float(DEFAULT_IMMEDIATE_PUBLISH_LOCK_MAX_WAIT_SECONDS),
        timeout_seconds=timeout_seconds,
        retry_seconds=float(DEFAULT_IMMEDIATE_PUBLISH_LOCK_RETRY_SECONDS),
    )
    while True:
        result = _run_unified_once(
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            mode=mode,
            profile=profile,
            platforms=target_platforms,
            telegram_chat_id=telegram_chat_id,
            count=count,
            schedule_minutes=schedule_minutes,
            extra_args=merged_extra_args,
        )
        if not publish_only or not _is_pipeline_lock_retry_reason("", result):
            return result
        remaining_wait_seconds = lock_wait_deadline - time.monotonic()
        if remaining_wait_seconds <= 0:
            return result
        time.sleep(min(float(DEFAULT_IMMEDIATE_PUBLISH_LOCK_RETRY_SECONDS), max(1.0, remaining_wait_seconds)))


def _run_direct_xiaohongshu_image_publish(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str,
    telegram_chat_id: str,
) -> Dict[str, Any]:
    started = time.time()
    runner, core = _load_runtime_modules()
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"direct_publish_xiaohongshu_image_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    image_targets = sorted(
        _list_processed_media(workspace, media_kind="image").values(),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not image_targets:
        return {
            "ok": False,
            "code": 2,
            "stdout": f"日志: {log_path.name}",
            "stderr": "未找到可发布图片素材（2_Processed_Images 为空）。",
            "elapsed": time.time() - started,
        }

    target = image_targets[0]
    args = _build_immediate_publish_args(
        runner=runner,
        workspace=workspace,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
    )
    args.upload_platforms = "xiaohongshu"
    args.collect_media_kind = "image"
    args.xiaohongshu_allow_image = True
    args.no_publish_skip_notify = True
    email_settings = runner._build_email_settings(args)
    workspace_ctx = core.init_workspace(str(workspace))
    target_meta = core._resolve_processed_video_metadata(workspace_ctx, target)
    source_url = str((target_meta or {}).get("source_url") or "").strip()
    ctx = _build_immediate_cycle_context(
        core=core,
        runner=runner,
        repo_root=repo_root,
        workspace=workspace,
        args=args,
        target=target,
        candidate_url=source_url,
        profile=profile,
    )
    events: list[Any] = []
    buffer = io.StringIO()

    def _publish_once_under_lock() -> bool:
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            return bool(
                runner._publish_once(
                    ctx,
                    args,
                    email_settings,
                    "xiaohongshu",
                    target,
                    "direct_xiaohongshu_image_publish",
                    events,
                )
            )

    try:
        publish_ok = bool(
            _with_platform_lock(
                workspace,
                "xiaohongshu",
                _publish_once_under_lock,
                timeout_seconds=_normalize_blocking_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1),
            )
        )
    except Exception as exc:
        output_text = buffer.getvalue().strip()
        final_text = output_text + ("\n" if output_text else "") + f"日志: {log_path.name}\n错误: {exc}"
        final_text = _prepend_task_log_header(
            final_text,
            task_identifier=_build_task_identifier(
                action="publish_run",
                value="image:xiaohongshu",
                log_path=str(log_path),
            ),
            menu_label="立即发布 / 图片 / 小红书",
            log_path=str(log_path),
        )
        log_path.write_text(final_text, encoding="utf-8")
        return {
            "ok": False,
            "code": 2,
            "stdout": output_text,
            "stderr": f"日志: {log_path.name}\n{exc}",
            "elapsed": time.time() - started,
        }

    output_text = buffer.getvalue().strip()
    event = events[-1] if events else None
    extra_lines = [f"日志: {log_path.name}", f"素材: {target.name}"]
    if event is not None:
        event_result = str(getattr(event, "result", "") or "").strip()
        event_error = str(getattr(event, "error", "") or "").strip()
        if event_result:
            extra_lines.append(f"平台结果: {event_result}")
        if event_error:
            extra_lines.append(f"平台错误: {event_error}")
    final_text = output_text + ("\n" if output_text else "") + "\n".join(extra_lines)
    final_text = _prepend_task_log_header(
        final_text,
        task_identifier=_build_task_identifier(
            action="publish_run",
            value="image:xiaohongshu",
            log_path=str(log_path),
        ),
        menu_label="立即发布 / 图片 / 小红书",
        log_path=str(log_path),
    )
    log_path.write_text(final_text, encoding="utf-8")

    success = bool(event is not None and bool(getattr(event, "success", False)) and publish_ok)
    duplicate_skip = bool(event is not None and str(getattr(event, "result", "") or "").strip().lower() == "skipped_duplicate")
    if duplicate_skip:
        final_text = final_text + "\n[Publish][Skipped] duplicate target blocked"
        log_path.write_text(final_text, encoding="utf-8")
    return {
        "ok": bool(success or duplicate_skip),
        "code": 0 if (success or duplicate_skip) else 2,
        "stdout": final_text,
        "stderr": "" if (success or duplicate_skip) else (str(getattr(event, "error", "") or "").strip() or "小红书图片发布未确认成功。"),
        "elapsed": time.time() - started,
    }


def _run_direct_kuaishou_image_publish(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str,
    telegram_chat_id: str,
) -> Dict[str, Any]:
    started = time.time()
    runner, core = _load_runtime_modules()
    log_dir = (workspace / DEFAULT_LOG_SUBDIR).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"direct_publish_kuaishou_image_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    image_targets = sorted(
        _list_processed_media(workspace, media_kind="image").values(),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not image_targets:
        return {
            "ok": False,
            "code": 2,
            "stdout": f"日志: {log_path.name}",
            "stderr": "未找到可发布图片素材（2_Processed_Images 为空）。",
            "elapsed": time.time() - started,
        }

    target = image_targets[0]
    args = _build_immediate_publish_args(
        runner=runner,
        workspace=workspace,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
    )
    args.upload_platforms = "kuaishou"
    args.collect_media_kind = "image"
    args.xiaohongshu_allow_image = True
    args.no_publish_skip_notify = True
    email_settings = runner._build_email_settings(args)
    workspace_ctx = core.init_workspace(str(workspace))
    target_meta = core._resolve_processed_video_metadata(workspace_ctx, target)
    source_url = str((target_meta or {}).get("source_url") or "").strip()
    ctx = _build_immediate_cycle_context(
        core=core,
        runner=runner,
        repo_root=repo_root,
        workspace=workspace,
        args=args,
        target=target,
        candidate_url=source_url,
        profile=profile,
    )
    events: list[Any] = []
    buffer = io.StringIO()

    def _publish_once_under_lock() -> bool:
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            return bool(
                runner._publish_once(
                    ctx,
                    args,
                    email_settings,
                    "kuaishou",
                    target,
                    "direct_kuaishou_image_publish",
                    events,
                )
            )

    try:
        publish_ok = bool(
            _with_platform_lock(
                workspace,
                "kuaishou",
                _publish_once_under_lock,
                timeout_seconds=_normalize_blocking_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1),
            )
        )
    except Exception as exc:
        output_text = buffer.getvalue().strip()
        final_text = output_text + ("\n" if output_text else "") + f"日志: {log_path.name}\n错误: {exc}"
        final_text = _prepend_task_log_header(
            final_text,
            task_identifier=_build_task_identifier(
                action="publish_run",
                value="image:kuaishou",
                log_path=str(log_path),
            ),
            menu_label="立即发布 / 图片 / 快手",
            log_path=str(log_path),
        )
        log_path.write_text(final_text, encoding="utf-8")
        return {
            "ok": False,
            "code": 2,
            "stdout": output_text,
            "stderr": f"日志: {log_path.name}\n{exc}",
            "elapsed": time.time() - started,
        }

    output_text = buffer.getvalue().strip()
    event = events[-1] if events else None
    extra_lines = [f"日志: {log_path.name}", f"素材: {target.name}"]
    if event is not None:
        event_result = str(getattr(event, "result", "") or "").strip()
        event_error = str(getattr(event, "error", "") or "").strip()
        if event_result:
            extra_lines.append(f"平台结果: {event_result}")
        if event_error:
            extra_lines.append(f"平台错误: {event_error}")
    final_text = output_text + ("\n" if output_text else "") + "\n".join(extra_lines)
    final_text = _prepend_task_log_header(
        final_text,
        task_identifier=_build_task_identifier(
            action="publish_run",
            value="image:kuaishou",
            log_path=str(log_path),
        ),
        menu_label="立即发布 / 图片 / 快手",
        log_path=str(log_path),
    )
    log_path.write_text(final_text, encoding="utf-8")

    success = bool(event is not None and bool(getattr(event, "success", False)) and publish_ok)
    duplicate_skip = bool(event is not None and str(getattr(event, "result", "") or "").strip().lower() == "skipped_duplicate")
    if duplicate_skip:
        final_text = final_text + "\n[Publish][Skipped] duplicate target blocked"
        log_path.write_text(final_text, encoding="utf-8")
    return {
        "ok": bool(success or duplicate_skip),
        "code": 0 if (success or duplicate_skip) else 2,
        "stdout": final_text,
        "stderr": "" if (success or duplicate_skip) else (str(getattr(event, "error", "") or "").strip() or "快手图片发布未确认成功。"),
        "elapsed": time.time() - started,
    }


def _discover_latest_live_candidates(
    *,
    repo_root: Path,
    timeout_seconds: int,
    profile: str,
    candidate_limit: int = DEFAULT_IMMEDIATE_CANDIDATE_LIMIT,
    discovery_limit: Optional[int] = None,
    allow_search_inferred_match: bool = False,
    include_images: bool = False,
    source_platforms: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    del timeout_seconds
    try:
        from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as core
    except Exception:
        import main as core  # type: ignore

    runtime_config = core._load_runtime_config(str(_default_runtime_config_path(repo_root)))
    profile_config = _load_profile_config_payload(repo_root)
    profile_payload = _resolve_profile_payload(repo_root=repo_root, profile=profile, profile_config=profile_config)
    resolved_source_platforms = _normalize_collect_source_platforms(source_platforms)
    if not resolved_source_platforms:
        resolved_source_platforms = _resolve_collect_publish_source_platforms(
            repo_root=repo_root,
            profile=profile,
            profile_payload=profile_payload,
            profile_config=profile_config,
        )
    keyword = str((profile_payload or {}).get("keyword", "") or runtime_config.get("keyword", "") or getattr(core, "DEFAULT_KEYWORD", "Cybertruck")).strip()
    debug_port = int(os.getenv("CYBERCAR_CHROME_DEBUG_PORT", str(getattr(core, "DEFAULT_PORT", 9333))))
    requested_limit = max(1, int(candidate_limit))
    resolved_discovery_limit = max(
        requested_limit,
        int(discovery_limit if discovery_limit is not None else requested_limit),
    )
    chrome_path = str(os.getenv("CYBERCAR_CHROME_PATH", "") or "").strip() or None
    chrome_user_data_dir = str(
        os.getenv(
            "CYBERCAR_CHROME_USER_DATA_DIR",
            str(getattr(core, "DEFAULT_CHROME_USER_DATA_DIR", "")),
        )
        or ""
    ).strip()
    per_source_candidates: list[list[dict[str, Any]]] = []
    for source_platform in resolved_source_platforms:
        if source_platform == "x":
            candidates = core.discover_x_media_candidates(
                keyword=keyword,
                url_limit=max(3, resolved_discovery_limit),
                debug_port=debug_port,
                scroll_rounds=getattr(core, "X_DISCOVERY_SCROLL_ROUNDS", 8),
                scroll_wait_seconds=getattr(core, "X_DISCOVERY_SCROLL_WAIT_SECONDS", 1.2),
                auto_open_chrome=True,
                chrome_path=chrome_path,
                chrome_user_data_dir=chrome_user_data_dir,
                include_images=include_images,
                allow_search_inferred_match=bool(allow_search_inferred_match),
                require_text_keyword_match=(not bool(allow_search_inferred_match)),
            )
            ordered_x_candidates = core._take_latest_x_candidates(
                candidates if isinstance(candidates, list) else [],
                resolved_discovery_limit,
            )
            normalized_x_candidates: list[dict[str, Any]] = []
            for item in ordered_x_candidates:
                if not isinstance(item, dict):
                    continue
                row = dict(item)
                row["source_platform"] = "x"
                normalized_x_candidates.append(row)
            per_source_candidates.append(normalized_x_candidates)
            continue
        if source_platform in {"douyin", "xiaohongshu"}:
            discovered_urls = core.discover_domestic_keyword_urls(
                source_platform,
                keyword,
                url_limit=max(3, resolved_discovery_limit),
                scroll_rounds=getattr(core, "X_DISCOVERY_SCROLL_ROUNDS", 8),
                scroll_wait_seconds=getattr(core, "X_DISCOVERY_SCROLL_WAIT_SECONDS", 1.2),
                debug_port=debug_port,
                auto_open_chrome=True,
                chrome_path=chrome_path,
                chrome_user_data_dir=chrome_user_data_dir,
            )
            platform_candidates: list[dict[str, Any]] = []
            for source_url in discovered_urls:
                normalized_url = str(source_url or "").strip()
                if not normalized_url:
                    continue
                platform_candidates.append(
                    {
                        "url": normalized_url,
                        "published_at": "",
                        "display_time": "",
                        "tweet_text": "",
                        "source_platform": source_platform,
                        "media_kind": "image" if include_images else "video",
                        "matched_keyword": keyword,
                        "match_mode": "search_result_inferred",
                        "discovery_source": f"{source_platform}_keyword_search",
                    }
                )
            per_source_candidates.append(platform_candidates)
    ordered_candidates: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    max_rows = max(len(rows) for rows in per_source_candidates) if per_source_candidates else 0
    for row_index in range(max_rows):
        for rows in per_source_candidates:
            if row_index >= len(rows):
                continue
            row = rows[row_index]
            source_url = str(row.get("url") or "").strip()
            if not source_url or source_url in seen_urls:
                continue
            seen_urls.add(source_url)
            ordered_candidates.append(row)
            if len(ordered_candidates) >= resolved_discovery_limit:
                break
        if len(ordered_candidates) >= resolved_discovery_limit:
            break
    return {
        "keyword": keyword,
        "candidates": ordered_candidates,
        "requested_limit": requested_limit,
        "discovery_limit": resolved_discovery_limit,
        "source_platforms": resolved_source_platforms,
        "source_scan_label": _collect_publish_source_scan_label(resolved_source_platforms),
    }


def _extract_processed_file_count(result: Dict[str, Any]) -> int:
    merged = "\n".join(
        part.strip()
        for part in (
            str(result.get("stdout") or ""),
            str(result.get("stderr") or ""),
        )
        if str(part or "").strip()
    )
    match = re.search(r"Processed files:\s*(\d+)", merged, flags=re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return 0
    return len(re.findall(r"\[Processor\]\s+Ready:", merged))


def _summarize_collect_publish_attempt(url: str, index: int, total: int, processed_count: int) -> str:
    return (
        f"候选 {index}/{total}：{url}\n"
        f"处理结果：{'已形成可发布素材' if processed_count > 0 else '未形成可发布素材'}"
    )


def _extract_error_preview(result: Dict[str, Any]) -> str:
    merged = "\n".join(
        part.strip()
        for part in (
            str(result.get("stderr") or ""),
            str(result.get("stdout") or ""),
        )
        if str(part or "").strip()
    )
    if not merged:
        return ""
    for line in merged.splitlines():
        text = str(line or "").strip()
        if not text:
            continue
        lower = text.lower()
        if any(token in lower for token in ("error:", "unable to", "failed", "exception", "ssl", "timeout")):
            return text[:220]
    return merged.splitlines()[-1].strip()[:220]


def _resolve_immediate_item_title(item: Dict[str, Any]) -> str:
    preview = re.sub(r"\s+", " ", str(item.get("tweet_text") or "").strip())
    if len(preview) > 120:
        preview = preview[:117].rstrip() + "..."
    if preview:
        return preview
    return "待发布媒体"


def _resolve_immediate_item_platform_text(item: Dict[str, Any], *, with_logo: bool = False) -> str:
    target_platforms = str(item.get("target_platforms") or "").strip()
    platforms = _resolve_platforms_expr(target_platforms) if target_platforms else PUBLISH_PLATFORM_ORDER.copy()
    if not platforms:
        platforms = PUBLISH_PLATFORM_ORDER.copy()
    if with_logo:
        return _platforms_to_logo_text(platforms)
    return _platforms_to_text(platforms)


def _build_card_preview_link_section(source_url: str) -> dict[str, Any]:
    link = str(source_url or "").strip()
    return {
        "title": "卡片预览链接",
        "emoji": "🔗",
        "items": ([link] if link else ["未记录卡片预览链接"]),
    }


def _build_immediate_candidate_info_section(
    item: Dict[str, Any],
    *,
    title: str = "候选信息",
    include_platform: bool = True,
) -> dict[str, Any]:
    items: list[dict[str, str]] = []
    if include_platform:
        items.append({"label": "平台", "value": _resolve_immediate_item_platform_text(item, with_logo=True)})
    items.extend(
        [
            {"label": "标题", "value": _resolve_immediate_item_title(item)},
        ]
    )
    return {
        "title": title,
        "emoji": "🎯",
        "items": items,
    }


def _build_immediate_task_overview_section(
    *,
    requested_limit: int,
    platforms: Optional[list[str]] = None,
    discovery_limit: Optional[int] = None,
    discovered_count: Optional[int] = None,
    sent_count: Optional[int] = None,
    reused_count: Optional[int] = None,
    skipped_count: Optional[int] = None,
    collapsed_count: Optional[int] = None,
    discovery_rounds: Optional[int] = None,
) -> dict[str, Any]:
    resolved_platforms = list(platforms or PUBLISH_PLATFORM_ORDER.copy())
    items: list[Any] = [
        {"label": "目标平台", "value": _platforms_to_logo_text(resolved_platforms)},
        {"label": "候选目标", "value": f"{max(1, int(requested_limit))} 条"},
    ]
    if discovery_limit is not None:
        items.append({"label": "扩展扫描", "value": f"{max(1, int(discovery_limit))} 条"})
    if discovered_count is not None:
        items.append({"label": "已发现候选", "value": f"{max(0, int(discovered_count))} 条"})
    if sent_count is not None:
        items.append({"label": "已发预审", "value": f"{max(0, int(sent_count))} 条"})
    if reused_count is not None:
        items.append({"label": "沿用在审", "value": f"{max(0, int(reused_count))} 条"})
    if skipped_count is not None:
        items.append({"label": "失败/跳过", "value": f"{max(0, int(skipped_count))} 条"})
    if collapsed_count is not None:
        items.append({"label": "同题材折叠", "value": f"{max(0, int(collapsed_count))} 条"})
    if discovery_rounds is not None:
        items.append({"label": "扫描轮次", "value": f"{max(0, int(discovery_rounds))} 轮"})
    return {
        "title": "任务概览",
        "emoji": "📦",
        "items": items,
    }


def _extract_attempt_reason(result: Dict[str, Any]) -> str:
    merged = "\n".join(
        part.strip()
        for part in (
            str(result.get("stdout") or ""),
            str(result.get("stderr") or ""),
        )
        if str(part or "").strip()
    )
    if not merged:
        return ""

    duration_match = re.search(
        r"Skip by duration filter: .*?actual duration=([0-9.]+)s, expected ([0-9.]+)-([0-9.]+)s",
        merged,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if duration_match:
        actual = str(duration_match.group(1) or "").strip()
        minimum = str(duration_match.group(2) or "").strip()
        maximum = str(duration_match.group(3) or "").strip()
        return f"视频时长 {actual} 秒，不在 {minimum}-{maximum} 秒范围。"

    lower = merged.lower()
    if "already been recorded in the archive" in lower or "archive dedupe may have skipped all" in lower:
        return "素材已在历史归档中，系统未重复下载。"
    if "previous pipeline still running" in lower or ("skip run" in lower and "still running" in lower):
        return "上一条采集流程仍在处理中，请稍后再试。"
    if "unable to download json metadata" in lower:
        if "unexpected_eof_while_reading" in lower or "eof occurred in violation of protocol" in lower:
            return "X 元数据下载失败：SSL 连接被对端中断（UNEXPECTED_EOF_WHILE_READING）。"
        if "connectionreseterror(10054" in lower or "connection aborted." in lower:
            return "X 元数据下载失败：连接被远端重置。"
        if "read timed out" in lower:
            return "X 元数据下载失败：请求超时。"
        return "X 元数据下载失败，未拿到可用素材。"
    if "no new files" in lower:
        return "未下载到新的可用素材。"
    if "unable to download" in lower or "download failed" in lower:
        return "下载失败，未拿到可用素材。"
    if "timeout" in lower:
        return "处理超时，未生成可发布素材。"

    preview = _extract_error_preview(result)
    if preview:
        if re.search(r"[\u4e00-\u9fff]", preview):
            return f"处理未通过：{preview}"
        return "处理未通过，请稍后重试。"
    return ""


def _is_immediate_collect_lock_retry_reason(reason: str, result: Optional[Dict[str, Any]] = None) -> bool:
    normalized = str(reason or "").strip()
    if "上一条采集流程仍在处理中" in normalized:
        return True
    if not isinstance(result, dict):
        return False
    merged = "\n".join(
        part.strip()
        for part in (
            str(result.get("stdout") or ""),
            str(result.get("stderr") or ""),
        )
        if str(part or "").strip()
    ).lower()
    return "previous pipeline still running" in merged or ("skip run" in merged and "still running" in merged)


def _is_immediate_collect_transient_retry_reason(reason: str, result: Optional[Dict[str, Any]] = None) -> bool:
    normalized = str(reason or "").strip()
    is_x_metadata_failure = "X 元数据下载失败" in normalized
    is_generic_download_failure = "下载失败，未拿到可用素材" in normalized
    if not is_x_metadata_failure and not is_generic_download_failure:
        return False
    lower = normalized.lower()
    if "unexpected_eof_while_reading" in lower or "ssl 连接被对端中断" in normalized:
        return True
    if "连接被远端重置" in normalized or "请求超时" in normalized:
        return True
    if is_generic_download_failure:
        return True
    if not isinstance(result, dict):
        return False
    merged = "\n".join(
        part.strip().lower()
        for part in (
            str(result.get("stdout") or ""),
            str(result.get("stderr") or ""),
        )
        if str(part or "").strip()
    )
    return any(
        token in merged
        for token in (
            "unexpected_eof_while_reading",
            "eof occurred in violation of protocol",
            "connectionreseterror(10054",
            "connection aborted.",
            "read timed out",
            "unable to download",
            "download failed",
        )
    )


def _is_pipeline_lock_retry_reason(reason: str, result: Optional[Dict[str, Any]] = None) -> bool:
    normalized = str(reason or "").strip()
    if "上一条采集流程仍在处理中" in normalized or "已有发布/采集 pipeline 正在运行" in normalized:
        return True
    if not isinstance(result, dict):
        return False
    merged = "\n".join(
        part.strip()
        for part in (
            str(result.get("stdout") or ""),
            str(result.get("stderr") or ""),
        )
        if str(part or "").strip()
    ).lower()
    return "previous pipeline still running" in merged or ("skip run" in merged and "still running" in merged)


def _lock_wait_deadline(*, max_wait_seconds: float, timeout_seconds: int, retry_seconds: float) -> float:
    # Short per-attempt subprocess timeouts should not collapse the overall lock wait budget.
    wait_seconds = max(float(max_wait_seconds), float(timeout_seconds), float(retry_seconds))
    return time.monotonic() + wait_seconds


def _run_collect_publish_latest_once(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str = DEFAULT_PROFILE,
    telegram_bot_identifier: str = "",
    telegram_chat_id: str = "",
    candidate_limit: int = DEFAULT_IMMEDIATE_CANDIDATE_LIMIT,
    media_kind: str = "video",
) -> Dict[str, Any]:
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    target_platforms = _collect_publish_target_platforms(normalized_media_kind)
    source_platforms = _resolve_collect_publish_source_platforms(repo_root=repo_root, profile=profile)
    discovered = _discover_latest_live_candidates(
        repo_root=repo_root,
        timeout_seconds=timeout_seconds,
        profile=profile,
        candidate_limit=candidate_limit,
        include_images=(normalized_media_kind == "image"),
        source_platforms=source_platforms,
    )
    discovered_source_platforms = _normalize_collect_source_platforms(discovered.get("source_platforms"))
    if discovered_source_platforms:
        source_platforms = discovered_source_platforms
    candidates = discovered.get("candidates") if isinstance(discovered, dict) else []
    recent_candidates = [item for item in candidates if isinstance(item, dict) and str(item.get("url", "") or "").strip()]
    if not recent_candidates:
        return {
            "ok": False,
            "code": 2,
            "stdout": "",
            "stderr": (
                f"{_collect_publish_no_candidate_detail(source_platforms, IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(normalized_media_kind, '媒体'))}"
                f"\n关键词：{discovered.get('keyword') or profile}"
            ),
            "elapsed": 0.0,
        }
    last_result: Dict[str, Any] = {
        "ok": False,
        "code": 3,
        "stdout": "",
        "stderr": "",
        "elapsed": 0.0,
    }
    attempt_notes: list[str] = []
    total = len(recent_candidates)
    for idx, candidate in enumerate(recent_candidates, start=1):
        candidate_url = str(candidate.get("url", "") or "").strip()
        if not candidate_url:
            continue
        collect_source_platform = _resolve_candidate_collect_source_platform(candidate)
        if not collect_source_platform:
            collect_source_platform = source_platforms[0] if source_platforms else "x"
        result = _run_unified_once(
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            mode="pipeline",
            profile=profile,
            platforms=target_platforms,
            telegram_chat_id=telegram_chat_id,
            count=1,
            extra_args=[
                "--tweet-url",
                candidate_url,
                "--source-platforms",
                collect_source_platform,
                "--no-telegram-collect-notify",
                "--no-telegram-prefilter",
                *_build_immediate_fast_x_download_args(repo_root),
                *(
                    [
                        "--collect-media-kind",
                        "image",
                        "--xiaohongshu-extra-images-per-run",
                        "6",
                    ]
                    if normalized_media_kind == "image"
                    else []
                ),
            ],
        )
        processed_count = _extract_processed_file_count(result)
        note = _summarize_collect_publish_attempt(candidate_url, idx, total, processed_count)
        reason_preview = _extract_attempt_reason(result)
        if reason_preview and processed_count <= 0:
            note += f"\n原因：{reason_preview}"
        attempt_notes.append(note)
        last_result = dict(result)
        merged_stdout = str(last_result.get("stdout") or "").strip()
        if processed_count > 0:
            prefix = f"即采即发命中候选：{idx}/{total}\nURL：{candidate_url}\n\n"
            last_result["stdout"] = prefix + merged_stdout if merged_stdout else prefix.rstrip()
            return last_result
    summary = (
        "即采即发未找到可发布素材。\n"
        f"{_collect_publish_source_recent_subtitle(source_platforms, candidate_limit)}\n"
        f"已尝试候选数：{len(attempt_notes)}\n\n"
        + "\n\n".join(attempt_notes[:8])
    )
    merged_stderr = str(last_result.get("stderr") or "").strip()
    last_result["ok"] = False
    last_result["code"] = int(last_result.get("code") or 3)
    last_result["stderr"] = summary + (f"\n\n最后一次执行输出：\n{merged_stderr}" if merged_stderr else "")
    return last_result


def _list_processed_media(workspace: Path, media_kind: str = "video") -> Dict[str, Path]:
    normalized_kind = _normalize_immediate_collect_media_kind(media_kind)
    processed_dir = (
        (workspace / "2_Processed_Images").resolve()
        if normalized_kind == "image"
        else (workspace / "2_Processed").resolve()
    )
    if not processed_dir.exists():
        return {}
    items: Dict[str, Path] = {}
    for path in processed_dir.iterdir():
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if normalized_kind == "image":
            if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
                continue
        elif suffix != ".mp4":
            continue
        if path.is_file():
            items[path.name] = path
    return items


def _list_processed_videos(workspace: Path) -> Dict[str, Path]:
    return _list_processed_media(workspace, media_kind="video")


def _load_runtime_modules() -> tuple[Any, Any]:
    try:
        from Collection.cybercar.cybercar_video_capture_and_publishing_module import hourly_distribution as runner
        from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as core
    except Exception:
        import hourly_distribution as runner  # type: ignore
        import main as core  # type: ignore
    return runner, core


def _build_immediate_publish_args(
    *,
    runner: Any,
    workspace: Path,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str,
    telegram_chat_id: str,
) -> Any:
    parser = runner._build_parser()
    args = parser.parse_args([])
    args.workspace = str(workspace)
    args.limit = 1
    args.non_wechat_max_videos = 1
    args.upload_platforms = ",".join(PUBLISH_PLATFORM_ORDER)
    args.no_save_draft = True
    args.wechat_publish_now = True
    args.notify_per_publish = True
    args.no_telegram_collect_notify = True
    args.telegram_prefilter_skip_only = True
    args.no_publish_skip_notify = True
    args.telegram_bot_token = str(telegram_bot_token or "").strip()
    args.telegram_chat_id = str(telegram_chat_id or "").strip()
    return args


def _list_downloaded_media(workspace: Path, media_kind: str = "video") -> Dict[str, Path]:
    normalized_kind = _normalize_immediate_collect_media_kind(media_kind)
    download_dir = (
        (workspace / "1_Downloads_Images").resolve()
        if normalized_kind == "image"
        else (workspace / "1_Downloads").resolve()
    )
    if not download_dir.exists():
        return {}
    items: Dict[str, Path] = {}
    for path in download_dir.iterdir():
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if normalized_kind == "image":
            if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
                continue
        elif suffix != ".mp4":
            continue
        items[path.name] = path
    return items


def _collect_result_indicates_success(result: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(result, dict):
        return False
    if bool(result.get("ok")):
        return True
    status = str(result.get("status") or "").strip().lower()
    if status in {"ok", "success", "done"}:
        return True
    try:
        return int(result.get("code")) == 0
    except Exception:
        return False


def _extract_x_status_id_from_url(source_url: str) -> str:
    match = re.search(r"/status/(\d+)", str(source_url or "").strip(), re.IGNORECASE)
    return str(match.group(1) or "").strip() if match else ""


def _normalize_x_status_url_for_match(source_url: str) -> str:
    match = re.search(
        r"https?://(?:www\.)?(?:x|twitter)\.com/([^/?#]+)/status/(\d+)",
        str(source_url or "").strip(),
        re.IGNORECASE,
    )
    if not match:
        return ""
    return f"https://x.com/{match.group(1)}/{ 'status' }/{match.group(2)}".replace("//status", "/status")


def _immediate_target_matches_source(
    *,
    core: Any,
    workspace_ctx: Any,
    target: Path,
    source_url: str,
) -> bool:
    expected_status_id = _extract_x_status_id_from_url(source_url)
    expected_status_url = _normalize_x_status_url_for_match(source_url)
    if not expected_status_id and not expected_status_url:
        return True

    target_name = target.name
    if expected_status_id and expected_status_id in target_name:
        return True

    resolver = getattr(core, "_resolve_processed_video_metadata", None)
    if not callable(resolver):
        return False
    try:
        metadata = resolver(workspace_ctx, target)
    except Exception:
        metadata = {}
    if not isinstance(metadata, dict):
        metadata = {}

    target_status_url = _normalize_x_status_url_for_match(str(metadata.get("status_url") or ""))
    if expected_status_url and target_status_url and target_status_url == expected_status_url:
        return True

    target_status_id = str(metadata.get("status_id") or "").strip()
    target_media_id = str(metadata.get("media_id") or "").strip()
    return bool(expected_status_id and expected_status_id in {target_status_id, target_media_id})


def _resolve_immediate_collect_target_for_source(
    *,
    core: Any,
    workspace_ctx: Any,
    source_url: str,
    candidate_targets: list[Path],
) -> Optional[Path]:
    expected_status_id = _extract_x_status_id_from_url(source_url)
    finder = getattr(core, "_find_processed_target_by_source", None)
    if callable(finder):
        try:
            matched_target, _matched_item = finder(
                workspace_ctx,
                source_url=source_url,
                status_id=expected_status_id,
            )
        except TypeError:
            matched_target, _matched_item = finder(workspace_ctx, source_url=source_url)
        if matched_target is not None:
            resolved = Path(matched_target).resolve()
            if resolved.exists() and resolved.is_file():
                return resolved

    for candidate in sorted(candidate_targets, key=lambda path: path.stat().st_mtime, reverse=True):
        if _immediate_target_matches_source(
            core=core,
            workspace_ctx=workspace_ctx,
            target=candidate,
            source_url=source_url,
        ):
            return candidate
    return None


def _adopt_downloaded_target(
    *,
    workspace: Path,
    media_kind: str,
    downloaded_target: Path,
) -> Optional[Path]:
    normalized_kind = _normalize_immediate_collect_media_kind(media_kind)
    target_dir = (
        (workspace / "2_Processed_Images").resolve()
        if normalized_kind == "image"
        else (workspace / "2_Processed").resolve()
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    target = (target_dir / downloaded_target.name).resolve()
    try:
        if target != downloaded_target.resolve():
            shutil.copy2(downloaded_target, target)
        return target
    except Exception:
        return None


def _adopt_downloaded_target_for_test_mode(
    *,
    workspace: Path,
    media_kind: str,
    downloaded_target: Path,
) -> Optional[Path]:
    return _adopt_downloaded_target(
        workspace=workspace,
        media_kind=media_kind,
        downloaded_target=downloaded_target,
    )


def _build_immediate_cycle_context(
    *,
    core: Any,
    runner: Any,
    repo_root: Path,
    workspace: Path,
    args: Any,
    target: Path,
    candidate_url: str,
    profile: str,
) -> Any:
    runtime_config = core._load_runtime_config(str(_default_runtime_config_path(repo_root)))
    exclude_keywords = core._normalize_keyword_list(runtime_config.get("exclude_keywords"), core.DEFAULT_EXCLUDE_KEYWORDS)
    require_any_keywords = core._normalize_keyword_list(
        runtime_config.get("require_any_keywords"),
        core.DEFAULT_REQUIRE_ANY_KEYWORDS,
    )
    collection_name = (
        str(getattr(args, "collection_name", "") or "").strip()
        or str(runtime_config.get("collection_name", "") or "").strip()
        or core.DEFAULT_COLLECTION_NAME
    )
    collection_names = {
        platform: core.resolve_platform_collection_name(
            runtime_config,
            platform,
            cli_collection_name=str(getattr(args, "collection_name", "") or "").strip(),
        )
        for platform in core.SUPPORTED_UPLOAD_PLATFORMS
    }
    resolved_proxy, resolved_use_system_proxy = _resolve_worker_network_mode()
    workspace_ctx = core.init_workspace(str(workspace))
    return runner.CycleContext(
        workspace=workspace_ctx,
        processed_outputs=[target],
        collected_x_urls=[candidate_url] if candidate_url else [],
        exclude_keywords=exclude_keywords,
        require_any_keywords=require_any_keywords,
        collection_name=collection_name,
        collection_names=collection_names,
        chrome_path=str(getattr(args, "chrome_path", "") or "").strip() or None,
        chrome_user_data_dir=str(getattr(args, "chrome_user_data_dir", "") or "").strip() or core.DEFAULT_CHROME_USER_DATA_DIR,
        proxy=resolved_proxy,
        use_system_proxy=bool(resolved_use_system_proxy),
        sorted_batch_dir=None,
        collected_at=time.strftime("%Y-%m-%d %H:%M:%S"),
        keyword=_normalize_profile_name(profile),
        requested_limit=1,
        extra_url_count=1,
        auto_discover_x=True,
    )


def _candidate_platform_hint(platforms: list[str]) -> str:
    names = [str(PUBLISH_PLATFORM_DISPLAY.get(token, token) or token) for token in platforms]
    return "/".join([name for name in names if name]) or "全平台"


def _send_immediate_candidate_prefilter_card(
    *,
    runner: Any,
    email_settings: Any,
    workspace: Path,
    item: Dict[str, Any],
    workspace_ctx: Any = None,
    fast_send: bool = True,
) -> Dict[str, Any]:
    resolved_workspace_ctx = workspace_ctx if workspace_ctx is not None else runner.core.init_workspace(str(workspace))
    platforms = _resolve_item_target_platforms(item)
    return runner._send_telegram_prefilter_for_candidate(
        workspace=resolved_workspace_ctx,
        email_settings=email_settings,
        source_url=str(item.get("source_url") or "").strip(),
        item_id=str(item.get("id") or "").strip(),
        idx=int(item.get("candidate_index") or 0),
        total=int(item.get("candidate_limit") or 0),
        platform_hint=_candidate_platform_hint(platforms),
        mode=str(item.get("workflow") or "immediate_manual_publish"),
        tweet_text=str(item.get("tweet_text") or "").strip(),
        published_at=str(item.get("published_at") or "").strip(),
        display_time=str(item.get("display_time") or "").strip(),
        target_platforms=str(item.get("target_platforms") or "").strip(),
        prefilter_warning=str(item.get("prefilter_warning") or "").strip(),
        fast_send=bool(fast_send),
    )


def _preflight_immediate_candidate_for_prefilter(
    *,
    workspace: Path,
    item_id: str,
    item: Dict[str, Any],
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    timeout_seconds: int,
    log_file: Path,
) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    platforms = _resolve_item_target_platforms(item)
    if "wechat" not in platforms:
        if str(item.get("prefilter_warning") or "").strip():
            return _update_prefilter_item(workspace, item_id, updates={"prefilter_warning": ""})
        return item

    platform_results = _normalize_platform_results(item.get("platform_results"))
    warning_text = ""
    login_probe = _preflight_immediate_platform_login(
        platform="wechat",
        telegram_bot_identifier=telegram_bot_identifier,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        timeout_seconds=timeout_seconds,
        log_file=log_file,
    )
    if not bool(login_probe.get("ready", True)):
        error_text = str(login_probe.get("error") or "视频号未登录").strip() or "视频号未登录"
        failure = _describe_platform_failure("wechat", error_text)
        platform_results["wechat"] = {
            "status": "login_required",
            "updated_at": _now_text(),
            "error": error_text,
            "failure_reason": str(failure.get("reason") or "").strip(),
            "failure_category": str(failure.get("category") or "").strip(),
            "failure_suggestion": str(failure.get("suggestion") or "").strip(),
        }
        warning_text = f"视频号预检：{error_text}"
    else:
        existing = platform_results.get("wechat")
        if isinstance(existing, dict) and str(existing.get("status") or "").strip().lower() == "login_required":
            platform_results.pop("wechat", None)

    return _update_prefilter_item(
        workspace,
        item_id,
        updates={
            "platform_results": platform_results,
            "prefilter_warning": warning_text,
        },
    )


def _should_reissue_immediate_candidate_card(item: Dict[str, Any]) -> bool:
    status = str(item.get("status") or "").strip().lower()
    if status not in IMMEDIATE_CANDIDATE_REISSUE_STATUSES:
        return False
    if int(item.get("message_id") or 0) <= 0:
        return False
    return bool(str(item.get("source_url") or "").strip())


def _reissue_immediate_candidate_prefilter_card(
    *,
    runner: Any,
    email_settings: Any,
    workspace: Path,
    item_id: str,
    item: Dict[str, Any],
    telegram_chat_id: str,
    workspace_ctx: Any = None,
) -> Dict[str, Any]:
    response = _send_immediate_candidate_prefilter_card(
        runner=runner,
        email_settings=email_settings,
        workspace=workspace,
        item=item,
        workspace_ctx=workspace_ctx,
        fast_send=False,
    )
    result_payload = response.get("result") if isinstance(response, dict) else {}
    if not isinstance(result_payload, dict):
        result_payload = {}
    chat_payload = result_payload.get("chat") if isinstance(result_payload.get("chat"), dict) else {}
    message_id = int(result_payload.get("message_id") or 0)
    if message_id <= 0:
        raise RuntimeError("telegram candidate prefilter message_id missing")
    current_status = str(item.get("status") or "").strip().lower() or "link_pending"
    return _update_prefilter_item(
        workspace,
        item_id,
        updates={
            "status": current_status,
            "message_id": message_id,
            "chat_id": str(chat_payload.get("id") or telegram_chat_id or ""),
            "action": "resent_existing_card",
            "last_error": "",
            "prefilter_retry_pending": False,
            "prefilter_last_retry_epoch": 0.0,
            "prefilter_last_retry_at": "",
        },
    )
def _queue_immediate_platform_jobs(
    *,
    workspace: Path,
    repo_root: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    item: Dict[str, Any],
    immediate_test_mode: bool = False,
) -> Dict[str, Any]:
    platform_results: Dict[str, Dict[str, Any]] = _normalize_platform_results(item.get("platform_results"))
    spawned = 0
    failed = 0
    skipped_duplicate = 0
    video_name = str(item.get("video_name") or "").strip()
    workspace_ctx = core.init_workspace(str(workspace)) if video_name else None
    target: Optional[Path] = None
    if video_name and workspace_ctx is not None:
        candidate_path = workspace_ctx.processed / video_name
        if candidate_path.exists() and candidate_path.is_file():
            target = candidate_path
    for platform in _resolve_item_target_platforms(item):
        existing = platform_results.get(platform, {})
        existing_status = str(existing.get("status") or "").strip().lower()
        if existing_status in {"queued", "running"}:
            continue
        if (not immediate_test_mode) and existing_status in {"success", "skipped_duplicate"}:
            continue
        if platform == "wechat":
            login_probe = _preflight_immediate_platform_login(
                platform=platform,
                telegram_bot_identifier=telegram_bot_identifier,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                timeout_seconds=timeout_seconds,
                log_file=workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log",
            )
            if not bool(login_probe.get("ready", True)):
                error_text = str(login_probe.get("error") or "视频号未登录").strip() or "视频号未登录"
                failure = _describe_platform_failure(platform, error_text)
                platform_results[platform] = {
                    "status": "login_required",
                    "updated_at": _now_text(),
                    "error": error_text,
                    "failure_reason": str(failure.get("reason") or "").strip(),
                    "failure_category": str(failure.get("category") or "").strip(),
                    "failure_suggestion": str(failure.get("suggestion") or "").strip(),
                }
                failed += 1
                continue
        if target is not None and workspace_ctx is not None and (not immediate_test_mode):
            is_dup, dup_match, _target_fp = core._is_uploaded_content_duplicate(
                workspace_ctx,
                target,
                platform=platform,
            )
            if is_dup:
                reason = str((dup_match or {}).get("_reason", "duplicate")).strip() or "duplicate"
                match_name = str((dup_match or {}).get("processed_name", "") or "").strip()
                key_text = str((dup_match or {}).get("_key", "") or "").strip()
                parts = [reason]
                if match_name:
                    parts.append(f"match={match_name}")
                if key_text:
                    parts.append(f"key={key_text}")
                platform_results[platform] = {
                    "status": "skipped_duplicate",
                    "updated_at": _now_text(),
                    "error": ", ".join(parts),
                }
                skipped_duplicate += 1
                continue
        try:
            job = _spawn_immediate_publish_platform_job(
                repo_root=repo_root,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                profile=profile,
                telegram_bot_identifier=telegram_bot_identifier,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                item_id=item_id,
                platform=platform,
            )
            platform_results[platform] = {
                "status": "queued",
                "queued_at": _now_text(),
                "pid": int(job.get("pid") or 0),
                "log_path": str(job.get("log_path") or "").strip(),
            }
            spawned += 1
        except Exception as exc:
            platform_results[platform] = {
                "status": "failed",
                "updated_at": _now_text(),
                "error": str(exc),
            }
            failed += 1
    summary = _summarize_platform_results({"platform_results": platform_results, "status": "publish_running"})
    final = _update_prefilter_item(
        workspace,
        item_id,
        updates={
            "status": str(summary.get("status") or "publish_running"),
            "publish_success_count": int(summary.get("publish_success_count") or 0),
            "publish_failed_count": int(summary.get("publish_failed_count") or 0),
            "platform_results": platform_results,
            "action": "publish",
        },
    )
    return {
        "spawned": spawned,
        "failed": failed,
        "skipped_duplicate": skipped_duplicate,
        "item": final,
    }


def _preflight_immediate_platform_login(
    *,
    platform: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    timeout_seconds: int,
    log_file: Optional[Path] = None,
) -> Dict[str, Any]:
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform != "wechat":
        return {"ready": True}
    runtime_ctx = _resolve_platform_login_runtime_context(core, normalized_platform)
    try:
        result = core.probe_platform_session_via_debug_port(
            platform_name=normalized_platform,
            open_url=str(runtime_ctx.get("open_url") or "").strip(),
            debug_port=int(runtime_ctx.get("debug_port") or 0),
            chrome_user_data_dir=str(runtime_ctx.get("chrome_user_data_dir") or "").strip(),
            auto_open_chrome=True,
            telegram_bot_token=str(telegram_bot_token or "").strip(),
            telegram_chat_id=str(telegram_chat_id or "").strip(),
            telegram_bot_identifier=str(telegram_bot_identifier or "").strip(),
            telegram_timeout_seconds=max(10, int(timeout_seconds or 20)),
        )
    except Exception as exc:
        if isinstance(log_file, Path):
            _append_log(
                log_file,
                "[Worker] preflight login probe failed "
                f"platform={normalized_platform} error={_exception_text(exc)}",
            )
        fallback_status: Dict[str, Any] = {}
        try:
            fallback_status = core.check_platform_login_status(
                platform_name=normalized_platform,
                open_url=str(runtime_ctx.get("open_url") or "").strip(),
                debug_port=int(runtime_ctx.get("debug_port") or 0),
                chrome_user_data_dir=str(runtime_ctx.get("chrome_user_data_dir") or "").strip(),
                auto_open_chrome=True,
                refresh_page=True,
            )
        except Exception as fallback_exc:
            if isinstance(log_file, Path):
                _append_log(
                    log_file,
                    "[Worker] preflight login fallback failed "
                    f"platform={normalized_platform} error={_exception_text(fallback_exc)}",
                )
            fallback_status = {"probe_error": _exception_text(fallback_exc)}
        if not isinstance(fallback_status, dict):
            fallback_status = {}
        if not bool(fallback_status.get("needs_login")):
            return {"ready": True, "error": _exception_text(exc), "fallback_result": fallback_status}
        result = {
            "status": "login_required",
            "reason": str(fallback_status.get("reason") or "").strip(),
            "current_url": str(fallback_status.get("url") or "").strip(),
            "root_cause_hint": str(fallback_status.get("reason") or "").strip() or _exception_text(exc),
            "probe_error": _exception_text(exc),
            "fallback_result": fallback_status,
        }
    if not isinstance(result, dict):
        return {"ready": True}
    if str(result.get("status") or "").strip().lower() != "login_required":
        return {"ready": True, "result": result}
    error_text = (
        str(result.get("root_cause_hint") or "").strip()
        or str(result.get("reason") or "").strip()
        or str(result.get("current_url") or "").strip()
        or "视频号未登录"
    )
    text_result: Dict[str, Any] = {}
    qr_result: Dict[str, Any] = {}
    notices: list[str] = []
    if bool(result.get("notified")):
        qr_result = dict(result.get("qr_result") or {}) if isinstance(result.get("qr_result"), dict) else {}
        text_result = dict(result.get("text_result") or {}) if isinstance(result.get("text_result"), dict) else {}
        notification_mode = str(result.get("notification_mode") or "").strip().lower()
        if notification_mode == "qr":
            notices.append("登录二维码已发送到 Telegram")
        elif notification_mode == "text":
            notices.append("已向 Telegram 发送登录提醒")
        if notification_mode != "text" and str(telegram_bot_token or "").strip() and str(telegram_chat_id or "").strip():
            text_result = _send_platform_login_text_notice(
                platform_name=normalized_platform,
                bot_token=str(telegram_bot_token or "").strip(),
                chat_id=str(telegram_chat_id or "").strip(),
                timeout_seconds=max(10, int(timeout_seconds or 20)),
                log_file=Path(log_file) if log_file is not None else Path.cwd() / "runtime" / "logs" / "telegram_command_worker.log",
                login_reason=str(result.get("reason") or "").strip() or error_text,
                qr_error=str(qr_result.get("error") or "").strip(),
                telegram_bot_identifier=str(telegram_bot_identifier or "").strip(),
            )
        if bool(text_result.get("sent")):
            notices.append("已向 Telegram 发送登录提醒")
        elif str(text_result.get("error") or "").strip():
            notices.append(f"登录提醒发送失败：{str(text_result.get('error') or '').strip()}")
        notices = list(dict.fromkeys([str(part or "").strip() for part in notices if str(part or "").strip()]))
        if notices:
            error_text = f"{error_text}；{'；'.join(notices)}"
        return {"ready": False, "error": error_text, "result": result, "qr_result": qr_result, "text_result": text_result}
    if str(telegram_bot_token or "").strip() and str(telegram_chat_id or "").strip():
        text_result = _send_platform_login_text_notice(
            platform_name=normalized_platform,
            bot_token=str(telegram_bot_token or "").strip(),
            chat_id=str(telegram_chat_id or "").strip(),
            timeout_seconds=max(10, int(timeout_seconds or 20)),
            log_file=Path(log_file) if log_file is not None else Path.cwd() / "runtime" / "logs" / "telegram_command_worker.log",
            login_reason=str(result.get("reason") or "").strip() or error_text,
            telegram_bot_identifier=str(telegram_bot_identifier or "").strip(),
        )
        if bool(text_result.get("sent")):
            notices.append("已向 Telegram 发送登录提醒")
        elif str(text_result.get("error") or "").strip():
            notices.append(f"登录提醒发送失败：{str(text_result.get('error') or '').strip()}")
        qr_result = _request_platform_login_qr(
            platform_name=normalized_platform,
            bot_token=str(telegram_bot_token or "").strip(),
            chat_id=str(telegram_chat_id or "").strip(),
            timeout_seconds=max(10, int(timeout_seconds or 20)),
            log_file=Path(log_file) if log_file is not None else Path.cwd() / "runtime" / "logs" / "telegram_command_worker.log",
            refresh_page=True,
        )
        if bool(qr_result.get("sent")):
            notices.append("登录二维码已发送到 Telegram")
        elif bool(qr_result.get("skipped")):
            notices.append("登录二维码已在近期发送")
        elif bool(qr_result.get("transport_error")):
            notices.append("Telegram 网络抖动导致二维码暂未送达，可稍后重试")
        elif str(qr_result.get("error") or "").strip():
            notices.append(f"二维码发送失败：{str(qr_result.get('error') or '').strip()}")
    notices = list(dict.fromkeys([str(part or "").strip() for part in notices if str(part or "").strip()]))
    if notices:
        error_text = f"{error_text}；{'；'.join(notices)}"
    return {"ready": False, "error": error_text, "result": result, "qr_result": qr_result, "text_result": text_result}


def _finalize_immediate_collect_target(
    *,
    runner: Any,
    workspace: Path,
    repo_root: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    item: Dict[str, Any],
    email_settings: Any,
    target: Path,
    reused_existing: bool,
    immediate_test_mode: bool = False,
) -> int:
    actor = str(item.get("actor") or "").strip() or "@manual"
    workflow = str(item.get("workflow") or "").strip().lower()
    updated_item = _update_prefilter_item(
        workspace,
        item_id,
        updates={
            "video_name": target.name,
            "processed_name": target.name,
            "collected_at": _now_text(),
            "action": "collect_reused" if reused_existing else "collect_done",
            "last_error": "",
            "reused_existing_material": bool(reused_existing),
            "immediate_test_mode": bool(immediate_test_mode),
        },
    )
    if workflow == IMMEDIATE_COLLECT_REVIEW_WORKFLOW:
        _apply_review_approve(
            workspace=workspace,
            video_name=target.name,
            actor=actor,
            item_id=item_id,
            media_kind=core._media_kind_from_path(target),
        )
        _update_prefilter_item(
            workspace,
            item_id,
            updates={
                "status": "up_confirmed",
                "action": "collect_done_review_only",
            },
        )
        source_note = (
            "检测到该原帖素材已在 2_Processed，跳过重复下载并直接复用。"
            if reused_existing
            else "当前候选已完成下载与落盘，可进入后续人工筛选使用。"
        )
        if immediate_test_mode:
            source_note = "测试模式已放宽采集筛选，本轮图片素材会优先保留，便于继续验证后续链路。"
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title="图片采集已完成",
            subtitle="测试模式已放宽采集筛选" if immediate_test_mode else "本轮仅执行采集，不触发任何平台发布",
            sections=[
                _build_immediate_candidate_info_section(updated_item),
                {
                    "title": "执行状态",
                    "emoji": "✅",
                    "items": [
                        source_note,
                        "当前素材已进入本地 processed 目录，后续是否使用由你继续决定。",
                    ],
                },
            ],
            status="done",
            platforms=["collect"],
            menu_label="立即采集 / 图片采集",
            task_identifier=_build_task_identifier(
                action="collect_now",
                value="image",
                item_id=item_id,
            ),
        )
        return 0
    approval_status = str(item.get("status") or "").strip().lower()
    if approval_status not in IMMEDIATE_PUBLISH_APPROVED_STATUSES:
        pending_item = _update_prefilter_item(
            workspace,
            item_id,
            updates={
                "status": "link_pending",
                "action": "collect_done_waiting_approval",
            },
        )
        source_note = (
            "检测到该原帖素材已在 2_Processed，已直接复用，但仍等待你手动确认是否发布。"
            if reused_existing
            else "当前候选已完成下载与落盘，但仍等待你手动确认是否发布。"
        )
        if immediate_test_mode:
            source_note = "测试模式已放宽采集筛选并保留本条候选，但仍等待你手动确认是否发布。"
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title="即采即发候选已就绪",
            subtitle="测试模式下将放宽后续过滤，但仍需人工确认" if immediate_test_mode else "素材已准备完成，未获得人工确认前不会进入平台发布",
            sections=[
                _build_immediate_candidate_info_section(pending_item),
                {
                    "title": "执行状态",
                    "emoji": "📝",
                    "items": [
                        source_note,
                        "请回到候选卡片，点击“普通发布”或“原创发布”后再进入平台排队。",
                    ],
                },
            ],
            status="done",
            platforms=["collect"],
            menu_label=_menu_breadcrumb_for_item(pending_item),
            task_identifier=_build_task_identifier(
                action="collect_publish_latest",
                value=str(pending_item.get("target_platforms") or ""),
                item_id=item_id,
            ),
        )
        return 0
    _apply_review_approve(
        workspace=workspace,
        video_name=target.name,
        actor=actor,
        item_id=item_id,
        media_kind=core._media_kind_from_path(target),
    )
    queue_result = _queue_immediate_platform_jobs(
        workspace=workspace,
        repo_root=repo_root,
        timeout_seconds=timeout_seconds,
        profile=profile,
        telegram_bot_identifier=telegram_bot_identifier,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        item_id=item_id,
        item=updated_item,
        immediate_test_mode=immediate_test_mode,
    )
    final_item = queue_result.get("item") if isinstance(queue_result.get("item"), dict) else updated_item
    spawned = int(queue_result.get("spawned") or 0)
    failed = int(queue_result.get("failed") or 0)
    skipped_duplicate = int(queue_result.get("skipped_duplicate") or 0)
    platform_results = _normalize_platform_results(final_item.get("platform_results"))
    source_note = (
        "检测到该原帖素材已在 2_Processed，跳过重复下载并直接进入平台过滤。"
        if reused_existing
        else "当前候选已完成下载与落盘，后台开始按统一平台规则排队。"
    )
    if immediate_test_mode:
        source_note = "测试模式已放宽采集和去重过滤，当前候选仍会进入真实下载与平台发布链路。"
    if spawned <= 0 and skipped_duplicate > 0 and failed <= 0:
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title="即采即发已完成去重",
            subtitle="目标平台均已有发布记录，本轮未重复发布",
            sections=[
                _build_immediate_candidate_info_section(final_item),
                {
                    "title": "执行状态",
                    "emoji": "✅",
                    "items": [
                        source_note,
                        "当前候选已复用既有素材，但所有目标平台都检测到历史发布记录，因此未再重复提交。",
                    ],
                },
                _build_platform_launch_result_section(platform_results),
            ],
            status="done",
            platforms=_resolve_item_target_platforms(final_item),
            menu_label=_menu_breadcrumb_for_item(final_item),
            task_identifier=_build_task_identifier(
                action="collect_publish_latest",
                value=str(final_item.get("target_platforms") or ""),
                item_id=item_id,
            ),
        )
        return 0
    if spawned <= 0:
        _update_prefilter_item(
            workspace,
            item_id,
            updates={
                "status": "publish_failed",
                "action": "publish_spawn_failed",
                "last_error": "所有平台后台任务均未启动成功",
            },
        )
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title="即采即发启动失败",
            subtitle="平台发布任务未成功排队",
            sections=[
                _build_immediate_candidate_info_section(final_item),
                {
                    "title": "执行状态",
                    "emoji": "⚠️",
                    "items": [
                        source_note,
                        "当前仅完成候选确认，尚未真正进入平台发布。",
                        "请先检查失败平台状态，再重新点击发布。",
                    ],
                },
                _build_platform_launch_result_section(platform_results),
            ],
            status="failed",
            platforms=_resolve_item_target_platforms(final_item),
            menu_label=_menu_breadcrumb_for_item(final_item),
            task_identifier=_build_task_identifier(
                action="collect_publish_latest",
                value=str(final_item.get("target_platforms") or ""),
                item_id=item_id,
            ),
        )
        return 2
    if failed > 0:
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title="即采即发部分平台未启动",
            subtitle="部分平台已排队，剩余平台需要人工关注",
            sections=[
                _build_immediate_candidate_info_section(final_item),
                {
                    "title": "执行状态",
                    "emoji": "⚠️",
                    "items": [
                        source_note,
                        "本条只代表已成功排队的平台会继续发布。",
                        "失败平台这次不会自动补发，请检查后重新触发。",
                    ],
                },
                _build_platform_launch_result_section(platform_results),
            ],
            status="failed",
            platforms=_resolve_item_target_platforms(final_item),
            menu_label=_menu_breadcrumb_for_item(final_item),
            task_identifier=_build_task_identifier(
                action="collect_publish_latest",
                value=str(final_item.get("target_platforms") or ""),
                item_id=item_id,
            ),
        )
    _send_background_feedback(
        runner=runner,
        email_settings=email_settings,
        workspace=workspace,
        title="即采即发任务已排队",
        subtitle="测试模式已放宽过滤，最终结果以后续平台通知为准" if immediate_test_mode else "后台已接管处理，最终结果以后续平台通知为准",
        sections=[
            _build_immediate_candidate_info_section(final_item),
            {
                "title": "执行状态",
                "emoji": "🚀",
                "items": [
                    source_note,
                    "测试模式下会放宽采集与重复内容过滤，但不代表平台已经发布成功。" if immediate_test_mode else "当前只确认后台任务已启动，不代表平台已经发布成功。",
                    "各平台任务互不阻塞，会按实际结果分别回传通知。",
                ],
            },
            _build_platform_launch_result_section(platform_results),
        ],
        status="running",
        platforms=_resolve_item_target_platforms(final_item),
        menu_label=_menu_breadcrumb_for_item(final_item),
        task_identifier=_build_task_identifier(
            action="collect_publish_latest",
            value=str(final_item.get("target_platforms") or ""),
            item_id=item_id,
        ),
    )
    return 0


def _run_immediate_collect_item_job(
    *,
    runner: Any,
    core: Any,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    immediate_test_mode: bool = False,
) -> int:
    item = _get_prefilter_item(workspace, item_id)
    if not item:
        return 2
    source_url = str(item.get("source_url") or "").strip()
    if not source_url:
        _update_prefilter_item(
            workspace,
            item_id,
            updates={"status": "download_failed", "action": "collect_missing_source", "last_error": "缺少原帖链接"},
        )
        return 2

    args = _build_immediate_publish_args(
        runner=runner,
        workspace=workspace,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
    )
    email_settings = runner._build_email_settings(args)
    workspace_ctx = core.init_workspace(str(workspace))
    media_kind = _normalize_immediate_collect_media_kind(str(item.get("media_kind") or "video"))
    target_platforms = _resolve_item_target_platforms(item)
    reused_target, _reused_item = core._find_processed_target_by_source(workspace_ctx, source_url=source_url)
    if reused_target is not None:
        return _finalize_immediate_collect_target(
            runner=runner,
            workspace=workspace,
            repo_root=repo_root,
            timeout_seconds=timeout_seconds,
            profile=profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            item_id=item_id,
            item=item,
            email_settings=email_settings,
            target=reused_target,
            reused_existing=True,
            immediate_test_mode=immediate_test_mode,
        )
    priority_request_path = _register_pipeline_priority_request(
        workspace=workspace,
        item_id=item_id,
        source="immediate_collect_item",
    )
    try:
        _update_prefilter_item(
            workspace,
            item_id,
            updates={"status": "download_running", "action": "collect", "last_error": ""},
        )

        collect_source_platform = _normalize_collect_source_platform(item.get("source_platform"))
        if not collect_source_platform:
            collect_source_platform = _resolve_collect_source_platform_from_url(source_url)
        if not collect_source_platform:
            fallback_sources = _resolve_collect_publish_source_platforms(repo_root=repo_root, profile=profile)
            collect_source_platform = fallback_sources[0] if fallback_sources else "x"
        collect_extra_args = [
            "--tweet-url",
            source_url,
            "--source-platforms",
            collect_source_platform,
            "--no-telegram-collect-notify",
            "--no-telegram-prefilter",
            "--no-publish-skip-notify",
            *_build_immediate_fast_x_download_args(repo_root),
        ]
        if media_kind == "image":
            collect_extra_args += [
                "--collect-media-kind",
                "image",
                "--xiaohongshu-extra-images-per-run",
                "6",
            ]
        base_proxy, base_use_system_proxy = _resolve_worker_network_mode()
        base_network_mode = "explicit_proxy" if base_proxy else ("system_proxy" if base_use_system_proxy else "direct_tun")
        system_proxy_available = _worker_system_proxy_available()
        forced_use_system_proxy = False
        forced_direct_tun = False
        elevated_socket_timeout: Optional[int] = None
        elevated_timeout_retry_used = False
        proxy_retry_used = False
        direct_tun_retry_used = False
        print(
            f"[ImmediateCollect] item_id={item_id} source_url={source_url} "
            f"media_kind={media_kind} network_mode={base_network_mode} system_proxy_available={system_proxy_available}"
        )
        lock_wait_deadline = _lock_wait_deadline(
            max_wait_seconds=float(DEFAULT_IMMEDIATE_COLLECT_LOCK_MAX_WAIT_SECONDS),
            timeout_seconds=timeout_seconds,
            retry_seconds=float(DEFAULT_IMMEDIATE_COLLECT_LOCK_RETRY_SECONDS),
        )
        transient_retry_attempts = 0
        target: Optional[Path] = None
        while target is None:
            attempt_proxy = base_proxy
            attempt_use_system_proxy = base_use_system_proxy
            if forced_direct_tun:
                attempt_proxy = ""
                attempt_use_system_proxy = False
            elif forced_use_system_proxy:
                attempt_proxy = ""
                attempt_use_system_proxy = True
            attempt_extra_args = list(collect_extra_args)
            if elevated_socket_timeout is not None:
                attempt_extra_args = _override_cli_arg(
                    attempt_extra_args,
                    "--x-download-socket-timeout",
                    str(max(5, int(elevated_socket_timeout))),
                )
            attempt_network_mode = (
                "explicit_proxy"
                if attempt_proxy
                else ("system_proxy" if attempt_use_system_proxy else "direct_tun")
            )
            print(
                f"[ImmediateCollect] collect attempt item_id={item_id} network_mode={attempt_network_mode} "
                f"socket_timeout={elevated_socket_timeout or 'default'}"
            )
            before_downloads = _list_downloaded_media(workspace, media_kind=media_kind)
            before_videos = (
                _list_processed_videos(workspace)
                if media_kind == "video"
                else _list_processed_media(workspace, media_kind=media_kind)
            )
            collect_result = _run_unified_once(
                repo_root=repo_root,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                mode="collect",
                profile=profile,
                platforms=target_platforms,
                telegram_chat_id=telegram_chat_id,
                count=1,
                extra_args=attempt_extra_args,
                pipeline_priority="high",
                proxy_override=attempt_proxy or None,
                use_system_proxy_override=attempt_use_system_proxy,
            )
            after_videos = (
                _list_processed_videos(workspace)
                if media_kind == "video"
                else _list_processed_media(workspace, media_kind=media_kind)
            )
            new_targets = [path for name, path in after_videos.items() if name not in before_videos]
            matched_processed_target = _resolve_immediate_collect_target_for_source(
                core=core,
                workspace_ctx=workspace_ctx,
                source_url=source_url,
                candidate_targets=new_targets,
            )
            if matched_processed_target is not None:
                target = matched_processed_target
                break

            after_downloads = _list_downloaded_media(workspace, media_kind=media_kind)
            new_downloads = [path for name, path in after_downloads.items() if name not in before_downloads]
            mismatched_material_detected = bool(new_targets) and _collect_result_indicates_success(collect_result)
            if new_downloads and _collect_result_indicates_success(collect_result):
                matched_download_target = _resolve_immediate_collect_target_for_source(
                    core=core,
                    workspace_ctx=workspace_ctx,
                    source_url=source_url,
                    candidate_targets=new_downloads,
                )
                if matched_download_target is not None:
                    adopter = _adopt_downloaded_target_for_test_mode if immediate_test_mode else _adopt_downloaded_target
                    adopted = adopter(
                        workspace=workspace,
                        media_kind=media_kind,
                        downloaded_target=matched_download_target,
                    )
                    if adopted is not None:
                        target = adopted
                        break
                else:
                    mismatched_material_detected = True

            reason = _extract_attempt_reason(collect_result) or "未生成可发布素材。"
            if mismatched_material_detected:
                reason = "本轮采集产出了与当前候选不匹配的素材，已跳过以避免串号。"
            print(
                f"[ImmediateCollect] collect attempt failed item_id={item_id} "
                f"network_mode={attempt_network_mode} reason={reason}"
            )
            remaining_wait_seconds = lock_wait_deadline - time.monotonic()
            if _is_immediate_collect_lock_retry_reason(reason, collect_result) and remaining_wait_seconds > 0:
                _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "download_running",
                        "action": "collect_waiting_lock",
                        "last_error": reason,
                        "updated_at": _now_text(),
                    },
                )
                time.sleep(min(float(DEFAULT_IMMEDIATE_COLLECT_LOCK_RETRY_SECONDS), max(1.0, remaining_wait_seconds)))
                continue
            if (
                _is_immediate_collect_transient_retry_reason(reason, collect_result)
                and not proxy_retry_used
                and not attempt_use_system_proxy
                and system_proxy_available
            ):
                proxy_retry_used = True
                forced_use_system_proxy = True
                elevated_socket_timeout = max(40, int(elevated_socket_timeout or 0))
                _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "download_running",
                        "action": "collect_retry_system_proxy",
                        "last_error": f"{reason}（已切换系统代理重试）",
                        "updated_at": _now_text(),
                    },
                )
                time.sleep(float(DEFAULT_IMMEDIATE_COLLECT_TRANSIENT_RETRY_SECONDS))
                continue
            if (
                _is_immediate_collect_transient_retry_reason(reason, collect_result)
                and not direct_tun_retry_used
                and attempt_use_system_proxy
            ):
                direct_tun_retry_used = True
                forced_direct_tun = True
                forced_use_system_proxy = False
                elevated_socket_timeout = max(40, int(elevated_socket_timeout or 0))
                _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "download_running",
                        "action": "collect_retry_direct_tun",
                        "last_error": f"{reason}（系统代理重试失败，已切换直连重试）",
                        "updated_at": _now_text(),
                    },
                )
                time.sleep(float(DEFAULT_IMMEDIATE_COLLECT_TRANSIENT_RETRY_SECONDS))
                continue
            if (
                _is_immediate_collect_transient_retry_reason(reason, collect_result)
                and not elevated_timeout_retry_used
            ):
                elevated_timeout_retry_used = True
                elevated_socket_timeout = max(40, int(elevated_socket_timeout or 0))
                _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "download_running",
                        "action": "collect_retry_timeout",
                        "last_error": f"{reason}（已放宽 X 超时后重试）",
                        "updated_at": _now_text(),
                    },
                )
                time.sleep(float(DEFAULT_IMMEDIATE_COLLECT_TRANSIENT_RETRY_SECONDS))
                continue
            if (
                _is_immediate_collect_transient_retry_reason(reason, collect_result)
                and transient_retry_attempts < int(DEFAULT_IMMEDIATE_COLLECT_TRANSIENT_RETRY_LIMIT)
            ):
                transient_retry_attempts += 1
                _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "download_running",
                        "action": "collect_retry_transient",
                        "last_error": f"{reason}（第 {transient_retry_attempts} 次瞬时重试）",
                        "updated_at": _now_text(),
                    },
                )
                time.sleep(float(DEFAULT_IMMEDIATE_COLLECT_TRANSIENT_RETRY_SECONDS))
                continue

            _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "download_failed",
                    "action": "collect_failed",
                    "last_error": reason,
                },
            )
            fallback_item = _pick_next_immediate_collect_fallback_item(
                workspace=workspace,
                current_item_id=item_id,
                media_kind=media_kind,
                profile=profile,
                chat_id=telegram_chat_id,
                candidate_limit=int(item.get("candidate_limit") or 0),
            )
            fallback_item_id = str(fallback_item.get("id") or "").strip() if isinstance(fallback_item, dict) else ""
            if fallback_item_id:
                try:
                    fallback_item = _update_prefilter_item(
                        workspace,
                        fallback_item_id,
                        updates={
                            "status": "publish_requested",
                            "action": "auto_fallback_from_collect_failed",
                            "last_error": "",
                        },
                    )
                    _spawn_immediate_publish_item_job(
                        repo_root=repo_root,
                        workspace=workspace,
                        timeout_seconds=timeout_seconds,
                        profile=profile,
                        telegram_bot_identifier=telegram_bot_identifier,
                        telegram_bot_token=telegram_bot_token,
                        telegram_chat_id=telegram_chat_id,
                        item_id=fallback_item_id,
                        immediate_test_mode=immediate_test_mode,
                    )
                    _send_background_feedback(
                        runner=runner,
                        email_settings=email_settings,
                        workspace=workspace,
                        title="即采即发候选已自动切换",
                        subtitle="当前候选下载失败，已接力到下一条候选继续处理",
                        sections=[
                            _build_immediate_candidate_info_section(item),
                            {
                                "title": "切换原因",
                                "emoji": "⚠️",
                                "items": [
                                    {"label": "当前候选失败", "value": reason},
                                    {"label": "接力候选", "value": str(fallback_item.get("source_url") or "").strip() or fallback_item_id},
                                ],
                            },
                        ],
                        status="running",
                        platforms=_resolve_item_target_platforms(item),
                    )
                    return 0
                except Exception as fallback_exc:
                    _append_log(
                        workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log",
                        f"[ImmediateCollect] fallback handoff failed current_item={item_id} next_item={fallback_item_id} error={fallback_exc}",
                    )
            _send_background_feedback(
                runner=runner,
                email_settings=email_settings,
                workspace=workspace,
                title="即采即发候选处理失败",
                subtitle="候选未生成可发布素材",
                sections=[
                    _build_immediate_candidate_info_section(item),
                    {
                        "title": "失败原因",
                        "emoji": "⚠️",
                        "items": [
                            {"label": "原因", "value": reason},
                        ],
                    }
                ],
                status="failed",
                platforms=_resolve_item_target_platforms(item),
            )
            return 2

        return _finalize_immediate_collect_target(
            runner=runner,
            workspace=workspace,
            repo_root=repo_root,
            timeout_seconds=timeout_seconds,
            profile=profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            item_id=item_id,
            item=item,
            email_settings=email_settings,
            target=target,
            reused_existing=False,
            immediate_test_mode=immediate_test_mode,
        )
    finally:
        _clear_pipeline_priority_request(priority_request_path)


def _publish_immediate_candidate_platform(
    *,
    runner: Any,
    core: Any,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    platform: str,
) -> int:
    item = _get_prefilter_item(workspace, item_id)
    if not item:
        return 2
    log_file = workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log"
    media_kind = _normalize_immediate_collect_media_kind(str(item.get("media_kind") or "video"))
    processed_name = str(item.get("processed_name") or item.get("video_name") or "").strip()
    if not processed_name:
        _record_error_event(
            workspace=workspace,
            log_file=log_file,
            category="immediate_publish_precheck_failed",
            message="missing processed media name before immediate publish",
            context={"item_id": item_id, "platform": str(platform or "").strip().lower()},
            retryable=False,
            severity="warning",
        )
        _enqueue_platform_result_event(
            workspace=workspace,
            item_id=item_id,
            platform=platform,
            updates={"status": "failed", "error": "缺少本地素材"},
        )
        return 2
    processed_dir = (workspace / ("2_Processed_Images" if media_kind == "image" else "2_Processed")).resolve()
    target = (processed_dir / processed_name).resolve()
    if not target.exists():
        try:
            workspace_ctx = core.init_workspace(str(workspace))
            resolved_target = core._resolve_processed_target_path(
                workspace_ctx,
                processed_name,
                include_images=(media_kind == "image"),
            )
        except Exception:
            resolved_target = None
        if resolved_target is not None:
            target = Path(resolved_target).resolve()
    if not target.exists():
        _record_error_event(
            workspace=workspace,
            log_file=log_file,
            category="immediate_publish_precheck_failed",
            message="processed target file missing before immediate publish",
            context={
                "item_id": item_id,
                "platform": str(platform or "").strip().lower(),
                "processed_name": processed_name,
                "target": str(target),
            },
            retryable=False,
            severity="warning",
        )
        _enqueue_platform_result_event(
            workspace=workspace,
            item_id=item_id,
            platform=platform,
            updates={"status": "failed", "error": f"素材文件不存在：{processed_name}"},
        )
        return 2
    workspace_ctx = core.init_workspace(str(workspace))
    if not _immediate_target_matches_source(
        core=core,
        workspace_ctx=workspace_ctx,
        target=target,
        source_url=str(item.get("source_url") or "").strip(),
    ):
        _record_error_event(
            workspace=workspace,
            log_file=log_file,
            category="immediate_publish_precheck_failed",
            message="processed target does not match candidate source url",
            context={
                "item_id": item_id,
                "platform": str(platform or "").strip().lower(),
                "processed_name": processed_name,
                "target": str(target),
                "source_url": str(item.get("source_url") or "").strip(),
            },
            retryable=False,
            severity="warning",
        )
        _enqueue_platform_result_event(
            workspace=workspace,
            item_id=item_id,
            platform=platform,
            updates={
                "status": "failed",
                "error": f"素材与当前候选不匹配：{processed_name}；已阻止发布以避免串号",
            },
        )
        return 2

    def _publish_once_under_lock() -> int:
        args = _build_immediate_publish_args(
            runner=runner,
            workspace=workspace,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
        )
        args.upload_platforms = platform
        args.notify_per_publish = False
        if media_kind == "image":
            args.collect_media_kind = "image"
            args.xiaohongshu_allow_image = True
        args.wechat_declare_original = bool(_normalize_optional_bool(item.get("wechat_declare_original"))) if platform == "wechat" else False
        email_settings = runner._build_email_settings(args)
        _enqueue_platform_result_event(
            workspace=workspace,
            item_id=item_id,
            platform=platform,
            updates={"status": "running", "started_at": _now_text()},
        )
        ctx = _build_immediate_cycle_context(
            core=core,
            runner=runner,
            repo_root=repo_root,
            workspace=workspace,
            args=args,
            target=target,
            candidate_url=str(item.get("source_url") or "").strip(),
            profile=profile,
        )
        events: list[Any] = []
        runner._publish_once(
            ctx,
            args,
            email_settings,
            platform,
            target,
            "collect_publish_latest",
            events,
        )
        event = events[-1] if events else None
        if event is not None and bool(getattr(event, "success", False)):
            _enqueue_platform_result_event(
                workspace=workspace,
                item_id=item_id,
                platform=platform,
                updates={
                    "status": "success",
                    "published_at": str(getattr(event, "published_at", "") or _now_text()),
                    "publish_id": str(getattr(event, "publish_id", "") or "").strip(),
                    "failure_reason": "",
                    "failure_category": "",
                    "failure_suggestion": "",
                },
            )
            return 0
        error_text = str(getattr(event, "error", "") or "") if event is not None else "发布失败"
        duplicate_marker = "duplicate target blocked"
        failure = _describe_platform_failure(platform, error_text)
        result_status = (
            "skipped_duplicate"
            if duplicate_marker in error_text.lower()
            else ("login_required" if _failure_requires_login(failure, error_text) else "failed")
        )
        if result_status == "failed" and _should_probe_platform_login_after_publish_failure(platform, error_text):
            result_status, error_text = _probe_platform_login_after_publish_failure(
                workspace=workspace,
                item_id=item_id,
                platform=platform,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                timeout_seconds=timeout_seconds,
                log_file=workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log",
                error_text=error_text,
            )
            failure = _describe_platform_failure(platform, error_text)
        promoted_unconfirmed = False
        if result_status == "failed" and _should_promote_wechat_unconfirmed_publish_to_success(
            workspace=workspace,
            item=item,
            platform=platform,
            error_text=error_text,
        ):
            promoted_unconfirmed = True
            result_status = "success"
            error_text = ""
            failure = {"reason": "", "category": "", "suggestion": "", "raw_signal": ""}
        if result_status in {"failed", "login_required"}:
            _record_error_event(
                workspace=workspace,
                log_file=log_file,
                category="immediate_publish_platform_failed",
                message="immediate publish platform execution reported failure",
                error_text=error_text or "发布失败",
                context={
                    "item_id": item_id,
                    "platform": str(platform or "").strip().lower(),
                    "status": result_status,
                    "processed_name": processed_name,
                    "target": str(target),
                    "failure_category": str(failure.get("category") or "").strip(),
                },
                retryable=(result_status == "login_required"),
                severity="warning",
            )
        updates = {
            "status": result_status,
            "error": error_text or "发布失败",
            "failure_reason": str(failure.get("reason") or "").strip(),
            "failure_category": str(failure.get("category") or "").strip(),
            "failure_suggestion": str(failure.get("suggestion") or "").strip(),
        }
        if result_status == "success":
            updates.update(
                {
                    "published_at": str(getattr(event, "published_at", "") or _now_text()),
                    "publish_id": str(getattr(event, "publish_id", "") or "").strip(),
                    "error": "",
                    "failure_reason": "",
                    "failure_category": "",
                    "failure_suggestion": "",
                }
            )
            if promoted_unconfirmed and not str(updates.get("publish_id") or "").strip():
                updates["publish_id"] = "wechat-unconfirmed-auto-confirm"
        _enqueue_platform_result_event(
            workspace=workspace,
            item_id=item_id,
            platform=platform,
            updates=updates,
        )
        return 0 if result_status in {"success", "skipped_duplicate"} else 2

    try:
        return _with_platform_lock(
            workspace,
            platform,
            _publish_once_under_lock,
            timeout_seconds=_normalize_blocking_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1),
        )
    except Exception as exc:
        error_text = str(exc)
        duplicate_marker = "duplicate target blocked"
        failure = _describe_platform_failure(platform, error_text)
        result_status = (
            "skipped_duplicate"
            if duplicate_marker in error_text.lower()
            else ("login_required" if _failure_requires_login(failure, error_text) else "failed")
        )
        if result_status == "failed" and _should_probe_platform_login_after_publish_failure(platform, error_text):
            result_status, error_text = _probe_platform_login_after_publish_failure(
                workspace=workspace,
                item_id=item_id,
                platform=platform,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
                error_text=error_text,
            )
            failure = _describe_platform_failure(platform, error_text)
        promoted_unconfirmed = False
        if result_status == "failed" and _should_promote_wechat_unconfirmed_publish_to_success(
            workspace=workspace,
            item=item,
            platform=platform,
            error_text=error_text,
        ):
            promoted_unconfirmed = True
            result_status = "success"
            error_text = ""
            failure = {"reason": "", "category": "", "suggestion": "", "raw_signal": ""}
        if result_status in {"failed", "login_required"}:
            _record_error_event(
                workspace=workspace,
                log_file=log_file,
                category="immediate_publish_platform_exception",
                message="immediate publish platform execution raised exception",
                error_text=error_text,
                exc=exc,
                context={
                    "item_id": item_id,
                    "platform": str(platform or "").strip().lower(),
                    "status": result_status,
                    "processed_name": processed_name,
                    "target": str(target),
                    "failure_category": str(failure.get("category") or "").strip(),
                },
                retryable=(result_status == "login_required"),
            )
        updates = {
            "status": result_status,
            "error": error_text,
            "failure_reason": str(failure.get("reason") or "").strip(),
            "failure_category": str(failure.get("category") or "").strip(),
            "failure_suggestion": str(failure.get("suggestion") or "").strip(),
        }
        if result_status == "success":
            updates.update(
                {
                    "published_at": _now_text(),
                    "publish_id": "",
                    "error": "",
                    "failure_reason": "",
                    "failure_category": "",
                    "failure_suggestion": "",
                }
            )
            if promoted_unconfirmed:
                updates["publish_id"] = "wechat-unconfirmed-auto-confirm"
        _enqueue_platform_result_event(
            workspace=workspace,
            item_id=item_id,
            platform=platform,
            updates=updates,
        )
        return 0 if result_status in {"success", "skipped_duplicate"} else 2


def _run_immediate_publish_item_job(
    *,
    runner: Any,
    core: Any,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    immediate_test_mode: bool = False,
) -> int:
    item = _get_prefilter_item(workspace, item_id)
    if not item:
        return 2
    if not str(item.get("video_name") or "").strip():
        return _run_immediate_collect_item_job(
            runner=runner,
            core=core,
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            item_id=item_id,
            immediate_test_mode=immediate_test_mode,
        )
    queue_result = _queue_immediate_platform_jobs(
        workspace=workspace,
        repo_root=repo_root,
        timeout_seconds=timeout_seconds,
        profile=profile,
        telegram_bot_identifier=telegram_bot_identifier,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        item_id=item_id,
        item=item,
        immediate_test_mode=immediate_test_mode,
    )
    return 0 if int(queue_result.get("spawned") or 0) > 0 else 2


def _find_new_prefilter_item(
    workspace: Path,
    *,
    before_ids: set[str],
    target_name: str,
) -> tuple[str, dict[str, Any]]:
    queue = _load_prefilter_queue(_prefilter_queue_path(workspace))
    items = queue.get("items", {})
    if not isinstance(items, dict):
        return "", {}
    for item_id, payload in items.items():
        if item_id in before_ids:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("video_name") or "").strip() == target_name:
            return str(item_id), payload
    for item_id, payload in items.items():
        if not isinstance(payload, dict):
            continue
        if str(payload.get("video_name") or "").strip() == target_name:
            return str(item_id), payload
    return "", {}


def _update_prefilter_item(
    workspace: Path,
    item_id: str,
    *,
    updates: Optional[Dict[str, Any]] = None,
) -> dict[str, Any]:
    def _mutate(queue: Dict[str, Any]) -> Dict[str, Any]:
        items = queue.get("items", {})
        if not isinstance(items, dict):
            items = {}
            queue["items"] = items
        row = items.get(item_id, {})
        if not isinstance(row, dict):
            row = {"id": item_id}
            items[item_id] = row
        if updates:
            row.update({k: v for k, v in updates.items()})
        row["updated_at"] = _now_text()
        items[item_id] = row
        return dict(row)

    row = _with_prefilter_queue_lock(workspace, _mutate)
    if not isinstance(row, dict):
        return {}
    return _refresh_prefilter_item_coordination_snapshot(workspace, item_id, row)


def _get_prefilter_item(workspace: Path, item_id: str) -> dict[str, Any]:
    def _read(queue: Dict[str, Any]) -> dict[str, Any]:
        items = queue.get("items", {})
        if not isinstance(items, dict):
            return {}
        row = items.get(item_id, {})
        return dict(row) if isinstance(row, dict) else {}

    row = _with_prefilter_queue_lock(workspace, _read)
    return dict(row) if isinstance(row, dict) else {}


def _pick_next_immediate_collect_fallback_item(
    *,
    workspace: Path,
    current_item_id: str,
    media_kind: str,
    profile: str,
    chat_id: str,
    candidate_limit: int,
) -> dict[str, Any]:
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind or "video")
    normalized_profile = _normalize_profile_name(profile)
    normalized_chat_id = str(chat_id or "").strip()
    normalized_limit = max(0, int(candidate_limit or 0))

    def _read(queue: Dict[str, Any]) -> dict[str, Any]:
        items = queue.get("items", {})
        if not isinstance(items, dict) or not items:
            return {}
        candidates: list[dict[str, Any]] = []
        for raw_id, raw_row in items.items():
            if not isinstance(raw_row, dict):
                continue
            row = dict(raw_row)
            item_id = str(row.get("id") or raw_id or "").strip()
            if not item_id or item_id == current_item_id:
                continue
            status = str(row.get("status") or "").strip().lower()
            action = str(row.get("action") or "").strip().lower()
            if status not in {"link_pending", "up_confirmed"}:
                continue
            if action == "skip":
                continue
            if _is_prefilter_filtered_terminal(row):
                continue
            if _normalize_immediate_collect_media_kind(str(row.get("media_kind") or "video")) != normalized_media_kind:
                continue
            row_profile = _normalize_profile_name(str(row.get("profile") or ""))
            if normalized_profile and row_profile and row_profile != normalized_profile:
                continue
            row_chat = str(row.get("chat_id") or "").strip()
            if normalized_chat_id and row_chat and row_chat != normalized_chat_id:
                continue
            try:
                row_limit = int(row.get("candidate_limit") or 0)
            except Exception:
                row_limit = 0
            if normalized_limit > 0 and row_limit > 0 and row_limit != normalized_limit:
                continue
            candidates.append(row)

        if not candidates:
            return {}

        def _candidate_key(row: Dict[str, Any]) -> tuple[int, str, str]:
            try:
                index = int(row.get("candidate_index") or 0)
            except Exception:
                index = 0
            updated = str(row.get("updated_at") or row.get("created_at") or "").strip()
            item_id = str(row.get("id") or "").strip()
            return (index if index > 0 else 999999, updated, item_id)

        candidates.sort(key=_candidate_key)
        return dict(candidates[0])

    selected = _with_prefilter_queue_lock(workspace, _read)
    return dict(selected) if isinstance(selected, dict) else {}


def _wait_for_immediate_review(
    *,
    workspace: Path,
    target_name: str,
    item_id: str,
    media_kind: str = "video",
    wait_seconds: int = DEFAULT_IMMEDIATE_REVIEW_WAIT_SECONDS,
) -> Dict[str, Any]:
    deadline = time.time() + max(3, int(wait_seconds))
    while time.time() < deadline:
        queue = _load_prefilter_queue(_prefilter_queue_path(workspace))
        items = queue.get("items", {})
        if isinstance(items, dict):
            row = items.get(item_id, {}) if item_id else {}
            if isinstance(row, dict):
                status = str(row.get("status") or "").strip().lower()
                if status == "down_confirmed":
                    return {"blocked": True, "reason": "你已点击跳过本条，全部平台停止发布。"}
                if status == "expired_pending":
                    return {"blocked": True, "reason": "预审卡片超过 10 分钟未处理，已自动过期并停止继续发布。"}
                if status == "up_confirmed":
                    return {"blocked": False, "reason": "你已确认保留本条，继续发布。"}
        review_state = workspace / DEFAULT_REVIEW_STATE_FILE
        if review_state.exists():
            entries = core._load_review_state_entries(review_state)
            row = core._get_review_state_entry(entries, target_name, media_kind=_normalize_review_media_kind(media_kind))
            if isinstance(row, dict) and str(row.get("status") or "").strip().lower() in {"rejected", "blocked"}:
                return {"blocked": True, "reason": "你已点击跳过本条，全部平台停止发布。"}
        time.sleep(1.0)
    return {"blocked": False, "reason": f"预审等待 {max(3, int(wait_seconds))} 秒后未收到跳过指令，默认继续发布。"}


def _send_background_feedback(
    *,
    runner: Any,
    email_settings: Any,
    workspace: Optional[Path] = None,
    title: str,
    subtitle: str,
    sections: list[dict[str, Any]],
    status: str,
    platforms: Optional[Iterable[str]] = None,
    menu_label: str = "",
    task_identifier: str = "",
) -> None:
    if not _should_send_background_feedback(status):
        if hasattr(runner, "core"):
            try:
                runner.core._log(f"[Notify] 后台结果已跳过（非最终状态）：{title}")
            except Exception:
                pass
        return
    normalized_title = _prefix_menu_title(title, menu_label)
    display_title = _decorate_feedback_title(normalized_title, platforms)
    normalized_sections = _normalize_task_log_sections(sections)
    if not task_identifier:
        log_name = ""
        for section in normalized_sections:
            if str(section.get("title") or "").strip() != "任务日志":
                continue
            items = section.get("items") or []
            if isinstance(items, list) and items:
                first = items[0]
                if isinstance(first, dict):
                    log_name = str(first.get("value") or "").strip()
                else:
                    log_name = str(first or "").strip()
                break
        task_identifier = _build_task_identifier(menu_label=menu_label, log_path=log_name)
    task_identifier_section = _build_task_identifier_section(task_identifier)
    menu_section = _build_menu_path_section(menu_label)
    if task_identifier_section is not None:
        normalized_sections = [task_identifier_section, *normalized_sections]
    if menu_section is not None:
        normalized_sections = [menu_section, *normalized_sections]
    normalized_sections = _optimize_feedback_sections_for_operator(normalized_sections)
    feedback_actions = _build_failure_feedback_actions(status=status, sections=normalized_sections)
    card = build_action_feedback(
        status=status,
        title=display_title,
        subtitle=subtitle,
        sections=normalized_sections,
        actions=feedback_actions,
        bot_name="CyberCar",
    )
    card["reply_markup"] = _with_home_button(card.get("reply_markup") if isinstance(card, dict) else None)
    bot_token = str(getattr(email_settings, "telegram_bot_token", "") or "").strip()
    chat_id = str(getattr(email_settings, "telegram_chat_id", "") or "").strip()
    if not bot_token or not chat_id:
        if hasattr(runner, "core"):
            try:
                runner.core._log(f"[Notify] 后台结果未发送：缺少 Telegram bot_token/chat_id。标题={display_title}")
            except Exception:
                pass
        return
    params: dict[str, Any] = {
        "chat_id": chat_id,
        "text": str(card.get("text") or "").strip(),
        "disable_web_page_preview": "false",
    }
    parse_mode = str(card.get("parse_mode") or "").strip()
    if parse_mode:
        params["parse_mode"] = parse_mode
    reply_markup = _outgoing_reply_markup(
        card.get("reply_markup") if isinstance(card.get("reply_markup"), dict) else None,
        for_edit=False,
    )
    if isinstance(reply_markup, dict):
        params["reply_markup"] = json.dumps(reply_markup, ensure_ascii=True)
    try:
        _shared_call_telegram_api(
            bot_token=bot_token,
            method="sendMessage",
            params=params,
            timeout_seconds=max(8, int(getattr(email_settings, "telegram_timeout_seconds", 20) or 20)),
            api_base=str(getattr(email_settings, "telegram_api_base", "") or "").strip(),
            use_post=True,
            max_retries=4,
        )
        if hasattr(runner, "core"):
            try:
                runner.core._log(f"[Notify] 后台结果已发送：{display_title}")
            except Exception:
                pass
    except Exception:
        if isinstance(workspace, Path):
            _enqueue_pending_background_feedback(
                workspace=workspace,
                title=display_title,
                subtitle=subtitle,
                sections=normalized_sections,
                status=status,
            )
            if hasattr(runner, "core"):
                try:
                    runner.core._log(f"[Notify] 后台结果发送失败，已加入待补发队列：{display_title}")
                except Exception:
                    pass


def _publish_immediate_candidate(
    *,
    runner: Any,
    core: Any,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    item_id: str,
    immediate_test_mode: bool = False,
) -> int:
    return _run_immediate_publish_item_job(
        runner=runner,
        core=core,
        repo_root=repo_root,
        workspace=workspace,
        timeout_seconds=timeout_seconds,
        profile=profile,
        telegram_bot_identifier=telegram_bot_identifier,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        item_id=item_id,
        immediate_test_mode=immediate_test_mode,
    )


def _build_comment_reply_result_card(result: Dict[str, Any]) -> Dict[str, Any]:
    payload = result if isinstance(result, dict) else {}
    platform_token = _normalize_platform_tokens([str(payload.get("platform") or "wechat")])
    platform_name = _menu_platform_label(platform_token[0] if platform_token else "wechat")
    reply_count = int(payload.get("replies_sent") or 0)
    login_required = bool(payload.get("login_required"))
    status = "success" if bool(payload.get("ok")) and reply_count > 0 else "failed"
    title = f"{platform_name}点赞评论已完成" if status == "success" else f"{platform_name}点赞评论未完成"
    subtitle = ""
    if login_required:
        subtitle = "视频号当前未登录，本轮未执行点赞评论。"
    sections: list[dict[str, Any]] = [
        {
            "title": "任务概览",
            "emoji": "📌",
            "items": [
                {"label": "扫描视频", "value": str(int(payload.get("posts_scanned") or 0))},
                {"label": "命中视频", "value": str(int(payload.get("posts_selected") or 0))},
                {"label": "实际回复", "value": str(reply_count)},
                {"label": "目标平台", "value": platform_name},
                {"label": "状态文件", "value": str(payload.get("state_path") or "-").strip() or "-"},
            ],
        }
    ]
    reason = str(payload.get("reason") or "").strip()
    platform_results = payload.get("platform_results") if isinstance(payload.get("platform_results"), list) else []
    if platform_results:
        platform_items: list[str] = []
        for raw in platform_results:
            if not isinstance(raw, dict):
                continue
            item_platform = _menu_platform_label(str(raw.get("platform") or ""))
            item_status = "success" if bool(raw.get("ok")) else "failed"
            item_reason = str(raw.get("reason") or "").strip()
            item_replies = int(raw.get("replies_sent") or 0)
            detail = f"{item_platform}: {item_status}, replies={item_replies}"
            if item_reason:
                detail += f", reason={item_reason}"
            platform_items.append(detail)
        if platform_items:
            sections.append({
                "title": "平台结果",
                "emoji": "🧭",
                "items": platform_items,
            })
    if login_required:
        notify_items: list[str] = []
        login_url = str(payload.get("login_url") or "").strip()
        notify_mode = str(payload.get("login_notification_mode") or "").strip()
        notify_error = str(payload.get("login_notification_error") or "").strip()
        qr_error = str(payload.get("login_qr_error") or "").strip()
        if login_url:
            notify_items.append(f"当前页面：{login_url}")
        if bool(payload.get("login_notify_sent")):
            mode_label = "二维码" if notify_mode == "qr" else "文字提醒"
            notify_items.append(f"已向 Telegram 发送{mode_label}。")
        else:
            error_text = notify_error or qr_error
            notify_items.append("已识别为视频号未登录。")
            if error_text:
                notify_items.append(f"但 Telegram 通知发送失败：{error_text}")
        sections.append({
            "title": "登录状态",
            "emoji": "🔐",
            "items": notify_items or ["已识别为视频号未登录。"],
        })
    if reason:
        sections.append({
            "title": "原因",
            "emoji": "⚠️",
            "items": [reason],
        })
    records = payload.get("records") if isinstance(payload.get("records"), list) else []
    if not records:
        sections.append(
            {
                "title": "结果",
                "emoji": "🧾",
                "items": ["本次没有新的评论回复。"],
            }
        )

    card = build_action_feedback(
        status=status,
        title=title,
        subtitle=subtitle,
        sections=sections,
        bot_name="CyberCar",
    )
    card["reply_markup"] = _build_inline_keyboard(
        [[{"text": "🏠 首页", "callback_data": build_home_callback_data("cybercar", "home")}]]
    )
    return card


def _resolve_comment_reply_post_link(raw: Dict[str, Any], platform_token: str) -> tuple[str, str]:
    if not isinstance(raw, dict):
        return "", ""
    for key in ("post_url", "post_public_url", "public_url", "share_url", "url"):
        value = str(raw.get(key) or "").strip()
        if value.startswith("http://") or value.startswith("https://"):
            return "Post URL", value
    title = str(raw.get("post_title") or "").strip()
    if not title:
        return "", ""
    encoded_title = quote(title)
    if platform_token == "douyin":
        return "Search URL", f"https://www.douyin.com/search/{encoded_title}"
    if platform_token == "kuaishou":
        return "Search URL", f"https://www.kuaishou.com/search/video?searchKey={encoded_title}"
    return "", ""


def _build_comment_reply_record_texts(result: Dict[str, Any]) -> list[str]:
    payload = result if isinstance(result, dict) else {}
    platform_token = _normalize_platform_tokens([str(payload.get("platform") or "wechat")])
    platform_name = _menu_platform_label(platform_token[0] if platform_token else "wechat")
    records = payload.get("records") if isinstance(payload.get("records"), list) else []
    messages: list[str] = []
    for idx, raw in enumerate(records, start=1):
        if not isinstance(raw, dict):
            continue
        record_platform_token = _normalize_platform_tokens([str(raw.get("platform") or "")])
        record_platform_name = _menu_platform_label(record_platform_token[0]) if record_platform_token else platform_name
        active_platform_token = record_platform_token[0] if record_platform_token else (platform_token[0] if platform_token else "wechat")
        author = str(raw.get("comment_author") or "-").strip() or "-"
        comment_time = str(raw.get("comment_time") or "-").strip() or "-"
        replied_at = str(raw.get("replied_at") or "-").strip() or "-"
        lines = [
            f"[{record_platform_name}] Reply {idx}",
            f"Post: {_preview_text(raw.get('post_title'), limit=120) or '-'}",
            f"Author: {author}",
            f"Comment Time: {comment_time}",
            f"Comment: {_preview_text(raw.get('comment_preview'), limit=200) or '-'}",
            f"Provider: {str(raw.get('reply_provider') or '-').strip() or '-'}",
            f"Reply: {_preview_text(raw.get('reply_text'), limit=200) or '-'}",
            f"Replied At: {replied_at}",
        ]
        link_label, link_value = _resolve_comment_reply_post_link(raw, active_platform_token)
        if link_label and link_value:
            lines.insert(2, f"{link_label}: {link_value}")
        messages.append("\n".join(lines))
    return messages


def _run_home_action_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    action: str,
    value: str,
    task_key: str,
    immediate_test_mode: bool = False,
) -> int:
    action_token = str(action or "").strip().lower()
    resolved_profile = _normalize_profile_name(profile)
    title = _home_action_title(action_token)
    raw_result: Dict[str, Any] = {}
    _update_home_action_task(
        workspace,
        task_key,
        status="running",
        detail="后台任务正在执行。",
        pid=os.getpid(),
    )
    result_status = "done"
    detail = ""
    exit_code = 0
    try:
        if action_token == "collect_now":
            media_kind, count = _parse_collect_request_value(value)
            raw_result = _run_collect_once(
                repo_root=repo_root,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                profile=resolved_profile,
                telegram_chat_id=telegram_chat_id,
                media_kind=media_kind,
                count=count if count > 0 else None,
            )
            detail = _summarize_run(raw_result, f"{IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, '媒体')}立即采集")
            result_status = "done" if _guess_feedback_status(detail) == "success" else "failed"
            exit_code = 0 if result_status == "done" else 2
        elif action_token == "publish_run":
            media_kind, platform_value = _parse_publish_request_value(value)
            available_platforms = _collect_publish_target_platforms(media_kind)
            platforms = available_platforms.copy() if platform_value == "all" else [platform_value]
            if media_kind == "image" and platforms == ["xiaohongshu"]:
                raw_result = _run_direct_xiaohongshu_image_publish(
                    repo_root=repo_root,
                    workspace=workspace,
                    timeout_seconds=timeout_seconds,
                    profile=resolved_profile,
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                )
            elif media_kind == "image" and platforms == ["kuaishou"]:
                raw_result = _run_direct_kuaishou_image_publish(
                    repo_root=repo_root,
                    workspace=workspace,
                    timeout_seconds=timeout_seconds,
                    profile=resolved_profile,
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                )
            else:
                raw_result = _run_distribution_once(
                    repo_root=repo_root,
                    workspace=workspace,
                    timeout_seconds=timeout_seconds,
                    platforms=platforms,
                    profile=resolved_profile,
                    telegram_chat_id=telegram_chat_id,
                    publish_only=True,
                    media_kind=media_kind,
                )
            detail = _summarize_run(
                raw_result,
                f"{IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, '媒体')}立即发布（平台 {_format_platform_text(platforms)}）",
            )
            result_status = "done" if _guess_feedback_status(detail) == "success" else "failed"
            exit_code = 0 if result_status == "done" else 2
        elif action_token == "schedule_run":
            media_kind, minutes, platform_value = _parse_schedule_callback_value(value)
            available_platforms = _collect_publish_target_platforms(media_kind)
            platforms = available_platforms.copy() if platform_value in {"", "all"} else [platform_value]
            raw_result = _run_distribution_once(
                repo_root=repo_root,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                platforms=platforms,
                profile=resolved_profile,
                telegram_chat_id=telegram_chat_id,
                schedule_minutes=minutes,
                publish_only=True,
                media_kind=media_kind,
            )
            detail = _summarize_run(
                raw_result,
                f"{IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, '媒体')}定时发布（平台 {_format_platform_text(platforms)}，窗口 {minutes} 分钟）",
            )
            result_status = "done" if _guess_feedback_status(detail) == "success" else "failed"
            exit_code = 0 if result_status == "done" else 2
        elif action_token == "login_qr":
            platform_value = str(value or "").strip().lower()
            result = _request_platform_login_qr(
                platform_name=platform_value,
                bot_token=telegram_bot_token,
                chat_id=telegram_chat_id,
                timeout_seconds=timeout_seconds,
                log_file=workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log",
                refresh_page=True,
                prefer_login_entry=True,
            )
            if bool(result.get("sent")):
                detail = f"{PUBLISH_PLATFORM_DISPLAY.get(platform_value, platform_value)}登录二维码已发送，请查看最新消息。"
                result_status = "done"
                exit_code = 0
            elif not bool(result.get("needs_login", True)):
                detail = f"{PUBLISH_PLATFORM_DISPLAY.get(platform_value, platform_value)}当前已登录，无需扫码。"
                result_status = "done"
                exit_code = 0
            else:
                if bool(result.get("transport_error")):
                    detail = (
                        f"{PUBLISH_PLATFORM_DISPLAY.get(platform_value, platform_value)}登录二维码已抓取，"
                        "但 Telegram 回传失败（网络抖动），请稍后重试。"
                    )
                else:
                    detail = str(result.get("error") or "获取登录二维码失败，请稍后重试。")
                result_status = "failed"
                exit_code = 2
        elif action_token == "collect_publish_latest":
            media_kind, candidate_limit = _parse_collect_publish_request_value(value)
            media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(media_kind, "媒体")
            exit_code = _run_collect_publish_latest_job(
                repo_root=repo_root,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                profile=resolved_profile,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                candidate_limit=max(1, candidate_limit),
                media_kind=media_kind,
                immediate_test_mode=immediate_test_mode,
            )
            detail = f"{media_label}即采即发后台任务已结束，请查看后续预审卡片或总结结果。"
            result_status = "done" if exit_code == 0 else "failed"
        elif action_token == "comment_reply_run":
            comment_platform, comment_limit = _parse_comment_reply_request_value(value)
            exit_code = _run_comment_reply_job(
                repo_root=repo_root,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                profile=resolved_profile,
                telegram_bot_identifier=telegram_bot_identifier,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                platform=comment_platform,
                post_limit=max(1, int(comment_limit)),
            )
            detail = f"{_menu_platform_label(comment_platform)}评论自动回复后台任务已结束，请查看最新结果卡片。"
            result_status = "done" if exit_code == 0 else "failed"
        else:
            detail = "当前动作未实现。"
            result_status = "failed"
            exit_code = 2
    except Exception as exc:
        detail = f"后台任务执行异常：{exc}"
        result_status = "failed"
        exit_code = 1

    updated_task = _update_home_action_task(
        workspace,
        task_key,
        status="done" if result_status == "done" else "failed",
        detail=detail,
        pid=os.getpid(),
    )
    loading_message_id = int(updated_task.get("loading_message_id") or 0)
    if telegram_bot_token and telegram_chat_id and loading_message_id > 0:
        deleted = _try_delete_telegram_message(
            bot_token=telegram_bot_token,
            chat_id=telegram_chat_id,
            message_id=loading_message_id,
            timeout_seconds=max(10, int(timeout_seconds)),
            log_file=workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log",
        )
        if deleted:
            updated_task = _update_home_action_task(workspace, task_key, extra={"loading_message_id": 0})
    if telegram_bot_token and telegram_chat_id and action_token in {"collect_now", "publish_run", "schedule_run", "login_qr"}:
        try:
            card = _home_feedback_response(
                status="success" if result_status == "done" else "failed",
                title=_home_action_result_title(
                    action_token,
                    "done" if result_status == "done" else "failed",
                    value,
                    _describe_home_action_task(updated_task),
                ),
                subtitle=f"当前配置：{resolved_profile}",
                detail=_describe_home_action_task(updated_task),
                menu_label=_menu_breadcrumb_for_action(action_token, value),
                task_identifier=_build_task_identifier(
                    action=action_token,
                    value=value,
                    log_path=str(updated_task.get("log_path") or ""),
                    updated_at=str(updated_task.get("updated_at") or ""),
                ),
            )
            if action_token == "publish_run":
                media_kind, platform_value = _parse_publish_request_value(value)
                publish_platforms = _collect_publish_target_platforms(media_kind) if platform_value == "all" else [platform_value]
                card = _build_distribution_result_card(
                    action_token=action_token,
                    result_status=result_status,
                    resolved_profile=resolved_profile,
                    platforms=publish_platforms,
                    raw_result=raw_result,
                    media_kind=media_kind,
                )
            elif action_token == "schedule_run":
                _media_kind, minutes, platform_value = _parse_schedule_callback_value(value)
                schedule_platforms = _collect_publish_target_platforms(_media_kind) if platform_value in {"", "all"} else [platform_value]
                card = _build_distribution_result_card(
                    action_token=action_token,
                    result_status=result_status,
                    resolved_profile=resolved_profile,
                    platforms=schedule_platforms,
                    raw_result=raw_result,
                    minutes=minutes,
                    media_kind=_media_kind,
                )
            elif action_token == "collect_now":
                media_kind, _count = _parse_collect_request_value(value)
                card = _build_collect_result_card(
                    result_status=result_status,
                    resolved_profile=resolved_profile,
                    media_kind=media_kind,
                    raw_result=raw_result,
                )
            _send_card_message(
                bot_token=telegram_bot_token,
                chat_id=telegram_chat_id,
                card=card,
                timeout_seconds=max(10, int(timeout_seconds)),
            )
        except Exception:
            _enqueue_pending_background_feedback(
                workspace=workspace,
                title=_home_action_result_title(
                    action_token,
                    "done" if result_status == "done" else "failed",
                    value,
                    _describe_home_action_task(updated_task),
                ),
                subtitle=f"当前配置：{resolved_profile}",
                sections=[{"title": "处理状态", "emoji": "📌", "items": [_describe_home_action_task(updated_task)]}],
                status="success" if result_status == "done" else "failed",
            )
    return int(exit_code)


def _run_comment_reply_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    platform: str,
    post_limit: int,
) -> int:
    requested_platform = str(platform or "").strip().lower()
    comment_platform = "all" if requested_platform in {"", "all"} else (_normalize_platform_tokens([requested_platform])[0] if _normalize_platform_tokens([requested_platform]) else "wechat")
    target_platforms = _resolve_comment_reply_platforms(comment_platform)
    try:
        _, core = _load_runtime_modules()
        engagement_module = _load_engagement_module()
        runtime_config: dict[str, Any] = {}
        workspace_ctx = None
        if "wechat" in target_platforms:
            workspace_ctx = core.init_workspace(str(workspace))
            config_path = _default_runtime_config_path(repo_root)
            runtime_config = core._load_runtime_config(str(config_path))

        platform_results: list[dict[str, Any]] = []
        all_records: list[dict[str, Any]] = []
        total_scanned = 0
        total_selected = 0
        total_replies = 0
        for platform_name in target_platforms:
            if platform_name == "wechat":
                wechat_runtime_ctx = _resolve_platform_login_runtime_context(core, "wechat")
                platform_result = core.run_wechat_comment_reply(
                    workspace=workspace_ctx,
                    runtime_config=runtime_config,
                    debug_port=int(wechat_runtime_ctx.get("debug_port") or getattr(core, "DEFAULT_WECHAT_DEBUG_PORT", 9334)),
                    chrome_path=None,
                    chrome_user_data_dir=str(wechat_runtime_ctx.get("chrome_user_data_dir") or ""),
                    auto_open_chrome=True,
                    max_posts_override=max(1, int(post_limit)),
                    max_replies_override=0,
                    latest_only=False,
                    debug=bool(runtime_config.get("comment_reply", {}).get("debug")) if isinstance(runtime_config.get("comment_reply"), dict) else False,
                    telegram_bot_identifier=telegram_bot_identifier,
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                    telegram_registry_file="",
                    telegram_timeout_seconds=max(10, int(timeout_seconds)),
                    telegram_api_base="",
                    notify_env_prefix=str(getattr(core, "DEFAULT_NOTIFY_ENV_PREFIX", "CYBERCAR_NOTIFY_")),
                )
            elif platform_name == "douyin":
                platform_result = engagement_module.run_douyin_engagement(
                    max_posts=max(1, int(post_limit)),
                    max_replies=0,
                    like_only=False,
                    latest_only=False,
                    debug=True,
                )
            elif platform_name == "kuaishou":
                platform_result = engagement_module.run_kuaishou_engagement(
                    max_posts=max(1, int(post_limit)),
                    max_replies=0,
                    like_only=False,
                    latest_only=False,
                    debug=True,
                )
            else:
                platform_result = {
                    "ok": False,
                    "platform": platform_name,
                    "reason": f"unsupported comment reply platform: {platform_name}",
                    "records": [],
                    "posts_scanned": 0,
                    "posts_selected": 0,
                    "replies_sent": 0,
                }
            if not isinstance(platform_result, dict):
                platform_result = {
                    "ok": False,
                    "platform": platform_name,
                    "reason": "invalid_result",
                    "records": [],
                    "posts_scanned": 0,
                    "posts_selected": 0,
                    "replies_sent": 0,
                }
            platform_result["platform"] = platform_name
            platform_results.append(platform_result)
            total_scanned += int(platform_result.get("posts_scanned") or 0)
            total_selected += int(platform_result.get("posts_selected") or 0)
            total_replies += int(platform_result.get("replies_sent") or 0)
            for raw in (platform_result.get("records") if isinstance(platform_result.get("records"), list) else []):
                if not isinstance(raw, dict):
                    continue
                item = dict(raw)
                item["platform"] = platform_name
                all_records.append(item)
        joined_state_paths = " | ".join(
            str(item.get("state_path") or "").strip()
            for item in platform_results
            if isinstance(item, dict) and str(item.get("state_path") or "").strip()
        )
        joined_markdown_paths = " | ".join(
            str(item.get("markdown_path") or "").strip()
            for item in platform_results
            if isinstance(item, dict) and str(item.get("markdown_path") or "").strip()
        )
        failed_reasons = [
            f"{_menu_platform_label(str(item.get('platform') or ''))}: {str(item.get('reason') or '').strip()}"
            for item in platform_results
            if isinstance(item, dict) and str(item.get("reason") or "").strip()
        ]
        result = {
            "ok": total_replies > 0 or any(bool(item.get("ok")) for item in platform_results if isinstance(item, dict)),
            "platform": "all" if len(target_platforms) > 1 else target_platforms[0],
            "target_platforms": target_platforms,
            "reason": "; ".join(failed_reasons),
            "state_path": joined_state_paths,
            "markdown_path": joined_markdown_paths,
            "records": all_records,
            "posts_scanned": total_scanned,
            "posts_selected": total_selected,
            "replies_sent": total_replies,
            "platform_results": platform_results,
        }
        if telegram_bot_token and telegram_chat_id:
            _send_card_message(
                bot_token=telegram_bot_token,
                chat_id=telegram_chat_id,
                card=_build_comment_reply_result_card(result if isinstance(result, dict) else {}),
                timeout_seconds=max(10, int(timeout_seconds)),
            )
            for detail_text in _build_comment_reply_record_texts(result if isinstance(result, dict) else {}):
                _send_reply(
                    bot_token=telegram_bot_token,
                    chat_id=telegram_chat_id,
                    text=detail_text,
                    timeout_seconds=max(10, int(timeout_seconds)),
                )
        return 0 if bool(result.get("ok")) else 2
    except Exception as exc:
        if telegram_bot_token and telegram_chat_id:
            try:
                _send_reply(
                    bot_token=telegram_bot_token,
                    chat_id=telegram_chat_id,
                    text=f"点赞评论失败\n错误：{exc}",
                    timeout_seconds=max(10, int(timeout_seconds)),
                )
            except Exception:
                pass
        return 1


def _run_collect_publish_latest_job(
    *,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    profile: str,
    telegram_bot_identifier: str = "",
    telegram_bot_token: str,
    telegram_chat_id: str,
    candidate_limit: int,
    media_kind: str = "video",
    immediate_test_mode: bool = False,
) -> int:
    runner, core = _load_runtime_modules()
    _cleanup_prefilter_queue(
        workspace,
        log_file=workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log",
    )
    args = _build_immediate_publish_args(
        runner=runner,
        workspace=workspace,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
    )
    email_settings = runner._build_email_settings(args)
    normalized_media_kind = _normalize_immediate_collect_media_kind(media_kind)
    media_label = IMMEDIATE_COLLECT_MEDIA_KIND_DISPLAY.get(normalized_media_kind, "媒体")
    target_platforms = _collect_publish_target_platforms(normalized_media_kind)
    requested_limit = max(1, int(candidate_limit))
    discovery_limit = _resolve_collect_publish_discovery_limit(requested_limit)
    round_limits = _resolve_collect_publish_round_limits(requested_limit, discovery_limit)
    source_platforms = _resolve_collect_publish_source_platforms(repo_root=repo_root, profile=profile)
    _send_background_feedback(
        runner=runner,
        email_settings=email_settings,
        workspace=workspace,
        title=f"{media_label}即采即发候选扫描中",
        subtitle=_collect_publish_source_target_subtitle(source_platforms, requested_limit),
        sections=[
            _build_immediate_task_overview_section(
                requested_limit=requested_limit,
                platforms=target_platforms,
                discovery_limit=discovery_limit,
            ),
            {
                "title": "执行说明",
                "emoji": "⏳",
                "items": [
                    _collect_publish_scan_detail(source_platforms, media_label),
                    f"只有你明确点击“普通发布”或“原创发布”的候选，才会进入下载和{_format_platform_text(target_platforms)}发布。",
                ],
            },
        ],
        status="running",
        platforms=target_platforms,
        menu_label=_menu_breadcrumb_for_action("collect_publish_latest", f"{normalized_media_kind}:{requested_limit}"),
    )
    workspace_ctx = runner.core.init_workspace(str(workspace))
    filter_seen_urls = getattr(core, "_filter_already_processed_x_urls", None)
    filter_workspace_ctx = workspace_ctx
    modern_engine = None
    if not callable(filter_seen_urls):
        try:
            from cybercar import engine as modern_engine
        except Exception:
            modern_engine = None  # type: ignore[assignment]
        filter_seen_urls = getattr(modern_engine, "_filter_already_processed_x_urls", None)
    if callable(filter_seen_urls) and not hasattr(filter_workspace_ctx, "root"):
        if modern_engine is None:
            try:
                from cybercar import engine as modern_engine
            except Exception:
                modern_engine = None  # type: ignore[assignment]
        if modern_engine is not None:
            try:
                filter_workspace_ctx = modern_engine.init_workspace(str(workspace))
            except Exception:
                filter_workspace_ctx = workspace_ctx
    filtered_seen_candidates = 0
    same_story_collapsed = 0
    media_kind_filtered_candidates = 0
    total_candidates = 0
    discovery_rounds_used = 0
    raw_candidates_discovered = 0
    candidates: list[dict[str, Any]] = []
    seen_discovered_urls: set[str] = set()
    for round_no, round_limit in enumerate(round_limits, start=1):
        discovery_rounds_used = round_no
        discovered = _discover_latest_live_candidates(
            repo_root=repo_root,
            timeout_seconds=timeout_seconds,
            profile=profile,
            candidate_limit=requested_limit,
            discovery_limit=round_limit,
            allow_search_inferred_match=immediate_test_mode,
            include_images=(normalized_media_kind == "image"),
            source_platforms=source_platforms,
        )
        discovered_source_platforms = _normalize_collect_source_platforms(discovered.get("source_platforms"))
        if discovered_source_platforms:
            source_platforms = discovered_source_platforms
        raw_candidates = [
            dict(item)
            for item in (discovered.get("candidates") or [])
            if isinstance(item, dict) and _candidate_source_url(item)
        ]
        fresh_round_candidates: list[dict[str, Any]] = []
        for candidate in raw_candidates:
            source_url = _candidate_source_url(candidate)
            if not source_url or source_url in seen_discovered_urls:
                continue
            seen_discovered_urls.add(source_url)
            fresh_round_candidates.append(candidate)
        raw_candidates_discovered += len(fresh_round_candidates)
        total_candidates = len(seen_discovered_urls)
        if fresh_round_candidates and normalized_media_kind == "image":
            image_round_candidates: list[dict[str, Any]] = []
            for candidate in fresh_round_candidates:
                if _candidate_matches_collect_publish_media_kind(candidate, normalized_media_kind):
                    image_round_candidates.append(candidate)
                else:
                    media_kind_filtered_candidates += 1
            fresh_round_candidates = image_round_candidates
        if not fresh_round_candidates:
            continue
        filtered_round_candidates = fresh_round_candidates
        if callable(filter_seen_urls):
            filtered_urls, skipped_urls = filter_seen_urls(
                filter_workspace_ctx,
                [_candidate_source_url(item) for item in fresh_round_candidates],
            )
            filtered_url_set = {str(url or "").strip() for url in filtered_urls if str(url or "").strip()}
            filtered_round_candidates = [
                item
                for item in fresh_round_candidates
                if _candidate_source_url(item) in filtered_url_set
            ]
            filtered_seen_candidates += len(skipped_urls)
        if not filtered_round_candidates:
            continue
        collapsed_existing = candidates + filtered_round_candidates
        collapsed_all, collapsed_count = _collapse_collect_publish_same_story_candidates(collapsed_existing)
        same_story_collapsed += collapsed_count
        candidates = collapsed_all
        if len(candidates) >= requested_limit:
            break
    candidates = candidates[:requested_limit]
    if not candidates:
        if (
            filtered_seen_candidates > 0
            or same_story_collapsed > 0
            or media_kind_filtered_candidates > 0
            or raw_candidates_discovered > 0
        ):
            _send_background_feedback(
                runner=runner,
                email_settings=email_settings,
                workspace=workspace,
                title=f"{media_label}即采即发候选已跳过",
                subtitle=_collect_publish_source_recent_subtitle(source_platforms, requested_limit),
                sections=[
                    _build_immediate_task_overview_section(
                        requested_limit=requested_limit,
                        platforms=target_platforms,
                        discovery_limit=round_limits[-1] if round_limits else discovery_limit,
                        discovered_count=total_candidates,
                        sent_count=0,
                        reused_count=0,
                        skipped_count=filtered_seen_candidates + media_kind_filtered_candidates,
                        collapsed_count=same_story_collapsed,
                        discovery_rounds=discovery_rounds_used,
                    ),
                    {
                        "title": "结果说明",
                        "emoji": "🧹",
                        "items": [
                            "本轮扫描到了候选，但历史账本过滤和同题材折叠后，没有新的预审卡需要发出。",
                            *(
                                [f"其中有 {media_kind_filtered_candidates} 条候选不包含图片，已按图片模式自动过滤。"]
                                if media_kind_filtered_candidates > 0
                                else []
                            ),
                        ],
                    },
                ],
                status="done",
                platforms=target_platforms,
                menu_label=_menu_breadcrumb_for_action("collect_publish_latest", f"{normalized_media_kind}:{requested_limit}"),
            )
            return 0
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title=f"{media_label}即采即发未发现候选",
            subtitle=_collect_publish_source_recent_subtitle(source_platforms, requested_limit),
            sections=[
                _build_immediate_task_overview_section(
                    requested_limit=requested_limit,
                    platforms=target_platforms,
                    discovery_limit=round_limits[-1] if round_limits else discovery_limit,
                    discovered_count=0,
                    skipped_count=filtered_seen_candidates + media_kind_filtered_candidates,
                    collapsed_count=same_story_collapsed,
                    discovery_rounds=discovery_rounds_used,
                ),
                {
                    "title": "结果说明",
                    "emoji": "⚠️",
                    "items": [
                        _collect_publish_no_candidate_detail(source_platforms, media_label),
                        *([f"本轮已有 {filtered_seen_candidates} 条历史候选被采集账本直接过滤。"] if filtered_seen_candidates > 0 else []),
                    ],
                }
            ],
            status="failed",
            platforms=target_platforms,
            menu_label=_menu_breadcrumb_for_action("collect_publish_latest", f"{normalized_media_kind}:{requested_limit}"),
        )
        return 2

    sent_candidates = 0
    reused_candidates = 0
    reissued_candidates = 0
    skipped_duplicates = 0
    fresh_candidates = 0
    attempted_new_candidates = 0
    last_send_error = ""
    reusable_items_to_reissue: list[tuple[str, Dict[str, Any]]] = []
    for idx, candidate in enumerate(candidates, start=1):
        if fresh_candidates >= requested_limit or attempted_new_candidates >= requested_limit:
            break
        upserted = _upsert_immediate_candidate_item(
            workspace=workspace,
            candidate=candidate,
            profile=profile,
            media_kind=normalized_media_kind,
            target_platforms=",".join(target_platforms),
            chat_id=telegram_chat_id,
            item_index=idx,
            total_count=requested_limit,
            allow_reuse=(not immediate_test_mode),
        )
        item = upserted.get("item") if isinstance(upserted, dict) else {}
        item_id = str(upserted.get("item_id") or "").strip()
        already_sent = bool(upserted.get("already_sent"))
        if item_id and isinstance(item, dict):
            item = _recover_orphaned_immediate_candidate(
                workspace=workspace,
                item_id=item_id,
                item=item,
            )
        current_status = str(item.get("status") or "").strip().lower() if isinstance(item, dict) else ""
        has_active_pending_card = (
            (not immediate_test_mode)
            and isinstance(item, dict)
            and current_status == "link_pending"
            and int(item.get("message_id") or 0) > 0
        )
        if current_status in IMMEDIATE_CANDIDATE_TERMINAL_SKIP_STATUSES:
            skipped_duplicates += 1
            continue
        if has_active_pending_card:
            reused_candidates += 1
            continue
        if already_sent:
            if current_status == "down_confirmed":
                skipped_duplicates += 1
                continue
            reused_candidates += 1
            if _should_reissue_immediate_candidate_card(item):
                reusable_items_to_reissue.append((item_id, dict(item)))
            continue
        if not item_id or not isinstance(item, dict):
            skipped_duplicates += 1
            continue
        if str(item.get("action") or "").strip().lower() != "resent_in_test_mode":
            item = _preflight_immediate_candidate_for_prefilter(
                workspace=workspace,
                item_id=item_id,
                item=item,
                telegram_bot_identifier=telegram_bot_identifier,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                timeout_seconds=timeout_seconds,
                log_file=workspace / DEFAULT_LOG_SUBDIR / "telegram_command_worker.log",
            )
        attempted_new_candidates += 1
        try:
            response = _send_immediate_candidate_prefilter_card(
                runner=runner,
                email_settings=email_settings,
                workspace=workspace,
                item=item,
                workspace_ctx=workspace_ctx,
            )
            result_payload = response.get("result") if isinstance(response, dict) else {}
            if not isinstance(result_payload, dict):
                result_payload = {}
            chat_payload = result_payload.get("chat") if isinstance(result_payload.get("chat"), dict) else {}
            message_id = int(result_payload.get("message_id") or 0)
            if message_id <= 0:
                raise RuntimeError("telegram candidate prefilter message_id missing")
            _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "link_pending",
                    "message_id": message_id,
                    "chat_id": str(chat_payload.get("id") or telegram_chat_id or ""),
                    "action": "sent",
                    "last_error": "",
                    "prefilter_retry_pending": False,
                    "prefilter_last_retry_epoch": 0.0,
                    "prefilter_last_retry_at": "",
                },
            )
            sent_candidates += 1
            fresh_candidates += 1
        except Exception as exc:
            retry_exc: Exception = exc
            try:
                retry_response = _send_immediate_candidate_prefilter_card(
                    runner=runner,
                    email_settings=email_settings,
                    workspace=workspace,
                    item=item,
                    workspace_ctx=workspace_ctx,
                    fast_send=False,
                )
                retry_result_payload = retry_response.get("result") if isinstance(retry_response, dict) else {}
                if not isinstance(retry_result_payload, dict):
                    retry_result_payload = {}
                retry_chat_payload = retry_result_payload.get("chat") if isinstance(retry_result_payload.get("chat"), dict) else {}
                retry_message_id = int(retry_result_payload.get("message_id") or 0)
                if retry_message_id <= 0:
                    raise RuntimeError("telegram candidate prefilter message_id missing")
                _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "link_pending",
                        "message_id": retry_message_id,
                        "chat_id": str(retry_chat_payload.get("id") or telegram_chat_id or ""),
                        "action": "sent_after_retry",
                        "last_error": "",
                        "prefilter_retry_pending": False,
                        "prefilter_last_retry_epoch": 0.0,
                        "prefilter_last_retry_at": "",
                    },
                )
                sent_candidates += 1
                fresh_candidates += 1
                continue
            except Exception as delayed_exc:
                retry_exc = delayed_exc
            last_send_error = _preview_text(str(retry_exc), limit=160)
            _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "send_failed",
                    "action": "send_failed",
                    "last_error": str(retry_exc),
                    "message_id": 0,
                    "prefilter_retry_pending": True,
                    "prefilter_retry_count": 0,
                    "prefilter_last_retry_epoch": 0.0,
                    "prefilter_last_retry_at": "",
                },
            )

    if (not immediate_test_mode) and fresh_candidates <= 0 and reusable_items_to_reissue:
        for reusable_item_id, reusable_item in reusable_items_to_reissue[:requested_limit]:
            try:
                _reissue_immediate_candidate_prefilter_card(
                    runner=runner,
                    email_settings=email_settings,
                    workspace=workspace,
                    item_id=reusable_item_id,
                    item=reusable_item,
                    telegram_chat_id=telegram_chat_id,
                    workspace_ctx=workspace_ctx,
                )
                reissued_candidates += 1
            except Exception as exc:
                last_send_error = _preview_text(str(exc), limit=160)
                _update_prefilter_item(
                    workspace,
                    reusable_item_id,
                    updates={
                        "action": "resent_existing_failed",
                        "last_error": str(exc),
                    },
                )
        if reissued_candidates > 0:
            sent_candidates += reissued_candidates
            reused_candidates = max(0, reused_candidates - reissued_candidates)

    if sent_candidates > 0 or reused_candidates > 0:
        next_step_items: list[str] = []
        if reissued_candidates > 0:
            next_step_items.append("本轮命中的历史在审候选已重发当前状态卡；直接从新卡继续处理即可。")
        if reused_candidates > 0 and not immediate_test_mode:
            next_step_items.append("其中有部分候选已存在历史预审记录，本轮未重复创建新的旧卡片。")
        if fresh_candidates == 0 and reissued_candidates == 0 and reused_candidates > 0 and not immediate_test_mode:
            next_step_items.append("这次命中的都是已在处理或已处理过的候选；请查看最近的候选卡片继续操作。")
        if immediate_test_mode:
            next_step_items.append("测试模式已放宽候选匹配并强制重发新预审卡，后续仍会进入真实采集和发布链路。")
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title=f"{media_label}即采即发候选已整理",
            subtitle=_collect_publish_source_recent_subtitle(source_platforms, requested_limit),
            sections=[
                _build_immediate_task_overview_section(
                    requested_limit=requested_limit,
                    platforms=target_platforms,
                    discovery_limit=round_limits[-1] if round_limits else discovery_limit,
                    discovered_count=total_candidates,
                    sent_count=fresh_candidates + reissued_candidates,
                    reused_count=reused_candidates,
                    skipped_count=skipped_duplicates + filtered_seen_candidates + media_kind_filtered_candidates,
                    collapsed_count=same_story_collapsed,
                    discovery_rounds=discovery_rounds_used,
                ),
                *(
                    [
                        {
                            "title": "下一步",
                            "emoji": "🧭",
                            "items": next_step_items,
                        }
                    ]
                    if next_step_items
                    else []
                ),
            ],
            status="done",
            platforms=target_platforms,
            menu_label=_menu_breadcrumb_for_action("collect_publish_latest", f"{normalized_media_kind}:{requested_limit}"),
        )
        return 0

    if skipped_duplicates > 0 or filtered_seen_candidates > 0 or media_kind_filtered_candidates > 0:
        _send_background_feedback(
            runner=runner,
            email_settings=email_settings,
            workspace=workspace,
            title=f"{media_label}即采即发候选已跳过",
            subtitle=_collect_publish_source_recent_subtitle(source_platforms, requested_limit),
            sections=[
                _build_immediate_task_overview_section(
                    requested_limit=requested_limit,
                    platforms=target_platforms,
                    discovery_limit=round_limits[-1] if round_limits else discovery_limit,
                    discovered_count=total_candidates,
                    sent_count=0,
                    reused_count=0,
                    skipped_count=skipped_duplicates + filtered_seen_candidates + media_kind_filtered_candidates,
                    collapsed_count=same_story_collapsed,
                    discovery_rounds=discovery_rounds_used,
                ),
                {
                    "title": "结果说明",
                    "emoji": "🧹",
                    "items": [
                        "本轮命中的候选都已在历史采集/审核/发布记录中出现，或被同题材折叠，因此未重复创建新预审卡。",
                    ],
                },
            ],
            status="done",
            platforms=target_platforms,
            menu_label=_menu_breadcrumb_for_action("collect_publish_latest", f"{normalized_media_kind}:{requested_limit}"),
        )
        return 0

    _send_background_feedback(
        runner=runner,
        email_settings=email_settings,
        workspace=workspace,
        title=f"{media_label}即采即发预审发送失败",
        subtitle=_collect_publish_source_recent_subtitle(source_platforms, requested_limit),
        sections=[
            _build_immediate_task_overview_section(
                requested_limit=requested_limit,
                platforms=target_platforms,
                discovery_limit=round_limits[-1] if round_limits else discovery_limit,
                discovered_count=total_candidates,
                sent_count=sent_candidates,
                reused_count=reused_candidates,
                skipped_count=skipped_duplicates + filtered_seen_candidates + media_kind_filtered_candidates,
                collapsed_count=same_story_collapsed,
                discovery_rounds=discovery_rounds_used,
            ),
            {
                "title": "失败线索",
                "emoji": "⚠️",
                "items": (
                    [
                        f"已尝试发送前 {max(1, attempted_new_candidates)} 条新候选，但 Telegram 预审卡未成功送达。",
                        "当前更像 Telegram 网络抖动，而不是候选扫描失败。",
                        "失败候选已保留到待补发队列；worker 轮询恢复后会自动重试送达预审卡。",
                    ]
                    + ([f"最后错误：{last_send_error}"] if last_send_error else [])
                ),
            }
        ],
        status="failed",
        platforms=target_platforms,
        menu_label=_menu_breadcrumb_for_action("collect_publish_latest", f"{normalized_media_kind}:{requested_limit}"),
    )
    return 3


def _is_allowed_command(command: str, allow_prefixes: list[str]) -> tuple[bool, str]:
    body = str(command or "").strip()
    if not body:
        return False, "empty command"
    lowered = body.lower()
    for prefix in allow_prefixes:
        p = str(prefix or "").strip()
        if not p:
            continue
        pl = p.lower()
        if lowered == pl or lowered.startswith(pl + " "):
            return True, p
    return False, "not in allowlist"


def _append_audit(
    audit_file: Path,
    *,
    update_id: int,
    chat_id: str,
    username: str,
    action: str,
    command: str,
    allowed: bool,
    reason: str,
    exit_code: Optional[int] = None,
    elapsed_seconds: Optional[float] = None,
    task_identifier: str = "",
) -> None:
    ts = _now_text()
    payload: Dict[str, Any] = {
        "ts": ts,
        "update_id": int(update_id),
        "chat_id": str(chat_id or ""),
        "username": str(username or ""),
        "action": str(action or ""),
        "command": str(command or ""),
        "allowed": bool(allowed),
        "reason": str(reason or ""),
    }
    resolved_task_identifier = str(task_identifier or "").strip() or _build_audit_task_identifier(action, command, ts)
    if resolved_task_identifier:
        payload["task_identifier"] = resolved_task_identifier
    if exit_code is not None:
        payload["exit_code"] = int(exit_code)
    if elapsed_seconds is not None:
        payload["elapsed_seconds"] = round(float(elapsed_seconds), 3)
    _append_jsonl(audit_file, payload)


def _help_text() -> str:
    command_lines = [
        f"{idx}. {item.get('usage')} {item.get('help_text')}"
        for idx, item in enumerate(TELEGRAM_COMMAND_SPECS, start=1)
    ]
    return (
        f"{BOT_NAME} 入口说明\n"
        "手机端命令菜单已切到固定底部快捷入口。\n"
        "日常操作优先使用底部快捷键；下面这些 slash 命令仅作为兜底。\n\n"
        "常用入口：/start、国内即采即发、海外即采即发、登录、点赞评论。\n\n"
        f"{chr(10).join(command_lines)}\n\n"
        "示例：\n"
        "- /start\n"
        "- /menu\n"
        "- 国内即采即发\n"
        "- 海外即采即发\n"
        "- 登录"
    )


def _help_response(default_profile: str = DEFAULT_PROFILE) -> Dict[str, Any]:
    return _home_response(default_profile, "已切到首页入口")
def _build_status(
    *,
    started_at: float,
    repo_root: Path,
    workspace: Path,
    allow_shell: bool,
    allow_prefixes: list[str],
    last_processed: int,
) -> str:
    uptime = max(0, int(time.time() - started_at))
    runtime_state = _inspect_runtime_execution_state(workspace)
    lock_state = runtime_state.get("lock", {})
    active_tasks = list(runtime_state.get("active_tasks") or [])
    waiting_prefilter = runtime_state.get("waiting_prefilter", {})
    if bool(lock_state.get("alive")):
        lock_text = (
            f"占用中｜{lock_state.get('mode') or 'pipeline'}｜"
            f"PID {int(lock_state.get('pid') or 0)}｜约 {int(lock_state.get('age_minutes') or 0)} 分钟"
        )
    elif bool(lock_state.get("exists")):
        lock_text = "残留锁文件"
    else:
        lock_text = "空闲"
    if active_tasks:
        preview = active_tasks[0]
        task_text = (
            f"{_home_action_title(str(preview.get('action') or ''))}｜"
            f"{str(preview.get('status') or '').strip().lower() or '-'}｜共 {len(active_tasks)} 条"
        )
    else:
        task_text = "无"
    waiting_text = str(int(waiting_prefilter.get("count") or 0))
    return (
        f"{BOT_NAME} 机器人状态：在线\\n"
        f"运行时长：{uptime}s\n"
        f"仓库目录：{repo_root}\n"
        f"工作区：{workspace}\n"
        f"全局锁：{lock_text}\n"
        f"互斥任务：{task_text}\n"
        f"即采即发等待锁：{waiting_text}\n"
        f"允许 Shell：{allow_shell}\n"
        f"允许前缀：{', '.join(allow_prefixes) if allow_prefixes else '（无）'}\n"
        f"最近处理 UpdateId：{last_processed}"
    )


def _parse_status_command(text: str) -> Optional[str]:
    m = re.match(r"(?is)^\s*/status(?:@[A-Za-z0-9_]+)?(?:\s+(.+))?\s*$", str(text or ""))
    if not m:
        return None
    return str(m.group(1) or "").strip()


def _parse_run_command(text: str) -> Optional[tuple[str, str]]:
    m = re.match(r"(?is)^\s*/run(?:@[A-Za-z0-9_]+)?\s+(\S+)\s+(.+?)\s*$", str(text or ""))
    if not m:
        return None
    return str(m.group(1) or "").strip(), str(m.group(2) or "").strip()


def _parse_slash_command_request(raw: str) -> Dict[str, Any]:
    text = str(raw or "").strip()
    m = re.match(r"(?is)^\s*/([A-Za-z0-9_]+)(?:@[A-Za-z0-9_]+)?(?:\s+(.*))?\s*$", text)
    if not m:
        return {"matched": False}
    name = str(m.group(1) or "").strip().lower()
    tail = str(m.group(2) or "").strip()

    if name in {"start", "menu"}:
        return {"matched": True, "action": "menu"}
    if name == "help":
        return {"matched": True, "action": "help"}
    if name == "run_collect_task":
        return {"matched": True, "action": "run_collect_task"}
    if name == "run_publish_task":
        return {"matched": True, "action": "run_publish_task"}
    if name == "wechat_login_qr":
        return {"matched": True, "action": "wechat_login_qr"}
    if name == "collect_log":
        return {"matched": True, "action": "collect_log"}
    if name == "publish_log":
        return {"matched": True, "action": "publish_log"}
    if name == "worker_status":
        return {"matched": True, "action": "worker_status"}
    return {"matched": False}


def _handle_command(
    *,
    text: str,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    allow_shell: bool,
    allow_prefixes: list[str],
    command_password: str,
    started_at: float,
    last_processed: int,
    update_id: int,
    chat_id: str,
    username: str,
    audit_file: Path,
    telegram_bot_identifier: str = "",
    default_profile: str = DEFAULT_PROFILE,
) -> Any:
    cmd = str(text or "").strip()
    token = cmd.lower()
    cmd_key = _normalize_command_key(cmd)
    runtime_bot_identifier = str(
        telegram_bot_identifier
        or _env_first(
            "CYBERCAR_NOTIFY_TELEGRAM_BOT_IDENTIFIER",
            "CYBERCAR_NOTIFY_TELEGRAM_KEYWORD",
            "NOTIFY_TELEGRAM_BOT_IDENTIFIER",
            "NOTIFY_TELEGRAM_KEYWORD",
            default="cybercar",
        )
        or ""
    ).strip()
    notify_chat_id = str(chat_id or "").strip()
    resolved_default_profile = _normalize_profile_name(default_profile)

    # /status <password>
    status_password = _parse_status_command(cmd)
    if status_password is not None:
        if not command_password:
            _append_audit(
                audit_file,
                update_id=update_id,
                chat_id=chat_id,
                username=username,
                action="status",
                command=cmd,
                allowed=False,
                reason="password not configured",
            )
            return "status disabled: command password is not configured."
        if status_password != command_password:
            _append_audit(
                audit_file,
                update_id=update_id,
                chat_id=chat_id,
                username=username,
                action="status",
                command=cmd,
                allowed=False,
                reason="bad password",
            )
            return "status denied: bad password."
        _append_audit(
            audit_file,
            update_id=update_id,
            chat_id=chat_id,
            username=username,
            action="status",
            command=cmd,
            allowed=True,
            reason="ok",
        )
        return _build_status(
            started_at=started_at,
            repo_root=repo_root,
            workspace=workspace,
            allow_shell=allow_shell,
            allow_prefixes=allow_prefixes,
            last_processed=last_processed,
        )

    # /run <password> <command>
    run_args = _parse_run_command(cmd)
    if run_args is not None:
        supplied_password, run_body = run_args
        if not allow_shell:
            _append_audit(
                audit_file,
                update_id=update_id,
                chat_id=chat_id,
                username=username,
                action="run",
                command=run_body,
                allowed=False,
                reason="allow_shell disabled",
            )
            return "run denied: allow_shell disabled."
        if not command_password:
            _append_audit(
                audit_file,
                update_id=update_id,
                chat_id=chat_id,
                username=username,
                action="run",
                command=run_body,
                allowed=False,
                reason="password not configured",
            )
            return "run denied: command password is not configured."
        if supplied_password != command_password:
            _append_audit(
                audit_file,
                update_id=update_id,
                chat_id=chat_id,
                username=username,
                action="run",
                command=run_body,
                allowed=False,
                reason="bad password",
            )
            return "run denied: bad password."
        ok, reason = _is_allowed_command(run_body, allow_prefixes)
        if not ok:
            _append_audit(
                audit_file,
                update_id=update_id,
                chat_id=chat_id,
                username=username,
                action="run",
                command=run_body,
                allowed=False,
                reason=reason,
            )
            return f"run denied: {reason}."
        result = _run_cmd(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", run_body],
            timeout_seconds=timeout_seconds,
            workdir=repo_root,
        )
        _append_audit(
            audit_file,
            update_id=update_id,
            chat_id=chat_id,
            username=username,
            action="run",
            command=run_body,
            allowed=True,
            reason="ok",
            exit_code=int(result.get("code") or 0),
            elapsed_seconds=float(result.get("elapsed") or 0.0),
        )
        out = str(result.get("stdout") or "").strip()
        err = str(result.get("stderr") or "").strip()
        msg_lines = [
            f"run: {run_body}",
            f"result: {'ok' if result.get('ok') else 'failed'}",
            f"exit_code: {result.get('code')}",
        ]
        if out:
            msg_lines.extend(["", "stdout:", out[-2000:]])
        if err:
            msg_lines.extend(["", "stderr:", err[-1200:]])
        return "\n".join(msg_lines)

    slash_req = _parse_slash_command_request(cmd)
    if bool(slash_req.get("matched")):
        error = str(slash_req.get("error") or "").strip()
        if error:
            return error
        action = str(slash_req.get("action") or "").strip()
        if action == "menu":
            return _home_response(
                resolved_default_profile,
                "已打开底部菜单入口",
                workspace=workspace,
                chat_id=chat_id,
            )
        if action == "help":
            return _help_response(resolved_default_profile)
        if action == "run_collect_task":
            result = _run_cmd(
                ["schtasks", "/Run", "/TN", DEFAULT_CRAWL_TASK_NAME],
                timeout_seconds=min(timeout_seconds, 120),
                workdir=repo_root,
            )
            return _summarize_run(result, "执行采集任务")
        if action == "run_publish_task":
            result = _run_cmd(
                ["schtasks", "/Run", "/TN", DEFAULT_DISTRIBUTION_TASK_NAME],
                timeout_seconds=min(timeout_seconds, 120),
                workdir=repo_root,
            )
            return _summarize_run(result, "执行发布任务")
        if action == "wechat_login_qr":
            result = _request_platform_login_qr(
                platform_name="wechat",
                bot_token="",
                chat_id=notify_chat_id,
                timeout_seconds=timeout_seconds,
                log_file=audit_file.parent / "telegram_command_worker.log",
                refresh_page=True,
                prefer_login_entry=True,
            )
            if bool(result.get("sent")):
                return "视频号登录二维码已发送，请扫码。"
            if not bool(result.get("needs_login", True)):
                return "当前视频号已登录，无需扫码。"
            return f"获取视频号登录二维码失败：{result.get('error') or 'unknown'}"
        if action == "collect_log":
            return _latest_log_tail(workspace, "cybercar_hourly", lines=30)
        if action == "publish_log":
            return _latest_log_tail(workspace, "cybercar_distribution_hourly", lines=30)
        if action == "worker_status":
            return _build_status(
                started_at=started_at,
                repo_root=repo_root,
                workspace=workspace,
                allow_shell=allow_shell,
                allow_prefixes=allow_prefixes,
                last_processed=last_processed,
            )

    if cmd_key in {"菜单", "menu", "/menu", "主菜单", "帮助菜单", "首页"}:
        return _home_response(
            resolved_default_profile,
            "已返回底部菜单入口",
            workspace=workspace,
            chat_id=chat_id,
        )
    if cmd_key in {"即采即发"}:
        return _build_collect_publish_latest_menu_card(default_profile=resolved_default_profile)
    if cmd_key in {"国内即采即发"}:
        return _build_collect_publish_latest_menu_card(
            default_profile=_collect_publish_profile_for_route("domestic", resolved_default_profile),
            callback_action=COLLECT_PUBLISH_ACTION_DOMESTIC,
            route_label=_collect_publish_route_label("domestic"),
        )
    if cmd_key in {"海外即采即发"}:
        return _build_collect_publish_latest_menu_card(
            default_profile=_collect_publish_profile_for_route("global", resolved_default_profile),
            callback_action=COLLECT_PUBLISH_ACTION_GLOBAL,
            route_label=_collect_publish_route_label("global"),
        )
    if cmd_key in {"进程查看", "查看进度", "进度查看", "流程进度"}:
        return _build_process_status_card(
            default_profile=resolved_default_profile,
            workspace=workspace,
        )
    if cmd_key in {"平台登录"}:
        return _build_login_menu_card(default_profile=resolved_default_profile)
    if cmd_key in {"点赞评论"}:
        return _build_comment_reply_menu_card(default_profile=resolved_default_profile)
    if cmd_key in {"帮助", "help", "/help", "命令"}:
        return _help_response(resolved_default_profile)
    if cmd_key in {"当前目录", "pwd"}:
        return f"{BOT_NAME} 当前目录:\n{repo_root}"
    if cmd_key in {"查看采集日志", "采集日志"}:
        return _latest_log_tail(workspace, "cybercar_hourly", lines=30)
    if cmd_key in {"查看发布日志", "发布日志"}:
        return _latest_log_tail(workspace, "cybercar_distribution_hourly", lines=30)
    if cmd_key in {"任务状态", "系统状态"}:
        return _build_status(
            started_at=started_at,
            repo_root=repo_root,
            workspace=workspace,
            allow_shell=allow_shell,
            allow_prefixes=allow_prefixes,
            last_processed=last_processed,
        )

    if cmd_key in {"执行采集", "定时采集"}:
        result = _run_cmd(
            ["schtasks", "/Run", "/TN", DEFAULT_CRAWL_TASK_NAME],
            timeout_seconds=min(timeout_seconds, 120),
            workdir=repo_root,
        )
        return _summarize_run(result, "执行采集任务")

    if cmd_key in {"执行发布"}:
        result = _run_cmd(
            ["schtasks", "/Run", "/TN", DEFAULT_DISTRIBUTION_TASK_NAME],
            timeout_seconds=min(timeout_seconds, 120),
            workdir=repo_root,
        )
        return _summarize_run(result, "执行发布任务")

    if token.startswith("执行 ") or token.startswith("cmd "):
        body = cmd.split(" ", 1)[1].strip() if " " in cmd else ""
        if not allow_shell:
            return "已禁用任意命令执行。启动 worker 时加 --allow-shell 才可用。"
        if not body:
            return "命令为空。示例：执行 Get-Location"
        ok, reason = _is_allowed_command(body, allow_prefixes)
        if not ok:
            _append_audit(
                audit_file,
                update_id=update_id,
                chat_id=chat_id,
                username=username,
                action="exec",
                command=body,
                allowed=False,
                reason=reason,
            )
            return f"执行被拒绝：{reason}."
        result = _run_cmd(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", body],
            timeout_seconds=timeout_seconds,
            workdir=repo_root,
        )
        _append_audit(
            audit_file,
            update_id=update_id,
            chat_id=chat_id,
            username=username,
            action="exec",
            command=body,
            allowed=True,
            reason="ok",
            exit_code=int(result.get("code") or 0),
            elapsed_seconds=float(result.get("elapsed") or 0.0),
        )
        out = str(result.get("stdout") or "").strip()
        err = str(result.get("stderr") or "").strip()
        msg_lines = [
            f"鎵ц鍛戒护: {body}",
            f"缁撴灉: {'鎴愬姛' if result.get('ok') else '澶辫触'}",
            f"閫€鍑虹爜: {result.get('code')}",
        ]
        if out:
            msg_lines.append("")
            msg_lines.append("stdout:")
            msg_lines.append(out[-2000:])
        if err:
            msg_lines.append("")
            msg_lines.append("stderr:")
            msg_lines.append(err[-1200:])
        return "\n".join(msg_lines)

    return f"{BOT_NAME} 鏃犳硶璇嗗埆鍛戒护: {cmd}\n\n{_help_text()}"


def handle_command_update(
    *,
    update: Dict[str, Any],
    bot_token: str,
    allowed_chat_id: str,
    allow_private_chat_commands: bool = False,
    command_password: str,
    started_at: float,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    allow_shell: bool,
    allow_prefixes: list[str],
    audit_file: Path,
    last_processed: int,
    log_file: Path,
    telegram_bot_identifier: str = "",
    default_profile: str = DEFAULT_PROFILE,
) -> Dict[str, Any]:
    if not isinstance(update, dict):
        return {"handled": False, "last_processed": int(last_processed)}

    update_id = int(update.get("update_id") or 0)
    if update_id <= int(last_processed):
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}

    msg = _extract_message(update)
    if not msg:
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}

    chat_id = str(msg.get("chat_id") or "").strip()
    chat_type = str(msg.get("chat_type") or "").strip().lower()
    text = str(msg.get("text") or "").strip()
    username = str(msg.get("username") or "").strip()
    if not chat_id or not text:
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}
    if not _chat_allowed(
        chat_id=chat_id,
        chat_type=chat_type,
        allowed_chat_id=allowed_chat_id,
        allow_private_chat_commands=allow_private_chat_commands,
    ):
        _append_log(log_file, f"[Worker] Skip chat_id={chat_id} (not allowed).")
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}

    _append_log(log_file, f"[Worker] <= update_id={update_id}, user={username or '-'}, text={text[:120]}")

    shared_link_reply = None
    if _should_try_shared_link_message(text):
        shared_link_reply = _handle_shared_immediate_link_message(
            text=text,
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            chat_id=chat_id,
            username=username,
            bot_token=bot_token,
            telegram_bot_identifier=telegram_bot_identifier,
            default_profile=default_profile,
            log_file=log_file,
        )
    if isinstance(shared_link_reply, dict):
        reply_payload = _normalize_reply_payload(shared_link_reply)
        _send_reply(
            bot_token=bot_token,
            chat_id=chat_id,
            text=str(reply_payload.get("text") or ""),
            timeout_seconds=int(_normalize_blocking_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1)),
            reply_markup=reply_payload.get("reply_markup") if isinstance(reply_payload.get("reply_markup"), dict) else None,
            parse_mode=str(reply_payload.get("parse_mode") or ""),
        )
        _append_log(log_file, f"[Worker] => update_id={update_id} shared-link replied.")
        new_last_processed = max(int(last_processed), update_id)
        return {"handled": True, "last_processed": new_last_processed, "update_id": update_id}

    raw_reply = _handle_command(
        text=text,
        repo_root=repo_root,
        workspace=workspace,
        timeout_seconds=timeout_seconds,
        allow_shell=allow_shell,
        allow_prefixes=allow_prefixes,
        command_password=command_password,
        started_at=started_at,
        last_processed=int(last_processed),
        update_id=update_id,
        chat_id=chat_id,
        username=username,
        audit_file=audit_file,
        telegram_bot_identifier=telegram_bot_identifier,
        default_profile=default_profile,
    )
    if isinstance(raw_reply, dict) and isinstance(raw_reply.get("home_card"), dict):
        force_new_home = bool(raw_reply.get("force_new_home"))
        if _should_reuse_home_message(workspace=workspace, chat_id=chat_id, force_new=force_new_home):
            force_new_home = False
        send_or_update_home_message(
            bot_token=bot_token,
            chat_id=chat_id,
            state_file=_home_state_path(workspace),
            bot_kind="cybercar",
            card=raw_reply["home_card"],
            timeout_seconds=int(_normalize_blocking_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1)),
            force_new=force_new_home,
        )
        _ensure_home_shortcut_keyboard(
            bot_token=bot_token,
            chat_id=chat_id,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            log_file=log_file,
        )
        _append_log(log_file, f"[Worker] => update_id={update_id} home updated.")
        new_last_processed = max(int(last_processed), update_id)
        return {"handled": True, "last_processed": new_last_processed, "update_id": update_id}

    reply_payload = _normalize_reply_payload(raw_reply)
    _send_reply(
        bot_token=bot_token,
        chat_id=chat_id,
        text=str(reply_payload.get("text") or ""),
        timeout_seconds=int(_normalize_blocking_timeout(timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1)),
        reply_markup=reply_payload.get("reply_markup") if isinstance(reply_payload.get("reply_markup"), dict) else None,
        parse_mode=str(reply_payload.get("parse_mode") or ""),
    )
    _append_log(log_file, f"[Worker] => update_id={update_id} replied.")
    new_last_processed = max(int(last_processed), update_id)
    return {"handled": True, "last_processed": new_last_processed, "update_id": update_id}


def handle_callback_update(
    *,
    update: Dict[str, Any],
    bot_token: str,
    allowed_chat_id: str,
    allow_private_chat_commands: bool = False,
    command_password: str,
    started_at: float,
    repo_root: Path,
    workspace: Path,
    timeout_seconds: int,
    allow_shell: bool,
    allow_prefixes: list[str],
    audit_file: Path,
    last_processed: int,
    log_file: Path,
    telegram_bot_identifier: str = "",
    default_profile: str = DEFAULT_PROFILE,
    immediate_test_mode: bool = False,
) -> Dict[str, Any]:
    if not isinstance(update, dict):
        return {"handled": False, "last_processed": int(last_processed)}

    update_id = int(update.get("update_id") or 0)
    if update_id <= int(last_processed):
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}

    callback = _extract_callback_query(update)
    if not callback:
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}

    chat_id = str(callback.get("chat_id") or "").strip()
    chat_type = str(callback.get("chat_type") or "").strip().lower()
    callback_data = str(callback.get("data") or "").strip()
    query_id = str(callback.get("query_id") or "").strip()
    username = str(callback.get("username") or "").strip() or "unknown"
    message_id = int(callback.get("message_id") or 0)
    inline_message_id = str(callback.get("inline_message_id") or "").strip()
    callback_answered = False

    def _answer_callback_once(text: str) -> None:
        nonlocal callback_answered
        message = str(text or "").strip()
        if callback_answered or not message:
            return
        try:
            _answer_callback_query(
                bot_token=bot_token,
                query_id=query_id,
                text=message,
                timeout_seconds=timeout_seconds,
            )
            callback_answered = True
        except Exception as exc:
            _append_log(log_file, f"[Worker] answerCallbackQuery failed: {exc}")

    def _answer_callback_queued() -> None:
        _answer_callback_once("已收到，正在处理。")

    if not _chat_allowed(
        chat_id=chat_id,
        chat_type=chat_type,
        allowed_chat_id=allowed_chat_id,
        allow_private_chat_commands=allow_private_chat_commands,
    ):
        _append_log(log_file, f"[Worker] Skip callback chat_id={chat_id} (not allowed).")
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}

    home_parsed = parse_home_callback_data(callback_data)
    if home_parsed and str(home_parsed.get("bot_kind") or "") == "cybercar":
        handled_home = _handle_home_callback(
            callback=callback,
            parsed=home_parsed,
            bot_token=bot_token,
            command_password=command_password,
            started_at=started_at,
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            allow_shell=allow_shell,
            allow_prefixes=allow_prefixes,
            audit_file=audit_file,
            update_id=update_id,
            default_profile=default_profile,
            immediate_test_mode=immediate_test_mode,
        )
        return {
            "handled": bool(handled_home.get("handled", True)),
            "last_processed": max(int(last_processed), update_id),
            "update_id": update_id,
        }

    wechat_qr_action = _parse_wechat_qr_callback_data(callback_data)
    if wechat_qr_action:
        _answer_callback_queued()
        qr_action = str(wechat_qr_action.get("action") or "").strip().lower()
        platform_name = str(wechat_qr_action.get("platform") or "wechat")
        platform_label = PUBLISH_PLATFORM_DISPLAY.get(platform_name, platform_name)
        wait_token = str(wechat_qr_action.get("wait_token") or "").strip()
        if qr_action == "done":
            result = _confirm_platform_login_done(
                platform_name=platform_name,
                bot_token=bot_token,
                chat_id=chat_id,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
                wait_token=wait_token,
            )
            callback_text = "已确认登录，任务继续执行。"
            if bool(result.get("needs_login", True)):
                callback_text = str(result.get("error") or "当前仍未检测到登录恢复，系统会继续自动轮询。")[:180]
        else:
            result = _refresh_platform_login_qr_message(
                platform_name=platform_name,
                bot_token=bot_token,
                chat_id=chat_id,
                message_id=message_id,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
                telegram_bot_identifier=telegram_bot_identifier,
                wait_token=wait_token,
            )
            callback_text = "二维码已在原卡片刷新。"
            if not bool(result.get("sent")):
                if not bool(result.get("needs_login", True)):
                    callback_text = f"当前{platform_label}已登录，无需扫码。"
                else:
                    callback_text = str(result.get("error") or "二维码刷新失败")[:180]
        if not bool(result.get("sent")) and qr_action == "done":
            if not bool(result.get("needs_login", True)):
                callback_text = "已确认登录，任务继续执行。"
            else:
                callback_text = str(result.get("error") or "当前仍未检测到登录恢复，系统会继续自动轮询。")[:180]
        _answer_callback_once(callback_text)
        if qr_action == "done" and (bool(result.get("sent")) or not bool(result.get("needs_login", True))):
            try:
                _try_clear_callback_buttons(
                    bot_token=bot_token,
                    chat_id=chat_id,
                    message_id=message_id,
                    inline_message_id=inline_message_id,
                    timeout_seconds=timeout_seconds,
                )
            except Exception as exc:
                _append_log(log_file, f"[Worker] clear wechat qr callback buttons failed: {exc}")
        return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}

    parsed = _parse_prefilter_callback_data(callback_data)
    if not parsed:
        if callback_data.startswith(f"{TELEGRAM_WECHAT_QR_CALLBACK_PREFIX}|"):
            _answer_callback_once("无效的二维码指令。")
            return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}
        if callback_data.startswith(f"{TELEGRAM_MENU_CALLBACK_PREFIX}|"):
            _answer_callback_once("旧 4 按钮入口已停用，请使用底部 6 菜单。")
            return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}
        if callback_data.startswith(f"{TELEGRAM_PREFILTER_CALLBACK_PREFIX}|"):
            _answer_callback_once("无效的审核指令。")
            return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}
        return {"handled": False, "last_processed": int(last_processed), "update_id": update_id}

    action, item_id = parsed
    _answer_callback_queued()
    actor = username if username.startswith("@") else f"@{username}"
    try:
        item = _get_prefilter_item(workspace, item_id)
    except TimeoutError as exc:
        _append_log(log_file, f"[Worker] prefilter callback queue lock timeout: {exc}")
        if not callback_answered:
            _answer_callback_once("系统正忙，请稍后重试。")
        return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}
    except Exception as exc:
        _append_log(log_file, f"[Worker] prefilter callback load item failed: {exc}")
        if not callback_answered:
            _answer_callback_once("处理失败，请稍后重试。")
        return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}
    if not isinstance(item, dict) or not item:
        if not callback_answered:
            _answer_callback_once("审核项不存在或已过期。")
        return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}

    now_text = _now_text()
    video_name = str(item.get("video_name") or "").strip()
    status = str(item.get("status") or "").strip().lower()
    workflow = str(item.get("workflow") or "").strip().lower()
    callback_reply = ""
    down_result_message = ""
    should_clear_buttons = True
    card_update: Optional[Dict[str, Any]] = None
    # Keep previously-sent callback ack state to avoid duplicate answerCallbackQuery calls.
    card_updated_inline = False
    delete_terminal_card = False

    def _answer_callback_immediately(text: str) -> None:
        nonlocal callback_answered
        message = str(text or "").strip()
        if callback_answered or not message:
            return
        _answer_callback_once(message)

    _answer_callback_immediately("已收到，正在处理。")

    def _try_update_card_immediately(card: Optional[Dict[str, Any]]) -> bool:
        nonlocal card_updated_inline
        if not isinstance(card, dict) or not card:
            return False
        try:
            _update_callback_message_card(
                bot_token=bot_token,
                chat_id=chat_id,
                card=card,
                message_id=message_id,
                inline_message_id=inline_message_id,
                timeout_seconds=timeout_seconds,
            )
            card_updated_inline = True
            return True
        except Exception as exc:
            _append_log(log_file, f"[Worker] edit prefilter card failed: {exc}")
            return False

    if action == "retry_failed_publish":
        if workflow != "immediate_manual_publish":
            callback_reply = "该候选不支持失败补发。"
        else:
            retry_platforms = [
                platform
                for platform, payload in _normalize_platform_results(item.get("platform_results")).items()
                if str((payload or {}).get("status") or "").strip().lower() in {"failed", "login_required"}
            ]
            if not retry_platforms:
                callback_reply = "当前没有可补发的失败平台。"
            else:
                runner, core = _load_runtime_modules()
                queue_result = _queue_immediate_platform_jobs(
                    workspace=workspace,
                    repo_root=repo_root,
                    timeout_seconds=timeout_seconds,
                    profile=default_profile,
                    telegram_bot_identifier=telegram_bot_identifier,
                    telegram_bot_token=bot_token,
                    telegram_chat_id=chat_id,
                    item_id=item_id,
                    item=item,
                    immediate_test_mode=immediate_test_mode,
                )
                refreshed_item = queue_result.get("item") if isinstance(queue_result.get("item"), dict) else _get_prefilter_item(workspace, item_id) or item
                spawned = int(queue_result.get("spawned") or 0)
                failed = int(queue_result.get("failed") or 0)
                skipped_duplicate = int(queue_result.get("skipped_duplicate") or 0)
                retried_labels = _format_platform_text(retry_platforms)
                if spawned > 0:
                    callback_reply = f"🔁 已补发失败平台：{retried_labels}"
                elif skipped_duplicate > 0 and failed <= 0:
                    callback_reply = f"🔁 失败平台已去重跳过：{retried_labels}"
                else:
                    callback_reply = f"⚠️ 失败平台补发未启动：{retried_labels}"
                card_update = _build_prefilter_status_card(
                    item=refreshed_item,
                    title="失败平台已补发" if spawned > 0 else "失败平台补发未启动",
                    subtitle="后台只重试之前失败或需要登录的平台" if spawned > 0 else "失败平台未成功重新排队，请查看进度",
                    status="running" if spawned > 0 else "failed",
                    result_section_title="执行状态",
                    result_items=[
                        f"本次补发范围：{retried_labels}",
                        "成功平台不会重复触发，后台只会重跑失败平台。",
                    ]
                    + (
                        ["部分平台仍未重新排队，请查看进度中的平台状态。"]
                        if failed > 0 and spawned > 0
                        else []
                    ),
                )
                should_clear_buttons = False
                _answer_callback_immediately(callback_reply)
    elif action in {"publish_normal", "publish_original"}:
        if workflow != "immediate_manual_publish":
            callback_reply = "该候选不支持直接发布。"
        elif status in {"publish_requested", "download_running", "publish_running", "publish_done"}:
            callback_reply = "本条已经进入发布流程，无需重复点击。"
        else:
            declare_original = action == "publish_original"
            updated_item = _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "publish_requested",
                    "updated_at": now_text,
                    "actor": actor,
                    "action": action,
                    "wechat_declare_original": declare_original,
                },
            )
            if video_name:
                _apply_review_approve(
                    workspace=workspace,
                    video_name=video_name,
                    actor=actor,
                    item_id=item_id,
                    media_kind=str(updated_item.get("media_kind") or "video"),
                )
            callback_reply = "📝 已选择原创发布，正在启动发布。" if declare_original else "⚡ 已选择普通发布，正在启动发布。"
            if immediate_test_mode:
                callback_reply += "（测试模式）"
            _answer_callback_immediately(callback_reply)
            started_item = _get_prefilter_item(workspace, item_id) or updated_item
            started_note = "已选择原创发布，后台任务正在排队，尚未确认平台发布成功。" if declare_original else "已选择普通发布，后台任务正在排队，尚未确认平台发布成功。"
            if immediate_test_mode:
                started_note = "测试模式已放宽候选与重复内容过滤，后台仍会继续真实采集和平台排队。"
            optimistic_card = _build_prefilter_status_card(
                item=started_item,
                title="即采即发测试已排队" if immediate_test_mode else "即采即发任务已排队",
                subtitle="测试模式仅放宽前置过滤，后续仍走真实链路" if immediate_test_mode else "当前卡片已锁定，等待后台下载素材并分平台执行",
                status="running",
                result_section_title="执行状态",
                result_items=[
                    started_note,
                    "最终是否发布成功，将以后续平台结果通知为准。",
                ],
            )
            optimistic_card_sent = _try_update_card_immediately(optimistic_card)
            if optimistic_card_sent:
                should_clear_buttons = False
            try:
                _spawn_immediate_publish_item_job(
                    repo_root=repo_root,
                    workspace=workspace,
                    timeout_seconds=timeout_seconds,
                    profile=default_profile,
                    telegram_bot_identifier=telegram_bot_identifier,
                    telegram_bot_token=bot_token,
                    telegram_chat_id=chat_id,
                    item_id=item_id,
                    immediate_test_mode=immediate_test_mode,
                )
                if not optimistic_card_sent:
                    started_item = _get_prefilter_item(workspace, item_id) or {}
                    final_note = "已选择原创发布，后台任务已提交，等待平台状态回传。" if declare_original else "已选择普通发布，后台任务已提交，等待平台状态回传。"
                    if immediate_test_mode:
                        final_note = "测试模式任务已提交，后续会继续真实采集和平台发布，只是放宽了前置筛选。"
                    card_update = _build_prefilter_status_card(
                        item=started_item,
                        title="即采即发测试已排队" if immediate_test_mode else "即采即发任务已排队",
                        subtitle="测试模式仅放宽前置过滤，后续仍走真实链路" if immediate_test_mode else "当前卡片已锁定，等待后台下载素材并分平台执行",
                        status="running",
                        result_section_title="执行状态",
                        result_items=[
                            final_note,
                            "最终是否发布成功，将以后续平台结果通知为准。",
                        ],
                    )
            except Exception as exc:
                _append_log(log_file, f"[Worker] immediate publish spawn failed: {exc}")
                failed_item = _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "link_pending",
                        "last_error": str(exc),
                        "action": "publish_spawn_failed",
                    },
                )
                callback_reply = "⚠️ 启动发布失败，请稍后重试。"
                should_clear_buttons = False
                card_updated_inline = False
                card_update = _build_prefilter_status_card(
                    item=failed_item,
                    title="即采即发启动失败",
                    subtitle="后台任务未成功启动，请稍后重试",
                    status="failed",
                    result_section_title="执行状态",
                    result_items=["后台任务启动失败，本条尚未进入发布队列"],
                )
    elif action == "up":
        if status == "down_confirmed":
            callback_reply = "该视频已被拒绝，不能改为放行。"
            updated_item = item
        elif workflow == IMMEDIATE_COLLECT_REVIEW_WORKFLOW:
            updated_item = _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "collect_requested",
                    "updated_at": now_text,
                    "actor": actor,
                    "action": "up",
                },
            )
            callback_reply = "✅ 已保留本条，开始采集。"
            if immediate_test_mode:
                callback_reply += "（测试模式）"
            _answer_callback_immediately(callback_reply)
            optimistic_card = _build_prefilter_status_card(
                item=updated_item,
                title="图片采集测试已排队" if immediate_test_mode else "图片采集已排队",
                subtitle="测试模式已放宽采集筛选，后续仍会真实下载" if immediate_test_mode else "当前卡片已锁定，等待后台下载素材",
                status="running",
                result_section_title="执行状态",
                result_items=[
                    "当前候选已进入测试模式采集链路。" if immediate_test_mode else "当前候选已进入采集队列。",
                    "本轮会继续真实下载，只是放宽前置采集筛选，便于验证后续链路。" if immediate_test_mode else "采集完成后会再回传结果卡片，不会自动进入发布。",
                ],
            )
            optimistic_card_sent = _try_update_card_immediately(optimistic_card)
            if optimistic_card_sent:
                should_clear_buttons = False
            try:
                _spawn_immediate_collect_item_job(
                    repo_root=repo_root,
                    workspace=workspace,
                    timeout_seconds=timeout_seconds,
                    profile=default_profile,
                    telegram_bot_identifier=telegram_bot_identifier,
                    telegram_bot_token=bot_token,
                    telegram_chat_id=chat_id,
                    item_id=item_id,
                    immediate_test_mode=immediate_test_mode,
                )
                if not optimistic_card_sent:
                    latest_item = _get_prefilter_item(workspace, item_id) or updated_item
                    card_update = _build_prefilter_status_card(
                        item=latest_item,
                        title="图片采集测试已排队" if immediate_test_mode else "图片采集已排队",
                        subtitle="测试模式已放宽采集筛选，后续仍会真实下载" if immediate_test_mode else "当前卡片已锁定，等待后台下载素材",
                        status="running",
                        result_section_title="执行状态",
                        result_items=[
                            "当前候选已进入测试模式采集链路。" if immediate_test_mode else "当前候选已进入采集队列。",
                            "本轮会继续真实下载，只是放宽前置采集筛选，便于验证后续链路。" if immediate_test_mode else "采集完成后会再回传结果卡片，不会自动进入发布。",
                        ],
                    )
            except Exception as exc:
                _append_log(log_file, f"[Worker] immediate collect spawn failed: {exc}")
                updated_item = _update_prefilter_item(
                    workspace,
                    item_id,
                    updates={
                        "status": "link_pending",
                        "last_error": str(exc),
                        "action": "collect_spawn_failed",
                    },
                )
                callback_reply = "⚠️ 启动采集失败，请稍后重试。"
                should_clear_buttons = False
                card_updated_inline = False
                card_update = _build_prefilter_status_card(
                    item=updated_item,
                    title="图片采集启动失败",
                    subtitle="后台采集任务未成功启动，请稍后重试",
                    status="failed",
                    result_section_title="执行状态",
                    result_items=["后台采集任务启动失败，本条尚未进入采集队列。"],
                )
        else:
            updated_item = _update_prefilter_item(
                workspace,
                item_id,
                updates={
                    "status": "up_confirmed",
                    "updated_at": now_text,
                    "actor": actor,
                    "action": "up",
                },
            )
            callback_reply = "✅ 已保留本条，后续将按原流程继续。"
            _answer_callback_immediately(callback_reply)
            card_update = _build_prefilter_status_card(
                item=updated_item,
                title="预审已保留",
                subtitle="当前卡片已锁定，不再重复处理",
                status="success",
                result_section_title="处理结果",
                result_items=["本条候选已保留，后续流程继续执行"],
            )
        try:
            _append_prefilter_feedback_event(
                workspace=workspace,
                action="up",
                item_id=item_id,
                video_name=video_name,
                actor=actor,
                chat_id=chat_id,
                message_id=message_id,
                queue_status=str(updated_item.get("status") or status or ""),
                changed=None,
            )
        except Exception as exc:
            _append_log(log_file, f"[Worker] up-feedback record failed: {exc}")
    else:
        changed = _apply_review_reject(
            workspace=workspace,
            video_name=video_name,
            actor=actor,
            item_id=item_id,
            media_kind=str(item.get("media_kind") or "video"),
        )
        ledger_changed = _record_prefilter_skip_source_in_collect_ledger(
            workspace=workspace,
            source_url=str(item.get("source_url") or "").strip(),
            media_kind=str(item.get("media_kind") or "video"),
            processed_name=str(item.get("processed_name") or item.get("video_name") or "").strip(),
        )
        updated_item = _update_prefilter_item(
            workspace,
            item_id,
            updates={
                "status": "down_confirmed",
                "updated_at": now_text,
                "actor": actor,
                "action": "skip",
            },
        )
        callback_reply = "⏭️ 已跳过本条，后续平台将自动忽略。"
        _answer_callback_immediately(callback_reply)
        down_result_message = _build_prefilter_down_result_message(
            video_name=video_name,
            actor=actor,
            now_text=now_text,
            item_id=item_id,
            changed=changed,
        )
        card_update = _build_prefilter_status_card(
            item=updated_item,
            title="预审已跳过",
            subtitle="当前卡片已锁定，不再进入后续发布",
            status="blocked",
            result_section_title="处理结果",
            result_items=[
                "本条候选已跳过，后续平台将自动忽略",
                "后续轮次也会继续跳过，不回滚已发布结果",
            ],
        )
        if changed:
            _append_log(log_file, f"[Worker] Prefilter downvote applied: {video_name} by {actor} ({item_id})")
        else:
            _append_log(log_file, f"[Worker] Prefilter downvote duplicate: {video_name} by {actor} ({item_id})")
        if ledger_changed:
            _append_log(log_file, f"[Worker] Prefilter skip synced to collect ledger: {str(item.get('source_url') or '').strip()} ({item_id})")
        try:
            _append_prefilter_feedback_event(
                workspace=workspace,
                action="down",
                item_id=item_id,
                video_name=video_name,
                actor=actor,
                chat_id=chat_id,
                message_id=message_id,
                queue_status=str(updated_item.get("status") or ""),
                changed=changed,
            )
        except Exception as exc:
            _append_log(log_file, f"[Worker] down-feedback record failed: {exc}")
        delete_terminal_card = True

    if not callback_answered:
        _answer_callback_immediately(callback_reply)

    if action == "up":
        delete_terminal_card = True

    if delete_terminal_card:
        deleted = _try_delete_telegram_message(
            bot_token=bot_token,
            chat_id=chat_id,
            message_id=message_id,
            timeout_seconds=timeout_seconds,
            log_file=log_file,
        )
        if deleted:
            card_update = None
            should_clear_buttons = False
            down_result_message = ""

    if card_update is not None and not card_updated_inline:
        try:
            _update_callback_message_card(
                bot_token=bot_token,
                chat_id=chat_id,
                card=card_update,
                message_id=message_id,
                inline_message_id=inline_message_id,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            _append_log(log_file, f"[Worker] edit prefilter card failed: {exc}")
    elif should_clear_buttons:
        try:
            _try_clear_callback_buttons(
                bot_token=bot_token,
                chat_id=chat_id,
                message_id=message_id,
                inline_message_id=inline_message_id,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            _append_log(log_file, f"[Worker] editMessageReplyMarkup failed: {exc}")

    if action == "down" and down_result_message and chat_id and card_update is None:
        try:
            _send_reply(
                bot_token=bot_token,
                chat_id=chat_id,
                text=down_result_message,
                timeout_seconds=max(20, int(timeout_seconds)),
            )
        except Exception as exc:
            _append_log(log_file, f"[Worker] down-result send failed: {exc}")

    return {"handled": True, "last_processed": max(int(last_processed), update_id), "update_id": update_id}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Auto execute Telegram bot commands directly.")
    parser.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    parser.add_argument("--repo-root", default=DEFAULT_REPO_ROOT)
    parser.add_argument("--offset-file", default=str(DEFAULT_OFFSET_FILE))
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--audit-file", default=str(DEFAULT_AUDIT_FILE))
    parser.add_argument(
        "--telegram-bot-token",
        default="",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default="",
    )
    parser.add_argument(
        "--command-password",
        default=_env_first(
            "CYBERCAR_TELEGRAM_COMMAND_PASSWORD",
            "TELEGRAM_COMMAND_PASSWORD",
            default="",
        ),
        help="Password for /status and /run commands.",
    )
    parser.add_argument("--default-profile", default="")
    parser.add_argument("--profile-config", default="")
    parser.add_argument(
        "--shell-allow-prefix",
        action="append",
        default=[],
        help="Allowlisted shell command prefix. Can be passed multiple times.",
    )
    parser.add_argument("--poll-interval-seconds", type=int, default=DEFAULT_POLL_INTERVAL_SECONDS)
    parser.add_argument("--poll-timeout-seconds", type=int, default=DEFAULT_POLL_TIMEOUT_SECONDS)
    parser.add_argument(
        "--poll-network-failure-restart-threshold",
        type=int,
        default=DEFAULT_POLL_NETWORK_FAILURE_RESTART_THRESHOLD,
        help="Restart worker after this many consecutive Telegram poll network failures. Set 0 to disable.",
    )
    parser.add_argument(
        "--poll-network-failure-restart-min-span-seconds",
        type=int,
        default=DEFAULT_POLL_NETWORK_FAILURE_RESTART_MIN_SPAN_SECONDS,
        help=(
            "Minimum consecutive Telegram poll network failure span required before restart is allowed. "
            "Set 0 to disable this guard."
        ),
    )
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--allow-shell", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--run-comment-reply-job", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--comment-reply-post-limit", type=int, default=3, help=argparse.SUPPRESS)
    parser.add_argument("--comment-reply-platform", default="wechat", help=argparse.SUPPRESS)
    parser.add_argument("--run-home-action-job", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--home-action", default="", help=argparse.SUPPRESS)
    parser.add_argument("--home-action-value", default="", help=argparse.SUPPRESS)
    parser.add_argument("--home-action-task-key", default="", help=argparse.SUPPRESS)
    parser.add_argument("--run-collect-publish-latest-job", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--job-candidate-limit",
        type=int,
        default=DEFAULT_IMMEDIATE_CANDIDATE_LIMIT,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--collect-publish-media-kind", default="video", help=argparse.SUPPRESS)
    parser.add_argument("--run-immediate-collect-item-job", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--run-immediate-publish-item-job", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--run-immediate-publish-platform-job", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--publish-item-id", default="", help=argparse.SUPPRESS)
    parser.add_argument("--publish-platform", default="", help=argparse.SUPPRESS)
    parser.add_argument(
        "--immediate-test-mode",
        action="store_true",
        help="Relax immediate candidate matching and duplicate filtering for testing while keeping the real downstream collect and publish flow enabled.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    started_at = time.time()
    workspace = Path(args.workspace).expanduser()
    repo_root = Path(args.repo_root).expanduser()
    poller_lock: Optional[Path] = None
    profile_config_arg = str(args.profile_config or "").strip()
    if profile_config_arg:
        profile_config_path = Path(profile_config_arg).expanduser()
        if not profile_config_path.is_absolute():
            profile_config_path = (repo_root / profile_config_path).resolve()
    else:
        profile_config_path = (repo_root / DEFAULT_PROFILE_CONFIG_REL).resolve()
    default_profile = _resolve_default_profile_name(
        repo_root=repo_root,
        cli_default_profile=str(args.default_profile or ""),
        profile_config_path=profile_config_path,
    )
    offset_file = (workspace / Path(str(args.offset_file))).resolve()
    state_file = (workspace / Path(str(args.state_file))).resolve()
    audit_file = (workspace / Path(str(args.audit_file))).resolve()
    log_file = workspace / DEFAULT_LOG_SUBDIR / f"telegram_command_worker_{datetime.now().strftime('%Y%m%d')}.log"

    explicit_bot_token = str(args.telegram_bot_token or "").strip()
    explicit_chat_id = str(args.telegram_chat_id or "").strip()
    resolved_telegram: Dict[str, Any] = {}
    if explicit_bot_token:
        resolved_telegram["bot_token"] = explicit_bot_token
        resolved_telegram["chat_id"] = explicit_chat_id
    else:
        resolved_telegram = _resolve_telegram_bot_settings(
            {
                "bot_token": "",
                "chat_id": "",
                "timeout_seconds": max(10, int(args.poll_timeout_seconds) + 15),
            },
            env_prefix="",
        )
    bot_token = str(resolved_telegram.get("bot_token") or explicit_bot_token or "").strip()
    if not bot_token:
        _append_log(
            log_file,
            "[Worker] Missing telegram bot token; explicit token or single-bot registry resolution failed; exit.",
        )
        return 1
    allowed_chat_id = str(explicit_chat_id or resolved_telegram.get("chat_id") or "").strip()
    telegram_bot_identifier = ""
    command_password = str(args.command_password or "").strip()
    poll_interval = max(0, int(args.poll_interval_seconds))
    poll_timeout = max(1, int(args.poll_timeout_seconds))
    poll_network_failure_restart_threshold = max(0, int(args.poll_network_failure_restart_threshold))
    poll_network_failure_restart_min_span_seconds = max(
        0, int(args.poll_network_failure_restart_min_span_seconds)
    )
    timeout_seconds = int(_normalize_blocking_timeout(args.timeout_seconds, DEFAULT_TIMEOUT_SECONDS, minimum=1))
    allow_shell = bool(args.allow_shell)
    api_timeout_seconds = max(10, poll_timeout + 15)
    consecutive_poll_failures = 0
    poll_failure_burst_started_at = 0.0
    poll_transport_warning_last_logged_at = 0.0
    suppressed_poll_transport_warnings = 0

    env_prefixes_raw = _env_first(
        "CYBERCAR_TELEGRAM_SHELL_ALLOW_PREFIXES",
        default="",
    )
    user_prefixes = _normalize_prefixes(args.shell_allow_prefix + ([env_prefixes_raw] if env_prefixes_raw else []))
    allow_prefixes = user_prefixes if user_prefixes else DEFAULT_SHELL_ALLOW_PREFIXES.copy()

    if bool(getattr(args, "run_home_action_job", False)):
        return _run_home_action_job(
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=default_profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=bot_token,
            telegram_chat_id=allowed_chat_id,
            action=str(getattr(args, "home_action", "") or "").strip(),
            value=str(getattr(args, "home_action_value", "") or "").strip(),
            task_key=str(getattr(args, "home_action_task_key", "") or "").strip(),
            immediate_test_mode=bool(getattr(args, "immediate_test_mode", False)),
        )
    if bool(getattr(args, "run_collect_publish_latest_job", False)):
        return _run_collect_publish_latest_job(
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=default_profile,
            telegram_bot_token=bot_token,
            telegram_chat_id=allowed_chat_id,
            candidate_limit=max(
                1,
                int(getattr(args, "job_candidate_limit", DEFAULT_IMMEDIATE_CANDIDATE_LIMIT) or DEFAULT_IMMEDIATE_CANDIDATE_LIMIT),
            ),
            media_kind=_normalize_immediate_collect_media_kind(str(getattr(args, "collect_publish_media_kind", "video") or "video")),
            immediate_test_mode=bool(getattr(args, "immediate_test_mode", False)),
        )
    if bool(getattr(args, "run_comment_reply_job", False)):
        return _run_comment_reply_job(
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=default_profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=bot_token,
            telegram_chat_id=allowed_chat_id,
            platform=str(getattr(args, "comment_reply_platform", "wechat") or "wechat"),
            post_limit=max(1, int(getattr(args, "comment_reply_post_limit", 3) or 3)),
        )
    if bool(getattr(args, "run_immediate_publish_item_job", False)):
        runner, core = _load_runtime_modules()
        return _publish_immediate_candidate(
            runner=runner,
            core=core,
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=default_profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=bot_token,
            telegram_chat_id=allowed_chat_id,
            item_id=str(getattr(args, "publish_item_id", "") or "").strip(),
            immediate_test_mode=bool(getattr(args, "immediate_test_mode", False)),
        )
    if bool(getattr(args, "run_immediate_collect_item_job", False)):
        runner, core = _load_runtime_modules()
        return _run_immediate_collect_item_job(
            runner=runner,
            core=core,
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=default_profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=bot_token,
            telegram_chat_id=allowed_chat_id,
            item_id=str(getattr(args, "publish_item_id", "") or "").strip(),
            immediate_test_mode=bool(getattr(args, "immediate_test_mode", False)),
        )
    if bool(getattr(args, "run_immediate_publish_platform_job", False)):
        runner, core = _load_runtime_modules()
        return _publish_immediate_candidate_platform(
            runner=runner,
            core=core,
            repo_root=repo_root,
            workspace=workspace,
            timeout_seconds=timeout_seconds,
            profile=default_profile,
            telegram_bot_identifier=telegram_bot_identifier,
            telegram_bot_token=bot_token,
            telegram_chat_id=allowed_chat_id,
            item_id=str(getattr(args, "publish_item_id", "") or "").strip(),
            platform=str(getattr(args, "publish_platform", "") or "").strip(),
        )

    _safe_update_worker_state(
        state_file,
        log_file,
        pid=os.getpid(),
        worker="cybercar_telegram_command_worker",
        status="starting",
        startup_stage="acquiring_lock",
        last_error="",
    )
    poller_lock = _acquire_poller_lock(workspace=workspace, log_file=log_file)
    if poller_lock is None:
        _safe_update_worker_state(
            state_file,
            log_file,
            pid=os.getpid(),
            worker="cybercar_telegram_command_worker",
            status="standby",
            startup_stage="lock_busy",
            last_error="poller lock busy",
        )
        return 0

    try:
        try:
            set_commands_state = _safe_update_worker_state(
                state_file,
                log_file,
                pid=os.getpid(),
                worker="cybercar_telegram_command_worker",
                status="starting",
                startup_stage="set_commands",
                last_error="",
            )
            if _should_attempt_set_commands(set_commands_state):
                attempt_at = _now_text()
                _safe_update_worker_state(
                    state_file,
                    log_file,
                    pid=os.getpid(),
                    set_commands_last_attempt_at=attempt_at,
                )
                _set_clickable_commands(
                    bot_token=bot_token,
                    timeout_seconds=max(10, min(timeout_seconds, 60)),
                    log_file=log_file,
                )
                _safe_update_worker_state(
                    state_file,
                    log_file,
                    pid=os.getpid(),
                    set_commands_updated_at=_now_text(),
                    set_commands_last_error="",
                )
            else:
                _append_log(log_file, "[Worker] setMyCommands skipped: recent success/failure backoff active.")
        except Exception as exc:
            error_text = _exception_text(exc)
            _append_log(
                log_file,
                "[Worker] setMyCommands failed; continue without blocking worker: "
                f"{error_text}",
            )
            _safe_update_worker_state(
                state_file,
                log_file,
                pid=os.getpid(),
                set_commands_last_error=error_text,
            )

        state = _safe_update_worker_state(
            state_file,
            log_file,
            pid=os.getpid(),
            worker="cybercar_telegram_command_worker",
            status="starting",
            startup_stage="bootstrap_state",
            last_error="",
        )
        last_processed = int(state.get("last_processed_update_id") or 0)
        offset = _load_offset(offset_file)
        if offset <= 0 and last_processed > 0:
            offset = last_processed
        if last_processed <= 0 and offset > 0:
            last_processed = offset
        bootstrap_fast_forward_pending = False

        # On first run, move cursor to current latest update to avoid replaying historical messages.
        if offset <= 0 and last_processed <= 0:
            try:
                _safe_update_worker_state(
                    state_file,
                    log_file,
                    pid=os.getpid(),
                    worker="cybercar_telegram_command_worker",
                    status="starting",
                    startup_stage="bootstrap_updates",
                    last_error="",
                )
                max_bootstrap_id = _bootstrap_latest_update_id(
                    bot_token=bot_token,
                    timeout_seconds=10,
                )
                if max_bootstrap_id > 0:
                    offset = max_bootstrap_id
                    last_processed = max_bootstrap_id
                    _safe_save_offset(offset_file, offset, log_file)
                    state["last_processed_update_id"] = last_processed
                    state["updated_at"] = _now_text()
                    _safe_save_state(state_file, state, log_file)
            except Exception as exc:
                _append_log(log_file, f"[Worker] bootstrap getUpdates failed: {exc}")
                _record_error_event(
                    workspace=workspace,
                    log_file=log_file,
                    category="polling.bootstrap_getupdates_failed",
                    message="bootstrap getUpdates failed",
                    exc=exc,
                    context={
                        "offset": int(offset or 0),
                        "last_processed": int(last_processed or 0),
                        "bot_token_set": bool(bot_token),
                    },
                    retryable=True,
                    severity="warning",
                )
                if offset <= 0 and last_processed <= 0:
                    bootstrap_fast_forward_pending = True

        _append_log(
            log_file,
            (
                f"[Worker] Started. allowed_chat_id={allowed_chat_id or '*'}, allow_shell={allow_shell}, "
                f"allow_prefixes={len(allow_prefixes)}, command_password_set={bool(command_password)}, "
                f"default_profile={default_profile}, offset={offset}, last_processed={last_processed}"
            ),
        )
        try:
            _refresh_home_surface_on_startup(
                bot_token=bot_token,
                chat_id=allowed_chat_id,
                workspace=workspace,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
                default_profile=default_profile,
            )
        except Exception as exc:
            _append_log(log_file, f"[Worker] startup home surface refresh skipped: {exc}")

        queue_maintenance_epoch = 0.0
        maintenance = _run_periodic_queue_maintenance(
            workspace,
            log_file=log_file,
            last_run_epoch=queue_maintenance_epoch,
            force=True,
        )
        if bool(maintenance.get("ran")):
            queue_maintenance_epoch = float(maintenance.get("last_run_epoch") or queue_maintenance_epoch)
            _update_worker_state(
                state_file,
                pid=os.getpid(),
                last_queue_maintenance_at=str(maintenance.get("ran_at") or ""),
                last_queue_maintenance_epoch=queue_maintenance_epoch,
            )

        while True:
            sleep_seconds = poll_interval
            try:
                _update_worker_state(
                    state_file,
                    pid=os.getpid(),
                    status="polling",
                    startup_stage="",
                    worker_heartbeat_at=_now_text(),
                    last_poll_started_at=_now_text(),
                    offset=offset,
                    last_processed_update_id=last_processed,
                )
                _recover_orphaned_home_action_tasks(
                    workspace=workspace,
                    bot_token=bot_token,
                    timeout_seconds=api_timeout_seconds,
                    log_file=log_file,
                )
                maintenance = _run_periodic_queue_maintenance(
                    workspace,
                    log_file=log_file,
                    last_run_epoch=queue_maintenance_epoch,
                )
                if bool(maintenance.get("ran")):
                    queue_maintenance_epoch = float(maintenance.get("last_run_epoch") or queue_maintenance_epoch)
                    _update_worker_state(
                        state_file,
                        pid=os.getpid(),
                        last_queue_maintenance_at=str(maintenance.get("ran_at") or ""),
                        last_queue_maintenance_epoch=queue_maintenance_epoch,
                    )
                flushed_platform_events = _flush_pending_platform_result_events(
                    workspace=workspace,
                    bot_token=bot_token,
                    chat_id=allowed_chat_id,
                    timeout_seconds=api_timeout_seconds,
                    log_file=log_file,
                )
                if flushed_platform_events > 0:
                    _append_log(log_file, f"[Worker] platform result events flushed: {flushed_platform_events}")
                _flush_pending_background_feedback(
                    workspace=workspace,
                    bot_token=bot_token,
                    chat_id=allowed_chat_id,
                    timeout_seconds=api_timeout_seconds,
                    log_file=log_file,
                )
                _flush_pending_prefilter_retries(
                    workspace=workspace,
                    bot_token=bot_token,
                    chat_id=allowed_chat_id,
                    timeout_seconds=api_timeout_seconds,
                    log_file=log_file,
                )
                updates_resp = _telegram_api(
                    bot_token=bot_token,
                    method="getUpdates",
                    params={
                        "offset": offset + 1,
                        "limit": max(1, int(DEFAULT_BOOTSTRAP_GETUPDATES_LIMIT)),
                        "timeout": poll_timeout,
                        "allowed_updates": json.dumps(TELEGRAM_ALLOWED_UPDATES, ensure_ascii=True),
                    },
                    timeout_seconds=api_timeout_seconds,
                    use_post=False,
                )
                updates = updates_resp.get("result") if isinstance(updates_resp, dict) else []
                if not isinstance(updates, list):
                    updates = []
                if bootstrap_fast_forward_pending and offset <= 0 and last_processed <= 0:
                    first_page_max_update_id = _max_update_id_from_updates(updates)
                    if first_page_max_update_id > 0:
                        max_update_id = first_page_max_update_id
                        if len(updates) >= max(1, int(DEFAULT_BOOTSTRAP_GETUPDATES_LIMIT)):
                            try:
                                max_update_id = max(
                                    max_update_id,
                                    _bootstrap_latest_update_id(
                                        bot_token=bot_token,
                                        timeout_seconds=api_timeout_seconds,
                                        start_after_update_id=first_page_max_update_id,
                                    ),
                                )
                            except Exception as scan_exc:
                                _append_log(
                                    log_file,
                                    (
                                        "[Worker] bootstrap fallback paging scan failed; "
                                        f"using first page cursor={first_page_max_update_id}: {_exception_text(scan_exc)}"
                                    ),
                                )
                        offset = max_update_id
                        last_processed = max_update_id
                        _safe_save_offset(offset_file, offset, log_file)
                        _safe_update_worker_state(
                            state_file,
                            log_file,
                            pid=os.getpid(),
                            last_processed_update_id=last_processed,
                            offset=offset,
                        )
                        _append_log(
                            log_file,
                            (
                                "[Worker] bootstrap fallback fast-forward applied: "
                                f"last_processed={last_processed}, first_page_max={first_page_max_update_id}, "
                                f"skipped_updates={len(updates)}"
                            ),
                        )
                        bootstrap_fast_forward_pending = False
                        continue
                if suppressed_poll_transport_warnings > 0:
                    _append_log(
                        log_file,
                        (
                            "[Worker] Telegram poll transport recovered; "
                            f"suppressed_warnings={suppressed_poll_transport_warnings}."
                        ),
                    )
                    suppressed_poll_transport_warnings = 0
                    poll_transport_warning_last_logged_at = 0.0
                consecutive_poll_failures = 0
                poll_failure_burst_started_at = 0.0
                _update_worker_state(
                    state_file,
                    pid=os.getpid(),
                    status="polling",
                    startup_stage="",
                    worker_heartbeat_at=_now_text(),
                    offset=offset,
                    last_processed_update_id=last_processed,
                    consecutive_poll_failures=0,
                    last_error="",
                )

                for update in updates:
                    if not isinstance(update, dict):
                        continue
                    update_id = int(update.get("update_id") or 0)
                    if update_id > offset:
                        offset = update_id
                        _save_offset(offset_file, offset)
                    handled_cmd = handle_command_update(
                        update=update,
                        bot_token=bot_token,
                        allowed_chat_id=allowed_chat_id,
                        allow_private_chat_commands=False,
                        command_password=command_password,
                        started_at=started_at,
                        repo_root=repo_root,
                        workspace=workspace,
                        timeout_seconds=timeout_seconds,
                        allow_shell=allow_shell,
                        allow_prefixes=allow_prefixes,
                        audit_file=audit_file,
                        last_processed=last_processed,
                        log_file=log_file,
                        telegram_bot_identifier=telegram_bot_identifier,
                        default_profile=default_profile,
                    )
                    handled_cb = handle_callback_update(
                        update=update,
                        bot_token=bot_token,
                        allowed_chat_id=allowed_chat_id,
                        allow_private_chat_commands=False,
                        command_password=command_password,
                        started_at=started_at,
                        repo_root=repo_root,
                        workspace=workspace,
                        timeout_seconds=timeout_seconds,
                        allow_shell=allow_shell,
                        allow_prefixes=allow_prefixes,
                        audit_file=audit_file,
                        last_processed=last_processed,
                        log_file=log_file,
                        telegram_bot_identifier=telegram_bot_identifier,
                        default_profile=default_profile,
                        immediate_test_mode=bool(getattr(args, "immediate_test_mode", False)),
                    )
                    new_last_processed = max(
                        int(last_processed),
                        int(handled_cmd.get("last_processed") or last_processed),
                        int(handled_cb.get("last_processed") or last_processed),
                    )
                    handled_any = bool(handled_cmd.get("handled")) or bool(handled_cb.get("handled"))
                    if handled_any and new_last_processed > last_processed:
                        last_processed = new_last_processed
                        _update_worker_state(
                            state_file,
                            pid=os.getpid(),
                            status="polling",
                            last_processed_update_id=last_processed,
                            offset=offset,
                            last_error="",
                        )

                if args.once:
                    _append_log(log_file, "[Worker] once mode done; exit.")
                    _update_worker_state(
                        state_file,
                        pid=os.getpid(),
                        status="stopped",
                        startup_stage="",
                        worker_heartbeat_at=_now_text(),
                        last_poll_completed_at=_now_text(),
                        offset=offset,
                        last_processed_update_id=last_processed,
                    )
                    return 0
                _update_worker_state(
                    state_file,
                    pid=os.getpid(),
                    status="idle",
                    startup_stage="",
                    worker_heartbeat_at=_now_text(),
                    last_poll_completed_at=_now_text(),
                    offset=offset,
                    last_processed_update_id=last_processed,
                    last_error="",
                )
            except KeyboardInterrupt:
                _append_log(log_file, "[Worker] stopped by keyboard interrupt.")
                _update_worker_state(
                    state_file,
                    pid=os.getpid(),
                    status="stopped",
                    startup_stage="",
                    worker_heartbeat_at=_now_text(),
                )
                return 0
            except Exception as exc:
                error_text = _exception_text(exc)
                poll_transport_error = _is_telegram_poll_network_error(exc)
                poll_rate_limit_error = _is_telegram_rate_limit_error_text(error_text)
                poll_conflict_error = _is_telegram_poll_conflict_error_text(error_text)
                poll_queue_lock_timeout_error = _is_prefilter_queue_lock_timeout_error_text(error_text)
                poll_queue_io_contention_error = _is_prefilter_queue_io_contention_error_text(error_text)
                poll_retryable_error = (
                    poll_transport_error
                    or poll_rate_limit_error
                    or poll_conflict_error
                    or poll_queue_lock_timeout_error
                    or poll_queue_io_contention_error
                )
                consecutive_poll_failures += 1
                if poll_retryable_error:
                    if consecutive_poll_failures == 1 or poll_failure_burst_started_at <= 0:
                        poll_failure_burst_started_at = time.time()
                    sleep_seconds = _compute_poll_transport_backoff_seconds(
                        consecutive_failures=consecutive_poll_failures,
                        base_interval_seconds=poll_interval,
                    )
                    retry_after_seconds = (
                        _extract_telegram_retry_after_seconds(error_text)
                        if poll_rate_limit_error
                        else 0
                    )
                    if retry_after_seconds > 0:
                        sleep_seconds = max(sleep_seconds, retry_after_seconds)
                    now_epoch = time.time()
                    if _should_log_poll_transport_warning(
                        consecutive_failures=consecutive_poll_failures,
                        last_logged_epoch=poll_transport_warning_last_logged_at,
                        now_epoch=now_epoch,
                    ):
                        suppressed_suffix = (
                            f", suppressed={suppressed_poll_transport_warnings}"
                            if suppressed_poll_transport_warnings > 0
                            else ""
                        )
                        warning_prefix = (
                            "[Worker] Telegram poll rate limited: "
                            if poll_rate_limit_error
                            else (
                                "[Worker] Telegram poll conflict warning: "
                                if poll_conflict_error
                                else (
                                    "[Worker] Telegram poll queue lock warning: "
                                    if poll_queue_lock_timeout_error
                                    else (
                                        "[Worker] Telegram poll queue io warning: "
                                        if poll_queue_io_contention_error
                                        else "[Worker] Telegram poll transport warning: "
                                    )
                                )
                            )
                        )
                        _append_log(
                            log_file,
                            (
                                f"{warning_prefix}{error_text}; failures={consecutive_poll_failures}; "
                                f"backoff={sleep_seconds}s"
                                f"{'; retry_after=' + str(retry_after_seconds) + 's' if retry_after_seconds > 0 else ''}"
                                f"{suppressed_suffix}"
                            ),
                        )
                        poll_transport_warning_last_logged_at = now_epoch
                        suppressed_poll_transport_warnings = 0
                    else:
                        suppressed_poll_transport_warnings += 1
                else:
                    _append_log(log_file, f"[Worker] loop error: {error_text}")
                _record_error_event(
                    workspace=workspace,
                    log_file=log_file,
                    category="polling.loop_exception",
                    message="telegram worker polling loop raised exception",
                    error_text=error_text,
                    exc=exc,
                    context={
                        "offset": int(offset or 0),
                        "last_processed": int(last_processed or 0),
                        "consecutive_poll_failures": int(consecutive_poll_failures or 0),
                        "poll_transport_error": bool(poll_transport_error),
                        "poll_rate_limit_error": bool(poll_rate_limit_error),
                        "poll_conflict_error": bool(poll_conflict_error),
                        "poll_queue_lock_timeout_error": bool(poll_queue_lock_timeout_error),
                        "poll_queue_io_contention_error": bool(poll_queue_io_contention_error),
                        "backoff_seconds": float(sleep_seconds or 0),
                    },
                    retryable=bool(poll_retryable_error),
                    severity="warning" if poll_retryable_error else "error",
                )
                _update_worker_state(
                    state_file,
                    pid=os.getpid(),
                    status="polling" if poll_retryable_error else "error",
                    startup_stage=(
                        "prefilter_queue_lock_retry"
                        if poll_queue_lock_timeout_error
                        else (
                            "prefilter_queue_write_retry"
                            if poll_queue_io_contention_error
                            else ("poll_transport_retry" if poll_retryable_error else "")
                        )
                    ),
                    worker_heartbeat_at=_now_text(),
                    last_error=(
                        "telegram poll rate limited; retrying with backoff"
                        if poll_rate_limit_error
                        else (
                        "telegram poll conflict detected; another poller may be active"
                            if poll_conflict_error
                            else (
                            "prefilter queue lock contention; retrying with backoff"
                                if poll_queue_lock_timeout_error
                                else (
                                    "prefilter queue io contention; retrying with backoff"
                                    if poll_queue_io_contention_error
                                    else (
                                        "telegram poll transport jitter; send path remains retryable"
                                        if poll_transport_error
                                        else error_text
                                    )
                                )
                            )
                        )
                    ),
                    consecutive_poll_failures=consecutive_poll_failures,
                )
                if args.once:
                    return 1
                failure_span_seconds = (
                    max(0.0, time.time() - poll_failure_burst_started_at)
                    if poll_transport_error and poll_failure_burst_started_at > 0
                    else 0.0
                )
                if _should_restart_after_poll_error(
                    exc,
                    consecutive_failures=consecutive_poll_failures,
                    threshold=poll_network_failure_restart_threshold,
                    failure_span_seconds=failure_span_seconds,
                    min_failure_span_seconds=poll_network_failure_restart_min_span_seconds,
                ):
                    _append_log(
                        log_file,
                        (
                            "[Worker] consecutive Telegram poll network failures reached threshold "
                            f"({consecutive_poll_failures}/{poll_network_failure_restart_threshold}); "
                            f"failure_span={int(failure_span_seconds)}s; exit for supervisor restart."
                        ),
                    )
                    _update_worker_state(
                        state_file,
                        pid=os.getpid(),
                        status="stopped",
                        startup_stage="restart_requested",
                        worker_heartbeat_at=_now_text(),
                        last_error=(
                            "telegram poll network failure threshold reached; "
                            f"consecutive_failures={consecutive_poll_failures}"
                        ),
                        consecutive_poll_failures=consecutive_poll_failures,
                    )
                    return 2
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    finally:
        _release_poller_lock(poller_lock)


if __name__ == "__main__":
    raise SystemExit(main())
