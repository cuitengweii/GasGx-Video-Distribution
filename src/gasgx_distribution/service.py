from __future__ import annotations

import copy
import json
import os
import re
import subprocess
import sys
import hmac
import hashlib
import base64
from io import BytesIO
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from threading import Lock, Thread
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from contextvars import ContextVar
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import requests
from PIL import Image, ImageStat

from cybercar import engine
from cybercar.settings import apply_runtime_environment as apply_cybercar_environment

from .db import connect, database_path, dict_from_row, init_db, now_ts, use_database
from .paths import get_paths
from .platforms import DEBUG_PORT_END, DEBUG_PORT_START, SUPPORTED_PLATFORMS, get_platform, normalize_platform, stable_debug_port
from .public_settings import load_distribution_settings, load_platform_publish_settings, resolve_material_dir
from .public_settings import save_distribution_settings as save_local_distribution_settings
from .supabase_backend import SupabaseError, SupabaseRestClient
from .video_matrix.cover_templates import load_cover_templates
from .video_matrix.output_root import resolve_video_matrix_output_root
from .video_matrix.settings import ProjectSettings
from .video_matrix.templates import load_templates
from .video_matrix.ui_state import load_ui_state

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}
AI_ROBOT_PLATFORMS = {"wecom", "dingtalk", "lark", "telegram", "whatsapp"}
AI_ROBOT_RETRY_LIMIT = 3
NOTIFICATION_EVENT_DEFINITIONS = [
    {
        "event_type": "wechat_login_qr",
        "label": "登录二维码",
        "default_severity": "blocking",
        "default_center": True,
        "routeable": True,
        "source": "登录巡检或发布前置检查生成/更新扫码二维码时触发",
        "subtypes": ["qr_generated", "qr_updated", "login_required"],
    },
    {
        "event_type": "login_status",
        "label": "登录状态",
        "default_severity": "blocking",
        "default_center": True,
        "routeable": True,
        "source": "登录巡检、发布前置检查或终端执行发现登录失效/即将失效/登录失败时触发",
        "subtypes": ["expired", "expiring_soon", "failed", "inspection_result"],
    },
    {
        "event_type": "publish_result",
        "label": "发布结果",
        "default_severity": "error",
        "default_center": True,
        "routeable": True,
        "source": "发布、上传、草稿保存、仅上传不发布、任务终态变化时触发",
        "subtypes": ["published", "failed", "upload_failed", "draft_saved", "uploaded_only", "queued", "running", "cancelled"],
    },
    {
        "event_type": "video_generation",
        "label": "视频生成",
        "default_severity": "warning",
        "default_center": True,
        "routeable": True,
        "source": "视频矩阵生成流水线完成、失败或异常中断时触发",
        "subtypes": ["completed", "failed", "interrupted"],
    },
    {
        "event_type": "material_issue",
        "label": "素材问题",
        "default_severity": "warning",
        "default_center": True,
        "routeable": True,
        "source": "素材盘点、生成前校验或发布前校验发现素材不足/分类不完整/不可用素材跳过时触发",
        "subtypes": ["insufficient", "category_incomplete", "skipped_unusable"],
    },
    {
        "event_type": "system_error",
        "label": "系统异常",
        "default_severity": "critical",
        "default_center": True,
        "routeable": True,
        "source": "关键依赖不可用、调度/作业失败、存储或配置异常被捕获时触发",
        "subtypes": ["dependency_unavailable", "scheduler_failed", "job_failed", "storage_error", "config_error"],
    },
    {
        "event_type": "ops_summary",
        "label": "运营汇总",
        "default_severity": "info",
        "default_center": True,
        "routeable": True,
        "source": "日报、运营摘要生成后推送完成或推送失败时触发",
        "subtypes": ["daily_sent", "daily_failed", "summary_sent", "summary_failed"],
    },
    {
        "event_type": "action_required",
        "label": "人工处理",
        "default_severity": "warning",
        "default_center": True,
        "routeable": True,
        "source": "流程需要人工确认、补充配置或处理队列积压时触发",
        "subtypes": ["confirmation_required", "configuration_required", "queue_backlog"],
    },
]
NOTIFICATION_EVENT_TYPES = {item["event_type"] for item in NOTIFICATION_EVENT_DEFINITIONS}
NOTIFICATION_EVENT_META = {item["event_type"]: item for item in NOTIFICATION_EVENT_DEFINITIONS}
NOTIFICATION_PLATFORMS = {"telegram", "dingtalk", "wecom"}
NOTIFICATION_OPEN_STATUSES = {"open", "acknowledged", "assigned"}
NOTIFICATION_CLOSED_STATUSES = {"resolved", "ignored"}
NOTIFICATION_POLICY_SEVERITY_ORDER = {"critical": 5, "blocking": 4, "error": 3, "warning": 2, "info": 1}
LOGIN_QR_NOTIFY_COOLDOWN_SECONDS = 1800
BRAND_INLINE_ASSET_MAX_CHARS = int(os.getenv("GASGX_BRAND_INLINE_ASSET_MAX_CHARS", "200000") or 200000)
BRAND_PUBLIC_SETTING_COLUMNS = "id,name,slogan,primary_color,theme_id,default_account_prefix,created_at,updated_at"
TERMINAL_LOGIN_PROBE_INTERVAL_SECONDS = int(os.getenv("GASGX_TERMINAL_LOGIN_PROBE_INTERVAL_SECONDS", "5") or 5)
TERMINAL_QR_EXPIRY_SECONDS = int(os.getenv("GASGX_TERMINAL_QR_EXPIRY_SECONDS", "60") or 60)
TERMINAL_LOGIN_PROBE_TIMEOUT_SECONDS = int(os.getenv("GASGX_TERMINAL_LOGIN_PROBE_TIMEOUT_SECONDS", "4") or 4)
TERMINAL_LOGIN_CONFIRM_TEXT = "请扫码，完成后点“我已登录”"
TERMINAL_LOGIN_READY_TEXT = "已人工确认登录，等待准备发布"
TERMINAL_PUBLISH_PREPARE_TEXT = "正在准备发布页面，请看浏览器"
TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT = "请在视频号页面手动发布，完成后点成功"
SEED_VERSION = "2026-04-29-supabase-db-init-v1"
SUPER_ADMIN_PASSWORD = "cuitengwei2023"
FEATURE_ENTRIES = [
    {"id": "overview", "label": "总览", "group": "业务工作台"},
    {"id": "accounts", "label": "账号矩阵", "group": "业务工作台"},
    {"id": "settings", "label": "公共设置", "group": "业务工作台"},
    {"id": "tasks", "label": "任务中心", "group": "业务工作台"},
    {"id": "terminal-execution", "label": "终端执行", "group": "业务工作台"},
    {"id": "stats", "label": "数据统计", "group": "业务工作台"},
    {"id": "ai-robot", "label": "AI机器人", "group": "业务工作台"},
    {"id": "video-matrix", "label": "视频生成", "group": "业务工作台"},
    {"id": "user-center", "label": "用户中心", "group": "系统管理"},
    {"id": "notifications", "label": "通知中心", "group": "系统管理"},
    {"id": "system-settings", "label": "系统设置", "group": "系统管理"},
    {"id": "help-center", "label": "帮助文档", "group": "系统管理"},
]
DEFAULT_ROLE_PERMISSIONS = {
    "super_admin": [item["id"] for item in FEATURE_ENTRIES],
    "publisher": ["overview", "accounts", "settings", "tasks", "terminal-execution", "video-matrix", "user-center", "notifications", "help-center"],
    "material_manager": ["overview", "accounts", "video-matrix", "user-center", "notifications", "help-center"],
    "data_monitor": ["overview", "stats", "user-center", "notifications", "help-center"],
}
DEFAULT_ROLE_NAMES = {
    "super_admin": "超级管理员",
    "publisher": "发布员",
    "material_manager": "素材维护员",
    "data_monitor": "数据监控员",
}
_brand_runtime: ContextVar[dict[str, Any] | None] = ContextVar("gasgx_brand_runtime", default=None)


def _account_slug(account_key: str) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(account_key or "").strip())
    token = "-".join(part for part in token.split("-") if part)
    if not token:
        raise ValueError("account_key is required")
    return token[:80]


def _looks_like_mojibake(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    if "?" in value and not re.search(r"[\u4e00-\u9fff]", value):
        return True
    mojibake_markers = ("澶", "鐕", "鍙", "鎸", "娴", "彂", "æ", "ç", "å")
    return any(marker in value for marker in mojibake_markers)


def _normalize_account_display_name(account: dict[str, Any], *, fallback_prefix: str = "账号") -> str:
    account_id = int(account.get("id") or 0)
    account_key = str(account.get("account_key") or "").strip()
    raw = str(account.get("display_name") or "").strip()
    if raw and not _looks_like_mojibake(raw):
        return raw
    if account_key and not _looks_like_mojibake(account_key):
        return account_key
    return f"{fallback_prefix} {account_id}" if account_id > 0 else fallback_prefix


def _normalize_account_niche(account: dict[str, Any]) -> str:
    raw = str(account.get("niche") or "").strip()
    if not raw or _looks_like_mojibake(raw):
        return ""
    return raw


def _normalize_account_runtime(account: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(account)
    normalized["display_name"] = _normalize_account_display_name(normalized)
    normalized["niche"] = _normalize_account_niche(normalized)
    return normalized


def _stable_int(seed: str, modulo: int) -> int:
    return int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16) % max(1, modulo)


def build_browser_fingerprint(account_key: str, platform: str) -> dict[str, Any]:
    token = f"{_account_slug(account_key)}:{normalize_platform(platform)}"
    width = 1280 + (_stable_int(token + ":w", 4) * 80)
    height = 820 + (_stable_int(token + ":h", 4) * 40)
    languages = ["zh-CN,zh;q=0.9,en;q=0.8", "zh-CN,zh;q=0.9", "zh-CN,en-US;q=0.8,en;q=0.7"]
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    ]
    language = languages[_stable_int(token + ":lang", len(languages))]
    return {
        "provider": "builtin-light",
        "user_agent": user_agents[_stable_int(token + ":ua", len(user_agents))],
        "language": language,
        "locale": language.split(",")[0],
        "timezone": "Asia/Shanghai",
        "window_size": {"width": width, "height": height},
        "proxy_slot": "",
    }


def browser_fingerprint_launch_args(fingerprint: dict[str, Any] | str | None) -> list[str]:
    if isinstance(fingerprint, str):
        fingerprint = _json_payload(fingerprint, {})
    if not isinstance(fingerprint, dict):
        return []
    args: list[str] = []
    user_agent = str(fingerprint.get("user_agent") or "").strip()
    if user_agent:
        args.append(f"--user-agent={user_agent}")
    language = str(fingerprint.get("language") or fingerprint.get("locale") or "").strip()
    if language:
        args.append(f"--lang={language.split(',')[0]}")
    size = fingerprint.get("window_size")
    if isinstance(size, dict):
        try:
            width = int(size.get("width") or 0)
            height = int(size.get("height") or 0)
        except Exception:
            width = height = 0
        if width > 0 and height > 0:
            args.append(f"--window-size={width},{height}")
    proxy = str(fingerprint.get("proxy_slot") or "").strip()
    if proxy:
        args.append(f"--proxy-server={proxy}")
    return args


@contextmanager
def _chrome_fingerprint_env(fingerprint: dict[str, Any] | str | None) -> Iterator[None]:
    args = browser_fingerprint_launch_args(fingerprint)
    previous = os.environ.get("CYBERCAR_CHROME_EXTRA_ARGS")
    if args:
        os.environ["CYBERCAR_CHROME_EXTRA_ARGS"] = json.dumps(args, ensure_ascii=False)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("CYBERCAR_CHROME_EXTRA_ARGS", None)
        else:
            os.environ["CYBERCAR_CHROME_EXTRA_ARGS"] = previous


def ensure_database() -> None:
    init_db()
    ensure_operator_auth_seed()


def use_brand_database(path: Path):
    return use_database(path)


@contextmanager
def use_brand_runtime(instance: dict[str, Any]) -> Iterator[None]:
    token = _brand_runtime.set(dict(instance))
    try:
        yield
    finally:
        _brand_runtime.reset(token)


def brand_database_backend() -> str:
    explicit = os.getenv("BRAND_DATABASE_BACKEND", "").strip().lower()
    if explicit:
        return explicit
    instance = _brand_runtime.get() or {}
    if instance.get("supabase_url") and instance.get("service_key_ref"):
        return "supabase"
    return "sqlite"


def _brand_supabase() -> SupabaseRestClient:
    instance = _brand_runtime.get() or {}
    if not instance or not instance.get("supabase_url") or not instance.get("service_key_ref"):
        return SupabaseRestClient.from_env(prefix="BRAND_SUPABASE")
    return SupabaseRestClient.from_instance(instance)


_SUPABASE_READ_CACHE_LOCK = Lock()
_SUPABASE_READ_CACHE: dict[str, Any] = {}
_SUPABASE_APP_SETTINGS_CACHE: dict[str, Any] = {}


def clear_supabase_read_cache() -> dict[str, Any]:
    """Drop in-process caches for repeated Supabase reads and app settings; the next requests refetch from PostgREST."""
    backend = brand_database_backend()
    if backend != "supabase":
        return {"ok": True, "backend": backend, "cleared": False}
    with _SUPABASE_READ_CACHE_LOCK:
        _SUPABASE_READ_CACHE.clear()
        _SUPABASE_APP_SETTINGS_CACHE.clear()
    return {"ok": True, "backend": "supabase", "cleared": True}


def clear_runtime_caches() -> dict[str, Any]:
    """Clear process-local caches without requiring Supabase to be on the request path."""
    supabase = clear_supabase_read_cache()
    return {"ok": True, "backend": brand_database_backend(), "supabase_read_cache": supabase}


def _invalidate_supabase_read_cache(*keys: str) -> None:
    if brand_database_backend() != "supabase" or not keys:
        return
    with _SUPABASE_READ_CACHE_LOCK:
        for key in keys:
            _SUPABASE_READ_CACHE.pop(key, None)


def _invalidate_supabase_app_settings_cache(*keys: str) -> None:
    if brand_database_backend() != "supabase" or not keys:
        return
    with _SUPABASE_READ_CACHE_LOCK:
        for key in keys:
            _SUPABASE_APP_SETTINGS_CACHE.pop(key, None)


def _supabase_read_cache_peek(key: str) -> bool:
    with _SUPABASE_READ_CACHE_LOCK:
        return key in _SUPABASE_READ_CACHE


def _supabase_read_cache_get(key: str) -> Any:
    with _SUPABASE_READ_CACHE_LOCK:
        return copy.deepcopy(_SUPABASE_READ_CACHE[key])


def _supabase_read_cache_set(key: str, value: Any) -> None:
    with _SUPABASE_READ_CACHE_LOCK:
        _SUPABASE_READ_CACHE[key] = copy.deepcopy(value)


def _json_payload(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


SYNC_TABLE_CONFLICT_KEYS = {
    "matrix_accounts": "id",
    "account_platforms": "account_id,platform",
    "browser_profiles": "account_id",
    "brand_settings": "id",
    "notification_routes": "event_type,platform",
    "notification_policies": "event_type,severity,platform,account_scope",
    "notification_incidents": "event_type,dedupe_key",
    "notification_actions": "id",
    "automation_tasks": "id",
    "video_stats_snapshots": "id",
    "wechat_stats_account_snapshots": "account_id,platform,stat_date",
    "ai_robot_configs": "platform",
    "ai_robot_messages": "id",
    "app_settings": "setting_key",
}
SYNC_JSON_FIELDS = {"payload_json", "fingerprint_json", "raw_json", "target_platforms_json", "escalation_platforms_json"}
SYNC_INTEGER_FIELDS_BY_TABLE = {
    "account_platforms": {"enabled"},
    "notification_routes": {"enabled"},
    "notification_policies": {"enabled", "escalation_enabled"},
    "ai_robot_configs": {"enabled"},
}


def _sync_outbox_enabled() -> bool:
    return brand_database_backend() == "sqlite"


def _sync_entity_id(table_name: str, payload: dict[str, Any]) -> str:
    keys = [key.strip() for key in SYNC_TABLE_CONFLICT_KEYS.get(table_name, "id").split(",") if key.strip()]
    parts = [f"{key}={payload.get(key)}" for key in keys if payload.get(key) is not None]
    return "|".join(parts) or str(payload.get("id") or "")


def _enqueue_sync_payload(
    conn,
    table_name: str,
    operation: str,
    payload: dict[str, Any],
    *,
    entity_id: str | None = None,
) -> None:
    if not _sync_outbox_enabled() or table_name not in SYNC_TABLE_CONFLICT_KEYS:
        return
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO sync_outbox(entity_type, table_name, entity_id, operation, payload_json, status, retry_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)
        """,
        (
            table_name,
            table_name,
            entity_id or _sync_entity_id(table_name, payload),
            operation,
            json.dumps(payload, ensure_ascii=False, default=str),
            ts,
            ts,
        ),
    )


def _enqueue_sync_row(conn, table_name: str, where_sql: str, params: tuple[Any, ...], *, entity_id: str | None = None) -> None:
    if not _sync_outbox_enabled():
        return
    row = conn.execute(f"SELECT * FROM {table_name} WHERE {where_sql}", params).fetchone()
    if row is not None:
        _enqueue_sync_payload(conn, table_name, "upsert", dict_from_row(row), entity_id=entity_id)


def _enqueue_sync_delete(conn, table_name: str, filters: dict[str, Any], *, entity_id: str | None = None) -> None:
    _enqueue_sync_payload(conn, table_name, "delete", {"filters": filters}, entity_id=entity_id or _sync_entity_id(table_name, filters))


def _enqueue_account_sync_rows(conn, account_id: int) -> None:
    _enqueue_sync_row(conn, "matrix_accounts", "id = ?", (account_id,))
    for row in conn.execute("SELECT * FROM account_platforms WHERE account_id = ?", (account_id,)):
        _enqueue_sync_payload(conn, "account_platforms", "upsert", dict_from_row(row))
    _enqueue_sync_row(conn, "browser_profiles", "account_id = ?", (account_id,))


def sync_status() -> dict[str, Any]:
    ensure_database()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM sync_outbox
            GROUP BY status
            """
        ).fetchall()
        counts = {str(row["status"]): int(row["count"]) for row in rows}
        last_synced = conn.execute("SELECT MAX(synced_at) AS value FROM sync_outbox WHERE status = 'synced'").fetchone()["value"]
        last_error = conn.execute(
            """
            SELECT error FROM sync_outbox
            WHERE status = 'failed' AND error <> ''
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        conflict_count = conn.execute("SELECT COUNT(*) AS count FROM sync_conflicts").fetchone()["count"]
    pending = counts.get("pending", 0)
    failed = counts.get("failed", 0)
    return {
        "ok": True,
        "backend": brand_database_backend(),
        "mode": "local_first",
        "local_database": str(database_path()),
        "supabase_configured": bool(os.getenv("SUPABASE_URL") and os.getenv("BRAND_SUPABASE_SERVICE_KEY")),
        "pending": pending,
        "failed": failed,
        "synced": counts.get("synced", 0),
        "queued": pending + failed,
        "conflicts": int(conflict_count or 0),
        "last_synced_at": last_synced,
        "last_error": str(last_error["error"] or "") if last_error else "",
    }


def retry_sync_outbox() -> dict[str, Any]:
    ensure_database()
    ts = now_ts()
    with connect() as conn:
        cursor = conn.execute(
            """
            UPDATE sync_outbox
            SET status = 'pending', error = '', updated_at = ?
            WHERE status = 'failed'
            """,
            (ts,),
        )
        retried = int(cursor.rowcount or 0)
    return {"ok": True, "retried": retried, "status": sync_status()}


def _sync_payload_for_supabase(table_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = copy.deepcopy(payload)
    for field in SYNC_JSON_FIELDS:
        if field in data:
            default = [] if str(data.get(field) or "").strip().startswith("[") else {}
            data[field] = _json_payload(data.get(field), default)
    for field in SYNC_INTEGER_FIELDS_BY_TABLE.get(table_name, set()):
        if field in data:
            data[field] = 1 if bool(data.get(field)) else 0
    return data


def _sync_filters_for_payload(table_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    keys = [key.strip() for key in SYNC_TABLE_CONFLICT_KEYS.get(table_name, "id").split(",") if key.strip()]
    return {key: payload[key] for key in keys if key in payload and payload[key] is not None}


def _record_sync_conflict(conn, table_name: str, entity_id: str, local_payload: dict[str, Any], remote_payload: dict[str, Any]) -> None:
    ts = now_ts()
    conn.execute(
        """
        INSERT INTO sync_conflicts(table_name, entity_id, local_payload_json, remote_payload_json, resolution, warning, created_at)
        VALUES (?, ?, ?, ?, 'local_overwrite', ?, ?)
        """,
        (
            table_name,
            entity_id,
            json.dumps(local_payload, ensure_ascii=False, default=str),
            json.dumps(remote_payload, ensure_ascii=False, default=str),
            "remote row was newer; local row overwrote remote by policy",
            ts,
        ),
    )


def _detect_sync_conflict(conn, client: SupabaseRestClient, table_name: str, entity_id: str, payload: dict[str, Any]) -> None:
    filters = _sync_filters_for_payload(table_name, payload)
    if not filters or not hasattr(client, "select_one"):
        return
    try:
        remote = client.select_one(table_name, filters=filters)
    except Exception:
        return
    if not isinstance(remote, dict) or not remote:
        return
    try:
        remote_updated = int(remote.get("updated_at") or 0)
        local_updated = int(payload.get("updated_at") or payload.get("created_at") or 0)
    except Exception:
        return
    if remote_updated > local_updated:
        _record_sync_conflict(conn, table_name, entity_id, payload, remote)


def push_sync_outbox_to_supabase(*, limit: int = 100) -> dict[str, Any]:
    ensure_database()
    limit = max(1, min(int(limit or 100), 500))
    pushed = 0
    failed = 0
    skipped = 0
    errors: list[str] = []
    try:
        client = _brand_supabase()
    except Exception as exc:
        error = str(exc)
        ts = now_ts()
        with connect() as conn:
            cursor = conn.execute(
                """
                UPDATE sync_outbox
                SET status = 'failed', retry_count = retry_count + 1, last_attempt_at = ?, updated_at = ?, error = ?
                WHERE id IN (
                    SELECT id FROM sync_outbox
                    WHERE status IN ('pending', 'failed')
                    ORDER BY id ASC
                    LIMIT ?
                )
                """,
                (ts, ts, error[:1000], limit),
            )
            failed = int(cursor.rowcount or 0)
        return {"ok": False, "pushed": 0, "failed": failed, "skipped": 0, "errors": [error], "status": sync_status()}
    resolved_ids: list[int] = []
    with connect() as conn:
        rows = [
            dict_from_row(row)
            for row in conn.execute(
                """
                SELECT * FROM sync_outbox
                WHERE status IN ('pending', 'failed')
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,),
            )
        ]
        for row in rows:
            ts = now_ts()
            table_name = str(row.get("table_name") or "")
            operation = str(row.get("operation") or "")
            try:
                payload = _json_payload(row.get("payload_json"), {})
                if table_name not in SYNC_TABLE_CONFLICT_KEYS or not isinstance(payload, dict):
                    skipped += 1
                    conn.execute(
                        "UPDATE sync_outbox SET status = 'synced', synced_at = ?, updated_at = ?, error = ? WHERE id = ?",
                        (ts, ts, "skipped unsupported sync item", row["id"]),
                    )
                    continue
                if operation == "delete":
                    filters = payload.get("filters")
                    if not isinstance(filters, dict) or not filters:
                        raise ValueError("delete sync item missing filters")
                    client.delete(table_name, filters=filters)
                elif operation == "upsert":
                    supabase_payload = _sync_payload_for_supabase(table_name, payload)
                    _detect_sync_conflict(conn, client, table_name, str(row.get("entity_id") or ""), supabase_payload)
                    client.upsert(table_name, supabase_payload, on_conflict=SYNC_TABLE_CONFLICT_KEYS[table_name])
                else:
                    raise ValueError(f"unsupported sync operation: {operation}")
                pushed += 1
                conn.execute(
                    "UPDATE sync_outbox SET status = 'synced', synced_at = ?, updated_at = ?, error = '' WHERE id = ?",
                    (ts, ts, row["id"]),
                )
            except Exception as exc:
                failed += 1
                error = str(exc)[:1000]
                errors.append(error)
                conn.execute(
                    """
                    UPDATE sync_outbox
                    SET status = 'failed', retry_count = retry_count + 1, last_attempt_at = ?, updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (ts, ts, error, row["id"]),
                )
    return {"ok": failed == 0, "pushed": pushed, "failed": failed, "skipped": skipped, "errors": errors[:5], "status": sync_status()}


def pull_accounts_from_supabase_to_sqlite() -> dict[str, Any]:
    """Import remote matrix account metadata into the local SQLite database without making Supabase the page backend."""
    ensure_database()
    client = _brand_supabase()
    accounts = client.select("matrix_accounts", order="id.asc")
    platforms = client.select("account_platforms", order="account_id.asc,platform.asc")
    try:
        profiles = client.select("browser_profiles")
    except Exception:
        profiles = []
    ts = now_ts()
    imported_accounts = 0
    skipped_accounts = 0
    imported_platforms = 0
    imported_profiles = 0
    account_id_map: dict[int, int] = {}
    with connect() as conn:
        for account in accounts:
            account_id = int(account.get("id") or 0)
            account_key = _account_slug(str(account.get("account_key") or account.get("display_name") or ""))
            if account_id <= 0 or not account_key:
                skipped_accounts += 1
                continue
            remote_updated = int(account.get("updated_at") or account.get("created_at") or ts)
            existing_by_id = conn.execute("SELECT * FROM matrix_accounts WHERE id = ?", (account_id,)).fetchone()
            existing_by_key = conn.execute("SELECT * FROM matrix_accounts WHERE account_key = ?", (account_key,)).fetchone()
            if existing_by_key is not None and int(existing_by_key["id"]) != account_id:
                local_account_id = int(existing_by_key["id"])
                account_id_map[account_id] = local_account_id
                if int(existing_by_key["updated_at"] or 0) > remote_updated:
                    skipped_accounts += 1
                    continue
                conn.execute(
                    """
                    UPDATE matrix_accounts
                    SET display_name = ?, niche = ?, status = ?, notes = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        str(account.get("display_name") or account_key).strip(),
                        str(account.get("niche") or "").strip(),
                        str(account.get("status") or "active").strip() or "active",
                        str(account.get("notes") or "").strip(),
                        remote_updated or ts,
                        local_account_id,
                    ),
                )
                imported_accounts += 1
                continue
            existing = existing_by_id or existing_by_key
            if existing is not None:
                account_id_map[account_id] = int(existing["id"])
                if int(existing["updated_at"] or 0) > remote_updated:
                    skipped_accounts += 1
                    continue
            conn.execute(
                """
                INSERT INTO matrix_accounts(id, account_key, display_name, niche, status, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    account_key = excluded.account_key,
                    display_name = excluded.display_name,
                    niche = excluded.niche,
                    status = excluded.status,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (
                    account_id,
                    account_key,
                    str(account.get("display_name") or account_key).strip(),
                    str(account.get("niche") or "").strip(),
                    str(account.get("status") or "active").strip() or "active",
                    str(account.get("notes") or "").strip(),
                    int(account.get("created_at") or remote_updated or ts),
                    remote_updated or ts,
                ),
            )
            account_id_map[account_id] = account_id
            imported_accounts += 1
        local_account_ids = {int(row["id"]) for row in conn.execute("SELECT id FROM matrix_accounts")}
        for platform in platforms:
            account_id = account_id_map.get(int(platform.get("account_id") or 0), int(platform.get("account_id") or 0))
            if account_id not in local_account_ids:
                continue
            token = normalize_platform(str(platform.get("platform") or ""))
            if not token:
                continue
            remote_updated = int(platform.get("updated_at") or platform.get("created_at") or ts)
            conn.execute(
                """
                INSERT INTO account_platforms(account_id, platform, handle, enabled, capability_status, login_status, last_checked_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, platform) DO UPDATE SET
                    handle = excluded.handle,
                    enabled = excluded.enabled,
                    capability_status = excluded.capability_status,
                    login_status = excluded.login_status,
                    last_checked_at = excluded.last_checked_at,
                    updated_at = excluded.updated_at
                """,
                (
                    account_id,
                    token,
                    str(platform.get("handle") or "").strip(),
                    1 if bool(platform.get("enabled", True)) else 0,
                    str(platform.get("capability_status") or "registered").strip() or "registered",
                    str(platform.get("login_status") or "unknown").strip() or "unknown",
                    platform.get("last_checked_at"),
                    int(platform.get("created_at") or remote_updated or ts),
                    remote_updated or ts,
                ),
            )
            imported_platforms += 1
        for profile in profiles:
            account_id = int(profile.get("account_id") or 0)
            if account_id <= 0 and profile.get("account_platform_id"):
                platform_row = conn.execute("SELECT account_id FROM account_platforms WHERE id = ?", (int(profile.get("account_platform_id") or 0),)).fetchone()
                account_id = int(platform_row["account_id"]) if platform_row else 0
            account_id = account_id_map.get(account_id, account_id)
            if account_id not in local_account_ids:
                continue
            account = conn.execute("SELECT account_key FROM matrix_accounts WHERE id = ?", (account_id,)).fetchone()
            profile_dir = str(profile.get("profile_dir") or profile_dir_for(str(account["account_key"] if account else account_id)))
            debug_port = int(profile.get("debug_port") or _profile_debug_port_from_seed(conn, str(account["account_key"] if account else account_id)))
            fingerprint = profile.get("fingerprint_json")
            if not isinstance(fingerprint, str):
                fingerprint = json.dumps(fingerprint or build_browser_fingerprint(str(account["account_key"] if account else account_id), "account"), ensure_ascii=False)
            remote_updated = int(profile.get("updated_at") or profile.get("created_at") or ts)
            conn.execute(
                """
                INSERT INTO browser_profiles(account_id, profile_dir, debug_port, fingerprint_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    profile_dir = excluded.profile_dir,
                    debug_port = excluded.debug_port,
                    fingerprint_json = excluded.fingerprint_json,
                    updated_at = excluded.updated_at
                """,
                (
                    account_id,
                    profile_dir,
                    debug_port,
                    fingerprint,
                    int(profile.get("created_at") or remote_updated or ts),
                    remote_updated or ts,
                ),
            )
            Path(profile_dir).mkdir(parents=True, exist_ok=True)
            imported_profiles += 1
    clear_supabase_read_cache()
    return {
        "ok": True,
        "accounts": imported_accounts,
        "skipped_accounts": skipped_accounts,
        "platforms": imported_platforms,
        "profiles": imported_profiles,
        "status": sync_status(),
    }


def _password_hash(password: str) -> str:
    return hashlib.sha256(f"gasgx-operator-auth:{password}".encode("utf-8")).hexdigest()


def ensure_operator_auth_seed() -> None:
    init_db()
    ts = now_ts()
    with connect() as conn:
        existing_permission_count = conn.execute("SELECT COUNT(*) AS count FROM operator_role_permissions").fetchone()["count"]
        for role_id, name in DEFAULT_ROLE_NAMES.items():
            role_exists = conn.execute("SELECT 1 FROM operator_roles WHERE id = ?", (role_id,)).fetchone() is not None
            conn.execute(
                """
                INSERT INTO operator_roles(id, name, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name, updated_at=excluded.updated_at
                """,
                (role_id, name, ts, ts),
            )
            should_seed_permissions = not role_exists or existing_permission_count == 0
            if should_seed_permissions:
                for permission in DEFAULT_ROLE_PERMISSIONS[role_id]:
                    conn.execute(
                        "INSERT OR IGNORE INTO operator_role_permissions(role_id, permission, created_at) VALUES (?, ?, ?)",
                        (role_id, permission, ts),
                    )
        conn.execute(
            """
            INSERT INTO operator_users(id, name, role_id, password_hash, created_at, updated_at)
            VALUES ('allen', 'Allen', 'super_admin', ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET role_id='super_admin', password_hash=excluded.password_hash, updated_at=excluded.updated_at
            """,
            (_password_hash(SUPER_ADMIN_PASSWORD), ts, ts),
        )
        default_users = [
            ("publisher", "发布员", "publisher"),
            ("material", "素材维护员", "material_manager"),
            ("analyst", "数据监控员", "data_monitor"),
        ]
        for user_id, name, role_id in default_users:
            conn.execute(
                """
                INSERT INTO operator_users(id, name, role_id, password_hash, created_at, updated_at)
                VALUES (?, ?, ?, '', ?, ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (user_id, name, role_id, ts, ts),
            )


def operator_auth_state(current_user_id: str = "allen", editing_role_id: str = "super_admin") -> dict[str, Any]:
    ensure_operator_auth_seed()
    with connect() as conn:
        roles = {
            row["id"]: {"name": row["name"], "permissions": []}
            for row in conn.execute("SELECT id, name FROM operator_roles ORDER BY created_at, id")
        }
        for row in conn.execute("SELECT role_id, permission FROM operator_role_permissions ORDER BY permission"):
            if row["role_id"] in roles:
                roles[row["role_id"]]["permissions"].append(row["permission"])
        users = [
            {"id": row["id"], "name": row["name"], "roleId": row["role_id"]}
            for row in conn.execute("SELECT id, name, role_id FROM operator_users ORDER BY created_at, id")
        ]
    if not any(user["id"] == current_user_id for user in users):
        current_user_id = "allen"
    if editing_role_id not in roles:
        editing_role_id = "super_admin"
    return {
        "currentUserId": current_user_id,
        "editingRoleId": editing_role_id,
        "roles": roles,
        "users": users,
        "features": FEATURE_ENTRIES,
    }


def login_operator_user(user_id: str, password: str) -> dict[str, Any]:
    ensure_operator_auth_seed()
    with connect() as conn:
        row = conn.execute("SELECT id, password_hash FROM operator_users WHERE id = ?", (user_id,)).fetchone()
    if row is None:
        raise ValueError("operator user not found")
    expected = str(row["password_hash"] or "")
    if expected and not hmac.compare_digest(expected, _password_hash(password)):
        raise ValueError("invalid password")
    if not expected and not str(password or "").strip():
        raise ValueError("password is required")
    return operator_auth_state(current_user_id=user_id)


def create_operator_user(name: str, role_id: str, password: str = "") -> dict[str, Any]:
    name = str(name or "").strip()
    role_id = str(role_id or "").strip()
    if not name:
        raise ValueError("name is required")
    ts = now_ts()
    user_id = f"user-{ts}"
    with connect() as conn:
        if conn.execute("SELECT 1 FROM operator_roles WHERE id = ?", (role_id,)).fetchone() is None:
            raise ValueError("role not found")
        conn.execute(
            "INSERT INTO operator_users(id, name, role_id, password_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, name, role_id, _password_hash(password) if password else "", ts, ts),
        )
    return operator_auth_state(current_user_id="allen")


def update_operator_user_role(user_id: str, role_id: str) -> dict[str, Any]:
    if user_id == "allen":
        raise ValueError("super admin role cannot be changed")
    ts = now_ts()
    with connect() as conn:
        if conn.execute("SELECT 1 FROM operator_roles WHERE id = ?", (role_id,)).fetchone() is None:
            raise ValueError("role not found")
        conn.execute("UPDATE operator_users SET role_id = ?, updated_at = ? WHERE id = ?", (role_id, ts, user_id))
    return operator_auth_state(current_user_id="allen")


def update_operator_user_password(user_id: str, password: str) -> dict[str, Any]:
    if user_id == "allen":
        raise ValueError("super admin password is fixed by system configuration")
    password = str(password or "").strip()
    if not password:
        raise ValueError("password is required")
    ts = now_ts()
    with connect() as conn:
        row = conn.execute("SELECT id FROM operator_users WHERE id = ?", (user_id,)).fetchone()
        if row is None:
            raise ValueError("operator user not found")
        conn.execute(
            "UPDATE operator_users SET password_hash = ?, updated_at = ? WHERE id = ?",
            (_password_hash(password), ts, user_id),
        )
    return operator_auth_state(current_user_id="allen")


def create_operator_role(name: str) -> dict[str, Any]:
    name = str(name or "").strip()
    if not name:
        raise ValueError("role name is required")
    ts = now_ts()
    role_id = f"role-{ts}"
    with connect() as conn:
        conn.execute("INSERT INTO operator_roles(id, name, created_at, updated_at) VALUES (?, ?, ?, ?)", (role_id, name, ts, ts))
        for permission in ["overview", "help-center"]:
            conn.execute("INSERT INTO operator_role_permissions(role_id, permission, created_at) VALUES (?, ?, ?)", (role_id, permission, ts))
    return operator_auth_state(editing_role_id=role_id)


def save_operator_role_permissions(role_id: str, permissions: list[str]) -> dict[str, Any]:
    if role_id == "super_admin":
        permissions = [item["id"] for item in FEATURE_ENTRIES]
    allowed = {item["id"] for item in FEATURE_ENTRIES}
    next_permissions = [item for item in permissions if item in allowed]
    ts = now_ts()
    with connect() as conn:
        if conn.execute("SELECT 1 FROM operator_roles WHERE id = ?", (role_id,)).fetchone() is None:
            raise ValueError("role not found")
        conn.execute("DELETE FROM operator_role_permissions WHERE role_id = ?", (role_id,))
        for permission in next_permissions:
            conn.execute("INSERT INTO operator_role_permissions(role_id, permission, created_at) VALUES (?, ?, ?)", (role_id, permission, ts))
    return operator_auth_state(editing_role_id=role_id)


def _config_root() -> Path:
    configured = get_paths().repo_root / "config" / "video_matrix"
    if configured.exists():
        return configured
    return Path(__file__).resolve().parents[2] / "config" / "video_matrix"


def _app_setting(key: str, default: Any = None) -> Any:
    if brand_database_backend() != "supabase":
        return default
    with _SUPABASE_READ_CACHE_LOCK:
        if key in _SUPABASE_APP_SETTINGS_CACHE:
            return copy.deepcopy(_SUPABASE_APP_SETTINGS_CACHE[key])
    row = _brand_supabase().select_one("app_settings", filters={"setting_key": key})
    parsed = _json_payload(row.get("payload_json") if row else None, default)
    with _SUPABASE_READ_CACHE_LOCK:
        _SUPABASE_APP_SETTINGS_CACHE[key] = copy.deepcopy(parsed)
    return copy.deepcopy(parsed)


def _save_app_setting(key: str, payload: Any) -> dict[str, Any]:
    client = _brand_supabase()
    data = {"setting_key": key, "payload_json": payload, "updated_at": now_ts()}
    if hasattr(client, "upsert"):
        _invalidate_supabase_app_settings_cache(key)
        return client.upsert("app_settings", data, on_conflict="setting_key")
    if hasattr(client, "insert"):
        _invalidate_supabase_app_settings_cache(key)
        return client.insert("app_settings", data)
    return data


def load_distribution_settings_db() -> dict[str, Any]:
    if brand_database_backend() != "supabase":
        return load_distribution_settings()
    payload = _app_setting("distribution_settings")
    if isinstance(payload, dict):
        # Normalize historical/remote payload shapes before returning to UI.
        settings = save_local_distribution_settings(payload)
        if settings != payload:
            _save_app_setting("distribution_settings", settings)
        return settings
    settings = load_distribution_settings()
    _save_app_setting("distribution_settings", settings)
    return settings


def save_distribution_settings_db(payload: dict[str, Any]) -> dict[str, Any]:
    if brand_database_backend() != "supabase":
        settings = save_local_distribution_settings(payload)
        ensure_database()
        with connect() as conn:
            _enqueue_sync_payload(
                conn,
                "app_settings",
                "upsert",
                {"setting_key": "distribution_settings", "payload_json": settings, "updated_at": now_ts()},
                entity_id="distribution_settings",
            )
        return settings
    settings = save_local_distribution_settings(payload)
    _save_app_setting("distribution_settings", settings)
    return settings


def list_operator_wechats() -> list[str]:
    settings = load_distribution_settings_db()
    operators = settings.get("common", {}).get("operator_wechats")
    if not isinstance(operators, list):
        return ["aamecc", "aalbcc"]
    values = []
    for item in operators:
        value = str(item or "").strip()
        if value and value not in values:
            values.append(value)
    return values or ["aamecc", "aalbcc"]


def add_operator_wechat(value: str) -> dict[str, Any]:
    operator = str(value or "").strip()
    if not operator:
        raise ValueError("operator_wechat is required")
    settings = load_distribution_settings_db()
    common = settings.setdefault("common", {})
    operators = list_operator_wechats()
    if operator not in operators:
        operators.append(operator)
    common["operator_wechats"] = operators
    save_distribution_settings_db(settings)
    return {"operator_wechat": operator, "items": operators}


def load_wechat_publish_settings_db() -> dict[str, Any]:
    settings = load_distribution_settings_db()
    common = settings["common"]
    platform = settings["platforms"].get("wechat", {})
    def resolve(value: Any, fallback: Any, *, blank_is_fallback: bool = True) -> Any:
        if value is None:
            return fallback
        text = str(value).strip().lower()
        if text == "inherit" or (blank_is_fallback and text == ""):
            return fallback
        return value
    return {
        **platform,
        "material_dir": common.get("material_dir", ""),
        "publish_mode": common.get("publish_mode", "publish") if platform.get("publish_mode") == "inherit" else platform.get("publish_mode", "publish"),
        "topics": common.get("topics", ""),
        "content_type": resolve(platform.get("content_type"), common.get("wechat_content_type", "short_video")),
        "visibility": resolve(platform.get("visibility"), common.get("wechat_visibility", "public")),
        "comment_permission": resolve(platform.get("comment_permission"), common.get("wechat_comment_permission", "public")),
        "collection_name": resolve(platform.get("collection_name"), common.get("wechat_collection_name", "GasGx"), blank_is_fallback=False),
        "declare_original": resolve(platform.get("declare_original"), common.get("wechat_declare_original", False)),
        "short_title": resolve(platform.get("short_title"), common.get("wechat_short_title", "GasGx燃气发电挖矿")),
        "location": resolve(platform.get("location"), common.get("wechat_location", ""), blank_is_fallback=False),
        "caption": resolve(platform.get("caption"), common.get("wechat_caption", "")),
        "upload_timeout": platform.get("upload_timeout") or common.get("upload_timeout", 60),
    }


def save_wechat_publish_settings_db(payload: dict[str, Any]) -> dict[str, Any]:
    settings = load_distribution_settings_db()
    settings["common"].update(
        {
            "material_dir": payload.get("material_dir", settings["common"].get("material_dir")),
            "publish_mode": payload.get("publish_mode", settings["common"].get("publish_mode")),
            "topics": payload.get("topics", settings["common"].get("topics")),
            "upload_timeout": payload.get("upload_timeout", settings["common"].get("upload_timeout")),
        }
    )
    settings["platforms"].setdefault("wechat", {}).update(
        {
            "caption": payload.get("caption", settings["platforms"].get("wechat", {}).get("caption")),
            "collection_name": payload.get("collection_name", settings["platforms"].get("wechat", {}).get("collection_name")),
            "declare_original": payload.get("declare_original", settings["platforms"].get("wechat", {}).get("declare_original")),
            "short_title": payload.get("short_title", settings["platforms"].get("wechat", {}).get("short_title")),
            "location": payload.get("location", settings["platforms"].get("wechat", {}).get("location")),
            "publish_mode": "inherit",
            "upload_timeout": payload.get("upload_timeout", settings["platforms"].get("wechat", {}).get("upload_timeout")),
        }
    )
    save_distribution_settings_db(settings)
    return load_wechat_publish_settings_db()


def _seed_analytics_items() -> list[tuple[str, str, dict[str, Any]]]:
    return [
        ("overview", "new_accounts", {"label": "新增账号数", "value": 2, "change": "+100%", "trend": "up"}),
        ("overview", "works", {"label": "累计作品总量", "value": 186, "change": "+18.6%", "trend": "up"}),
        ("overview", "exposure", {"label": "累计总曝光", "value": "68.4万", "change": "+24.8%", "trend": "up"}),
        ("overview", "followers", {"label": "矩阵总粉丝", "value": "4.8万", "change": "+9.7%", "trend": "up"}),
        ("account_rank", "gasgx-green", {"row": ["GasGx小绿", "视频号", "正常", "86,200", "18,600", "12,480", "+860", "42.1%", "8.6%", 12, "爆款账号", ""]}),
        ("account_rank", "gasgx-yellow", {"row": ["GasGx小黄", "抖音", "正常", "72,100", "16,900", "10,220", "+640", "37.8%", "7.9%", 10, "稳定账号", ""]}),
        ("account_rank", "case-xhs", {"row": ["发电机组案例", "小红书", "低流量", "18,400", "3,420", "3,180", "+92", "28.4%", "4.1%", 5, "潜力账号", "低流量"]}),
        ("content_top", "field-engine", {"title": "燃气发动机组现场并机", "value": "8.6万", "tag": "爆款"}),
        ("content_top", "oilfield-power", {"title": "油气田自发电改造案例", "value": "6.9万", "tag": "爆款"}),
        ("traffic", "recommend", {"label": "推荐流量", "value": "54%"}),
        ("traffic", "search", {"label": "搜索流量", "value": "18%"}),
        ("traffic", "home", {"label": "主页流量", "value": "12%"}),
        ("conversion", "profile", {"label": "主页访问量", "value": "17,860"}),
        ("conversion", "dm", {"label": "私信咨询量", "value": "1,286"}),
        ("conversion", "leads", {"label": "有效线索数", "value": "426"}),
        ("operation", "publish", {"label": "计划发布量 VS 实际发布量", "value": 92}),
        ("operation", "copy", {"label": "周期文案产出数", "value": 84}),
        ("operation", "edit", {"label": "剪辑产出数", "value": 78}),
        ("risk", "violation", {"text": "违规作品 1 条，待整改"}),
        ("risk", "drop", {"text": "1 个账号播放断崖下跌"}),
        ("risk", "sleep", {"text": "1 个账号长期断更休眠"}),
    ]


def list_analytics_items() -> dict[str, list[dict[str, Any]]]:
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        if hasattr(client, "select_where"):
            real_items = _build_real_analytics_items()
            if real_items:
                return real_items
    if brand_database_backend() == "supabase":
        if _supabase_read_cache_peek("analytics_items"):
            return _supabase_read_cache_get("analytics_items")
        rows = _brand_supabase().select("analytics_items", order="sort_order.asc,id.asc")
        items = [(str(row["section"]), str(row["item_key"]), _json_payload(row.get("payload_json"), {})) for row in rows]
    else:
        items = _seed_analytics_items()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for section, key, payload in items:
        item = dict(payload or {})
        item.setdefault("key", key)
        grouped.setdefault(section, []).append(item)
    if brand_database_backend() == "supabase":
        return _cache_supabase_read("analytics_items", grouped)
    return copy.deepcopy(grouped)


def _insert_seed_item(table: str, key_field: str, key: str, payload: dict[str, Any]) -> bool:
    client = _brand_supabase()
    if client.select_one(table, filters={key_field: key}) is not None:
        return False
    data = dict(payload)
    data[key_field] = key
    client.insert(table, data)
    return True


def initialize_system() -> dict[str, Any]:
    if brand_database_backend() != "supabase":
        return {"ok": False, "backend": "sqlite", "error": "system initialization is Supabase-only"}
    client = _brand_supabase()
    ts = now_ts()
    inserted: dict[str, int] = {}
    skipped: dict[str, int] = {}

    def mark(name: str, did_insert: bool) -> None:
        target = inserted if did_insert else skipped
        target[name] = target.get(name, 0) + 1

    mark("distribution_settings", _insert_seed_item("app_settings", "setting_key", "distribution_settings", {"payload_json": load_distribution_settings(), "updated_at": ts}))
    config_dir = _config_root()
    settings = ProjectSettings.from_file(config_dir / "defaults.json")
    bgm_path = config_dir / "bgm_library.json"
    video_state = {
        "settings": {
            "project_name": settings.project_name,
            "source_root": str(settings.source_root),
            "library_root": str(settings.library_root),
            "output_root": str(settings.output_root),
            "output_count": settings.output_count,
            "target_width": settings.target_width,
            "target_height": settings.target_height,
            "recent_limits": settings.recent_limits,
            "material_categories": settings.material_categories,
        },
        "ui_state": load_ui_state(config_dir / "ui_state.json"),
        "templates": load_templates(config_dir / "templates.json"),
        "cover_templates": load_cover_templates(config_dir / "cover_templates.json"),
        "bgm_library": _json_payload(bgm_path.read_text(encoding="utf-8"), {}) if bgm_path.exists() else {},
    }
    mark("video_matrix_state", _insert_seed_item("app_settings", "setting_key", "video_matrix_state", {"payload_json": video_state, "updated_at": ts}))

    for index, (section, key, payload) in enumerate(_seed_analytics_items(), start=1):
        exists = client.select_one("analytics_items", filters={"section": section, "item_key": key})
        if exists is None:
            client.insert("analytics_items", {"section": section, "item_key": key, "payload_json": payload, "source": "seed", "sort_order": index, "created_at": ts, "updated_at": ts})
            mark("analytics_items", True)
        else:
            mark("analytics_items", False)

    existing = client.select_one("app_seed_runs", filters={"seed_version": SEED_VERSION})
    if existing is None:
        client.insert("app_seed_runs", {"seed_version": SEED_VERSION, "summary_json": {"inserted": inserted, "skipped": skipped}, "applied_at": ts})
        mark("seed_runs", True)
    else:
        mark("seed_runs", False)
    _invalidate_supabase_read_cache("analytics_items", "dashboard_summary")
    _invalidate_supabase_app_settings_cache("distribution_settings", "video_matrix_state")
    return {"ok": True, "backend": "supabase", "seed_version": SEED_VERSION, "inserted": inserted, "skipped": skipped}


def load_brand_settings() -> dict[str, Any]:
    if brand_database_backend() == "supabase":
        if _supabase_read_cache_peek("brand_settings"):
            return _supabase_read_cache_get("brand_settings")
        row = _brand_supabase().select_one("brand_settings", filters={"id": 1})
        if row is None:
            ts = now_ts()
            row = _brand_supabase().upsert(
                "brand_settings",
                {
                    "id": 1,
                    "name": "GasGx",
                    "slogan": "Video Distribution",
                    "logo_asset_path": "",
                    "primary_color": "#5dd62c",
                    "theme_id": "gasgx-green",
                    "default_account_prefix": "GasGx",
                    "created_at": ts,
                    "updated_at": ts,
                },
                on_conflict="id",
            )
        _supabase_read_cache_set("brand_settings", row)
        return copy.deepcopy(row)
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM brand_settings WHERE id = 1").fetchone()
        if row is None:
            ts = now_ts()
            conn.execute(
                """
                INSERT INTO brand_settings(id, name, slogan, logo_asset_path, primary_color, theme_id, default_account_prefix, created_at, updated_at)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("GasGx", "Video Distribution", "", "#5dd62c", "gasgx-green", "GasGx", ts, ts),
            )
            row = conn.execute("SELECT * FROM brand_settings WHERE id = 1").fetchone()
        return dict_from_row(row)


def public_brand_settings(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    if settings is None and brand_database_backend() == "supabase":
        if _supabase_read_cache_peek("brand_settings_public"):
            return _supabase_read_cache_get("brand_settings_public")
        rows = _brand_supabase().select("brand_settings", filters={"id": 1}, columns=BRAND_PUBLIC_SETTING_COLUMNS)
        if rows:
            data = dict(rows[0])
            logo_rows = _brand_supabase().select_where(
                "brand_settings",
                params={"id": "eq.1", "logo_asset_path": "not.like.data:%"},
                columns="logo_asset_path",
            )
            data_url_rows = _brand_supabase().select_where(
                "brand_settings",
                params={"id": "eq.1", "logo_asset_path": "like.data:%"},
                columns="id",
            )
            data["logo_asset_path"] = str((logo_rows[0] if logo_rows else {}).get("logo_asset_path") or "")
            data["logo_asset_omitted"] = bool(data_url_rows)
            data["logo_asset_chars"] = 0
            return _cache_supabase_read("brand_settings_public", data)
    data = copy.deepcopy(settings if settings is not None else load_brand_settings())
    logo = str(data.get("logo_asset_path") or "")
    if logo.startswith("data:") and len(logo) > BRAND_INLINE_ASSET_MAX_CHARS:
        data["logo_asset_path"] = ""
        data["logo_asset_omitted"] = True
        data["logo_asset_chars"] = len(logo)
    else:
        data["logo_asset_omitted"] = False
        data["logo_asset_chars"] = len(logo)
    return data


def _cache_supabase_read(key: str, value: Any) -> Any:
    _supabase_read_cache_set(key, value)
    return copy.deepcopy(value)


def save_brand_settings(payload: dict[str, Any]) -> dict[str, Any]:
    if brand_database_backend() == "supabase":
        current = load_brand_settings()
        next_data = {
            "name": str(payload.get("name") or current.get("name") or "GasGx").strip() or "GasGx",
            "slogan": str(payload.get("slogan") or current.get("slogan") or "Video Distribution").strip() or "Video Distribution",
            "logo_asset_path": str(payload.get("logo_asset_path") or current.get("logo_asset_path") or "").strip(),
            "primary_color": str(payload.get("primary_color") or current.get("primary_color") or "#5dd62c").strip() or "#5dd62c",
            "theme_id": str(payload.get("theme_id") or current.get("theme_id") or "gasgx-green").strip() or "gasgx-green",
            "default_account_prefix": str(payload.get("default_account_prefix") or current.get("default_account_prefix") or "GasGx").strip() or "GasGx",
            "updated_at": now_ts(),
        }
        row = _brand_supabase().update("brand_settings", next_data, filters={"id": 1})
        _invalidate_supabase_read_cache("brand_settings", "brand_settings_public")
        return row
    ensure_database()
    current = load_brand_settings()
    next_data = {
        "name": str(payload.get("name") or current.get("name") or "GasGx").strip() or "GasGx",
        "slogan": str(payload.get("slogan") or current.get("slogan") or "Video Distribution").strip() or "Video Distribution",
        "logo_asset_path": str(payload.get("logo_asset_path") or current.get("logo_asset_path") or "").strip(),
        "primary_color": str(payload.get("primary_color") or current.get("primary_color") or "#5dd62c").strip() or "#5dd62c",
        "theme_id": str(payload.get("theme_id") or current.get("theme_id") or "gasgx-green").strip() or "gasgx-green",
        "default_account_prefix": str(payload.get("default_account_prefix") or current.get("default_account_prefix") or "GasGx").strip() or "GasGx",
    }
    with connect() as conn:
        ts = now_ts()
        conn.execute(
            """
            UPDATE brand_settings
            SET name = ?, slogan = ?, logo_asset_path = ?, primary_color = ?, theme_id = ?, default_account_prefix = ?, updated_at = ?
            WHERE id = 1
            """,
            (
                next_data["name"],
                next_data["slogan"],
                next_data["logo_asset_path"],
                next_data["primary_color"],
                next_data["theme_id"],
                next_data["default_account_prefix"],
                ts,
            ),
        )
        _enqueue_sync_row(conn, "brand_settings", "id = 1", ())
    return load_brand_settings()


def _normalize_ai_platform(platform: str) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(platform or "").strip())
    aliases = {"enterprise-wechat": "wecom", "wechat-work": "wecom", "feishu": "lark"}
    token = aliases.get(token, token)
    if token not in AI_ROBOT_PLATFORMS:
        raise ValueError("platform must be wecom, dingtalk, lark, telegram, or whatsapp")
    return token


def _public_ai_config(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["enabled"] = bool(data.get("enabled"))
    data["has_webhook_secret"] = bool(data.pop("webhook_secret", ""))
    data["has_signing_secret"] = bool(data.pop("signing_secret", ""))
    return data


def _private_ai_config(platform: str) -> dict[str, Any] | None:
    token = _normalize_ai_platform(platform)
    if brand_database_backend() == "supabase":
        return _brand_supabase().select_one("ai_robot_configs", filters={"platform": token})
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM ai_robot_configs WHERE platform = ?", (token,)).fetchone()
        return dict_from_row(row) if row else None


def list_ai_robot_configs() -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        if _supabase_read_cache_peek("ai_robot_configs"):
            return _supabase_read_cache_get("ai_robot_configs")
        rows = {
            row["platform"]: _public_ai_config(row)
            for row in _brand_supabase().select("ai_robot_configs", order="platform.asc")
        }
        result = [_default_ai_robot_config(platform, rows.get(platform)) for platform in sorted(AI_ROBOT_PLATFORMS)]
        return _cache_supabase_read("ai_robot_configs", result)
    ensure_database()
    with connect() as conn:
        rows = {
            row["platform"]: _public_ai_config(dict_from_row(row))
            for row in conn.execute("SELECT * FROM ai_robot_configs ORDER BY platform")
        }
    return [_default_ai_robot_config(platform, rows.get(platform)) for platform in sorted(AI_ROBOT_PLATFORMS)]


def _default_ai_robot_config(platform: str, row: dict[str, Any] | None = None) -> dict[str, Any]:
    return row or {
        "id": None,
        "platform": platform,
        "enabled": False,
        "bot_name": "",
        "webhook_url": "",
        "target_id": "",
        "created_at": None,
        "updated_at": None,
        "has_webhook_secret": False,
        "has_signing_secret": False,
    }


def get_ai_robot_config(platform: str) -> dict[str, Any] | None:
    token = _normalize_ai_platform(platform)
    if brand_database_backend() == "supabase":
        row = _brand_supabase().select_one("ai_robot_configs", filters={"platform": token})
        return _public_ai_config(row) if row else None
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM ai_robot_configs WHERE platform = ?", (token,)).fetchone()
        return _public_ai_config(dict_from_row(row)) if row else None


def delete_ai_robot_config(platform: str) -> bool:
    token = _normalize_ai_platform(platform)
    if brand_database_backend() == "supabase":
        ok = _brand_supabase().delete("ai_robot_configs", filters={"platform": token})
        _invalidate_supabase_read_cache("ai_robot_configs")
        return ok
    ensure_database()
    with connect() as conn:
        cursor = conn.execute("DELETE FROM ai_robot_configs WHERE platform = ?", (token,))
        deleted = cursor.rowcount > 0
        if deleted:
            _enqueue_sync_delete(conn, "ai_robot_configs", {"platform": token}, entity_id=token)
        return deleted


def save_ai_robot_config(platform: str, payload: dict[str, Any]) -> dict[str, Any]:
    token = _normalize_ai_platform(platform)
    ts = now_ts()
    fields = {
        "enabled": 1 if payload.get("enabled") else 0,
        "bot_name": str(payload.get("bot_name") or "").strip(),
        "webhook_url": str(payload.get("webhook_url") or "").strip(),
        "webhook_secret": str(payload.get("webhook_secret") or "").strip(),
        "signing_secret": str(payload.get("signing_secret") or "").strip(),
        "target_id": str(payload.get("target_id") or "").strip(),
    }
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        existing = client.select_one("ai_robot_configs", filters={"platform": token})
        data = {
            "platform": token,
            "enabled": fields["enabled"],
            "bot_name": fields["bot_name"],
            "webhook_url": fields["webhook_url"],
            "webhook_secret": fields["webhook_secret"] or str((existing or {}).get("webhook_secret") or ""),
            "signing_secret": fields["signing_secret"] or str((existing or {}).get("signing_secret") or ""),
            "target_id": fields["target_id"],
            "created_at": int((existing or {}).get("created_at") or ts),
            "updated_at": ts,
        }
        row = client.upsert("ai_robot_configs", data, on_conflict="platform")
        _invalidate_supabase_read_cache("ai_robot_configs")
        return _public_ai_config(row)
    with connect() as conn:
        existing = conn.execute("SELECT * FROM ai_robot_configs WHERE platform = ?", (token,)).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO ai_robot_configs(platform, enabled, bot_name, webhook_url, webhook_secret, signing_secret, target_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token,
                    fields["enabled"],
                    fields["bot_name"],
                    fields["webhook_url"],
                    fields["webhook_secret"],
                    fields["signing_secret"],
                    fields["target_id"],
                    ts,
                    ts,
                ),
            )
        else:
            webhook_secret = fields["webhook_secret"] or str(existing["webhook_secret"] or "")
            signing_secret = fields["signing_secret"] or str(existing["signing_secret"] or "")
            conn.execute(
                """
                UPDATE ai_robot_configs
                SET enabled = ?, bot_name = ?, webhook_url = ?, webhook_secret = ?, signing_secret = ?, target_id = ?, updated_at = ?
                WHERE platform = ?
                """,
                (
                    fields["enabled"],
                    fields["bot_name"],
                    fields["webhook_url"],
                    webhook_secret,
                    signing_secret,
                    fields["target_id"],
                    ts,
                    token,
                ),
            )
        _enqueue_sync_row(conn, "ai_robot_configs", "platform = ?", (token,), entity_id=token)
    return get_ai_robot_config(token) or {}


def verify_ai_robot_webhook(platform: str, body: bytes, signature: str) -> dict[str, Any]:
    token = _normalize_ai_platform(platform)
    if brand_database_backend() == "supabase":
        row = _brand_supabase().select_one("ai_robot_configs", filters={"platform": token})
        signing_secret = str((row or {}).get("signing_secret") or "")
    else:
        ensure_database()
        with connect() as conn:
            row = conn.execute("SELECT signing_secret FROM ai_robot_configs WHERE platform = ?", (token,)).fetchone()
        signing_secret = str(row["signing_secret"] or "") if row is not None else ""
    if not signing_secret:
        raise ValueError("signing_secret is not configured")
    digest = hmac.new(signing_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    expected = f"sha256={digest}"
    ok = hmac.compare_digest(signature, digest) or hmac.compare_digest(signature, expected)
    if not ok:
        raise ValueError("invalid webhook signature")
    return {"ok": True, "platform": token}


def _verify_ai_robot_webhook_sqlite(platform: str, body: bytes, signature: str) -> dict[str, Any]:
    token = _normalize_ai_platform(platform)
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT signing_secret FROM ai_robot_configs WHERE platform = ?", (token,)).fetchone()
    if row is None or not str(row["signing_secret"] or ""):
        raise ValueError("signing_secret is not configured")
    digest = hmac.new(str(row["signing_secret"]).encode("utf-8"), body, hashlib.sha256).hexdigest()
    expected = f"sha256={digest}"
    ok = hmac.compare_digest(signature, digest) or hmac.compare_digest(signature, expected)
    if not ok:
        raise ValueError("invalid webhook signature")
    return {"ok": True, "platform": token}


def enqueue_ai_robot_message(platform: str, payload: dict[str, Any], *, test: bool = False) -> dict[str, Any]:
    token = _normalize_ai_platform(platform)
    ensure_database()
    config = get_ai_robot_config(token)
    supported = bool(config and config.get("enabled") and config.get("webhook_url"))
    status = "pending" if supported else "unsupported"
    summary = "queued for robot sender" if supported else f"{token} robot is not enabled or missing webhook_url"
    message = dict(payload or {})
    if test:
        message.setdefault("text", "GasGx AI robot test message")
        message["test"] = True
    ts = now_ts()
    if brand_database_backend() == "supabase":
        inserted = _brand_supabase().insert(
            "ai_robot_messages",
            {
                "platform": token,
                "message_type": str(message.get("message_type") or "text"),
                "payload_json": message,
                "status": status,
                "summary": summary,
                "retry_count": 0,
                "created_at": ts,
                "updated_at": ts,
            },
        )
        _invalidate_supabase_read_cache("ai_robot_messages")
        return inserted
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO ai_robot_messages(platform, message_type, payload_json, status, summary, retry_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                token,
                str(message.get("message_type") or "text"),
                json.dumps(message, ensure_ascii=False),
                status,
                summary,
                0,
                ts,
                ts,
            ),
        )
        message_id = int(cursor.lastrowid)
        _enqueue_sync_row(conn, "ai_robot_messages", "id = ?", (message_id,))
    return get_ai_robot_message(message_id) or {}


def get_ai_robot_message(message_id: int) -> dict[str, Any] | None:
    if brand_database_backend() == "supabase":
        return _brand_supabase().select_one("ai_robot_messages", filters={"id": message_id})
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM ai_robot_messages WHERE id = ?", (message_id,)).fetchone()
        return dict_from_row(row) if row else None


def list_ai_robot_messages() -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        if _supabase_read_cache_peek("ai_robot_messages"):
            return _supabase_read_cache_get("ai_robot_messages")
        rows = _brand_supabase().select("ai_robot_messages", order="id.desc")
        _supabase_read_cache_set("ai_robot_messages", rows)
        return copy.deepcopy(rows)
    ensure_database()
    with connect() as conn:
        return [dict_from_row(row) for row in conn.execute("SELECT * FROM ai_robot_messages ORDER BY id DESC LIMIT 100")]


def run_ai_robot_sender_worker(*, limit: int = 10) -> dict[str, Any]:
    limit = max(1, int(limit or 10))
    escalations = process_notification_escalations(limit=limit)
    messages = _claim_ai_robot_messages(limit)
    sent = 0
    failed = 0
    results: list[dict[str, Any]] = []
    for message in messages:
        try:
            _send_ai_robot_message(message)
        except Exception as exc:
            failed += 1
            updated = _mark_ai_robot_message_failed(message, str(exc))
        else:
            sent += 1
            updated = _mark_ai_robot_message_sent(message)
        results.append(updated)
    return {"ok": failed == 0, "claimed": len(messages), "sent": sent, "failed": failed, "escalations": escalations, "messages": results}


def send_ai_robot_message_now(message: dict[str, Any]) -> dict[str, Any]:
    status = str((message or {}).get("status") or "")
    if status not in {"pending", "retry", "sending"}:
        return message
    claimed = _mark_ai_robot_message_sending(message)
    try:
        _send_ai_robot_message(claimed)
    except Exception as exc:
        return _mark_ai_robot_message_failed(claimed, str(exc))
    return _mark_ai_robot_message_sent(claimed)


def save_notification_route(event_type: str, platform: str, enabled: bool) -> dict[str, Any]:
    event = str(event_type or "").strip()
    if event not in NOTIFICATION_EVENT_TYPES:
        raise ValueError("unsupported notification event_type")
    token = _normalize_ai_platform(platform)
    if token not in NOTIFICATION_PLATFORMS:
        raise ValueError("notification platform must be telegram, dingtalk, or wecom")
    ts = now_ts()
    if brand_database_backend() == "supabase":
        try:
            row = _brand_supabase().upsert(
                "notification_routes",
                {"event_type": event, "platform": token, "enabled": 1 if enabled else 0, "created_at": ts, "updated_at": ts},
                on_conflict="event_type,platform",
            )
        except SupabaseError as exc:
            return {"event_type": event, "platform": token, "enabled": bool(enabled), "ok": False, "storage_unavailable": True, "error": str(exc)}
        _invalidate_supabase_read_cache("notification_routes")
        return row
    ensure_database()
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO notification_routes(event_type, platform, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(event_type, platform) DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at
            """,
            (event, token, 1 if enabled else 0, ts, ts),
        )
        _enqueue_sync_row(conn, "notification_routes", "event_type = ? AND platform = ?", (event, token))
    return {"event_type": event, "platform": token, "enabled": bool(enabled)}


def list_notification_routes() -> list[dict[str, Any]]:
    defaults = [
        {**NOTIFICATION_EVENT_META[event], "platform": platform, "enabled": False}
        for event in sorted(NOTIFICATION_EVENT_TYPES)
        for platform in sorted(NOTIFICATION_PLATFORMS)
    ]
    if brand_database_backend() == "supabase":
        if _supabase_read_cache_peek("notification_routes"):
            return _supabase_read_cache_get("notification_routes")
        try:
            rows = {
                (row["event_type"], row["platform"]): {**row, "enabled": bool(row.get("enabled"))}
                for row in _brand_supabase().select("notification_routes", order="event_type.asc,platform.asc")
            }
        except SupabaseError:
            rows = {}
        merged = [{**item, **rows.get((item["event_type"], item["platform"]), {})} for item in defaults]
        return _cache_supabase_read("notification_routes", merged)
    ensure_database()
    with connect() as conn:
        rows = {
            (row["event_type"], row["platform"]): {**dict_from_row(row), "enabled": bool(row["enabled"])}
            for row in conn.execute("SELECT * FROM notification_routes ORDER BY event_type, platform")
        }
    return [{**item, **rows.get((item["event_type"], item["platform"]), {})} for item in defaults]


def _enabled_notification_platforms(event_type: str) -> list[str]:
    routes = [item for item in list_notification_routes() if item["event_type"] == event_type and bool(item.get("enabled"))]
    configs = {item["platform"]: item for item in list_ai_robot_configs()}
    return [
        str(item["platform"])
        for item in routes
        if bool(configs.get(str(item["platform"]), {}).get("enabled"))
        and bool(configs.get(str(item["platform"]), {}).get("webhook_url"))
    ]


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    raw = _json_payload(value, [] if isinstance(value, str) else value)
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, str):
        return [item.strip() for item in raw.split(",") if item.strip()]
    return []


def _normalize_notification_platforms(value: Any) -> list[str]:
    result: list[str] = []
    for item in _json_list(value):
        try:
            token = _normalize_ai_platform(item)
        except ValueError:
            continue
        if token in NOTIFICATION_PLATFORMS and token not in result:
            result.append(token)
    return result


def _default_notification_cooldown_seconds(event_type: str, severity: str) -> int:
    if event_type == "ops_summary":
        return 0
    if event_type == "wechat_login_qr":
        return LOGIN_QR_NOTIFY_COOLDOWN_SECONDS
    if severity == "critical":
        return 300
    return 600


def _default_notification_escalation_minutes(severity: str) -> int:
    if severity == "critical":
        return 10
    if severity == "blocking":
        return 20
    return 0


def _default_notification_policy(event_type: str) -> dict[str, Any]:
    meta = NOTIFICATION_EVENT_META[event_type]
    severity = str(meta.get("default_severity") or "info")
    escalation_minutes = _default_notification_escalation_minutes(severity)
    return {
        "event_type": event_type,
        "severity": severity,
        "platform": "",
        "account_scope": "",
        "enabled": True,
        "target_platforms": [],
        "cooldown_seconds": _default_notification_cooldown_seconds(event_type, severity),
        "quiet_start": "",
        "quiet_end": "",
        "escalation_enabled": escalation_minutes > 0,
        "escalation_minutes": escalation_minutes,
        "escalation_platforms": [],
        "owner_hint": "",
        "label": meta.get("label") or event_type,
        "source": meta.get("source") or "",
        "default_severity": severity,
    }


def _normalize_notification_policy(row: dict[str, Any]) -> dict[str, Any]:
    event = str(row.get("event_type") or "").strip()
    if event not in NOTIFICATION_EVENT_TYPES:
        event = "action_required"
    base = _default_notification_policy(event)
    severity = str(row.get("severity") or base["severity"]).strip().lower()
    if severity not in NOTIFICATION_POLICY_SEVERITY_ORDER:
        severity = base["severity"]
    return {
        **base,
        "id": row.get("id"),
        "event_type": event,
        "severity": severity,
        "platform": str(row.get("platform") or "").strip(),
        "account_scope": str(row.get("account_scope") or "").strip(),
        "enabled": bool(row.get("enabled", base["enabled"])),
        "target_platforms": _normalize_notification_platforms(row.get("target_platforms_json", row.get("target_platforms"))),
        "cooldown_seconds": max(0, int(row.get("cooldown_seconds") if row.get("cooldown_seconds") is not None else base["cooldown_seconds"])),
        "quiet_start": str(row.get("quiet_start") or "").strip(),
        "quiet_end": str(row.get("quiet_end") or "").strip(),
        "escalation_enabled": bool(row.get("escalation_enabled", base["escalation_enabled"])),
        "escalation_minutes": max(0, int(row.get("escalation_minutes") if row.get("escalation_minutes") is not None else base["escalation_minutes"])),
        "escalation_platforms": _normalize_notification_platforms(row.get("escalation_platforms_json", row.get("escalation_platforms"))),
        "owner_hint": str(row.get("owner_hint") or "").strip(),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def list_notification_policies() -> list[dict[str, Any]]:
    defaults = {
        (event, _default_notification_policy(event)["severity"], "", ""): _default_notification_policy(event)
        for event in sorted(NOTIFICATION_EVENT_TYPES)
    }
    rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if brand_database_backend() == "supabase":
        try:
            raw_rows = _brand_supabase().select("notification_policies", order="event_type.asc,severity.asc,platform.asc,account_scope.asc")
        except SupabaseError:
            raw_rows = []
        for row in raw_rows:
            policy = _normalize_notification_policy(row)
            rows[(policy["event_type"], policy["severity"], policy["platform"], policy["account_scope"])] = policy
    else:
        ensure_database()
        with connect() as conn:
            for row in conn.execute("SELECT * FROM notification_policies ORDER BY event_type, severity, platform, account_scope"):
                policy = _normalize_notification_policy(dict_from_row(row))
                rows[(policy["event_type"], policy["severity"], policy["platform"], policy["account_scope"])] = policy
    merged = [{**item, **rows.get(key, {})} for key, item in defaults.items()]
    extras = [item for key, item in rows.items() if key not in defaults]
    return sorted(
        [*merged, *extras],
        key=lambda item: (
            str(item.get("event_type") or ""),
            -NOTIFICATION_POLICY_SEVERITY_ORDER.get(str(item.get("severity") or "info"), 0),
            str(item.get("platform") or ""),
            str(item.get("account_scope") or ""),
        ),
    )


def _policy_storage_payload(raw: dict[str, Any], *, for_supabase: bool = False) -> dict[str, Any]:
    event = str(raw.get("event_type") or "").strip()
    if event not in NOTIFICATION_EVENT_TYPES:
        raise ValueError("unsupported notification event_type")
    base = _default_notification_policy(event)
    severity = str(raw.get("severity") or base["severity"]).strip().lower()
    if severity not in NOTIFICATION_POLICY_SEVERITY_ORDER:
        severity = base["severity"]
    platform = str(raw.get("platform") or "").strip()
    if platform:
        platform = _normalize_ai_platform(platform)
        if platform not in NOTIFICATION_PLATFORMS:
            raise ValueError("notification platform must be telegram, dingtalk, or wecom")
    target_platforms = _normalize_notification_platforms(raw.get("target_platforms", raw.get("target_platforms_json")))
    escalation_platforms = _normalize_notification_platforms(raw.get("escalation_platforms", raw.get("escalation_platforms_json")))
    ts = now_ts()
    payload = {
        "event_type": event,
        "severity": severity,
        "platform": platform,
        "account_scope": str(raw.get("account_scope") or "").strip(),
        "enabled": 1 if bool(raw.get("enabled", True)) else 0,
        "target_platforms_json": target_platforms if for_supabase else json.dumps(target_platforms, ensure_ascii=False),
        "cooldown_seconds": max(0, int(raw.get("cooldown_seconds") if raw.get("cooldown_seconds") is not None else base["cooldown_seconds"])),
        "quiet_start": str(raw.get("quiet_start") or "").strip(),
        "quiet_end": str(raw.get("quiet_end") or "").strip(),
        "escalation_enabled": 1 if bool(raw.get("escalation_enabled", base["escalation_enabled"])) else 0,
        "escalation_minutes": max(0, int(raw.get("escalation_minutes") if raw.get("escalation_minutes") is not None else base["escalation_minutes"])),
        "escalation_platforms_json": escalation_platforms if for_supabase else json.dumps(escalation_platforms, ensure_ascii=False),
        "owner_hint": str(raw.get("owner_hint") or "").strip(),
        "created_at": int(raw.get("created_at") or ts),
        "updated_at": ts,
    }
    return payload


def save_notification_policies(payload: Any) -> list[dict[str, Any]]:
    policies = payload.get("policies") if isinstance(payload, dict) else payload
    if not isinstance(policies, list):
        raise ValueError("policies must be a list")
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        for raw in policies:
            if not isinstance(raw, dict):
                continue
            data = _policy_storage_payload(raw, for_supabase=True)
            client.upsert("notification_policies", data, on_conflict="event_type,severity,platform,account_scope")
        _invalidate_supabase_read_cache("notification_policies")
        return list_notification_policies()
    ensure_database()
    with connect() as conn:
        for raw in policies:
            if not isinstance(raw, dict):
                continue
            data = _policy_storage_payload(raw, for_supabase=False)
            conn.execute(
                """
                INSERT INTO notification_policies(
                    event_type, severity, platform, account_scope, enabled, target_platforms_json,
                    cooldown_seconds, quiet_start, quiet_end, escalation_enabled, escalation_minutes,
                    escalation_platforms_json, owner_hint, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_type, severity, platform, account_scope) DO UPDATE SET
                    enabled = excluded.enabled,
                    target_platforms_json = excluded.target_platforms_json,
                    cooldown_seconds = excluded.cooldown_seconds,
                    quiet_start = excluded.quiet_start,
                    quiet_end = excluded.quiet_end,
                    escalation_enabled = excluded.escalation_enabled,
                    escalation_minutes = excluded.escalation_minutes,
                    escalation_platforms_json = excluded.escalation_platforms_json,
                    owner_hint = excluded.owner_hint,
                    updated_at = excluded.updated_at
                """,
                (
                    data["event_type"],
                    data["severity"],
                    data["platform"],
                    data["account_scope"],
                    data["enabled"],
                    data["target_platforms_json"],
                    data["cooldown_seconds"],
                    data["quiet_start"],
                    data["quiet_end"],
                    data["escalation_enabled"],
                    data["escalation_minutes"],
                    data["escalation_platforms_json"],
                    data["owner_hint"],
                    data["created_at"],
                    data["updated_at"],
                ),
            )
            _enqueue_sync_row(
                conn,
                "notification_policies",
                "event_type = ? AND severity = ? AND platform = ? AND account_scope = ?",
                (data["event_type"], data["severity"], data["platform"], data["account_scope"]),
            )
    return list_notification_policies()


def _account_scope_matches(scope: str, payload: dict[str, Any]) -> bool:
    token = str(scope or "").strip()
    if not token:
        return True
    account_id = str(payload.get("account_id") or "").strip()
    account_key = str(payload.get("account_key") or payload.get("account") or "").strip()
    return token in {account_id, account_key, f"account:{account_id}", f"account:{account_key}"}


def _matching_notification_policy(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    severity = str(payload.get("severity") or NOTIFICATION_EVENT_META[event_type].get("default_severity") or "info").strip().lower()
    platform = str(payload.get("platform") or "").strip()
    candidates = []
    for policy in list_notification_policies():
        if policy["event_type"] != event_type:
            continue
        if policy.get("severity") not in {"", severity}:
            continue
        if policy.get("platform") and policy.get("platform") != platform:
            continue
        if not _account_scope_matches(str(policy.get("account_scope") or ""), payload):
            continue
        score = 0
        if policy.get("severity") == severity:
            score += 4
        if policy.get("platform"):
            score += 2
        if policy.get("account_scope"):
            score += 1
        candidates.append((score, policy))
    if not candidates:
        return _default_notification_policy(event_type)
    return sorted(candidates, key=lambda item: item[0], reverse=True)[0][1]


def _notification_delivery_platforms(event_type: str, policy: dict[str, Any]) -> list[str]:
    targets = _normalize_notification_platforms(policy.get("target_platforms"))
    if not targets:
        return _enabled_notification_platforms(event_type)
    configs = {item["platform"]: item for item in list_ai_robot_configs()}
    return [
        platform
        for platform in targets
        if bool(configs.get(platform, {}).get("enabled")) and bool(configs.get(platform, {}).get("webhook_url"))
    ]


def _notification_in_quiet_window(policy: dict[str, Any], *, now: datetime | None = None) -> bool:
    start = str(policy.get("quiet_start") or "").strip()
    end = str(policy.get("quiet_end") or "").strip()
    if not start or not end:
        return False
    if not re.match(r"^\d{2}:\d{2}$", start) or not re.match(r"^\d{2}:\d{2}$", end):
        return False
    current = (now or datetime.now().astimezone()).strftime("%H:%M")
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def list_notification_event_definitions() -> list[dict[str, Any]]:
    return [dict(item) for item in NOTIFICATION_EVENT_DEFINITIONS]


def _notification_text(event_type: str, payload: dict[str, Any]) -> str:
    meta = NOTIFICATION_EVENT_META.get(event_type, {"label": event_type})
    subtype = str(payload.get("subtype") or "").strip()
    title = str(payload.get("title") or meta["label"]).strip()
    summary = str(payload.get("summary") or payload.get("reason") or "").strip()
    severity = str(payload.get("severity") or meta.get("default_severity") or "info").strip()
    lines = [f"[{severity}] {title}"]
    if subtype:
        lines.append(f"类型: {event_type}/{subtype}")
    else:
        lines.append(f"类型: {event_type}")
    if summary:
        lines.append(f"摘要: {summary}")
    for key in ("account", "account_id", "platform", "task_id", "run_id", "action", "owner_hint"):
        value = str(payload.get(key) or "").strip()
        if value:
            lines.append(f"{key}: {value}")
    for key, label in (("source_url", "来源"), ("action_url", "处理")):
        value = str(payload.get(key) or "").strip()
        if value:
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def _notification_dedupe_key(event_type: str, payload: dict[str, Any]) -> str:
    explicit = str(payload.get("dedupe_key") or "").strip()
    if explicit:
        return explicit[:240]
    parts = [
        event_type,
        str(payload.get("subtype") or ""),
        str(payload.get("account_id") or payload.get("account") or ""),
        str(payload.get("account_key") or ""),
        str(payload.get("platform") or ""),
        str(payload.get("task_id") or payload.get("run_id") or ""),
        str(payload.get("title") or ""),
    ]
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{event_type}:{digest}"


def _incident_next_escalate_at(policy: dict[str, Any], status: str, ts: int) -> int | None:
    if status != "open":
        return None
    minutes = int(policy.get("escalation_minutes") or 0)
    if not bool(policy.get("escalation_enabled")) or minutes <= 0:
        return None
    return ts + minutes * 60


def _normalize_notification_incident(row: dict[str, Any]) -> dict[str, Any]:
    payload = _json_payload(row.get("payload_json"), {})
    return {
        **row,
        "payload": payload if isinstance(payload, dict) else {},
        "account_id": int(row.get("account_id") or 0) if row.get("account_id") is not None else None,
        "occurrence_count": int(row.get("occurrence_count") or 0),
        "escalation_count": int(row.get("escalation_count") or 0),
    }


def _incident_payload_for_storage(event_type: str, message: dict[str, Any], policy: dict[str, Any], ts: int) -> dict[str, Any]:
    meta = NOTIFICATION_EVENT_META[event_type]
    severity = str(message.get("severity") or meta.get("default_severity") or "info").strip().lower()
    account_id_value = message.get("account_id")
    try:
        account_id = int(account_id_value) if account_id_value not in {None, ""} else None
    except Exception:
        account_id = None
    return {
        "event_type": event_type,
        "subtype": str(message.get("subtype") or "").strip(),
        "dedupe_key": _notification_dedupe_key(event_type, message),
        "severity": severity,
        "status": "open",
        "title": str(message.get("title") or meta.get("label") or event_type).strip(),
        "summary": str(message.get("summary") or message.get("reason") or "").strip(),
        "account_id": account_id,
        "account_key": str(message.get("account_key") or message.get("account") or "").strip(),
        "platform": str(message.get("platform") or "").strip(),
        "owner_hint": str(message.get("owner_hint") or policy.get("owner_hint") or "").strip(),
        "source_url": str(message.get("source_url") or "").strip(),
        "action_url": str(message.get("action_url") or "").strip(),
        "occurrence_count": 1,
        "escalation_count": 0,
        "first_seen_at": ts,
        "last_seen_at": ts,
        "acknowledged_at": None,
        "resolved_at": None,
        "assigned_to": "",
        "last_escalated_at": None,
        "next_escalate_at": _incident_next_escalate_at(policy, "open", ts),
        "payload_json": message,
        "created_at": ts,
        "updated_at": ts,
    }


def _upsert_notification_incident(event_type: str, message: dict[str, Any], policy: dict[str, Any], *, notify: bool) -> dict[str, Any]:
    ts = now_ts()
    data = _incident_payload_for_storage(event_type, message, policy, ts)
    cooldown = max(0, int(policy.get("cooldown_seconds") or 0))
    quiet = _notification_in_quiet_window(policy)
    policy_enabled = bool(policy.get("enabled", True))
    reason = ""
    send_allowed = bool(notify and policy_enabled and not quiet)
    if not policy_enabled:
        reason = "policy_disabled"
    elif quiet:
        reason = "quiet_window"
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        try:
            existing = client.select_one("notification_incidents", filters={"event_type": event_type, "dedupe_key": data["dedupe_key"]})
            if existing:
                existing_status = str(existing.get("status") or "open")
                closed = existing_status in NOTIFICATION_CLOSED_STATUSES
                in_cooldown = cooldown > 0 and not closed and int(existing.get("last_seen_at") or 0) >= ts - cooldown
                if in_cooldown:
                    send_allowed = False
                    reason = reason or "cooldown"
                next_status = "open" if closed else existing_status
                update_payload = {
                    **{key: data[key] for key in ("subtype", "severity", "title", "summary", "account_id", "account_key", "platform", "owner_hint", "source_url", "action_url", "payload_json")},
                    "status": next_status,
                    "occurrence_count": int(existing.get("occurrence_count") or 1) + 1,
                    "last_seen_at": ts,
                    "updated_at": ts,
                    "resolved_at": None if closed else existing.get("resolved_at"),
                    "next_escalate_at": _incident_next_escalate_at(policy, next_status, ts) if closed else existing.get("next_escalate_at"),
                }
                row = client.update("notification_incidents", update_payload, filters={"id": existing["id"]})
            else:
                row = client.insert("notification_incidents", data)
            _invalidate_supabase_read_cache("notification_incidents")
            incident = _normalize_notification_incident(row or data)
        except SupabaseError as exc:
            incident = {**data, "id": 0, "storage_unavailable": True, "error": str(exc)}
            send_allowed = bool(notify and policy_enabled and not quiet)
    else:
        ensure_database()
        with connect() as conn:
            existing_row = conn.execute(
                "SELECT * FROM notification_incidents WHERE event_type = ? AND dedupe_key = ?",
                (event_type, data["dedupe_key"]),
            ).fetchone()
            if existing_row is None:
                conn.execute(
                    """
                    INSERT INTO notification_incidents(
                        event_type, subtype, dedupe_key, severity, status, title, summary, account_id,
                        account_key, platform, owner_hint, source_url, action_url, occurrence_count,
                        escalation_count, first_seen_at, last_seen_at, acknowledged_at, resolved_at,
                        assigned_to, last_escalated_at, next_escalate_at, payload_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data["event_type"],
                        data["subtype"],
                        data["dedupe_key"],
                        data["severity"],
                        data["status"],
                        data["title"],
                        data["summary"],
                        data["account_id"],
                        data["account_key"],
                        data["platform"],
                        data["owner_hint"],
                        data["source_url"],
                        data["action_url"],
                        data["occurrence_count"],
                        data["escalation_count"],
                        data["first_seen_at"],
                        data["last_seen_at"],
                        data["acknowledged_at"],
                        data["resolved_at"],
                        data["assigned_to"],
                        data["last_escalated_at"],
                        data["next_escalate_at"],
                        json.dumps(data["payload_json"], ensure_ascii=False, default=str),
                        data["created_at"],
                        data["updated_at"],
                    ),
                )
                row = conn.execute("SELECT * FROM notification_incidents WHERE event_type = ? AND dedupe_key = ?", (event_type, data["dedupe_key"])).fetchone()
            else:
                existing = dict_from_row(existing_row)
                existing_status = str(existing.get("status") or "open")
                closed = existing_status in NOTIFICATION_CLOSED_STATUSES
                in_cooldown = cooldown > 0 and not closed and int(existing.get("last_seen_at") or 0) >= ts - cooldown
                if in_cooldown:
                    send_allowed = False
                    reason = reason or "cooldown"
                next_status = "open" if closed else existing_status
                next_escalate_at = _incident_next_escalate_at(policy, next_status, ts) if closed else existing.get("next_escalate_at")
                conn.execute(
                    """
                    UPDATE notification_incidents
                    SET subtype = ?, severity = ?, status = ?, title = ?, summary = ?, account_id = ?,
                        account_key = ?, platform = ?, owner_hint = ?, source_url = ?, action_url = ?,
                        occurrence_count = occurrence_count + 1, last_seen_at = ?, resolved_at = ?,
                        next_escalate_at = ?, payload_json = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        data["subtype"],
                        data["severity"],
                        next_status,
                        data["title"],
                        data["summary"],
                        data["account_id"],
                        data["account_key"],
                        data["platform"],
                        data["owner_hint"],
                        data["source_url"],
                        data["action_url"],
                        ts,
                        None if closed else existing.get("resolved_at"),
                        next_escalate_at,
                        json.dumps(data["payload_json"], ensure_ascii=False, default=str),
                        ts,
                        existing["id"],
                    ),
                )
                row = conn.execute("SELECT * FROM notification_incidents WHERE id = ?", (existing["id"],)).fetchone()
            if row is not None:
                _enqueue_sync_row(conn, "notification_incidents", "id = ?", (int(row["id"]),))
                incident = _normalize_notification_incident(dict_from_row(row))
            else:
                incident = data
    return {"incident": incident, "send_allowed": send_allowed, "suppressed_reason": reason}


def route_operation_notification(event_type: str, payload: dict[str, Any] | None = None, *, notify: bool = True) -> dict[str, Any]:
    event = str(event_type or "").strip()
    if event not in NOTIFICATION_EVENT_TYPES:
        raise ValueError("unsupported notification event_type")
    message = dict(payload or {})
    meta = NOTIFICATION_EVENT_META[event]
    message.setdefault("message_type", event)
    message.setdefault("severity", meta.get("default_severity") or "info")
    message.setdefault("notification_center", bool(meta.get("default_center")))
    message.setdefault("routeable", bool(meta.get("routeable")))
    policy = _matching_notification_policy(event, message)
    incident_result = _upsert_notification_incident(event, message, policy, notify=notify and bool(message["notification_center"]))
    incident = incident_result.get("incident") or {}
    message.setdefault("incident_id", incident.get("id"))
    platforms = _notification_delivery_platforms(event, policy) if incident_result.get("send_allowed") and bool(meta.get("routeable")) else []
    text = str(message.get("text") or "").strip() or _notification_text(event, message)
    results: list[dict[str, Any]] = []
    for platform in platforms:
        try:
            queued = enqueue_ai_robot_message(platform, {**message, "text": text, "message_type": event}, test=False)
            results.append(send_ai_robot_message_now(queued) if str(queued.get("status") or "") != "unsupported" else queued)
        except Exception as exc:
            results.append({"platform": platform, "ok": False, "error": str(exc)})
    return {
        "event_type": event,
        "subtype": message.get("subtype") or "",
        "severity": message["severity"],
        "notification_center": bool(message["notification_center"]),
        "routeable": bool(message["routeable"]),
        "notification_platforms": platforms,
        "notification_results": results,
        "incident": incident,
        "suppressed_reason": incident_result.get("suppressed_reason") or "",
    }


def list_notification_incidents(*, status: str = "", limit: int = 100) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 100), 500))
    status_token = str(status or "").strip().lower()
    if brand_database_backend() == "supabase":
        try:
            filters = {"status": status_token} if status_token else None
            rows = _brand_supabase().select("notification_incidents", filters=filters, order="updated_at.desc,id.desc")[:limit]
        except SupabaseError:
            return []
        return [_normalize_notification_incident(row) for row in rows]
    ensure_database()
    with connect() as conn:
        if status_token:
            rows = conn.execute(
                "SELECT * FROM notification_incidents WHERE status = ? ORDER BY updated_at DESC, id DESC LIMIT ?",
                (status_token, limit),
            )
        else:
            rows = conn.execute("SELECT * FROM notification_incidents ORDER BY updated_at DESC, id DESC LIMIT ?", (limit,))
        return [_normalize_notification_incident(dict_from_row(row)) for row in rows]


def get_notification_incident(incident_id: int) -> dict[str, Any] | None:
    incident_id = int(incident_id or 0)
    if incident_id <= 0:
        return None
    if brand_database_backend() == "supabase":
        try:
            row = _brand_supabase().select_one("notification_incidents", filters={"id": incident_id})
        except SupabaseError:
            return None
        return _normalize_notification_incident(row) if row else None
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM notification_incidents WHERE id = ?", (incident_id,)).fetchone()
        return _normalize_notification_incident(dict_from_row(row)) if row else None


def _record_notification_action(incident_id: int, action: str, actor: str, note: str = "", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    ts = now_ts()
    data = {
        "incident_id": int(incident_id),
        "action": str(action or "").strip(),
        "actor": str(actor or "system").strip(),
        "note": str(note or "").strip(),
        "payload_json": payload or {},
        "created_at": ts,
    }
    if brand_database_backend() == "supabase":
        try:
            return _brand_supabase().insert("notification_actions", data)
        except SupabaseError:
            return data
    with connect() as conn:
        conn.execute(
            "INSERT INTO notification_actions(incident_id, action, actor, note, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (data["incident_id"], data["action"], data["actor"], data["note"], json.dumps(data["payload_json"], ensure_ascii=False), ts),
        )
        row = conn.execute("SELECT * FROM notification_actions WHERE rowid = last_insert_rowid()").fetchone()
        if row is not None:
            _enqueue_sync_row(conn, "notification_actions", "id = ?", (int(row["id"]),))
            return dict_from_row(row)
    return data


def _update_notification_incident_fields(incident_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    fields = {**fields, "updated_at": now_ts()}
    if brand_database_backend() == "supabase":
        row = _brand_supabase().update("notification_incidents", fields, filters={"id": incident_id})
        _invalidate_supabase_read_cache("notification_incidents")
        return _normalize_notification_incident(row)
    with connect() as conn:
        assignments = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values())
        conn.execute(f"UPDATE notification_incidents SET {assignments} WHERE id = ?", (*values, incident_id))
        _enqueue_sync_row(conn, "notification_incidents", "id = ?", (incident_id,))
        row = conn.execute("SELECT * FROM notification_incidents WHERE id = ?", (incident_id,)).fetchone()
        return _normalize_notification_incident(dict_from_row(row)) if row else {}


def _incident_resend_payload(incident: dict[str, Any]) -> dict[str, Any]:
    payload = dict(incident.get("payload") or _json_payload(incident.get("payload_json"), {}) or {})
    payload.update(
        {
            "message_type": incident.get("event_type"),
            "subtype": incident.get("subtype") or payload.get("subtype") or "resend",
            "severity": incident.get("severity") or payload.get("severity") or "info",
            "title": incident.get("title") or payload.get("title") or "通知重发",
            "summary": incident.get("summary") or payload.get("summary") or "",
            "account_id": incident.get("account_id") or payload.get("account_id"),
            "account_key": incident.get("account_key") or payload.get("account_key"),
            "platform": incident.get("platform") or payload.get("platform"),
            "owner_hint": incident.get("owner_hint") or payload.get("owner_hint"),
            "source_url": incident.get("source_url") or payload.get("source_url"),
            "action_url": incident.get("action_url") or payload.get("action_url"),
            "incident_id": incident.get("id"),
        }
    )
    payload["text"] = _notification_text(str(incident.get("event_type") or "action_required"), payload)
    return payload


def resend_notification_incident(incident_id: int) -> dict[str, Any]:
    incident = get_notification_incident(incident_id)
    if not incident:
        raise KeyError("incident not found")
    event = str(incident.get("event_type") or "action_required")
    policy = _matching_notification_policy(event, incident.get("payload") or incident)
    platforms = _notification_delivery_platforms(event, policy)
    payload = _incident_resend_payload(incident)
    results: list[dict[str, Any]] = []
    for platform in platforms:
        try:
            queued = enqueue_ai_robot_message(platform, payload, test=False)
            results.append(send_ai_robot_message_now(queued) if str(queued.get("status") or "") != "unsupported" else queued)
        except Exception as exc:
            results.append({"platform": platform, "ok": False, "error": str(exc)})
    return {"incident": incident, "notification_platforms": platforms, "notification_results": results}


def act_on_notification_incident(incident_id: int, action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    incident = get_notification_incident(incident_id)
    if not incident:
        raise KeyError("incident not found")
    action_token = str(action or "").strip().lower()
    body = dict(payload or {})
    actor = str(body.get("actor") or "Allen").strip()
    note = str(body.get("note") or "").strip()
    ts = now_ts()
    if action_token == "ack":
        fields = {"status": "acknowledged", "acknowledged_at": incident.get("acknowledged_at") or ts, "next_escalate_at": None}
    elif action_token == "assign":
        assigned_to = str(body.get("assigned_to") or body.get("assignee") or actor).strip()
        fields = {"status": "assigned", "assigned_to": assigned_to, "acknowledged_at": incident.get("acknowledged_at") or ts, "next_escalate_at": None}
    elif action_token == "resolve":
        fields = {"status": "resolved", "resolved_at": ts, "next_escalate_at": None}
    elif action_token == "ignore":
        fields = {"status": "ignored", "resolved_at": ts, "next_escalate_at": None}
    elif action_token == "resend":
        _record_notification_action(int(incident["id"]), action_token, actor, note, body)
        resend = resend_notification_incident(int(incident["id"]))
        return {"incident": resend["incident"], "action": action_token, "resend": resend}
    else:
        raise ValueError("action must be ack, assign, resolve, ignore, or resend")
    updated = _update_notification_incident_fields(int(incident["id"]), fields)
    _record_notification_action(int(incident["id"]), action_token, actor, note, body)
    return {"incident": updated, "action": action_token}


def resolve_login_notification_incidents(account_id: int, platform: str = "wechat") -> int:
    account_id = int(account_id or 0)
    if account_id <= 0:
        return 0
    ts = now_ts()
    resolved = 0
    if brand_database_backend() == "supabase":
        try:
            rows = _brand_supabase().select("notification_incidents", filters={"account_id": account_id}, order="id.desc")
            for row in rows:
                if row.get("event_type") not in {"wechat_login_qr", "login_status"}:
                    continue
                if str(row.get("platform") or platform or "wechat") != platform:
                    continue
                if str(row.get("status") or "") not in NOTIFICATION_OPEN_STATUSES:
                    continue
                _brand_supabase().update("notification_incidents", {"status": "resolved", "resolved_at": ts, "next_escalate_at": None, "updated_at": ts}, filters={"id": row["id"]})
                _record_notification_action(int(row["id"]), "resolve", "system", "login_ready")
                resolved += 1
        except SupabaseError:
            return 0
        return resolved
    ensure_database()
    with connect() as conn:
        rows = [
            dict_from_row(row)
            for row in conn.execute(
                """
                SELECT * FROM notification_incidents
                WHERE account_id = ? AND event_type IN ('wechat_login_qr', 'login_status') AND status IN ('open', 'acknowledged', 'assigned')
                """,
                (account_id,),
            )
        ]
        for row in rows:
            if row.get("platform") and str(row.get("platform")) != platform:
                continue
            conn.execute(
                "UPDATE notification_incidents SET status = 'resolved', resolved_at = ?, next_escalate_at = NULL, updated_at = ? WHERE id = ?",
                (ts, ts, row["id"]),
            )
            _enqueue_sync_row(conn, "notification_incidents", "id = ?", (int(row["id"]),))
            resolved_ids.append(int(row["id"]))
            resolved += 1
    for incident_id in resolved_ids:
        _record_notification_action(incident_id, "resolve", "system", "login_ready")
    return resolved


def _due_notification_incidents(limit: int) -> list[dict[str, Any]]:
    ts = now_ts()
    if brand_database_backend() == "supabase":
        try:
            rows = _brand_supabase().select_where(
                "notification_incidents",
                params={"status": "eq.open", "next_escalate_at": f"lte.{ts}"},
                order="next_escalate_at.asc,id.asc",
            )[:limit]
        except SupabaseError:
            return []
        return [_normalize_notification_incident(row) for row in rows if row.get("next_escalate_at")]
    ensure_database()
    with connect() as conn:
        return [
            _normalize_notification_incident(dict_from_row(row))
            for row in conn.execute(
                """
                SELECT * FROM notification_incidents
                WHERE status = 'open' AND next_escalate_at IS NOT NULL AND next_escalate_at <= ?
                ORDER BY next_escalate_at ASC, id ASC
                LIMIT ?
                """,
                (ts, max(1, int(limit or 20))),
            )
        ]


def process_notification_escalations(*, limit: int = 20) -> dict[str, Any]:
    due = _due_notification_incidents(max(1, int(limit or 20)))
    ts = now_ts()
    escalated = 0
    queued = 0
    results: list[dict[str, Any]] = []
    for incident in due:
        event = str(incident.get("event_type") or "action_required")
        policy = _matching_notification_policy(event, incident.get("payload") or incident)
        minutes = max(1, int(policy.get("escalation_minutes") or _default_notification_escalation_minutes(str(incident.get("severity") or "info")) or 10))
        platforms = _normalize_notification_platforms(policy.get("escalation_platforms")) or _notification_delivery_platforms(event, policy)
        payload = _incident_resend_payload(incident)
        payload["subtype"] = payload.get("subtype") or "escalated"
        payload["title"] = f"升级提醒：{incident.get('title') or event}"
        payload["summary"] = f"未确认超时，已发生 {int(incident.get('occurrence_count') or 1)} 次"
        payload["text"] = _notification_text(event, payload)
        platform_results: list[dict[str, Any]] = []
        for platform in platforms:
            try:
                message = enqueue_ai_robot_message(platform, payload, test=False)
                platform_results.append(message)
                if str(message.get("status") or "") != "unsupported":
                    queued += 1
            except Exception as exc:
                platform_results.append({"platform": platform, "ok": False, "error": str(exc)})
        next_at = ts + minutes * 60
        if brand_database_backend() == "supabase":
            try:
                _brand_supabase().update(
                    "notification_incidents",
                    {
                        "escalation_count": int(incident.get("escalation_count") or 0) + 1,
                        "last_escalated_at": ts,
                        "next_escalate_at": next_at,
                        "updated_at": ts,
                    },
                    filters={"id": incident["id"]},
                )
            except SupabaseError:
                pass
        else:
            with connect() as conn:
                conn.execute(
                    """
                    UPDATE notification_incidents
                    SET escalation_count = escalation_count + 1, last_escalated_at = ?, next_escalate_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (ts, next_at, ts, incident["id"]),
                )
                _enqueue_sync_row(conn, "notification_incidents", "id = ?", (int(incident["id"]),))
        _record_notification_action(int(incident["id"]), "escalate", "system", "unacknowledged_timeout", {"platforms": platforms})
        escalated += 1
        results.append({"incident_id": incident.get("id"), "platforms": platforms, "results": platform_results})
    return {"ok": True, "due": len(due), "escalated": escalated, "queued": queued, "results": results}


def notification_sla_summary() -> dict[str, Any]:
    incidents = list_notification_incidents(limit=500)
    now = now_ts()
    open_items = [item for item in incidents if str(item.get("status") or "") in NOTIFICATION_OPEN_STATUSES]
    closed_items = [item for item in incidents if str(item.get("status") or "") in NOTIFICATION_CLOSED_STATUSES]
    ack_seconds = [
        int(item.get("acknowledged_at") or 0) - int(item.get("first_seen_at") or 0)
        for item in incidents
        if item.get("acknowledged_at") and item.get("first_seen_at")
    ]
    resolve_seconds = [
        int(item.get("resolved_at") or 0) - int(item.get("first_seen_at") or 0)
        for item in closed_items
        if item.get("resolved_at") and item.get("first_seen_at")
    ]
    top_events: dict[str, int] = {}
    for item in incidents:
        key = str(item.get("event_type") or "unknown")
        top_events[key] = top_events.get(key, 0) + int(item.get("occurrence_count") or 1)
    messages = list_ai_robot_messages()
    failed_messages = [item for item in messages if str(item.get("status") or "") in {"failed", "retry", "unsupported"}]
    sent_messages = [item for item in messages if str(item.get("status") or "") == "sent"]
    total_delivery = len(failed_messages) + len(sent_messages)
    return {
        "open_count": len(open_items),
        "unacknowledged_count": len([item for item in open_items if str(item.get("status") or "") == "open"]),
        "closed_count": len(closed_items),
        "avg_ack_seconds": round(sum(ack_seconds) / len(ack_seconds)) if ack_seconds else 0,
        "avg_resolve_seconds": round(sum(resolve_seconds) / len(resolve_seconds)) if resolve_seconds else 0,
        "timed_out_count": len([item for item in open_items if item.get("next_escalate_at") and int(item.get("next_escalate_at") or 0) <= now]),
        "escalation_count": sum(int(item.get("escalation_count") or 0) for item in incidents),
        "send_failed_count": len(failed_messages),
        "delivery_failed_rate": round((len(failed_messages) / total_delivery * 100), 1) if total_delivery else 0,
        "top_events": [
            {"event_type": key, "label": NOTIFICATION_EVENT_META.get(key, {}).get("label") or key, "count": value}
            for key, value in sorted(top_events.items(), key=lambda item: item[1], reverse=True)[:5]
        ],
    }


def build_daily_ops_summary(target_date: str = "") -> dict[str, Any]:
    day = str(target_date or datetime.now().astimezone().date().isoformat())
    incidents = list_notification_incidents(limit=500)
    sla = notification_sla_summary()
    by_event: dict[str, int] = {}
    for item in incidents:
        key = str(item.get("event_type") or "unknown")
        by_event[key] = by_event.get(key, 0) + int(item.get("occurrence_count") or 1)
    top = ", ".join(f"{NOTIFICATION_EVENT_META.get(key, {}).get('label') or key} {value}" for key, value in sorted(by_event.items(), key=lambda item: item[1], reverse=True)[:3]) or "暂无"
    text = "\n".join(
        [
            f"GasGx 运营通知日报 {day}",
            f"未闭环: {sla['open_count']}，未确认: {sla['unacknowledged_count']}，已关闭: {sla['closed_count']}",
            f"平均确认: {sla['avg_ack_seconds']} 秒，平均关闭: {sla['avg_resolve_seconds']} 秒",
            f"升级次数: {sla['escalation_count']}，送达失败率: {sla['delivery_failed_rate']}%",
            f"TOP 问题: {top}",
        ]
    )
    return {"target_date": day, "sla": sla, "top_events": by_event, "text": text}


def send_daily_ops_summary(target_date: str = "", *, notify: bool = True) -> dict[str, Any]:
    summary = build_daily_ops_summary(target_date)
    result = route_operation_notification(
        "ops_summary",
        {
            "subtype": "daily_sent",
            "severity": "info",
            "title": f"运营通知日报 {summary['target_date']}",
            "summary": f"未闭环 {summary['sla']['open_count']} / 未确认 {summary['sla']['unacknowledged_count']}",
            "dedupe_key": f"ops_summary:{summary['target_date']}",
            "text": summary["text"],
            "payload": summary,
        },
        notify=notify,
    )
    return {"ok": True, "summary": summary, "notification": result}


def send_notification_route_probe(event_type: str, platform: str) -> dict[str, Any]:
    event = str(event_type or "").strip()
    if event not in NOTIFICATION_EVENT_TYPES:
        raise ValueError("unsupported notification event_type")
    token = _normalize_ai_platform(platform)
    if token not in NOTIFICATION_PLATFORMS:
        raise ValueError("notification platform must be telegram, dingtalk, or wecom")
    meta = NOTIFICATION_EVENT_META[event]
    payload: dict[str, Any] = {
        "message_type": event,
        "subtype": "route_probe",
        "severity": meta.get("default_severity") or "info",
        "title": f"通知链路联调：{meta.get('label') or event}",
        "summary": f"通知路由已开启（{event} -> {token}）",
        "platform": token,
        "event_type": event,
        "route_probe": True,
    }
    payload["text"] = _notification_text(event, payload)
    queued = enqueue_ai_robot_message(token, payload, test=False)
    if str(queued.get("status") or "") == "unsupported":
        return queued
    return send_ai_robot_message_now(queued)


def _qr_fingerprint(item: dict[str, Any]) -> str:
    source = "|".join(
        [
            str(item.get("account_id") or ""),
            str(item.get("profile_dir") or ""),
            str(item.get("reason") or ""),
            str(item.get("url") or item.get("current_url") or ""),
            str(item.get("qr_path") or ""),
        ]
    )
    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:24]


def _login_qr_root() -> Path:
    path = get_paths().runtime_root / "login_qr_batches"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _login_qr_text(batch_id: str, items: list[dict[str, Any]]) -> str:
    lines = [f"视频号登录失效批次 {batch_id}", "请按账号逐个扫码，扫码后等待下一轮巡检恢复。"]
    for index, item in enumerate(items, start=1):
        lines.append(
            f"{index}. {item.get('display_name') or item.get('account_key')} | port={item.get('debug_port')} | "
            f"reason={item.get('reason') or 'login_required'} | profile={item.get('profile_dir')}"
        )
        if item.get("qr_path"):
            lines.append(f"   QR: {item.get('qr_path')}")
    return "\n".join(lines)


def _send_notification_text(platform: str, text: str) -> dict[str, Any]:
    message = enqueue_ai_robot_message(platform, {"text": text, "message_type": "wechat_login_qr", "subtype": "qr_generated"}, test=False)
    if str(message.get("status") or "") == "unsupported":
        return message
    return send_ai_robot_message_now(message)


def record_wechat_login_qr_batch(login_required: list[dict[str, Any]], *, notify: bool = True) -> dict[str, Any] | None:
    if not login_required:
        return None
    ts = now_ts()
    batch_id = time.strftime("wechat-login-%Y%m%d-%H%M%S")
    batch_dir = _login_qr_root() / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    items: list[dict[str, Any]] = []
    for raw in login_required:
        item = dict(raw)
        item["account_id"] = int(item.get("account_id") or 0)
        item["account_key"] = str(item.get("account_key") or f"account-{item['account_id']}")
        item["display_name"] = str(item.get("display_name") or item["account_key"])
        item["profile_dir"] = str(item.get("profile_dir") or "")
        item["debug_port"] = int(item.get("debug_port") or 0)
        item["reason"] = str(item.get("reason") or "login_required")
        item["url"] = str(item.get("url") or item.get("current_url") or "")
        qr_result = item.get("qr_result") if isinstance(item.get("qr_result"), dict) else {}
        item["qr_path"] = str(item.get("qr_path") or qr_result.get("path") or qr_result.get("file") or "")
        item["qr_fingerprint"] = _qr_fingerprint(item)
        items.append(item)
    cooldown_before = ts - LOGIN_QR_NOTIFY_COOLDOWN_SECONDS
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        try:
            duplicate_rows: list[dict[str, Any]] = []
            for item in items:
                existing = client.select_one("login_qr_items", filters={"account_id": item["account_id"], "platform": "wechat", "qr_fingerprint": item["qr_fingerprint"]})
                if existing and int(existing.get("updated_at") or 0) >= cooldown_before:
                    duplicate_rows.append(existing)
            if len(duplicate_rows) == len(items):
                return {
                    "batch_id": "",
                    "items": items,
                    "skipped": True,
                    "reason": "duplicate_login_qr_cooldown",
                    "notification_platforms": [],
                    "notification_results": [],
                }
            client.insert(
                "login_qr_batches",
                {"batch_id": batch_id, "event_type": "wechat_login_qr", "status": "pending", "payload_json": {"items": items}, "created_at": ts, "updated_at": ts},
            )
            for item in items:
                existing = client.select_one("login_qr_items", filters={"account_id": item["account_id"], "platform": "wechat", "qr_fingerprint": item["qr_fingerprint"]})
                if existing is None:
                    client.insert(
                        "login_qr_items",
                        {
                            "batch_id": batch_id,
                            "account_id": item["account_id"],
                            "account_key": item["account_key"],
                            "display_name": item["display_name"],
                            "platform": "wechat",
                            "profile_dir": item["profile_dir"],
                            "debug_port": item["debug_port"],
                            "reason": item["reason"],
                            "url": item["url"],
                            "qr_path": item["qr_path"],
                            "qr_fingerprint": item["qr_fingerprint"],
                            "status": "pending",
                            "created_at": ts,
                            "updated_at": ts,
                        },
                    )
        except SupabaseError as exc:
            return {
                "batch_id": batch_id,
                "items": items,
                "storage_unavailable": True,
                "error": str(exc),
                "notification_platforms": [],
                "notification_results": [],
            }
    else:
        ensure_database()
        with connect() as conn:
            duplicate_count = 0
            for item in items:
                row = conn.execute(
                    """
                    SELECT id FROM login_qr_items
                    WHERE account_id = ? AND platform = 'wechat' AND qr_fingerprint = ? AND updated_at >= ?
                    LIMIT 1
                    """,
                    (item["account_id"], item["qr_fingerprint"], cooldown_before),
                ).fetchone()
                if row is not None:
                    duplicate_count += 1
            if duplicate_count == len(items):
                return {
                    "batch_id": "",
                    "items": items,
                    "skipped": True,
                    "reason": "duplicate_login_qr_cooldown",
                    "notification_platforms": [],
                    "notification_results": [],
                }
            conn.execute(
                "INSERT INTO login_qr_batches(batch_id, event_type, status, payload_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (batch_id, "wechat_login_qr", "pending", json.dumps({"items": items}, ensure_ascii=False), ts, ts),
            )
            for item in items:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO login_qr_items(
                        batch_id, account_id, account_key, display_name, platform, profile_dir, debug_port,
                        reason, url, qr_path, qr_fingerprint, status, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, 'wechat', ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (
                        batch_id,
                        item["account_id"],
                        item["account_key"],
                        item["display_name"],
                        item["profile_dir"],
                        item["debug_port"],
                        item["reason"],
                        item["url"],
                        item["qr_path"],
                        item["qr_fingerprint"],
                        ts,
                        ts,
                    ),
                )
    text = _login_qr_text(batch_id, items)
    result = route_operation_notification(
        "wechat_login_qr",
        {
            "subtype": "qr_generated",
            "severity": "blocking",
            "title": "视频号登录二维码待扫码",
            "summary": f"{len(items)} 个账号需要扫码",
            "dedupe_key": f"wechat_login_qr:{hashlib.sha256('|'.join(item['qr_fingerprint'] for item in items).encode('utf-8')).hexdigest()[:24]}",
            "account_id": items[0].get("account_id") if len(items) == 1 else None,
            "account_key": items[0].get("account_key") if len(items) == 1 else "",
            "account": items[0].get("display_name") if len(items) == 1 else "",
            "platform": "wechat",
            "text": text,
            "items": items,
        },
        notify=notify,
    )
    return {
        "batch_id": batch_id,
        "items": items,
        "notification_platforms": result.get("notification_platforms", []),
        "notification_results": result.get("notification_results", []),
        "incident": result.get("incident", {}),
        "suppressed_reason": result.get("suppressed_reason", ""),
    }


def list_login_qr_batches(limit: int = 20) -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        try:
            return _brand_supabase().select("login_qr_batches", order="id.desc")[:limit]
        except SupabaseError:
            return []
    ensure_database()
    with connect() as conn:
        return [
            dict_from_row(row)
            for row in conn.execute("SELECT * FROM login_qr_batches ORDER BY id DESC LIMIT ?", (max(1, int(limit or 20)),))
        ]


TERMINAL_COLORS = [
    {"hex": "#EF4444", "name": "标准红"},
    {"hex": "#EAB308", "name": "明亮黄"},
    {"hex": "#22C55E", "name": "安全绿"},
    {"hex": "#3B82F6", "name": "科技蓝"},
    {"hex": "#F97316", "name": "活力橙"},
]
TERMINAL_LEGACY_COLOR_ALIASES = {
    "#A855F7": "#22C55E",
    "#EC4899": "#EF4444",
}
TERMINAL_COLOR_NAME_BY_HEX = {item["hex"].lower(): item["name"] for item in TERMINAL_COLORS}


def _terminal_state_path() -> Path:
    return get_paths().runtime_root / "terminal_execution_state.json"


def _terminal_business_date() -> str:
    timezone_name = str(load_distribution_settings_db().get("common", {}).get("timezone") or "").strip()
    if timezone_name:
        try:
            return datetime.now(ZoneInfo(timezone_name)).date().isoformat()
        except ZoneInfoNotFoundError:
            pass
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    return datetime.now(local_tz).date().isoformat()


def _terminal_state_day_from_timestamp(timestamp_value: Any) -> str:
    try:
        ts = int(timestamp_value or 0)
    except Exception:
        return ""
    if ts <= 0:
        return ""
    timezone_name = str(load_distribution_settings_db().get("common", {}).get("timezone") or "").strip()
    if timezone_name:
        try:
            return datetime.fromtimestamp(ts, ZoneInfo(timezone_name)).date().isoformat()
        except ZoneInfoNotFoundError:
            pass
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    return datetime.fromtimestamp(ts, local_tz).date().isoformat()


def _normalize_terminal_color_hex(color: Any, *, slot_index: int = 0) -> str:
    fallback = TERMINAL_COLORS[max(0, int(slot_index)) % len(TERMINAL_COLORS)]["hex"]
    token = str(color or "").strip().upper()
    if token in TERMINAL_LEGACY_COLOR_ALIASES:
        token = TERMINAL_LEGACY_COLOR_ALIASES[token]
    if not re.fullmatch(r"#[0-9A-F]{6}", token):
        return fallback
    if token.lower() not in TERMINAL_COLOR_NAME_BY_HEX:
        return fallback
    return token


def _terminal_color_name(color: Any) -> str:
    token = str(color or "").strip().lower()
    return TERMINAL_COLOR_NAME_BY_HEX.get(token, "")


def _terminal_qr_cache_root() -> Path:
    return get_paths().runtime_root / "terminal_qr_cache"


def _terminal_qr_cache_path(window_id: int) -> Path:
    return _terminal_qr_cache_root() / f"window-{int(window_id):02d}.png"


def terminal_qr_image_path(window_id: int) -> str | None:
    path = _terminal_qr_cache_path(window_id)
    return str(path) if path.exists() else None


def _terminal_qr_cache_bust() -> int:
    return int(time.time() * 1000)


def _terminal_qr_url(window_id: int, version: int | None = None) -> str:
    base = f"/api/terminal-execution/windows/{int(window_id):d}/qr-image"
    if version is None:
        return base
    return f"{base}?v={int(version)}"


def _write_terminal_qr_data_url_cache(window_id: int, data_url: str) -> str:
    if "base64," not in data_url or not data_url.startswith("data:image/"):
        return ""
    try:
        encoded = data_url.split("base64,", 1)[1]
        photo_bytes = base64.b64decode(encoded)
    except Exception:
        return ""
    if not photo_bytes:
        return ""
    cache_path = _terminal_qr_cache_path(window_id)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(photo_bytes)
    return str(cache_path)


def _public_terminal_window(window: dict[str, Any]) -> dict[str, Any]:
    visible = copy.deepcopy(window)
    visible.pop("qr_data_url", None)
    accounts = visible.get("accounts") or []
    if isinstance(accounts, list):
        visible["accounts"] = [
            _normalize_account_runtime(account) if isinstance(account, dict) else account
            for account in accounts
        ]
    window_id = int(visible.get("id") or 0)
    qr_path = str(visible.get("qr_path") or "").strip()
    qr_url = str(visible.get("qr_url") or "").strip()
    if qr_path:
        if not qr_url or qr_url.startswith("data:"):
            visible["qr_url"] = _terminal_qr_url(window_id)
        else:
            visible["qr_url"] = qr_url
    elif qr_url.startswith("data:"):
        visible["qr_url"] = ""
    current = _terminal_current_account(visible)
    if isinstance(current, dict):
        runtime = _terminal_browser_runtime_for_account(int(current.get("id") or 0), "wechat")
        if runtime:
            current.update(runtime)
            visible["browser_open"] = runtime.get("browser_open")
    return visible


_TERMINAL_EXECUTION_STATE_CACHE: dict[str, Any] = {"expires_at": 0.0, "value": None}


def _invalidate_terminal_execution_state_cache() -> None:
    _TERMINAL_EXECUTION_STATE_CACHE["expires_at"] = 0.0
    _TERMINAL_EXECUTION_STATE_CACHE["value"] = None


def _load_terminal_state() -> dict[str, Any]:
    path = _terminal_state_path()
    if not path.exists():
        return {"windows": [], "config": [], "initialized": False, "updated_at": 0}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"windows": [], "config": [], "initialized": False, "updated_at": 0}
    if not isinstance(payload, dict):
        return {"windows": [], "config": [], "initialized": False, "updated_at": 0}
    windows = payload.get("windows") or []
    if isinstance(windows, list):
        for window in windows:
            if not isinstance(window, dict):
                continue
            window_id = int(window.get("id") or 0)
            qr_data_url = str(window.pop("qr_data_url", "") or "").strip()
            if qr_data_url:
                qr_path = str(window.get("qr_path") or "").strip()
                if not qr_path or not Path(qr_path).exists():
                    qr_path = _write_terminal_qr_data_url_cache(window_id, qr_data_url)
                if qr_path:
                    window["qr_path"] = qr_path
                    window["qr_url"] = _terminal_qr_url(window_id)
            if str(window.get("qr_url") or "").strip().startswith("data:"):
                window["qr_url"] = ""
            if not str(window.get("qr_path") or "").strip() and str(window.get("qr_cache_path") or "").strip():
                window["qr_path"] = str(window.get("qr_cache_path") or "")
            if (
                bool(payload.get("login_started"))
                and not str(window.get("qr_path") or "").strip()
                and _terminal_qr_cache_path(window_id).exists()
            ):
                window["qr_path"] = str(_terminal_qr_cache_path(window_id))
                window["qr_url"] = _terminal_qr_url(window_id)
            if bool(payload.get("login_started")) and str(window.get("qr_url") or "").strip():
                if int(window.get("qr_expires_at") or 0) <= 0:
                    window["qr_expires_at"] = now_ts() + TERMINAL_QR_EXPIRY_SECONDS
            if bool(payload.get("login_started")) and str(window.get("qr_url") or "").strip():
                qr_path = str(window.get("qr_path") or "").strip()
                path = Path(qr_path) if qr_path else _terminal_qr_cache_path(window_id)
                try:
                    if not path.exists():
                        window["qr_url"] = ""
                        window["qr_path"] = ""
                except Exception:
                    window["qr_url"] = ""
                    window["qr_path"] = ""
            accounts = window.get("accounts") or []
            if isinstance(accounts, list):
                for account in accounts:
                    if isinstance(account, dict):
                        account.update(_normalize_account_runtime(account))
    return payload


def _save_terminal_state(payload: dict[str, Any]) -> None:
    if not str(payload.get("business_date") or "").strip():
        payload["business_date"] = _terminal_business_date()
    payload["updated_at"] = now_ts()
    path = _terminal_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _invalidate_terminal_execution_state_cache()


def _reset_terminal_window_for_new_business_date(window: dict[str, Any]) -> bool:
    if not isinstance(window, dict):
        return False
    changed = False
    window_id = int(window.get("id") or 0)
    if int(window.get("current_index") or 0) != 0:
        window["current_index"] = 0
        changed = True
    if bool(window.get("completed")):
        window.pop("completed", None)
        changed = True
    if window.get("publish_run"):
        _clear_terminal_publish_run(window)
        changed = True
    if (
        str(window.get("qr_path") or "").strip()
        or str(window.get("qr_url") or "").strip()
        or int(window.get("qr_expires_at") or 0) > 0
    ):
        _clear_terminal_qr_cache(window_id)
        _set_terminal_window_qr(window, "")
        changed = True
    if int(window.get("manual_available_at") or 0) != 0:
        window["manual_available_at"] = 0
        changed = True
    accounts = window.get("accounts") or []
    if isinstance(accounts, list):
        for index, account in enumerate(accounts):
            if not isinstance(account, dict):
                continue
            _clear_terminal_account_error(account)
            expected_status = "pending"
            expected_text = "等待打开登录浏览器" if index == 0 else "未登录"
            if str(account.get("status") or "") != expected_status:
                account["status"] = expected_status
                changed = True
            if str(account.get("status_text") or "") != expected_text:
                account["status_text"] = expected_text
                changed = True
            if account.get("task_id") is not None:
                account["task_id"] = None
                changed = True
    return changed


def _apply_terminal_daily_rollover(state: dict[str, Any]) -> bool:
    if not isinstance(state, dict):
        return False
    today = _terminal_business_date()
    stored = str(state.get("business_date") or "").strip()
    inferred = stored or _terminal_state_day_from_timestamp(state.get("updated_at"))
    changed = False
    if stored != today:
        state["business_date"] = today
        changed = True
    if not inferred or inferred == today:
        return changed
    windows = state.get("windows") or []
    if isinstance(windows, list):
        for window in windows:
            if isinstance(window, dict):
                changed = _reset_terminal_window_for_new_business_date(window) or changed
    if int(state.get("next_probe_at") or 0) != 0:
        state["next_probe_at"] = 0
        changed = True
    if int(state.get("probe_cursor") or 0) != 0:
        state["probe_cursor"] = 0
        changed = True
    return changed


def _load_terminal_state_with_rollover() -> dict[str, Any]:
    state = _load_terminal_state()
    if _apply_terminal_daily_rollover(state):
        _save_terminal_state(state)
    return state


def _clear_terminal_qr_cache(window_id: int) -> None:
    try:
        path = _terminal_qr_cache_path(window_id)
        if path.exists():
            path.unlink()
    except Exception:
        return


def _looks_like_terminal_qr_image(photo_bytes: bytes) -> bool:
    if not isinstance(photo_bytes, (bytes, bytearray)) or not photo_bytes:
        return False
    try:
        image = Image.open(BytesIO(bytes(photo_bytes))).convert("L")
    except Exception:
        return False
    width, height = image.size
    if width < 120 or height < 120:
        return False
    pixels = image.load()
    if pixels is None:
        return False
    sample_step = max(1, min(width, height) // 120)
    row_lines = [int(height * ratio) for ratio in (0.2, 0.35, 0.5, 0.65, 0.8)]
    col_lines = [int(width * ratio) for ratio in (0.2, 0.35, 0.5, 0.65, 0.8)]
    dark = 0
    bright = 0
    total = 0
    transitions = 0

    def _bin(value: int) -> int:
        return 1 if value < 128 else 0

    for y in row_lines:
        yy = max(0, min(height - 1, y))
        prev = None
        for x in range(0, width, sample_step):
            v = int(pixels[x, yy])
            total += 1
            if v <= 95:
                dark += 1
            elif v >= 170:
                bright += 1
            bit = _bin(v)
            if prev is not None and bit != prev:
                transitions += 1
            prev = bit

    for x in col_lines:
        xx = max(0, min(width - 1, x))
        prev = None
        for y in range(0, height, sample_step):
            v = int(pixels[xx, y])
            total += 1
            if v <= 95:
                dark += 1
            elif v >= 170:
                bright += 1
            bit = _bin(v)
            if prev is not None and bit != prev:
                transitions += 1
            prev = bit

    if total <= 0:
        return False
    dark_ratio = dark / total
    bright_ratio = bright / total
    contrast = float(ImageStat.Stat(image).stddev[0] or 0.0)
    if dark_ratio < 0.12 or bright_ratio < 0.08:
        return False
    if contrast < 34.0:
        return False
    if transitions < 110:
        return False
    return True


def _write_terminal_qr_cache(
    window_id: int,
    account_id: int,
    *,
    auto_open_browser: bool = False,
    refresh_page: bool = True,
    timeout_seconds: float = 2.5,
) -> str:
    try:
        account = get_account(account_id) or {}
    except (SupabaseError, requests.RequestException, OSError, TimeoutError):
        return ""
    platform = next((item for item in account.get("platforms", []) if item.get("platform") == "wechat"), None)
    if not platform:
        return ""
    wechat_platform = get_platform("wechat")
    if wechat_platform is None:
        return ""
    page = None
    try:
        page = engine._connect_chrome(  # type: ignore[attr-defined]
            debug_port=int(platform.get("debug_port") or 0),
            auto_open_chrome=bool(auto_open_browser),
            chrome_user_data_dir=str(platform.get("profile_dir") or ""),
            startup_url=wechat_platform.open_url,
        )
    except Exception:
        page = None
    photo_bytes = b""
    if page is not None:
        try:
            if refresh_page:
                try:
                    page.refresh()
                except Exception:
                    pass
            qr_source = engine._extract_login_qr_source(
                page,
                timeout_seconds=max(0.6, float(timeout_seconds)),
                platform_name="wechat",
            )  # type: ignore[attr-defined]
            if qr_source:
                _, photo_bytes = engine._load_qr_image_source(qr_source)  # type: ignore[attr-defined]
        except Exception:
            photo_bytes = b""
        # Do not fallback to page screenshot: it can capture logo/loading overlays instead of real QR.
        # If QR source extraction fails, caller should surface refresh failure and retry.
    if not isinstance(photo_bytes, (bytes, bytearray)) or not photo_bytes:
        return ""
    if not _looks_like_terminal_qr_image(bytes(photo_bytes)):
        return ""
    cache_path = _terminal_qr_cache_path(window_id)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(bytes(photo_bytes))
    return str(cache_path)


def _set_terminal_window_qr(window: dict[str, Any], qr_path: str) -> None:
    window_id = int(window.get("id") or 0)
    window["qr_path"] = qr_path
    if qr_path:
        window["qr_sequence"] = int(window.get("qr_sequence") or 0) + 1
        window["qr_url"] = _terminal_qr_url(window_id, _terminal_qr_cache_bust())
        window["qr_expires_at"] = now_ts() + TERMINAL_QR_EXPIRY_SECONDS
        return
    window["qr_url"] = ""
    window["qr_expires_at"] = 0


def _account_operator_wechat(account: dict[str, Any]) -> str:
    for key in ("operator_wechat", "operator_weixin", "wechat_operator"):
        value = str(account.get(key) or "").strip()
        if value:
            return value
    notes = str(account.get("notes") or "")
    patterns = [
        r"绑定运营微信[:：]\s*([A-Za-z0-9_.@\-]+)",
        r"运营微信[:：]\s*([A-Za-z0-9_.@\-]+)",
        r"operator[_\s-]*wechat[:：=]\s*([A-Za-z0-9_.@\-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, notes, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return "未绑定运营微信"


def _has_wechat_platform(account: dict[str, Any]) -> bool:
    return any(str(item.get("platform") or "") == "wechat" for item in account.get("platforms") or [])


def _terminal_operator_groups() -> list[dict[str, Any]]:
    accounts = [
        account for account in list_accounts()
        if str(account.get("status") or "") == "active" and _has_wechat_platform(account)
    ]
    configured_operators = list_operator_wechats()
    grouped: dict[str, list[dict[str, Any]]] = {operator: [] for operator in configured_operators}
    unbound: list[dict[str, Any]] = []
    for account in accounts:
        operator = _account_operator_wechat(account)
        if operator == "未绑定运营微信" and configured_operators:
            unbound.append(account)
            continue
        grouped.setdefault(operator, []).append(account)
    if not configured_operators and unbound:
        grouped["未绑定运营微信"] = unbound
    result = []
    for operator, items in grouped.items():
        items.sort(key=lambda item: int(item.get("id") or 0))
        result.append({
            "operator_wechat": operator,
            "accounts": [
                {
                    "id": int(item.get("id") or 0),
                    "account_key": item.get("account_key"),
                    "display_name": item.get("display_name") or item.get("account_key"),
                    "publish_success_count": int(item.get("publish_success_count") or 0),
                    "login_status": next((platform.get("login_status") for platform in item.get("platforms", []) if platform.get("platform") == "wechat"), ""),
                }
                for item in items
            ],
        })
    result.sort(key=lambda item: (-len(item.get("accounts") or []), str(item["operator_wechat"])))
    return result


def terminal_execution_state() -> dict[str, Any]:
    now = time.monotonic()
    cached = _TERMINAL_EXECUTION_STATE_CACHE
    if cached["value"] is not None and now < float(cached["expires_at"] or 0.0):
        return cached["value"]
    groups = _terminal_operator_groups()
    state = _load_terminal_state_with_rollover()
    login_started = bool(state.get("login_started"))
    windows = state.get("windows") or []
    normalized = False
    config_rows = state.get("config") or []
    if isinstance(config_rows, list):
        for index, row in enumerate(config_rows, start=1):
            if not isinstance(row, dict):
                continue
            slot_id = int(row.get("id") or index)
            color = _normalize_terminal_color_hex(row.get("color"), slot_index=max(0, slot_id - 1))
            if str(row.get("color") or "").strip().upper() != color:
                row["color"] = color
                normalized = True
    for window in windows:
        if isinstance(window, dict):
            normalized = _normalize_terminal_window_runtime(window) or normalized
    if normalized:
        _save_terminal_state(state)
    visible_windows = [_public_terminal_window(window) for window in windows if isinstance(window, dict)]
    if not login_started:
        for visible_window in visible_windows:
            if not isinstance(visible_window, dict):
                continue
            visible_window["manual_available_at"] = 0
            visible_accounts = []
            for index, account in enumerate(visible_window.get("accounts") or []):
                visible_account = dict(account)
                if index == int(visible_window.get("current_index") or 0) and str(visible_account.get("status") or "") != "success":
                    visible_account["status"] = "pending"
                    visible_account["status_text"] = "等待开始登录"
                    visible_account["task_id"] = None
                visible_accounts.append(visible_account)
            visible_window["accounts"] = visible_accounts
    for window in visible_windows:
        if not isinstance(window, dict):
            continue
        window["publish_precheck"] = _terminal_publish_precheck(
            window,
            login_started=login_started,
            require_login_probe=False,
        )
    result = {
        "ok": True,
        "colors": TERMINAL_COLORS,
        "operators": groups,
        "windows": visible_windows,
        "config": state.get("config") or [],
        "active_platform": str(state.get("active_platform") or "wechat"),
        "platform_capabilities": state.get("platform_capabilities") or _terminal_platform_capabilities(),
        "profile_by_platform": state.get("profile_by_platform") or _terminal_profile_by_platform(),
        "initialized": bool(state.get("initialized")) or bool(windows),
        "login_started": login_started,
        "summary": _terminal_summary(windows, groups),
    }
    cached["value"] = result
    cached["expires_at"] = now + 3.0
    return result


def _terminal_summary(windows: list[dict[str, Any]], groups: list[dict[str, Any]]) -> dict[str, Any]:
    total = sum(len(group.get("accounts") or []) for group in groups)
    success = 0
    for window in windows:
        for account in window.get("accounts") or []:
            if str(account.get("status") or "") == "success":
                success += 1
    return {"total": total, "success": success, "active_windows": len([item for item in windows if item.get("enabled")])}


def _terminal_platform_capabilities() -> dict[str, dict[str, Any]]:
    capabilities: dict[str, dict[str, Any]] = {}
    for platform in SUPPORTED_PLATFORMS:
        session_policy = "daily_qr" if platform.key == "wechat" else "persistent"
        capabilities[platform.key] = {
            "label": platform.label,
            "sessionPolicy": session_policy,
            "canOpenBrowser": bool(platform.can_open_browser),
            "canLoginStatus": bool(platform.can_login_status),
            "canPublish": bool(platform.can_publish),
            "canComment": bool(platform.can_comment),
            "canMessage": bool(platform.can_message),
            "canStats": bool(platform.can_stats),
            "openUrl": platform.open_url,
        }
    return capabilities


def _terminal_profile_by_platform() -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    for platform in SUPPORTED_PLATFORMS:
        session_policy = "daily_qr" if platform.key == "wechat" else "persistent"
        profiles[platform.key] = {
            "platform": platform.key,
            "label": platform.label,
            "sessionPolicy": session_policy,
            "openUrl": platform.open_url,
            "browserRuntime": "terminal-execution",
        }
    return profiles


def _close_wechat_browser_for_account(account_id: int) -> None:
    account = get_account(account_id) or {}
    platform = next((item for item in account.get("platforms", []) if item.get("platform") == "wechat"), None)
    if not platform:
        return
    try:
        debug_port = int(platform.get("debug_port") or 0)
    except Exception:
        debug_port = 0
    profile_dir = str(platform.get("profile_dir") or "")
    if debug_port <= 0 or not profile_dir:
        return
    try:
        process_rows = _list_windows_chrome_processes() if os.name == "nt" else []
        pid = _find_windows_chrome_pid_by_debug_port(debug_port, profile_dir, processes=process_rows)
        if pid:
            engine._terminate_windows_process_tree(int(pid))  # type: ignore[attr-defined]
    except Exception:
        return


def _terminal_browser_runtime_for_account(account_id: int, platform_name: str = "wechat") -> dict[str, Any]:
    if account_id <= 0:
        return {}
    account = get_account(account_id) or {}
    platform = next((item for item in account.get("platforms", []) if item.get("platform") == platform_name), None)
    if not platform:
        return {}
    try:
        debug_port = int(platform.get("debug_port") or 0)
    except Exception:
        debug_port = 0
    profile_dir = str(platform.get("profile_dir") or "").strip()
    if debug_port <= 0 or not profile_dir:
        return {}
    browser_open: bool | None = None
    try:
        # Probe CDP first; only verify the owning profile when a browser appears to be open.
        browser_open = bool(engine._is_chrome_debug_port_ready(debug_port, timeout=0.2))  # type: ignore[attr-defined]
    except Exception:
        browser_open = None
    if browser_open is True and os.name == "nt":
        try:
            browser_open = bool(_find_windows_chrome_pid_by_debug_port(debug_port, profile_dir))
        except Exception:
            browser_open = None
    return {
        "browser_open": browser_open,
        "debug_port": debug_port,
        "profile_dir": profile_dir,
    }


def _windows_colorref_from_hex(value: str) -> int | None:
    color = str(value or "").strip()
    if not re.fullmatch(r"#[0-9a-fA-F]{6}", color):
        return None
    red = int(color[1:3], 16)
    green = int(color[3:5], 16)
    blue = int(color[5:7], 16)
    return red | (green << 8) | (blue << 16)


def _list_windows_chrome_processes() -> list[dict[str, Any]]:
    if os.name != "nt":
        return []
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | "
                "Select-Object ProcessId,CommandLine | ConvertTo-Json -Depth 3",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            check=False,
        )
        payload = json.loads(result.stdout or "[]")
        if isinstance(payload, dict):
            payload = [payload]
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        command_line = str(item.get("CommandLine") or "")
        if not command_line:
            continue
        process_id = item.get("ProcessId")
        if process_id is None:
            continue
        try:
            process_id_value = int(process_id)
        except Exception:
            continue
        debug_port_raw = engine._extract_cli_flag_value(command_line, "remote-debugging-port")  # type: ignore[attr-defined]
        user_data_dir_raw = engine._extract_cli_flag_value(command_line, "user-data-dir")  # type: ignore[attr-defined]
        try:
            parsed_debug_port = int(debug_port_raw) if debug_port_raw else 0
        except Exception:
            parsed_debug_port = 0
        rows.append(
            {
                "process_id": process_id_value,
                "command_line": command_line,
                "is_child": "--type=" in command_line,
                "debug_port": parsed_debug_port,
                "user_data_dir": str(user_data_dir_raw or ""),
                "normalized_user_data_dir": engine._normalize_user_data_dir(str(user_data_dir_raw or "")) if user_data_dir_raw else "",  # type: ignore[attr-defined]
            }
        )
    return rows


def _find_windows_chrome_pid_by_debug_port(
    debug_port: int,
    profile_dir: str = "",
    *,
    processes: list[dict[str, Any]] | None = None,
) -> int:
    if os.name != "nt" or int(debug_port or 0) <= 0:
        return 0
    rows = processes if isinstance(processes, list) else _list_windows_chrome_processes()
    target_dir = engine._normalize_user_data_dir(profile_dir) if profile_dir else ""  # type: ignore[attr-defined]
    browser_rows = [
        row
        for row in rows
        if int(row.get("debug_port") or 0) == int(debug_port) and not bool(row.get("is_child"))
    ]
    if target_dir:
        for row in browser_rows:
            if str(row.get("normalized_user_data_dir") or "") == target_dir:
                return int(row.get("process_id") or 0)
        return 0
    for row in browser_rows:
        return int(row.get("process_id") or 0)
    return 0


def _raise_windows_chrome_window(debug_port: int, profile_dir: str) -> bool:
    if os.name != "nt" or int(debug_port or 0) <= 0:
        return False
    try:
        process_rows = _list_windows_chrome_processes()
        pid = _find_windows_chrome_pid_by_debug_port(debug_port, profile_dir, processes=process_rows)
        if pid <= 0:
            return False
        return bool(engine._set_windows_process_window_mode(int(pid), "normal"))  # type: ignore[attr-defined]
    except Exception:
        return False


def _raise_account_browser_window(account_id: int, platform_name: str = "wechat") -> bool:
    runtime = _terminal_browser_runtime_for_account(account_id, platform_name)
    try:
        debug_port = int(runtime.get("debug_port") or 0)
    except Exception:
        debug_port = 0
    profile_dir = str(runtime.get("profile_dir") or "").strip()
    if debug_port <= 0 or not profile_dir:
        return False
    return _raise_windows_chrome_window(debug_port, profile_dir)


def _apply_windows_chrome_window_color(
    debug_port: int,
    profile_dir: str,
    color: str,
    *,
    chrome_processes: list[dict[str, Any]] | None = None,
) -> bool:
    if os.name != "nt":
        return False
    colorref = _windows_colorref_from_hex(color)
    if colorref is None:
        return False
    pid = _find_windows_chrome_pid_by_debug_port(debug_port, profile_dir, processes=chrome_processes)
    if pid <= 0:
        return False
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:
        return False

    user32 = ctypes.WinDLL("user32", use_last_error=True)
    dwmapi = ctypes.WinDLL("dwmapi", use_last_error=True)
    enum_windows = user32.EnumWindows
    enum_windows.argtypes = [ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM), wintypes.LPARAM]
    enum_windows.restype = wintypes.BOOL
    get_window_thread_process_id = user32.GetWindowThreadProcessId
    get_window_thread_process_id.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    get_window_thread_process_id.restype = wintypes.DWORD
    is_window_visible = user32.IsWindowVisible
    is_window_visible.argtypes = [wintypes.HWND]
    is_window_visible.restype = wintypes.BOOL
    set_window_attribute = dwmapi.DwmSetWindowAttribute
    set_window_attribute.argtypes = [wintypes.HWND, ctypes.c_uint, ctypes.c_void_p, ctypes.c_uint]
    set_window_attribute.restype = ctypes.c_long

    hwnds: list[int] = []

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def _callback(hwnd: int, _lparam: int) -> bool:
        window_pid = wintypes.DWORD()
        get_window_thread_process_id(hwnd, ctypes.byref(window_pid))
        if int(window_pid.value) == pid and bool(is_window_visible(hwnd)):
            hwnds.append(int(hwnd))
        return True

    try:
        enum_windows(_callback, 0)
    except Exception:
        return False
    if not hwnds:
        return False

    applied = False
    color_value = wintypes.DWORD(colorref)
    size = ctypes.sizeof(color_value)
    for hwnd in hwnds:
        for attribute in (34, 35):
            try:
                if set_window_attribute(wintypes.HWND(hwnd), attribute, ctypes.byref(color_value), size) == 0:
                    applied = True
            except Exception:
                continue
    return applied


def _apply_account_browser_window_color(open_result: dict[str, Any], color: str) -> bool:
    try:
        debug_port = int(open_result.get("debug_port") or 0)
    except Exception:
        debug_port = 0
    profile_dir = str(open_result.get("profile_dir") or "").strip()
    if debug_port <= 0 or not profile_dir:
        return False
    deadline = time.monotonic() + 3.0
    cached_processes: list[dict[str, Any]] = []
    next_refresh_at = 0.0
    while time.monotonic() < deadline:
        now = time.monotonic()
        if os.name == "nt" and now >= next_refresh_at:
            cached_processes = _list_windows_chrome_processes()
            next_refresh_at = now + 0.8
        if _apply_windows_chrome_window_color(
            debug_port,
            profile_dir,
            color,
            chrome_processes=cached_processes if os.name == "nt" else None,
        ):
            return True
        time.sleep(0.2)
    return False


def _terminal_color_title_badge(color: str) -> str:
    token = str(color or "").strip().lower()
    mapping = {
        "#ef4444": "🔴",
        "#22c55e": "🟢",
        "#3b82f6": "🔵",
        "#f97316": "🟠",
        "#eab308": "🟡",
        "#a855f7": "🟢",
        "#ec4899": "🔴",
    }
    return mapping.get(token, "")


def _inject_terminal_account_browser_marker(account_id: int, platform: str, color: str, terminal_window_id: int = 0) -> bool:
    account = get_account(account_id)
    if account is None:
        return False
    token = normalize_platform(platform)
    capability = get_platform(token)
    if capability is None:
        return False
    profile = next((item for item in account.get("platforms", []) if str(item.get("platform") or "") == token), None)
    if not isinstance(profile, dict):
        return False
    return _inject_account_browser_marker(
        account,
        profile,
        capability,
        accent_override=color,
        title_badge=_terminal_color_title_badge(color),
        terminal_window_id=terminal_window_id,
    )


def _terminal_account_cards(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": int(item.get("id") or 0),
            "display_name": item.get("display_name") or item.get("account_key"),
            "account_key": item.get("account_key"),
            "status": "pending",
            "status_text": "未登录",
            "task_id": None,
            "publish_success_count": int(item.get("publish_success_count") or 0),
        }
        for item in accounts
    ]


def _carry_terminal_window_runtime(window: dict[str, Any], previous: dict[str, Any]) -> bool:
    accounts = window.get("accounts") or []
    previous_accounts = previous.get("accounts") or []
    if not accounts or not previous_accounts:
        return False
    previous_by_id = {int(item.get("id") or 0): item for item in previous_accounts if isinstance(item, dict)}
    previous_current_index = int(previous.get("current_index") or 0)
    previous_current = previous_accounts[previous_current_index] if previous_current_index < len(previous_accounts) else {}
    previous_current_id = int(previous_current.get("id") or 0) if isinstance(previous_current, dict) else 0
    next_current_index = next(
        (index for index, account in enumerate(accounts) if int(account.get("id") or 0) == previous_current_id),
        0,
    )
    window["current_index"] = next_current_index
    for account in accounts:
        previous_account = previous_by_id.get(int(account.get("id") or 0))
        if not previous_account:
            continue
        for key in ("status", "status_text", "task_id", "error_stage", "error_title", "error_detail"):
            account[key] = previous_account.get(key)
    next_current = accounts[next_current_index]
    if int(next_current.get("id") or 0) != previous_current_id:
        return False
    qr_path = str(previous.get("qr_path") or "").strip()
    if qr_path and Path(qr_path).exists():
        window["qr_path"] = qr_path
        window["qr_url"] = str(previous.get("qr_url") or _terminal_qr_url(int(window.get("id") or 0)))
        window["manual_available_at"] = int(previous.get("manual_available_at") or 0)
        window["qr_expires_at"] = int(previous.get("qr_expires_at") or 0) or (now_ts() + TERMINAL_QR_EXPIRY_SECONDS)
        return True
    return False


def start_terminal_execution(payload: dict[str, Any]) -> dict[str, Any]:
    groups = {item["operator_wechat"]: item for item in _terminal_operator_groups()}
    previous_state = _load_terminal_state_with_rollover()
    previous_login_started = bool(previous_state.get("login_started"))
    previous_windows = {
        int(item.get("id") or 0): item
        for item in previous_state.get("windows") or []
        if isinstance(item, dict)
    }
    windows = []
    saved_config = []
    retained_window_ids: set[int] = set()
    for index, raw in enumerate(payload.get("windows") or [], start=1):
        enabled = bool(raw.get("enabled", True))
        if not enabled:
            continue
        operator = str(raw.get("operator_wechat") or "").strip()
        group = groups.get(operator) or {"accounts": []}
        accounts = _terminal_account_cards(group.get("accounts") or [])
        color = _normalize_terminal_color_hex(raw.get("color"), slot_index=index - 1)
        qr_path = ""
        window_id = int(raw.get("id") or index)
        window = {
            "id": window_id,
            "enabled": True,
            "operator_wechat": operator,
            "color": color,
            "color_name": _terminal_color_name(color),
            "current_index": 0,
            "qr_path": qr_path,
            "qr_url": "",
            "qr_sequence": 0,
            "qr_expires_at": 0,
            "manual_available_at": 0,
            "accounts": accounts,
        }
        if accounts:
            current = accounts[0]
            current["status"] = "pending"
            current["status_text"] = "等待开始登录"
        previous_window = previous_windows.get(window_id)
        if previous_login_started and previous_window and str(previous_window.get("operator_wechat") or "") == operator:
            retained_window_ids.add(window_id)
            if not _carry_terminal_window_runtime(window, previous_window):
                _clear_terminal_qr_cache(window_id)
        elif previous_login_started:
            _clear_terminal_qr_cache(window_id)
        windows.append(window)
        saved_config.append({
            "id": window_id,
            "enabled": True,
            "operator_wechat": operator,
            "color": color,
        })
    if not windows:
        raise ValueError("至少需要启用一个已绑定运营微信的终端")
    if previous_login_started:
        for previous_id in previous_windows:
            if previous_id not in retained_window_ids and all(int(item.get("id") or 0) != previous_id for item in windows):
                _clear_terminal_qr_cache(previous_id)
    state = {
        "windows": windows,
        "config": saved_config,
        "initialized": True,
        "login_started": previous_login_started,
        "active_platform": "wechat",
        "platform_capabilities": _terminal_platform_capabilities(),
        "profile_by_platform": _terminal_profile_by_platform(),
    }
    if previous_login_started:
        state["probe_cursor"] = int(previous_state.get("probe_cursor") or 0)
        state["next_probe_at"] = int(previous_state.get("next_probe_at") or 0)
    _save_terminal_state(state)
    return terminal_execution_state()


def start_terminal_login() -> dict[str, Any]:
    state = _load_terminal_state_with_rollover()
    windows = state.get("windows") or []
    if not windows:
        raise ValueError("请先初始化执行矩阵")
    if not any(int(window.get("current_index") or 0) < len(window.get("accounts") or []) for window in windows if isinstance(window, dict)):
        raise ValueError("当前配置下没有可登录账号，请先绑定运营微信与账号")
    opened_count = 0
    for window in windows:
        accounts = window.get("accounts") or []
        current_index = int(window.get("current_index") or 0)
        if current_index < len(accounts):
            current = accounts[current_index]
            if str(current.get("status") or "") != "success" and not current.get("task_id"):
                _open_terminal_window_current_account(window)
                opened_count += 1
            elif str(current.get("status") or "") not in {"success", "running"}:
                _clear_terminal_account_error(current)
                current["status"] = "pending"
                current["status_text"] = "等待打开登录浏览器"
                current["task_id"] = None
                window["qr_path"] = ""
                window["qr_url"] = ""
                window["qr_expires_at"] = 0
                window["manual_available_at"] = 0
    state["login_started"] = True
    state["probe_cursor"] = 0
    state["next_probe_at"] = 0
    _save_terminal_state(state)
    return terminal_execution_state()


def _open_terminal_window_current_account(window: dict[str, Any]) -> bool:
    accounts = window.get("accounts") or []
    current_index = int(window.get("current_index") or 0)
    if current_index >= len(accounts):
        return False
    current = accounts[current_index]
    account_id = int(current.get("id") or 0)
    _clear_terminal_account_error(current)
    current["status"] = "opening"
    current["status_text"] = "正在打开登录浏览器"
    _clear_terminal_qr_cache(int(window.get("id") or 0))
    _set_terminal_window_qr(window, "")
    try:
        open_result = open_account_browser(account_id, "wechat")
        _apply_account_browser_window_color(open_result, str(window.get("color") or ""))
        _inject_terminal_account_browser_marker(account_id, "wechat", str(window.get("color") or ""), int(window.get("id") or 0))
        _raise_windows_chrome_window(int(open_result.get("debug_port") or 0), str(open_result.get("profile_dir") or ""))
        current["status"] = "waiting_qr"
        current["status_text"] = TERMINAL_LOGIN_CONFIRM_TEXT
    except Exception as exc:
        _set_terminal_account_error(
            current,
            stage="login_browser",
            title="打开登录浏览器失败",
            message=str(exc),
        )
    window["manual_available_at"] = 0
    return True


def _terminal_publish_runs_root() -> Path:
    path = get_paths().runtime_root / "terminal_publish_runs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _clear_terminal_publish_run(window: dict[str, Any]) -> None:
    window["publish_run"] = {}


def _terminal_window_is_completed(window: dict[str, Any]) -> bool:
    accounts = window.get("accounts") or []
    current_index = int(window.get("current_index") or 0)
    return bool(accounts) and current_index >= len(accounts)


def _terminal_current_account(window: dict[str, Any]) -> dict[str, Any] | None:
    accounts = window.get("accounts") or []
    current_index = int(window.get("current_index") or 0)
    if current_index < 0 or current_index >= len(accounts):
        return None
    current = accounts[current_index]
    return current if isinstance(current, dict) else None


def _terminal_run_matches_current(run: dict[str, Any] | None, current: dict[str, Any] | None) -> bool:
    if not isinstance(run, dict) or not isinstance(current, dict):
        return False
    run_account_id = int(run.get("account_id") or 0)
    current_account_id = int(current.get("id") or 0)
    return run_account_id > 0 and run_account_id == current_account_id


def _terminal_message_is_manual_confirmable_failure(message: str) -> bool:
    error_text = str(message or "").lower()
    return any(
        marker in error_text
        for marker in (
            "未检测到视频号发布成功记录",
            "未检测到发布证据",
            "publish process finished but no wechat evidence",
            "publish_unconfirmed",
            "publish was not confirmed",
            "发布未确认",
        )
    )


def _terminal_run_is_manual_confirmable(run: dict[str, Any] | None) -> bool:
    if not isinstance(run, dict):
        return False
    run_status = str(run.get("status") or "").strip().lower()
    if run_status not in {"failed", "error"}:
        return False
    return _terminal_message_is_manual_confirmable_failure(f"{run.get('error') or ''} {run.get('error_title') or ''}")


def _terminal_account_has_legacy_manual_confirm_failure(account: dict[str, Any]) -> bool:
    status = str(account.get("status") or "").strip().lower()
    if status not in {"error", "failed"}:
        return False
    return _terminal_message_is_manual_confirmable_failure(
        f"{account.get('status_text') or ''} {account.get('error_detail') or ''} {account.get('error_title') or ''}"
    )


def _set_terminal_account_waiting_publish_confirm(account: dict[str, Any]) -> None:
    _clear_terminal_account_error(account)
    account["status"] = "running"
    account["status_text"] = TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT


def _normalize_terminal_window_runtime(window: dict[str, Any]) -> bool:
    changed = False
    window_id = int(window.get("id") or 0)
    normalized_color = _normalize_terminal_color_hex(window.get("color"), slot_index=max(0, window_id - 1))
    if str(window.get("color") or "").strip().upper() != normalized_color:
        window["color"] = normalized_color
        changed = True
    expected_color_name = _terminal_color_name(normalized_color)
    if str(window.get("color_name") or "") != expected_color_name:
        window["color_name"] = expected_color_name
        changed = True
    accounts = window.get("accounts") or []
    current_index = int(window.get("current_index") or 0)
    run = window.get("publish_run")
    if _terminal_window_is_completed(window):
        if not bool(window.get("completed")):
            window["completed"] = True
            changed = True
        if window.get("publish_run"):
            _clear_terminal_publish_run(window)
            changed = True
        if str(window.get("qr_path") or "") or str(window.get("qr_url") or "") or int(window.get("qr_expires_at") or 0):
            _clear_terminal_qr_cache(int(window.get("id") or 0))
            _set_terminal_window_qr(window, "")
            changed = True
        if int(window.get("manual_available_at") or 0):
            window["manual_available_at"] = 0
            changed = True
        return changed
    if bool(window.get("completed")):
        window.pop("completed", None)
        changed = True
    current = _terminal_current_account(window)
    if current is not None and _terminal_run_matches_current(run if isinstance(run, dict) else None, current) and _terminal_run_is_manual_confirmable(run):
        if str(current.get("status") or "") != "running" or str(current.get("status_text") or "") != TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT:
            _set_terminal_account_waiting_publish_confirm(current)
            changed = True
    if isinstance(run, dict) and run and current is not None and not _terminal_run_matches_current(run, current):
        _clear_terminal_publish_run(window)
        changed = True
    for index, account in enumerate(accounts):
        if not isinstance(account, dict) or not _terminal_account_has_legacy_manual_confirm_failure(account):
            if isinstance(account, dict):
                normalized_account = _normalize_account_runtime(account)
                if normalized_account != account:
                    accounts[index] = normalized_account
                    changed = True
            continue
        normalized_account = _normalize_account_runtime(account)
        if normalized_account != account:
            accounts[index] = normalized_account
            account = normalized_account
            changed = True
        if index < current_index:
            _clear_terminal_account_error(account)
            account["status"] = "success"
            account["status_text"] = "发布成功"
            account["publish_success_count"] = max(1, int(account.get("publish_success_count") or 0))
            changed = True
        elif index == current_index and not (isinstance(run, dict) and run):
            _set_terminal_account_waiting_publish_confirm(account)
            changed = True
    return changed


def _is_terminal_publish_running(window: dict[str, Any]) -> bool:
    run = window.get("publish_run")
    if not isinstance(run, dict):
        return False
    current = _terminal_current_account(window)
    return _terminal_run_matches_current(run, current) and str(run.get("status") or "").strip().lower() == "running"


def _clear_terminal_account_error(account: dict[str, Any]) -> None:
    for key in ("error_stage", "error_title", "error_detail"):
        account.pop(key, None)


def _set_terminal_account_error(account: dict[str, Any], *, stage: str, title: str, message: str) -> None:
    account["status"] = "error"
    account["status_text"] = message
    account["error_stage"] = stage
    account["error_title"] = title
    account["error_detail"] = message


def _terminal_precheck_issue(code: str, label: str, message: str) -> dict[str, str]:
    return {"code": code, "label": label, "message": message}


def _terminal_publish_precheck(window: dict[str, Any], *, login_started: bool, require_login_probe: bool = False) -> dict[str, Any]:
    issues: dict[str, list[dict[str, str]]] = {"p0": [], "p1": [], "p2": []}
    accounts = window.get("accounts") or []
    current_index = int(window.get("current_index") or 0)
    if _terminal_window_is_completed(window):
        return {"ok": False, "issues": issues, "selected_video": ""}
    if current_index >= len(accounts):
        issues["p0"].append(_terminal_precheck_issue("no_current_account", "当前账号缺失", "当前窗口没有可发布账号。"))
        return {"ok": False, "issues": issues, "selected_video": ""}
    current = accounts[current_index]
    current_status = str(current.get("status") or "").strip().lower()
    account_id = int(current.get("id") or 0)
    if account_id <= 0:
        issues["p0"].append(_terminal_precheck_issue("account_invalid", "账号信息缺失", "当前账号 ID 无效，无法启动发布。"))
        return {"ok": False, "issues": issues, "selected_video": ""}
    if not login_started:
        issues["p0"].append(_terminal_precheck_issue("login_not_started", "尚未启动登录流程", "请先打开登录浏览器并完成扫码登录。"))
    if not require_login_probe:
        if current_status not in {"ready", "running", "success"}:
            issues["p0"].append(_terminal_precheck_issue("login_not_ready", "登录未确认", "请先在浏览器完成扫码，然后点击“我已登录”。"))
        return {"ok": not issues["p0"], "issues": issues, "selected_video": ""}
    account = get_account(account_id) or {}
    if not account:
        issues["p0"].append(_terminal_precheck_issue("account_not_found", "账号不存在", "账号记录不存在或已被删除。"))
        return {"ok": False, "issues": issues, "selected_video": ""}
    platform_row = next((item for item in account.get("platforms", []) if str(item.get("platform") or "") == "wechat"), None)
    if not platform_row:
        issues["p0"].append(_terminal_precheck_issue("wechat_platform_missing", "视频号配置缺失", "账号未配置视频号平台，无法发布。"))
    else:
        debug_port = int(platform_row.get("debug_port") or 0)
        profile_dir = str(platform_row.get("profile_dir") or "").strip()
        if debug_port <= 0 or not profile_dir:
            issues["p0"].append(_terminal_precheck_issue("wechat_platform_invalid", "视频号配置不完整", "请检查 profile_dir 与 debug_port 配置。"))
    if current_status != "ready":
        issues["p0"].append(_terminal_precheck_issue("login_not_ready", "登录未确认", "请先在浏览器完成扫码，然后点击“我已登录”。"))
    selected_video = ""
    try:
        from . import matrix_publish as mp
        publish_date = datetime.now().astimezone().date().isoformat()
        candidates = mp.list_candidate_videos()
        used = mp._consumed_index(today=publish_date)
        for path in candidates:
            asset_key = mp._relative_asset_key(path)
            if (asset_key, account_id, "wechat", publish_date) in used:
                continue
            selected_video = str(path)
            break
        if not selected_video:
            issues["p0"].append(_terminal_precheck_issue("no_material", "当天无可用素材", "当前账号当天没有可用视频素材。"))
    except Exception as exc:
        issues["p1"].append(_terminal_precheck_issue("material_check_failed", "素材检查失败", f"素材预检失败：{exc}"))
    return {"ok": not issues["p0"], "issues": issues, "selected_video": selected_video}


def _friendly_terminal_run_error(message: str) -> str:
    text = str(message or "").strip()
    if not text:
        return "发布流程失败，请重试"
    normalized = text.lower()
    if "publish process finished but no wechat evidence" in normalized:
        return "发布流程已执行，但未检测到视频号发布成功记录"
    if "invalid publish pid" in normalized:
        return "发布进程异常，请重试"
    if "no available material for today" in normalized:
        return "当前账号当天没有可用视频素材"
    if "account not found" in normalized:
        return "账号不存在"
    if "wechat platform config missing" in normalized:
        return "视频号配置缺失"
    if "wechat_login_required" in normalized:
        return "账号登录已失效，请先重新扫码登录"
    if "platform_login_required" in normalized:
        return "平台登录状态异常，请先完成登录检测"
    if "publish_unconfirmed" in normalized or "发布未确认" in normalized:
        return "发布未确认，请先在视频号后台核实发布结果"
    return text


def _terminal_publish_failure_reason(run: dict[str, Any]) -> str:
    log_path = str(run.get("log_path") or "").strip()
    if not log_path:
        return "未检测到发布证据"
    try:
        raw = Path(log_path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return "未检测到发布证据"
    lowered = raw.lower()
    if "e_publish_unconfirmed_draft_saved" in lowered:
        return "发布未确认，草稿回退也未确认，请先在视频号后台人工核实"
    if "publish was not confirmed" in lowered:
        return "发布未确认，请先在视频号后台人工核实"
    if "draft fallback attempt failed" in lowered:
        return "发布未确认且草稿回退失败，请先在视频号后台人工核实"
    return "未检测到发布证据"


def _terminal_publish_log_has_success_marker(run: dict[str, Any]) -> bool:
    log_path = str(run.get("log_path") or "").strip()
    if not log_path:
        return False
    try:
        raw = Path(log_path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return False
    lowered = raw.lower()
    success_markers = (
        "[success:wechat]",
        "[success] 草稿已保存",
        "draft save success marker detected",
    )
    return any(marker in lowered for marker in success_markers)


def _build_terminal_publish_plan_item(account: dict[str, Any], platform_row: dict[str, Any], source_video: Path) -> Any:
    from . import matrix_publish as mp

    run_token = datetime.now().strftime("%Y%m%d_%H%M%S")
    workspace = _terminal_publish_runs_root() / f"{run_token}_{str(account.get('account_key') or account.get('id') or 'account')}"
    return mp.PublishPlanItem(
        account_id=int(account.get("id") or 0),
        account_key=str(account.get("account_key") or ""),
        display_name=str(account.get("display_name") or account.get("account_key") or ""),
        profile_dir=Path(str(platform_row.get("profile_dir") or "")),
        debug_port=int(platform_row.get("debug_port") or 0),
        fingerprint=platform_row.get("fingerprint") or {},
        source_video=source_video,
        workspace=workspace,
        batch_index=0,
        batch_position=0,
        platform="wechat",
        publish_date=datetime.now().astimezone().date().isoformat(),
    )


def _start_terminal_wechat_publish(window: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    from . import matrix_publish as mp

    account_id = int(current.get("id") or 0)
    account = get_account(account_id)
    if not account:
        raise RuntimeError("account not found")
    platform_row = next((item for item in account.get("platforms", []) if str(item.get("platform") or "") == "wechat"), None)
    if not platform_row:
        raise RuntimeError("wechat platform config missing")
    candidates = mp.list_candidate_videos()
    publish_date = datetime.now().astimezone().date().isoformat()
    used = mp._consumed_index(today=publish_date)
    source_video: Path | None = None
    for path in candidates:
        asset_key = mp._relative_asset_key(path)
        if (asset_key, account_id, "wechat", publish_date) in used:
            continue
        source_video = path
        break
    if source_video is None:
        raise RuntimeError("no available material for today")
    item = _build_terminal_publish_plan_item(account, platform_row, source_video)
    settings = load_platform_publish_settings("wechat")
    prepared = mp.prepare_workspace(item)
    runtime_config = mp._runtime_publish_config("wechat", settings, item.workspace)
    token = "wechat"
    cmd: list[str] = [
        sys.executable,
        "-m",
        "cybercar.pipeline",
        "--publish-only",
        "--upload-platforms",
        token,
        "--limit",
        "1",
        "--config",
        str(runtime_config),
        "--workspace",
        str(item.workspace),
        "--debug-port",
        str(int(item.debug_port)),
        "--chrome-user-data-dir",
        str(item.profile_dir),
        "--upload-timeout",
        str(int(settings.get("upload_timeout") or 60)),
        "--wechat-debug-port",
        str(int(item.debug_port)),
        "--wechat-chrome-user-data-dir",
        str(item.profile_dir),
        "--collection-name",
        str(settings.get("collection_name") or ""),
    ]
    caption = mp._caption_with_topics(settings)
    if caption:
        cmd.extend(["--caption", caption])
    if bool(settings.get("declare_original")):
        cmd.append("--wechat-declare-original")
    if str(settings.get("publish_mode") or "publish") == "draft":
        cmd.append("--wechat-save-draft-only")
    else:
        cmd.append("--wechat-publish-now")
    env = {
        **os.environ,
        "CYBERCAR_DISABLE_REQUIRED_HASHTAGS": "1",
    }
    extra_args = browser_fingerprint_launch_args(item.fingerprint)
    if extra_args:
        env["CYBERCAR_CHROME_EXTRA_ARGS"] = json.dumps(extra_args, ensure_ascii=False)
    log_path = Path(item.workspace) / "terminal_wechat_publish.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stream = log_path.open("w", encoding="utf-8", errors="replace")
    creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    process = subprocess.Popen(
        cmd,
        cwd=str(get_paths().repo_root),
        env=env,
        stdout=stream,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        creationflags=creationflags,
    )
    stream.close()
    run_id = uuid.uuid4().hex
    return {
        "run_id": run_id,
        "status": "running",
        "pid": int(process.pid),
        "started_at": now_ts(),
        "account_id": account_id,
        "platform": "wechat",
        "workspace": str(item.workspace),
        "prepared_video": str(prepared),
        "video": str(source_video),
        "asset_key": mp._relative_asset_key(source_video),
        "publish_date": publish_date,
        "log_path": str(log_path),
        "manual_publish": True,
    }


def _queue_terminal_draft_task(account_id: int) -> dict[str, Any]:
    try:
        return create_task({"account_id": account_id, "platform": "wechat", "task_type": "draft", "payload": {"source": "terminal-execution"}})
    except ValueError as exc:
        duplicate = re.search(r"#(\d+)", str(exc))
        if duplicate:
            task = get_task(int(duplicate.group(1)))
            if task:
                return task
        raise


def _check_terminal_login_status_with_timeout(account_id: int, platform: str) -> dict[str, Any]:
    holder: dict[str, Any] = {}

    def _runner() -> None:
        try:
            holder["result"] = check_login_status(account_id, platform)
        except Exception as exc:
            holder["error"] = exc

    timeout_seconds = max(1, int(TERMINAL_LOGIN_PROBE_TIMEOUT_SECONDS or 4))
    thread = Thread(target=_runner, daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        raise TimeoutError(f"登录检测超时（{timeout_seconds}s）")
    if "error" in holder:
        raise holder["error"]
    result = holder.get("result")
    return result if isinstance(result, dict) else {"status": "unknown"}


def _terminate_terminal_publish_run_process(run: dict[str, Any] | None) -> None:
    if not isinstance(run, dict):
        return
    try:
        pid = int(run.get("pid") or 0)
    except Exception:
        pid = 0
    if pid <= 0 or not _pid_is_running(pid):
        return
    try:
        if os.name == "nt":
            engine._terminate_windows_process_tree(pid)  # type: ignore[attr-defined]
        else:
            os.kill(pid, 15)
    except Exception:
        pass


def _terminal_current_needs_browser_open(window: dict[str, Any]) -> bool:
    current = _terminal_current_account(window)
    if not isinstance(current, dict):
        return False
    if current.get("task_id"):
        return False
    current_status = str(current.get("status") or "").strip().lower()
    if current_status == "pending" and not window.get("qr_url"):
        return True
    if current_status not in {"opening", "waiting_qr"}:
        return False
    runtime = _terminal_browser_runtime_for_account(int(current.get("id") or 0), "wechat")
    return runtime.get("browser_open") is False


def _terminal_current_needs_login_probe(window: dict[str, Any]) -> bool:
    current = _terminal_current_account(window)
    if not isinstance(current, dict) or current.get("task_id"):
        return False
    if _terminal_current_needs_browser_open(window):
        return False
    current_status = str(current.get("status") or "").strip().lower()
    return current_status in {"opening", "waiting_qr"}


def _advance_terminal_window(
    window: dict[str, Any],
    *,
    allow_browser_open: bool = True,
    allow_login_probe: bool = True,
) -> bool:
    if _normalize_terminal_window_runtime(window):
        return True
    accounts = window.get("accounts") or []
    current_index = int(window.get("current_index") or 0)
    if current_index >= len(accounts):
        return False
    current = accounts[current_index]
    run = window.get("publish_run")
    run_status = (
        str(run.get("status") or "").strip().lower()
        if _terminal_run_matches_current(run if isinstance(run, dict) else None, current)
        else ""
    )
    if run_status == "running":
        current["status"] = "running"
        current["status_text"] = TERMINAL_PUBLISH_PREPARE_TEXT
        return True
    if run_status == "success":
        current["status"] = "running"
        current["status_text"] = TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT
        return True
    if run_status == "failed":
        if _terminal_run_is_manual_confirmable(run if isinstance(run, dict) else None):
            _set_terminal_account_waiting_publish_confirm(current)
            return True
        reason = _friendly_terminal_run_error(str((run or {}).get("error") or "").strip()) if isinstance(run, dict) else ""
        current["status"] = "error"
        current["status_text"] = f"发布失败：{reason}" if reason else "发布失败"
        return True
    account_id = int(current.get("id") or 0)
    current_status = str(current.get("status") or "").strip().lower()
    if current_status == "pending" and not current.get("task_id") and not window.get("qr_url"):
        if not allow_browser_open:
            return False
        return _open_terminal_window_current_account(window)
    if current_status in {"opening", "waiting_qr"} and not current.get("task_id"):
        runtime = _terminal_browser_runtime_for_account(account_id, "wechat")
        if runtime.get("browser_open") is False:
            if not allow_browser_open:
                return False
            return _open_terminal_window_current_account(window)
    if not current.get("task_id") and now_ts() < int(window.get("manual_available_at") or 0):
        if str(current.get("status") or "") != "success":
            current["status"] = "waiting_qr"
            current["status_text"] = TERMINAL_LOGIN_CONFIRM_TEXT
        return False
    if current_status not in {"opening", "waiting_qr"}:
        return False
    if not allow_login_probe:
        return False
    try:
        result = _check_terminal_login_status_with_timeout(account_id, "wechat")
    except Exception as exc:
        current["status"] = "error"
        current["status_text"] = str(exc)
        return True
    if str(result.get("status") or "") != "ready":
        current["status"] = "waiting_qr"
        current["status_text"] = TERMINAL_LOGIN_CONFIRM_TEXT
        if window.get("qr_url") or window.get("qr_path"):
            _clear_terminal_qr_cache(int(window.get("id") or 0))
            _set_terminal_window_qr(window, "")
        return True
    _clear_terminal_qr_cache(int(window.get("id") or 0))
    _set_terminal_window_qr(window, "")
    if current.get("task_id"):
        current["task_id"] = None
    current["status"] = "ready"
    current["status_text"] = TERMINAL_LOGIN_READY_TEXT
    return True


def _poll_terminal_publish_runs(windows: list[dict[str, Any]]) -> bool:
    changed = False
    for window in windows:
        run = window.get("publish_run")
        if not isinstance(run, dict) or str(run.get("status") or "").strip().lower() != "running":
            continue
        pid = int(run.get("pid") or 0)
        current_index = int(window.get("current_index") or 0)
        accounts = window.get("accounts") or []
        if current_index >= len(accounts):
            _clear_terminal_publish_run(window)
            changed = True
            continue
        current = accounts[current_index]
        if not _terminal_run_matches_current(run, current):
            _clear_terminal_publish_run(window)
            changed = True
            continue

        if pid <= 0:
            run["status"] = "failed"
            run["finished_at"] = now_ts()
            run["error"] = "invalid publish pid"
            run["error_stage"] = "publish_run"
            run["error_title"] = "发布执行失败"
            _set_terminal_account_error(
                current,
                stage="publish_run",
                title="发布执行失败",
                message="发布失败：进程无效",
            )
            changed = True
            continue
        if _pid_is_running(pid):
            _clear_terminal_account_error(current)
            current["status"] = "running"
            current["status_text"] = TERMINAL_PUBLISH_PREPARE_TEXT
            changed = True
            continue

        run["finished_at"] = now_ts()
        run["status"] = "manual_confirm"
        run["error_stage"] = ""
        run["error_title"] = ""
        run["error"] = ""
        _set_terminal_account_waiting_publish_confirm(current)
        changed = True
    return changed


def poll_terminal_execution(*, allow_browser_open: bool = True, allow_login_probe: bool = True) -> dict[str, Any]:
    state = _load_terminal_state_with_rollover()
    if not bool(state.get("login_started")):
        return terminal_execution_state()
    windows = [window for window in (state.get("windows") or []) if isinstance(window, dict)]
    normalized = False
    for window in windows:
        normalized = _normalize_terminal_window_runtime(window) or normalized
    publish_changed = _poll_terminal_publish_runs(windows)
    cleared_probe_at = int(state.get("next_probe_at") or 0) != 0
    state["next_probe_at"] = 0
    if normalized or publish_changed or cleared_probe_at:
        _save_terminal_state(state)
    return terminal_execution_state()


def manual_terminal_publish(window_id: int) -> dict[str, Any]:
    state = _load_terminal_state_with_rollover()
    target = next((item for item in state.get("windows") or [] if int(item.get("id") or 0) == int(window_id)), None)
    if target is None:
        raise KeyError("window not found")
    if not bool(state.get("login_started")):
        return terminal_execution_state()
    if _normalize_terminal_window_runtime(target):
        _save_terminal_state(state)
        if _terminal_window_is_completed(target):
            return terminal_execution_state()
    if now_ts() < int(target.get("manual_available_at") or 0):
        return terminal_execution_state()
    accounts = target.get("accounts") or []
    current_index = int(target.get("current_index") or 0)
    if current_index < len(accounts):
        current = accounts[current_index]
        precheck = _terminal_publish_precheck(
            target,
            login_started=bool(state.get("login_started")),
            require_login_probe=False,
        )
        target["publish_precheck"] = precheck
        if not bool(precheck.get("ok")):
            issue = ((precheck.get("issues") or {}).get("p0") or [{}])[0]
            issue_code = str(issue.get("code") or "")
            title = "发布启动失败"
            stage = "publish_start"
            if issue_code.startswith("login_"):
                title = "登录检测失败"
                stage = "login_probe"
            _set_terminal_account_error(
                current,
                stage=stage,
                title=title,
                message=f"{title}：{str(issue.get('message') or '预检未通过')}",
            )
            _save_terminal_state(state)
            return terminal_execution_state()
        _clear_terminal_account_error(current)
        if current.get("task_id"):
            current["task_id"] = None
        if _is_terminal_publish_running(target):
            current["status"] = "running"
            current["status_text"] = TERMINAL_PUBLISH_PREPARE_TEXT
            _save_terminal_state(state)
            return terminal_execution_state()
        try:
            run = _start_terminal_wechat_publish(target, current)
            target["publish_run"] = run
            current["task_id"] = None
            current["status"] = "running"
            current["status_text"] = TERMINAL_PUBLISH_PREPARE_TEXT
        except Exception as exc:
            friendly_error = _friendly_terminal_run_error(str(exc))
            _set_terminal_account_error(
                current,
                stage="publish_start",
                title="发布启动失败",
                message=f"发布启动失败：{friendly_error}",
            )
    target["manual_available_at"] = 0
    _save_terminal_state(state)
    return terminal_execution_state()


def open_terminal_account_qr(window_id: int, account_id: int) -> dict[str, Any]:
    state = _load_terminal_state_with_rollover()
    target = next((item for item in state.get("windows") or [] if int(item.get("id") or 0) == int(window_id)), None)
    if target is None:
        raise KeyError("window not found")
    if _normalize_terminal_window_runtime(target):
        _save_terminal_state(state)
        if _terminal_window_is_completed(target):
            return terminal_execution_state()
    current = _terminal_current_account(target)
    run = target.get("publish_run")
    run_status = str((run or {}).get("status") or "").strip().lower() if isinstance(run, dict) else ""
    if _terminal_run_matches_current(run if isinstance(run, dict) else None, current) and (
        run_status in {"running", "manual_confirm", "success"} or _terminal_run_is_manual_confirmable(run)
    ):
        return terminal_execution_state()
    accounts = target.get("accounts") or []
    next_index = next((index for index, item in enumerate(accounts) if int(item.get("id") or 0) == int(account_id)), -1)
    if next_index < 0:
        raise KeyError("account not found")
    if current is not None and int(current.get("id") or 0) == int(account_id):
        runtime = _terminal_browser_runtime_for_account(int(account_id), "wechat")
        if runtime.get("browser_open") is True:
            _inject_terminal_account_browser_marker(int(account_id), "wechat", str(target.get("color") or ""), int(target.get("id") or 0))
            _raise_account_browser_window(int(account_id), "wechat")
            _clear_terminal_account_error(current)
            _clear_terminal_qr_cache(int(target.get("id") or 0))
            _set_terminal_window_qr(target, "")
            if str(current.get("status") or "").strip().lower() != "ready":
                current["status"] = "waiting_qr"
                current["status_text"] = TERMINAL_LOGIN_CONFIRM_TEXT
            current["task_id"] = None
            target["manual_available_at"] = 0
            state["next_probe_at"] = 0
            _save_terminal_state(state)
            return terminal_execution_state()
    _clear_terminal_publish_run(target)
    target.pop("completed", None)
    target["current_index"] = next_index
    target["manual_available_at"] = 0
    target["qr_expires_at"] = 0
    _clear_terminal_qr_cache(int(target.get("id") or 0))
    for account in accounts:
        if int(account.get("id") or 0) == int(account_id):
            _clear_terminal_account_error(account)
            account["status"] = "pending"
            account["status_text"] = "等待打开登录浏览器"
            account["task_id"] = None
            break
    _open_terminal_window_current_account(target)
    state["next_probe_at"] = 0
    _save_terminal_state(state)
    return terminal_execution_state()


def confirm_terminal_login_ready(window_id: int) -> dict[str, Any]:
    state = _load_terminal_state_with_rollover()
    target = next((item for item in state.get("windows") or [] if int(item.get("id") or 0) == int(window_id)), None)
    if target is None:
        raise KeyError("window not found")
    if not bool(state.get("login_started")):
        return terminal_execution_state()
    if _normalize_terminal_window_runtime(target):
        _save_terminal_state(state)
        if _terminal_window_is_completed(target):
            return terminal_execution_state()
    current = _terminal_current_account(target)
    if current is None:
        return terminal_execution_state()
    run = target.get("publish_run")
    run_status = str((run or {}).get("status") or "").strip().lower() if isinstance(run, dict) else ""
    if _terminal_run_matches_current(run if isinstance(run, dict) else None, current) and (
        run_status in {"running", "manual_confirm", "success"} or _terminal_run_is_manual_confirmable(run)
    ):
        return terminal_execution_state()
    _clear_terminal_account_error(current)
    _clear_terminal_qr_cache(int(target.get("id") or 0))
    _set_terminal_window_qr(target, "")
    current["status"] = "ready"
    current["status_text"] = TERMINAL_LOGIN_READY_TEXT
    current["task_id"] = None
    target["manual_available_at"] = 0
    state["next_probe_at"] = 0
    _save_terminal_state(state)
    return terminal_execution_state()


def confirm_terminal_publish_success(window_id: int) -> dict[str, Any]:
    state = _load_terminal_state_with_rollover()
    target = next((item for item in state.get("windows") or [] if int(item.get("id") or 0) == int(window_id)), None)
    if target is None:
        raise KeyError("window not found")
    if not bool(state.get("login_started")):
        return terminal_execution_state()
    normalized = _normalize_terminal_window_runtime(target)
    if _terminal_window_is_completed(target):
        if normalized:
            _save_terminal_state(state)
        return terminal_execution_state()
    accounts = target.get("accounts") or []
    current_index = int(target.get("current_index") or 0)
    if current_index >= len(accounts):
        return terminal_execution_state()
    current = accounts[current_index]
    run = target.get("publish_run")
    run_status = str((run or {}).get("status") or "").strip().lower() if isinstance(run, dict) else ""
    run_matches_current = _terminal_run_matches_current(run if isinstance(run, dict) else None, current)
    can_confirm = run_matches_current and (
        run_status in {"running", "manual_confirm", "success"} or _terminal_run_is_manual_confirmable(run)
    )
    if not can_confirm:
        if normalized:
            _save_terminal_state(state)
        return terminal_execution_state()
    if str(current.get("status") or "") != "success":
        _clear_terminal_account_error(current)
        current["status"] = "success"
        current["status_text"] = "发布成功"
        current["publish_success_count"] = int(current.get("publish_success_count") or 0) + 1
    if isinstance(run, dict):
        try:
            from . import matrix_publish as mp
            state_payload = mp._load_state()
            consumed = list(state_payload.get("consumed", [])) if isinstance(state_payload.get("consumed"), list) else []
            consumed.append(
                {
                    "asset_key": str(run.get("asset_key") or ""),
                    "account_id": int(run.get("account_id") or 0),
                    "platform": "wechat",
                    "publish_date": str(run.get("publish_date") or datetime.now().astimezone().date().isoformat()),
                    "success": True,
                    "finished_at": int(time.time()),
                }
            )
            state_payload["consumed"] = consumed[-500:]
            mp._save_state(state_payload)
        except Exception:
            pass
    _terminate_terminal_publish_run_process(run if isinstance(run, dict) else None)
    _clear_terminal_publish_run(target)
    _close_wechat_browser_for_account(int(current.get("id") or 0))
    _clear_terminal_qr_cache(int(target.get("id") or 0))
    target["qr_path"] = ""
    target["qr_url"] = ""
    target["qr_expires_at"] = 0
    target["manual_available_at"] = 0
    next_index = current_index + 1
    target["current_index"] = next_index
    if next_index < len(accounts):
        next_account = accounts[next_index]
        target.pop("completed", None)
        _clear_terminal_account_error(next_account)
        next_account["status"] = "pending"
        next_account["status_text"] = "等待打开登录浏览器"
        next_account["task_id"] = None
        _open_terminal_window_current_account(target)
        state["next_probe_at"] = 0
    else:
        target["completed"] = True
    _save_terminal_state(state)
    return terminal_execution_state()


def _claim_ai_robot_messages(limit: int) -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        rows = _brand_supabase().select_where(
            "ai_robot_messages",
            params={"status": "in.(pending,retry)", "retry_count": f"lt.{AI_ROBOT_RETRY_LIMIT}"},
            order="id.asc",
        )
        claimed = rows[:limit]
        ts = now_ts()
        for row in claimed:
            _brand_supabase().update(
                "ai_robot_messages",
                {"status": "sending", "summary": "claimed by robot sender", "last_attempt_at": ts, "updated_at": ts},
                filters={"id": row["id"]},
            )
            row["status"] = "sending"
            row["last_attempt_at"] = ts
        _invalidate_supabase_read_cache("ai_robot_messages")
        return claimed
    ensure_database()
    with connect() as conn:
        rows = [
            dict_from_row(row)
            for row in conn.execute(
                """
                SELECT * FROM ai_robot_messages
                WHERE status IN ('pending', 'retry') AND retry_count < ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (AI_ROBOT_RETRY_LIMIT, limit),
            )
        ]
        ts = now_ts()
        for row in rows:
            conn.execute(
                """
                UPDATE ai_robot_messages
                SET status = 'sending', summary = 'claimed by robot sender', last_attempt_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (ts, ts, row["id"]),
            )
            row["status"] = "sending"
            row["last_attempt_at"] = ts
            _enqueue_sync_row(conn, "ai_robot_messages", "id = ?", (int(row["id"]),))
        return rows


def _send_ai_robot_message(message: dict[str, Any]) -> None:
    platform = _normalize_ai_platform(str(message.get("platform") or ""))
    config = _private_ai_config(platform)
    if not config or not bool(config.get("enabled")):
        raise ValueError(f"{platform} robot is not enabled")
    webhook_url = str(config.get("webhook_url") or "").strip()
    if not webhook_url:
        raise ValueError(f"{platform} robot webhook_url is required")
    payload = _message_payload(message)
    text = _message_text(payload)
    body, headers = _robot_http_request(platform, webhook_url, config, text, payload)
    if platform == "dingtalk":
        webhook_url = _signed_dingtalk_webhook_url(webhook_url, str(config.get("webhook_secret") or "").strip())
    response = requests.post(webhook_url, json=body, headers=headers, timeout=float(os.getenv("AI_ROBOT_SEND_TIMEOUT", "10") or 10))
    if response.status_code >= 400:
        raise RuntimeError(f"{platform} send failed: {response.status_code} {response.text[:500]}")
    try:
        data = response.json()
    except Exception:
        data = {}
    if isinstance(data, dict):
        if platform == "wecom" and int(data.get("errcode") or 0) != 0:
            raise RuntimeError(f"wecom send failed: {data}")
        if platform == "dingtalk" and int(data.get("errcode") or 0) != 0:
            raise RuntimeError(f"dingtalk send failed: {data}")
        if platform == "lark" and int(data.get("code") or 0) != 0:
            raise RuntimeError(f"lark send failed: {data}")
        if platform == "telegram" and data.get("ok") is False:
            raise RuntimeError(f"telegram send failed: {data}")


def _message_payload(message: dict[str, Any]) -> dict[str, Any]:
    raw = message.get("payload_json")
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(str(raw or "{}"))
    except Exception:
        parsed = {}
    return parsed if isinstance(parsed, dict) else {}


def _message_text(payload: dict[str, Any]) -> str:
    text = str(payload.get("text") or payload.get("content") or "").strip()
    if text:
        return text
    return json.dumps(payload, ensure_ascii=False)


def _signed_dingtalk_webhook_url(webhook_url: str, secret: str) -> str:
    if not secret:
        return webhook_url
    timestamp = str(now_ts() * 1000)
    digest = hmac.new(secret.encode("utf-8"), f"{timestamp}\n{secret}".encode("utf-8"), hashlib.sha256).digest()
    sign = base64.b64encode(digest).decode("ascii")
    parsed = urlsplit(webhook_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({"timestamp": timestamp, "sign": sign})
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def _lark_webhook_sign(timestamp: str, secret: str) -> str:
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), b"", hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def _robot_http_request(
    platform: str,
    webhook_url: str,
    config: dict[str, Any],
    text: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    headers: dict[str, str] = {}
    secret = str(config.get("webhook_secret") or "").strip()
    target_id = str(config.get("target_id") or "").strip()
    if platform == "wecom":
        return {"msgtype": "text", "text": {"content": text}}, headers
    if platform == "dingtalk":
        return {"msgtype": "text", "text": {"content": text}}, headers
    if platform == "lark":
        body: dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
        if secret:
            timestamp = str(now_ts())
            body["timestamp"] = timestamp
            body["sign"] = _lark_webhook_sign(timestamp, secret)
        return body, headers
    if platform == "telegram":
        body = {"text": text, "parse_mode": str(payload.get("parse_mode") or "")}
        reply_markup = payload.get("reply_markup") if isinstance(payload.get("reply_markup"), dict) else {}
        action_url = str(payload.get("action_url") or "").strip()
        if reply_markup:
            body["reply_markup"] = reply_markup
        elif action_url.startswith(("http://", "https://")):
            body["reply_markup"] = {"inline_keyboard": [[{"text": "打开处理", "url": action_url}]]}
        if target_id:
            body["chat_id"] = target_id
        return {key: value for key, value in body.items() if value}, headers
    if platform == "whatsapp":
        if secret:
            headers["authorization"] = f"Bearer {secret}"
        body = {"text": text, "recipient": target_id, "payload": payload}
        return {key: value for key, value in body.items() if value}, headers
    return {"text": text, "payload": payload}, headers


def _mark_ai_robot_message_sending(message: dict[str, Any]) -> dict[str, Any]:
    ts = now_ts()
    payload = {"status": "sending", "summary": "claimed by robot sender", "last_attempt_at": ts, "updated_at": ts}
    if brand_database_backend() == "supabase":
        row = _brand_supabase().update("ai_robot_messages", payload, filters={"id": message["id"]})
        _invalidate_supabase_read_cache("ai_robot_messages")
        return row
    with connect() as conn:
        conn.execute(
            """
            UPDATE ai_robot_messages
            SET status = ?, summary = ?, last_attempt_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (payload["status"], payload["summary"], payload["last_attempt_at"], payload["updated_at"], message["id"]),
        )
        _enqueue_sync_row(conn, "ai_robot_messages", "id = ?", (int(message["id"]),))
    return get_ai_robot_message(int(message["id"])) or {**message, **payload}


def _mark_ai_robot_message_sent(message: dict[str, Any]) -> dict[str, Any]:
    ts = now_ts()
    payload = {"status": "sent", "summary": "sent by robot sender", "error": "", "sent_at": ts, "updated_at": ts}
    if brand_database_backend() == "supabase":
        row = _brand_supabase().update("ai_robot_messages", payload, filters={"id": message["id"]})
        _invalidate_supabase_read_cache("ai_robot_messages")
        return row
    with connect() as conn:
        conn.execute(
            """
            UPDATE ai_robot_messages
            SET status = ?, summary = ?, error = ?, sent_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (payload["status"], payload["summary"], payload["error"], payload["sent_at"], payload["updated_at"], message["id"]),
        )
        _enqueue_sync_row(conn, "ai_robot_messages", "id = ?", (int(message["id"]),))
    return get_ai_robot_message(int(message["id"])) or {}


def _mark_ai_robot_message_failed(message: dict[str, Any], error: str) -> dict[str, Any]:
    ts = now_ts()
    retry_count = int(message.get("retry_count") or 0) + 1
    status = "failed" if retry_count >= AI_ROBOT_RETRY_LIMIT else "retry"
    summary = "robot sender failed; retry scheduled" if status == "retry" else "robot sender failed; retry limit reached"
    safe_error = _redact_ai_robot_error(error)
    payload = {
        "status": status,
        "summary": summary,
        "error": safe_error[:1000],
        "retry_count": retry_count,
        "last_attempt_at": ts,
        "updated_at": ts,
    }
    if brand_database_backend() == "supabase":
        row = _brand_supabase().update("ai_robot_messages", payload, filters={"id": message["id"]})
        _invalidate_supabase_read_cache("ai_robot_messages")
        return row
    with connect() as conn:
        conn.execute(
            """
            UPDATE ai_robot_messages
            SET status = ?, summary = ?, error = ?, retry_count = ?, last_attempt_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, summary, error[:1000], retry_count, ts, ts, message["id"]),
        )
        _enqueue_sync_row(conn, "ai_robot_messages", "id = ?", (int(message["id"]),))
    return get_ai_robot_message(int(message["id"])) or {}


def _redact_ai_robot_error(error: str) -> str:
    text = str(error or "")
    text = re.sub(r"bot[^/\s]+/", "bot***REDACTED***/", text)
    return re.sub(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b", "***TOKEN***", text)


def resolve_telegram_bot_setup(token: str) -> dict[str, Any]:
    bot_token = str(token or "").strip()
    if not bot_token:
        raise ValueError("telegram bot token is required")
    timeout = float(os.getenv("AI_ROBOT_SEND_TIMEOUT", "10") or 10)
    me_response = requests.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=timeout)
    me_payload = _telegram_json(me_response)
    username = str(((me_payload.get("result") or {}).get("username")) or "")
    updates_response = requests.get(f"https://api.telegram.org/bot{bot_token}/getUpdates", timeout=timeout)
    updates_payload = _telegram_json(updates_response)
    updates = updates_payload.get("result") if isinstance(updates_payload.get("result"), list) else []
    chat = next(
        (
            item.get("message", {}).get("chat")
            or item.get("channel_post", {}).get("chat")
            or item.get("my_chat_member", {}).get("chat")
            for item in updates
            if isinstance(item, dict)
        ),
        None,
    )
    chat_id = str((chat or {}).get("id") or "")
    return {
        "ok": True,
        "username": username,
        "chat_id": chat_id,
        "webhook_url": f"https://api.telegram.org/bot{bot_token}/sendMessage",
    }


def _telegram_json(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception as exc:
        raise ValueError("telegram returned a non-json response") from exc
    if response.status_code >= 400 or payload.get("ok") is False:
        raise ValueError(str(payload.get("description") or f"telegram returned {response.status_code}"))
    return payload


def _matrix_publish_success_counts() -> dict[int, int]:
    path = get_paths().runtime_root / "matrix_publish_state.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    runs = payload.get("runs") if isinstance(payload, dict) else []
    if not isinstance(runs, list):
        runs = []
    consumed = payload.get("consumed") if isinstance(payload, dict) else []
    if not isinstance(consumed, list):
        consumed = []
    counts: dict[int, int] = {}
    seen: set[tuple[int, str, str, str]] = set()

    def success_key(item: dict[str, Any]) -> tuple[int, str, str, str] | None:
        try:
            account_id = int(item.get("account_id") or 0)
        except Exception:
            account_id = 0
        if account_id <= 0:
            return None
        platform = str(item.get("platform") or "wechat").strip() or "wechat"
        publish_date = str(item.get("publish_date") or item.get("finished_at") or "").strip()
        asset_key = str(item.get("asset_key") or item.get("prepared_video") or item.get("video") or item.get("workspace") or "").strip()
        return (account_id, platform, publish_date, asset_key)

    def record_has_publish_evidence(item: dict[str, Any]) -> bool:
        if item.get("evidence_ok") is True:
            return True
        workspace = str(item.get("workspace") or "").strip()
        if not workspace:
            return not consumed and "asset_key" not in item and "prepared_video" not in item and "video" not in item
        platform = str(item.get("platform") or "wechat").strip() or "wechat"
        token = "wechat" if platform == "wechat" else platform
        evidence = Path(workspace) / f"uploaded_records_{token}.jsonl"
        try:
            return evidence.exists() and evidence.stat().st_size > 0
        except OSError:
            return False

    for item in consumed:
        if not isinstance(item, dict) or not bool(item.get("success")):
            continue
        key = success_key(item)
        if key is None or key in seen:
            continue
        seen.add(key)
        counts[key[0]] = counts.get(key[0], 0) + 1

    for item in runs:
        if not isinstance(item, dict) or not bool(item.get("success")) or not record_has_publish_evidence(item):
            continue
        key = success_key(item)
        if key is None or key in seen:
            continue
        seen.add(key)
        counts[key[0]] = counts.get(key[0], 0) + 1
    return counts


def _matrix_publish_success_counts_for_backend() -> dict[int, int]:
    if brand_database_backend() == "supabase":
        return {}
    return _matrix_publish_success_counts()


def _profile_debug_port_from_seed(conn, account_key: str) -> int:
    preferred = stable_debug_port(account_key, "account")
    used = {
        int(row["debug_port"])
        for row in conn.execute("SELECT debug_port FROM browser_profiles")
        if row["debug_port"] is not None
    }
    span = DEBUG_PORT_END - DEBUG_PORT_START + 1
    for offset in range(span):
        candidate = DEBUG_PORT_START + ((preferred - DEBUG_PORT_START + offset) % span)
        if candidate not in used:
            return candidate
    raise RuntimeError("no free browser debug port is available")


def _profile_debug_port_supabase(account_key: str) -> int:
    preferred = stable_debug_port(account_key, "account")
    client = _brand_supabase()
    used = {
        int(row["debug_port"])
        for row in client.select("browser_profiles")
        if row.get("debug_port") is not None
    }
    span = DEBUG_PORT_END - DEBUG_PORT_START + 1
    for offset in range(span):
        candidate = DEBUG_PORT_START + ((preferred - DEBUG_PORT_START + offset) % span)
        if candidate not in used:
            return candidate
    raise RuntimeError("no free browser debug port is available")


def _decode_platform_profile(platform: dict[str, Any]) -> dict[str, Any]:
    data = dict(platform)
    fingerprint = _json_payload(data.get("fingerprint_json"), {})
    if not fingerprint and data.get("account_key"):
        fingerprint = build_browser_fingerprint(str(data["account_key"]), "account")
    data["fingerprint"] = fingerprint
    if str(data.get("login_status") or "").strip().lower() == "unknown":
        data["login_status"] = "ready"
    return data


def _profile_for_account_from_rows(account_id: int, platforms: list[dict[str, Any]], profiles: list[dict[str, Any]]) -> dict[str, Any] | None:
    platform_ids = {item.get("id") for item in platforms}
    for profile in profiles:
        if profile.get("account_id") == account_id:
            return profile
    for profile in profiles:
        if profile.get("account_platform_id") in platform_ids:
            return profile
    return None


def _profile_dir_matches_account(profile_dir: str, account_key: str) -> bool:
    expected = str(profile_dir_for(account_key))
    actual = str(profile_dir or "").strip()
    if not actual:
        return False
    try:
        expected_norm = engine._normalize_user_data_dir(expected)  # type: ignore[attr-defined]
        actual_norm = engine._normalize_user_data_dir(actual)  # type: ignore[attr-defined]
    except Exception:
        expected_norm = expected.rstrip("\\/")
        actual_norm = actual.rstrip("\\/")
        if os.name == "nt":
            expected_norm = expected_norm.lower()
            actual_norm = actual_norm.lower()
    return actual_norm == expected_norm


def _video_key(path: Path) -> str:
    stat = path.stat()
    return f"{path.name}|{stat.st_size}|{int(stat.st_mtime)}"


def _remaining_material_video_count() -> int:
    try:
        material_dir = resolve_material_dir()
    except Exception:
        return 0
    if not material_dir.exists():
        return 0
    state_path = get_paths().runtime_root / "matrix_publish_state.json"
    used: set[str] = set()
    if state_path.exists():
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8-sig"))
            if isinstance(payload, dict):
                used = set(str(item) for item in payload.get("used_videos", []))
        except Exception:
            used = set()
    remaining = 0
    for path in material_dir.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in VIDEO_EXTENSIONS:
            continue
        try:
            key = _video_key(path)
        except OSError:
            continue
        if key not in used:
            remaining += 1
    return remaining


def open_material_directory(raw_path: str) -> dict[str, Any]:
    material_dir = resolve_material_dir(material_dir_override=raw_path)
    return _open_directory(material_dir)


def _open_directory(path: Path) -> dict[str, Any]:
    path.mkdir(parents=True, exist_ok=True)
    if sys.platform.startswith("win"):
        os.startfile(str(path))  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    else:
        subprocess.Popen(["xdg-open", str(path)])
    return {"ok": True, "path": str(path)}


def open_system_directory(kind: str) -> dict[str, Any]:
    paths = get_paths()
    targets = {
        "materials": paths.runtime_root / "video_matrix" / "incoming",
        "output": resolve_video_matrix_output_root(),
        "logs": paths.runtime_root / "video_matrix" / "logs",
        "cache": paths.runtime_root / "video_matrix" / "web_uploads",
    }
    if kind not in targets:
        raise ValueError("unknown system directory")
    return _open_directory(targets[kind])


def database_dictionary() -> dict[str, Any]:
    backend = brand_database_backend()
    if backend == "supabase":
        schema_path = get_paths().repo_root / "config" / "supabase" / "brand_baseline.sql"
        return {
            "backend": backend,
            "source": str(schema_path),
            "tables": _parse_supabase_schema(schema_path.read_text(encoding="utf-8") if schema_path.exists() else ""),
        }
    with connect() as conn:
        tables = []
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for row in rows:
            name = row["name"]
            columns = []
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall():
                constraints = []
                if column["pk"]:
                    constraints.append("PRIMARY KEY")
                if column["notnull"]:
                    constraints.append("NOT NULL")
                if column["dflt_value"] is not None:
                    constraints.append(f"DEFAULT {column['dflt_value']}")
                columns.append({
                    "name": column["name"],
                    "type": column["type"] or "unknown",
                    "constraints": " ".join(constraints),
                })
            tables.append({"name": name, "columns": columns})
    return {"backend": backend, "source": "sqlite", "tables": tables}


def _parse_supabase_schema(sql: str) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    pattern = re.compile(r"create\s+table\s+if\s+not\s+exists\s+([a-zA-Z0-9_]+)\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)
    for match in pattern.finditer(sql):
        table_name = match.group(1)
        columns = []
        for raw_line in match.group(2).splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            token = line.split(None, 1)[0].strip('"')
            if token.lower() in {"constraint", "primary", "foreign", "unique", "check"} or token.lower().startswith(("unique(", "primary key", "foreign key", "check(")):
                continue
            rest = line.split(None, 1)[1] if len(line.split(None, 1)) > 1 else ""
            type_match = re.match(r"([a-zA-Z0-9_]+(?:\s*\([^)]*\))?(?:\[\])?)\s*(.*)", rest, re.DOTALL)
            columns.append({
                "name": token,
                "type": type_match.group(1).strip() if type_match else rest,
                "constraints": type_match.group(2).strip() if type_match else "",
            })
        tables.append({"name": table_name, "columns": columns})
    return tables


def list_accounts() -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        if _supabase_read_cache_peek("accounts"):
            return _supabase_read_cache_get("accounts")
        publish_success_counts = _matrix_publish_success_counts_for_backend()
        client = _brand_supabase()
        accounts = client.select("matrix_accounts", order="id.desc")
        profiles = client.select("browser_profiles")
        platforms_by_account: dict[int, list[dict[str, Any]]] = {}
        for platform in client.select("account_platforms", order="platform.asc"):
            account_id = int(platform.get("account_id") or 0)
            platforms_by_account.setdefault(account_id, []).append(platform)
        profiles_by_account: dict[int, dict[str, Any]] = {}
        profiles_by_platform: dict[int, dict[str, Any]] = {}
        for profile in profiles:
            account_id = int(profile.get("account_id") or 0)
            if account_id:
                profiles_by_account[account_id] = profile
            platform_id = int(profile.get("account_platform_id") or 0)
            if platform_id:
                profiles_by_platform[platform_id] = profile
        for account in accounts:
            account_id = int(account["id"])
            platforms = [dict(item) for item in platforms_by_account.get(account_id, [])]
            profile = profiles_by_account.get(account_id)
            for platform in platforms:
                platform["account_key"] = account.get("account_key")
                platform_profile = profiles_by_platform.get(int(platform.get("id") or 0), profile)
                if platform_profile:
                    platform["profile_dir"] = platform_profile.get("profile_dir", "")
                    platform["debug_port"] = platform_profile.get("debug_port")
                    platform["fingerprint_json"] = platform_profile.get("fingerprint_json", {})
                platform.update(_decode_platform_profile(platform))
            account["platforms"] = platforms
            account["publish_success_count"] = publish_success_counts.get(account_id, 0)
        return _cache_supabase_read("accounts", accounts)
    ensure_database()
    publish_success_counts = _matrix_publish_success_counts()
    with connect() as conn:
        accounts = [dict_from_row(row) for row in conn.execute("SELECT * FROM matrix_accounts ORDER BY id DESC")]
        for account in accounts:
            account.update(_normalize_account_runtime(account))
            platforms = conn.execute(
                """
                SELECT ap.*, bp.profile_dir, bp.debug_port, bp.fingerprint_json
                FROM account_platforms ap
                LEFT JOIN browser_profiles bp ON bp.account_id = ap.account_id
                WHERE ap.account_id = ?
                ORDER BY ap.platform
                """,
                (account["id"],),
            ).fetchall()
            account["platforms"] = [_decode_platform_profile(dict_from_row(row)) for row in platforms]
            account["publish_success_count"] = publish_success_counts.get(int(account["id"]), 0)
        return accounts


def get_account(account_id: int) -> dict[str, Any] | None:
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        account = client.select_one("matrix_accounts", filters={"id": account_id})
        if account is None:
            return None
        platforms = client.select("account_platforms", filters={"account_id": account_id}, order="platform.asc")
        profile = _profile_for_account_from_rows(int(account_id), platforms, client.select("browser_profiles"))
        for platform in platforms:
            platform["account_key"] = account.get("account_key")
            if profile:
                platform["profile_dir"] = profile.get("profile_dir", "")
                platform["debug_port"] = profile.get("debug_port")
                platform["fingerprint_json"] = profile.get("fingerprint_json", {})
            platform.update(_decode_platform_profile(platform))
        account["platforms"] = platforms
        account["publish_success_count"] = 0
        return account
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM matrix_accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            return None
        account = dict_from_row(row)
        account.update(_normalize_account_runtime(account))
        account["publish_success_count"] = _matrix_publish_success_counts().get(int(account_id), 0)
        account["platforms"] = [
            _decode_platform_profile(dict_from_row(item))
            for item in conn.execute(
                """
                SELECT ap.*, bp.profile_dir, bp.debug_port, bp.fingerprint_json
                FROM account_platforms ap
                LEFT JOIN browser_profiles bp ON bp.account_id = ap.account_id
                WHERE ap.account_id = ?
                ORDER BY ap.platform
                """,
                (account_id,),
            )
        ]
        return account


def create_account(payload: dict[str, Any]) -> dict[str, Any]:
    if brand_database_backend() == "supabase":
        ts = now_ts()
        account_key = _account_slug(str(payload.get("account_key") or payload.get("display_name") or ""))
        display_name = str(payload.get("display_name") or account_key).strip()
        platforms = payload.get("platforms")
        if not isinstance(platforms, list) or not platforms:
            platforms = ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili", "tiktok", "x"]
        normalized_platforms: list[str] = []
        seen_platforms: set[str] = set()
        for platform in platforms:
            token = normalize_platform(str(platform))
            capability = get_platform(token)
            if capability is None:
                raise ValueError(f"unsupported platform: {platform}")
            if token in seen_platforms:
                continue
            seen_platforms.add(token)
            normalized_platforms.append(token)
        client = _brand_supabase()
        account = client.insert(
            "matrix_accounts",
            {
                "account_key": account_key,
                "display_name": display_name,
                "niche": _normalize_account_niche({"niche": payload.get("niche")}),
                "status": str(payload.get("status") or "active").strip() or "active",
                "notes": str(payload.get("notes") or "").strip(),
                "created_at": ts,
                "updated_at": ts,
            },
        )
        account_id = int(account.get("id") or 0)
        if account_id <= 0:
            raise RuntimeError("supabase create_account did not return account id")
        first_platform_id = 0
        for platform in normalized_platforms:
            platform_row = client.insert(
                "account_platforms",
                {
                    "account_id": account_id,
                    "platform": platform,
                    "handle": "",
                    "capability_status": "registered",
                    "created_at": ts,
                    "updated_at": ts,
                },
            )
            if first_platform_id <= 0:
                first_platform_id = int(platform_row.get("id") or 0)
        if first_platform_id <= 0 and normalized_platforms:
            first_platform = client.select_one(
                "account_platforms",
                filters={
                    "account_id": account_id,
                    "platform": normalized_platforms[0],
                },
            )
            first_platform_id = int((first_platform or {}).get("id") or 0)
        profile_dir = profile_dir_for(account_key)
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_payload = {
            "account_id": account_id,
            "profile_dir": str(profile_dir),
            "debug_port": _profile_debug_port_supabase(account_key),
            "fingerprint_json": build_browser_fingerprint(account_key, "account"),
            "created_at": ts,
            "updated_at": ts,
        }
        try:
            client.insert("browser_profiles", profile_payload)
        except SupabaseError:
            if first_platform_id <= 0:
                raise
            client.insert(
                "browser_profiles",
                {
                    "account_platform_id": first_platform_id,
                    "profile_dir": profile_payload["profile_dir"],
                    "debug_port": profile_payload["debug_port"],
                    "fingerprint_json": profile_payload["fingerprint_json"],
                    "created_at": ts,
                    "updated_at": ts,
                },
            )
        _invalidate_supabase_read_cache("accounts", "dashboard_summary")
        return get_account(account_id) or {
            "id": account_id,
            "account_key": account_key,
            "display_name": display_name,
            "niche": _normalize_account_niche({"niche": payload.get("niche")}),
            "status": str(payload.get("status") or "active").strip() or "active",
            "notes": str(payload.get("notes") or "").strip(),
            "created_at": account.get("created_at", ts),
            "updated_at": account.get("updated_at", ts),
            "platforms": [],
            "publish_success_count": 0,
        }
    ensure_database()
    ts = now_ts()
    account_key = _account_slug(str(payload.get("account_key") or payload.get("display_name") or ""))
    display_name = str(payload.get("display_name") or account_key).strip()
    platforms = payload.get("platforms")
    if not isinstance(platforms, list) or not platforms:
        platforms = ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili", "tiktok", "x"]
    with connect() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO matrix_accounts(account_key, display_name, niche, status, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_key,
                    display_name,
                    _normalize_account_niche({"niche": payload.get("niche")}),
                    str(payload.get("status") or "active").strip() or "active",
                    str(payload.get("notes") or "").strip(),
                    ts,
                    ts,
                ),
            )
            account_id = int(cursor.lastrowid)
        except sqlite3.IntegrityError:
            existing = conn.execute("SELECT id FROM matrix_accounts WHERE account_key = ?", (account_key,)).fetchone()
            if existing is None:
                raise
            account_id = int(existing["id"])
        for platform in platforms:
            ensure_account_platform(conn, account_id, str(platform))
        _enqueue_sync_row(conn, "matrix_accounts", "id = ?", (account_id,))
    return get_account(account_id) or {}


def repair_account_configs() -> dict[str, Any]:
    required_platforms = [item.key for item in SUPPORTED_PLATFORMS]
    checked_accounts = 0
    repaired_accounts = 0
    created_platforms = 0
    created_profiles = 0
    updated_profiles = 0
    repaired_account_ids: list[int] = []

    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        accounts = client.select("matrix_accounts", order="id.asc")
        platform_rows = client.select("account_platforms", order="account_id.asc,platform.asc")
        profile_rows = client.select("browser_profiles")
        platforms_by_account: dict[int, set[str]] = {}
        platform_id_to_account: dict[int, int] = {}
        for row in platform_rows:
            account_id = int(row.get("account_id") or 0)
            if account_id <= 0:
                continue
            token = normalize_platform(str(row.get("platform") or ""))
            if token:
                platforms_by_account.setdefault(account_id, set()).add(token)
            if row.get("id") is not None:
                platform_id_to_account[int(row["id"])] = account_id
        profiles_by_account: dict[int, dict[str, Any]] = {}
        for row in profile_rows:
            account_id = int(row.get("account_id") or 0)
            if account_id <= 0 and row.get("account_platform_id") is not None:
                account_id = platform_id_to_account.get(int(row["account_platform_id"]), 0)
            if account_id > 0:
                profiles_by_account[account_id] = row
        for account in accounts:
            account_id = int(account.get("id") or 0)
            if account_id <= 0:
                continue
            checked_accounts += 1
            existing_platforms = platforms_by_account.get(account_id, set())
            missing_platforms = [platform for platform in required_platforms if platform not in existing_platforms]
            profile = profiles_by_account.get(account_id)
            profile_missing = profile is None
            profile_fingerprint_missing = bool(profile) and not _json_payload(profile.get("fingerprint_json"), {})
            profile_dir_mismatch = bool(profile) and not _profile_dir_matches_account(str(profile.get("profile_dir") or ""), str(account.get("account_key") or ""))
            if not missing_platforms and not profile_missing and not profile_fingerprint_missing and not profile_dir_mismatch:
                continue
            repaired_accounts += 1
            repaired_account_ids.append(account_id)
            for platform in missing_platforms:
                ensure_account_platform(None, account_id, platform)
                created_platforms += 1
            if (profile_missing or profile_fingerprint_missing or profile_dir_mismatch) and not missing_platforms:
                ensure_account_platform(None, account_id, next(iter(existing_platforms), required_platforms[0]))
            if profile_missing:
                created_profiles += 1
            elif profile_fingerprint_missing or profile_dir_mismatch:
                updated_profiles += 1
        if repaired_accounts:
            _invalidate_supabase_read_cache("accounts", "dashboard_summary")
        return {
            "ok": True,
            "checked_accounts": checked_accounts,
            "repaired_accounts": repaired_accounts,
            "created_platforms": created_platforms,
            "created_profiles": created_profiles,
            "updated_profiles": updated_profiles,
            "required_platforms": len(required_platforms),
            "repaired_account_ids": repaired_account_ids,
        }

    ensure_database()
    with connect() as conn:
        accounts = conn.execute("SELECT id, account_key, display_name, niche FROM matrix_accounts ORDER BY id ASC").fetchall()
        for raw_account in accounts:
            account = _normalize_account_runtime(dict(raw_account))
            account_id = int(account["id"])
            checked_accounts += 1
            existing_platforms = {
                normalize_platform(str(row["platform"] or ""))
                for row in conn.execute("SELECT platform FROM account_platforms WHERE account_id = ?", (account_id,))
            }
            missing_platforms = [platform for platform in required_platforms if platform not in existing_platforms]
            profile = conn.execute("SELECT * FROM browser_profiles WHERE account_id = ?", (account_id,)).fetchone()
            profile_missing = profile is None
            profile_fingerprint_missing = bool(profile) and not _json_payload(profile["fingerprint_json"], {})
            profile_dir_mismatch = bool(profile) and not _profile_dir_matches_account(str(profile["profile_dir"] or ""), str(account.get("account_key") or ""))
            if not missing_platforms and not profile_missing and not profile_fingerprint_missing and not profile_dir_mismatch:
                continue
            repaired_accounts += 1
            repaired_account_ids.append(account_id)
            for platform in missing_platforms:
                ensure_account_platform(conn, account_id, platform)
                created_platforms += 1
            if (profile_missing or profile_fingerprint_missing or profile_dir_mismatch) and not missing_platforms:
                ensure_account_platform(conn, account_id, next(iter(existing_platforms), required_platforms[0]))
            if profile_missing:
                created_profiles += 1
            elif profile_fingerprint_missing or profile_dir_mismatch:
                updated_profiles += 1
    return {
        "ok": True,
        "checked_accounts": checked_accounts,
        "repaired_accounts": repaired_accounts,
        "created_platforms": created_platforms,
        "created_profiles": created_profiles,
        "updated_profiles": updated_profiles,
        "required_platforms": len(required_platforms),
        "repaired_account_ids": repaired_account_ids,
    }


def update_account(account_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    if brand_database_backend() == "supabase":
        allowed = {"display_name", "niche", "status", "notes"}
        update = {key: str(payload.get(key) or "").strip() for key in allowed if key in payload}
        if update:
            update["updated_at"] = now_ts()
            _brand_supabase().update("matrix_accounts", update, filters={"id": account_id})
            _invalidate_supabase_read_cache("accounts")
        return get_account(account_id)
    ensure_database()
    allowed = {"display_name", "niche", "status", "notes"}
    assignments: list[str] = []
    values: list[Any] = []
    for key in allowed:
        if key in payload:
            assignments.append(f"{key} = ?")
            values.append(str(payload.get(key) or "").strip())
    if assignments:
        assignments.append("updated_at = ?")
        values.append(now_ts())
        values.append(account_id)
        with connect() as conn:
            conn.execute(f"UPDATE matrix_accounts SET {', '.join(assignments)} WHERE id = ?", values)
            _enqueue_sync_row(conn, "matrix_accounts", "id = ?", (account_id,))
    return get_account(account_id)


def delete_account(account_id: int) -> bool:
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        account = client.select_one("matrix_accounts", filters={"id": account_id})
        if account is None:
            return False
        platforms = client.select("account_platforms", filters={"account_id": account_id})
        platform_ids = [int(item["id"]) for item in platforms if item.get("id") is not None]
        client.delete("automation_tasks", filters={"account_id": account_id})
        client.delete("video_stats_snapshots", filters={"account_id": account_id})
        try:
            client.delete("wechat_stats_account_snapshots", filters={"account_id": account_id})
        except SupabaseError:
            pass
        try:
            client.delete("browser_profiles", filters={"account_id": account_id})
        except SupabaseError:
            for platform_id in platform_ids:
                client.delete("browser_profiles", filters={"account_platform_id": platform_id})
        client.delete("account_platforms", filters={"account_id": account_id})
        client.delete("matrix_accounts", filters={"id": account_id})
        _invalidate_supabase_read_cache("accounts", "dashboard_summary", "stats:all", "stats:accounts")
        return True
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT id FROM matrix_accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            return False
        conn.execute("DELETE FROM automation_tasks WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM video_stats_snapshots WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM wechat_stats_account_snapshots WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM browser_profiles WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM account_platforms WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM matrix_accounts WHERE id = ?", (account_id,))
        _enqueue_sync_delete(conn, "automation_tasks", {"account_id": account_id}, entity_id=f"account_id={account_id}")
        _enqueue_sync_delete(conn, "video_stats_snapshots", {"account_id": account_id}, entity_id=f"account_id={account_id}")
        _enqueue_sync_delete(conn, "wechat_stats_account_snapshots", {"account_id": account_id}, entity_id=f"account_id={account_id}")
        _enqueue_sync_delete(conn, "browser_profiles", {"account_id": account_id}, entity_id=f"account_id={account_id}")
        _enqueue_sync_delete(conn, "account_platforms", {"account_id": account_id}, entity_id=f"account_id={account_id}")
        _enqueue_sync_delete(conn, "matrix_accounts", {"id": account_id}, entity_id=str(account_id))
    return True


def ensure_account_platform(conn, account_id: int, platform: str, handle: str = "") -> dict[str, Any]:
    token = normalize_platform(platform)
    capability = get_platform(token)
    if capability is None:
        raise ValueError(f"unsupported platform: {platform}")
    ts = now_ts()
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        account = client.select_one("matrix_accounts", filters={"id": account_id})
        if account is None:
            raise ValueError("account not found")
        existing = client.select_one("account_platforms", filters={"account_id": account_id, "platform": token})
        dirty = existing is None
        if existing is None:
            ap = client.insert(
                "account_platforms",
                {
                    "account_id": account_id,
                    "platform": token,
                    "handle": handle,
                    "capability_status": "registered",
                    "created_at": ts,
                    "updated_at": ts,
                },
            )
        else:
            ap = existing
        profile_dir = profile_dir_for(str(account["account_key"]))
        profile_dir.mkdir(parents=True, exist_ok=True)
        account_platforms = client.select("account_platforms", filters={"account_id": account_id})
        profile = _profile_for_account_from_rows(account_id, account_platforms, client.select("browser_profiles"))
        fingerprint = build_browser_fingerprint(str(account["account_key"]), "account")
        if profile is None:
            payload = {
                "account_id": account_id,
                "profile_dir": str(profile_dir),
                "debug_port": _profile_debug_port_supabase(str(account["account_key"])),
                "fingerprint_json": fingerprint,
                "created_at": ts,
                "updated_at": ts,
            }
            try:
                client.insert("browser_profiles", payload)
            except Exception:
                client.insert(
                    "browser_profiles",
                    {
                        "account_platform_id": ap["id"],
                        "profile_dir": payload["profile_dir"],
                        "debug_port": payload["debug_port"],
                        "fingerprint_json": payload["fingerprint_json"],
                        "created_at": ts,
                        "updated_at": ts,
                    },
                )
            dirty = True
        else:
            update: dict[str, Any] = {"updated_at": ts}
            if not _json_payload(profile.get("fingerprint_json"), {}):
                update["fingerprint_json"] = fingerprint
            if not _profile_dir_matches_account(str(profile.get("profile_dir") or ""), str(account["account_key"])):
                update["profile_dir"] = str(profile_dir)
            if len(update) > 1:
                if profile.get("account_id") == account_id:
                    client.update("browser_profiles", update, filters={"account_id": account_id})
                else:
                    client.update("browser_profiles", update, filters={"id": profile["id"]})
                dirty = True
        if dirty:
            _invalidate_supabase_read_cache("accounts")
        return ap
    conn.execute(
        """
        INSERT OR IGNORE INTO account_platforms(account_id, platform, handle, capability_status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (account_id, token, handle, "registered", ts, ts),
    )
    ap = conn.execute(
        "SELECT ap.*, ma.account_key FROM account_platforms ap JOIN matrix_accounts ma ON ma.id = ap.account_id WHERE ap.account_id = ? AND ap.platform = ?",
        (account_id, token),
    ).fetchone()
    if ap is None:
        raise RuntimeError("account platform was not created")
    profile_dir = profile_dir_for(str(ap["account_key"]))
    fingerprint = build_browser_fingerprint(str(ap["account_key"]), "account")
    conn.execute(
        """
        INSERT OR IGNORE INTO browser_profiles(account_id, profile_dir, debug_port, fingerprint_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            account_id,
            str(profile_dir),
            _profile_debug_port_from_seed(conn, str(ap["account_key"])),
            json.dumps(fingerprint, ensure_ascii=False),
            ts,
            ts,
        ),
    )
    conn.execute(
        "UPDATE browser_profiles SET fingerprint_json = ? WHERE account_id = ? AND (fingerprint_json IS NULL OR fingerprint_json = '' OR fingerprint_json = '{}')",
        (json.dumps(fingerprint, ensure_ascii=False), account_id),
    )
    profile = conn.execute("SELECT * FROM browser_profiles WHERE account_id = ?", (account_id,)).fetchone()
    if profile is not None and not _profile_dir_matches_account(str(profile["profile_dir"] or ""), str(ap["account_key"])):
        conn.execute(
            "UPDATE browser_profiles SET profile_dir = ?, updated_at = ? WHERE account_id = ?",
            (str(profile_dir), ts, account_id),
        )
    profile_dir.mkdir(parents=True, exist_ok=True)
    _enqueue_sync_row(conn, "account_platforms", "account_id = ? AND platform = ?", (account_id, token))
    _enqueue_sync_row(conn, "browser_profiles", "account_id = ?", (account_id,))
    return dict_from_row(ap)


def profile_dir_for(account_key: str, platform: str | None = None) -> Path:
    return get_paths().profiles_root / _account_slug(account_key)


def _account_browser_marker_payload(
    account: dict[str, Any],
    platform_profile: dict[str, Any],
    capability: Any,
    accent_override: str | None = None,
    title_badge: str = "",
    terminal_window_id: int = 0,
) -> dict[str, Any]:
    account_id = int(account.get("id") or 0)
    display_name = str(account.get("display_name") or account.get("account_key") or f"Account {account_id}").strip()
    account_key = str(account.get("account_key") or "").strip()
    platform_label = str(getattr(capability, "label", "") or getattr(capability, "key", "") or "").strip()
    operator_wechat = _account_operator_wechat(account)
    window_id = int(terminal_window_id or 0)
    window_label = f"终端执行窗口 {window_id:02d}" if window_id > 0 else ""
    palette = ["#EF4444", "#EAB308", "#22C55E", "#3B82F6", "#F97316"]
    seed = account_id or sum(ord(ch) for ch in account_key or display_name)
    accent = str(accent_override or "").strip()
    accent = TERMINAL_LEGACY_COLOR_ALIASES.get(accent.upper(), accent)
    if not re.fullmatch(r"#[0-9a-fA-F]{6}", accent):
        accent = palette[seed % len(palette)]
    return {
        "account_id": account_id,
        "title": display_name,
        "platform": platform_label,
        "operator_wechat": operator_wechat,
        "meta": "",
        "accent": accent,
        "title_badge": str(title_badge or "").strip(),
        "window_label": window_label,
    }


def _account_browser_marker_script(payload: dict[str, Any]) -> str:
    marker = json.dumps(payload, ensure_ascii=False)
    return f"""
(() => {{
  const marker = {marker};
  const install = () => {{
    if (!document.documentElement) return false;
    try {{
      const observer = window.__gasgxAccountMarkerObserver;
      if (observer && typeof observer.disconnect === 'function') observer.disconnect();
    }} catch (_error) {{}}
    window.__gasgxAccountMarkerObserver = null;
    document.querySelectorAll('#gasgx-account-marker').forEach((el) => el.remove());
    const badge = String(marker.title_badge || '').trim();
    const base = marker.title ? `GasGx-${{marker.title}}` : 'GasGx';
    const prefix = badge ? `[${{badge}} ${{base}}] ` : `[${{base}}] `;
    if (document.title && !document.title.startsWith(prefix)) {{
      document.title = `${{prefix}}${{document.title.replace(/^\\[[^\\]]+\\]\\s*/, '')}}`;
    }}
    return true;
  }};
  if (!install()) document.addEventListener('DOMContentLoaded', install, {{ once: true }});
}})();
""".strip()


def _chrome_cdp_page_targets(debug_port: int) -> list[dict[str, Any]]:
    for endpoint in (f"http://127.0.0.1:{debug_port}/json/list", f"http://127.0.0.1:{debug_port}/json"):
        try:
            response = requests.get(endpoint, timeout=1.5)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            continue
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            continue
        targets = [
            item
            for item in payload
            if isinstance(item, dict)
            and str(item.get("type") or "") == "page"
            and str(item.get("webSocketDebuggerUrl") or "")
        ]
        if targets:
            return targets
    return []


def _create_chrome_cdp_connection(ws_url: str, debug_port: int) -> Any:
    import websocket  # type: ignore[import-untyped]

    try:
        return websocket.create_connection(ws_url, timeout=1.5, origin=f"http://127.0.0.1:{debug_port}")
    except TypeError:
        return websocket.create_connection(ws_url, timeout=1.5)


def _cdp_send(ws: Any, message_id: int, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    ws.send(json.dumps({"id": message_id, "method": method, "params": params or {}}, ensure_ascii=False))
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        raw = ws.recv()
        payload = json.loads(raw)
        if isinstance(payload, dict) and payload.get("id") == message_id:
            return payload
    return {}


def _inject_account_browser_marker(
    account: dict[str, Any],
    platform_profile: dict[str, Any],
    capability: Any,
    accent_override: str | None = None,
    title_badge: str = "",
    terminal_window_id: int = 0,
) -> bool:
    try:
        debug_port = int(platform_profile.get("debug_port") or 0)
    except Exception:
        return False
    if debug_port <= 0:
        return False
    payload = _account_browser_marker_payload(
        account,
        platform_profile,
        capability,
        accent_override=accent_override,
        title_badge=title_badge,
        terminal_window_id=terminal_window_id,
    )
    script = _account_browser_marker_script(payload)
    applied = False
    seen_targets: set[str] = set()
    deadline = time.monotonic() + 3.0
    idle_rounds = 0
    while time.monotonic() < deadline:
        targets = _chrome_cdp_page_targets(debug_port)
        injected_this_round = False
        for target in targets:
            ws_url = str(target.get("webSocketDebuggerUrl") or "")
            if not ws_url or ws_url in seen_targets:
                continue
            ws = None
            try:
                ws = _create_chrome_cdp_connection(ws_url, debug_port)
                _cdp_send(ws, 1, "Page.enable")
                _cdp_send(ws, 2, "Page.addScriptToEvaluateOnNewDocument", {"source": script})
                _cdp_send(ws, 3, "Runtime.evaluate", {"expression": script, "returnByValue": True})
                applied = True
                injected_this_round = True
                seen_targets.add(ws_url)
            except Exception:
                continue
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
        if not injected_this_round:
            idle_rounds += 1
            if applied and idle_rounds >= 2:
                break
        else:
            idle_rounds = 0
        time.sleep(0.2)
    return applied


def open_account_browser(account_id: int, platform: str) -> dict[str, Any]:
    account = get_account(account_id)
    if account is None:
        raise KeyError("account not found")
    token = normalize_platform(platform)
    capability = get_platform(token)
    if capability is None or not capability.can_open_browser:
        return {"ok": False, "status": "unsupported", "platform": token}
    with connect() as conn:
        ensure_account_platform(conn, account_id, token)
    refreshed = get_account(account_id) or {}
    ap = next((item for item in refreshed.get("platforms", []) if item["platform"] == token), None)
    if ap is None:
        raise RuntimeError("account platform missing")
    profile_dir = Path(str(ap["profile_dir"]))
    profile_dir.mkdir(parents=True, exist_ok=True)
    with _chrome_fingerprint_env(ap.get("fingerprint")):
        engine._ensure_chrome_debug_port(
            debug_port=int(ap["debug_port"]),
            auto_open_chrome=True,
            chrome_user_data_dir=str(profile_dir),
            startup_url=capability.open_url,
        )
    marker_applied = _inject_account_browser_marker(refreshed, ap, capability)
    return {
        "ok": True,
        "platform": token,
        "debug_port": int(ap["debug_port"]),
        "profile_dir": str(profile_dir),
        "open_url": capability.open_url,
        "fingerprint": ap.get("fingerprint") or {},
        "marker_applied": marker_applied,
    }


def check_login_status(account_id: int, platform: str) -> dict[str, Any]:
    account = get_account(account_id)
    if account is None:
        raise KeyError("account not found")
    token = normalize_platform(platform)
    capability = get_platform(token)
    if capability is None or not capability.can_login_status:
        return {"ok": False, "status": "unsupported", "platform": token}
    with connect() as conn:
        ensure_account_platform(conn, account_id, token)
    refreshed = get_account(account_id) or {}
    ap = next((item for item in refreshed.get("platforms", []) if item["platform"] == token), None)
    if ap is None:
        raise RuntimeError("account platform missing")
    profile_dir = Path(str(ap["profile_dir"]))
    apply_cybercar_environment()
    with _chrome_fingerprint_env(ap.get("fingerprint")):
        result = engine.probe_platform_session_via_debug_port(
            platform_name=token,
            open_url=capability.open_url,
            debug_port=int(ap["debug_port"]),
            chrome_user_data_dir=str(profile_dir),
            disconnect_after_probe=(token != "wechat"),
            enable_wechat_keepalive=(token == "wechat"),
        )
    result.setdefault("account_id", account_id)
    result.setdefault("account_key", account.get("account_key"))
    result.setdefault("display_name", account.get("display_name"))
    result.setdefault("debug_port", int(ap["debug_port"]))
    result.setdefault("profile_dir", str(profile_dir))
    result.setdefault("fingerprint", ap.get("fingerprint") or {})
    status = str(result.get("status") or ("ready" if result.get("ok") else "unknown"))
    with connect() as conn:
        conn.execute(
            "UPDATE account_platforms SET login_status = ?, last_checked_at = ?, updated_at = ? WHERE account_id = ? AND platform = ?",
            (status, now_ts(), now_ts(), account_id, token),
        )
        _enqueue_sync_row(conn, "account_platforms", "account_id = ? AND platform = ?", (account_id, token))
    if status == "ready":
        result["resolved_login_incidents"] = resolve_login_notification_incidents(account_id, token)
    return result


def create_task(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_database()
    task_type = str(payload.get("task_type") or "").strip().lower()
    if task_type not in {"publish", "draft", "comment", "message", "stats"}:
        raise ValueError("task_type must be publish, draft, comment, message, or stats")
    account_id = payload.get("account_id")
    platform = normalize_platform(str(payload.get("platform") or ""))
    capability = get_platform(platform) if platform else None
    supported = True
    if capability is None:
        supported = task_type == "stats" and not platform
    elif task_type in {"publish", "draft"}:
        supported = capability.can_publish
    elif task_type == "comment":
        supported = capability.can_comment
    elif task_type == "message":
        supported = capability.can_message
    elif task_type == "stats":
        supported = capability.can_stats
    status = "pending" if supported else "unsupported"
    summary = "queued for manual worker execution" if supported else f"{platform or 'platform'} does not support {task_type} in phase 1"
    ts = now_ts()
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        if status in {"pending", "running"} and account_id:
            existing = client.select_where(
                "automation_tasks",
                params={
                    "account_id": f"eq.{int(account_id)}",
                    "platform": f"eq.{platform}",
                    "task_type": f"eq.{task_type}",
                    "status": "in.(pending,running)",
                },
                order="id.desc",
            )
            if existing:
                raise ValueError(f"duplicate active task already queued: #{existing[0]['id']}")
        task = client.insert(
            "automation_tasks",
            {
                "account_id": int(account_id) if account_id else None,
                "platform": platform,
                "task_type": task_type,
                "payload_json": payload.get("payload") or {},
                "status": status,
                "summary": summary,
                "created_at": ts,
                "updated_at": ts,
            },
        )
        _invalidate_supabase_read_cache("dashboard_summary")
        return task
    with connect() as conn:
        if status in {"pending", "running"} and account_id:
            existing = conn.execute(
                """
                SELECT id FROM automation_tasks
                WHERE account_id = ?
                  AND platform = ?
                  AND task_type = ?
                  AND status IN ('pending', 'running')
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(account_id), platform, task_type),
            ).fetchone()
            if existing is not None:
                raise ValueError(f"duplicate active task already queued: #{existing['id']}")
        cursor = conn.execute(
            """
            INSERT INTO automation_tasks(account_id, platform, task_type, payload_json, status, summary, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(account_id) if account_id else None,
                platform,
                task_type,
                json.dumps(payload.get("payload") or {}, ensure_ascii=False),
                status,
                summary,
                ts,
                ts,
            ),
        )
        task_id = int(cursor.lastrowid)
        _enqueue_sync_row(conn, "automation_tasks", "id = ?", (task_id,))
    return get_task(task_id) or {}


def list_tasks() -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        return _brand_supabase().select("automation_tasks", order="id.desc")
    ensure_database()
    with connect() as conn:
        return [dict_from_row(row) for row in conn.execute("SELECT * FROM automation_tasks ORDER BY id DESC LIMIT 200")]


def get_task(task_id: int) -> dict[str, Any] | None:
    if brand_database_backend() == "supabase":
        return _brand_supabase().select_one("automation_tasks", filters={"id": task_id})
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM automation_tasks WHERE id = ?", (task_id,)).fetchone()
        return dict_from_row(row) if row else None


def delete_task(task_id: int) -> bool:
    if brand_database_backend() == "supabase":
        ok = _brand_supabase().delete("automation_tasks", filters={"id": task_id})
        if ok:
            _invalidate_supabase_read_cache("dashboard_summary")
        return ok
    ensure_database()
    with connect() as conn:
        cursor = conn.execute("DELETE FROM automation_tasks WHERE id = ?", (task_id,))
        deleted = bool(cursor.rowcount)
        if deleted:
            _enqueue_sync_delete(conn, "automation_tasks", {"id": task_id}, entity_id=str(task_id))
        return deleted


def delete_tasks(task_ids: list[int]) -> int:
    ids = sorted({int(task_id) for task_id in task_ids if int(task_id) > 0})
    if not ids:
        return 0
    if brand_database_backend() == "supabase":
        deleted = 0
        client = _brand_supabase()
        for task_id in ids:
            if client.delete("automation_tasks", filters={"id": task_id}):
                deleted += 1
        if deleted:
            _invalidate_supabase_read_cache("dashboard_summary")
        return deleted
    ensure_database()
    placeholders = ",".join("?" for _ in ids)
    with connect() as conn:
        cursor = conn.execute(f"DELETE FROM automation_tasks WHERE id IN ({placeholders})", ids)
        deleted = int(cursor.rowcount or 0)
        if deleted:
            for task_id in ids:
                _enqueue_sync_delete(conn, "automation_tasks", {"id": task_id}, entity_id=str(task_id))
        return deleted


def update_tasks_status(task_ids: list[int], status: str) -> int:
    ids = sorted({int(task_id) for task_id in task_ids if int(task_id) > 0})
    normalized = str(status or "").strip().lower()
    if normalized not in {"pending", "paused"}:
        raise ValueError("status must be pending or paused")
    if not ids:
        return 0
    ts = now_ts()
    if brand_database_backend() == "supabase":
        updated = 0
        client = _brand_supabase()
        for task_id in ids:
            row = client.update("automation_tasks", {"status": normalized, "updated_at": ts}, filters={"id": task_id})
            if row:
                updated += 1
        if updated:
            _invalidate_supabase_read_cache("dashboard_summary")
        return updated
    ensure_database()
    placeholders = ",".join("?" for _ in ids)
    with connect() as conn:
        cursor = conn.execute(
            f"UPDATE automation_tasks SET status = ?, updated_at = ? WHERE id IN ({placeholders})",
            [normalized, ts, *ids],
        )
        updated = int(cursor.rowcount or 0)
        if updated:
            for task_id in ids:
                _enqueue_sync_row(conn, "automation_tasks", "id = ?", (task_id,))
        return updated


def _stats_int(value: Any) -> int:
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace(",", "").replace("，", "")
            if cleaned.endswith("万"):
                return int(float(cleaned[:-1] or 0) * 10000)
            return int(float(cleaned or 0))
        return int(value or 0)
    except Exception:
        return 0


def _stats_float(value: Any) -> float:
    try:
        if isinstance(value, str):
            return float(value.strip().replace("%", "") or 0)
        return float(value or 0)
    except Exception:
        return 0.0


def _stats_date(value: Any) -> str:
    return str(value or "").strip()[:10]


def _stats_json(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


def _normalize_account_stats_snapshot(item: dict[str, Any], *, captured_at: int | None = None) -> dict[str, Any]:
    ts = now_ts() if captured_at is None else int(captured_at)
    return {
        "account_id": int(item.get("account_id") or 0),
        "platform": normalize_platform(str(item.get("platform") or "wechat")) or "wechat",
        "stat_date": _stats_date(item.get("stat_date") or item.get("date")),
        "views": _stats_int(item.get("views")),
        "likes": _stats_int(item.get("likes")),
        "comments": _stats_int(item.get("comments")),
        "shares": _stats_int(item.get("shares")),
        "messages": _stats_int(item.get("messages")),
        "followers": _stats_int(item.get("followers")),
        "follower_delta": _stats_int(item.get("follower_delta")),
        "profile_visits": _stats_int(item.get("profile_visits")),
        "leads": _stats_int(item.get("leads")),
        "completed_rate": _stats_float(item.get("completed_rate")),
        "interaction_rate": _stats_float(item.get("interaction_rate")),
        "works_count": _stats_int(item.get("works_count")),
        "source": str(item.get("source") or "wechat_stats_capture"),
        "raw_json": _stats_json(item.get("raw_json") if "raw_json" in item else item),
        "captured_at": int(item.get("captured_at") or ts),
        "created_at": ts,
        "updated_at": ts,
    }


def _normalize_video_stats_snapshot(item: dict[str, Any], *, captured_at: int | None = None) -> dict[str, Any]:
    ts = now_ts() if captured_at is None else int(captured_at)
    video_ref = str(item.get("video_ref") or item.get("feed_id") or "").strip()
    return {
        "account_id": item.get("account_id"),
        "platform": normalize_platform(str(item.get("platform") or "wechat")),
        "video_ref": video_ref,
        "stat_date": _stats_date(item.get("stat_date") or item.get("date")),
        "title": str(item.get("title") or "").strip(),
        "feed_id": str(item.get("feed_id") or video_ref).strip(),
        "views": _stats_int(item.get("views")),
        "likes": _stats_int(item.get("likes")),
        "comments": _stats_int(item.get("comments")),
        "shares": _stats_int(item.get("shares")),
        "messages": _stats_int(item.get("messages")),
        "published_at": str(item.get("published_at") or ""),
        "source": str(item.get("source") or ""),
        "raw_json": _stats_json(item.get("raw_json") if "raw_json" in item else item),
        "captured_at": int(item.get("captured_at") or ts),
    }


def upsert_wechat_account_stats_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    ensure_database()
    payload = _normalize_account_stats_snapshot(item)
    if payload["account_id"] <= 0 or not payload["stat_date"]:
        raise ValueError("account_id and stat_date are required")
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        row = client.upsert(
            "wechat_stats_account_snapshots",
            {**payload, "raw_json": _json_payload(payload["raw_json"], {})},
            on_conflict="account_id,platform,stat_date",
        )
        _invalidate_supabase_read_cache("dashboard_summary", "stats:all", "stats:accounts")
        return row
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO wechat_stats_account_snapshots(
                account_id, platform, stat_date, views, likes, comments, shares, messages,
                followers, follower_delta, profile_visits, leads, completed_rate,
                interaction_rate, works_count, source, raw_json, captured_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, platform, stat_date) DO UPDATE SET
                views = excluded.views,
                likes = excluded.likes,
                comments = excluded.comments,
                shares = excluded.shares,
                messages = excluded.messages,
                followers = excluded.followers,
                follower_delta = excluded.follower_delta,
                profile_visits = excluded.profile_visits,
                leads = excluded.leads,
                completed_rate = excluded.completed_rate,
                interaction_rate = excluded.interaction_rate,
                works_count = excluded.works_count,
                source = excluded.source,
                raw_json = excluded.raw_json,
                captured_at = excluded.captured_at,
                updated_at = excluded.updated_at
            """,
            (
                payload["account_id"],
                payload["platform"],
                payload["stat_date"],
                payload["views"],
                payload["likes"],
                payload["comments"],
                payload["shares"],
                payload["messages"],
                payload["followers"],
                payload["follower_delta"],
                payload["profile_visits"],
                payload["leads"],
                payload["completed_rate"],
                payload["interaction_rate"],
                payload["works_count"],
                payload["source"],
                payload["raw_json"],
                payload["captured_at"],
                payload["created_at"],
                payload["updated_at"],
            ),
        )
        _enqueue_sync_row(
            conn,
            "wechat_stats_account_snapshots",
            "account_id = ? AND platform = ? AND stat_date = ?",
            (payload["account_id"], payload["platform"], payload["stat_date"]),
        )
    return payload


def _delete_matching_video_snapshot(client: SupabaseRestClient, payload: dict[str, Any]) -> None:
    if payload.get("stat_date") and payload.get("video_ref") and payload.get("account_id"):
        client.delete(
            "video_stats_snapshots",
            filters={
                "account_id": payload["account_id"],
                "platform": payload["platform"],
                "stat_date": payload["stat_date"],
                "video_ref": payload["video_ref"],
            },
        )


def _insert_video_stats_snapshot_sqlite(conn, payload: dict[str, Any]) -> int:
    if payload.get("stat_date") and payload.get("video_ref") and payload.get("account_id"):
        conn.execute(
            """
            DELETE FROM video_stats_snapshots
            WHERE account_id = ? AND platform = ? AND stat_date = ? AND video_ref = ?
            """,
            (payload["account_id"], payload["platform"], payload["stat_date"], payload["video_ref"]),
        )
    cursor = conn.execute(
        """
        INSERT INTO video_stats_snapshots(
            account_id, platform, video_ref, stat_date, title, feed_id, views, likes,
            comments, shares, messages, published_at, source, raw_json, captured_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["account_id"],
            payload["platform"],
            payload["video_ref"],
            payload["stat_date"],
            payload["title"],
            payload["feed_id"],
            payload["views"],
            payload["likes"],
            payload["comments"],
            payload["shares"],
            payload["messages"],
            payload["published_at"],
            payload["source"],
            payload["raw_json"],
            payload["captured_at"],
        ),
    )
    return int(cursor.lastrowid)


def import_stats(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_database()
    ts = now_ts()
    account_rows = payload.get("account_snapshots")
    if not isinstance(account_rows, list):
        account_rows = []
    account_inserted = 0
    for item in account_rows:
        if isinstance(item, dict):
            upsert_wechat_account_stats_snapshot(item)
            account_inserted += 1
    rows = payload.get("video_snapshots")
    if not isinstance(rows, list):
        rows = payload.get("items")
    if not isinstance(rows, list):
        rows = [] if account_rows and "video_ref" not in payload and "views" not in payload else [payload]
    inserted = 0
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        for item in rows:
            if not isinstance(item, dict):
                continue
            if str(item.get("snapshot_type") or "").strip() == "account_daily":
                upsert_wechat_account_stats_snapshot(item)
                account_inserted += 1
                continue
            normalized = _normalize_video_stats_snapshot(item, captured_at=ts)
            _delete_matching_video_snapshot(client, normalized)
            client.insert(
                "video_stats_snapshots",
                {
                    **normalized,
                    "raw_json": _json_payload(normalized["raw_json"], {}),
                },
            )
            inserted += 1
        if inserted:
            _invalidate_supabase_read_cache("dashboard_summary", "stats:all", "stats:accounts")
        return {"ok": True, "inserted": inserted, "account_snapshots": account_inserted}
    with connect() as conn:
        for item in rows:
            if not isinstance(item, dict):
                continue
            if str(item.get("snapshot_type") or "").strip() == "account_daily":
                upsert_wechat_account_stats_snapshot(item)
                account_inserted += 1
                continue
            snapshot_id = _insert_video_stats_snapshot_sqlite(conn, _normalize_video_stats_snapshot(item, captured_at=ts))
            _enqueue_sync_row(conn, "video_stats_snapshots", "id = ?", (snapshot_id,))
            inserted += 1
    return {"ok": True, "inserted": inserted, "account_snapshots": account_inserted}


def list_stats(account_id: int | None = None, platform: str = "", stat_date: str = "") -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        params: dict[str, str] = {}
        if account_id:
            params["account_id"] = f"eq.{account_id}"
        token = normalize_platform(platform)
        if token:
            params["platform"] = f"eq.{token}"
        day = _stats_date(stat_date)
        if day:
            params["stat_date"] = f"eq.{day}"
        cache_key = "stats:all"
        if not params and _supabase_read_cache_peek(cache_key):
            return _supabase_read_cache_get(cache_key)
        client = _brand_supabase()
        try:
            if hasattr(client, "select_where"):
                rows = client.select_where("video_stats_snapshots", params=params, order="captured_at.desc,id.desc")
            else:
                rows = client.select("video_stats_snapshots")
                if params:
                    if account_id:
                        rows = [row for row in rows if int(row.get("account_id") or 0) == int(account_id)]
                    if token:
                        rows = [row for row in rows if normalize_platform(str(row.get("platform") or "")) == token]
                    if day:
                        rows = [row for row in rows if str(row.get("stat_date") or "") == day]
        except Exception:
            rows = []
        if not params:
            _supabase_read_cache_set(cache_key, rows)
        return copy.deepcopy(rows)
    ensure_database()
    clauses: list[str] = []
    values: list[Any] = []
    if account_id:
        clauses.append("account_id = ?")
        values.append(account_id)
    token = normalize_platform(platform)
    if token:
        clauses.append("platform = ?")
        values.append(token)
    day = _stats_date(stat_date)
    if day:
        clauses.append("stat_date = ?")
        values.append(day)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect() as conn:
        return [
            dict_from_row(row)
            for row in conn.execute(
                f"SELECT * FROM video_stats_snapshots {where} ORDER BY captured_at DESC, id DESC LIMIT 500",
                values,
            )
        ]


def list_wechat_account_stats(account_id: int | None = None, stat_date: str = "") -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        params: dict[str, str] = {}
        if account_id:
            params["account_id"] = f"eq.{account_id}"
        day = _stats_date(stat_date)
        if day:
            params["stat_date"] = f"eq.{day}"
        cache_key = "stats:accounts" if not params else ""
        if cache_key and _supabase_read_cache_peek(cache_key):
            return _supabase_read_cache_get(cache_key)
        try:
            client = _brand_supabase()
            if hasattr(client, "select_where"):
                rows = client.select_where("wechat_stats_account_snapshots", params=params, order="stat_date.desc,captured_at.desc,id.desc")
            else:
                rows = client.select("wechat_stats_account_snapshots")
                if params:
                    if account_id:
                        rows = [row for row in rows if int(row.get("account_id") or 0) == int(account_id)]
                    if day:
                        rows = [row for row in rows if str(row.get("stat_date") or "") == day]
        except Exception:
            rows = []
        if cache_key:
            _supabase_read_cache_set(cache_key, rows)
        return copy.deepcopy(rows)
    ensure_database()
    clauses: list[str] = []
    values: list[Any] = []
    if account_id:
        clauses.append("account_id = ?")
        values.append(account_id)
    day = _stats_date(stat_date)
    if day:
        clauses.append("stat_date = ?")
        values.append(day)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect() as conn:
        return [
            dict_from_row(row)
            for row in conn.execute(
                f"SELECT * FROM wechat_stats_account_snapshots {where} ORDER BY stat_date DESC, captured_at DESC, id DESC LIMIT 1000",
                values,
            )
        ]


def latest_wechat_account_stats_index() -> dict[int, dict[str, Any]]:
    rows = list_wechat_account_stats()
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        account_id = int(row.get("account_id") or 0)
        if account_id > 0 and account_id not in result:
            result[account_id] = row
    return result


def upsert_wechat_stats_capture_run(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_database()
    ts = now_ts()
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("run_id is required")
    data = {
        "run_id": run_id,
        "target_date": _stats_date(payload.get("target_date")),
        "status": str(payload.get("status") or "running").strip(),
        "batch_size": _stats_int(payload.get("batch_size") or 5),
        "limit_accounts": _stats_int(payload.get("limit_accounts")),
        "dry_run": 1 if bool(payload.get("dry_run")) else 0,
        "account_total": _stats_int(payload.get("account_total")),
        "captured_accounts": _stats_int(payload.get("captured_accounts")),
        "skipped_accounts": _stats_int(payload.get("skipped_accounts")),
        "failed_accounts": _stats_int(payload.get("failed_accounts")),
        "payload_json": _stats_json(payload.get("payload_json") if "payload_json" in payload else payload),
        "error": str(payload.get("error") or ""),
        "started_at": int(payload.get("started_at") or ts),
        "finished_at": payload.get("finished_at"),
        "created_at": ts,
        "updated_at": ts,
    }
    if brand_database_backend() == "supabase":
        try:
            return _brand_supabase().upsert(
                "wechat_stats_capture_runs",
                {**data, "payload_json": _json_payload(data["payload_json"], {})},
                on_conflict="run_id",
            )
        except SupabaseError:
            return data
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO wechat_stats_capture_runs(
                run_id, target_date, status, batch_size, limit_accounts, dry_run,
                account_total, captured_accounts, skipped_accounts, failed_accounts,
                payload_json, error, started_at, finished_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                target_date = excluded.target_date,
                status = excluded.status,
                batch_size = excluded.batch_size,
                limit_accounts = excluded.limit_accounts,
                dry_run = excluded.dry_run,
                account_total = excluded.account_total,
                captured_accounts = excluded.captured_accounts,
                skipped_accounts = excluded.skipped_accounts,
                failed_accounts = excluded.failed_accounts,
                payload_json = excluded.payload_json,
                error = excluded.error,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                updated_at = excluded.updated_at
            """,
            (
                data["run_id"],
                data["target_date"],
                data["status"],
                data["batch_size"],
                data["limit_accounts"],
                data["dry_run"],
                data["account_total"],
                data["captured_accounts"],
                data["skipped_accounts"],
                data["failed_accounts"],
                data["payload_json"],
                data["error"],
                data["started_at"],
                data["finished_at"],
                data["created_at"],
                data["updated_at"],
            ),
        )
    return data


def latest_wechat_stats_capture_run() -> dict[str, Any]:
    if brand_database_backend() == "supabase":
        try:
            rows = _brand_supabase().select("wechat_stats_capture_runs", order="started_at.desc,id.desc")
        except SupabaseError:
            return {}
        return copy.deepcopy(rows[0]) if rows else {}
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM wechat_stats_capture_runs ORDER BY started_at DESC, id DESC LIMIT 1").fetchone()
        return dict_from_row(row) if row else {}


def _format_compact_number(value: int) -> str:
    number = int(value or 0)
    if abs(number) >= 10000:
        return f"{number / 10000:.1f}万"
    return f"{number:,}"


def _format_signed(value: int) -> str:
    number = int(value or 0)
    return f"+{number:,}" if number >= 0 else f"{number:,}"


def _rate_text(value: Any) -> str:
    rate = _stats_float(value)
    return f"{rate:.1f}%"


def _account_name_index() -> dict[int, dict[str, Any]]:
    return {int(item.get("id") or 0): item for item in list_accounts()}


def _build_real_analytics_items() -> dict[str, list[dict[str, Any]]]:
    account_rows = list_wechat_account_stats()
    video_rows = list_stats(platform="wechat")
    if not account_rows and not video_rows:
        return {}
    latest_date = max([str(row.get("stat_date") or "") for row in account_rows if row.get("stat_date")] or [""])
    if latest_date:
        account_rows = [row for row in account_rows if str(row.get("stat_date") or "") == latest_date]
        video_rows_for_date = [row for row in video_rows if str(row.get("stat_date") or "") == latest_date]
    else:
        video_rows_for_date = video_rows
    account_index = _account_name_index()
    total_views = sum(_stats_int(row.get("views")) for row in account_rows) or sum(_stats_int(row.get("views")) for row in video_rows_for_date)
    total_likes = sum(_stats_int(row.get("likes")) for row in account_rows) or sum(_stats_int(row.get("likes")) for row in video_rows_for_date)
    total_comments = sum(_stats_int(row.get("comments")) for row in account_rows) or sum(_stats_int(row.get("comments")) for row in video_rows_for_date)
    total_shares = sum(_stats_int(row.get("shares")) for row in account_rows) or sum(_stats_int(row.get("shares")) for row in video_rows_for_date)
    total_messages = sum(_stats_int(row.get("messages")) for row in account_rows)
    followers = sum(_stats_int(row.get("followers")) for row in account_rows)
    follower_delta = sum(_stats_int(row.get("follower_delta")) for row in account_rows)
    works_count = sum(_stats_int(row.get("works_count")) for row in account_rows) or len(video_rows_for_date)
    overview = [
        {"label": "统计日期", "value": latest_date or "最新", "change": "", "trend": "up"},
        {"label": "账号播放量", "value": _format_compact_number(total_views), "change": "", "trend": "up"},
        {"label": "矩阵总粉丝", "value": _format_compact_number(followers), "change": _format_signed(follower_delta), "trend": "up" if follower_delta >= 0 else "down"},
        {"label": "作品采集数", "value": works_count, "change": "", "trend": "up"},
    ]
    account_rank = []
    for row in sorted(account_rows, key=lambda item: _stats_int(item.get("views")), reverse=True):
        account = account_index.get(int(row.get("account_id") or 0), {})
        views = _stats_int(row.get("views"))
        interactions = _stats_int(row.get("likes")) + _stats_int(row.get("comments")) + _stats_int(row.get("shares"))
        tier = "爆款账号" if views >= 50000 else ("稳定账号" if views >= 10000 else "潜力账号")
        warning = "低流量" if views < 1000 else ""
        account_rank.append(
            {
                "row": [
                    account.get("display_name") or account.get("account_key") or f"账号 {row.get('account_id')}",
                    "视频号",
                    account.get("status") or "active",
                    _format_compact_number(views),
                    _format_compact_number(views),
                    _format_compact_number(_stats_int(row.get("followers"))),
                    _format_signed(_stats_int(row.get("follower_delta"))),
                    _rate_text(row.get("completed_rate")),
                    _rate_text(row.get("interaction_rate") or ((interactions / views * 100) if views else 0)),
                    _stats_int(row.get("works_count")),
                    tier,
                    warning,
                ]
            }
        )
    content_top = [
        {
            "title": str(row.get("title") or row.get("video_ref") or "未命名作品"),
            "value": _format_compact_number(_stats_int(row.get("views"))),
            "tag": "高播放" if _stats_int(row.get("views")) >= 10000 else "普通",
        }
        for row in sorted(video_rows_for_date, key=lambda item: _stats_int(item.get("views")), reverse=True)[:10]
    ]
    conversion = [
        {"label": "主页访问量", "value": _format_compact_number(sum(_stats_int(row.get("profile_visits")) for row in account_rows))},
        {"label": "私信咨询量", "value": _format_compact_number(total_messages)},
        {"label": "有效线索数", "value": _format_compact_number(sum(_stats_int(row.get("leads")) for row in account_rows))},
        {"label": "整体互动量", "value": _format_compact_number(total_likes + total_comments + total_shares)},
    ]
    captured = len(account_rows)
    active_accounts = len([item for item in account_index.values() if str(item.get("status") or "") == "active"])
    coverage = round((captured / active_accounts * 100), 1) if active_accounts else 0
    risks = []
    low_rows = [row for row in account_rows if _stats_int(row.get("views")) < 1000]
    if low_rows:
        risks.append({"text": f"{len(low_rows)} 个账号播放低于 1000，需复盘内容或登录状态"})
    if coverage < 100 and active_accounts:
        risks.append({"text": f"本次采集覆盖 {captured}/{active_accounts} 个 active 账号"})
    if not risks:
        risks.append({"text": "本次真实采集未发现低流量或覆盖异常"})
    return {
        "overview": overview,
        "account_rank": account_rank,
        "content_top": content_top,
        "traffic": [{"label": "授权后台采集", "value": "100%"}],
        "conversion": conversion,
        "operation": [
            {"label": "账号采集覆盖率", "value": min(100, max(0, coverage))},
            {"label": "作品明细入库完成度", "value": 100 if video_rows_for_date else 0},
            {"label": "数据写入成功率", "value": 100},
        ],
        "risk": risks,
    }


def dashboard_summary() -> dict[str, Any]:
    if brand_database_backend() == "supabase":
        return _dashboard_summary_supabase()
    ensure_database()
    with connect() as conn:
        account_count = conn.execute("SELECT COUNT(*) AS c FROM matrix_accounts").fetchone()["c"]
        platform_count = conn.execute("SELECT COUNT(*) AS c FROM account_platforms WHERE enabled = 1").fetchone()["c"]
        running = conn.execute("SELECT COUNT(*) AS c FROM automation_tasks WHERE status IN ('pending', 'running')").fetchone()["c"]
        failed = conn.execute("SELECT COUNT(*) AS c FROM automation_tasks WHERE status = 'failed'").fetchone()["c"]
        unsupported = conn.execute("SELECT COUNT(*) AS c FROM automation_tasks WHERE status = 'unsupported'").fetchone()["c"]
        latest_stat_date = conn.execute("SELECT MAX(stat_date) AS d FROM wechat_stats_account_snapshots").fetchone()["d"]
        if latest_stat_date:
            stats = conn.execute(
                """
                SELECT
                    COALESCE(SUM(views), 0) AS views,
                    COALESCE(SUM(likes), 0) AS likes,
                    COALESCE(SUM(comments), 0) AS comments,
                    COALESCE(SUM(messages), 0) AS messages
                FROM wechat_stats_account_snapshots
                WHERE stat_date = ?
                """,
                (latest_stat_date,),
            ).fetchone()
        else:
            stats = conn.execute(
                "SELECT COALESCE(SUM(views), 0) AS views, COALESCE(SUM(likes), 0) AS likes, COALESCE(SUM(comments), 0) AS comments, COALESCE(SUM(messages), 0) AS messages FROM video_stats_snapshots"
            ).fetchone()
    return {
        "accounts": int(account_count),
        "platforms": int(platform_count),
        "running_tasks": int(running),
        "failed_tasks": int(failed),
        "unsupported_tasks": int(unsupported),
        "remaining_material_videos": _remaining_material_video_count(),
        "views": int(stats["views"]),
        "likes": int(stats["likes"]),
        "comments": int(stats["comments"]),
        "messages": int(stats["messages"]),
    }


def _dashboard_summary_supabase() -> dict[str, Any]:
    if _supabase_read_cache_peek("dashboard_summary"):
        return _supabase_read_cache_get("dashboard_summary")
    client = _brand_supabase()
    try:
        payload = client.rpc("dashboard_summary")
    except Exception:
        legacy = _dashboard_summary_supabase_legacy(client)
        _supabase_read_cache_set("dashboard_summary", legacy)
        return copy.deepcopy(legacy)
    row = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(row, dict):
        legacy = _dashboard_summary_supabase_legacy(client)
        _supabase_read_cache_set("dashboard_summary", legacy)
        return copy.deepcopy(legacy)
    normalized = _normalize_dashboard_summary(row)
    _supabase_read_cache_set("dashboard_summary", normalized)
    return copy.deepcopy(normalized)


def _normalize_dashboard_summary(row: dict[str, Any]) -> dict[str, int]:
    return {
        "accounts": int(row.get("accounts") or 0),
        "platforms": int(row.get("platforms") or 0),
        "running_tasks": int(row.get("running_tasks") or 0),
        "failed_tasks": int(row.get("failed_tasks") or 0),
        "unsupported_tasks": int(row.get("unsupported_tasks") or 0),
        "remaining_material_videos": int(row.get("remaining_material_videos") or 0),
        "views": int(row.get("views") or 0),
        "likes": int(row.get("likes") or 0),
        "comments": int(row.get("comments") or 0),
        "messages": int(row.get("messages") or 0),
    }


def _dashboard_summary_supabase_legacy(client: SupabaseRestClient) -> dict[str, int]:
    accounts = client.select("matrix_accounts")
    platforms = [item for item in client.select("account_platforms") if bool(item.get("enabled", True))]
    tasks = client.select("automation_tasks")
    stats_rows = client.select("video_stats_snapshots")
    try:
        account_stats = client.select("wechat_stats_account_snapshots")
    except SupabaseError:
        account_stats = []
    latest_stat_date = max([str(item.get("stat_date") or "") for item in account_stats if item.get("stat_date")] or [""])
    if latest_stat_date:
        summary_rows = [item for item in account_stats if str(item.get("stat_date") or "") == latest_stat_date]
    else:
        summary_rows = stats_rows
    return _normalize_dashboard_summary(
        {
            "accounts": len(accounts),
            "platforms": len(platforms),
            "running_tasks": len([item for item in tasks if item.get("status") in {"pending", "running"}]),
            "failed_tasks": len([item for item in tasks if item.get("status") == "failed"]),
            "unsupported_tasks": len([item for item in tasks if item.get("status") == "unsupported"]),
            "remaining_material_videos": 0,
            "views": sum(int(item.get("views") or 0) for item in summary_rows),
            "likes": sum(int(item.get("likes") or 0) for item in summary_rows),
            "comments": sum(int(item.get("comments") or 0) for item in summary_rows),
            "messages": sum(int(item.get("messages") or 0) for item in summary_rows),
        }
    )
