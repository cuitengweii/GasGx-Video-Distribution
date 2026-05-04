from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import hmac
import hashlib
import base64
import sqlite3
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from contextvars import ContextVar
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import requests

from cybercar import engine
from cybercar.settings import apply_runtime_environment as apply_cybercar_environment

from .db import connect, dict_from_row, init_db, now_ts, use_database
from .paths import get_paths
from .platforms import DEBUG_PORT_END, DEBUG_PORT_START, get_platform, normalize_platform, stable_debug_port
from .public_settings import load_distribution_settings, resolve_material_dir
from .public_settings import save_distribution_settings as save_local_distribution_settings
from .supabase_backend import SupabaseError, SupabaseRestClient
from .video_matrix.cover_templates import load_cover_templates
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
LOGIN_QR_NOTIFY_COOLDOWN_SECONDS = 1800
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
    if not instance:
        return SupabaseRestClient.from_env(prefix="BRAND_SUPABASE")
    return SupabaseRestClient.from_instance(instance)


def _json_payload(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _password_hash(password: str) -> str:
    return hashlib.sha256(f"gasgx-operator-auth:{password}".encode("utf-8")).hexdigest()


def ensure_operator_auth_seed() -> None:
    init_db()
    ts = now_ts()
    with connect() as conn:
        for role_id, name in DEFAULT_ROLE_NAMES.items():
            conn.execute(
                """
                INSERT INTO operator_roles(id, name, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name, updated_at=excluded.updated_at
                """,
                (role_id, name, ts, ts),
            )
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
    row = _brand_supabase().select_one("app_settings", filters={"setting_key": key})
    return _json_payload(row.get("payload_json") if row else None, default)


def _save_app_setting(key: str, payload: Any) -> dict[str, Any]:
    return _brand_supabase().upsert(
        "app_settings",
        {"setting_key": key, "payload_json": payload, "updated_at": now_ts()},
        on_conflict="setting_key",
    )


def load_distribution_settings_db() -> dict[str, Any]:
    if brand_database_backend() != "supabase":
        return load_distribution_settings()
    payload = _app_setting("distribution_settings")
    if isinstance(payload, dict):
        return payload
    settings = load_distribution_settings()
    _save_app_setting("distribution_settings", settings)
    return settings


def save_distribution_settings_db(payload: dict[str, Any]) -> dict[str, Any]:
    if brand_database_backend() != "supabase":
        return save_local_distribution_settings(payload)
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
    return {
        **platform,
        "material_dir": common.get("material_dir", ""),
        "publish_mode": common.get("publish_mode", "publish") if platform.get("publish_mode") == "inherit" else platform.get("publish_mode", "publish"),
        "topics": common.get("topics", ""),
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
        rows = _brand_supabase().select("analytics_items", order="sort_order.asc,id.asc")
        items = [(str(row["section"]), str(row["item_key"]), _json_payload(row.get("payload_json"), {})) for row in rows]
    else:
        items = _seed_analytics_items()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for section, key, payload in items:
        item = dict(payload or {})
        item.setdefault("key", key)
        grouped.setdefault(section, []).append(item)
    return grouped


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
    return {"ok": True, "backend": "supabase", "seed_version": SEED_VERSION, "inserted": inserted, "skipped": skipped}


def load_brand_settings() -> dict[str, Any]:
    if brand_database_backend() == "supabase":
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
        return row
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
        return _brand_supabase().update("brand_settings", next_data, filters={"id": 1})
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
                now_ts(),
            ),
        )
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
        rows = {
            row["platform"]: _public_ai_config(row)
            for row in _brand_supabase().select("ai_robot_configs", order="platform.asc")
        }
        return [_default_ai_robot_config(platform, rows.get(platform)) for platform in sorted(AI_ROBOT_PLATFORMS)]
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
        return _brand_supabase().delete("ai_robot_configs", filters={"platform": token})
    ensure_database()
    with connect() as conn:
        cursor = conn.execute("DELETE FROM ai_robot_configs WHERE platform = ?", (token,))
        return cursor.rowcount > 0


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
        return _brand_supabase().insert(
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
        return _brand_supabase().select("ai_robot_messages", order="id.desc")
    ensure_database()
    with connect() as conn:
        return [dict_from_row(row) for row in conn.execute("SELECT * FROM ai_robot_messages ORDER BY id DESC LIMIT 100")]


def run_ai_robot_sender_worker(*, limit: int = 10) -> dict[str, Any]:
    limit = max(1, int(limit or 10))
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
    return {"ok": failed == 0, "claimed": len(messages), "sent": sent, "failed": failed, "messages": results}


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
            return _brand_supabase().upsert(
                "notification_routes",
                {"event_type": event, "platform": token, "enabled": 1 if enabled else 0, "created_at": ts, "updated_at": ts},
                on_conflict="event_type,platform",
            )
        except SupabaseError as exc:
            return {"event_type": event, "platform": token, "enabled": bool(enabled), "ok": False, "storage_unavailable": True, "error": str(exc)}
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
    return {"event_type": event, "platform": token, "enabled": bool(enabled)}


def list_notification_routes() -> list[dict[str, Any]]:
    defaults = [
        {**NOTIFICATION_EVENT_META[event], "platform": platform, "enabled": False}
        for event in sorted(NOTIFICATION_EVENT_TYPES)
        for platform in sorted(NOTIFICATION_PLATFORMS)
    ]
    if brand_database_backend() == "supabase":
        try:
            rows = {
                (row["event_type"], row["platform"]): {**row, "enabled": bool(row.get("enabled"))}
                for row in _brand_supabase().select("notification_routes", order="event_type.asc,platform.asc")
            }
        except SupabaseError:
            rows = {}
        return [{**item, **rows.get((item["event_type"], item["platform"]), {})} for item in defaults]
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
    for key in ("account", "platform", "task_id", "run_id", "action"):
        value = str(payload.get(key) or "").strip()
        if value:
            lines.append(f"{key}: {value}")
    return "\n".join(lines)


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
    platforms = _enabled_notification_platforms(event) if notify and bool(meta.get("routeable")) else []
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
    }


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
    notification_results: list[dict[str, Any]] = []
    platforms = _enabled_notification_platforms("wechat_login_qr") if notify else []
    text = _login_qr_text(batch_id, items)
    if notify and platforms:
        for platform in platforms:
            try:
                notification_results.append(_send_notification_text(platform, text))
            except Exception as exc:
                notification_results.append({"platform": platform, "ok": False, "error": str(exc)})
    return {"batch_id": batch_id, "items": items, "notification_platforms": platforms, "notification_results": notification_results}


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
    {"hex": "#3B82F6", "name": "科技蓝"},
    {"hex": "#F97316", "name": "活力橙"},
    {"hex": "#A855F7", "name": "神秘紫"},
    {"hex": "#EC4899", "name": "醒目粉"},
    {"hex": "#EAB308", "name": "明亮黄"},
]


def _terminal_state_path() -> Path:
    return get_paths().runtime_root / "terminal_execution_state.json"


def _load_terminal_state() -> dict[str, Any]:
    path = _terminal_state_path()
    if not path.exists():
        return {"windows": [], "config": [], "initialized": False, "updated_at": 0}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"windows": [], "config": [], "initialized": False, "updated_at": 0}
    return payload if isinstance(payload, dict) else {"windows": [], "config": [], "initialized": False, "updated_at": 0}


def _save_terminal_state(payload: dict[str, Any]) -> None:
    payload["updated_at"] = now_ts()
    path = _terminal_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    groups = _terminal_operator_groups()
    state = _load_terminal_state()
    login_started = bool(state.get("login_started"))
    windows = state.get("windows") or []
    visible_windows = windows
    if not login_started:
        visible_windows = []
        for window in windows:
            visible_window = dict(window)
            visible_window["qr_data_url"] = ""
            visible_window["manual_available_at"] = 0
            visible_accounts = []
            for index, account in enumerate(window.get("accounts") or []):
                visible_account = dict(account)
                if index == int(window.get("current_index") or 0) and str(visible_account.get("status") or "") != "success":
                    visible_account["status"] = "pending"
                    visible_account["status_text"] = "等待开始登录"
                    visible_account["task_id"] = None
                visible_accounts.append(visible_account)
            visible_window["accounts"] = visible_accounts
            visible_windows.append(visible_window)
    return {
        "ok": True,
        "colors": TERMINAL_COLORS,
        "operators": groups,
        "windows": visible_windows,
        "config": state.get("config") or [],
        "initialized": bool(state.get("initialized")) or bool(windows),
        "login_started": login_started,
        "summary": _terminal_summary(windows, groups),
    }


def _terminal_summary(windows: list[dict[str, Any]], groups: list[dict[str, Any]]) -> dict[str, Any]:
    total = sum(len(group.get("accounts") or []) for group in groups)
    success = 0
    for window in windows:
        for account in window.get("accounts") or []:
            if str(account.get("status") or "") == "success":
                success += 1
    return {"total": total, "success": success, "active_windows": len([item for item in windows if item.get("enabled")])}


def _terminal_qr_data_url(account_id: int) -> str:
    account = get_account(account_id) or {}
    platform = next((item for item in account.get("platforms", []) if item.get("platform") == "wechat"), None)
    if not platform:
        return ""
    try:
        result = engine._prepare_platform_login_qr_notice(  # type: ignore[attr-defined]
            platform_name="wechat",
            open_url=get_platform("wechat").open_url,  # type: ignore[union-attr]
            debug_port=int(platform.get("debug_port") or 0),
            chrome_user_data_dir=str(platform.get("profile_dir") or ""),
            auto_open_chrome=False,
            refresh_page=False,
            allow_navigation=False,
        )
    except Exception:
        return ""
    photo_bytes = result.get("photo_bytes") if isinstance(result, dict) else b""
    if not isinstance(photo_bytes, (bytes, bytearray)) or not photo_bytes:
        return ""
    mime = str(result.get("mime") or "image/png")
    return f"data:{mime};base64,{base64.b64encode(bytes(photo_bytes)).decode('ascii')}"


def _close_wechat_browser_for_account(account_id: int) -> None:
    account = get_account(account_id) or {}
    platform = next((item for item in account.get("platforms", []) if item.get("platform") == "wechat"), None)
    if not platform:
        return
    try:
        pid = engine._find_debug_chrome_process_pid(int(platform.get("debug_port") or 0), str(platform.get("profile_dir") or ""))  # type: ignore[attr-defined]
        if pid:
            engine._terminate_windows_process_tree(int(pid))  # type: ignore[attr-defined]
    except Exception:
        return


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


def start_terminal_execution(payload: dict[str, Any]) -> dict[str, Any]:
    groups = {item["operator_wechat"]: item for item in _terminal_operator_groups()}
    windows = []
    saved_config = []
    for index, raw in enumerate(payload.get("windows") or [], start=1):
        saved_config.append({
            "id": int(raw.get("id") or index),
            "enabled": bool(raw.get("enabled", True)),
            "operator_wechat": str(raw.get("operator_wechat") or "").strip(),
            "color": str(raw.get("color") or TERMINAL_COLORS[(index - 1) % len(TERMINAL_COLORS)]["hex"]),
        })
        if not bool(raw.get("enabled", True)):
            continue
        operator = str(raw.get("operator_wechat") or "").strip()
        if operator not in groups:
            continue
        group = groups[operator]
        accounts = _terminal_account_cards(group.get("accounts") or [])
        if not accounts:
            continue
        color = str(raw.get("color") or TERMINAL_COLORS[(index - 1) % len(TERMINAL_COLORS)]["hex"])
        current = accounts[0]
        current["status"] = "pending"
        current["status_text"] = "等待开始登录"
        qr_data_url = ""
        windows.append({
            "id": int(raw.get("id") or index),
            "enabled": True,
            "operator_wechat": operator,
            "color": color,
            "color_name": next((item["name"] for item in TERMINAL_COLORS if item["hex"].lower() == color.lower()), ""),
            "current_index": 0,
            "qr_data_url": qr_data_url,
            "manual_available_at": 0,
            "accounts": accounts,
        })
    if not windows:
        raise ValueError("至少需要启用一个已绑定运营微信的终端")
    state = {"windows": windows, "config": saved_config, "initialized": True, "login_started": False}
    _save_terminal_state(state)
    return terminal_execution_state()


def start_terminal_login() -> dict[str, Any]:
    state = _load_terminal_state()
    windows = state.get("windows") or []
    if not windows:
        raise ValueError("请先初始化执行矩阵")
    for window in windows:
        accounts = window.get("accounts") or []
        current_index = int(window.get("current_index") or 0)
        if current_index >= len(accounts):
            continue
        current = accounts[current_index]
        current["status"] = "opening"
        current["status_text"] = "正在打开浏览器"
        try:
            open_account_browser(int(current["id"]), "wechat")
            current["status"] = "waiting_qr"
            current["status_text"] = "等待扫码中..."
            window["qr_data_url"] = _terminal_qr_data_url(int(current["id"]))
        except Exception as exc:
            current["status"] = "error"
            current["status_text"] = str(exc)
            window["qr_data_url"] = ""
        window["manual_available_at"] = now_ts() + 60
    state["login_started"] = True
    _save_terminal_state(state)
    return terminal_execution_state()


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


def _advance_terminal_window(window: dict[str, Any]) -> None:
    accounts = window.get("accounts") or []
    current_index = int(window.get("current_index") or 0)
    if current_index >= len(accounts):
        return
    current = accounts[current_index]
    account_id = int(current.get("id") or 0)
    try:
        result = check_login_status(account_id, "wechat")
    except Exception as exc:
        current["status"] = "error"
        current["status_text"] = str(exc)
        return
    if str(result.get("status") or "") != "ready":
        current["status"] = "waiting_qr"
        current["status_text"] = "等待扫码中..."
        if not window.get("qr_data_url"):
            window["qr_data_url"] = _terminal_qr_data_url(account_id)
        return
    if current.get("task_id"):
        task = get_task(int(current.get("task_id") or 0)) or {}
        task_status = str(task.get("status") or "").lower()
        if task_status in {"success", "completed", "published"}:
            current["status"] = "success"
            current["status_text"] = "发布成功"
            current["publish_success_count"] = int(current.get("publish_success_count") or 0) + 1
            _close_wechat_browser_for_account(account_id)
            next_index = current_index + 1
            window["current_index"] = next_index
            window["qr_data_url"] = ""
            window["manual_available_at"] = now_ts() + 60
            if next_index >= len(accounts):
                return
            next_account = accounts[next_index]
            next_account["status"] = "opening"
            next_account["status_text"] = "正在打开浏览器"
            try:
                open_account_browser(int(next_account["id"]), "wechat")
                next_account["status"] = "waiting_qr"
                next_account["status_text"] = "等待扫码中..."
                window["qr_data_url"] = _terminal_qr_data_url(int(next_account["id"]))
            except Exception as exc:
                next_account["status"] = "error"
                next_account["status_text"] = str(exc)
        elif task_status in {"failed", "error", "unsupported"}:
            current["status"] = "error"
            current["status_text"] = str(task.get("summary") or task.get("error") or "任务执行失败")
        else:
            current["status"] = "running"
            current["status_text"] = "已登录，草稿任务执行中"
        return
    if not current.get("task_id"):
        task = _queue_terminal_draft_task(account_id)
        current["task_id"] = task.get("id")
    current["status"] = "running"
    current["status_text"] = "已登录，草稿任务已加入队列"


def poll_terminal_execution() -> dict[str, Any]:
    state = _load_terminal_state()
    if not bool(state.get("login_started")):
        return terminal_execution_state()
    for window in state.get("windows") or []:
        _advance_terminal_window(window)
    _save_terminal_state(state)
    return terminal_execution_state()


def manual_terminal_publish(window_id: int) -> dict[str, Any]:
    state = _load_terminal_state()
    target = next((item for item in state.get("windows") or [] if int(item.get("id") or 0) == int(window_id)), None)
    if target is None:
        raise KeyError("window not found")
    if not bool(state.get("login_started")):
        return terminal_execution_state()
    if now_ts() < int(target.get("manual_available_at") or 0):
        return terminal_execution_state()
    accounts = target.get("accounts") or []
    current_index = int(target.get("current_index") or 0)
    if current_index < len(accounts):
        current = accounts[current_index]
        task = _queue_terminal_draft_task(int(current.get("id") or 0))
        current["task_id"] = task.get("id")
        current["status"] = "running"
        current["status_text"] = "已人工触发草稿任务"
    target["manual_available_at"] = now_ts() + 60
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
        return _brand_supabase().update("ai_robot_messages", payload, filters={"id": message["id"]})
    with connect() as conn:
        conn.execute(
            """
            UPDATE ai_robot_messages
            SET status = ?, summary = ?, last_attempt_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (payload["status"], payload["summary"], payload["last_attempt_at"], payload["updated_at"], message["id"]),
        )
    return get_ai_robot_message(int(message["id"])) or {**message, **payload}


def _mark_ai_robot_message_sent(message: dict[str, Any]) -> dict[str, Any]:
    ts = now_ts()
    payload = {"status": "sent", "summary": "sent by robot sender", "error": "", "sent_at": ts, "updated_at": ts}
    if brand_database_backend() == "supabase":
        return _brand_supabase().update("ai_robot_messages", payload, filters={"id": message["id"]})
    with connect() as conn:
        conn.execute(
            """
            UPDATE ai_robot_messages
            SET status = ?, summary = ?, error = ?, sent_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (payload["status"], payload["summary"], payload["error"], payload["sent_at"], payload["updated_at"], message["id"]),
        )
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
        return _brand_supabase().update("ai_robot_messages", payload, filters={"id": message["id"]})
    with connect() as conn:
        conn.execute(
            """
            UPDATE ai_robot_messages
            SET status = ?, summary = ?, error = ?, retry_count = ?, last_attempt_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, summary, error[:1000], retry_count, ts, ts, message["id"]),
        )
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
        return {}
    counts: dict[int, int] = {}
    for item in runs:
        if not isinstance(item, dict) or not bool(item.get("success")):
            continue
        try:
            account_id = int(item.get("account_id") or 0)
        except Exception:
            account_id = 0
        if account_id <= 0:
            continue
        counts[account_id] = counts.get(account_id, 0) + 1
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
    for path in material_dir.iterdir():
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
    material_dir = resolve_material_dir({"material_dir": raw_path})
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
    settings = ProjectSettings.from_file(_config_root() / "defaults.json")
    ui_state = load_ui_state(_config_root() / "ui_state.json")
    output_root = Path(str(ui_state.get("output_root") or settings.output_root)).expanduser()
    if not output_root.is_absolute():
        output_root = paths.repo_root / output_root
    targets = {
        "materials": resolve_material_dir(),
        "output": output_root.resolve(),
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
            if token.lower() in {"constraint", "primary", "foreign", "unique", "check"}:
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
        publish_success_counts = _matrix_publish_success_counts_for_backend()
        client = _brand_supabase()
        accounts = client.select("matrix_accounts", order="id.desc")
        profiles = client.select("browser_profiles")
        for account in accounts:
            platforms = client.select("account_platforms", filters={"account_id": account["id"]}, order="platform.asc")
            profile = _profile_for_account_from_rows(int(account["id"]), platforms, profiles)
            for platform in platforms:
                platform["account_key"] = account.get("account_key")
                if profile:
                    platform["profile_dir"] = profile.get("profile_dir", "")
                    platform["debug_port"] = profile.get("debug_port")
                    platform["fingerprint_json"] = profile.get("fingerprint_json", {})
                platform.update(_decode_platform_profile(platform))
            account["platforms"] = platforms
            account["publish_success_count"] = publish_success_counts.get(int(account["id"]), 0)
        return accounts
    ensure_database()
    publish_success_counts = _matrix_publish_success_counts()
    with connect() as conn:
        accounts = [dict_from_row(row) for row in conn.execute("SELECT * FROM matrix_accounts ORDER BY id DESC")]
        for account in accounts:
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
        client = _brand_supabase()
        account = client.insert(
            "matrix_accounts",
            {
                "account_key": account_key,
                "display_name": display_name,
                "niche": str(payload.get("niche") or "").strip(),
                "status": str(payload.get("status") or "active").strip() or "active",
                "notes": str(payload.get("notes") or "").strip(),
                "created_at": ts,
                "updated_at": ts,
            },
        )
        for platform in platforms:
            ensure_account_platform(None, int(account["id"]), str(platform))
        return get_account(int(account["id"])) or {}
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
                    str(payload.get("niche") or "").strip(),
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
    return get_account(account_id) or {}


def update_account(account_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    if brand_database_backend() == "supabase":
        allowed = {"display_name", "niche", "status", "notes"}
        update = {key: str(payload.get(key) or "").strip() for key in allowed if key in payload}
        if update:
            update["updated_at"] = now_ts()
            _brand_supabase().update("matrix_accounts", update, filters={"id": account_id})
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
            client.delete("browser_profiles", filters={"account_id": account_id})
        except SupabaseError:
            for platform_id in platform_ids:
                client.delete("browser_profiles", filters={"account_platform_id": platform_id})
        client.delete("account_platforms", filters={"account_id": account_id})
        client.delete("matrix_accounts", filters={"id": account_id})
        return True
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT id FROM matrix_accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            return False
        conn.execute("DELETE FROM automation_tasks WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM video_stats_snapshots WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM browser_profiles WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM account_platforms WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM matrix_accounts WHERE id = ?", (account_id,))
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
        elif not _json_payload(profile.get("fingerprint_json"), {}):
            update = {"fingerprint_json": fingerprint, "updated_at": ts}
            if profile.get("account_id") == account_id:
                client.update("browser_profiles", update, filters={"account_id": account_id})
            else:
                client.update("browser_profiles", update, filters={"id": profile["id"]})
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
    profile_dir.mkdir(parents=True, exist_ok=True)
    return dict_from_row(ap)


def profile_dir_for(account_key: str, platform: str | None = None) -> Path:
    return get_paths().profiles_root / _account_slug(account_key)


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
    return {
        "ok": True,
        "platform": token,
        "debug_port": int(ap["debug_port"]),
        "profile_dir": str(profile_dir),
        "open_url": capability.open_url,
        "fingerprint": ap.get("fingerprint") or {},
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
        return _brand_supabase().delete("automation_tasks", filters={"id": task_id})
    ensure_database()
    with connect() as conn:
        cursor = conn.execute("DELETE FROM automation_tasks WHERE id = ?", (task_id,))
        return bool(cursor.rowcount)


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
        return deleted
    ensure_database()
    placeholders = ",".join("?" for _ in ids)
    with connect() as conn:
        cursor = conn.execute(f"DELETE FROM automation_tasks WHERE id IN ({placeholders})", ids)
        return int(cursor.rowcount or 0)


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
        return updated
    ensure_database()
    placeholders = ",".join("?" for _ in ids)
    with connect() as conn:
        cursor = conn.execute(
            f"UPDATE automation_tasks SET status = ?, updated_at = ? WHERE id IN ({placeholders})",
            [normalized, ts, *ids],
        )
        return int(cursor.rowcount or 0)


def import_stats(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_database()
    ts = now_ts()
    rows = payload.get("items")
    if not isinstance(rows, list):
        rows = [payload]
    inserted = 0
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        for item in rows:
            if not isinstance(item, dict):
                continue
            client.insert(
                "video_stats_snapshots",
                {
                    "account_id": item.get("account_id"),
                    "platform": normalize_platform(str(item.get("platform") or "")),
                    "video_ref": str(item.get("video_ref") or ""),
                    "views": int(item.get("views") or 0),
                    "likes": int(item.get("likes") or 0),
                    "comments": int(item.get("comments") or 0),
                    "shares": int(item.get("shares") or 0),
                    "messages": int(item.get("messages") or 0),
                    "published_at": str(item.get("published_at") or ""),
                    "captured_at": int(item.get("captured_at") or ts),
                },
            )
            inserted += 1
        return {"ok": True, "inserted": inserted}
    with connect() as conn:
        for item in rows:
            if not isinstance(item, dict):
                continue
            conn.execute(
                """
                INSERT INTO video_stats_snapshots(account_id, platform, video_ref, views, likes, comments, shares, messages, published_at, captured_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.get("account_id"),
                    normalize_platform(str(item.get("platform") or "")),
                    str(item.get("video_ref") or ""),
                    int(item.get("views") or 0),
                    int(item.get("likes") or 0),
                    int(item.get("comments") or 0),
                    int(item.get("shares") or 0),
                    int(item.get("messages") or 0),
                    str(item.get("published_at") or ""),
                    int(item.get("captured_at") or ts),
                ),
            )
            inserted += 1
    return {"ok": True, "inserted": inserted}


def list_stats(account_id: int | None = None, platform: str = "") -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        params: dict[str, str] = {}
        if account_id:
            params["account_id"] = f"eq.{account_id}"
        token = normalize_platform(platform)
        if token:
            params["platform"] = f"eq.{token}"
        return _brand_supabase().select_where("video_stats_snapshots", params=params, order="captured_at.desc,id.desc")
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
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect() as conn:
        return [
            dict_from_row(row)
            for row in conn.execute(
                f"SELECT * FROM video_stats_snapshots {where} ORDER BY captured_at DESC, id DESC LIMIT 500",
                values,
            )
        ]


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
    client = _brand_supabase()
    try:
        payload = client.rpc("dashboard_summary")
    except Exception:
        return _dashboard_summary_supabase_legacy(client)
    row = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(row, dict):
        return _dashboard_summary_supabase_legacy(client)
    return _normalize_dashboard_summary(row)


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
    return _normalize_dashboard_summary(
        {
            "accounts": len(accounts),
            "platforms": len(platforms),
            "running_tasks": len([item for item in tasks if item.get("status") in {"pending", "running"}]),
            "failed_tasks": len([item for item in tasks if item.get("status") == "failed"]),
            "unsupported_tasks": len([item for item in tasks if item.get("status") == "unsupported"]),
            "remaining_material_videos": 0,
            "views": sum(int(item.get("views") or 0) for item in stats_rows),
            "likes": sum(int(item.get("likes") or 0) for item in stats_rows),
            "comments": sum(int(item.get("comments") or 0) for item in stats_rows),
            "messages": sum(int(item.get("messages") or 0) for item in stats_rows),
        }
    )
