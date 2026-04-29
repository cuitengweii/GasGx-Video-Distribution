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
from .platforms import get_platform, normalize_platform, stable_debug_port
from .public_settings import load_distribution_settings, resolve_material_dir
from .public_settings import save_distribution_settings as save_local_distribution_settings
from .supabase_backend import SupabaseRestClient
from .video_matrix.cover_templates import load_cover_templates
from .video_matrix.settings import ProjectSettings
from .video_matrix.templates import load_templates
from .video_matrix.ui_state import load_ui_state

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}
AI_ROBOT_PLATFORMS = {"wecom", "dingtalk", "lark", "telegram", "whatsapp"}
AI_ROBOT_RETRY_LIMIT = 3
SEED_VERSION = "2026-04-29-supabase-db-init-v1"
_brand_runtime: ContextVar[dict[str, Any] | None] = ContextVar("gasgx_brand_runtime", default=None)


def _account_slug(account_key: str) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(account_key or "").strip())
    token = "-".join(part for part in token.split("-") if part)
    if not token:
        raise ValueError("account_key is required")
    return token[:80]


def ensure_database() -> None:
    init_db()


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
            "hud_enable_live_data": settings.hud_enable_live_data,
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
    if sys.platform.startswith("win"):
        os.startfile(str(material_dir))  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(material_dir)])
    else:
        subprocess.Popen(["xdg-open", str(material_dir)])
    return {"ok": True, "path": str(material_dir)}


def list_accounts() -> list[dict[str, Any]]:
    if brand_database_backend() == "supabase":
        publish_success_counts = _matrix_publish_success_counts_for_backend()
        client = _brand_supabase()
        accounts = client.select("matrix_accounts", order="id.desc")
        for account in accounts:
            platforms = client.select("account_platforms", filters={"account_id": account["id"]}, order="platform.asc")
            profiles = {item["account_platform_id"]: item for item in client.select("browser_profiles")}
            for platform in platforms:
                profile = profiles.get(platform.get("id"))
                if profile:
                    platform["profile_dir"] = profile.get("profile_dir", "")
                    platform["debug_port"] = profile.get("debug_port")
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
                SELECT ap.*, bp.profile_dir, bp.debug_port
                FROM account_platforms ap
                LEFT JOIN browser_profiles bp ON bp.account_platform_id = ap.id
                WHERE ap.account_id = ?
                ORDER BY ap.platform
                """,
                (account["id"],),
            ).fetchall()
            account["platforms"] = [dict_from_row(row) for row in platforms]
            account["publish_success_count"] = publish_success_counts.get(int(account["id"]), 0)
        return accounts


def get_account(account_id: int) -> dict[str, Any] | None:
    if brand_database_backend() == "supabase":
        client = _brand_supabase()
        account = client.select_one("matrix_accounts", filters={"id": account_id})
        if account is None:
            return None
        platforms = client.select("account_platforms", filters={"account_id": account_id}, order="platform.asc")
        profiles = {item["account_platform_id"]: item for item in client.select("browser_profiles")}
        for platform in platforms:
            profile = profiles.get(platform.get("id"))
            if profile:
                platform["profile_dir"] = profile.get("profile_dir", "")
                platform["debug_port"] = profile.get("debug_port")
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
            dict_from_row(item)
            for item in conn.execute(
                """
                SELECT ap.*, bp.profile_dir, bp.debug_port
                FROM account_platforms ap
                LEFT JOIN browser_profiles bp ON bp.account_platform_id = ap.id
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
        profile_dir = profile_dir_for(str(account["account_key"]), token)
        profile_dir.mkdir(parents=True, exist_ok=True)
        if client.select_one("browser_profiles", filters={"account_platform_id": ap["id"]}) is None:
            client.insert(
                "browser_profiles",
                {
                    "account_platform_id": ap["id"],
                    "profile_dir": str(profile_dir),
                    "debug_port": stable_debug_port(str(account["account_key"]), token),
                    "created_at": ts,
                    "updated_at": ts,
                },
            )
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
    profile_dir = profile_dir_for(str(ap["account_key"]), token)
    conn.execute(
        """
        INSERT OR IGNORE INTO browser_profiles(account_platform_id, profile_dir, debug_port, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (int(ap["id"]), str(profile_dir), stable_debug_port(str(ap["account_key"]), token), ts, ts),
    )
    profile_dir.mkdir(parents=True, exist_ok=True)
    return dict_from_row(ap)


def profile_dir_for(account_key: str, platform: str) -> Path:
    return get_paths().profiles_root / _account_slug(account_key) / normalize_platform(platform)


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
    result = engine.probe_platform_session_via_debug_port(
        platform_name=token,
        open_url=capability.open_url,
        debug_port=int(ap["debug_port"]),
        chrome_user_data_dir=str(profile_dir),
        disconnect_after_probe=(token != "wechat"),
        enable_wechat_keepalive=(token == "wechat"),
    )
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
    if task_type not in {"publish", "comment", "message", "stats"}:
        raise ValueError("task_type must be publish, comment, message, or stats")
    account_id = payload.get("account_id")
    platform = normalize_platform(str(payload.get("platform") or ""))
    capability = get_platform(platform) if platform else None
    supported = True
    if capability is None:
        supported = task_type == "stats" and not platform
    elif task_type == "publish":
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
