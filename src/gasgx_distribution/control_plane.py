from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv

from .db import dict_from_row, init_db, now_ts
from .paths import get_paths
from .supabase_backend import SupabaseRestClient

load_dotenv()
APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
SCHEMA_VERSION = os.getenv("SCHEMA_VERSION", "2026.04.28.tenant-v1")

CONTROL_SCHEMA = """
CREATE TABLE IF NOT EXISTS brand_instances (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT NOT NULL UNIQUE,
    supabase_url TEXT NOT NULL DEFAULT '',
    service_key_ref TEXT NOT NULL DEFAULT '',
    anon_key TEXT NOT NULL DEFAULT '',
    database_path TEXT NOT NULL DEFAULT '',
    schema_version TEXT NOT NULL DEFAULT '',
    app_version TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS brand_templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upgrade_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_schema_version TEXT NOT NULL,
    target_app_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    summary TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upgrade_run_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    brand_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT NOT NULL DEFAULT '',
    started_at INTEGER,
    finished_at INTEGER,
    FOREIGN KEY(run_id) REFERENCES upgrade_runs(id) ON DELETE CASCADE
);
"""

DEFAULT_TEMPLATE = {
    "brand": {
        "name": "GasGx",
        "slogan": "Video Distribution",
        "primary_color": "#5dd62c",
        "theme_id": "gasgx-green",
        "default_account_prefix": "GasGx",
    },
    "ai_robot_platforms": ["wecom", "dingtalk", "lark", "telegram", "whatsapp"],
}


def control_backend() -> str:
    explicit = os.getenv("CONTROL_DB_BACKEND", "").strip().lower()
    if explicit:
        return explicit
    if os.getenv("CONTROL_SUPABASE_URL") and os.getenv("CONTROL_SUPABASE_SERVICE_ROLE_KEY"):
        return "supabase"
    return "sqlite"


def _supabase() -> SupabaseRestClient:
    return SupabaseRestClient.from_env(prefix="CONTROL_SUPABASE")


def _slug(value: str) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    token = "-".join(part for part in token.split("-") if part)
    return token[:64] or "default"


def _control_database_path() -> Path:
    paths = get_paths()
    paths.ensure()
    return getattr(paths, "control_database_path", paths.runtime_root / "control_plane.db")


def brand_database_path(brand_id: str) -> Path:
    paths = get_paths()
    paths.ensure()
    root = getattr(paths, "brand_databases_root", paths.runtime_root / "brands")
    return root / _slug(brand_id) / "gasgx_distribution.db"


@contextmanager
def connect(path: Path | None = None) -> Iterator[sqlite3.Connection]:
    db_path = path or _control_database_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_control_database() -> None:
    if control_backend() == "supabase":
        ensure_default_template()
        ensure_default_brand_instance()
        return
    with connect() as conn:
        conn.executescript(CONTROL_SCHEMA)
    ensure_default_template()
    ensure_default_brand_instance()


def ensure_default_template() -> None:
    ts = now_ts()
    if control_backend() == "supabase":
        _supabase().upsert(
            "brand_templates",
            {
                "id": "default",
                "name": "Default GasGx Template",
                "payload_json": DEFAULT_TEMPLATE,
                "created_at": ts,
                "updated_at": ts,
            },
            on_conflict="id",
        )
        return
    with connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO brand_templates(id, name, payload_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("default", "Default GasGx Template", json.dumps(DEFAULT_TEMPLATE, ensure_ascii=False), ts, ts),
        )


def ensure_default_brand_instance() -> dict[str, Any]:
    ts = now_ts()
    default_db = get_paths().database_path
    if control_backend() == "supabase":
        row = _supabase().upsert(
            "brand_instances",
            {
                "id": "default",
                "name": "GasGx",
                "domain": "localhost",
                "database_path": str(default_db),
                "schema_version": SCHEMA_VERSION,
                "app_version": APP_VERSION,
                "status": "active",
                "created_at": ts,
                "updated_at": ts,
            },
            on_conflict="id",
        )
        return public_brand_instance(row)
    with connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO brand_instances(
                id, name, domain, database_path, schema_version, app_version, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("default", "GasGx", "localhost", str(default_db), SCHEMA_VERSION, APP_VERSION, "active", ts, ts),
        )
        row = conn.execute("SELECT * FROM brand_instances WHERE id = ?", ("default",)).fetchone()
    return public_brand_instance(dict_from_row(row))


def public_brand_instance(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data.pop("service_key_ref", None)
    data["has_service_key_ref"] = bool(row.get("service_key_ref"))
    return data


def private_brand_instance(row: dict[str, Any]) -> dict[str, Any]:
    return dict(row)


def list_brand_instances() -> list[dict[str, Any]]:
    ensure_control_database()
    if control_backend() == "supabase":
        return [
            public_brand_instance(row)
            for row in _supabase().select("brand_instances", order="created_at.desc,id.asc")
        ]
    with connect() as conn:
        return [
            public_brand_instance(dict_from_row(row))
            for row in conn.execute("SELECT * FROM brand_instances ORDER BY created_at DESC, id")
        ]


def get_brand_instance(brand_id: str) -> dict[str, Any] | None:
    ensure_control_database()
    if control_backend() == "supabase":
        rows = _supabase().select("brand_instances", filters={"id": _slug(brand_id)})
        return public_brand_instance(rows[0]) if rows else None
    with connect() as conn:
        row = conn.execute("SELECT * FROM brand_instances WHERE id = ?", (_slug(brand_id),)).fetchone()
        return public_brand_instance(dict_from_row(row)) if row else None


def find_brand_instance(*, host: str = "", brand_id: str = "") -> dict[str, Any]:
    return public_brand_instance(find_brand_runtime_instance(host=host, brand_id=brand_id))


def find_brand_runtime_instance(*, host: str = "", brand_id: str = "") -> dict[str, Any]:
    ensure_control_database()
    token = _slug(brand_id)
    normalized_host = str(host or "").split(":", 1)[0].lower()
    if control_backend() == "supabase":
        rows = _supabase().select("brand_instances", filters={"id": token}) if brand_id else []
        if not rows and normalized_host:
            rows = _supabase().select("brand_instances", filters={"domain": normalized_host})
        if not rows:
            rows = _supabase().select("brand_instances", filters={"id": "default"})
        return private_brand_instance(rows[0]) if rows else ensure_default_brand_instance()
    with connect() as conn:
        row = None
        if brand_id:
            row = conn.execute("SELECT * FROM brand_instances WHERE id = ?", (token,)).fetchone()
        if row is None and normalized_host:
            row = conn.execute("SELECT * FROM brand_instances WHERE lower(domain) = ?", (normalized_host,)).fetchone()
        if row is None:
            row = conn.execute("SELECT * FROM brand_instances WHERE id = ?", ("default",)).fetchone()
    if row is None:
        return ensure_default_brand_instance()
    return private_brand_instance(dict_from_row(row))


def create_brand_instance(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_control_database()
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    brand_id = _slug(str(payload.get("id") or name))
    domain = str(payload.get("domain") or f"{brand_id}.local").strip().lower()
    ts = now_ts()
    db_path = str(brand_database_path(brand_id))
    if control_backend() == "supabase":
        row = _supabase().insert(
            "brand_instances",
            {
                "id": brand_id,
                "name": name,
                "domain": domain,
                "supabase_url": str(payload.get("supabase_url") or "").strip(),
                "service_key_ref": str(payload.get("service_key_ref") or "").strip(),
                "anon_key": str(payload.get("anon_key") or "").strip(),
                "database_path": db_path,
                "schema_version": "",
                "app_version": APP_VERSION,
                "status": str(payload.get("status") or "active").strip() or "active",
                "created_at": ts,
                "updated_at": ts,
            },
        )
        return public_brand_instance(row)
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO brand_instances(
                id, name, domain, supabase_url, service_key_ref, anon_key, database_path,
                schema_version, app_version, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                brand_id,
                name,
                domain,
                str(payload.get("supabase_url") or "").strip(),
                str(payload.get("service_key_ref") or "").strip(),
                str(payload.get("anon_key") or "").strip(),
                db_path,
                "",
                APP_VERSION,
                str(payload.get("status") or "active").strip() or "active",
                ts,
                ts,
            ),
        )
    return get_brand_instance(brand_id) or {}


def provision_brand_instance(brand_id: str) -> dict[str, Any]:
    instance = get_brand_instance(brand_id)
    if instance is None:
        raise KeyError("brand not found")
    if control_backend() == "supabase" and instance.get("supabase_url"):
        return _mark_brand_provisioned(str(instance["id"]))
    db_path = Path(str(instance["database_path"]))
    init_db(db_path)
    with connect() as conn:
        conn.execute(
            "UPDATE brand_instances SET schema_version = ?, app_version = ?, updated_at = ? WHERE id = ?",
            (SCHEMA_VERSION, APP_VERSION, now_ts(), instance["id"]),
        )
    from . import service

    with service.use_brand_database(db_path):
        service.save_brand_settings(DEFAULT_TEMPLATE["brand"])
    return get_brand_instance(str(instance["id"])) or {}


def _mark_brand_provisioned(brand_id: str) -> dict[str, Any]:
    ts = now_ts()
    if control_backend() == "supabase":
        row = _supabase().update(
            "brand_instances",
            {"schema_version": SCHEMA_VERSION, "app_version": APP_VERSION, "updated_at": ts},
            filters={"id": _slug(brand_id)},
        )
        return public_brand_instance(row)
    with connect() as conn:
        conn.execute(
            "UPDATE brand_instances SET schema_version = ?, app_version = ?, updated_at = ? WHERE id = ?",
            (SCHEMA_VERSION, APP_VERSION, ts, _slug(brand_id)),
        )
    return get_brand_instance(brand_id) or {}


def run_full_upgrade() -> dict[str, Any]:
    ensure_control_database()
    ts = now_ts()
    if control_backend() == "supabase":
        run = _supabase().insert(
            "upgrade_runs",
            {
                "target_schema_version": SCHEMA_VERSION,
                "target_app_version": APP_VERSION,
                "status": "running",
                "created_at": ts,
                "updated_at": ts,
            },
        )
        run_id = int(run["id"])
        failures = 0
        for instance in list_brand_instances():
            if instance.get("status") != "active":
                continue
            item = _supabase().insert(
                "upgrade_run_items",
                {"run_id": run_id, "brand_id": instance["id"], "status": "running", "started_at": now_ts()},
            )
            try:
                _mark_brand_provisioned(str(instance["id"]))
                _supabase().update(
                    "upgrade_run_items",
                    {"status": "succeeded", "finished_at": now_ts()},
                    filters={"id": item["id"]},
                )
            except Exception as exc:
                failures += 1
                _supabase().update(
                    "upgrade_run_items",
                    {"status": "failed", "error": str(exc), "finished_at": now_ts()},
                    filters={"id": item["id"]},
                )
        status = "failed" if failures else "succeeded"
        summary = f"{failures} brand upgrade failures" if failures else "all active brands upgraded"
        _supabase().update(
            "upgrade_runs",
            {"status": status, "summary": summary, "updated_at": now_ts()},
            filters={"id": run_id},
        )
        return get_upgrade_run(run_id) or {}
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO upgrade_runs(target_schema_version, target_app_version, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (SCHEMA_VERSION, APP_VERSION, "running", ts, ts),
        )
        run_id = int(cursor.lastrowid)
    failures = 0
    for instance in list_brand_instances():
        if instance.get("status") != "active":
            continue
        item_started = now_ts()
        with connect() as conn:
            item_cursor = conn.execute(
                "INSERT INTO upgrade_run_items(run_id, brand_id, status, started_at) VALUES (?, ?, ?, ?)",
                (run_id, instance["id"], "running", item_started),
            )
            item_id = int(item_cursor.lastrowid)
        try:
            db_path = Path(str(instance["database_path"]))
            init_db(db_path)
            with connect() as conn:
                conn.execute(
                    "UPDATE brand_instances SET schema_version = ?, app_version = ?, updated_at = ? WHERE id = ?",
                    (SCHEMA_VERSION, APP_VERSION, now_ts(), instance["id"]),
                )
                conn.execute(
                    "UPDATE upgrade_run_items SET status = ?, finished_at = ? WHERE id = ?",
                    ("succeeded", now_ts(), item_id),
                )
        except Exception as exc:
            failures += 1
            with connect() as conn:
                conn.execute(
                    "UPDATE upgrade_run_items SET status = ?, error = ?, finished_at = ? WHERE id = ?",
                    ("failed", str(exc), now_ts(), item_id),
                )
    status = "failed" if failures else "succeeded"
    summary = f"{failures} brand upgrade failures" if failures else "all active brands upgraded"
    with connect() as conn:
        conn.execute(
            "UPDATE upgrade_runs SET status = ?, summary = ?, updated_at = ? WHERE id = ?",
            (status, summary, now_ts(), run_id),
        )
    return get_upgrade_run(run_id) or {}


def get_upgrade_run(run_id: int) -> dict[str, Any] | None:
    ensure_control_database()
    if control_backend() == "supabase":
        rows = _supabase().select("upgrade_runs", filters={"id": run_id})
        if not rows:
            return None
        data = rows[0]
        data["items"] = _supabase().select("upgrade_run_items", filters={"run_id": run_id}, order="id.asc")
        return data
    with connect() as conn:
        row = conn.execute("SELECT * FROM upgrade_runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        data = dict_from_row(row)
        data["items"] = [
            dict_from_row(item)
            for item in conn.execute("SELECT * FROM upgrade_run_items WHERE run_id = ? ORDER BY id", (run_id,))
        ]
    return data
