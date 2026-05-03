from __future__ import annotations

import sqlite3
import time
from contextvars import ContextVar
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .paths import get_paths

_database_path_override: ContextVar[Path | None] = ContextVar("gasgx_distribution_database_path", default=None)

SCHEMA = """
CREATE TABLE IF NOT EXISTS matrix_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    niche TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    notes TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS account_platforms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    platform TEXT NOT NULL,
    handle TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    capability_status TEXT NOT NULL DEFAULT 'registered',
    login_status TEXT NOT NULL DEFAULT 'unknown',
    last_checked_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(account_id, platform),
    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS browser_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL UNIQUE,
    profile_dir TEXT NOT NULL,
    debug_port INTEGER NOT NULL UNIQUE,
    fingerprint_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS notification_routes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    platform TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(event_type, platform)
);

CREATE TABLE IF NOT EXISTS login_qr_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL DEFAULT 'wechat_login_qr',
    status TEXT NOT NULL DEFAULT 'pending',
    payload_json TEXT NOT NULL DEFAULT '{}',
    notified_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS login_qr_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    account_id INTEGER NOT NULL,
    account_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    platform TEXT NOT NULL DEFAULT 'wechat',
    profile_dir TEXT NOT NULL,
    debug_port INTEGER NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    qr_path TEXT NOT NULL DEFAULT '',
    qr_fingerprint TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(account_id, platform, qr_fingerprint),
    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE CASCADE,
    FOREIGN KEY(batch_id) REFERENCES login_qr_batches(batch_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS automation_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER,
    platform TEXT NOT NULL DEFAULT '',
    task_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    summary TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS video_stats_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER,
    platform TEXT NOT NULL,
    video_ref TEXT NOT NULL DEFAULT '',
    views INTEGER NOT NULL DEFAULT 0,
    likes INTEGER NOT NULL DEFAULT 0,
    comments INTEGER NOT NULL DEFAULT 0,
    shares INTEGER NOT NULL DEFAULT 0,
    messages INTEGER NOT NULL DEFAULT 0,
    published_at TEXT NOT NULL DEFAULT '',
    captured_at INTEGER NOT NULL,
    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS ai_robot_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 0,
    bot_name TEXT NOT NULL DEFAULT '',
    webhook_url TEXT NOT NULL DEFAULT '',
    webhook_secret TEXT NOT NULL DEFAULT '',
    signing_secret TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ai_robot_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    message_type TEXT NOT NULL DEFAULT 'text',
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    summary TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at INTEGER,
    sent_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS brand_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    name TEXT NOT NULL DEFAULT 'GasGx',
    slogan TEXT NOT NULL DEFAULT 'Video Distribution',
    logo_asset_path TEXT NOT NULL DEFAULT '',
    primary_color TEXT NOT NULL DEFAULT '#5dd62c',
    theme_id TEXT NOT NULL DEFAULT 'gasgx-green',
    default_account_prefix TEXT NOT NULL DEFAULT 'GasGx',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS operator_roles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS operator_users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    role_id TEXT NOT NULL,
    password_hash TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(role_id) REFERENCES operator_roles(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS operator_role_permissions (
    role_id TEXT NOT NULL,
    permission TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(role_id, permission),
    FOREIGN KEY(role_id) REFERENCES operator_roles(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    app_version TEXT NOT NULL DEFAULT '',
    applied_at INTEGER NOT NULL
);
"""


def now_ts() -> int:
    return int(time.time())


def dict_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def database_path() -> Path:
    override = _database_path_override.get()
    if override is not None:
        override.parent.mkdir(parents=True, exist_ok=True)
        return override
    paths = get_paths()
    paths.ensure()
    return paths.database_path


@contextmanager
def use_database(path: Path) -> Iterator[None]:
    token = _database_path_override.set(path)
    try:
        yield
    finally:
        _database_path_override.reset(token)


@contextmanager
def connect(path: Path | None = None) -> Iterator[sqlite3.Connection]:
    db_path = path or database_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(path: Path | None = None) -> None:
    with connect(path) as conn:
        conn.executescript(SCHEMA)
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(ai_robot_messages)")}
        if "retry_count" not in columns:
            conn.execute("ALTER TABLE ai_robot_messages ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
        if "last_attempt_at" not in columns:
            conn.execute("ALTER TABLE ai_robot_messages ADD COLUMN last_attempt_at INTEGER")
        if "sent_at" not in columns:
            conn.execute("ALTER TABLE ai_robot_messages ADD COLUMN sent_at INTEGER")
        profile_columns = {row["name"] for row in conn.execute("PRAGMA table_info(browser_profiles)")}
        if "account_id" not in profile_columns:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS browser_profiles_next (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL UNIQUE,
                    profile_dir TEXT NOT NULL,
                    debug_port INTEGER NOT NULL UNIQUE,
                    fingerprint_json TEXT NOT NULL DEFAULT '{}',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE CASCADE
                );
                INSERT OR IGNORE INTO browser_profiles_next(account_id, profile_dir, debug_port, fingerprint_json, created_at, updated_at)
                SELECT ap.account_id, bp.profile_dir, bp.debug_port, COALESCE(bp.fingerprint_json, '{}'), bp.created_at, bp.updated_at
                FROM browser_profiles bp
                JOIN account_platforms ap ON ap.id = bp.account_platform_id
                ORDER BY bp.id;
                DROP TABLE browser_profiles;
                ALTER TABLE browser_profiles_next RENAME TO browser_profiles;
                """
            )
            profile_columns = {row["name"] for row in conn.execute("PRAGMA table_info(browser_profiles)")}
        if "fingerprint_json" not in profile_columns:
            conn.execute("ALTER TABLE browser_profiles ADD COLUMN fingerprint_json TEXT NOT NULL DEFAULT '{}'")
