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
    vpn_node_key TEXT NOT NULL DEFAULT '',
    vpn_proxy_url TEXT NOT NULL DEFAULT '',
    account_publish_mode TEXT NOT NULL DEFAULT 'inherit',
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

CREATE TABLE IF NOT EXISTS notification_policies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT '',
    platform TEXT NOT NULL DEFAULT '',
    account_scope TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    target_platforms_json TEXT NOT NULL DEFAULT '[]',
    cooldown_seconds INTEGER NOT NULL DEFAULT 600,
    quiet_start TEXT NOT NULL DEFAULT '',
    quiet_end TEXT NOT NULL DEFAULT '',
    escalation_enabled INTEGER NOT NULL DEFAULT 0,
    escalation_minutes INTEGER NOT NULL DEFAULT 0,
    escalation_platforms_json TEXT NOT NULL DEFAULT '[]',
    owner_hint TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(event_type, severity, platform, account_scope)
);

CREATE TABLE IF NOT EXISTS notification_incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    subtype TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    status TEXT NOT NULL DEFAULT 'open',
    title TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    account_id INTEGER,
    account_key TEXT NOT NULL DEFAULT '',
    platform TEXT NOT NULL DEFAULT '',
    owner_hint TEXT NOT NULL DEFAULT '',
    source_url TEXT NOT NULL DEFAULT '',
    action_url TEXT NOT NULL DEFAULT '',
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    escalation_count INTEGER NOT NULL DEFAULT 0,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    acknowledged_at INTEGER,
    resolved_at INTEGER,
    assigned_to TEXT NOT NULL DEFAULT '',
    last_escalated_at INTEGER,
    next_escalate_at INTEGER,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(event_type, dedupe_key)
);

CREATE TABLE IF NOT EXISTS notification_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    FOREIGN KEY(incident_id) REFERENCES notification_incidents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_notification_incidents_status ON notification_incidents(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_notification_incidents_escalation ON notification_incidents(next_escalate_at, status);
CREATE INDEX IF NOT EXISTS idx_notification_actions_incident ON notification_actions(incident_id, created_at);

CREATE TABLE IF NOT EXISTS operation_notices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    category_label TEXT NOT NULL DEFAULT '',
    view TEXT NOT NULL DEFAULT '',
    view_label TEXT NOT NULL DEFAULT '',
    action_code TEXT NOT NULL,
    action_label TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'success',
    summary TEXT NOT NULL DEFAULT '',
    params_json TEXT NOT NULL DEFAULT '{}',
    actor_id TEXT NOT NULL DEFAULT '',
    actor_name TEXT NOT NULL DEFAULT '',
    merge_key TEXT NOT NULL DEFAULT '',
    merged_count INTEGER NOT NULL DEFAULT 1,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    delivery_status TEXT NOT NULL DEFAULT 'pending',
    delivery_targets_json TEXT NOT NULL DEFAULT '[]',
    delivery_result_json TEXT NOT NULL DEFAULT '{}',
    delivered_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operation_notices_category ON operation_notices(category, created_at);
CREATE INDEX IF NOT EXISTS idx_operation_notices_status ON operation_notices(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_operation_notices_merge ON operation_notices(merge_key, delivery_status, delivered_at);

CREATE TABLE IF NOT EXISTS operation_notice_category_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL UNIQUE,
    category_label TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operation_notice_category_settings_enabled ON operation_notice_category_settings(enabled, sort_order);

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
    stat_date TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    feed_id TEXT NOT NULL DEFAULT '',
    views INTEGER NOT NULL DEFAULT 0,
    likes INTEGER NOT NULL DEFAULT 0,
    comments INTEGER NOT NULL DEFAULT 0,
    shares INTEGER NOT NULL DEFAULT 0,
    messages INTEGER NOT NULL DEFAULT 0,
    published_at TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '{}',
    captured_at INTEGER NOT NULL,
    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS wechat_stats_account_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    platform TEXT NOT NULL DEFAULT 'wechat',
    stat_date TEXT NOT NULL,
    views INTEGER NOT NULL DEFAULT 0,
    likes INTEGER NOT NULL DEFAULT 0,
    comments INTEGER NOT NULL DEFAULT 0,
    shares INTEGER NOT NULL DEFAULT 0,
    messages INTEGER NOT NULL DEFAULT 0,
    followers INTEGER NOT NULL DEFAULT 0,
    follower_delta INTEGER NOT NULL DEFAULT 0,
    profile_visits INTEGER NOT NULL DEFAULT 0,
    leads INTEGER NOT NULL DEFAULT 0,
    completed_rate REAL NOT NULL DEFAULT 0,
    interaction_rate REAL NOT NULL DEFAULT 0,
    works_count INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'wechat_stats_capture',
    raw_json TEXT NOT NULL DEFAULT '{}',
    captured_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(account_id, platform, stat_date),
    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS wechat_stats_capture_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    target_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    batch_size INTEGER NOT NULL DEFAULT 5,
    limit_accounts INTEGER NOT NULL DEFAULT 0,
    dry_run INTEGER NOT NULL DEFAULT 0,
    account_total INTEGER NOT NULL DEFAULT 0,
    captured_accounts INTEGER NOT NULL DEFAULT 0,
    skipped_accounts INTEGER NOT NULL DEFAULT 0,
    failed_accounts INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}',
    error TEXT NOT NULL DEFAULT '',
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
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

CREATE TABLE IF NOT EXISTS sync_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    table_name TEXT NOT NULL,
    entity_id TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at INTEGER,
    synced_at INTEGER,
    error TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sync_outbox_status ON sync_outbox(status, updated_at);

CREATE TABLE IF NOT EXISTS sync_conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    entity_id TEXT NOT NULL DEFAULT '',
    local_payload_json TEXT NOT NULL DEFAULT '{}',
    remote_payload_json TEXT NOT NULL DEFAULT '{}',
    resolution TEXT NOT NULL DEFAULT 'local_overwrite',
    warning TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
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
                    vpn_node_key TEXT NOT NULL DEFAULT '',
                    vpn_proxy_url TEXT NOT NULL DEFAULT '',
                    account_publish_mode TEXT NOT NULL DEFAULT 'inherit',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY(account_id) REFERENCES matrix_accounts(id) ON DELETE CASCADE
                );
                INSERT OR IGNORE INTO browser_profiles_next(account_id, profile_dir, debug_port, fingerprint_json, vpn_node_key, vpn_proxy_url, account_publish_mode, created_at, updated_at)
                SELECT ap.account_id, bp.profile_dir, bp.debug_port, COALESCE(bp.fingerprint_json, '{}'), COALESCE(bp.vpn_node_key, ''), COALESCE(bp.vpn_proxy_url, ''), COALESCE(bp.account_publish_mode, 'inherit'), bp.created_at, bp.updated_at
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
        if "vpn_node_key" not in profile_columns:
            conn.execute("ALTER TABLE browser_profiles ADD COLUMN vpn_node_key TEXT NOT NULL DEFAULT ''")
        if "vpn_proxy_url" not in profile_columns:
            conn.execute("ALTER TABLE browser_profiles ADD COLUMN vpn_proxy_url TEXT NOT NULL DEFAULT ''")
        if "account_publish_mode" not in profile_columns:
            conn.execute("ALTER TABLE browser_profiles ADD COLUMN account_publish_mode TEXT NOT NULL DEFAULT 'inherit'")
        stats_columns = {row["name"] for row in conn.execute("PRAGMA table_info(video_stats_snapshots)")}
        for name, ddl in {
            "stat_date": "ALTER TABLE video_stats_snapshots ADD COLUMN stat_date TEXT NOT NULL DEFAULT ''",
            "title": "ALTER TABLE video_stats_snapshots ADD COLUMN title TEXT NOT NULL DEFAULT ''",
            "feed_id": "ALTER TABLE video_stats_snapshots ADD COLUMN feed_id TEXT NOT NULL DEFAULT ''",
            "source": "ALTER TABLE video_stats_snapshots ADD COLUMN source TEXT NOT NULL DEFAULT ''",
            "raw_json": "ALTER TABLE video_stats_snapshots ADD COLUMN raw_json TEXT NOT NULL DEFAULT '{}'",
        }.items():
            if name not in stats_columns:
                conn.execute(ddl)
