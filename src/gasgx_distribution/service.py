from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from cybercar import engine
from cybercar.settings import apply_runtime_environment as apply_cybercar_environment

from .db import connect, dict_from_row, init_db, now_ts
from .paths import get_paths
from .platforms import get_platform, normalize_platform, stable_debug_port
from .public_settings import resolve_material_dir

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}


def _account_slug(account_key: str) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(account_key or "").strip())
    token = "-".join(part for part in token.split("-") if part)
    if not token:
        raise ValueError("account_key is required")
    return token[:80]


def ensure_database() -> None:
    init_db()


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
    ensure_database()
    ts = now_ts()
    account_key = _account_slug(str(payload.get("account_key") or payload.get("display_name") or ""))
    display_name = str(payload.get("display_name") or account_key).strip()
    platforms = payload.get("platforms")
    if not isinstance(platforms, list) or not platforms:
        platforms = ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili", "tiktok", "x"]
    with connect() as conn:
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
        for platform in platforms:
            ensure_account_platform(conn, account_id, str(platform))
    return get_account(account_id) or {}


def update_account(account_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
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
    ensure_database()
    with connect() as conn:
        return [dict_from_row(row) for row in conn.execute("SELECT * FROM automation_tasks ORDER BY id DESC LIMIT 200")]


def get_task(task_id: int) -> dict[str, Any] | None:
    ensure_database()
    with connect() as conn:
        row = conn.execute("SELECT * FROM automation_tasks WHERE id = ?", (task_id,)).fetchone()
        return dict_from_row(row) if row else None


def delete_task(task_id: int) -> bool:
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
