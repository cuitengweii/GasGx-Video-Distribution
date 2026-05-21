from __future__ import annotations

import json
import os
import hmac
import hashlib
import sqlite3
import time
import contextlib
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from fastapi.testclient import TestClient

from gasgx_distribution import service
from gasgx_distribution import db as dist_db
from gasgx_distribution import matrix_publish
from gasgx_distribution import wechat_stats_capture
from gasgx_distribution.db import connect
from gasgx_distribution.platforms import SUPPORTED_PLATFORMS
from gasgx_distribution.supabase_backend import SupabaseError, SupabaseRestClient
from gasgx_distribution.video_matrix.ingestion import _select_source_files
from gasgx_distribution.web import create_app


def _isolated_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CONTROL_DB_BACKEND", "sqlite")
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "sqlite")
    class FakePaths:
        repo_root = tmp_path
        runtime_root = tmp_path / "runtime"
        profiles_root = tmp_path / "profiles" / "matrix"
        database_path = tmp_path / "runtime" / "gasgx_distribution.db"
        control_database_path = tmp_path / "runtime" / "control_plane.db"
        brand_databases_root = tmp_path / "runtime" / "brands"

        def ensure(self) -> None:
            self.runtime_root.mkdir(parents=True, exist_ok=True)
            self.profiles_root.mkdir(parents=True, exist_ok=True)
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
            self.control_database_path.parent.mkdir(parents=True, exist_ok=True)
            self.brand_databases_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("gasgx_distribution.db.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.service.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.wechat_stats_capture.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.public_settings.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.control_plane.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.paths.get_paths", lambda: FakePaths())
    service._SUPABASE_READ_CACHE.clear()
    service._SUPABASE_APP_SETTINGS_CACHE.clear()
    dist_db.init_db(FakePaths.database_path)


def test_account_crud_reuses_one_browser_profile_per_matrix_account(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    account = service.create_account(
        {
            "account_key": "GasGx CN 01",
            "display_name": "GasGx CN 01",
            "platforms": ["douyin", "x", "linkedin"],
        }
    )

    assert account["account_key"] == "gasgx-cn-01"
    assert {item["platform"] for item in account["platforms"]} == {"douyin", "x", "linkedin"}
    profile_dirs = {item["profile_dir"] for item in account["platforms"]}
    debug_ports = {item["debug_port"] for item in account["platforms"]}
    fingerprints = {json.dumps(item["fingerprint"], sort_keys=True) for item in account["platforms"]}
    assert len(profile_dirs) == 1
    assert len(debug_ports) == 1
    assert len(fingerprints) == 1
    assert next(iter(profile_dirs)).replace("\\", "/").endswith("profiles/matrix/gasgx-cn-01")
    for item in account["platforms"]:
        assert Path(item["profile_dir"]).exists()
        assert "profiles" in item["profile_dir"]
        assert isinstance(item["debug_port"], int)
        assert 12000 <= int(item["debug_port"]) <= 32000
        assert item["fingerprint"]["provider"] == "builtin-light"

    updated = service.update_account(int(account["id"]), {"notes": "phase-one"})
    assert updated is not None
    assert updated["notes"] == "phase-one"


def test_account_platform_unknown_login_status_renders_as_ready(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    account = service.create_account(
        {
            "account_key": "GasGx CN 02",
            "display_name": "GasGx CN 02",
            "platforms": ["wechat"],
        }
    )

    platform = next(item for item in account["platforms"] if item["platform"] == "wechat")
    assert platform["login_status"] == "ready"


def test_operator_auth_seed_uses_database_and_super_admin_password(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.ensure_database()

    state = service.operator_auth_state()
    assert state["roles"]["super_admin"]["name"] == "超级管理员"
    assert set(state["roles"]["super_admin"]["permissions"]) >= {"overview", "video-matrix", "system-settings"}
    assert any(user["id"] == "allen" and user["roleId"] == "super_admin" for user in state["users"])

    with sqlite3.connect(tmp_path / "runtime" / "gasgx_distribution.db") as conn:
        conn.row_factory = sqlite3.Row
        user = conn.execute("SELECT password_hash FROM operator_users WHERE id = 'allen'").fetchone()
    assert user is not None
    assert user["password_hash"]
    assert user["password_hash"] != "cuitengwei2023"

    assert service.login_operator_user("allen", "cuitengwei2023")["currentUserId"] == "allen"
    try:
        service.login_operator_user("allen", "wrong")
    except ValueError as exc:
        assert "invalid password" in str(exc)
    else:
        raise AssertionError("wrong password should fail")


def test_operator_auth_api_persists_roles_users_and_permissions(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service._invalidate_terminal_execution_state_cache()
    service._invalidate_terminal_execution_state_cache()
    service._invalidate_terminal_execution_state_cache()
    service._invalidate_terminal_execution_state_cache()
    service._invalidate_terminal_execution_state_cache()
    client = TestClient(create_app())

    login = client.post("/api/auth/login", json={"user_id": "allen", "password": "cuitengwei2023"})
    assert login.status_code == 200
    assert login.json()["currentUserId"] == "allen"

    role = client.post("/api/auth/roles", json={"name": "审核员"})
    assert role.status_code == 200
    role_id = role.json()["editingRoleId"]
    permissions = client.put(f"/api/auth/roles/{role_id}/permissions", json={"permissions": ["overview", "stats", "help-center"]})
    assert permissions.status_code == 200
    assert set(permissions.json()["roles"][role_id]["permissions"]) == {"overview", "stats", "help-center"}

    created = client.post("/api/auth/users", json={"name": "Mia", "role_id": role_id, "password": "mia123"})
    assert created.status_code == 200
    assert any(user["name"] == "Mia" and user["roleId"] == role_id for user in created.json()["users"])
    user_id = next(user["id"] for user in created.json()["users"] if user["name"] == "Mia")
    reset = client.patch(f"/api/auth/users/{user_id}/password", json={"password": "new-mia-pass"})
    assert reset.status_code == 200
    assert client.post("/api/auth/login", json={"user_id": user_id, "password": "new-mia-pass"}).status_code == 200
    assert client.post("/api/auth/login", json={"user_id": user_id, "password": "mia123"}).status_code == 401


def test_operator_auth_default_role_permissions_can_be_removed(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    permissions = client.put("/api/auth/roles/publisher/permissions", json={"permissions": ["overview", "help-center"]})
    assert permissions.status_code == 200
    assert set(permissions.json()["roles"]["publisher"]["permissions"]) == {"overview", "help-center"}

    state = client.get("/api/auth/state?current_user_id=allen&editing_role_id=publisher")
    assert state.status_code == 200
    assert set(state.json()["roles"]["publisher"]["permissions"]) == {"overview", "help-center"}


def test_help_doc_api_returns_whitelisted_markdown(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    response = client.get("/api/help-docs/WORKSPACE_OVERVIEW.md")
    assert response.status_code == 200
    payload = response.json()
    assert payload["path"] == "docs/help/WORKSPACE_OVERVIEW.md"
    assert payload["content"].startswith("# ")

    assert client.get("/api/help-docs/../README.md").status_code == 404


def test_help_docs_are_user_facing_knowledge_base_pages() -> None:
    help_dir = Path(__file__).resolve().parents[1] / "docs" / "help"
    for path in help_dir.glob("*.md"):
        text = path.read_text(encoding="utf-8")
        assert "GasGx Video Distribution" not in text
        assert "Pixabay" not in text
        assert "## 常见问题" in text
        assert "## 操作检查清单" in text or "## 开发排查清单" in text
        assert len(text.splitlines()) >= 40


def test_creating_many_wechat_accounts_allocates_unique_profiles_ports_and_fingerprints(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    accounts = [
        service.create_account({"account_key": f"gasgx-{index:02d}", "display_name": f"GasGx {index:02d}", "platforms": ["wechat"]})
        for index in range(1, 51)
    ]

    profiles = []
    ports = []
    fingerprints = []
    for account in accounts:
        platform = account["platforms"][0]
        profiles.append(platform["profile_dir"])
        ports.append(platform["debug_port"])
        fingerprints.append(json.dumps(platform["fingerprint"], sort_keys=True))
    assert len(set(profiles)) == 50
    assert len(set(ports)) == 50
    assert len(set(fingerprints)) > 1
    assert all(12000 <= int(port) <= 32000 for port in ports)


def test_matrix_publish_dry_run_uses_persisted_browser_profile_port(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})
    material_dir = tmp_path / "runtime" / "materials" / "videos"
    material_dir.mkdir(parents=True)
    video = material_dir / "v1.mp4"
    video.write_bytes(b"video")
    monkeypatch.setattr(matrix_publish, "materials_video_dir", lambda: material_dir)

    result = matrix_publish.run_wechat_publish(dry_run=True)

    wechat = account["platforms"][0]
    assert result["ok"] is True
    assert result["items"][0]["profile_dir"] == wechat["profile_dir"]
    assert result["items"][0]["debug_port"] == wechat["debug_port"]


def test_matrix_publish_preflight_skips_when_planned_account_needs_login(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})
    material_dir = tmp_path / "runtime" / "materials" / "videos"
    material_dir.mkdir(parents=True)
    (material_dir / "v1.mp4").write_bytes(b"video")
    monkeypatch.setattr(matrix_publish, "materials_video_dir", lambda: material_dir)
    monkeypatch.setattr(
        service,
        "check_login_status",
        lambda account_id, platform: {
            "status": "login_required",
            "reason": "login_url",
            "account_id": account_id,
            "account_key": "gasgx-01",
            "display_name": "GasGx 01",
            "profile_dir": str(tmp_path / "profiles" / "gasgx-01" / "wechat"),
            "debug_port": 12001,
        },
    )

    result = matrix_publish.run_wechat_publish()

    assert result["skipped"] is True
    assert result["reason"] == "wechat_login_required"
    assert not (tmp_path / "runtime" / "matrix_publish_runs").exists()


def test_login_qr_batch_deduplicates_and_does_not_require_configured_robot(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})
    platform = account["platforms"][0]
    payload = {
        "status": "login_required",
        "reason": "login_url",
        "account_id": account["id"],
        "account_key": account["account_key"],
        "display_name": account["display_name"],
        "profile_dir": platform["profile_dir"],
        "debug_port": platform["debug_port"],
        "url": "https://channels.weixin.qq.com/login.html",
    }

    first = service.record_wechat_login_qr_batch([payload], notify=True)
    second = service.record_wechat_login_qr_batch([payload], notify=True)

    assert first and first["items"][0]["qr_fingerprint"] == second["items"][0]["qr_fingerprint"]
    assert second["skipped"] is True
    assert second["reason"] == "duplicate_login_qr_cooldown"
    with dist_db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS c FROM login_qr_batches").fetchone()["c"] == 1
        assert conn.execute("SELECT COUNT(*) AS c FROM login_qr_items").fetchone()["c"] == 1


def test_notification_events_cover_ai_robot_route_matrix(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    events = service.list_notification_event_definitions()
    event_types = {item["event_type"] for item in events}

    assert {
        "wechat_login_qr",
        "login_status",
        "publish_result",
        "video_generation",
        "material_issue",
        "system_error",
        "ops_summary",
        "action_required",
    }.issubset(event_types)
    assert all(item["default_center"] is True for item in events)
    assert all(item["routeable"] is True for item in events)

    routes = service.list_notification_routes()
    assert len(routes) == len(event_types) * 3
    material_routes = [item for item in routes if item["event_type"] == "material_issue"]
    assert {item["platform"] for item in material_routes} == {"telegram", "dingtalk", "wecom"}
    assert all(item["default_severity"] == "warning" for item in material_routes)


def test_route_operation_notification_uses_event_metadata(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    result = service.route_operation_notification(
        "video_generation",
        {"subtype": "completed", "title": "视频生成完成", "run_id": "run-1"},
        notify=False,
    )

    assert result["event_type"] == "video_generation"
    assert result["subtype"] == "completed"
    assert result["severity"] == "warning"
    assert result["notification_center"] is True
    assert result["routeable"] is True
    assert result["notification_platforms"] == []


def test_notification_events_api_exposes_route_metadata(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    response = client.get("/api/notification-events")

    assert response.status_code == 200
    payload = response.json()
    assert any(item["event_type"] == "ops_summary" and "daily_sent" in item["subtypes"] for item in payload)


def test_notification_route_api_can_send_probe_message(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    sent: list[dict[str, object]] = []

    class FakeResponse:
        status_code = 200
        text = '{"ok": true}'

        def json(self) -> dict[str, object]:
            return {"ok": True}

    def fake_post(url: str, json: dict[str, object], headers: dict[str, str], timeout: float) -> FakeResponse:
        sent.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("gasgx_distribution.service.requests.post", fake_post)

    config = client.put(
        "/api/ai-robots/telegram/config",
        json={
            "enabled": True,
            "bot_name": "GasGx Bot",
            "webhook_url": "https://example.test/bot",
            "target_id": "chat-1",
        },
    )
    assert config.status_code == 200

    route = client.post(
        "/api/notification-routes/action_required/telegram",
        json={"enabled": True, "send_probe": True},
    )
    assert route.status_code == 200
    payload = route.json()
    assert payload["enabled"] is True
    assert payload["probe"]["status"] == "sent"
    assert sent[-1]["url"] == "https://example.test/bot"
    assert sent[-1]["json"]["chat_id"] == "chat-1"
    assert "通知链路联调" in str(sent[-1]["json"]["text"])
    assert "人工处理" in str(sent[-1]["json"]["text"])
    assert "通知路由已开启（人工处理 → 电报）" in str(sent[-1]["json"]["text"])


def test_notification_text_uses_chinese_labels_for_material_issue(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    text = service._notification_text(
        "material_issue",
        {
            "subtype": "insufficient",
            "title": "素材不足",
            "summary": "当天可用素材不够，请尽快补充。",
            "account_id": 7,
            "platform": "wechat",
        },
    )

    assert "【警告】素材不足" in text
    assert "事件: 素材问题" in text
    assert "类型: 素材问题 / 素材不足" in text
    assert "摘要: 当天可用素材不够，请尽快补充。" in text
    assert "账号ID: 7" in text
    assert "平台: 视频号" in text


def test_notification_text_uses_chinese_labels_for_video_generation(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    text = service._notification_text(
        "video_generation",
        {
            "subtype": "completed",
            "title": "视频生成完成",
            "summary": "全部账号已处理完毕。",
            "platform": "wechat",
        },
    )

    assert "【警告】视频生成完成" in text
    assert "事件: 视频生成" in text
    assert "类型: 视频生成 / 已完成" in text
    assert "摘要: 全部账号已处理完毕。" in text
    assert "平台: 视频号" in text


def test_notification_policy_dedupes_incidents_and_tracks_actions(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    sent: list[dict[str, object]] = []

    class FakeResponse:
        status_code = 200
        text = '{"ok": true}'

        def json(self) -> dict[str, object]:
            return {"ok": True}

    def fake_post(url: str, json: dict[str, object], headers: dict[str, str], timeout: float) -> FakeResponse:
        sent.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("gasgx_distribution.service.requests.post", fake_post)
    service.save_ai_robot_config(
        "telegram",
        {
            "enabled": True,
            "bot_name": "GasGx Bot",
            "webhook_url": "https://example.test/bot",
            "target_id": "chat-1",
        },
    )
    service.save_notification_route("publish_result", "telegram", True)

    first = service.route_operation_notification(
        "publish_result",
        {
            "subtype": "failed",
            "severity": "error",
            "title": "发布失败",
            "summary": "upload failed",
            "dedupe_key": "publish:acct-1:asset-1",
            "account_id": 7,
            "platform": "wechat",
        },
    )
    second = service.route_operation_notification(
        "publish_result",
        {
            "subtype": "failed",
            "severity": "error",
            "title": "发布失败",
            "summary": "upload failed again",
            "dedupe_key": "publish:acct-1:asset-1",
            "account_id": 7,
            "platform": "wechat",
        },
    )

    assert first["notification_platforms"] == ["telegram"]
    assert second["notification_platforms"] == []
    assert second["suppressed_reason"] == "cooldown"
    assert len(sent) == 1
    incident = service.list_notification_incidents()[0]
    assert incident["occurrence_count"] == 2
    assert incident["status"] == "open"

    ack = service.act_on_notification_incident(int(incident["id"]), "ack", {"actor": "Allen"})
    assert ack["incident"]["status"] == "acknowledged"
    resolved = service.act_on_notification_incident(int(incident["id"]), "resolve", {"actor": "Allen"})
    assert resolved["incident"]["status"] == "resolved"
    sla = service.notification_sla_summary()
    assert sla["closed_count"] == 1
    with dist_db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS c FROM notification_actions").fetchone()["c"] == 2


def test_notification_policy_api_and_sla_endpoint(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    policies = client.get("/api/notification-policies")
    assert policies.status_code == 200
    assert any(item["event_type"] == "material_issue" for item in policies.json())

    saved = client.put(
        "/api/notification-policies",
        json={
            "policies": [
                {
                    "event_type": "material_issue",
                    "severity": "warning",
                    "enabled": True,
                    "target_platforms": ["telegram", "wecom"],
                    "cooldown_seconds": 42,
                    "escalation_enabled": False,
                }
            ]
        },
    )
    assert saved.status_code == 200
    material = next(item for item in saved.json() if item["event_type"] == "material_issue")
    assert material["cooldown_seconds"] == 42
    assert material["target_platforms"] == ["telegram", "wecom"]

    incident = service.route_operation_notification("material_issue", {"dedupe_key": "mat-1", "summary": "素材不足"}, notify=False)["incident"]
    action = client.post(f"/api/notification-incidents/{incident['id']}/assign", json={"actor": "Allen", "assigned_to": "Allen"})
    assert action.status_code == 200
    assert action.json()["incident"]["status"] == "assigned"
    sla = client.get("/api/stats/notification-sla")
    assert sla.status_code == 200
    assert sla.json()["open_count"] == 1


def test_ai_robot_worker_processes_due_notification_escalations(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    sent: list[dict[str, object]] = []

    class FakeResponse:
        status_code = 200
        text = '{"ok": true}'

        def json(self) -> dict[str, object]:
            return {"ok": True}

    def fake_post(url: str, json: dict[str, object], headers: dict[str, str], timeout: float) -> FakeResponse:
        sent.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("gasgx_distribution.service.requests.post", fake_post)
    service.save_ai_robot_config(
        "telegram",
        {
            "enabled": True,
            "bot_name": "GasGx Bot",
            "webhook_url": "https://example.test/bot",
            "target_id": "chat-1",
        },
    )
    service.save_notification_policies(
        {
            "policies": [
                {
                    "event_type": "system_error",
                    "severity": "critical",
                    "enabled": True,
                    "target_platforms": ["telegram"],
                    "cooldown_seconds": 0,
                    "escalation_enabled": True,
                    "escalation_minutes": 10,
                    "escalation_platforms": ["telegram"],
                }
            ]
        }
    )
    incident = service.route_operation_notification(
        "system_error",
        {"dedupe_key": "system:down", "title": "依赖不可用", "summary": "storage down"},
        notify=False,
    )["incident"]
    with dist_db.connect() as conn:
        conn.execute("UPDATE notification_incidents SET next_escalate_at = ? WHERE id = ?", (service.now_ts() - 1, incident["id"]))

    result = service.run_ai_robot_sender_worker(limit=5)

    assert result["escalations"]["escalated"] == 1
    assert result["sent"] == 1
    assert "升级提醒" in sent[-1]["json"]["text"]
    updated = service.get_notification_incident(int(incident["id"]))
    assert updated and updated["escalation_count"] == 1


def test_accounts_include_matrix_publish_success_count(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    first = service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})
    second = service.create_account({"account_key": "gasgx-02", "display_name": "GasGx 02", "platforms": ["wechat"]})
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        json.dumps(
            {
                "consumed": [
                    {"account_id": first["id"], "platform": "wechat", "asset_key": "a.mp4", "publish_date": "2026-05-06", "success": True},
                    {"account_id": first["id"], "platform": "wechat", "asset_key": "a.mp4", "publish_date": "2026-05-06", "success": True},
                    {"account_id": first["id"], "platform": "wechat", "asset_key": "b.mp4", "publish_date": "2026-05-06", "success": True},
                    {"account_id": second["id"], "platform": "wechat", "asset_key": "c.mp4", "publish_date": "2026-05-06", "success": True},
                ],
                "runs": [
                    {"account_id": first["id"], "success": True},
                    {"account_id": second["id"], "success": True, "evidence_ok": False},
                ]
            }
        ),
        encoding="utf-8",
    )

    accounts = {item["id"]: item for item in service.list_accounts()}

    assert accounts[first["id"]]["publish_success_count"] == 2
    assert accounts[second["id"]]["publish_success_count"] == 1


def test_task_creation_marks_phase_one_unsupported_platform(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "global-01", "display_name": "Global 01", "platforms": ["linkedin"]})

    task = service.create_task({"account_id": account["id"], "platform": "linkedin", "task_type": "publish"})

    assert task["status"] == "unsupported"
    assert "does not support publish" in task["summary"]


def test_task_creation_rejects_duplicate_active_task(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})

    first = service.create_task({"account_id": account["id"], "platform": "wechat", "task_type": "publish"})

    assert first["status"] == "pending"
    try:
        service.create_task({"account_id": account["id"], "platform": "wechat", "task_type": "publish"})
    except ValueError as exc:
        assert "duplicate active task" in str(exc)
    else:
        raise AssertionError("duplicate active task was accepted")


def test_task_creation_accepts_draft_task(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})

    task = service.create_task({"account_id": account["id"], "platform": "wechat", "task_type": "draft"})

    assert task["task_type"] == "draft"
    assert task["status"] == "pending"


def test_api_smoke_accounts_tasks_and_stats(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    created = client.post(
        "/api/accounts",
        json={"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat", "instagram"]},
    )
    assert created.status_code == 200
    account = created.json()
    assert account["id"] >= 1

    task = client.post(
        "/api/tasks",
        json={"account_id": account["id"], "platform": "instagram", "task_type": "message", "payload": {}},
    )
    assert task.status_code == 200
    assert task.json()["status"] == "unsupported"

    imported = client.post(
        "/api/stats/import",
        json={"account_id": account["id"], "platform": "wechat", "video_ref": "v1", "views": 100, "comments": 3},
    )
    assert imported.status_code == 200
    assert imported.json()["inserted"] == 1
    stats = client.get(f"/api/stats?account_id={account['id']}&platform=wechat")
    assert stats.status_code == 200
    assert stats.json()[0]["views"] == 100


def test_local_first_api_does_not_touch_supabase_when_config_is_bad(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("SUPABASE_URL", "https://invalid.local.supabase.co")
    monkeypatch.setenv("BRAND_SUPABASE_SERVICE_KEY", "bad-service-key")

    def fail_supabase():
        raise AssertionError("local-first endpoints must not create a Supabase client")

    monkeypatch.setattr(service, "_brand_supabase", fail_supabase)
    client = TestClient(create_app())

    assert client.get("/api/summary").status_code == 200
    assert client.get("/api/accounts").status_code == 200
    assert client.get("/api/tasks").status_code == 200
    status = client.get("/api/sync/status")
    assert status.status_code == 200
    assert status.json()["backend"] == "sqlite"


def test_local_writes_enqueue_sync_outbox(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    account = service.create_account({"account_key": "gasgx-sync-01", "display_name": "GasGx Sync 01", "platforms": ["wechat"]})
    service.create_task({"account_id": account["id"], "platform": "wechat", "task_type": "publish", "payload": {"source": "test"}})
    service.save_brand_settings({"name": "GasGx Sync"})

    with connect() as conn:
        tables = [row["table_name"] for row in conn.execute("SELECT table_name FROM sync_outbox WHERE status = 'pending'")]

    assert "matrix_accounts" in tables
    assert "account_platforms" in tables
    assert "browser_profiles" in tables
    assert "automation_tasks" in tables
    assert "brand_settings" in tables


def test_sync_push_marks_outbox_items_synced(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-sync-02", "display_name": "GasGx Sync 02", "platforms": ["wechat"]})
    service.create_task({"account_id": account["id"], "platform": "wechat", "task_type": "publish", "payload": {"source": "test"}})

    class FakeSyncClient:
        def __init__(self) -> None:
            self.upserts: list[tuple[str, dict[str, Any], str]] = []
            self.deletes: list[tuple[str, dict[str, Any]]] = []

        def select_one(self, table: str, *, filters: dict[str, Any]) -> dict[str, Any] | None:
            return None

        def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: str) -> dict[str, Any]:
            self.upserts.append((table, payload, on_conflict))
            return payload

        def delete(self, table: str, *, filters: dict[str, Any]) -> bool:
            self.deletes.append((table, filters))
            return True

    fake = FakeSyncClient()
    monkeypatch.setattr(service, "_brand_supabase", lambda: fake)
    client = TestClient(create_app())

    response = client.post("/api/sync/supabase/push")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["pushed"] >= 1
    assert payload["status"]["pending"] == 0
    assert payload["status"]["synced"] >= payload["pushed"]
    assert {item[0] for item in fake.upserts} >= {"matrix_accounts", "automation_tasks"}


def test_sync_push_rebuilds_current_backup_snapshot(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-sync-clean", "display_name": "GasGx Sync Clean", "platforms": ["wechat"]})
    stale_dir = tmp_path / "pytest-stale" / "profiles" / "matrix" / "stale"
    with connect() as conn:
        conn.execute("UPDATE browser_profiles SET profile_dir = ? WHERE account_id = ?", (str(stale_dir), account["id"]))
        service._enqueue_sync_payload(
            conn,
            "browser_profiles",
            "upsert",
            {
                "id": 9999,
                "account_id": 9999,
                "profile_dir": str(stale_dir),
                "debug_port": 19999,
                "fingerprint_json": "{}",
                "created_at": 1,
                "updated_at": 1,
            },
        )

    class FakeSyncClient:
        def __init__(self) -> None:
            self.upserts: list[tuple[str, dict[str, Any], str]] = []

        def select_one(self, table: str, *, filters: dict[str, Any]) -> dict[str, Any] | None:
            return None

        def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: str) -> dict[str, Any]:
            self.upserts.append((table, payload, on_conflict))
            return payload

        def delete(self, table: str, *, filters: dict[str, Any]) -> bool:
            return True

    fake = FakeSyncClient()
    monkeypatch.setattr(service, "_brand_supabase", lambda: fake)

    result = service.push_sync_outbox_to_supabase()

    assert result["ok"] is True
    profile_payloads = [payload for table, payload, _ in fake.upserts if table == "browser_profiles"]
    assert profile_payloads
    assert all("pytest-stale" not in str(item.get("profile_dir") or "") for item in profile_payloads)
    assert str(profile_payloads[0]["profile_dir"]).endswith(str(Path("profiles") / "matrix" / "gasgx-sync-clean"))


def test_sync_push_browser_profile_falls_back_to_legacy_schema(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "gasgx-sync-legacy", "display_name": "GasGx Sync Legacy", "platforms": ["wechat"]})

    class LegacyBrowserProfileClient:
        def __init__(self) -> None:
            self.browser_upserts: list[tuple[dict[str, Any], str]] = []

        def select_one(self, table: str, *, filters: dict[str, Any]) -> dict[str, Any] | None:
            return None

        def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: str) -> dict[str, Any]:
            if table == "browser_profiles" and "account_id" in payload:
                raise SupabaseError(
                    'supabase request failed: 400 {"code":"PGRST204","message":"Could not find the '
                    "'account_id' column of 'browser_profiles' in the schema cache\"}"
                )
            if table == "browser_profiles":
                self.browser_upserts.append((payload, on_conflict))
            return payload

        def delete(self, table: str, *, filters: dict[str, Any]) -> bool:
            return True

    fake = LegacyBrowserProfileClient()
    monkeypatch.setattr(service, "_brand_supabase", lambda: fake)

    result = service.push_sync_outbox_to_supabase()

    assert result["ok"] is True
    assert fake.browser_upserts
    legacy_payload, conflict = fake.browser_upserts[0]
    assert conflict == "account_platform_id"
    assert "account_id" not in legacy_payload
    assert "id" not in legacy_payload
    assert legacy_payload["account_platform_id"] > 0


def test_sync_payload_enabled_fields_stay_integer_for_supabase() -> None:
    ap = service._sync_payload_for_supabase("account_platforms", {"enabled": True})
    route = service._sync_payload_for_supabase("notification_routes", {"enabled": 0})
    robot = service._sync_payload_for_supabase("ai_robot_configs", {"enabled": "1"})

    assert ap["enabled"] == 1
    assert isinstance(ap["enabled"], int)
    assert route["enabled"] == 0
    assert isinstance(route["enabled"], int)
    assert robot["enabled"] == 1
    assert isinstance(robot["enabled"], int)


def test_sync_push_failure_keeps_local_api_available(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "gasgx-sync-fail", "display_name": "GasGx Sync Fail", "platforms": ["wechat"]})

    class FailingSyncClient:
        def select_one(self, table: str, *, filters: dict[str, Any]) -> dict[str, Any] | None:
            return None

        def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: str) -> dict[str, Any]:
            raise TimeoutError("supabase timeout")

        def delete(self, table: str, *, filters: dict[str, Any]) -> bool:
            raise TimeoutError("supabase timeout")

    monkeypatch.setattr(service, "_brand_supabase", lambda: FailingSyncClient())
    client = TestClient(create_app())

    pushed = client.post("/api/sync/supabase/push")
    assert pushed.status_code == 200
    assert pushed.json()["ok"] is False
    assert pushed.json()["status"]["failed"] >= 1
    assert client.get("/api/accounts").status_code == 200
    retry = client.post("/api/sync/retry")
    assert retry.status_code == 200
    assert retry.json()["retried"] >= 1


def test_sync_pull_imports_remote_accounts_to_sqlite(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    class FakePullClient:
        def select(self, table: str, *, order: str = "", filters: dict[str, Any] | None = None, columns: str = "*") -> list[dict[str, Any]]:
            del order, filters, columns
            if table == "matrix_accounts":
                return [
                    {
                        "id": 31,
                        "account_key": "gasgx-remote-31",
                        "display_name": "GasGx Remote 31",
                        "niche": "gas",
                        "status": "active",
                        "notes": "remote",
                        "created_at": 100,
                        "updated_at": 200,
                    }
                ]
            if table == "account_platforms":
                return [
                    {
                        "id": 3101,
                        "account_id": 31,
                        "platform": "wechat",
                        "handle": "remote31",
                        "enabled": True,
                        "capability_status": "registered",
                        "login_status": "ready",
                        "created_at": 100,
                        "updated_at": 200,
                    }
                ]
            if table == "browser_profiles":
                return [
                    {
                        "id": 31001,
                        "account_id": 31,
                        "profile_dir": str(tmp_path / "profiles" / "matrix" / "gasgx-remote-31"),
                        "debug_port": 12331,
                        "fingerprint_json": {"provider": "remote"},
                        "created_at": 100,
                        "updated_at": 200,
                    }
                ]
            raise AssertionError(f"unexpected table {table}")

    monkeypatch.setattr(service, "_brand_supabase", lambda: FakePullClient())
    client = TestClient(create_app())

    response = client.post("/api/sync/supabase/pull")

    assert response.status_code == 200
    payload = response.json()
    assert payload["accounts"] == 1
    assert payload["platforms"] == 1
    assert payload["profiles"] == 1
    account = service.get_account(31)
    assert account is not None
    assert account["display_name"] == "GasGx Remote 31"
    assert account["platforms"][0]["platform"] == "wechat"
    assert account["platforms"][0]["debug_port"] == 12331


def test_sync_pull_does_not_mark_existing_local_outbox_rows_synced(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "gasgx-local-pending", "display_name": "GasGx Local Pending", "platforms": ["wechat"]})

    class EmptyPullClient:
        def select(self, table: str, *, order: str = "", filters: dict[str, Any] | None = None, columns: str = "*") -> list[dict[str, Any]]:
            del table, order, filters, columns
            return []

    monkeypatch.setattr(service, "_brand_supabase", lambda: EmptyPullClient())
    client = TestClient(create_app())
    response = client.post("/api/sync/supabase/pull")
    assert response.status_code == 200

    with connect() as conn:
        rows = [
            (str(row["table_name"]), str(row["status"]))
            for row in conn.execute(
                "SELECT table_name, status FROM sync_outbox WHERE table_name IN ('matrix_accounts', 'account_platforms', 'browser_profiles')"
            ).fetchall()
        ]
    assert rows
    assert all(status == "pending" for _, status in rows)


def test_sync_pull_handles_remote_account_key_conflict(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    local = service.create_account({"account_key": "gasgx-dup-01", "display_name": "Local Name", "platforms": ["wechat"]})

    class ConflictPullClient:
        def select(self, table: str, *, order: str = "", filters: dict[str, Any] | None = None, columns: str = "*") -> list[dict[str, Any]]:
            del order, filters, columns
            if table == "matrix_accounts":
                return [
                    {
                        "id": int(local["id"]) + 1000,
                        "account_key": "gasgx-dup-01",
                        "display_name": "Remote Name",
                        "niche": "remote-niche",
                        "status": "paused",
                        "notes": "from-remote",
                        "created_at": 100,
                        "updated_at": 9999999999,
                    }
                ]
            if table == "account_platforms":
                return [
                    {
                        "id": 7001,
                        "account_id": int(local["id"]) + 1000,
                        "platform": "wechat",
                        "handle": "remote-handle",
                        "enabled": True,
                        "capability_status": "registered",
                        "login_status": "ready",
                        "created_at": 100,
                        "updated_at": 9999999999,
                    }
                ]
            if table == "browser_profiles":
                return [
                    {
                        "id": 7002,
                        "account_id": int(local["id"]) + 1000,
                        "profile_dir": str(tmp_path / "profiles" / "matrix" / "gasgx-dup-01"),
                        "debug_port": 12999,
                        "fingerprint_json": {"provider": "remote"},
                        "created_at": 100,
                        "updated_at": 9999999999,
                    }
                ]
            raise AssertionError(f"unexpected table {table}")

    monkeypatch.setattr(service, "_brand_supabase", lambda: ConflictPullClient())
    client = TestClient(create_app())
    response = client.post("/api/sync/supabase/pull")
    assert response.status_code == 200
    assert response.json()["accounts"] == 1

    refreshed = service.get_account(int(local["id"]))
    assert refreshed is not None
    assert refreshed["display_name"] == "Remote Name"
    assert refreshed["status"] == "paused"
    wechat = next(item for item in refreshed["platforms"] if item["platform"] == "wechat")
    assert wechat["handle"] == "remote-handle"
    assert int(wechat["debug_port"]) == 12999


def test_wechat_stats_account_snapshots_are_idempotent_and_drive_summary(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})

    first = service.import_stats(
        {
            "account_snapshots": [
                {
                    "account_id": account["id"],
                    "platform": "wechat",
                    "stat_date": "2026-05-04",
                    "views": 1000,
                    "likes": 80,
                    "comments": 12,
                    "followers": 300,
                    "works_count": 2,
                }
            ],
            "video_snapshots": [
                {"account_id": account["id"], "platform": "wechat", "stat_date": "2026-05-04", "video_ref": "v1", "title": "A", "views": 600},
                {"account_id": account["id"], "platform": "wechat", "stat_date": "2026-05-04", "video_ref": "v2", "title": "B", "views": 400},
            ],
        }
    )
    second = service.import_stats(
        {
            "account_snapshots": [
                {
                    "account_id": account["id"],
                    "platform": "wechat",
                    "stat_date": "2026-05-04",
                    "views": 1200,
                    "likes": 90,
                    "comments": 13,
                    "followers": 310,
                    "works_count": 2,
                }
            ],
            "video_snapshots": [
                {"account_id": account["id"], "platform": "wechat", "stat_date": "2026-05-04", "video_ref": "v1", "title": "A", "views": 700},
            ],
        }
    )

    assert first["account_snapshots"] == 1
    assert second["account_snapshots"] == 1
    account_stats = service.list_wechat_account_stats(account_id=account["id"], stat_date="2026-05-04")
    assert len(account_stats) == 1
    assert account_stats[0]["views"] == 1200
    video_stats = service.list_stats(account_id=account["id"], platform="wechat", stat_date="2026-05-04")
    assert sorted(item["video_ref"] for item in video_stats) == ["v1", "v2"]
    assert next(item for item in video_stats if item["video_ref"] == "v1")["views"] == 700
    assert service.dashboard_summary()["views"] == 1200


def test_wechat_stats_parser_extracts_account_and_work_metrics() -> None:
    parsed = wechat_stats_capture.parse_wechat_stats_payload(
        {
            "text": "播放量 1.2万\n点赞 860\n评论 42\n分享 18\n粉丝数 5000\n新增粉丝 +120\n主页访问量 300\n完播率 41.5%\n互动率 7.2%",
            "works": [
                {"title": "燃气机组现场", "video_ref": "feed-1", "views": "8000", "likes": "600"},
                {"title": "油田自发电", "video_ref": "feed-2", "views": "4000", "comments": "12"},
            ],
        },
        account_id=7,
        target_date="2026-05-04",
    )

    assert parsed["account_snapshot"]["views"] == 12000
    assert parsed["account_snapshot"]["followers"] == 5000
    assert parsed["account_snapshot"]["completed_rate"] == 41.5
    assert len(parsed["video_snapshots"]) == 2
    assert parsed["video_snapshots"][0]["feed_id"] == "feed-1"


def test_wechat_account_match_token_uses_suffix_after_gasgx_prefix() -> None:
    assert wechat_stats_capture._normalize_account_match_token("GasGx | 阿根廷-01") == wechat_stats_capture._normalize_account_match_token("阿根廷 01")
    assert wechat_stats_capture._normalize_account_match_token("GASGX|燃气A组") == wechat_stats_capture._normalize_account_match_token("燃气A组")
    assert wechat_stats_capture._normalize_account_match_token("普通账号B") == wechat_stats_capture._normalize_account_match_token("普通账号 B")


def test_wechat_stats_capture_dry_run_uses_authorized_wechat_profiles(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})

    result = wechat_stats_capture.run_wechat_stats_capture(target_date="2026-05-04", limit=1, dry_run=True)

    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["target_date"] == "2026-05-04"
    assert result["planned_accounts"][0]["debug_port"] >= 12000
    assert service.latest_wechat_stats_capture_run()["status"] == "completed"


def test_wechat_stats_capture_dry_run_can_target_single_account(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    first = service.create_account({"account_key": "gasgx-one", "display_name": "GasGx One", "platforms": ["wechat"]})
    second = service.create_account({"account_key": "gasgx-two", "display_name": "GasGx Two", "platforms": ["wechat"]})

    result = wechat_stats_capture.run_wechat_stats_capture(target_date="2026-05-04", dry_run=True, account_id=int(second["id"]))

    assert result["ok"] is True
    assert result["account_id"] == int(second["id"])
    assert result["account_total"] == 1
    assert len(result["planned_accounts"]) == 1
    assert int(result["planned_accounts"][0]["account_id"]) == int(second["id"])
    assert int(result["planned_accounts"][0]["account_id"]) != int(first["id"])


def test_wechat_stats_capture_no_data_rows_marked_skipped(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-nodata", "display_name": "GasGx NoData", "platforms": ["wechat"]})

    def fake_capture(*_args, **_kwargs):
        return {
            "account_snapshots": [],
            "video_snapshots": [],
            "source_results": {"follower": {"rows": 0}, "post": {"rows": 0}},
            "missing_sources": [],
            "covered_dates": [],
            "no_data": True,
        }

    monkeypatch.setattr(wechat_stats_capture, "_capture_account", fake_capture)
    result = wechat_stats_capture.run_wechat_stats_capture(target_date="2026-05-17", dry_run=False, account_id=int(account["id"]))

    assert result["ok"] is True
    assert result["captured_accounts"] == 0
    assert result["skipped_accounts"] == 1
    assert result["failed_accounts"] == 0
    assert result["results"][0]["status"] == "skipped"
    assert result["results"][0]["reason"] == "no_data"


def test_wechat_stats_capture_new_tab_keep_open_does_not_close_capture_tab(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    class FakeTab:
        def __init__(self) -> None:
            self.close_calls = 0
            self.visited: list[str] = []

        def get(self, url: str) -> None:
            self.visited.append(str(url))

        def close(self) -> None:
            self.close_calls += 1

    class FakeOwnerPage(FakeTab):
        def __init__(self) -> None:
            super().__init__()
            self.capture_tab = FakeTab()
            self.new_tab_calls: list[bool] = []

        def new_tab(self, background: bool = False) -> FakeTab:
            self.new_tab_calls.append(bool(background))
            return self.capture_tab

    owner_page = FakeOwnerPage()
    disconnect_calls: list[Any] = []

    def fake_capture_source(_page, *, source_key: str, **_kwargs):
        if source_key == "follower":
            return {
                "table_rows": [["2026/05/17", "1", "2", "1", "101"]],
                "path": "",
                "source_hash": "",
                "original_path": "",
            }
        return {
            "table_rows": [["2026/05/17", "220", "16", "11", "9", "5", "3"]],
            "path": "",
            "source_hash": "",
            "original_path": "",
        }

    monkeypatch.setattr(wechat_stats_capture, "apply_cybercar_environment", lambda: None)
    monkeypatch.setattr(wechat_stats_capture.service, "_chrome_fingerprint_env", lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(wechat_stats_capture.engine, "_connect_chrome", lambda **_kwargs: owner_page)
    monkeypatch.setattr(wechat_stats_capture.engine, "inspect_platform_login_gate", lambda *_args, **_kwargs: {"needs_login": False})
    monkeypatch.setattr(wechat_stats_capture, "_assert_wechat_account_identity", lambda *_args, **_kwargs: {"checked": True})
    monkeypatch.setattr(wechat_stats_capture, "_capture_stats_source", fake_capture_source)
    monkeypatch.setattr(wechat_stats_capture.engine, "_disconnect_chrome_page_quietly", lambda page: disconnect_calls.append(page))

    parsed = wechat_stats_capture._capture_account(
        {
            "id": 88,
            "account_key": "gasgx-tab-test",
            "display_name": "GasGx Tab Test",
            "wechat_profile_dir": str(tmp_path / "profile"),
            "wechat_debug_port": 18088,
            "wechat_fingerprint": {},
        },
        target_date="2026-05-17",
        run_id="wechat-stats-test",
        open_capture_in_new_tab=True,
        capture_tab_foreground=True,
        keep_capture_tab_open=True,
        auto_open_browser=False,
    )

    assert owner_page.new_tab_calls == [False]
    assert owner_page.capture_tab.close_calls == 0
    assert disconnect_calls == []
    assert parsed["account_snapshots"]


def test_wechat_stats_capture_new_tab_closes_capture_tab_when_keep_open_disabled(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    class FakeTab:
        def __init__(self) -> None:
            self.close_calls = 0
            self.visited: list[str] = []

        def get(self, url: str) -> None:
            self.visited.append(str(url))

        def close(self) -> None:
            self.close_calls += 1

    class FakeOwnerPage(FakeTab):
        def __init__(self) -> None:
            super().__init__()
            self.capture_tab = FakeTab()
            self.new_tab_calls: list[bool] = []

        def new_tab(self, background: bool = False) -> FakeTab:
            self.new_tab_calls.append(bool(background))
            return self.capture_tab

    owner_page = FakeOwnerPage()
    disconnect_calls: list[Any] = []

    def fake_capture_source(_page, *, source_key: str, **_kwargs):
        if source_key == "follower":
            return {
                "table_rows": [["2026/05/17", "1", "2", "1", "101"]],
                "path": "",
                "source_hash": "",
                "original_path": "",
            }
        return {
            "table_rows": [["2026/05/17", "220", "16", "11", "9", "5", "3"]],
            "path": "",
            "source_hash": "",
            "original_path": "",
        }

    monkeypatch.setattr(wechat_stats_capture, "apply_cybercar_environment", lambda: None)
    monkeypatch.setattr(wechat_stats_capture.service, "_chrome_fingerprint_env", lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(wechat_stats_capture.engine, "_connect_chrome", lambda **_kwargs: owner_page)
    monkeypatch.setattr(wechat_stats_capture.engine, "inspect_platform_login_gate", lambda *_args, **_kwargs: {"needs_login": False})
    monkeypatch.setattr(wechat_stats_capture, "_assert_wechat_account_identity", lambda *_args, **_kwargs: {"checked": True})
    monkeypatch.setattr(wechat_stats_capture, "_capture_stats_source", fake_capture_source)
    monkeypatch.setattr(wechat_stats_capture.engine, "_disconnect_chrome_page_quietly", lambda page: disconnect_calls.append(page))

    parsed = wechat_stats_capture._capture_account(
        {
            "id": 89,
            "account_key": "gasgx-tab-test-2",
            "display_name": "GasGx Tab Test 2",
            "wechat_profile_dir": str(tmp_path / "profile2"),
            "wechat_debug_port": 18089,
            "wechat_fingerprint": {},
        },
        target_date="2026-05-17",
        run_id="wechat-stats-test-2",
        open_capture_in_new_tab=True,
        capture_tab_foreground=True,
        keep_capture_tab_open=False,
        auto_open_browser=False,
    )

    assert owner_page.new_tab_calls == [False]
    assert owner_page.capture_tab.close_calls == 1
    assert owner_page.close_calls == 1
    assert disconnect_calls == []
    assert parsed["account_snapshots"]


def test_stats_capture_run_now_api_accepts_account_id(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    captured: dict[str, Any] = {}

    def fake_trigger(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "status": "started"}

    monkeypatch.setattr("gasgx_distribution.web.trigger_matrix_wechat_stats_capture", fake_trigger)
    client = TestClient(create_app())
    response = client.post(
        "/api/jobs/matrix-wechat/stats-capture/run-now",
        json={
            "target_date": "2026-05-04",
            "dry_run": False,
            "account_id": 77,
            "keep_browser_open_on_login_required": True,
            "open_capture_in_new_tab": True,
            "capture_tab_foreground": True,
            "keep_capture_tab_open": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "started"
    assert captured["target_date"] == "2026-05-04"
    assert captured["dry_run"] is False
    assert int(captured["account_id"]) == 77
    assert captured["keep_browser_open_on_login_required"] is True
    assert captured["open_capture_in_new_tab"] is True
    assert captured["capture_tab_foreground"] is True
    assert captured["keep_capture_tab_open"] is True


def test_stats_capture_run_now_api_keeps_legacy_default_flags(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    captured: dict[str, Any] = {}

    def fake_trigger(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "status": "started"}

    monkeypatch.setattr("gasgx_distribution.web.trigger_matrix_wechat_stats_capture", fake_trigger)
    client = TestClient(create_app())
    response = client.post(
        "/api/jobs/matrix-wechat/stats-capture/run-now",
        json={
            "target_date": "2026-05-04",
            "dry_run": False,
            "account_id": 78,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "started"
    assert captured["open_capture_in_new_tab"] is False
    assert captured["capture_tab_foreground"] is False
    assert captured["keep_capture_tab_open"] is False


def test_wechat_engagement_run_now_api_accepts_payload(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    captured: dict[str, Any] = {}

    def fake_trigger(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "status": "started"}

    monkeypatch.setattr("gasgx_distribution.web.trigger_matrix_wechat_engagement_run_now", fake_trigger)
    client = TestClient(create_app())
    response = client.post(
        "/api/jobs/matrix-wechat/engagement/run-now",
        json={
            "account_id": 91,
            "enable_comment": True,
            "enable_private_message": False,
            "comment_limit": 5,
            "private_message_limit": 7,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "started"
    assert captured["account_id"] == 91
    assert captured["enable_comment"] is True
    assert captured["enable_private_message"] is False
    assert captured["comment_limit"] == 5
    assert captured["private_message_limit"] == 7


def test_run_terminal_wechat_auto_engagement_uses_account_scoped_state_files(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account(
        {
            "account_key": "engage-01",
            "display_name": "Engage 01",
            "platforms": ["wechat"],
        }
    )
    comment_calls: dict[str, Any] = {}
    private_calls: dict[str, Any] = {}
    monkeypatch.setattr(
        service.engine,
        "run_wechat_comment_reply",
        lambda **kwargs: comment_calls.update(kwargs) or {"ok": True, "replies_sent": 2, "reason": ""},
    )
    monkeypatch.setattr(
        service.engine,
        "run_wechat_private_message_reply",
        lambda **kwargs: private_calls.update(kwargs) or {"ok": True, "replies_sent": 1, "reason": ""},
    )
    monkeypatch.setattr(service, "load_app_config", lambda: {"notify": {"env_prefix": "CYBERCAR_NOTIFY_"}})

    result = service.run_terminal_wechat_auto_engagement(
        account_id=int(account["id"]),
        enable_comment=True,
        enable_private_message=True,
        comment_limit=5,
        private_message_limit=5,
    )

    assert result["status"] == "completed"
    assert result["ok"] is True
    assert result["state_files"]["comment_state"] == f"wechat_comment_reply_state_a{account['id']}.json"
    assert result["state_files"]["private_state"] == f"wechat_private_message_reply_state_a{account['id']}.json"
    assert comment_calls["max_posts_override"] == 5
    assert comment_calls["max_replies_override"] == 5
    assert private_calls["max_conversations_override"] == 5
    assert private_calls["max_replies_override"] == 5
    assert comment_calls["state_filename"] == f"wechat_comment_reply_state_a{account['id']}.json"
    assert private_calls["state_filename"] == f"wechat_private_message_reply_state_a{account['id']}.json"


def test_wechat_stats_csv_parsers_extract_rolling_7d_rows(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    follower_csv = tmp_path / "follower.csv"
    follower_csv.write_text(
        "时间,净增关注,新增关注,取消关注,关注者总数\n"
        "2026/05/11,1,2,1,100\n"
        "2026/05/12,2,4,2,102\n"
        "2026/05/13,3,6,3,105\n"
        "2026/05/14,4,8,4,109\n"
        "2026/05/15,5,10,5,114\n"
        "2026/05/16,6,12,6,120\n"
        "2026/05/17,7,14,7,127\n",
        encoding="utf-8",
    )
    post_csv = tmp_path / "post.csv"
    post_csv.write_text(
        "时间,播放,赞A,赞B,评论,分享,关注\n"
        "2026/05/11,100,10,5,3,2,1\n"
        "2026/05/12,120,11,6,4,2,1\n"
        "2026/05/13,140,12,7,5,3,1\n"
        "2026/05/14,160,13,8,6,3,2\n"
        "2026/05/15,180,14,9,7,4,2\n"
        "2026/05/16,200,15,10,8,4,3\n"
        "2026/05/17,220,16,11,9,5,3\n",
        encoding="utf-8",
    )
    follower_rows = wechat_stats_capture.parse_follower_stats_csv(follower_csv, account_id=9, meta={"source_file": "follower.csv"})
    post_rows = wechat_stats_capture.parse_post_stats_csv(post_csv, account_id=9, meta={"source_file": "post.csv"})

    assert len(follower_rows) == 7
    assert follower_rows[0]["stat_date"] == "2026-05-11"
    assert follower_rows[-1]["followers"] == 127
    assert follower_rows[-1]["raw_json"]["sources"]["follower"]["new_followers"] == 14
    assert follower_rows[-1]["raw_json"]["sources"]["follower"]["unfollows"] == 7
    assert len(post_rows) == 7
    assert post_rows[0]["views"] == 100
    assert post_rows[-1]["likes"] == 27
    assert post_rows[-1]["comments"] == 9
    assert post_rows[-1]["shares"] == 5
    assert post_rows[-1]["raw_json"]["sources"]["post"]["post_follows"] == 3


def test_wechat_account_snapshot_merge_keeps_dual_sources_same_day(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "merge-01", "display_name": "Merge 01", "platforms": ["wechat"]})

    service.import_stats(
        {
            "account_snapshots": [
                {
                    "account_id": account["id"],
                    "platform": "wechat",
                    "stat_date": "2026-05-17",
                    "followers": 1200,
                    "follower_delta": 15,
                    "source": "wechat_official_csv",
                    "raw_json": {"capture_window": "rolling_7d", "sources": {"follower": {"new_followers": 20, "unfollows": 5}}},
                }
            ]
        }
    )
    service.import_stats(
        {
            "account_snapshots": [
                {
                    "account_id": account["id"],
                    "platform": "wechat",
                    "stat_date": "2026-05-17",
                    "views": 2200,
                    "likes": 120,
                    "comments": 18,
                    "shares": 11,
                    "source": "wechat_official_csv",
                    "raw_json": {"capture_window": "rolling_7d", "sources": {"post": {"post_follows": 9}}},
                }
            ]
        }
    )

    rows = service.list_wechat_account_stats(account_id=account["id"], stat_date="2026-05-17")
    assert len(rows) == 1
    row = rows[0]
    assert row["followers"] == 1200
    assert row["follower_delta"] == 15
    assert row["views"] == 2200
    assert row["likes"] == 120
    payload = json.loads(str(row["raw_json"]))
    assert payload["sources"]["follower"]["new_followers"] == 20
    assert payload["sources"]["post"]["post_follows"] == 9


def test_stats_weekly_summary_and_csv_export(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    account_a = service.create_account({"account_key": "wx-a", "display_name": "WX A", "platforms": ["wechat"]})
    account_b = service.create_account({"account_key": "wx-b", "display_name": "WX B", "platforms": ["wechat"]})

    for day in range(11, 18):
        stat_date = f"2026-05-{day:02d}"
        service.import_stats(
            {
                "account_snapshots": [
                    {
                        "account_id": account_a["id"],
                        "platform": "wechat",
                        "stat_date": stat_date,
                        "views": 100 + day,
                        "likes": 10 + day,
                        "comments": 2,
                        "shares": 1,
                        "followers": 1000 + day,
                        "follower_delta": 3,
                        "source": "wechat_official_csv",
                        "raw_json": {
                            "capture_window": "rolling_7d",
                            "sources": {
                                "follower": {"new_followers": 4, "unfollows": 1},
                                "post": {"post_follows": 1},
                            },
                        },
                    }
                ]
            }
        )
    for day in range(13, 18):
        stat_date = f"2026-05-{day:02d}"
        service.import_stats(
            {
                "account_snapshots": [
                    {
                        "account_id": account_b["id"],
                        "platform": "wechat",
                        "stat_date": stat_date,
                        "views": 50 + day,
                        "likes": 5 + day,
                        "comments": 1,
                        "shares": 1,
                        "followers": 800 + day,
                        "follower_delta": 1,
                        "source": "wechat_official_csv",
                        "raw_json": {"capture_window": "rolling_7d", "sources": {"post": {"post_follows": 1}}},
                    }
                ]
            }
        )

    service.upsert_wechat_stats_capture_run(
        {
            "run_id": "run-login-required",
            "target_date": "2026-05-17",
            "status": "completed",
            "account_total": 2,
            "captured_accounts": 1,
            "skipped_accounts": 1,
            "failed_accounts": 0,
            "payload_json": {
                "results": [
                    {"account_id": account_a["id"], "status": "captured"},
                    {"account_id": account_b["id"], "status": "skipped", "reason": "login_required"},
                ]
            },
        }
    )

    weekly = client.get("/api/stats/weekly-summary?platform=wechat")
    assert weekly.status_code == 200
    payload = weekly.json()
    assert payload["window"]["start_date"] == "2026-05-11"
    assert payload["window"]["end_date"] == "2026-05-17"
    assert payload["account_total"] == 2
    rows = {int(item["account_id"]): item for item in payload["rows"]}
    assert rows[account_a["id"]]["capture_status"] == "完整"
    assert rows[account_a["id"]]["coverage_days"] == 7
    assert rows[account_b["id"]]["capture_status"] == "未登录"
    assert rows[account_b["id"]]["coverage_days"] == 5
    assert "follower" in rows[account_b["id"]]["missing_sources"]
    assert payload["status_counts"]["完整"] == 1
    assert payload["status_counts"]["未登录"] == 1
    assert payload["totals"]["views_7d"] > 0

    weekly_csv = client.get("/api/stats/weekly-summary.csv?platform=wechat")
    assert weekly_csv.status_code == 200
    assert weekly_csv.headers.get("content-type", "").startswith("text/csv")
    csv_text = weekly_csv.text
    assert "账号ID,账号标识,账号名称,平台,近7天播放" in csv_text
    assert "wx-a" in csv_text
    assert "WX A" in csv_text


def test_delete_account_removes_related_platform_profile_tasks_and_stats(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    account = client.post(
        "/api/accounts",
        json={"account_key": "delete-01", "display_name": "Delete 01", "platforms": ["wechat", "douyin"]},
    ).json()
    task = client.post("/api/tasks", json={"account_id": account["id"], "platform": "wechat", "task_type": "publish"}).json()
    assert client.post("/api/stats/import", json={"account_id": account["id"], "platform": "wechat", "video_ref": "v1", "views": 10}).status_code == 200
    service.import_stats({"account_snapshots": [{"account_id": account["id"], "platform": "wechat", "stat_date": "2026-05-04", "views": 10}]})

    deleted = client.delete(f"/api/accounts/{account['id']}")

    assert deleted.status_code == 200
    assert deleted.json() == {"ok": True, "deleted": account["id"]}
    assert client.delete(f"/api/accounts/{account['id']}").status_code == 404
    assert client.get("/api/accounts").json() == []
    assert client.get(f"/api/tasks/{task['id']}").status_code == 404
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS c FROM account_platforms WHERE account_id = ?", (account["id"],)).fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM browser_profiles WHERE account_id = ?", (account["id"],)).fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM video_stats_snapshots WHERE account_id = ?", (account["id"],)).fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM wechat_stats_account_snapshots WHERE account_id = ?", (account["id"],)).fetchone()["c"] == 0


def test_repair_account_configs_api_fills_missing_platforms_and_profile(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    account = client.post(
        "/api/accounts",
        json={"account_key": "repair-01", "display_name": "Repair 01", "platforms": ["wechat"]},
    ).json()
    with connect() as conn:
        conn.execute("DELETE FROM browser_profiles WHERE account_id = ?", (account["id"],))

    repaired = client.post("/api/accounts/repair-config")

    assert repaired.status_code == 200
    payload = repaired.json()
    assert payload["checked_accounts"] == 1
    assert payload["repaired_accounts"] == 1
    assert payload["created_platforms"] == len(SUPPORTED_PLATFORMS) - 1
    assert payload["created_profiles"] == 1
    account_after = client.get("/api/accounts").json()[0]
    assert {item["platform"] for item in account_after["platforms"]} == {item.key for item in SUPPORTED_PLATFORMS}
    with connect() as conn:
        profile = conn.execute("SELECT * FROM browser_profiles WHERE account_id = ?", (account["id"],)).fetchone()
        assert profile is not None
        assert profile["fingerprint_json"] not in {"", "{}"}

    second = client.post("/api/accounts/repair-config").json()
    assert second["repaired_accounts"] == 0
    assert second["created_platforms"] == 0
    assert second["created_profiles"] == 0

    wrong_profile_dir = tmp_path / "profiles" / "matrix" / "wrong-account"
    with connect() as conn:
        before = conn.execute("SELECT debug_port FROM browser_profiles WHERE account_id = ?", (account["id"],)).fetchone()
        conn.execute("UPDATE browser_profiles SET profile_dir = ? WHERE account_id = ?", (str(wrong_profile_dir), account["id"]))
        conn.commit()

    third = client.post("/api/accounts/repair-config").json()
    assert third["repaired_accounts"] == 1
    assert third["updated_profiles"] == 1
    with connect() as conn:
        profile = conn.execute("SELECT * FROM browser_profiles WHERE account_id = ?", (account["id"],)).fetchone()
        assert profile["profile_dir"] == str(service.profile_dir_for(account["account_key"]))
        assert profile["debug_port"] == before["debug_port"]

    fourth = client.post("/api/accounts/repair-config").json()
    assert fourth["repaired_accounts"] == 0


def test_control_plane_provisions_isolated_brand_databases(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    first = client.post("/control/brands", json={"id": "brand-a", "name": "Brand A", "domain": "a.example.test"})
    second = client.post("/control/brands", json={"id": "brand-b", "name": "Brand B", "domain": "b.example.test"})
    assert first.status_code == 200
    assert second.status_code == 200
    assert client.post("/control/brands/brand-a/provision").status_code == 200
    assert client.post("/control/brands/brand-b/provision").status_code == 200

    created = client.post(
        "/api/accounts",
        headers={"x-brand-instance": "brand-a"},
        json={"account_key": "a-01", "display_name": "A 01", "platforms": ["wechat"]},
    )
    assert created.status_code == 200
    assert len(client.get("/api/accounts", headers={"x-brand-instance": "brand-a"}).json()) == 1
    assert client.get("/api/accounts", headers={"x-brand-instance": "brand-b"}).json() == []


def test_brand_settings_are_server_side_per_brand(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    assert client.post("/control/brands", json={"id": "brand-a", "name": "Brand A", "domain": "a.example.test"}).status_code == 200
    assert client.post("/control/brands/brand-a/provision").status_code == 200

    patched = client.patch(
        "/api/brand",
        headers={"x-brand-instance": "brand-a"},
        json={"name": "Brand A", "slogan": "Client Console", "theme_id": "methane-teal", "default_account_prefix": "BA"},
    )
    assert patched.status_code == 200
    assert patched.json()["name"] == "Brand A"

    brand_a = client.get("/api/brand", headers={"x-brand-instance": "brand-a"}).json()
    default_brand = client.get("/api/brand").json()
    assert brand_a["settings"]["name"] == "Brand A"
    assert default_brand["settings"]["name"] == "GasGx"


def test_system_supabase_health_reports_current_brand(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    assert client.post("/control/brands", json={"id": "brand-a", "name": "Brand A", "domain": "a.example.test"}).status_code == 200
    assert client.post("/control/brands/brand-a/provision").status_code == 200

    health = client.get("/api/system/supabase-health", headers={"x-brand-instance": "brand-a"})

    assert health.status_code == 200
    data = health.json()
    assert data["ok"] is True
    assert data["brand_id"] == "brand-a"
    assert data["schema_version"]
    checks = {item["name"]: item for item in data["checks"]}
    assert checks["control_plane"]["details"]["backend"] == "sqlite"
    assert checks["tenant"]["details"]["brand_id"] == "brand-a"
    assert checks["brand_database"]["details"]["brand_name"] == "GasGx"
    assert checks["ai_robot_queue"]["details"]["queued_message_count"] == 0


def test_video_matrix_template_preview_returns_png(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    response = client.post(
        "/api/video-matrix/template-preview",
        json={
            "template": {
                "name": "Preview Test",
                "show_hud": True,
                "show_slogan": True,
                "show_title": True,
                "watermark_mode": "auto",
                "watermark_position": "bottom_right",
                "watermark_font_size": 36,
                "watermark_color": "#FFFFFF",
                "hud_bar_y": 1700,
                "hud_bar_height": 140,
                "hud_bar_width": 760,
                "hud_x": 60,
                "hud_y": 1760,
                "hud_font_size": 30,
                "slogan_x": 80,
                "slogan_y": 240,
                "slogan_font_size": 58,
                "title_x": 80,
                "title_y": 340,
                "title_font_size": 32,
                "hud_bar_color": "#0E1A10",
                "hud_bar_opacity": 0.38,
                "primary_color": "#5DD62C",
                "secondary_color": "#FFFFFF",
            },
            "slogan": "Stop Flaring. Start Hashing.",
            "title": "Gas To Compute",
            "hud_text": "Gas Input -> Power\nPower -> Hashrate",
            "sequence_tag": "0516-01",
        },
    )

    assert response.status_code == 200
    assert response.json()["data_url"].startswith("data:image/png;base64,")


def test_video_matrix_cover_template_library_can_be_rebuilt(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setattr("gasgx_distribution.video_matrix_api.COVER_TEMPLATES_PATH", tmp_path / "cover_templates.json")
    client = TestClient(create_app())

    response = client.put(
        "/api/video-matrix/cover-templates",
        json={
            "selected_cover": "cover_template_01",
            "templates": {
                "cover_template_01": {
                    "name": "Edited Cover",
                    "mask_mode": "top_gradient",
                    "mask_color": "#5DD62C",
                    "mask_opacity": 0.8,
                    "profile_brand_text": "GasGx Custom",
                    "brand_font_size": 64,
                    "profile_brand_offset_y": 22,
                    "primary_color": "#FFFFFF",
                    "secondary_color": "#D6DED2",
                }
            },
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert list(data["cover_templates"]) == ["cover_template_01"]
    assert data["cover_templates"]["cover_template_01"]["mask_mode"] == "top_gradient"
    assert data["cover_templates"]["cover_template_01"]["profile_brand_text"] == "GasGx Custom"


def test_video_matrix_source_selection_respects_active_categories(tmp_path: Path) -> None:
    categories = [{"id": "category_A", "label": "A"}, {"id": "category_B", "label": "B"}]
    source_root = tmp_path / "incoming"
    category_a = source_root / "category_A"
    category_b = source_root / "category_B"
    category_a.mkdir(parents=True)
    category_b.mkdir(parents=True)
    first = category_a / "machine.mp4"
    second = category_b / "screen.mp4"
    first.write_bytes(b"a")
    second.write_bytes(b"b")

    selected = _select_source_files(
        source_root,
        recent_limits={"category_A": 5, "category_B": 5},
        categories=categories,
        active_category_ids=["category_B"],
    )

    assert selected == [second]


def test_control_upgrade_records_each_active_brand(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    assert client.post("/control/brands", json={"id": "brand-a", "name": "Brand A", "domain": "a.example.test"}).status_code == 200
    assert client.post("/control/brands", json={"id": "brand-b", "name": "Brand B", "domain": "b.example.test"}).status_code == 200

    run = client.post("/control/upgrades")
    assert run.status_code == 200
    data = run.json()
    assert data["status"] == "succeeded"
    assert {item["brand_id"] for item in data["items"]} >= {"default", "brand-a", "brand-b"}


def test_control_plane_can_use_supabase_rest_backend(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("CONTROL_DB_BACKEND", "supabase")
    monkeypatch.setenv("CONTROL_SUPABASE_URL", "https://control.example.supabase.co")
    monkeypatch.setenv("CONTROL_SUPABASE_SERVICE_ROLE_KEY", "service-role")
    store: dict[str, list[dict[str, object]]] = {
        "brand_instances": [],
        "brand_templates": [],
        "upgrade_runs": [],
        "upgrade_run_items": [],
    }
    counters = {"upgrade_runs": 0, "upgrade_run_items": 0}

    class FakeResponse:
        def __init__(self, payload: object, status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code
            self.text = json.dumps(payload)

        def json(self) -> object:
            return self._payload

    def table_from_url(url: str) -> str:
        return url.rstrip("/").split("/")[-1]

    def fake_get(url: str, headers: dict[str, str], params: dict[str, str], timeout: int) -> FakeResponse:
        del headers, timeout
        rows = list(store[table_from_url(url)])
        for key, value in params.items():
            if key in {"select", "order"}:
                continue
            expected = value.removeprefix("eq.")
            rows = [row for row in rows if str(row.get(key)) == expected]
        return FakeResponse(rows)

    def fake_post(url: str, headers: dict[str, str], params: dict[str, str] | None = None, json: dict[str, object] | None = None, timeout: int = 30) -> FakeResponse:
        del headers, timeout
        table = table_from_url(url)
        payload = dict(json or {})
        if params and params.get("on_conflict"):
            key = str(params["on_conflict"])
            for row in store[table]:
                if row.get(key) == payload.get(key):
                    row.update(payload)
                    return FakeResponse([row])
        if table in counters:
            counters[table] += 1
            payload.setdefault("id", counters[table])
        store[table].append(payload)
        return FakeResponse([payload])

    def fake_patch(url: str, headers: dict[str, str], params: dict[str, str], json: dict[str, object], timeout: int) -> FakeResponse:
        del headers, timeout
        table = table_from_url(url)
        matches = store[table]
        for key, value in params.items():
            expected = value.removeprefix("eq.")
            matches = [row for row in matches if str(row.get(key)) == expected]
        if matches:
            matches[0].update(json)
            return FakeResponse([matches[0]])
        return FakeResponse([])

    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.get", fake_get)
    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.post", fake_post)
    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.patch", fake_patch)

    service._invalidate_terminal_execution_state_cache()
    client = TestClient(create_app())
    created = client.post(
        "/control/brands",
        json={"id": "brand-s", "name": "Brand Supabase", "domain": "s.example.test", "supabase_url": "https://brand.supabase.co", "service_key_ref": "vault://brand-s"},
    )
    assert created.status_code == 200
    assert created.json()["has_service_key_ref"] is True
    assert "service_key_ref" not in created.json()

    brands = client.get("/control/brands")
    assert brands.status_code == 200
    assert {item["id"] for item in brands.json()} >= {"default", "brand-s"}

    upgrade = client.post("/control/upgrades")
    assert upgrade.status_code == 200
    assert upgrade.json()["status"] == "succeeded"


def test_brand_runtime_can_use_supabase_for_brand_and_ai_robot(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")
    monkeypatch.setenv("BRAND_SUPABASE_SERVICE_KEY", "brand-service")
    store: dict[str, list[dict[str, object]]] = {
        "brand_settings": [],
        "ai_robot_configs": [],
        "ai_robot_messages": [],
    }
    counters = {"ai_robot_configs": 0, "ai_robot_messages": 0}

    class FakeResponse:
        def __init__(self, payload: object, status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code
            self.text = json.dumps(payload)

        def json(self) -> object:
            return self._payload

    def table_from_url(url: str) -> str:
        return url.rstrip("/").split("/")[-1]

    def fake_get(url: str, headers: dict[str, str], params: dict[str, str], timeout: int) -> FakeResponse:
        del headers, timeout
        rows = list(store[table_from_url(url)])
        for key, value in params.items():
            if key in {"select", "order"}:
                continue
            expected = value.removeprefix("eq.")
            rows = [row for row in rows if str(row.get(key)) == expected]
        return FakeResponse(rows)

    def fake_post(url: str, headers: dict[str, str], params: dict[str, str] | None = None, json: dict[str, object] | None = None, timeout: int = 30) -> FakeResponse:
        del headers, timeout
        table = table_from_url(url)
        payload = dict(json or {})
        if params and params.get("on_conflict"):
            key = str(params["on_conflict"])
            for row in store[table]:
                if str(row.get(key)) == str(payload.get(key)):
                    row.update(payload)
                    return FakeResponse([row])
        if table in counters:
            counters[table] += 1
            payload.setdefault("id", counters[table])
        store[table].append(payload)
        return FakeResponse([payload])

    def fake_patch(url: str, headers: dict[str, str], params: dict[str, str], json: dict[str, object], timeout: int) -> FakeResponse:
        del headers, timeout
        table = table_from_url(url)
        matches = store[table]
        for key, value in params.items():
            expected = value.removeprefix("eq.")
            matches = [row for row in matches if str(row.get(key)) == expected]
        if matches:
            matches[0].update(json)
            return FakeResponse([matches[0]])
        return FakeResponse([])

    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.get", fake_get)
    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.post", fake_post)
    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.patch", fake_patch)

    runtime = {"supabase_url": "https://brand.example.supabase.co", "service_key_ref": "env:BRAND_SUPABASE_SERVICE_KEY"}
    with service.use_brand_runtime(runtime):
        saved_brand = service.save_brand_settings({"name": "Brand Remote", "slogan": "Remote Console"})
        assert saved_brand["name"] == "Brand Remote"

        config = service.save_ai_robot_config(
            "telegram",
            {
                "enabled": True,
                "bot_name": "Remote Bot",
                "webhook_url": "https://example.test/bot",
                "webhook_secret": "send-secret",
                "signing_secret": "sign-secret",
                "target_id": "chat-remote",
            },
        )
        assert config["has_signing_secret"] is True
        assert "sign-secret" not in json.dumps(config)

        message = service.enqueue_ai_robot_message("telegram", {"message_type": "text", "text": "hello"}, test=True)
        assert message["status"] == "pending"

    assert store["brand_settings"][0]["name"] == "Brand Remote"
    assert store["ai_robot_configs"][0]["signing_secret"] == "sign-secret"
    assert store["ai_robot_messages"][0]["payload_json"]["test"] is True


def test_supabase_client_does_not_use_publishable_key_as_service_role(monkeypatch) -> None:
    monkeypatch.setenv("CONTROL_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("CONTROL_SUPABASE_SERVICE_ROLE_KEY", "")
    monkeypatch.setenv("CONTROL_SUPABASE_SERVICE_KEY", "")
    monkeypatch.setenv("CONTROL_SUPABASE_KEY", "publishable")
    monkeypatch.setenv("SUPABASE_KEY", "publishable")

    try:
        SupabaseRestClient.from_env(prefix="CONTROL_SUPABASE")
    except SupabaseError as exc:
        assert "service role key" in str(exc)
    else:
        raise AssertionError("publishable key was accepted as service role")


def test_brand_supabase_accounts_tasks_and_stats(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")
    monkeypatch.setenv("BRAND_SUPABASE_SERVICE_KEY", "brand-service")
    store: dict[str, list[dict[str, object]]] = {
        "matrix_accounts": [],
        "account_platforms": [],
        "browser_profiles": [],
        "automation_tasks": [],
        "video_stats_snapshots": [],
        "wechat_stats_account_snapshots": [],
        "wechat_stats_capture_runs": [],
    }
    counters = {key: 0 for key in store}

    class FakeResponse:
        def __init__(self, payload: object, status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code
            self.text = json.dumps(payload)

        def json(self) -> object:
            return self._payload

    def table_from_url(url: str) -> str:
        return url.rstrip("/").split("/")[-1]

    def matches(row: dict[str, object], params: dict[str, str]) -> bool:
        for key, value in params.items():
            if key in {"select", "order"}:
                continue
            if value.startswith("eq.") and str(row.get(key)) != value[3:]:
                return False
            if value.startswith("in.("):
                values = value[4:-1].split(",")
                if str(row.get(key)) not in values:
                    return False
        return True

    def fake_get(url: str, headers: dict[str, str], params: dict[str, str], timeout: int) -> FakeResponse:
        del headers, timeout
        rows = [row for row in store[table_from_url(url)] if matches(row, params)]
        return FakeResponse(rows)

    rpc_calls: list[str] = []

    def fake_post(url: str, headers: dict[str, str], params: dict[str, str] | None = None, json: dict[str, object] | None = None, timeout: int = 30) -> FakeResponse:
        del headers, params, timeout
        if "/rpc/" in url:
            rpc_calls.append(url.rstrip("/").split("/")[-1])
            return FakeResponse(
                {
                    "accounts": len(store["matrix_accounts"]),
                    "platforms": len([row for row in store["account_platforms"] if row.get("enabled", 1)]),
                    "running_tasks": len([row for row in store["automation_tasks"] if row.get("status") in {"pending", "running"}]),
                    "failed_tasks": len([row for row in store["automation_tasks"] if row.get("status") == "failed"]),
                    "unsupported_tasks": len([row for row in store["automation_tasks"] if row.get("status") == "unsupported"]),
                    "remaining_material_videos": 0,
                    "views": sum(int(row.get("views") or 0) for row in store["video_stats_snapshots"]),
                    "likes": sum(int(row.get("likes") or 0) for row in store["video_stats_snapshots"]),
                    "comments": sum(int(row.get("comments") or 0) for row in store["video_stats_snapshots"]),
                    "messages": sum(int(row.get("messages") or 0) for row in store["video_stats_snapshots"]),
                }
            )
        table = table_from_url(url)
        payload = dict(json or {})
        counters[table] += 1
        payload.setdefault("id", counters[table])
        store[table].append(payload)
        return FakeResponse([payload])

    def fake_patch(url: str, headers: dict[str, str], params: dict[str, str], json: dict[str, object], timeout: int) -> FakeResponse:
        del headers, timeout
        rows = [row for row in store[table_from_url(url)] if matches(row, params)]
        if rows:
            rows[0].update(json)
            return FakeResponse([rows[0]])
        return FakeResponse([])

    def fake_delete(url: str, headers: dict[str, str], params: dict[str, str], timeout: int) -> FakeResponse:
        del headers, timeout
        table = table_from_url(url)
        before = len(store[table])
        store[table] = [row for row in store[table] if not matches(row, params)]
        return FakeResponse({"deleted": before - len(store[table])}, 200)

    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.get", fake_get)
    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.post", fake_post)
    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.patch", fake_patch)
    monkeypatch.setattr("gasgx_distribution.supabase_backend.requests.delete", fake_delete)

    runtime = {"supabase_url": "https://brand.example.supabase.co", "service_key_ref": "env:BRAND_SUPABASE_SERVICE_KEY"}
    with service.use_brand_runtime(runtime):
        account = service.create_account({"account_key": "remote-01", "display_name": "Remote 01", "platforms": ["wechat"]})
        assert account["platforms"][0]["platform"] == "wechat"

        task = service.create_task({"account_id": account["id"], "platform": "wechat", "task_type": "publish"})
        assert task["status"] == "pending"
        try:
            service.create_task({"account_id": account["id"], "platform": "wechat", "task_type": "publish"})
        except ValueError as exc:
            assert "duplicate active task" in str(exc)
        else:
            raise AssertionError("duplicate active task was accepted")

        imported = service.import_stats({"account_id": account["id"], "platform": "wechat", "video_ref": "v1", "views": 7, "comments": 2})
        assert imported["inserted"] == 1
        summary = service.dashboard_summary()
        assert summary["accounts"] == 1
        assert summary["running_tasks"] == 1
        assert summary["views"] == 7
        assert rpc_calls == ["dashboard_summary"]
        assert service.delete_task(int(task["id"])) is True


def test_ai_robot_config_webhook_and_test_message(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    sent: list[dict[str, object]] = []

    class FakeResponse:
        status_code = 200
        text = '{"ok": true}'

        def json(self) -> dict[str, object]:
            return {"ok": True}

    def fake_post(url: str, json: dict[str, object], headers: dict[str, str], timeout: float) -> FakeResponse:
        sent.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("gasgx_distribution.service.requests.post", fake_post)

    saved = client.put(
        "/api/ai-robots/telegram/config",
        json={
            "enabled": True,
            "bot_name": "GasGx Bot",
            "webhook_url": "https://example.test/bot",
            "webhook_secret": "send-secret",
            "signing_secret": "sign-secret",
            "target_id": "chat-1",
        },
    )
    assert saved.status_code == 200
    config = saved.json()
    assert config["enabled"] is True
    assert config["has_webhook_secret"] is True
    assert config["has_signing_secret"] is True
    assert "sign-secret" not in json.dumps(config)

    resaved = client.put(
        "/api/ai-robots/telegram/config",
        json={
            "enabled": True,
            "bot_name": "GasGx Bot Updated",
            "webhook_url": "https://example.test/bot",
            "target_id": "chat-1",
        },
    )
    assert resaved.status_code == 200
    assert resaved.json()["has_signing_secret"] is True

    test_message = client.post(
        "/api/ai-robots/telegram/test-message",
        json={"message_type": "text", "text": "hello"},
    )
    assert test_message.status_code == 200
    assert test_message.json()["status"] == "sent"
    assert sent[-1]["json"] == {"text": "hello", "chat_id": "chat-1"}

    wecom_saved = client.put(
        "/api/ai-robots/wecom/config",
        json={
            "enabled": True,
            "bot_name": "企业微信机器人",
            "webhook_url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc",
        },
    )
    assert wecom_saved.status_code == 200
    wecom_config = wecom_saved.json()
    assert wecom_config["enabled"] is True
    assert wecom_config["webhook_url"].startswith("https://qyapi.weixin.qq.com/")
    assert wecom_config["target_id"] == ""
    wecom_message = client.post(
        "/api/ai-robots/wecom/test-message",
        json={"message_type": "text", "text": "wecom hello"},
    )
    assert wecom_message.status_code == 200
    assert wecom_message.json()["status"] == "sent"
    assert sent[-1]["json"] == {"msgtype": "text", "text": {"content": "wecom hello"}}

    disabled = client.put(
        "/api/ai-robots/wecom/config",
        json={
            "enabled": False,
            "bot_name": "企业微信机器人",
            "webhook_url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc",
        },
    )
    assert disabled.status_code == 200
    assert disabled.json()["enabled"] is False
    disabled_message = client.post(
        "/api/ai-robots/wecom/test-message",
        json={"message_type": "text", "text": "disabled"},
    )
    assert disabled_message.status_code == 200
    assert disabled_message.json()["status"] == "unsupported"
    assert "not enabled" in disabled_message.json()["summary"]

    for platform, url in [
        ("dingtalk", "https://oapi.dingtalk.com/robot/send?access_token=abc"),
        ("lark", "https://open.feishu.cn/open-apis/bot/v2/hook/abc"),
    ]:
        saved_webhook_only = client.put(
            f"/api/ai-robots/{platform}/config",
            json={
                "enabled": True,
                "bot_name": f"{platform} webhook bot",
                "webhook_url": url,
            },
        )
        assert saved_webhook_only.status_code == 200
        assert saved_webhook_only.json()["enabled"] is True
        assert saved_webhook_only.json()["target_id"] == ""
        sent_before = len(sent)
        webhook_only_test = client.post(
            f"/api/ai-robots/{platform}/test-message",
            json={"message_type": "text", "text": f"{platform} hello"},
        )
        assert webhook_only_test.status_code == 200
        assert webhook_only_test.json()["status"] == "sent"
        assert len(sent) == sent_before + 1
        if platform == "dingtalk":
            assert sent[-1]["json"] == {"msgtype": "text", "text": {"content": "dingtalk hello"}}
        else:
            assert sent[-1]["json"] == {"msg_type": "text", "content": {"text": "lark hello"}}

    signed_dingtalk = client.put(
        "/api/ai-robots/dingtalk/config",
        json={
            "enabled": True,
            "bot_name": "signed dingtalk bot",
            "webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=abc",
            "webhook_secret": "SECabc",
        },
    )
    assert signed_dingtalk.status_code == 200
    signed_dingtalk_message = client.post(
        "/api/ai-robots/dingtalk/test-message",
        json={"message_type": "text", "text": "signed dingtalk"},
    )
    assert signed_dingtalk_message.status_code == 200
    query = parse_qs(urlsplit(str(sent[-1]["url"])).query)
    assert query["access_token"] == ["abc"]
    assert query["timestamp"]
    assert query["sign"]

    signed_lark = client.put(
        "/api/ai-robots/lark/config",
        json={
            "enabled": True,
            "bot_name": "signed lark bot",
            "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/abc",
            "webhook_secret": "SECabc",
        },
    )
    assert signed_lark.status_code == 200
    signed_lark_message = client.post(
        "/api/ai-robots/lark/test-message",
        json={"message_type": "text", "text": "signed lark"},
    )
    assert signed_lark_message.status_code == 200
    assert sent[-1]["json"]["msg_type"] == "text"
    assert sent[-1]["json"]["content"] == {"text": "signed lark"}
    assert sent[-1]["json"]["timestamp"]
    assert sent[-1]["json"]["sign"]

    lark_challenge = client.post(
        "/api/ai-robots/lark/webhook",
        json={"type": "url_verification", "challenge": "challenge-token"},
    )
    assert lark_challenge.status_code == 200
    assert lark_challenge.json() == {"challenge": "challenge-token"}

    body = b'{"text":"from platform"}'
    signature = hmac.new(b"sign-secret", body, hashlib.sha256).hexdigest()
    webhook = client.post(
        "/api/ai-robots/telegram/webhook",
        content=body,
        headers={"x-gasgx-signature": signature, "content-type": "application/json"},
    )
    assert webhook.status_code == 200
    assert webhook.json()["ok"] is True

    rejected = client.post(
        "/api/ai-robots/telegram/webhook",
        content=body,
        headers={"x-gasgx-signature": "bad"},
    )
    assert rejected.status_code == 401

    deleted = client.delete("/api/ai-robots/telegram/config")
    assert deleted.status_code == 200
    telegram = next(item for item in client.get("/api/ai-robots/configs").json() if item["platform"] == "telegram")
    assert telegram["enabled"] is False
    assert telegram["webhook_url"] == ""


def test_ai_robot_sender_worker_sends_and_records_failures(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    sent: list[dict[str, object]] = []

    class FakeResponse:
        status_code = 200
        text = '{"ok": true}'

        def json(self) -> dict[str, object]:
            return {"ok": True}

    def fake_post(url: str, json: dict[str, object], headers: dict[str, str], timeout: float) -> FakeResponse:
        sent.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("gasgx_distribution.service.requests.post", fake_post)

    config = client.put(
        "/api/ai-robots/telegram/config",
        json={
            "enabled": True,
            "bot_name": "GasGx Bot",
            "webhook_url": "https://api.telegram.org/botTOKEN/sendMessage",
            "webhook_secret": "",
            "signing_secret": "sign-secret",
            "target_id": "chat-1",
        },
    )
    assert config.status_code == 200
    message = client.post("/api/ai-robots/telegram/messages", json={"message_type": "text", "text": "hello"})
    assert message.status_code == 200

    worker = client.post("/api/ai-robots/messages/send-worker?limit=1")
    assert worker.status_code == 200
    assert worker.json()["sent"] == 1
    assert sent[0]["json"] == {"text": "hello", "chat_id": "chat-1"}
    messages = client.get("/api/ai-robots/messages").json()
    assert messages[0]["status"] == "sent"
    assert messages[0]["retry_count"] == 0
    assert messages[0]["sent_at"]

    def fake_fail(url: str, json: dict[str, object], headers: dict[str, str], timeout: float) -> FakeResponse:
        del url, json, headers, timeout
        raise RuntimeError("network down")

    monkeypatch.setattr("gasgx_distribution.service.requests.post", fake_fail)
    failed_message = client.post("/api/ai-robots/telegram/messages", json={"message_type": "text", "text": "retry me"})
    assert failed_message.status_code == 200
    failed = client.post("/api/ai-robots/messages/send-worker?limit=1")
    assert failed.status_code == 200
    assert failed.json()["failed"] == 1
    messages = client.get("/api/ai-robots/messages").json()
    retry = next(item for item in messages if item["id"] == failed_message.json()["id"])
    assert retry["status"] == "retry"
    assert retry["retry_count"] == 1
    assert "network down" in retry["error"]


def test_telegram_resolve_uses_local_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    calls: list[str] = []

    def fake_call_telegram_api(*, bot_token: str, method: str, timeout_seconds: int, use_post: bool, **_: object) -> dict[str, object]:
        assert bot_token == "SECRET_TOKEN"
        assert timeout_seconds >= 5
        assert use_post is False
        calls.append(method)
        if method == "getMe":
            return {"ok": True, "result": {"username": "GasGxBot"}}
        if method == "getUpdates":
            return {"ok": True, "result": [{"message": {"chat": {"id": -10042}}}]}
        raise AssertionError(method)

    monkeypatch.setattr("gasgx_distribution.service.call_telegram_api", fake_call_telegram_api)
    client = TestClient(create_app())

    response = client.post("/api/ai-robots/telegram/resolve", json={"token": "SECRET_TOKEN"})

    assert response.status_code == 200
    data = response.json()
    assert data["username"] == "GasGxBot"
    assert data["chat_id"] == "-10042"
    assert data["webhook_url"] == "https://api.telegram.org/botSECRET_TOKEN/sendMessage"
    assert calls == ["getMe", "getUpdates"]


def test_dashboard_summary_counts_remaining_material_videos(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    material_dir = tmp_path / "runtime" / "materials" / "videos"
    material_dir.mkdir(parents=True)
    used = material_dir / "used.mp4"
    remaining = material_dir / "remaining.mp4"
    ignored = material_dir / "ignored.txt"
    used.write_bytes(b"used")
    remaining.write_bytes(b"remaining")
    ignored.write_text("ignored", encoding="utf-8")
    material_ts = datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc).timestamp()
    os.utime(used, (material_ts, material_ts))
    os.utime(remaining, (material_ts, material_ts))
    monkeypatch.setattr(matrix_publish, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(matrix_publish, "_today_date", lambda _tz: date(2026, 5, 18))
    monkeypatch.setattr(matrix_publish, "get_paths", lambda: type("P", (), {"runtime_root": tmp_path / "runtime"})())
    monkeypatch.setattr(matrix_publish, "resolve_material_dir", lambda material_dir_override=None: tmp_path / "runtime" / "materials" / "videos")
    consumed_today = matrix_publish._relative_asset_key(used)
    (tmp_path / "runtime" / "matrix_publish_state.json").write_text(
        json.dumps(
            {
                "consumed": [
                    {"asset_key": consumed_today, "account_id": 1, "platform": "wechat", "publish_date": "2026-05-18"},
                ],
                "runs": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = service.dashboard_summary()

    assert summary["remaining_material_videos"] == 1


def test_terminal_summary_keeps_today_remaining_zero_when_only_history_materials_exist(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    material_dir = tmp_path / "runtime" / "materials" / "videos" / "20260520_batch"
    material_dir.mkdir(parents=True)
    history_video = material_dir / "history.mp4"
    history_video.write_bytes(b"history")
    history_ts = datetime(2026, 5, 20, 9, 0, tzinfo=timezone.utc).timestamp()
    os.utime(history_video, (history_ts, history_ts))

    monkeypatch.setattr(matrix_publish, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(matrix_publish, "_today_date", lambda _tz: date(2026, 5, 21))
    monkeypatch.setattr(matrix_publish, "get_paths", lambda: type("P", (), {"runtime_root": tmp_path / "runtime"})())
    monkeypatch.setattr(matrix_publish, "resolve_material_dir", lambda material_dir_override=None: tmp_path / "runtime" / "materials" / "videos")
    (tmp_path / "runtime" / "matrix_publish_state.json").write_text(
        json.dumps({"consumed": [], "runs": []}, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = service._terminal_summary([], [])

    assert summary["today_materials"] == 0
    assert summary["remaining_material_videos"] == 0


def test_terminal_summary_counts_today_remaining_materials(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    material_dir = tmp_path / "runtime" / "materials" / "videos" / "20260518_batch"
    material_dir.mkdir(parents=True)
    first = material_dir / "alpha.mp4"
    second = material_dir / "beta.mp4"
    third = material_dir / "gamma.mp4"
    first.write_bytes(b"alpha")
    second.write_bytes(b"beta")
    third.write_bytes(b"gamma")
    os.utime(first, (1000, 1000))
    os.utime(second, (2000, 2000))
    os.utime(third, (3000, 3000))

    monkeypatch.setattr(matrix_publish, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(matrix_publish, "_today_date", lambda _tz: date(2026, 5, 18))
    monkeypatch.setattr(matrix_publish, "get_paths", lambda: type("P", (), {"runtime_root": tmp_path / "runtime"})())
    monkeypatch.setattr(matrix_publish, "resolve_material_dir", lambda material_dir_override=None: tmp_path / "runtime" / "materials" / "videos")
    consumed_today = matrix_publish._relative_asset_key(first)
    consumed_yesterday = matrix_publish._relative_asset_key(second)
    (tmp_path / "runtime" / "matrix_publish_state.json").write_text(
        json.dumps(
            {
                "consumed": [
                    {"asset_key": consumed_today, "account_id": 1, "platform": "wechat", "publish_date": "2026-05-18"},
                    {"asset_key": consumed_yesterday, "account_id": 2, "platform": "wechat", "publish_date": "2026-05-17"},
                ],
                "runs": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = service._terminal_summary([], [])

    assert summary["today_materials"] == 3
    assert summary["remaining_material_videos"] == 2


def test_open_browser_uses_account_specific_profile(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-x-01", "display_name": "GasGx X 01", "platforms": ["x"]})
    calls: list[dict[str, object]] = []
    marker_calls: list[tuple[dict[str, Any], dict[str, Any], Any]] = []
    monkeypatch.setattr("gasgx_distribution.service.engine._ensure_chrome_debug_port", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        service,
        "_inject_account_browser_marker",
        lambda account, platform_profile, capability: marker_calls.append((account, platform_profile, capability)) or True,
    )

    result = service.open_account_browser(int(account["id"]), "x")

    assert result["ok"] is True
    assert result["marker_applied"] is True
    assert calls
    assert calls[0]["auto_open_chrome"] is True
    assert "gasgx-x-01" in str(calls[0]["chrome_user_data_dir"])
    assert marker_calls
    assert marker_calls[0][0]["display_name"] == "GasGx X 01"


def test_account_browser_marker_uses_cdp_current_and_future_documents(monkeypatch) -> None:
    sent: list[dict[str, Any]] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, str]]:
            return [
                {
                    "type": "page",
                    "url": "https://channels.weixin.qq.com/platform/post/create",
                    "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/1",
                }
            ]

    class FakeSocket:
        def __init__(self) -> None:
            self.last_id = 0

        def send(self, payload: str) -> None:
            message = json.loads(payload)
            self.last_id = int(message["id"])
            sent.append(message)

        def recv(self) -> str:
            return json.dumps({"id": self.last_id, "result": {}})

        def close(self) -> None:
            return None

    class FakeCapability:
        key = "wechat"
        label = "视频号"
        open_url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(service.requests, "get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(service, "_create_chrome_cdp_connection", lambda *_args, **_kwargs: FakeSocket())

    ok = service._inject_account_browser_marker(
        {"id": 7, "account_key": "gasgx-test", "display_name": "GasGx test", "notes": "运营微信: aamecc"},
        {"debug_port": 12007, "handle": "wechat-handle"},
        FakeCapability(),
        terminal_window_id=3,
    )

    assert ok is True
    methods = [item["method"] for item in sent]
    assert "Page.addScriptToEvaluateOnNewDocument" in methods
    assert "Runtime.evaluate" in methods
    source = next(item["params"]["source"] for item in sent if item["method"] == "Page.addScriptToEvaluateOnNewDocument")
    assert "GasGx test" in source
    assert '"operator_wechat": "aamecc"' in source
    assert "document.title" in source
    assert "disconnect" in source
    assert "#gasgx-account-marker" in source
    assert "createElement('div')" not in source
    assert "请确认这是当前要操作的账号" not in source
    assert "GasGx-" in source


def test_account_browser_marker_injects_late_opened_popup_target(monkeypatch) -> None:
    target_calls = {"count": 0}
    sent_targets: list[str] = []

    class FakeSocket:
        def __init__(self, ws_url: str) -> None:
            self.ws_url = ws_url
            self.last_id = 0

        def send(self, payload: str) -> None:
            message = json.loads(payload)
            self.last_id = int(message["id"])
            sent_targets.append(self.ws_url)

        def recv(self) -> str:
            return json.dumps({"id": self.last_id, "result": {}})

        def close(self) -> None:
            return None

    class FakeCapability:
        key = "wechat"
        label = "视频号"
        open_url = "https://channels.weixin.qq.com/login.html"

    def fake_targets(_debug_port: int) -> list[dict[str, str]]:
        target_calls["count"] += 1
        if target_calls["count"] == 1:
            return [{"type": "page", "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/a"}]
        if target_calls["count"] == 2:
            return [
                {"type": "page", "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/a"},
                {"type": "page", "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/b"},
            ]
        return [{"type": "page", "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/a"}]

    now = {"value": 0.0}

    def fake_monotonic() -> float:
        current = now["value"]
        now["value"] = current + 0.25
        return current

    monkeypatch.setattr(service, "_chrome_cdp_page_targets", fake_targets)
    monkeypatch.setattr(service, "_create_chrome_cdp_connection", lambda ws_url, _debug_port: FakeSocket(ws_url))
    monkeypatch.setattr(service.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(service.time, "sleep", lambda *_args, **_kwargs: None)

    ok = service._inject_account_browser_marker(
        {"id": 8, "account_key": "gasgx-popup", "display_name": "GasGx popup"},
        {"debug_port": 12008, "handle": "wechat-popup"},
        FakeCapability(),
    )

    assert ok is True
    assert "ws://127.0.0.1/devtools/page/a" in sent_targets
    assert "ws://127.0.0.1/devtools/page/b" in sent_targets


def test_find_windows_chrome_pid_by_debug_port_prefers_browser_process(monkeypatch) -> None:
    class FakeResult:
        stdout = json.dumps(
            [
                {
                    "ProcessId": 321,
                    "CommandLine": "\"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe\" --type=renderer --remote-debugging-port=21288",
                },
                {
                    "ProcessId": 654,
                    "CommandLine": "\"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe\" --remote-debugging-port=21288 --user-data-dir=G:\\Profiles\\A",
                },
            ],
            ensure_ascii=False,
        )

    monkeypatch.setattr(service.subprocess, "run", lambda *_args, **_kwargs: FakeResult())

    pid = service._find_windows_chrome_pid_by_debug_port(21288)

    assert pid == 654
    assert service._find_windows_chrome_pid_by_debug_port(21288, r"G:\Profiles\A") == 654
    assert service._find_windows_chrome_pid_by_debug_port(21288, r"G:\Profiles\B") == 0


def test_supabase_system_initialize_is_idempotent(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")

    class FakeClient:
        def __init__(self) -> None:
            self.tables: dict[str, list[dict[str, object]]] = {"app_settings": [], "analytics_items": [], "app_seed_runs": []}

        def select_one(self, table: str, *, filters: dict[str, object]) -> dict[str, object] | None:
            for row in self.tables.setdefault(table, []):
                if all(row.get(key) == value for key, value in filters.items()):
                    return row
            return None

        def insert(self, table: str, payload: dict[str, object]) -> dict[str, object]:
            row = dict(payload)
            row.setdefault("id", len(self.tables.setdefault(table, [])) + 1)
            self.tables[table].append(row)
            return row

        def upsert(self, table: str, payload: dict[str, object], *, on_conflict: str) -> dict[str, object]:
            existing = self.select_one(table, filters={on_conflict: payload[on_conflict]})
            if existing is not None:
                existing.update(payload)
                return existing
            return self.insert(table, payload)

    fake = FakeClient()
    monkeypatch.setattr(service, "_brand_supabase", lambda: fake)

    first = service.initialize_system()
    second = service.initialize_system()

    assert first["ok"] is True
    assert first["inserted"]["distribution_settings"] == 1
    assert first["inserted"]["video_matrix_state"] == 1
    assert first["inserted"]["analytics_items"] > 1
    assert second["skipped"]["distribution_settings"] == 1
    assert second["skipped"]["video_matrix_state"] == 1
    assert len(fake.tables["app_settings"]) == 2
    assert len(fake.tables["app_seed_runs"]) == 1


def test_supabase_distribution_settings_roundtrip(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")
    store: dict[str, dict[str, object]] = {}

    class FakeClient:
        def select_one(self, table: str, *, filters: dict[str, object]) -> dict[str, object] | None:
            return store.get(str(filters["setting_key"]))

        def upsert(self, table: str, payload: dict[str, object], *, on_conflict: str) -> dict[str, object]:
            store[str(payload["setting_key"])] = dict(payload)
            return store[str(payload["setting_key"])]

    monkeypatch.setattr(service, "_brand_supabase", lambda: FakeClient())

    saved = service.save_distribution_settings_db({"common": {"material_dir": "runtime/a", "publish_mode": "draft", "upload_timeout": 120}})
    loaded = service.load_distribution_settings_db()

    assert saved["common"]["publish_mode"] == "draft"
    assert loaded["common"]["material_dir"] == "runtime/a"


def test_clear_supabase_read_cache_endpoint_sqlite(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    response = client.post("/api/system/supabase-read-cache/clear")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["cleared"] is False
    assert body["backend"] == "sqlite"


def test_clear_supabase_read_cache_supabase_backend(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")
    service._supabase_read_cache_set("dashboard_summary", {"accounts": 3})
    service._SUPABASE_APP_SETTINGS_CACHE["distribution_settings"] = {"common": {}}
    cleared = service.clear_supabase_read_cache()
    assert cleared["cleared"] is True
    assert not service._supabase_read_cache_peek("dashboard_summary")
    assert "distribution_settings" not in service._SUPABASE_APP_SETTINGS_CACHE


def test_supabase_read_cache_covers_common_list_endpoints(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")

    class FakeClient:
        def __init__(self) -> None:
            self.calls: dict[str, int] = {"select": 0, "select_one": 0}

        def select(self, table: str, *, order: str | None = None, filters: dict[str, object] | None = None):
            self.calls["select"] += 1
            if table == "matrix_accounts":
                return [{"id": 1, "account_key": "a1", "display_name": "A1"}]
            if table == "account_platforms":
                return [{"id": 11, "account_id": 1, "platform": "wechat", "enabled": 1}]
            if table == "browser_profiles":
                return [{"account_id": 1, "profile_dir": "profiles/a1", "debug_port": 9333, "fingerprint_json": {"provider": "builtin-light"}}]
            if table == "ai_robot_configs":
                return [{"platform": "wecom", "enabled": 1, "bot_name": "Bot", "webhook_url": "https://example.test", "webhook_secret": "secret", "signing_secret": "", "target_id": "tid", "created_at": 1, "updated_at": 1}]
            if table == "notification_routes":
                return [{"event_type": "action_required", "platform": "wecom", "enabled": 1}]
            if table == "analytics_items":
                return [{"section": "overview", "item_key": "new_accounts", "payload_json": {"label": "x"}, "sort_order": 1}]
            raise AssertionError(f"unexpected select table: {table}")

        def select_one(self, table: str, *, filters: dict[str, object]):
            self.calls["select_one"] += 1
            if table == "brand_settings":
                return {
                    "id": 1,
                    "name": "GasGx",
                    "slogan": "Video Distribution",
                    "logo_asset_path": "",
                    "primary_color": "#5dd62c",
                    "theme_id": "gasgx-green",
                    "default_account_prefix": "GasGx",
                }
            if table == "app_settings":
                return {"setting_key": str(filters["setting_key"]), "payload_json": {"common": {"material_dir": "runtime/a"}}}
            return None

    fake = FakeClient()
    monkeypatch.setattr(service, "_brand_supabase", lambda: fake)

    assert service.load_brand_settings()["id"] == 1
    assert service.load_brand_settings()["id"] == 1
    assert service.load_distribution_settings_db()["common"]["material_dir"] == "runtime/a"
    assert service.load_distribution_settings_db()["common"]["material_dir"] == "runtime/a"
    assert len(service.list_accounts()) == 1
    assert len(service.list_accounts()) == 1
    assert len(service.list_ai_robot_configs()) == 5
    assert len(service.list_ai_robot_configs()) == 5
    assert len(service.list_notification_routes()) >= 1
    assert len(service.list_notification_routes()) >= 1
    assert service.list_analytics_items()["overview"][0]["key"] == "new_accounts"
    assert service.list_analytics_items()["overview"][0]["key"] == "new_accounts"

    assert fake.calls["select_one"] == 2
    assert fake.calls["select"] == 6

    service.clear_supabase_read_cache()
    assert service.load_brand_settings()["id"] == 1
    assert fake.calls["select_one"] == 3


def test_terminal_execution_state_exposes_platform_breakdown(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    payload = client.get("/api/terminal-execution/state")

    assert payload.status_code == 200
    data = payload.json()
    assert data["active_platform"] == "wechat"
    assert data["platform_capabilities"]["wechat"]["sessionPolicy"] == "daily_qr"
    assert data["platform_capabilities"]["douyin"]["sessionPolicy"] == "persistent"
    assert data["profile_by_platform"]["wechat"]["browserRuntime"] == "terminal-execution"


def test_terminal_account_names_fall_back_when_legacy_data_contains_question_marks(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-legacy-01", "display_name": "GasGx Legacy 01", "platforms": ["wechat"]})

    with connect() as conn:
        conn.execute(
            "UPDATE matrix_accounts SET display_name = ?, niche = ? WHERE id = ?",
            ("GasGx ????", "?????", account["id"]),
        )
        conn.commit()

    service._invalidate_terminal_execution_state_cache()

    accounts = service.list_accounts()
    updated = next(item for item in accounts if int(item["id"]) == int(account["id"]))
    assert updated["display_name"] == "gasgx-legacy-01"
    assert updated["niche"] == ""

    runtime_dir = tmp_path / "runtime"
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "current_index": 0,
                        "accounts": [
                            {
                                "id": int(account["id"]),
                                "account_key": "gasgx-legacy-01",
                                "display_name": "GasGx ????",
                                "status": "waiting_qr",
                                "status_text": "等待扫码",
                                "task_id": None,
                            }
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    service._invalidate_terminal_execution_state_cache()
    terminal_state = service.terminal_execution_state()
    terminal_account = terminal_state["windows"][0]["accounts"][0]
    assert terminal_account["display_name"] == "gasgx-legacy-01"


def test_terminal_execution_state_normalizes_legacy_qr_data_url(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "aamecc",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "qr_data_url": "data:image/png;base64,QUJD",
                        "accounts": [
                            {"id": 9, "display_name": "GasGx test07", "account_key": "gasgx-gasgx-test07-2510", "status": "waiting_qr", "status_text": "????...", "task_id": None, "publish_success_count": 0}
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(service, "now_ts", lambda: 1000)
    service._invalidate_terminal_execution_state_cache()
    client = TestClient(create_app())
    payload = client.get("/api/terminal-execution/state")

    assert payload.status_code == 200
    data = payload.json()
    assert "qr_data_url" not in data["windows"][0]
    assert data["windows"][0]["qr_url"] == "/api/terminal-execution/windows/1/qr-image"
    assert data["windows"][0]["qr_expires_at"] == 1060
    assert (runtime_dir / "terminal_qr_cache" / "window-01.png").read_bytes() == b"ABC"


def test_terminal_execution_state_recovers_existing_qr_cache(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    cache_dir = runtime_dir / "terminal_qr_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "window-01.png").write_bytes(b"cached-qr")
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "aamecc",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 9, "display_name": "GasGx test07", "status": "waiting_qr", "status_text": "等待扫码中..."}
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(service, "now_ts", lambda: 1000)
    service._invalidate_terminal_execution_state_cache()
    client = TestClient(create_app())
    payload = client.get("/api/terminal-execution/state")

    assert payload.status_code == 200
    data = payload.json()
    assert data["windows"][0]["qr_url"] == "/api/terminal-execution/windows/1/qr-image"
    assert data["windows"][0]["qr_path"].endswith("window-01.png")
    assert data["windows"][0]["qr_expires_at"] == 1060


def test_terminal_execution_state_normalizes_legacy_palette_colors(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 3,
                        "enabled": True,
                        "operator_wechat": "op-a",
                        "color": "#A855F7",
                        "color_name": "神秘紫",
                        "current_index": 0,
                        "accounts": [],
                    },
                    {
                        "id": 4,
                        "enabled": True,
                        "operator_wechat": "op-b",
                        "color": "#EC4899",
                        "color_name": "醒目粉",
                        "current_index": 0,
                        "accounts": [],
                    },
                ],
                "config": [
                    {"id": 3, "enabled": True, "operator_wechat": "op-a", "color": "#A855F7"},
                    {"id": 4, "enabled": True, "operator_wechat": "op-b", "color": "#EC4899"},
                ],
                "initialized": True,
                "login_started": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    service._invalidate_terminal_execution_state_cache()
    payload = service.terminal_execution_state()

    assert payload["colors"][:5] == [
        {"hex": "#EF4444", "name": "标准红"},
        {"hex": "#EAB308", "name": "明亮黄"},
        {"hex": "#22C55E", "name": "安全绿"},
        {"hex": "#3B82F6", "name": "科技蓝"},
        {"hex": "#F97316", "name": "活力橙"},
    ]
    assert payload["windows"][0]["color"] == "#22C55E"
    assert payload["windows"][0]["color_name"] == "安全绿"
    assert payload["windows"][1]["color"] == "#EF4444"
    assert payload["windows"][1]["color_name"] == "标准红"
    assert payload["config"][0]["color"] == "#22C55E"
    assert payload["config"][1]["color"] == "#EF4444"

    persisted = json.loads((runtime_dir / "terminal_execution_state.json").read_text(encoding="utf-8"))
    assert persisted["windows"][0]["color"] == "#22C55E"
    assert persisted["windows"][1]["color"] == "#EF4444"
    assert persisted["config"][0]["color"] == "#22C55E"
    assert persisted["config"][1]["color"] == "#EF4444"


def test_terminal_execution_state_resets_windows_after_business_date_rollover(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    cache_dir = runtime_dir / "terminal_qr_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "window-01.png").write_bytes(b"old-qr")
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op-a",
                        "color": "#3B82F6",
                        "current_index": 2,
                        "completed": True,
                        "manual_available_at": 1234,
                        "qr_path": str(cache_dir / "window-01.png"),
                        "qr_url": "/api/terminal-execution/windows/1/qr-image",
                        "qr_expires_at": 9999,
                        "accounts": [
                            {"id": 10, "display_name": "A", "status": "success", "status_text": "发布成功", "task_id": "done-1"},
                            {"id": 11, "display_name": "B", "status": "success", "status_text": "发布成功", "task_id": "done-2"},
                        ],
                        "publish_run": {"status": "success", "account_id": 11},
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
                "business_date": "2026-05-06",
                "next_probe_at": 9000,
                "probe_cursor": 3,
                "updated_at": 1714972800,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "_terminal_business_date", lambda: "2026-05-07")

    service._invalidate_terminal_execution_state_cache()
    payload = service.terminal_execution_state()

    window = payload["windows"][0]
    assert window["current_index"] == 0
    assert "completed" not in window
    assert window["publish_run"] == {}
    assert window["manual_available_at"] == 0
    assert window["qr_url"] == ""
    assert window["accounts"][0]["status"] == "pending"
    assert window["accounts"][0]["status_text"] == "等待打开登录浏览器"
    assert window["accounts"][0]["task_id"] is None
    assert window["accounts"][1]["status"] == "pending"
    assert window["accounts"][1]["status_text"] == "未登录"
    assert window["accounts"][1]["task_id"] is None

    persisted = json.loads((runtime_dir / "terminal_execution_state.json").read_text(encoding="utf-8"))
    assert persisted["business_date"] == "2026-05-07"
    assert persisted["next_probe_at"] == 0
    assert persisted["probe_cursor"] == 0


def test_terminal_execution_state_keeps_completed_window_with_same_business_date(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op-a",
                        "color": "#3B82F6",
                        "current_index": 2,
                        "completed": True,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "qr_expires_at": 0,
                        "accounts": [
                            {"id": 10, "display_name": "A", "status": "success", "status_text": "发布成功", "task_id": None},
                            {"id": 11, "display_name": "B", "status": "success", "status_text": "发布成功", "task_id": None},
                        ],
                        "publish_run": {},
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
                "business_date": "2026-05-07",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "_terminal_business_date", lambda: "2026-05-07")

    service._invalidate_terminal_execution_state_cache()
    payload = service.terminal_execution_state()

    window = payload["windows"][0]
    assert window["current_index"] == 2
    assert window["completed"] is True

def test_terminal_config_update_preserves_active_qr_for_same_window(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    cache_dir = runtime_dir / "terminal_qr_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    qr_path = cache_dir / "window-01.png"
    qr_path.write_bytes(b"active-qr")
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op-a",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 1234,
                        "qr_path": str(qr_path),
                        "qr_url": "/api/terminal-execution/windows/1/qr-image",
                        "accounts": [{"id": 10, "display_name": "A", "status": "waiting_qr", "status_text": "waiting", "task_id": None}],
                    }
                ],
                "config": [{"id": 1, "enabled": True, "operator_wechat": "op-a", "color": "#3B82F6"}],
                "initialized": True,
                "login_started": True,
                "probe_cursor": 0,
                "next_probe_at": 2000,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service,
        "_terminal_operator_groups",
        lambda: [{"operator_wechat": "op-a", "accounts": [{"id": 10, "account_key": "a", "display_name": "A"}]}],
    )
    monkeypatch.setattr(service, "now_ts", lambda: 1500)

    result = service.start_terminal_execution({"windows": [{"id": 1, "enabled": True, "operator_wechat": "op-a", "color": "#F97316"}]})

    window = result["windows"][0]
    assert result["login_started"] is True
    assert window["color"] == "#F97316"
    assert str(window["qr_url"]).startswith("/api/terminal-execution/windows/1/qr-image")
    assert window["qr_expires_at"] == 1560
    assert window["accounts"][0]["status"] == "waiting_qr"
    assert window["manual_available_at"] == 1234


def test_terminal_emergency_publish_route_does_not_mutate_windows(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    account = service.create_account(
        {
            "account_key": "GasGx Emergency A",
            "display_name": "GasGx Emergency A",
            "platforms": ["wechat"],
            "notes": "运营微信: aamecc",
        }
    )
    service.start_terminal_execution({"windows": [{"id": 1, "enabled": True, "operator_wechat": "aamecc", "color": "#F97316"}]})
    runtime_dir = tmp_path / "runtime"
    material_dir = runtime_dir / "materials" / "videos" / "20260518_batch"
    material_dir.mkdir(parents=True, exist_ok=True)
    sample = material_dir / "sample.mp4"
    sample.write_bytes(b"sample")
    os.utime(sample, (1000, 1000))

    monkeypatch.setattr(
        service,
        "open_account_browser",
        lambda account_id, platform, *, apply_marker=True: (_ for _ in ()).throw(
            RuntimeError("debug port 9222 is already used by another browser profile")
        ),
    )
    monkeypatch.setattr(service, "load_platform_publish_settings", lambda platform: {"publish_mode": "publish", "upload_timeout": 120, "caption": "caption", "topics": "", "collection_name": "", "declare_original": False})
    import gasgx_distribution.matrix_publish as mp
    monkeypatch.setattr(mp, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(mp, "_today_date", lambda _tz: date(2026, 5, 18))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [sample])
    monkeypatch.setattr(mp, "_consumed_index", lambda today=None: set())
    monkeypatch.setattr(mp, "prepare_workspace", lambda item: item.workspace.mkdir(parents=True, exist_ok=True))
    popen_calls: list[dict[str, Any]] = []

    class FakeProcess:
        pid = 4321

    def fake_popen(cmd, **kwargs):
        popen_calls.append({"cmd": list(cmd), "env": dict(kwargs.get("env") or {})})
        return FakeProcess()

    monkeypatch.setattr(service.subprocess, "Popen", fake_popen)
    client = TestClient(create_app())

    response = client.post(f"/api/accounts/{account['id']}/platforms/wechat/emergency-publish")

    assert response.status_code == 200
    payload = response.json()
    window = payload["windows"][0]
    assert window["current_index"] == 0
    assert window.get("publish_run") in ({}, None)
    assert len(payload["emergency_publish_runs"]) == 1
    assert payload["emergency_publish_runs"][0]["account_id"] == account["id"]
    assert popen_calls
    assert "--wechat-publish-now" in popen_calls[0]["cmd"]


def test_terminal_config_update_clears_stale_qr_when_window_operator_changes(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    cache_dir = runtime_dir / "terminal_qr_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    qr_path = cache_dir / "window-01.png"
    qr_path.write_bytes(b"stale-qr")
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op-a",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "qr_path": str(qr_path),
                        "qr_url": "/api/terminal-execution/windows/1/qr-image",
                        "accounts": [{"id": 10, "display_name": "A", "status": "waiting_qr", "status_text": "waiting", "task_id": None}],
                    }
                ],
                "config": [{"id": 1, "enabled": True, "operator_wechat": "op-a", "color": "#3B82F6"}],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service,
        "_terminal_operator_groups",
        lambda: [{"operator_wechat": "op-b", "accounts": [{"id": 11, "account_key": "b", "display_name": "B"}]}],
    )

    result = service.start_terminal_execution({"windows": [{"id": 1, "enabled": True, "operator_wechat": "op-b", "color": "#A855F7"}]})

    window = result["windows"][0]
    assert result["login_started"] is True
    assert window["operator_wechat"] == "op-b"
    assert window["qr_url"] == ""
    assert window["accounts"][0]["id"] == 11
    assert not qr_path.exists()


def test_terminal_config_update_keeps_enabled_windows_even_when_operator_accounts_empty(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(
        service,
        "_terminal_operator_groups",
        lambda: [
            {"operator_wechat": "op-a", "accounts": [{"id": 10, "account_key": "a", "display_name": "A"}]},
            {"operator_wechat": "op-b", "accounts": []},
        ],
    )

    result = service.start_terminal_execution(
        {
            "windows": [
                {"id": 1, "enabled": True, "operator_wechat": "op-a", "color": "#3B82F6"},
                {"id": 2, "enabled": True, "operator_wechat": "op-b", "color": "#F97316"},
            ]
        }
    )

    assert [int(item.get("id") or 0) for item in result["windows"]] == [1, 2]
    assert any(int(item.get("id") or 0) == 2 for item in result["config"])
    second = next(item for item in result["windows"] if int(item.get("id") or 0) == 2)
    assert second["accounts"] == []
    assert any(issue["code"] == "no_current_account" for issue in (second.get("publish_precheck", {}).get("issues", {}).get("p0") or []))


def test_terminal_config_update_keeps_unknown_operator_windows(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(
        service,
        "_terminal_operator_groups",
        lambda: [{"operator_wechat": "op-a", "accounts": []}],
    )

    result = service.start_terminal_execution(
        {
            "windows": [
                {"id": 1, "enabled": True, "operator_wechat": "op-a", "color": "#3B82F6"},
                {"id": 2, "enabled": True, "operator_wechat": "op-b", "color": "#F97316"},
            ]
        }
    )

    assert [int(item.get("id") or 0) for item in result["windows"]] == [1, 2]
    assert [item.get("operator_wechat") for item in result["config"]] == ["op-a", "op-b"]


def test_brand_api_omits_large_inline_logo_asset(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    large_logo = "data:image/png;base64," + ("A" * (service.BRAND_INLINE_ASSET_MAX_CHARS + 1))
    service.save_brand_settings({"name": "GasGx", "logo_asset_path": large_logo})
    client = TestClient(create_app())

    response = client.get("/api/brand")

    assert response.status_code == 200
    settings = response.json()["settings"]
    assert settings["logo_asset_path"] == ""
    assert settings["logo_asset_omitted"] is True
    assert settings["logo_asset_chars"] == len(large_logo)
    assert service.load_brand_settings()["logo_asset_path"] == large_logo


def test_public_brand_settings_supabase_uses_lightweight_columns(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")

    class FakeClient:
        def __init__(self) -> None:
            self.select_calls: list[dict[str, object]] = []
            self.select_where_calls: list[dict[str, object]] = []

        def select(self, table: str, *, filters: dict[str, object] | None = None, order: str = "", columns: str = "*"):
            self.select_calls.append({"table": table, "filters": filters, "columns": columns})
            assert columns == service.BRAND_PUBLIC_SETTING_COLUMNS
            return [{
                "id": 1,
                "name": "GasGx",
                "slogan": "Video Distribution",
                "primary_color": "#5dd62c",
                "theme_id": "gasgx-green",
                "default_account_prefix": "GasGx",
            }]

        def select_where(self, table: str, *, params: dict[str, str], order: str = "", columns: str = "*"):
            self.select_where_calls.append({"table": table, "params": params, "columns": columns})
            if params["logo_asset_path"].startswith("not.like"):
                return []
            return [{"id": 1}]

    fake = FakeClient()
    monkeypatch.setattr(service, "_brand_supabase", lambda: fake)

    settings = service.public_brand_settings()

    assert settings["logo_asset_path"] == ""
    assert settings["logo_asset_omitted"] is True
    assert fake.select_calls[0]["columns"] != "*"
    assert {call["columns"] for call in fake.select_where_calls} == {"logo_asset_path", "id"}


def test_terminal_poll_does_not_probe_login_status(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    windows = [
        {
            "id": index,
            "enabled": True,
            "operator_wechat": f"op{index}",
            "color": "#3B82F6",
            "current_index": 0,
            "manual_available_at": 0,
            "accounts": [{"id": index, "status": "waiting_qr", "status_text": "等待扫码中...", "task_id": None}],
        }
        for index in range(1, 4)
    ]
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps({"windows": windows, "config": [], "initialized": True, "login_started": True}, ensure_ascii=False),
        encoding="utf-8",
    )
    calls: list[int] = []
    monkeypatch.setattr(service, "check_login_status", lambda account_id, platform: calls.append(account_id) or {"status": "waiting"})

    service._invalidate_terminal_execution_state_cache()
    service.poll_terminal_execution()
    service.poll_terminal_execution()

    assert calls == []
    state = json.loads((runtime_dir / "terminal_execution_state.json").read_text(encoding="utf-8"))
    assert int(state.get("probe_cursor") or 0) == 0
    assert int(state.get("next_probe_at") or 0) == 0


def test_terminal_poll_passive_mode_does_not_open_or_probe(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "accounts": [{"id": 9, "status": "pending", "status_text": "waiting", "task_id": None}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service,
        "_terminal_current_needs_browser_open",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("passive poll should not probe browser-open state")),
    )
    monkeypatch.setattr(
        service,
        "_terminal_current_needs_login_probe",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("passive poll should not probe login state")),
    )
    monkeypatch.setattr(
        service,
        "open_account_browser",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("passive poll should not open browsers")),
    )
    monkeypatch.setattr(
        service,
        "check_login_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("passive poll should not check login")),
    )
    monkeypatch.setattr(service, "_terminal_browser_runtime_for_account", lambda *_args, **_kwargs: {})

    service._invalidate_terminal_execution_state_cache()
    result = service.poll_terminal_execution(allow_browser_open=False, allow_login_probe=False)

    assert result["windows"][0]["accounts"][0]["status"] == "pending"
    state = json.loads((runtime_dir / "terminal_execution_state.json").read_text(encoding="utf-8"))
    assert int(state.get("next_probe_at") or 0) == 0


def test_terminal_poll_does_not_reopen_waiting_qr_when_browser_is_closed(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    windows = [
        {
            "id": 1,
            "enabled": True,
            "operator_wechat": "op1",
            "color": "#3B82F6",
            "current_index": 0,
            "manual_available_at": 0,
            "accounts": [{"id": 9, "status": "waiting_qr", "status_text": service.TERMINAL_LOGIN_CONFIRM_TEXT, "task_id": None}],
        }
    ]
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps({"windows": windows, "config": [], "initialized": True, "login_started": True}, ensure_ascii=False),
        encoding="utf-8",
    )
    opened: list[int] = []
    monkeypatch.setattr(service, "_terminal_browser_runtime_for_account", lambda *_args, **_kwargs: {"browser_open": False})
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: opened.append(account_id) or {"ok": True})
    monkeypatch.setattr(service, "check_login_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not probe login")))

    service._invalidate_terminal_execution_state_cache()
    result = service.poll_terminal_execution()

    assert opened == []
    assert result["windows"][0]["accounts"][0]["status"] == "waiting_qr"


def test_terminal_poll_does_not_wait_for_login_probe(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    windows = [
        {
            "id": 1,
            "enabled": True,
            "operator_wechat": "op1",
            "color": "#3B82F6",
            "current_index": 0,
            "manual_available_at": 0,
            "accounts": [{"id": 9, "status": "waiting_qr", "status_text": service.TERMINAL_LOGIN_CONFIRM_TEXT, "task_id": None}],
        }
    ]
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps({"windows": windows, "config": [], "initialized": True, "login_started": True}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "_terminal_browser_runtime_for_account", lambda *_args, **_kwargs: {"browser_open": True})
    monkeypatch.setattr(service, "TERMINAL_LOGIN_PROBE_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(service, "check_login_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not check login")))

    service._invalidate_terminal_execution_state_cache()
    started_at = time.monotonic()
    result = service.poll_terminal_execution()

    assert time.monotonic() - started_at < 0.5
    current = result["windows"][0]["accounts"][0]
    assert current["status"] == "waiting_qr"
    assert current["status_text"] == service.TERMINAL_LOGIN_CONFIRM_TEXT


def test_terminal_start_login_opens_all_windows_for_current_batch(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    windows = [
        {
            "id": index,
            "enabled": True,
            "operator_wechat": f"op{index}",
            "color": "#3B82F6",
            "current_index": 0,
            "manual_available_at": 0,
            "accounts": [{"id": index, "status": "pending", "status_text": "waiting", "task_id": None}],
        }
        for index in range(1, 4)
    ]
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps({"windows": windows, "config": [], "initialized": True, "login_started": False}, ensure_ascii=False),
        encoding="utf-8",
    )
    now = {"value": 100}
    opened: list[int] = []
    monkeypatch.setattr(service, "now_ts", lambda: now["value"])
    monkeypatch.setattr(service, "TERMINAL_LOGIN_PROBE_INTERVAL_SECONDS", 10)
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: opened.append(account_id) or {"ok": True})

    service._invalidate_terminal_execution_state_cache()
    first_state = service.start_terminal_login()

    assert opened == [1, 2, 3]
    assert [item["accounts"][0]["status"] for item in first_state["windows"]] == ["waiting_qr", "waiting_qr", "waiting_qr"]
    assert [item["accounts"][0]["status_text"] for item in first_state["windows"]] == [service.TERMINAL_LOGIN_CONFIRM_TEXT] * 3
    assert [item["qr_url"] for item in first_state["windows"]] == ["", "", ""]
    assert first_state["login_started"] is True
    assert [item["manual_available_at"] for item in first_state["windows"]] == [0, 0, 0]


def test_terminal_confirm_login_ready_marks_account_after_fast_probe(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": str(runtime_dir / "terminal_qr_cache" / "window-01.png"),
                        "qr_url": "/api/terminal-execution/windows/1/qr-image",
                        "accounts": [{"id": 9, "status": "waiting_qr", "status_text": "等待扫码中...", "task_id": None}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    probed: list[tuple[int, str]] = []

    def _probe(account_id: int, platform: str) -> dict[str, object]:
        probed.append((account_id, platform))
        return {"ok": True, "status": "ready"}

    monkeypatch.setattr(service, "check_login_status", _probe)

    service._invalidate_terminal_execution_state_cache()
    result = service.confirm_terminal_login_ready(1)

    account = result["windows"][0]["accounts"][0]
    assert account["status"] == "ready"
    assert account["status_text"] == service.TERMINAL_LOGIN_READY_TEXT
    assert result["windows"][0]["qr_url"] == ""
    assert probed == []


def test_terminal_confirm_login_ready_rejects_when_probe_not_ready(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "accounts": [{"id": 9, "status": "waiting_qr", "status_text": "请扫码", "task_id": None}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "check_login_status", lambda *_args, **_kwargs: {"ok": False, "status": "login_required"})

    service._invalidate_terminal_execution_state_cache()
    result = service.confirm_terminal_login_ready(1)

    account = result["windows"][0]["accounts"][0]
    assert account["status"] == "ready"
    assert account["status_text"] == service.TERMINAL_LOGIN_READY_TEXT


def test_terminal_confirm_login_ready_sets_error_on_probe_timeout(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "accounts": [{"id": 9, "status": "waiting_qr", "status_text": "请扫码", "task_id": None}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "check_login_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("登录检测超时（2s）")))

    service._invalidate_terminal_execution_state_cache()
    result = service.confirm_terminal_login_ready(1)
    account = result["windows"][0]["accounts"][0]
    assert account["status"] == "ready"
    assert account["status_text"] == service.TERMINAL_LOGIN_READY_TEXT


def test_terminal_start_login_rejects_when_no_executable_accounts(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {"id": 1, "enabled": True, "operator_wechat": "op-a", "color": "#3B82F6", "current_index": 0, "accounts": []},
                    {"id": 2, "enabled": True, "operator_wechat": "op-b", "color": "#F97316", "current_index": 0, "accounts": []},
                ],
                "config": [],
                "initialized": True,
                "login_started": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    service._invalidate_terminal_execution_state_cache()
    try:
        service.start_terminal_login()
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "当前配置下没有可登录账号" in str(exc)


def test_terminal_start_login_applies_window_color_to_browser(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#F97316",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "accounts": [{"id": 9, "status": "pending", "status_text": "waiting", "task_id": None}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    open_result = {"ok": True, "debug_port": 12009, "profile_dir": str(tmp_path / "profile")}
    applied: list[tuple[dict[str, Any], str]] = []
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: open_result)
    monkeypatch.setattr(
        service,
        "_apply_account_browser_window_color",
        lambda result, color: applied.append((result, color)) or True,
    )

    service._invalidate_terminal_execution_state_cache()
    service.start_terminal_login()

    assert applied == [(open_result, "#F97316")]


def test_terminal_state_keeps_ready_account_manual_when_browser_closed(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-ready-closed", "display_name": "Ready Closed", "platforms": ["wechat"]})
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {
                                "id": int(account["id"]),
                                "display_name": "Ready Closed",
                                "status": "ready",
                                "status_text": "已登录，等待手动发布",
                                "task_id": None,
                                "publish_success_count": 0,
                            }
                        ],
                        "publish_run": {},
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service.engine, "_find_debug_chrome_process_pid", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service.engine, "_is_chrome_debug_port_ready", lambda *_args, **_kwargs: False)

    service._invalidate_terminal_execution_state_cache()
    result = service.terminal_execution_state()

    window = result["windows"][0]
    current = window["accounts"][0]
    assert current["browser_open"] is False
    assert window["browser_open"] is False
    assert not any(item["code"] == "browser_closed" for item in window["publish_precheck"]["issues"]["p0"])


def test_terminal_execution_state_does_not_duplicate_runtime_probe_before_login(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {
                                "id": 101,
                                "display_name": "A",
                                "status": "waiting_qr",
                                "status_text": "waiting",
                                "task_id": None,
                                "publish_success_count": 0,
                            }
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "_terminal_operator_groups", lambda: [])
    calls = {"count": 0}
    monkeypatch.setattr(
        service,
        "_terminal_browser_runtime_for_account",
        lambda *_args, **_kwargs: calls.__setitem__("count", calls["count"] + 1) or {},
    )

    service._invalidate_terminal_execution_state_cache()
    result = service.terminal_execution_state()

    assert calls["count"] == 1
    assert result["windows"][0]["accounts"][0]["status"] == "pending"


def test_terminal_execution_state_runtime_probe_prefers_debug_port_health(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-port-probe", "display_name": "Probe Port", "platforms": ["wechat"]})
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "accounts": [
                            {
                                "id": int(account["id"]),
                                "display_name": "Probe Port",
                                "status": "ready",
                                "status_text": "已登录",
                                "task_id": None,
                                "publish_success_count": 0,
                            }
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service.engine,
        "_find_debug_chrome_process_pid",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("pid scan should not run in terminal state probe")),
    )
    monkeypatch.setattr(service.engine, "_is_chrome_debug_port_ready", lambda *_args, **_kwargs: True)
    profile_checks: list[tuple[int, str]] = []
    monkeypatch.setattr(
        service,
        "_find_windows_chrome_pid_by_debug_port",
        lambda debug_port, profile_dir: profile_checks.append((int(debug_port), str(profile_dir))) or 1234,
    )

    service._invalidate_terminal_execution_state_cache()
    result = service.terminal_execution_state()

    assert result["windows"][0]["accounts"][0]["browser_open"] is True
    assert profile_checks


def test_terminal_browser_runtime_treats_wrong_profile_port_as_closed(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-wrong-profile", "display_name": "Wrong Profile", "platforms": ["wechat"]})
    checked: list[tuple[int, str]] = []
    monkeypatch.setattr(service.os, "name", "nt", raising=False)
    monkeypatch.setattr(service.engine, "_is_chrome_debug_port_ready", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        service,
        "_find_windows_chrome_pid_by_debug_port",
        lambda debug_port, profile_dir: checked.append((int(debug_port), str(profile_dir))) or 0,
    )

    runtime = service._terminal_browser_runtime_for_account(int(account["id"]), "wechat")

    assert runtime["browser_open"] is False
    assert checked
    assert "gasgx-wrong-profile" in checked[0][1].replace("\\", "/")


def test_terminal_manual_publish_waits_for_human_success_confirmation(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [{"id": 1, "status": "ready", "status_text": service.TERMINAL_LOGIN_READY_TEXT, "task_id": None}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service,
        "_start_terminal_wechat_publish",
        lambda window, current: {
            "run_id": "run-1",
            "status": "running",
            "pid": 1234,
            "account_id": 1,
            "workspace": str(runtime_dir / "terminal_publish_runs" / "run-1"),
            "asset_key": "v1.mp4",
            "publish_date": "2026-05-06",
            "log_path": str(runtime_dir / "terminal_publish_runs" / "run-1" / "terminal_wechat_publish.log"),
        },
    )
    monkeypatch.setattr(
        service,
        "get_account",
        lambda account_id: {
            "id": account_id,
            "account_key": "acc-1",
            "display_name": "acc-1",
            "platforms": [
                {
                    "platform": "wechat",
                    "profile_dir": str(runtime_dir / "profiles" / "wechat"),
                    "debug_port": 9333,
                    "fingerprint": {},
                }
            ],
        },
    )
    monkeypatch.setattr(service, "check_login_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("manual publish should not probe login")))
    import gasgx_distribution.matrix_publish as mp
    sample = runtime_dir / "materials" / "videos" / "one.mp4"
    sample.parent.mkdir(parents=True, exist_ok=True)
    sample.write_bytes(b"video")
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [sample])
    monkeypatch.setattr(mp, "_consumed_index", lambda today=None: set())

    result = service.manual_terminal_publish(1)

    window = result["windows"][0]
    assert window["accounts"][0]["status"] == "running"
    assert window["accounts"][0]["status_text"] == service.TERMINAL_PUBLISH_PREPARE_TEXT
    assert window["qr_url"] == ""


def test_terminal_wechat_publish_command_follows_publish_mode(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    sample = runtime_dir / "materials" / "videos" / "one.mp4"
    sample.parent.mkdir(parents=True, exist_ok=True)
    sample.write_bytes(b"video")
    profile_dir = tmp_path / "profiles" / "wechat"
    profile_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        service,
        "get_account",
        lambda account_id: {
            "id": account_id,
            "account_key": "acc-1",
            "display_name": "acc-1",
            "platforms": [
                {
                    "platform": "wechat",
                    "profile_dir": str(profile_dir),
                    "debug_port": 9333,
                    "fingerprint": {},
                }
            ],
        },
    )
    settings_payload = {
        "publish_mode": "publish",
        "upload_timeout": 120,
        "caption": "caption",
        "topics": "",
        "collection_name": "",
        "declare_original": False,
    }
    monkeypatch.setattr(service, "load_platform_publish_settings", lambda platform: dict(settings_payload))
    import gasgx_distribution.matrix_publish as mp
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [sample])
    monkeypatch.setattr(mp, "_consumed_index", lambda today=None: set())
    monkeypatch.setattr(mp, "prepare_workspace", lambda item: item.workspace.mkdir(parents=True, exist_ok=True))
    popen_calls: list[dict[str, Any]] = []

    class FakeProcess:
        pid = 4321

    def fake_popen(cmd, **kwargs):
        popen_calls.append({"cmd": list(cmd), "env": dict(kwargs.get("env") or {})})
        return FakeProcess()

    monkeypatch.setattr(service.subprocess, "Popen", fake_popen)

    run = service._start_terminal_wechat_publish({"id": 1, "color": "#3B82F6"}, {"id": 1})

    assert run["manual_publish"] is True
    assert popen_calls
    cmd = popen_calls[0]["cmd"]
    assert "--wechat-publish-now" in cmd
    assert "--wechat-save-draft-only" not in cmd
    assert "--wechat-upload-only" not in cmd
    assert "--no-save-draft" not in cmd
    assert "CYBERCAR_KEEP_WORK_TABS" not in popen_calls[0]["env"]

    settings_payload["publish_mode"] = "draft"
    run = service._start_terminal_wechat_publish({"id": 1, "color": "#3B82F6"}, {"id": 1})

    assert run["manual_publish"] is True
    cmd = popen_calls[1]["cmd"]
    assert "--wechat-save-draft-only" in cmd
    assert "--wechat-publish-now" not in cmd
    assert "--wechat-upload-only" not in cmd
    assert "--no-save-draft" not in cmd


def test_terminal_poll_moves_finished_publish_run_to_manual_confirm(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [{"id": 1, "status": "running", "status_text": "?????...", "task_id": None}],
                        "publish_run": {"status": "running", "pid": 555, "workspace": str(runtime_dir / "w1"), "account_id": 1},
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
                "next_probe_at": 999,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "now_ts", lambda: 100)
    monkeypatch.setattr(service, "_pid_is_running", lambda pid: False)
    monkeypatch.setattr(service, "_advance_terminal_window", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not advance terminal windows")))
    import gasgx_distribution.matrix_publish as mp
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not inspect publish evidence")))
    saved_publish_states: list[dict[str, Any]] = []
    monkeypatch.setattr(mp, "_save_state", lambda payload: saved_publish_states.append(payload))
    result = service.poll_terminal_execution()
    window = result["windows"][0]
    assert window["current_index"] == 0
    assert window["publish_run"]["status"] == "manual_confirm"
    assert window["accounts"][0]["status"] == "running"
    assert window["accounts"][0]["status_text"] == service.TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT
    assert saved_publish_states == []


def test_terminal_open_account_qr_sets_error_stage_on_failure(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [{"id": 1, "status": "pending", "status_text": "??????", "task_id": None, "publish_success_count": 0}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: (_ for _ in ()).throw(RuntimeError("profile locked")))

    result = service.open_terminal_account_qr(1, 1)

    window = result["windows"][0]
    account = window["accounts"][0]
    assert account["status"] == "error"
    assert account["error_stage"] == "login_browser"
    assert account["error_title"] == "\u6253\u5f00\u767b\u5f55\u6d4f\u89c8\u5668\u5931\u8d25"
    assert account["status_text"] == "profile locked"
    assert window["qr_url"] == ""

def test_terminal_manual_publish_sets_error_stage_on_missing_material(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [{"id": 1, "status": "ready", "status_text": service.TERMINAL_LOGIN_READY_TEXT, "task_id": None, "publish_success_count": 0}],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        service,
        "get_account",
        lambda account_id: {
            "id": account_id,
            "account_key": "acc-1",
            "display_name": "acc-1",
            "platforms": [
                {
                    "platform": "wechat",
                    "profile_dir": str(runtime_dir / "profiles" / "wechat"),
                    "debug_port": 9333,
                    "fingerprint": {},
                }
            ],
        },
    )
    import gasgx_distribution.matrix_publish as mp
    monkeypatch.setattr(service, "check_login_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("manual publish should not probe login")))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [])

    result = service.manual_terminal_publish(1)

    window = result["windows"][0]
    account = window["accounts"][0]
    assert account["status"] == "error"
    assert account["error_stage"] == "publish_start"
    assert account["error_title"] == "发布启动失败"
    assert "当前账号当天没有可用视频素材" in account["status_text"]


def test_terminal_poll_finished_publish_run_does_not_mark_failure(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [{"id": 1, "status": "running", "status_text": "发布执行中...", "task_id": None, "publish_success_count": 0}],
                        "publish_run": {"status": "running", "pid": 555, "workspace": str(runtime_dir / "w1"), "account_id": 1},
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
                "next_probe_at": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "now_ts", lambda: 100)
    monkeypatch.setattr(service, "_pid_is_running", lambda pid: False)
    monkeypatch.setattr(service, "_advance_terminal_window", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not advance terminal windows")))
    import gasgx_distribution.matrix_publish as mp
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not inspect publish evidence")))

    result = service.poll_terminal_execution()

    window = result["windows"][0]
    account = window["accounts"][0]
    assert window["publish_run"]["status"] == "manual_confirm"
    assert account["status"] == "running"
    assert account["status_text"] == service.TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT
    assert "error_stage" not in account
    assert window["publish_run"].get("error") == ""


def test_terminal_poll_ignores_log_failure_and_waits_for_manual_confirm(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    workspace = runtime_dir / "w1"
    workspace.mkdir(parents=True, exist_ok=True)
    log_path = workspace / "terminal_wechat_publish.log"
    log_path.write_text(
        "[Uploader:wechat] publish was not confirmed, fallback to save draft.\n"
        "[Scheduler:wechat] Publish failed: ... E_PUBLISH_UNCONFIRMED_DRAFT_SAVED ...\n",
        encoding="utf-8",
    )
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [{"id": 1, "status": "running", "status_text": "发布执行中...", "task_id": None, "publish_success_count": 0}],
                        "publish_run": {"status": "running", "pid": 555, "workspace": str(workspace), "account_id": 1, "log_path": str(log_path)},
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
                "next_probe_at": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "now_ts", lambda: 100)
    monkeypatch.setattr(service, "_pid_is_running", lambda pid: False)
    monkeypatch.setattr(service, "_advance_terminal_window", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not advance terminal windows")))
    import gasgx_distribution.matrix_publish as mp
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not inspect publish evidence")))

    result = service.poll_terminal_execution()

    account = result["windows"][0]["accounts"][0]
    run = result["windows"][0]["publish_run"]
    assert run["status"] == "manual_confirm"
    assert account["status"] == "running"
    assert account["status_text"] == service.TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT
    assert str(run.get("error") or "") == ""


def test_terminal_poll_ignores_log_success_and_waits_for_manual_confirm(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    workspace = runtime_dir / "w1"
    workspace.mkdir(parents=True, exist_ok=True)
    log_path = workspace / "terminal_wechat_publish.log"
    log_path.write_text(
        "[Uploader:wechat] Clicked publish button by primary selector.\n"
        "[Success:wechat] 发布已确认。\n",
        encoding="utf-8",
    )
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [{"id": 1, "status": "running", "status_text": "running", "task_id": None, "publish_success_count": 0}],
                        "publish_run": {"status": "running", "pid": 555, "workspace": str(workspace), "account_id": 1, "log_path": str(log_path)},
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
                "next_probe_at": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "now_ts", lambda: 100)
    monkeypatch.setattr(service, "_pid_is_running", lambda pid: False)
    monkeypatch.setattr(service, "_advance_terminal_window", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not advance terminal windows")))
    import gasgx_distribution.matrix_publish as mp
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not inspect publish evidence")))

    result = service.poll_terminal_execution()

    account = result["windows"][0]["accounts"][0]
    run = result["windows"][0]["publish_run"]
    assert run["status"] == "manual_confirm"
    assert "evidence_ok" not in run
    assert "success_inferred_from_log" not in run
    assert account["status"] == "running"
    assert account["status_text"] == service.TERMINAL_MANUAL_PUBLISH_CONFIRM_TEXT


def test_terminal_open_account_qr_switches_current_account(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 1, "status": "running", "status_text": "???????????", "task_id": 11, "publish_success_count": 0},
                            {"id": 2, "status": "pending", "status_text": "??????", "task_id": None, "publish_success_count": 0},
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "now_ts", lambda: 100)
    opened: list[int] = []
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: opened.append(account_id) or {"ok": True})

    result = service.open_terminal_account_qr(1, 2)

    window = result["windows"][0]
    assert opened == [2]
    assert window["current_index"] == 1
    assert window["accounts"][1]["status"] == "waiting_qr"
    assert window["accounts"][1]["status_text"] == service.TERMINAL_LOGIN_CONFIRM_TEXT
    assert window["qr_url"] == ""


def test_terminal_open_account_qr_keeps_current_account_waiting_for_manual_login_confirmation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": str(runtime_dir / "terminal_qr_cache" / "window-01.png"),
                        "qr_url": "/api/terminal-execution/windows/1/qr-image",
                        "accounts": [
                            {"id": 1, "status": "waiting_qr", "status_text": service.TERMINAL_LOGIN_CONFIRM_TEXT, "task_id": None, "publish_success_count": 0}
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "_terminal_browser_runtime_for_account", lambda *_args, **_kwargs: {"browser_open": True})
    monkeypatch.setattr(service, "_check_terminal_login_status_with_timeout", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("open account should not probe login")))
    monkeypatch.setattr(service, "_inject_terminal_account_browser_marker", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_raise_account_browser_window", lambda *_args, **_kwargs: True)

    result = service.open_terminal_account_qr(1, 1)

    window = result["windows"][0]
    account = window["accounts"][0]
    assert account["status"] == "waiting_qr"
    assert account["status_text"] == service.TERMINAL_LOGIN_CONFIRM_TEXT
    assert window["qr_url"] == ""


def test_terminal_qr_cache_uses_current_page_source_first(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        service,
        "get_account",
        lambda account_id: {
            "id": account_id,
            "platforms": [
                {
                    "platform": "wechat",
                    "debug_port": 9334,
                    "profile_dir": str(runtime_dir / "profiles" / "wechat"),
                }
            ],
        },
    )

    connect_calls: list[dict[str, object]] = []

    class FakePage:
        def refresh(self) -> None:
            connect_calls.append({"refresh": True})

    monkeypatch.setattr(service.engine, "_connect_chrome", lambda **kwargs: connect_calls.append(kwargs) or FakePage())
    monkeypatch.setattr(service.engine, "_extract_login_qr_source", lambda page, timeout_seconds=0, platform_name="": "data:image/png;base64,ZmFrZQ==")
    monkeypatch.setattr(service.engine, "_load_qr_image_source", lambda source: ("image/png", b"fast-qr"))
    monkeypatch.setattr(service, "_looks_like_terminal_qr_image", lambda _b: True)

    path = service._write_terminal_qr_cache(1, 2)

    assert path.endswith("window-01.png")
    assert Path(path).read_bytes() == b"fast-qr"
    assert connect_calls and isinstance(connect_calls[0], dict)
    assert connect_calls[0]["auto_open_chrome"] is False


def test_terminal_open_account_qr_opens_browser_without_extracting_qr(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 1, "status": "pending", "status_text": "pending", "task_id": None, "publish_success_count": 0},
                            {"id": 2, "status": "pending", "status_text": "pending", "task_id": None, "publish_success_count": 0},
                        ],
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    opened: list[int] = []
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: opened.append(account_id) or {"ok": True})
    monkeypatch.setattr(service, "_write_terminal_qr_cache", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("qr extraction should not run")))

    result = service.open_terminal_account_qr(1, 2)

    window = result["windows"][0]
    assert opened == [2]
    assert window["current_index"] == 1
    assert window["accounts"][1]["status"] == "waiting_qr"
    assert window["accounts"][1]["status_text"] == service.TERMINAL_LOGIN_CONFIRM_TEXT
    assert window["qr_url"] == ""

def test_terminal_confirm_success_opens_next_account_browser(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": str(runtime_dir / "terminal_qr_cache" / "window-01.png"),
                        "qr_url": "/api/terminal-execution/windows/1/qr-image",
                        "accounts": [
                            {"id": 1, "status": "running", "status_text": "已触发发布，等待人工确认成功", "task_id": 55, "publish_success_count": 0},
                            {"id": 2, "status": "pending", "status_text": "未登录", "task_id": None, "publish_success_count": 0},
                        ],
                        "publish_run": {
                            "status": "success",
                            "account_id": 1,
                            "asset_key": "videos/one.mp4",
                            "publish_date": "2026-05-06",
                        },
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    closed: list[int] = []
    opened: list[int] = []
    monkeypatch.setattr(service, "_close_wechat_browser_for_account", lambda account_id: closed.append(account_id))
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: opened.append(account_id) or {"ok": True})
    result = service.confirm_terminal_publish_success(1)

    window = result["windows"][0]
    assert closed == [1]
    assert opened == [2]
    assert window["current_index"] == 1
    assert window["accounts"][0]["status"] == "success"
    assert window["accounts"][0]["publish_success_count"] == 1
    assert window["accounts"][1]["status"] == "waiting_qr"
    assert window["accounts"][1]["status_text"] == service.TERMINAL_LOGIN_CONFIRM_TEXT
    assert window["qr_url"] == ""



def test_terminal_poll_ignores_stale_publish_run_for_current_account(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 1,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 1, "status": "success", "status_text": "done", "task_id": None, "publish_success_count": 1},
                            {"id": 2, "status": "waiting_qr", "status_text": "waiting qr", "task_id": None, "publish_success_count": 0},
                        ],
                        "publish_run": {
                            "status": "failed",
                            "account_id": 1,
                            "error": "publish process finished but no wechat evidence",
                        },
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
                "next_probe_at": 999,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "now_ts", lambda: 100)
    monkeypatch.setattr(service, "check_login_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("poll should not check login")))

    result = service.poll_terminal_execution()

    window = result["windows"][0]
    assert window["current_index"] == 1
    assert window["publish_run"] == {}
    assert window["accounts"][1]["status"] == "waiting_qr"
    assert window["accounts"][1]["status_text"] == "waiting qr"


def test_terminal_confirm_ignores_stale_publish_run(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 1,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 1, "status": "success", "status_text": "done", "task_id": None, "publish_success_count": 1},
                            {"id": 2, "status": "running", "status_text": "publishing", "task_id": None, "publish_success_count": 0},
                        ],
                        "publish_run": {
                            "status": "success",
                            "account_id": 1,
                            "asset_key": "videos/old.mp4",
                            "publish_date": "2026-05-06",
                        },
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = service.confirm_terminal_publish_success(1)

    window = result["windows"][0]
    assert window["current_index"] == 1
    assert window["publish_run"] == {}
    assert window["accounts"][1]["status"] == "running"
    assert window["accounts"][1]["publish_success_count"] == 0


def test_terminal_confirm_manual_confirmable_failure_advances(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 1, "status": "error", "status_text": "needs human check", "task_id": None, "publish_success_count": 0},
                            {"id": 2, "status": "pending", "status_text": "pending", "task_id": None, "publish_success_count": 0},
                        ],
                        "publish_run": {
                            "status": "failed",
                            "account_id": 1,
                            "asset_key": "videos/one.mp4",
                            "publish_date": "2026-05-06",
                            "error": "publish process finished but no wechat evidence",
                        },
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    closed: list[int] = []
    monkeypatch.setattr(service, "_close_wechat_browser_for_account", lambda account_id: closed.append(account_id))
    opened: list[int] = []
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: opened.append(account_id) or {"ok": True})

    result = service.confirm_terminal_publish_success(1)

    window = result["windows"][0]
    assert closed == [1]
    assert window["current_index"] == 1
    assert window["publish_run"] == {}
    assert window["accounts"][0]["status"] == "success"
    assert window["accounts"][0]["publish_success_count"] == 1
    assert opened == [2]
    assert window["accounts"][1]["status"] == "waiting_qr"
    assert window["accounts"][1]["status_text"] == service.TERMINAL_LOGIN_CONFIRM_TEXT
    import gasgx_distribution.matrix_publish as mp
    consumed = mp._load_state().get("consumed") or []
    assert consumed[-1]["asset_key"] == "videos/one.mp4"
    assert consumed[-1]["account_id"] == 1


def test_terminal_confirm_last_account_enters_completed_state(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "stale.png",
                        "qr_url": "/api/terminal-execution/windows/1/qr-image",
                        "accounts": [
                            {"id": 1, "status": "running", "status_text": "waiting confirm", "task_id": None, "publish_success_count": 0},
                        ],
                        "publish_run": {
                            "status": "success",
                            "account_id": 1,
                            "asset_key": "videos/one.mp4",
                            "publish_date": "2026-05-06",
                        },
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    closed: list[int] = []
    monkeypatch.setattr(service, "_close_wechat_browser_for_account", lambda account_id: closed.append(account_id))

    result = service.confirm_terminal_publish_success(1)

    window = result["windows"][0]
    assert closed == [1]
    assert window["current_index"] == 1
    assert window["completed"] is True
    assert window["publish_run"] == {}
    assert window["qr_url"] == ""
    assert window["accounts"][0]["status"] == "success"


def test_terminal_confirm_keeps_browser_open_when_stats_busy(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 1, "status": "running", "status_text": "waiting confirm", "task_id": None, "publish_success_count": 0},
                        ],
                        "publish_run": {
                            "status": "success",
                            "account_id": 1,
                            "asset_key": "videos/one.mp4",
                            "publish_date": "2026-05-06",
                        },
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    closed: list[int] = []
    monkeypatch.setattr(service, "_close_wechat_browser_for_account", lambda account_id: closed.append(account_id))
    monkeypatch.setattr("gasgx_distribution.scheduler.is_matrix_wechat_stats_capture_busy_for_account", lambda account_id: True)

    result = service.confirm_terminal_publish_success(1)

    window = result["windows"][0]
    assert closed == []
    assert window["completed"] is True
    assert window["accounts"][0]["status"] == "success"


def test_terminal_confirm_success_keeps_previous_success_when_next_browser_fails(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "terminal_execution_state.json").write_text(
        json.dumps(
            {
                "windows": [
                    {
                        "id": 1,
                        "enabled": True,
                        "operator_wechat": "op1",
                        "color": "#3B82F6",
                        "current_index": 0,
                        "manual_available_at": 0,
                        "qr_path": "",
                        "qr_url": "",
                        "accounts": [
                            {"id": 1, "status": "running", "status_text": "waiting confirm", "task_id": None, "publish_success_count": 0},
                            {"id": 2, "status": "pending", "status_text": "pending", "task_id": None, "publish_success_count": 0},
                        ],
                        "publish_run": {
                            "status": "success",
                            "account_id": 1,
                            "asset_key": "videos/one.mp4",
                            "publish_date": "2026-05-06",
                        },
                    }
                ],
                "config": [],
                "initialized": True,
                "login_started": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "_close_wechat_browser_for_account", lambda account_id: None)
    monkeypatch.setattr(service, "open_account_browser", lambda account_id, platform: (_ for _ in ()).throw(RuntimeError("browser failed")))

    result = service.confirm_terminal_publish_success(1)

    window = result["windows"][0]
    assert window["current_index"] == 1
    assert window["accounts"][0]["status"] == "success"
    assert window["accounts"][0]["publish_success_count"] == 1
    assert window["accounts"][1]["status"] == "error"
    assert window["accounts"][1]["error_stage"] == "login_browser"
    assert window["qr_url"] == ""

def test_terminal_qr_cache_falls_back_when_account_lookup_times_out(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    def fail_get_account(account_id: int) -> dict[str, Any] | None:
        raise SupabaseError("timeout")

    monkeypatch.setattr(service, "get_account", fail_get_account)

    assert service._write_terminal_qr_cache(1, 123) == ""


def test_supabase_list_accounts_uses_bulk_platform_fetch(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object] | None, str | None]] = []

        def select(self, table: str, *, order: str | None = None, filters: dict[str, object] | None = None):
            self.calls.append((table, filters, order))
            if table == "matrix_accounts":
                return [
                    {"id": 2, "account_key": "a2", "display_name": "A2"},
                    {"id": 1, "account_key": "a1", "display_name": "A1"},
                ]
            if table == "account_platforms":
                return [
                    {"id": 11, "account_id": 1, "platform": "wechat", "enabled": 1},
                    {"id": 12, "account_id": 2, "platform": "douyin", "enabled": 1},
                ]
            if table == "browser_profiles":
                return [
                    {"account_id": 1, "profile_dir": "profiles/a1", "debug_port": 9333, "fingerprint_json": {"provider": "builtin-light"}},
                    {"account_platform_id": 12, "profile_dir": "profiles/a2", "debug_port": 9444, "fingerprint_json": {"provider": "builtin-light"}},
                ]
            raise AssertionError(f"unexpected select table: {table}")

        def select_one(self, table: str, *, filters: dict[str, object]):
            raise AssertionError(f"unexpected select_one table: {table}")

    fake = FakeClient()
    monkeypatch.setattr(service, "_brand_supabase", lambda: fake)

    accounts = service.list_accounts()

    assert [item["id"] for item in accounts] == [2, 1]
    assert [item["platforms"][0]["platform"] for item in accounts] == ["douyin", "wechat"]
    assert len(fake.calls) == 3
    assert fake.calls[1][0] == "browser_profiles"
    assert fake.calls[2][0] == "account_platforms"
