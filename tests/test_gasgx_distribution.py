from __future__ import annotations

import json
import os
import hmac
import hashlib
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from fastapi.testclient import TestClient

from gasgx_distribution import service
from gasgx_distribution import db as dist_db
from gasgx_distribution import matrix_publish
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
    monkeypatch.setattr("gasgx_distribution.public_settings.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.control_plane.get_paths", lambda: FakePaths())
    dist_db.init_db(FakePaths.database_path)


def test_account_crud_creates_independent_platform_profiles(monkeypatch, tmp_path: Path) -> None:
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
    for item in account["platforms"]:
        assert Path(item["profile_dir"]).exists()
        assert "profiles" in item["profile_dir"]
        assert isinstance(item["debug_port"], int)
        assert 12000 <= int(item["debug_port"]) <= 32000
        assert item["fingerprint"]["provider"] == "builtin-light"

    updated = service.update_account(int(account["id"]), {"notes": "phase-one"})
    assert updated is not None
    assert updated["notes"] == "phase-one"


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


def test_accounts_include_matrix_publish_success_count(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    first = service.create_account({"account_key": "gasgx-01", "display_name": "GasGx 01", "platforms": ["wechat"]})
    second = service.create_account({"account_key": "gasgx-02", "display_name": "GasGx 02", "platforms": ["wechat"]})
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        json.dumps(
            {
                "runs": [
                    {"account_id": first["id"], "success": True},
                    {"account_id": first["id"], "success": True},
                    {"account_id": first["id"], "success": False},
                    {"account_id": second["id"], "success": True},
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

    class FakeResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload
            self.status_code = 200

        def json(self) -> dict[str, object]:
            return self._payload

    def fake_get(url: str, timeout: float) -> FakeResponse:
        assert "SECRET_TOKEN" in url
        if url.endswith("/getMe"):
            return FakeResponse({"ok": True, "result": {"username": "GasGxBot"}})
        return FakeResponse({"ok": True, "result": [{"message": {"chat": {"id": -10042}}}]})

    monkeypatch.setattr("gasgx_distribution.service.requests.get", fake_get)
    client = TestClient(create_app())

    response = client.post("/api/ai-robots/telegram/resolve", json={"token": "SECRET_TOKEN"})

    assert response.status_code == 200
    data = response.json()
    assert data["username"] == "GasGxBot"
    assert data["chat_id"] == "-10042"
    assert data["webhook_url"] == "https://api.telegram.org/botSECRET_TOKEN/sendMessage"


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
    os.utime(used, (1000, 1000))
    os.utime(remaining, (2000, 2000))
    used_key = f"{used.name}|{used.stat().st_size}|{int(used.stat().st_mtime)}"
    (tmp_path / "runtime" / "matrix_publish_state.json").write_text(
        json.dumps({"used_videos": [used_key], "runs": []}),
        encoding="utf-8",
    )

    summary = service.dashboard_summary()

    assert summary["remaining_material_videos"] == 1


def test_open_browser_uses_account_specific_profile(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    account = service.create_account({"account_key": "gasgx-x-01", "display_name": "GasGx X 01", "platforms": ["x"]})
    calls: list[dict[str, object]] = []
    monkeypatch.setattr("gasgx_distribution.service.engine._ensure_chrome_debug_port", lambda **kwargs: calls.append(kwargs))

    result = service.open_account_browser(int(account["id"]), "x")

    assert result["ok"] is True
    assert calls
    assert calls[0]["auto_open_chrome"] is True
    assert "gasgx-x-01" in str(calls[0]["chrome_user_data_dir"])


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
