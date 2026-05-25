from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from gasgx_distribution import db as dist_db
from gasgx_distribution.public_settings import (
    load_distribution_settings,
    load_platform_publish_settings,
    load_wechat_publish_settings,
    resolve_effective_publish_mode,
    resolve_material_dir,
    save_distribution_settings,
    save_wechat_publish_settings,
)
from gasgx_distribution.web import create_app


def _isolated_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "sqlite")

    class FakePaths:
        repo_root = tmp_path
        runtime_root = tmp_path / "runtime"
        profiles_root = tmp_path / "profiles" / "matrix"
        database_path = tmp_path / "runtime" / "gasgx_distribution.db"

        def ensure(self) -> None:
            self.runtime_root.mkdir(parents=True, exist_ok=True)
            self.profiles_root.mkdir(parents=True, exist_ok=True)
            self.database_path.parent.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("gasgx_distribution.db.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.service.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.public_settings.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.matrix_publish.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.paths.get_paths", lambda: FakePaths())
    dist_db.init_db(FakePaths.database_path)


def test_wechat_public_settings_persist_and_normalize(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    saved = save_wechat_publish_settings(
        {
            "material_dir": "runtime/materials/videos",
            "publish_mode": "draft",
            "topics": "#one #two",
            "collection_name": "test collection",
            "caption": "hello",
            "declare_original": "true",
            "short_title": "GasGx Test",
            "location": "天津",
            "upload_timeout": 10,
        }
    )

    assert saved["publish_mode"] == "draft"
    assert saved["declare_original"] is True
    assert saved["upload_timeout"] == 60
    assert saved["topics"] == "#one #two"
    assert saved["short_title"] == "GasGx Test"
    assert saved["location"] == "天津"
    assert load_wechat_publish_settings()["collection_name"] == "test collection"


def test_wechat_public_settings_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    result = client.patch(
        "/api/settings/wechat-publish",
        json={
            "material_dir": "runtime/materials/videos",
            "publish_mode": "publish",
            "collection_name": "test collection",
            "caption": "shared caption",
            "declare_original": False,
            "short_title": "GasGx API",
            "location": "",
            "upload_timeout": 120,
        },
    )

    assert result.status_code == 200
    assert result.json()["caption"] == "shared caption"
    assert result.json()["short_title"] == "GasGx API"
    assert result.json()["location"] == ""
    assert client.get("/api/settings/wechat-publish").json()["upload_timeout"] == 120


def test_resolve_material_dir_follows_video_matrix_output(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    assert resolve_material_dir() == (tmp_path / "runtime" / "materials" / "videos").resolve()


def test_resolve_material_dir_custom_when_follow_disabled(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    save_distribution_settings(
        {
            "common": {
                "material_dir": "runtime/materials/custom",
                "material_dir_follows_video_matrix": False,
            },
        }
    )
    assert resolve_material_dir() == (tmp_path / "runtime" / "materials" / "custom").resolve()


def test_distribution_settings_store_common_and_platforms(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    saved = save_distribution_settings(
        {
            "common": {
                "material_dir": "runtime/materials/videos",
                "publish_mode": "draft",
                "topics": "#gas #power",
                "upload_timeout": 120,
                "wechat_content_type": "image_text",
                "wechat_visibility": "friends",
                "wechat_comment_permission": "followers",
                "wechat_collection_name": "test collection",
                "wechat_declare_original": True,
                "wechat_short_title": "GasGx Matrix",
                "wechat_location": "上海",
                "wechat_caption": "common caption",
            },
            "jobs": {
                "matrix_wechat_publish": {
                    "batch_size": 5,
                    "batch_interval_min_minutes": 10,
                    "batch_interval_max_minutes": 3,
                    "enabled": "true",
                    "schedule_mode": "daily",
                    "daily_time": "25:90",
                    "run_interval_minutes": 30,
                    "rotate_start_group": "true",
                    "shuffle_within_batch": "false",
                    "retry_failed_last": True,
                }
            },
            "platforms": {
                "wechat": {
                    "caption": "wechat caption",
                    "collection_name": "test collection",
                    "declare_original": True,
                    "short_title": "GasGx Matrix",
                    "location": "上海",
                },
                "douyin": {
                    "caption": "douyin caption",
                    "visibility": "private",
                    "publish_mode": "publish",
                },
            },
        }
    )

    assert saved["common"]["publish_mode"] == "draft"
    assert saved["common"]["topics"] == "#gas #power"
    assert saved["common"]["wechat_content_type"] == "image_text"
    assert saved["common"]["wechat_visibility"] == "friends"
    assert saved["common"]["wechat_comment_permission"] == "followers"
    assert saved["common"]["wechat_collection_name"] == "test collection"
    assert saved["common"]["wechat_declare_original"] is True
    assert saved["common"]["wechat_short_title"] == "GasGx Matrix"
    assert saved["common"]["wechat_caption"] == "common caption"
    assert saved["jobs"]["matrix_wechat_publish"]["batch_size"] == 5
    assert saved["jobs"]["matrix_wechat_publish"]["enabled"] is True
    assert saved["jobs"]["matrix_wechat_publish"]["schedule_mode"] == "daily"
    assert saved["jobs"]["matrix_wechat_publish"]["daily_time"] == "23:59"
    assert saved["jobs"]["matrix_wechat_publish"]["run_interval_minutes"] == 30
    assert saved["jobs"]["matrix_wechat_publish"]["batch_interval_max_minutes"] == 10
    assert saved["jobs"]["matrix_wechat_publish"]["shuffle_within_batch"] is False
    assert saved["platforms"]["wechat"]["declare_original"] is True
    assert saved["platforms"]["wechat"]["short_title"] == "GasGx Matrix"
    assert saved["platforms"]["wechat"]["location"] == "上海"
    assert saved["platforms"]["douyin"]["visibility"] == "private"
    assert load_distribution_settings()["platforms"]["douyin"]["caption"] == "douyin caption"


def test_distribution_settings_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    result = client.patch(
        "/api/settings/distribution",
        json={
            "common": {"material_dir": "runtime/materials/videos", "publish_mode": "publish", "upload_timeout": 180},
            "jobs": {"matrix_wechat_publish": {"batch_size": 3, "enabled": True, "run_interval_minutes": 60, "rotate_start_group": False}},
            "platforms": {"tiktok": {"caption": "TikTok caption", "comment_permission": "closed"}},
        },
    )

    assert result.status_code == 200
    assert "charset=utf-8" in result.headers["content-type"]
    assert result.json()["jobs"]["matrix_wechat_publish"]["batch_size"] == 3
    assert result.json()["jobs"]["matrix_wechat_publish"]["enabled"] is True
    assert result.json()["platforms"]["tiktok"]["caption"] == "TikTok caption"
    assert client.get("/api/settings/distribution").json()["common"]["upload_timeout"] == 180


def test_wechat_platform_inherit_values_resolve_to_common(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    save_distribution_settings(
        {
            "common": {
                "wechat_content_type": "image_text",
                "wechat_visibility": "friends",
                "wechat_comment_permission": "followers",
                "wechat_collection_name": "test collection",
                "wechat_declare_original": True,
                "wechat_short_title": "GasGx Matrix",
                "wechat_location": "上海",
                "wechat_caption": "common caption",
            },
            "platforms": {
                "wechat": {
                    "content_type": "inherit",
                    "visibility": "inherit",
                    "comment_permission": "inherit",
                    "collection_name": "inherit",
                    "declare_original": "inherit",
                    "short_title": "inherit",
                    "location": "inherit",
                    "caption": "inherit",
                },
            },
        }
    )

    resolved = load_platform_publish_settings("wechat")
    assert resolved["content_type"] == "image_text"
    assert resolved["visibility"] == "friends"
    assert resolved["comment_permission"] == "followers"
    assert resolved["collection_name"] == "test collection"
    assert resolved["declare_original"] is True
    assert resolved["short_title"] == "GasGx Matrix"
    assert resolved["location"] == "上海"
    assert resolved["caption"] == "common caption"


def test_wechat_platform_blank_location_does_not_fallback_to_common(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    save_distribution_settings(
        {
            "common": {
                "wechat_location": "涓婃捣",
            },
            "platforms": {
                "wechat": {
                    "location": "",
                },
            },
        }
    )

    resolved = load_platform_publish_settings("wechat")
    assert resolved["location"] == ""


def test_wechat_publish_settings_api_blank_location_keeps_no_location(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    save_distribution_settings({"common": {"wechat_location": "涓婃捣"}})
    client = TestClient(create_app())

    result = client.patch(
        "/api/settings/wechat-publish",
        json={
            "material_dir": "runtime/materials/videos",
            "publish_mode": "publish",
            "collection_name": "test collection",
            "caption": "shared caption",
            "declare_original": False,
            "short_title": "GasGx API",
            "location": "",
            "upload_timeout": 120,
        },
    )

    assert result.status_code == 200
    assert result.json()["location"] == ""
    assert client.get("/api/settings/wechat-publish").json()["location"] == ""


def test_distribution_settings_default_wechat_platform_uses_inherit(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    saved = save_distribution_settings({})
    wechat = saved["platforms"]["wechat"]
    assert wechat["content_type"] == "inherit"
    assert wechat["visibility"] == "inherit"
    assert wechat["comment_permission"] == "inherit"
    assert wechat["collection_name"] == "inherit"
    assert wechat["declare_original"] == "inherit"
    assert wechat["short_title"] == "inherit"
    assert wechat["location"] == "inherit"
    assert wechat["caption"] == "inherit"


def test_operator_wechats_api_persists_to_distribution_settings(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    added = client.post("/api/operator-wechats", json={"operator_wechat": "aamebb"})

    assert added.status_code == 200
    added_items = added.json()["items"]
    assert added_items[:2] == ["aamecc", "aalbcc"]
    assert "aamebb" in added_items
    assert client.get("/api/operator-wechats").json() == added_items
    assert load_distribution_settings()["common"]["operator_wechats"] == added_items

    duplicate = client.post("/api/operator-wechats", json={"operator_wechat": "aamebb"})

    assert duplicate.status_code == 200
    assert duplicate.json()["items"] == added_items


def test_operator_wechats_api_includes_values_inferred_from_existing_accounts(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    result = client.get("/api/operator-wechats")

    assert result.status_code == 200
    items = result.json()
    assert items[:2] == ["aamecc", "aalbcc"]
    assert len(items) > 2
    assert all(isinstance(item, str) and item.strip() for item in items)
    assert set(items) - {"aamecc", "aalbcc"}


def test_open_material_dir_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    opened = []

    def fake_open_material_directory(raw_path: str) -> dict[str, str | bool]:
        opened.append(raw_path)
        return {"ok": True, "path": str(tmp_path / raw_path)}

    monkeypatch.setattr("gasgx_distribution.service.open_material_directory", fake_open_material_directory)
    client = TestClient(create_app())

    denied = client.post("/api/settings/material-dir/open", json={"material_dir": "runtime/materials/videos", "password": "wrong"})
    result = client.post("/api/settings/material-dir/open", json={"material_dir": "runtime/materials/videos", "password": "cuitengwei2023"})

    assert denied.status_code == 401
    assert result.status_code == 200
    assert result.json()["ok"] is True
    assert opened == ["runtime/materials/videos"]


def test_open_system_directory_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    opened = []

    def fake_open_system_directory(kind: str) -> dict[str, str | bool]:
        opened.append(kind)
        return {"ok": True, "path": str(tmp_path / kind)}

    monkeypatch.setattr("gasgx_distribution.service.open_system_directory", fake_open_system_directory)
    client = TestClient(create_app())

    result = client.post("/api/system/open-directory/output")

    assert result.status_code == 200
    assert result.json()["ok"] is True
    assert opened == ["output"]


def test_system_initialize_requires_super_admin_password(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    initialized = []

    def fake_initialize_system() -> dict[str, bool]:
        initialized.append(True)
        return {"ok": True}

    monkeypatch.setattr("gasgx_distribution.service.initialize_system", fake_initialize_system)
    client = TestClient(create_app())

    denied = client.post("/api/system/initialize", json={"password": "wrong"})
    allowed = client.post("/api/system/initialize", json={"password": "cuitengwei2023"})

    assert denied.status_code == 401
    assert allowed.status_code == 200
    assert allowed.json()["ok"] is True
    assert initialized == [True]


def test_database_dictionary_api_returns_tables(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    result = client.get("/api/system/database-dictionary")

    assert result.status_code == 200
    payload = result.json()
    assert payload["tables"]
    table_names = {table["name"] for table in payload["tables"]}
    assert "matrix_accounts" in table_names
    matrix_accounts = next(table for table in payload["tables"] if table["name"] == "matrix_accounts")
    assert any(column["name"] == "account_key" for column in matrix_accounts["columns"])


def test_matrix_wechat_job_status_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    result = client.get("/api/jobs/matrix-wechat/status")

    assert result.status_code == 200
    assert result.json()["enabled"] is False
    assert "thread_alive" in result.json()


def test_delete_task_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    account = client.post(
        "/api/accounts",
        json={"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]},
    ).json()
    task = client.post(
        "/api/tasks",
        json={"account_id": account["id"], "platform": "wechat", "task_type": "publish"},
    ).json()

    result = client.delete(f"/api/tasks/{task['id']}")

    assert result.status_code == 200
    assert all(item["id"] != task["id"] for item in client.get("/api/tasks").json())


def test_bulk_task_status_and_delete_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())
    account = client.post(
        "/api/accounts",
        json={"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]},
    ).json()
    first = client.post(
        "/api/tasks",
        json={"account_id": account["id"], "platform": "wechat", "task_type": "publish"},
    ).json()
    second = client.post(
        "/api/tasks",
        json={"account_id": account["id"], "platform": "wechat", "task_type": "draft"},
    ).json()

    paused = client.post("/api/tasks/bulk-status", json={"ids": [first["id"], second["id"]], "status": "paused"})

    assert paused.status_code == 200
    assert paused.json()["updated"] == 2
    tasks = client.get("/api/tasks").json()
    assert {item["id"]: item["status"] for item in tasks}[first["id"]] == "paused"
    assert {item["id"]: item["status"] for item in tasks}[second["id"]] == "paused"

    deleted = client.post("/api/tasks/bulk-delete", json={"ids": [first["id"], second["id"]]})

    assert deleted.status_code == 200
    assert deleted.json()["deleted"] == 2
    assert client.get("/api/tasks").json() == []


def test_distribution_settings_keep_vpn_section_and_use_vpn(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    saved = save_distribution_settings(
        {
            "common": {},
            "jobs": {"matrix_wechat_publish": {"use_vpn": True}},
            "platforms": {},
            "vpn": {"subscription_url": "https://example.invalid/subscribe"},
        }
    )

    assert saved["jobs"]["matrix_wechat_publish"]["use_vpn"] is True
    assert saved["vpn"]["subscription_url"] == "https://example.invalid/subscribe"
    assert load_distribution_settings()["vpn"]["subscription_url"] == "https://example.invalid/subscribe"


def test_resolve_effective_publish_mode_prefers_account_override() -> None:
    assert resolve_effective_publish_mode("publish", "draft") == "draft"
    assert resolve_effective_publish_mode("draft", "inherit") == "draft"
    assert resolve_effective_publish_mode("publish", "inherit") == "publish"
