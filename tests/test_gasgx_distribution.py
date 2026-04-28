from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi.testclient import TestClient

from gasgx_distribution import service
from gasgx_distribution import db as dist_db
from gasgx_distribution.web import create_app


def _isolated_paths(monkeypatch, tmp_path: Path) -> None:
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

    updated = service.update_account(int(account["id"]), {"notes": "phase-one"})
    assert updated is not None
    assert updated["notes"] == "phase-one"


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
