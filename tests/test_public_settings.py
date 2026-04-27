from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from gasgx_distribution import db as dist_db
from gasgx_distribution.public_settings import (
    load_distribution_settings,
    load_wechat_publish_settings,
    save_distribution_settings,
    save_wechat_publish_settings,
)
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
    monkeypatch.setattr("gasgx_distribution.matrix_publish.get_paths", lambda: FakePaths())
    dist_db.init_db(FakePaths.database_path)


def test_wechat_public_settings_persist_and_normalize(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    saved = save_wechat_publish_settings(
        {
            "material_dir": "runtime/materials/videos",
            "publish_mode": "draft",
            "collection_name": "test collection",
            "caption": "hello",
            "declare_original": "true",
            "upload_timeout": 10,
        }
    )

    assert saved["publish_mode"] == "draft"
    assert saved["declare_original"] is True
    assert saved["upload_timeout"] == 60
    assert load_wechat_publish_settings()["collection_name"] == "test collection"


def test_wechat_public_settings_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    result = client.patch(
        "/api/settings/wechat-publish",
        json={
            "material_dir": "runtime/materials/videos",
            "publish_mode": "publish",
            "collection_name": "赛博皮卡天津港现车",
            "caption": "统一文案",
            "declare_original": False,
            "upload_timeout": 120,
        },
    )

    assert result.status_code == 200
    assert result.json()["caption"] == "统一文案"
    assert client.get("/api/settings/wechat-publish").json()["upload_timeout"] == 120


def test_distribution_settings_store_common_and_platforms(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    saved = save_distribution_settings(
        {
            "common": {
                "material_dir": "runtime/materials/videos",
                "publish_mode": "draft",
                "upload_timeout": 120,
            },
            "platforms": {
                "wechat": {
                    "caption": "视频号文案",
                    "collection_name": "赛博皮卡天津港现车",
                    "declare_original": True,
                },
                "douyin": {
                    "caption": "抖音文案",
                    "visibility": "private",
                    "publish_mode": "publish",
                },
            },
        }
    )

    assert saved["common"]["publish_mode"] == "draft"
    assert saved["platforms"]["wechat"]["declare_original"] is True
    assert saved["platforms"]["douyin"]["visibility"] == "private"
    assert load_distribution_settings()["platforms"]["douyin"]["caption"] == "抖音文案"


def test_distribution_settings_api(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    client = TestClient(create_app())

    result = client.patch(
        "/api/settings/distribution",
        json={
            "common": {"material_dir": "runtime/materials/videos", "publish_mode": "publish", "upload_timeout": 180},
            "platforms": {"tiktok": {"caption": "TikTok 文案", "comment_permission": "closed"}},
        },
    )

    assert result.status_code == 200
    assert result.json()["platforms"]["tiktok"]["caption"] == "TikTok 文案"
    assert client.get("/api/settings/distribution").json()["common"]["upload_timeout"] == 180
