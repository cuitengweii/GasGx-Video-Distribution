from __future__ import annotations

import time
from pathlib import Path

from gasgx_distribution import db as dist_db
from gasgx_distribution import service
from gasgx_distribution.matrix_publish import build_publish_plan, list_candidate_videos, run_wechat_publish


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
    monkeypatch.setattr("gasgx_distribution.matrix_publish.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.public_settings.get_paths", lambda: FakePaths())
    dist_db.init_db(FakePaths.database_path)


def _write_video(path: Path, mtime: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"video")
    path.touch()
    import os

    os.utime(path, (mtime, mtime))


def test_publish_plan_assigns_newest_unused_video_by_account_order(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    service.create_account({"account_key": "a-02", "display_name": "B", "platforms": ["wechat"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    now = int(time.time())
    _write_video(base / "old.mp4", now - 20)
    _write_video(base / "newest.mp4", now)
    _write_video(base / "middle.mp4", now - 10)

    plan = build_publish_plan()

    assert [item.account_id for item in plan] == [1, 2]
    assert [item.source_video.name for item in plan] == ["newest.mp4", "middle.mp4"]


def test_dry_run_does_not_mark_videos_used(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    video = tmp_path / "runtime" / "materials" / "videos" / "one.mp4"
    _write_video(video, int(time.time()))

    result = run_wechat_publish(dry_run=True)

    assert result["dry_run"] is True
    assert list_candidate_videos()[0].name == "one.mp4"
