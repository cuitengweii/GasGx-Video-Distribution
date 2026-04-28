from __future__ import annotations

from pathlib import Path

from gasgx_distribution import db as dist_db
from gasgx_distribution import scheduler


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
    monkeypatch.setattr("gasgx_distribution.scheduler.get_paths", lambda: FakePaths())
    dist_db.init_db(FakePaths.database_path)


def test_scheduler_run_once_records_state(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(scheduler, "run_wechat_publish", lambda: {"ok": True, "count": 2})

    result = scheduler._run_once(reason="test")
    status = scheduler.scheduler_status()

    assert result["ok"] is True
    assert status["last_ok"] is True
    assert status["last_result"]["count"] == 2
    assert status["next_run_at"]


def test_scheduler_daily_time_rolls_to_next_day(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    settings = {"schedule_mode": "daily", "daily_time": "09:30"}
    now = 1_775_787_000
    first = scheduler._next_daily_run(settings, now=now)
    second = scheduler._next_daily_run(settings, now=first)

    assert first > now
    assert second - first == 24 * 60 * 60
