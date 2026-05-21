from __future__ import annotations

from pathlib import Path
from typing import Any

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
    monkeypatch.setattr("gasgx_distribution.paths.get_paths", lambda: FakePaths())
    dist_db.init_db(FakePaths.database_path)


def test_scheduler_run_once_records_state(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(scheduler, "run_matrix_publish", lambda: {"ok": True, "count": 2})

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


def test_trigger_stats_capture_passes_keep_browser_flag(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    captured: dict[str, Any] = {}

    def fake_run_wechat_stats_capture(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    class InlineThread:
        def __init__(self, *, target=None, name=None, daemon=None):
            self._target = target

        def start(self) -> None:
            if self._target:
                self._target()

    monkeypatch.setattr(scheduler, "run_wechat_stats_capture", fake_run_wechat_stats_capture)
    monkeypatch.setattr(scheduler.threading, "Thread", InlineThread)

    result = scheduler.trigger_matrix_wechat_stats_capture(
        target_date="2026-05-17",
        dry_run=False,
        account_id=58,
        keep_browser_open_on_login_required=True,
        open_capture_in_new_tab=True,
        capture_tab_foreground=True,
        keep_capture_tab_open=True,
    )

    assert result["status"] == "started"
    assert captured["target_date"] == "2026-05-17"
    assert captured["dry_run"] is False
    assert captured["account_id"] == 58
    assert captured["keep_browser_open_on_login_required"] is True
    assert captured["open_capture_in_new_tab"] is True
    assert captured["capture_tab_foreground"] is True
    assert captured["keep_capture_tab_open"] is True
