import json
from pathlib import Path
from types import SimpleNamespace

from cybercar.telegram import supervisor


def _write_worker_state(runtime_root: Path, payload: dict[str, object]) -> None:
    path = runtime_root / "runtime" / "telegram_command_worker_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_inspect_worker_health_reports_ready_when_heartbeat_is_fresh(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "runtime-root"
    _write_worker_state(
        runtime_root,
        {
            "pid": 4321,
            "status": "idle",
            "worker_heartbeat_at": "2026-03-17 10:00:30",
            "updated_at": "2026-03-17 10:00:30",
        },
    )

    monkeypatch.setattr(supervisor.time, "time", lambda: 1763373660.0)
    monkeypatch.setattr(supervisor, "_worker_pid_is_running", lambda pid: pid == 4321)

    health = supervisor.inspect_worker_health(
        runtime_root,
        stale_heartbeat_seconds=120,
        startup_grace_seconds=90,
    )

    assert health["healthy"] is True
    assert health["reason"] == "ready"
    assert health["pid"] == 4321
    assert health["status"] == "idle"


def test_inspect_worker_health_reports_stale_heartbeat(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "runtime-root"
    _write_worker_state(
        runtime_root,
        {
            "pid": 4321,
            "status": "polling",
            "worker_heartbeat_at": "2000-01-01 00:00:00",
            "updated_at": "2000-01-01 00:00:00",
        },
    )

    monkeypatch.setattr(supervisor.time, "time", lambda: 1763374200.0)
    monkeypatch.setattr(supervisor, "_worker_pid_is_running", lambda pid: pid == 4321)

    health = supervisor.inspect_worker_health(
        runtime_root,
        stale_heartbeat_seconds=120,
        startup_grace_seconds=90,
    )

    assert health["healthy"] is False
    assert health["reason"] == "stale_heartbeat"


def test_ensure_worker_running_skips_recover_when_worker_is_healthy(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    runtime_root = repo_root / "runtime"
    runtime_root.mkdir(parents=True)
    _write_worker_state(
        runtime_root,
        {
            "pid": 9876,
            "status": "idle",
            "worker_heartbeat_at": "2026-03-17 10:00:50",
            "updated_at": "2026-03-17 10:00:50",
        },
    )
    fake_paths = SimpleNamespace(repo_root=repo_root, runtime_root=runtime_root)

    recover_calls: list[int] = []

    monkeypatch.setattr(supervisor, "apply_runtime_environment", lambda: fake_paths)
    monkeypatch.setattr(supervisor, "get_paths", lambda: fake_paths)
    monkeypatch.setattr(supervisor.time, "time", lambda: 1763373660.0)
    monkeypatch.setattr(supervisor, "_worker_pid_is_running", lambda pid: pid == 9876)
    monkeypatch.setattr(supervisor, "recover_bot_surface", lambda retries: recover_calls.append(retries) or {"ok": True})

    result = supervisor.ensure_worker_running(stale_heartbeat_seconds=120, startup_grace_seconds=90)

    assert result["ok"] is True
    assert result["action"] == "noop"
    assert recover_calls == []


def test_ensure_worker_running_recovers_unhealthy_worker(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    runtime_root = repo_root / "runtime"
    runtime_root.mkdir(parents=True)
    _write_worker_state(
        runtime_root,
        {
            "pid": 7654,
            "status": "error",
            "worker_heartbeat_at": "2026-03-17 10:00:00",
            "updated_at": "2026-03-17 10:00:00",
        },
    )
    fake_paths = SimpleNamespace(repo_root=repo_root, runtime_root=runtime_root)

    recover_calls: list[int] = []

    monkeypatch.setattr(supervisor, "apply_runtime_environment", lambda: fake_paths)
    monkeypatch.setattr(supervisor, "get_paths", lambda: fake_paths)
    monkeypatch.setattr(supervisor.time, "time", lambda: 1763373900.0)
    monkeypatch.setattr(supervisor, "_worker_pid_is_running", lambda pid: False)
    monkeypatch.setattr(
        supervisor,
        "recover_bot_surface",
        lambda retries: recover_calls.append(retries)
        or {
            "ok": True,
            "attempts": 1,
            "worker_pid": 8888,
            "terminated_pids": [7654],
        },
    )

    result = supervisor.ensure_worker_running(
        stale_heartbeat_seconds=120,
        startup_grace_seconds=90,
        recover_retries=2,
    )

    assert result["ok"] is True
    assert result["action"] == "recovered"
    assert recover_calls == [2]
    state_payload = json.loads((runtime_root / supervisor.DEFAULT_SUPERVISOR_STATE_FILE).read_text(encoding="utf-8"))
    assert len(state_payload["recoveries"]) == 1
    assert state_payload["action"] == "recovered"


def test_ensure_worker_running_throttles_when_recovery_budget_is_exhausted(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    runtime_root = repo_root / "runtime"
    runtime_root.mkdir(parents=True)
    _write_worker_state(
        runtime_root,
        {
            "pid": 7654,
            "status": "stopped",
            "worker_heartbeat_at": "2026-03-17 10:00:00",
            "updated_at": "2026-03-17 10:00:00",
        },
    )
    (runtime_root / supervisor.DEFAULT_SUPERVISOR_STATE_FILE).write_text(
        json.dumps(
            {
                "recoveries": [
                    1763373600.0,
                    1763373650.0,
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    fake_paths = SimpleNamespace(repo_root=repo_root, runtime_root=runtime_root)

    recover_calls: list[int] = []

    monkeypatch.setattr(supervisor, "apply_runtime_environment", lambda: fake_paths)
    monkeypatch.setattr(supervisor, "get_paths", lambda: fake_paths)
    monkeypatch.setattr(supervisor.time, "time", lambda: 1763373900.0)
    monkeypatch.setattr(supervisor, "_worker_pid_is_running", lambda pid: False)
    monkeypatch.setattr(supervisor, "recover_bot_surface", lambda retries: recover_calls.append(retries) or {"ok": True})

    result = supervisor.ensure_worker_running(
        stale_heartbeat_seconds=120,
        startup_grace_seconds=90,
        max_recoveries_per_window=2,
        recovery_window_seconds=3600,
    )

    assert result["ok"] is False
    assert result["action"] == "throttled"
    assert recover_calls == []
