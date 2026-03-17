from __future__ import annotations

import json
import os
import shutil
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from ..support.config import load_telegram_config
from ..support.paths import apply_runtime_environment, get_paths
from .bootstrap import _load_worker_lock_owner, _load_worker_state, _worker_pid_is_running, recover_bot_surface

DEFAULT_CHECK_INTERVAL_SECONDS = 30
DEFAULT_STALE_HEARTBEAT_SECONDS = 120
DEFAULT_STARTUP_GRACE_SECONDS = 90
DEFAULT_RECOVER_RETRIES = 3
DEFAULT_MAX_RECOVERIES_PER_WINDOW = 6
DEFAULT_RECOVERY_WINDOW_SECONDS = 3600
DEFAULT_SUPERVISOR_STATE_FILE = "telegram_worker_supervisor_state.json"
DEFAULT_SUPERVISOR_LOCK_DIR = "telegram_worker_supervisor.lock"

_READY_STATUSES = {"idle", "polling"}
_STARTING_STATUSES = {"starting", "standby"}
_UNHEALTHY_STATUSES = {"error", "stopped"}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return int(default)
    return parsed


def _telegram_supervisor_config() -> dict[str, Any]:
    telegram_cfg = load_telegram_config()
    raw = telegram_cfg.get("supervisor")
    return dict(raw) if isinstance(raw, dict) else {}


def resolve_supervisor_settings() -> dict[str, int]:
    cfg = _telegram_supervisor_config()
    return {
        "check_interval_seconds": max(5, _to_int(cfg.get("check_interval_seconds"), DEFAULT_CHECK_INTERVAL_SECONDS)),
        "stale_heartbeat_seconds": max(30, _to_int(cfg.get("stale_heartbeat_seconds"), DEFAULT_STALE_HEARTBEAT_SECONDS)),
        "startup_grace_seconds": max(30, _to_int(cfg.get("startup_grace_seconds"), DEFAULT_STARTUP_GRACE_SECONDS)),
        "recover_retries": max(1, _to_int(cfg.get("recover_retries"), DEFAULT_RECOVER_RETRIES)),
        "max_recoveries_per_window": max(
            1,
            _to_int(cfg.get("max_recoveries_per_window"), DEFAULT_MAX_RECOVERIES_PER_WINDOW),
        ),
        "recovery_window_seconds": max(
            60,
            _to_int(cfg.get("recovery_window_seconds"), DEFAULT_RECOVERY_WINDOW_SECONDS),
        ),
    }


def _supervisor_state_path(runtime_root: Path) -> Path:
    return (runtime_root / DEFAULT_SUPERVISOR_STATE_FILE).resolve()


def _supervisor_lock_dir(runtime_root: Path) -> Path:
    return (runtime_root / DEFAULT_SUPERVISOR_LOCK_DIR).resolve()


def _supervisor_log_path(runtime_root: Path) -> Path:
    return (runtime_root / "logs" / f"telegram_supervisor_{time.strftime('%Y%m%d')}.log").resolve()


def _load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json_payload(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)
    return path


def _append_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    line = f"[{_now_text()}] {message}"
    print(line, flush=True)
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def _parse_timestamp(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return None


def _pid_from_payload(payload: dict[str, Any], *keys: str) -> int:
    for key in keys:
        try:
            pid = int(payload.get(key) or 0)
        except Exception:
            pid = 0
        if pid > 0:
            return pid
    return 0


def inspect_worker_health(
    runtime_root: Path,
    *,
    stale_heartbeat_seconds: int,
    startup_grace_seconds: int,
) -> dict[str, Any]:
    state = _load_worker_state(runtime_root)
    lock_owner = _load_worker_lock_owner(runtime_root)
    pid = _pid_from_payload(state, "pid")
    if pid <= 0:
        pid = _pid_from_payload(lock_owner, "pid")
    status = str(state.get("status") or "").strip().lower()
    heartbeat_epoch = None
    for key in (
        "worker_heartbeat_at",
        "last_poll_completed_at",
        "last_poll_started_at",
        "updated_at",
    ):
        heartbeat_epoch = _parse_timestamp(state.get(key))
        if heartbeat_epoch is not None:
            break
    if heartbeat_epoch is None:
        heartbeat_epoch = _parse_timestamp(lock_owner.get("created_at"))
    heartbeat_age_seconds = None if heartbeat_epoch is None else max(0, int(time.time() - heartbeat_epoch))
    running = pid > 0 and _worker_pid_is_running(pid)
    health = {
        "checked_at": _now_text(),
        "healthy": False,
        "reason": "unknown",
        "pid": pid,
        "running": running,
        "status": status,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "heartbeat_at": datetime.fromtimestamp(heartbeat_epoch).strftime("%Y-%m-%d %H:%M:%S")
        if heartbeat_epoch is not None
        else "",
        "state": state,
        "lock_owner": lock_owner,
    }

    if not state:
        if running and heartbeat_age_seconds is not None and heartbeat_age_seconds <= startup_grace_seconds:
            health["healthy"] = True
            health["reason"] = "state_pending"
            return health
        health["reason"] = "missing_state"
        return health
    if pid <= 0:
        health["reason"] = "missing_pid"
        return health
    if not running:
        health["reason"] = "pid_not_running"
        return health
    if status in _UNHEALTHY_STATUSES:
        health["reason"] = f"status_{status}"
        return health
    if status in _STARTING_STATUSES:
        if heartbeat_age_seconds is None or heartbeat_age_seconds <= startup_grace_seconds:
            health["healthy"] = True
            health["reason"] = "starting"
            return health
        health["reason"] = "startup_stalled"
        return health
    if status in _READY_STATUSES:
        if heartbeat_age_seconds is None:
            health["reason"] = "missing_heartbeat"
            return health
        if heartbeat_age_seconds > stale_heartbeat_seconds:
            health["reason"] = "stale_heartbeat"
            return health
        health["healthy"] = True
        health["reason"] = "ready"
        return health
    if heartbeat_age_seconds is not None and heartbeat_age_seconds <= stale_heartbeat_seconds:
        health["healthy"] = True
        health["reason"] = "recent_unknown_status"
        return health
    health["reason"] = f"unknown_status_{status or 'empty'}"
    return health


def _load_supervisor_state(runtime_root: Path) -> dict[str, Any]:
    return _load_json_payload(_supervisor_state_path(runtime_root))


def _trim_recoveries(recovery_epochs: list[float], *, now_epoch: float, window_seconds: int) -> list[float]:
    return [float(item) for item in recovery_epochs if now_epoch - float(item) <= window_seconds]


def _store_supervisor_state(
    runtime_root: Path,
    *,
    health: dict[str, Any],
    action: str,
    recoveries: list[float],
    last_error: str = "",
    recovery_result: dict[str, Any] | None = None,
) -> Path:
    payload = {
        "updated_at": _now_text(),
        "action": action,
        "last_error": str(last_error or "").strip(),
        "recoveries": recoveries[-20:],
        "health": {
            "healthy": bool(health.get("healthy")),
            "reason": str(health.get("reason") or ""),
            "pid": int(health.get("pid") or 0),
            "status": str(health.get("status") or ""),
            "running": bool(health.get("running")),
            "heartbeat_age_seconds": health.get("heartbeat_age_seconds"),
            "heartbeat_at": str(health.get("heartbeat_at") or ""),
            "checked_at": str(health.get("checked_at") or ""),
        },
        "recovery_result": dict(recovery_result or {}),
    }
    return _save_json_payload(_supervisor_state_path(runtime_root), payload)


def _acquire_supervisor_lock(runtime_root: Path) -> Path | None:
    lock_dir = _supervisor_lock_dir(runtime_root)
    owner_path = lock_dir / "owner.json"
    lock_dir.parent.mkdir(parents=True, exist_ok=True)

    for _ in range(2):
        try:
            lock_dir.mkdir()
        except FileExistsError:
            owner = _load_json_payload(owner_path)
            owner_pid = _pid_from_payload(owner, "pid")
            owner_host = str(owner.get("host") or "").strip().lower()
            same_host = not owner_host or owner_host == socket.gethostname().lower()
            if owner_pid > 0 and same_host and _worker_pid_is_running(owner_pid):
                return None
            shutil.rmtree(lock_dir, ignore_errors=True)
            continue
        owner = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "created_at": _now_text(),
            "acquired_epoch": time.time(),
        }
        _save_json_payload(owner_path, owner)
        return lock_dir
    return None


def _release_supervisor_lock(lock_dir: Path | None) -> None:
    if lock_dir is None:
        return
    shutil.rmtree(lock_dir, ignore_errors=True)


def _ensure_worker_running_locked(
    runtime_root: Path,
    *,
    stale_heartbeat_seconds: int,
    startup_grace_seconds: int,
    recover_retries: int,
    max_recoveries_per_window: int,
    recovery_window_seconds: int,
    log_file: Path,
) -> dict[str, Any]:
    now_epoch = time.time()
    supervisor_state = _load_supervisor_state(runtime_root)
    existing_recoveries = supervisor_state.get("recoveries")
    recoveries = _trim_recoveries(
        list(existing_recoveries) if isinstance(existing_recoveries, list) else [],
        now_epoch=now_epoch,
        window_seconds=recovery_window_seconds,
    )
    health = inspect_worker_health(
        runtime_root,
        stale_heartbeat_seconds=stale_heartbeat_seconds,
        startup_grace_seconds=startup_grace_seconds,
    )
    summary = {
        "ok": True,
        "action": "noop",
        "health": health,
        "recoveries_in_window": len(recoveries),
        "recovery_window_seconds": recovery_window_seconds,
        "supervisor_state_file": str(_supervisor_state_path(runtime_root)),
        "log_file": str(log_file),
    }
    if bool(health.get("healthy")):
        _store_supervisor_state(runtime_root, health=health, action="healthy", recoveries=recoveries)
        return summary

    if len(recoveries) >= max_recoveries_per_window:
        summary["ok"] = False
        summary["action"] = "throttled"
        summary["error"] = (
            "telegram worker recovery throttled: "
            f"{len(recoveries)} recoveries in the last {recovery_window_seconds} seconds"
        )
        _append_log(log_file, f"[Supervisor] {summary['error']}; reason={health.get('reason')}")
        _store_supervisor_state(
            runtime_root,
            health=health,
            action="throttled",
            recoveries=recoveries,
            last_error=str(summary["error"]),
        )
        return summary

    _append_log(
        log_file,
        (
            "[Supervisor] worker unhealthy, attempting recover: "
            f"reason={health.get('reason')}, status={health.get('status')}, pid={health.get('pid')}, "
            f"heartbeat_age={health.get('heartbeat_age_seconds')}"
        ),
    )
    try:
        recovery_result = recover_bot_surface(retries=recover_retries)
    except Exception as exc:
        summary["ok"] = False
        summary["action"] = "recover_failed"
        summary["error"] = str(exc)
        _append_log(log_file, f"[Supervisor] recover failed: {exc}")
        _store_supervisor_state(
            runtime_root,
            health=health,
            action="recover_failed",
            recoveries=recoveries,
            last_error=str(exc),
        )
        return summary

    recoveries.append(now_epoch)
    recoveries = _trim_recoveries(recoveries, now_epoch=now_epoch, window_seconds=recovery_window_seconds)
    summary["action"] = "recovered"
    summary["recovery_result"] = recovery_result
    summary["recoveries_in_window"] = len(recoveries)
    _append_log(
        log_file,
        (
            "[Supervisor] recover succeeded: "
            f"worker_pid={int(recovery_result.get('worker_pid') or 0)}, "
            f"attempts={int(recovery_result.get('attempts') or 0)}"
        ),
    )
    _store_supervisor_state(
        runtime_root,
        health=health,
        action="recovered",
        recoveries=recoveries,
        recovery_result=recovery_result,
    )
    return summary


def ensure_worker_running(
    *,
    stale_heartbeat_seconds: int | None = None,
    startup_grace_seconds: int | None = None,
    recover_retries: int | None = None,
    max_recoveries_per_window: int | None = None,
    recovery_window_seconds: int | None = None,
) -> dict[str, Any]:
    apply_runtime_environment()
    paths = get_paths()
    runtime_root = paths.runtime_root
    runtime_root.mkdir(parents=True, exist_ok=True)
    settings = resolve_supervisor_settings()
    log_file = _supervisor_log_path(runtime_root)
    lock_dir = _acquire_supervisor_lock(runtime_root)
    if lock_dir is None:
        _append_log(log_file, "[Supervisor] another supervisor instance is already active; skip one-shot check.")
        return {
            "ok": True,
            "action": "lock_busy",
            "log_file": str(log_file),
            "supervisor_state_file": str(_supervisor_state_path(runtime_root)),
        }
    try:
        return _ensure_worker_running_locked(
            runtime_root,
            stale_heartbeat_seconds=max(
                30,
                int(stale_heartbeat_seconds if stale_heartbeat_seconds is not None else settings["stale_heartbeat_seconds"]),
            ),
            startup_grace_seconds=max(
                30,
                int(startup_grace_seconds if startup_grace_seconds is not None else settings["startup_grace_seconds"]),
            ),
            recover_retries=max(
                1,
                int(recover_retries if recover_retries is not None else settings["recover_retries"]),
            ),
            max_recoveries_per_window=max(
                1,
                int(
                    max_recoveries_per_window
                    if max_recoveries_per_window is not None
                    else settings["max_recoveries_per_window"]
                ),
            ),
            recovery_window_seconds=max(
                60,
                int(recovery_window_seconds if recovery_window_seconds is not None else settings["recovery_window_seconds"]),
            ),
            log_file=log_file,
        )
    finally:
        _release_supervisor_lock(lock_dir)


def run_supervisor(
    *,
    check_interval_seconds: int | None = None,
    stale_heartbeat_seconds: int | None = None,
    startup_grace_seconds: int | None = None,
    recover_retries: int | None = None,
    max_recoveries_per_window: int | None = None,
    recovery_window_seconds: int | None = None,
) -> int:
    apply_runtime_environment()
    paths = get_paths()
    runtime_root = paths.runtime_root
    runtime_root.mkdir(parents=True, exist_ok=True)
    settings = resolve_supervisor_settings()
    log_file = _supervisor_log_path(runtime_root)
    interval_seconds = max(
        5,
        int(check_interval_seconds if check_interval_seconds is not None else settings["check_interval_seconds"]),
    )
    lock_dir = _acquire_supervisor_lock(runtime_root)
    if lock_dir is None:
        _append_log(log_file, "[Supervisor] another supervisor instance is already active; exit.")
        return 0
    _append_log(log_file, f"[Supervisor] started with check_interval_seconds={interval_seconds}")
    try:
        while True:
            _ensure_worker_running_locked(
                runtime_root,
                stale_heartbeat_seconds=max(
                    30,
                    int(
                        stale_heartbeat_seconds
                        if stale_heartbeat_seconds is not None
                        else settings["stale_heartbeat_seconds"]
                    ),
                ),
                startup_grace_seconds=max(
                    30,
                    int(
                        startup_grace_seconds
                        if startup_grace_seconds is not None
                        else settings["startup_grace_seconds"]
                    ),
                ),
                recover_retries=max(
                    1,
                    int(recover_retries if recover_retries is not None else settings["recover_retries"]),
                ),
                max_recoveries_per_window=max(
                    1,
                    int(
                        max_recoveries_per_window
                        if max_recoveries_per_window is not None
                        else settings["max_recoveries_per_window"]
                    ),
                ),
                recovery_window_seconds=max(
                    60,
                    int(recovery_window_seconds if recovery_window_seconds is not None else settings["recovery_window_seconds"]),
                ),
                log_file=log_file,
            )
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        _append_log(log_file, "[Supervisor] stopped by keyboard interrupt.")
        return 0
    finally:
        _release_supervisor_lock(lock_dir)
