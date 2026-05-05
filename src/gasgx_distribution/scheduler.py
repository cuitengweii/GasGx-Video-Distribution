from __future__ import annotations

import json
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .matrix_publish import check_wechat_matrix_login_status, run_matrix_publish
from .paths import get_paths
from .public_settings import load_distribution_settings
from .wechat_stats_capture import run_wechat_stats_capture

_LOCK = threading.Lock()
_THREAD: threading.Thread | None = None
_STOP = threading.Event()
_RUNNING = threading.Event()
_STATS_RUNNING = threading.Event()


def scheduler_state_path() -> Path:
    path = get_paths().runtime_root / "matrix_scheduler_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _read_state() -> dict[str, Any]:
    path = scheduler_state_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_state(payload: dict[str, Any]) -> None:
    scheduler_state_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _job_settings() -> dict[str, Any]:
    return dict(load_distribution_settings().get("jobs", {}).get("matrix_wechat_publish", {}))


def _stats_job_settings() -> dict[str, Any]:
    return dict(load_distribution_settings().get("jobs", {}).get("matrix_wechat_stats_capture", {}))


def _now() -> int:
    return int(time.time())


def _next_interval_run(settings: dict[str, Any], *, now: int | None = None) -> int:
    base = _now() if now is None else int(now)
    interval = int(settings.get("run_interval_minutes") or 1440)
    return base + max(5, interval) * 60


def _parse_daily_time(value: Any) -> tuple[int, int]:
    try:
        hour_text, minute_text = str(value or "09:00").strip().split(":", 1)
        hour = min(23, max(0, int(hour_text)))
        minute = min(59, max(0, int(minute_text)))
    except Exception:
        hour, minute = 9, 0
    return hour, minute


def _next_daily_run(settings: dict[str, Any], *, now: int | None = None) -> int:
    base_ts = _now() if now is None else int(now)
    current = datetime.fromtimestamp(base_ts)
    hour, minute = _parse_daily_time(settings.get("daily_time"))
    target = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target.timestamp() <= base_ts:
        target += timedelta(days=1)
    return int(target.timestamp())


def _next_run_at(settings: dict[str, Any], *, now: int | None = None) -> int:
    if str(settings.get("schedule_mode") or "interval").strip().lower() == "daily":
        return _next_daily_run(settings, now=now)
    return _next_interval_run(settings, now=now)


def _login_check_interval_seconds(settings: dict[str, Any]) -> int:
    return max(300, int(settings.get("login_check_interval_minutes") or 30) * 60)


def _base_status() -> dict[str, Any]:
    settings = _job_settings()
    stats_settings = _stats_job_settings()
    state = _read_state()
    return {
        "enabled": bool(settings.get("enabled")),
        "running": _RUNNING.is_set(),
        "run_interval_minutes": int(settings.get("run_interval_minutes") or 1440),
        "schedule_mode": str(settings.get("schedule_mode") or "interval"),
        "daily_time": str(settings.get("daily_time") or "09:00"),
        "last_started_at": state.get("last_started_at"),
        "last_finished_at": state.get("last_finished_at"),
        "last_ok": state.get("last_ok"),
        "last_error": state.get("last_error", ""),
        "last_result": state.get("last_result", {}),
        "next_run_at": state.get("next_run_at"),
        "last_login_check_at": state.get("last_login_check_at"),
        "last_login_check_ok": state.get("last_login_check_ok"),
        "last_login_check_result": state.get("last_login_check_result", {}),
        "pending_login_batch": state.get("pending_login_batch", {}),
        "stats_capture": {
            "enabled": bool(stats_settings.get("enabled")),
            "running": _STATS_RUNNING.is_set(),
            "batch_size": int(stats_settings.get("batch_size") or 5),
            "schedule_mode": str(stats_settings.get("schedule_mode") or "daily"),
            "daily_time": str(stats_settings.get("daily_time") or "08:30"),
            "last_started_at": state.get("stats_last_started_at"),
            "last_finished_at": state.get("stats_last_finished_at"),
            "last_ok": state.get("stats_last_ok"),
            "last_error": state.get("stats_last_error", ""),
            "last_result": state.get("stats_last_result", {}),
            "next_run_at": state.get("stats_next_run_at"),
        },
    }


def scheduler_status() -> dict[str, Any]:
    status = _base_status()
    status["thread_alive"] = bool(_THREAD and _THREAD.is_alive())
    return status


def _run_once(reason: str = "scheduled") -> dict[str, Any]:
    if _RUNNING.is_set():
        return {"ok": False, "skipped": True, "reason": "already_running"}
    _RUNNING.set()
    started = _now()
    state = _read_state()
    state.update(
        {
            "last_started_at": started,
            "last_finished_at": None,
            "last_ok": None,
            "last_error": "",
            "last_reason": reason,
        }
    )
    _write_state(state)
    try:
        result = run_matrix_publish()
        ok = bool(result.get("ok"))
        state.update(
            {
                "last_finished_at": _now(),
                "last_ok": ok,
                "last_error": "" if ok else json.dumps(result, ensure_ascii=False),
                "last_result": result,
            }
        )
        return {"ok": ok, "result": result}
    except Exception as exc:
        state.update(
            {
                "last_finished_at": _now(),
                "last_ok": False,
                "last_error": f"{exc}\n{traceback.format_exc()}",
                "last_result": {},
            }
        )
        return {"ok": False, "error": str(exc)}
    finally:
        state["next_run_at"] = _next_run_at(_job_settings())
        _write_state(state)
        _RUNNING.clear()


def _run_login_check_once(reason: str = "scheduled") -> dict[str, Any]:
    if _RUNNING.is_set():
        return {"ok": False, "skipped": True, "reason": "publish_running"}
    settings = _job_settings()
    batch_size = max(1, int(settings.get("login_check_batch_size") or 5))
    state = _read_state()
    try:
        result = check_wechat_matrix_login_status(batch_size=batch_size, notify=True)
    except Exception as exc:
        result = {"ok": False, "error": f"{exc}\n{traceback.format_exc()}"}
    state.update(
        {
            "last_login_check_at": _now(),
            "last_login_check_reason": reason,
            "last_login_check_ok": bool(result.get("ok")),
            "last_login_check_result": result,
            "pending_login_batch": result.get("login_batch") or {},
        }
    )
    _write_state(state)
    return result


def _run_stats_capture_once(reason: str = "scheduled") -> dict[str, Any]:
    if _RUNNING.is_set():
        return {"ok": False, "skipped": True, "reason": "publish_running"}
    if _STATS_RUNNING.is_set():
        return {"ok": False, "skipped": True, "reason": "stats_capture_running"}
    _STATS_RUNNING.set()
    settings = _stats_job_settings()
    state = _read_state()
    state.update(
        {
            "stats_last_started_at": _now(),
            "stats_last_finished_at": None,
            "stats_last_ok": None,
            "stats_last_error": "",
            "stats_last_reason": reason,
        }
    )
    _write_state(state)
    try:
        result = run_wechat_stats_capture(
            batch_size=max(1, int(settings.get("batch_size") or 5)),
            limit=max(0, int(settings.get("limit") or 0)),
            dry_run=False,
            notify=True,
        )
        ok = bool(result.get("ok"))
        state.update(
            {
                "stats_last_finished_at": _now(),
                "stats_last_ok": ok,
                "stats_last_error": "" if ok else json.dumps(result, ensure_ascii=False),
                "stats_last_result": result,
            }
        )
        return result
    except Exception as exc:
        result = {"ok": False, "error": f"{exc}\n{traceback.format_exc()}"}
        state.update(
            {
                "stats_last_finished_at": _now(),
                "stats_last_ok": False,
                "stats_last_error": result["error"],
                "stats_last_result": result,
            }
        )
        return result
    finally:
        state["stats_next_run_at"] = _next_run_at(_stats_job_settings())
        _write_state(state)
        _STATS_RUNNING.clear()


def trigger_matrix_wechat_job() -> dict[str, Any]:
    def _runner() -> None:
        _run_once(reason="manual")

    with _LOCK:
        if _RUNNING.is_set():
            return {"ok": False, "status": "already_running"}
        thread = threading.Thread(target=_runner, name="gasgx-matrix-wechat-manual", daemon=True)
        thread.start()
    return {"ok": True, "status": "started"}


def _scheduler_loop() -> None:
    while not _STOP.is_set():
        settings = _job_settings()
        stats_settings = _stats_job_settings()
        enabled = bool(settings.get("enabled"))
        stats_enabled = bool(stats_settings.get("enabled"))
        state = _read_state()
        next_run_at = int(state.get("next_run_at") or 0)
        stats_next_run_at = int(state.get("stats_next_run_at") or 0)
        last_login_check_at = int(state.get("last_login_check_at") or 0)
        now = _now()
        if not next_run_at:
            state["next_run_at"] = _next_run_at(settings, now=now)
            _write_state(state)
        elif enabled and now >= next_run_at and not _RUNNING.is_set():
            _run_once(reason="scheduled")
        elif enabled and (not last_login_check_at or now - last_login_check_at >= _login_check_interval_seconds(settings)):
            _run_login_check_once(reason="scheduled")
        if not stats_next_run_at:
            state["stats_next_run_at"] = _next_run_at(stats_settings, now=now)
            _write_state(state)
        elif stats_enabled and now >= stats_next_run_at and not _STATS_RUNNING.is_set():
            _run_stats_capture_once(reason="scheduled")
        _STOP.wait(10)


def trigger_matrix_wechat_login_check() -> dict[str, Any]:
    return _run_login_check_once(reason="manual")


def trigger_matrix_wechat_stats_capture(*, target_date: str = "", limit: int = 0, dry_run: bool = False) -> dict[str, Any]:
    def _runner() -> None:
        if _STATS_RUNNING.is_set():
            return
        _STATS_RUNNING.set()
        try:
            run_wechat_stats_capture(target_date=target_date, limit=limit, dry_run=dry_run, notify=True)
        finally:
            _STATS_RUNNING.clear()

    with _LOCK:
        if _STATS_RUNNING.is_set():
            return {"ok": False, "status": "already_running"}
        thread = threading.Thread(target=_runner, name="gasgx-matrix-wechat-stats-manual", daemon=True)
        thread.start()
    return {"ok": True, "status": "started"}


def start_scheduler() -> None:
    global _THREAD
    with _LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=_scheduler_loop, name="gasgx-matrix-scheduler", daemon=True)
        _THREAD.start()
