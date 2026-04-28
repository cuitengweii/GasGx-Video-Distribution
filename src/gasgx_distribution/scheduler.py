from __future__ import annotations

import json
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .matrix_publish import run_wechat_publish
from .paths import get_paths
from .public_settings import load_distribution_settings

_LOCK = threading.Lock()
_THREAD: threading.Thread | None = None
_STOP = threading.Event()
_RUNNING = threading.Event()


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


def _base_status() -> dict[str, Any]:
    settings = _job_settings()
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
        result = run_wechat_publish()
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
        enabled = bool(settings.get("enabled"))
        state = _read_state()
        next_run_at = int(state.get("next_run_at") or 0)
        now = _now()
        if not next_run_at:
            state["next_run_at"] = _next_run_at(settings, now=now)
            _write_state(state)
        elif enabled and now >= next_run_at and not _RUNNING.is_set():
            _run_once(reason="scheduled")
        _STOP.wait(10)


def start_scheduler() -> None:
    global _THREAD
    with _LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=_scheduler_loop, name="gasgx-matrix-scheduler", daemon=True)
        _THREAD.start()
