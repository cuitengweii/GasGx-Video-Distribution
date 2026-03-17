from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ..common.bot_notify import resolve_telegram_bot_settings
from ..support.config import load_default_profile_name, load_notify_config, load_telegram_config
from ..support.paths import apply_runtime_environment, get_paths
from . import legacy_worker
from .home import refresh_home_surface as _refresh_home_surface
from .transport import set_clickable_commands as _set_clickable_commands

_WORKER_READY_STATUSES = {"starting", "polling", "idle"}


def _contains_flag(argv: list[str], flag: str) -> bool:
    return any(str(item).strip() == flag for item in argv)


def build_worker_argv(passthrough: list[str] | None = None) -> list[str]:
    apply_runtime_environment()
    paths = get_paths()
    telegram_cfg = load_telegram_config()
    target = _resolve_target()
    argv = list(passthrough or [])
    if not _contains_flag(argv, "--repo-root"):
        argv = ["--repo-root", str(paths.repo_root), *argv]
    if not _contains_flag(argv, "--workspace"):
        argv = ["--workspace", str(paths.runtime_root), *argv]
    if not _contains_flag(argv, "--default-profile"):
        argv = ["--default-profile", load_default_profile_name(), *argv]
    if not _contains_flag(argv, "--poll-timeout-seconds"):
        argv = ["--poll-timeout-seconds", str(int(telegram_cfg.get("poll_timeout_seconds") or 10)), *argv]
    if not _contains_flag(argv, "--poll-interval-seconds"):
        argv = ["--poll-interval-seconds", str(int(telegram_cfg.get("poll_interval_seconds") or 0)), *argv]
    if not _contains_flag(argv, "--poll-network-failure-restart-threshold"):
        argv = [
            "--poll-network-failure-restart-threshold",
            str(int(telegram_cfg.get("restart_threshold") or 6)),
            *argv,
        ]
    if not _contains_flag(argv, "--telegram-bot-token"):
        bot_token = str(target.get("bot_token") or "").strip()
        if bot_token:
            argv = ["--telegram-bot-token", bot_token, *argv]
    if not _contains_flag(argv, "--telegram-chat-id"):
        chat_id = str(target.get("chat_id") or "").strip()
        if chat_id:
            argv = ["--telegram-chat-id", chat_id, *argv]
    return argv


def _resolve_target() -> dict[str, object]:
    apply_runtime_environment()
    paths = get_paths()
    notify_cfg = load_notify_config()
    telegram_cfg = load_telegram_config()
    resolved = resolve_telegram_bot_settings(
        {
            "registry_file": str(telegram_cfg.get("registry_file") or "").strip(),
            "timeout_seconds": int(telegram_cfg.get("poll_timeout_seconds") or 10) + 15,
        },
        env_prefix=str(notify_cfg.get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )
    return {
        "paths": paths,
        "telegram_config": telegram_cfg,
        "bot_token": str(resolved.get("bot_token") or "").strip(),
        "chat_id": str(resolved.get("chat_id") or "").strip(),
        "default_profile": load_default_profile_name(),
    }


def _runtime_worker_state_path(runtime_root: Path) -> Path:
    return (runtime_root / legacy_worker.DEFAULT_STATE_FILE).resolve()


def _runtime_worker_lock_path(runtime_root: Path) -> Path:
    return (runtime_root / legacy_worker.DEFAULT_POLLER_LOCK_DIR).resolve()


def _load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _worker_pid_is_running(pid: int) -> bool:
    try:
        return bool(legacy_worker._pid_is_running(int(pid)))
    except Exception:
        return False


def _load_worker_state(runtime_root: Path) -> dict[str, Any]:
    return _load_json_payload(_runtime_worker_state_path(runtime_root))


def _load_worker_lock_owner(runtime_root: Path) -> dict[str, Any]:
    return _load_json_payload(_runtime_worker_lock_path(runtime_root) / "owner.json")


def _terminate_process(pid: int, *, wait_seconds: float = 8.0) -> bool:
    clean_pid = int(pid or 0)
    if clean_pid <= 0 or not _worker_pid_is_running(clean_pid):
        return False
    try:
        os.kill(clean_pid, signal.SIGTERM)
    except Exception:
        pass
    deadline = time.time() + max(0.5, float(wait_seconds))
    while time.time() < deadline:
        if not _worker_pid_is_running(clean_pid):
            return True
        time.sleep(0.2)
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(clean_pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=max(3, int(wait_seconds)),
            )
        except Exception:
            pass
    deadline = time.time() + 3.0
    while time.time() < deadline:
        if not _worker_pid_is_running(clean_pid):
            return True
        time.sleep(0.2)
    return not _worker_pid_is_running(clean_pid)


def _terminate_existing_worker(runtime_root: Path) -> dict[str, Any]:
    terminated: list[int] = []
    candidates: list[int] = []
    lock_owner = _load_worker_lock_owner(runtime_root)
    state = _load_worker_state(runtime_root)
    for raw_pid in (lock_owner.get("pid"), state.get("pid")):
        try:
            pid = int(raw_pid or 0)
        except Exception:
            pid = 0
        if pid > 0 and pid not in candidates:
            candidates.append(pid)
    for pid in candidates:
        if _terminate_process(pid):
            terminated.append(pid)
    lock_dir = _runtime_worker_lock_path(runtime_root)
    if lock_dir.exists():
        try:
            legacy_worker.shutil.rmtree(lock_dir)
        except Exception:
            pass
    return {
        "terminated_pids": terminated,
        "previous_state_pid": int(state.get("pid") or 0),
        "previous_lock_pid": int(lock_owner.get("pid") or 0),
    }


def _build_worker_env(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_root = str((repo_root / "src").resolve())
    current = str(env.get("PYTHONPATH") or "").strip()
    env["PYTHONPATH"] = src_root if not current else os.pathsep.join([src_root, current])
    return env


def _spawn_worker_process(*, repo_root: Path, runtime_root: Path) -> dict[str, Any]:
    runtime_root.mkdir(parents=True, exist_ok=True)
    log_dir = (runtime_root / "logs").resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / "telegram_worker_latest.out.log"
    stderr_path = log_dir / "telegram_worker_latest.err.log"
    cmd = [sys.executable, "-m", "cybercar", "telegram", "worker"]
    creationflags = 0
    if os.name == "nt":
        creationflags |= int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        creationflags |= int(getattr(subprocess, "DETACHED_PROCESS", 0))
        creationflags |= int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    stdout_handle = stdout_path.open("a", encoding="utf-8")
    stderr_handle = stderr_path.open("a", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            env=_build_worker_env(repo_root),
            stdin=subprocess.DEVNULL,
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=creationflags,
        )
    finally:
        stdout_handle.close()
        stderr_handle.close()
    return {
        "pid": int(proc.pid or 0),
        "stdout_log": str(stdout_path),
        "stderr_log": str(stderr_path),
        "command": cmd,
    }


def _wait_for_worker_ready(runtime_root: Path, *, timeout_seconds: float = 20.0) -> dict[str, Any]:
    deadline = time.time() + max(3.0, float(timeout_seconds))
    last_state: dict[str, Any] = {}
    while time.time() < deadline:
        state = _load_worker_state(runtime_root)
        if state:
            last_state = state
        pid = int(state.get("pid") or 0)
        status = str(state.get("status") or "").strip().lower()
        if pid > 0 and status in _WORKER_READY_STATUSES and _worker_pid_is_running(pid):
            return state
        time.sleep(0.5)
    raise RuntimeError(f"telegram worker did not become ready in time: {last_state}")


def _restart_worker_process(*, repo_root: Path, runtime_root: Path, startup_timeout_seconds: float) -> dict[str, Any]:
    stopped = _terminate_existing_worker(runtime_root)
    spawned = _spawn_worker_process(repo_root=repo_root, runtime_root=runtime_root)
    state = _wait_for_worker_ready(runtime_root, timeout_seconds=startup_timeout_seconds)
    return {
        **stopped,
        **spawned,
        "state": state,
    }


def worker_main(passthrough: list[str] | None = None) -> int:
    argv = build_worker_argv(passthrough)
    old_argv = sys.argv
    sys.argv = [old_argv[0], *argv]
    try:
        return int(legacy_worker.main())
    finally:
        sys.argv = old_argv


def set_clickable_commands() -> dict[str, object]:
    target = _resolve_target()
    bot_token = str(target.get("bot_token") or "")
    if not bot_token:
        raise RuntimeError("telegram bot token is not configured")
    paths = target["paths"]
    assert isinstance(paths, object)
    log_file = get_paths().log_dir / f"telegram_worker_{time.strftime('%Y%m%d')}.log"
    _set_clickable_commands(
        bot_token=bot_token,
        timeout_seconds=max(10, int(target["telegram_config"].get("poll_timeout_seconds") or 10) + 5),  # type: ignore[index]
        log_file=log_file,
    )
    return {"ok": True, "log_file": str(log_file)}


def refresh_home_surface() -> dict[str, object]:
    target = _resolve_target()
    bot_token = str(target.get("bot_token") or "")
    chat_id = str(target.get("chat_id") or "")
    if not bot_token or not chat_id:
        raise RuntimeError("telegram bot token/chat_id is not configured")
    paths = get_paths()
    log_file = paths.log_dir / f"telegram_worker_{time.strftime('%Y%m%d')}.log"
    telegram_cfg = target["telegram_config"]
    _refresh_home_surface(
        bot_token=bot_token,
        chat_id=chat_id,
        workspace=paths.runtime_root,
        timeout_seconds=max(30, int(telegram_cfg.get("poll_timeout_seconds") or 10) + 20),  # type: ignore[index]
        log_file=log_file,
        default_profile=str(target.get("default_profile") or "cybertruck"),
    )
    return {"ok": True, "log_file": str(log_file)}


def recover_bot_surface(*, retries: int = 3, retry_delay_seconds: float = 2.0) -> dict[str, object]:
    target = _resolve_target()
    bot_token = str(target.get("bot_token") or "")
    chat_id = str(target.get("chat_id") or "")
    if not bot_token or not chat_id:
        raise RuntimeError("telegram bot token/chat_id is not configured")
    paths = get_paths()
    log_file = paths.log_dir / f"telegram_worker_{time.strftime('%Y%m%d')}.log"
    telegram_cfg = target["telegram_config"]
    timeout_seconds = max(30, int(telegram_cfg.get("poll_timeout_seconds") or 10) + 20)  # type: ignore[index]
    attempts = max(1, int(retries))
    last_error: Exception | None = None
    restart_summary = _restart_worker_process(
        repo_root=paths.repo_root,
        runtime_root=paths.runtime_root,
        startup_timeout_seconds=max(15.0, float(timeout_seconds)),
    )

    for attempt in range(1, attempts + 1):
        try:
            _set_clickable_commands(
                bot_token=bot_token,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
            )
            _refresh_home_surface(
                bot_token=bot_token,
                chat_id=chat_id,
                workspace=paths.runtime_root,
                timeout_seconds=timeout_seconds,
                log_file=log_file,
                default_profile=str(target.get("default_profile") or "cybertruck"),
            )
            return {
                "ok": True,
                "attempts": attempt,
                "log_file": str(log_file),
                "chat_id": chat_id,
                "worker_pid": int(restart_summary.get("pid") or 0),
                "terminated_pids": list(restart_summary.get("terminated_pids") or []),
                "stdout_log": str(restart_summary.get("stdout_log") or ""),
                "stderr_log": str(restart_summary.get("stderr_log") or ""),
            }
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(max(0.5, float(retry_delay_seconds)))

    raise RuntimeError(f"telegram recover failed after {attempts} attempts: {last_error}") from last_error
