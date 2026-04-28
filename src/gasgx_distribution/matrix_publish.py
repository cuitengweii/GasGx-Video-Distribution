from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import get_paths
from .public_settings import load_distribution_settings, load_wechat_publish_settings, resolve_material_dir
from .service import list_accounts

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}


@dataclass(frozen=True)
class PublishPlanItem:
    account_id: int
    account_key: str
    display_name: str
    profile_dir: Path
    source_video: Path
    workspace: Path
    batch_index: int
    batch_position: int


def materials_video_dir() -> Path:
    return resolve_material_dir()


def state_path() -> Path:
    path = get_paths().runtime_root / "matrix_publish_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def publish_lock_path() -> Path:
    path = get_paths().runtime_root / "matrix_publish.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _account_debug_port(account_id: int) -> int:
    return 9400 + max(1, int(account_id or 1))


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_publish_lock() -> dict[str, Any]:
    path = publish_lock_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _acquire_publish_lock() -> tuple[bool, dict[str, Any]]:
    path = publish_lock_path()
    existing = _read_publish_lock()
    existing_pid = int(existing.get("pid") or 0)
    if existing and _pid_is_running(existing_pid):
        return False, existing
    if existing:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    payload = {"pid": os.getpid(), "started_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    try:
        handle = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False, _read_publish_lock()
    with os.fdopen(handle, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    return True, payload


def _release_publish_lock() -> None:
    try:
        publish_lock_path().unlink()
    except FileNotFoundError:
        pass


def _load_state() -> dict[str, Any]:
    path = state_path()
    if not path.exists():
        return {"used_videos": [], "runs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {"used_videos": [], "runs": []}
    return payload if isinstance(payload, dict) else {"used_videos": [], "runs": []}


def _save_state(payload: dict[str, Any]) -> None:
    state_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _video_key(path: Path) -> str:
    stat = path.stat()
    return f"{path.name}|{stat.st_size}|{int(stat.st_mtime)}"


def list_candidate_videos(*, include_used: bool = False) -> list[Path]:
    used = set(str(item) for item in _load_state().get("used_videos", []))
    videos = [
        path
        for path in materials_video_dir().iterdir()
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS
    ]
    videos.sort(key=lambda item: (item.stat().st_mtime, item.name), reverse=True)
    if include_used:
        return videos
    return [path for path in videos if _video_key(path) not in used]


def _wechat_profile(platforms: list[dict[str, Any]]) -> Path | None:
    for platform in platforms:
        if platform.get("platform") == "wechat":
            raw = str(platform.get("profile_dir") or "").strip()
            if raw:
                return Path(raw)
    return None


def list_wechat_accounts() -> list[dict[str, Any]]:
    accounts = [account for account in list_accounts() if str(account.get("status") or "") == "active"]
    result: list[dict[str, Any]] = []
    for account in accounts:
        profile_dir = _wechat_profile(account.get("platforms") or [])
        if profile_dir is None:
            continue
        result.append({**account, "wechat_profile_dir": str(profile_dir)})
    result.sort(key=lambda item: int(item.get("id") or 0))
    return result


def _matrix_wechat_job_settings() -> dict[str, Any]:
    return dict(load_distribution_settings().get("jobs", {}).get("matrix_wechat_publish", {}))


def _last_successful_account_id() -> int:
    runs = _load_state().get("runs", [])
    if not isinstance(runs, list):
        return 0
    for item in reversed(runs):
        if not isinstance(item, dict) or not bool(item.get("success")):
            continue
        try:
            return int(item.get("account_id") or 0)
        except Exception:
            return 0
    return 0


def _rotate_accounts_after_last_success(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(accounts) <= 1:
        return accounts
    last_account_id = _last_successful_account_id()
    if last_account_id <= 0:
        return accounts
    for index, account in enumerate(accounts):
        if int(account.get("id") or 0) == last_account_id:
            start = (index + 1) % len(accounts)
            return accounts[start:] + accounts[:start]
    return accounts


def _rotate_batches(accounts: list[dict[str, Any]], batch_size: int, *, enabled: bool) -> list[list[dict[str, Any]]]:
    batches = [accounts[index : index + batch_size] for index in range(0, len(accounts), batch_size)]
    if not enabled or len(batches) <= 1:
        return batches
    day_index = int(time.strftime("%j"))
    start = day_index % len(batches)
    return batches[start:] + batches[:start]


def _ordered_account_batches(accounts: list[dict[str, Any]], settings: dict[str, Any]) -> list[list[dict[str, Any]]]:
    batch_size = max(1, int(settings.get("batch_size") or 5))
    if bool(settings.get("rotate_start_group", True)):
        accounts = _rotate_accounts_after_last_success(accounts)
    batches = _rotate_batches(accounts, batch_size, enabled=bool(settings.get("rotate_start_group", True)))
    if bool(settings.get("shuffle_within_batch", True)):
        seed = f"{time.strftime('%Y-%m-%d')}:{len(accounts)}:{batch_size}:{len(_load_state().get('runs', []))}"
        rng = random.Random(seed)
        for batch in batches:
            first = batch[:1]
            rest = batch[1:]
            rng.shuffle(rest)
            batch[:] = first + rest
    return batches


def build_publish_plan(*, limit: int = 0) -> list[PublishPlanItem]:
    accounts = list_wechat_accounts()
    settings = _matrix_wechat_job_settings()
    videos = list_candidate_videos()
    if limit > 0:
        accounts = accounts[:limit]
    account_batches = _ordered_account_batches(accounts, settings)
    ordered_accounts = [account for batch in account_batches for account in batch]
    count = min(len(ordered_accounts), len(videos))
    account_batch_lookup = {
        int(account["id"]): (batch_index, batch_position)
        for batch_index, batch in enumerate(account_batches, start=1)
        for batch_position, account in enumerate(batch, start=1)
    }
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    root = get_paths().runtime_root / "matrix_publish_runs"
    plan: list[PublishPlanItem] = []
    for account, video in zip(ordered_accounts[:count], videos[:count]):
        account_key = str(account.get("account_key") or f"account-{account.get('id')}").strip()
        workspace = root / f"{timestamp}_{account_key}"
        batch_index, batch_position = account_batch_lookup.get(int(account["id"]), (1, len(plan) + 1))
        plan.append(
            PublishPlanItem(
                account_id=int(account["id"]),
                account_key=account_key,
                display_name=str(account.get("display_name") or account_key),
                profile_dir=Path(str(account["wechat_profile_dir"])),
                source_video=video,
                workspace=workspace,
                batch_index=batch_index,
                batch_position=batch_position,
            )
        )
    return plan


def plan_batches(plan: list[PublishPlanItem]) -> list[list[PublishPlanItem]]:
    grouped: dict[int, list[PublishPlanItem]] = {}
    for item in plan:
        grouped.setdefault(item.batch_index, []).append(item)
    return [sorted(items, key=lambda item: item.batch_position) for _, items in sorted(grouped.items())]


def prepare_workspace(item: PublishPlanItem) -> Path:
    processed = item.workspace / "2_Processed"
    processed.mkdir(parents=True, exist_ok=True)
    target = processed / item.source_video.name
    shutil.copy2(item.source_video, target)
    return target


def _runtime_config_for_wechat(settings: dict[str, Any], workspace: Path) -> Path:
    config = {
        "paths": {
            "runtime_root": str(workspace),
            "profiles_root": "profiles",
            "default_profile_dir": "default",
            "wechat_profile_dir": "wechat",
            "x_profile_dir": "x_collect",
        },
        "publish": {
            "default_platforms": "wechat",
            "platforms": {
                "wechat": {
                    "collection_name": str(settings.get("collection_name") or ""),
                    "short_title": str(settings.get("short_title") or "GasGx"),
                    "location": str(settings.get("location") or ""),
                    "save_draft": str(settings.get("publish_mode") or "publish") == "draft",
                    "publish_now": str(settings.get("publish_mode") or "publish") != "draft",
                    "declare_original": bool(settings.get("declare_original")),
                    "publish_click_confirmed": False,
                    "upload_timeout": int(settings.get("upload_timeout") or 60),
                }
            },
        },
        "collection_name": str(settings.get("collection_name") or ""),
        "short_title": str(settings.get("short_title") or "GasGx"),
        "location": str(settings.get("location") or ""),
        "topics": str(settings.get("topics") or ""),
    }
    path = workspace / "matrix_wechat_publish_config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _caption_with_topics(settings: dict[str, Any]) -> str:
    caption = str(settings.get("caption") or "").strip()
    topics = str(settings.get("topics") or "").strip()
    if caption and topics:
        return f"{caption}\n{topics}"
    return caption or topics


def _wechat_publish_evidence_path(workspace: Path) -> Path:
    return workspace / "uploaded_records_wechat.jsonl"


def _has_wechat_publish_evidence(workspace: Path) -> bool:
    evidence = _wechat_publish_evidence_path(workspace)
    return evidence.exists() and evidence.stat().st_size > 0


def run_wechat_publish(*, limit: int = 0, dry_run: bool = False) -> dict[str, Any]:
    plan = build_publish_plan(limit=limit)
    job_settings = _matrix_wechat_job_settings()
    settings = load_wechat_publish_settings()
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "settings": settings,
            "job_settings": job_settings,
            "batches": [
                [
                    {
                        "account_id": item.account_id,
                        "account_key": item.account_key,
                        "display_name": item.display_name,
                        "video": str(item.source_video),
                        "profile_dir": str(item.profile_dir),
                        "workspace": str(item.workspace),
                        "batch_index": item.batch_index,
                        "batch_position": item.batch_position,
                    }
                    for item in batch
                ]
                for batch in plan_batches(plan)
            ],
            "items": [
                {
                    "account_id": item.account_id,
                    "account_key": item.account_key,
                    "display_name": item.display_name,
                    "video": str(item.source_video),
                    "profile_dir": str(item.profile_dir),
                    "workspace": str(item.workspace),
                    "batch_index": item.batch_index,
                    "batch_position": item.batch_position,
                }
                for item in plan
            ],
        }

    locked, lock_payload = _acquire_publish_lock()
    if not locked:
        return {"ok": False, "skipped": True, "reason": "publish_lock_active", "lock": lock_payload}

    state = _load_state()
    used = set(str(item) for item in state.get("used_videos", []))
    runs = list(state.get("runs", [])) if isinstance(state.get("runs"), list) else []
    results: list[dict[str, Any]] = []
    try:
        for item in plan:
            debug_port = _account_debug_port(item.account_id)
            prepared = prepare_workspace(item)
            runtime_config = _runtime_config_for_wechat(settings, item.workspace)
            cmd = [
                sys.executable,
                "-m",
                "cybercar.pipeline",
                "--publish-only",
                "--upload-platforms",
                "wechat",
                "--limit",
                "1",
                "--config",
                str(runtime_config),
                "--workspace",
                str(item.workspace),
                "--debug-port",
                str(debug_port),
                "--wechat-debug-port",
                str(debug_port),
                "--chrome-user-data-dir",
                str(item.profile_dir),
                "--wechat-chrome-user-data-dir",
                str(item.profile_dir),
                "--collection-name",
                str(settings.get("collection_name") or ""),
                "--upload-timeout",
                str(int(settings.get("upload_timeout") or 60)),
            ]
            caption = _caption_with_topics(settings)
            if caption:
                cmd.extend(["--caption", caption])
            if bool(settings.get("declare_original")):
                cmd.append("--wechat-declare-original")
            if str(settings.get("publish_mode") or "publish") == "draft":
                cmd.append("--wechat-save-draft-only")
            else:
                cmd.append("--wechat-publish-now")
            started = time.strftime("%Y-%m-%d %H:%M:%S")
            log_path = item.workspace / "matrix_wechat_publish.log"
            with log_path.open("w", encoding="utf-8", errors="replace") as log_file:
                completed = subprocess.run(
                    cmd,
                    cwd=str(get_paths().repo_root),
                    env={**os.environ, "CYBERCAR_DISABLE_REQUIRED_HASHTAGS": "1"},
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
            evidence_ok = _has_wechat_publish_evidence(item.workspace)
            success = completed.returncode == 0 and evidence_ok
            video_key = _video_key(item.source_video)
            if success:
                used.add(video_key)
            record = {
                "account_id": item.account_id,
                "account_key": item.account_key,
                "display_name": item.display_name,
                "video": str(item.source_video),
                "prepared_video": str(prepared),
                "profile_dir": str(item.profile_dir),
                "debug_port": debug_port,
                "workspace": str(item.workspace),
                "returncode": completed.returncode,
                "success": success,
                "evidence_ok": evidence_ok,
                "log": str(log_path),
                "started_at": started,
                "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if completed.returncode == 0 and not evidence_ok:
                record["error"] = "wechat publish returned 0 but no uploaded_records_wechat.jsonl evidence was written"
            results.append(record)
            runs.append(record)
            state["used_videos"] = sorted(used)
            state["runs"] = runs[-200:]
            _save_state(state)
        return {"ok": all(item["success"] for item in results), "count": len(results), "results": results}
    finally:
        _release_publish_lock()
