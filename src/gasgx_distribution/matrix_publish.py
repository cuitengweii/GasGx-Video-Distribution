from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import get_paths
from .public_settings import load_wechat_publish_settings, resolve_material_dir
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


def materials_video_dir() -> Path:
    return resolve_material_dir()


def state_path() -> Path:
    path = get_paths().runtime_root / "matrix_publish_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


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


def build_publish_plan(*, limit: int = 0) -> list[PublishPlanItem]:
    accounts = list_wechat_accounts()
    videos = list_candidate_videos()
    if limit > 0:
        accounts = accounts[:limit]
    count = min(len(accounts), len(videos))
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    root = get_paths().runtime_root / "matrix_publish_runs"
    plan: list[PublishPlanItem] = []
    for account, video in zip(accounts[:count], videos[:count]):
        account_key = str(account.get("account_key") or f"account-{account.get('id')}").strip()
        workspace = root / f"{timestamp}_{account_key}"
        plan.append(
            PublishPlanItem(
                account_id=int(account["id"]),
                account_key=account_key,
                display_name=str(account.get("display_name") or account_key),
                profile_dir=Path(str(account["wechat_profile_dir"])),
                source_video=video,
                workspace=workspace,
            )
        )
    return plan


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
                    "save_draft": str(settings.get("publish_mode") or "publish") == "draft",
                    "publish_now": str(settings.get("publish_mode") or "publish") != "draft",
                    "declare_original": bool(settings.get("declare_original")),
                    "publish_click_confirmed": False,
                    "upload_timeout": int(settings.get("upload_timeout") or 60),
                }
            },
        },
        "collection_name": str(settings.get("collection_name") or ""),
    }
    path = workspace / "matrix_wechat_publish_config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run_wechat_publish(*, limit: int = 0, dry_run: bool = False) -> dict[str, Any]:
    plan = build_publish_plan(limit=limit)
    settings = load_wechat_publish_settings()
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "settings": settings,
            "items": [
                {
                    "account_id": item.account_id,
                    "display_name": item.display_name,
                    "video": str(item.source_video),
                    "profile_dir": str(item.profile_dir),
                    "workspace": str(item.workspace),
                }
                for item in plan
            ],
        }

    state = _load_state()
    used = set(str(item) for item in state.get("used_videos", []))
    runs = list(state.get("runs", [])) if isinstance(state.get("runs"), list) else []
    results: list[dict[str, Any]] = []
    for item in plan:
        prepared = prepare_workspace(item)
        runtime_config = _runtime_config_for_wechat(settings, item.workspace)
        cmd = [
            sys.executable,
            "-m",
            "cybercar",
            "publish",
            "--profile",
            "cybertruck",
            "--platforms",
            "wechat",
            "--limit",
            "1",
            "--config",
            str(runtime_config),
            "--workspace",
            str(item.workspace),
            "--wechat-chrome-user-data-dir",
            str(item.profile_dir),
            "--collection-name",
            str(settings.get("collection_name") or ""),
            "--upload-timeout",
            str(int(settings.get("upload_timeout") or 60)),
        ]
        caption = str(settings.get("caption") or "").strip()
        if caption:
            cmd.extend(["--caption", caption])
        if bool(settings.get("declare_original")):
            cmd.append("--wechat-declare-original")
        if str(settings.get("publish_mode") or "publish") == "draft":
            cmd.append("--wechat-save-draft-only")
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        completed = subprocess.run(
            cmd,
            cwd=str(get_paths().repo_root),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        success = completed.returncode == 0
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
            "workspace": str(item.workspace),
            "returncode": completed.returncode,
            "success": success,
            "started_at": started,
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        results.append(record)
        runs.append(record)
        state["used_videos"] = sorted(used)
        state["runs"] = runs[-200:]
        _save_state(state)
    return {"ok": all(item["success"] for item in results), "count": len(results), "results": results}
