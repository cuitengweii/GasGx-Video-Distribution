from __future__ import annotations

import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from pathlib import Path
from typing import Any

from .paths import get_paths
from .public_settings import (
    load_distribution_settings,
    load_platform_publish_settings,
    load_wechat_publish_settings,
    resolve_material_dir,
)
from . import service
from .platforms import get_platform

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}


@dataclass(frozen=True)
class PublishPlanItem:
    account_id: int
    account_key: str
    display_name: str
    profile_dir: Path
    debug_port: int
    fingerprint: dict[str, Any]
    source_video: Path
    workspace: Path
    batch_index: int
    batch_position: int
    platform: str = "wechat"
    publish_date: str = ""


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
        return {"consumed": [], "runs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {"consumed": [], "runs": []}
    return payload if isinstance(payload, dict) else {"consumed": [], "runs": []}


def _save_state(payload: dict[str, Any]) -> None:
    state_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _video_key(path: Path) -> str:
    stat = path.stat()
    return f"{path.name}|{stat.st_size}|{int(stat.st_mtime)}"


def _load_timezone() -> ZoneInfo:
    raw = str(load_distribution_settings().get("common", {}).get("timezone") or "").strip()
    if raw:
        try:
            return ZoneInfo(raw)
        except ZoneInfoNotFoundError:
            pass
    return datetime.now().astimezone().tzinfo or timezone.utc


def _today_date(tz: ZoneInfo) -> date:
    return datetime.now(tz).date()


def _parse_yyyymmdd(token: str) -> date | None:
    if len(token) != 8 or not token.isdigit():
        return None
    try:
        return date(int(token[0:4]), int(token[4:6]), int(token[6:8]))
    except ValueError:
        return None


def _manifest_created_day(manifest_path: Path, tz: ZoneInfo) -> date | None:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    raw = str(payload.get("created_at") or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz).date()


def asset_day(path: Path, tz: ZoneInfo | None = None) -> date:
    active_tz = tz or _load_timezone()
    parent_name = path.parent.name
    if parent_name and parent_name[:8].isdigit() and len(parent_name) >= 9 and parent_name[8] == "_":
        parsed = _parse_yyyymmdd(parent_name[:8])
        if parsed is not None:
            return parsed
    manifest_path = path.with_name(f"{path.stem}_manifest.json")
    if manifest_path.exists():
        manifest_day = _manifest_created_day(manifest_path, active_tz)
        if manifest_day is not None:
            return manifest_day
    return datetime.fromtimestamp(path.stat().st_mtime, active_tz).date()


def _asset_manifest_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}_manifest.json")


def _asset_manifest_metadata(path: Path) -> dict[str, Any]:
    manifest_path = _asset_manifest_path(path)
    if not manifest_path.exists():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    dedupe = payload.get("dedupe") if isinstance(payload.get("dedupe"), dict) else {}
    return {
        "narrative_template_id": str(payload.get("narrative_template_id") or dedupe.get("narrative_template_id") or "").strip(),
        "account_pool_id": str(payload.get("account_pool_id") or dedupe.get("account_pool_id") or "").strip(),
        "text_signature": str(dedupe.get("text_signature") or "").strip(),
        "first_frame_hash": str(dedupe.get("first_frame_hash") or "").strip(),
        "cover_frame_hash": str(dedupe.get("cover_frame_hash") or "").strip(),
        "content_fingerprint": str(dedupe.get("content_fingerprint") or "").strip(),
        "bgm_fingerprint": str(dedupe.get("bgm_fingerprint") or "").strip(),
        "bgm_name": str(payload.get("bgm_name") or "").strip(),
    }


def _last_account_asset_metadata(account_id: int) -> dict[str, Any]:
    for item in reversed(_consumed_records()):
        try:
            if int(item.get("account_id") or 0) != int(account_id):
                continue
        except Exception:
            continue
        metadata = item.get("asset_metadata") if isinstance(item.get("asset_metadata"), dict) else {}
        if metadata:
            return metadata
    return {}


def _asset_allowed_for_account(path: Path, account_id: int) -> bool:
    previous = _last_account_asset_metadata(account_id)
    if not previous:
        return True
    current = _asset_manifest_metadata(path)
    if not current:
        return True
    guarded_keys = ("first_frame_hash", "bgm_name", "narrative_template_id", "text_signature")
    for key in guarded_keys:
        if current.get(key) and current.get(key) == previous.get(key):
            return False
    return True


def _relative_asset_key(path: Path) -> str:
    root = materials_video_dir()
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path.resolve())


def _consumed_records() -> list[dict[str, Any]]:
    state = _load_state()
    records = state.get("consumed", [])
    if isinstance(records, list):
        return [item for item in records if isinstance(item, dict)]
    return []


def _consumed_index(today: str | None = None) -> set[tuple[str, int, str, str]]:
    result: set[tuple[str, int, str, str]] = set()
    for item in _consumed_records():
        asset_key = str(item.get("asset_key") or "").strip()
        account_id = int(item.get("account_id") or 0)
        platform = str(item.get("platform") or "").strip()
        publish_date = str(item.get("publish_date") or "").strip()
        if asset_key and account_id > 0 and platform and publish_date and (today is None or publish_date == today):
            result.add((asset_key, account_id, platform, publish_date))
    return result


def _consumed_slots(today: str) -> set[tuple[int, str, str]]:
    slots: set[tuple[int, str, str]] = set()
    for item in _consumed_records():
        publish_date = str(item.get("publish_date") or "").strip()
        if publish_date != today:
            continue
        account_id = int(item.get("account_id") or 0)
        platform = str(item.get("platform") or "").strip()
        if account_id > 0 and platform:
            slots.add((account_id, platform, publish_date))
    return slots


def _consumed_asset_locks(today: str) -> dict[int, str]:
    """Return today's locked asset per account (latest record wins)."""
    locks: dict[int, str] = {}
    for item in _consumed_records():
        publish_date = str(item.get("publish_date") or "").strip()
        if publish_date != today:
            continue
        account_id = int(item.get("account_id") or 0)
        asset_key = str(item.get("asset_key") or "").strip()
        if account_id > 0 and asset_key:
            locks[account_id] = asset_key
    return locks


def _scan_material_candidates(root: Path) -> list[Path]:
    videos: list[Path] = []
    for path in root.iterdir():
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
            videos.append(path)
            continue
        if path.is_dir():
            for child in path.iterdir():
                if child.is_file() and child.suffix.lower() in VIDEO_EXTENSIONS:
                    videos.append(child)
    return videos


def list_candidate_videos(*, include_used: bool = False) -> list[Path]:
    tz = _load_timezone()
    today = _today_date(tz)
    videos = _scan_material_candidates(materials_video_dir())
    today_videos = [path for path in videos if asset_day(path, tz) == today]
    today_videos.sort(key=lambda item: (item.stat().st_mtime, item.name), reverse=True)
    return today_videos


def _account_platform_slots(accounts: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    slots: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for account in accounts:
        for platform in sorted(account.get("platforms") or [], key=lambda item: str(item.get("platform") or "")):
            token = str(platform.get("platform") or "").strip()
            cap = get_platform(token)
            if not cap or not cap.can_publish or not bool(platform.get("enabled", True)):
                continue
            slots.append((account, platform))
    return slots


def _wechat_profile(platforms: list[dict[str, Any]]) -> Path | None:
    for platform in platforms:
        if platform.get("platform") == "wechat":
            raw = str(platform.get("profile_dir") or "").strip()
            if raw:
                return Path(raw)
    return None


def list_wechat_accounts() -> list[dict[str, Any]]:
    accounts = [account for account in service.list_accounts() if str(account.get("status") or "") == "active"]
    result: list[dict[str, Any]] = []
    for account in accounts:
        profile_dir = _wechat_profile(account.get("platforms") or [])
        if profile_dir is None:
            continue
        platform = next((item for item in account.get("platforms") or [] if item.get("platform") == "wechat"), {})
        result.append(
            {
                **account,
                "wechat_profile_dir": str(profile_dir),
                "wechat_debug_port": int(platform.get("debug_port") or 0),
                "wechat_fingerprint": platform.get("fingerprint") or {},
            }
        )
    result.sort(key=lambda item: int(item.get("id") or 0))
    return result


def list_active_publish_accounts() -> list[dict[str, Any]]:
    accounts = [account for account in service.list_accounts() if str(account.get("status") or "") == "active"]
    result: list[dict[str, Any]] = []
    for account in accounts:
        platforms = [item for item in account.get("platforms") or [] if bool(item.get("enabled", True))]
        if not platforms:
            continue
        result.append({**account, "platforms": platforms})
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
    accounts = list_active_publish_accounts()
    settings = _matrix_wechat_job_settings()
    videos = list_candidate_videos()
    if limit > 0:
        accounts = accounts[:limit]
    account_batches = _ordered_account_batches(accounts, settings)
    ordered_accounts = [account for batch in account_batches for account in batch]
    slots = _account_platform_slots(ordered_accounts)
    today = _today_date(_load_timezone()).isoformat()
    consumed = _consumed_slots(today)
    locked_assets = _consumed_asset_locks(today)
    pending_by_account: dict[int, list[dict[str, Any]]] = {}
    for account, platform in slots:
        account_id = int(account.get("id") or 0)
        key = (account_id, str(platform.get("platform") or ""), today)
        if key in consumed:
            continue
        pending_by_account.setdefault(account_id, []).append(platform)
    video_by_key = {_relative_asset_key(path): path for path in videos}
    assigned_video_by_account: dict[int, Path] = {}
    assigned_asset_keys: set[str] = set()
    materials_root = materials_video_dir()
    for account in ordered_accounts:
        account_id = int(account.get("id") or 0)
        if not pending_by_account.get(account_id):
            continue
        locked_key = str(locked_assets.get(account_id) or "").strip()
        if locked_key:
            locked_path = video_by_key.get(locked_key)
            if locked_path is None:
                candidate = materials_root / locked_key
                if candidate.exists() and candidate.is_file():
                    locked_path = candidate
            if locked_path is None:
                continue
            assigned_video_by_account[account_id] = locked_path
            assigned_asset_keys.add(locked_key)
            continue
        selected: Path | None = None
        for path in videos:
            asset_key = _relative_asset_key(path)
            if asset_key in assigned_asset_keys:
                continue
            if not _asset_allowed_for_account(path, account_id):
                continue
            selected = path
            assigned_asset_keys.add(asset_key)
            break
        if selected is None:
            break
        assigned_video_by_account[account_id] = selected
    account_batch_lookup = {
        int(account["id"]): (batch_index, batch_position)
        for batch_index, batch in enumerate(account_batches, start=1)
        for batch_position, account in enumerate(batch, start=1)
    }
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    root = get_paths().runtime_root / "matrix_publish_runs"
    plan: list[PublishPlanItem] = []
    for account in ordered_accounts:
        account_id = int(account.get("id") or 0)
        platforms = pending_by_account.get(account_id) or []
        video = assigned_video_by_account.get(account_id)
        if not platforms or video is None:
            continue
        account_key = str(account.get("account_key") or f"account-{account.get('id')}").strip()
        workspace = root / f"{timestamp}_{account_key}"
        batch_index, batch_position = account_batch_lookup.get(int(account["id"]), (1, len(plan) + 1))
        for platform in platforms:
            plan.append(
                PublishPlanItem(
                    account_id=int(account["id"]),
                    account_key=account_key,
                    display_name=str(account.get("display_name") or account_key),
                    profile_dir=Path(str(platform.get("profile_dir") or account.get("wechat_profile_dir") or "")),
                    debug_port=int(platform.get("debug_port") or account.get("wechat_debug_port") or 0),
                    fingerprint=dict(platform.get("fingerprint") or account.get("wechat_fingerprint") or {}),
                    source_video=video,
                    workspace=workspace,
                    batch_index=batch_index,
                    batch_position=batch_position,
                    platform=str(platform.get("platform") or "wechat"),
                    publish_date=today,
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
                    "short_title": str(settings.get("short_title") or "GasGx燃气发电挖矿"),
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
        "short_title": str(settings.get("short_title") or "GasGx燃气发电挖矿"),
        "location": str(settings.get("location") or ""),
        "topics": str(settings.get("topics") or ""),
    }
    path = workspace / "matrix_wechat_publish_config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _runtime_config_for_non_wechat_platform(platform: str, settings: dict[str, Any], workspace: Path) -> Path:
    token = str(platform or "").strip().lower()
    draft = str(settings.get("publish_mode") or "publish") == "draft"
    platform_block: dict[str, Any] = {
        "save_draft": draft,
        "publish_now": not draft,
        "upload_timeout": int(settings.get("upload_timeout") or 60),
    }
    collection_name = str(settings.get("collection_name") or "").strip()
    if collection_name:
        platform_block["collection_name"] = collection_name
    config = {
        "paths": {
            "runtime_root": str(workspace),
            "profiles_root": "profiles",
            "default_profile_dir": "default",
            "wechat_profile_dir": "wechat",
            "x_profile_dir": "x_collect",
        },
        "publish": {"platforms": {token: platform_block}},
        "topics": str(settings.get("topics") or ""),
    }
    path = workspace / f"matrix_{token}_publish_config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _runtime_publish_config(platform: str, settings: dict[str, Any], workspace: Path) -> Path:
    if str(platform or "").strip().lower() == "wechat":
        return _runtime_config_for_wechat(settings, workspace)
    return _runtime_config_for_non_wechat_platform(platform, settings, workspace)


def _caption_with_topics(settings: dict[str, Any]) -> str:
    caption = str(settings.get("caption") or "").strip()
    topics = str(settings.get("topics") or "").strip()
    if caption and topics:
        return f"{caption}\n{topics}"
    return caption or topics


def _asset_generated_caption(source_video: Path) -> str:
    candidates: list[Path] = [source_video.with_name(f"{source_video.stem}_copy.txt")]
    manifest_path = _asset_manifest_path(source_video)
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            raw_copy_path = str(payload.get("copy_path") or "").strip()
            if raw_copy_path:
                copy_path = Path(raw_copy_path)
                if not copy_path.is_absolute():
                    copy_path = manifest_path.parent / copy_path
                candidates.append(copy_path)
    for path in candidates:
        try:
            if path.exists() and path.is_file():
                text = path.read_text(encoding="utf-8", errors="replace").strip()
                if text:
                    return text
        except Exception:
            continue
    return ""


def _sanitize_generated_caption(text: str) -> str:
    content = str(text or "").replace("\r\n", "\n")
    # Drop template-only blocks that should not appear in publish descriptions.
    content = re.sub(r"(?ims)^\s*片尾文案\s*:\s*\n(?:.+\n?)*?(?=^\s*HUD\s*:|^\s*$)", "", content)
    content = re.sub(r"(?ims)^\s*HUD\s*:\s*\n(?:.+\n?)*?(?=^\s*$|$)", "", content)
    lines = [line.strip() for line in content.split("\n") if line.strip()]
    return "\n".join(lines).strip()


def caption_for_publish(settings: dict[str, Any], source_video: Path) -> str:
    configured_caption = str(settings.get("caption") or "").strip()
    generated_caption = _sanitize_generated_caption(_asset_generated_caption(source_video))
    topics = str(settings.get("topics") or "").strip()
    base = configured_caption or generated_caption
    if base and topics:
        return f"{base}\n{topics}"
    return base or topics


def _publish_evidence_path(workspace: Path, platform: str) -> Path:
    token = str(platform or "wechat").strip().lower() or "wechat"
    return workspace / f"uploaded_records_{token}.jsonl"


def _has_publish_evidence(workspace: Path, platform: str) -> bool:
    evidence = _publish_evidence_path(workspace, platform)
    return evidence.exists() and evidence.stat().st_size > 0


def _has_wechat_publish_evidence(workspace: Path) -> bool:
    return _has_publish_evidence(workspace, "wechat")


def check_wechat_matrix_login_status(
    *,
    batch_size: int = 5,
    account_ids: list[int] | None = None,
    notify: bool = True,
) -> dict[str, Any]:
    accounts = list_wechat_accounts()
    if account_ids is not None:
        wanted = {int(item) for item in account_ids}
        accounts = [item for item in accounts if int(item.get("id") or 0) in wanted]
    else:
        accounts = sorted(
            accounts,
            key=lambda item: (
                int(next((platform.get("last_checked_at") or 0 for platform in item.get("platforms", []) if platform.get("platform") == "wechat"), 0)),
                int(item.get("id") or 0),
            ),
        )[: max(1, int(batch_size or 5))]
    results: list[dict[str, Any]] = []
    for account in accounts:
        account_id = int(account.get("id") or 0)
        try:
            result = service.check_login_status(account_id, "wechat")
        except Exception as exc:
            result = {
                "ok": False,
                "status": "unknown",
                "account_id": account_id,
                "account_key": account.get("account_key"),
                "display_name": account.get("display_name"),
                "error": str(exc),
            }
        results.append(result)
    login_required = [
        item
        for item in results
        if str(item.get("status") or "").lower() in {"login_required", "required"}
        or bool(item.get("needs_login"))
        or str(item.get("reason") or "").lower() == "login_url"
    ]
    batch = service.record_wechat_login_qr_batch(login_required, notify=notify) if login_required else None
    return {
        "ok": not login_required,
        "checked": len(results),
        "login_required_count": len(login_required),
        "results": results,
        "login_batch": batch,
    }


def _login_status_blocks_publish(result: dict[str, Any]) -> bool:
    status = str(result.get("status") or "").strip().lower()
    if status == "unsupported":
        return True
    if not result.get("ok") and status in {"unavailable", "error", "unknown"}:
        return True
    if status in {"login_required", "required"}:
        return True
    if bool(result.get("needs_login")):
        return True
    if str(result.get("reason") or "").strip().lower() == "login_url":
        return True
    return False


def check_matrix_publish_preflight(plan: list[PublishPlanItem], *, notify: bool = True) -> dict[str, Any]:
    wechat_ids = sorted({item.account_id for item in plan if item.platform == "wechat"})
    wechat_preflight = check_wechat_matrix_login_status(
        account_ids=wechat_ids,
        notify=notify,
    )
    non_wechat_results: list[dict[str, Any]] = []
    seen_pairs: set[tuple[int, str]] = set()
    for item in plan:
        if item.platform == "wechat":
            continue
        key = (item.account_id, item.platform)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        try:
            probe = service.check_login_status(item.account_id, item.platform)
        except Exception as exc:
            probe = {
                "ok": False,
                "status": "error",
                "platform": item.platform,
                "account_id": item.account_id,
                "error": str(exc),
            }
        non_wechat_results.append({**probe, "account_id": item.account_id, "platform": item.platform})
    blocking = [row for row in non_wechat_results if _login_status_blocks_publish(row)]
    non_wechat_ok = not blocking
    return {
        "ok": bool(wechat_preflight.get("ok")) and non_wechat_ok,
        "wechat_preflight": wechat_preflight,
        "non_wechat": {
            "checked": len(non_wechat_results),
            "results": non_wechat_results,
            "blocking": blocking,
        },
    }


def _serialize_plan_item(item: PublishPlanItem) -> dict[str, Any]:
    return {
        "account_id": item.account_id,
        "account_key": item.account_key,
        "display_name": item.display_name,
        "platform": item.platform,
        "video": str(item.source_video),
        "profile_dir": str(item.profile_dir),
        "debug_port": item.debug_port,
        "fingerprint": item.fingerprint,
        "workspace": str(item.workspace),
        "batch_index": item.batch_index,
        "batch_position": item.batch_position,
    }


def run_matrix_publish(
    *,
    limit: int = 0,
    dry_run: bool = False,
    platforms: frozenset[str] | None = None,
) -> dict[str, Any]:
    full_plan = build_publish_plan(limit=limit)
    if platforms is not None:
        plan = [item for item in full_plan if item.platform in platforms]
    else:
        plan = list(full_plan)
    job_settings = _matrix_wechat_job_settings()
    wechat_settings = load_wechat_publish_settings()
    platform_settings = {token: load_platform_publish_settings(token) for token in sorted({item.platform for item in plan})}
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "settings": wechat_settings,
            "platform_settings": platform_settings,
            "job_settings": job_settings,
            "batches": [[_serialize_plan_item(item) for item in batch] for batch in plan_batches(plan)],
            "items": [_serialize_plan_item(item) for item in plan],
        }

    locked, lock_payload = _acquire_publish_lock()
    if not locked:
        return {"ok": False, "skipped": True, "reason": "publish_lock_active", "lock": lock_payload}

    state = _load_state()
    consumed = list(state.get("consumed", [])) if isinstance(state.get("consumed"), list) else []
    runs = list(state.get("runs", [])) if isinstance(state.get("runs"), list) else []
    results: list[dict[str, Any]] = []
    try:
        if not plan:
            return {"ok": True, "count": 0, "results": [], "skipped": True, "reason": "empty_plan"}
        preflight = check_matrix_publish_preflight(plan, notify=True)
        if not preflight.get("ok"):
            wechat_ok = bool(preflight.get("wechat_preflight", {}).get("ok"))
            reason = "wechat_login_required" if not wechat_ok else "platform_login_required"
            return {
                "ok": False,
                "skipped": True,
                "reason": reason,
                "preflight": preflight,
                "count": 0,
                "results": [],
            }
        for item in plan:
            settings = load_platform_publish_settings(item.platform)
            debug_port = int(item.debug_port)
            prepared = prepare_workspace(item)
            runtime_config = _runtime_publish_config(item.platform, settings, item.workspace)
            token = str(item.platform or "wechat").strip().lower() or "wechat"
            cmd: list[str] = [
                sys.executable,
                "-m",
                "cybercar.pipeline",
                "--publish-only",
                "--upload-platforms",
                token,
                "--limit",
                "1",
                "--config",
                str(runtime_config),
                "--workspace",
                str(item.workspace),
                "--debug-port",
                str(debug_port),
                "--chrome-user-data-dir",
                str(item.profile_dir),
                "--upload-timeout",
                str(int(settings.get("upload_timeout") or 60)),
            ]
            if token == "wechat":
                cmd.extend(
                    [
                        "--wechat-debug-port",
                        str(debug_port),
                        "--wechat-chrome-user-data-dir",
                        str(item.profile_dir),
                        "--collection-name",
                        str(settings.get("collection_name") or ""),
                    ]
                )
            caption = caption_for_publish(settings, item.source_video)
            if caption:
                cmd.extend(["--caption", caption])
            if token == "wechat":
                if bool(settings.get("declare_original")):
                    cmd.append("--wechat-declare-original")
                if str(settings.get("publish_mode") or "publish") == "draft":
                    cmd.append("--wechat-save-draft-only")
                else:
                    cmd.append("--wechat-publish-now")
            started = time.strftime("%Y-%m-%d %H:%M:%S")
            log_path = item.workspace / f"matrix_{token}_publish.log"
            with log_path.open("w", encoding="utf-8", errors="replace") as log_file:
                env = {**os.environ, "CYBERCAR_DISABLE_REQUIRED_HASHTAGS": "1"}
                extra_args = service.browser_fingerprint_launch_args(item.fingerprint)
                if extra_args:
                    env["CYBERCAR_CHROME_EXTRA_ARGS"] = json.dumps(extra_args, ensure_ascii=False)
                completed = subprocess.run(
                    cmd,
                    cwd=str(get_paths().repo_root),
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
            evidence_ok = _has_publish_evidence(item.workspace, token)
            success = completed.returncode == 0 and evidence_ok
            record = {
                "account_id": item.account_id,
                "account_key": item.account_key,
                "platform": item.platform,
                "asset_key": _relative_asset_key(item.source_video),
                "publish_date": item.publish_date or _today_date(_load_timezone()).isoformat(),
                "display_name": item.display_name,
                "video": str(item.source_video),
                "prepared_video": str(prepared),
                "profile_dir": str(item.profile_dir),
                "fingerprint": item.fingerprint,
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
                record["error"] = (
                    f"{token} publish returned 0 but no uploaded_records_{token}.jsonl evidence was written"
                )
            results.append(record)
            runs.append(record)
            if success:
                consumed.append(
                    {
                        "asset_key": record["asset_key"],
                        "account_id": item.account_id,
                        "platform": item.platform,
                        "publish_date": record["publish_date"],
                        "success": True,
                        "finished_at": int(time.time()),
                        "asset_metadata": _asset_manifest_metadata(item.source_video),
                    }
                )
            state["consumed"] = consumed[-500:]
            state["runs"] = runs[-200:]
            _save_state(state)
        return {"ok": all(item["success"] for item in results), "count": len(results), "results": results}
    finally:
        _release_publish_lock()


def run_wechat_publish(*, limit: int = 0, dry_run: bool = False) -> dict[str, Any]:
    return run_matrix_publish(limit=limit, dry_run=dry_run, platforms=frozenset({"wechat"}))
