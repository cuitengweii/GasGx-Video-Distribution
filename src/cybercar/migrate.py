from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from .settings import apply_runtime_environment


LEGACY_RUNTIME_ROOT = Path(r"D:\code\Runtime\CyberCar_Workspace")
LEGACY_DEFAULT_PROFILE = Path(r"D:\code\Runtime\ChromeDebugProfile_CyberCar")
LEGACY_WECHAT_PROFILE = Path(r"D:\code\Runtime\ChromeDebugProfile_CyberCar_WeChat")
STATE_PATTERNS = [
    "review_state.json",
    "content_fingerprint_index*.json",
    "uploaded_content_fingerprint_index*.json",
    "uploaded_records_*.jsonl",
    "draft_upload_history*.txt",
    "history*.txt",
    "wechat_comment_reply_state.json",
]
MEDIA_DIRS = ["2_Processed", "2_Processed_Images"]
OPTIONAL_PENDING_DIRS = ["1_Downloads", "1_Downloads_Images"]


@dataclass(frozen=True)
class MigrationSummary:
    copied: list[str]
    skipped: list[str]


def _copy_file(src: Path, dst: Path, copied: list[str], skipped: list[str]) -> None:
    if not src.exists():
        skipped.append(str(src))
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    copied.append(str(dst))


def _copy_tree(src: Path, dst: Path, copied: list[str], skipped: list[str]) -> None:
    if not src.exists():
        skipped.append(str(src))
        return
    shutil.copytree(src, dst, dirs_exist_ok=True)
    copied.append(str(dst))


def migrate_legacy_assets(
    *,
    legacy_runtime_root: Path = LEGACY_RUNTIME_ROOT,
    legacy_default_profile: Path = LEGACY_DEFAULT_PROFILE,
    legacy_wechat_profile: Path = LEGACY_WECHAT_PROFILE,
    target_runtime_root: Path | None = None,
    target_default_profile: Path | None = None,
    target_wechat_profile: Path | None = None,
) -> MigrationSummary:
    paths = apply_runtime_environment()
    runtime_root = Path(target_runtime_root) if target_runtime_root is not None else paths.runtime_root
    default_profile_root = Path(target_default_profile) if target_default_profile is not None else paths.default_profile_dir
    wechat_profile_root = Path(target_wechat_profile) if target_wechat_profile is not None else paths.wechat_profile_dir
    runtime_root.mkdir(parents=True, exist_ok=True)
    default_profile_root.mkdir(parents=True, exist_ok=True)
    wechat_profile_root.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    skipped: list[str] = []

    for directory in MEDIA_DIRS:
        _copy_tree(legacy_runtime_root / directory, runtime_root / directory, copied, skipped)

    for directory in OPTIONAL_PENDING_DIRS:
        source_dir = legacy_runtime_root / directory
        if source_dir.exists() and any(child.is_file() for child in source_dir.rglob("*")):
            _copy_tree(source_dir, runtime_root / directory, copied, skipped)
        else:
            skipped.append(str(source_dir))

    for pattern in STATE_PATTERNS:
        for source_file in legacy_runtime_root.glob(pattern):
            _copy_file(source_file, runtime_root / source_file.name, copied, skipped)

    runtime_state_dir = legacy_runtime_root / "runtime"
    if runtime_state_dir.exists():
        for source_file in runtime_state_dir.iterdir():
            name = source_file.name
            if source_file.is_dir():
                if name in {"logs", "pipeline_priority_requests", "platform_publish_locks"} or name.startswith("telegram"):
                    skipped.append(str(source_file))
                    continue
                _copy_tree(source_file, runtime_root / name, copied, skipped)
                continue
            if name.startswith("telegram") or ".tmp-" in name:
                skipped.append(str(source_file))
                continue
            _copy_file(source_file, runtime_root / name, copied, skipped)

    _copy_tree(legacy_default_profile, default_profile_root, copied, skipped)
    _copy_tree(legacy_wechat_profile, wechat_profile_root, copied, skipped)
    return MigrationSummary(copied=copied, skipped=skipped)
