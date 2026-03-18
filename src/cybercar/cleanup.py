from __future__ import annotations

import fnmatch
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .settings import apply_runtime_environment, load_app_config


DEFAULT_CLEANUP_TARGETS: dict[str, dict[str, Any]] = {
    "downloads": {"enabled": True, "retention_days": 3},
    "downloads_images": {"enabled": True, "retention_days": 3},
    "processed_videos": {"enabled": True, "retention_days": 14},
    "processed_images": {"enabled": True, "retention_days": 14},
    "archive": {"enabled": True, "retention_days": 30},
    "sorted_batches": {"enabled": True, "retention_days": 7},
    "debug_workspaces": {"enabled": True, "retention_days": 3},
}

_GROUPED_SIDE_SUFFIXES = (
    ".caption.txt",
    ".info.json",
    ".json",
    ".txt",
)


@dataclass(frozen=True)
class CleanupTarget:
    name: str
    mode: str
    retention_days: int
    root: Path
    pattern: str = ""


def _default_cleanup_config() -> dict[str, Any]:
    targets = {name: dict(payload) for name, payload in DEFAULT_CLEANUP_TARGETS.items()}
    return {"enabled": True, "targets": targets}


def _merge_cleanup_config(raw: Any) -> dict[str, Any]:
    defaults = _default_cleanup_config()
    if not isinstance(raw, dict):
        return defaults
    merged = {"enabled": bool(raw.get("enabled", defaults["enabled"])), "targets": {}}
    payload_targets = raw.get("targets") if isinstance(raw.get("targets"), dict) else {}
    for name, default_target in defaults["targets"].items():
        value = payload_targets.get(name) if isinstance(payload_targets.get(name), dict) else {}
        retention_days = int(value.get("retention_days") or default_target["retention_days"])
        merged["targets"][name] = {
            "enabled": bool(value.get("enabled", default_target["enabled"])),
            "retention_days": max(0, retention_days),
        }
    return merged


def _build_cleanup_targets(runtime_root: Path, cleanup_cfg: dict[str, Any]) -> list[CleanupTarget]:
    targets_cfg = cleanup_cfg.get("targets") if isinstance(cleanup_cfg.get("targets"), dict) else {}

    def _target(name: str, *, mode: str, root: Path, pattern: str = "") -> CleanupTarget | None:
        payload = targets_cfg.get(name) if isinstance(targets_cfg.get(name), dict) else {}
        if not bool(payload.get("enabled", False)):
            return None
        return CleanupTarget(
            name=name,
            mode=mode,
            retention_days=max(0, int(payload.get("retention_days") or 0)),
            root=root,
            pattern=pattern,
        )

    built = [
        _target("downloads", mode="file_groups", root=runtime_root / "1_Downloads"),
        _target("downloads_images", mode="file_groups", root=runtime_root / "1_Downloads_Images"),
        _target("processed_videos", mode="file_groups", root=runtime_root / "2_Processed"),
        _target("processed_images", mode="file_groups", root=runtime_root / "2_Processed_Images"),
        _target("archive", mode="file_groups", root=runtime_root / "3_Archive"),
        _target("sorted_batches", mode="child_dirs", root=runtime_root / "4_Sorted_By_Time"),
        _target("debug_workspaces", mode="child_dirs_by_pattern", root=runtime_root, pattern="debug_collect*"),
    ]
    return [target for target in built if target is not None]


def _canonical_group_name(path: Path) -> str:
    lower_name = path.name.lower()
    for suffix in _GROUPED_SIDE_SUFFIXES:
        if lower_name.endswith(suffix):
            return path.name[: -len(suffix)]
    return path.stem


def _path_size(path: Path) -> int:
    if path.is_file():
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0
    total = 0
    for child in path.rglob("*"):
        if not child.is_file():
            continue
        try:
            total += int(child.stat().st_size)
        except OSError:
            continue
    return total


def _delete_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=False)
        return
    path.unlink(missing_ok=True)


def _collect_expired_file_groups(root: Path, cutoff: datetime) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    grouped: dict[tuple[str, str], list[Path]] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        group_key = (str(path.parent), _canonical_group_name(path))
        grouped.setdefault(group_key, []).append(path)

    expired: list[dict[str, Any]] = []
    for (_, _group_name), files in grouped.items():
        mtimes = [datetime.fromtimestamp(path.stat().st_mtime) for path in files]
        if not mtimes or max(mtimes) >= cutoff:
            continue
        paths = sorted(files, key=lambda item: str(item))
        expired.append(
            {
                "path": str(paths[0].parent),
                "entries": [str(item) for item in paths],
                "bytes": sum(_path_size(item) for item in paths),
                "last_modified": max(mtimes).isoformat(timespec="seconds"),
            }
        )
    expired.sort(key=lambda item: (item["last_modified"], item["path"]))
    return expired


def _collect_expired_child_dirs(root: Path, cutoff: datetime, pattern: str = "") -> list[dict[str, Any]]:
    if not root.exists():
        return []
    expired: list[dict[str, Any]] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        if pattern and not fnmatch.fnmatch(child.name, pattern):
            continue
        last_modified = datetime.fromtimestamp(child.stat().st_mtime)
        if last_modified >= cutoff:
            continue
        expired.append(
            {
                "path": str(child),
                "entries": [str(child)],
                "bytes": _path_size(child),
                "last_modified": last_modified.isoformat(timespec="seconds"),
            }
        )
    expired.sort(key=lambda item: (item["last_modified"], item["path"]))
    return expired


def cleanup_runtime(
    *,
    runtime_root: Path,
    cleanup_cfg: dict[str, Any],
    apply: bool = False,
    print_files: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    now_dt = now or datetime.now()
    if not bool(cleanup_cfg.get("enabled", True)):
        return {
            "ok": True,
            "apply": bool(apply),
            "runtime_root": str(runtime_root),
            "checked_at": now_dt.isoformat(timespec="seconds"),
            "targets": [],
            "summary": {"targets": 0, "entries": 0, "bytes": 0},
            "disabled": True,
        }
    targets = _build_cleanup_targets(runtime_root, cleanup_cfg)
    summary_targets: list[dict[str, Any]] = []
    total_bytes = 0
    total_entries = 0

    for target in targets:
        cutoff = now_dt - timedelta(days=max(0, target.retention_days))
        if target.mode == "file_groups":
            expired = _collect_expired_file_groups(target.root, cutoff)
        elif target.mode == "child_dirs":
            expired = _collect_expired_child_dirs(target.root, cutoff)
        else:
            expired = _collect_expired_child_dirs(target.root, cutoff, pattern=target.pattern)

        deleted_entries = 0
        reclaimed_bytes = 0
        errors: list[str] = []
        for item in expired:
            for entry in item["entries"]:
                try:
                    if apply:
                        _delete_path(Path(entry))
                    deleted_entries += 1
                except Exception as exc:
                    errors.append(f"{entry}: {exc}")
            reclaimed_bytes += int(item.get("bytes") or 0)

        total_bytes += reclaimed_bytes
        total_entries += deleted_entries
        payload = {
            "name": target.name,
            "root": str(target.root),
            "retention_days": target.retention_days,
            "apply": bool(apply),
            "groups": len(expired),
            "entries": deleted_entries if apply else sum(len(item["entries"]) for item in expired),
            "bytes": reclaimed_bytes,
            "errors": errors,
        }
        if print_files:
            payload["matches"] = expired
        summary_targets.append(payload)

    return {
        "ok": True,
        "apply": bool(apply),
        "runtime_root": str(runtime_root),
        "checked_at": now_dt.isoformat(timespec="seconds"),
        "targets": summary_targets,
        "summary": {
            "targets": len(summary_targets),
            "entries": total_entries if apply else sum(int(item["entries"]) for item in summary_targets),
            "bytes": total_bytes,
        },
    }


def run_cleanup(*, apply: bool = False, print_files: bool = False) -> dict[str, Any]:
    paths = apply_runtime_environment()
    config = load_app_config()
    cleanup_cfg = _merge_cleanup_config(config.get("cleanup"))
    return cleanup_runtime(
        runtime_root=paths.runtime_root,
        cleanup_cfg=cleanup_cfg,
        apply=apply,
        print_files=print_files,
    )
