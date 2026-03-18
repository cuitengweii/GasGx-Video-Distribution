from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from cybercar.cleanup import cleanup_runtime


def _write_file(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _set_mtime(path: Path, when: datetime) -> None:
    timestamp = when.timestamp()
    if path.is_dir():
        for child in path.rglob("*"):
            if child.exists():
                child.touch()
    path.touch()
    import os

    os.utime(path, (timestamp, timestamp))


def _cleanup_config() -> dict[str, object]:
    return {
        "enabled": True,
        "targets": {
            "downloads": {"enabled": True, "retention_days": 3},
            "downloads_images": {"enabled": True, "retention_days": 3},
            "processed_videos": {"enabled": True, "retention_days": 14},
            "processed_images": {"enabled": True, "retention_days": 14},
            "archive": {"enabled": True, "retention_days": 30},
            "sorted_batches": {"enabled": True, "retention_days": 7},
            "debug_workspaces": {"enabled": True, "retention_days": 3},
        },
    }


def test_cleanup_runtime_dry_run_reports_expired_groups_and_dirs(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    now = datetime(2026, 3, 18, 9, 0, 0)

    old_video = runtime_root / "2_Processed" / "old_clip.mp4"
    old_caption = runtime_root / "2_Processed" / "old_clip.caption.txt"
    new_video = runtime_root / "2_Processed" / "fresh_clip.mp4"
    old_archive = runtime_root / "3_Archive" / "old_archive.mp4"
    sorted_dir = runtime_root / "4_Sorted_By_Time" / "20260301_010101"
    debug_dir = runtime_root / "debug_collect_old"

    _write_file(old_video, "video")
    _write_file(old_caption, "caption")
    _write_file(new_video, "video-new")
    _write_file(old_archive, "archive")
    _write_file(sorted_dir / "item.mp4", "sorted")
    _write_file(debug_dir / "trace.txt", "debug")

    _set_mtime(old_video, now - timedelta(days=20))
    _set_mtime(old_caption, now - timedelta(days=20))
    _set_mtime(new_video, now - timedelta(days=1))
    _set_mtime(old_archive, now - timedelta(days=31))
    _set_mtime(sorted_dir / "item.mp4", now - timedelta(days=8))
    _set_mtime(sorted_dir, now - timedelta(days=8))
    _set_mtime(debug_dir / "trace.txt", now - timedelta(days=5))
    _set_mtime(debug_dir, now - timedelta(days=5))

    result = cleanup_runtime(
        runtime_root=runtime_root,
        cleanup_cfg=_cleanup_config(),
        apply=False,
        print_files=True,
        now=now,
    )

    assert result["ok"] is True
    targets = {item["name"]: item for item in result["targets"]}
    assert targets["processed_videos"]["groups"] == 1
    assert targets["processed_videos"]["entries"] == 2
    assert len(targets["processed_videos"]["matches"]) == 1
    assert targets["archive"]["entries"] == 1
    assert targets["sorted_batches"]["groups"] == 1
    assert targets["debug_workspaces"]["groups"] == 1
    assert old_video.exists() is True
    assert old_caption.exists() is True
    assert new_video.exists() is True


def test_cleanup_runtime_apply_deletes_expired_material(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    now = datetime(2026, 3, 18, 9, 0, 0)

    old_video = runtime_root / "2_Processed" / "old_clip.mp4"
    old_caption = runtime_root / "2_Processed" / "old_clip.caption.txt"
    old_image = runtime_root / "2_Processed_Images" / "old_image.jpg"
    old_batch = runtime_root / "4_Sorted_By_Time" / "20260301_010101"

    _write_file(old_video, "video")
    _write_file(old_caption, "caption")
    _write_file(old_image, "image")
    _write_file(old_batch / "item.mp4", "sorted")

    _set_mtime(old_video, now - timedelta(days=20))
    _set_mtime(old_caption, now - timedelta(days=20))
    _set_mtime(old_image, now - timedelta(days=20))
    _set_mtime(old_batch / "item.mp4", now - timedelta(days=8))
    _set_mtime(old_batch, now - timedelta(days=8))

    result = cleanup_runtime(
        runtime_root=runtime_root,
        cleanup_cfg=_cleanup_config(),
        apply=True,
        print_files=False,
        now=now,
    )

    assert result["ok"] is True
    assert old_video.exists() is False
    assert old_caption.exists() is False
    assert old_image.exists() is False
    assert old_batch.exists() is False
    assert result["summary"]["entries"] >= 4
