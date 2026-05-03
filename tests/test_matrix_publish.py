from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

from gasgx_distribution import db as dist_db
from gasgx_distribution import service
from gasgx_distribution.matrix_publish import (
    _caption_with_topics,
    _runtime_config_for_wechat,
    build_publish_plan,
    list_candidate_videos,
    publish_lock_path,
    run_wechat_publish,
)
from gasgx_distribution.public_settings import save_distribution_settings


def _pipeline_cmd(calls: list[list[str]]) -> list[str]:
    return next(cmd for cmd in calls if len(cmd) >= 3 and cmd[1:3] == ["-m", "cybercar.pipeline"])


def _isolated_paths(monkeypatch, tmp_path: Path) -> None:
    class FakePaths:
        repo_root = tmp_path
        runtime_root = tmp_path / "runtime"
        profiles_root = tmp_path / "profiles" / "matrix"
        database_path = tmp_path / "runtime" / "gasgx_distribution.db"

        def ensure(self) -> None:
            self.runtime_root.mkdir(parents=True, exist_ok=True)
            self.profiles_root.mkdir(parents=True, exist_ok=True)
            self.database_path.parent.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("gasgx_distribution.db.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.service.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.matrix_publish.get_paths", lambda: FakePaths())
    monkeypatch.setattr("gasgx_distribution.public_settings.get_paths", lambda: FakePaths())
    dist_db.init_db(FakePaths.database_path)


def _write_video(path: Path, mtime: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"video")
    path.touch()
    import os

    os.utime(path, (mtime, mtime))


def test_publish_plan_assigns_newest_unused_video_by_account_order(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    service.create_account({"account_key": "a-02", "display_name": "B", "platforms": ["wechat"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    now = int(time.time())
    _write_video(base / "old.mp4", now - 20)
    _write_video(base / "newest.mp4", now)
    _write_video(base / "middle.mp4", now - 10)

    plan = build_publish_plan()

    assert [item.account_id for item in plan] == [1, 2]
    assert [item.source_video.name for item in plan] == ["newest.mp4", "middle.mp4"]


def test_dry_run_does_not_mark_videos_used(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    video = tmp_path / "runtime" / "materials" / "videos" / "one.mp4"
    _write_video(video, int(time.time()))

    result = run_wechat_publish(dry_run=True)

    assert result["dry_run"] is True
    assert list_candidate_videos()[0].name == "one.mp4"


def test_publish_plan_uses_configured_batches_without_rotation_or_shuffle(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    save_distribution_settings(
        {
            "jobs": {
                "matrix_wechat_publish": {
                    "batch_size": 2,
                    "rotate_start_group": False,
                    "shuffle_within_batch": False,
                }
            }
        }
    )
    for index in range(1, 6):
        service.create_account({"account_key": f"a-{index:02d}", "display_name": f"A{index}", "platforms": ["wechat"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    now = int(time.time())
    for index in range(1, 6):
        _write_video(base / f"{index}.mp4", now + index)

    plan = build_publish_plan()

    assert [item.account_key for item in plan] == ["a-01", "a-02", "a-03", "a-04", "a-05"]
    assert [(item.batch_index, item.batch_position) for item in plan] == [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1)]


def test_dry_run_exposes_batch_plan(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    save_distribution_settings(
        {
            "jobs": {
                "matrix_wechat_publish": {
                    "batch_size": 2,
                    "rotate_start_group": False,
                    "shuffle_within_batch": False,
                }
            }
        }
    )
    for index in range(1, 4):
        service.create_account({"account_key": f"a-{index:02d}", "display_name": f"A{index}", "platforms": ["wechat"]})
        _write_video(tmp_path / "runtime" / "materials" / "videos" / f"{index}.mp4", int(time.time()) + index)

    result = run_wechat_publish(dry_run=True)

    assert result["job_settings"]["batch_size"] == 2
    assert [[item["account_key"] for item in batch] for batch in result["batches"]] == [["a-01", "a-02"], ["a-03"]]


def test_publish_plan_rotates_after_last_successful_account(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    save_distribution_settings(
        {
            "jobs": {
                "matrix_wechat_publish": {
                    "batch_size": 5,
                    "rotate_start_group": True,
                    "shuffle_within_batch": False,
                }
            }
        }
    )
    for index in range(1, 5):
        service.create_account({"account_key": f"a-{index:02d}", "display_name": f"A{index}", "platforms": ["wechat"]})
        _write_video(tmp_path / "runtime" / "materials" / "videos" / f"{index}.mp4", int(time.time()) + index)
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        '{"used_videos":[],"runs":[{"account_id":3,"success":true}]}',
        encoding="utf-8",
    )

    plan = build_publish_plan()

    assert [item.account_key for item in plan] == ["a-04", "a-01", "a-02", "a-03"]


def test_caption_with_topics_appends_global_topics() -> None:
    assert _caption_with_topics({"caption": "hello", "topics": "#gas #power"}) == "hello\n#gas #power"
    assert _caption_with_topics({"caption": "", "topics": "#gas #power"}) == "#gas #power"


def test_wechat_runtime_config_includes_independent_video_account_fields(tmp_path: Path) -> None:
    path = _runtime_config_for_wechat(
        {
            "collection_name": "test collection",
            "short_title": "GasGx Short",
            "location": "",
            "publish_mode": "draft",
            "declare_original": True,
            "upload_timeout": 120,
            "topics": "#gas",
        },
        tmp_path,
    )

    payload = path.read_text(encoding="utf-8")

    assert '"short_title": "GasGx Short"' in payload
    assert '"location": ""' in payload


def test_wechat_publish_uses_pipeline_draft_mode(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))
    save_distribution_settings({"common": {"publish_mode": "draft"}, "platforms": {"wechat": {"publish_mode": "inherit"}}})
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        workspaces = list((tmp_path / "runtime" / "matrix_publish_runs").glob("*"))
        if workspaces:
            (workspaces[0] / "uploaded_records_wechat.jsonl").write_text('{"ok":true}\n', encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("gasgx_distribution.matrix_publish.subprocess.run", fake_run)

    result = run_wechat_publish()

    assert result["ok"] is True
    assert calls
    cmd = _pipeline_cmd(calls)
    assert cmd[0:3] == [cmd[0], "-m", "cybercar.pipeline"]
    assert "--publish-only" in cmd
    assert "--wechat-save-draft-only" in cmd
    assert "--wechat-publish-now" not in cmd
    profile_arg = cmd[cmd.index("--chrome-user-data-dir") + 1].replace("\\", "/")
    wechat_profile_arg = cmd[cmd.index("--wechat-chrome-user-data-dir") + 1].replace("\\", "/")
    assert profile_arg.endswith("profiles/matrix/a-01")
    assert wechat_profile_arg.endswith("profiles/matrix/a-01")
    assert cmd[cmd.index("--debug-port") + 1].isdigit()
    assert cmd[cmd.index("--wechat-debug-port") + 1] == cmd[cmd.index("--debug-port") + 1]


def test_wechat_publish_disables_cybercar_required_hashtags(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))
    envs = []

    def fake_run(cmd, **kwargs):
        del cmd
        envs.append(kwargs.get("env") or {})
        (tmp_path / "runtime" / "matrix_publish_runs").mkdir(parents=True, exist_ok=True)
        workspaces = list((tmp_path / "runtime" / "matrix_publish_runs").glob("*"))
        if workspaces:
            (workspaces[0] / "uploaded_records_wechat.jsonl").write_text('{"ok":true}\n', encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("gasgx_distribution.matrix_publish.subprocess.run", fake_run)

    result = run_wechat_publish()

    assert result["ok"] is True
    assert envs
    pipeline_env = next(env for env in envs if "CYBERCAR_DISABLE_REQUIRED_HASHTAGS" in env)
    assert pipeline_env["CYBERCAR_DISABLE_REQUIRED_HASHTAGS"] == "1"


def test_wechat_publish_requires_uploaded_record_evidence(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    video = tmp_path / "runtime" / "materials" / "videos" / "one.mp4"
    _write_video(video, int(time.time()))

    def fake_run(cmd, **kwargs):
        del cmd, kwargs
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("gasgx_distribution.matrix_publish.subprocess.run", fake_run)

    result = run_wechat_publish()

    assert result["ok"] is False
    assert result["results"][0]["success"] is False
    assert result["results"][0]["evidence_ok"] is False
    assert list_candidate_videos()[0].name == "one.mp4"


def test_wechat_publish_skips_when_lock_is_active(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))
    publish_lock_path().write_text(json.dumps({"pid": os.getpid(), "started_at": "now"}), encoding="utf-8")

    result = run_wechat_publish()

    assert result["ok"] is False
    assert result["skipped"] is True
    assert result["reason"] == "publish_lock_active"
