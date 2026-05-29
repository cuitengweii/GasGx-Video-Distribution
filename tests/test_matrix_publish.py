from __future__ import annotations

import json
import os
import time
from datetime import date, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from gasgx_distribution import db as dist_db
from gasgx_distribution import service
from gasgx_distribution.matrix_publish import (
    _caption_with_topics,
    caption_for_publish,
    asset_day,
    _relative_asset_key,
    _runtime_config_for_wechat,
    build_publish_plan,
    build_account_domestic_publish_items,
    list_candidate_videos,
    publish_lock_path,
    run_account_platform_publish,
    run_account_domestic_publish,
    run_matrix_publish,
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
    monkeypatch.setattr("gasgx_distribution.paths.get_paths", lambda: FakePaths())
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


def test_publish_plan_skips_same_account_recent_dedupe_traits(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    now = int(time.time())
    newest = base / "same_bgm.mp4"
    fresh = base / "fresh_story.mp4"
    _write_video(newest, now)
    _write_video(fresh, now - 10)
    newest.with_name("same_bgm_manifest.json").write_text(
        json.dumps(
            {
                "narrative_template_id": "quick_showcase",
                "bgm_name": "used.mp3",
                "dedupe": {"first_frame_hash": "frame-a", "text_signature": "text-a"},
            }
        ),
        encoding="utf-8",
    )
    fresh.with_name("fresh_story_manifest.json").write_text(
        json.dumps(
            {
                "narrative_template_id": "faq_explainer",
                "bgm_name": "fresh.mp3",
                "dedupe": {"first_frame_hash": "frame-b", "text_signature": "text-b"},
            }
        ),
        encoding="utf-8",
    )
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        json.dumps(
            {
                "consumed": [
                    {
                        "asset_key": "old.mp4",
                        "account_id": 1,
                        "platform": "wechat",
                        "publish_date": time.strftime("%Y-%m-%d", time.localtime(now - 86400)),
                        "success": True,
                        "finished_at": now - 86400,
                        "asset_metadata": {
                            "first_frame_hash": "frame-a",
                            "bgm_name": "used.mp3",
                            "narrative_template_id": "quick_showcase",
                            "text_signature": "text-a",
                        },
                    }
                ],
                "runs": [],
            }
        ),
        encoding="utf-8",
    )

    plan = build_publish_plan()

    assert [item.source_video.name for item in plan] == ["fresh_story.mp4"]


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
    state_path.write_text('{"consumed":[],"runs":[{"account_id":3,"success":true}]}', encoding="utf-8")

    plan = build_publish_plan()

    assert [item.account_key for item in plan] == ["a-04", "a-01", "a-02", "a-03"]


def test_list_candidate_videos_includes_batch_subdirectories_and_filters_previous_day(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    base = tmp_path / "runtime" / "materials" / "videos"
    today = int(time.time())
    yesterday = today - 86400
    today_prefix = time.strftime("%Y%m%d", time.localtime(today))
    yesterday_prefix = time.strftime("%Y%m%d", time.localtime(yesterday))
    _write_video(base / f"{today_prefix}_120000_abcd1234" / "today.mp4", today)
    _write_video(base / "plain.mp4", today)
    _write_video(base / f"{yesterday_prefix}_120000_abcd1234" / "old.mp4", yesterday)

    candidates = list_candidate_videos()

    assert {item.name for item in candidates} == {"today.mp4", "plain.mp4"}
    assert asset_day(base / f"{today_prefix}_120000_abcd1234" / "today.mp4").isoformat() == time.strftime("%Y-%m-%d", time.localtime(today))


def test_publish_plan_allows_same_day_multi_platform_slots_with_same_video(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat", "douyin"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    _write_video(base / "one.mp4", int(time.time()))
    plan = build_publish_plan()

    assert [item.platform for item in plan] == ["douyin", "wechat"]
    assert [item.source_video.name for item in plan] == ["one.mp4", "one.mp4"]

    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        json.dumps(
            {
                "consumed": [
                    {
                        "asset_key": "one.mp4",
                        "account_id": 1,
                        "platform": "douyin",
                        "publish_date": time.strftime("%Y-%m-%d"),
                        "success": True,
                        "finished_at": int(time.time()),
                    }
                ],
                "runs": [],
            }
        ),
        encoding="utf-8",
    )

    plan = build_publish_plan()

    assert [item.platform for item in plan] == ["wechat"]
    assert [item.source_video.name for item in plan] == ["one.mp4"]


def test_account_platform_publish_reuses_locked_source_and_disables_telegram(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat", "douyin"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    now = int(time.time())
    locked = base / "locked.mp4"
    other = base / "other.mp4"
    _write_video(locked, now)
    _write_video(other, now - 10)
    publish_day = "2026-05-18"
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        json.dumps(
            {
                "consumed": [
                    {
                        "asset_key": _relative_asset_key(locked),
                        "account_id": 1,
                        "platform": "wechat",
                        "publish_date": publish_day,
                        "success": True,
                        "finished_at": now,
                    }
                ],
                "runs": [],
            }
        ),
        encoding="utf-8",
    )
    import gasgx_distribution.matrix_publish as mp

    monkeypatch.setattr(mp, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(mp, "_today_date", lambda _tz: date(2026, 5, 18))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [locked, other])
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda _workspace, _platform: True)
    monkeypatch.setattr(
        mp,
        "load_platform_publish_settings",
        lambda platform: {
            "publish_mode": "publish",
            "upload_timeout": 120,
            "caption": "caption",
            "topics": "",
            "collection_name": "",
            "declare_original": False,
        },
    )
    popen_calls: list[list[str]] = []

    class FakeCompleted:
        returncode = 0

    def fake_run(cmd, **kwargs):
        popen_calls.append(list(cmd))
        return FakeCompleted()

    monkeypatch.setattr(mp.subprocess, "run", fake_run)

    result = run_account_platform_publish(account_id=1, platform="douyin")

    assert result["ok"] is True
    assert result["results"][0]["asset_key"] == _relative_asset_key(locked)
    assert result["results"][0]["video"] == str(locked)
    assert "--disable-notify" in popen_calls[0]
    assert "--no-telegram-prefilter" in popen_calls[0]
    assert "--no-telegram-collect-notify" in popen_calls[0]
    assert "--no-publish-skip-notify" in popen_calls[0]


def test_run_account_platform_publish_can_disable_auto_open_chrome(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["douyin"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    locked = base / "locked.mp4"
    _write_video(locked, int(time.time()))
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(json.dumps({"consumed": [], "runs": []}), encoding="utf-8")
    import gasgx_distribution.matrix_publish as mp

    monkeypatch.setattr(mp, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(mp, "_today_date", lambda _tz: date(2026, 5, 18))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [locked])
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda _workspace, _platform: True)
    monkeypatch.setattr(
        mp,
        "load_platform_publish_settings",
        lambda platform: {
            "publish_mode": "publish",
            "upload_timeout": 120,
            "caption": "caption",
            "topics": "",
            "collection_name": "",
            "declare_original": False,
        },
    )
    popen_calls: list[list[str]] = []

    class FakeCompleted:
        returncode = 0

    def fake_run(cmd, **kwargs):
        popen_calls.append(list(cmd))
        return FakeCompleted()

    monkeypatch.setattr(mp.subprocess, "run", fake_run)

    result = run_account_platform_publish(account_id=1, platform="douyin", auto_open_chrome=False)

    assert result["ok"] is True
    assert result["results"][0]["asset_key"] == _relative_asset_key(locked)
    assert "--no-auto-open-chrome" in popen_calls[0]


def test_publish_command_for_item_uses_longer_timeout_for_fast_domestic_publish(tmp_path: Path) -> None:
    import gasgx_distribution.matrix_publish as mp

    runtime_config = tmp_path / "runtime_config.json"
    item = SimpleNamespace(
        platform="douyin",
        debug_port=16204,
        profile_dir=tmp_path / "profiles" / "matrix" / "a-01",
        source_video=tmp_path / "materials" / "shared.mp4",
        workspace=tmp_path / "workspace",
        account_publish_mode="inherit",
    )
    settings = {
        "publish_mode": "publish",
        "upload_timeout": 60,
        "caption": "caption",
        "topics": "",
        "collection_name": "",
        "declare_original": False,
    }

    cmd = mp._publish_command_for_item(  # type: ignore[attr-defined]
        item,
        settings,
        runtime_config,
        disable_telegram=True,
        auto_open_chrome=False,
        fast_publish=True,
    )

    assert "--upload-timeout" in cmd
    assert cmd[cmd.index("--upload-timeout") + 1] == "120"


def test_build_account_domestic_publish_items_reuses_shared_source(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account(
        {
            "account_key": "a-01",
            "display_name": "A",
            "platforms": ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili"],
        }
    )
    base = tmp_path / "runtime" / "materials" / "videos"
    shared = base / "shared.mp4"
    other = base / "other.mp4"
    _write_video(shared, int(time.time()))
    _write_video(other, int(time.time()) - 10)
    import gasgx_distribution.matrix_publish as mp

    monkeypatch.setattr(mp, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(mp, "_today_date", lambda _tz: date(2026, 5, 28))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [other, shared])
    monkeypatch.setattr(mp, "_consumed_asset_locks", lambda today=None: {1: _relative_asset_key(shared)})

    items = build_account_domestic_publish_items(1)

    assert [item.platform for item in items] == ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili"]
    assert {item.source_video.name for item in items} == {"shared.mp4"}
    assert [item.workspace.name for item in items] == ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili"]


def test_run_account_domestic_publish_reuses_shared_source_and_disables_telegram(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account(
        {
            "account_key": "a-01",
            "display_name": "A",
            "platforms": ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili"],
        }
    )
    base = tmp_path / "runtime" / "materials" / "videos"
    shared = base / "shared.mp4"
    other = base / "other.mp4"
    _write_video(shared, int(time.time()))
    _write_video(other, int(time.time()) - 10)
    import gasgx_distribution.matrix_publish as mp

    monkeypatch.setattr(mp, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(mp, "_today_date", lambda _tz: date(2026, 5, 28))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [other, shared])
    monkeypatch.setattr(mp, "_consumed_asset_locks", lambda today=None: {1: _relative_asset_key(shared)})
    monkeypatch.setattr(
        mp,
        "load_platform_publish_settings",
        lambda platform: {
            "publish_mode": "publish",
            "upload_timeout": 120,
            "caption": "caption",
            "topics": "",
            "collection_name": "",
            "declare_original": False,
        },
    )
    monkeypatch.setattr(mp.service, "browser_fingerprint_launch_args", lambda fingerprint: [])
    popen_calls: list[list[str]] = []

    class FakeCompleted:
        returncode = 0

    def fake_run(cmd, **kwargs):
        popen_calls.append(list(cmd))
        return FakeCompleted()

    monkeypatch.setattr(mp.subprocess, "run", fake_run)
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda _workspace, _platform: True)

    result = run_account_domestic_publish(1, auto_open_chrome=False)

    assert result["ok"] is True
    assert result["count"] == 5
    assert result["platforms"] == ["wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili"]
    assert result["source_video"] == str(shared)
    assert len(popen_calls) == 5
    uploaded = []
    for cmd in popen_calls:
        assert "--disable-notify" in cmd
        assert "--no-telegram-prefilter" in cmd
        assert "--no-telegram-collect-notify" in cmd
        assert "--no-publish-skip-notify" in cmd
        assert "--no-auto-open-chrome" in cmd
        assert "--force-publish-now" in cmd
        assert "--upload-platforms" in cmd
        uploaded.append(cmd[cmd.index("--upload-platforms") + 1])
    assert set(uploaded) == {"wechat", "douyin", "kuaishou", "xiaohongshu", "bilibili"}


def test_run_account_domestic_publish_serializes_platforms(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account(
        {
            "account_key": "a-01",
            "display_name": "A",
            "platforms": ["douyin", "xiaohongshu"],
        }
    )
    base = tmp_path / "runtime" / "materials" / "videos"
    shared = base / "shared.mp4"
    _write_video(shared, int(time.time()))
    import gasgx_distribution.matrix_publish as mp

    monkeypatch.setattr(mp, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(mp, "_today_date", lambda _tz: date(2026, 5, 28))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [shared])
    monkeypatch.setattr(mp, "_consumed_asset_locks", lambda today=None: {1: _relative_asset_key(shared)})
    monkeypatch.setattr(
        mp,
        "load_platform_publish_settings",
        lambda platform: {
            "publish_mode": "publish",
            "upload_timeout": 120,
            "caption": "caption",
            "topics": "",
            "collection_name": "",
            "declare_original": False,
        },
    )

    active = 0
    max_active = 0
    call_order: list[str] = []

    def fake_run_publish_item(item, settings, *, disable_telegram, auto_open_chrome, fast_publish):
        nonlocal active, max_active
        assert disable_telegram is True
        assert auto_open_chrome is False
        assert fast_publish is True
        call_order.append(f"{item.platform}:start")
        active += 1
        max_active = max(max_active, active)
        time.sleep(0.05)
        active -= 1
        call_order.append(f"{item.platform}:end")
        return {
            "account_id": item.account_id,
            "account_key": item.account_key,
            "platform": item.platform,
            "asset_key": _relative_asset_key(item.source_video),
            "publish_date": item.publish_date,
            "display_name": item.display_name,
            "video": str(item.source_video),
            "prepared_video": str(item.source_video),
            "profile_dir": str(item.profile_dir),
            "fingerprint": item.fingerprint,
            "debug_port": int(item.debug_port),
            "workspace": str(item.workspace),
            "account_publish_mode": item.account_publish_mode,
            "vpn_node_key": item.vpn_node_key,
            "vpn_country_code": item.vpn_country_code,
            "vpn_country_label": item.vpn_country_label,
            "vpn_proxy_url": item.vpn_proxy_url,
            "vpn_enabled": False,
            "effective_publish_mode": "publish",
            "returncode": 0,
            "success": True,
            "evidence_ok": True,
            "log": "ok",
            "started_at": "2026-05-28 00:00:00",
            "finished_at": "2026-05-28 00:00:01",
            "telegram_disabled": True,
        }

    monkeypatch.setattr(mp, "_run_publish_item", fake_run_publish_item)

    result = run_account_domestic_publish(1, auto_open_chrome=False)

    assert result["ok"] is True
    assert result["platforms"] == ["douyin", "xiaohongshu"]
    assert max_active == 1
    assert call_order == ["douyin:start", "douyin:end", "xiaohongshu:start", "xiaohongshu:end"]


def test_publish_plan_reuses_locked_asset_for_remaining_platforms(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat", "douyin"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    now = int(time.time())
    _write_video(base / "old.mp4", now - 10)
    _write_video(base / "new.mp4", now)
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        json.dumps(
            {
                "consumed": [
                    {
                        "asset_key": "old.mp4",
                        "account_id": 1,
                        "platform": "wechat",
                        "publish_date": time.strftime("%Y-%m-%d"),
                        "success": True,
                        "finished_at": int(time.time()),
                    }
                ],
                "runs": [],
            }
        ),
        encoding="utf-8",
    )

    plan = build_publish_plan()

    assert [item.platform for item in plan] == ["douyin"]
    assert [item.source_video.name for item in plan] == ["old.mp4"]


def test_publish_plan_skips_same_account_same_platform_same_day(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    base = tmp_path / "runtime" / "materials" / "videos"
    _write_video(base / "one.mp4", int(time.time()))
    state_path = tmp_path / "runtime" / "matrix_publish_state.json"
    state_path.write_text(
        json.dumps(
            {
                "consumed": [
                    {
                        "asset_key": "one.mp4",
                        "account_id": 1,
                        "platform": "wechat",
                        "publish_date": time.strftime("%Y-%m-%d"),
                        "success": True,
                        "finished_at": int(time.time()),
                    }
                ],
                "runs": [],
            }
        ),
        encoding="utf-8",
    )

    plan = build_publish_plan()

    assert plan == []


def test_caption_with_topics_appends_global_topics() -> None:
    assert _caption_with_topics({"caption": "hello", "topics": "#gas #power"}) == "hello\n#gas #power"
    assert _caption_with_topics({"caption": "", "topics": "#gas #power"}) == "#gas #power"


def test_caption_for_publish_uses_generated_copy_when_caption_empty(tmp_path: Path) -> None:
    video_path = tmp_path / "demo.mp4"
    video_path.write_bytes(b"video")
    copy_path = tmp_path / "demo_copy.txt"
    copy_path.write_text("AI generated description", encoding="utf-8")

    caption = caption_for_publish({"caption": "", "topics": "#gas"}, video_path)

    assert caption == "AI generated description\n#gas"

def test_caption_for_publish_prefers_configured_caption_over_generated_copy(tmp_path: Path) -> None:
    video_path = tmp_path / "demo.mp4"
    video_path.write_bytes(b"video")
    copy_path = tmp_path / "demo_copy.txt"
    copy_path.write_text("AI generated description", encoding="utf-8")

    caption = caption_for_publish({"caption": "manual caption", "topics": "#gas"}, video_path)

    assert caption == "manual caption\n#gas"


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


def test_emergency_publish_forces_publish_now_over_common_draft(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["douyin"]})
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))
    save_distribution_settings({"common": {"publish_mode": "draft"}, "platforms": {"douyin": {"publish_mode": "inherit"}}})
    import gasgx_distribution.matrix_publish as mp

    monkeypatch.setattr(mp, "_load_timezone", lambda: timezone.utc)
    monkeypatch.setattr(mp, "_today_date", lambda _tz: date(2026, 5, 18))
    monkeypatch.setattr(mp, "list_candidate_videos", lambda *args, **kwargs: [tmp_path / "runtime" / "materials" / "videos" / "one.mp4"])
    monkeypatch.setattr(mp, "_has_publish_evidence", lambda _workspace, _platform: True)
    monkeypatch.setattr(
        mp,
        "load_platform_publish_settings",
        lambda platform: {
            "publish_mode": "draft",
            "upload_timeout": 120,
            "caption": "caption",
            "topics": "",
            "collection_name": "",
            "declare_original": False,
        },
    )
    calls: list[list[str]] = []

    class FakeCompleted:
        returncode = 0

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        return FakeCompleted()

    monkeypatch.setattr(mp.subprocess, "run", fake_run)

    result = run_account_platform_publish(account_id=1, platform="douyin")

    assert result["ok"] is True
    cmd = _pipeline_cmd(calls)
    assert "--force-publish-now" in cmd

    from cybercar.pipeline import _resolve_platform_publish_mode_with_config

    args = SimpleNamespace(
        force_publish_now=True,
        publish_only=False,
        wechat_publish_now=False,
        wechat_save_draft_only=False,
        no_save_draft=False,
        bilibili_auto_publish_random_schedule=False,
        kuaishou_auto_publish_random_schedule=False,
    )
    mode = _resolve_platform_publish_mode_with_config(
        args,
        "douyin",
        {"common": {"publish_mode": "draft"}, "platforms": {"douyin": {"publish_mode": "inherit"}}},
    )
    assert mode.publish_now is True
    assert mode.save_draft is False


def test_wechat_publish_uses_account_vpn_for_bound_accounts(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    vpn_key = "vmess-ca-knyr-b-psakt-net-20101-加拿大-can-x1-0-ver10s"
    service.create_account(
        {
            "account_key": "a-01",
            "display_name": "A",
            "platforms": ["wechat"],
            "account_publish_mode": "publish",
            "vpn_node_key": vpn_key,
        }
    )
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))
    save_distribution_settings(
        {
            "common": {"publish_mode": "draft"},
        }
    )
    clash_calls: list[dict[str, Any]] = []
    envs: list[dict[str, str]] = []
    calls: list[list[str]] = []

    def fake_configure_account_clash(account: dict[str, Any]) -> bool:
        clash_calls.append(dict(account))
        return True

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        env = dict(kwargs.get("env") or {})
        envs.append(env)
        workspaces = list((tmp_path / "runtime" / "matrix_publish_runs").glob("*"))
        if workspaces:
            (workspaces[0] / "uploaded_records_wechat.jsonl").write_text('{"ok":true}\n', encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("gasgx_distribution.matrix_publish.subprocess.run", fake_run)

    result = run_wechat_publish()

    assert result["ok"] is True
    assert clash_calls == []
    assert envs
    vpn_env = next(env for env in envs if "CYBERCAR_VPN_NODE_KEY" in env)
    assert vpn_env["CYBERCAR_VPN_NODE_KEY"] == vpn_key
    assert vpn_env["CYBERCAR_DISABLE_REQUIRED_HASHTAGS"] == "1"
    assert calls
    cmd = _pipeline_cmd(calls)
    assert "--wechat-publish-now" in cmd
    assert "--wechat-save-draft-only" not in cmd


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


def test_douyin_publish_invokes_pipeline_without_wechat_flags(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["douyin"]})
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))
    calls: list[list[str]] = []

    def fake_check_login_status(account_id: int, platform: str) -> dict:
        return {"ok": True, "status": "ready", "platform": platform, "account_id": account_id}

    monkeypatch.setattr(service, "check_login_status", fake_check_login_status)

    def fake_run(cmd: list[str], **kwargs):
        del kwargs
        calls.append(cmd)
        workspaces = list((tmp_path / "runtime" / "matrix_publish_runs").glob("*"))
        if workspaces:
            (workspaces[0] / "uploaded_records_douyin.jsonl").write_text('{"ok":true}\n', encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("gasgx_distribution.matrix_publish.subprocess.run", fake_run)

    result = run_matrix_publish()

    assert result["ok"] is True
    cmd = _pipeline_cmd(calls)
    assert cmd[cmd.index("--upload-platforms") + 1] == "douyin"
    assert "--wechat-debug-port" not in cmd
    assert "--wechat-chrome-user-data-dir" not in cmd


def test_matrix_publish_preflight_blocks_non_wechat(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["douyin"]})
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))

    def fake_check_login_status(account_id: int, platform: str) -> dict:
        return {
            "ok": True,
            "status": "login_required",
            "platform": platform,
            "account_id": account_id,
            "reason": "login_url",
        }

    monkeypatch.setattr(service, "check_login_status", fake_check_login_status)

    result = run_matrix_publish()

    assert result.get("skipped") is True
    assert result.get("reason") == "platform_login_required"
    assert not (tmp_path / "runtime" / "matrix_publish_runs").exists()


def test_wechat_publish_skips_when_lock_is_active(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)
    service.create_account({"account_key": "a-01", "display_name": "A", "platforms": ["wechat"]})
    _write_video(tmp_path / "runtime" / "materials" / "videos" / "one.mp4", int(time.time()))
    publish_lock_path().write_text(json.dumps({"pid": os.getpid(), "started_at": "now"}), encoding="utf-8")

    result = run_wechat_publish()

    assert result["ok"] is False
    assert result["skipped"] is True
    assert result["reason"] == "publish_lock_active"



