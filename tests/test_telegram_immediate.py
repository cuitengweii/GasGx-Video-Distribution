import argparse
import json
import os
from pathlib import Path
from types import SimpleNamespace

from Collection.cybercar.cybercar_video_capture_and_publishing_module import telegram_command_worker as worker_impl
from cybercar import pipeline
from cybercar.telegram import actions, commands, prefilter, state


BOT_TOKEN = "123456:abcdefghijklmnopqrstuvwxyzABCDE"
CHAT_ID = "chat-1"
DEFAULT_PROFILE = "cybertruck"


class FakeCore:
    def init_workspace(self, workspace: str) -> SimpleNamespace:
        root = Path(workspace)
        return SimpleNamespace(
            processed=root / "2_Processed",
            processed_images=root / "2_Processed_Images",
        )

    def _find_processed_target_by_source(self, workspace_ctx: object, *, source_url: str) -> tuple[None, None]:
        return None, None

    def _is_uploaded_content_duplicate(self, workspace_ctx: object, target: Path, *, platform: str) -> tuple[bool, dict, None]:
        return False, {}, None

    def _media_kind_from_path(self, target: Path) -> str:
        return "image" if target.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".bmp"} else "video"


class FakeRunner:
    CycleContext = SimpleNamespace

    def __init__(self, core: FakeCore, *, chat_id: str = CHAT_ID) -> None:
        self.core = core
        self.chat_id = chat_id
        self.sent_candidates: list[dict[str, object]] = []
        self.built_args: list[object] = []
        self._message_id = 600

    def _build_parser(self) -> argparse.ArgumentParser:
        return argparse.ArgumentParser(add_help=False)

    def _build_email_settings(self, args: object) -> dict[str, object]:
        self.built_args.append(args)
        return {"workspace": getattr(args, "workspace", "")}

    def _send_telegram_prefilter_for_candidate(self, **kwargs: object) -> dict[str, object]:
        self.sent_candidates.append(dict(kwargs))
        self._message_id += 1
        return {
            "result": {
                "message_id": self._message_id,
                "chat": {"id": self.chat_id},
            }
        }


def _make_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    (workspace / "runtime" / "logs").mkdir(parents=True)
    (workspace / "2_Processed").mkdir(parents=True)
    (workspace / "2_Processed_Images").mkdir(parents=True)
    return workspace


def _worker_kwargs(workspace: Path, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "bot_token": BOT_TOKEN,
        "allowed_chat_id": CHAT_ID,
        "allow_private_chat_commands": False,
        "command_password": "",
        "started_at": 0.0,
        "repo_root": workspace,
        "workspace": workspace,
        "timeout_seconds": 30,
        "allow_shell": False,
        "allow_prefixes": [],
        "audit_file": workspace / "runtime" / "telegram_command_worker_audit.jsonl",
        "last_processed": 0,
        "log_file": workspace / "runtime" / "logs" / "telegram_worker.log",
        "telegram_bot_identifier": "",
        "default_profile": DEFAULT_PROFILE,
    }
    payload.update(overrides)
    return payload


def _make_callback_update(
    callback_data: str,
    *,
    update_id: int = 1,
    message_id: int = 77,
    chat_id: str = CHAT_ID,
    username: str = "tester",
) -> dict[str, object]:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": f"cb-{update_id}",
            "data": callback_data,
            "from": {"id": 1001, "username": username},
            "message": {
                "message_id": message_id,
                "chat": {"id": chat_id, "type": "private"},
            },
        },
    }


def _action_tasks(workspace: Path) -> dict[str, object]:
    payload = state.load_state(state.action_queue_path(workspace))
    tasks = payload.get("tasks", {})
    return tasks if isinstance(tasks, dict) else {}


def _prefilter_items(workspace: Path) -> dict[str, object]:
    payload = prefilter.load_queue(state.prefilter_queue_path(workspace))
    items = payload.get("items", {})
    return items if isinstance(items, dict) else {}


def _save_prefilter_items(workspace: Path, items: dict[str, object]) -> None:
    prefilter.save_queue(
        state.prefilter_queue_path(workspace),
        {
            "version": 1,
            "items": items,
        },
    )


def _video_item(item_id: str = "item-video", **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": item_id,
        "status": "link_pending",
        "workflow": "immediate_manual_publish",
        "media_kind": "video",
        "source_url": "https://x.test/post/video",
        "tweet_text": "video candidate",
        "published_at": "2026-03-15 10:00:00",
        "display_time": "10m",
        "target_platforms": "wechat,douyin,xiaohongshu,kuaishou,bilibili",
        "chat_id": CHAT_ID,
        "candidate_index": 1,
        "candidate_limit": 2,
        "video_name": "clip.mp4",
        "processed_name": "clip.mp4",
    }
    payload.update(overrides)
    return payload


def _image_item(item_id: str = "item-image", **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": item_id,
        "status": "link_pending",
        "workflow": "immediate_collect_review",
        "media_kind": "image",
        "source_url": "https://x.test/post/image",
        "tweet_text": "image candidate",
        "published_at": "2026-03-15 10:05:00",
        "display_time": "5m",
        "target_platforms": "douyin,xiaohongshu,kuaishou",
        "chat_id": CHAT_ID,
        "candidate_index": 1,
        "candidate_limit": 2,
    }
    payload.update(overrides)
    return payload


def _install_transport_mocks(monkeypatch) -> SimpleNamespace:
    record = SimpleNamespace(
        answers=[],
        toasts=[],
        cards=[],
        updated_cards=[],
        clear_buttons=[],
        replies=[],
        placeholders=[],
    )

    monkeypatch.setattr(worker_impl, "_append_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_impl, "_append_prefilter_feedback_event", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_answer_callback_query", lambda **kwargs: record.answers.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "answer_interaction_toast", lambda **kwargs: record.toasts.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "send_interaction_result", lambda **kwargs: record.cards.append(dict(kwargs)))

    def send_loading_placeholder(**kwargs: object) -> int:
        record.placeholders.append(dict(kwargs))
        return 901

    monkeypatch.setattr(worker_impl, "_send_loading_placeholder", send_loading_placeholder)
    monkeypatch.setattr(worker_impl, "_update_callback_message_card", lambda **kwargs: record.updated_cards.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_try_clear_callback_buttons", lambda **kwargs: record.clear_buttons.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: record.replies.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_try_delete_telegram_message", lambda **kwargs: False)
    return record


def test_handle_home_collect_publish_video_queues_task(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    def spawn_home_action_job(**kwargs: object) -> dict[str, object]:
        spawned.append(dict(kwargs))
        return {"ok": True, "pid": 321, "log_path": str(workspace / "runtime" / "logs" / "home-video.log")}

    monkeypatch.setattr(worker_impl, "_spawn_home_action_job", spawn_home_action_job)

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "collect_publish_latest", "video:3"),
    )
    result = commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert result["handled"] is True
    assert len(spawned) == 1
    assert len(record.placeholders) == 1

    tasks = _action_tasks(workspace)
    assert len(tasks) == 1
    task = next(iter(tasks.values()))
    assert isinstance(task, dict)
    assert task["action"] == "collect_publish_latest"
    assert task["value"] == "video:3"
    assert task["status"] == "running"
    assert task["pid"] == 321
    assert task["loading_message_id"] == 901
    assert "\u5019\u9009" in str(task["detail"])
    assert "\u666e\u901a\u53d1\u5e03" in str(task["detail"])


def test_handle_home_collect_publish_propagates_immediate_test_mode(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    def spawn_home_action_job(**kwargs: object) -> dict[str, object]:
        spawned.append(dict(kwargs))
        return {"ok": True, "pid": 322, "log_path": str(workspace / "runtime" / "logs" / "home-video-test.log")}

    monkeypatch.setattr(worker_impl, "_spawn_home_action_job", spawn_home_action_job)

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "collect_publish_latest", "video:1"),
    )
    commands.handle_callback_update(
        update=update,
        **_worker_kwargs(workspace, immediate_test_mode=True),
    )

    assert len(spawned) == 1
    assert spawned[0]["immediate_test_mode"] is True


def test_handle_home_collect_publish_default_mode_does_not_pass_immediate_test_mode(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    def spawn_home_action_job(**kwargs: object) -> dict[str, object]:
        spawned.append(dict(kwargs))
        return {"ok": True, "pid": 323, "log_path": str(workspace / "runtime" / "logs" / "home-video-normal.log")}

    monkeypatch.setattr(worker_impl, "_spawn_home_action_job", spawn_home_action_job)

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "collect_publish_latest", "video:1"),
    )
    commands.handle_callback_update(
        update=update,
        **_worker_kwargs(workspace),
    )

    assert len(spawned) == 1
    assert spawned[0]["immediate_test_mode"] is False


def test_handle_home_collect_publish_image_queues_task(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_spawn_home_action_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {
            "ok": True,
            "pid": 654,
            "log_path": str(workspace / "runtime" / "logs" / "home-image.log"),
        },
    )

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "collect_publish_latest", "image:5"),
    )
    commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert len(spawned) == 1
    tasks = _action_tasks(workspace)
    task = next(iter(tasks.values()))
    assert isinstance(task, dict)
    assert task["value"] == "image:5"
    assert task["status"] == "running"
    assert "\u56fe\u7247" in str(task["detail"])


def test_handle_home_collect_publish_deduplicates_running_task(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_spawn_home_action_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {
            "ok": True,
            "pid": 999,
            "log_path": str(workspace / "runtime" / "logs" / "home-dedupe.log"),
        },
    )

    callback_data = commands.build_home_callback_data("cybercar", "collect_publish_latest", "video:1")
    commands.handle_callback_update(
        update=_make_callback_update(callback_data, update_id=1),
        **_worker_kwargs(workspace),
    )
    commands.handle_callback_update(
        update=_make_callback_update(callback_data, update_id=2),
        **_worker_kwargs(workspace),
    )

    assert len(spawned) == 1
    tasks = _action_tasks(workspace)
    assert len(tasks) == 1


def test_recover_orphaned_home_action_marks_task_blocked_without_unsolicited_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    sent_cards: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_append_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_impl, "_pid_is_running", lambda pid: False)
    monkeypatch.setattr(worker_impl, "_send_card_message", lambda **kwargs: sent_cards.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_try_delete_telegram_message", lambda **kwargs: False)

    claimed = worker_impl._claim_home_action_task(
        workspace=workspace,
        chat_id=CHAT_ID,
        action="collect_publish_latest",
        value="video:10",
        profile=DEFAULT_PROFILE,
        username="tester",
    )
    task_key = str(claimed["task_key"])
    worker_impl._update_home_action_task(
        workspace,
        task_key,
        status="running",
        detail="still running",
        pid=4321,
        extra={"loading_message_id": 0},
    )

    recovered = worker_impl._recover_orphaned_home_action_tasks(
        workspace=workspace,
        bot_token=BOT_TOKEN,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
    )

    assert recovered == 1
    assert sent_cards == []
    task = _action_tasks(workspace)[task_key]
    assert isinstance(task, dict)
    assert task["status"] == "blocked"
    assert "回传中断" in str(task["detail"])


def test_recover_orphaned_home_action_notifies_when_loading_placeholder_exists(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    sent_cards: list[dict[str, object]] = []
    deleted_messages: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_append_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_impl, "_pid_is_running", lambda pid: False)
    monkeypatch.setattr(worker_impl, "_send_card_message", lambda **kwargs: sent_cards.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_try_delete_telegram_message",
        lambda **kwargs: deleted_messages.append(dict(kwargs)) or True,
    )

    claimed = worker_impl._claim_home_action_task(
        workspace=workspace,
        chat_id=CHAT_ID,
        action="collect_publish_latest",
        value="image:3",
        profile=DEFAULT_PROFILE,
        username="tester",
    )
    task_key = str(claimed["task_key"])
    worker_impl._update_home_action_task(
        workspace,
        task_key,
        status="running",
        detail="still running",
        pid=9876,
        extra={"loading_message_id": 901},
    )

    recovered = worker_impl._recover_orphaned_home_action_tasks(
        workspace=workspace,
        bot_token=BOT_TOKEN,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
    )

    assert recovered == 1
    assert len(deleted_messages) == 1
    assert len(sent_cards) == 1
    task = _action_tasks(workspace)[task_key]
    assert isinstance(task, dict)
    assert task["status"] == "blocked"
    assert task["loading_message_id"] == 0


def test_run_collect_publish_latest_job_video_records_prefilter_items(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {
            "keyword": DEFAULT_PROFILE,
            "candidates": [
                {
                    "url": "https://x.test/post/video-1",
                    "published_at": "2026-03-15 10:00:00",
                    "display_time": "10m",
                    "tweet_text": "video one",
                },
                {
                    "url": "https://x.test/post/video-2",
                    "published_at": "2026-03-15 09:55:00",
                    "display_time": "15m",
                    "tweet_text": "video two",
                },
            ],
        },
    )

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=2,
        media_kind="video",
    )

    assert exit_code == 0
    items = _prefilter_items(workspace)
    assert len(items) == 2
    for item in items.values():
        assert isinstance(item, dict)
        assert item["workflow"] == "immediate_manual_publish"
        assert item["status"] == "link_pending"
        assert item["target_platforms"] == "wechat,douyin,xiaohongshu,kuaishou,bilibili"
        assert item["chat_id"] == CHAT_ID
        assert int(item["message_id"]) > 0
    assert len(runner.sent_candidates) == 2
    assert runner.sent_candidates[0]["mode"] == "immediate_manual_publish"
    assert feedbacks[0]["status"] == "running"
    assert feedbacks[-1]["status"] == "done"


def test_run_collect_publish_latest_job_test_mode_forces_new_prefilter_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    discovered_kwargs: list[dict[str, object]] = []
    candidate = {
        "url": "https://x.test/post/video-reused",
        "published_at": "2026-03-15 10:02:00",
        "display_time": "8m",
        "tweet_text": "video reused",
    }
    existing_id = worker_impl._build_immediate_candidate_item_id(candidate["url"], candidate["published_at"], "video")

    _save_prefilter_items(
        workspace,
        {
            existing_id: _video_item(
                existing_id,
                source_url=candidate["url"],
                published_at=candidate["published_at"],
                display_time=candidate["display_time"],
                tweet_text=candidate["tweet_text"],
                status="publish_partial",
                action="publish",
                message_id=812,
                platform_results={"wechat": {"status": "success"}},
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)

    def discover_latest_live_candidates(**kwargs: object) -> dict[str, object]:
        discovered_kwargs.append(dict(kwargs))
        return {"keyword": DEFAULT_PROFILE, "candidates": [candidate]}

    monkeypatch.setattr(worker_impl, "_discover_latest_live_candidates", discover_latest_live_candidates)

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=1,
        media_kind="video",
        immediate_test_mode=True,
    )

    assert exit_code == 0
    assert discovered_kwargs[0]["allow_search_inferred_match"] is True
    assert len(runner.sent_candidates) == 1
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "link_pending"
    assert item["action"] == "sent"
    assert int(item["message_id"]) > 0
    assert item["platform_results"] == {}


def test_run_collect_publish_latest_job_default_mode_reuses_existing_prefilter_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    discovered_kwargs: list[dict[str, object]] = []
    candidate = {
        "url": "https://x.test/post/video-reused-normal",
        "published_at": "2026-03-15 10:03:00",
        "display_time": "7m",
        "tweet_text": "video reused normal",
    }
    existing_id = worker_impl._build_immediate_candidate_item_id(candidate["url"], candidate["published_at"], "video")

    _save_prefilter_items(
        workspace,
        {
            existing_id: _video_item(
                existing_id,
                source_url=candidate["url"],
                published_at=candidate["published_at"],
                display_time=candidate["display_time"],
                tweet_text=candidate["tweet_text"],
                status="publish_partial",
                action="publish",
                message_id=913,
                platform_results={"wechat": {"status": "success"}},
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)

    def discover_latest_live_candidates(**kwargs: object) -> dict[str, object]:
        discovered_kwargs.append(dict(kwargs))
        return {"keyword": DEFAULT_PROFILE, "candidates": [candidate]}

    monkeypatch.setattr(worker_impl, "_discover_latest_live_candidates", discover_latest_live_candidates)

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=1,
        media_kind="video",
    )

    assert exit_code == 0
    assert discovered_kwargs[0]["allow_search_inferred_match"] is False
    assert len(runner.sent_candidates) == 0
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "publish_partial"
    assert item["action"] == "publish"
    assert item["message_id"] == 913
    assert item["platform_results"] == {"wechat": {"status": "success"}}


def test_run_collect_publish_latest_job_image_records_review_only_items(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {
            "keyword": DEFAULT_PROFILE,
            "candidates": [
                {
                    "url": "https://x.test/post/image-1",
                    "published_at": "2026-03-15 10:01:00",
                    "display_time": "9m",
                    "tweet_text": "image one",
                },
                {
                    "url": "https://x.test/post/image-2",
                    "published_at": "2026-03-15 09:59:00",
                    "display_time": "11m",
                    "tweet_text": "image two",
                },
            ],
        },
    )

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=2,
        media_kind="image",
    )

    assert exit_code == 0
    items = _prefilter_items(workspace)
    assert len(items) == 2
    for item in items.values():
        assert isinstance(item, dict)
        assert item["workflow"] == "immediate_collect_review"
        assert item["status"] == "link_pending"
        assert item["target_platforms"] == "douyin,xiaohongshu,kuaishou"
    assert runner.sent_candidates[0]["mode"] == "immediate_collect_review"


def test_run_collect_publish_latest_job_returns_failure_without_candidates(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": []},
    )

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=2,
        media_kind="video",
    )

    assert exit_code == 2
    assert _prefilter_items(workspace) == {}
    assert feedbacks[-1]["status"] == "failed"


def test_run_collect_publish_latest_job_fails_when_message_id_missing(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []

    def send_prefilter_without_message_id(**kwargs: object) -> dict[str, object]:
        runner.sent_candidates.append(dict(kwargs))
        return {
            "result": {
                "message_id": 0,
                "chat": {"id": runner.chat_id},
            }
        }

    runner._send_telegram_prefilter_for_candidate = send_prefilter_without_message_id  # type: ignore[assignment]
    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {
            "keyword": DEFAULT_PROFILE,
            "candidates": [
                {
                    "url": "https://x.test/post/video-1",
                    "published_at": "2026-03-15 10:00:00",
                    "display_time": "10m",
                    "tweet_text": "video one",
                },
            ],
        },
    )

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=1,
        media_kind="video",
    )

    assert exit_code == 3
    items = _prefilter_items(workspace)
    assert len(items) == 1
    failed_item = next(iter(items.values()))
    assert isinstance(failed_item, dict)
    assert failed_item["status"] == "send_failed"
    assert failed_item["action"] == "send_failed"
    assert "message_id missing" in str(failed_item["last_error"])
    assert feedbacks[-1]["status"] == "failed"


def test_run_collect_publish_latest_job_stops_after_requested_failed_new_candidates(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    attempts: list[dict[str, object]] = []

    def send_prefilter_failure(**kwargs: object) -> dict[str, object]:
        attempts.append(dict(kwargs))
        raise RuntimeError(f"telegram send failed {len(attempts)}")

    runner._send_telegram_prefilter_for_candidate = send_prefilter_failure  # type: ignore[assignment]
    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {
            "keyword": DEFAULT_PROFILE,
            "candidates": [
                {
                    "url": "https://x.test/post/video-1",
                    "published_at": "2026-03-15 10:00:00",
                    "display_time": "10m",
                    "tweet_text": "video one",
                },
                {
                    "url": "https://x.test/post/video-2",
                    "published_at": "2026-03-15 09:58:00",
                    "display_time": "12m",
                    "tweet_text": "video two",
                },
                {
                    "url": "https://x.test/post/video-3",
                    "published_at": "2026-03-15 09:55:00",
                    "display_time": "15m",
                    "tweet_text": "video three",
                },
            ],
        },
    )

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=2,
        media_kind="video",
    )

    assert exit_code == 3
    assert len(attempts) == 2
    items = _prefilter_items(workspace)
    assert len(items) == 2
    assert all(isinstance(item, dict) and item["status"] == "send_failed" for item in items.values())
    failed_feedback = feedbacks[-1]
    assert failed_feedback["status"] == "failed"
    sections = failed_feedback["sections"]
    assert isinstance(sections, list)
    failure_items = sections[-1]["items"]
    assert "已尝试发送前 2 条新候选，但 Telegram 预审卡未成功送达。" in failure_items
    assert "当前更像 Telegram 网络抖动，而不是 X 候选扫描失败。" in failure_items
    assert any("telegram send failed 2" in str(item) for item in failure_items)


def test_build_immediate_fast_x_download_args(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        worker_impl.core,
        "_load_runtime_config",
        lambda path: {
            "x_download": {
                "socket_timeout_seconds": 25,
                "extractor_retries": 2,
                "download_retries": 2,
                "fragment_retries": 2,
                "retry_sleep_seconds": 1.0,
                "batch_retry_sleep_seconds": 1.0,
            }
        },
    )

    extra_args = worker_impl._build_immediate_fast_x_download_args(tmp_path)

    assert extra_args[extra_args.index("--x-download-socket-timeout") + 1] == "25"
    assert extra_args[extra_args.index("--x-download-extractor-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-fragment-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-retry-sleep") + 1] == "1"
    assert extra_args[extra_args.index("--x-download-batch-retry-sleep") + 1] == "1"


def test_send_telegram_prefilter_for_candidate_fallback_keeps_buttons(monkeypatch, tmp_path: Path) -> None:
    attempts: list[dict[str, object]] = []

    def send_card(settings: object, card: dict[str, object], **kwargs: object) -> dict[str, object]:
        attempts.append({"card": dict(card), "kwargs": dict(kwargs)})
        if len(attempts) == 1:
            raise RuntimeError("primary send failed")
        return {"result": {"message_id": 777, "chat": {"id": CHAT_ID}}}

    monkeypatch.setattr(pipeline, "_send_telegram_card_message", send_card)
    result = pipeline._send_telegram_prefilter_for_candidate(
        workspace=SimpleNamespace(root=tmp_path),
        email_settings=SimpleNamespace(
            enabled=True,
            telegram_bot_token=BOT_TOKEN,
            telegram_chat_id=CHAT_ID,
            telegram_timeout_seconds=20,
            telegram_api_base="",
        ),
        source_url="https://x.test/post/video-1",
        item_id="item-video",
        idx=1,
        total=3,
        platform_hint="视频号 / 抖音",
        mode="immediate_manual_publish",
        tweet_text="video one <b>tag</b>",
        published_at="2026-03-15 10:00:00",
        display_time="10m",
        target_platforms="wechat,douyin,xiaohongshu,kuaishou,bilibili",
        fast_send=True,
    )

    assert result["result"]["message_id"] == 777
    assert len(attempts) == 2
    assert attempts[0]["kwargs"]["max_attempts"] == 1
    assert attempts[0]["kwargs"]["api_retries"] == 0
    assert attempts[0]["kwargs"]["timeout_seconds_override"] == 8
    assert attempts[1]["kwargs"]["max_attempts"] == 1
    assert attempts[1]["kwargs"]["api_retries"] == 0
    assert attempts[1]["kwargs"]["timeout_seconds_override"] == 8
    primary_card = attempts[0]["card"]
    fallback_card = attempts[1]["card"]
    assert isinstance(primary_card, dict)
    assert isinstance(fallback_card, dict)
    assert fallback_card["reply_markup"] == primary_card["reply_markup"]
    assert fallback_card["parse_mode"] == ""
    assert "链接：https://x.test/post/video-1" in str(fallback_card["text"])


def test_send_telegram_prefilter_for_candidate_fast_send_shortens_primary_send(monkeypatch, tmp_path: Path) -> None:
    attempts: list[dict[str, object]] = []

    def send_card(settings: object, card: dict[str, object], **kwargs: object) -> dict[str, object]:
        attempts.append({"card": dict(card), "kwargs": dict(kwargs)})
        return {"result": {"message_id": 778, "chat": {"id": CHAT_ID}}}

    monkeypatch.setattr(pipeline, "_send_telegram_card_message", send_card)
    result = pipeline._send_telegram_prefilter_for_candidate(
        workspace=SimpleNamespace(root=tmp_path),
        email_settings=SimpleNamespace(
            enabled=True,
            telegram_bot_token=BOT_TOKEN,
            telegram_chat_id=CHAT_ID,
            telegram_timeout_seconds=20,
            telegram_api_base="",
        ),
        source_url="https://x.test/post/video-fast",
        item_id="item-video-fast",
        idx=1,
        total=1,
        platform_hint="video",
        mode="immediate_manual_publish",
        tweet_text="video fast",
        published_at="2026-03-15 10:00:00",
        display_time="1m",
        target_platforms="wechat,douyin",
        fast_send=True,
    )

    assert result["result"]["message_id"] == 778
    assert len(attempts) == 1
    assert attempts[0]["kwargs"]["max_attempts"] == 1
    assert attempts[0]["kwargs"]["api_retries"] == 0
    assert attempts[0]["kwargs"]["timeout_seconds_override"] == 8


def test_handle_prefilter_publish_normal_queues_publish_job(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)
    approvals: list[dict[str, object]] = []
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_item_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {"ok": True, "pid": 410, "log_path": str(workspace / "runtime" / "logs" / "publish.log")},
    )

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    result = commands.handle_callback_update(
        update=_make_callback_update("ctpf|publish_normal|item-video"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(spawned) == 1
    assert len(approvals) == 1
    assert approvals[0]["media_kind"] == "video"
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_requested"
    assert updated["action"] == "publish_normal"
    assert updated["wechat_declare_original"] is False
    assert len(record.updated_cards) == 1


def test_handle_prefilter_publish_original_sets_wechat_original(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    _install_transport_mocks(monkeypatch)
    approvals: list[dict[str, object]] = []
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_item_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {"ok": True, "pid": 411, "log_path": str(workspace / "runtime" / "logs" / "publish-original.log")},
    )

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    commands.handle_callback_update(
        update=_make_callback_update("ctpf|publish_original|item-video"),
        **_worker_kwargs(workspace),
    )

    assert len(spawned) == 1
    assert approvals[0]["media_kind"] == "video"
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_requested"
    assert updated["action"] == "publish_original"
    assert updated["wechat_declare_original"] is True


def test_handle_prefilter_publish_propagates_immediate_test_mode(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: None)
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_item_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {"ok": True, "pid": 412, "log_path": str(workspace / "runtime" / "logs" / "publish-test.log")},
    )

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    commands.handle_callback_update(
        update=_make_callback_update("ctpf|publish_normal|item-video"),
        **_worker_kwargs(workspace, immediate_test_mode=True),
    )

    assert len(spawned) == 1
    assert spawned[0]["immediate_test_mode"] is True
    assert len(record.updated_cards) == 1
    assert "\u6d4b\u8bd5\u6a21\u5f0f" in str(record.updated_cards[0]["card"]["text"])


def test_handle_prefilter_image_up_queues_collect_job(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_collect_item_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {"ok": True, "pid": 512, "log_path": str(workspace / "runtime" / "logs" / "collect-image.log")},
    )

    item = _image_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    commands.handle_callback_update(
        update=_make_callback_update("ctpf|up|item-image"),
        **_worker_kwargs(workspace),
    )

    assert len(spawned) == 1
    updated = _prefilter_items(workspace)["item-image"]
    assert isinstance(updated, dict)
    assert updated["status"] == "collect_requested"
    assert updated["action"] == "up"
    assert len(record.updated_cards) == 1
    assert "\u56fe\u7247\u91c7\u96c6\u5df2\u6392\u961f" in str(record.updated_cards[0]["card"]["text"])


def test_run_immediate_publish_item_job_queues_platform_results(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    spawned_platforms: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "_preflight_immediate_platform_login", lambda **kwargs: {"ready": True})
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_platform_job",
        lambda **kwargs: spawned_platforms.append(dict(kwargs)) or {
            "ok": True,
            "pid": 700 + len(spawned_platforms),
            "log_path": str(workspace / "runtime" / "logs" / f"{kwargs['platform']}.log"),
        },
    )

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_publish_item_job(
        runner=object(),
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
    )

    assert exit_code == 0
    assert {entry["platform"] for entry in spawned_platforms} == {"wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili"}
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_running"
    platform_results = updated["platform_results"]
    assert isinstance(platform_results, dict)
    for platform in ["wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili"]:
        assert platform_results[platform]["status"] == "queued"


def test_run_immediate_publish_item_job_test_mode_bypasses_duplicate_filter(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    spawned_platforms: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(
        worker_impl,
        "_preflight_immediate_platform_login",
        lambda **kwargs: {"ready": True},
    )
    monkeypatch.setattr(
        fake_core,
        "_is_uploaded_content_duplicate",
        lambda workspace_ctx, target, *, platform: (True, {"_reason": "duplicate"}, None),
    )
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_platform_job",
        lambda **kwargs: spawned_platforms.append(dict(kwargs))
        or {"ok": True, "pid": 700 + len(spawned_platforms), "log_path": str(workspace / "runtime" / "logs" / f"{kwargs['platform']}.log")},
    )

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_publish_item_job(
        runner=object(),
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
        immediate_test_mode=True,
    )

    assert exit_code == 0
    assert {entry["platform"] for entry in spawned_platforms} == {"wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili"}
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_running"
    platform_results = updated["platform_results"]
    assert isinstance(platform_results, dict)
    for platform in ["wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili"]:
        assert platform_results[platform]["status"] == "queued"


def test_run_immediate_publish_item_job_default_mode_keeps_duplicate_filter(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    spawned_platforms: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(
        worker_impl,
        "_preflight_immediate_platform_login",
        lambda **kwargs: {"ready": True},
    )
    monkeypatch.setattr(
        fake_core,
        "_is_uploaded_content_duplicate",
        lambda workspace_ctx, target, *, platform: (True, {"_reason": "duplicate"}, None),
    )
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_platform_job",
        lambda **kwargs: spawned_platforms.append(dict(kwargs))
        or {"ok": True, "pid": 800 + len(spawned_platforms), "log_path": str(workspace / "runtime" / "logs" / f"{kwargs['platform']}.log")},
    )

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_publish_item_job(
        runner=object(),
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
    )

    assert exit_code == 2
    assert spawned_platforms == []
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    platform_results = updated["platform_results"]
    assert isinstance(platform_results, dict)
    for platform in ["wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili"]:
        assert platform_results[platform]["status"] == "skipped_duplicate"


def test_run_immediate_publish_item_job_requests_wechat_qr_when_login_required(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    qr_requests: list[dict[str, object]] = []
    spawned_platforms: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs)) or {"ok": True, "needs_login": True, "sent": True},
    )
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_platform_job",
        lambda **kwargs: spawned_platforms.append(dict(kwargs)) or {
            "ok": True,
            "pid": 800 + len(spawned_platforms),
            "log_path": str(workspace / "runtime" / "logs" / f"{kwargs['platform']}.log"),
        },
    )

    monkeypatch.setattr(
        fake_core,
        "probe_platform_session_via_debug_port",
        lambda **kwargs: {
            "status": "login_required",
            "reason": "login_url",
            "current_url": "https://channels.weixin.qq.com/login.html",
            "root_cause_hint": "login_url",
        },
        raising=False,
    )

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_publish_item_job(
        runner=object(),
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
    )

    assert exit_code == 0
    assert len(qr_requests) == 1
    assert qr_requests[0]["platform_name"] == "wechat"
    assert qr_requests[0]["bot_token"] == BOT_TOKEN
    assert qr_requests[0]["chat_id"] == CHAT_ID
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    platform_results = updated["platform_results"]
    assert isinstance(platform_results, dict)
    assert platform_results["wechat"]["status"] == "login_required"
    assert "二维码已发送到 Telegram" in str(platform_results["wechat"]["error"])
    assert {entry["platform"] for entry in spawned_platforms} == {"douyin", "xiaohongshu", "kuaishou", "bilibili"}


def test_publish_platform_job_wechat_failure_requests_qr_and_sends_summary(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []
    qr_requests: list[dict[str, object]] = []

    fake_runner._publish_once = lambda ctx, args, email_settings, platform, target, source, events: events.append(  # type: ignore[attr-defined]
        SimpleNamespace(success=False, error="publish blocked by login page")
    )

    monkeypatch.setattr(worker_impl, "_with_platform_lock", lambda workspace, platform, fn, timeout_seconds: fn())
    monkeypatch.setattr(worker_impl, "_build_immediate_cycle_context", lambda **kwargs: object())
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs)) or {"ok": True, "needs_login": True, "sent": True},
    )

    item = _video_item(target_platforms="wechat")
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = worker_impl._publish_immediate_candidate_platform(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
        platform="wechat",
    )

    assert exit_code == 2
    assert len(qr_requests) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_failed"
    assert updated["platform_results"]["wechat"]["status"] == "login_required"
    assert "二维码已发送到 Telegram" in str(updated["platform_results"]["wechat"]["error"])
    assert len(feedbacks) == 2
    assert "视频号需要重新登录" in str(feedbacks[0]["title"])
    assert "即采即发发布失败" in str(feedbacks[1]["title"])


def test_publish_platform_job_wechat_failure_keeps_original_error_when_qr_probe_fails(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []
    qr_requests: list[dict[str, object]] = []
    original_error = "未能确认合集选择成功: 赛博皮卡精选; 当前字段值=-; 读取来源=empty"

    fake_runner._publish_once = lambda ctx, args, email_settings, platform, target, source, events: events.append(  # type: ignore[attr-defined]
        SimpleNamespace(success=False, error=original_error)
    )

    monkeypatch.setattr(worker_impl, "_with_platform_lock", lambda workspace, platform, fn, timeout_seconds: fn())
    monkeypatch.setattr(worker_impl, "_build_immediate_cycle_context", lambda **kwargs: object())
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs))
        or {"ok": False, "needs_login": True, "sent": False, "error": "Connection aborted"},
    )

    item = _video_item(target_platforms="wechat")
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = worker_impl._publish_immediate_candidate_platform(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
        platform="wechat",
    )

    assert exit_code == 2
    assert len(qr_requests) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["platform_results"]["wechat"]["status"] == "failed"
    assert updated["platform_results"]["wechat"]["error"] == original_error
    assert len(feedbacks) == 2
    assert "视频号发布失败" in str(feedbacks[0]["title"])
    assert "即采即发发布失败" in str(feedbacks[1]["title"])


def test_publish_platform_job_success_sends_platform_feedback_and_summary(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []

    fake_runner._publish_once = lambda ctx, args, email_settings, platform, target, source, events: events.append(  # type: ignore[attr-defined]
        SimpleNamespace(success=True, published_at="18:45:00", publish_id="CT-SUCCESS")
    )

    monkeypatch.setattr(worker_impl, "_with_platform_lock", lambda workspace, platform, fn, timeout_seconds: fn())
    monkeypatch.setattr(worker_impl, "_build_immediate_cycle_context", lambda **kwargs: object())
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))

    item = _video_item(target_platforms="kuaishou")
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = worker_impl._publish_immediate_candidate_platform(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
        platform="kuaishou",
    )

    assert exit_code == 0
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_done"
    assert updated["platform_results"]["kuaishou"]["status"] == "success"
    assert len(feedbacks) == 2
    assert "快手发布已确认" in str(feedbacks[0]["title"])
    assert "即采即发已全部完成" in str(feedbacks[1]["title"])


def test_run_immediate_collect_item_job_test_mode_keeps_real_collect_and_adopts_download(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedback_cards: list[dict[str, object]] = []
    approvals: list[dict[str, object]] = []
    collect_calls: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    def run_unified_once(**kwargs: object) -> dict[str, object]:
        collect_calls.append(dict(kwargs))
        downloaded = workspace / "1_Downloads_Images" / "fresh-image.jpg"
        downloaded.parent.mkdir(parents=True, exist_ok=True)
        downloaded.write_text("image", encoding="utf-8")
        return {"status": "success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedback_cards.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))

    item = _image_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_collect_item_job(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-image",
        immediate_test_mode=True,
    )

    assert exit_code == 0
    assert len(collect_calls) == 1
    assert len(approvals) == 1
    updated = _prefilter_items(workspace)["item-image"]
    assert isinstance(updated, dict)
    assert updated["status"] == "up_confirmed"
    assert updated["immediate_test_mode"] is True
    assert updated["processed_name"] == "fresh-image.jpg"
    assert (workspace / "2_Processed_Images" / "fresh-image.jpg").exists()
    assert len(feedback_cards) == 1
    assert "\u56fe\u7247\u91c7\u96c6\u5df2\u5b8c\u6210" in str(feedback_cards[0]["title"])
    assert "\u6d4b\u8bd5\u6a21\u5f0f" in str(feedback_cards[0]["subtitle"])


def test_run_immediate_collect_item_job_retries_transient_x_metadata_failure(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    collect_calls: list[dict[str, object]] = []
    feedbacks: list[dict[str, object]] = []
    approvals: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "time", SimpleNamespace(sleep=lambda seconds: None, monotonic=worker_impl.time.monotonic, time=worker_impl.time.time))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))

    def run_unified_once(**kwargs: object) -> dict[str, object]:
        collect_calls.append(dict(kwargs))
        if len(collect_calls) == 1:
            return {"stderr": "ERROR: unable to download JSON metadata: UNEXPECTED_EOF_WHILE_READING"}
        processed = workspace / "2_Processed" / "retry-ok.mp4"
        processed.write_text("video", encoding="utf-8")
        return {"status": "success"}

    def queue_immediate_platform_jobs(**kwargs: object) -> dict[str, object]:
        item_id = str(kwargs["item_id"])
        updated = worker_impl._update_prefilter_item(
            workspace,
            item_id,
            updates={
                "status": "publish_running",
                "platform_results": {"wechat": {"status": "queued"}},
                "action": "publish",
            },
        )
        return {"spawned": 1, "failed": 0, "skipped_duplicate": 0, "item": updated}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)
    monkeypatch.setattr(worker_impl, "_queue_immediate_platform_jobs", queue_immediate_platform_jobs)

    item = _video_item(video_name="", processed_name="", status="publish_requested")
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_collect_item_job(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
    )

    assert exit_code == 0
    assert len(collect_calls) == 2
    assert len(approvals) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_running"
    assert updated["processed_name"] == "retry-ok.mp4"
    assert feedbacks[-1]["status"] == "running"


def test_run_immediate_collect_item_job_passes_fast_x_download_args(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    collect_calls: list[dict[str, object]] = []
    approvals: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_queue_immediate_platform_jobs",
        lambda **kwargs: {
            "spawned": 1,
            "failed": 0,
            "skipped_duplicate": 0,
            "item": worker_impl._update_prefilter_item(
                workspace,
                str(kwargs["item_id"]),
                updates={
                    "status": "publish_running",
                    "platform_results": {"wechat": {"status": "queued"}},
                    "action": "publish",
                },
            ),
        },
    )

    def run_unified_once(**kwargs: object) -> dict[str, object]:
        collect_calls.append(dict(kwargs))
        processed = workspace / "2_Processed" / "fast-video.mp4"
        processed.parent.mkdir(parents=True, exist_ok=True)
        processed.write_text("video", encoding="utf-8")
        return {"status": "success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)

    item = _video_item(video_name="", processed_name="", status="publish_requested", source_url="https://x.test/post/fast-video")
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_collect_item_job(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
    )

    assert exit_code == 0
    assert len(approvals) == 1
    extra_args = list(collect_calls[0]["extra_args"])
    assert extra_args[extra_args.index("--x-download-socket-timeout") + 1] == "25"
    assert extra_args[extra_args.index("--x-download-extractor-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-fragment-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-batch-retry-sleep") + 1] == "1"


def test_handle_prefilter_publish_spawn_failure_rolls_back(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)

    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_spawn_immediate_publish_item_job", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("publish boom")))

    item = _video_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    commands.handle_callback_update(
        update=_make_callback_update("ctpf|publish_normal|item-video"),
        **_worker_kwargs(workspace),
    )

    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "link_pending"
    assert updated["action"] == "publish_spawn_failed"
    assert "publish boom" in str(updated["last_error"])
    assert len(record.updated_cards) == 2
    assert "\u5373\u91c7\u5373\u53d1\u542f\u52a8\u5931\u8d25" in str(record.updated_cards[-1]["card"]["text"])


def test_prefilter_queue_lock_recovers_when_owner_pid_is_dead(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    queue_path = state.prefilter_queue_path(workspace)
    lock_dir = queue_path.with_name(f"{queue_path.name}.lock")
    lock_dir.mkdir(parents=True)
    owner_payload = {
        "pid": 999999,
        "host": worker_impl.socket.gethostname(),
        "path": str(queue_path),
        "created_at": "2026-03-15 00:00:00",
        "acquired_epoch": 0,
    }
    (lock_dir / "owner.json").write_text(json.dumps(owner_payload), encoding="utf-8")

    monkeypatch.setattr(worker_impl, "_pid_is_running", lambda pid: False)

    result = worker_impl._with_prefilter_queue_lock(
        workspace,
        lambda queue: queue["items"].setdefault("item-video", {"id": "item-video", "status": "link_pending"}),
        timeout_seconds=1,
    )

    assert result == {"id": "item-video", "status": "link_pending"}
    payload = prefilter.load_queue(queue_path)
    assert "item-video" in payload["items"]
    assert not lock_dir.exists()


def test_atomic_write_json_cleans_tmp_file_when_replace_fails(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "payload.json"
    tmp_paths: list[Path] = []
    original_replace = os.replace

    def failing_replace(src: os.PathLike[str] | str, dst: os.PathLike[str] | str) -> None:
        tmp_paths.append(Path(src))
        raise PermissionError("replace blocked")

    monkeypatch.setattr(worker_impl.os, "replace", failing_replace)

    try:
        worker_impl._atomic_write_json(path, {"ok": True})
    except PermissionError:
        pass
    else:
        raise AssertionError("expected PermissionError")
    finally:
        monkeypatch.setattr(worker_impl.os, "replace", original_replace)

    assert tmp_paths, "temporary file path should be captured"
    assert not tmp_paths[0].exists()
