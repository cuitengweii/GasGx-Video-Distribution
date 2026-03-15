import argparse
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

    def _is_uploaded_content_duplicate(self, workspace_ctx: object, target: Path, *, platform: str) -> tuple[bool, dict, None]:
        return False, {}, None


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
        "telegram_bot_identifier": "cybercar",
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
        telegram_bot_identifier="cybercar",
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
        telegram_bot_identifier="cybercar",
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
        telegram_bot_identifier="cybercar",
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
        telegram_bot_identifier="cybercar",
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
    primary_card = attempts[0]["card"]
    fallback_card = attempts[1]["card"]
    assert isinstance(primary_card, dict)
    assert isinstance(fallback_card, dict)
    assert fallback_card["reply_markup"] == primary_card["reply_markup"]
    assert fallback_card["parse_mode"] == ""
    assert "链接：https://x.test/post/video-1" in str(fallback_card["text"])


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
        telegram_bot_identifier="cybercar",
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
        telegram_bot_identifier="cybercar",
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
