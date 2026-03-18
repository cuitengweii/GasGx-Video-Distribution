from __future__ import annotations

from pathlib import Path

from Collection.cybercar.cybercar_video_capture_and_publishing_module import telegram_command_worker as worker_impl
from cybercar.telegram import prefilter, state


BOT_TOKEN = "123456:abcdefghijklmnopqrstuvwxyzABCDE"
CHAT_ID = "chat-1"
DEFAULT_PROFILE = "cybertruck"


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


def _make_message_update(
    text: str,
    *,
    update_id: int = 1,
    chat_id: str = CHAT_ID,
    username: str = "tester",
) -> dict[str, object]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": 88,
            "date": 0,
            "chat": {"id": chat_id, "type": "private"},
            "from": {"id": 1001, "username": username},
            "text": text,
        },
    }


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


def test_handle_command_update_shared_x_link_queues_immediate_collect_job(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    replies: list[dict[str, object]] = []
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: replies.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_collect_item_job",
        lambda **kwargs: spawned.append(dict(kwargs))
        or {"ok": True, "pid": 531, "log_path": str(workspace / "runtime" / "logs" / "shared-link.log")},
    )

    result = worker_impl.handle_command_update(
        update=_make_message_update("https://x.com/tester/status/2033331774894358749"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(spawned) == 1
    assert len(replies) == 1
    assert replies[0]["parse_mode"] == "HTML"
    assert "视频即采即发" in str(replies[0]["text"])
    item_id = str(spawned[0]["item_id"])
    item = _prefilter_items(workspace)[item_id]
    assert isinstance(item, dict)
    assert item["status"] == "publish_requested"
    assert item["action"] == "shared_link"
    assert item["source_url"] == "https://x.com/tester/status/2033331774894358749"
    assert item["target_platforms"] == "wechat,douyin,xiaohongshu,kuaishou,bilibili"


def test_handle_command_update_shared_x_user_status_link_queues_immediate_collect_job(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    replies: list[dict[str, object]] = []
    spawned: list[dict[str, object]] = []
    shared_url = "https://x.com/TeslaHype/status/2034131604747043265?s=20"

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: replies.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_collect_item_job",
        lambda **kwargs: spawned.append(dict(kwargs))
        or {"ok": True, "pid": 533, "log_path": str(workspace / "runtime" / "logs" / "shared-link-user.log")},
    )

    result = worker_impl.handle_command_update(
        update=_make_message_update(shared_url),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(spawned) == 1
    assert len(replies) == 1
    assert replies[0]["parse_mode"] == "HTML"
    item_id = str(spawned[0]["item_id"])
    item = _prefilter_items(workspace)[item_id]
    assert isinstance(item, dict)
    assert item["source_url"] == "https://x.com/TeslaHype/status/2034131604747043265"


def test_handle_command_update_shared_x_image_link_uses_image_pipeline(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    replies: list[dict[str, object]] = []
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: replies.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_probe_shared_link_media_kind", lambda *_args, **_kwargs: "image")
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_collect_item_job",
        lambda **kwargs: spawned.append(dict(kwargs))
        or {"ok": True, "pid": 534, "log_path": str(workspace / "runtime" / "logs" / "shared-link-image.log")},
    )

    result = worker_impl.handle_command_update(
        update=_make_message_update("https://x.com/tester/status/2033331774894358749"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(spawned) == 1
    assert len(replies) == 1
    assert replies[0]["parse_mode"] == "HTML"
    assert "图片即采即发" in str(replies[0]["text"])
    item_id = str(spawned[0]["item_id"])
    item = _prefilter_items(workspace)[item_id]
    assert isinstance(item, dict)
    assert item["media_kind"] == "image"
    assert item["target_platforms"] == "douyin,xiaohongshu,kuaishou"


def test_handle_command_update_shared_x_link_deduplicates_running_item(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    replies: list[dict[str, object]] = []
    spawned: list[dict[str, object]] = []
    source_url = "https://x.com/tester/status/2033331774894358749"
    item_id = worker_impl._build_immediate_candidate_item_id(source_url, "", "video")

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: replies.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_collect_item_job",
        lambda **kwargs: spawned.append(dict(kwargs))
        or {"ok": True, "pid": 532, "log_path": str(workspace / "runtime" / "logs" / "shared-link.log")},
    )

    _save_prefilter_items(
        workspace,
        {
            item_id: _video_item(
                item_id=item_id,
                source_url=source_url,
                published_at="",
                display_time="Telegram 分享",
                status="download_running",
                action="shared_link",
                target_platforms="wechat,douyin,xiaohongshu,kuaishou,bilibili",
            )
        },
    )

    result = worker_impl.handle_command_update(
        update=_make_message_update(f"转发链接 {source_url}"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert spawned == []
    assert len(replies) == 1
    assert "分享链接已在处理中" in str(replies[0]["text"])
    item = _prefilter_items(workspace)[item_id]
    assert isinstance(item, dict)
    assert item["status"] == "download_running"
