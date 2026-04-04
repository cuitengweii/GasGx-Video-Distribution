import argparse
import json
import os
from datetime import timedelta
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


def _write_profiles_config(repo_root: Path, payload: dict[str, object]) -> None:
    config_dir = repo_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "profiles.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


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


def _flush_platform_result_events(workspace: Path, runner: FakeRunner) -> int:
    args = worker_impl._build_immediate_publish_args(
        runner=runner,
        workspace=workspace,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
    )
    email_settings = runner._build_email_settings(args)
    return worker_impl._flush_pending_platform_result_events(
        workspace=workspace,
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        runner=runner,
        email_settings=email_settings,
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
        "workflow": "immediate_manual_publish",
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


def _reply_markup_texts(reply_markup: dict[str, object]) -> list[str]:
    rows = reply_markup.get("inline_keyboard", [])
    if not isinstance(rows, list):
        return []
    texts: list[str] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        for button in row:
            if isinstance(button, dict):
                texts.append(str(button.get("text") or ""))
    return texts


def _reply_markup_callback_data(reply_markup: dict[str, object]) -> list[str]:
    rows = reply_markup.get("inline_keyboard", [])
    if not isinstance(rows, list):
        return []
    callback_data_values: list[str] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        for button in row:
            if isinstance(button, dict):
                callback_data_values.append(str(button.get("callback_data") or ""))
    return callback_data_values


def test_poll_transport_backoff_grows_and_caps() -> None:
    samples = [
        worker_impl._compute_poll_transport_backoff_seconds(
            consecutive_failures=failures,
            base_interval_seconds=0,
            max_backoff_seconds=30,
        )
        for failures in (1, 2, 3, 4, 5, 8)
    ]
    assert samples == [1, 2, 4, 8, 16, 30]
    assert (
        worker_impl._compute_poll_transport_backoff_seconds(
            consecutive_failures=3,
            base_interval_seconds=12,
            max_backoff_seconds=30,
        )
        == 12
    )


def test_exception_text_falls_back_to_exception_class_name() -> None:
    assert worker_impl._exception_text(RuntimeError()) == "RuntimeError"
    assert worker_impl._exception_text(RuntimeError("boom")) == "boom"


def test_telegram_transport_error_text_detects_gateway_failures() -> None:
    assert worker_impl._is_telegram_transport_error_text("telegram getUpdates failed: Bad Gateway")
    assert worker_impl._is_telegram_transport_error_text("HTTP 504 Gateway Timeout")
    assert not worker_impl._is_telegram_transport_error_text("telegram getUpdates failed: Forbidden")


def test_telegram_rate_limit_error_text_and_retry_after_parsing() -> None:
    error_text = "telegram getUpdates failed: Too Many Requests: retry after 5"
    assert worker_impl._is_telegram_rate_limit_error_text(error_text)
    assert worker_impl._extract_telegram_retry_after_seconds(error_text) == 5
    assert worker_impl._extract_telegram_retry_after_seconds("telegram getUpdates failed: Too Many Requests") == 0


def test_telegram_poll_conflict_error_text_detects_conflict() -> None:
    assert worker_impl._is_telegram_poll_conflict_error_text(
        "telegram getUpdates failed: Conflict: terminated by other getUpdates request; make sure that only one bot instance is running"
    )
    assert not worker_impl._is_telegram_poll_conflict_error_text("telegram getUpdates failed: Forbidden")


def test_answer_callback_query_ignores_stale_query_error(monkeypatch) -> None:
    monkeypatch.setattr(
        worker_impl,
        "_telegram_api",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "telegram answerCallbackQuery failed: Bad Request: query is too old and response timeout expired or query ID is invalid"
            )
        ),
    )
    worker_impl._answer_callback_query(
        bot_token=BOT_TOKEN,
        query_id="cb-1",
        text="ok",
        timeout_seconds=10,
    )


def test_answer_callback_query_keeps_non_stale_error(monkeypatch) -> None:
    monkeypatch.setattr(
        worker_impl,
        "_telegram_api",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("telegram answerCallbackQuery failed: Forbidden")),
    )
    try:
        worker_impl._answer_callback_query(
            bot_token=BOT_TOKEN,
            query_id="cb-1",
            text="ok",
            timeout_seconds=10,
        )
    except RuntimeError as exc:
        assert "Forbidden" in str(exc)
    else:
        raise AssertionError("expected non-stale callback error to be raised")


def test_should_log_poll_transport_warning_first_failures_then_interval() -> None:
    now = 1000.0
    assert worker_impl._should_log_poll_transport_warning(
        consecutive_failures=1,
        last_logged_epoch=999.0,
        now_epoch=now,
        min_interval_seconds=60,
    )
    assert worker_impl._should_log_poll_transport_warning(
        consecutive_failures=3,
        last_logged_epoch=999.0,
        now_epoch=now,
        min_interval_seconds=60,
    )
    assert not worker_impl._should_log_poll_transport_warning(
        consecutive_failures=4,
        last_logged_epoch=980.0,
        now_epoch=now,
        min_interval_seconds=60,
    )
    assert worker_impl._should_log_poll_transport_warning(
        consecutive_failures=4,
        last_logged_epoch=930.0,
        now_epoch=now,
        min_interval_seconds=60,
    )


def test_poll_network_restart_requires_sustained_failure_span() -> None:
    exc = RuntimeError("HTTPSConnectionPool(host='api.telegram.org'): ConnectionResetError(10054)")
    assert not worker_impl._should_restart_after_poll_error(
        exc,
        consecutive_failures=6,
        threshold=6,
        failure_span_seconds=120,
        min_failure_span_seconds=600,
    )
    assert worker_impl._should_restart_after_poll_error(
        exc,
        consecutive_failures=6,
        threshold=6,
        failure_span_seconds=610,
        min_failure_span_seconds=600,
    )
    assert worker_impl._should_restart_after_poll_error(
        exc,
        consecutive_failures=6,
        threshold=6,
        failure_span_seconds=0,
        min_failure_span_seconds=0,
    )


def test_poll_network_restart_ignores_rate_limit_error() -> None:
    exc = RuntimeError("telegram getUpdates failed: Too Many Requests: retry after 5")
    assert not worker_impl._should_restart_after_poll_error(
        exc,
        consecutive_failures=10,
        threshold=6,
        failure_span_seconds=900,
        min_failure_span_seconds=600,
    )


def test_poll_network_restart_ignores_conflict_error() -> None:
    exc = RuntimeError(
        "telegram getUpdates failed: Conflict: terminated by other getUpdates request; make sure that only one bot instance is running"
    )
    assert not worker_impl._should_restart_after_poll_error(
        exc,
        consecutive_failures=10,
        threshold=6,
        failure_span_seconds=900,
        min_failure_span_seconds=600,
    )


def test_refresh_platform_login_qr_message_accepts_telegram_bot_identifier(tmp_path: Path) -> None:
    result = worker_impl._refresh_platform_login_qr_message(
        platform_name="wechat",
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        message_id=0,
        timeout_seconds=30,
        log_file=tmp_path / "telegram_worker.log",
        telegram_bot_identifier="cybercar-main-bot",
    )

    assert result["ok"] is False
    assert result["needs_login"] is True
    assert result["error"] == "invalid qr message id"


def test_refresh_platform_login_qr_message_retries_transport_reset(tmp_path: Path, monkeypatch) -> None:
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    remembered: list[tuple[str, str]] = []
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_core,
        "_prepare_platform_login_qr_notice",
        lambda **_kwargs: {
            "ok": True,
            "needs_login": True,
            "platform": "wechat",
            "filename": "wechat_login_qr.png",
            "mime": "image/png",
            "caption": "scan me",
            "reply_markup": {"inline_keyboard": []},
            "photo_bytes": b"png-bytes",
            "cache_key": "wechat|9334|D:/profiles/wechat",
            "fingerprint": "fp-1",
        },
        raising=False,
    )
    monkeypatch.setattr(
        worker_core,
        "_resolve_runtime_telegram_notify_settings",
        lambda **_kwargs: SimpleNamespace(telegram_api_base="https://api.telegram.org"),
        raising=False,
    )
    monkeypatch.setattr(
        worker_core,
        "_remember_wechat_qr_notice",
        lambda cache_key, fingerprint: remembered.append((str(cache_key), str(fingerprint))),
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_shared_call_telegram_api",
        lambda **kwargs: calls.append(dict(kwargs)) or {"ok": True, "result": {"message_id": 88}},
    )

    result = worker_impl._refresh_platform_login_qr_message(
        platform_name="wechat",
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        message_id=88,
        timeout_seconds=30,
        log_file=tmp_path / "telegram_worker.log",
    )

    assert result["ok"] is True
    assert result["sent"] is True
    assert result["edited"] is True
    assert len(calls) == 1
    assert calls[0]["method"] == "editMessageMedia"
    assert calls[0]["use_post"] is True
    assert calls[0]["max_retries"] == 2
    assert remembered == [("wechat|9334|D:/profiles/wechat", "fp-1")]


def test_refresh_platform_login_qr_message_prefers_wechat_login_entry(tmp_path: Path, monkeypatch) -> None:
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    runtime_ctx_calls: list[bool] = []

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda _core, _platform_name, prefer_login_entry=False: runtime_ctx_calls.append(bool(prefer_login_entry)) or {
            "platform": "wechat",
            "open_url": "https://channels.weixin.qq.com/login.html",
            "debug_port": 9334,
            "chrome_user_data_dir": "D:/profiles/wechat",
        },
    )
    monkeypatch.setattr(
        worker_core,
        "_prepare_platform_login_qr_notice",
        lambda **kwargs: {
            "ok": True,
            "needs_login": True,
            "platform": "wechat",
            "filename": "wechat_login_qr.png",
            "mime": "image/png",
            "caption": "scan me",
            "reply_markup": {"inline_keyboard": []},
            "photo_bytes": b"png-bytes",
            "cache_key": "wechat|9334|D:/profiles/wechat",
            "fingerprint": "fp-login-entry",
            "open_target_url": kwargs.get("open_url"),
        },
        raising=False,
    )
    monkeypatch.setattr(
        worker_core,
        "_resolve_runtime_telegram_notify_settings",
        lambda **_kwargs: SimpleNamespace(telegram_api_base="https://api.telegram.org"),
        raising=False,
    )
    monkeypatch.setattr(worker_core, "_remember_wechat_qr_notice", lambda *_args, **_kwargs: None, raising=False)
    monkeypatch.setattr(
        worker_impl,
        "_shared_call_telegram_api",
        lambda **_kwargs: {"ok": True, "result": {"message_id": 88}},
    )

    result = worker_impl._refresh_platform_login_qr_message(
        platform_name="wechat",
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        message_id=88,
        timeout_seconds=30,
        log_file=tmp_path / "telegram_worker.log",
    )

    assert result["ok"] is True
    assert runtime_ctx_calls == [True]
    assert result["open_target_url"] == "https://channels.weixin.qq.com/login.html"


def test_refresh_platform_login_qr_message_retries_when_fingerprint_unchanged(tmp_path: Path, monkeypatch) -> None:
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    prepare_calls: list[str] = []
    remembered: list[tuple[str, str]] = []
    worker_core.WECHAT_LOGIN_QR_NOTICE_CACHE = {"wechat|9334|d:/profiles/wechat": ("fp-old", 123.0)}

    def fake_prepare(**_kwargs):
        prepare_calls.append("call")
        fingerprint = "fp-old" if len(prepare_calls) == 1 else "fp-new"
        return {
            "ok": True,
            "needs_login": True,
            "platform": "wechat",
            "filename": "wechat_login_qr.png",
            "mime": "image/png",
            "caption": "scan me",
            "reply_markup": {"inline_keyboard": []},
            "photo_bytes": b"png-new" if fingerprint == "fp-new" else b"png-old",
            "cache_key": "wechat|9334|D:/profiles/wechat",
            "fingerprint": fingerprint,
        }

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda _core, _platform_name, prefer_login_entry=False: {
            "platform": "wechat",
            "open_url": "https://channels.weixin.qq.com/login.html",
            "debug_port": 9334,
            "chrome_user_data_dir": "D:/profiles/wechat",
        },
    )
    monkeypatch.setattr(worker_core, "_prepare_platform_login_qr_notice", fake_prepare, raising=False)
    monkeypatch.setattr(
        worker_core,
        "_resolve_runtime_telegram_notify_settings",
        lambda **_kwargs: SimpleNamespace(telegram_api_base="https://api.telegram.org"),
        raising=False,
    )
    monkeypatch.setattr(
        worker_core,
        "_platform_login_qr_cache_key",
        lambda platform_name, debug_port, chrome_user_data_dir: f"{platform_name}|{debug_port}|{chrome_user_data_dir}",
        raising=False,
    )
    monkeypatch.setattr(
        worker_core,
        "_remember_wechat_qr_notice",
        lambda cache_key, fingerprint: remembered.append((str(cache_key), str(fingerprint))),
        raising=False,
    )
    monkeypatch.setattr(worker_impl.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        worker_impl,
        "_shared_call_telegram_api",
        lambda **_kwargs: {"ok": True, "result": {"message_id": 88}},
    )

    result = worker_impl._refresh_platform_login_qr_message(
        platform_name="wechat",
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        message_id=88,
        timeout_seconds=30,
        log_file=tmp_path / "telegram_worker.log",
    )

    assert result["ok"] is True
    assert result["sent"] is True
    assert len(prepare_calls) == 2
    assert remembered == [("wechat|9334|D:/profiles/wechat", "fp-new")]


def test_build_immediate_cycle_context_passes_platform_collection_names(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    target = workspace / "2_Processed_Images" / "sample.jpg"
    target.write_bytes(b"img")
    runtime_config = {
        "collection_name": "Global Collection",
        "collection_names": {
            "kuaishou": "Kuaishou Collection",
            "xiaohongshu": "XHS Collection",
        },
    }

    class StubCore:
        DEFAULT_COLLECTION_NAME = "Default Collection"
        DEFAULT_CHROME_USER_DATA_DIR = "D:/profiles/chrome"
        DEFAULT_EXCLUDE_KEYWORDS = []
        DEFAULT_REQUIRE_ANY_KEYWORDS = []
        SUPPORTED_UPLOAD_PLATFORMS = ("wechat", "douyin", "xiaohongshu", "kuaishou", "bilibili")

        @staticmethod
        def _load_runtime_config(path: str) -> dict[str, object]:
            return runtime_config

        @staticmethod
        def _normalize_keyword_list(raw: object, default: object) -> list[str]:
            return list(default) if isinstance(default, (list, tuple)) else []

        @staticmethod
        def resolve_platform_collection_name(config: dict[str, object], platform: str, *, cli_collection_name: str = "") -> str:
            if cli_collection_name:
                return cli_collection_name
            mapping = config.get("collection_names") if isinstance(config.get("collection_names"), dict) else {}
            value = str(mapping.get(platform, "") or "").strip()
            if value:
                return value
            return str(config.get("collection_name", "") or StubCore.DEFAULT_COLLECTION_NAME)

        @staticmethod
        def init_workspace(path: str) -> SimpleNamespace:
            return SimpleNamespace(root=Path(path))

    args = SimpleNamespace(
        collection_name="",
        chrome_path="",
        chrome_user_data_dir="",
    )

    ctx = worker_impl._build_immediate_cycle_context(
        core=StubCore,
        runner=SimpleNamespace(CycleContext=SimpleNamespace),
        repo_root=tmp_path,
        workspace=workspace,
        args=args,
        target=target,
        candidate_url="https://x.test/post/1",
        profile="cybertruck",
    )

    assert ctx.collection_name == "Global Collection"
    assert ctx.collection_names["kuaishou"] == "Kuaishou Collection"
    assert ctx.collection_names["douyin"] == "Global Collection"


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
    monkeypatch.setattr(worker_impl, "_send_interaction_result_async", lambda **kwargs: record.cards.append(dict(kwargs)))

    def send_loading_placeholder(**kwargs: object) -> int:
        record.placeholders.append(dict(kwargs))
        return 901

    monkeypatch.setattr(worker_impl, "_send_loading_placeholder", send_loading_placeholder)
    monkeypatch.setattr(worker_impl, "_update_callback_message_card", lambda **kwargs: record.updated_cards.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_try_clear_callback_buttons", lambda **kwargs: record.clear_buttons.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: record.replies.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_try_delete_telegram_message", lambda **kwargs: False)
    return record


def test_home_card_and_immediate_menu_include_process_status_button(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)

    home_card = worker_impl._build_home_card(
        default_profile=DEFAULT_PROFILE,
        workspace=workspace,
        chat_id=CHAT_ID,
    )
    immediate_card = worker_impl._build_collect_publish_latest_menu_card(default_profile=DEFAULT_PROFILE)

    assert "📍 进度" in _reply_markup_texts(home_card["reply_markup"])
    assert "📍 进度" in _reply_markup_texts(immediate_card["reply_markup"])


def test_home_feedback_response_includes_process_status_button() -> None:
    card = worker_impl._home_feedback_response(
        status="running",
        title="即采即发进行中",
        subtitle="当前配置：cybertruck",
        detail="后台任务已启动。",
        menu_label="即采即发 / 视频 / 3条",
        task_identifier="collect_publish_latest|video|3|20260317_050000",
    )

    texts = _reply_markup_texts(card["reply_markup"])
    assert "📍 进度" in texts
    assert "🏠 首页" in texts


def test_home_reply_keyboard_uses_same_short_labels_as_inline_actions() -> None:
    keyboard = worker_impl._build_home_reply_keyboard()
    rows = keyboard["keyboard"]
    texts = [str(button.get("text") or "") for row in rows for button in row]

    assert texts == ["🔐 登录", "📍 进度", "🇨🇳 国内即采即发", "🌐 海外即采即发", "💬 点赞评论"]


def test_comment_reply_menu_card_merges_same_count_into_single_action() -> None:
    card = worker_impl._build_comment_reply_menu_card(default_profile=DEFAULT_PROFILE)
    texts = _reply_markup_texts(card["reply_markup"])

    assert "💬 3个" in texts
    assert "💬 5个" in texts
    assert "💬 7个" in texts
    assert "💬 10个" in texts


def test_comment_reply_request_value_normalizes_platform_modes() -> None:
    assert worker_impl._normalize_home_action_value("comment_reply_run", "3") == "all:3"
    assert worker_impl._normalize_home_action_value("comment_reply_run", "all:5") == "all:5"
    assert worker_impl._normalize_home_action_value("comment_reply_run", "wechat:5") == "all:5"
    assert worker_impl._normalize_home_action_value("comment_reply_run", "douyin") == "all:3"
    assert worker_impl._normalize_home_action_value("comment_reply_run", "kuaishou:7") == "all:7"


def test_build_comment_reply_record_texts_includes_post_url_when_available() -> None:
    messages = worker_impl._build_comment_reply_record_texts(
        {
            "platform": "douyin",
            "records": [
                {
                    "post_title": "douyin post",
                    "post_url": "https://www.douyin.com/video/1234567890",
                    "comment_author": "tester",
                    "comment_time": "1m",
                    "comment_preview": "Looks great",
                    "reply_provider": "spark",
                    "reply_text": "Thanks",
                    "replied_at": "2026-03-21 18:00:00",
                }
            ],
        }
    )

    assert any("Post URL: https://www.douyin.com/video/1234567890" in text for text in messages)
    assert any("Provider: spark" in text for text in messages)


def test_build_comment_reply_record_texts_falls_back_to_public_search_url() -> None:
    messages = worker_impl._build_comment_reply_record_texts(
        {
            "platform": "kuaishou",
            "records": [
                {
                    "post_title": "Cybertruck test clip",
                    "comment_author": "tester",
                    "comment_time": "1m",
                    "comment_preview": "Looks great",
                    "reply_provider": "fallback",
                    "reply_text": "Thanks",
                    "replied_at": "2026-03-21 18:00:00",
                }
            ],
        }
    )

    assert any("Search URL: https://www.kuaishou.com/search/video?searchKey=Cybertruck%20test%20clip" in text for text in messages)
    assert any("Provider: fallback" in text for text in messages)


def test_build_comment_reply_result_card_includes_post_link_field() -> None:
    card = worker_impl._build_comment_reply_result_card(
        {
            "ok": True,
            "platform": "douyin",
            "state_path": "douyin_state.json",
            "records": [
                {
                    "post_title": "douyin post",
                    "post_url": "https://www.douyin.com/video/1234567890",
                    "post_published_text": "2026-03-21 18:00:00",
                    "comment_author": "tester",
                    "comment_time": "1m",
                    "comment_preview": "Looks great",
                    "reply_provider": "spark",
                    "reply_text": "Thanks",
                    "replied_at": "2026-03-21 18:01:00",
                }
            ],
            "posts_scanned": 1,
            "posts_selected": 1,
            "replies_sent": 1,
        }
    )

    assert "Post URL" in str(card.get("text") or "")
    assert "https://www.douyin.com/video/1234567890" in str(card.get("text") or "")
    assert "回复来源" in str(card.get("text") or "")
    assert "spark" in str(card.get("text") or "")


def test_normalize_shortcut_text_accepts_new_short_labels() -> None:
    assert worker_impl._normalize_shortcut_text("🔐 登录") == "平台登录"
    assert worker_impl._normalize_shortcut_text("📍 进度") == "进程查看"
    assert worker_impl._normalize_shortcut_text("🇨🇳 国内即采即发") == "国内即采即发"
    assert worker_impl._normalize_shortcut_text("🌐 海外即采即发") == "海外即采即发"
    assert worker_impl._normalize_shortcut_text("⚡ 即采即发") == "即采即发"


def test_normalize_command_key_accepts_new_short_labels() -> None:
    assert worker_impl._normalize_command_key("🔐 登录") == "平台登录"
    assert worker_impl._normalize_command_key("📍 进度") == "进程查看"
    assert worker_impl._normalize_command_key("🇨🇳 国内即采即发") == "国内即采即发"
    assert worker_impl._normalize_command_key("🌐 海外即采即发") == "海外即采即发"
    assert worker_impl._normalize_command_key("⚡ 即采即发") == "即采即发"


def test_handle_command_domestic_shortcut_returns_domestic_route_menu(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    result = worker_impl._handle_command(
        text="国内即采即发",
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        allow_shell=False,
        allow_prefixes=[],
        command_password="",
        started_at=0.0,
        last_processed=0,
        update_id=1,
        chat_id=CHAT_ID,
        username="tester",
        audit_file=workspace / "runtime" / "telegram_command_worker_audit.jsonl",
        default_profile=DEFAULT_PROFILE,
    )

    assert isinstance(result, dict)
    assert worker_impl.DEFAULT_DOMESTIC_COLLECT_PUBLISH_PROFILE in str(result["text"])
    callback_values = _reply_markup_callback_data(result["reply_markup"])
    assert any("collect_publish_latest_domestic" in item for item in callback_values)


def test_handle_command_global_shortcut_returns_global_route_menu(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    result = worker_impl._handle_command(
        text="海外即采即发",
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        allow_shell=False,
        allow_prefixes=[],
        command_password="",
        started_at=0.0,
        last_processed=0,
        update_id=1,
        chat_id=CHAT_ID,
        username="tester",
        audit_file=workspace / "runtime" / "telegram_command_worker_audit.jsonl",
        default_profile=DEFAULT_PROFILE,
    )

    assert isinstance(result, dict)
    assert worker_impl.DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE in str(result["text"])
    callback_values = _reply_markup_callback_data(result["reply_markup"])
    assert any("collect_publish_latest_global" in item for item in callback_values)


def test_refresh_home_surface_on_startup_force_refresh_updates_home_and_shortcut(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    home_state_path = worker_impl._home_state_path(workspace)
    shortcut_state_path = worker_impl._home_shortcut_state_path(workspace)
    worker_impl._save_state(
        home_state_path,
        {
            "chat_id": CHAT_ID,
            "message_id": 2781,
            "recent_message_ids": [2781],
            "surface_version": worker_impl.DEFAULT_HOME_SURFACE_VERSION,
            "updated_at": "2026-03-18 03:36:04",
        },
    )
    worker_impl._save_state(
        shortcut_state_path,
        {
            "chat_id": CHAT_ID,
            "message_id": 3051,
            "recent_message_ids": [3051],
            "keyboard_version": worker_impl.DEFAULT_HOME_SHORTCUT_KEYBOARD_VERSION,
            "surface_version": worker_impl.DEFAULT_HOME_SURFACE_VERSION,
            "updated_at": "2026-03-18 03:36:04",
        },
    )

    sent: list[dict[str, object]] = []
    shortcuts: list[dict[str, object]] = []
    monkeypatch.setattr(worker_impl, "_append_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        worker_impl,
        "send_or_update_home_message",
        lambda **kwargs: sent.append(dict(kwargs)) or {"ok": True, "action": "sent", "message_id": 4001},
    )
    monkeypatch.setattr(
        worker_impl,
        "_ensure_home_shortcut_keyboard",
        lambda **kwargs: shortcuts.append(dict(kwargs)),
    )

    worker_impl._refresh_home_surface_on_startup(
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        workspace=workspace,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        default_profile=DEFAULT_PROFILE,
        force_refresh=True,
    )

    assert len(sent) == 1
    assert sent[0]["force_new"] is True
    assert "已刷新首页状态" in str(sent[0]["card"]["text"])
    assert len(shortcuts) == 1
    assert shortcuts[0]["force_refresh"] is True


def test_build_process_status_card_includes_worker_queue_and_log_sections(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    worker_state_path = workspace / worker_impl.DEFAULT_STATE_FILE
    worker_state_path.parent.mkdir(parents=True, exist_ok=True)
    worker_state_path.write_text(
        json.dumps(
            {
                "status": "polling",
                "worker_heartbeat_at": "2026-03-17 05:02:33",
                "last_processed_update_id": 67138940,
                "consecutive_poll_failures": 0,
                "last_error": "",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    log_path = workspace / "runtime" / "logs" / "home_action_collect_publish_latest_test.log"
    log_path.write_text(
        "\n".join(
            [
                "[2026-03-17 05:02:30] collect started",
                "[2026-03-17 05:02:40] candidate queued",
                "[2026-03-17 05:02:50] publish running",
            ]
        ),
        encoding="utf-8",
    )

    claim = worker_impl._claim_home_action_task(
        workspace=workspace,
        chat_id=CHAT_ID,
        action="collect_publish_latest",
        value="video:3",
        profile=DEFAULT_PROFILE,
        username="tester",
    )
    worker_impl._update_home_action_task(
        workspace,
        str(claim["task_key"]),
        status="running",
        detail="视频即采即发后台任务已启动。",
        log_path=str(log_path),
        pid=321,
    )
    _save_prefilter_items(
        workspace,
        {
            "item-video": _video_item(
                status="publish_running",
                processed_name="clip.mp4",
                updated_at="2099-01-01 00:00:00",
                created_at="2099-01-01 00:00:00",
            )
        },
    )

    card = worker_impl._build_process_status_card(
        default_profile=DEFAULT_PROFILE,
        workspace=workspace,
    )
    text = str(card["text"])

    assert "即采即发进程查看" in text
    assert "当前发布中" in text
    assert "执行状态" in text
    assert "⚠️ 后台任务" in text
    assert "⚠️ 即采即发队列" in text
    assert "Bot 心跳" in text
    assert "当前活跃任务" in text
    assert "即采即发队列" in text
    assert "home_action_collect_publish_latest_test.log" in text
    assert "clip.mp4" in text
    assert "队列更新时间" in text
    assert "✅ 队列更新时间" in text
    assert "✅ 视频｜发布中" in text
    assert "📊 当前积压数" in text
    assert "✅ 发布中" in text
    assert text.index("当前发布中") < text.index("Bot 心跳")
    assert text.index("当前活跃任务") < text.index("Bot 心跳")
    assert "🔄 刷新" in _reply_markup_texts(card["reply_markup"])
    assert "🧹 队列清理" in _reply_markup_texts(card["reply_markup"])


def test_build_process_status_card_groups_ready_signals_when_everything_is_idle(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    worker_state_path = workspace / worker_impl.DEFAULT_STATE_FILE
    worker_state_path.parent.mkdir(parents=True, exist_ok=True)
    worker_state_path.write_text(
        json.dumps(
            {
                "status": "polling",
                "worker_heartbeat_at": "2026-03-21 10:30:00",
                "last_processed_update_id": 67139670,
                "consecutive_poll_failures": 0,
                "last_error": "",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    card = worker_impl._build_process_status_card(
        default_profile=DEFAULT_PROFILE,
        workspace=workspace,
    )
    text = str(card["text"])

    assert "可继续操作" in text
    assert "✅ 全局流水线锁" in text
    assert "✅ 后台任务" in text
    assert "✅ 即采即发队列" in text
    assert "空闲" in text


def test_build_process_status_card_hides_old_prefilter_history(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    _save_prefilter_items(
        workspace,
        {
            "recent-item": _video_item(
                "recent-item",
                status="publish_running",
                processed_name="recent.mp4",
                updated_at="2099-01-01 00:00:00",
                created_at="2099-01-01 00:00:00",
            ),
            "old-item": _video_item(
                "old-item",
                status="publish_done",
                processed_name="old.mp4",
                updated_at="2000-01-01 00:00:00",
                created_at="2000-01-01 00:00:00",
            ),
        },
    )

    card = worker_impl._build_process_status_card(
        default_profile=DEFAULT_PROFILE,
        workspace=workspace,
    )
    text = str(card["text"])

    assert "recent.mp4" in text
    assert "old.mp4" not in text
    assert "当前发布中" in text
    assert "当前积压数" in text
    assert "活跃窗口" in text
    assert "已隐藏 1 条非活跃历史记录" in text


def test_build_runtime_status_section_hides_stale_waiting_prefilter_items(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    now_text = worker_impl._now_text()
    recent_text = (worker_impl._parse_worker_time_text(now_text) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _save_prefilter_items(
        workspace,
        {
            "stale-item": _video_item(
                "stale-item",
                status="download_running",
                action="collect_waiting_lock",
                created_at="2000-01-01 00:00:00",
                updated_at="2000-01-01 00:00:00",
            ),
            "recent-item": _video_item(
                "recent-item",
                status="download_running",
                action="collect_waiting_lock",
                created_at=recent_text,
                updated_at=recent_text,
            ),
        },
    )

    section = worker_impl._build_runtime_status_section(workspace)
    text = "\n".join(
        (str(item.get("label") or "") + str(item.get("value") or "")) if isinstance(item, dict) else str(item)
        for item in section["items"]
    )

    assert "即采即发等待锁1 条｜示例 recent-item" in text
    assert "stale-item" not in text


def test_cleanup_inactive_prefilter_items_removes_hidden_history_and_keeps_live_retry_rows(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    now_text = worker_impl._now_text()
    recent_text = (worker_impl._parse_worker_time_text(now_text) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _save_prefilter_items(
        workspace,
        {
            "stale-item": _video_item(
                "stale-item",
                status="download_running",
                action="collect_waiting_lock",
                created_at="2000-01-01 00:00:00",
                updated_at="2000-01-01 00:00:00",
            ),
            "recent-item": _video_item(
                "recent-item",
                status="download_running",
                action="collect_waiting_lock",
                created_at=recent_text,
                updated_at=recent_text,
            ),
            "pending-item": _video_item(
                "pending-item",
                status="link_pending",
                action="sent",
                source_url="https://x.com/pending/status/123",
                created_at=recent_text,
                updated_at=recent_text,
                message_id=789,
            ),
            "publish-item": _video_item(
                "publish-item",
                status="publish_running",
                created_at="2000-01-01 00:00:00",
                updated_at="2000-01-01 00:00:00",
            ),
            "skipped-item": _video_item(
                "skipped-item",
                status="down_confirmed",
                action="skip",
                created_at=recent_text,
                updated_at=recent_text,
                message_id=123,
            ),
            "retry-item": _video_item(
                "retry-item",
                status="send_failed",
                created_at=recent_text,
                updated_at=recent_text,
                prefilter_retry_pending=True,
            ),
            "overflow-item": _video_item(
                "overflow-item",
                status="link_pending",
                created_at=recent_text,
                updated_at=recent_text,
                message_id=456,
                candidate_index=2,
                candidate_limit=1,
            ),
        },
    )

    summary = worker_impl._cleanup_inactive_prefilter_items(workspace)
    items = _prefilter_items(workspace)

    assert summary["removed_inactive"] == 5
    assert summary["filter_synced"] == 1
    assert summary["removed_ids"] == ["stale-item", "pending-item", "publish-item"]
    assert "stale-item" not in items
    assert "recent-item" in items
    assert "pending-item" not in items
    assert "publish-item" not in items
    assert "skipped-item" not in items
    assert "retry-item" in items
    assert "overflow-item" not in items

    ledger = json.loads((workspace / "candidate_ledger.json").read_text(encoding="utf-8"))
    ledger_items = ledger.get("items", {})
    assert ledger_items["x:123:123"]["state"] == "review_skipped"


def test_cleanup_inactive_prefilter_items_removes_pending_cards_and_syncs_collect_filter(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    now_text = worker_impl._now_text()
    recent_text = (worker_impl._parse_worker_time_text(now_text) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _save_prefilter_items(
        workspace,
        {
            "pending-item": _video_item(
                "pending-item",
                status="link_pending",
                action="sent",
                source_url="https://x.com/pending/status/456",
                created_at=recent_text,
                updated_at=recent_text,
                message_id=777,
            ),
            "recent-item": _video_item(
                "recent-item",
                status="download_running",
                action="collect_waiting_lock",
                created_at=recent_text,
                updated_at=recent_text,
            ),
        },
    )

    summary = worker_impl._cleanup_inactive_prefilter_items(workspace)
    items = _prefilter_items(workspace)

    assert summary["removed_inactive"] == 1
    assert summary["filter_synced"] == 1
    assert "pending-item" not in items
    assert "recent-item" in items

    ledger = json.loads((workspace / "candidate_ledger.json").read_text(encoding="utf-8"))
    ledger_items = ledger.get("items", {})
    assert ledger_items["x:456:456"]["state"] == "review_skipped"


def test_handle_home_process_status_callback_renders_progress_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "process_status"),
    )
    result = commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert result["handled"] is True
    assert len(record.cards) == 1
    assert int(record.cards[0].get("message_id") or 0) == 0
    assert str(record.cards[0].get("inline_message_id") or "") == ""
    card = record.cards[0]["card"]
    assert "即采即发进程查看" in str(card["text"])


def test_handle_home_process_status_refresh_callback_edits_existing_progress_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "process_status_refresh"),
        message_id=321,
    )
    result = commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert result["handled"] is True
    assert len(record.cards) == 1
    assert int(record.cards[0].get("message_id") or 0) == 321
    card = record.cards[0]["card"]
    assert "即采即发进程查看" in str(card["text"])


def test_handle_home_process_status_cleanup_queue_callback_prunes_inactive_items(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    now_text = worker_impl._now_text()
    recent_text = (worker_impl._parse_worker_time_text(now_text) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _save_prefilter_items(
        workspace,
        {
            "stale-item": _video_item(
                "stale-item",
                status="download_running",
                action="collect_waiting_lock",
                created_at="2000-01-01 00:00:00",
                updated_at="2000-01-01 00:00:00",
            ),
            "recent-item": _video_item(
                "recent-item",
                status="download_running",
                action="collect_waiting_lock",
                created_at=recent_text,
                updated_at=recent_text,
            ),
            "skipped-item": _video_item(
                "skipped-item",
                status="down_confirmed",
                action="skip",
                created_at=recent_text,
                updated_at=recent_text,
                message_id=123,
            ),
            "overflow-item": _video_item(
                "overflow-item",
                status="link_pending",
                created_at=recent_text,
                updated_at=recent_text,
                message_id=456,
                candidate_index=2,
                candidate_limit=1,
            ),
        },
    )
    record = _install_transport_mocks(monkeypatch)

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "process_status_cleanup_queue"),
    )
    result = commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert result["handled"] is True
    assert len(record.cards) == 1
    assert "已清理 3 条非活跃队列项" in str(record.cards[0]["card"]["text"])
    items = _prefilter_items(workspace)
    assert "stale-item" not in items
    assert "recent-item" in items
    assert "skipped-item" not in items
    assert "overflow-item" not in items


def test_handle_command_update_process_status_shortcut_preserves_html_parse_mode(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    replies: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: replies.append(dict(kwargs)))

    result = worker_impl.handle_command_update(
        update=_make_message_update("📍 进度"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(replies) == 1
    assert replies[0]["parse_mode"] == "HTML"
    assert "<b>" in str(replies[0]["text"])


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


def test_handle_home_collect_publish_menu_domestic_returns_route_specific_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "collect_publish_latest_menu_domestic"),
    )
    result = commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert result["handled"] is True
    assert len(record.cards) == 1
    card = record.cards[0]["card"]
    assert worker_impl.DEFAULT_DOMESTIC_COLLECT_PUBLISH_PROFILE in str(card["text"])
    callback_values = _reply_markup_callback_data(card["reply_markup"])
    assert any("collect_publish_latest_domestic" in item for item in callback_values)


def test_handle_home_collect_publish_global_routes_to_global_profile(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_spawn_home_action_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {
            "ok": True,
            "pid": 388,
            "log_path": str(workspace / "runtime" / "logs" / "home-global.log"),
        },
    )

    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "collect_publish_latest_global", "video:3"),
    )
    result = commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert result["handled"] is True
    assert len(spawned) == 1
    assert spawned[0]["action"] == "collect_publish_latest"
    assert spawned[0]["profile"] == worker_impl.DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE
    tasks = _action_tasks(workspace)
    task = next(iter(tasks.values()))
    assert isinstance(task, dict)
    assert task["action"] == "collect_publish_latest"
    assert task["profile"] == worker_impl.DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE
    assert task["value"] == "video:3"


def test_resolve_collect_publish_source_platforms_uses_profile_config(tmp_path: Path) -> None:
    repo_root = tmp_path
    _write_profiles_config(
        repo_root,
        {
            "default_profile": "cybertruck",
            "profiles": {
                "x_to_cn": {"source_platforms": "x"},
                "cn_to_global": {"source_platforms": "douyin,xiaohongshu"},
            },
        },
    )

    global_sources = worker_impl._resolve_collect_publish_source_platforms(
        repo_root=repo_root,
        profile="cn_to_global",
    )
    domestic_sources = worker_impl._resolve_collect_publish_source_platforms(
        repo_root=repo_root,
        profile="x_to_cn",
    )

    assert global_sources == ["douyin", "xiaohongshu"]
    assert domestic_sources == ["x"]


def test_discover_latest_live_candidates_global_uses_domestic_sources(tmp_path: Path, monkeypatch) -> None:
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    repo_root = tmp_path
    _write_profiles_config(
        repo_root,
        {
            "default_profile": "cn_to_global",
            "profiles": {
                "cn_to_global": {
                    "keyword": "cybertruck",
                    "source_platforms": "douyin,xiaohongshu",
                }
            },
        },
    )
    domestic_calls: list[str] = []
    x_calls: list[dict[str, object]] = []

    monkeypatch.setattr(worker_core, "_load_runtime_config", lambda _path: {"keyword": "cybertruck"}, raising=False)
    monkeypatch.setattr(worker_core, "DEFAULT_PORT", 9333, raising=False)
    monkeypatch.setattr(worker_core, "DEFAULT_CHROME_USER_DATA_DIR", "", raising=False)
    monkeypatch.setattr(worker_core, "X_DISCOVERY_SCROLL_ROUNDS", 2, raising=False)
    monkeypatch.setattr(worker_core, "X_DISCOVERY_SCROLL_WAIT_SECONDS", 0.1, raising=False)
    monkeypatch.setattr(
        worker_core,
        "discover_domestic_keyword_urls",
        lambda platform, keyword, **kwargs: domestic_calls.append(str(platform)) or [f"https://{platform}.example/{keyword}/1"],
        raising=False,
    )
    monkeypatch.setattr(
        worker_core,
        "discover_x_media_candidates",
        lambda **kwargs: x_calls.append(dict(kwargs)) or [],
        raising=False,
    )
    monkeypatch.setattr(worker_core, "_take_latest_x_candidates", lambda candidates, limit: list(candidates)[:limit], raising=False)

    result = worker_impl._discover_latest_live_candidates(
        repo_root=repo_root,
        timeout_seconds=30,
        profile="cn_to_global",
        candidate_limit=2,
        include_images=False,
    )

    assert result["source_platforms"] == ["douyin", "xiaohongshu"]
    assert domestic_calls == ["douyin", "xiaohongshu"]
    assert x_calls == []
    candidates = result["candidates"]
    assert isinstance(candidates, list)
    assert len(candidates) == 2
    assert {str(item.get("source_platform") or "") for item in candidates} == {"douyin", "xiaohongshu"}


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


def test_handle_home_comment_reply_legacy_platform_callback_spawns_all_platforms(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    _install_transport_mocks(monkeypatch)
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_spawn_home_action_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {
            "ok": True,
            "pid": 777,
            "log_path": str(workspace / "runtime" / "logs" / "home-comment.log"),
        },
    )

    # Simulate stale Telegram inline keyboard callback data.
    update = _make_callback_update(
        commands.build_home_callback_data("cybercar", "comment_reply_run", "wechat:10"),
    )
    commands.handle_callback_update(update=update, **_worker_kwargs(workspace))

    assert len(spawned) == 1
    assert spawned[0]["value"] == "all:10"
    tasks = _action_tasks(workspace)
    task = next(iter(tasks.values()))
    assert isinstance(task, dict)
    assert task["value"] == "all:10"
    assert task["status"] == "running"


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


def test_recover_orphaned_home_action_marks_stale_running_task_blocked(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    sent_cards: list[dict[str, object]] = []
    terminated: list[int] = []
    log_path = workspace / "runtime" / "logs" / "stale_home_action.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("stale", encoding="utf-8")
    stale_epoch = worker_impl.time.time() - (worker_impl.DEFAULT_ACTION_QUEUE_STALE_SECONDS + 120)
    os.utime(log_path, (stale_epoch, stale_epoch))

    monkeypatch.setattr(worker_impl, "_append_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker_impl, "_pid_is_running", lambda pid: True)
    monkeypatch.setattr(worker_impl, "_terminate_pid_best_effort", lambda pid, log_file=None: terminated.append(int(pid)) or True)
    monkeypatch.setattr(worker_impl, "_send_card_message", lambda **kwargs: sent_cards.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_try_delete_telegram_message",
        lambda **kwargs: True,
    )

    claimed = worker_impl._claim_home_action_task(
        workspace=workspace,
        chat_id=CHAT_ID,
        action="collect_publish_latest",
        value="video:1",
        profile=DEFAULT_PROFILE,
        username="tester",
    )
    task_key = str(claimed["task_key"])
    worker_impl._update_home_action_task(
        workspace,
        task_key,
        status="running",
        detail="still running",
        pid=24680,
        log_path=str(log_path),
        extra={"loading_message_id": 902},
    )
    payload = state.load_state(state.action_queue_path(workspace))
    tasks = payload.get("tasks", {})
    assert isinstance(tasks, dict)
    task = tasks.get(task_key, {})
    assert isinstance(task, dict)
    task["updated_epoch"] = stale_epoch
    task["updated_at"] = "2000-01-01 00:00:00"
    tasks[task_key] = task
    payload["tasks"] = tasks
    state.save_state(state.action_queue_path(workspace), payload)

    recovered = worker_impl._recover_orphaned_home_action_tasks(
        workspace=workspace,
        bot_token=BOT_TOKEN,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
    )

    assert recovered == 1
    assert terminated == [24680]
    assert len(sent_cards) == 1
    task = _action_tasks(workspace)[task_key]
    assert isinstance(task, dict)
    assert task["status"] == "blocked"
    assert "30分钟无进展" in str(task["detail"])


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


def test_run_collect_publish_latest_job_global_route_passes_domestic_source_platforms(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    discovered_kwargs: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)

    def discover_latest_live_candidates(**kwargs: object) -> dict[str, object]:
        discovered_kwargs.append(dict(kwargs))
        return {
            "keyword": "cybertruck",
            "source_platforms": ["douyin", "xiaohongshu"],
            "candidates": [
                {
                    "url": "https://www.douyin.com/video/12345",
                    "published_at": "2026-03-15 10:00:00",
                    "display_time": "10m",
                    "tweet_text": "douyin candidate",
                    "source_platform": "douyin",
                }
            ],
        }

    monkeypatch.setattr(worker_impl, "_discover_latest_live_candidates", discover_latest_live_candidates)

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=worker_impl.DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=1,
        media_kind="video",
    )

    assert exit_code == 0
    assert discovered_kwargs
    assert discovered_kwargs[0]["source_platforms"] == ["douyin", "xiaohongshu"]


def test_run_collect_publish_latest_job_prefilter_keeps_candidate_and_marks_wechat_login_warning(tmp_path: Path, monkeypatch) -> None:
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
                    "url": "https://x.test/post/video-1",
                    "published_at": "2026-03-15 10:00:00",
                    "display_time": "10m",
                    "tweet_text": "video one",
                },
            ],
        },
    )
    monkeypatch.setattr(
        worker_impl,
        "_preflight_immediate_platform_login",
        lambda **kwargs: {"ready": False, "error": "视频号未登录；登录二维码已发送到 Telegram"},
    )

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        candidate_limit=1,
        media_kind="video",
    )

    assert exit_code == 0
    items = _prefilter_items(workspace)
    assert len(items) == 1
    item = next(iter(items.values()))
    assert isinstance(item, dict)
    assert item["status"] == "link_pending"
    assert "视频号预检" in str(item["prefilter_warning"])
    platform_results = item["platform_results"]
    assert isinstance(platform_results, dict)
    assert platform_results["wechat"]["status"] == "login_required"
    assert len(runner.sent_candidates) == 1
    assert "视频号预检" in str(runner.sent_candidates[0]["prefilter_warning"])


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


def test_run_collect_publish_latest_job_default_mode_reissues_existing_active_prefilter_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    discovered_kwargs: list[dict[str, object]] = []
    feedbacks: list[dict[str, object]] = []
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
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))

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
    assert len(runner.sent_candidates) == 1
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "publish_partial"
    assert item["action"] == "resent_existing_card"
    assert item["message_id"] != 913
    assert item["message_id"] > 0
    assert item["platform_results"] == {"wechat": {"status": "success"}}
    assert feedbacks[-1]["sections"][0]["items"][3]["value"] == "1 条"
    assert "重发当前状态卡" in feedbacks[-1]["sections"][1]["items"][0]


def test_run_collect_publish_latest_job_default_mode_keeps_existing_link_pending_card_without_resend(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    candidate = {
        "url": "https://x.test/post/video-reused-orphaned",
        "published_at": "2026-03-15 10:03:00",
        "display_time": "7m",
        "tweet_text": "video reused orphaned",
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
                status="link_pending",
                action="sent",
                message_id=913,
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [candidate]},
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

    assert exit_code == 0
    assert runner.sent_candidates == []
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "link_pending"
    assert item["action"] == "sent"
    assert item["message_id"] == 913
    overview_items = feedbacks[-1]["sections"][0]["items"]
    assert any(str(entry.get("value") or "").startswith("1 ") for entry in overview_items if isinstance(entry, dict))


def test_run_collect_publish_latest_job_expires_stale_link_pending_card_and_filters_rerun(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    candidate = {
        "url": "https://x.com/repeat/status/111",
        "published_at": "2026-03-15 10:04:00",
        "display_time": "6m",
        "tweet_text": "video stale pending",
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
                status="link_pending",
                action="sent",
                message_id=913,
                created_at="2026-03-15 09:40:00",
                updated_at="2026-03-15 09:49:00",
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [candidate]},
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

    assert exit_code == 0
    assert runner.sent_candidates == []
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "expired_pending"
    assert item["action"] == "expired"
    assert item["message_id"] == 913

    ledger = json.loads((workspace / "candidate_ledger.json").read_text(encoding="utf-8"))
    ledger_items = ledger.get("items", {})
    assert ledger_items["x:111:111"]["state"] == "review_skipped"
    assert ledger_items["x:111:111"]["status_url"] == candidate["url"]


def test_run_collect_publish_latest_job_recovers_orphaned_publish_running_and_reissues_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    candidate = {
        "url": "https://x.test/post/video-orphaned-running",
        "published_at": "2026-03-15 10:03:00",
        "display_time": "7m",
        "tweet_text": "video orphaned running",
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
                status="publish_running",
                action="publish",
                message_id=913,
                platform_results={
                    "wechat": {
                        "status": "queued",
                        "pid": 654321,
                        "log_path": str(workspace / "runtime" / "logs" / "immediate_publish_wechat.log"),
                    },
                    "douyin": {"status": "success"},
                },
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [candidate]},
    )
    monkeypatch.setattr(worker_impl, "_pid_is_running", lambda pid: False if int(pid) == 654321 else True)

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
    assert len(runner.sent_candidates) == 1
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "publish_partial"
    assert item["action"] == "resent_existing_card"
    assert item["message_id"] != 913
    platform_results = item["platform_results"]
    assert isinstance(platform_results, dict)
    assert platform_results["wechat"]["status"] == "failed"
    assert "后台发布进程已退出" in str(platform_results["wechat"]["error"])
    overview_items = feedbacks[-1]["sections"][0]["items"]
    assert any(str(entry.get("value") or "").startswith("1 ") for entry in overview_items if isinstance(entry, dict))


def test_run_collect_publish_latest_job_recovers_orphaned_publish_running_from_success_log(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    candidate = {
        "url": "https://x.test/post/video-orphaned-success",
        "published_at": "2026-03-15 10:03:00",
        "display_time": "7m",
        "tweet_text": "video orphaned success",
    }
    existing_id = worker_impl._build_immediate_candidate_item_id(candidate["url"], candidate["published_at"], "video")
    log_path = workspace / "runtime" / "logs" / "immediate_publish_wechat.log"
    log_path.write_text(
        "\n".join(
            [
                "[2026-03-18 14:18:33] [Uploader:wechat] Publish feedback inferred by post-submit state.",
                "[2026-03-18 14:18:33] [Success:wechat] 发布已确认。",
            ]
        ),
        encoding="utf-8",
    )

    _save_prefilter_items(
        workspace,
        {
            existing_id: _video_item(
                existing_id,
                source_url=candidate["url"],
                published_at=candidate["published_at"],
                display_time=candidate["display_time"],
                tweet_text=candidate["tweet_text"],
                status="publish_running",
                action="publish",
                message_id=914,
                platform_results={
                    "wechat": {
                        "status": "queued",
                        "pid": 654322,
                        "log_path": str(log_path),
                    },
                    "douyin": {"status": "success"},
                },
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [candidate]},
    )
    monkeypatch.setattr(worker_impl, "_pid_is_running", lambda pid: False if int(pid) == 654322 else True)

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
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "publish_done"
    platform_results = item["platform_results"]
    assert isinstance(platform_results, dict)
    assert platform_results["wechat"]["status"] == "success"
    assert platform_results["wechat"]["published_at"] == "14:18:33"
    assert "error" not in platform_results["wechat"]


def test_run_collect_publish_latest_job_reissues_active_publish_running_card(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    candidate = {
        "url": "https://x.test/post/video-active-running",
        "published_at": "2026-03-15 10:03:00",
        "display_time": "7m",
        "tweet_text": "video active running",
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
                status="publish_running",
                action="publish",
                message_id=913,
                platform_results={
                    "wechat": {
                        "status": "queued",
                        "pid": 654321,
                        "log_path": str(workspace / "runtime" / "logs" / "immediate_publish_wechat.log"),
                    },
                },
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [candidate]},
    )
    monkeypatch.setattr(worker_impl, "_pid_is_running", lambda pid: True if int(pid) == 654321 else False)

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
    assert len(runner.sent_candidates) == 1
    item = _prefilter_items(workspace)[existing_id]
    assert isinstance(item, dict)
    assert item["status"] == "publish_running"
    assert item["action"] == "resent_existing_card"
    assert item["message_id"] != 913
    assert item["message_id"] > 0
    overview_items = feedbacks[-1]["sections"][0]["items"]
    assert any(str(entry.get("value") or "").startswith("1 ") for entry in overview_items if isinstance(entry, dict))


def test_run_collect_publish_latest_job_keeps_reusing_existing_card_when_new_prefilter_sent(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    candidate_fresh = {
        "url": "https://x.test/post/video-fresh",
        "published_at": "2026-03-15 10:04:00",
        "display_time": "6m",
        "tweet_text": "video fresh",
    }
    candidate_reused = {
        "url": "https://x.test/post/video-reused-existing",
        "published_at": "2026-03-15 10:03:00",
        "display_time": "7m",
        "tweet_text": "video reused existing",
    }
    reused_id = worker_impl._build_immediate_candidate_item_id(candidate_reused["url"], candidate_reused["published_at"], "video")

    _save_prefilter_items(
        workspace,
        {
            reused_id: _video_item(
                reused_id,
                source_url=candidate_reused["url"],
                published_at=candidate_reused["published_at"],
                display_time=candidate_reused["display_time"],
                tweet_text=candidate_reused["tweet_text"],
                status="publish_partial",
                action="publish",
                message_id=913,
                platform_results={"wechat": {"status": "success"}},
            ),
        },
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [candidate_fresh, candidate_reused]},
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
    assert len(runner.sent_candidates) == 1
    item = _prefilter_items(workspace)[reused_id]
    assert isinstance(item, dict)
    assert item["action"] == "publish"
    assert item["message_id"] == 913


def test_update_prefilter_item_persists_row_changes(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    item_id = "item-persisted"
    _save_prefilter_items(
        workspace,
        {
            item_id: _video_item(
                item_id,
                status="publish_partial",
                action="publish",
                message_id=913,
            ),
        },
    )

    updated = worker_impl._update_prefilter_item(
        workspace,
        item_id,
        updates={
            "action": "resent_existing_card",
            "message_id": 1201,
        },
    )

    assert updated["action"] == "resent_existing_card"
    assert updated["message_id"] == 1201
    persisted = _prefilter_items(workspace)[item_id]
    assert isinstance(persisted, dict)
    assert persisted["action"] == "resent_existing_card"
    assert persisted["message_id"] == 1201


def test_update_prefilter_item_runs_coordination_snapshot_outside_queue_lock(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    item_id = "item-lock-scope"
    _save_prefilter_items(
        workspace,
        {
            item_id: _video_item(
                item_id,
                status="publish_partial",
                action="publish",
                message_id=913,
            ),
        },
    )

    original_lock = worker_impl._with_prefilter_queue_lock
    state = {"inside_lock": False, "calls": []}

    def wrapped_lock(workspace_arg: Path, callback, *, timeout_seconds: float = 30):
        assert state["inside_lock"] is False
        state["inside_lock"] = True
        try:
            return original_lock(workspace_arg, callback, timeout_seconds=timeout_seconds)
        finally:
            state["inside_lock"] = False

    def fake_snapshot(item: dict[str, object], workspace_arg: Path) -> dict[str, object]:
        assert workspace_arg == workspace
        state["calls"].append(state["inside_lock"])
        row = dict(item)
        row["coordination_summary"] = "outside-lock"
        return row

    monkeypatch.setattr(worker_impl, "_with_prefilter_queue_lock", wrapped_lock)
    monkeypatch.setattr(worker_impl, "_apply_coordination_snapshot", fake_snapshot)

    updated = worker_impl._update_prefilter_item(workspace, item_id, updates={"message_id": 1202})

    assert updated["coordination_summary"] == "outside-lock"
    assert state["calls"] == [False]
    persisted = _prefilter_items(workspace)[item_id]
    assert isinstance(persisted, dict)
    assert persisted["coordination_summary"] == "outside-lock"


def test_upsert_immediate_candidate_item_runs_coordination_snapshot_outside_queue_lock(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    original_lock = worker_impl._with_prefilter_queue_lock
    state = {"inside_lock": False, "calls": []}

    def wrapped_lock(workspace_arg: Path, callback, *, timeout_seconds: float = 30):
        assert state["inside_lock"] is False
        state["inside_lock"] = True
        try:
            return original_lock(workspace_arg, callback, timeout_seconds=timeout_seconds)
        finally:
            state["inside_lock"] = False

    def fake_snapshot(item: dict[str, object], workspace_arg: Path) -> dict[str, object]:
        assert workspace_arg == workspace
        state["calls"].append(state["inside_lock"])
        row = dict(item)
        row["coordination_summary"] = "outside-lock-upsert"
        return row

    monkeypatch.setattr(worker_impl, "_with_prefilter_queue_lock", wrapped_lock)
    monkeypatch.setattr(worker_impl, "_apply_coordination_snapshot", fake_snapshot)

    payload = worker_impl._upsert_immediate_candidate_item(
        workspace=workspace,
        candidate={
            "url": "https://x.test/post/video-lock-scope",
            "published_at": "2026-03-15 10:05:00",
            "display_time": "5m",
            "tweet_text": "lock scope",
        },
        profile=DEFAULT_PROFILE,
        media_kind="video",
        target_platforms="wechat,douyin",
        chat_id=CHAT_ID,
        item_index=1,
        total_count=1,
    )

    item = payload["item"]
    assert isinstance(item, dict)
    assert item["coordination_summary"] == "outside-lock-upsert"
    assert state["calls"] == [False]
    persisted = _prefilter_items(workspace)[str(payload["item_id"])]
    assert isinstance(persisted, dict)
    assert persisted["coordination_summary"] == "outside-lock-upsert"


def test_run_collect_publish_latest_job_image_records_publish_items(tmp_path: Path, monkeypatch) -> None:
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
        assert item["workflow"] == "immediate_manual_publish"
        assert item["status"] == "link_pending"
        assert item["target_platforms"] == "douyin,xiaohongshu,kuaishou"
    assert runner.sent_candidates[0]["mode"] == "immediate_manual_publish"


def test_run_collect_publish_latest_job_image_filters_out_video_candidates(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    probe_calls: list[str] = []

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {
            "keyword": DEFAULT_PROFILE,
            "candidates": [
                {"url": "https://x.com/mix/status/101", "published_at": "2026-03-15T10:05:00Z", "display_time": "1m", "tweet_text": "video one"},
                {"url": "https://x.com/mix/status/102", "published_at": "2026-03-15T10:04:00Z", "display_time": "2m", "tweet_text": "video two"},
                {"url": "https://x.com/mix/status/103", "published_at": "2026-03-15T10:03:00Z", "display_time": "3m", "tweet_text": "video three"},
                {"url": "https://x.com/mix/status/104", "published_at": "2026-03-15T10:02:00Z", "display_time": "4m", "tweet_text": "video four"},
                {"url": "https://x.com/mix/status/105", "published_at": "2026-03-15T10:01:00Z", "display_time": "5m", "tweet_text": "image one"},
            ],
        },
    )

    def probe_media_kind(url: str) -> str:
        probe_calls.append(url)
        return "image" if url.endswith("/105") else "video"

    monkeypatch.setattr(worker_impl, "_probe_shared_link_media_kind", probe_media_kind)

    exit_code = actions.run_collect_publish_latest_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token="",
        telegram_chat_id=CHAT_ID,
        candidate_limit=5,
        media_kind="image",
    )

    assert exit_code == 0
    assert len(runner.sent_candidates) == 1
    items = _prefilter_items(workspace)
    assert len(items) == 1
    only_item = next(iter(items.values()))
    assert isinstance(only_item, dict)
    assert only_item["source_url"] == "https://x.com/mix/status/105"
    assert only_item["media_kind"] == "image"
    assert set(probe_calls) == {
        "https://x.com/mix/status/101",
        "https://x.com/mix/status/102",
        "https://x.com/mix/status/103",
        "https://x.com/mix/status/104",
        "https://x.com/mix/status/105",
    }


def test_build_telegram_prefilter_reply_markup_hides_original_for_image_publish() -> None:
    reply_markup = pipeline._build_telegram_prefilter_reply_markup(
        "https://x.test/post/image-1",
        "item-image",
        mode="immediate_manual_publish",
        target_platforms="douyin,xiaohongshu,kuaishou",
    )

    texts = _reply_markup_texts(reply_markup)
    assert "⚡ 发布" in texts
    assert "⏭ 跳过" in texts
    assert "📝 原创" not in texts


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
    assert failed_item["prefilter_retry_pending"] is True
    assert failed_item["prefilter_retry_count"] == 0
    assert int(failed_item["message_id"]) == 0
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
    assert len(attempts) == 4
    assert attempts[0]["fast_send"] is True
    assert attempts[1]["fast_send"] is False
    assert attempts[2]["fast_send"] is True
    assert attempts[3]["fast_send"] is False
    items = _prefilter_items(workspace)
    assert len(items) == 2
    assert all(isinstance(item, dict) and item["status"] == "send_failed" for item in items.values())
    assert all(isinstance(item, dict) and item["prefilter_retry_pending"] is True for item in items.values())
    failed_feedback = feedbacks[-1]
    assert failed_feedback["status"] == "failed"
    sections = failed_feedback["sections"]
    assert isinstance(sections, list)
    failure_items = sections[-1]["items"]
    assert "已尝试发送前 2 条新候选，但 Telegram 预审卡未成功送达。" in failure_items
    assert "当前更像 Telegram 网络抖动，而不是候选扫描失败。" in failure_items
    assert "失败候选已保留到待补发队列；worker 轮询恢复后会自动重试送达预审卡。" in failure_items
    assert any("telegram send failed 4" in str(item) for item in failure_items)


def test_run_collect_publish_latest_job_succeeds_after_immediate_retry(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    attempts: list[dict[str, object]] = []

    def send_prefilter_retry_once(**kwargs: object) -> dict[str, object]:
        attempts.append(dict(kwargs))
        if len(attempts) == 1:
            raise RuntimeError("telegram fast send failed")
        return {
            "result": {
                "message_id": 889,
                "chat": {"id": CHAT_ID},
            }
        }

    runner._send_telegram_prefilter_for_candidate = send_prefilter_retry_once  # type: ignore[assignment]
    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {
            "keyword": DEFAULT_PROFILE,
            "candidates": [
                {
                    "url": "https://x.test/post/video-retry-success",
                    "published_at": "2026-03-15 10:00:00",
                    "display_time": "10m",
                    "tweet_text": "video retry success",
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

    assert exit_code == 0
    assert len(attempts) == 2
    assert attempts[0]["fast_send"] is True
    assert attempts[1]["fast_send"] is False
    items = _prefilter_items(workspace)
    assert len(items) == 1
    item = next(iter(items.values()))
    assert isinstance(item, dict)
    assert item["status"] == "link_pending"
    assert item["action"] == "sent_after_retry"
    assert item["last_error"] == ""
    assert item["prefilter_retry_pending"] is False
    assert int(item["message_id"]) == 889
    assert feedbacks[-1]["status"] == "done"


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
    assert "--no-x-download-fail-fast" in extra_args
    assert "--x-download-fail-fast" not in extra_args


def test_extract_attempt_reason_prefers_x_metadata_failure_over_no_new_files() -> None:
    result = {
        "stdout": "Warning: yt-dlp exited with 1 and no new files.",
        "stderr": "ERROR: [twitter] 2033332422322897018: Unable to download JSON metadata: HTTPSConnectionPool(host='x.com', port=443): Read timed out. (read timeout=25.0)",
    }

    reason = worker_impl._extract_attempt_reason(result)

    assert reason == "X 元数据下载失败：请求超时。"


def test_flush_pending_prefilter_retries_resends_failed_candidate(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    log_file = workspace / "runtime" / "logs" / "telegram_worker.log"

    item = _video_item(
        "item-video",
        status="send_failed",
        action="send_failed",
        message_id=0,
        prefilter_retry_pending=True,
        prefilter_retry_count=0,
        prefilter_last_retry_epoch=0.0,
        last_error="telegram timeout",
    )
    _save_prefilter_items(workspace, {str(item["id"]): item})

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))

    worker_impl._flush_pending_prefilter_retries(
        workspace=workspace,
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=log_file,
    )

    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "link_pending"
    assert updated["action"] == "resent"
    assert updated["prefilter_retry_pending"] is False
    assert updated["prefilter_retry_count"] == 1
    assert int(updated["message_id"]) > 0
    assert len(runner.sent_candidates) == 1
    assert runner.sent_candidates[0]["fast_send"] is False


def test_cleanup_prefilter_queue_removes_old_send_failed_and_polluted_rows(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    _save_prefilter_items(
        workspace,
        {
            "old-send-failed": _video_item(
                "old-send-failed",
                status="send_failed",
                action="send_failed",
                message_id=0,
                created_at="2000-01-01 00:00:00",
                updated_at="2000-01-01 00:00:00",
            ),
            "polluted-item": {
                "id": "polluted-item",
                "workflow": "immediate_manual_publish",
                "status": "link_pending",
                "message_id": 0,
            },
            "active-item": _video_item("active-item", status="link_pending", updated_at="2099-01-01 00:00:00"),
        },
    )

    summary = worker_impl._cleanup_prefilter_queue(workspace)
    items = _prefilter_items(workspace)

    assert summary["removed_terminal"] == 1
    assert summary["removed_polluted"] == 1
    assert set(items) == {"active-item"}


def test_cleanup_prefilter_queue_removes_old_publish_terminal_rows(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    _save_prefilter_items(
        workspace,
        {
            "old-partial": _video_item(
                "old-partial",
                status="publish_partial",
                updated_at="2000-01-01 00:00:00",
                created_at="2000-01-01 00:00:00",
            ),
            "old-done": _video_item(
                "old-done",
                status="publish_done",
                updated_at="2000-01-01 00:00:00",
                created_at="2000-01-01 00:00:00",
            ),
            "recent-running": _video_item(
                "recent-running",
                status="publish_running",
                updated_at="2099-01-01 00:00:00",
                created_at="2099-01-01 00:00:00",
            ),
        },
    )

    summary = worker_impl._cleanup_prefilter_queue(workspace)
    items = _prefilter_items(workspace)

    assert summary["removed_terminal"] == 2
    assert summary["removed_polluted"] == 0
    assert set(items) == {"recent-running"}


def test_run_periodic_queue_maintenance_skips_when_interval_not_reached(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    calls: list[str] = []

    monkeypatch.setattr(worker_impl, "_prune_home_action_tasks", lambda workspace: calls.append("tasks") or 2)
    monkeypatch.setattr(
        worker_impl,
        "_cleanup_prefilter_queue",
        lambda workspace, log_file=None: calls.append("prefilter") or {"removed_terminal": 1, "removed_polluted": 1},
    )

    result = worker_impl._run_periodic_queue_maintenance(
        workspace,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        last_run_epoch=worker_impl.time.time(),
        interval_seconds=300,
    )

    assert result["ran"] is False
    assert calls == []


def test_run_periodic_queue_maintenance_runs_and_reports_summary(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    calls: list[str] = []

    monkeypatch.setattr(worker_impl, "_prune_home_action_tasks", lambda workspace: calls.append("tasks") or 2)
    monkeypatch.setattr(
        worker_impl,
        "_cleanup_prefilter_queue",
        lambda workspace, log_file=None: calls.append("prefilter") or {"removed_terminal": 3, "removed_polluted": 1},
    )

    result = worker_impl._run_periodic_queue_maintenance(
        workspace,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        last_run_epoch=0.0,
        interval_seconds=300,
        force=True,
    )

    assert result["ran"] is True
    assert result["pruned_tasks"] == 2
    assert result["prefilter_removed_terminal"] == 3
    assert result["prefilter_removed_polluted"] == 1
    assert calls == ["tasks", "prefilter"]


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
    assert attempts[0]["kwargs"]["max_attempts"] == 2
    assert attempts[0]["kwargs"]["api_retries"] == 1
    assert attempts[0]["kwargs"]["timeout_seconds_override"] == 8
    assert attempts[1]["kwargs"]["max_attempts"] == 1
    assert attempts[1]["kwargs"]["api_retries"] == 1
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
    assert attempts[0]["kwargs"]["max_attempts"] == 2
    assert attempts[0]["kwargs"]["api_retries"] == 1
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


def test_handle_prefilter_publish_callback_acks_before_queue_read(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)

    def _raise_lock_timeout(_workspace: Path, _item_id: str) -> dict[str, object]:
        raise TimeoutError(f"Timed out waiting for lock: {workspace / 'runtime' / 'telegram_prefilter_queue.json'}")

    monkeypatch.setattr(worker_impl, "_get_prefilter_item", _raise_lock_timeout)

    result = commands.handle_callback_update(
        update=_make_callback_update("ctpf|publish_normal|item-video"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(record.answers) == 1
    assert "已收到" in str(record.answers[0].get("text") or "")
    assert len(record.updated_cards) == 0


def test_handle_prefilter_publish_normal_queues_image_publish_job(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)
    approvals: list[dict[str, object]] = []
    spawned: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_item_job",
        lambda **kwargs: spawned.append(dict(kwargs)) or {"ok": True, "pid": 415, "log_path": str(workspace / "runtime" / "logs" / "publish-image.log")},
    )

    item = _image_item()
    _save_prefilter_items(workspace, {str(item["id"]): item})

    result = commands.handle_callback_update(
        update=_make_callback_update("ctpf|publish_normal|item-image"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(spawned) == 1
    assert len(approvals) == 0
    updated = _prefilter_items(workspace)["item-image"]
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

    item = _image_item(workflow="immediate_collect_review")
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
    text_notices: list[dict[str, object]] = []
    spawned_platforms: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 901},
    )
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
    assert len(text_notices) == 1
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


def test_preflight_immediate_platform_login_uses_probe_notification_without_duplicate_qr_request(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    qr_requests: list[dict[str, object]] = []
    text_notices: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda core, platform_name: {
            "platform": "wechat",
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )

    fake_core = FakeCore()
    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(
        fake_core,
        "probe_platform_session_via_debug_port",
        lambda **kwargs: {
            "status": "login_required",
            "reason": "login_url",
            "current_url": "https://channels.weixin.qq.com/login.html",
            "notified": True,
            "notification_mode": "qr",
            "qr_result": {"sent": True, "needs_login": True},
        },
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs)) or {"ok": True, "needs_login": True, "sent": True},
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 902},
    )

    result = worker_impl._preflight_immediate_platform_login(
        platform="wechat",
        telegram_bot_identifier="",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_command_worker.log",
    )

    assert result["ready"] is False
    assert "Telegram" in str(result["error"])
    assert len(text_notices) == 1
    assert qr_requests == []


def test_preflight_immediate_platform_login_falls_back_to_status_check_when_probe_raises(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    qr_requests: list[dict[str, object]] = []
    text_notices: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda core, platform_name: {
            "platform": "wechat",
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )

    fake_core = FakeCore()
    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(
        fake_core,
        "probe_platform_session_via_debug_port",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("debug probe failed")),
        raising=False,
    )
    monkeypatch.setattr(
        fake_core,
        "check_platform_login_status",
        lambda **kwargs: {
            "ok": True,
            "needs_login": True,
            "reason": "login_url",
            "url": "https://channels.weixin.qq.com/login.html",
        },
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs)) or {"ok": True, "needs_login": True, "sent": True},
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 903},
    )

    result = worker_impl._preflight_immediate_platform_login(
        platform="wechat",
        telegram_bot_identifier="",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_command_worker.log",
    )

    assert result["ready"] is False
    assert "Telegram" in str(result["error"])
    assert len(text_notices) == 1
    assert len(qr_requests) == 1


def test_preflight_immediate_platform_login_sends_text_notice_before_qr(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    events: list[str] = []

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda core, platform_name: {
            "platform": "wechat",
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )

    fake_core = FakeCore()
    monkeypatch.setattr(worker_impl, "core", fake_core)
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
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: events.append("text") or {"ok": True, "sent": True, "message_id": 904},
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: events.append("qr")
        or {
            "ok": False,
            "needs_login": True,
            "transport_error": True,
            "sent": False,
            "error": "ConnectionResetError(10054)",
        },
    )

    result = worker_impl._preflight_immediate_platform_login(
        platform="wechat",
        telegram_bot_identifier="",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_command_worker.log",
    )

    assert result["ready"] is False
    assert events == ["text", "qr"]
    assert "已向 Telegram 发送登录提醒" in str(result["error"])
    assert "Telegram 网络抖动导致二维码暂未送达" in str(result["error"])


def test_run_comment_reply_job_routes_to_douyin_engagement(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    sent_texts: list[str] = []
    sent_cards: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: sent_texts.append(str(kwargs["text"])) or None)
    monkeypatch.setattr(worker_impl, "_send_card_message", lambda **kwargs: sent_cards.append(dict(kwargs)) or None)
    monkeypatch.setattr(worker_impl, "_build_comment_reply_result_card", lambda result: {"result": result})
    monkeypatch.setattr(
        worker_impl,
        "_load_engagement_module",
        lambda: SimpleNamespace(
            run_douyin_engagement=lambda **kwargs: {
                "ok": True,
                "platform": "douyin",
                "records": [
                    {
                        "post_title": "CyberTruck clip",
                        "comment_author": "tester",
                        "comment_time": "1h",
                        "comment_preview": "Looks great",
                        "reply_text": "Thanks!",
                        "replied_at": "2026-03-19 10:00:00",
                    }
                ],
                "posts_scanned": 1,
                "posts_selected": 1,
                "replies_sent": 1,
            }
        ),
    )

    exit_code = worker_impl._run_comment_reply_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_identifier="",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        platform="douyin",
        post_limit=1,
    )

    assert exit_code == 0
    assert any("目标平台：抖音" in text for text in sent_texts)
    assert any("最近 1 个有评论视频" in text for text in sent_texts)
    assert any("[抖音] Reply 1" in text for text in sent_texts)
    assert any("Comment: Looks great" in text for text in sent_texts)
    assert sent_cards[-1]["card"]["result"]["platform"] == "douyin"


def test_run_comment_reply_job_all_runs_three_platforms(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    sent_texts: list[str] = []
    sent_cards: list[dict[str, object]] = []

    class FakeCommentCore(FakeCore):
        def _load_runtime_config(self, path: str) -> dict[str, object]:
            return {"comment_reply": {"debug": True}}

        def run_wechat_comment_reply(self, **kwargs: object) -> dict[str, object]:
            return {
                "ok": True,
                "platform": "wechat",
                "state_path": "wechat_state.json",
                "markdown_path": "wechat_records.md",
                "records": [{"post_title": "wechat post", "comment_author": "w", "comment_time": "1m", "comment_preview": "cw", "reply_text": "rw", "replied_at": "2026-03-19 10:00:00"}],
                "posts_scanned": 1,
                "posts_selected": 1,
                "replies_sent": 1,
            }

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: sent_texts.append(str(kwargs["text"])) or None)
    monkeypatch.setattr(worker_impl, "_send_card_message", lambda **kwargs: sent_cards.append(dict(kwargs)) or None)
    monkeypatch.setattr(worker_impl, "_build_comment_reply_result_card", lambda result: {"result": result})
    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (SimpleNamespace(), FakeCommentCore()))
    monkeypatch.setattr(
        worker_impl,
        "_load_engagement_module",
        lambda: SimpleNamespace(
            run_douyin_engagement=lambda **kwargs: {
                "ok": True,
                "platform": "douyin",
                "state_path": "douyin_state.json",
                "markdown_path": "douyin_records.md",
                "records": [{"post_title": "douyin post", "comment_author": "d", "comment_time": "2m", "comment_preview": "cd", "reply_text": "rd", "replied_at": "2026-03-19 10:01:00"}],
                "posts_scanned": 2,
                "posts_selected": 1,
                "replies_sent": 1,
            },
            run_kuaishou_engagement=lambda **kwargs: {
                "ok": True,
                "platform": "kuaishou",
                "state_path": "kuaishou_state.json",
                "markdown_path": "kuaishou_records.md",
                "records": [{"post_title": "kuaishou post", "comment_author": "k", "comment_time": "3m", "comment_preview": "ck", "reply_text": "rk", "replied_at": "2026-03-19 10:02:00"}],
                "posts_scanned": 3,
                "posts_selected": 2,
                "replies_sent": 1,
            },
        ),
    )

    exit_code = worker_impl._run_comment_reply_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_identifier="",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        platform="all",
        post_limit=3,
    )

    assert exit_code == 0
    assert any("目标平台：视频号 / 抖音 / 快手" in text for text in sent_texts)
    assert any("[视频号] Reply 1" in text for text in sent_texts)
    assert any("[抖音] Reply 2" in text for text in sent_texts)
    assert any("[快手] Reply 3" in text for text in sent_texts)
    aggregate = sent_cards[-1]["card"]["result"]
    assert aggregate["platform"] == "all"
    assert aggregate["replies_sent"] == 3
    assert len(aggregate["platform_results"]) == 3


def test_run_comment_reply_job_routes_to_kuaishou_engagement(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    sent_cards: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_send_reply", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_send_card_message", lambda **kwargs: sent_cards.append(dict(kwargs)) or None)
    monkeypatch.setattr(worker_impl, "_build_comment_reply_result_card", lambda result: {"result": result})
    monkeypatch.setattr(
        worker_impl,
        "_load_engagement_module",
        lambda: SimpleNamespace(
            run_kuaishou_engagement=lambda **kwargs: {
                "ok": True,
                "platform": "kuaishou",
                "records": [],
                "posts_scanned": 1,
                "posts_selected": 1,
                "replies_sent": 1,
            }
        ),
    )

    exit_code = worker_impl._run_comment_reply_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_identifier="",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        platform="kuaishou",
        post_limit=1,
    )

    assert exit_code == 0
    assert sent_cards[-1]["card"]["result"]["platform"] == "kuaishou"


def test_build_process_prefilter_section_hides_unsent_and_terminal_history(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    now_text = worker_impl._now_text()
    recent_text = (worker_impl._parse_worker_time_text(now_text) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _save_prefilter_items(
        workspace,
        {
            "unsent-link": {
                "id": "unsent-link",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "link_pending",
                "created_at": recent_text,
                "updated_at": recent_text,
                "tweet_text": "pending but unsent",
                "message_id": 0,
            },
            "active-publish": {
                "id": "active-publish",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "publish_running",
                "created_at": recent_text,
                "updated_at": recent_text,
                "processed_name": "active.mp4",
            },
            "partial-history": {
                "id": "partial-history",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "publish_partial",
                "created_at": "2026-03-18 08:40:00",
                "updated_at": "2026-03-18 08:40:00",
                "processed_name": "partial.mp4",
            },
            "done-history": {
                "id": "done-history",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "publish_done",
                "created_at": "2026-03-18 08:35:00",
                "updated_at": "2026-03-18 08:35:00",
                "processed_name": "done.mp4",
            },
        },
    )

    section = worker_impl._build_process_prefilter_section(workspace)

    assert section["title"] == "即采即发队列"
    text = "\n".join(
        (str(item.get("label") or "") + str(item.get("value") or "")) if isinstance(item, dict) else str(item)
        for item in section["items"]
    )
    assert "当前积压数1" in text
    assert "发布中1" in text
    assert "待人工确认" not in text
    assert "部分完成" not in text
    assert "全部完成" not in text


def test_build_process_prefilter_section_reports_empty_when_only_history_exists(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    now_text = worker_impl._now_text()
    recent_text = (worker_impl._parse_worker_time_text(now_text) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _save_prefilter_items(
        workspace,
        {
            "unsent-link": {
                "id": "unsent-link",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "link_pending",
                "created_at": recent_text,
                "updated_at": recent_text,
                "tweet_text": "pending but unsent",
                "message_id": 0,
            },
            "partial-history": {
                "id": "partial-history",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "publish_partial",
                "created_at": "2026-03-18 08:40:00",
                "updated_at": "2026-03-18 08:40:00",
                "processed_name": "partial.mp4",
            },
        },
    )

    section = worker_impl._build_process_prefilter_section(workspace)

    assert section["title"] == "即采即发队列"
    assert any("当前没有活跃的即采即发积压" in str(item) for item in section["items"])


def test_build_process_prefilter_section_hides_skipped_down_confirmed_items(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    now_text = worker_impl._now_text()
    recent_text = (worker_impl._parse_worker_time_text(now_text) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _save_prefilter_items(
        workspace,
        {
            "skipped-review": {
                "id": "skipped-review",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "down_confirmed",
                "action": "skip",
                "created_at": recent_text,
                "updated_at": recent_text,
                "processed_name": "skipped.mp4",
                "message_id": 123,
            },
            "pending-publish": {
                "id": "pending-publish",
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "down_confirmed",
                "created_at": recent_text,
                "updated_at": recent_text,
                "processed_name": "pending.mp4",
                "message_id": 124,
            },
        },
    )

    section = worker_impl._build_process_prefilter_section(workspace)

    text = "\n".join(
        (str(item.get("label") or "") + str(item.get("value") or "")) if isinstance(item, dict) else str(item)
        for item in section["items"]
    )
    assert "当前积压数1" in text
    assert "pending.mp4" in text
    assert "skipped.mp4" not in text


def test_upsert_immediate_candidate_preserves_skipped_terminal_state(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    candidate = {
        "url": "https://x.test/post/skipped",
        "published_at": "2026-03-18 17:45:00",
        "display_time": "1m",
        "tweet_text": "skipped candidate",
        "match_mode": "keyword",
        "matched_keyword": "cybertruck",
    }
    item_id = worker_impl._build_immediate_candidate_item_id(candidate["url"], candidate["published_at"])
    _save_prefilter_items(
        workspace,
        {
            item_id: {
                "id": item_id,
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "down_confirmed",
                "action": "skip",
                "created_at": "2026-03-18 17:45:12",
                "updated_at": "2026-03-18 17:45:36",
                "tweet_text": "old skipped row",
                "message_id": 321,
            }
        },
    )

    result = worker_impl._upsert_immediate_candidate_item(
        workspace=workspace,
        candidate=candidate,
        media_kind="video",
        item_index=1,
        total_count=1,
        profile="cybertruck",
        target_platforms="wechat,douyin",
        chat_id=CHAT_ID,
        allow_reuse=False,
    )

    item = result["item"]
    assert item["status"] == "down_confirmed"
    assert item["action"] == "skip"
    assert item["message_id"] == 321


def test_upsert_immediate_candidate_preserves_publish_done_terminal_state(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    candidate = {
        "url": "https://x.test/post/published",
        "published_at": "2026-03-18 17:45:00",
        "display_time": "1m",
        "tweet_text": "published candidate",
        "match_mode": "keyword",
        "matched_keyword": "cybertruck",
    }
    item_id = worker_impl._build_immediate_candidate_item_id(candidate["url"], candidate["published_at"])
    _save_prefilter_items(
        workspace,
        {
            item_id: {
                "id": item_id,
                "workflow": "immediate_manual_publish",
                "media_kind": "video",
                "status": "publish_done",
                "action": "publish",
                "created_at": "2026-03-18 17:45:12",
                "updated_at": "2026-03-18 17:45:36",
                "tweet_text": "old published row",
                "message_id": 654,
            }
        },
    )

    result = worker_impl._upsert_immediate_candidate_item(
        workspace=workspace,
        candidate=candidate,
        media_kind="video",
        item_index=1,
        total_count=1,
        profile="cybertruck",
        target_platforms="wechat,douyin",
        chat_id=CHAT_ID,
        allow_reuse=True,
    )

    item = result["item"]
    assert item["status"] == "publish_done"
    assert item["action"] == "publish"
    assert item["message_id"] == 654


def test_run_collect_publish_latest_job_filters_candidates_seen_in_collect_ledger(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    sent_urls: list[str] = []
    seen_candidate = {
        "url": "https://x.com/seen/status/111",
        "published_at": "2026-03-20 10:00:00",
        "display_time": "2m",
        "tweet_text": "seen candidate",
    }
    fresh_candidate = {
        "url": "https://x.com/fresh/status/222",
        "published_at": "2026-03-20 10:01:00",
        "display_time": "1m",
        "tweet_text": "fresh candidate",
    }
    (workspace / "candidate_ledger.json").write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-03-20 10:05:00",
                "items": {
                    "x:111:111": {
                        "candidate_id": "x:111:111",
                        "status_id": "111",
                        "status_url": "https://x.com/seen/status/111",
                        "state": "published",
                        "processed_name": "seen.mp4",
                        "updated_at": "2026-03-20 10:05:00",
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [seen_candidate, fresh_candidate]},
    )

    original_send = worker_impl._send_immediate_candidate_prefilter_card

    def capture_send(*args: object, **kwargs: object) -> dict[str, object]:
        item = dict(kwargs.get("item") or {})
        sent_urls.append(str(item.get("source_url") or ""))
        return original_send(*args, **kwargs)

    monkeypatch.setattr(worker_impl, "_send_immediate_candidate_prefilter_card", capture_send)

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
    assert sent_urls == ["https://x.com/fresh/status/222"]
    overview_items = feedbacks[-1]["sections"][0]["items"]
    assert any(str(entry.get("value") or "").startswith("1 ") for entry in overview_items if isinstance(entry, dict))


def test_prefilter_skip_syncs_source_url_to_collect_ledger_and_blocks_rerun(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    sent_urls: list[str] = []
    source_url = "https://x.com/repeat/status/111"

    changed = worker_impl._record_prefilter_skip_source_in_collect_ledger(
        workspace=workspace,
        source_url=source_url,
        media_kind="video",
    )

    assert changed is True

    original_send = worker_impl._send_immediate_candidate_prefilter_card

    def capture_send(*args: object, **kwargs: object) -> dict[str, object]:
        item = dict(kwargs.get("item") or {})
        sent_urls.append(str(item.get("source_url") or ""))
        return original_send(*args, **kwargs)

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {
            "keyword": DEFAULT_PROFILE,
            "candidates": [
                {
                    "url": source_url,
                    "published_at": "2026-03-21T06:41:00.000Z",
                    "display_time": "1m",
                    "tweet_text": "repeat candidate",
                }
            ],
        },
    )
    monkeypatch.setattr(worker_impl, "_send_immediate_candidate_prefilter_card", capture_send)

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
    assert sent_urls == []
    assert feedbacks[-1]["status"] == "done"
    overview_items = feedbacks[-1]["sections"][0]["items"]
    assert any(isinstance(entry, dict) and entry.get("label") == "失败/跳过" and str(entry.get("value") or "").startswith("1 ") for entry in overview_items)


def test_run_collect_publish_latest_job_extends_discovery_rounds_until_new_candidates_filled(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    sent_urls: list[str] = []
    discovery_limits: list[int] = []
    seen_candidate = {
        "url": "https://x.com/seen/status/111",
        "published_at": "2026-03-20 10:00:00",
        "display_time": "2m",
        "tweet_text": "seen candidate",
    }
    fresh_candidate_one = {
        "url": "https://x.com/fresh/status/222",
        "published_at": "2026-03-20 10:01:00",
        "display_time": "1m",
        "tweet_text": "Cybertruck delivery clip one",
    }
    fresh_candidate_two = {
        "url": "https://x.com/fresh/status/333",
        "published_at": "2026-03-20 10:02:00",
        "display_time": "1m",
        "tweet_text": "Cybertruck delivery clip two",
    }
    (workspace / "candidate_ledger.json").write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-03-20 10:05:00",
                "items": {
                    "x:111:111": {
                        "candidate_id": "x:111:111",
                        "status_id": "111",
                        "status_url": "https://x.com/seen/status/111",
                        "state": "published",
                        "processed_name": "seen.mp4",
                        "updated_at": "2026-03-20 10:05:00",
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def discover_latest_live_candidates(**kwargs: object) -> dict[str, object]:
        limit = int(kwargs.get("discovery_limit") or 0)
        discovery_limits.append(limit)
        if limit <= 4:
            candidates = [seen_candidate, fresh_candidate_one]
        else:
            candidates = [seen_candidate, fresh_candidate_one, fresh_candidate_two]
        return {"keyword": DEFAULT_PROFILE, "candidates": candidates}

    original_send = worker_impl._send_immediate_candidate_prefilter_card

    def capture_send(*args: object, **kwargs: object) -> dict[str, object]:
        item = dict(kwargs.get("item") or {})
        sent_urls.append(str(item.get("source_url") or ""))
        return original_send(*args, **kwargs)

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_discover_latest_live_candidates", discover_latest_live_candidates)
    monkeypatch.setattr(worker_impl, "_send_immediate_candidate_prefilter_card", capture_send)

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
    assert discovery_limits == [4, 8]
    assert sent_urls == ["https://x.com/fresh/status/222", "https://x.com/fresh/status/333"]


def test_run_collect_publish_latest_job_collapses_same_story_candidates(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    core = FakeCore()
    runner = FakeRunner(core)
    feedbacks: list[dict[str, object]] = []
    sent_urls: list[str] = []
    same_story_one = {
        "url": "https://x.com/story/status/111",
        "published_at": "2026-03-20 10:00:00",
        "display_time": "2m",
        "tweet_text": "67-year-old Karen Cooke Lewis scratched a Tesla Cybertruck in North Carolina and was arrested",
    }
    same_story_two = {
        "url": "https://x.com/story/status/222",
        "published_at": "2026-03-20 10:01:00",
        "display_time": "1m",
        "tweet_text": "Karen Cooke Lewis, 67, was arrested in North Carolina after scratching a Tesla Cybertruck with nails",
    }
    unique_story = {
        "url": "https://x.com/story/status/333",
        "published_at": "2026-03-20 10:02:00",
        "display_time": "1m",
        "tweet_text": "Cybertruck was spotted in Beijing yesterday by Ming",
    }

    original_send = worker_impl._send_immediate_candidate_prefilter_card

    def capture_send(*args: object, **kwargs: object) -> dict[str, object]:
        item = dict(kwargs.get("item") or {})
        sent_urls.append(str(item.get("source_url") or ""))
        return original_send(*args, **kwargs)

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (runner, core))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_impl,
        "_discover_latest_live_candidates",
        lambda **kwargs: {"keyword": DEFAULT_PROFILE, "candidates": [same_story_one, same_story_two, unique_story]},
    )
    monkeypatch.setattr(worker_impl, "_send_immediate_candidate_prefilter_card", capture_send)

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
    assert sent_urls == ["https://x.com/story/status/111", "https://x.com/story/status/333"]
    overview_items = feedbacks[-1]["sections"][0]["items"]
    assert any(isinstance(entry, dict) and entry.get("label") == "同题材折叠" and str(entry.get("value") or "").startswith("1 ") for entry in overview_items)


def test_prefilter_progress_status_label_marks_skipped_terminal_items() -> None:
    assert worker_impl._prefilter_progress_status_label(
        "down_confirmed",
        {"status": "down_confirmed", "action": "skip"},
    ) == "已跳过"
    assert worker_impl._prefilter_progress_status_label(
        "down_confirmed",
        {"status": "down_confirmed", "action": "down"},
    ) == "待发布"


def test_resolve_platform_login_runtime_context_prefers_wechat_publish_url() -> None:
    fake_core = SimpleNamespace(
        DEFAULT_WECHAT_DEBUG_PORT=9334,
        DEFAULT_WECHAT_CHROME_USER_DATA_DIR=r"D:\profiles\wechat",
        DEFAULT_CHROME_USER_DATA_DIR=r"D:\profiles\default",
        PLATFORM_CREATE_POST_URLS={"wechat": "https://channels.weixin.qq.com/platform/post/create"},
        PLATFORM_LOGIN_ENTRY_URLS={"wechat": "https://channels.weixin.qq.com/login.html"},
    )

    runtime_ctx = worker_impl._resolve_platform_login_runtime_context(fake_core, "wechat")

    assert runtime_ctx["open_url"] == "https://channels.weixin.qq.com/platform/post/create"


def test_resolve_platform_login_runtime_context_can_prefer_wechat_login_entry() -> None:
    fake_core = SimpleNamespace(
        DEFAULT_WECHAT_DEBUG_PORT=9334,
        DEFAULT_WECHAT_CHROME_USER_DATA_DIR=r"D:\profiles\wechat",
        DEFAULT_CHROME_USER_DATA_DIR=r"D:\profiles\default",
        PLATFORM_CREATE_POST_URLS={"wechat": "https://channels.weixin.qq.com/platform/post/create"},
        PLATFORM_LOGIN_ENTRY_URLS={"wechat": "https://channels.weixin.qq.com/login.html"},
    )

    runtime_ctx = worker_impl._resolve_platform_login_runtime_context(
        fake_core,
        "wechat",
        prefer_login_entry=True,
    )

    assert runtime_ctx["open_url"] == "https://channels.weixin.qq.com/login.html"


def test_resolve_platform_login_runtime_context_wechat_defaults_to_shared_browser(monkeypatch) -> None:
    fake_core = SimpleNamespace(
        DEFAULT_PORT=9333,
        DEFAULT_WECHAT_DEBUG_PORT=9334,
        DEFAULT_CHROME_USER_DATA_DIR=r"D:\profiles\shared",
        DEFAULT_WECHAT_CHROME_USER_DATA_DIR=r"D:\profiles\wechat",
        PLATFORM_CREATE_POST_URLS={"wechat": "https://channels.weixin.qq.com/platform/post/create"},
        PLATFORM_LOGIN_ENTRY_URLS={"wechat": "https://channels.weixin.qq.com/login.html"},
    )
    monkeypatch.delenv("CYBERCAR_WECHAT_CHROME_DEBUG_PORT", raising=False)
    monkeypatch.delenv("CYBERCAR_WECHAT_CHROME_USER_DATA_DIR", raising=False)
    monkeypatch.setenv("CYBERCAR_CHROME_DEBUG_PORT", "9444")
    monkeypatch.setenv("CYBERCAR_CHROME_USER_DATA_DIR", r"D:\profiles\shared_env")

    runtime_ctx = worker_impl._resolve_platform_login_runtime_context(fake_core, "wechat")

    assert runtime_ctx["debug_port"] == 9444
    assert runtime_ctx["chrome_user_data_dir"] == r"D:\profiles\shared_env"


def test_resolve_platform_login_runtime_context_wechat_explicit_override_ignored(monkeypatch) -> None:
    fake_core = SimpleNamespace(
        DEFAULT_PORT=9333,
        DEFAULT_WECHAT_DEBUG_PORT=9334,
        DEFAULT_CHROME_USER_DATA_DIR=r"D:\profiles\shared",
        DEFAULT_WECHAT_CHROME_USER_DATA_DIR=r"D:\profiles\wechat",
        PLATFORM_CREATE_POST_URLS={"wechat": "https://channels.weixin.qq.com/platform/post/create"},
        PLATFORM_LOGIN_ENTRY_URLS={"wechat": "https://channels.weixin.qq.com/login.html"},
    )
    monkeypatch.setenv("CYBERCAR_CHROME_DEBUG_PORT", "9444")
    monkeypatch.setenv("CYBERCAR_CHROME_USER_DATA_DIR", r"D:\profiles\shared_env")
    monkeypatch.setenv("CYBERCAR_WECHAT_CHROME_DEBUG_PORT", "9555")
    monkeypatch.setenv("CYBERCAR_WECHAT_CHROME_USER_DATA_DIR", r"D:\profiles\wechat_env")

    runtime_ctx = worker_impl._resolve_platform_login_runtime_context(fake_core, "wechat")

    assert runtime_ctx["debug_port"] == 9444
    assert runtime_ctx["chrome_user_data_dir"] == r"D:\profiles\shared_env"


def test_probe_platform_login_after_publish_failure_keeps_original_error_when_session_ready(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda core, platform_name: {
            "platform": "wechat",
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: {"ok": True, "needs_login": False, "url": kwargs["open_url"]},
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("qr request should not run when session is ready")),
    )

    status, message = worker_impl._probe_platform_login_after_publish_failure(
        workspace=workspace,
        item_id="item-video",
        platform="wechat",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        error_text="publish blocked by login page",
    )

    assert status == "failed"
    assert message == "publish blocked by login page"


def test_probe_platform_login_after_publish_failure_requests_qr_only_after_confirmed_login(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    qr_requests: list[dict[str, object]] = []
    text_notices: list[dict[str, object]] = []
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda core, platform_name: {
            "platform": "wechat",
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: {"ok": True, "needs_login": True, "reason": "login_url", "url": kwargs["open_url"]},
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs)) or {"ok": True, "needs_login": True, "sent": True},
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 908},
    )

    status, message = worker_impl._probe_platform_login_after_publish_failure(
        workspace=workspace,
        item_id="item-video",
        platform="wechat",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        error_text="publish blocked by login page",
    )

    assert status == "login_required"
    assert len(text_notices) == 1
    assert "视频号登录二维码已发送到 Telegram" in message
    assert len(qr_requests) == 1
    assert qr_requests[0]["refresh_page"] is True
    assert qr_requests[0]["prefer_login_entry"] is True


def test_probe_platform_login_after_publish_failure_uses_explicit_login_error_when_status_probe_fails(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    qr_requests: list[dict[str, object]] = []
    text_notices: list[dict[str, object]] = []
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda core, platform_name: {
            "platform": "wechat",
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("debug port probe failed")),
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs)) or {"ok": True, "needs_login": True, "sent": True},
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 909},
    )

    status, message = worker_impl._probe_platform_login_after_publish_failure(
        workspace=workspace,
        item_id="item-video",
        platform="wechat",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        error_text="publish blocked by login page",
    )

    assert status == "login_required"
    assert len(text_notices) == 1
    assert "Telegram" in message
    assert len(qr_requests) == 1


def test_probe_platform_login_after_publish_failure_keeps_login_required_when_qr_transport_fails(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    events: list[str] = []
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda core, platform_name: {
            "platform": "wechat",
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: {"ok": True, "needs_login": True, "reason": "login_url", "url": kwargs["open_url"]},
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: events.append("text") or {"ok": True, "sent": True, "message_id": 910},
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: events.append("qr")
        or {
            "ok": False,
            "needs_login": True,
            "transport_error": True,
            "sent": False,
            "error": "ConnectionResetError(10054)",
        },
    )

    status, message = worker_impl._probe_platform_login_after_publish_failure(
        workspace=workspace,
        item_id="item-video",
        platform="wechat",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        timeout_seconds=30,
        log_file=workspace / "runtime" / "logs" / "telegram_worker.log",
        error_text="publish blocked by login page",
    )

    assert status == "login_required"
    assert events == ["text", "qr"]
    assert "登录提醒" in message
    assert "Telegram" in message


def test_publish_platform_job_wechat_failure_requests_qr_and_sends_summary(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []
    qr_requests: list[dict[str, object]] = []
    text_notices: list[dict[str, object]] = []
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    fake_runner._publish_once = lambda ctx, args, email_settings, platform, target, source, events: events.append(  # type: ignore[attr-defined]
        SimpleNamespace(success=False, error="publish blocked by login page")
    )

    monkeypatch.setattr(worker_impl, "_with_platform_lock", lambda workspace, platform, fn, timeout_seconds: fn())
    monkeypatch.setattr(worker_impl, "_build_immediate_cycle_context", lambda **kwargs: object())
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: {"ok": True, "needs_login": True, "reason": "login_url", "url": kwargs["open_url"]},
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs)) or {"ok": True, "needs_login": True, "sent": True},
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 905},
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
    assert len(text_notices) == 1
    assert len(qr_requests) == 1
    pending = _prefilter_items(workspace)["item-video"]
    assert isinstance(pending, dict)
    assert pending.get("platform_results") in ({}, None)
    assert _flush_platform_result_events(workspace, fake_runner) == 2
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_failed"
    assert updated["platform_results"]["wechat"]["status"] == "login_required"
    assert "二维码已发送到 Telegram" in str(updated["platform_results"]["wechat"]["error"])
    assert len(feedbacks) == 2
    assert "视频号需要重新登录" in str(feedbacks[0]["title"])
    assert "即采即发发布失败" in str(feedbacks[1]["title"])


def test_publish_platform_job_wechat_transport_qr_failure_still_marks_login_required(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []
    qr_requests: list[dict[str, object]] = []
    text_notices: list[dict[str, object]] = []
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    fake_runner._publish_once = lambda ctx, args, email_settings, platform, target, source, events: events.append(  # type: ignore[attr-defined]
        SimpleNamespace(success=False, error="publish blocked by login page")
    )

    monkeypatch.setattr(worker_impl, "_with_platform_lock", lambda workspace, platform, fn, timeout_seconds: fn())
    monkeypatch.setattr(worker_impl, "_build_immediate_cycle_context", lambda **kwargs: object())
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: {"ok": True, "needs_login": True, "reason": "login_url", "url": kwargs["open_url"]},
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs))
        or {
            "ok": False,
            "needs_login": True,
            "transport_error": True,
            "sent": False,
            "error": "ConnectionResetError(10054)",
        },
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 906},
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
    assert len(text_notices) == 1
    assert len(qr_requests) == 1
    assert _flush_platform_result_events(workspace, fake_runner) == 2
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["platform_results"]["wechat"]["status"] == "login_required"
    assert "Telegram" in str(updated["platform_results"]["wechat"]["error"])
    assert "登录提醒" in str(updated["platform_results"]["wechat"]["error"])
    assert len(feedbacks) == 2


def test_flush_platform_result_events_retries_after_queue_lock_timeout(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []
    qr_requests: list[dict[str, object]] = []
    text_notices: list[dict[str, object]] = []
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    fake_runner._publish_once = lambda ctx, args, email_settings, platform, target, source, events: events.append(  # type: ignore[attr-defined]
        SimpleNamespace(success=False, error="publish blocked by login page")
    )

    monkeypatch.setattr(worker_impl, "_with_platform_lock", lambda workspace, platform, fn, timeout_seconds: fn())
    monkeypatch.setattr(worker_impl, "_build_immediate_cycle_context", lambda **kwargs: object())
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: {"ok": True, "needs_login": True, "reason": "login_url", "url": kwargs["open_url"]},
        raising=False,
    )
    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: qr_requests.append(dict(kwargs))
        or {
            "ok": False,
            "needs_login": True,
            "transport_error": True,
            "sent": False,
            "error": "ConnectionResetError(10054)",
        },
    )
    monkeypatch.setattr(
        worker_impl,
        "_send_platform_login_text_notice",
        lambda **kwargs: text_notices.append(dict(kwargs)) or {"ok": True, "sent": True, "message_id": 907},
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
    assert len(text_notices) == 1
    assert len(qr_requests) == 1
    original_merge = worker_impl._merge_platform_result
    attempts = {"count": 0}

    def flaky_merge(*, workspace: Path, item_id: str, platform: str, updates: dict[str, object]) -> dict[str, object]:
        attempts["count"] += 1
        status = str(updates.get("status") or "").strip().lower()
        if status == "login_required" and attempts["count"] == 2:
            raise TimeoutError("Timed out waiting for lock: queue")
        return original_merge(workspace=workspace, item_id=item_id, platform=platform, updates=updates)

    monkeypatch.setattr(worker_impl, "_merge_platform_result", flaky_merge)

    assert _flush_platform_result_events(workspace, fake_runner) == 1
    interim = _prefilter_items(workspace)["item-video"]
    assert isinstance(interim, dict)
    assert interim["platform_results"]["wechat"]["status"] == "running"
    assert len(feedbacks) == 0

    monkeypatch.setattr(worker_impl, "_merge_platform_result", original_merge)
    assert _flush_platform_result_events(workspace, fake_runner) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["platform_results"]["wechat"]["status"] == "login_required"
    assert len(feedbacks) == 2
    sections = list(feedbacks[0]["sections"])
    assert any("Telegram" in str(section) for section in sections)
    return
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["platform_results"]["wechat"]["status"] == "running"
    assert len(feedbacks) == 1
    assert "视频号需要重新登录" in str(feedbacks[0]["title"])
    sections = list(feedbacks[0]["sections"])
    assert any("Telegram" in str(section) for section in sections)


def test_publish_platform_job_wechat_failure_keeps_original_error_when_qr_probe_fails(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []
    qr_requests: list[dict[str, object]] = []
    original_error = "未能确认合集选择成功: 赛博皮卡精选; 当前字段值=-; 读取来源=empty"
    from Collection.cybercar.cybercar_video_capture_and_publishing_module import main as worker_core

    fake_runner._publish_once = lambda ctx, args, email_settings, platform, target, source, events: events.append(  # type: ignore[attr-defined]
        SimpleNamespace(success=False, error=original_error)
    )

    monkeypatch.setattr(worker_impl, "_with_platform_lock", lambda workspace, platform, fn, timeout_seconds: fn())
    monkeypatch.setattr(worker_impl, "_build_immediate_cycle_context", lambda **kwargs: object())
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(
        worker_core,
        "check_platform_login_status",
        lambda **kwargs: {"ok": True, "needs_login": True, "reason": "login_url", "url": kwargs["open_url"]},
        raising=False,
    )
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
    assert _flush_platform_result_events(workspace, fake_runner) == 2
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["platform_results"]["wechat"]["status"] == "failed"
    assert updated["platform_results"]["wechat"]["error"] == original_error
    assert len(feedbacks) == 2
    assert "视频号发布失败" in str(feedbacks[0]["title"])
    assert "即采即发发布失败" in str(feedbacks[1]["title"])


def test_publish_platform_job_wechat_lock_timeout_does_not_trigger_login_probe(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    video_path = workspace / "2_Processed" / "clip.mp4"
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []
    qr_requests: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_with_platform_lock",
        lambda workspace, platform, fn, timeout_seconds: (_ for _ in ()).throw(
            TimeoutError(f"Timed out waiting for lock: {workspace / 'runtime' / 'platform_publish_locks' / 'wechat.lockdir'}")
        ),
    )
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
    assert qr_requests == []
    assert _flush_platform_result_events(workspace, fake_runner) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["platform_results"]["wechat"]["status"] == "failed"
    assert "Timed out waiting for lock" in str(updated["platform_results"]["wechat"]["error"])
    assert len(feedbacks) == 2


def test_publish_platform_job_rejects_mismatched_target_before_publish(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    wrong_name = "DRAFT_999999999999999999__wrong.mp4"
    video_path = workspace / "2_Processed" / wrong_name
    video_path.write_text("ok", encoding="utf-8")

    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    publish_attempts: list[object] = []

    fake_runner._publish_once = lambda *args, **kwargs: publish_attempts.append((args, kwargs))  # type: ignore[attr-defined]
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)

    item = _video_item(
        target_platforms="wechat",
        video_name=wrong_name,
        processed_name=wrong_name,
        source_url="https://x.com/tester/status/2033331774894358749",
    )
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
    assert publish_attempts == []
    assert _flush_platform_result_events(workspace, fake_runner) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["platform_results"]["wechat"]["status"] == "failed"
    assert "不匹配" in str(updated["platform_results"]["wechat"]["error"])


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
    assert _flush_platform_result_events(workspace, fake_runner) == 2
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

    item = _image_item(workflow="immediate_collect_review")
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


def test_run_immediate_collect_item_job_adopts_downloaded_video_after_successful_collect(tmp_path: Path, monkeypatch) -> None:
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
        downloaded = workspace / "1_Downloads" / "fresh-video.mp4"
        downloaded.parent.mkdir(parents=True, exist_ok=True)
        downloaded.write_text("video", encoding="utf-8")
        return {"ok": True, "code": 0, "stdout": "collect success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)

    item = _video_item(video_name="", processed_name="", status="publish_requested", source_url="https://x.test/post/fresh-video")
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
    assert len(collect_calls) == 1
    assert len(approvals) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_running"
    assert updated["processed_name"] == "fresh-video.mp4"
    assert (workspace / "2_Processed" / "fresh-video.mp4").exists()


def test_run_immediate_collect_item_job_prefers_processed_target_matching_source(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    approvals: list[dict[str, object]] = []
    expected_status_id = "2033331774894358749"
    expected_target = workspace / "2_Processed" / f"DRAFT_{expected_status_id}__right.mp4"

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
        wrong = workspace / "2_Processed" / "DRAFT_999999999999999999__wrong.mp4"
        wrong.parent.mkdir(parents=True, exist_ok=True)
        wrong.write_text("wrong", encoding="utf-8")
        expected_target.write_text("right", encoding="utf-8")
        return {"ok": True, "code": 0, "stdout": "collect success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)
    monkeypatch.setattr(
        fake_core,
        "_find_processed_target_by_source",
        lambda workspace_ctx, *, source_url="", status_id="": (expected_target, {"processed_name": expected_target.name})
        if expected_target.exists()
        else (None, None),
        raising=False,
    )

    item = _video_item(
        video_name="",
        processed_name="",
        status="publish_requested",
        source_url=f"https://x.com/tester/status/{expected_status_id}",
    )
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
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["processed_name"] == expected_target.name


def test_run_immediate_collect_item_job_rejects_mismatched_downloaded_video(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    feedbacks: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: None)

    def run_unified_once(**kwargs: object) -> dict[str, object]:
        downloaded = workspace / "1_Downloads" / "DRAFT_999999999999999999__wrong.mp4"
        downloaded.parent.mkdir(parents=True, exist_ok=True)
        downloaded.write_text("wrong", encoding="utf-8")
        return {"ok": True, "code": 0, "stdout": "collect success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)

    item = _video_item(
        video_name="",
        processed_name="",
        status="publish_requested",
        source_url="https://x.com/tester/status/2033331774894358749",
    )
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

    assert exit_code == 2
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "download_failed"
    assert "不匹配" in str(updated["last_error"])
    assert feedbacks[-1]["status"] == "failed"


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


def test_run_immediate_collect_item_job_retries_generic_download_failure(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    collect_calls: list[dict[str, object]] = []
    approvals: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "time", SimpleNamespace(sleep=lambda seconds: None, monotonic=worker_impl.time.monotonic, time=worker_impl.time.time))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))

    def run_unified_once(**kwargs: object) -> dict[str, object]:
        collect_calls.append(dict(kwargs))
        if len(collect_calls) == 1:
            return {"stderr": "ERROR: download failed: unable to download media from x"}
        processed = workspace / "2_Processed" / "retry-generic-ok.mp4"
        processed.write_text("video", encoding="utf-8")
        return {"status": "success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)
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
    assert updated["processed_name"] == "retry-generic-ok.mp4"


def test_run_immediate_collect_item_job_falls_back_to_direct_tun_after_system_proxy_failure(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    collect_calls: list[dict[str, object]] = []
    approvals: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "time", SimpleNamespace(sleep=lambda seconds: None, monotonic=worker_impl.time.monotonic, time=worker_impl.time.time))
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: approvals.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_resolve_worker_network_mode", lambda: ("http://127.0.0.1:7890", False))
    monkeypatch.setattr(worker_impl, "_worker_system_proxy_available", lambda: True)

    def run_unified_once(**kwargs: object) -> dict[str, object]:
        collect_calls.append(dict(kwargs))
        if len(collect_calls) < 3:
            return {"stderr": "ERROR: download failed: unable to download media from x"}
        processed = workspace / "2_Processed" / "retry-direct-tun-ok.mp4"
        processed.write_text("video", encoding="utf-8")
        return {"status": "success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)
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
    assert len(collect_calls) == 3
    assert collect_calls[0]["proxy_override"] == "http://127.0.0.1:7890"
    assert collect_calls[0]["use_system_proxy_override"] is False
    assert collect_calls[1]["proxy_override"] is None
    assert collect_calls[1]["use_system_proxy_override"] is True
    assert collect_calls[2]["proxy_override"] is None
    assert collect_calls[2]["use_system_proxy_override"] is False
    assert len(approvals) == 1
    updated = _prefilter_items(workspace)["item-video"]
    assert isinstance(updated, dict)
    assert updated["status"] == "publish_running"
    assert updated["processed_name"] == "retry-direct-tun-ok.mp4"


def test_run_immediate_collect_item_job_auto_fallbacks_to_next_candidate_on_final_failure(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    spawned_jobs: list[dict[str, object]] = []
    feedbacks: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: feedbacks.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_is_immediate_collect_transient_retry_reason", lambda *args, **kwargs: False)
    monkeypatch.setattr(worker_impl, "_run_unified_once", lambda **kwargs: {"stderr": "ERROR: download failed: unable to download media from x"})
    monkeypatch.setattr(
        worker_impl,
        "_spawn_immediate_publish_item_job",
        lambda **kwargs: spawned_jobs.append(dict(kwargs)) or {"ok": True, "pid": 12345, "log_path": str(workspace / "runtime" / "logs" / "fallback.log"), "item_id": str(kwargs.get("item_id") or "")},
    )

    failed_item = _video_item(
        item_id="item-failed",
        status="publish_requested",
        source_url="https://x.com/current/status/111",
        candidate_index=1,
        candidate_limit=5,
        profile=DEFAULT_PROFILE,
        media_kind="video",
    )
    next_item = _video_item(
        item_id="item-next",
        status="link_pending",
        action="sent",
        source_url="https://x.com/next/status/222",
        candidate_index=2,
        candidate_limit=5,
        profile=DEFAULT_PROFILE,
        media_kind="video",
        message_id=902,
    )
    _save_prefilter_items(workspace, {"item-failed": failed_item, "item-next": next_item})

    exit_code = actions.run_immediate_collect_item_job(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-failed",
    )

    assert exit_code == 0
    assert len(spawned_jobs) == 1
    assert str(spawned_jobs[0].get("item_id") or "") == "item-next"
    queue = _prefilter_items(workspace)
    assert queue["item-failed"]["status"] == "download_failed"
    assert queue["item-next"]["status"] == "publish_requested"
    assert queue["item-next"]["action"] == "auto_fallback_from_collect_failed"
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
    assert "--source-platforms" in extra_args
    assert extra_args[extra_args.index("--source-platforms") + 1] == "x"
    assert "--require-x-live-discovery" not in extra_args
    assert extra_args[extra_args.index("--x-download-socket-timeout") + 1] == "25"
    assert extra_args[extra_args.index("--x-download-extractor-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-fragment-retries") + 1] == "2"
    assert extra_args[extra_args.index("--x-download-batch-retry-sleep") + 1] == "1"


def test_run_immediate_collect_item_job_uses_item_source_platform_for_global_route(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    fake_core = FakeCore()
    fake_runner = FakeRunner(fake_core)
    collect_calls: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "core", fake_core)
    monkeypatch.setattr(worker_impl, "_send_background_feedback", lambda **kwargs: None)
    monkeypatch.setattr(worker_impl, "_apply_review_approve", lambda **kwargs: None)
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
        processed = workspace / "2_Processed" / "global-video.mp4"
        processed.parent.mkdir(parents=True, exist_ok=True)
        processed.write_text("video", encoding="utf-8")
        return {"status": "success"}

    monkeypatch.setattr(worker_impl, "_run_unified_once", run_unified_once)

    item = _video_item(
        video_name="",
        processed_name="",
        status="publish_requested",
        source_url="https://www.xiaohongshu.com/explore/abc123",
        source_platform="xiaohongshu",
    )
    _save_prefilter_items(workspace, {str(item["id"]): item})

    exit_code = actions.run_immediate_collect_item_job(
        runner=fake_runner,
        core=fake_core,
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=worker_impl.DEFAULT_GLOBAL_COLLECT_PUBLISH_PROFILE,
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        item_id="item-video",
    )

    assert exit_code == 0
    extra_args = list(collect_calls[0]["extra_args"])
    assert "--source-platforms" in extra_args
    assert extra_args[extra_args.index("--source-platforms") + 1] == "xiaohongshu"


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


def test_handle_prefilter_retry_failed_publish_requeues_failed_platforms_only(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    record = _install_transport_mocks(monkeypatch)
    queue_calls: list[dict[str, object]] = []

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (SimpleNamespace(), FakeCore()))

    def queue_immediate_platform_jobs(**kwargs: object) -> dict[str, object]:
        queue_calls.append(dict(kwargs))
        updated = worker_impl._update_prefilter_item(
            workspace,
            str(kwargs["item_id"]),
            updates={
                "status": "publish_running",
                "platform_results": {
                    "wechat": {"status": "queued"},
                    "douyin": {"status": "success"},
                },
                "action": "publish",
            },
        )
        return {"spawned": 1, "failed": 0, "skipped_duplicate": 0, "item": updated}

    monkeypatch.setattr(worker_impl, "_queue_immediate_platform_jobs", queue_immediate_platform_jobs)

    item = _video_item(
        status="publish_partial",
        platform_results={
            "wechat": {"status": "failed"},
            "douyin": {"status": "success"},
        },
    )
    _save_prefilter_items(workspace, {str(item["id"]): item})

    result = commands.handle_callback_update(
        update=_make_callback_update("ctpf|retry_failed_publish|item-video"),
        **_worker_kwargs(workspace),
    )

    assert result["handled"] is True
    assert len(queue_calls) == 1
    assert queue_calls[0]["item_id"] == "item-video"
    assert len(record.updated_cards) == 1
    assert "失败平台已补发" in str(record.updated_cards[-1]["card"]["text"])


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


def test_atomic_write_json_retries_plain_permission_error_on_windows(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "payload.json"
    original_replace = os.replace
    attempts = {"count": 0}

    def flaky_replace(src: os.PathLike[str] | str, dst: os.PathLike[str] | str) -> None:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise PermissionError("replace blocked")
        return original_replace(src, dst)

    monkeypatch.setattr(worker_impl.os, "replace", flaky_replace)
    monkeypatch.setattr(worker_impl, "os", worker_impl.os)
    monkeypatch.setattr(worker_impl, "time", worker_impl.time)
    monkeypatch.setattr(worker_impl.time, "sleep", lambda seconds: None)

    worker_impl._atomic_write_json(path, {"ok": True})

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["ok"] is True
    assert attempts["count"] == 3


def test_build_process_log_section_folds_repeated_init_lines(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    log_path = workspace / "runtime" / "logs" / "home-progress.log"
    log_path.write_text(
        "\n".join(
            [
                "[2026-03-17 05:30:26] [Init] Workspace ready: D:/code/CyberCar/runtime",
                "[2026-03-17 05:30:27] [Init] Workspace ready: D:/code/CyberCar/runtime",
                "[2026-03-17 05:30:28] [Notify] backend result sent",
            ]
        ),
        encoding="utf-8",
    )
    claim = worker_impl._claim_home_action_task(
        workspace=workspace,
        chat_id=CHAT_ID,
        action="collect_publish_latest",
        value="image:5",
        profile=DEFAULT_PROFILE,
        username="tester",
    )
    worker_impl._update_home_action_task(
        workspace,
        str(claim["task_key"]),
        status="running",
        detail="图片即采即发后台任务正在运行。",
        log_path=str(log_path),
        pid=321,
    )

    section = worker_impl._build_process_log_section(workspace)
    values = [str(item.get("value")) if isinstance(item, dict) else str(item) for item in section["items"]]

    assert any("已折叠 1 条重复初始化日志" in value for value in values)
    assert any("backend result sent" in value for value in values)


def test_run_home_action_job_reports_login_qr_as_qr_sent_before_scan(tmp_path: Path, monkeypatch) -> None:
    workspace = _make_workspace(tmp_path)
    sent_cards: list[dict[str, object]] = []

    monkeypatch.setattr(
        worker_impl,
        "_request_platform_login_qr",
        lambda **kwargs: {"ok": True, "needs_login": True, "sent": True},
    )
    monkeypatch.setattr(worker_impl, "_send_card_message", lambda **kwargs: sent_cards.append(dict(kwargs)))
    monkeypatch.setattr(worker_impl, "_try_delete_telegram_message", lambda **kwargs: False)

    claim = worker_impl._claim_home_action_task(
        workspace=workspace,
        chat_id=CHAT_ID,
        action="login_qr",
        value="kuaishou",
        profile=DEFAULT_PROFILE,
        username="tester",
    )

    exit_code = worker_impl._run_home_action_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile=DEFAULT_PROFILE,
        telegram_bot_identifier="cybercar",
        telegram_bot_token=BOT_TOKEN,
        telegram_chat_id=CHAT_ID,
        action="login_qr",
        value="kuaishou",
        task_key=str(claim["task_key"]),
    )

    assert exit_code == 0
    assert len(sent_cards) == 1
    assert "二维码已发送" in str(sent_cards[0]["card"]["text"])
    assert "平台登录已完成" not in str(sent_cards[0]["card"]["text"])


def test_build_process_log_section_sanitizes_garbled_lines(tmp_path: Path) -> None:
    workspace = _make_workspace(tmp_path)
    log_path = workspace / "runtime" / "logs" / "home_action_collect_publish_latest_test.log"
    log_path.write_text(
        "[2026-03-17 05:30:29] [Notify] ?????? ? / ?? / 5????\n",
        encoding="utf-8",
    )

    section = worker_impl._build_process_log_section(workspace)
    items = [str(item.get("value")) if isinstance(item, dict) else str(item) for item in section["items"]]

    assert any("\u65e5\u5fd7\u6587\u672c\u5b58\u5728\u7f16\u7801\u5f02\u5e38" in item for item in items)


def test_update_worker_state_preserves_last_error_until_poll_success(tmp_path: Path) -> None:
    state_path = tmp_path / "worker_state.json"

    worker_impl._update_worker_state(
        state_path,
        status="polling",
        worker_heartbeat_at="2026-03-17 09:00:00",
        consecutive_poll_failures=6,
        last_error="telegram poll transport jitter; send path remains retryable",
    )
    worker_impl._update_worker_state(
        state_path,
        status="polling",
        worker_heartbeat_at="2026-03-17 09:00:10",
        last_poll_started_at="2026-03-17 09:00:10",
    )
    payload = worker_impl._load_state(state_path)
    assert payload["consecutive_poll_failures"] == 6
    assert payload["last_error"] == "telegram poll transport jitter; send path remains retryable"

    worker_impl._update_worker_state(
        state_path,
        status="polling",
        worker_heartbeat_at="2026-03-17 09:00:20",
        consecutive_poll_failures=0,
        last_error="",
    )
    payload = worker_impl._load_state(state_path)
    assert payload["consecutive_poll_failures"] == 0
    assert payload["last_error"] == ""


def test_optimize_feedback_sections_for_operator_moves_logs_to_machine_info() -> None:
    sections = worker_impl._optimize_feedback_sections_for_operator(
        [
            {"title": "任务标识", "emoji": "🏷️", "items": [{"label": "当前任务", "value": "collect|image|3"}]},
            {"title": "菜单链路", "emoji": "🧭", "items": [{"label": "当前链路", "value": "即采即发 > 图片"}]},
            {
                "title": "执行摘要",
                "emoji": "📌",
                "items": [
                    {"label": "目标平台", "value": "抖音 / 小红书"},
                    {"label": "执行结果", "value": "成功"},
                    {"label": "耗时", "value": "12.4s"},
                ],
            },
            {"title": "任务日志", "emoji": "🧾", "items": [{"label": "日志名", "value": "job.log"}]},
        ]
    )

    assert sections[0]["title"] == "人工关注"
    assert any(str(item.get("label")) == "执行结果" for item in sections[0]["items"])
    assert sections[-1]["title"] == "机器信息"
    machine_values = [str(item.get("value")) for item in sections[-1]["items"] if isinstance(item, dict)]
    assert "collect|image|3" in machine_values
    assert "job.log" in machine_values


def test_build_platform_status_summary_marks_platforms_individually() -> None:
    summary = worker_impl._build_platform_status_summary(
        ["wechat", "douyin", "xiaohongshu"],
        "\n".join(
            [
                "[scheduler:wechat] publish failed: login required",
                "[scheduler:douyin] publish failed: upload rejected",
                "[scheduler:xiaohongshu] publish success",
            ]
        ),
        effective_status="failed",
    )

    assert "🔐 视频号登录" in summary
    assert "📣 抖音失败" in summary
    assert "📣 小红书失败" not in summary
    assert "⚠️ 小红书待确认" in summary or "✅ 小红书成功" in summary
def test_immediate_publish_feedback_omits_duplicate_platform_in_candidate_section() -> None:
    item = _image_item(
        target_platforms="douyin,xiaohongshu,kuaishou",
        platform_results={
            "xiaohongshu": {
                "status": "success",
                "publish_id": "xh-1",
            }
        },
    )

    payload = worker_impl._build_immediate_platform_feedback_payload(
        item=item,
        platform="xiaohongshu",
        result={"status": "success", "publish_id": "xh-1"},
    )

    sections = list(payload.get("sections") or [])
    candidate_section = next(section for section in sections if section.get("title") == "候选信息")
    labels = [str(entry.get("label") or "") for entry in candidate_section.get("items", []) if isinstance(entry, dict)]

    assert "平台" not in labels


def test_build_failure_feedback_actions_prefers_login_and_progress_for_login_failures() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {"title": "失败原因", "items": [{"label": "原因", "value": "平台未登录"}]},
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    assert texts == ["🔐 登录", "📍 进度"]


def test_build_failure_feedback_actions_uses_progress_for_generic_failures() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {"title": "失败原因", "items": [{"label": "原因", "value": "平台处理失败"}]},
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    assert texts == ["📍 进度"]


def test_build_failure_feedback_actions_adds_retry_for_collect_publish_summary() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {"title": "任务标识", "items": [{"label": "当前任务", "value": "collect_publish_latest|item-video"}]},
            {"title": "平台状态", "items": [{"label": "🎥 视频号", "value": "❌ 发布失败"}]},
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    callbacks = [str(action.get("callback_data") or "") for action in actions]
    assert texts == ["🔁 补发", "📍 进度"]
    assert callbacks[0] == "ctpf|retry_failed_publish|item-video"


def test_build_failure_feedback_actions_adds_retry_when_task_identifier_is_compacted_into_machine_info() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {"title": "机器信息", "items": [{"label": "当前任务", "value": "collect_publish_latest|item-video"}]},
            {"title": "平台状态", "items": [{"label": "🎥 视频号", "value": "❌ 发布失败"}]},
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    callbacks = [str(action.get("callback_data") or "") for action in actions]
    assert texts == ["🔁 补发", "📍 进度"]
    assert callbacks[0] == "ctpf|retry_failed_publish|item-video"


def test_build_failure_feedback_actions_prefers_refresh_for_retryable_failures() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {"title": "失败原因", "items": [{"label": "原因", "value": "上传失败，network timeout"}]},
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    assert texts == ["🔄 刷新", "📍 进度"]


def test_build_failure_feedback_actions_prefers_home_for_duplicate_skip_failures() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {"title": "结果说明", "items": ["平台已有历史发布记录，本轮已自动跳过。"]},
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    assert texts == ["🏠 首页"]


def test_build_failure_feedback_actions_does_not_treat_summary_login_hint_as_login_failure() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {"title": "执行摘要", "items": [{"label": "结果", "value": "本轮存在部分平台成功，部分平台失败或需要登录。"}]},
            {"title": "平台状态", "items": [{"label": "📝 小红书", "value": "✅ 已确认"}, {"label": "🎵 抖音", "value": "📣 发布失败"}]},
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    assert texts == ["📍 进度"]


def test_build_failure_feedback_actions_omits_login_for_mixed_platform_progress_cards() -> None:
    actions = worker_impl._build_failure_feedback_actions(
        status="failed",
        sections=[
            {
                "title": "平台状态",
                "items": [
                    {"label": "📱 视频号 ❌", "value": "🔐 需要登录"},
                    {"label": "🎵 抖音 ❌", "value": "⏳ 发布中"},
                    {"label": "⚡ 快手", "value": "🕓 已排队"},
                    {"label": "📝 小红书", "value": "✅ 已确认"},
                ],
            }
        ],
    )

    texts = [str(action.get("text") or "") for action in actions]
    assert texts == ["📍 进度"]


def test_build_shared_link_status_card_compacts_body_for_operator_scan() -> None:
    item = _video_item(
        target_platforms="wechat,douyin,xiaohongshu,kuaishou,bilibili",
        source_url="https://x.com/kauai_renatus/status/2033954550592487896",
        title="JESUS LOVES KAUAI",
        actor="ocuitengwei",
        updated_at="2026-03-18 14:02:15",
    )

    card = worker_impl._build_shared_link_status_card(
        item=item,
        title="分享链接已接收",
        subtitle="后台即采即发任务已排队",
        status="running",
        result_items=[
            "已改机顶即采即发链路排队，后台会先采集素材，再继续进入平台发布。",
            "如需查看当前进度，可直接点“进度”。",
        ],
    )

    text = str(card["text"])
    assert "<b>⏳ 分享链接已接收</b>" in text
    assert "· 即采即发 / 视频 / 全部平台｜后台即采即发任务已排队" in text
    assert "<b>📌 执行摘要</b>" in text
    assert "如需查看当前进度，可直接点“进度”。" not in text
    assert "<b>🧾 候选信息</b>" in text
    assert "原帖链接" not in text
    assert "操作记录" not in text
    assert "任务标识" not in text
    assert "菜单链路" not in text
    assert _reply_markup_texts(card["reply_markup"]) == ["📍 进度", "🏠 首页"]
