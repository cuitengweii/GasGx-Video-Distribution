from __future__ import annotations

import json
import time
from typing import Any

import pytest
from cybercar import engine


def test_is_collection_match_strips_counter_suffix() -> None:
    assert engine._is_collection_match("赛博皮卡-天津港现车 共11个内容", "赛博皮卡-天津港现车")
    assert engine._is_collection_match(" 添加到合集 赛博皮卡测评 ", "赛博皮卡测评")


def test_select_collection_supports_finder_card_picker(monkeypatch: pytest.MonkeyPatch) -> None:
    states = [
        {"hasField": True, "current": "", "source": "empty"},
        {"hasField": True, "current": "赛博皮卡-天津港现车 共11个内容", "source": "finder-card"},
        {"hasField": True, "current": "赛博皮卡-天津港现车 共11个内容", "source": "finder-card"},
    ]

    def fake_get_collection_state(_: Any) -> dict[str, Any]:
        if len(states) > 1:
            return states.pop(0)
        return states[0]

    class FakeCtx:
        def __init__(self) -> None:
            self.select_scripts: list[str] = []

        def run_js(self, script: str, *args: Any) -> Any:
            if "function setInputValue" in script:
                self.select_scripts.append(script)
                assert ".finder-card .post-album-action-text" in script
                assert ".finder-common-dialog" in script
                assert "visible_options" in script
                assert args == ("赛博皮卡-天津港现车",)
                return {
                    "state": "clicked",
                    "option": "赛博皮卡-天津港现车",
                    "visible_options": ["赛博皮卡-天津港现车"],
                }
            return True

    monkeypatch.setattr(engine, "_get_collection_state", fake_get_collection_state)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *args, **kwargs: False)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    ctx = FakeCtx()
    engine._select_collection(ctx, "赛博皮卡-天津港现车")

    assert ctx.select_scripts


def test_select_collection_error_reports_visible_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        engine,
        "_get_collection_state",
        lambda _ctx: {"hasField": True, "current": "", "source": "empty"},
    )
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *args, **kwargs: False)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    class FakeCtx:
        def run_js(self, script: str, *args: Any) -> Any:
            if "visible_options" in script and args:
                return {
                    "state": "option_not_found",
                    "visible_options": ["album-option-a", "album-option-b"],
                }
            return True

    with pytest.raises(RuntimeError) as exc_info:
        engine._select_collection(FakeCtx(), "target-album")
    message = str(exc_info.value)
    assert "album-option-a" in message
    assert "album-option-b" in message


def test_select_collection_skips_when_field_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(engine, "_get_collection_state", lambda _ctx: {"hasField": False, "current": ""})
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._select_collection(object(), "target-album")


def test_comment_login_notification_prefers_qr(monkeypatch: pytest.MonkeyPatch) -> None:
    qr_calls: list[dict[str, Any]] = []
    text_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(engine, "inspect_platform_login_gate", lambda *_args, **_kwargs: {"needs_login": True, "url": "https://channels.weixin.qq.com/login.html", "reason": "login_url"})

    def fake_send_qr(**kwargs: Any) -> dict[str, Any]:
        qr_calls.append(dict(kwargs))
        return {"ok": True, "sent": True, "kind": "qr"}

    def fake_send_text(**kwargs: Any) -> dict[str, Any]:
        text_calls.append(dict(kwargs))
        return {"ok": True, "sent": True, "kind": "text"}

    monkeypatch.setattr(engine, "send_platform_login_qr_notification", fake_send_qr)
    monkeypatch.setattr(engine, "_send_platform_login_text_notification", fake_send_text)

    result = engine._maybe_notify_wechat_comment_login_required(
        page=object(),
        chrome_user_data_dir="D:/profiles/wechat",
        open_url="https://channels.weixin.qq.com/platform/post/comment",
        login_reason="comment_manager_not_ready",
        telegram_bot_token="token",
        telegram_chat_id="chat",
    )

    assert result["sent"] is True
    assert result["needs_login"] is True
    assert result["notification_mode"] == "qr"
    assert result["url"] == "https://channels.weixin.qq.com/login.html"
    assert len(qr_calls) == 1
    assert not text_calls
    assert qr_calls[0]["page"] is not None
    assert qr_calls[0]["auto_open_chrome"] is False
    assert qr_calls[0]["allow_navigation"] is False


def test_comment_login_notification_falls_back_to_text(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(engine, "inspect_platform_login_gate", lambda *_args, **_kwargs: {"needs_login": True, "url": "https://channels.weixin.qq.com/login.html", "reason": "comment_manager_not_ready"})
    monkeypatch.setattr(engine, "send_platform_login_qr_notification", lambda **_kwargs: {"ok": False, "sent": False, "error": "wechat login qr not found"})

    text_calls: list[dict[str, Any]] = []

    def fake_send_text(**kwargs: Any) -> dict[str, Any]:
        text_calls.append(dict(kwargs))
        return {"ok": True, "sent": True, "kind": "text"}

    monkeypatch.setattr(engine, "_send_platform_login_text_notification", fake_send_text)

    result = engine._maybe_notify_wechat_comment_login_required(
        page=object(),
        chrome_user_data_dir="D:/profiles/wechat",
        open_url="https://channels.weixin.qq.com/platform/post/comment",
        login_reason="comment_manager_not_ready",
        telegram_bot_token="token",
        telegram_chat_id="chat",
    )

    assert result["sent"] is True
    assert result["needs_login"] is True
    assert result["notification_mode"] == "text"
    assert result["qr_result"]["error"] == "wechat login qr not found"
    assert len(text_calls) == 1
    assert text_calls[0]["qr_error"] == "wechat login qr not found"


def test_comment_login_notification_skips_when_page_is_not_login_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda *_args, **_kwargs: {"needs_login": False, "url": "https://channels.weixin.qq.com/platform/interaction/comment"},
    )

    result = engine._maybe_notify_wechat_comment_login_required(
        page=object(),
        chrome_user_data_dir="D:/profiles/wechat",
        open_url="https://channels.weixin.qq.com/platform/post/comment",
        login_reason="comment_manager_open_failed",
        telegram_bot_token="token",
        telegram_chat_id="chat",
    )

    assert result["ok"] is True
    assert result["needs_login"] is False
    assert result["skipped"] is True
    assert result["reason"] == "not_login_gate"


def test_platform_login_confirm_wait_window_covers_qr_ttl() -> None:
    assert engine.PLATFORM_LOGIN_CONFIRM_WAIT_SECONDS >= engine.WECHAT_LOGIN_QR_NOTICE_TTL_SECONDS


def test_wechat_persistent_login_url_prefers_create_entry_over_legacy_login_html(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(engine.PLATFORM_CREATE_POST_URLS, "wechat", "https://channels.weixin.qq.com/platform/post/create")
    monkeypatch.setitem(engine.PLATFORM_LOGIN_ENTRY_URLS, "wechat", "https://channels.weixin.qq.com/login.html")

    assert engine._wechat_persistent_login_url() == "https://channels.weixin.qq.com/platform/post/create"


def test_begin_platform_login_wait_reuses_recent_waiting_token(monkeypatch: pytest.MonkeyPatch) -> None:
    now_ts = 1_700_000_000.0
    writes: list[dict[str, Any]] = []

    monkeypatch.setattr(engine.time, "time", lambda: now_ts)
    monkeypatch.setattr(
        engine,
        "_read_platform_login_signal",
        lambda *_args, **_kwargs: {
            "platform": "wechat",
            "profile_dir": "D:/profiles/wechat",
            "open_url": "https://channels.weixin.qq.com/login.html",
            "status": "waiting",
            "token": "wait-token-existing",
            "created_at": now_ts - 10.0,
            "confirmed_at": 0.0,
        },
    )
    monkeypatch.setattr(
        engine,
        "_write_platform_login_signal",
        lambda _platform, _profile, payload: writes.append(dict(payload)),
    )

    wait_token = engine._begin_platform_login_wait(
        platform_name="wechat",
        profile_dir="D:/profiles/wechat",
        open_url="https://channels.weixin.qq.com/login.html",
    )

    assert wait_token == "wait-token-existing"
    assert len(writes) == 1
    assert writes[0]["status"] == "waiting"
    assert writes[0]["token"] == "wait-token-existing"


def test_begin_platform_login_wait_rotates_stale_waiting_token(monkeypatch: pytest.MonkeyPatch) -> None:
    now_ts = 1_700_000_000.0
    writes: list[dict[str, Any]] = []

    class _FakeUuid:
        hex = "wait-token-new"

    monkeypatch.setattr(engine.time, "time", lambda: now_ts)
    monkeypatch.setattr(
        engine,
        "_read_platform_login_signal",
        lambda *_args, **_kwargs: {
            "platform": "wechat",
            "profile_dir": "D:/profiles/wechat",
            "open_url": "https://channels.weixin.qq.com/login.html",
            "status": "waiting",
            "token": "wait-token-old",
            "created_at": now_ts - float(engine.PLATFORM_LOGIN_WAIT_TOKEN_REUSE_SECONDS) - 1.0,
            "confirmed_at": 0.0,
        },
    )
    monkeypatch.setattr(engine.uuid, "uuid4", lambda: _FakeUuid())
    monkeypatch.setattr(
        engine,
        "_write_platform_login_signal",
        lambda _platform, _profile, payload: writes.append(dict(payload)),
    )

    wait_token = engine._begin_platform_login_wait(
        platform_name="wechat",
        profile_dir="D:/profiles/wechat",
        open_url="https://channels.weixin.qq.com/login.html",
    )

    assert wait_token == "wait-token-new"
    assert len(writes) == 1
    assert writes[0]["token"] == "wait-token-new"


def test_match_platform_login_gate_detects_wechat_login_failed_retry_message() -> None:
    result = engine._match_platform_login_gate_from_snapshot(
        "wechat",
        url="https://channels.weixin.qq.com/login.html",
        text="登录失败，稍后重试",
    )

    assert result["needs_login"] is True
    assert result["reason"] == "wechat_login_failed_retry"


def test_prepare_platform_login_qr_notice_keeps_current_login_gate_page(monkeypatch: pytest.MonkeyPatch) -> None:
    page = object()
    stabilize_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda *_args, **_kwargs: {"needs_login": True, "url": "https://channels.weixin.qq.com/login.html"},
    )
    monkeypatch.setattr(
        engine,
        "_stabilize_platform_session_page",
        lambda current_page, **kwargs: stabilize_calls.append(dict(kwargs)) or current_page,
    )
    monkeypatch.setattr(engine, "_extract_login_qr_source", lambda *_args, **_kwargs: "data:image/png;base64,QUJD")
    monkeypatch.setattr(engine, "_capture_login_qr_screenshot", lambda *_args, **_kwargs: b"png-bytes")

    result = engine._prepare_platform_login_qr_notice(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        page=page,
        chrome_user_data_dir="D:/profiles/wechat",
        auto_open_chrome=False,
    )

    assert result["ok"] is True
    assert result["needs_login"] is True
    assert stabilize_calls == []


def test_prepare_platform_login_qr_notice_freezes_existing_page_without_navigation(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakePage:
        def __init__(self) -> None:
            self.get_calls: list[str] = []
            self.refresh_calls = 0
            self.url = "https://channels.weixin.qq.com/platform/post/comment"

        def get(self, url: str) -> None:
            self.get_calls.append(str(url))

        def refresh(self) -> None:
            self.refresh_calls += 1

    page = FakePage()
    stabilize_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda *_args, **_kwargs: {"needs_login": False, "url": "https://channels.weixin.qq.com/platform/post/comment"},
    )
    monkeypatch.setattr(
        engine,
        "_stabilize_platform_session_page",
        lambda current_page, **kwargs: stabilize_calls.append(dict(kwargs)) or current_page,
    )
    monkeypatch.setattr(engine, "_is_platform_login_gate", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_extract_login_qr_source", lambda *_args, **_kwargs: "data:image/png;base64,QUJD")
    monkeypatch.setattr(engine, "_capture_login_qr_screenshot", lambda *_args, **_kwargs: b"png-bytes")

    result = engine._prepare_platform_login_qr_notice(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/comment",
        page=page,
        chrome_user_data_dir="D:/profiles/wechat",
        auto_open_chrome=False,
        refresh_page=True,
        allow_navigation=False,
    )

    assert result["ok"] is True
    assert result["needs_login"] is True
    assert stabilize_calls == []
    assert page.get_calls == []
    assert page.refresh_calls == 0


def test_prepare_platform_login_qr_notice_retries_screenshot_before_source_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    screenshot_calls: list[int] = []
    source_calls: list[str] = []

    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda *_args, **_kwargs: {"needs_login": True, "url": "https://channels.weixin.qq.com/login.html"},
    )
    monkeypatch.setattr(engine, "_extract_login_qr_source", lambda *_args, **_kwargs: "data:image/png;base64,QUJD")

    def fake_capture(*_args: Any, **_kwargs: Any) -> bytes:
        screenshot_calls.append(1)
        return b"" if len(screenshot_calls) == 1 else b"png-bytes"

    monkeypatch.setattr(engine, "_capture_login_qr_screenshot", fake_capture)
    monkeypatch.setattr(
        engine,
        "_load_qr_image_source",
        lambda source: source_calls.append(str(source)) or ("image/png", b"source-bytes"),
    )

    result = engine._prepare_platform_login_qr_notice(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        page=object(),
        chrome_user_data_dir="D:/profiles/wechat",
        auto_open_chrome=False,
    )

    assert result["ok"] is True
    assert len(screenshot_calls) == 2
    assert source_calls == []
    assert result["photo_bytes"] == b"png-bytes"


def test_wait_for_platform_login_confirmation_uses_stabilized_page_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakePage:
        def __init__(self, url: str) -> None:
            self.url = url

        def get(self, url: str) -> None:
            self.url = str(url)

        def refresh(self) -> None:
            return None

    login_page = FakePage("https://channels.weixin.qq.com/login.html")
    business_page = FakePage("https://channels.weixin.qq.com/platform/post/create")
    stabilize_calls: list[dict[str, Any]] = []
    marked_ready: list[dict[str, Any]] = []
    cleared_wait: list[dict[str, Any]] = []

    monkeypatch.setattr(
        engine,
        "_stabilize_platform_session_page",
        lambda page, **kwargs: stabilize_calls.append(dict(kwargs)) or business_page,
    )
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda page, _platform: {
            "needs_login": "login.html" in str(getattr(page, "url", "")),
            "url": str(getattr(page, "url", "")),
        },
    )
    monkeypatch.setattr(
        engine,
        "_clear_platform_login_signal",
        lambda platform_name, profile_dir, wait_token="": cleared_wait.append(
            {"platform": platform_name, "profile_dir": profile_dir, "wait_token": wait_token}
        ),
    )
    monkeypatch.setattr(
        engine,
        "_mark_platform_session_ready",
        lambda platform_name, profile_dir, **kwargs: marked_ready.append(
            {"platform": platform_name, "profile_dir": profile_dir, **dict(kwargs)}
        ),
    )

    confirmed = engine._wait_for_platform_login_confirmation(
        login_page,
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        chrome_user_data_dir="D:/profiles/wechat",
        wait_token="wait-1",
        timeout_seconds=1,
    )

    assert confirmed is True
    assert stabilize_calls
    assert all(bool(call.get("close_stale_login_tabs")) for call in stabilize_calls)
    assert len(marked_ready) == 1
    assert marked_ready[0]["url"] == "https://channels.weixin.qq.com/platform/post/create"
    assert len(cleared_wait) == 1
    assert cleared_wait[0]["wait_token"] == "wait-1"


def test_wait_for_platform_login_confirmation_wechat_confirmed_signal_triggers_entry_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakePage:
        def __init__(self, url: str) -> None:
            self.url = url
            self.get_calls: list[str] = []
            self.refresh_calls = 0

        def get(self, url: str) -> None:
            self.get_calls.append(str(url))
            self.url = str(url)

        def refresh(self) -> None:
            self.refresh_calls += 1

    page = FakePage("https://channels.weixin.qq.com/login.html")
    needs_login_sequence = [True, False]

    monkeypatch.setattr(engine, "_stabilize_platform_session_page", lambda current_page, **_kwargs: current_page)
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda current_page, _platform: {
            "needs_login": needs_login_sequence.pop(0) if needs_login_sequence else False,
            "url": str(getattr(current_page, "url", "")),
            "reason": "login_url" if needs_login_sequence else "",
        },
    )
    monkeypatch.setattr(
        engine,
        "_read_platform_login_signal",
        lambda *_args, **_kwargs: {
            "status": "confirmed",
            "token": "wait-1",
            "confirmed_at": 1.0,
        },
    )
    monkeypatch.setattr(engine, "_clear_platform_login_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_mark_platform_session_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_mark_platform_session_login_required", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    confirmed = engine._wait_for_platform_login_confirmation(
        page,
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        chrome_user_data_dir="D:/profiles/wechat",
        wait_token="wait-1",
        timeout_seconds=2,
    )

    assert confirmed is True
    assert page.get_calls == ["https://channels.weixin.qq.com/platform/post/create"]
    assert page.refresh_calls == 0


def test_wait_for_platform_login_confirmation_wechat_keeps_long_timeout_window(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakePage:
        def __init__(self, url: str) -> None:
            self.url = url

        def get(self, url: str) -> None:
            self.url = str(url)

        def refresh(self) -> None:
            return None

    page = FakePage("https://channels.weixin.qq.com/login.html")
    inspect_calls: list[str] = []

    monkeypatch.setattr(engine, "_stabilize_platform_session_page", lambda current_page, **_kwargs: current_page)
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda current_page, _platform: inspect_calls.append(str(getattr(current_page, "url", ""))) or {
            "needs_login": True,
            "url": str(getattr(current_page, "url", "")),
            "reason": "login_url",
        },
    )
    monkeypatch.setattr(engine, "_read_platform_login_signal", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(engine, "_clear_platform_login_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_mark_platform_session_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_mark_platform_session_login_required", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)
    time_samples = iter([100.0, 100.0, 100.0, 131.0, 146.0])
    monkeypatch.setattr(engine.time, "time", lambda: next(time_samples))

    confirmed = engine._wait_for_platform_login_confirmation(
        page,
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        chrome_user_data_dir="D:/profiles/wechat",
        wait_token="wait-1",
        timeout_seconds=45,
    )

    assert confirmed is False
    assert len(inspect_calls) == 3


def test_wait_for_platform_login_confirmation_wechat_login_failed_toast_triggers_qr_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakePage:
        def __init__(self, url: str) -> None:
            self.url = url
            self.get_calls: list[str] = []
            self.refresh_calls = 0

        def get(self, url: str) -> None:
            self.url = str(url)
            self.get_calls.append(str(url))

        def refresh(self) -> None:
            self.refresh_calls += 1

    page = FakePage("https://channels.weixin.qq.com/login.html")
    inspect_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(engine, "_stabilize_platform_session_page", lambda current_page, **_kwargs: current_page)
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda current_page, _platform: inspect_calls.append({"url": str(getattr(current_page, "url", ""))}) or {
            "needs_login": True,
            "url": str(getattr(current_page, "url", "")),
            "reason": "wechat_login_failed_retry",
        },
    )
    monkeypatch.setattr(engine, "_read_platform_login_signal", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(engine, "_clear_platform_login_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_mark_platform_session_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_mark_platform_session_login_required", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)
    time_samples = iter([100.0, 100.0, 100.1, 104.1, 104.2, 111.0])
    monkeypatch.setattr(engine.time, "time", lambda: next(time_samples))

    confirmed = engine._wait_for_platform_login_confirmation(
        page,
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        chrome_user_data_dir="D:/profiles/wechat",
        wait_token="wait-1",
        timeout_seconds=10,
    )

    assert confirmed is False
    assert page.get_calls == ["https://channels.weixin.qq.com/platform/post/create"]
    assert page.refresh_calls == 0
    assert len(inspect_calls) == 3


def test_build_platform_login_qr_reply_markup_contains_done_and_refresh_actions() -> None:
    wait_token = "wait-token-1"

    reply_markup = engine._build_platform_login_qr_reply_markup(
        platform_name="wechat",
        wait_token=wait_token,
    )
    rows = reply_markup.get("inline_keyboard")
    assert isinstance(rows, list)
    assert rows
    buttons = rows[0]
    assert isinstance(buttons, list)
    texts = [str(button.get("text") or "") for button in buttons if isinstance(button, dict)]
    callback_data = [str(button.get("callback_data") or "") for button in buttons if isinstance(button, dict)]

    assert "我已登录" in texts
    assert "刷新二维码" in texts
    assert engine._platform_login_callback_data("done", "wechat", wait_token) in callback_data
    assert engine._platform_login_callback_data("refresh", "wechat", wait_token) in callback_data


def test_write_json_atomic_replaces_target_without_temp_residue(tmp_path) -> None:
    path = tmp_path / "wechat_state.json"
    path.write_text("{}", encoding="utf-8")

    engine._write_json_atomic(path, {"status": "ready", "page_title_excerpt": "视频号助手"})

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["status"] == "ready"
    assert payload["page_title_excerpt"] == "视频号助手"
    assert not list(tmp_path.glob("*.tmp"))


def test_build_login_qr_rect_script_wechat_skips_bare_canvas_selector() -> None:
    script = engine._build_login_qr_rect_script("wechat")

    assert "\"canvas\"" not in script
    assert "[class*='qrcode'] canvas" in script
    assert "[class*='scan'] canvas" in script


def test_build_login_qr_rect_script_non_wechat_keeps_bare_canvas_selector() -> None:
    script = engine._build_login_qr_rect_script("kuaishou")

    assert "\"canvas\"" in script
    assert "img[alt*='qrcode' i]" in script


def test_prepare_platform_login_qr_surface_wechat_restores_window_and_scrolls(monkeypatch: pytest.MonkeyPatch) -> None:
    pauses: list[str] = []

    class FakeCtx:
        def __init__(self) -> None:
            self.scripts: list[str] = []

        def run_js(self, script: str, *_args: Any) -> Any:
            self.scripts.append(script)
            return {"prepared": True, "reason": "wechat-scroll-qr-into-view"}

    class FakePage(FakeCtx):
        def __init__(self) -> None:
            super().__init__()
            self.cdp_calls: list[tuple[str, dict[str, Any]]] = []
            self.frame = FakeCtx()

        def run_cdp(self, method: str, **kwargs: Any) -> dict[str, Any]:
            self.cdp_calls.append((method, dict(kwargs)))
            if method == "Browser.getWindowForTarget":
                return {"windowId": 7}
            return {}

        def get_frames(self, timeout: float | None = None) -> list[FakeCtx]:
            assert timeout == 1.2
            return [self.frame]

    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda reason: pauses.append(str(reason)))

    page = FakePage()
    engine._prepare_platform_login_qr_surface(page, "wechat")

    assert page.cdp_calls[0] == ("Browser.getWindowForTarget", {})
    assert page.cdp_calls[1][0] == "Browser.setWindowBounds"
    assert page.cdp_calls[1][1]["bounds"]["windowState"] == "normal"
    assert page.cdp_calls[2][0] == "Browser.setWindowBounds"
    assert page.cdp_calls[2][1]["bounds"]["width"] == 1280
    assert page.scripts
    assert pauses == ["wechat login qr surface settle"]


def test_capture_login_qr_screenshot_prepares_wechat_surface_before_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    prepare_calls: list[str] = []

    class FakePage:
        def __init__(self) -> None:
            self.screenshot_calls: list[dict[str, Any]] = []

        def get_frames(self, timeout: float | None = None) -> list[Any]:
            assert timeout == 1.5
            return []

        def run_js(self, _script: str, *_args: Any) -> Any:
            return {"left": 12, "top": 24, "right": 220, "bottom": 232}

        def get_screenshot(self, **kwargs: Any) -> bytes:
            self.screenshot_calls.append(dict(kwargs))
            return b"png-bytes"

    monkeypatch.setattr(engine, "_prepare_platform_login_qr_surface", lambda _page, platform: prepare_calls.append(str(platform)))

    page = FakePage()
    shot = engine._capture_login_qr_screenshot(page, "wechat")

    assert shot == b"png-bytes"
    assert prepare_calls == ["wechat"]
    assert page.screenshot_calls[0]["left_top"] == (12, 24)
    assert page.screenshot_calls[0]["right_bottom"] == (220, 232)


def test_send_telegram_photo_retries_after_connection_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_call(**kwargs: Any) -> dict[str, Any]:
        calls.append(dict(kwargs))
        return {"ok": True, "result": {"message_id": 321}}

    monkeypatch.setattr(engine, "shared_call_telegram_api", fake_call)

    payload = engine._send_telegram_photo(
        bot_token="token",
        chat_id="chat",
        photo_bytes=b"png-bytes",
        filename="wechat_login_qr.png",
        caption="scan me",
        timeout_seconds=15,
        api_base="https://api.telegram.org",
        parse_mode="HTML",
    )

    assert payload["ok"] is True
    assert len(calls) == 1
    assert calls[0]["method"] == "sendPhoto"
    assert calls[0]["use_post"] is True
    assert calls[0]["max_retries"] == 2
    assert "files" in calls[0]


def test_send_platform_login_qr_notification_returns_structured_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        engine,
        "_prepare_platform_login_qr_notice",
        lambda **_kwargs: {
            "ok": True,
            "needs_login": True,
            "platform": "wechat",
            "profile_dir": "D:/profiles/wechat",
            "runtime_debug_port": 9334,
            "open_target_url": "https://channels.weixin.qq.com/login.html",
            "mime": "image/png",
            "photo_bytes": b"png-bytes",
            "filename": "wechat_login_qr.png",
            "caption": "scan me",
            "reply_markup": {"inline_keyboard": []},
            "fingerprint": "fp-1",
            "cache_key": "wechat|9334|D:/profiles/wechat",
        },
    )
    monkeypatch.setattr(
        engine,
        "_resolve_runtime_telegram_notify_settings",
        lambda **_kwargs: engine.NotifySettings(
            enabled=True,
            provider="telegram_bot",
            env_prefix="CYBERCAR_NOTIFY_",
            telegram_bot_token="token",
            telegram_chat_id="chat",
            telegram_timeout_seconds=20,
            telegram_api_base="https://api.telegram.org",
        ),
    )
    monkeypatch.setattr(
        engine,
        "_send_telegram_photo",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("ConnectionResetError(10054)")),
    )

    result = engine.send_platform_login_qr_notification(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/login.html",
        telegram_bot_token="token",
        telegram_chat_id="chat",
    )

    assert result["ok"] is False
    assert result["sent"] is False
    assert result["needs_login"] is True
    assert result["transport_error"] is True
    assert result["qr_prepared"] is True


def test_send_platform_login_qr_notification_skips_preparation_when_recent_qr_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    prepare_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        engine,
        "_prepare_platform_login_qr_notice",
        lambda **kwargs: prepare_calls.append(dict(kwargs)) or {"ok": True},
    )
    cache_key = engine._platform_login_qr_cache_key("wechat", 9334, "D:/profiles/wechat")
    engine.WECHAT_LOGIN_QR_NOTICE_CACHE[cache_key.lower()] = ("fp-existing", engine.time.time())

    try:
        result = engine.send_platform_login_qr_notification(
            platform_name="wechat",
            open_url="https://channels.weixin.qq.com/login.html",
            debug_port=9334,
            chrome_user_data_dir="D:/profiles/wechat",
            auto_open_chrome=False,
            allow_duplicate=False,
            telegram_bot_token="token",
            telegram_chat_id="chat",
        )
    finally:
        engine.WECHAT_LOGIN_QR_NOTICE_CACHE.pop(cache_key.lower(), None)

    assert result["ok"] is True
    assert result["needs_login"] is True
    assert result["sent"] is False
    assert result["skipped"] is True
    assert prepare_calls == []


def test_wechat_publish_feedback_timeout_is_extended_to_60_seconds() -> None:
    assert engine.WECHAT_PUBLISH_FEEDBACK_TIMEOUT_SECONDS == 60


def test_is_wechat_publish_confirmed_from_state_accepts_manage_progress_state() -> None:
    assert engine._is_wechat_publish_confirmed_from_state(
        {
            "failure_hint": False,
            "success_hint": False,
            "progress_hint": True,
            "manage_hint": True,
            "compose_hint": False,
            "has_draft_action": False,
            "has_publish_action": False,
            "url": "https://channels.weixin.qq.com/platform/post/list",
        }
    )


def test_is_wechat_publish_confirmed_from_state_rejects_compose_progress_state() -> None:
    assert not engine._is_wechat_publish_confirmed_from_state(
        {
            "failure_hint": False,
            "success_hint": False,
            "progress_hint": True,
            "manage_hint": False,
            "compose_hint": True,
            "has_draft_action": True,
            "has_publish_action": True,
            "url": "https://channels.weixin.qq.com/platform/post/create",
        }
    )


def test_is_wechat_publish_confirmed_from_state_accepts_manage_list_with_publish_entry() -> None:
    assert engine._is_wechat_publish_confirmed_from_state(
        {
            "failure_hint": False,
            "success_hint": False,
            "progress_hint": False,
            "manage_hint": False,
            "compose_hint": False,
            "has_draft_action": False,
            "has_publish_action": True,
            "url": "https://channels.weixin.qq.com/micro/content/post/list",
        }
    )


def test_is_wechat_publish_submission_in_flight_accepts_progress_state() -> None:
    assert engine._is_wechat_publish_submission_in_flight(
        {
            "failure_hint": False,
            "success_hint": False,
            "progress_hint": True,
            "manage_hint": False,
            "compose_hint": True,
        }
    )


def test_wait_wechat_publish_feedback_probes_post_list_early(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 0.0}
    verify_calls: list[tuple[str, float, float]] = []

    class FakePage:
        def get(self, _url: str) -> None:
            return None

        def run_js(self, _script: str, *_args: Any) -> Any:
            return {}

    monkeypatch.setattr(engine.time, "time", lambda: float(clock["now"]))
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: clock.__setitem__("now", float(clock["now"]) + float(seconds)))
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_read_wechat_publish_state",
        lambda *_args, **_kwargs: {
            "failure_hint": False,
            "success_hint": False,
            "progress_hint": True,
            "manage_hint": False,
            "compose_hint": True,
            "has_draft_action": True,
            "has_publish_action": True,
            "url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])

    def fake_verify(page: Any, expected_title: str, timeout_seconds: float = 0.0) -> dict[str, Any]:
        verify_calls.append((expected_title, float(timeout_seconds), float(clock["now"])))
        assert page is not None
        return {"title": expected_title}

    monkeypatch.setattr(engine, "_verify_wechat_publish_in_post_list", fake_verify)

    engine._wait_wechat_publish_feedback(None, FakePage(), expected_title="发布标题", timeout_seconds=60)

    assert verify_calls
    assert verify_calls[0][0] == "发布标题"
    assert verify_calls[0][1] == 6.0
    assert verify_calls[0][2] < 60.0


def test_wait_wechat_publish_feedback_extends_deadline_for_inflight_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 0.0}
    verify_calls: list[float] = []

    class FakePage:
        def get(self, _url: str) -> None:
            return None

        def run_js(self, _script: str, *_args: Any) -> Any:
            return {}

    monkeypatch.setattr(engine.time, "time", lambda: float(clock["now"]))
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: clock.__setitem__("now", float(clock["now"]) + float(seconds)))
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_read_wechat_publish_state",
        lambda *_args, **_kwargs: {
            "failure_hint": False,
            "success_hint": False,
            "progress_hint": True,
            "manage_hint": False,
            "compose_hint": True,
            "has_draft_action": True,
            "has_publish_action": True,
            "url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])

    def fake_verify(_page: Any, expected_title: str, timeout_seconds: float = 0.0) -> dict[str, Any] | None:
        del expected_title, timeout_seconds
        verify_calls.append(float(clock["now"]))
        if clock["now"] >= 70.0:
            return {"title": "发布标题"}
        return None

    monkeypatch.setattr(engine, "_verify_wechat_publish_in_post_list", fake_verify)

    engine._wait_wechat_publish_feedback(None, FakePage(), expected_title="发布标题", timeout_seconds=60)

    assert verify_calls
    assert max(verify_calls) >= 70.0
    assert clock["now"] >= 70.0
    assert clock["now"] < 105.0


def test_wait_wechat_publish_feedback_accepts_publish_click_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    read_calls: list[str] = []
    log_messages: list[str] = []

    monkeypatch.setattr(
        engine,
        "_read_wechat_publish_state",
        lambda *_args, **_kwargs: read_calls.append("read") or {},
    )
    monkeypatch.setattr(engine, "_log", lambda message: log_messages.append(str(message)))

    engine._wait_wechat_publish_feedback(None, None, publish_click_confirmed=True)

    assert read_calls == []
    assert any("Publish button click accepted" in item for item in log_messages)


def test_wait_upload_ready_accepts_hidden_form_media_heuristic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        engine,
        "_read_editor_status",
        lambda *_args, **_kwargs: {
            "text": "",
            "progress": "",
            "uploadHidden": True,
            "formBtnsReady": True,
            "descLabelReady": True,
            "descEditorReady": True,
            "mediaReady": True,
            "busy": False,
            "done": False,
        },
    )
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._wait_upload_ready(object(), "editor", timeout_seconds=2)
    assert result == "editor"


def test_wait_upload_ready_timeout_uses_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    timeline = {"now": 0.0}
    monkeypatch.setattr(engine.time, "time", lambda: float(timeline["now"]))
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: timeline.__setitem__("now", float(timeline["now"]) + float(seconds)))
    monkeypatch.setattr(
        engine,
        "_read_editor_status",
        lambda *_args, **_kwargs: {
            "text": "uploading",
            "progress": "",
            "uploadHidden": False,
            "formBtnsReady": False,
            "descLabelReady": False,
            "descEditorReady": False,
            "mediaReady": False,
            "busy": True,
            "done": False,
        },
    )
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    with pytest.raises(TimeoutError, match="E_UPLOAD_TIMEOUT"):
        engine._wait_upload_ready(object(), "editor", timeout_seconds=2)


def test_fill_draft_once_defaults_publish_click_confirmation_to_false(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    target = tmp_path / "wechat-publish.mp4"
    target.write_bytes(b"video")
    wait_calls: list[dict[str, Any]] = []

    class FakeFileInput:
        def input(self, _path: str) -> None:
            return None

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_wechat_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda *_args, **_kwargs: "editor")
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _name, action, retries=3: action())
    monkeypatch.setattr(engine, "_find_upload_file_input", lambda *_args, **_kwargs: FakeFileInput())
    monkeypatch.setattr(engine, "_wait_upload_ready", lambda _page, ctx, timeout_seconds=0: ctx)
    monkeypatch.setattr(engine, "_clear_location_if_selected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_wechat_short_title", lambda *_args, **_kwargs: "发布标题")
    monkeypatch.setattr(engine, "_select_collection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_wechat_primary_publish_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    def fake_wait(*_args: Any, **kwargs: Any) -> None:
        wait_calls.append(dict(kwargs))

    monkeypatch.setattr(engine, "_wait_wechat_publish_feedback", fake_wait)

    result = engine._fill_draft_once(
        FakePage(),
        target,
        "caption",
        "collection",
        False,
        True,
        False,
        30,
    )

    assert result == "editor"
    assert wait_calls == [{"expected_title": "发布标题", "publish_click_confirmed": False}]


def test_fill_draft_once_uses_configured_short_title(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    target = tmp_path / "wechat-short-title.mp4"
    target.write_bytes(b"video")
    captured: dict[str, Any] = {}

    class FakeFileInput:
        def input(self, _path: str) -> None:
            return None

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_wechat_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda *_args, **_kwargs: "editor")
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _name, action, retries=3: action())
    monkeypatch.setattr(engine, "_find_upload_file_input", lambda *_args, **_kwargs: FakeFileInput())
    monkeypatch.setattr(engine, "_wait_upload_ready", lambda _page, ctx, timeout_seconds=0: ctx)
    monkeypatch.setattr(engine, "_clear_location_if_selected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_select_collection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_save_draft", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    def fake_short_title(_ctx: Any, title: str, *, configured: bool = False) -> str:
        captured["title"] = title
        captured["configured"] = configured
        return title

    monkeypatch.setattr(engine, "_fill_wechat_short_title", fake_short_title)

    engine._fill_draft_once(
        FakePage(),
        target,
        "天然气发电描述很长",
        "collection",
        True,
        False,
        False,
        30,
        short_title="GasGx",
    )

    assert captured == {"title": "GasGx", "configured": True}


def test_fill_draft_once_passes_publish_click_confirmation_to_feedback_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    target = tmp_path / "wechat-publish-confirmed.mp4"
    target.write_bytes(b"video")
    wait_calls: list[dict[str, Any]] = []

    class FakeFileInput:
        def input(self, _path: str) -> None:
            return None

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_wechat_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda *_args, **_kwargs: "editor")
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _name, action, retries=3: action())
    monkeypatch.setattr(engine, "_find_upload_file_input", lambda *_args, **_kwargs: FakeFileInput())
    monkeypatch.setattr(engine, "_wait_upload_ready", lambda _page, ctx, timeout_seconds=0: ctx)
    monkeypatch.setattr(engine, "_clear_location_if_selected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_wechat_short_title", lambda *_args, **_kwargs: "发布标题")
    monkeypatch.setattr(engine, "_select_collection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_wechat_primary_publish_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    def fake_wait(*_args: Any, **kwargs: Any) -> None:
        wait_calls.append(dict(kwargs))

    monkeypatch.setattr(engine, "_wait_wechat_publish_feedback", fake_wait)

    result = engine._fill_draft_once(
        FakePage(),
        target,
        "caption",
        "collection",
        False,
        True,
        False,
        30,
        wechat_publish_click_confirmed=True,
    )

    assert result == "editor"
    assert wait_calls == [{"expected_title": "发布标题", "publish_click_confirmed": True}]


def test_fill_draft_once_wechat_publish_button_missing_falls_back_to_draft(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    target = tmp_path / "wechat-publish-fallback.mp4"
    target.write_bytes(b"video")
    fallback_save_calls: list[str] = []

    class FakeFileInput:
        def input(self, _path: str) -> None:
            return None

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_wechat_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda *_args, **_kwargs: "editor")
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _name, action, retries=3: action())
    monkeypatch.setattr(engine, "_find_upload_file_input", lambda *_args, **_kwargs: FakeFileInput())
    monkeypatch.setattr(engine, "_wait_upload_ready", lambda _page, ctx, timeout_seconds=0: ctx)
    monkeypatch.setattr(engine, "_clear_location_if_selected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_wechat_short_title", lambda *_args, **_kwargs: "wechat title")
    monkeypatch.setattr(engine, "_select_collection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_wechat_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_save_draft", lambda *_args, **_kwargs: fallback_save_calls.append("saved"))
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="automatically saved as draft"):
        engine._fill_draft_once(
            FakePage(),
            target,
            "caption",
            "collection",
            False,
            True,
            False,
            30,
        )

    assert fallback_save_calls == ["saved"]


def test_fill_draft_once_wechat_publish_button_missing_raises_coded_error_when_draft_fallback_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    target = tmp_path / "wechat-publish-fallback-failed.mp4"
    target.write_bytes(b"video")

    class FakeFileInput:
        def input(self, _path: str) -> None:
            return None

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_wechat_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda *_args, **_kwargs: "editor")
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _name, action, retries=3: action())
    monkeypatch.setattr(engine, "_find_upload_file_input", lambda *_args, **_kwargs: FakeFileInput())
    monkeypatch.setattr(engine, "_wait_upload_ready", lambda _page, ctx, timeout_seconds=0: ctx)
    monkeypatch.setattr(engine, "_clear_location_if_selected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_wechat_short_title", lambda *_args, **_kwargs: "wechat title")
    monkeypatch.setattr(engine, "_select_collection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_wechat_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        engine,
        "_save_draft",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("save failed")),
    )
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="E_PUBLISH_BUTTON_MISSING") as exc_info:
        engine._fill_draft_once(
            FakePage(),
            target,
            "caption",
            "collection",
            False,
            True,
            False,
            30,
        )

    assert "draft fallback attempt failed" in str(exc_info.value)
    assert "Failed to locate publish button" not in str(exc_info.value)


def test_fill_draft_once_wechat_publish_unconfirmed_falls_back_to_draft(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    target = tmp_path / "wechat-publish-unconfirmed.mp4"
    target.write_bytes(b"video")
    fallback_save_calls: list[str] = []

    class FakeFileInput:
        def input(self, _path: str) -> None:
            return None

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_wechat_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda *_args, **_kwargs: "editor")
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _name, action, retries=3: action())
    monkeypatch.setattr(engine, "_find_upload_file_input", lambda *_args, **_kwargs: FakeFileInput())
    monkeypatch.setattr(engine, "_wait_upload_ready", lambda _page, ctx, timeout_seconds=0: ctx)
    monkeypatch.setattr(engine, "_clear_location_if_selected", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_wechat_short_title", lambda *_args, **_kwargs: "wechat title")
    monkeypatch.setattr(engine, "_select_collection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_wechat_primary_publish_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_wait_wechat_publish_feedback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("wechat publish timeout")),
    )
    monkeypatch.setattr(engine, "_save_draft", lambda *_args, **_kwargs: fallback_save_calls.append("saved"))
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="automatically saved as draft"):
        engine._fill_draft_once(
            FakePage(),
            target,
            "caption",
            "collection",
            False,
            True,
            False,
            30,
        )

    assert fallback_save_calls == ["saved"]


def test_get_page_frames_with_timeout_returns_empty_when_browser_hangs() -> None:
    class FakePage:
        def get_frames(self, timeout: float = 0.0) -> list[Any]:
            del timeout
            time.sleep(0.5)
            return [object()]

    started_at = time.perf_counter()
    frames = engine._get_page_frames_with_timeout(FakePage(), timeout_seconds=0.05)
    elapsed = time.perf_counter() - started_at

    assert frames == []
    assert elapsed < 0.35


def test_find_upload_file_input_uses_fast_probe_and_generic_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    probe_timeouts: list[float] = []
    fallback_result = object()

    class FakeOwner:
        def ele(self, _selector: str, timeout: float = 0.0) -> Any:
            probe_timeouts.append(float(timeout))
            return None

    monkeypatch.setattr(engine, "_collect_upload_contexts", lambda *_args, **_kwargs: [FakeOwner()])
    monkeypatch.setattr(engine, "_find_upload_file_input_generic", lambda *_args, **_kwargs: fallback_result)

    result = engine._find_upload_file_input(object(), object())

    assert result is fallback_result
    assert probe_timeouts
    assert max(probe_timeouts) <= 1.2


def test_merge_comment_reply_config_uses_longer_random_waits() -> None:
    cfg = engine._merge_comment_reply_config({})

    assert cfg["reply_min_chars"] == 5
    assert cfg["reply_max_chars"] == 20
    assert cfg["min_reply_interval_seconds"] == 3
    assert cfg["max_reply_interval_seconds"] == 10
    assert cfg["min_like_to_reply_interval_seconds"] == 3
    assert cfg["max_like_to_reply_interval_seconds"] == 10
    assert cfg["self_author_markers"] == ["cybercar"]


def test_apply_comment_reply_like_to_reply_wait_uses_random_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []

    monkeypatch.setattr(engine.random, "uniform", lambda a, b: 6.25)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: waits.append(float(seconds)))

    waited = engine._apply_comment_reply_like_to_reply_wait(
        {
            "min_like_to_reply_interval_seconds": 3,
            "max_like_to_reply_interval_seconds": 10,
        },
        debug=False,
    )

    assert waited == 6.25
    assert waits == [6.25]


def test_humanized_wechat_comment_pause_prefers_page_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    timeout_calls: list[int] = []
    sleep_calls: list[float] = []
    uniform_calls: list[tuple[float, float]] = []

    class FakePage:
        def wait_for_timeout(self, milliseconds: int) -> None:
            timeout_calls.append(int(milliseconds))

    def fake_uniform(a: float, b: float) -> float:
        uniform_calls.append((float(a), float(b)))
        return 4.2

    monkeypatch.setattr(engine.random, "uniform", fake_uniform)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: sleep_calls.append(float(seconds)))

    waited = engine._humanized_wechat_comment_reaction_pause(FakePage(), "wechat comment submit click")

    assert waited == 4.2
    assert uniform_calls == [(3.0, 10.0)]
    assert timeout_calls == [4200]
    assert sleep_calls == []


def test_append_comment_reply_markdown_writes_json_code_block(tmp_path) -> None:
    target = tmp_path / "runtime" / "wechat_comment_reply_records.md"

    engine._append_comment_reply_markdown(
        target,
        "wechat",
        {
            "fingerprint": "abc123",
            "comment_preview": "奇丑无比",
            "reply_text": "喜欢的就喜欢",
            "replied_at": "2026-03-18 10:11:12",
        },
    )

    body = target.read_text(encoding="utf-8")
    assert "## 2026-03-18 10:11:12 | wechat" in body
    assert "```json" in body
    assert '"fingerprint": "abc123"' in body
    assert '"reply_text": "喜欢的就喜欢"' in body


def test_probe_platform_session_ready_keeps_browser_connected_for_wechat_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disconnect_calls: list[int] = []
    ready_marks: list[dict[str, Any]] = []

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_connect_chrome", lambda **_kwargs: FakePage())
    monkeypatch.setattr(engine, "_stabilize_platform_session_page", lambda page, **_kwargs: page)
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda *_args, **_kwargs: {
            "needs_login": False,
            "reason": "",
            "url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(
        engine,
        "_page_current_url",
        lambda *_args, **_kwargs: "https://channels.weixin.qq.com/platform/post/create",
    )
    monkeypatch.setattr(engine, "_is_platform_session_monitor_relevant_url", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_has_recent_platform_session_ready", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_maybe_touch_wechat_login_keepalive",
        lambda *_args, **_kwargs: {
            "ok": True,
            "performed": False,
            "skipped": True,
            "reason": "interval_not_due",
            "last_keepalive_at": 100.0,
            "interval_seconds": 1800,
        },
    )
    monkeypatch.setattr(engine, "_disconnect_chrome_page_quietly", lambda *_args, **_kwargs: disconnect_calls.append(1))
    monkeypatch.setattr(
        engine,
        "_mark_platform_session_ready",
        lambda *args, **kwargs: ready_marks.append({"args": args, "kwargs": kwargs}),
    )

    result = engine.probe_platform_session_via_debug_port(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        debug_port=9334,
        chrome_user_data_dir="D:/profiles/wechat",
        disconnect_after_probe=False,
        enable_wechat_keepalive=True,
    )

    assert result["status"] == "ready"
    assert result["platform"] == "wechat"
    assert result["keepalive"]["enabled"] is True
    assert result["keepalive"]["performed"] is False
    assert disconnect_calls == []
    assert len(ready_marks) == 1


def test_probe_platform_session_ready_records_wechat_keepalive_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready_marks: list[dict[str, Any]] = []

    class FakePage:
        url = "https://channels.weixin.qq.com/platform/post/create"

    monkeypatch.setattr(engine, "_connect_chrome", lambda **_kwargs: FakePage())
    monkeypatch.setattr(engine, "_stabilize_platform_session_page", lambda page, **_kwargs: page)
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda *_args, **_kwargs: {
            "needs_login": False,
            "reason": "",
            "url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(
        engine,
        "_page_current_url",
        lambda *_args, **_kwargs: "https://channels.weixin.qq.com/platform/post/create",
    )
    monkeypatch.setattr(engine, "_is_platform_session_monitor_relevant_url", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_has_recent_platform_session_ready", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_maybe_touch_wechat_login_keepalive",
        lambda *_args, **_kwargs: {
            "ok": True,
            "performed": True,
            "skipped": False,
            "reason": "",
            "keepalive_at": 123.0,
            "current_url": "https://channels.weixin.qq.com/platform/post/create",
            "interval_seconds": 1800,
        },
    )
    monkeypatch.setattr(engine, "_disconnect_chrome_page_quietly", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        engine,
        "_mark_platform_session_ready",
        lambda *args, **kwargs: ready_marks.append({"args": args, "kwargs": kwargs}),
    )

    result = engine.probe_platform_session_via_debug_port(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        debug_port=9334,
        chrome_user_data_dir="D:/profiles/wechat",
        enable_wechat_keepalive=True,
    )

    assert result["status"] == "ready"
    assert result["keepalive"]["performed"] is True
    assert len(ready_marks) == 1
    assert ready_marks[0]["kwargs"]["keepalive_at"] == 123.0


def test_probe_platform_session_rechecks_wechat_when_active_tab_is_irrelevant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakePage:
        def __init__(self) -> None:
            self.url = "chrome://newtab/"
            self.get_calls: list[str] = []

        def get(self, url: str) -> None:
            self.get_calls.append(str(url))
            self.url = str(url)

    page = FakePage()
    inspect_calls = {"count": 0}

    monkeypatch.setattr(engine, "_connect_chrome", lambda **_kwargs: page)
    monkeypatch.setattr(engine, "_stabilize_platform_session_page", lambda current_page, **_kwargs: current_page)

    def fake_inspect(*_args, **_kwargs):
        inspect_calls["count"] += 1
        if inspect_calls["count"] == 1:
            return {"needs_login": False, "reason": "", "url": "chrome://newtab/"}
        return {
            "needs_login": False,
            "reason": "",
            "url": "https://channels.weixin.qq.com/platform/post/create",
        }

    monkeypatch.setattr(engine, "inspect_platform_login_gate", fake_inspect)
    monkeypatch.setattr(engine, "_page_current_url", lambda current: str(getattr(current, "url", "") or ""))
    monkeypatch.setattr(engine, "_disconnect_chrome_page_quietly", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_has_recent_platform_session_ready", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_mark_platform_session_ready", lambda *_args, **_kwargs: None)

    result = engine.probe_platform_session_via_debug_port(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        debug_port=9334,
        chrome_user_data_dir="D:/profiles/wechat",
        enable_wechat_keepalive=False,
    )

    assert result["status"] == "ready"
    assert page.get_calls == ["https://channels.weixin.qq.com/platform/post/create"]


def test_wechat_keepalive_does_not_reload_when_current_page_is_relevant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakePage:
        def __init__(self) -> None:
            self.url = "https://channels.weixin.qq.com/platform/post/create"
            self.get_calls: list[str] = []
            self.refresh_calls = 0

        def get(self, url: str) -> None:
            self.get_calls.append(str(url))
            self.url = str(url)

        def refresh(self) -> None:
            self.refresh_calls += 1

    page = FakePage()

    monkeypatch.setattr(engine.time, "time", lambda: 2000.0)
    monkeypatch.setattr(
        engine,
        "_read_platform_session_state",
        lambda *_args, **_kwargs: {"status": "ready", "keepalive_at": 0},
    )
    monkeypatch.setattr(engine, "_resolve_wechat_keepalive_interval_seconds", lambda *_args, **_kwargs: 300)
    monkeypatch.setattr(engine, "_page_current_url", lambda current: str(getattr(current, "url", "") or ""))
    monkeypatch.setattr(engine, "_is_platform_session_monitor_relevant_url", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda *_args, **_kwargs: {
            "needs_login": False,
            "reason": "",
            "url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )
    monkeypatch.setattr(engine, "_build_platform_login_diagnostics", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(engine, "_mark_platform_session_ready", lambda *_args, **_kwargs: None)

    result = engine._maybe_touch_wechat_login_keepalive(
        page,
        chrome_user_data_dir="D:/profiles/wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
    )

    assert result["ok"] is True
    assert result["performed"] is True
    assert result["login_required"] is False
    assert page.get_calls == []
    assert page.refresh_calls == 0


def test_wechat_keepalive_uses_transient_probe_tab_when_active_page_is_irrelevant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Setter:
        def auto_handle_alert(self, **_kwargs: Any) -> None:
            return None

    class FakeTab:
        def __init__(self) -> None:
            self.tab_id = "keepalive-tab"
            self.url = "about:blank"
            self.get_calls: list[str] = []
            self.closed = False
            self.set = _Setter()

        def get(self, url: str) -> None:
            self.get_calls.append(str(url))
            self.url = str(url)

        def close(self) -> None:
            self.closed = True

    class FakePage:
        def __init__(self, tab: FakeTab) -> None:
            self.tab_id = "root-tab"
            self.url = "chrome://newtab/"
            self._tab = tab
            self.new_tab_calls: list[bool] = []
            self.get_calls: list[str] = []
            self.refresh_calls = 0

        def new_tab(self, background: bool = False) -> FakeTab:
            self.new_tab_calls.append(bool(background))
            return self._tab

        def get(self, url: str) -> None:
            self.get_calls.append(str(url))
            self.url = str(url)

        def refresh(self) -> None:
            self.refresh_calls += 1

    tab = FakeTab()
    page = FakePage(tab)

    monkeypatch.setattr(engine.time, "time", lambda: 2000.0)
    monkeypatch.setattr(engine, "_resolve_wechat_keepalive_interval_seconds", lambda *_args, **_kwargs: 300)
    monkeypatch.setattr(engine, "_read_platform_session_state", lambda *_args, **_kwargs: {"status": "ready", "keepalive_at": 0})
    monkeypatch.setattr(engine, "_page_current_url", lambda current: str(getattr(current, "url", "") or ""))
    monkeypatch.setattr(
        engine,
        "_is_platform_session_monitor_relevant_url",
        lambda _platform, current_url, _open_url="": "channels.weixin.qq.com/" in str(current_url or "").lower(),
    )
    monkeypatch.setattr(
        engine,
        "inspect_platform_login_gate",
        lambda current, *_args, **_kwargs: {
            "needs_login": False,
            "reason": "",
            "url": str(getattr(current, "url", "") or ""),
        },
    )
    monkeypatch.setattr(engine, "_build_platform_login_diagnostics", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(engine, "_mark_platform_session_ready", lambda *_args, **_kwargs: None)

    result = engine._maybe_touch_wechat_login_keepalive(
        page,
        chrome_user_data_dir="D:/profiles/wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
    )

    assert result["ok"] is True
    assert result["performed"] is True
    assert result["login_required"] is False
    assert result["probe_source"] == "transient_tab"
    assert page.get_calls == []
    assert page.refresh_calls == 0
    assert page.new_tab_calls != []
    assert tab.get_calls == ["https://channels.weixin.qq.com/platform/post/create"]
    assert tab.closed is True


def test_send_platform_login_text_notification_is_disabled() -> None:
    result = engine._send_platform_login_text_notification(
        platform_name="wechat",
        open_url="https://channels.weixin.qq.com/platform/post/create",
        telegram_bot_token="token",
        telegram_chat_id="chat",
    )

    assert result["ok"] is True
    assert result["sent"] is False
    assert result["skipped"] is True
