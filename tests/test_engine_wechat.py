from __future__ import annotations

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
                    "visible_options": ["赛博皮卡-天津港现车", "赛博皮卡预定"],
                }
            return True

    with pytest.raises(RuntimeError, match="可见选项=赛博皮卡-天津港现车,赛博皮卡预定"):
        engine._select_collection(FakeCtx(), "赛博皮卡精选")


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


def test_merge_comment_reply_config_uses_short_random_waits() -> None:
    cfg = engine._merge_comment_reply_config({})

    assert cfg["reply_min_chars"] == 5
    assert cfg["reply_max_chars"] == 20
    assert cfg["min_reply_interval_seconds"] == 1
    assert cfg["max_reply_interval_seconds"] == 5
    assert cfg["min_like_to_reply_interval_seconds"] == 1
    assert cfg["max_like_to_reply_interval_seconds"] == 5
    assert cfg["self_author_markers"] == ["cybercar"]


def test_apply_comment_reply_like_to_reply_wait_uses_random_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []

    monkeypatch.setattr(engine.random, "uniform", lambda a, b: 3.25)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: waits.append(float(seconds)))

    waited = engine._apply_comment_reply_like_to_reply_wait(
        {
            "min_like_to_reply_interval_seconds": 1,
            "max_like_to_reply_interval_seconds": 5,
        },
        debug=False,
    )

    assert waited == 3.25
    assert waits == [3.25]


def test_humanized_wechat_comment_pause_prefers_page_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    timeout_calls: list[int] = []
    sleep_calls: list[float] = []

    class FakePage:
        def wait_for_timeout(self, milliseconds: int) -> None:
            timeout_calls.append(int(milliseconds))

    monkeypatch.setattr(engine.random, "uniform", lambda a, b: 0.42)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: sleep_calls.append(float(seconds)))

    waited = engine._humanized_wechat_comment_reaction_pause(FakePage(), "wechat comment submit click")

    assert waited == 0.42
    assert timeout_calls == [420]
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
