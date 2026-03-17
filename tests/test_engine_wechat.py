from __future__ import annotations

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
    assert result["notification_mode"] == "qr"
    assert result["url"] == "https://channels.weixin.qq.com/login.html"
    assert len(qr_calls) == 1
    assert not text_calls
    assert qr_calls[0]["page"] is not None
    assert qr_calls[0]["auto_open_chrome"] is False


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
    assert result["notification_mode"] == "text"
    assert result["qr_result"]["error"] == "wechat login qr not found"
    assert len(text_calls) == 1
    assert text_calls[0]["qr_error"] == "wechat login qr not found"


def test_merge_comment_reply_config_uses_short_random_waits() -> None:
    cfg = engine._merge_comment_reply_config({})

    assert cfg["min_reply_interval_seconds"] == 1
    assert cfg["max_reply_interval_seconds"] == 5
    assert cfg["min_like_to_reply_interval_seconds"] == 1
    assert cfg["max_like_to_reply_interval_seconds"] == 5


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
