from __future__ import annotations

from cybercar import engine, session


def test_open_url_for_wechat_can_prefer_login_entry() -> None:
    assert session._open_url("wechat", prefer_login_entry=True) == str(engine.PLATFORM_LOGIN_ENTRY_URLS.get("wechat") or "")


def test_login_status_enables_keepalive_only_for_wechat(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(session, "_chrome_settings", lambda: (9333, 9334, 9335))
    monkeypatch.setattr(session, "_profile_dir", lambda _platform: "D:/profiles/target")
    monkeypatch.setattr(
        engine,
        "probe_platform_session_via_debug_port",
        lambda **kwargs: calls.append(dict(kwargs))
        or {
            "ok": True,
            "status": "ready",
            "platform": str(kwargs.get("platform_name") or ""),
        },
    )

    wechat = session.login_status("wechat")
    x_status = session.login_status("x")

    assert wechat["platform"] == "wechat"
    assert x_status["platform"] == "x"
    assert calls[0]["platform_name"] == "wechat"
    assert calls[0]["open_url"] == "https://channels.weixin.qq.com/login.html"
    assert calls[0]["disconnect_after_probe"] is False
    assert calls[0]["enable_wechat_keepalive"] is True
    assert calls[1]["platform_name"] == "x"
    assert calls[1]["disconnect_after_probe"] is True
    assert calls[1]["enable_wechat_keepalive"] is False
