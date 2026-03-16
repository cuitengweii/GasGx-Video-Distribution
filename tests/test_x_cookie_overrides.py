from __future__ import annotations

import json
from pathlib import Path

from cybercar import engine


def _write_cookie_file(path: Path, *, active_account: str = "backup") -> None:
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "active_account": active_account,
                "replace_profile_cookies": True,
                "domain": ".x.com",
                "path": "/",
                "secure": True,
                "accounts": {
                    "primary": {
                        "cookies": {
                            "auth_token": "primary-auth-token",
                            "ct0": "primary-ct0-token",
                        }
                    },
                    "backup": {
                        "cookies": {
                            "auth_token": "backup-auth-token",
                            "ct0": "backup-ct0-token",
                            "twid": "u=12345",
                        }
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_build_x_cookie_context_uses_manual_cookie_file(tmp_path: Path, monkeypatch) -> None:
    cookie_file = tmp_path / "x_cookies.json"
    _write_cookie_file(cookie_file)
    monkeypatch.setattr(engine, "_log", lambda message: None)

    ctx = engine._build_x_cookie_context_with_overrides(
        str(tmp_path / "missing_profile"),
        x_cookie_file=str(cookie_file),
    )

    assert ctx is not None
    session, ct0 = ctx
    assert ct0 == "backup-ct0-token"
    assert session.cookies.get("auth_token") == "backup-auth-token"
    assert session.cookies.get("twid") == "u=12345"


def test_export_x_cookies_for_ytdlp_uses_active_manual_account(tmp_path: Path, monkeypatch) -> None:
    cookie_file = tmp_path / "x_cookies.json"
    _write_cookie_file(cookie_file, active_account="primary")
    monkeypatch.setattr(engine, "_log", lambda message: None)

    cookie_path, status = engine._export_x_cookies_for_ytdlp(
        str(tmp_path / "missing_profile"),
        x_cookie_file=str(cookie_file),
    )

    assert status == "exported"
    assert cookie_path is not None
    content = cookie_path.read_text(encoding="utf-8")
    assert "auth_token\tprimary-auth-token" in content
    assert "ct0\tprimary-ct0-token" in content
    cookie_path.unlink(missing_ok=True)
