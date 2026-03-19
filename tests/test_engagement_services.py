from __future__ import annotations

from pathlib import Path

from cybercar import engagement
from cybercar.services.engagement.common import PLATFORM_CAPABILITIES


def test_platform_capabilities_enable_douyin_and_kuaishou_engagement() -> None:
    assert PLATFORM_CAPABILITIES["douyin"]["engagement_supported"] is True
    assert PLATFORM_CAPABILITIES["douyin"]["implemented"] is True
    assert PLATFORM_CAPABILITIES["kuaishou"]["engagement_supported"] is True
    assert PLATFORM_CAPABILITIES["kuaishou"]["implemented"] is True


def test_run_douyin_engagement_delegates_to_platform_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(
        engagement,
        "load_app_config",
        lambda: {
            "chrome": {"default_debug_port": 9555},
            "notify": {"env_prefix": "TEST_NOTIFY_"},
            "comment_reply": {"enabled": True},
        },
    )
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")
    monkeypatch.setattr(
        engagement,
        "reply_douyin_focused_generated",
        lambda **_kwargs: {"ok": False, "reason": "focused_reply_editor_not_ready"},
    )

    def fake_run_platform_comment_reply(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "platform": kwargs["platform_name"]}

    monkeypatch.setattr(engagement, "run_platform_comment_reply", fake_run_platform_comment_reply)

    result = engagement.run_douyin_engagement(max_posts=3, max_replies=2, like_only=True, latest_only=True, debug=True)

    assert result == {"ok": True, "platform": "douyin"}
    assert captured["platform_name"] == "douyin"
    assert captured["workspace"] == f"workspace:{FakePaths.runtime_root}"
    assert captured["debug_port"] == 9555
    assert captured["chrome_user_data_dir"] == str(FakePaths.default_profile_dir)
    assert captured["max_posts_override"] == 3
    assert captured["max_replies_override"] == 2
    assert captured["latest_only"] is True
    assert captured["debug"] is True
    assert captured["notify_env_prefix"] == "TEST_NOTIFY_"


def test_run_douyin_engagement_prefers_focused_result_when_editor_ready(monkeypatch, tmp_path: Path) -> None:
    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(
        engagement,
        "load_app_config",
        lambda: {
            "chrome": {"default_debug_port": 9555},
            "notify": {"env_prefix": "TEST_NOTIFY_"},
            "comment_reply": {"enabled": True},
        },
    )
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")
    monkeypatch.setattr(
        engagement,
        "reply_douyin_focused_generated",
        lambda **_kwargs: {"ok": True, "platform": "douyin", "replies_sent": 1},
    )
    monkeypatch.setattr(
        engagement,
        "run_platform_comment_reply",
        lambda **_kwargs: {"ok": False, "platform": "douyin"},
    )

    result = engagement.run_douyin_engagement(max_posts=1, max_replies=1, debug=True)

    assert result == {"ok": True, "platform": "douyin", "replies_sent": 1}


def test_run_kuaishou_engagement_delegates_to_platform_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(
        engagement,
        "load_app_config",
        lambda: {
            "chrome": {"default_debug_port": 9444},
            "notify": {"env_prefix": "KS_NOTIFY_"},
            "comment_reply": {"enabled": True},
        },
    )
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")

    def fake_run_platform_comment_reply(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "platform": kwargs["platform_name"]}

    monkeypatch.setattr(engagement, "run_platform_comment_reply", fake_run_platform_comment_reply)

    result = engagement.run_kuaishou_engagement(max_posts=4, max_replies=1, like_only=False, latest_only=False, debug=False)

    assert result == {"ok": True, "platform": "kuaishou"}
    assert captured["platform_name"] == "kuaishou"
    assert captured["debug_port"] == 9444
    assert captured["chrome_user_data_dir"] == str(FakePaths.default_profile_dir)
    assert captured["max_posts_override"] == 4
    assert captured["max_replies_override"] == 1
    assert captured["notify_env_prefix"] == "KS_NOTIFY_"


def test_run_douyin_focused_engagement_delegates_to_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(
        engagement,
        "load_app_config",
        lambda: {
            "chrome": {"default_debug_port": 9777},
            "notify": {"env_prefix": "DY_NOTIFY_"},
            "comment_reply": {"enabled": True},
        },
    )
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")

    def fake_reply_douyin_focused_editor(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "platform": "douyin"}

    monkeypatch.setattr(engagement, "reply_douyin_focused_editor", fake_reply_douyin_focused_editor)

    result = engagement.run_douyin_focused_engagement(reply_text="hello", debug=True, ignore_state=True)

    assert result == {"ok": True, "platform": "douyin"}
    assert captured["workspace"] == f"workspace:{FakePaths.runtime_root}"
    assert captured["debug_port"] == 9777
    assert captured["chrome_user_data_dir"] == str(FakePaths.default_profile_dir)
    assert captured["reply_text"] == "hello"
    assert captured["debug"] is True
    assert captured["ignore_state"] is True
    assert captured["notify_env_prefix"] == "DY_NOTIFY_"


def test_run_kuaishou_focused_engagement_delegates_to_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(
        engagement,
        "load_app_config",
        lambda: {
            "chrome": {"default_debug_port": 9888},
            "notify": {"env_prefix": "KS_NOTIFY_"},
            "comment_reply": {"enabled": True},
        },
    )
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")

    def fake_reply_kuaishou_focused_editor(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "platform": "kuaishou"}

    monkeypatch.setattr(engagement, "reply_kuaishou_focused_editor", fake_reply_kuaishou_focused_editor)

    result = engagement.run_kuaishou_focused_engagement(reply_text="hello", debug=True, ignore_state=True)

    assert result == {"ok": True, "platform": "kuaishou"}
    assert captured["workspace"] == f"workspace:{FakePaths.runtime_root}"
    assert captured["debug_port"] == 9888
    assert captured["chrome_user_data_dir"] == str(FakePaths.default_profile_dir)
    assert captured["reply_text"] == "hello"
    assert captured["debug"] is True
    assert captured["ignore_state"] is True
    assert captured["notify_env_prefix"] == "KS_NOTIFY_"


def test_run_douyin_focused_generated_engagement_delegates_to_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(
        engagement,
        "load_app_config",
        lambda: {
            "chrome": {"default_debug_port": 9777},
            "notify": {"env_prefix": "DY_NOTIFY_"},
            "comment_reply": {"enabled": True},
        },
    )
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")

    def fake_reply_douyin_focused_generated(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "platform": "douyin", "replies_sent": 1}

    monkeypatch.setattr(engagement, "reply_douyin_focused_generated", fake_reply_douyin_focused_generated)

    result = engagement.run_douyin_focused_generated_engagement(debug=True, ignore_state=True)

    assert result == {"ok": True, "platform": "douyin", "replies_sent": 1}
    assert captured["workspace"] == f"workspace:{FakePaths.runtime_root}"
    assert captured["debug_port"] == 9777
    assert captured["chrome_user_data_dir"] == str(FakePaths.default_profile_dir)
    assert captured["debug"] is True
    assert captured["ignore_state"] is True
    assert captured["notify_env_prefix"] == "DY_NOTIFY_"


def test_run_kuaishou_focused_generated_engagement_delegates_to_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(
        engagement,
        "load_app_config",
        lambda: {
            "chrome": {"default_debug_port": 9888},
            "notify": {"env_prefix": "KS_NOTIFY_"},
            "comment_reply": {"enabled": True},
        },
    )
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")

    def fake_reply_kuaishou_focused_generated(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "platform": "kuaishou", "replies_sent": 1}

    monkeypatch.setattr(engagement, "reply_kuaishou_focused_generated", fake_reply_kuaishou_focused_generated)

    result = engagement.run_kuaishou_focused_generated_engagement(debug=True, ignore_state=True)

    assert result == {"ok": True, "platform": "kuaishou", "replies_sent": 1}
    assert captured["workspace"] == f"workspace:{FakePaths.runtime_root}"
    assert captured["debug_port"] == 9888
    assert captured["chrome_user_data_dir"] == str(FakePaths.default_profile_dir)
    assert captured["debug"] is True
    assert captured["ignore_state"] is True
    assert captured["notify_env_prefix"] == "KS_NOTIFY_"


def test_diagnose_platform_engagement_delegates_to_runtime(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        default_profile_dir = tmp_path / "default"

    monkeypatch.setattr(engagement, "apply_runtime_environment", lambda: FakePaths())
    monkeypatch.setattr(engagement, "load_app_config", lambda: {"chrome": {"default_debug_port": 9666}})
    monkeypatch.setattr(engagement.engine, "init_workspace", lambda root: f"workspace:{root}")

    def fake_diagnose_platform_comment_page(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "platform": kwargs["platform_name"]}

    monkeypatch.setattr(engagement, "diagnose_platform_comment_page", fake_diagnose_platform_comment_page)

    result = engagement.diagnose_platform_engagement("kuaishou")

    assert result == {"ok": True, "platform": "kuaishou"}
    assert captured["platform_name"] == "kuaishou"
    assert captured["workspace"] == f"workspace:{FakePaths.runtime_root}"
    assert captured["debug_port"] == 9666
    assert captured["chrome_user_data_dir"] == str(FakePaths.default_profile_dir)
