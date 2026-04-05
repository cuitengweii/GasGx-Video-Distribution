from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from Collection.cybercar.cybercar_video_capture_and_publishing_module import telegram_command_worker as worker_impl


def test_resolve_platform_login_runtime_context_prefers_wechat_specific_env(monkeypatch) -> None:
    monkeypatch.setenv("CYBERCAR_WECHAT_CHROME_DEBUG_PORT", "9334")
    monkeypatch.setenv("CYBERCAR_WECHAT_CHROME_USER_DATA_DIR", r"D:\profiles\wechat")
    monkeypatch.setenv("CYBERCAR_CHROME_DEBUG_PORT", "9333")
    monkeypatch.setenv("CYBERCAR_CHROME_USER_DATA_DIR", r"D:\profiles\default")

    core = SimpleNamespace(
        DEFAULT_PORT=9333,
        DEFAULT_WECHAT_DEBUG_PORT=9333,
        DEFAULT_WECHAT_CHROME_USER_DATA_DIR=r"D:\profiles\fallback_wechat",
        PLATFORM_CREATE_POST_URLS={"wechat": "https://channels.weixin.qq.com/platform/post/create"},
        PLATFORM_LOGIN_ENTRY_URLS={"wechat": "https://channels.weixin.qq.com/login.html"},
    )

    ctx = worker_impl._resolve_platform_login_runtime_context(core, "wechat")

    assert ctx["platform"] == "wechat"
    assert int(ctx["debug_port"]) == 9334
    assert str(ctx["chrome_user_data_dir"]) == r"D:\profiles\wechat"
    assert str(ctx["open_url"]) == "https://channels.weixin.qq.com/platform/post/create"


def test_run_comment_reply_job_uses_resolved_wechat_runtime_context(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    (workspace / "runtime" / "logs").mkdir(parents=True)

    calls: list[dict[str, object]] = []

    class FakeCore:
        DEFAULT_WECHAT_DEBUG_PORT = 9333
        DEFAULT_WECHAT_CHROME_USER_DATA_DIR = r"D:\profiles\default"
        DEFAULT_NOTIFY_ENV_PREFIX = "CYBERCAR_NOTIFY_"

        def init_workspace(self, _workspace: str):
            return SimpleNamespace(root=Path(_workspace))

        def _load_runtime_config(self, _path: str) -> dict[str, object]:
            return {}

        def run_wechat_comment_reply(self, **kwargs):
            calls.append(dict(kwargs))
            return {
                "ok": True,
                "platform": "wechat",
                "records": [],
                "posts_scanned": 0,
                "posts_selected": 0,
                "replies_sent": 0,
            }

    monkeypatch.setattr(worker_impl, "_load_runtime_modules", lambda: (SimpleNamespace(), FakeCore()))
    monkeypatch.setattr(worker_impl, "_load_engagement_module", lambda: SimpleNamespace())
    monkeypatch.setattr(
        worker_impl,
        "_resolve_platform_login_runtime_context",
        lambda _core, platform_name, **_kwargs: {
            "platform": str(platform_name),
            "debug_port": 9334,
            "chrome_user_data_dir": r"D:\profiles\wechat",
            "open_url": "https://channels.weixin.qq.com/platform/post/create",
        },
    )

    exit_code = worker_impl._run_comment_reply_job(
        repo_root=workspace,
        workspace=workspace,
        timeout_seconds=30,
        profile="cybertruck",
        telegram_bot_identifier="",
        telegram_bot_token="",
        telegram_chat_id="",
        platform="wechat",
        post_limit=1,
    )

    assert exit_code == 0
    assert len(calls) == 1
    assert int(calls[0]["debug_port"]) == 9334
    assert str(calls[0]["chrome_user_data_dir"]) == r"D:\profiles\wechat"


def test_resolve_platform_login_runtime_context_prefers_core_runtime_resolver() -> None:
    class FakeCore:
        PLATFORM_CREATE_POST_URLS = {"wechat": "https://channels.weixin.qq.com/platform/post/create"}
        PLATFORM_LOGIN_ENTRY_URLS = {"wechat": "https://channels.weixin.qq.com/login.html"}

        @staticmethod
        def resolve_platform_runtime_context(platform_name: str, *, prefer_login_entry: bool = False):
            return {
                "platform": str(platform_name),
                "debug_port": 9666,
                "chrome_user_data_dir": r"D:\profiles\wechat_core",
                "open_url": "https://channels.weixin.qq.com/login.html"
                if prefer_login_entry
                else "https://channels.weixin.qq.com/platform/post/create",
            }

    ctx = worker_impl._resolve_platform_login_runtime_context(FakeCore(), "wechat", prefer_login_entry=True)

    assert ctx["platform"] == "wechat"
    assert int(ctx["debug_port"]) == 9666
    assert str(ctx["chrome_user_data_dir"]) == r"D:\profiles\wechat_core"
    assert str(ctx["open_url"]) == "https://channels.weixin.qq.com/login.html"
