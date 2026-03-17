from pathlib import Path

import requests

from cybercar.common import telegram_api
from cybercar.common import telegram_ui
from cybercar.common.telegram_bot_registry import load_registry

from cybercar.cli import build_parser
from cybercar.common.telegram_bot_registry import DEFAULT_REGISTRY_FILE
from cybercar.telegram.bootstrap import build_worker_argv


def test_cli_parses_telegram_worker_command() -> None:
    parser = build_parser()
    args = parser.parse_args(["telegram", "worker"])
    assert args.command == "telegram"
    assert args.telegram_command == "worker"


def test_worker_argv_defaults_to_local_repo_and_runtime() -> None:
    argv = build_worker_argv([])
    assert "--repo-root" in argv
    assert "--workspace" in argv
    assert "--default-profile" in argv
    assert "--telegram-registry-file" not in argv
    assert "--telegram-bot-token" in argv
    assert "--telegram-chat-id" in argv


def test_telegram_scripts_exist() -> None:
    root = Path(__file__).resolve().parents[1]
    required = [
        root / "scripts" / "telegram_worker.ps1",
        root / "scripts" / "telegram_set_commands.ps1",
        root / "scripts" / "telegram_recover.ps1",
        root / "scripts" / "telegram_unified_runner.ps1",
    ]
    missing = [str(path) for path in required if not path.exists()]
    assert not missing, missing


def test_telegram_registry_defaults_to_runtime_secrets() -> None:
    assert "runtime" in str(DEFAULT_REGISTRY_FILE)
    assert "secrets" in str(DEFAULT_REGISTRY_FILE)


def test_registry_does_not_auto_promote_first_bot_to_manager(tmp_path: Path) -> None:
    registry_file = tmp_path / "telegram_bot_registry.json"
    registry_file.write_text(
        """
{
  "version": 1,
  "bots": [
    {
      "bot_name": "CyberCar",
      "bot_username": "cybercar_cui_bot",
      "bot_token": "123456:token",
      "chat_id": "6067625538",
      "keywords": ["CyberCar", "cybercar"],
      "enabled": true
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )
    registry = load_registry(registry_file)
    bot = registry["bots"][0]
    assert bot["bot_username"] == "cybercar_cui_bot"
    assert bot["enabled"] is True


def test_call_telegram_api_resets_session_after_connection_error(monkeypatch) -> None:
    class FakeResponse:
        status_code = 200
        text = '{"ok": true, "result": {"message_id": 1}}'

        def json(self) -> dict[str, object]:
            return {"ok": True, "result": {"message_id": 1}}

    class FailingSession:
        def __init__(self) -> None:
            self.closed = False

        def mount(self, prefix: str, adapter: object) -> None:
            return None

        def post(self, endpoint: str, data: dict[str, object], timeout: int) -> FakeResponse:
            raise requests.exceptions.ConnectionError("Connection aborted.")

        def close(self) -> None:
            self.closed = True

    class SuccessSession:
        def __init__(self) -> None:
            self.closed = False
            self.calls: list[tuple[str, dict[str, object], int]] = []

        def mount(self, prefix: str, adapter: object) -> None:
            return None

        def post(self, endpoint: str, data: dict[str, object], timeout: int) -> FakeResponse:
            self.calls.append((endpoint, data, timeout))
            return FakeResponse()

        def close(self) -> None:
            self.closed = True

    failing_session = FailingSession()
    success_session = SuccessSession()
    created_sessions: list[object] = [failing_session, success_session]

    def session_factory() -> object:
        return created_sessions.pop(0)

    monkeypatch.setattr(telegram_api, "_SESSIONS", {})
    monkeypatch.setattr(telegram_api.requests, "Session", session_factory)
    monkeypatch.setattr(telegram_api.time, "sleep", lambda seconds: None)

    response = telegram_api.call_telegram_api(
        bot_token="123456:token",
        method="sendMessage",
        params={"chat_id": "1", "text": "hello"},
        timeout_seconds=8,
        use_post=True,
        max_retries=1,
    )

    assert response["ok"] is True
    assert failing_session.closed is True
    assert success_session.calls


def test_call_telegram_api_keeps_get_and_post_sessions_isolated(monkeypatch) -> None:
    class FakeResponse:
        status_code = 200
        text = '{"ok": true, "result": []}'

        def json(self) -> dict[str, object]:
            return {"ok": True, "result": []}

    class FailingGetSession:
        def __init__(self) -> None:
            self.closed = False

        def mount(self, prefix: str, adapter: object) -> None:
            return None

        def get(self, endpoint: str, params: dict[str, object], timeout: int) -> FakeResponse:
            raise requests.exceptions.ConnectionError("ConnectionResetError(10054)")

        def close(self) -> None:
            self.closed = True

    class PostSession:
        def __init__(self) -> None:
            self.closed = False
            self.calls: list[tuple[str, dict[str, object], int]] = []

        def mount(self, prefix: str, adapter: object) -> None:
            return None

        def post(self, endpoint: str, data: dict[str, object], timeout: int) -> FakeResponse:
            self.calls.append((endpoint, data, timeout))
            return FakeResponse()

        def close(self) -> None:
            self.closed = True

    get_session = FailingGetSession()
    post_session = PostSession()
    created_sessions: list[object] = [get_session, post_session]

    def session_factory() -> object:
        return created_sessions.pop(0)

    monkeypatch.setattr(telegram_api, "_SESSIONS", {})
    monkeypatch.setattr(telegram_api.requests, "Session", session_factory)
    monkeypatch.setattr(telegram_api.time, "sleep", lambda seconds: None)

    try:
        telegram_api.call_telegram_api(
            bot_token="123456:token",
            method="getUpdates",
            params={"timeout": 1},
            timeout_seconds=5,
            use_post=False,
            max_retries=0,
        )
    except requests.exceptions.ConnectionError:
        pass
    else:
        raise AssertionError("expected getUpdates transport failure")

    response = telegram_api.call_telegram_api(
        bot_token="123456:token",
        method="sendMessage",
        params={"chat_id": "1", "text": "hello"},
        timeout_seconds=8,
        use_post=True,
        max_retries=0,
    )

    assert response["ok"] is True
    assert get_session.closed is False
    assert post_session.closed is False
    assert post_session.calls


def test_build_telegram_card_uses_clean_section_layout() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "subtitle": "3个平台已完成",
            "bot_name": "CyberCar",
            "sections": [
                {"title": "执行摘要", "emoji": "📋", "items": [{"label": "结果", "value": "全部完成"}]},
                {"title": "任务日志", "emoji": "🧾", "items": ["日志文件：publish.log"]},
            ],
        },
    )

    text = str(card["text"])
    assert "<b>✅ 发布完成</b>" in text
    assert "<i>· 3个平台已完成</i>" in text
    assert "• <b>结果</b>：全部完成" in text
    assert "\n\n<b>🧾 任务日志</b>" in text


def test_build_callback_toast_returns_clean_chinese_copy() -> None:
    assert telegram_ui.build_callback_toast("process_status", "success") == "进度已刷新"
    assert telegram_ui.build_callback_toast("refresh_qr", "failed") == "二维码刷新失败"


def test_build_telegram_card_prioritizes_failure_reason_before_machine_sections() -> None:
    card = telegram_ui.build_telegram_card(
        "alert",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {"title": "运行上下文", "emoji": "🧭", "items": [{"label": "工作区", "value": "D:/code/CyberCar"}]},
                {"title": "失败原因", "emoji": "⚠️", "items": [{"label": "原因", "value": "平台未登录"}]},
                {"title": "处理建议", "emoji": "🔧", "items": ["请先登录后重试。"]},
            ],
        },
    )

    text = str(card["text"])
    assert text.index("<b>⚠️ 失败原因</b>") < text.index("<b>🧭 运行上下文</b>")
    assert text.index("<b>🔧 处理建议</b>") < text.index("<b>🧭 运行上下文</b>")
    assert "• <b>🔐 原因</b>：登录失效" in text
    assert "• 🛠️ 去登录" in text


def test_build_telegram_card_marks_notify_and_network_failures_with_distinct_icons() -> None:
    card = telegram_ui.build_telegram_card(
        "alert",
        {
            "status": "failed",
            "title": "发送失败",
            "sections": [
                {"title": "失败原因", "emoji": "⚠️", "items": [{"label": "原因", "value": "Telegram timeout while sending card"}]},
                {"title": "结果说明", "emoji": "⚠️", "items": [{"label": "说明", "value": "chat_id missing for notify bot"}]},
            ],
        },
    )

    text = str(card["text"])
    assert text.startswith("<b>🌐 发送失败</b>")
    assert "• <b>🌐 原因</b>：网络超时" in text
    assert "• <b>📨 说明</b>：chat_id missing fo..." in text


def test_build_telegram_card_uses_login_icon_in_failure_header() -> None:
    card = telegram_ui.build_telegram_card(
        "alert",
        {
            "status": "failed",
            "title": "发布失败",
            "sections": [
                {"title": "失败原因", "emoji": "⚠️", "items": [{"label": "原因", "value": "平台未登录"}]},
            ],
        },
    )

    assert str(card["text"]).startswith("<b>🔐 发布失败</b>")


def test_build_telegram_card_compacts_success_focus_to_three_primary_items() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "人工关注",
                    "emoji": "🎯",
                    "items": [
                        {"label": "执行结果", "value": "成功"},
                        {"label": "目标平台", "value": "抖音 / 小红书"},
                        {"label": "候选数量", "value": "5"},
                        {"label": "时间窗口", "value": "30 分钟"},
                    ],
                },
                {"title": "执行结果", "emoji": "📝", "items": ["已完成全部平台发布。"]},
            ],
        },
    )

    text = str(card["text"])
    assert text.count("• <b>") == 4
    assert text.index("• <b>执行结果</b>：成功") < text.index("<b>🤖 机器信息</b>")
    assert "<b>📝 执行结果</b>" not in text
    assert "• <b>时间窗口</b>：30 分钟" in text


def test_build_telegram_card_marks_partial_success_in_header() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "人工关注",
                    "emoji": "🎯",
                    "items": [
                        {"label": "执行结果", "value": "部分完成"},
                        {"label": "平台摘要", "value": "✅ 小红书成功 / 📣 抖音失败"},
                    ],
                }
            ],
        },
    )

    assert str(card["text"]).startswith("<b>🟡 发布完成（部分）</b>")


def test_build_telegram_card_marks_skipped_success_in_header() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "人工关注",
                    "emoji": "🎯",
                    "items": [
                        {"label": "执行结果", "value": "跳过"},
                    ],
                }
            ],
        },
    )

    assert str(card["text"]).startswith("<b>⏭️ 发布完成（跳过）</b>")


def test_build_telegram_card_compacts_config_subtitle() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "subtitle": "当前配置：cybertruck-profile-long-name",
            "sections": [{"title": "人工关注", "emoji": "🎯", "items": [{"label": "执行结果", "value": "成功"}]}],
        },
    )

    assert "<i>· 配置：cybertruck-profile-lon...</i>" in str(card["text"])


def test_build_telegram_card_prefers_platform_summary_for_subtitle() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "subtitle": "当前配置：cybertruck",
            "sections": [
                {
                    "title": "人工关注",
                    "emoji": "🎯",
                    "items": [
                        {"label": "执行结果", "value": "部分完成"},
                        {"label": "平台摘要", "value": "✅ 小红书成功 / 📣 抖音失败 / 🔐 视频号登录"},
                    ],
                }
            ],
        },
    )

    text = str(card["text"])
    assert "<i>· 1个平台成功 / 1个平台失败</i>" in text


def test_build_telegram_card_suppresses_long_success_tail_when_focus_exists() -> None:
    card = telegram_ui.build_telegram_card(
        "publish_result",
        {
            "status": "success",
            "title": "发布完成",
            "sections": [
                {
                    "title": "人工关注",
                    "emoji": "🎯",
                    "items": [
                        {"label": "执行结果", "value": "成功"},
                        {"label": "目标平台", "value": "抖音"},
                        {"label": "候选数量", "value": "3"},
                    ],
                },
                {"title": "执行结果", "emoji": "📝", "items": ["这里是一大段成功说明，列表里不需要重复出现。"]},
                {
                    "title": "机器信息",
                    "emoji": "🤖",
                    "items": [
                        {"label": "任务", "value": "publish|douyin"},
                        {"label": "日志", "value": "job.log"},
                        {"label": "耗时", "value": "12.3s"},
                    ],
                },
            ],
        },
    )

    text = str(card["text"])
    assert "<b>📝 执行结果</b>" not in text
    assert "• <b>任务</b>：publish|douyin" in text
    assert "• <b>日志</b>：job.log" in text
    assert "• <b>耗时</b>：12.3s" not in text
