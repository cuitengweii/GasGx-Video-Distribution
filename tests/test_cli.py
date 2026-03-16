from cybercar.cli import build_parser
from cybercar.telegram.bootstrap import recover_bot_surface


def test_cli_includes_core_commands() -> None:
    parser = build_parser()
    args = parser.parse_args(["immediate", "--profile", "cybertruck", "--limit", "1"])
    assert args.command == "immediate"
    assert args.profile == "cybertruck"
    assert args.limit == 1


def test_cli_login_subcommand_parses_platform() -> None:
    parser = build_parser()
    args = parser.parse_args(["login", "status", "--platform", "wechat"])
    assert args.command == "login"
    assert args.login_command == "status"
    assert args.platform == "wechat"


def test_cli_telegram_recover_subcommand_parses_retries() -> None:
    parser = build_parser()
    args = parser.parse_args(["telegram", "recover", "--retries", "5"])
    assert args.command == "telegram"
    assert args.telegram_command == "recover"
    assert args.retries == 5


def test_recover_bot_surface_retries_until_refresh_succeeds(monkeypatch, tmp_path) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "cybercar.telegram.bootstrap._resolve_target",
        lambda: {
            "bot_token": "123456:token",
            "chat_id": "chat-1",
            "default_profile": "cybertruck",
            "telegram_config": {"poll_timeout_seconds": 10},
        },
    )

    class FakePaths:
        runtime_root = tmp_path / "runtime"
        log_dir = tmp_path / "logs"

    monkeypatch.setattr("cybercar.telegram.bootstrap.get_paths", lambda: FakePaths())
    monkeypatch.setattr(
        "cybercar.telegram.bootstrap._set_clickable_commands",
        lambda **kwargs: calls.append("set"),
    )

    def fake_refresh_home_surface(**kwargs):
        calls.append("home")
        if calls.count("home") < 2:
            raise RuntimeError("transient telegram failure")
        return None

    monkeypatch.setattr("cybercar.telegram.bootstrap._refresh_home_surface", fake_refresh_home_surface)
    monkeypatch.setattr("cybercar.telegram.bootstrap.time.sleep", lambda seconds: None)

    result = recover_bot_surface(retries=3)

    assert result["ok"] is True
    assert result["attempts"] == 2
    assert calls.count("set") == 2
    assert calls.count("home") == 2
