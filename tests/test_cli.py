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


def test_cli_telegram_supervise_subcommand_parses_once_and_thresholds() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "telegram",
            "supervise",
            "--once",
            "--stale-heartbeat-seconds",
            "180",
            "--startup-grace-seconds",
            "120",
        ]
    )
    assert args.command == "telegram"
    assert args.telegram_command == "supervise"
    assert args.once is True
    assert args.stale_heartbeat_seconds == 180
    assert args.startup_grace_seconds == 120


def test_recover_bot_surface_retries_until_refresh_succeeds(monkeypatch, tmp_path) -> None:
    calls: list[str] = []
    restart_calls: list[dict[str, object]] = []

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
        repo_root = tmp_path
        runtime_root = tmp_path / "runtime"
        log_dir = tmp_path / "logs"

    monkeypatch.setattr("cybercar.telegram.bootstrap.get_paths", lambda: FakePaths())
    monkeypatch.setattr(
        "cybercar.telegram.bootstrap._restart_worker_process",
        lambda **kwargs: restart_calls.append(dict(kwargs))
        or {
            "pid": 4321,
            "terminated_pids": [1234],
            "stdout_log": str(tmp_path / "logs" / "telegram_worker_latest.out.log"),
            "stderr_log": str(tmp_path / "logs" / "telegram_worker_latest.err.log"),
        },
    )
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
    assert result["worker_pid"] == 4321
    assert result["terminated_pids"] == [1234]
    assert calls.count("set") == 2
    assert calls.count("home") == 2
    assert len(restart_calls) == 1
    assert restart_calls[0]["repo_root"] == tmp_path
    assert restart_calls[0]["runtime_root"] == tmp_path / "runtime"
