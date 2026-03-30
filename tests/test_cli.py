from types import SimpleNamespace

from cybercar import cli
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


def test_cli_login_subcommand_accepts_tiktok_platform() -> None:
    parser = build_parser()
    args = parser.parse_args(["login", "open", "--platform", "tiktok"])
    assert args.command == "login"
    assert args.login_command == "open"
    assert args.platform == "tiktok"


def test_cli_login_subcommand_accepts_x_platform() -> None:
    parser = build_parser()
    args = parser.parse_args(["login", "open", "--platform", "x"])
    assert args.command == "login"
    assert args.login_command == "open"
    assert args.platform == "x"


def test_cli_cleanup_subcommand_parses_apply_and_print_files() -> None:
    parser = build_parser()
    args = parser.parse_args(["cleanup", "--apply", "--print-files"])
    assert args.command == "cleanup"
    assert args.apply is True
    assert args.print_files is True


def test_print_json_falls_back_when_console_cannot_encode(monkeypatch) -> None:
    class _DummyBuffer:
        def __init__(self) -> None:
            self.chunks: list[bytes] = []

        def write(self, data: bytes) -> int:
            self.chunks.append(bytes(data))
            return len(data)

    dummy_buffer = _DummyBuffer()
    monkeypatch.setattr(cli.sys, "stdout", SimpleNamespace(encoding="gbk", buffer=dummy_buffer))

    def _broken_print(_text: str) -> None:
        raise UnicodeEncodeError("gbk", "ə", 0, 1, "illegal multibyte sequence")

    monkeypatch.setattr("builtins.print", _broken_print)

    cli._print_json({"message": "ə"})

    merged = b"".join(dummy_buffer.chunks).decode("gbk", errors="replace")
    assert '"message": "?"' in merged


def test_cli_engage_douyin_subcommand_parses_options() -> None:
    parser = build_parser()
    args = parser.parse_args(["engage", "douyin", "--max-posts", "5", "--latest-only"])
    assert args.command == "engage"
    assert args.engage_command == "douyin"
    assert args.max_posts == 5
    assert args.latest_only is True


def test_cli_engage_kuaishou_subcommand_parses_options() -> None:
    parser = build_parser()
    args = parser.parse_args(["engage", "kuaishou", "--max-replies", "2", "--like-only"])
    assert args.command == "engage"
    assert args.engage_command == "kuaishou"
    assert args.max_replies == 2
    assert args.like_only is True


def test_cli_engage_diagnose_subcommand_parses_platform() -> None:
    parser = build_parser()
    args = parser.parse_args(["engage", "diagnose", "--platform", "douyin"])
    assert args.command == "engage"
    assert args.engage_command == "diagnose"
    assert args.platform == "douyin"


def test_cli_engage_douyin_focused_subcommand_parses_reply_text() -> None:
    parser = build_parser()
    args = parser.parse_args(["engage", "douyin-focused", "--reply-text", "hello", "--debug", "--ignore-state"])
    assert args.command == "engage"
    assert args.engage_command == "douyin-focused"
    assert args.reply_text == "hello"
    assert args.debug is True
    assert args.ignore_state is True


def test_cli_engage_kuaishou_focused_subcommand_parses_reply_text() -> None:
    parser = build_parser()
    args = parser.parse_args(["engage", "kuaishou-focused", "--reply-text", "hello", "--debug", "--ignore-state"])
    assert args.command == "engage"
    assert args.engage_command == "kuaishou-focused"
    assert args.reply_text == "hello"
    assert args.debug is True
    assert args.ignore_state is True


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
