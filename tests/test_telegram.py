from pathlib import Path

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
