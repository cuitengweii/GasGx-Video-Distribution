from pathlib import Path

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


def test_telegram_scripts_exist() -> None:
    root = Path(__file__).resolve().parents[1]
    required = [
        root / "scripts" / "telegram_worker.ps1",
        root / "scripts" / "telegram_set_commands.ps1",
        root / "scripts" / "register_telegram_worker_task.ps1",
        root / "scripts" / "unregister_telegram_worker_task.ps1",
        root / "scripts" / "telegram_unified_runner.ps1",
    ]
    missing = [str(path) for path in required if not path.exists()]
    assert not missing, missing


def test_telegram_registry_defaults_to_runtime_secrets() -> None:
    assert "runtime" in str(DEFAULT_REGISTRY_FILE)
    assert "secrets" in str(DEFAULT_REGISTRY_FILE)
