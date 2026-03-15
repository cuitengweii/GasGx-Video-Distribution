from cybercar.cli import build_parser


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
