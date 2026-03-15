from cybercar.settings import get_paths, load_app_config


def test_app_config_has_local_runtime_paths() -> None:
    cfg = load_app_config()
    assert cfg["paths"]["runtime_root"] == "runtime"


def test_paths_point_inside_repo() -> None:
    paths = get_paths()
    assert str(paths.runtime_root).endswith("runtime")
    assert str(paths.default_profile_dir).endswith("profiles\\default") or str(paths.default_profile_dir).endswith("profiles/default")
