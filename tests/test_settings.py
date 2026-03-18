import os

from cybercar.settings import apply_runtime_environment, get_paths, load_app_config


def test_app_config_has_local_runtime_paths() -> None:
    cfg = load_app_config()
    assert cfg["paths"]["runtime_root"] == "runtime"
    assert cfg["network"]["proxy"] == "http://127.0.0.1:33210"
    assert cfg["cleanup"]["targets"]["processed_videos"]["retention_days"] == 14
    assert cfg["publish"]["platforms"]["wechat"]["upload_timeout"] == 30


def test_paths_point_inside_repo() -> None:
    paths = get_paths()
    assert str(paths.runtime_root).endswith("runtime")
    assert str(paths.default_profile_dir).endswith("profiles\\default") or str(paths.default_profile_dir).endswith("profiles/default")
    assert str(paths.x_profile_dir).endswith("profiles\\x_collect") or str(paths.x_profile_dir).endswith("profiles/x_collect")
    assert str(paths.x_cookie_file_path).endswith("config\\x_cookies.local.json") or str(paths.x_cookie_file_path).endswith("config/x_cookies.local.json")


def test_apply_runtime_environment_sets_default_proxy_env(monkeypatch) -> None:
    monkeypatch.delenv("CYBERCAR_PROXY", raising=False)
    monkeypatch.delenv("CYBERCAR_USE_SYSTEM_PROXY", raising=False)

    apply_runtime_environment()

    assert os.environ["CYBERCAR_PROXY"] == "http://127.0.0.1:33210"
    assert os.environ["CYBERCAR_USE_SYSTEM_PROXY"] == "0"
