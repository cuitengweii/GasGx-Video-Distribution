from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class AppPaths:
    repo_root: Path
    config_dir: Path
    app_config_path: Path
    profile_config_path: Path
    runtime_root: Path
    log_dir: Path
    profiles_root: Path
    default_profile_dir: Path
    wechat_profile_dir: Path
    x_profile_dir: Path
    x_cookie_file_path: Path

    def ensure(self) -> None:
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.profiles_root.mkdir(parents=True, exist_ok=True)
        self.default_profile_dir.mkdir(parents=True, exist_ok=True)
        self.wechat_profile_dir.mkdir(parents=True, exist_ok=True)
        self.x_profile_dir.mkdir(parents=True, exist_ok=True)
        self.x_cookie_file_path.parent.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8-sig")
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else {}


def _resolve_path(root: Path, raw: Any, default: Path) -> Path:
    token = str(raw or "").strip()
    if not token:
        return default
    path = Path(token)
    if not path.is_absolute():
        path = (root / path).resolve()
    return path


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    token = str(value or "").strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _load_global_env_defaults(repo_root: Path) -> None:
    env_file = Path(r"D:\code\.global.env")
    if not env_file.exists():
        return
    try:
        lines = env_file.read_text(encoding="utf-8-sig").splitlines()
    except Exception:
        return
    for line in lines:
        item = line.strip()
        if not item or item.startswith("#") or "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value.startswith("${") and value.endswith("}"):
            ref = value[2:-1].strip()
            if ref:
                value = os.environ.get(ref, value)
        os.environ.setdefault(key, value)


def _resolve_env_placeholder(value: str) -> str:
    token = value.strip()
    if token.startswith("${") and token.endswith("}"):
        ref = token[2:-1].strip()
        if ref:
            return str(os.environ.get(ref, "")).strip()
    return token


@lru_cache(maxsize=1)
def load_app_config() -> dict[str, Any]:
    return _load_json(_repo_root() / "config" / "app.json")


@lru_cache(maxsize=1)
def load_profile_config() -> dict[str, Any]:
    return _load_json(_repo_root() / "config" / "profiles.json")


@lru_cache(maxsize=1)
def get_paths() -> AppPaths:
    repo_root = _repo_root()
    config = load_app_config()
    path_cfg = config.get("paths") if isinstance(config.get("paths"), dict) else {}
    x_cfg = config.get("x") if isinstance(config.get("x"), dict) else {}
    config_dir = repo_root / "config"
    runtime_root = _resolve_path(repo_root, path_cfg.get("runtime_root"), repo_root / "runtime")
    profiles_root = _resolve_path(repo_root, path_cfg.get("profiles_root"), repo_root / "profiles")
    default_profile_dir = _resolve_path(profiles_root, path_cfg.get("default_profile_dir"), profiles_root / "default")
    wechat_profile_dir = _resolve_path(profiles_root, path_cfg.get("wechat_profile_dir"), profiles_root / "wechat")
    x_profile_dir = _resolve_path(profiles_root, path_cfg.get("x_profile_dir"), profiles_root / "x_collect")
    x_cookie_file_path = _resolve_path(repo_root, x_cfg.get("cookie_file"), config_dir / "x_cookies.local.json")
    return AppPaths(
        repo_root=repo_root,
        config_dir=config_dir,
        app_config_path=config_dir / "app.json",
        profile_config_path=config_dir / "profiles.json",
        runtime_root=runtime_root,
        log_dir=runtime_root / "logs",
        profiles_root=profiles_root,
        default_profile_dir=default_profile_dir,
        wechat_profile_dir=wechat_profile_dir,
        x_profile_dir=x_profile_dir,
        x_cookie_file_path=x_cookie_file_path,
    )


def apply_runtime_environment() -> AppPaths:
    paths = get_paths()
    paths.ensure()
    _load_global_env_defaults(paths.repo_root)
    config = load_app_config()
    os.environ.setdefault("CYBERCAR_CHROME_USER_DATA_DIR", str(paths.default_profile_dir))
    os.environ.setdefault("CYBERCAR_WECHAT_CHROME_USER_DATA_DIR", str(paths.wechat_profile_dir))
    os.environ.setdefault("CYBERCAR_X_CHROME_USER_DATA_DIR", str(paths.x_profile_dir))
    os.environ.setdefault("CYBERCAR_X_COOKIE_FILE", str(paths.x_cookie_file_path))
    chrome_cfg = config.get("chrome") if isinstance(config.get("chrome"), dict) else {}
    network_cfg = config.get("network") if isinstance(config.get("network"), dict) else {}
    default_port = str(chrome_cfg.get("default_debug_port") or "").strip()
    wechat_port = str(chrome_cfg.get("wechat_debug_port") or "").strip()
    x_port = str(chrome_cfg.get("x_debug_port") or "").strip()
    default_proxy = _resolve_env_placeholder(str(network_cfg.get("proxy") or ""))
    if default_proxy:
        os.environ.setdefault("CYBERCAR_PROXY", default_proxy)
    if "use_system_proxy" in network_cfg:
        os.environ.setdefault(
            "CYBERCAR_USE_SYSTEM_PROXY",
            "1" if _to_bool(network_cfg.get("use_system_proxy"), default=False) else "0",
        )
    if default_port:
        os.environ.setdefault("CYBERCAR_CHROME_DEBUG_PORT", default_port)
    if wechat_port:
        os.environ.setdefault("CYBERCAR_WECHAT_CHROME_DEBUG_PORT", wechat_port)
    if x_port:
        os.environ.setdefault("CYBERCAR_X_CHROME_DEBUG_PORT", x_port)
    return paths
