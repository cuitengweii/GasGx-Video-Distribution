from __future__ import annotations

from typing import Any

from . import engine
from .settings import apply_runtime_environment, load_app_config


def run_wechat_engagement(
    *,
    max_posts: int = 0,
    max_replies: int = 0,
    like_only: bool = False,
    latest_only: bool = False,
    debug: bool = False,
) -> dict[str, Any]:
    paths = apply_runtime_environment()
    app_config = load_app_config()
    workspace = engine.init_workspace(str(paths.runtime_root))
    chrome_cfg = app_config.get("chrome") if isinstance(app_config.get("chrome"), dict) else {}
    runtime_cfg = dict(app_config)
    comment_cfg = dict(runtime_cfg.get("comment_reply") or {})
    if like_only:
        comment_cfg["auto_like"] = True
        comment_cfg["enabled"] = True
    runtime_cfg["comment_reply"] = comment_cfg
    return engine.run_wechat_comment_reply(
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("wechat_debug_port") or 9334),
        chrome_user_data_dir=str(paths.wechat_profile_dir),
        max_posts_override=max_posts,
        max_replies_override=max_replies,
        latest_only=latest_only,
        debug=debug,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )
