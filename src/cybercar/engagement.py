from __future__ import annotations

from typing import Any

from . import engine
from .settings import apply_runtime_environment, load_app_config
from .services.engagement.runtime import (
    diagnose_platform_comment_page,
    reply_douyin_focused_generated,
    reply_douyin_focused_editor,
    reply_kuaishou_focused_editor,
    reply_kuaishou_focused_generated,
    run_platform_comment_reply,
)


def _build_engagement_runtime_config(*, like_only: bool) -> tuple[engine.Workspace, dict[str, Any], dict[str, Any], Any]:
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
    return workspace, runtime_cfg, chrome_cfg, paths


def run_wechat_engagement(
    *,
    max_posts: int = 0,
    max_replies: int = 0,
    like_only: bool = False,
    latest_only: bool = False,
    debug: bool = False,
) -> dict[str, Any]:
    app_config = load_app_config()
    workspace, runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=like_only)
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


def run_douyin_engagement(
    *,
    max_posts: int = 0,
    max_replies: int = 0,
    like_only: bool = False,
    latest_only: bool = False,
    debug: bool = False,
) -> dict[str, Any]:
    workspace, runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=like_only)
    app_config = load_app_config()
    focused_result = reply_douyin_focused_generated(
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
        debug=debug,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )
    if bool(focused_result.get("ok")) or str(focused_result.get("reason") or "") not in {
        "douyin_comment_page_not_open",
        "focused_reply_editor_not_ready",
    }:
        return focused_result
    return run_platform_comment_reply(
        platform_name="douyin",
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
        max_posts_override=max_posts,
        max_replies_override=max_replies,
        latest_only=latest_only,
        debug=debug,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )


def run_kuaishou_engagement(
    *,
    max_posts: int = 0,
    max_replies: int = 0,
    like_only: bool = False,
    latest_only: bool = False,
    debug: bool = False,
) -> dict[str, Any]:
    workspace, runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=like_only)
    app_config = load_app_config()
    return run_platform_comment_reply(
        platform_name="kuaishou",
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
        max_posts_override=max_posts,
        max_replies_override=max_replies,
        latest_only=latest_only,
        debug=debug,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )


def run_douyin_focused_generated_engagement(
    *,
    debug: bool = False,
    ignore_state: bool = False,
) -> dict[str, Any]:
    workspace, runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=False)
    app_config = load_app_config()
    return reply_douyin_focused_generated(
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
        debug=debug,
        ignore_state=ignore_state,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )


def run_kuaishou_focused_generated_engagement(
    *,
    debug: bool = False,
    ignore_state: bool = False,
) -> dict[str, Any]:
    workspace, runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=False)
    app_config = load_app_config()
    return reply_kuaishou_focused_generated(
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
        debug=debug,
        ignore_state=ignore_state,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )


def run_douyin_focused_engagement(
    *,
    reply_text: str,
    debug: bool = False,
    ignore_state: bool = False,
) -> dict[str, Any]:
    workspace, runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=False)
    app_config = load_app_config()
    return reply_douyin_focused_editor(
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
        reply_text=str(reply_text or ""),
        debug=debug,
        ignore_state=ignore_state,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )


def run_kuaishou_focused_engagement(
    *,
    reply_text: str,
    debug: bool = False,
    ignore_state: bool = False,
) -> dict[str, Any]:
    workspace, runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=False)
    app_config = load_app_config()
    return reply_kuaishou_focused_editor(
        workspace=workspace,
        runtime_config=runtime_cfg,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
        reply_text=str(reply_text or ""),
        debug=debug,
        ignore_state=ignore_state,
        notify_env_prefix=str((app_config.get("notify") or {}).get("env_prefix") or "CYBERCAR_NOTIFY_"),
    )


def diagnose_platform_engagement(platform_name: str) -> dict[str, Any]:
    workspace, _runtime_cfg, chrome_cfg, paths = _build_engagement_runtime_config(like_only=False)
    platform = str(platform_name or "").strip().lower()
    if platform == "wechat":
        return {
            "ok": False,
            "platform": platform,
            "reason": "diagnose_not_supported_for_wechat",
        }
    return diagnose_platform_comment_page(
        platform_name=platform,
        workspace=workspace,
        debug_port=int(chrome_cfg.get("default_debug_port") or 9333),
        chrome_user_data_dir=str(paths.default_profile_dir),
    )
