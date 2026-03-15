from __future__ import annotations

from typing import Any

from ...session import capture_login_qr, login_status, open_login


PLATFORM_CAPABILITIES: dict[str, dict[str, Any]] = {
    "wechat": {"login_supported": True, "publish_supported": True, "engagement_supported": True, "implemented": True},
    "douyin": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
    "xiaohongshu": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
    "kuaishou": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
    "bilibili": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
}


def probe_session(platform: str) -> dict[str, Any]:
    return login_status(platform)


def open_session(platform: str) -> dict[str, Any]:
    return open_login(platform)


def capture_qr(platform: str = "wechat") -> dict[str, Any]:
    return capture_login_qr(platform)
