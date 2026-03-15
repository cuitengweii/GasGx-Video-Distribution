from __future__ import annotations

from typing import Any


PLATFORM_CAPABILITIES: dict[str, dict[str, Any]] = {
    "wechat": {"login_supported": True, "publish_supported": True, "engagement_supported": True, "implemented": True},
    "douyin": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
    "xiaohongshu": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
    "kuaishou": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
    "bilibili": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": True},
}


def platform_meta(platform: str) -> dict[str, Any]:
    return dict(PLATFORM_CAPABILITIES.get(str(platform or "").strip().lower(), {}))
