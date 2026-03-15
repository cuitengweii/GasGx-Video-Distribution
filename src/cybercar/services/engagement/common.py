from __future__ import annotations

from typing import Any


PLATFORM_CAPABILITIES: dict[str, dict[str, Any]] = {
    "wechat": {"login_supported": True, "publish_supported": True, "engagement_supported": True, "implemented": True},
    "douyin": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": False},
    "xiaohongshu": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": False},
    "kuaishou": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": False},
    "bilibili": {"login_supported": True, "publish_supported": True, "engagement_supported": False, "implemented": False},
}


def unsupported(platform: str) -> dict[str, Any]:
    return {
        "ok": False,
        "platform": str(platform or "").strip().lower(),
        "implemented": False,
        "reason": "engagement automation is only implemented for wechat in this release",
    }
