from __future__ import annotations

from dataclasses import dataclass
from zlib import crc32

DEBUG_PORT_START = 12000
DEBUG_PORT_END = 32000


@dataclass(frozen=True)
class PlatformCapability:
    key: str
    label: str
    region: str
    can_open_browser: bool
    can_login_status: bool
    can_publish: bool
    can_comment: bool
    can_message: bool
    can_stats: bool
    open_url: str


SUPPORTED_PLATFORMS: tuple[PlatformCapability, ...] = (
    PlatformCapability("wechat", "视频号", "cn", True, True, True, True, False, True, "https://channels.weixin.qq.com/platform/post/create"),
    PlatformCapability("douyin", "抖音", "cn", True, True, True, True, False, True, "https://creator.douyin.com/creator-micro/content/upload"),
    PlatformCapability("kuaishou", "快手", "cn", True, True, True, True, False, True, "https://cp.kuaishou.com/article/publish/video"),
    PlatformCapability("xiaohongshu", "小红书", "cn", True, True, True, False, False, True, "https://creator.xiaohongshu.com/publish/publish"),
    PlatformCapability("bilibili", "B站", "cn", True, True, True, False, False, True, "https://member.bilibili.com/platform/upload/video/frame"),
    PlatformCapability("x", "X", "global", True, True, True, False, True, True, "https://x.com/compose/post"),
    PlatformCapability("linkedin", "LinkedIn", "global", True, False, False, False, False, True, "https://www.linkedin.com/"),
    PlatformCapability("facebook", "Facebook", "global", True, False, False, False, False, True, "https://www.facebook.com/"),
    PlatformCapability("youtube", "YouTube", "global", True, False, False, False, False, True, "https://studio.youtube.com/"),
    PlatformCapability("vk", "VK", "global", True, False, False, False, False, True, "https://vk.com/"),
    PlatformCapability("instagram", "Instagram", "global", True, False, False, False, False, True, "https://www.instagram.com/"),
    PlatformCapability("tiktok", "TikTok", "global", True, True, True, False, False, True, "https://www.tiktok.com/upload?lang=en"),
)

PLATFORM_MAP = {item.key: item for item in SUPPORTED_PLATFORMS}


def normalize_platform(platform: str) -> str:
    token = str(platform or "").strip().lower()
    if token == "twitter":
        return "x"
    return token


def get_platform(platform: str) -> PlatformCapability | None:
    return PLATFORM_MAP.get(normalize_platform(platform))


def stable_debug_port(account_key: str, platform: str) -> int:
    token = f"{account_key.strip().lower()}:{normalize_platform(platform)}"
    return DEBUG_PORT_START + (crc32(token.encode("utf-8")) % (DEBUG_PORT_END - DEBUG_PORT_START + 1))
