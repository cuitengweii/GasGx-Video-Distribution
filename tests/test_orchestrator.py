from __future__ import annotations

from cybercar import orchestrator


def test_profile_mapping_prefers_upload_platforms_over_target_platforms() -> None:
    profile = orchestrator.ProfileMapping(
        name="cn_to_global",
        payload={
            "upload_platforms": "tiktok,x",
            "target_platforms": "wechat,douyin",
        },
    )

    assert profile.upload_platforms == "tiktok,x"


def test_merge_runtime_config_applies_profile_source_isolation_fields() -> None:
    base = {
        "sources": {
            "platforms": "x",
            "keywords": ["cybertruck"],
            "watch_accounts": {"x": []},
        }
    }
    profile = orchestrator.ProfileMapping(
        name="cn_to_global",
        payload={
            "source_platforms": "douyin,xiaohongshu",
            "source_keywords": ["cybertruck", "赛博皮卡"],
            "source_watch_accounts": {"douyin": ["https://www.douyin.com/user/abc"]},
            "source_latest_keywords_state_file": "runtime/source_keywords_latest_cn_to_global.json",
            "prefer_latest_keywords": True,
        },
    )

    merged = orchestrator._merge_runtime_config(base, profile)
    sources = dict(merged.get("sources") or {})

    assert sources.get("platforms") == "douyin,xiaohongshu"
    assert sources.get("keywords") == ["cybertruck", "赛博皮卡"]
    assert sources.get("watch_accounts") == {"douyin": ["https://www.douyin.com/user/abc"]}
    assert sources.get("latest_keywords_state_file") == "runtime/source_keywords_latest_cn_to_global.json"
    assert sources.get("prefer_latest_keywords") is True

