from __future__ import annotations

from cybercar import engine


def test_resolve_platform_collection_name_prefers_publish_platform_config() -> None:
    runtime_config = {
        "collection_name": "Global Collection",
        "collection_names": {
            "douyin": "Legacy Map Collection",
        },
        "publish": {
            "platforms": {
                "douyin": {"collection_name": "Structured Douyin Collection"},
            },
        },
    }

    assert engine.resolve_platform_collection_name(runtime_config, "douyin") == "Structured Douyin Collection"


def test_load_runtime_config_promotes_legacy_platform_collection_into_publish_platforms(tmp_path) -> None:
    config_path = tmp_path / "runtime.json"
    config_path.write_text(
        """
{
  "collection_name": "Global Collection",
  "douyin_collection_name": "Legacy Douyin Collection"
}
""".strip(),
        encoding="utf-8",
    )

    payload = engine._load_runtime_config(str(config_path))

    assert payload["collection_name"] == "Global Collection"
    assert payload["collection_names"]["douyin"] == "Legacy Douyin Collection"
    assert payload["publish"]["platforms"]["douyin"]["collection_name"] == "Legacy Douyin Collection"


def test_resolve_platform_publish_config_reads_structured_platform_settings() -> None:
    runtime_config = {
        "publish": {
            "platforms": {
                "wechat": {
                    "collection_name": "Wechat Collection",
                    "save_draft": True,
                    "publish_now": False,
                    "declare_original": True,
                    "upload_timeout": 480,
                }
            }
        }
    }

    payload = engine.resolve_platform_publish_config(runtime_config, "wechat")

    assert payload["collection_name"] == "Wechat Collection"
    assert payload["save_draft"] is True
    assert payload["publish_now"] is False
    assert payload["declare_original"] is True
    assert payload["upload_timeout"] == 30


def test_load_runtime_config_merges_sources_and_cross_border_publish_settings(tmp_path) -> None:
    config_path = tmp_path / "runtime.json"
    config_path.write_text(
        """
{
  "sources": {
    "platforms": "douyin,xiaohongshu",
    "keywords": ["cybertruck", "tesla"],
    "watch_accounts": {
      "douyin": ["https://www.douyin.com/user/demo"],
      "xiaohongshu": ["https://www.xiaohongshu.com/user/profile/demo"]
    }
  },
  "publish": {
    "platforms": {
      "tiktok": {
        "publish_now": true,
        "save_draft": false,
        "upload_timeout": 30
      },
      "x": {
        "publish_now": true,
        "save_draft": false,
        "upload_timeout": 30
      }
    }
  }
}
""".strip(),
        encoding="utf-8",
    )

    payload = engine._load_runtime_config(str(config_path))

    assert payload["sources"]["platforms"] == "douyin,xiaohongshu"
    assert payload["sources"]["keywords"] == ["cybertruck", "tesla"]
    assert payload["sources"]["watch_accounts"]["douyin"] == ["https://www.douyin.com/user/demo"]
    assert payload["publish"]["platforms"]["tiktok"]["publish_now"] is True
    assert payload["publish"]["platforms"]["x"]["publish_now"] is True


def test_load_runtime_config_accepts_utf8_bom_json(tmp_path) -> None:
    config_path = tmp_path / "runtime_bom.json"
    config_path.write_text(
        '{"publish":{"platforms":{"x":{"publish_now":false,"save_draft":false}}}}',
        encoding="utf-8-sig",
    )

    payload = engine._load_runtime_config(str(config_path))

    assert payload["publish"]["platforms"]["x"]["publish_now"] is False


def test_normalize_upload_platforms_accepts_tiktok_and_x_literals() -> None:
    assert engine._normalize_upload_platforms("wechat,tiktok,x") == ["wechat", "tiktok", "x"]
