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
