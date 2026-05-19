from pathlib import Path

import cybercar.settings as settings


def test_interaction_management_config_roundtrip(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings, "_repo_root", lambda: tmp_path)
    settings.load_app_config.cache_clear()

    payload = {
        "comment_reply": {
            "enabled": True,
            "max_posts_per_run": 9,
            "max_replies_per_run": 4,
            "reply_min_chars": 8,
            "reply_max_chars": 40,
            "min_reply_interval_seconds": 1,
            "max_reply_interval_seconds": 5,
            "prompt_template": "comment prompt",
            "fallback_replies": ["a", "b"],
        },
        "private_message_reply": {
            "enabled": False,
            "max_conversations_per_run": 6,
            "max_replies_per_run": 3,
            "reply_min_chars": 30,
            "reply_max_chars": 120,
            "min_reply_interval_seconds": 1,
            "max_reply_interval_seconds": 5,
            "min_action_delay_seconds": 2,
            "max_action_delay_seconds": 6,
            "prompt_template": "private prompt",
            "fallback_replies": ["x"],
        },
        "spark_ai": {"provider": "spark"},
        "chrome": {"wechat_debug_port": 9334},
    }

    saved = settings.save_app_config(payload)
    assert saved["comment_reply"]["max_posts_per_run"] == 9
    assert saved["private_message_reply"]["enabled"] is False
    assert saved["private_message_reply"]["min_action_delay_seconds"] == 2

    loaded = settings.load_app_config()
    assert loaded["comment_reply"]["prompt_template"] == "comment prompt"
    assert loaded["private_message_reply"]["fallback_replies"] == ["x"]
    assert loaded["private_message_reply"]["max_action_delay_seconds"] == 6

    service_text = (Path(__file__).resolve().parents[1] / "src" / "gasgx_distribution" / "service.py").read_text(encoding="utf-8")
    web_text = (Path(__file__).resolve().parents[1] / "src" / "gasgx_distribution" / "web.py").read_text(encoding="utf-8")
    assert '"interaction-management"' in service_text
    assert "/api/interaction-management/config" in web_text
    assert "/api/interaction-management/private-msg/run" in web_text


def test_publisher_role_includes_interaction_management_permission() -> None:
    service_text = (Path(__file__).resolve().parents[1] / "src" / "gasgx_distribution" / "service.py").read_text(encoding="utf-8")
    assert '"interaction-management"' in service_text
