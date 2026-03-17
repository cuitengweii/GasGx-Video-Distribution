from __future__ import annotations

from types import SimpleNamespace

from cybercar import pipeline


def test_resolve_platform_publish_mode_with_config_prefers_wechat_structured_defaults() -> None:
    args = SimpleNamespace(
        wechat_publish_now=False,
        publish_only=False,
        wechat_save_draft_only=False,
        no_save_draft=False,
        kuaishou_auto_publish_random_schedule=False,
        bilibili_auto_publish_random_schedule=False,
    )
    runtime_config = {
        "publish": {
            "platforms": {
                "wechat": {
                    "save_draft": True,
                    "publish_now": False,
                    "declare_original": True,
                    "upload_timeout": 480,
                }
            }
        }
    }

    mode = pipeline._resolve_platform_publish_mode_with_config(args, "wechat", runtime_config)

    assert mode.save_draft is True
    assert mode.publish_now is False
    assert pipeline._resolve_wechat_declare_original(args, runtime_config) is True
    assert pipeline._resolve_platform_upload_timeout(args, runtime_config, "wechat", minimum=30) == 480


def test_resolve_platform_publish_mode_with_config_reads_bilibili_random_schedule_defaults() -> None:
    args = SimpleNamespace(
        wechat_publish_now=False,
        publish_only=False,
        wechat_save_draft_only=False,
        no_save_draft=False,
        kuaishou_auto_publish_random_schedule=False,
        bilibili_auto_publish_random_schedule=False,
        upload_timeout=420,
        bilibili_random_schedule_max_minutes=pipeline.DEFAULT_BILIBILI_RANDOM_SCHEDULE_MAX_MINUTES,
    )
    runtime_config = {
        "publish": {
            "platforms": {
                "bilibili": {
                    "publish_now": True,
                    "auto_publish_random_schedule": True,
                    "random_schedule_max_minutes": 360,
                    "upload_timeout": 900,
                }
            }
        }
    }

    mode = pipeline._resolve_platform_publish_mode_with_config(args, "bilibili", runtime_config)

    assert mode.publish_now is True
    assert mode.bilibili_auto_publish_random_schedule is True
    assert (
        pipeline._resolve_platform_random_schedule_max_minutes(
            args,
            runtime_config,
            "bilibili",
            cli_attr="bilibili_random_schedule_max_minutes",
            parser_default=pipeline.DEFAULT_BILIBILI_RANDOM_SCHEDULE_MAX_MINUTES,
            minimum=pipeline.BILIBILI_RANDOM_SCHEDULE_MIN_LEAD_MINUTES,
        )
        == 360
    )
    assert pipeline._resolve_platform_upload_timeout(args, runtime_config, "bilibili", minimum=600) == 900
