from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from cybercar import pipeline


def test_build_parser_wechat_defaults_ignore_shared_browser_env(monkeypatch) -> None:
    monkeypatch.setenv("CYBERCAR_CHROME_DEBUG_PORT", "9444")
    monkeypatch.setenv("CYBERCAR_CHROME_USER_DATA_DIR", r"D:\profiles\shared_env")
    monkeypatch.delenv("CYBERCAR_WECHAT_CHROME_DEBUG_PORT", raising=False)
    monkeypatch.delenv("CYBERCAR_WECHAT_CHROME_USER_DATA_DIR", raising=False)
    monkeypatch.setattr(pipeline.core, "DEFAULT_WECHAT_DEBUG_PORT", 9334, raising=False)

    args = pipeline._build_parser().parse_args([])

    assert args.wechat_debug_port == 9334
    assert str(args.wechat_chrome_user_data_dir).replace("\\", "/").endswith("profiles/wechat")


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
    assert pipeline._resolve_platform_upload_timeout(args, runtime_config, "wechat", minimum=30) == 30


def test_resolve_platform_publish_mode_with_config_reads_bilibili_random_schedule_defaults() -> None:
    args = SimpleNamespace(
        wechat_publish_now=False,
        publish_only=False,
        wechat_save_draft_only=False,
        no_save_draft=False,
        kuaishou_auto_publish_random_schedule=False,
        bilibili_auto_publish_random_schedule=False,
        upload_timeout=30,
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
    assert pipeline._resolve_platform_upload_timeout(args, runtime_config, "bilibili", minimum=600) == 30


def test_resolve_platform_publish_mode_with_config_reads_x_platform_defaults() -> None:
    args = SimpleNamespace(
        wechat_publish_now=False,
        publish_only=False,
        wechat_save_draft_only=False,
        no_save_draft=False,
        kuaishou_auto_publish_random_schedule=False,
        bilibili_auto_publish_random_schedule=False,
        upload_timeout=30,
    )
    runtime_config = {
        "publish": {
            "platforms": {
                "x": {
                    "save_draft": False,
                    "publish_now": True,
                    "upload_timeout": 120,
                }
            }
        }
    }

    mode = pipeline._resolve_platform_publish_mode_with_config(args, "x", runtime_config)

    assert mode.save_draft is False
    assert mode.publish_now is True
    assert pipeline._resolve_platform_upload_timeout(args, runtime_config, "x", minimum=30) == 30


def test_run_publish_schedule_dispatches_tiktok_and_x_when_requested(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "DRAFT_test_video.mp4"
    target.write_bytes(b"video")
    workspace = SimpleNamespace(root=tmp_path)
    ctx = pipeline.CycleContext(
        workspace=workspace,
        processed_outputs=[target],
        collected_x_urls=[],
        exclude_keywords=[],
        require_any_keywords=[],
        collection_name="cybertruck",
        chrome_path=None,
        chrome_user_data_dir=str(tmp_path / "profile"),
        proxy=None,
        use_system_proxy=False,
        sorted_batch_dir=None,
        collected_at="2026-03-30 22:30:00",
        keyword="cybertruck",
        requested_limit=1,
        extra_url_count=0,
        auto_discover_x=False,
        collection_names={},
    )
    args = SimpleNamespace(
        upload_platforms="tiktok,x",
        collect_media_kind="video",
        no_publish_skip_notify=True,
        upload_only_approved=False,
        non_wechat_max_videos=1,
        xiaohongshu_allow_image=False,
        xiaohongshu_extra_images_per_run=0,
        non_wechat_random_window_minutes=0,
        publish_only=True,
        review_state_file="",
        disable_publish_summary_notify=True,
    )
    email_settings = pipeline.EmailSettings(
        enabled=False,
        provider="",
        env_prefix="",
        resend_api_key="",
        resend_from_email="",
        resend_endpoint="",
        resend_timeout_seconds=10,
        recipients=[],
        telegram_bot_token="",
        telegram_chat_id="",
        telegram_timeout_seconds=10,
        telegram_api_base="",
    )
    dispatched: list[str] = []

    monkeypatch.setattr(pipeline, "_all_summary_platforms", lambda: ["tiktok", "x"])
    monkeypatch.setattr(pipeline, "_build_pending_counts", lambda ctx, platforms: {})
    monkeypatch.setattr(pipeline, "_build_publish_summary", lambda **kwargs: ("", []))
    monkeypatch.setattr(pipeline, "_send_publish_summary_notification", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "_publish_once", lambda _ctx, _args, _email, platform, *_a, **_k: dispatched.append(str(platform)) or True)
    monkeypatch.setattr(pipeline.core, "_log", lambda message: None)
    monkeypatch.setattr(pipeline.core, "_backfill_uploaded_fingerprint_index", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline.core, "_build_shared_source_targets", lambda *args, **kwargs: [target])
    monkeypatch.setattr(
        pipeline.core,
        "build_content_coordination_snapshot",
        lambda *args, **kwargs: {
            "review_status": "approved",
            "unpublished_platforms": ["tiktok", "x"],
            "platform_status": {},
        },
    )

    pipeline._run_publish_schedule(ctx, args, email_settings)

    assert set(dispatched) == {"tiktok", "x"}


def _build_email_settings() -> pipeline.EmailSettings:
    return pipeline.EmailSettings(
        enabled=False,
        provider="",
        env_prefix="",
        resend_api_key="",
        resend_from_email="",
        resend_endpoint="",
        resend_timeout_seconds=10,
        recipients=[],
        telegram_bot_token="",
        telegram_chat_id="",
        telegram_timeout_seconds=10,
        telegram_api_base="",
    )


def _build_cycle_context(tmp_path: Path, target: Path) -> pipeline.CycleContext:
    return pipeline.CycleContext(
        workspace=SimpleNamespace(root=tmp_path),
        processed_outputs=[target],
        collected_x_urls=[],
        exclude_keywords=[],
        require_any_keywords=[],
        collection_name="cybertruck",
        chrome_path=None,
        chrome_user_data_dir=str(tmp_path / "profile"),
        proxy=None,
        use_system_proxy=False,
        sorted_batch_dir=None,
        collected_at="2026-03-30 23:00:00",
        keyword="cybertruck",
        requested_limit=1,
        extra_url_count=0,
        auto_discover_x=False,
        collection_names={},
    )


@pytest.mark.parametrize(
    ("platform_config", "expected_result"),
    [
        ({"publish_now": False, "save_draft": False}, "uploaded_only"),
        ({"publish_now": False, "save_draft": True}, "draft_saved"),
        ({"publish_now": True, "save_draft": False}, "published"),
    ],
)
def test_publish_once_sets_result_by_mode(
    tmp_path: Path,
    monkeypatch,
    platform_config: dict[str, bool],
    expected_result: str,
) -> None:
    target = tmp_path / "video.mp4"
    target.write_bytes(b"video")
    ctx = _build_cycle_context(tmp_path, target)
    args = SimpleNamespace(
        debug_port=9222,
        caption="",
        config=str(tmp_path / "runtime.json"),
        no_auto_open_chrome=True,
        notify_per_publish=False,
        no_save_draft=False,
        upload_timeout=30,
    )
    events: list[pipeline.PublishEvent] = []

    monkeypatch.setattr(
        pipeline,
        "_coordination_eligible_platforms",
        lambda *_a, **_k: ({"review_status": "approved", "platform_status": {}}, ["tiktok"]),
    )
    monkeypatch.setattr(
        pipeline.core,
        "_load_runtime_config",
        lambda _path: {"publish": {"platforms": {"tiktok": platform_config}}},
    )
    monkeypatch.setattr(pipeline.core, "fill_draft_tiktok", lambda *args, **kwargs: target)
    monkeypatch.setattr(pipeline.core, "_should_record_publish_fingerprint", lambda *args, **kwargs: False)
    monkeypatch.setattr(pipeline.core, "_append_draft_upload_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline.core, "_record_uploaded_content_fingerprint", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "_build_publish_notification_card", lambda **kwargs: ("", {}))
    monkeypatch.setattr(pipeline.core, "_log", lambda *args, **kwargs: None)

    ok = pipeline._publish_once(
        ctx=ctx,
        args=args,
        email_settings=_build_email_settings(),
        platform="tiktok",
        target=target,
        stage="immediate_1",
        events=events,
    )

    assert ok is True
    assert len(events) == 1
    assert events[0].result == expected_result


def _build_event(video_name: str, platform_name: str, result: str, success: bool = True) -> pipeline.PublishEvent:
    return pipeline.PublishEvent(
        platform=platform_name,
        stage="immediate_1",
        success=success,
        result=result,
        published_at="23:00:00",
        video_name=video_name,
        publish_id=f"CT-{platform_name.upper()}",
        desc_prefix10="",
        source_url="",
        error="",
    )


def test_recycle_does_not_count_uploaded_only_as_success(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "video.mp4"
    target.write_bytes(b"video")
    ctx = _build_cycle_context(tmp_path, target)
    args = SimpleNamespace(recycle_bin_subdir="5_Recycle_Bin")
    moved: list[str] = []

    monkeypatch.setattr(
        pipeline,
        "_move_video_bundle_to_recycle",
        lambda video, recycle_dir: moved.append(video.name) or 1,
    )
    monkeypatch.setattr(pipeline.core, "_log", lambda *args, **kwargs: None)

    recycled = pipeline._recycle_fully_published_videos(
        ctx=ctx,
        args=args,
        target_by_name={target.name: target},
        planned_platforms_by_video={target.name: {"x", "tiktok"}},
        publish_events=[
            _build_event(target.name, "x", "uploaded_only"),
            _build_event(target.name, "tiktok", "published"),
        ],
    )

    assert recycled == 0
    assert moved == []


def test_recycle_allows_published_plus_skipped_duplicate(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "video.mp4"
    target.write_bytes(b"video")
    ctx = _build_cycle_context(tmp_path, target)
    args = SimpleNamespace(recycle_bin_subdir="5_Recycle_Bin")
    moved: list[str] = []

    monkeypatch.setattr(
        pipeline,
        "_move_video_bundle_to_recycle",
        lambda video, recycle_dir: moved.append(video.name) or 1,
    )
    monkeypatch.setattr(pipeline.core, "_log", lambda *args, **kwargs: None)

    recycled = pipeline._recycle_fully_published_videos(
        ctx=ctx,
        args=args,
        target_by_name={target.name: target},
        planned_platforms_by_video={target.name: {"x", "tiktok"}},
        publish_events=[
            _build_event(target.name, "x", "skipped_duplicate"),
            _build_event(target.name, "tiktok", "published"),
        ],
    )

    assert recycled == 1
    assert moved == [target.name]


def test_send_login_required_alert_only_pushes_qr_card(tmp_path: Path, monkeypatch) -> None:
    qr_calls: list[dict[str, object]] = []
    text_calls: list[dict[str, object]] = []
    logs: list[str] = []

    monkeypatch.setattr(
        pipeline.core,
        "send_platform_login_qr_notification",
        lambda **kwargs: qr_calls.append(dict(kwargs)) or {"ok": True, "sent": True},
    )
    monkeypatch.setattr(
        pipeline,
        "_send_telegram_text",
        lambda *args, **kwargs: text_calls.append({"args": args, "kwargs": dict(kwargs)}),
    )
    monkeypatch.setattr(pipeline.core, "_log", lambda message: logs.append(str(message)))

    args = SimpleNamespace(monitor_url="")
    settings = pipeline.EmailSettings(
        enabled=True,
        provider="",
        env_prefix="",
        resend_api_key="",
        resend_from_email="",
        resend_endpoint="",
        resend_timeout_seconds=10,
        recipients=[],
        telegram_bot_token="token",
        telegram_chat_id="chat",
        telegram_timeout_seconds=20,
        telegram_api_base="",
    )

    pipeline._send_login_required_alert(
        args=args,
        settings=settings,
        platform="xiaohongshu",
        stage="publish",
        error_text="未登录，请扫码登录后继续",
        debug_port=9336,
        chrome_user_data_dir=str(tmp_path / "profile"),
    )
    pipeline._send_login_required_alert(
        args=args,
        settings=settings,
        platform="xiaohongshu",
        stage="publish",
        error_text="未登录，请扫码登录后继续",
        debug_port=9336,
        chrome_user_data_dir=str(tmp_path / "profile"),
    )

    assert len(qr_calls) == 1
    assert str(qr_calls[0].get("open_url") or "").startswith("https://creator.xiaohongshu.com")
    assert text_calls == []
    assert any("登录二维码已发送" in line for line in logs)
