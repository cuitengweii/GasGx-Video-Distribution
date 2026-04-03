from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cybercar import engine


def test_resolve_platform_collection_name_prefers_douyin_default_over_global_collection() -> None:
    runtime_config = {
        "collection_name": "赛博皮卡天津港现车",
    }

    assert engine.resolve_platform_collection_name(runtime_config, "douyin") == "赛博皮卡现车：aawbcc"
    assert engine.resolve_platform_collection_name(runtime_config, "bilibili") == "赛博皮卡天津港现车"


def _workspace(tmp_path: Path) -> engine.Workspace:
    root = tmp_path / "workspace"
    root.mkdir()
    return engine.Workspace(
        root=root,
        downloads=root / "1_Downloads",
        processed=root / "2_Processed",
        archive=root / "3_Archive",
        history=root / "history.jsonl",
        image_downloads=root / "1_Downloads_Images",
        image_processed=root / "2_Processed_Images",
        image_history=root / "image_history.jsonl",
    )


def test_is_douyin_collection_match_strips_counter_suffix() -> None:
    assert engine._is_douyin_collection_match("Cybertruck Clips 共 6 作品", "Cybertruck Clips")
    assert engine._is_douyin_collection_match("添加合集 Cybertruck Clips", "Cybertruck Clips")


def test_normalize_douyin_collection_value_strips_suffix_alias() -> None:
    assert engine._normalize_douyin_collection_value("赛博皮卡现车：aawbcc") == "赛博皮卡现车"


def test_is_douyin_collection_match_accepts_prefix_suffix_alias() -> None:
    assert engine._is_douyin_collection_match("赛博皮卡现车：aawbcc", "赛博皮卡天津港现车")


def test_resolve_platform_collection_name_prefers_platform_specific_value() -> None:
    runtime_config = {
        "collection_name": "Global Collection",
        "collection_names": {
            "douyin": "Douyin Collection",
            "wechat": "Wechat Collection",
        },
    }

    assert engine.resolve_platform_collection_name(runtime_config, "douyin") == "Douyin Collection"
    assert engine.resolve_platform_collection_name(runtime_config, "wechat") == "Wechat Collection"
    assert engine.resolve_platform_collection_name(runtime_config, "bilibili") == "Global Collection"


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


def test_resolve_platform_collection_name_supports_legacy_platform_key() -> None:
    runtime_config = {
        "collection_name": "Global Collection",
        "douyin_collection_name": "Legacy Douyin Collection",
    }

    assert engine.resolve_platform_collection_name(runtime_config, "douyin") == "Legacy Douyin Collection"


def test_resolve_platform_collection_name_uses_douyin_default_collection() -> None:
    assert engine.resolve_platform_collection_name({}, "douyin") == "赛博皮卡现车：aawbcc"


def test_prepare_image_post_text_payload_keeps_douyin_caption(monkeypatch: pytest.MonkeyPatch) -> None:
    title_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        engine,
        "_fill_optional_platform_title_field",
        lambda *args, **kwargs: title_calls.append(dict(kwargs)) or "",
    )

    caption = "Cybertruck\nStore visit"
    result = engine._prepare_image_post_text_payload(
        object(),
        object(),
        platform_name="douyin",
        caption=caption,
    )

    assert result == caption
    assert title_calls == []


def test_select_douyin_collection_supports_dropdown_picker(monkeypatch: pytest.MonkeyPatch) -> None:
    states = [
        {"hasField": True, "current": "", "episode": "", "source": "empty"},
        {"hasField": True, "current": "Cybertruck Clips", "episode": "Episode 7", "source": "selected"},
    ]

    def fake_get_state(_primary: Any, _fallback: Any) -> dict[str, Any]:
        if len(states) > 1:
            return states.pop(0)
        return states[0]

    class FakeCtx:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def run_js(self, script: str, *args: Any) -> Any:
            self.calls.append((script, args))
            assert ".semi-select-option" in script
            assert ".semi-select-selection-text" in script
            assert '[class*="option-title"]' in script
            assert "visible_options" in script
            assert args == ("Cybertruck Clips",)
            return {
                "state": "clicked",
                "option": "Cybertruck Clips",
                "visible_options": ["Cybertruck Clips"],
            }

    monkeypatch.setattr(engine, "_get_douyin_collection_state", fake_get_state)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    ctx = FakeCtx()
    engine._select_douyin_collection(ctx, None, "Cybertruck Clips")

    assert len(ctx.calls) == 1


def test_select_douyin_collection_stops_after_first_matching_click(monkeypatch: pytest.MonkeyPatch) -> None:
    states = [
        {"hasField": True, "current": "", "episode": "", "source": "empty"},
        {"hasField": True, "current": "赛博皮卡现车：aawbcc", "episode": "", "source": "selected"},
    ]

    def fake_get_state(_primary: Any, _fallback: Any) -> dict[str, Any]:
        if len(states) > 1:
            return states.pop(0)
        return states[0]

    class FirstCtx:
        def __init__(self) -> None:
            self.calls = 0

        def run_js(self, script: str, *args: Any) -> Any:
            self.calls += 1
            assert args == ("赛博皮卡现车：aawbcc",)
            return {
                "state": "clicked",
                "option": "赛博皮卡现车：aawbcc",
                "visible_options": ["赛博皮卡现车：aawbcc"],
            }

    class SecondCtx:
        def __init__(self) -> None:
            self.calls = 0

        def run_js(self, script: str, *args: Any) -> Any:
            self.calls += 1
            raise AssertionError("fallback context should not be clicked after primary selected target")

    monkeypatch.setattr(engine, "_get_douyin_collection_state", fake_get_state)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    primary = FirstCtx()
    fallback = SecondCtx()

    engine._select_douyin_collection(primary, fallback, "赛博皮卡现车：aawbcc")

    assert primary.calls == 1
    assert fallback.calls == 0


def test_select_douyin_collection_accepts_matching_click_when_state_reader_stays_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    states = [
        {"hasField": True, "current": "", "episode": "", "source": "empty"},
        {"hasField": True, "current": "", "episode": "", "source": "empty"},
    ]

    def fake_get_state(_primary: Any, _fallback: Any) -> dict[str, Any]:
        if len(states) > 1:
            return states.pop(0)
        return states[0]

    class FakeCtx:
        def __init__(self) -> None:
            self.calls = 0

        def run_js(self, script: str, *args: Any) -> Any:
            self.calls += 1
            return {
                "state": "clicked",
                "option": "赛博皮卡现车：aawbcc",
                "visible_options": ["赛博皮卡现车：aawbcc"],
            }

    monkeypatch.setattr(engine, "_get_douyin_collection_state", fake_get_state)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    ctx = FakeCtx()
    engine._select_douyin_collection(ctx, None, "赛博皮卡现车：aawbcc")

    assert ctx.calls == 1


def test_fill_draft_douyin_passes_collection_name_to_generic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _workspace(tmp_path)
    target = tmp_path / "douyin-image.png"
    target.write_text("image", encoding="utf-8")

    root_page = object()
    work_page = object()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(engine, "_connect_chrome", lambda **kwargs: root_page)
    monkeypatch.setattr(engine, "_prepare_upload_tab", lambda page: work_page)
    monkeypatch.setattr(engine, "_close_work_tab", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_prepare_caption_for_platform", lambda caption, platform_name="": caption)
    monkeypatch.setattr(engine, "_load_caption_for_video", lambda target_path: "")

    def fake_fill_draft_once_generic(*args: Any, **kwargs: Any) -> object:
        captured.update(kwargs)
        return work_page

    monkeypatch.setattr(engine, "_fill_draft_once_generic", fake_fill_draft_once_generic)

    used_target = engine.fill_draft_douyin(
        workspace,
        caption="Cybertruck",
        target_video=target,
        collection_name="Cybertruck Clips",
        auto_open_chrome=False,
        check_duplicate_before_upload=False,
        record_after_success=False,
    )

    assert used_target == target
    assert captured["collection_name"] == "Cybertruck Clips"


def test_looks_like_douyin_image_upload_ready_accepts_editor_state() -> None:
    text = "作品描述 编辑图片 已添加1张图片 继续添加 发布设置 发布时间 立即发布 预览图文"
    actions = ["编辑图片 已添加1张图片 继续添加", "发布时间", "立即发布", "发布"]

    assert engine._looks_like_douyin_image_upload_ready(text, actions) is True


def test_looks_like_douyin_image_upload_ready_rejects_upload_shell_only() -> None:
    text = "鐐瑰嚮涓婁紶 鎴栫洿鎺ュ皢鍥剧墖鏂囦欢鎷栧叆姝ゅ尯鍩?鎺ㄨ崘涓婁紶 9:16 鐨勭珫鐗堝浘鐗?"
    actions = ["鐐瑰嚮涓婁紶", "鍙戝竷"]

    assert engine._looks_like_douyin_image_upload_ready(text, actions) is False


def test_looks_like_douyin_image_upload_ready_accepts_normal_chinese_editor_state() -> None:
    text = "作品描述 编辑图片 已添加1张图片 继续添加 发布设置 发布时间 立即发布 预览图文"
    actions = ["编辑图片", "继续添加", "发布设置", "发布"]

    assert engine._looks_like_douyin_image_upload_ready(text, actions) is True


def test_looks_like_douyin_image_upload_ready_rejects_normal_chinese_upload_shell_only() -> None:
    text = "点击上传 或直接将图片文件拖入此区域 推荐上传 9:16 的竖版图片"
    actions = ["点击上传", "发布"]

    assert engine._looks_like_douyin_image_upload_ready(text, actions) is False
