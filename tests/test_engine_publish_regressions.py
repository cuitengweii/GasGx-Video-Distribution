from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from cybercar import engine


def test_resolve_post_editor_context_fallback_returns_page_without_name_error(monkeypatch) -> None:
    fake_page = SimpleNamespace(run_js=lambda script: {}, url="https://www.tiktok.com/upload")
    monkeypatch.setattr(engine, "_get_page_frames_with_timeout", lambda page, timeout_seconds=2.5: [])
    monkeypatch.setattr(engine, "_log", lambda message: None)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: None)

    resolved = engine._resolve_post_editor_context(fake_page, timeout_seconds=1)

    assert resolved is fake_page


def test_random_publish_mode_helpers_have_defined_choice_flags(monkeypatch) -> None:
    monkeypatch.setattr(engine, "_log", lambda message: None)
    monkeypatch.setattr(engine.random, "randint", lambda a, b: 0)
    monkeypatch.setattr(engine.random, "getrandbits", lambda n: 0)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *args, **kwargs: True)

    assert engine._configure_douyin_random_publish_mode(object(), object()) == "immediate"
    assert engine._configure_bilibili_random_publish_mode(object(), object()) == "immediate"
    assert engine._configure_kuaishou_random_publish_mode(object(), object()) == "immediate"


def test_extract_upload_progress_skips_regex_errors_and_keeps_matching(monkeypatch) -> None:
    calls = {"count": 0}
    original_search = re.search

    def fake_search(pattern: str, text: str, flags: int = 0):
        calls["count"] += 1
        if calls["count"] == 1:
            raise re.error("boom")
        return original_search(pattern, text, flags)

    monkeypatch.setattr(engine.re, "search", fake_search)

    progress = engine._extract_upload_progress("processing 42%")
    assert "processing 42%" in progress.lower()


def test_split_caption_candidates_handles_mixed_punctuation() -> None:
    text = "Hot And Humid Texas Day. Reorganizing The CyberTruck Frunk！Ryobi stackable box; works well."
    parts = engine._split_caption_candidates(text)

    assert len(parts) >= 2
    assert any("CyberTruck Frunk" in part for part in parts)


def test_required_hashtags_can_be_disabled_for_matrix_distribution(monkeypatch) -> None:
    monkeypatch.setenv("CYBERCAR_DISABLE_REQUIRED_HASHTAGS", "1")

    caption = engine._ensure_required_hashtags("GasGx caption")

    assert caption == "GasGx caption"
    assert "Cybertruck" not in caption


def test_limit_caption_hashtags_truncates_to_max() -> None:
    caption = "Deploy generators near the source #天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿"

    limited = engine._limit_caption_hashtags(caption, 4)

    assert limited.count("#") == 4
    assert "#海外挖矿" not in limited


def test_fill_caption_verifies_configured_caption_when_required_hashtags_disabled(monkeypatch) -> None:
    class FakeEditor:
        tag = "div"

        def attr(self, _name: str) -> str:
            return "caption-editor"

    editor = FakeEditor()
    monkeypatch.setenv("CYBERCAR_DISABLE_REQUIRED_HASHTAGS", "1")
    monkeypatch.setattr(engine, "_find_visible_caption_editor", lambda _ctx: editor)
    monkeypatch.setattr(engine, "_input_caption_with_keyboard", lambda _editor, _caption: True)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_read_element_text", lambda _editor: "GasGx caption #天然气")
    monkeypatch.setattr(engine, "_read_caption_text", lambda _ctx: "")
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._fill_caption(object(), "GasGx caption #天然气")


def test_fill_caption_continues_when_matrix_caption_readback_is_empty(monkeypatch) -> None:
    class FakeEditor:
        tag = "div"

        def attr(self, _name: str) -> str:
            return "caption-editor"

    editor = FakeEditor()
    messages: list[str] = []
    monkeypatch.setenv("CYBERCAR_DISABLE_REQUIRED_HASHTAGS", "1")
    monkeypatch.setattr(engine, "_find_visible_caption_editor", lambda _ctx: editor)
    monkeypatch.setattr(engine, "_input_caption_with_keyboard", lambda _editor, _caption: True)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_read_element_text", lambda _editor: "")
    monkeypatch.setattr(engine, "_read_caption_text", lambda _ctx: "")
    monkeypatch.setattr(engine, "_log", messages.append)

    engine._fill_caption(object(), "GasGx caption #天然气")

    assert any("continue after successful keyboard input" in item for item in messages)


def test_caption_from_info_json_uses_semantic_text_instead_of_generic_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    info_path = tmp_path / "sample.info.json"
    info_path.write_text(
        json.dumps(
            {
                "title": "Hot And Humid Texas Day",
                "description": (
                    "Hot And Humid Texas Day. I turned on the garage fans. "
                    "Reorganizing the CyberTruck frunk with stackable boxes."
                ),
                "uploader": "Jones1",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    captured: dict[str, str] = {}

    def fake_build_bilingual_caption(
        base_text: str,
        proxy: str | None,
        spark_ai: dict[str, object] | None,
        *,
        use_system_proxy: bool = False,
    ) -> tuple[str, str]:
        del proxy, spark_ai, use_system_proxy
        captured["base_text"] = base_text
        return "ZH", "EN"

    monkeypatch.setattr(engine, "_build_bilingual_caption", fake_build_bilingual_caption)

    caption = engine._caption_from_info_json(info_path)

    assert caption == "ZH\nEN"
    assert captured.get("base_text")
    assert captured["base_text"] != "Cybertruck 最新画面"
    assert "cybertruck frunk" in captured["base_text"].lower()


def test_fill_draft_once_generic_kuaishou_uses_generic_publish_fallback(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    button_calls: list[tuple[str, ...]] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://cp.kuaishou.com/article/publish/video"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        button_calls.append(text_tuple)
        return text_tuple == ("发布作品",)

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_ensure_kuaishou_publish_mode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_kuaishou_upload_file_input", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_scroll_kuaishou_publish_controls_into_view", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_publish_confirm_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_publish_confirm_dialog_only", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_wait_publish_feedback", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    result = engine._fill_draft_once_generic(
        page=page,
        target=target,
        final_caption="caption",
        open_url=page.url,
        platform_name="kuaishou",
        save_draft=False,
        publish_now=True,
        upload_timeout=30,
        draft_button_texts=("保存草稿",),
        publish_button_texts=("发布作品",),
    )

    assert result is page
    assert ("立即发布",) in button_calls
    assert ("发布作品",) in button_calls


def test_fill_draft_once_generic_bilibili_uses_generic_publish_fallback(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    button_calls: list[tuple[str, ...]] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://member.bilibili.com/platform/upload/video/frame"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        button_calls.append(text_tuple)
        return text_tuple == ("立即投稿",)

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_bilibili_upload_file_input", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_prepare_bilibili_cover_editor", lambda *_args, **_kwargs: Path(target))
    monkeypatch.setattr(engine, "_fill_bilibili_title_from_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_reset_bilibili_publish_probe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_bilibili_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_wait_publish_feedback", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    result = engine._fill_draft_once_generic(
        page=page,
        target=target,
        final_caption="caption",
        open_url=page.url,
        platform_name="bilibili",
        save_draft=False,
        publish_now=True,
        upload_timeout=30,
        draft_button_texts=("保存草稿",),
        publish_button_texts=("立即投稿",),
    )

    assert result is page
    assert ("立即投稿",) in button_calls


def test_fill_draft_once_generic_bilibili_uses_upload_timeout_for_feedback_wait(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    captured_timeouts: list[int] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://member.bilibili.com/platform/upload/video/frame"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_wait(*_args, **kwargs):
        captured_timeouts.append(int(kwargs.get("timeout_seconds", 0) or 0))
        return None

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_bilibili_upload_file_input", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_prepare_bilibili_cover_editor", lambda *_args, **_kwargs: Path(target))
    monkeypatch.setattr(engine, "_fill_bilibili_title_from_caption", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_reset_bilibili_publish_probe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_bilibili_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_click_bilibili_publish_confirm_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_wait_publish_feedback", fake_wait)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    result = engine._fill_draft_once_generic(
        page=page,
        target=target,
        final_caption="caption",
        open_url=page.url,
        platform_name="bilibili",
        save_draft=False,
        publish_now=True,
        upload_timeout=600,
        draft_button_texts=("淇濆瓨鑽夌",),
        publish_button_texts=("绔嬪嵆鎶曠",),
    )

    assert result is page
    assert 600 in captured_timeouts


def test_fill_draft_once_generic_bilibili_selects_default_creative_statement(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    calls: list[tuple[str, str]] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://member.bilibili.com/platform/upload/video/frame"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_select(_primary_ctx, _fallback_ctx, creative_statement: str = "") -> None:
        calls.append(("creative_statement", creative_statement))

    def fake_partition(_primary_ctx, _fallback_ctx, partition_name: str = "") -> None:
        calls.append(("partition", partition_name))

    def fake_fill_title(*_args, **_kwargs) -> None:
        calls.append(("title", "filled"))

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_bilibili_upload_file_input", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_prepare_bilibili_cover_editor", lambda *_args, **_kwargs: Path(target))
    monkeypatch.setattr(engine, "_fill_bilibili_title_from_caption", fake_fill_title)
    monkeypatch.setattr(engine, "_select_bilibili_creative_statement", fake_select)
    monkeypatch.setattr(engine, "_select_bilibili_partition", fake_partition)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_reset_bilibili_publish_probe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_bilibili_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_wait_publish_feedback", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    result = engine._fill_draft_once_generic(
        page=page,
        target=target,
        final_caption="caption",
        open_url=page.url,
        platform_name="bilibili",
        save_draft=False,
        publish_now=True,
        upload_timeout=30,
        draft_button_texts=("淇濆瓨鑽夌",),
        publish_button_texts=("绔嬪嵆鎶曠",),
    )

    assert result is page
    assert calls == [
        ("creative_statement", "内容无需标注"),
        ("title", "filled"),
        ("partition", "科技数码"),
        ("creative_statement", "内容无需标注"),
    ]


def test_ensure_bilibili_cover_image_path_prefers_existing_auto_cover(tmp_path: Path) -> None:
    target = tmp_path / "vibe_10.mp4"
    target.write_bytes(b"video")
    raw_cover = tmp_path / "vibe_10_auto_cover_raw.png"
    raw_cover.write_bytes(b"raw")
    cover = tmp_path / "vibe_10_auto_cover.png"
    cover.write_bytes(b"cover")

    resolved = engine._ensure_bilibili_cover_image_path(target)

    assert resolved == cover


def test_ensure_bilibili_cover_image_path_builds_from_raw_cover(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "vibe_10.mp4"
    target.write_bytes(b"video")
    raw_cover = tmp_path / "vibe_10_auto_cover_raw.png"
    raw_cover.write_bytes(b"raw")
    built_cover = tmp_path / "vibe_10_auto_cover.png"
    calls: list[tuple[Path, Path, str]] = []

    def fake_decorate(source_path: Path, target_path: Path, title: str) -> None:
        calls.append((source_path, target_path, title))
        target_path.write_bytes(b"decorated")

    monkeypatch.setattr(engine, "_decorate_cover", fake_decorate)

    resolved = engine._ensure_bilibili_cover_image_path(target)

    assert resolved == built_cover
    assert built_cover.exists()
    assert calls == [(raw_cover, built_cover, "vibe_10")]


def test_prepare_bilibili_cover_editor_uploads_both_ratios_and_closes(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "vibe_10.mp4"
    target.write_bytes(b"video")
    cover = tmp_path / "vibe_10_auto_cover.png"
    cover.write_bytes(b"cover")

    click_calls: list[tuple[str, ...]] = []
    uploaded: list[tuple[str, str]] = []
    state = {"open": False}

    class FakeInput:
        def __init__(self, ratio: str) -> None:
            self.ratio = ratio

        def input(self, value: str) -> None:
            uploaded.append((self.ratio, Path(value).name))

    inputs = {
        "4_3": FakeInput("4_3"),
        "16_9": FakeInput("16_9"),
    }

    def fake_click_first(_primary_ctx, _fallback_ctx, texts, **_kwargs):
        click_calls.append(tuple(texts or ()))
        state["open"] = True
        return True

    def fake_state(*_args, **_kwargs):
        return {
            "open": bool(state["open"]),
            "submit_visible": bool(state["open"]),
            "dialog_text": "双比例同步改动" if state["open"] else "",
        }

    def fake_find(_primary_ctx, _fallback_ctx, ratio: str):
        return inputs[ratio]

    def fake_submit(_primary_ctx, _fallback_ctx):
        state["open"] = False
        return True

    monkeypatch.setattr(engine, "_ensure_bilibili_cover_image_path", lambda *_args, **_kwargs: cover)
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_read_bilibili_cover_editor_state", fake_state)
    monkeypatch.setattr(engine, "_find_bilibili_cover_editor_file_input", fake_find)
    monkeypatch.setattr(engine, "_click_bilibili_cover_editor_submit", fake_submit)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action, retries=3: action())
    monkeypatch.setattr(engine, "_read_file_input_binding_state", lambda file_input: {"count": 1, "visible": True, "names": [file_input.ratio]})
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_humanized_publish_retry_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._prepare_bilibili_cover_editor(SimpleNamespace(), SimpleNamespace(), SimpleNamespace(), target)

    assert result == cover
    assert uploaded == [("4_3", "vibe_10_auto_cover.png"), ("16_9", "vibe_10_auto_cover.png")]
    assert any("封面设置" in tuple_text for tuple_text in click_calls)


def test_prepare_bilibili_publish_dom_defaults_selects_creative_statement(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        engine,
        "_select_bilibili_creative_statement",
        lambda *args, **_kwargs: calls.append(args[2] if len(args) > 2 else ""),
    )

    engine._prepare_bilibili_publish_dom_defaults(object(), object())

    assert calls == ["内容无需标注"]


def test_normalize_bilibili_creative_statement_value_accepts_aliases() -> None:
    base = engine._normalize_bilibili_creative_statement_value("\u5185\u5bb9\u65e0\u9700\u6807\u6ce8")
    assert engine._normalize_bilibili_creative_statement_value("\u65e0\u9700\u6dfb\u52a0\u81ea\u4e3b\u58f0\u660e") == base
    assert engine._normalize_bilibili_creative_statement_value("\u65e0\u9700\u81ea\u4e3b\u58f0\u660e") == base
    assert engine._normalize_bilibili_creative_statement_value("\u65e0\u9700\u58f0\u660e") == base


def test_fill_bilibili_title_from_caption_uses_static_fallback_when_builder_empty(monkeypatch) -> None:
    class FakeCtx:
        def ele(self, _selector: str, timeout: float = 1.0):
            return None

        def run_js(self, _script: str, title: str):
            return {"state": "set", "value": title}

    monkeypatch.setattr(engine, "_build_bilibili_title_from_caption", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(engine, "_caption_verification_marker", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(engine, "_normalize_blocking_timeout", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(engine, "_humanized_publish_retry_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    title = engine._fill_bilibili_title_from_caption(FakeCtx(), FakeCtx(), "", timeout_seconds=2)

    assert title == "GasGx \u81ea\u52a8\u53d1\u5e03\u89c6\u9891"


def test_select_bilibili_partition_accepts_fallback_selection(monkeypatch) -> None:
    logs: list[str] = []

    class FakeCtx:
        def run_js(self, script: str, *_args):
            if "selected_fallback" in script:
                return {"state": "selected_fallback", "current": "动画", "option": "动画"}
            if "missing_label" in script:
                return {"hasField": True, "current": "动画", "source": "selection_text"}
            return True

    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_retry_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda message: logs.append(str(message)))

    engine._select_bilibili_partition(FakeCtx(), None, "科技数码")

    assert any("Partition fallback selected" in line for line in logs)


def test_fill_draft_once_generic_douyin_publish_unconfirmed_falls_back_to_draft(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    clicked_button_texts: list[tuple[str, ...]] = []
    self_statement_calls: list[str] = []
    collection_calls: list[str] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://creator.douyin.com/creator-micro/content/upload"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        clicked_button_texts.append(text_tuple)
        return text_tuple == ("SAVE_DRAFT",)

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_ensure_douyin_publish_mode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_upload_file_input_generic", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        engine,
        "_select_douyin_collection",
        lambda *args, **_kwargs: collection_calls.append(args[2] if len(args) > 2 else ""),
    )
    monkeypatch.setattr(
        engine,
        "_select_douyin_self_statement",
        lambda *args, **_kwargs: self_statement_calls.append(args[2] if len(args) > 2 else ""),
    )
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_douyin_primary_publish_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        engine,
        "_finalize_douyin_publish",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish feedback timeout")),
    )
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    with pytest.raises(RuntimeError, match="E_PUBLISH_UNCONFIRMED_DRAFT_SAVED"):
        engine._fill_draft_once_generic(
            page=page,
            target=target,
            final_caption="caption",
            open_url=page.url,
            platform_name="douyin",
            save_draft=False,
            publish_now=True,
            upload_timeout=30,
            draft_button_texts=("SAVE_DRAFT",),
            publish_button_texts=("PUBLISH_NOW",),
            collection_name="CyberCar",
    )

    assert ("SAVE_DRAFT",) in clicked_button_texts
    assert collection_calls == []
    assert self_statement_calls == []


def test_fill_draft_once_generic_douyin_uses_generic_publish_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    clicked_button_texts: list[tuple[str, ...]] = []
    finalize_calls = {"count": 0}

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://creator.douyin.com/creator-micro/content/upload"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        clicked_button_texts.append(text_tuple)
        return text_tuple == engine.DOUYIN_PUBLISH_BUTTON_TEXTS

    def fake_finalize(*_args, **_kwargs):
        finalize_calls["count"] += 1
        return "immediate"

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_ensure_douyin_publish_mode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_upload_file_input_generic", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_select_douyin_collection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_douyin_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_finalize_douyin_publish", fake_finalize)
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    result = engine._fill_draft_once_generic(
        page=page,
        target=target,
        final_caption="caption",
        open_url=page.url,
        platform_name="douyin",
        save_draft=False,
        publish_now=True,
        upload_timeout=30,
        draft_button_texts=("SAVE_DRAFT",),
        publish_button_texts=engine.DOUYIN_PUBLISH_BUTTON_TEXTS,
        collection_name="CyberCar",
    )

    assert result is page
    assert engine.DOUYIN_PUBLISH_BUTTON_TEXTS in clicked_button_texts
    assert finalize_calls["count"] == 1


def test_fill_draft_once_generic_xiaohongshu_publish_unconfirmed_manage_verified_treated_as_success(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    click_calls: list[tuple[str, ...]] = []
    publish_clicks = {"count": 0}

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://creator.xiaohongshu.com/publish/publish"

        def run_js(self, *_args, **_kwargs):
            return ""

        def get(self, _url: str) -> None:
            return None

    def fake_click_publish(*_args, **_kwargs) -> bool:
        publish_clicks["count"] += 1
        return publish_clicks["count"] == 1

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        click_calls.append(text_tuple)
        return False

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_activate_upload_trigger_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_find_upload_file_input_generic", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_is_image_file", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_xiaohongshu_title_from_caption", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: ["token-hit"])
    monkeypatch.setattr(engine, "_click_xiaohongshu_primary_publish_button", fake_click_publish)
    monkeypatch.setattr(engine, "_click_xiaohongshu_publish_confirm_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_wait_publish_feedback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish feedback timeout")),
    )
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_verify_platform_publish_in_manage_page", lambda *_args, **_kwargs: "token-hit")
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    result = engine._fill_draft_once_generic(
        page=page,
        target=target,
        final_caption="caption",
        open_url=page.url,
        platform_name="xiaohongshu",
        save_draft=False,
        publish_now=True,
        upload_timeout=30,
        draft_button_texts=("保存为草稿",),
        publish_button_texts=("发布笔记",),
    )

    assert result is page
    assert ("保存为草稿",) in click_calls


def test_fill_draft_once_generic_xiaohongshu_publish_unconfirmed_manage_verify_miss_keeps_coded_error(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    publish_clicks = {"count": 0}

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://creator.xiaohongshu.com/publish/publish"

        def run_js(self, *_args, **_kwargs):
            return ""

        def get(self, _url: str) -> None:
            return None

    def fake_click_publish(*_args, **_kwargs) -> bool:
        publish_clicks["count"] += 1
        return publish_clicks["count"] == 1

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_activate_upload_trigger_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_find_upload_file_input_generic", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_is_image_file", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_fill_xiaohongshu_title_from_caption", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: ["token-miss"])
    monkeypatch.setattr(engine, "_click_xiaohongshu_primary_publish_button", fake_click_publish)
    monkeypatch.setattr(engine, "_click_xiaohongshu_publish_confirm_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_wait_publish_feedback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish feedback timeout")),
    )
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_verify_platform_publish_in_manage_page", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    with pytest.raises(RuntimeError, match="E_PUBLISH_UNCONFIRMED_DRAFT_SAVED"):
        engine._fill_draft_once_generic(
            page=page,
            target=target,
            final_caption="caption",
            open_url=page.url,
            platform_name="xiaohongshu",
            save_draft=False,
            publish_now=True,
            upload_timeout=30,
            draft_button_texts=("保存为草稿",),
            publish_button_texts=("发布笔记",),
        )


def test_fill_draft_once_generic_tiktok_publish_button_missing_falls_back_to_draft(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    clicked_button_texts: list[tuple[str, ...]] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://www.tiktok.com/upload"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        clicked_button_texts.append(text_tuple)
        return text_tuple == ("SAVE_DRAFT",)

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_upload_file_input_generic", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_force_tiktok_caption", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    with pytest.raises(RuntimeError, match="E_PUBLISH_BUTTON_MISSING"):
        engine._fill_draft_once_generic(
            page=page,
            target=target,
            final_caption="caption",
            open_url=page.url,
            platform_name="tiktok",
            save_draft=False,
            publish_now=True,
            upload_timeout=30,
            draft_button_texts=("SAVE_DRAFT",),
            publish_button_texts=("POST_NOW",),
        )

    assert ("SAVE_DRAFT",) in clicked_button_texts


def test_fill_draft_x_passes_draft_button_texts_to_generic_uploader(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    captured: dict[str, object] = {}

    monkeypatch.setattr(engine, "_find_latest_processed", lambda workspace: target)
    monkeypatch.setattr(engine, "_is_image_file", lambda path: False)
    monkeypatch.setattr(engine, "_connect_chrome", lambda **kwargs: object())
    monkeypatch.setattr(engine, "_prepare_upload_tab", lambda page: object())
    monkeypatch.setattr(engine, "_close_work_tab", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_load_caption_for_video", lambda path: "Cybertruck")
    monkeypatch.setattr(
        engine,
        "_fill_draft_once_generic",
        lambda *args, **kwargs: captured.update(kwargs) or object(),
    )

    result = engine.fill_draft_x(
        workspace=SimpleNamespace(),
        target_video=target,
        publish_now=False,
        save_draft=False,
    )

    assert result == target
    assert tuple(captured.get("draft_button_texts") or ()) != ()


def test_fill_caption_generic_skips_hard_failure_for_x_and_tiktok(monkeypatch) -> None:
    class EmptyCtx:
        def ele(self, selector: str, timeout: float = 2.0):
            return None

    monkeypatch.setattr(engine, "_prepare_caption_for_platform", lambda caption, platform_name: caption)
    monkeypatch.setattr(engine, "_caption_verification_marker", lambda caption: "cybertruck")
    monkeypatch.setattr(engine, "_log", lambda message: None)

    engine._fill_caption_generic(EmptyCtx(), EmptyCtx(), "cybertruck", "x")
    engine._fill_caption_generic(EmptyCtx(), EmptyCtx(), "cybertruck", "tiktok")


def test_prepare_caption_for_platform_caps_x_to_non_premium_limit() -> None:
    raw = "a" * (engine.X_NON_PREMIUM_POST_CHAR_LIMIT + 25)
    prepared = engine._prepare_caption_for_platform(raw, "x")
    assert len(prepared) == engine.X_NON_PREMIUM_POST_CHAR_LIMIT


def test_prepare_caption_for_platform_dedupes_tiktok_repeated_segments() -> None:
    raw = "Cybertruck clip drop test.\nCybertruck clip drop test.\nCybertruck clip drop test."
    prepared = engine._prepare_caption_for_platform(raw, "tiktok")
    assert prepared.count("Cybertruck clip drop test.") == 1


def test_prepare_caption_for_platform_kuaishou_matches_other_platforms() -> None:
    raw = "GasGx caption with legacy brand tag #CyberCar"
    kuaishou = engine._prepare_caption_for_platform(raw, "kuaishou")
    douyin = engine._prepare_caption_for_platform(raw, "douyin")

    assert kuaishou == douyin


def test_collapse_repeated_caption_blocks_reduces_duplicate_paragraphs() -> None:
    raw = "Cybertruck is insane.\n\nCybertruck is insane.\n\nCybertruck is insane."
    collapsed = engine._collapse_repeated_caption_blocks(raw)
    assert collapsed == "Cybertruck is insane."


def test_force_x_non_premium_caption_prefers_first_successful_owner(monkeypatch) -> None:
    calls: list[tuple[object, str]] = []

    class Ctx:
        def __init__(self, name: str):
            self.name = name

        def run_js(self, script: str, value: str):
            calls.append((self.name, value))
            return {"state": "set", "value": value[:10], "over_limit": False}

    monkeypatch.setattr(engine, "_log", lambda message: None)
    ok = engine._force_x_non_premium_caption(Ctx("primary"), Ctx("fallback"), "cybertruck")

    assert ok is True
    assert len(calls) == 1
    assert calls[0][0] == "primary"


def test_force_x_non_premium_caption_rejects_empty_js_result(monkeypatch) -> None:
    class Ctx:
        def run_js(self, script: str, value: str):
            return {"state": "set", "value": "", "over_limit": False}

    monkeypatch.setattr(engine, "_log", lambda message: None)
    ok = engine._force_x_non_premium_caption(Ctx(), None, "cybertruck")
    assert ok is False


def test_force_tiktok_caption_prefers_first_successful_owner(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    class Ctx:
        def __init__(self, name: str):
            self.name = name

        def run_js(self, script: str, value: str):
            calls.append((self.name, value))
            return {"state": "set", "value": value, "repeated": False}

    monkeypatch.setattr(engine, "_log", lambda message: None)
    ok = engine._force_tiktok_caption(
        Ctx("primary"),
        Ctx("fallback"),
        "Cybertruck clip\n\nCybertruck clip",
    )

    assert ok is True
    assert len(calls) == 1
    assert calls[0][0] == "primary"
    assert calls[0][1].count("Cybertruck clip") == 1


def test_detect_x_publish_via_network_success(monkeypatch) -> None:
    monkeypatch.setattr(
        engine,
        "_read_x_publish_probe",
        lambda ctx: [
            {
                "method": "POST",
                "url": "https://x.com/i/api/graphql/abc/CreateTweet",
                "status": 200,
                "body": '{"data":{"create_tweet":{"tweet_results":{"result":{"rest_id":"123"}}}}}',
            }
        ],
    )
    ok, reason = engine._detect_x_publish_via_network(object())
    assert ok is True
    assert "network success marker" in reason


def test_detect_x_publish_via_network_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        engine,
        "_read_x_publish_probe",
        lambda ctx: [
            {
                "method": "POST",
                "url": "https://x.com/i/api/graphql/abc/CreateTweet",
                "status": 200,
                "body": '{"errors":[{"message":"rate limit"}]}',
            }
        ],
    )
    ok, reason = engine._detect_x_publish_via_network(object())
    assert ok is False
    assert "network failure marker" in reason


def test_read_x_publish_composer_state_uses_fallback_owner() -> None:
    class BrokenCtx:
        def run_js(self, script: str):
            raise RuntimeError("boom")

    class GoodCtx:
        def run_js(self, script: str):
            return {
                "url": "https://x.com/compose/post",
                "editor_found": True,
                "caption_len": 22,
                "caption_preview": "Cybertruck test caption",
                "button_found": True,
                "button_disabled": False,
                "button_text": "Post",
                "over_limit": False,
                "over_by": 0,
                "compose_url": True,
            }

    state = engine._read_x_publish_composer_state(BrokenCtx(), GoodCtx())
    assert state["editor_found"] is True
    assert state["button_found"] is True
    assert state["button_disabled"] is False
    assert state["caption_len"] == 22
    assert state["compose_url"] is True


def test_wait_publish_feedback_x_over_limit_recovers_once_without_blind_click(monkeypatch) -> None:
    calls = {"force_caption": 0, "x_click": 0, "generic_click": 0, "net": 0}

    def fake_net_probe(ctx):
        calls["net"] += 1
        if calls["net"] >= 2:
            return True, "network success marker"
        return False, ""

    monkeypatch.setattr(engine, "_detect_x_publish_via_network", fake_net_probe)
    monkeypatch.setattr(
        engine,
        "_read_page_snapshot",
        lambda primary, fallback: ("https://x.com/compose/post", "你超出了 508 的字符数限制 -508"),
    )
    monkeypatch.setattr(
        engine,
        "_read_x_publish_composer_state",
        lambda primary, fallback: {
            "button_found": True,
            "button_disabled": True,
            "over_limit": True,
            "over_by": 508,
            "caption_len": 36,
            "button_text": "发帖",
        },
    )
    monkeypatch.setattr(
        engine,
        "_force_x_non_premium_caption",
        lambda primary, fallback, caption: calls.__setitem__("force_caption", calls["force_caption"] + 1),
    )
    monkeypatch.setattr(
        engine,
        "_click_x_primary_publish_button",
        lambda primary, fallback: calls.__setitem__("x_click", calls["x_click"] + 1) or True,
    )
    monkeypatch.setattr(
        engine,
        "_click_first_matching_button",
        lambda *args, **kwargs: calls.__setitem__("generic_click", calls["generic_click"] + 1) or True,
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda primary, fallback: [])
    monkeypatch.setattr(engine, "_log", lambda message: None)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: None)

    engine._wait_publish_feedback(object(), object(), platform_name="x", timeout_seconds=8)

    assert calls["force_caption"] == 1
    assert calls["x_click"] == 0
    assert calls["generic_click"] == 0


def test_wait_publish_feedback_x_empty_caption_recovery_skips_publish_click(monkeypatch) -> None:
    calls = {"force_caption": 0, "x_click": 0}
    timeline = {"now": 0.0}

    def fake_time() -> float:
        return timeline["now"]

    def fake_sleep(seconds: float) -> None:
        timeline["now"] += float(seconds)

    monkeypatch.setattr(engine.time, "time", fake_time)
    monkeypatch.setattr(engine.time, "sleep", fake_sleep)
    monkeypatch.setattr(engine, "_detect_x_publish_via_network", lambda ctx: (False, ""))
    monkeypatch.setattr(engine, "_read_page_snapshot", lambda primary, fallback: ("https://x.com/compose/post", ""))
    monkeypatch.setattr(
        engine,
        "_read_x_publish_composer_state",
        lambda primary, fallback: {
            "url": "https://x.com/compose/post",
            "button_found": True,
            "button_disabled": False,
            "over_limit": False,
            "caption_len": 0,
            "button_text": "Post",
            "has_post_success_toast": False,
        },
    )
    monkeypatch.setattr(
        engine,
        "_force_x_non_premium_caption",
        lambda primary, fallback, caption: calls.__setitem__("force_caption", calls["force_caption"] + 1) or True,
    )
    monkeypatch.setattr(
        engine,
        "_click_x_primary_publish_button",
        lambda primary, fallback: calls.__setitem__("x_click", calls["x_click"] + 1) or True,
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda primary, fallback: [])
    monkeypatch.setattr(engine, "_log", lambda message: None)

    with pytest.raises(RuntimeError, match="E_PUBLISH_UNCONFIRMED"):
        engine._wait_publish_feedback(object(), object(), platform_name="x", timeout_seconds=8)

    assert calls["force_caption"] == 1
    assert calls["x_click"] == 0


def test_wait_publish_feedback_x_clicks_when_button_ready(monkeypatch) -> None:
    calls = {"x_click": 0}
    monkeypatch.setattr(engine, "_detect_x_publish_via_network", lambda ctx: (False, ""))
    monkeypatch.setattr(engine, "_read_page_snapshot", lambda primary, fallback: ("https://x.com/home", ""))
    monkeypatch.setattr(
        engine,
        "_read_x_publish_composer_state",
        lambda primary, fallback: {
            "button_found": True,
            "button_disabled": False,
            "over_limit": False,
            "caption_len": 32,
            "button_text": "Post",
        },
    )
    monkeypatch.setattr(
        engine,
        "_click_x_primary_publish_button",
        lambda primary, fallback: calls.__setitem__("x_click", calls["x_click"] + 1) or True,
    )
    monkeypatch.setattr(engine, "_log", lambda message: None)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: None)

    engine._wait_publish_feedback(object(), object(), platform_name="x", timeout_seconds=8)
    assert calls["x_click"] == 1


def test_wait_publish_feedback_x_accepts_status_page_when_snapshot_stays_compose(monkeypatch) -> None:
    calls = {"x_click": 0}
    monkeypatch.setattr(engine, "_detect_x_publish_via_network", lambda ctx: (False, ""))
    monkeypatch.setattr(engine, "_read_page_snapshot", lambda primary, fallback: ("https://x.com/compose/post", ""))
    monkeypatch.setattr(
        engine,
        "_read_x_publish_composer_state",
        lambda primary, fallback: {
            "url": "https://x.com/CyberCar_/status/2038995656069861586",
            "button_found": True,
            "button_disabled": True,
            "over_limit": False,
            "caption_len": 0,
            "button_text": "发帖",
            "has_post_success_toast": False,
        },
    )
    monkeypatch.setattr(
        engine,
        "_click_x_primary_publish_button",
        lambda primary, fallback: calls.__setitem__("x_click", calls["x_click"] + 1) or True,
    )
    monkeypatch.setattr(engine, "_log", lambda message: None)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: None)

    engine._wait_publish_feedback(object(), object(), platform_name="x", timeout_seconds=8)
    assert calls["x_click"] == 0


def test_wait_publish_feedback_x_accepts_home_page_with_cleared_caption(monkeypatch) -> None:
    calls = {"x_click": 0}
    monkeypatch.setattr(engine, "_detect_x_publish_via_network", lambda ctx: (False, ""))
    monkeypatch.setattr(engine, "_read_page_snapshot", lambda primary, fallback: ("https://x.com/compose/post", ""))
    monkeypatch.setattr(
        engine,
        "_read_x_publish_composer_state",
        lambda primary, fallback: {
            "url": "https://x.com/home",
            "button_found": True,
            "button_disabled": True,
            "over_limit": False,
            "caption_len": 0,
            "button_text": "发帖",
            "has_post_success_toast": False,
        },
    )
    monkeypatch.setattr(
        engine,
        "_click_x_primary_publish_button",
        lambda primary, fallback: calls.__setitem__("x_click", calls["x_click"] + 1) or True,
    )
    monkeypatch.setattr(engine, "_log", lambda message: None)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: None)

    engine._wait_publish_feedback(object(), object(), platform_name="x", timeout_seconds=8)
    assert calls["x_click"] == 0


def test_read_page_snapshot_ignores_other_browser_tabs() -> None:
    class ForeignTab:
        def run_js(self, _script: str):
            return {
                "url": "https://creator.xiaohongshu.com/login?source=&redirectReason=401",
                "text": "小红书登录页",
            }

    class BrowserWithForeignTab:
        def get_tabs(self):
            return [ForeignTab()]

    class FakeCtx:
        def __init__(self, url: str, text: str) -> None:
            self._url = url
            self._text = text
            self.browser = BrowserWithForeignTab()

        def run_js(self, _script: str):
            return {"url": self._url, "text": self._text}

        def get_frames(self, timeout: float = 0):
            del timeout
            return []

    primary = FakeCtx("https://creator.douyin.com/creator-micro/content/upload", "抖音发布页")
    fallback = FakeCtx("https://creator.douyin.com/creator-micro/content/upload", "抖音发布页")

    url, text = engine._read_page_snapshot(primary, fallback)

    assert url.startswith("https://creator.douyin.com/")
    assert "抖音发布页" in text
    assert "小红书登录页" not in text


def test_wait_publish_feedback_kuaishou_extends_window_when_progress_active(monkeypatch) -> None:
    timeline = {"now": 0.0, "calls": 0}

    def fake_time() -> float:
        return timeline["now"]

    def fake_sleep(seconds: float) -> None:
        timeline["now"] += float(seconds)

    def fake_snapshot(*_args, **_kwargs):
        timeline["calls"] += 1
        if timeline["calls"] < 10:
            return ("https://cp.kuaishou.com/article/publish/video", "处理中 正在排队")
        return ("https://cp.kuaishou.com/article/publish/video", "发布成功")

    monkeypatch.setattr(engine.time, "time", fake_time)
    monkeypatch.setattr(engine.time, "sleep", fake_sleep)
    monkeypatch.setattr(engine, "_read_page_snapshot", fake_snapshot)
    monkeypatch.setattr(engine, "_click_kuaishou_publish_confirm_dialog_only", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_is_kuaishou_publish_confirmed_by_heuristic", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_ensure_kuaishou_not_in_unfinished_edit_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._wait_publish_feedback(object(), object(), platform_name="kuaishou", timeout_seconds=8)
    assert timeline["calls"] >= 10


def test_click_kuaishou_primary_publish_button_uses_bottom_publish_div_selector(monkeypatch) -> None:
    clicks: list[str] = []

    class FakeBtn:
        def __init__(self, name: str) -> None:
            self.name = name

        def run_js(self, _script: str, *_args, **_kwargs):
            return {
                "wrap": "发布 取消",
                "texts": ["发布 取消", "发布", "取消"],
                "rectTop": 1189,
                "viewportHeight": 1280,
                "buttonText": "发布",
                "submitText": "",
                "saveDisabled": "",
                "publishFlag": "",
                "tagName": "div",
                "cls": "_button_3a3lq_1 _button-primary_3a3lq_60",
            }

        def click(self, *_args, **_kwargs) -> None:
            clicks.append(self.name)

    class FakeCtx:
        def ele(self, selector: str, *_args, **_kwargs):
            if "contains(@class,'_button-primary')" in selector and "normalize-space(.)='发布'" in selector:
                return FakeBtn("publish")
            return None

        def run_js(self, *_args, **_kwargs):
            raise AssertionError("JS fallback should not run when the bottom publish div selector matches")

    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_kuaishou_primary_publish_button(FakeCtx(), FakeCtx()) is True
    assert clicks == ["publish"]


def test_detect_kuaishou_publish_via_network_success(monkeypatch) -> None:
    monkeypatch.setattr(
        engine,
        "_read_kuaishou_publish_probe",
        lambda ctx: [
            {
                "method": "POST",
                "url": "https://cp.kuaishou.com/rest/cp/works/v2/video/pc/submit?foo=1",
                "status": 200,
                "body": "{\"code\":0,\"message\":\"ok\"}",
            }
        ],
    )

    ok, reason = engine._detect_kuaishou_publish_via_network(object())

    assert ok is True
    assert "submit request succeeded" in reason


def test_detect_kuaishou_publish_via_network_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        engine,
        "_read_kuaishou_publish_probe",
        lambda ctx: [
            {
                "method": "POST",
                "url": "https://cp.kuaishou.com/rest/cp/works/v2/video/pc/submit?foo=1",
                "status": 200,
                "body": "{\"code\":-1,\"message\":\"fail\"}",
            }
        ],
    )

    ok, reason = engine._detect_kuaishou_publish_via_network(object())

    assert ok is False
    assert "network failure marker" in reason


def test_detect_kuaishou_publish_via_network_uses_performance_resources(monkeypatch) -> None:
    monkeypatch.setattr(engine, "_read_kuaishou_publish_probe", lambda ctx: [])
    monkeypatch.setattr(
        engine,
        "_read_kuaishou_publish_resource_entries",
        lambda ctx: [
            {
                "name": "https://cp.kuaishou.com/rest/cp/works/v2/video/pc/submit?foo=1",
                "initiatorType": "fetch",
            }
        ],
    )

    ok, reason = engine._detect_kuaishou_publish_via_network(object())

    assert ok is True
    assert "performance resource submit observed" in reason


def test_wait_publish_feedback_kuaishou_accepts_network_submit(monkeypatch) -> None:
    calls = {"net": 0, "dialog": 0}
    timeline = {"now": 0.0, "calls": 0}

    def fake_time() -> float:
        return timeline["now"]

    def fake_sleep(seconds: float) -> None:
        timeline["now"] += float(seconds)

    def fake_snapshot(*_args, **_kwargs):
        timeline["calls"] += 1
        return ("https://cp.kuaishou.com/article/publish/video", "")

    def fake_net_probe(ctx):
        del ctx
        calls["net"] += 1
        if calls["net"] >= 2:
            return True, "submit request succeeded: status=200, url=https://cp.kuaishou.com/rest/cp/works/v2/video/pc/submit"
        return False, ""

    monkeypatch.setattr(engine.time, "time", fake_time)
    monkeypatch.setattr(engine.time, "sleep", fake_sleep)
    monkeypatch.setattr(engine, "_read_page_snapshot", fake_snapshot)
    monkeypatch.setattr(
        engine,
        "_click_kuaishou_publish_confirm_dialog_only",
        lambda *_args, **_kwargs: calls.__setitem__("dialog", calls["dialog"] + 1) or False,
    )
    monkeypatch.setattr(engine, "_detect_kuaishou_publish_via_network", fake_net_probe)
    monkeypatch.setattr(engine, "_is_kuaishou_publish_confirmed_by_heuristic", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_ensure_kuaishou_not_in_unfinished_edit_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._wait_publish_feedback(object(), object(), platform_name="kuaishou", timeout_seconds=8)

    assert calls["net"] >= 2
    assert calls["dialog"] >= 1
    assert timeline["calls"] >= 1


def test_wait_publish_feedback_bilibili_accepts_delivery_success_marker(monkeypatch) -> None:
    timeline = {"now": 0.0, "calls": 0}

    def fake_time() -> float:
        return timeline["now"]

    def fake_sleep(seconds: float) -> None:
        timeline["now"] += float(seconds)

    def fake_snapshot(*_args, **_kwargs):
        timeline["calls"] += 1
        if timeline["calls"] < 3:
            return ("https://member.bilibili.com/platform/upload/video/frame", "上传中")
        return ("https://member.bilibili.com/platform/upload/video/frame", "投递成功 查看稿件 再投一条")

    monkeypatch.setattr(engine.time, "time", fake_time)
    monkeypatch.setattr(engine.time, "sleep", fake_sleep)
    monkeypatch.setattr(engine, "_read_page_snapshot", fake_snapshot)
    monkeypatch.setattr(engine, "_click_bilibili_publish_confirm_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_retry_bilibili_publish_if_still_editing", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._wait_publish_feedback(object(), object(), platform_name="bilibili", timeout_seconds=8)
    assert timeline["calls"] >= 3


def test_click_bilibili_publish_confirm_button_prefers_dialog_confirm_over_nav_posting(monkeypatch) -> None:
    clicks: list[str] = []

    class FakeBtn:
        def __init__(self, name: str) -> None:
            self.name = name

        def run_js(self, script: str, *_args, **_kwargs):
            if "getBoundingClientRect" in script:
                return True
            if "aria-disabled" in script or "disabled" in script:
                return False
            if "resolveClickTarget" in script:
                return {
                    "tag": "DIV",
                    "cls": "submit-add",
                    "text": "立即投稿",
                    "top": 1180,
                    "left": 560,
                    "width": 120,
                    "height": 40,
                }
            return True

        def click(self, *_args, **_kwargs) -> None:
            clicks.append(self.name)

    class FakeCtx:
        def ele(self, selector: str, *_args, **_kwargs):
            if "submit-add" in selector or "立即投稿" in selector:
                return FakeBtn("confirm")
            if selector == "text:确认投稿":
                return FakeBtn("matched")
            if selector == "text:投稿":
                return FakeBtn("nav")
            return None

        def run_js(self, *_args, **_kwargs):
            raise AssertionError("JS fallback should not run when a confirm selector is available")

    monkeypatch.setattr(engine, "_ensure_bilibili_publish_agreement_checked", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_select_bilibili_creative_statement", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_bilibili_creative_statement_prompt_go_declare", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_bilibili_publish_confirm_button(FakeCtx(), FakeCtx()) is True
    assert clicks == ["confirm"]


def test_click_bilibili_primary_publish_button_uses_ancestor_target(monkeypatch) -> None:
    clicks: list[str] = []

    class FakeBtn:
        def __init__(self, name: str) -> None:
            self.name = name

        def run_js(self, script: str, *_args, **_kwargs):
            if "getBoundingClientRect" in script:
                return True
            if "aria-disabled" in script or "disabled" in script:
                return False
            if "resolveClickTarget" in script:
                return {
                    "tag": "DIV",
                    "cls": "submit-add",
                    "text": "立即投稿",
                    "top": 1180,
                    "left": 560,
                    "width": 120,
                    "height": 40,
                }
            return True

        def click(self, *_args, **_kwargs) -> None:
            clicks.append(self.name)

    class FakeCtx:
        def ele(self, selector: str, *_args, **_kwargs):
            if "submit-add" in selector or "立即投稿" in selector:
                return FakeBtn("ancestor")
            if "立即投稿" in selector or "投稿" in selector:
                return FakeBtn("matched")
            return None

        def run_js(self, *_args, **_kwargs):
            raise AssertionError("JS fallback should not run when selector click succeeds")

    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_bilibili_primary_publish_button(FakeCtx(), FakeCtx()) is True
    assert clicks == ["ancestor"]


def test_click_bilibili_publish_confirm_button_uses_ancestor_target(monkeypatch) -> None:
    clicks: list[str] = []

    class FakeBtn:
        def __init__(self, name: str) -> None:
            self.name = name

        def run_js(self, script: str, *_args, **_kwargs):
            if "getBoundingClientRect" in script:
                return True
            if "aria-disabled" in script or "disabled" in script:
                return False
            if "resolveClickTarget" in script:
                return {
                    "tag": "DIV",
                    "cls": "submit-add",
                    "text": "立即投稿",
                    "top": 1180,
                    "left": 560,
                    "width": 120,
                    "height": 40,
                }
            return True

        def click(self, *_args, **_kwargs) -> None:
            clicks.append(self.name)

    class FakeCtx:
        def ele(self, selector: str, *_args, **_kwargs):
            if "submit-add" in selector or "立即投稿" in selector:
                return FakeBtn("ancestor")
            if "立即投稿" in selector or "投稿" in selector:
                return FakeBtn("matched")
            return None

        def run_js(self, *_args, **_kwargs):
            raise AssertionError("JS fallback should not run when selector click succeeds")

    monkeypatch.setattr(engine, "_ensure_bilibili_publish_agreement_checked", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_select_bilibili_creative_statement", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_bilibili_creative_statement_prompt_go_declare", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_bilibili_publish_confirm_button(FakeCtx(), FakeCtx()) is True
    assert clicks == ["ancestor"]


def test_click_bilibili_publish_confirm_button_js_fallback_ignores_nav_posting(monkeypatch) -> None:
    seen_scripts: list[str] = []

    class FakeCtx:
        def ele(self, *_args, **_kwargs):
            return None

        def run_js(self, script: str, *_args, **_kwargs):
            seen_scripts.append(script)
            assert "isNavLike" in script
            assert "[role=\"dialog\"]" in script or "dialog" in script
            return "投稿"

    monkeypatch.setattr(engine, "_ensure_bilibili_publish_agreement_checked", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_select_bilibili_creative_statement", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_bilibili_creative_statement_prompt_go_declare", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_bilibili_publish_confirm_button(FakeCtx(), FakeCtx()) is True
    assert seen_scripts


def test_click_bilibili_publish_confirm_button_handles_creative_statement_prompt(monkeypatch) -> None:
    calls = {"select": 0}

    class FakeCtx:
        def run_js(self, script: str, *_args, **_kwargs):
            if "去声明" in script and "dialog" in script:
                return {"state": "clicked", "text": "去声明"}
            return False

    monkeypatch.setattr(engine, "_ensure_bilibili_publish_agreement_checked", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        engine,
        "_select_bilibili_creative_statement",
        lambda *_args, **_kwargs: calls.__setitem__("select", calls["select"] + 1),
    )
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_bilibili_publish_confirm_button(FakeCtx(), FakeCtx()) is False
    assert calls["select"] == 2


def test_click_bilibili_publish_confirm_button_prefers_creative_statement_before_publish(monkeypatch) -> None:
    order: list[str] = []

    class FakeCtx:
        def ele(self, *_args, **_kwargs):
            return None

        def run_js(self, script: str, *_args, **_kwargs):
            if "确认投稿" in script or "确认发布" in script or "继续投稿" in script:
                order.append("publish")
                return True
            return False

    monkeypatch.setattr(engine, "_ensure_bilibili_publish_agreement_checked", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        engine,
        "_select_bilibili_creative_statement",
        lambda *_args, **_kwargs: order.append("select"),
    )
    monkeypatch.setattr(engine, "_click_bilibili_creative_statement_prompt_go_declare", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_bilibili_publish_confirm_button(FakeCtx(), FakeCtx()) is True
    assert order[:2] == ["select", "publish"]


def test_click_bilibili_publish_confirm_button_reclicks_primary_after_creative_statement(monkeypatch) -> None:
    order: list[str] = []

    class FakeCtx:
        pass

    monkeypatch.setattr(engine, "_ensure_bilibili_publish_agreement_checked", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        engine,
        "_select_bilibili_creative_statement",
        lambda *_args, **_kwargs: order.append("select"),
    )
    monkeypatch.setattr(engine, "_click_bilibili_creative_statement_prompt_go_declare", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_click_bilibili_primary_publish_button",
        lambda *_args, **_kwargs: order.append("primary") or True,
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: ["立即投稿"])
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    assert engine._click_bilibili_publish_confirm_button(FakeCtx(), FakeCtx()) is True
    assert order == ["select", "primary"]


def test_select_bilibili_creative_statement_keeps_working_when_already_selected(monkeypatch) -> None:
    calls: list[str] = []

    class FakeCtx:
        def run_js(self, script: str, *_args, **_kwargs):
            calls.append(script)
            return {"state": "already"}

    monkeypatch.setattr(
        engine,
        "_read_bilibili_creative_statement_state",
        lambda *_args, **_kwargs: {"hasField": True, "current": "内容无需标注"},
    )
    monkeypatch.setattr(engine, "_click_bilibili_creative_statement_prompt_go_declare", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_humanized_publish_settle_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_retry_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._select_bilibili_creative_statement(FakeCtx(), FakeCtx(), "内容无需标注")
    assert calls
    assert any("already_confirmed" in script for script in calls)


def test_is_bilibili_publish_success_snapshot_accepts_view_and_republish_pair() -> None:
    assert (
        engine._is_bilibili_publish_success_snapshot(
            "https://member.bilibili.com/platform/upload/video/frame",
            "查看稿件",
            actions=["再投一条"],
        )
        is True
    )


def test_retry_bilibili_publish_if_still_editing_skips_retry_when_delivery_success(monkeypatch) -> None:
    monkeypatch.setattr(
        engine,
        "_read_page_snapshot",
        lambda *_args, **_kwargs: (
            "https://member.bilibili.com/platform/upload/video/frame",
            "投递成功 查看稿件 再投一条",
        ),
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: ["查看稿件", "再投一条"])
    monkeypatch.setattr(
        engine,
        "_click_bilibili_primary_publish_button",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not retry click after success")),
    )
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)

    assert engine._retry_bilibili_publish_if_still_editing(object(), object()) is False


def test_fill_draft_once_generic_kuaishou_publish_unconfirmed_falls_back_to_draft(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    clicked_button_texts: list[tuple[str, ...]] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://cp.kuaishou.com/article/publish/video"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        clicked_button_texts.append(text_tuple)
        return text_tuple == ("保存草稿",)

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_ensure_kuaishou_publish_mode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_kuaishou_upload_file_input", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_scroll_kuaishou_publish_controls_into_view", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_primary_publish_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_click_kuaishou_publish_confirm_dialog_only", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(
        engine,
        "_wait_publish_feedback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish feedback timeout")),
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    with pytest.raises(RuntimeError, match="E_PUBLISH_UNCONFIRMED_DRAFT_SAVED"):
        engine._fill_draft_once_generic(
            page=page,
            target=target,
            final_caption="caption",
            open_url=page.url,
            platform_name="kuaishou",
            save_draft=False,
            publish_now=True,
            upload_timeout=30,
            draft_button_texts=("保存草稿",),
            publish_button_texts=("发布作品",),
        )

    assert ("保存草稿",) in clicked_button_texts


def test_fill_draft_once_generic_kuaishou_publish_button_missing_uses_delayed_feedback_wait(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")
    wait_calls: list[dict[str, object]] = []
    clicked_button_texts: list[tuple[str, ...]] = []

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://cp.kuaishou.com/article/publish/video"

        def run_js(self, *_args, **_kwargs):
            return ""

    def fake_click_first(_ctx, _page, texts, **_kwargs):
        text_tuple = tuple(texts or ())
        clicked_button_texts.append(text_tuple)
        return text_tuple == ("保存草稿",)

    def fake_wait(*_args, **kwargs):
        wait_calls.append(dict(kwargs))
        raise RuntimeError("not confirmed")

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_ensure_kuaishou_publish_mode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_kuaishou_upload_file_input", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_scroll_kuaishou_publish_controls_into_view", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_publish_confirm_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", fake_click_first)
    monkeypatch.setattr(engine, "_wait_publish_feedback", fake_wait)
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    with pytest.raises(RuntimeError, match="E_PUBLISH_UNCONFIRMED_DRAFT_SAVED"):
        engine._fill_draft_once_generic(
            page=page,
            target=target,
            final_caption="caption",
            open_url=page.url,
            platform_name="kuaishou",
            save_draft=False,
            publish_now=True,
            upload_timeout=30,
            draft_button_texts=("保存草稿",),
            publish_button_texts=("发布作品",),
        )

    assert ("保存草稿",) in clicked_button_texts
    assert wait_calls
    assert wait_calls[0].get("platform_name") == "kuaishou"


def test_fill_draft_once_generic_kuaishou_publish_button_missing_raises_coded_error_when_draft_fallback_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"video")

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakePage:
        def __init__(self) -> None:
            self.url = "https://cp.kuaishou.com/article/publish/video"

        def run_js(self, *_args, **_kwargs):
            return ""

    monkeypatch.setattr(engine, "_current_page_matches_publish_entry", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_check_platform_login_ready", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_resolve_post_editor_context", lambda page, **_kwargs: page)
    monkeypatch.setattr(engine, "_ensure_kuaishou_publish_mode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_find_kuaishou_upload_file_input", lambda *_args, **_kwargs: FakeInput())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_scroll_kuaishou_publish_controls_into_view", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_primary_publish_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_kuaishou_publish_confirm_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_wait_publish_feedback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("not confirmed")),
    )
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: ["发布作品", "保存草稿"])
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    page = FakePage()
    with pytest.raises(RuntimeError, match="E_PUBLISH_UNCONFIRMED_DRAFT_SAVED") as exc_info:
        engine._fill_draft_once_generic(
            page=page,
            target=target,
            final_caption="caption",
            open_url=page.url,
            platform_name="kuaishou",
            save_draft=False,
            publish_now=True,
            upload_timeout=30,
            draft_button_texts=("保存草稿",),
            publish_button_texts=("发布作品",),
        )
    assert "draft fallback attempt failed" in str(exc_info.value)
    assert "Failed to locate publish button" not in str(exc_info.value)


def test_wait_upload_ready_generic_douyin_accepts_relaxed_editor_readiness(monkeypatch) -> None:
    class FakeCtx:
        def run_js(self, _script: str):
            return ""

    ctx = FakeCtx()
    page = FakeCtx()

    monkeypatch.setattr(
        engine,
        "_read_douyin_video_upload_state",
        lambda *_args, **_kwargs: {
            "ready": False,
            "busy": False,
            "upload_entry": False,
            "caption_input": False,
            "title_input": False,
            "editor_hints": True,
            "publish_btn": True,
            "schedule_toggle": False,
            "sample_texts": [],
        },
    )
    monkeypatch.setattr(engine, "_is_image_file", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    result = engine._wait_upload_ready_generic(
        page,
        ctx,
        platform_name="douyin",
        timeout_seconds=30,
        upload_target=Path("sample.mp4"),
    )

    assert result is ctx


def test_find_upload_file_input_generic_prefers_hidden_image_input_over_visible_video_input() -> None:
    class FakeInput:
        def __init__(self, accept: str, visible: bool) -> None:
            self._accept = accept
            self._visible = visible

        def attr(self, name: str):
            if name == "accept":
                return self._accept
            return ""

        def run_js(self, script: str):
            if "getComputedStyle(this)" in script:
                return self._visible
            if "getAttribute('accept')" in script:
                return self._accept
            return ""

    class FakeOwner:
        def __init__(self, image_input: FakeInput, video_input: FakeInput) -> None:
            self.image_input = image_input
            self.video_input = video_input

        def ele(self, selector: str, timeout: float = 0):
            del timeout
            if "contains(translate(@accept,'IMAGE','image'),'image')" in selector or "contains(@accept,'image')" in selector:
                return self.image_input
            if "contains(translate(@accept,'VIDEO','video'),'video')" in selector or "contains(@accept,'video')" in selector:
                return self.video_input
            return None

        def get_frames(self, timeout: float = 0):
            del timeout
            return []

    image_input = FakeInput("image/png,image/jpeg", visible=False)
    video_input = FakeInput("video/*,.mp4", visible=True)
    owner = FakeOwner(image_input, video_input)

    selected = engine._find_upload_file_input_generic(owner, None, prefer_video=False)

    assert selected is image_input


def test_fill_caption_generic_xiaohongshu_allows_best_effort_when_editor_is_publish_ready(
    monkeypatch,
) -> None:
    class EmptyCtx:
        def ele(self, selector: str, timeout: float = 2.0):
            del selector, timeout
            return None

    monkeypatch.setattr(engine, "_prepare_caption_for_platform", lambda caption, platform_name: caption)
    monkeypatch.setattr(engine, "_caption_verification_marker", lambda caption: "cybertruck")
    monkeypatch.setattr(engine, "_force_xiaohongshu_caption", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        engine,
        "_read_xiaohongshu_publish_state",
        lambda *_args, **_kwargs: {
            "media_count": 1,
            "publish_entry": True,
            "has_publish_action": True,
        },
    )
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._fill_caption_generic(EmptyCtx(), EmptyCtx(), "cybertruck", "xiaohongshu")
