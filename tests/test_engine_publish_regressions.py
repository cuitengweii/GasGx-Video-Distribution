from __future__ import annotations

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


def test_fill_draft_once_generic_douyin_publish_unconfirmed_falls_back_to_draft(
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
    monkeypatch.setattr(engine, "_select_douyin_collection", lambda *_args, **_kwargs: None)
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
    with pytest.raises(RuntimeError, match="automatically saved as draft"):
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
    with pytest.raises(RuntimeError, match="automatically saved as draft"):
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
    with pytest.raises(RuntimeError, match="automatically saved as draft"):
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
