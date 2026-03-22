from __future__ import annotations

from pathlib import Path

from cybercar.services.engagement import runtime


class _FakeWorkspace:
    def __init__(self, root: Path) -> None:
        self.root = root


def test_markdown_path_uses_platform_specific_filename(tmp_path: Path) -> None:
    workspace = _FakeWorkspace(tmp_path / "runtime")

    assert runtime._markdown_path(workspace, "douyin").name == "douyin_comment_reply_records.md"
    assert runtime._markdown_path(workspace, "kuaishou").name == "kuaishou_comment_reply_records.md"


def test_wait_kuaishou_reply_confirm_only_accepts_reply_near_current_comment(monkeypatch) -> None:
    calls: list[tuple[int, str]] = []

    def fake_run_js_dict(_page, _script, comment_index, reply_text):
        calls.append((int(comment_index), str(reply_text)))
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: bool(predicate()))

    assert runtime._wait_kuaishou_reply_confirm(object(), 2, "ok") is True
    assert calls == [(2, "ok")]


def test_wait_douyin_reply_confirm_only_accepts_reply_near_current_comment(monkeypatch) -> None:
    calls: list[tuple[int, str]] = []

    def fake_run_js_dict(_page, _script, comment_index, reply_text):
        calls.append((int(comment_index), str(reply_text)))
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: bool(predicate()))

    assert runtime._wait_douyin_reply_confirm(object(), 3, "ok") is True
    assert calls == [(3, "ok")]


def test_submit_kuaishou_reply_v3_uses_comment_index_and_reply_text(monkeypatch) -> None:
    calls: list[tuple[object, tuple[object, ...]]] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        calls.append((script, args))
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_kuaishou_reply_v3(object(), 3, "ok") is True
    assert any(args == (3,) for _script, args in calls)
    assert any(args == (3, "ok") for _script, args in calls)
    assert wait_calls


def test_submit_kuaishou_reply_v4_uses_staged_confirm_flow(monkeypatch) -> None:
    calls: list[tuple[object, tuple[object, ...]]] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        calls.append((script, args))
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_kuaishou_reply_v4(object(), 4, "ok") is True
    assert any(args == (4,) for _script, args in calls)
    assert any(args == (4, "ok") for _script, args in calls)
    assert len(wait_calls) >= 2


def test_submit_kuaishou_reply_v5_uses_platform_specific_confirm_selector(monkeypatch) -> None:
    scripts: list[str] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        scripts.append(script)
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_kuaishou_reply_v5(object(), 4, "ok") is True
    assert any(".comment-btn.sure-btn.sure-btn--is-active" in script for script in scripts)
    assert len(wait_calls) >= 2


def test_submit_douyin_reply_v2_uses_platform_specific_send_selector(monkeypatch) -> None:
    scripts: list[str] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        scripts.append(script)
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_douyin_reply_v2(object(), 1, "ok") is True
    assert any("button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary" in script for script in scripts)
    assert wait_calls


def test_submit_douyin_reply_v3_prefers_contenteditable_and_send_selector(monkeypatch) -> None:
    scripts: list[str] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        scripts.append(script)
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_douyin_reply_v3(object(), 1, "ok") is True
    assert any('[contenteditable="true"]' in script for script in scripts)
    assert any("button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary" in script for script in scripts)
    assert wait_calls


def test_submit_douyin_reply_v4_tries_exec_command_before_send(monkeypatch) -> None:
    scripts: list[str] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        scripts.append(script)
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_douyin_reply_v4(object(), 1, "ok") is True
    assert any("document.execCommand('insertText'" in script for script in scripts)
    assert any("button.douyin-creator-interactive-button.douyin-creator-interactive-button-primary" in script for script in scripts)
    assert wait_calls


def test_submit_douyin_reply_v5_clicks_contenteditable_reply_box(monkeypatch) -> None:
    scripts: list[str] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        scripts.append(script)
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_douyin_reply_v5(object(), 1, "ok") is True
    assert any('div[contenteditable="true"]' in script for script in scripts)
    assert any("click(input)" in script for script in scripts)
    assert wait_calls


def test_submit_douyin_reply_v6_activates_specific_input_box_before_fill(monkeypatch) -> None:
    scripts: list[str] = []
    wait_calls: list[int] = []

    def fake_run_js_dict(_page, script, *args):
        scripts.append(script)
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: wait_calls.append(1) or bool(predicate()))

    assert runtime._submit_douyin_reply_v6(object(), 1, "ok") is True
    assert any('div[class*="input-"][contenteditable="true"]' in script for script in scripts)
    assert any("document.activeElement === input" in script for script in scripts)
    assert wait_calls


def test_submit_douyin_reply_v7_prefers_element_level_reply_editor(monkeypatch) -> None:
    class _FakeElement:
        def click(self, by_js=False):
            return None

    class _FakePage:
        def __init__(self):
            self.calls = []

        def ele(self, selector, timeout=0):
            self.calls.append((selector, timeout))
            return _FakeElement()

    page = _FakePage()
    monkeypatch.setattr(runtime, "_run_js_dict", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(runtime.engine, "_is_visible_element", lambda _ele: True)
    monkeypatch.setattr(runtime.engine, "_input_text_field_with_keyboard", lambda _ele, _text: True)

    assert runtime._submit_douyin_reply_v7(page, 1, "ok") is True
    assert any("placeholder*='回复'" in selector or "aria-label*='回复'" in selector for selector, _timeout in page.calls)


def test_submit_douyin_reply_v8_uses_cdp_insert_text_when_available(monkeypatch) -> None:
    class _FakeElement:
        def click(self, by_js=False):
            return None

        def run_js(self, _script):
            return None

    class _FakePage:
        def __init__(self):
            self.calls = []
            self.cdp_calls = []

        def ele(self, selector, timeout=0):
            self.calls.append((selector, timeout))
            return _FakeElement()

        def run_cdp(self, method, **kwargs):
            self.cdp_calls.append((method, kwargs))
            return None

    page = _FakePage()
    monkeypatch.setattr(runtime, "_run_js_dict", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: bool(predicate()))
    monkeypatch.setattr(runtime.engine, "_is_visible_element", lambda _ele: True)
    monkeypatch.setattr(runtime.engine, "_input_text_field_with_keyboard", lambda _ele, _text: True)

    assert runtime._submit_douyin_reply_v8(page, 1, "ok") is True
    assert ("Input.insertText", {"text": "ok"}) in page.cdp_calls


def test_submit_douyin_reply_v8_accepts_focused_active_editor_when_selector_misses(monkeypatch) -> None:
    class _FakePage:
        def __init__(self):
            self.cdp_calls = []

        def ele(self, selector, timeout=0):
            return None

        def run_cdp(self, method, **kwargs):
            self.cdp_calls.append((method, kwargs))
            return None

    call_index = {"i": 0}

    def fake_run_js_dict(_page, _script, *args):
        call_index["i"] += 1
        if call_index["i"] == 1:
            return {"ok": True}
        if call_index["i"] == 2:
            return {"ok": True}
        if call_index["i"] == 3:
            return {"ok": True}
        return {"ok": True}

    page = _FakePage()
    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: bool(predicate()))
    monkeypatch.setattr(runtime.engine, "_is_visible_element", lambda _ele: True)
    monkeypatch.setattr(runtime.engine, "_input_text_field_with_keyboard", lambda _ele, _text: True)

    assert runtime._submit_douyin_reply_v8(page, 1, "ok") is True
    assert ("Input.insertText", {"text": "ok"}) in page.cdp_calls


def test_submit_douyin_reply_v9_prefers_already_focused_editor(monkeypatch) -> None:
    class _FakeElement:
        def click(self, by_js=False):
            return None

    class _FakePage:
        def __init__(self):
            self.cdp_calls = []

        def ele(self, selector, timeout=0):
            return _FakeElement()

        def run_cdp(self, method, **kwargs):
            self.cdp_calls.append((method, kwargs))
            return None

    page = _FakePage()
    call_index = {"i": 0}

    def fake_run_js_dict(_page, _script, *args):
        call_index["i"] += 1
        return {"ok": True}

    monkeypatch.setattr(runtime, "_run_js_dict", fake_run_js_dict)
    monkeypatch.setattr(runtime, "_wait_until", lambda predicate, **_kwargs: bool(predicate()))
    monkeypatch.setattr(runtime.engine, "_is_visible_element", lambda _ele: True)

    assert runtime._submit_douyin_reply_v9(page, 1, "ok") is True
    assert ("Input.insertText", {"text": "ok"}) in page.cdp_calls


def test_extract_kuaishou_comments_marks_reply_flag_from_runtime_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "_run_js_list",
        lambda _page, _script: [
            {
                "index": 0,
                "author": "user",
                "time_text": "1小时前",
                "content": "hello",
                "has_reply": True,
                "liked": False,
            }
        ],
    )

    result = runtime._extract_kuaishou_comments(object())

    assert result[0]["has_reply"] is True


def test_should_skip_comment_reply_guarded_skips_self_author_comment() -> None:
    comment = {
        "author": "CyberCar 作者",
        "content": "谢谢支持，后面还有更多惊喜。",
    }
    should_skip, reason = runtime._should_skip_comment_reply_guarded(comment)
    assert should_skip is True
    assert reason == "self_author_comment"


def test_should_skip_comment_reply_guarded_skips_empty_comment_content() -> None:
    comment = {
        "author": "normal_user",
        "content": "   ",
    }
    should_skip, reason = runtime._should_skip_comment_reply_guarded(comment)
    assert should_skip is True
    assert reason == "empty_comment_content"


def test_should_skip_comment_reply_guarded_skips_exact_self_author_name() -> None:
    comment = {
        "author": "CyberCar",
        "content": "hello",
    }

    should_skip, reason = runtime._should_skip_comment_reply_guarded(comment)

    assert should_skip is True
    assert reason == "self_author_comment"


def test_should_skip_comment_reply_guarded_skips_self_author_reply_prefix() -> None:
    comment = {
        "author": "CyberCar回复张某先生",
        "content": "谢谢关注",
    }

    should_skip, reason = runtime._should_skip_comment_reply_guarded(comment)

    assert should_skip is True
    assert reason == "self_author_comment"


def test_should_skip_comment_reply_guarded_keeps_other_user_replying_to_self() -> None:
    comment = {
        "author": "张某先生回复CyberCar",
        "content": "这是真的吗",
    }

    should_skip, reason = runtime._should_skip_comment_reply_guarded(comment)

    assert should_skip is False
    assert reason == ""
