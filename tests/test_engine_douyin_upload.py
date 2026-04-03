from __future__ import annotations

from typing import Any

from cybercar import engine


def test_read_generic_file_inputs_snapshot_aggregates_frame_contexts() -> None:
    class FakeOwner:
        def __init__(self, payload: dict[str, Any], frames: list[Any] | None = None) -> None:
            self.payload = payload
            self._frames = frames or []

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return list(self._frames)

        def run_js(self, _script: str) -> dict[str, Any]:
            return dict(self.payload)

    frame = FakeOwner(
        {
            "total": 1,
            "max_count": 2,
            "sample": [{"name": "frame-upload"}],
            "root_count": 3,
        }
    )
    primary = FakeOwner({"total": 0, "max_count": 0, "sample": [], "root_count": 1}, frames=[frame])

    snapshot = engine._read_generic_file_inputs_snapshot(primary, None)

    assert snapshot["total"] == 1
    assert snapshot["max_count"] == 2
    assert snapshot["root_count"] == 4
    assert snapshot["sample"] == [{"name": "frame-upload"}]


def test_activate_upload_trigger_generic_scans_frame_contexts(monkeypatch) -> None:
    class FakeFrame:
        def __init__(self) -> None:
            self.run_js_calls = 0

        def run_js(self, script: str) -> str:
            self.run_js_calls += 1
            if "__TRIGGER_PATTERN__" in script:
                raise AssertionError("trigger pattern placeholder should be replaced")
            if "collectAllRoots" in script and "clicked:" in script:
                return "clicked:iframe-upload|storage|6400"
            return ""

    class FakeOwner:
        def __init__(self, frame: FakeFrame) -> None:
            self.frame = frame

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return [self.frame]

        def run_js(self, _script: str) -> str:
            return "not_found"

    frame = FakeFrame()
    owner = FakeOwner(frame)

    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._activate_upload_trigger_generic(owner, None, platform_name="xiaohongshu")

    assert frame.run_js_calls == 1


def test_collect_upload_contexts_recurses_nested_frames() -> None:
    class FakeOwner:
        def __init__(self, frames: list[Any] | None = None) -> None:
            self._frames = frames or []

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return list(self._frames)

    leaf = FakeOwner()
    child = FakeOwner([leaf])
    root = FakeOwner([child])

    contexts = engine._collect_upload_contexts(root, None)

    assert contexts == [root, child, leaf]


def test_activate_upload_trigger_generic_prefers_douyin_button_selector(monkeypatch) -> None:
    class FakeElement:
        def __init__(self) -> None:
            self.run_js_calls = 0
            self.click_calls = 0
            self.click_by_js_calls = 0

        def run_js(self, _script: str) -> None:
            self.run_js_calls += 1

        def click(self, by_js: bool = False) -> None:
            if by_js:
                self.click_by_js_calls += 1
            else:
                self.click_calls += 1

    class FakeOwner:
        def __init__(self, element: FakeElement) -> None:
            self.element = element
            self.selectors: list[str] = []

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return []

        def ele(self, selector: str, timeout: float = 0) -> Any:
            self.selectors.append(selector)
            if selector == "css:button[class*='container-drag-btn']":
                return self.element
            return None

    element = FakeElement()
    owner = FakeOwner(element)

    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_is_visible_element", lambda _ele: True)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    engine._activate_upload_trigger_generic(owner, None, platform_name="douyin")

    assert owner.selectors[0] == "css:button[class*='container-drag-btn']"
    assert element.click_calls == 1
    assert element.click_by_js_calls == 0


def test_activate_upload_trigger_generic_uses_douyin_js_scorer_after_selector_miss(monkeypatch) -> None:
    class FakeOwner:
        def __init__(self) -> None:
            self.run_js_calls: list[str] = []
            self.selector_calls = 0

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return []

        def run_js(self, script: str) -> str:
            self.run_js_calls.append(script)
            if "clicked:douyin-js|" in script:
                return "clicked:douyin-js|drop-qMdG9E||340"
            return ""

        def ele(self, selector: str, timeout: float = 0) -> Any:
            self.selector_calls += 1
            return None

    owner = FakeOwner()

    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    engine._activate_upload_trigger_generic(owner, None, platform_name="douyin")

    assert len(owner.run_js_calls) == 1
    assert owner.selector_calls > 0


def test_activate_upload_trigger_generic_douyin_js_targets_upload_shell(monkeypatch) -> None:
    class FakeOwner:
        def __init__(self) -> None:
            self.run_js_calls: list[str] = []

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return []

        def run_js(self, script: str) -> str:
            self.run_js_calls.append(script)
            return "not_found"

        def ele(self, selector: str, timeout: float = 0) -> Any:
            return None

    owner = FakeOwner()

    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_is_visible_element", lambda _ele: False)

    try:
        engine._activate_upload_trigger_generic(owner, None, platform_name="douyin")
    except RuntimeError:
        pass

    assert owner.run_js_calls
    script = next(call for call in owner.run_js_calls if "clicked:douyin-js|" in call)
    assert "content-right" in script
    assert "container-drag-btn" in script
    assert "phone-screen" in script
    assert "允许" in script
    assert "发布设置" in script


def test_read_douyin_image_upload_state_detects_ready_editor() -> None:
    class FakeOwner:
        def run_js(self, _script: str) -> dict[str, Any]:
            return {
                "ready": True,
                "busy": False,
                "upload_entry": False,
                "image_added": True,
                "editor_hints": True,
                "publish_btn": True,
                "sample_texts": ["编辑图片", "已添加1张图片", "发布"],
            }

    state = engine._read_douyin_image_upload_state(FakeOwner(), None)

    assert state["ready"] is True
    assert state["image_added"] is True
    assert state["publish_btn"] is True


def test_read_douyin_video_upload_state_detects_ready_editor() -> None:
    class FakeOwner:
        def run_js(self, _script: str) -> dict[str, Any]:
            return {
                "ready": True,
                "busy": False,
                "upload_entry": False,
                "caption_input": True,
                "title_input": False,
                "editor_hints": True,
                "publish_btn": True,
                "schedule_toggle": True,
                "sample_texts": ["作品描述", "立即发布", "发布"],
            }

    state = engine._read_douyin_video_upload_state(FakeOwner(), None)

    assert state["ready"] is True
    assert state["caption_input"] is True
    assert state["publish_btn"] is True


def test_read_douyin_video_upload_state_merges_primary_and_fallback() -> None:
    class PrimaryOwner:
        def run_js(self, _script: str) -> dict[str, Any]:
            return {
                "ready": False,
                "busy": False,
                "upload_entry": True,
                "caption_input": False,
                "title_input": False,
                "editor_hints": False,
                "publish_btn": False,
                "schedule_toggle": False,
                "sample_texts": ["上传视频"],
            }

    class FallbackOwner:
        def run_js(self, _script: str) -> dict[str, Any]:
            return {
                "ready": False,
                "busy": False,
                "upload_entry": False,
                "caption_input": True,
                "title_input": False,
                "editor_hints": True,
                "publish_btn": True,
                "schedule_toggle": False,
                "sample_texts": ["作品描述", "立即发布"],
            }

    state = engine._read_douyin_video_upload_state(PrimaryOwner(), FallbackOwner())

    assert state["upload_entry"] is True
    assert state["caption_input"] is True
    assert state["editor_hints"] is True
    assert state["publish_btn"] is True
    assert "上传视频" in state["sample_texts"]
    assert "作品描述" in state["sample_texts"]


def test_wait_upload_ready_generic_accepts_douyin_image_editor_ready(monkeypatch, tmp_path) -> None:
    class FakeOwner:
        def __init__(self) -> None:
            self.calls = 0

        def run_js(self, _script: str) -> str:
            self.calls += 1
            return "编辑图片 已添加1张图片 继续添加 发布设置 发布"

    owner = FakeOwner()
    target = tmp_path / "sample.jpg"
    target.write_bytes(b"x")

    monkeypatch.setattr(
        engine,
        "_read_douyin_image_upload_state",
        lambda *_args, **_kwargs: {
            "ready": True,
            "busy": False,
            "upload_entry": False,
            "image_added": True,
            "editor_hints": True,
            "publish_btn": True,
            "sample_texts": ["编辑图片", "已添加1张图片", "发布"],
        },
    )
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._wait_upload_ready_generic(None, owner, "douyin", timeout_seconds=3, upload_target=target)

    assert result is owner


def test_wait_upload_ready_generic_waits_for_douyin_video_editor_ready(monkeypatch, tmp_path) -> None:
    class FakeOwner:
        def __init__(self) -> None:
            self.calls = 0

        def run_js(self, _script: str) -> str:
            self.calls += 1
            return "上传视频 发布设置"

    class FakeClock:
        def __init__(self) -> None:
            self.now = 0.0

        def time(self) -> float:
            self.now += 1.0
            return self.now

    owner = FakeOwner()
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"x")
    clock = FakeClock()
    sleep_calls: list[float] = []
    states = [
        {
            "ready": False,
            "busy": False,
            "upload_entry": True,
            "caption_input": False,
            "title_input": False,
            "editor_hints": False,
            "publish_btn": False,
            "schedule_toggle": False,
            "sample_texts": ["上传视频"],
        },
        {
            "ready": True,
            "busy": False,
            "upload_entry": False,
            "caption_input": True,
            "title_input": False,
            "editor_hints": True,
            "publish_btn": True,
            "schedule_toggle": True,
            "sample_texts": ["作品描述", "立即发布", "发布"],
        },
    ]

    def fake_read_state(*_args, **_kwargs):
        return states.pop(0)

    monkeypatch.setattr(engine, "_read_douyin_video_upload_state", fake_read_state)
    monkeypatch.setattr(engine.time, "time", clock.time)
    monkeypatch.setattr(engine.time, "sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._wait_upload_ready_generic(None, owner, "douyin", timeout_seconds=10, upload_target=target)

    assert result is owner
    assert sleep_calls == [2.0]
    assert owner.calls >= 2


def test_wait_upload_ready_generic_douyin_best_effort_after_bound_upload_entry(
    monkeypatch,
    tmp_path,
) -> None:
    class FakeOwner:
        def run_js(self, _script: str) -> str:
            return "上传视频"

    owner = FakeOwner()
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"x")
    timeline = {"now": 0.0}
    sleep_calls: list[float] = []

    monkeypatch.setattr(
        engine,
        "_read_douyin_video_upload_state",
        lambda *_args, **_kwargs: {
            "ready": False,
            "busy": False,
            "upload_entry": True,
            "caption_input": False,
            "title_input": False,
            "editor_hints": False,
            "publish_btn": False,
            "schedule_toggle": False,
            "sample_texts": ["上传视频"],
        },
    )
    monkeypatch.setattr(engine.time, "time", lambda: float(timeline["now"]))
    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(float(seconds))
        timeline["now"] = float(timeline["now"]) + float(seconds)

    monkeypatch.setattr(engine.time, "sleep", fake_sleep)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._wait_upload_ready_generic(
        None,
        owner,
        "douyin",
        timeout_seconds=120,
        upload_target=target,
        upload_binding_confirmed=True,
    )

    assert result is owner
    assert sum(sleep_calls) >= 20.0


def test_stage_generic_upload_via_page_set_accepts_douyin_editor_ready_without_file_inputs(
    monkeypatch, tmp_path
) -> None:
    class FakeSet:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def upload_files(self, path: str) -> None:
            self.calls.append(path)

    class FakePage:
        def __init__(self) -> None:
            self.set = FakeSet()

    target = tmp_path / "sample.jpg"
    target.write_bytes(b"x")
    page = FakePage()

    monkeypatch.setattr(engine, "_activate_upload_trigger_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_retry_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        engine,
        "_read_generic_file_inputs_snapshot",
        lambda *_args, **_kwargs: {"total": 0, "max_count": 0, "sample": [], "root_count": 0},
    )
    monkeypatch.setattr(engine, "_log_upload_surface_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        engine,
        "_read_douyin_image_upload_state",
        lambda *_args, **_kwargs: {
            "ready": False,
            "busy": False,
            "upload_entry": False,
            "image_added": True,
            "editor_hints": True,
            "publish_btn": False,
            "sample_texts": ["编辑图片", "已添加1张图片", "继续添加"],
        },
    )
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._stage_generic_upload_via_page_set(page, None, None, target, "douyin")

    assert result is True
    assert page.set.calls == [str(target)]


def test_fill_draft_once_generic_selects_douyin_collection_for_video(monkeypatch, tmp_path) -> None:
    target = tmp_path / "sample.mp4"
    target.write_bytes(b"x")
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(engine, "_dismiss_unfinished_dialog", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(engine, "_ensure_douyin_publish_mode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_run_page_action", lambda _page, _label, action: action())
    monkeypatch.setattr(engine, "_wait_upload_ready_generic", lambda _page, ctx, **_kwargs: ctx)
    monkeypatch.setattr(engine, "_fill_caption_generic", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_click_douyin_primary_publish_button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(engine, "_finalize_douyin_publish", lambda *_args, **_kwargs: "immediate")
    monkeypatch.setattr(engine, "_build_publish_verification_tokens", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_humanized_publish_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_humanized_publish_retry_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_prepare_image_post_text_payload", lambda *_args, **_kwargs: "ignored")
    monkeypatch.setattr(engine, "_collect_visible_action_texts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(engine, "_select_douyin_collection", lambda _ctx, _page, name: calls.append(("douyin", name)))

    class FakeInput:
        def input(self, _value: str) -> None:
            return None

    class FakeOwner:
        def __init__(self) -> None:
            self.url = "https://creator.douyin.com/creator-micro/content/upload"

        def run_js(self, *_args, **_kwargs):
            return ""

    monkeypatch.setattr(engine, "_find_upload_file_input_generic", lambda *_args, **_kwargs: FakeInput())

    page = FakeOwner()
    result = engine._fill_draft_once_generic(
        page=page,
        target=target,
        final_caption="video caption",
        open_url="https://creator.douyin.com/creator-micro/content/upload",
        platform_name="douyin",
        save_draft=True,
        publish_now=False,
        upload_timeout=30,
        draft_button_texts=("保存草稿",),
        publish_button_texts=("发布",),
        collection_name="赛博皮卡现车：aawbcc",
    )

    assert result is page
    assert calls == [("douyin", "赛博皮卡现车：aawbcc")]

def test_ensure_douyin_publish_mode_image_body_pattern_uses_stable_tokens(monkeypatch) -> None:
    scripts: list[str] = []

    class FakeOwner:
        url = ""

        def run_js(self, script: str) -> dict[str, str]:
            scripts.append(script)
            return {"state": "not_found"}

    monkeypatch.setattr(engine, "_humanized_publish_retry_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    ok = engine._ensure_douyin_publish_mode(FakeOwner(), None, prefer_video=False, max_rounds=1)

    assert ok is False
    assert scripts
    joined = "\n".join(scripts)
    assert "\\u5df2\\u6dfb\\u52a0\\\\s*\\\\d+\\\\s*\\u5f20\\u56fe\\u7247" in joined
    assert "\\u7f16\\u8f91\\u56fe\\u7247" in joined
    assert "\\u7ee7\\u7eed\\u6dfb\\u52a0" in joined
    assert "\\u9884\\u89c8\\u56fe\\u6587" in joined
    assert "\\u5bb8sx" not in joined

