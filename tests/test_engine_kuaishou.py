from __future__ import annotations

from typing import Any
from pathlib import Path

from cybercar import engine


def test_pick_kuaishou_publish_wrap_text_prefers_publish_panel_context() -> None:
    wrap = engine._pick_kuaishou_publish_wrap_text(
        (
            "发布",
            "查看权限 发布时间 发布 取消",
            "上传图文 首页 内容管理 互动管理",
        )
    )

    assert wrap == "查看权限 发布时间 发布 取消"


def test_pick_kuaishou_publish_wrap_text_rejects_shell_only_context() -> None:
    wrap = engine._pick_kuaishou_publish_wrap_text(
        (
            "发布作品",
            "上传图文 首页 内容管理 互动管理 数据中心 成长中心 创作服务",
        )
    )

    assert wrap == ""


def test_click_kuaishou_primary_publish_button_uses_publish_panel_context() -> None:
    class FakeButton:
        def __init__(self) -> None:
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            if "const nodes = []" in script:
                return {
                    "wrap": "查看权限 发布时间 发布 取消",
                    "texts": [
                        "发布",
                        "查看权限 发布时间 发布 取消",
                        "上传图文 首页 内容管理",
                    ],
                }
            return True

        def click(self, by_js: bool = False) -> None:
            self.clicked = True

    class FakeOwner:
        def __init__(self, button: FakeButton) -> None:
            self.button = button

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if "../button[normalize-space(.)='取消']" in selector or selector == "xpath://button[normalize-space(.)='发布']":
                return self.button
            return None

    button = FakeButton()
    owner = FakeOwner(button)

    assert engine._click_kuaishou_primary_publish_button(owner, None) is True
    assert button.clicked is True


def test_click_kuaishou_primary_publish_button_accepts_bottom_publish_work_button() -> None:
    class FakeButton:
        def __init__(self) -> None:
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            if "const nodes = []" in script:
                return {
                    "wrap": "发布作品",
                    "texts": [
                        "发布作品",
                        "取消",
                    ],
                    "rectTop": 820,
                    "viewportHeight": 1000,
                    "buttonText": "发布作品",
                }
            return True

        def click(self, by_js: bool = False) -> None:
            self.clicked = True

    class FakeOwner:
        def __init__(self, button: FakeButton) -> None:
            self.button = button

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if "发布作品" in selector:
                return self.button
            return None

    button = FakeButton()
    owner = FakeOwner(button)

    assert engine._click_kuaishou_primary_publish_button(owner, None) is True
    assert button.clicked is True


def test_click_kuaishou_publish_confirm_dialog_only_ignores_compose_page() -> None:
    class FakeOwner:
        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del selector, timeout
            return None

        def run_js(self, script: str, *_args: Any) -> Any:
            if "const modalRoots" in script:
                return False
            return {}

    owner = FakeOwner()

    assert engine._click_kuaishou_publish_confirm_dialog_only(owner, None) is False


def test_click_kuaishou_publish_confirm_dialog_only_clicks_dialog_button() -> None:
    class FakeButton:
        def __init__(self) -> None:
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            if "target.click();" in script:
                self.clicked = True
                return True
            return True

        def click(self, by_js: bool = False) -> None:
            del by_js
            self.clicked = True

    class FakeOwner:
        def __init__(self, button: FakeButton) -> None:
            self.button = button

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if "@role='dialog'" in selector or "contains(@class,'dialog')" in selector:
                return self.button
            return None

        def run_js(self, script: str, *_args: Any) -> Any:
            if "const modalRoots" in script:
                return False
            return {}

    button = FakeButton()
    owner = FakeOwner(button)

    assert engine._click_kuaishou_publish_confirm_dialog_only(owner, None) is True
    assert button.clicked is True


def test_wait_upload_ready_generic_waits_for_kuaishou_image_editor_form(
    monkeypatch,
) -> None:
    class FakeOwner:
        def __init__(self) -> None:
            self.state_calls = 0

        def run_js(self, script: str, *_args: Any) -> Any:
            if "return (document.body && document.body.innerText) || '';" in script:
                return "上传图文 发布作品"
            if "Upload appears completed by image editor readiness" in script:
                return {}
            if "const bodyText = norm((document.body && document.body.innerText) || '');" in script:
                self.state_calls += 1
                if self.state_calls == 1:
                    return {
                        "ready": False,
                        "busy": False,
                        "upload_entry": True,
                        "desc_input": False,
                        "title_input": False,
                        "publish_btn": True,
                        "cancel_btn": False,
                        "editor_hints": False,
                        "sample_texts": ["发布作品"],
                    }
                return {
                    "ready": True,
                    "busy": False,
                    "upload_entry": False,
                    "desc_input": True,
                    "title_input": False,
                    "publish_btn": True,
                    "cancel_btn": True,
                    "editor_hints": True,
                    "sample_texts": ["作品描述", "取消", "发布"],
                }
            return {}

    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    owner = FakeOwner()
    result = engine._wait_upload_ready_generic(
        owner,
        owner,
        platform_name="kuaishou",
        timeout_seconds=30,
        upload_target=Path("sample.jpg"),
    )

    assert result is owner
    assert owner.state_calls >= 2


def test_stage_kuaishou_image_upload_via_page_set_clicks_upload_button_first(
    monkeypatch,
) -> None:
    events: list[str] = []

    class FakeSetter:
        def upload_files(self, target_path: str) -> None:
            events.append(f"upload:{Path(target_path).name}")

    class FakePage:
        def __init__(self) -> None:
            self.set = FakeSetter()

    snapshots = [
        {"max_count": 0, "total": 2},
        {"max_count": 1, "total": 2},
    ]

    monkeypatch.setattr(engine, "_ensure_kuaishou_publish_mode", lambda *args, **kwargs: events.append("ensure"))
    monkeypatch.setattr(
        engine,
        "_click_first_matching_button",
        lambda *args, **kwargs: events.append("click:upload") or True,
    )
    monkeypatch.setattr(engine, "_activate_upload_trigger_generic", lambda *args, **kwargs: events.append("activate"))
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log_upload_surface_snapshot", lambda *args, **kwargs: events.append("snapshot"))
    monkeypatch.setattr(engine, "_read_generic_file_inputs_snapshot", lambda *args, **kwargs: snapshots.pop(0))

    result = engine._stage_kuaishou_image_upload_via_page_set(
        FakePage(),
        object(),
        object(),
        Path("sample.jpg"),
    )

    assert result is True
    assert events[:3] == ["ensure", "click:upload", "upload:sample.jpg"]


def test_pick_kuaishou_file_input_candidate_prefers_add_image_input() -> None:
    dom_index, candidate, score = engine._pick_kuaishou_file_input_candidate(
        [
            {
                "idx": 0,
                "accept": "video/*,.mp4,.mov",
                "wrap": "还有上次未发布的视频，是否继续编辑？继续编辑 放弃 拖拽视频到此或点击上传 上传视频",
                "multiple": False,
                "visible": False,
            },
            {
                "idx": 1,
                "accept": "image/png, image/jpg, image/jpeg, image/webp",
                "wrap": "发布图文 作品描述 发布设置 发布 取消",
                "multiple": True,
                "visible": False,
            },
            {
                "idx": 2,
                "accept": "image/png, image/jpg, image/jpeg, image/webp",
                "wrap": "编辑图片 1/31 添加图片",
                "multiple": False,
                "visible": False,
            },
        ],
        prefer_video=False,
    )

    assert dom_index == 2
    assert candidate["wrap"] == "编辑图片 1/31 添加图片"
    assert score > 0


def test_pick_kuaishou_file_input_candidate_rejects_video_shell_for_image_publish() -> None:
    dom_index, candidate, score = engine._pick_kuaishou_file_input_candidate(
        [
            {
                "idx": 0,
                "accept": "video/*,.mp4,.mov",
                "wrap": "还有上次未发布的视频，是否继续编辑？继续编辑 放弃 拖拽视频到此或点击上传 上传视频",
                "multiple": False,
                "visible": False,
            },
            {
                "idx": 1,
                "accept": "image/png, image/jpg, image/jpeg, image/webp",
                "wrap": "发布图文 作品描述 发布设置 发布 取消",
                "multiple": True,
                "visible": False,
            },
        ],
        prefer_video=False,
    )

    assert dom_index == 1
    assert candidate["accept"].startswith("image/")
    assert score > 0
