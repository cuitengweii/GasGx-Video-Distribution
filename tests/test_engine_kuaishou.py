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
