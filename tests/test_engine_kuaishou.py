from __future__ import annotations

from typing import Any

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

