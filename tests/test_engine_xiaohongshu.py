from __future__ import annotations

from typing import Any

from cybercar import engine


def test_trim_text_by_utf16_units_counts_emoji_like_browser() -> None:
    text = "1234567890123456789\U0001F386"

    trimmed = engine._trim_text_by_utf16_units(text, 20)

    assert trimmed == "1234567890123456789"
    assert engine._utf16_code_unit_length(trimmed) == 19


def test_build_xiaohongshu_title_from_caption_limits_utf16_length() -> None:
    caption = "1234567890123456789\U0001F386\nThe next Cybertruck you see"

    title = engine._build_xiaohongshu_title_from_caption(caption, limit=20)

    assert title == "1234567890123456789"
    assert engine._utf16_code_unit_length(title) <= 20


def test_pick_xiaohongshu_publish_wrap_text_prefers_publish_panel_context() -> None:
    wrap = engine._pick_xiaohongshu_publish_wrap_text(
        (
            "\u53d1\u5e03\u7b14\u8bb0",
            "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u516c\u5f00\u53ef\u89c1 \u5b9a\u65f6\u53d1\u5e03",
            "\u6682\u5b58\u79bb\u5f00 \u53d1\u5e03",
        )
    )

    assert wrap == "\u6682\u5b58\u79bb\u5f00 \u53d1\u5e03"


def test_click_xiaohongshu_primary_publish_button_skips_sidebar_publish_note(monkeypatch) -> None:
    class FakeButton:
        def __init__(
            self,
            *,
            wrap: str,
            texts: tuple[str, ...],
            rect_top: float,
            button_text: str,
        ) -> None:
            self.wrap = wrap
            self.texts = texts
            self.rect_top = rect_top
            self.button_text = button_text
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            if "window.getComputedStyle(this)" in script:
                return True
            if "const norm = (s)" in script:
                return {
                    "wrap": self.wrap,
                    "texts": list(self.texts),
                    "rectTop": self.rect_top,
                    "viewportHeight": 1000,
                    "buttonText": self.button_text,
                }
            return True

        def click(self, by_js: bool = False) -> None:
            del by_js
            self.clicked = True

    class FakeOwner:
        def __init__(self, sidebar_button: FakeButton, footer_button: FakeButton) -> None:
            self.sidebar_button = sidebar_button
            self.footer_button = footer_button

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if selector == "css:button.custom-button.bg-red":
                return self.sidebar_button
            if selector == "xpath://button[contains(@class,'custom-button') and contains(@class,'bg-red')]":
                return self.footer_button
            return None

    sidebar_button = FakeButton(
        wrap="\u53d1\u5e03\u7b14\u8bb0 \u9996\u9875 \u7b14\u8bb0\u7ba1\u7406 \u6570\u636e\u770b\u677f",
        texts=(
            "\u53d1\u5e03\u7b14\u8bb0",
            "\u9996\u9875",
            "\u7b14\u8bb0\u7ba1\u7406",
        ),
        rect_top=96,
        button_text="\u53d1\u5e03\u7b14\u8bb0",
    )
    footer_button = FakeButton(
        wrap="\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u5b9a\u65f6\u53d1\u5e03",
        texts=(
            "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u5b9a\u65f6\u53d1\u5e03",
            "\u6682\u5b58\u79bb\u5f00 \u53d1\u5e03",
            "\u53d1\u5e03",
        ),
        rect_top=820,
        button_text="\u53d1\u5e03",
    )
    owner = FakeOwner(sidebar_button, footer_button)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    assert engine._click_xiaohongshu_primary_publish_button(owner, None) is True
    assert sidebar_button.clicked is False
    assert footer_button.clicked is True


def test_click_xiaohongshu_primary_publish_button_accepts_bottom_publish_note(monkeypatch) -> None:
    class FakeButton:
        def __init__(
            self,
            *,
            wrap: str,
            texts: tuple[str, ...],
            rect_top: float,
            button_text: str,
        ) -> None:
            self.wrap = wrap
            self.texts = texts
            self.rect_top = rect_top
            self.button_text = button_text
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            if "window.getComputedStyle(this)" in script:
                return True
            if "const norm = (s)" in script:
                return {
                    "wrap": self.wrap,
                    "texts": list(self.texts),
                    "rectTop": self.rect_top,
                    "viewportHeight": 1000,
                    "buttonText": self.button_text,
                }
            return True

        def click(self, by_js: bool = False) -> None:
            del by_js
            self.clicked = True

    class FakeOwner:
        def __init__(self, sidebar_button: FakeButton, footer_button: FakeButton) -> None:
            self.sidebar_button = sidebar_button
            self.footer_button = footer_button

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if selector == "css:button.custom-button.bg-red":
                return self.sidebar_button
            if selector == "xpath://button[contains(@class,'custom-button') and contains(@class,'bg-red')]":
                return self.footer_button
            return None

    sidebar_button = FakeButton(
        wrap="\u53d1\u5e03\u7b14\u8bb0 \u9996\u9875 \u7b14\u8bb0\u7ba1\u7406 \u6570\u636e\u770b\u677f",
        texts=(
            "\u53d1\u5e03\u7b14\u8bb0",
            "\u9996\u9875",
            "\u7b14\u8bb0\u7ba1\u7406",
        ),
        rect_top=96,
        button_text="\u53d1\u5e03\u7b14\u8bb0",
    )
    footer_button = FakeButton(
        wrap="\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u5b9a\u65f6\u53d1\u5e03",
        texts=(
            "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u5b9a\u65f6\u53d1\u5e03",
            "\u6682\u5b58\u79bb\u5f00 \u53d1\u5e03\u7b14\u8bb0",
            "\u53d1\u5e03\u7b14\u8bb0",
        ),
        rect_top=820,
        button_text="\u53d1\u5e03\u7b14\u8bb0",
    )
    owner = FakeOwner(sidebar_button, footer_button)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    assert engine._click_xiaohongshu_primary_publish_button(owner, None) is True
    assert sidebar_button.clicked is False
    assert footer_button.clicked is True


def test_click_xiaohongshu_primary_publish_button_accepts_bottom_publish_work(monkeypatch) -> None:
    class FakeButton:
        def __init__(
            self,
            *,
            wrap: str,
            texts: tuple[str, ...],
            rect_top: float,
            button_text: str,
        ) -> None:
            self.wrap = wrap
            self.texts = texts
            self.rect_top = rect_top
            self.button_text = button_text
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            if "window.getComputedStyle(this)" in script:
                return True
            if "const norm = (s)" in script:
                return {
                    "wrap": self.wrap,
                    "texts": list(self.texts),
                    "rectTop": self.rect_top,
                    "viewportHeight": 1000,
                    "buttonText": self.button_text,
                }
            return True

        def click(self, by_js: bool = False) -> None:
            del by_js
            self.clicked = True

    class FakeOwner:
        def __init__(self, sidebar_button: FakeButton, footer_button: FakeButton) -> None:
            self.sidebar_button = sidebar_button
            self.footer_button = footer_button

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if selector == "css:button.custom-button.bg-red":
                return self.sidebar_button
            if selector == "xpath://button[contains(@class,'custom-button') and contains(@class,'bg-red')]":
                return self.footer_button
            return None

    sidebar_button = FakeButton(
        wrap="\u53d1\u5e03\u7b14\u8bb0 \u9996\u9875 \u7b14\u8bb0\u7ba1\u7406 \u6570\u636e\u770b\u677f",
        texts=(
            "\u53d1\u5e03\u7b14\u8bb0",
            "\u9996\u9875",
            "\u7b14\u8bb0\u7ba1\u7406",
        ),
        rect_top=96,
        button_text="\u53d1\u5e03\u7b14\u8bb0",
    )
    footer_button = FakeButton(
        wrap="\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u5b9a\u65f6\u53d1\u5e03",
        texts=(
            "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u5b9a\u65f6\u53d1\u5e03",
            "\u6682\u5b58\u79bb\u5f00 \u53d1\u5e03\u4f5c\u54c1",
            "\u53d1\u5e03\u4f5c\u54c1",
        ),
        rect_top=820,
        button_text="\u53d1\u5e03\u4f5c\u54c1",
    )
    owner = FakeOwner(sidebar_button, footer_button)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    assert engine._click_xiaohongshu_primary_publish_button(owner, None) is True
    assert sidebar_button.clicked is False
    assert footer_button.clicked is True


def test_click_xiaohongshu_primary_publish_button_uses_shadow_dom_fallback(monkeypatch) -> None:
    class FakeOwner:
        def __init__(self) -> None:
            self.js_scripts: list[str] = []

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del selector, timeout
            return None

        def run_js(self, script: str, *_args: Any) -> Any:
            self.js_scripts.append(script)
            if "collectRoots(document)" in script and "shadowRoot" in script and "contentDocument" in script:
                return True
            return False

    owner = FakeOwner()
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    assert engine._click_xiaohongshu_primary_publish_button(owner, None) is True
    assert len(owner.js_scripts) == 1


def test_click_xiaohongshu_primary_publish_button_accepts_publish_video_container(monkeypatch) -> None:
    class FakeButton:
        def __init__(self) -> None:
            self.clicked = False
            self.run_js_calls: list[str] = []

        def run_js(self, script: str, *_args: Any) -> Any:
            self.run_js_calls.append(script)
            if "const norm = (s)" in script:
                return {
                    "wrap": "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u516c\u5f00\u53ef\u89c1 \u5b9a\u65f6\u53d1\u5e03",
                    "texts": [
                        "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u516c\u5f00\u53ef\u89c1 \u5b9a\u65f6\u53d1\u5e03",
                        "\u53d1\u5e03\u7b14\u8bb0",
                        "\u516c\u5f00\u53ef\u89c1",
                    ],
                    "rectTop": 820,
                    "viewportHeight": 1000,
                    "buttonText": "\u53d1\u5e03",
                    "submitText": "\u53d1\u5e03",
                    "saveDisabled": "false",
                    "publishFlag": "true",
                    "tagName": "xhs-publish-btn",
                    "cls": "publish-page-publish-btn",
                }
            return True

        def click(self, by_js: bool = False) -> None:
            del by_js
            self.clicked = True

    class FakeOwner:
        def __init__(self) -> None:
            self.button = FakeButton()

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if selector in {
                "css:xhs-publish-btn.publish-page-publish-btn",
                "xpath://xhs-publish-btn[contains(@class,'publish-page-publish-btn')]",
                "xpath://xhs-publish-btn[@submit-text='发布' or @submit-text='发布笔记' or @submit-text='发布作品']",
                "xpath://xhs-publish-btn[@is-publish='true' and @save-disabled='false']",
                "css:div.publish-video",
                "xpath://div[contains(@class,'publish-video')]",
                "xpath://div[contains(@class,'btn-wrapper') and .//*[normalize-space(.)='\u53d1\u5e03\u7b14\u8bb0']]",
                "xpath://div[contains(@class,'btn-inner') and .//*[normalize-space(.)='\u53d1\u5e03\u7b14\u8bb0']]",
                "xpath://span[contains(@class,'btn-text') and normalize-space(.)='\u53d1\u5e03\u7b14\u8bb0']/ancestor::div[contains(@class,'publish-video')][1]",
                "xpath://span[normalize-space(.)='\u53d1\u5e03\u7b14\u8bb0']/ancestor::div[contains(@class,'publish-video')][1]",
            }:
                return self.button
            return None

    owner = FakeOwner()
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    assert engine._click_xiaohongshu_primary_publish_button(owner, None) is True
    assert owner.button.clicked is True


def test_click_xiaohongshu_primary_publish_button_clicks_shadow_button_when_available(monkeypatch) -> None:
    class FakeInnerButton:
        def __init__(self) -> None:
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            del script
            return True

        def click(self, by_js: bool = False) -> None:
            del by_js
            self.clicked = True

    class FakeShadowRoot:
        def __init__(self, inner_button: FakeInnerButton) -> None:
            self.inner_button = inner_button

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if selector in {
                "css:button.ce-btn.bg-red",
                "css:button.bg-red",
                "xpath://button[contains(@class,'ce-btn') and contains(@class,'bg-red')]",
                "xpath://button[contains(@class,'bg-red')]",
                "xpath://button[normalize-space(.)='发布' or normalize-space(.)='发布笔记' or normalize-space(.)='发布作品' or normalize-space(.)='立即发布']",
            }:
                return self.inner_button
            return None

    class FakeButton:
        def __init__(self, shadow_root: FakeShadowRoot) -> None:
            self.shadow_root = shadow_root
            self.clicked = False

        def run_js(self, script: str, *_args: Any) -> Any:
            if "const norm = (s)" in script:
                return {
                    "wrap": "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u516c\u5f00\u53ef\u89c1 \u5b9a\u65f6\u53d1\u5e03",
                    "texts": [
                        "\u66f4\u591a\u8bbe\u7f6e \u6536\u8d77 \u516c\u5f00\u53ef\u89c1 \u5b9a\u65f6\u53d1\u5e03",
                        "\u53d1\u5e03\u7b14\u8bb0",
                        "\u516c\u5f00\u53ef\u89c1",
                    ],
                    "rectTop": 820,
                    "viewportHeight": 1000,
                    "buttonText": "\u53d1\u5e03",
                    "submitText": "\u53d1\u5e03",
                    "saveDisabled": "false",
                    "publishFlag": "true",
                    "tagName": "xhs-publish-btn",
                    "cls": "publish-page-publish-btn",
                }
            return True

        def click(self, by_js: bool = False) -> None:
            del by_js
            self.clicked = True

    class FakeOwner:
        def __init__(self) -> None:
            self.inner_button = FakeInnerButton()
            self.button = FakeButton(FakeShadowRoot(self.inner_button))

        def ele(self, selector: str, timeout: float = 0.0) -> Any:
            del timeout
            if selector in {
                "css:xhs-publish-btn.publish-page-publish-btn",
                "xpath://xhs-publish-btn[contains(@class,'publish-page-publish-btn')]",
                "xpath://xhs-publish-btn[@submit-text='鍙戝竷' or @submit-text='鍙戝竷绗旇' or @submit-text='鍙戝竷浣滃搧']",
                "xpath://xhs-publish-btn[@is-publish='true' and @save-disabled='false']",
            }:
                return self.button
            return None

    owner = FakeOwner()
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    assert engine._click_xiaohongshu_primary_publish_button(owner, None) is True
    assert owner.button.clicked is False
    assert owner.inner_button.clicked is True


def test_ensure_xiaohongshu_upload_mode_uses_recursive_frame_search(monkeypatch) -> None:
    class FakeOwner:
        def __init__(self) -> None:
            self.js_scripts: list[str] = []
            self.calls = 0

        def run_js(self, script: str, *_args: Any) -> Any:
            self.js_scripts.append(script)
            self.calls += 1
            if self.calls == 1:
                return {"state": "clicked", "target": "上传视频", "other": "上传图文", "current": "", "available": ["上传视频"]}
            return {"state": "already", "target": "上传视频", "other": "上传图文", "current": "上传视频", "available": ["上传视频"]}

    owner = FakeOwner()
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    assert engine._ensure_xiaohongshu_upload_mode(owner, None, prefer_video=True) is True
    assert len(owner.js_scripts) == 2
    assert "collectRoots(document)" in owner.js_scripts[0]


def test_is_xiaohongshu_publish_button_context_rejects_sidebar_note() -> None:
    assert not engine._is_xiaohongshu_publish_button_context(
        ("\u53d1\u5e03\u7b14\u8bb0", "\u9996\u9875", "\u7b14\u8bb0\u7ba1\u7406"),
        "\u53d1\u5e03\u7b14\u8bb0",
        "",
        rect_top=120,
        viewport_height=1000,
    )

    assert engine._is_xiaohongshu_publish_button_context(
        ("footer-area",),
        "\u53d1\u5e03",
        "",
        rect_top=820,
        viewport_height=1000,
    )
    assert engine._is_xiaohongshu_publish_button_context(
        ("footer-area",),
        "\u53d1\u5e03\u7b14\u8bb0",
        "",
        rect_top=820,
        viewport_height=1000,
    )
    assert engine._is_xiaohongshu_publish_button_context(
        ("footer-area",),
        "\u53d1\u5e03\u4f5c\u54c1",
        "",
        rect_top=820,
        viewport_height=1000,
    )


def test_wait_publish_feedback_rechecks_xiaohongshu_confirm_button(monkeypatch) -> None:
    state = {
        "confirmed": False,
        "confirm_calls": 0,
    }
    clock = iter(range(100))

    monkeypatch.setattr(engine.time, "time", lambda: float(next(clock)))
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        engine,
        "_read_page_snapshot",
        lambda *_args, **_kwargs: (
            "https://creator.xiaohongshu.com/publish/publish",
            "\u53d1\u5e03\u6210\u529f" if state["confirmed"] else "",
        ),
    )

    def fake_confirm(*_args: Any, **_kwargs: Any) -> bool:
        state["confirm_calls"] += 1
        state["confirmed"] = True
        return True

    monkeypatch.setattr(engine, "_click_xiaohongshu_publish_confirm_button", fake_confirm)

    engine._wait_publish_feedback(None, None, platform_name="xiaohongshu", timeout_seconds=8)

    assert state["confirm_calls"] >= 1


def test_read_page_snapshot_prefers_published_success_tab() -> None:
    class FakeCtx:
        def __init__(self, url: str, text: str) -> None:
            self.url = url
            self.text = text

        def run_js(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {"url": self.url, "text": self.text}

    class FakePage(FakeCtx):
        def __init__(self, tabs: list[FakeCtx]) -> None:
            super().__init__("about:blank", "草稿页")
            self._tabs = tabs

        def get_tabs(self, tab_type: str = "page") -> list[FakeCtx]:
            del tab_type
            return self._tabs

    blank_page = FakePage(
        [
            FakeCtx("about:blank", "发布笔记 首页 笔记管理"),
            FakeCtx("https://creator.xiaohongshu.com/publish/publish?source=&published=true", "发布成功"),
        ]
    )

    url, text = engine._read_page_snapshot(blank_page, None)

    assert url == "https://creator.xiaohongshu.com/publish/publish?source=&published=true"
    assert "发布成功" in text


def test_read_xiaohongshu_publish_state_prefers_published_success_tab() -> None:
    class FakeCtx:
        def __init__(self, url: str, text: str, *, media_count: int = 0, editor_len: int = 0) -> None:
            self.url = url
            self.text = text
            self.media_count = media_count
            self.editor_len = editor_len

        def run_js(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {
                "url": self.url,
                "text": self.text,
                "media_count": self.media_count,
                "editor_len": self.editor_len,
                "action_texts": ["发布"] if self.media_count else [],
                "has_publish_action": self.media_count > 0,
                "has_draft_action": False,
                "has_retry_action": False,
                "publish_entry": self.media_count > 0,
                "draft_entry": False,
                "success_hint": "发布成功" in self.text,
                "progress_hint": False,
                "failure_hint": False,
                "manage_url_hint": "manage" in self.url,
                "manage_text_hint": "管理" in self.text,
            }

    class FakePage(FakeCtx):
        def __init__(self, tabs: list[FakeCtx]) -> None:
            super().__init__("about:blank", "发布笔记 首页 笔记管理", media_count=1, editor_len=12)
            self._tabs = tabs

        def get_tabs(self, tab_type: str = "page") -> list[FakeCtx]:
            del tab_type
            return self._tabs

    state = engine._read_xiaohongshu_publish_state(
        FakePage(
            [
                FakeCtx("about:blank", "发布笔记 首页 笔记管理", media_count=1, editor_len=12),
                FakeCtx(
                    "https://creator.xiaohongshu.com/publish/publish?source=&published=true",
                    "发布成功",
                    media_count=0,
                    editor_len=0,
                ),
            ]
        ),
        None,
    )

    assert state["url"] == "https://creator.xiaohongshu.com/publish/publish?source=&published=true"
    assert engine._is_xiaohongshu_publish_confirmed_from_state(state) is True


def test_read_xiaohongshu_publish_state_uses_browser_peer_tabs() -> None:
    class FakeCtx:
        def __init__(self, url: str, text: str, *, media_count: int = 0, editor_len: int = 0) -> None:
            self.url = url
            self.text = text
            self.media_count = media_count
            self.editor_len = editor_len

        def run_js(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {
                "url": self.url,
                "text": self.text,
                "media_count": self.media_count,
                "editor_len": self.editor_len,
                "action_texts": ["发布"] if self.media_count else [],
                "has_publish_action": self.media_count > 0,
                "has_draft_action": False,
                "has_retry_action": False,
                "publish_entry": self.media_count > 0,
                "draft_entry": False,
                "success_hint": "发布成功" in self.text,
                "progress_hint": False,
                "failure_hint": False,
                "manage_url_hint": "manage" in self.url,
                "manage_text_hint": "管理" in self.text,
            }

    class FakeBrowser:
        def __init__(self, tabs: list[FakeCtx]) -> None:
            self._tabs = tabs

        def get_tabs(self, tab_type: str = "page") -> list[FakeCtx]:
            del tab_type
            return self._tabs

    class FakeWorkTab(FakeCtx):
        def __init__(self, browser: FakeBrowser) -> None:
            super().__init__("about:blank", "发布笔记 首页 笔记管理", media_count=1, editor_len=12)
            self.browser = browser

    browser = FakeBrowser(
        [
            FakeCtx("about:blank", "发布笔记 首页 笔记管理", media_count=1, editor_len=12),
            FakeCtx(
                "https://creator.xiaohongshu.com/publish/publish?source=&published=true",
                "发布成功",
                media_count=0,
                editor_len=0,
            ),
        ]
    )
    state = engine._read_xiaohongshu_publish_state(FakeWorkTab(browser), None)

    assert state["url"] == "https://creator.xiaohongshu.com/publish/publish?source=&published=true"
    assert engine._is_xiaohongshu_publish_confirmed_from_state(state) is True
