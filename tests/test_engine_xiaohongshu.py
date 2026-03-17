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
