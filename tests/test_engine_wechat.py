from __future__ import annotations

from typing import Any

import pytest

from cybercar import engine


def test_is_collection_match_strips_counter_suffix() -> None:
    assert engine._is_collection_match("赛博皮卡-天津港现车 共11个内容", "赛博皮卡-天津港现车")
    assert engine._is_collection_match(" 添加到合集 赛博皮卡测评 ", "赛博皮卡测评")


def test_select_collection_supports_finder_card_picker(monkeypatch: pytest.MonkeyPatch) -> None:
    states = [
        {"hasField": True, "current": "", "source": "empty"},
        {"hasField": True, "current": "赛博皮卡-天津港现车 共11个内容", "source": "finder-card"},
        {"hasField": True, "current": "赛博皮卡-天津港现车 共11个内容", "source": "finder-card"},
    ]

    def fake_get_collection_state(_: Any) -> dict[str, Any]:
        if len(states) > 1:
            return states.pop(0)
        return states[0]

    class FakeCtx:
        def __init__(self) -> None:
            self.select_scripts: list[str] = []

        def run_js(self, script: str, *args: Any) -> Any:
            if "function setInputValue" in script:
                self.select_scripts.append(script)
                assert ".finder-card .post-album-action-text" in script
                assert ".finder-common-dialog" in script
                assert "visible_options" in script
                assert args == ("赛博皮卡-天津港现车",)
                return {
                    "state": "clicked",
                    "option": "赛博皮卡-天津港现车",
                    "visible_options": ["赛博皮卡-天津港现车"],
                }
            return True

    monkeypatch.setattr(engine, "_get_collection_state", fake_get_collection_state)
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *args, **kwargs: False)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    ctx = FakeCtx()
    engine._select_collection(ctx, "赛博皮卡-天津港现车")

    assert ctx.select_scripts


def test_select_collection_error_reports_visible_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        engine,
        "_get_collection_state",
        lambda _ctx: {"hasField": True, "current": "", "source": "empty"},
    )
    monkeypatch.setattr(engine, "_click_first_matching_button", lambda *args, **kwargs: False)
    monkeypatch.setattr(engine.time, "sleep", lambda *_args, **_kwargs: None)

    class FakeCtx:
        def run_js(self, script: str, *args: Any) -> Any:
            if "visible_options" in script and args:
                return {
                    "state": "option_not_found",
                    "visible_options": ["赛博皮卡-天津港现车", "赛博皮卡预定"],
                }
            return True

    with pytest.raises(RuntimeError, match="可见选项=赛博皮卡-天津港现车,赛博皮卡预定"):
        engine._select_collection(FakeCtx(), "赛博皮卡精选")
