from __future__ import annotations

from cybercar import engine


def test_normalize_douyin_collection_value_handles_unbalanced_parenthesis() -> None:
    # Regression: malformed collection strings should not crash regex handling.
    assert engine._normalize_douyin_collection_value("(collection") == "(collection"


def test_click_wechat_primary_publish_button_accepts_primary_class_selector(monkeypatch) -> None:
    clicked = {"value": False}

    class FakeButton:
        def run_js(self, script: str):
            if "return !!(" in script:
                return False
            return None

        def click(self, by_js: bool = False) -> None:
            del by_js
            clicked["value"] = True

    class FakeOwner:
        def __init__(self) -> None:
            self.button = FakeButton()
            self.selectors: list[str] = []

        def ele(self, selector: str, timeout: float = 0):
            del timeout
            self.selectors.append(selector)
            if selector == "css:.form-btns button.weui-desktop-btn_primary":
                return self.button
            return None

        def run_js(self, _script: str):
            raise AssertionError("JS fallback should not run when primary selector click succeeds")

    owner = FakeOwner()
    monkeypatch.setattr(engine, "_is_visible_element", lambda _ele: True)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._click_wechat_primary_publish_button(owner, None, timeout_seconds=2)

    assert result is True
    assert clicked["value"] is True
    assert "css:.form-btns button.weui-desktop-btn_primary" in owner.selectors


def test_click_save_draft_button_accepts_temp_save_selector(monkeypatch) -> None:
    clicked = {"value": False}

    class FakeButton:
        def run_js(self, script: str):
            if "innerText" in script:
                return "暂存离开"
            return None

        def click(self, by_js: bool = False) -> None:
            del by_js
            clicked["value"] = True

    class FakeOwner:
        def __init__(self) -> None:
            self.button = FakeButton()
            self.selectors: list[str] = []

        def ele(self, selector: str, timeout: float = 0):
            del timeout
            self.selectors.append(selector)
            if selector == "text:暂存离开":
                return self.button
            return None

        def run_js(self, _script: str):
            raise AssertionError("JS fallback should not run when selector click succeeds")

    owner = FakeOwner()
    monkeypatch.setattr(engine, "_is_visible_element", lambda _ele: True)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._click_save_draft_button(owner)

    assert result is True
    assert clicked["value"] is True
    assert "text:暂存离开" in owner.selectors


def test_click_save_draft_button_accepts_save_as_draft_selector(monkeypatch) -> None:
    clicked = {"value": False}

    class FakeButton:
        def run_js(self, script: str):
            if "innerText" in script:
                return "保存为草稿"
            return None

        def click(self, by_js: bool = False) -> None:
            del by_js
            clicked["value"] = True

    class FakeOwner:
        def __init__(self) -> None:
            self.button = FakeButton()
            self.selectors: list[str] = []

        def ele(self, selector: str, timeout: float = 0):
            del timeout
            self.selectors.append(selector)
            if selector == "text:保存为草稿":
                return self.button
            return None

        def run_js(self, _script: str):
            raise AssertionError("JS fallback should not run when selector click succeeds")

    owner = FakeOwner()
    monkeypatch.setattr(engine, "_is_visible_element", lambda _ele: True)
    monkeypatch.setattr(engine, "_humanized_publish_reaction_pause", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    result = engine._click_save_draft_button(owner)

    assert result is True
    assert clicked["value"] is True
    assert "text:保存为草稿" in owner.selectors


def test_save_draft_raises_clear_error_when_draft_button_not_found(monkeypatch) -> None:
    monkeypatch.setattr(engine, "_reset_wechat_draft_save_probe", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(engine, "_click_save_draft_button", lambda *_args, **_kwargs: False)

    try:
        engine._save_draft(object(), retry_count=1)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert str(exc) == "Failed to locate a visible draft-save button. Please save draft manually on page."
