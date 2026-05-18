from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_terminal_publish_merges_single_window_response() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert "function mergeTerminalWindowState(nextState, windowId)" in app_js
    assert "const targetWindowId = String(windowId || \"\").trim();" in app_js
    assert "const nextWindow = nextWindows.find((item) => String(item?.id || \"\") === targetWindowId);" in app_js
    assert "const mergedWindows = currentWindows.map((item) => {" in app_js
    assert "if (!replaced) mergedWindows.push(nextWindow);" in app_js
    assert "mergeTerminalWindowState(nextState, windowId);" in app_js


def test_terminal_publish_is_manual_confirm_only_without_polling() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert "/api/terminal-execution/poll" not in app_js
    assert "function scheduleTerminalPublishFollowup" not in app_js
