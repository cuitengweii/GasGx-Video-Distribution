from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_terminal_publish_merges_single_window_response() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert "function mergeTerminalWindowState(nextState, windowId)" in app_js
    assert 'const targetWindowId = String(windowId || "").trim();' in app_js
    assert 'const nextWindow = nextWindows.find((item) => String(item?.id || "") === targetWindowId);' in app_js
    assert "const mergedWindows = currentWindows.map((item) => {" in app_js
    assert "if (!replaced) mergedWindows.push(nextWindow);" in app_js
    assert "mergeTerminalWindowState(nextState, windowId);" in app_js


def test_terminal_publish_keeps_terminal_polling_hook() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert "/api/terminal-execution/poll" in app_js
    assert "refreshTerminalWechatState" in app_js


def test_terminal_wechat_top_account_module_is_rendered() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    app_css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")

    assert "terminal-wechat-quick-action" in app_js
    assert "data-terminal-wechat-login-selected" in app_js
    assert "data-terminal-wechat-publish-selected" in app_js
    assert "/api/accounts/${selected.accountId}/platforms/wechat/emergency-publish" in app_js
    assert "terminalWechatHasActiveSupplementalRuns" in app_js
    assert "label: accountName" in app_js
    assert "#terminal-execution[data-terminal-route=\"wechat\"] .terminal-wechat-page" in app_css
    assert "width: 100%;" in app_css
