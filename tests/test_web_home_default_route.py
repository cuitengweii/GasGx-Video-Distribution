from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_web_home_defaults_to_overview_on_empty_hash() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert 'if (!raw) return { view: "overview", route: "hub" };' in app_js
    assert 'if (raw === "terminal-execution") return { view: "terminal-execution", route: "hub" };' in app_js
    assert 'data-terminal-close-config' in app_js
    assert 'terminal-close-btn' in app_js
    assert 'if (view === "terminal-execution" && updateHash) {' in app_js
    assert 'if (view === "terminal-execution" && terminalCurrentRoute() === "hub") return;' in app_js
    assert 'const showTerminalLoading = view === "terminal-execution" && terminalCurrentRoute() !== "hub";' in app_js
    assert 'loading: terminalCurrentRoute() !== "hub",' in app_js
    assert 'if (isLoading && !isHubRoute) {' in app_js
    assert "Page entry is passive; browser opening only happens after an explicit operator action." in app_js


def test_terminal_manual_confirm_helper_exists_before_status_render() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    helper_index = app_js.index("function terminalAccountIsManualConfirmable(account)")
    status_index = app_js.index("function terminalStatusNeedsManualConfirm(account, statusText)")

    assert helper_index < status_index
    assert "return terminalAccountHasLegacyManualConfirmFailure(account);" in app_js


def test_terminal_confirm_next_shows_persistent_loading_state() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")

    assert "confirming_next" in app_js
    assert "const terminalConfirmingWindowIds = new Set();" in app_js
    assert "if (terminalConfirmingWindowIds.size) return;" in app_js
    assert "terminalConfirmingWindowIds.add(String(windowId));" in app_js
    assert "terminalConfirmingWindowIds.delete(String(windowId));" in app_js
    assert "发布后进入下一账号" in app_js
    assert "正在进入下一个账号" in app_js
    assert "正在进入下一个..." in app_js
    assert "replaceTerminalWindowNode(refreshRoot, windowId, pendingWindow" in app_js
    assert ".terminal-task-column.confirming-next" in css
    assert ".terminal-col-btn.secondary.strong-loading:disabled" in css
    assert ".terminal-col-btn.strong-loading .btn-spinner" in css
    assert "border-top-color: #fff;" in css


def test_terminal_entry_does_not_show_flow_guide_modal() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert "showTerminalFlowGuideModal" not in app_js
    assert "每次进入平台时展示" not in app_js
    assert "流程错误节点" not in app_js


def test_terminal_wechat_entry_does_not_start_hot_polling() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    web_py = (ROOT / "src" / "gasgx_distribution" / "web.py").read_text(encoding="utf-8")

    assert "let terminalLivePollingEnabled = false;" in app_js
    assert "Boolean(state.terminalExecution?.login_started) && terminalLivePollingEnabled" in app_js
    assert "window.setInterval(pollOnce, 3000)" in app_js
    assert "window.setInterval(updateTerminalQrCountdowns, 1000)" in app_js
    assert "body: JSON.stringify({ allow_browser_open: true, allow_login_probe: true })" in app_js
    assert 'await warmTerminalBrowsersBeforeRender("正在恢复登录浏览器，请稍候...");' not in app_js
    assert "class TerminalPollPayload" in web_py
    assert "allow_browser_open: bool = False" in web_py
    assert "allow_login_probe: bool = False" in web_py
