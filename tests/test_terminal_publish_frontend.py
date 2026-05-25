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
    assert "TERMINAL_WECHAT_AUTO_STATS_KEY" in app_js
    assert "TERMINAL_WECHAT_AUTO_COMMENT_KEY" in app_js
    assert "TERMINAL_WECHAT_AUTO_PRIVATE_MESSAGE_KEY" in app_js
    assert "TERMINAL_WECHAT_AUTO_COMMENT_LIMIT_KEY" in app_js
    assert "TERMINAL_WECHAT_AUTO_PRIVATE_MESSAGE_LIMIT_KEY" in app_js
    assert "terminal-wechat-auto-stats-toggle" in app_js
    assert "terminal-wechat-auto-comment-toggle" in app_js
    assert "terminal-wechat-auto-private-message-toggle" in app_js
    assert "terminal-wechat-auto-comment-limit" in app_js
    assert "terminal-wechat-auto-private-message-limit" in app_js
    assert "triggerTerminalWechatAutoSideTasks(targetAccountId);" in app_js
    assert "triggerTerminalWechatAutoSideTasks(selected.accountId);" in app_js
    assert "open_capture_in_new_tab: true" in app_js
    assert "capture_tab_foreground: true" in app_js
    assert "keep_capture_tab_open: true" in app_js
    assert "/api/jobs/matrix-wechat/engagement/run-now" in app_js
    assert "terminalWechatHasActiveSupplementalRuns" in app_js
    assert "label: accountName" in app_js
    assert "#terminal-execution[data-terminal-route=\"wechat\"] .terminal-wechat-page" in app_css
    assert ".terminal-wechat-auto-stats-toggle" in app_css
    assert ".terminal-wechat-auto-engagement-toggle" in app_css
    assert ".terminal-wechat-auto-engagement-limit-field" in app_css
    assert ".terminal-wechat-auto-stats-hint" in app_css
    assert "width: 100%;" in app_css


def test_terminal_wechat_selected_account_list_uses_all_wechat_accounts() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert "function terminalWechatSelectableAccounts()" in app_js
    assert "const windows = Array.isArray(state.terminalExecution?.windows) ? state.terminalExecution.windows : [];" in app_js
    assert "const accountsById = new Map((state.accounts || []).map((account) => [String(account.id || \"\"), account]));" in app_js
    assert "const seenIds = new Set();" in app_js
    assert "for (const window of windows)" in app_js
    assert "const liveAccount = accountsById.get(String(accountId)) || {};" in app_js
    assert "if (selectable.length) return selectable;" in app_js
    assert "return terminalWechatSelectableAccounts().map((account) => {" in app_js
    assert "const accountName = cleanAccountDisplayName(account);" in app_js
    assert "const statusLabel = accountStatusLabel(account.status);" not in app_js
    assert "label: accountName" in app_js
