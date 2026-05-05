from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_overview_keeps_operator_friendly_entry_layout() -> None:
    html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "index.html").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")

    assert "今日运营重点" in html
    assert "常用入口" in html
    assert "待处理提醒" in html
    assert "视频生成" in html
    assert "执行队列" in html
    assert 'data-view="tasks" data-permission="tasks">任务中心</button>' in html
    assert 'data-view="terminal-execution" data-permission="terminal-execution">终端执行</button>' in html
    assert html.index('data-view="tasks"') < html.index('data-view="terminal-execution"') < html.index('data-view="stats"')
    assert "terminal-init-modal" in html
    assert "terminal-modal-actions" in html
    assert "data-terminal-save-config" in html
    assert "terminal-header-subtitle" in html
    assert "terminal-platform-bar" in html
    assert "terminal-platform-config-panel" in html
    assert '<div class="page-toolbar">' not in html
    assert 'id="refresh"' not in html
    assert 'data-quick-view="notifications" data-permission="notifications">查看提醒</button>' not in html
    assert 'data-quick-view="tasks" data-permission="tasks">任务队列</button>' not in html
    assert 'data-quick-view="video-matrix" data-permission="video-matrix">生成视频</button>' not in html
    assert "终端前置配置区" in html
    assert "确认配置并进入" not in html
    assert 'id="terminal-start-system"' not in html
    assert 'id="terminal-start-login">获取登录二维码</button>' in html
    assert 'data-quick-view="notifications"' in html
    assert 'data-quick-view="tasks"' in html
    assert 'data-quick-view="video-matrix"' in html
    assert ".operator-layout" in css
    assert ".operation-focus-grid" in css
    assert ".operation-route-grid" in css
    assert ".platform-capability-panel" in css
    assert ".terminal-console" in css
    assert ".terminal-task-column" in css
    assert ".terminal-qr-placeholder" in css
    assert ".terminal-modal-actions" in css
    assert ".terminal-workspace.terminal-workspace-wechat" in css
    assert ".terminal-window-actions" in css
    assert "position: fixed;" in css
    assert "left: 260px;" in css
    assert "grid-template-columns: repeat(5, minmax(0, 1fr));" in css
    assert "overflow-x: hidden;" in css
    assert "min-width: 0;" in css
    assert "pointer-events: none;" in css
    assert "pointer-events: auto;" in css

    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    assert "/api/terminal-execution/state" in app
    assert "/api/terminal-execution/start" in app
    assert "/api/terminal-execution/start-login" in app
    assert "/api/terminal-execution/poll" not in app
    assert "/api/terminal-execution/windows/" in app
    assert "/accounts/" in app
    assert "/qr" in app
    assert "confirm-publish-success" in app
    assert "data-terminal-confirm-success" in app
    assert "data-terminal-qr-refresh" in app
    assert "#terminal-config-list" in app
    assert "#terminal-save-config, [data-terminal-save-config]" in app
    assert "#terminal-start-system" not in app
    assert "!state.terminalExecution.initialized || state.terminalConfigOpen" in app
    assert "const qrVisible = loginStarted && window.qr_url;" in app
    assert "state.terminalQrVisible && loginStarted && window.qr_url" not in app
    assert "视频号采用每日登录扫码队列；登录后由矩阵调度继续推进。" not in app
    assert "一次登录长期有效；失效后重新检测或重新登录。" in app
    assert "setInterval(async" not in app
    assert "installGlobalButtonLoading" in app


def test_terminal_config_does_not_start_login_side_effects() -> None:
    service = (ROOT / "src" / "gasgx_distribution" / "service.py").read_text(encoding="utf-8")
    configure_block = service.split("def start_terminal_execution", 1)[1].split("def start_terminal_login", 1)[0]
    login_block = service.split("def start_terminal_login", 1)[1].split("def _queue_terminal_draft_task", 1)[0]

    assert "open_account_browser" not in configure_block
    assert "_terminal_qr_data_url" not in configure_block
    assert "_write_terminal_qr_cache" not in configure_block
    assert "previous_login_started" in configure_block
    assert "open_account_browser" in login_block
    assert "_write_terminal_qr_cache" in login_block
