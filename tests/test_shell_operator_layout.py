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
    assert '<div class="page-toolbar">' not in html
    assert 'id="refresh"' not in html
    assert 'data-quick-view="notifications" data-permission="notifications">查看提醒</button>' not in html
    assert 'data-quick-view="tasks" data-permission="tasks">任务队列</button>' not in html
    assert 'data-quick-view="video-matrix" data-permission="video-matrix">生成视频</button>' not in html
    assert "终端前置配置区" in html
    assert "确认并初始化执行矩阵" in html
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
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    assert "/api/terminal-execution/state" in app
    assert "#terminal-config-list" in app
    assert "加载运营微信配置..." in app
