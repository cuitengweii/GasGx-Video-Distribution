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
    assert 'data-quick-view="notifications"' in html
    assert 'data-quick-view="tasks"' in html
    assert 'data-quick-view="video-matrix"' in html
    assert ".operator-layout" in css
    assert ".operation-focus-grid" in css
    assert ".operation-route-grid" in css
    assert ".platform-capability-panel" in css
