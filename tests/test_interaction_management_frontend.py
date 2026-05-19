from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_interaction_management_ui_is_wired_into_shell() -> None:
    html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "index.html").read_text(encoding="utf-8")
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")

    assert 'data-view="interaction-management"' in html
    assert 'data-interaction-tab="comment"' not in html
    assert 'data-interaction-tab="private_message"' not in html
    assert 'data-interaction-tab="barrage"' not in html
    assert 'id="interaction-management-root"' in html

    assert '/api/interaction-management/config' in app_js
    assert '/api/interaction-management/status' in app_js
    assert '/api/interaction-management/comment/run' in app_js
    assert '/api/interaction-management/private-msg/run' in app_js
    assert 'function renderInteractionManagement()' in app_js
    assert 'root.querySelector("#interaction-management-save")?.addEventListener' in app_js
    assert 'root.querySelector("[data-interaction-comment-run]")?.addEventListener' in app_js
    assert 'root.querySelector("[data-interaction-private-run]")?.addEventListener' in app_js
    assert 'root.querySelectorAll("[data-interaction-tab]")' in app_js

    assert '.interaction-management-layout' in css
    assert '.interaction-tab-switcher' in css
    assert '.interaction-placeholder' in css
    assert '.system-status.success' in css
