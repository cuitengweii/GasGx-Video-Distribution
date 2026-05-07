from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_public_settings_does_not_render_platform_override_cards() -> None:
    html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "index.html").read_text(encoding="utf-8")
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    settings_section = html.split('<section id="settings"', 1)[1].split('<section id="ai-robot"', 1)[0]

    assert 'id="platform-settings-list"' not in settings_section
    assert "平台独立配置" not in settings_section
    assert "平台发布配置" in html
    assert "Object.entries(existingPlatforms)" in app
