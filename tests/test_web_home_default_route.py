from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_web_home_defaults_to_overview_on_empty_hash() -> None:
    app_js = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")

    assert 'if (!raw) return { view: "overview", route: "hub" };' in app_js
    assert 'if (raw === "terminal-execution") return { view: "terminal-execution", route: "hub" };' in app_js
