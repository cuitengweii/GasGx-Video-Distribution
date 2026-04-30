from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_task_center_renders_account_name_in_task_rows() -> None:
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")

    assert "const accountLabel = account" in app
    assert "${accountLabel}" in app
    assert "task-account-name" in app
    assert "<strong>#${task.id} ${platformName(task.platform)}</strong>" in app
    assert "<strong>#${task.id} ${task.task_type} ${platformName(task.platform)}</strong>" not in app
    assert ".task-title-wrap" in css
    assert ".task-account-name" in css
