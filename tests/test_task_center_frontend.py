from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_task_center_renders_account_name_in_task_rows() -> None:
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "styles.css").read_text(encoding="utf-8")

    assert "function taskAccountLabel(task)" in app
    assert "${accountLabel}" in app
    assert "task-account-name" in app
    assert "<strong>#${task.id} ${platformName(task.platform)}</strong>" in app
    assert "<strong>#${task.id} ${task.task_type} ${platformName(task.platform)}</strong>" not in app
    html = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "index.html").read_text(encoding="utf-8")
    assert '<select name="task_type">' in html
    task_type_select = html.split('<select name="task_type">', 1)[1].split("</select>", 1)[0]
    assert '<option value="draft" selected>保存草稿</option>' in task_type_select
    assert "taskFilters" in app
    assert '["draft", "保存草稿"]' in app
    assert '["publish", "自动发布"]' in app
    assert '["comment", "自动评论"]' in app
    assert '["message", "自动私信"]' in app
    assert '["stats", "数据统计"]' in app
    assert "data-task-filter=\"account\"" in app
    assert "function taskAccountFilterOptions()" in app
    assert "${taskAccountFilterOptions()}" in app
    assert "taskFilterOptions(state.tasks, (task) => task.account_id, taskAccountLabel)" not in app
    assert "data-task-filter=\"platform\"" in app
    assert "data-task-filter=\"status\"" in app
    assert "data-task-filter=\"taskType\"" in app
    assert "data-task-select-all" in app
    assert "data-task-select=\"${task.id}\"" in app
    assert "data-task-bulk-status=\"paused\"" in app
    assert "data-task-bulk-delete" in app
    assert "/api/tasks/bulk-status" in app
    assert "/api/tasks/bulk-delete" in app
    assert "任务类型：${taskTypeLabel(task.task_type)}" in app
    assert 'TASK_TYPE_OPTIONS.map(([value, label])' in app
    assert "if (text === \"paused\") return \"已暂停\"" in app
    assert ".task-toolbar" in css
    assert ".task-filter-grid" in css
    assert ".task-row-check" in css
    assert ".task-title-wrap" in css
    assert ".task-account-name" in css
