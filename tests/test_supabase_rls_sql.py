from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _sql(name: str) -> str:
    return (ROOT / "config" / "supabase" / name).read_text(encoding="utf-8").lower()


def test_brand_baseline_supabase_sql_parts_stay_under_editor_limit() -> None:
    parts_dir = ROOT / "config" / "supabase" / "brand_baseline_parts"
    parts = sorted(parts_dir.glob("*.sql"))
    assert [part.name for part in parts] == [
        "01_core_tables.sql",
        "02_app_tables.sql",
        "03_functions_and_rls.sql",
        "04_account_policies.sql",
        "05_settings_policies.sql",
        "06_video_seed_policies.sql",
    ]
    for part in parts:
        assert len(part.read_text(encoding="utf-8").splitlines()) <= 100


def test_control_plane_supabase_sql_defines_role_based_rls() -> None:
    sql = _sql("control_plane.sql")
    for token in ("owner", "admin", "operator", "viewer"):
        assert token in sql
    for table in ("brand_instances", "brand_templates", "upgrade_runs", "upgrade_run_items", "control_members"):
        assert f"alter table {table} enable row level security" in sql
    assert "control_has_role(array['owner', 'admin'])" in sql
    assert "control_has_role(array['owner', 'admin', 'operator'])" in sql


def test_brand_baseline_supabase_sql_defines_role_based_rls() -> None:
    sql = _sql("brand_baseline.sql")
    for token in ("owner", "admin", "operator", "viewer"):
        assert token in sql
    for table in (
        "matrix_accounts",
        "account_platforms",
        "browser_profiles",
        "automation_tasks",
        "video_stats_snapshots",
        "ai_robot_configs",
        "ai_robot_messages",
        "brand_settings",
        "schema_migrations",
        "app_settings",
        "analytics_items",
        "video_matrix_assets",
        "video_matrix_jobs",
        "app_seed_runs",
        "brand_members",
    ):
        assert f"alter table {table} enable row level security" in sql
    assert "brand_has_role(array['owner', 'admin'])" in sql
    assert "brand_has_role(array['owner', 'admin', 'operator'])" in sql


def test_brand_baseline_supabase_sql_defines_database_initialization_tables() -> None:
    sql = _sql("brand_baseline.sql")
    for table in ("app_settings", "analytics_items", "video_matrix_assets", "video_matrix_jobs", "app_seed_runs"):
        assert f"create table if not exists {table}" in sql
    assert "payload_json jsonb" in sql
    assert "assets_json jsonb" in sql
    assert "unique(section, item_key)" in sql


def test_brand_baseline_supabase_sql_defines_dashboard_summary_rpc() -> None:
    sql = _sql("brand_baseline.sql")
    assert "create or replace function dashboard_summary()" in sql
    assert "returns table" in sql
    assert "from matrix_accounts" in sql
    assert "from account_platforms where enabled = 1" in sql
    assert "from automation_tasks where status in ('pending', 'running')" in sql
    assert "sum(views) from video_stats_snapshots" in sql
