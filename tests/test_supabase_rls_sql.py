from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _sql(name: str) -> str:
    return (ROOT / "config" / "supabase" / name).read_text(encoding="utf-8").lower()


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
        "brand_members",
    ):
        assert f"alter table {table} enable row level security" in sql
    assert "brand_has_role(array['owner', 'admin'])" in sql
    assert "brand_has_role(array['owner', 'admin', 'operator'])" in sql
