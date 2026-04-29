from pathlib import Path


def test_required_docs_exist() -> None:
    root = Path(__file__).resolve().parents[1]
    required = [
        root / "docs" / "README.md",
        root / "docs" / "projects" / "cybercar" / "CYBERCAR_STATE.md",
        root / "docs" / "projects" / "cybercar" / "CYBERCAR_DECISIONS.md",
        root / "docs" / "projects" / "cybercar" / "CYBERCAR_ARCHITECTURE.md",
        root / "docs" / "projects" / "cybercar" / "CYBERCAR_LESSONS.md",
        root / "docs" / "runbooks" / "LOCAL_RUNTIME_SETUP.md",
        root / "docs" / "runbooks" / "LOGIN_RECOVERY.md",
        root / "docs" / "runbooks" / "IMMEDIATE_PIPELINE.md",
        root / "docs" / "help" / "WORKSPACE_OVERVIEW.md",
        root / "docs" / "help" / "ACCOUNT_MATRIX.md",
        root / "docs" / "help" / "PUBLIC_SETTINGS.md",
        root / "docs" / "help" / "VIDEO_GENERATION_WORKBENCH.md",
        root / "docs" / "help" / "TASK_CENTER.md",
        root / "docs" / "help" / "DATA_STATISTICS.md",
        root / "docs" / "help" / "AI_ROBOT.md",
        root / "docs" / "help" / "USER_CENTER.md",
        root / "docs" / "help" / "NOTIFICATION_CENTER.md",
        root / "docs" / "help" / "SYSTEM_SETTINGS.md",
        root / "docs" / "help" / "DEVELOPER_VIDEO_GENERATION_ALGORITHM.md",
    ]
    missing = [str(path) for path in required if not path.exists()]
    assert not missing, missing
