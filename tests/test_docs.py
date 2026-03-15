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
    ]
    missing = [str(path) for path in required if not path.exists()]
    assert not missing, missing
