from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class DistributionPaths:
    repo_root: Path
    runtime_root: Path
    profiles_root: Path
    database_path: Path
    control_database_path: Path
    brand_databases_root: Path

    def ensure(self) -> None:
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.profiles_root.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.control_database_path.parent.mkdir(parents=True, exist_ok=True)
        self.brand_databases_root.mkdir(parents=True, exist_ok=True)


def get_paths() -> DistributionPaths:
    root = repo_root()
    runtime_root = root / "runtime"
    return DistributionPaths(
        repo_root=root,
        runtime_root=runtime_root,
        profiles_root=root / "profiles" / "matrix",
        database_path=runtime_root / "gasgx_distribution.db",
        control_database_path=runtime_root / "control_plane.db",
        brand_databases_root=runtime_root / "brands",
    )
