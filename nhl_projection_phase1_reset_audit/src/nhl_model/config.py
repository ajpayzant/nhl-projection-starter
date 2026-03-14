from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _detect_project_root() -> Path:
    env_root = os.getenv("NHL_MODEL_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists() and (parent / "requirements.txt").exists():
            return parent
    return here.parents[2]


@dataclass(frozen=True)
class ProjectPaths:
    project_root: Path
    data_raw: Path
    data_processed: Path
    logs: Path
    templates: Path


ROOT = _detect_project_root()
PATHS = ProjectPaths(
    project_root=ROOT,
    data_raw=ROOT / "data" / "raw",
    data_processed=ROOT / "data" / "processed",
    logs=ROOT / "logs",
    templates=ROOT / "templates",
)

for p in [PATHS.data_raw, PATHS.data_processed, PATHS.logs, PATHS.templates]:
    p.mkdir(parents=True, exist_ok=True)
