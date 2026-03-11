from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _looks_like_repo_root(path: Path) -> bool:
    return (
        (path / 'pyproject.toml').exists()
        and (path / 'requirements.txt').exists()
        and (path / 'src' / 'nhl_model').exists()
    )


def _discover_root() -> Path:
    env_root = os.getenv('NHL_MODEL_ROOT')
    if env_root:
        candidate = Path(env_root).expanduser().resolve()
        if _looks_like_repo_root(candidate):
            return candidate

    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents]:
        if _looks_like_repo_root(candidate):
            return candidate

    here = Path(__file__).resolve()
    for candidate in [*here.parents]:
        if _looks_like_repo_root(candidate):
            return candidate

    return here.parents[3]


@dataclass(frozen=True)
class Paths:
    root: Path = _discover_root()

    @property
    def project_root(self) -> Path:
        return self.root

    @property
    def data_raw(self) -> Path:
        return self.root / 'data' / 'raw'

    @property
    def data_processed(self) -> Path:
        return self.root / 'data' / 'processed'

    @property
    def logs(self) -> Path:
        return self.root / 'logs'

    def ensure(self) -> None:
        for p in [self.data_raw, self.data_processed, self.logs]:
            p.mkdir(parents=True, exist_ok=True)


PATHS = Paths()
PATHS.ensure()
