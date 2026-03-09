from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _default_root() -> Path:
    env_root = os.getenv("NHL_MODEL_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class Paths:
    root: Path = _default_root()

    @property
    def data_raw(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def data_processed(self) -> Path:
        return self.root / "data" / "processed"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    def ensure(self) -> None:
        for p in [self.data_raw, self.data_processed, self.logs]:
            p.mkdir(parents=True, exist_ok=True)


PATHS = Paths()
PATHS.ensure()
