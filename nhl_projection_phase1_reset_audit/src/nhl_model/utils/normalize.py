from __future__ import annotations

import re
from typing import Iterable


def sanitize_columns(columns: Iterable[str]) -> list[str]:
    out = []
    for c in columns:
        c = str(c).strip().lower()
        c = c.replace("%", "pct")
        c = c.replace("+/-", "plusminus")
        c = re.sub(r"[^a-z0-9]+", "_", c)
        c = re.sub(r"_+", "_", c).strip("_")
        out.append(c)
    return out
