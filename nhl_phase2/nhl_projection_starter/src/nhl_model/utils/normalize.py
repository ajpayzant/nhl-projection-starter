from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def safe_get(obj: Any, path: list[Any], default: Any = None) -> Any:
    cur = obj
    for key in path:
        if isinstance(cur, dict):
            if key not in cur:
                return default
            cur = cur[key]
        elif isinstance(cur, list) and isinstance(key, int):
            if key < 0 or key >= len(cur):
                return default
            cur = cur[key]
        else:
            return default
    return cur


def first_non_null(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is not None:
            return value
    return default


def coerce_name(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ["default", "full", "name", "en"]:
            if value.get(key):
                return str(value[key])
    return str(value)


def flatten_scalar_dict(d: dict[str, Any], *, prefix: str = "", max_depth: int = 2, _depth: int = 0) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in d.items():
        col = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict) and _depth < max_depth:
            nested = flatten_scalar_dict(value, prefix=f"{col}_", max_depth=max_depth, _depth=_depth + 1)
            if nested:
                out.update(nested)
            else:
                out[col] = str(value)
        elif isinstance(value, list):
            if all(not isinstance(x, (dict, list)) for x in value):
                out[col] = " | ".join(map(str, value))
            else:
                out[col] = str(value)
        else:
            out[col] = value
    return out


def sanitize_columns(columns: Iterable[str]) -> list[str]:
    cleaned: list[str] = []
    for col in columns:
        value = str(col).strip().replace("%", "pct").replace("/", "_per_")
        value = value.replace(" ", "_").replace("-", "_").replace(".", "_")
        while "__" in value:
            value = value.replace("__", "_")
        cleaned.append(value.lower())
    return cleaned
