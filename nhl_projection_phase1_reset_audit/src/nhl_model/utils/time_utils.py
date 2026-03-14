from __future__ import annotations


def mmss_to_seconds(value: str | None) -> int | None:
    if value is None:
        return None
    value = str(value).strip()
    if not value or ":" not in value:
        return None
    parts = value.split(":")
    if len(parts) != 2:
        return None
    try:
        minutes = int(parts[0])
        seconds = int(parts[1])
        return minutes * 60 + seconds
    except Exception:
        return None
