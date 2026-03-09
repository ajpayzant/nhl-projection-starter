from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NHLProjectionStarter/0.1; +https://example.local)",
    "Accept": "application/json, text/html, */*",
}


@dataclass
class FetchResult:
    url: str
    status_code: int
    elapsed_seconds: float
    content_type: str
    text: str | None = None
    json_data: Any | None = None


class HTTPError(RuntimeError):
    pass


class SimpleHTTPClient:
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def get(self, url: str, expect_json: bool = False, params: dict[str, Any] | None = None) -> FetchResult:
        start = time.perf_counter()
        response = self.session.get(url, timeout=self.timeout, params=params)
        elapsed = time.perf_counter() - start
        ctype = response.headers.get("Content-Type", "")

        if response.status_code >= 400:
            raise HTTPError(f"GET {url} failed with status {response.status_code}")

        if expect_json:
            try:
                payload = response.json()
            except Exception as exc:  # pragma: no cover
                raise HTTPError(f"Expected JSON from {url} but parsing failed: {exc}") from exc
            return FetchResult(
                url=url,
                status_code=response.status_code,
                elapsed_seconds=elapsed,
                content_type=ctype,
                json_data=payload,
                text=None,
            )

        return FetchResult(
            url=url,
            status_code=response.status_code,
            elapsed_seconds=elapsed,
            content_type=ctype,
            text=response.text,
            json_data=None,
        )


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
