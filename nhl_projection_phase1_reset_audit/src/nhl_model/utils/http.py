from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class HTTPResult:
    status_code: int
    url: str
    text: str | None
    json_data: Any | None
    content_type: str | None


class SimpleHTTPClient:
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            }
        )

    def get(self, url: str, expect_json: bool = False) -> HTTPResult:
        resp = self.session.get(url, timeout=self.timeout)
        ctype = resp.headers.get("Content-Type")
        if expect_json:
            try:
                data = resp.json()
            except Exception:
                data = None
            return HTTPResult(resp.status_code, resp.url, resp.text, data, ctype)
        return HTTPResult(resp.status_code, resp.url, resp.text, None, ctype)
