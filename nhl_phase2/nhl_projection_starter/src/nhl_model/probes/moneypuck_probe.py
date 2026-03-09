from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from nhl_model.config import PATHS
from nhl_model.utils.http import SimpleHTTPClient, write_text


@dataclass
class MoneyPuckLink:
    href: str
    text: str
    absolute_url: str


class MoneyPuckProbe:
    BASE_URL = "https://www.moneypuck.com/data.htm"

    def __init__(self):
        self.http = SimpleHTTPClient(timeout=30)
        self.out_dir = PATHS.data_raw / "probes" / "moneypuck"
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict[str, pd.DataFrame]:
        result = self.http.get(self.BASE_URL, expect_json=False)
        html = result.text or ""
        write_text(self.out_dir / "data_page.html", html)

        soup = BeautifulSoup(html, "lxml")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(" ", strip=True)
            abs_url = urljoin(self.BASE_URL, href)
            if ".csv" in href.lower() or "data" in href.lower() or "dictionary" in href.lower():
                links.append(MoneyPuckLink(href=href, text=text, absolute_url=abs_url))

        links_df = pd.DataFrame([asdict(x) for x in links]).drop_duplicates()
        links_df.to_csv(self.out_dir / "extracted_links.csv", index=False)

        summary_df = pd.DataFrame(
            [
                {
                    "url": self.BASE_URL,
                    "status_code": result.status_code,
                    "content_type": result.content_type,
                    "elapsed_seconds": round(result.elapsed_seconds, 4),
                    "contains_2008_2009": "2008-2009" in html,
                    "contains_game_level": "game level" in html.lower(),
                    "contains_lines_pairings": "lines/defensive pairings" in html.lower() or "lines/pairings" in html.lower(),
                    "link_count": len(links_df),
                }
            ]
        )
        summary_df.to_csv(self.out_dir / "summary.csv", index=False)
        return {"summary": summary_df, "links": links_df}


def run_probe() -> dict[str, pd.DataFrame]:
    return MoneyPuckProbe().run()


if __name__ == "__main__":
    out = run_probe()
    for name, df in out.items():
        print(f"\n{name}\n", df.head())
