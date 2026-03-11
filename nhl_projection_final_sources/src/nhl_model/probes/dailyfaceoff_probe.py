from __future__ import annotations

from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from nhl_model.config import PATHS
from nhl_model.utils.http import SimpleHTTPClient, write_text


class DailyFaceoffProbe:
    STARTING_GOALIES_URL = "https://www.dailyfaceoff.com/starting-goalies"
    TEAMS_URL = "https://www.dailyfaceoff.com/teams"

    def __init__(self):
        self.http = SimpleHTTPClient(timeout=30)
        self.out_dir = PATHS.data_raw / "probes" / "dailyfaceoff"
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict[str, pd.DataFrame]:
        page_results = []

        goalies = self.http.get(self.STARTING_GOALIES_URL, expect_json=False)
        goalies_html = goalies.text or ""
        write_text(self.out_dir / "starting_goalies.html", goalies_html)
        page_results.append(
            {
                "page": "starting_goalies",
                "url": self.STARTING_GOALIES_URL,
                "status_code": goalies.status_code,
                "content_type": goalies.content_type,
                "elapsed_seconds": round(goalies.elapsed_seconds, 4),
                "contains_confirmed": "confirmed" in goalies_html.lower(),
                "contains_unconfirmed": "unconfirmed" in goalies_html.lower(),
            }
        )

        teams = self.http.get(self.TEAMS_URL, expect_json=False)
        teams_html = teams.text or ""
        write_text(self.out_dir / "teams.html", teams_html)
        page_results.append(
            {
                "page": "teams",
                "url": self.TEAMS_URL,
                "status_code": teams.status_code,
                "content_type": teams.content_type,
                "elapsed_seconds": round(teams.elapsed_seconds, 4),
                "contains_line_combinations": "line combinations" in teams_html.lower(),
                "contains_power_play": "power play" in teams_html.lower(),
            }
        )

        links_df = self._extract_links(teams_html)
        links_df.to_csv(self.out_dir / "team_links.csv", index=False)

        summary_df = pd.DataFrame(page_results)
        summary_df.to_csv(self.out_dir / "summary.csv", index=False)
        return {"summary": summary_df, "team_links": links_df}

    def _extract_links(self, html: str) -> pd.DataFrame:
        soup = BeautifulSoup(html, "lxml")
        rows = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(" ", strip=True)
            if not href:
                continue
            abs_url = urljoin(self.TEAMS_URL, href)
            if "/teams/" in href or "line-combinations" in href:
                rows.append({"text": text, "href": href, "absolute_url": abs_url})
        return pd.DataFrame(rows).drop_duplicates()


def run_probe() -> dict[str, pd.DataFrame]:
    return DailyFaceoffProbe().run()


if __name__ == "__main__":
    out = run_probe()
    for name, df in out.items():
        print(f"\n{name}\n", df.head())
