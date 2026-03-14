from __future__ import annotations

from pathlib import Path
import re
import pandas as pd
from bs4 import BeautifulSoup

from nhl_model.config import PATHS
from nhl_model.utils.http import SimpleHTTPClient
from nhl_model.utils.io import write_df, write_text

DATA_URL = "https://www.moneypuck.com/data.htm"


def season_to_mp_year(season: str) -> str:
    # 20252026 -> 2025
    return season[:4]


def run_audit(season: str) -> dict[str, pd.DataFrame]:
    raw_root = PATHS.data_raw / "audit" / "moneypuck" / f"season_{season}_regular"
    proc_root = PATHS.data_processed / "audit" / "moneypuck" / f"season_{season}_regular"
    client = SimpleHTTPClient()
    result = client.get(DATA_URL, expect_json=False)
    html = result.text or ""
    write_text(raw_root / "data_page.html", html)

    soup = BeautifulSoup(html, "lxml")
    rows = []
    season_year = season_to_mp_year(season)
    hrefs = soup.find_all("a", href=True)
    for a in hrefs:
        href = a.get("href")
        text = " ".join(a.get_text(" ", strip=True).split())
        absolute = href
        if href and href.startswith("moneypuck/"):
            absolute = f"https://www.moneypuck.com/{href}"
        category = "other"
        if "dictionary" in text.lower() or "dictionary" in (href or "").lower():
            category = "dictionary"
        elif f"seasonSummary/{season_year}/regular/skaters.csv" in (href or ""):
            category = "season_summary_skaters"
        elif f"seasonSummary/{season_year}/regular/goalies.csv" in (href or ""):
            category = "season_summary_goalies"
        elif f"seasonSummary/{season_year}/regular/teams.csv" in (href or ""):
            category = "season_summary_teams"
        elif f"seasonSummary/{season_year}/regular/lines.csv" in (href or ""):
            category = "season_summary_lines"
        elif re.search(r"shots_\d{4}\.zip", href or ""):
            category = "shot_data_zip"
        rows.append({
            "category": category,
            "link_text": text,
            "href": href,
            "absolute_url": absolute,
        })

    links_df = pd.DataFrame(rows)
    write_df(links_df, proc_root / "discovered_links.csv")

    summary_df = pd.DataFrame([{
        "url": DATA_URL,
        "status_code": result.status_code,
        "content_type": result.content_type,
        "link_count": len(links_df),
        "matched_core_links": int(links_df['category'].isin([
            'dictionary','season_summary_skaters','season_summary_goalies','season_summary_teams','season_summary_lines','shot_data_zip'
        ]).sum()) if not links_df.empty else 0,
    }])
    write_df(summary_df, proc_root / "summary.csv")

    return {"summary": summary_df, "links": links_df}
