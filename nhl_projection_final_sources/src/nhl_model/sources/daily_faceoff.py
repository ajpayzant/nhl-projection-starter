from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from nhl_model.utils.http import SimpleHTTPClient, write_text

BASE_URL = "https://www.dailyfaceoff.com"
TEAMS_URL = f"{BASE_URL}/teams"


def _slug_date(date_str: str) -> tuple[str, str]:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    iso_slug = dt.strftime("%Y-%m-%d")
    mdyyyy_slug = f"{dt.month}-{dt.day}-{dt.year}"
    return iso_slug, mdyyyy_slug


def fetch_teams_index(client: SimpleHTTPClient, raw_root: Path) -> tuple[pd.DataFrame, str]:
    html = client.get(TEAMS_URL, expect_json=False).text or ""
    write_text(raw_root / "teams_index.html", html)
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/teams/" in href and "line-combinations" in href:
            text = a.get_text(" ", strip=True)
            rows.append({"text": text, "href": href, "absolute_url": urljoin(BASE_URL, href)})
    df = pd.DataFrame(rows).drop_duplicates(subset=["absolute_url"]).reset_index(drop=True)
    return df, html


def _extract_section_block(text: str, start_label: str, stop_labels: list[str]) -> str:
    match = re.search(re.escape(start_label), text, flags=re.IGNORECASE)
    if not match:
        return ""
    start = match.end()
    tail = text[start:]
    stop_positions = []
    for stop_label in stop_labels:
        sm = re.search(re.escape(stop_label), tail, flags=re.IGNORECASE)
        if sm:
            stop_positions.append(start + sm.start())
    end = min(stop_positions) if stop_positions else len(text)
    return text[start:end].strip()


def parse_line_combinations_page(html: str, page_url: str) -> dict[str, pd.DataFrame]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    page_title = soup.title.get_text(" ", strip=True) if soup.title else ""
    team_name = page_title.replace(" Line Combinations", "").replace(" NHL", "").strip() if page_title else None

    last_updated_match = re.search(r"Last updated:\s*([^\n]+)", text, flags=re.IGNORECASE)
    source_match = re.search(r"Source:\s*([^\n]+)", text, flags=re.IGNORECASE)
    meta_df = pd.DataFrame(
        [
            {
                "page_url": page_url,
                "team_name": team_name,
                "last_updated_text": last_updated_match.group(1).strip() if last_updated_match else None,
                "source_text": source_match.group(1).strip() if source_match else None,
            }
        ]
    )

    stops = [
        "Defensive Pairings",
        "1st Powerplay Unit",
        "2nd Powerplay Unit",
        "1st Penalty Kill Unit",
        "2nd Penalty Kill Unit",
        "Goalies",
        "Injuries",
        "Badges:",
    ]
    forwards_block = _extract_section_block(text, "Forwards", stops)
    defense_block = _extract_section_block(
        text,
        "Defensive Pairings",
        ["1st Powerplay Unit", "2nd Powerplay Unit", "1st Penalty Kill Unit", "2nd Penalty Kill Unit", "Goalies", "Injuries", "Badges:"],
    )
    pp1_block = _extract_section_block(text, "1st Powerplay Unit", ["2nd Powerplay Unit", "1st Penalty Kill Unit", "2nd Penalty Kill Unit", "Goalies", "Injuries", "Badges:"])
    pp2_block = _extract_section_block(text, "2nd Powerplay Unit", ["1st Penalty Kill Unit", "2nd Penalty Kill Unit", "Goalies", "Injuries", "Badges:"])
    pk1_block = _extract_section_block(text, "1st Penalty Kill Unit", ["2nd Penalty Kill Unit", "Goalies", "Injuries", "Badges:"])
    pk2_block = _extract_section_block(text, "2nd Penalty Kill Unit", ["Goalies", "Injuries", "Badges:"])
    goalies_block = _extract_section_block(text, "Goalies", ["Injuries", "Badges:"])

    def _players_from_block(block: str) -> list[str]:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        noise = {"lw", "c", "rw", "click player jersey for news, stats and more!"}
        return [ln for ln in lines if ln.lower() not in noise]

    forwards = _players_from_block(forwards_block)
    f_rows = []
    if len(forwards) >= 12:
        for i in range(0, min(len(forwards), 12), 3):
            if i + 2 < len(forwards):
                f_rows.append(
                    {
                        "team_name": team_name,
                        "line_number": i // 3 + 1,
                        "lw": forwards[i],
                        "c": forwards[i + 1],
                        "rw": forwards[i + 2],
                        "page_url": page_url,
                    }
                )

    defense = _players_from_block(defense_block)
    d_rows = []
    if len(defense) >= 6:
        for i in range(0, min(len(defense), 6), 2):
            if i + 1 < len(defense):
                d_rows.append(
                    {
                        "team_name": team_name,
                        "pair_number": i // 2 + 1,
                        "ld": defense[i],
                        "rd": defense[i + 1],
                        "page_url": page_url,
                    }
                )

    def _unit_rows(block: str, unit_name: str) -> list[dict[str, Any]]:
        players = _players_from_block(block)
        return [
            {"team_name": team_name, "unit_name": unit_name, "slot_number": i + 1, "player_name": name, "page_url": page_url}
            for i, name in enumerate(players)
        ]

    return {
        "meta": meta_df,
        "forward_lines": pd.DataFrame(f_rows),
        "defense_pairs": pd.DataFrame(d_rows),
        "pp_units": pd.DataFrame(_unit_rows(pp1_block, "PP1") + _unit_rows(pp2_block, "PP2")),
        "pk_units": pd.DataFrame(_unit_rows(pk1_block, "PK1") + _unit_rows(pk2_block, "PK2")),
        "goalies": pd.DataFrame(
            [{"team_name": team_name, "goalie_order": i + 1, "player_name": name, "page_url": page_url} for i, name in enumerate(_players_from_block(goalies_block))]
        ),
    }


def fetch_starting_goalies(client: SimpleHTTPClient, date_str: str, raw_root: Path) -> tuple[pd.DataFrame, str, str]:
    iso_slug, mdyyyy_slug = _slug_date(date_str)
    urls = [
        f"{BASE_URL}/starting-goalies/{iso_slug}",
        f"{BASE_URL}/starting-goalies/{mdyyyy_slug}",
    ]
    last_exc = None
    for url in urls:
        try:
            html = client.get(url, expect_json=False).text or ""
            write_text(raw_root / f"starting_goalies_{url.split('/')[-1]}.html", html)
            return parse_starting_goalies_page(html, url), html, url
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"Unable to fetch Daily Faceoff starting goalies page for {date_str}: {last_exc}")


def parse_starting_goalies_page(html: str, page_url: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)
    rows: list[dict[str, Any]] = []

    for line in text.splitlines():
        line_clean = line.strip()
        if not line_clean:
            continue
        if any(key in line_clean.lower() for key in ["confirmed", "expected", "likely", "unconfirmed"]):
            rows.append({"page_url": page_url, "raw_line": line_clean})

    return pd.DataFrame(rows)
