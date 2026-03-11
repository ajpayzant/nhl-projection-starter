from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup, Comment

from nhl_model.utils.http import SimpleHTTPClient, write_text
from nhl_model.utils.normalize import sanitize_columns

BASE_URL = "https://www.hockey-reference.com/leagues"


def season_to_hr_end_year(season: str | int) -> int:
    season_str = str(season)
    if len(season_str) == 8:
        return int(season_str[4:])
    raise ValueError(f"Expected season like 20252026, got {season}")


def build_urls(season: str | int) -> dict[str, str]:
    yr = season_to_hr_end_year(season)
    return {
        "skaters_standard": f"{BASE_URL}/NHL_{yr}_skaters.html",
        "skaters_advanced": f"{BASE_URL}/NHL_{yr}_skaters-advanced.html",
        "goalies_standard": f"{BASE_URL}/NHL_{yr}_goalies.html",
    }


def _expand_commented_tables(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        comment_text = str(comment)
        if "<table" in comment_text:
            try:
                fragment = BeautifulSoup(comment_text, "lxml")
                comment.replace_with(fragment)
            except Exception:
                continue
    return str(soup)


def _read_html_tables(html: str) -> list[pd.DataFrame]:
    expanded = _expand_commented_tables(html)
    try:
        tables = pd.read_html(StringIO(expanded))
    except ValueError:
        return []
    out: list[pd.DataFrame] = []
    for df in tables:
        if isinstance(df.columns, pd.MultiIndex):
            cols = []
            for col in df.columns:
                parts = [str(x).strip() for x in col if str(x).strip() and str(x).strip() != 'nan']
                cols.append("_".join(parts))
            df.columns = cols
        else:
            df.columns = [str(c).strip() for c in df.columns]
        df.columns = sanitize_columns(df.columns)
        out.append(df)
    return out


def _select_table(tables: list[pd.DataFrame], required_cols: set[str], fallback_keywords: set[str]) -> pd.DataFrame:
    for df in tables:
        cols = set(df.columns)
        if required_cols.issubset(cols):
            return df.copy()
    for df in tables:
        cols = set(df.columns)
        if required_cols.intersection(cols) and fallback_keywords.intersection(cols):
            return df.copy()
    return pd.DataFrame()


def fetch_and_parse_tables(client: SimpleHTTPClient, season: str | int, raw_root: Path) -> dict[str, pd.DataFrame]:
    urls = build_urls(season)
    parsed: dict[str, pd.DataFrame] = {}
    for label, url in urls.items():
        result = client.get(url, expect_json=False)
        write_text(raw_root / f"{label}.html", result.text or "")
        tables = _read_html_tables(result.text or "")
        if label == "skaters_standard":
            df = _select_table(
                tables,
                required_cols={"player", "team", "pos", "gp", "evg", "ppg", "shg", "sog", "toi"},
                fallback_keywords={"fow", "fol", "fo%", "tsa", "blk", "hit", "take", "give"},
            )
        elif label == "skaters_advanced":
            df = _select_table(
                tables,
                required_cols={"player", "tm", "pos", "gp", "cf", "ca", "cf%", "toi/60", "toi(ev)"},
                fallback_keywords={"e+/-", "satt_", "thru%"},
            )
        else:
            df = _select_table(
                tables,
                required_cols={"player", "team", "gp", "gs", "ga", "shots", "sv", "sv%", "min"},
                fallback_keywords={"so", "gaa", "qs", "gsaa"},
            )
        if not df.empty:
            df["season"] = str(season)
            df["source_url"] = url
        parsed[label] = df
    return parsed


def build_table_dictionary(parsed: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for table_name, df in parsed.items():
        if df.empty:
            rows.append({"table_name": table_name, "status": "empty", "column_name": None, "sample_values": None})
            continue
        for col in df.columns:
            values = [str(x) for x in df[col].dropna().astype(str).unique()[:5]]
            rows.append(
                {
                    "table_name": table_name,
                    "status": "ok",
                    "column_name": col,
                    "dtype": str(df[col].dtype),
                    "sample_values": " | ".join(values),
                }
            )
    return pd.DataFrame(rows)
