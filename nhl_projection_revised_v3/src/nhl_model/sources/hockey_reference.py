from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup, Comment

from nhl_model.utils.http import SimpleHTTPClient, write_text
from nhl_model.utils.normalize import sanitize_columns

BASE_URL = 'https://www.hockey-reference.com/leagues'


def season_to_hr_end_year(season: str | int) -> int:
    season_str = str(season)
    if len(season_str) == 8:
        return int(season_str[4:])
    raise ValueError(f'Expected season like 20252026, got {season}')


def build_urls(season: str | int) -> dict[str, str]:
    yr = season_to_hr_end_year(season)
    return {
        'skaters_standard': f'{BASE_URL}/NHL_{yr}_skaters.html',
        'skaters_advanced': f'{BASE_URL}/NHL_{yr}_skaters-advanced.html',
        'goalies_standard': f'{BASE_URL}/NHL_{yr}_goalies.html',
    }


def _expand_commented_tables(html: str) -> str:
    soup = BeautifulSoup(html, 'lxml')
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        comment_text = str(comment)
        if '<table' in comment_text.lower():
            try:
                fragment = BeautifulSoup(comment_text, 'lxml')
                comment.replace_with(fragment)
            except Exception:
                continue
    return str(soup)


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        cols = []
        for col in df.columns:
            parts = [
                str(x).strip()
                for x in col
                if str(x).strip() and str(x).strip().lower() != 'nan'
            ]
            cols.append('_'.join(parts))
        df.columns = cols
    else:
        df.columns = [str(c).strip() for c in df.columns]

    df.columns = sanitize_columns(df.columns)

    rename_map = {
        'tm': 'team',
        'name': 'player',
        'sv_pct': 'save_pct',
        'svpct': 'save_pct',
        'fo_pct': 'faceoff_pct',
        'fopct': 'faceoff_pct',
        'cfpct': 'cf_pct',
        'ffpct': 'ff_pct',
        'svpct_goalie': 'save_pct',
    }
    df = df.rename(columns=rename_map)

    if 'player' in df.columns:
        df = df[df['player'].astype(str).str.lower() != 'player'].copy()
    if 'rk' in df.columns:
        df = df[df['rk'].astype(str).str.lower() != 'rk'].copy()

    return df.reset_index(drop=True)


def _read_html_tables(html: str) -> list[pd.DataFrame]:
    expanded = _expand_commented_tables(html)
    try:
        tables = pd.read_html(StringIO(expanded))
    except ValueError:
        return []

    out: list[pd.DataFrame] = []
    for df in tables:
        out.append(_clean_columns(df))
    return out


def _score_table(df: pd.DataFrame, table_type: str) -> int:
    cols = set(df.columns)
    score = 0

    common_keys = {'player', 'team', 'gp'}
    for c in common_keys:
        if c in cols:
            score += 3

    if table_type == 'skaters_standard':
        wanted = {
            'pos', 'goals', 'assists', 'points', 'evg', 'ppg', 'shg', 'sog',
            'tsa', 'toi', 'fow', 'fol', 'faceoff_pct', 'blk', 'hit', 'take', 'give'
        }
    elif table_type == 'skaters_advanced':
        wanted = {
            'pos', 'cf', 'ca', 'cf_pct', 'ff', 'fa', 'ff_pct', 'toi_per_60',
            'toi_ev', 'toi_pp', 'toi_sh', 'satt', 'thru_pct', 'e+_'
        }
    elif table_type == 'goalies_standard':
        wanted = {
            'gs', 'ga', 'shots', 'sv', 'save_pct', 'min', 'so', 'gaa'
        }
    else:
        wanted = set()

    for c in wanted:
        if c in cols:
            score += 2

    if len(df) >= 20:
        score += 2
    if len(df.columns) >= 8:
        score += 1

    return score


def _select_best_table(tables: list[pd.DataFrame], table_type: str) -> pd.DataFrame:
    if not tables:
        return pd.DataFrame()

    scored: list[tuple[int, pd.DataFrame]] = []
    for df in tables:
        scored.append((_score_table(df, table_type), df))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_df = scored[0]

    if best_score < 8:
        return pd.DataFrame()

    return best_df.copy()


def fetch_and_parse_tables(client: SimpleHTTPClient, season: str | int, raw_root: Path) -> dict[str, pd.DataFrame]:
    urls = build_urls(season)
    parsed: dict[str, pd.DataFrame] = {}

    for label, url in urls.items():
        result = client.get(url, expect_json=False)
        html = result.text or ''
        write_text(raw_root / f'{label}.html', html)

        expanded = _expand_commented_tables(html)
        write_text(raw_root / f'{label}_expanded.html', expanded)

        diagnostic = (
            f'url={url}\n'
            f'status_code={result.status_code}\n'
            f'content_type={result.content_type}\n'
            f'html_length={len(html)}\n'
            f'contains_table={"<table" in html.lower()}\n'
            f'contains_player={"player" in html.lower()}\n'
        )
        write_text(raw_root / f'{label}_diagnostic.txt', diagnostic)

        tables = _read_html_tables(html)
        summary_rows: list[dict[str, Any]] = []
        for i, df in enumerate(tables):
            summary_rows.append(
                {
                    'table_type': label,
                    'table_index': i,
                    'n_rows': len(df),
                    'n_cols': len(df.columns),
                    'columns': ' | '.join(map(str, df.columns[:30])),
                    'score': _score_table(df, label),
                }
            )
        pd.DataFrame(summary_rows).to_csv(raw_root / f'{label}_table_candidates.csv', index=False)

        df = _select_best_table(tables, label)
        if not df.empty:
            df['season'] = str(season)
            df['source_url'] = url

        parsed[label] = df

    return parsed


def build_table_dictionary(parsed: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for table_name, df in parsed.items():
        if df.empty:
            rows.append(
                {
                    'table_name': table_name,
                    'status': 'empty',
                    'column_name': None,
                    'dtype': None,
                    'sample_values': None,
                }
            )
            continue

        for col in df.columns:
            values = [str(x) for x in df[col].dropna().astype(str).unique()[:5]]
            rows.append(
                {
                    'table_name': table_name,
                    'status': 'ok',
                    'column_name': col,
                    'dtype': str(df[col].dtype),
                    'sample_values': ' | '.join(values),
                }
            )

    return pd.DataFrame(rows)
