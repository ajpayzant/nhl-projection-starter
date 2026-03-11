from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from nhl_model.config import PATHS
from nhl_model.utils.http import SimpleHTTPClient, write_json, write_text

STATS_BASE = "https://api.nhle.com/stats/rest/en"
MONEYPCK_DATA_PAGE = "https://www.moneypuck.com/data.htm"


def build_dataset_dictionary(base_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted(base_dir.rglob('*.csv')):
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        for col in df.columns:
            series = df[col]
            sample_values = [str(x) for x in series.dropna().astype(str).unique()[:5]]
            rows.append(
                {
                    'dataset': str(path.relative_to(base_dir)),
                    'column_name': col,
                    'dtype': str(series.dtype),
                    'non_null_count': int(series.notna().sum()),
                    'null_count': int(series.isna().sum()),
                    'n_unique': int(series.nunique(dropna=True)),
                    'sample_values': ' | '.join(sample_values),
                }
            )
    return pd.DataFrame(rows)


def fetch_nhl_glossary(client: SimpleHTTPClient) -> dict[str, Any]:
    url = f"{STATS_BASE}/glossary"
    return client.get(url, expect_json=True).json_data


def glossary_to_df(payload: dict[str, Any]) -> pd.DataFrame:
    data = payload.get('data', []) if isinstance(payload, dict) else []
    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        rows.append({k: v for k, v in item.items() if not isinstance(v, (dict, list))})
    return pd.DataFrame(rows)


def fetch_moneypuck_dictionary_links(client: SimpleHTTPClient) -> tuple[pd.DataFrame, str]:
    html = client.get(MONEYPCK_DATA_PAGE, expect_json=False).text or ''
    soup = BeautifulSoup(html, 'lxml')
    rows: list[dict[str, str]] = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(' ', strip=True)
        text_lower = text.lower()
        href_lower = href.lower()
        if 'dictionary' in text_lower or 'dictionary' in href_lower or 'glossary' in text_lower or 'glossary' in href_lower:
            rows.append({'link_text': text, 'href': href})
    return pd.DataFrame(rows), html


def build_and_save_reference_outputs(season: str, season_type_label: str) -> dict[str, Path]:
    reference_root = PATHS.data_processed / 'references' / f'season_{season}_{season_type_label}'
    reference_root.mkdir(parents=True, exist_ok=True)

    dictionary_inputs = [
        PATHS.data_processed / 'nhl_api' / f'season_{season}_{season_type_label}',
        PATHS.data_processed / 'hockey_reference' / f'season_{season}_{season_type_label}',
        PATHS.data_processed / 'moneypuck' / f'season_{season}_{season_type_label}',
    ]
    dictionary_frames = [build_dataset_dictionary(path) for path in dictionary_inputs if path.exists()]
    dictionary_df = pd.concat(dictionary_frames, ignore_index=True) if dictionary_frames else pd.DataFrame()
    dictionary_path = reference_root / 'dataset_dictionary.csv'
    dictionary_df.to_csv(dictionary_path, index=False)

    client = SimpleHTTPClient(timeout=60)
    glossary_payload = fetch_nhl_glossary(client)
    glossary_json_path = reference_root / 'nhl_glossary.json'
    write_json(glossary_json_path, glossary_payload)

    glossary_df = glossary_to_df(glossary_payload)
    glossary_csv_path = reference_root / 'nhl_glossary.csv'
    glossary_df.to_csv(glossary_csv_path, index=False)

    mp_links_df, mp_html = fetch_moneypuck_dictionary_links(client)
    mp_links_csv_path = reference_root / 'moneypuck_dictionary_links.csv'
    mp_links_df.to_csv(mp_links_csv_path, index=False)
    mp_html_path = reference_root / 'moneypuck_data_page.html'
    write_text(mp_html_path, mp_html)

    return {
        'dataset_dictionary_csv': dictionary_path,
        'nhl_glossary_json': glossary_json_path,
        'nhl_glossary_csv': glossary_csv_path,
        'moneypuck_dictionary_links_csv': mp_links_csv_path,
        'moneypuck_data_page_html': mp_html_path,
    }
