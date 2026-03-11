from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from nhl_model.utils.http import SimpleHTTPClient, write_text
from nhl_model.utils.normalize import sanitize_columns

DATA_URL = "https://www.moneypuck.com/data.htm"


@dataclass
class DownloadArtifact:
    category: str
    label: str
    url: str
    raw_path: str | None
    extracted_paths: str | None
    status: str
    notes: str | None = None


def season_to_moneypuck_year(season: str | int) -> str:
    season_str = str(season)
    if len(season_str) == 8:
        return season_str[:4]
    raise ValueError(f"Expected season like 20252026, got {season}")


def discover_links(client: SimpleHTTPClient, season: str | int, season_type_label: str = "regular") -> tuple[pd.DataFrame, str]:
    year_key = season_to_moneypuck_year(season)
    html = client.get(DATA_URL, expect_json=False).text or ""
    soup = BeautifulSoup(html, "lxml")

    rows: list[dict[str, str]] = []
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        text = a.get_text(' ', strip=True)
        abs_url = urljoin(DATA_URL, href)
        href_lower = href.lower()
        text_lower = text.lower()

        tag = None
        if 'dictionary' in href_lower or 'dictionary' in text_lower:
            tag = 'dictionary'
        elif year_key in href and season_type_label in href_lower and 'seasonsummary' in href_lower:
            if 'skater' in href_lower:
                tag = 'season_summary_skaters'
            elif 'goalie' in href_lower:
                tag = 'season_summary_goalies'
            elif 'line' in href_lower or 'pair' in href_lower:
                tag = 'season_summary_lines'
            elif 'team' in href_lower:
                tag = 'season_summary_teams'
        elif year_key in href and season_type_label in href_lower and any(x in href_lower for x in ['/games/', 'gamebygame', 'gamebygame']) :
            if 'skater' in href_lower:
                tag = 'game_skaters'
            elif 'goalie' in href_lower:
                tag = 'game_goalies'
            elif 'line' in href_lower or 'pair' in href_lower:
                tag = 'game_lines'
        elif year_key in text and 'shot' in text_lower:
            tag = 'shot_data'

        if tag:
            rows.append({'tag': tag, 'link_text': text, 'href': href, 'absolute_url': abs_url})

    df = pd.DataFrame(rows).drop_duplicates(subset=['tag', 'absolute_url']).reset_index(drop=True)
    return df, html


def _download_binary(client: SimpleHTTPClient, url: str) -> tuple[bytes, str]:
    response = client.session.get(url, timeout=60)
    response.raise_for_status()
    return response.content, response.headers.get('Content-Type', '')


def download_artifacts(
    client: SimpleHTTPClient,
    links_df: pd.DataFrame,
    raw_root: Path,
    processed_root: Path,
    include_shot_data: bool = False,
) -> pd.DataFrame:
    artifacts: list[DownloadArtifact] = []
    for row in links_df.itertuples(index=False):
        tag = str(row.tag)
        if tag == 'shot_data' and not include_shot_data:
            artifacts.append(DownloadArtifact(tag, tag, row.absolute_url, None, None, 'skipped', 'shot data skipped by default'))
            continue

        try:
            content, ctype = _download_binary(client, row.absolute_url)
            suffix = '.zip' if row.absolute_url.lower().endswith('.zip') or 'zip' in ctype.lower() else '.csv'
            safe_name = re.sub(r'[^a-zA-Z0-9_\-]+', '_', tag)
            raw_path = raw_root / f"{safe_name}{suffix}"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(content)

            extracted_paths: list[str] = []
            if suffix == '.zip':
                extract_dir = processed_root / safe_name
                extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(BytesIO(content)) as zf:
                    for member in zf.namelist():
                        if member.endswith('/'):
                            continue
                        target = extract_dir / Path(member).name
                        target.write_bytes(zf.read(member))
                        extracted_paths.append(str(target.relative_to(processed_root)))
            else:
                try:
                    df = pd.read_csv(BytesIO(content), low_memory=False)
                    df.columns = sanitize_columns(df.columns)
                    csv_out = processed_root / f"{safe_name}.csv"
                    parquet_out = processed_root / f"{safe_name}.parquet"
                    csv_out.parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(csv_out, index=False)
                    try:
                        df.to_parquet(parquet_out, index=False)
                    except Exception:
                        pass
                    extracted_paths.append(str(csv_out.relative_to(processed_root)))
                except Exception as exc:
                    artifacts.append(DownloadArtifact(tag, tag, row.absolute_url, str(raw_path), None, 'saved_raw_only', f'csv parse failed: {exc}'))
                    continue

            artifacts.append(DownloadArtifact(tag, tag, row.absolute_url, str(raw_path), ' | '.join(extracted_paths) if extracted_paths else None, 'ok', None))
        except Exception as exc:
            artifacts.append(DownloadArtifact(tag, tag, row.absolute_url, None, None, 'error', str(exc)))

    return pd.DataFrame([a.__dict__ for a in artifacts])
