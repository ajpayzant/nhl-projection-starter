from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import SEASON_TYPE_MAP
from nhl_model.sources.moneypuck import DATA_URL, discover_links, download_artifacts
from nhl_model.utils.http import SimpleHTTPClient, write_text
from nhl_model.utils.io import write_df


@dataclass
class MoneyPuckIngestResult:
    discovered_links: pd.DataFrame
    download_manifest: pd.DataFrame


class MoneyPuckIngestor:
    def __init__(self, season: str, season_type: int = 2, include_shot_data: bool = False):
        self.season = str(season)
        self.season_type = int(season_type)
        self.include_shot_data = include_shot_data
        self.season_type_label = SEASON_TYPE_MAP.get(self.season_type, str(self.season_type))
        season_label = f"season_{self.season}_{self.season_type_label}"
        self.raw_root = PATHS.data_raw / 'moneypuck' / season_label
        self.processed_root = PATHS.data_processed / 'moneypuck' / season_label
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)
        self.client = SimpleHTTPClient(timeout=90)

    def run(self) -> MoneyPuckIngestResult:
        links_df, html = discover_links(self.client, self.season, self.season_type_label)
        write_text(self.raw_root / 'data_page.html', html)
        write_df(links_df, self.processed_root / 'discovered_links')

        manifest_df = download_artifacts(
            client=self.client,
            links_df=links_df,
            raw_root=self.raw_root / 'downloads',
            processed_root=self.processed_root / 'downloads',
            include_shot_data=self.include_shot_data,
        )
        manifest_df['data_page_url'] = DATA_URL
        write_df(manifest_df, self.processed_root / 'download_manifest')
        return MoneyPuckIngestResult(discovered_links=links_df, download_manifest=manifest_df)
