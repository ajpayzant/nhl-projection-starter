from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import SEASON_TYPE_MAP
from nhl_model.sources.hockey_reference import build_table_dictionary, fetch_and_parse_tables
from nhl_model.utils.http import SimpleHTTPClient
from nhl_model.utils.io import write_df


@dataclass
class HockeyReferenceIngestResult:
    skaters_standard: pd.DataFrame
    skaters_advanced: pd.DataFrame
    goalies_standard: pd.DataFrame
    table_dictionary: pd.DataFrame
    table_summary: pd.DataFrame


class HockeyReferenceIngestor:
    def __init__(self, season: str, season_type: int = 2):
        self.season = str(season)
        self.season_type = int(season_type)
        season_label = f"season_{self.season}_{SEASON_TYPE_MAP.get(self.season_type, self.season_type)}"
        self.raw_root = PATHS.data_raw / 'hockey_reference' / season_label
        self.processed_root = PATHS.data_processed / 'hockey_reference' / season_label
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)
        self.client = SimpleHTTPClient(timeout=60)

    def run(self) -> HockeyReferenceIngestResult:
        parsed = fetch_and_parse_tables(self.client, self.season, self.raw_root)
        dictionary_df = build_table_dictionary(parsed)

        summary_rows = []
        for table_name, df in parsed.items():
            summary_rows.append(
                {
                    'table_name': table_name,
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'status': 'ok' if not df.empty else 'empty',
                }
            )
            if not df.empty:
                write_df(df, self.processed_root / table_name)
        summary_df = pd.DataFrame(summary_rows)

        write_df(dictionary_df, self.processed_root / 'table_dictionary')
        write_df(summary_df, self.processed_root / 'table_summary')

        return HockeyReferenceIngestResult(
            skaters_standard=parsed.get('skaters_standard', pd.DataFrame()),
            skaters_advanced=parsed.get('skaters_advanced', pd.DataFrame()),
            goalies_standard=parsed.get('goalies_standard', pd.DataFrame()),
            table_dictionary=dictionary_df,
            table_summary=summary_df,
        )
