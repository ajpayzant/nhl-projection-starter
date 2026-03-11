from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.sources.daily_faceoff import fetch_starting_goalies, fetch_teams_index, parse_line_combinations_page
from nhl_model.utils.http import SimpleHTTPClient, write_text
from nhl_model.utils.io import write_df


@dataclass
class DailyFaceoffIngestResult:
    team_links: pd.DataFrame
    forward_lines: pd.DataFrame
    defense_pairs: pd.DataFrame
    pp_units: pd.DataFrame
    pk_units: pd.DataFrame
    goalies: pd.DataFrame
    starting_goalies: pd.DataFrame
    team_meta: pd.DataFrame


class DailyFaceoffIngestor:
    def __init__(self, date_str: str):
        self.date_str = date_str
        self.raw_root = PATHS.data_raw / 'dailyfaceoff' / date_str
        self.processed_root = PATHS.data_processed / 'dailyfaceoff' / date_str
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)
        self.client = SimpleHTTPClient(timeout=60)

    def run(self) -> DailyFaceoffIngestResult:
        team_links_df, _ = fetch_teams_index(self.client, self.raw_root)
        write_df(team_links_df, self.processed_root / 'team_links')

        meta_frames: list[pd.DataFrame] = []
        forwards_frames: list[pd.DataFrame] = []
        defense_frames: list[pd.DataFrame] = []
        pp_frames: list[pd.DataFrame] = []
        pk_frames: list[pd.DataFrame] = []
        goalie_frames: list[pd.DataFrame] = []

        for row in team_links_df.itertuples(index=False):
            try:
                html = self.client.get(row.absolute_url, expect_json=False).text or ''
                slug = Path(str(row.absolute_url)).name or 'team'
                write_text(self.raw_root / 'teams' / f'{slug}.html', html)
                parsed = parse_line_combinations_page(html, row.absolute_url)
                if not parsed['meta'].empty:
                    meta_frames.append(parsed['meta'])
                if not parsed['forward_lines'].empty:
                    forwards_frames.append(parsed['forward_lines'])
                if not parsed['defense_pairs'].empty:
                    defense_frames.append(parsed['defense_pairs'])
                if not parsed['pp_units'].empty:
                    pp_frames.append(parsed['pp_units'])
                if not parsed['pk_units'].empty:
                    pk_frames.append(parsed['pk_units'])
                if not parsed['goalies'].empty:
                    goalie_frames.append(parsed['goalies'])
            except Exception:
                continue

        starting_goalies_df, _, starting_goalies_url = fetch_starting_goalies(self.client, self.date_str, self.raw_root)
        if not starting_goalies_df.empty:
            starting_goalies_df['source_url'] = starting_goalies_url

        forward_lines_df = pd.concat(forwards_frames, ignore_index=True) if forwards_frames else pd.DataFrame()
        defense_pairs_df = pd.concat(defense_frames, ignore_index=True) if defense_frames else pd.DataFrame()
        pp_units_df = pd.concat(pp_frames, ignore_index=True) if pp_frames else pd.DataFrame()
        pk_units_df = pd.concat(pk_frames, ignore_index=True) if pk_frames else pd.DataFrame()
        goalies_df = pd.concat(goalie_frames, ignore_index=True) if goalie_frames else pd.DataFrame()
        team_meta_df = pd.concat(meta_frames, ignore_index=True) if meta_frames else pd.DataFrame()

        write_df(team_meta_df, self.processed_root / 'team_meta')
        write_df(forward_lines_df, self.processed_root / 'forward_lines')
        write_df(defense_pairs_df, self.processed_root / 'defense_pairs')
        write_df(pp_units_df, self.processed_root / 'pp_units')
        write_df(pk_units_df, self.processed_root / 'pk_units')
        write_df(goalies_df, self.processed_root / 'goalies')
        write_df(starting_goalies_df, self.processed_root / 'starting_goalies')

        return DailyFaceoffIngestResult(
            team_links=team_links_df,
            forward_lines=forward_lines_df,
            defense_pairs=defense_pairs_df,
            pp_units=pp_units_df,
            pk_units=pk_units_df,
            goalies=goalies_df,
            starting_goalies=starting_goalies_df,
            team_meta=team_meta_df,
        )
