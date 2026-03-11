from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import SEASON_TYPE_MAP
from nhl_model.utils.io import write_df
from nhl_model.utils.normalize import parse_mmss_to_seconds, split_ratio_field


@dataclass
class SilverBuildResult:
    skaters: pd.DataFrame
    goalies: pd.DataFrame
    teams: pd.DataFrame
    shifts: pd.DataFrame


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def _clean_nhl_skaters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    drop_cols = [c for c in ['playerid', 'sweaternumber', 'name_cs', 'name_fi', 'name_sk', 'name_sv'] if c in df.columns]
    df = df.drop(columns=drop_cols, errors='ignore').copy()
    if 'toi' in df.columns:
        df['toi_seconds'] = df['toi'].map(parse_mmss_to_seconds)
        df['toi_minutes'] = df['toi_seconds'] / 60.0
    if 'sog' in df.columns:
        df = df.rename(columns={'sog': 'shots_on_goal'})
    if 'blockedshots' in df.columns:
        df = df.rename(columns={'blockedshots': 'blocked_shots'})
    if 'powerplaygoals' in df.columns:
        df = df.rename(columns={'powerplaygoals': 'pp_goals'})
    if 'faceoffwinningpctg' in df.columns:
        df = df.rename(columns={'faceoffwinningpctg': 'faceoff_win_pct'})
    return df


def _clean_nhl_goalies(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    drop_cols = [c for c in ['playerid', 'sweaternumber', 'name_cs', 'name_fi', 'name_sk', 'name_sv'] if c in df.columns]
    df = df.drop(columns=drop_cols, errors='ignore').copy()
    if 'toi' in df.columns:
        df['toi_seconds'] = df['toi'].map(parse_mmss_to_seconds)
        df['toi_minutes'] = df['toi_seconds'] / 60.0
    ratio_specs = {
        'evenstrengthshotsagainst': 'ev',
        'powerplayshotsagainst': 'pp',
        'shorthandedshotsagainst': 'sh',
        'saveshotsagainst': 'all',
    }
    for col, prefix in ratio_specs.items():
        if col in df.columns:
            split = df[col].map(split_ratio_field)
            df[f'{prefix}_saves_from_ratio'] = split.map(lambda x: x[0] if isinstance(x, tuple) else None)
            df[f'{prefix}_shots_against_from_ratio'] = split.map(lambda x: x[1] if isinstance(x, tuple) else None)
    if 'starter' in df.columns:
        df = df.rename(columns={'starter': 'is_starter'})
    if 'savepctg' in df.columns:
        df = df.rename(columns={'savepctg': 'save_pct'})
    return df


def _clean_nhl_teams(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    keep_drop = ['requested_team']
    df = df.drop(columns=[c for c in keep_drop if c in df.columns], errors='ignore').copy()
    return df


def _clean_nhl_shifts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if 'duration' in df.columns:
        df['shift_duration_seconds'] = df['duration'].map(parse_mmss_to_seconds)
    return df


def build_silver_tables(season: str, season_type: int = 2) -> SilverBuildResult:
    season_label = f'season_{season}_{SEASON_TYPE_MAP.get(season_type, season_type)}'
    nhl_root = PATHS.data_processed / 'nhl_api' / season_label
    silver_root = PATHS.data_processed / 'silver' / season_label
    silver_root.mkdir(parents=True, exist_ok=True)

    skaters = _clean_nhl_skaters(_read_csv(nhl_root / 'skater_game_stats.csv'))
    goalies = _clean_nhl_goalies(_read_csv(nhl_root / 'goalie_game_stats.csv'))
    teams = _clean_nhl_teams(_read_csv(nhl_root / 'team_game_stats.csv'))
    shifts = _clean_nhl_shifts(_read_csv(nhl_root / 'shift_charts.csv'))

    write_df(skaters, silver_root / 'skater_game_stats_silver')
    write_df(goalies, silver_root / 'goalie_game_stats_silver')
    write_df(teams, silver_root / 'team_game_stats_silver')
    write_df(shifts, silver_root / 'shift_charts_silver')

    return SilverBuildResult(skaters=skaters, goalies=goalies, teams=teams, shifts=shifts)
