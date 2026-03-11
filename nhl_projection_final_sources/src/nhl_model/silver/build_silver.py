from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import SEASON_TYPE_MAP
from nhl_model.utils.io import write_df
from nhl_model.utils.normalize import parse_mmss_to_seconds, sanitize_columns, split_ratio_field


@dataclass
class SilverBuildResult:
    nhl_skaters: pd.DataFrame
    nhl_goalies: pd.DataFrame
    nhl_teams: pd.DataFrame
    nhl_shifts: pd.DataFrame
    mp_skaters: pd.DataFrame
    mp_goalies: pd.DataFrame
    mp_teams: pd.DataFrame
    mp_lines: pd.DataFrame
    dfo_forward_lines: pd.DataFrame
    dfo_defense_pairs: pd.DataFrame
    dfo_pp_units: pd.DataFrame
    dfo_pk_units: pd.DataFrame
    dfo_starting_goalies: pd.DataFrame


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
    rename_map = {
        'sog': 'shots_on_goal',
        'blockedshots': 'blocked_shots',
        'powerplaygoals': 'pp_goals',
        'faceoffwinningpctg': 'faceoff_win_pct',
        'plusminus': 'plus_minus',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    # Derived faceoff counts if available later
    if 'faceoffwins' in df.columns and 'faceoffslost' in df.columns and 'faceoffstaken' not in df.columns:
        df['faceoffstaken'] = df['faceoffwins'].fillna(0) + df['faceoffslost'].fillna(0)
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
    rename_map = {
        'starter': 'is_starter',
        'savepctg': 'save_pct',
        'goalsagainst': 'goals_against',
        'shotsagainst': 'shots_against',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if 'decision' in df.columns:
        df['won_game'] = df['decision'].eq('W')
        df['lost_game'] = df['decision'].eq('L')
        df['ot_loss'] = df['decision'].eq('O')
    if 'goals_against' in df.columns and 'toi_minutes' in df.columns:
        df['shutout_flag'] = (df['goals_against'] == 0) & (df['toi_minutes'] >= 50)
    return df


def _clean_nhl_teams(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.drop(columns=[c for c in ['requested_team'] if c in df.columns], errors='ignore').copy()
    rename_map = {'team_sog': 'team_shots_on_goal', 'team_score': 'team_goals'}
    return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})


def _clean_nhl_shifts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if 'duration' in df.columns:
        df['shift_duration_seconds'] = df['duration'].map(parse_mmss_to_seconds)
    if 'starttime' in df.columns:
        df['shift_start_seconds'] = df['starttime'].map(parse_mmss_to_seconds)
    if 'endtime' in df.columns:
        df['shift_end_seconds'] = df['endtime'].map(parse_mmss_to_seconds)
    return df


def _clean_generic_df(df: pd.DataFrame, rename_map: dict[str, str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df.columns = sanitize_columns(df.columns)
    if rename_map:
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    return df


def build_silver_tables(season: str, season_type: int = 2, dailyfaceoff_date: str | None = None) -> SilverBuildResult:
    season_label = f'season_{season}_{SEASON_TYPE_MAP.get(season_type, season_type)}'
    nhl_root = PATHS.data_processed / 'nhl_api' / season_label
    mp_root = PATHS.data_processed / 'moneypuck' / season_label / 'downloads'
    silver_root = PATHS.data_processed / 'silver' / season_label
    silver_root.mkdir(parents=True, exist_ok=True)

    nhl_skaters = _clean_nhl_skaters(_read_csv(nhl_root / 'skater_game_stats.csv'))
    nhl_goalies = _clean_nhl_goalies(_read_csv(nhl_root / 'goalie_game_stats.csv'))
    nhl_teams = _clean_nhl_teams(_read_csv(nhl_root / 'team_game_stats.csv'))
    nhl_shifts = _clean_nhl_shifts(_read_csv(nhl_root / 'shift_charts.csv'))

    mp_skaters = _clean_generic_df(_read_csv(mp_root / 'season_summary_skaters.csv'))
    mp_goalies = _clean_generic_df(_read_csv(mp_root / 'season_summary_goalies.csv'))
    mp_teams = _clean_generic_df(_read_csv(mp_root / 'season_summary_teams.csv'))
    mp_lines = _clean_generic_df(_read_csv(mp_root / 'season_summary_lines.csv'))

    # Daily Faceoff is date-based rather than season-based. Use provided date if available; otherwise latest folder.
    dfo_base = PATHS.data_processed / 'dailyfaceoff'
    dfo_root = pd.Timestamp(dailyfaceoff_date).strftime('%Y-%m-%d') if dailyfaceoff_date else None
    chosen_dfo_dir: Path | None = None
    if dfo_root and (dfo_base / dfo_root).exists():
        chosen_dfo_dir = dfo_base / dfo_root
    elif dfo_base.exists():
        candidates = sorted([p for p in dfo_base.iterdir() if p.is_dir()])
        if candidates:
            chosen_dfo_dir = candidates[-1]

    dfo_forward_lines = _clean_generic_df(_read_csv(chosen_dfo_dir / 'forward_lines.csv')) if chosen_dfo_dir else pd.DataFrame()
    dfo_defense_pairs = _clean_generic_df(_read_csv(chosen_dfo_dir / 'defense_pairs.csv')) if chosen_dfo_dir else pd.DataFrame()
    dfo_pp_units = _clean_generic_df(_read_csv(chosen_dfo_dir / 'pp_units.csv')) if chosen_dfo_dir else pd.DataFrame()
    dfo_pk_units = _clean_generic_df(_read_csv(chosen_dfo_dir / 'pk_units.csv')) if chosen_dfo_dir else pd.DataFrame()
    dfo_starting_goalies = _clean_generic_df(_read_csv(chosen_dfo_dir / 'starting_goalies.csv')) if chosen_dfo_dir else pd.DataFrame()

    write_df(nhl_skaters, silver_root / 'nhl' / 'skater_game_stats_silver')
    write_df(nhl_goalies, silver_root / 'nhl' / 'goalie_game_stats_silver')
    write_df(nhl_teams, silver_root / 'nhl' / 'team_game_stats_silver')
    write_df(nhl_shifts, silver_root / 'nhl' / 'shift_charts_silver')

    write_df(mp_skaters, silver_root / 'moneypuck' / 'season_summary_skaters_silver')
    write_df(mp_goalies, silver_root / 'moneypuck' / 'season_summary_goalies_silver')
    write_df(mp_teams, silver_root / 'moneypuck' / 'season_summary_teams_silver')
    write_df(mp_lines, silver_root / 'moneypuck' / 'season_summary_lines_silver')

    write_df(dfo_forward_lines, silver_root / 'dailyfaceoff' / 'forward_lines_silver')
    write_df(dfo_defense_pairs, silver_root / 'dailyfaceoff' / 'defense_pairs_silver')
    write_df(dfo_pp_units, silver_root / 'dailyfaceoff' / 'pp_units_silver')
    write_df(dfo_pk_units, silver_root / 'dailyfaceoff' / 'pk_units_silver')
    write_df(dfo_starting_goalies, silver_root / 'dailyfaceoff' / 'starting_goalies_silver')

    return SilverBuildResult(
        nhl_skaters=nhl_skaters,
        nhl_goalies=nhl_goalies,
        nhl_teams=nhl_teams,
        nhl_shifts=nhl_shifts,
        mp_skaters=mp_skaters,
        mp_goalies=mp_goalies,
        mp_teams=mp_teams,
        mp_lines=mp_lines,
        dfo_forward_lines=dfo_forward_lines,
        dfo_defense_pairs=dfo_defense_pairs,
        dfo_pp_units=dfo_pp_units,
        dfo_pk_units=dfo_pk_units,
        dfo_starting_goalies=dfo_starting_goalies,
    )
