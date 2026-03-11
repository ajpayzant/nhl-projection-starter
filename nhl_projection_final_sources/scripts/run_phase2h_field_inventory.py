from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import SEASON_TYPE_MAP


TARGET_PATTERNS = {
    'time_on_ice': ['toi', 'time_on_ice'],
    'even_strength_toi': ['even', 'ev_toi', 'toi_ev'],
    'power_play_toi': ['powerplay', 'pp_toi', 'toi_pp'],
    'penalty_kill_toi': ['shorthanded', 'pk_toi', 'toi_sh'],
    'shots_on_goal': ['sog', 'shots_on_goal'],
    'shot_attempts': ['tsa', 'sat', 'shot_attempt'],
    'goals': ['goals', 'goal_'],
    'assists': ['assists', 'assist_'],
    'points': ['points', 'point_', 'pts'],
    'power_play_goals': ['pp_goals', 'powerplaygoals'],
    'power_play_points': ['powerplaypoints', 'pp_points'],
    'faceoff_wins': ['faceoffwins', 'fow'],
    'faceoffs_taken': ['faceoffstaken', 'fol', 'faceoffs'],
    'blocked_shots': ['blocked_shots', 'blk'],
    'hits': ['hits', 'hit'],
    'save_pct': ['save_pct', 'savepct'],
    'shots_against': ['shots_against', 'shotsagainst'],
    'goals_against': ['goals_against', 'goalsagainst'],
    'expected_goals': ['xg', 'expected_goals'],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build a field inventory across current outputs')
    parser.add_argument('--season', required=True, help='Season ID like 20252026')
    parser.add_argument('--season-type', type=int, default=2, help='2=regular season, 3=playoffs')
    parser.add_argument('--date', required=False, help='Optional Daily Faceoff date YYYY-MM-DD')
    return parser.parse_args()


def _read_head(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, nrows=5, low_memory=False)
    except Exception:
        return pd.DataFrame()


def main() -> None:
    args = parse_args()
    season_label = f"season_{args.season}_{SEASON_TYPE_MAP.get(args.season_type, args.season_type)}"

    datasets = {
        'nhl_skaters': PATHS.data_processed / 'silver' / season_label / 'nhl' / 'skater_game_stats_silver.csv',
        'nhl_goalies': PATHS.data_processed / 'silver' / season_label / 'nhl' / 'goalie_game_stats_silver.csv',
        'nhl_teams': PATHS.data_processed / 'silver' / season_label / 'nhl' / 'team_game_stats_silver.csv',
        'nhl_shifts': PATHS.data_processed / 'silver' / season_label / 'nhl' / 'shift_charts_silver.csv',
        'mp_skaters': PATHS.data_processed / 'silver' / season_label / 'moneypuck' / 'season_summary_skaters_silver.csv',
        'mp_goalies': PATHS.data_processed / 'silver' / season_label / 'moneypuck' / 'season_summary_goalies_silver.csv',
        'mp_teams': PATHS.data_processed / 'silver' / season_label / 'moneypuck' / 'season_summary_teams_silver.csv',
        'mp_lines': PATHS.data_processed / 'silver' / season_label / 'moneypuck' / 'season_summary_lines_silver.csv',
    }
    if args.date:
        datasets.update({
            'dfo_forward_lines': PATHS.data_processed / 'silver' / season_label / 'dailyfaceoff' / 'forward_lines_silver.csv',
            'dfo_pp_units': PATHS.data_processed / 'silver' / season_label / 'dailyfaceoff' / 'pp_units_silver.csv',
            'dfo_pk_units': PATHS.data_processed / 'silver' / season_label / 'dailyfaceoff' / 'pk_units_silver.csv',
            'dfo_starting_goalies': PATHS.data_processed / 'silver' / season_label / 'dailyfaceoff' / 'starting_goalies_silver.csv',
        })

    rows = []
    for dataset_name, path in datasets.items():
        df = _read_head(path)
        cols = [c.lower() for c in df.columns]
        for target, patterns in TARGET_PATTERNS.items():
            matches = [c for c in cols if any(p in c for p in patterns)]
            rows.append({
                'dataset': dataset_name,
                'target_field': target,
                'matches': ' | '.join(matches),
                'has_match': bool(matches),
                'path': str(path),
            })

    out_df = pd.DataFrame(rows)
    out_dir = PATHS.data_processed / 'validation' / season_label
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / 'phase2_field_inventory.csv'
    out_df.to_csv(out_path, index=False)
    print('\nField inventory summary')
    print(out_df.to_string(index=False))
    print(f"\nSaved: {out_path}")


if __name__ == '__main__':
    main()
