from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import SEASON_TYPE_MAP


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Validate revised Phase 2 outputs')
    parser.add_argument('--season', required=True, help='Season ID like 20252026')
    parser.add_argument('--season-type', type=int, default=2, help='2=regular season, 3=playoffs')
    parser.add_argument('--date', required=False, help='Optional Daily Faceoff date YYYY-MM-DD')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    season_label = f"season_{args.season}_{SEASON_TYPE_MAP.get(args.season_type, args.season_type)}"

    print('\nProject paths')
    print(f'project_root:   {PATHS.project_root}')
    print(f'data_raw:       {PATHS.data_raw}')
    print(f'data_processed: {PATHS.data_processed}')

    checks = [
        ('NHL games', PATHS.data_processed / 'nhl_api' / season_label / 'games.csv', ['game_id', 'game_date', 'away_team_abbrev', 'home_team_abbrev']),
        ('NHL teams', PATHS.data_processed / 'nhl_api' / season_label / 'team_game_stats.csv', ['game_id', 'team_abbrev', 'opponent_abbrev']),
        ('NHL skaters', PATHS.data_processed / 'nhl_api' / season_label / 'skater_game_stats.csv', ['game_id', 'player_id', 'player_name', 'team_abbrev']),
        ('NHL goalies', PATHS.data_processed / 'nhl_api' / season_label / 'goalie_game_stats.csv', ['game_id', 'player_id', 'player_name', 'team_abbrev']),
        ('NHL shifts', PATHS.data_processed / 'nhl_api' / season_label / 'shift_charts.csv', ['game_id', 'playerid', 'teamabbrev']),
        ('MP manifest', PATHS.data_processed / 'moneypuck' / season_label / 'download_manifest.csv', ['category', 'status']),
        ('MP skaters', PATHS.data_processed / 'moneypuck' / season_label / 'downloads' / 'season_summary_skaters.csv', []),
        ('MP goalies', PATHS.data_processed / 'moneypuck' / season_label / 'downloads' / 'season_summary_goalies.csv', []),
        ('MP teams', PATHS.data_processed / 'moneypuck' / season_label / 'downloads' / 'season_summary_teams.csv', []),
        ('MP lines', PATHS.data_processed / 'moneypuck' / season_label / 'downloads' / 'season_summary_lines.csv', []),
        ('Silver NHL skaters', PATHS.data_processed / 'silver' / season_label / 'nhl' / 'skater_game_stats_silver.csv', ['player_id', 'team_abbrev']),
        ('Silver NHL goalies', PATHS.data_processed / 'silver' / season_label / 'nhl' / 'goalie_game_stats_silver.csv', ['player_id', 'team_abbrev']),
        ('Silver NHL teams', PATHS.data_processed / 'silver' / season_label / 'nhl' / 'team_game_stats_silver.csv', ['game_id', 'team_abbrev']),
        ('Silver NHL shifts', PATHS.data_processed / 'silver' / season_label / 'nhl' / 'shift_charts_silver.csv', ['game_id', 'playerid']),
        ('Silver MP skaters', PATHS.data_processed / 'silver' / season_label / 'moneypuck' / 'season_summary_skaters_silver.csv', []),
        ('Silver MP goalies', PATHS.data_processed / 'silver' / season_label / 'moneypuck' / 'season_summary_goalies_silver.csv', []),
    ]

    if args.date:
        dfo_root = PATHS.data_processed / 'dailyfaceoff' / args.date
        checks.extend([
            ('DFO team links', dfo_root / 'team_links.csv', []),
            ('DFO forward lines', dfo_root / 'forward_lines.csv', []),
            ('DFO defense pairs', dfo_root / 'defense_pairs.csv', []),
            ('DFO PP units', dfo_root / 'pp_units.csv', []),
            ('DFO PK units', dfo_root / 'pk_units.csv', []),
            ('DFO starting goalies', dfo_root / 'starting_goalies.csv', []),
            ('Silver DFO forward lines', PATHS.data_processed / 'silver' / season_label / 'dailyfaceoff' / 'forward_lines_silver.csv', []),
            ('Silver DFO PP units', PATHS.data_processed / 'silver' / season_label / 'dailyfaceoff' / 'pp_units_silver.csv', []),
            ('Silver DFO starting goalies', PATHS.data_processed / 'silver' / season_label / 'dailyfaceoff' / 'starting_goalies_silver.csv', []),
        ])

    summary_rows = []
    for label, path, required_cols in checks:
        df = _read_csv(path)
        missing_cols = [c for c in required_cols if c not in df.columns]
        summary_rows.append(
            {
                'dataset': label,
                'path': str(path),
                'exists': path.exists(),
                'rows': len(df),
                'cols': len(df.columns),
                'required_missing': ' | '.join(missing_cols) if missing_cols else '',
                'status': 'ok' if path.exists() and not df.empty and not missing_cols else 'review',
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    out_path = PATHS.data_processed / 'validation' / season_label
    out_path.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(out_path / 'phase2_validation_summary.csv', index=False)

    print('\nValidation summary')
    print(summary_df.to_string(index=False))
    print(f"\nSaved: {out_path / 'phase2_validation_summary.csv'}")


if __name__ == '__main__':
    main()
