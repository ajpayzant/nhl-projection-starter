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
    parser = argparse.ArgumentParser(description='Validate Phase 2 outputs')
    parser.add_argument('--season', required=True, help='Season ID like 20252026')
    parser.add_argument('--season-type', type=int, default=2, help='2=regular season, 3=playoffs')
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
        ('HR skaters standard', PATHS.data_processed / 'hockey_reference' / season_label / 'skaters_standard.csv', ['player', 'team', 'gp']),
        ('HR skaters advanced', PATHS.data_processed / 'hockey_reference' / season_label / 'skaters_advanced.csv', ['player', 'team', 'gp']),
        ('HR goalies standard', PATHS.data_processed / 'hockey_reference' / season_label / 'goalies_standard.csv', ['player', 'gp']),
        ('MP manifest', PATHS.data_processed / 'moneypuck' / season_label / 'download_manifest.csv', ['category', 'status']),
    ]

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
