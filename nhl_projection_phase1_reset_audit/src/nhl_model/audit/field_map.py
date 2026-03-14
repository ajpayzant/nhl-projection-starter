from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import TARGET_OUTPUTS, FIELD_PATTERNS
from nhl_model.utils.io import write_df, write_text


def _match_fields(columns: Iterable[str], patterns: list[str]) -> list[str]:
    out = []
    for c in columns:
        lc = c.lower()
        if any(p in lc for p in patterns):
            out.append(c)
    return out


def build_field_map(season: str, slate_date: str) -> dict[str, pd.DataFrame]:
    out_root = PATHS.data_processed / 'field_map' / f'season_{season}_regular'
    datasets = {
        'nhl_sample_roster': PATHS.data_processed / 'audit' / 'nhl_api' / f'season_{season}_regular' / 'sample_roster.csv',
        'moneypuck_links': PATHS.data_processed / 'audit' / 'moneypuck' / f'season_{season}_regular' / 'discovered_links.csv',
        'sheet_template_team_proj': PATHS.templates / 'google_sheets' / 'team_projections_review.csv',
        'sheet_template_player_proj': PATHS.templates / 'google_sheets' / 'player_projections_review.csv',
    }
    silver_root = PATHS.data_processed / 'silver' / f'season_{season}_regular'
    datasets.update({
        'silver_nhl_skaters': silver_root / 'nhl' / 'skater_game_stats_silver.csv',
        'silver_nhl_goalies': silver_root / 'nhl' / 'goalie_game_stats_silver.csv',
        'silver_nhl_teams': silver_root / 'nhl' / 'team_game_stats_silver.csv',
        'silver_mp_skaters': silver_root / 'moneypuck' / 'season_summary_skaters_silver.csv',
        'silver_mp_goalies': silver_root / 'moneypuck' / 'season_summary_goalies_silver.csv',
        'silver_mp_teams': silver_root / 'moneypuck' / 'season_summary_teams_silver.csv',
        'silver_mp_lines': silver_root / 'moneypuck' / 'season_summary_lines_silver.csv',
    })

    coverage_rows = []
    mapping_rows = []
    planning_notes = []

    for dataset_name, path in datasets.items():
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        cols = list(df.columns)
        for group, targets in TARGET_OUTPUTS.items():
            for target in targets:
                matches = _match_fields(cols, FIELD_PATTERNS.get(target, []))
                coverage_rows.append({
                    'dataset': dataset_name,
                    'target_group': group,
                    'target_field': target,
                    'has_match': bool(matches),
                    'matches': ' | '.join(matches[:20]),
                    'path': str(path),
                })
                for m in matches:
                    mapping_rows.append({
                        'target_group': group,
                        'target_field': target,
                        'candidate_source_dataset': dataset_name,
                        'candidate_source_field': m,
                        'candidate_clean_field': m.lower(),
                        'notes': '',
                    })

    coverage_df = pd.DataFrame(coverage_rows)
    mapping_df = pd.DataFrame(mapping_rows)
    write_df(coverage_df, out_root / 'phase1_field_coverage.csv')
    write_df(mapping_df, out_root / 'phase1_field_map.csv')

    if not coverage_df.empty:
        for group, targets in TARGET_OUTPUTS.items():
            planning_notes.append(f'[{group.upper()} TARGETS]')
            sub = coverage_df[coverage_df['target_group'] == group]
            for target in targets:
                hit = sub[sub['target_field'] == target]
                any_hit = bool(hit['has_match'].any()) if not hit.empty else False
                planning_notes.append(f"- {target}: {'FOUND' if any_hit else 'MISSING/REVIEW'}")
            planning_notes.append('')
    planning_notes.append('Google Sheets templates were generated under templates/google_sheets/.')
    planning_notes.append('Manual lineups, units, goalies, and overrides should be entered there or mirrored into Google Sheets.')
    write_text(out_root / 'phase1_planning_notes.txt', '\n'.join(planning_notes))

    return {'coverage': coverage_df, 'field_map': mapping_df}
