from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import TARGET_OUTPUTS, FIELD_PATTERNS
from nhl_model.utils.io import write_df, write_text


DERIVATION_HINTS = {
    "team_assists": "Can be derived from summed player assists by team-game after cleaning.",
    "power_play_assists": "Can be derived from PP primary + PP secondary assist fields if available, or from event-level play-by-play tagging.",
    "power_play_points": "Derived as power_play_goals + power_play_assists.",
    "points": "Derived as goals + assists if not directly present.",
    "goalie_saves": "Derived as shots_against - goals_against if not directly present.",
    "team_pp_toi": "Derived as team total skater PP TOI or game special-teams context.",
    "team_pk_toi": "Derived as opponent PP TOI or summed PK skater TOI.",
}

DATASET_PRIORITY = {
    "team": [
        "nhl_team_game_sample",
        "moneypuck_season_summary_teams",
        "nhl_play_by_play_sample",
    ],
    "skater": [
        "nhl_skater_game_sample",
        "moneypuck_season_summary_skaters",
        "moneypuck_season_summary_lines",
        "nhl_shift_sample",
        "nhl_play_by_play_sample",
    ],
    "goalie": [
        "nhl_goalie_game_sample",
        "moneypuck_season_summary_goalies",
        "nhl_play_by_play_sample",
    ],
}


def _match_fields(columns: Iterable[str], patterns: list[str]) -> list[str]:
    out = []
    for c in columns:
        lc = c.lower()
        for p in patterns:
            if p in lc:
                out.append(c)
                break
    return out


def build_field_map(season: str, slate_date: str) -> dict[str, pd.DataFrame]:
    out_root = PATHS.data_processed / "field_map" / f"season_{season}_regular"
    actual_root = PATHS.data_processed / "audit" / "actual_tables" / f"season_{season}_regular"

    datasets = {
        "nhl_team_game_sample": actual_root / "nhl_team_game_sample.csv",
        "nhl_skater_game_sample": actual_root / "nhl_skater_game_sample.csv",
        "nhl_goalie_game_sample": actual_root / "nhl_goalie_game_sample.csv",
        "nhl_shift_sample": actual_root / "nhl_shift_sample.csv",
        "nhl_play_by_play_sample": actual_root / "nhl_play_by_play_sample.csv",
        "nhl_team_rosters": actual_root / "nhl_team_rosters.csv",
        "moneypuck_season_summary_skaters": actual_root / "moneypuck_season_summary_skaters.csv",
        "moneypuck_season_summary_goalies": actual_root / "moneypuck_season_summary_goalies.csv",
        "moneypuck_season_summary_teams": actual_root / "moneypuck_season_summary_teams.csv",
        "moneypuck_season_summary_lines": actual_root / "moneypuck_season_summary_lines.csv",
        "sheet_template_team_proj": PATHS.templates / "google_sheets" / "team_projections_review.csv",
        "sheet_template_player_proj": PATHS.templates / "google_sheets" / "player_projections_review.csv",
    }

    coverage_rows = []
    mapping_rows = []
    planning_notes: list[str] = []

    loaded: dict[str, pd.DataFrame] = {}
    for dataset_name, path in datasets.items():
        if not path.exists():
            continue
        try:
            loaded[dataset_name] = pd.read_csv(path, low_memory=False)
        except Exception:
            continue

    for dataset_name, df in loaded.items():
        cols = list(df.columns)
        for group, targets in TARGET_OUTPUTS.items():
            for target in targets:
                matches = _match_fields(cols, FIELD_PATTERNS.get(target, []))
                coverage_rows.append(
                    {
                        "dataset": dataset_name,
                        "target_group": group,
                        "target_field": target,
                        "has_match": bool(matches),
                        "matches": " | ".join(matches[:25]),
                        "path": str(datasets[dataset_name]),
                    }
                )
                for m in matches:
                    mapping_rows.append(
                        {
                            "target_group": group,
                            "target_field": target,
                            "candidate_source_dataset": dataset_name,
                            "candidate_source_field": m,
                            "candidate_clean_field": m.lower(),
                            "mapping_type": "direct",
                            "notes": "",
                        }
                    )

    # Add derivation hints so the field map is useful even when a field is not directly present.
    derivation_rows = [
        {
            "target_group": "team",
            "target_field": "assists",
            "candidate_source_dataset": "derived",
            "candidate_source_field": "sum(player_assists_by_team_game)",
            "candidate_clean_field": "team_assists",
            "mapping_type": "derived",
            "notes": DERIVATION_HINTS["team_assists"],
        },
        {
            "target_group": "team",
            "target_field": "power_play_assists",
            "candidate_source_dataset": "derived",
            "candidate_source_field": "sum(player_pp_assists_by_team_game)",
            "candidate_clean_field": "team_pp_assists",
            "mapping_type": "derived",
            "notes": DERIVATION_HINTS["power_play_assists"],
        },
        {
            "target_group": "skater",
            "target_field": "power_play_points",
            "candidate_source_dataset": "derived",
            "candidate_source_field": "pp_goals + pp_assists",
            "candidate_clean_field": "pp_points",
            "mapping_type": "derived",
            "notes": DERIVATION_HINTS["power_play_points"],
        },
        {
            "target_group": "goalie",
            "target_field": "saves",
            "candidate_source_dataset": "derived",
            "candidate_source_field": "shots_against - goals_against",
            "candidate_clean_field": "saves",
            "mapping_type": "derived",
            "notes": DERIVATION_HINTS["goalie_saves"],
        },
        {
            "target_group": "team",
            "target_field": "power_play_toi",
            "candidate_source_dataset": "derived",
            "candidate_source_field": "sum(skater_pp_toi_by_team_game)",
            "candidate_clean_field": "team_pp_toi",
            "mapping_type": "derived",
            "notes": DERIVATION_HINTS["team_pp_toi"],
        },
        {
            "target_group": "team",
            "target_field": "penalty_kill_toi",
            "candidate_source_dataset": "derived",
            "candidate_source_field": "sum(skater_pk_toi_by_team_game)",
            "candidate_clean_field": "team_pk_toi",
            "mapping_type": "derived",
            "notes": DERIVATION_HINTS["team_pk_toi"],
        },
    ]

    coverage_df = pd.DataFrame(coverage_rows)
    mapping_df = pd.concat([pd.DataFrame(mapping_rows), pd.DataFrame(derivation_rows)], ignore_index=True)

    # Build a recommended-map view: best candidate dataset per target group/field
    recommended_rows = []
    for group, targets in TARGET_OUTPUTS.items():
        for target in targets:
            target_cov = coverage_df[
                (coverage_df["target_group"] == group) &
                (coverage_df["target_field"] == target) &
                (coverage_df["has_match"] == True)
            ].copy()

            chosen_dataset = None
            chosen_matches = ""
            priority_order = DATASET_PRIORITY.get(group, [])
            for ds in priority_order:
                hit = target_cov[target_cov["dataset"] == ds]
                if not hit.empty:
                    chosen_dataset = ds
                    chosen_matches = hit.iloc[0]["matches"]
                    break

            if chosen_dataset is None:
                deriv = mapping_df[
                    (mapping_df["target_group"] == group) &
                    (mapping_df["target_field"] == target) &
                    (mapping_df["mapping_type"] == "derived")
                ]
                if not deriv.empty:
                    chosen_dataset = deriv.iloc[0]["candidate_source_dataset"]
                    chosen_matches = deriv.iloc[0]["candidate_source_field"]

            recommended_rows.append(
                {
                    "target_group": group,
                    "target_field": target,
                    "recommended_source": chosen_dataset,
                    "recommended_fields": chosen_matches,
                }
            )

    recommended_df = pd.DataFrame(recommended_rows)

    write_df(coverage_df, out_root / "phase1_field_coverage.csv")
    write_df(mapping_df, out_root / "phase1_field_map.csv")
    write_df(recommended_df, out_root / "phase1_recommended_map.csv")

    planning_notes.append("PHASE 1 COMPLETE = source reachability + actual stat-table download + target field map.")
    planning_notes.append("")
    planning_notes.append("Recommended sources by target field:")
    for row in recommended_df.itertuples(index=False):
        planning_notes.append(
            f"- [{row.target_group}] {row.target_field}: "
            f"{row.recommended_source if pd.notna(row.recommended_source) else 'REVIEW'} "
            f"-> {row.recommended_fields if pd.notna(row.recommended_fields) else ''}"
        )

    planning_notes.append("")
    planning_notes.append("Operational notes:")
    planning_notes.append("- Manual lineup / units / goalie inputs should live in Google Sheets.")
    planning_notes.append("- NHL sample game tables are official game-level examples for TOI / goals / assists / points / goalie stats.")
    planning_notes.append("- MoneyPuck season summary tables are the main context/rate/xG inputs for later modeling.")
    planning_notes.append("- Some team-level outputs are best derived from player-level projections and reconciled after allocation.")
    planning_notes.append("- OT should be handled later as a weighted adjustment layer, not a direct raw data field.")

    write_text(out_root / "phase1_planning_notes.txt", "\n".join(planning_notes))

    return {
        "coverage": coverage_df,
        "field_map": mapping_df,
        "recommended_map": recommended_df,
    }
