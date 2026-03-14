from __future__ import annotations

from pathlib import Path
import pandas as pd

from nhl_model.config import PATHS
from nhl_model.utils.io import write_df


def generate_templates(slate_date: str) -> dict[str, pd.DataFrame]:
    out_root = PATHS.templates / "google_sheets"
    templates = {
        "slate_games.csv": pd.DataFrame(columns=[
            "slate_date", "game_id", "away_team", "home_team", "run_flag", "last_run_time", "projection_version"
        ]),
        "team_rosters.csv": pd.DataFrame(columns=[
            "team", "player_name", "player_id", "position", "active_flag"
        ]),
        "manual_lines.csv": pd.DataFrame(columns=[
            "slate_date", "game_id", "team", "line_type", "slot", "player_name", "player_id"
        ]),
        "manual_special_teams.csv": pd.DataFrame(columns=[
            "slate_date", "game_id", "team", "unit_type", "slot", "player_name", "player_id"
        ]),
        "manual_goalies.csv": pd.DataFrame(columns=[
            "slate_date", "game_id", "team", "starting_goalie", "goalie_id"
        ]),
        "manual_overrides.csv": pd.DataFrame(columns=[
            "slate_date", "game_id", "team", "player_name", "player_id", "override_type", "override_value", "note"
        ]),
        "team_projections_review.csv": pd.DataFrame(columns=[
            "slate_date", "game_id", "team", "opponent", "projected_goals", "projected_assists", "projected_sog",
            "projected_pp_toi", "projected_pp_goals", "projected_pp_assists", "projected_pk_toi", "ot_probability"
        ]),
        "player_projections_review.csv": pd.DataFrame(columns=[
            "slate_date", "game_id", "team", "opponent", "player_name", "player_id", "position", "line_assignment",
            "pp_unit", "pk_unit", "projected_toi", "projected_pp_toi", "projected_pk_toi", "projected_sog",
            "projected_goals", "projected_assists", "projected_points", "projected_pp_goals", "projected_pp_assists",
            "projected_pp_points", "projected_saves", "projected_goals_against", "ot_adjusted_flag", "override_applied_flag"
        ]),
    }

    for fname, df in templates.items():
        write_df(df, out_root / fname)

    sample_slate = pd.DataFrame([
        {"slate_date": slate_date, "game_id": "2025020001", "away_team": "BOS", "home_team": "TOR", "run_flag": 1, "last_run_time": "", "projection_version": "v1"}
    ])
    write_df(sample_slate, out_root / "slate_games_example.csv")
    return templates
