from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.constants import CURRENT_AND_RECENT_TEAM_ABBREVS, SEASON_TYPE_MAP
from nhl_model.utils.http import SimpleHTTPClient, write_json
from nhl_model.utils.io import write_df
from nhl_model.utils.normalize import coerce_name, first_non_null, flatten_scalar_dict, sanitize_columns, safe_get


WEB_BASE = "https://api-web.nhle.com/v1"
STATS_BASE = "https://api.nhle.com/stats/rest/en"


@dataclass
class IngestResult:
    games: pd.DataFrame
    team_game_stats: pd.DataFrame
    skater_game_stats: pd.DataFrame
    goalie_game_stats: pd.DataFrame
    shift_charts: pd.DataFrame
    summary: pd.DataFrame
    errors: pd.DataFrame


class NHLAPIIngestor:
    def __init__(
        self,
        season: str,
        season_type: int = 2,
        max_games: int | None = None,
        sleep_seconds: float = 0.15,
        save_raw_json: bool = True,
        team_abbrevs: list[str] | None = None,
    ):
        self.season = str(season)
        self.season_type = int(season_type)
        self.max_games = max_games
        self.sleep_seconds = sleep_seconds
        self.save_raw_json = save_raw_json
        self.team_abbrevs = team_abbrevs or CURRENT_AND_RECENT_TEAM_ABBREVS
        self.client = SimpleHTTPClient(timeout=45)

        season_label = f"season_{self.season}_{SEASON_TYPE_MAP.get(self.season_type, self.season_type)}"
        self.raw_root = PATHS.data_raw / "nhl_api" / season_label
        self.processed_root = PATHS.data_processed / "nhl_api" / season_label
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)

    def run(self) -> IngestResult:
        schedule_df = self._build_schedule_table()
        if schedule_df.empty:
            raise RuntimeError("Schedule collection returned zero games. Stop here and inspect the raw schedule files.")

        if self.max_games is not None:
            schedule_df = schedule_df.head(self.max_games).copy()

        schedule_df = schedule_df.sort_values(["game_date", "game_id"]).reset_index(drop=True)
        write_df(schedule_df, self.processed_root / "games")

        team_rows: list[dict[str, Any]] = []
        skater_rows: list[dict[str, Any]] = []
        goalie_rows: list[dict[str, Any]] = []
        shift_rows: list[dict[str, Any]] = []
        summary_rows: list[dict[str, Any]] = []
        error_rows: list[dict[str, Any]] = []

        total_games = len(schedule_df)
        for idx, game_row in enumerate(schedule_df.itertuples(index=False), start=1):
            game_id = int(game_row.game_id)
            try:
                boxscore_payload = self._fetch_boxscore(game_id)
                team_rows.extend(self._parse_team_game_rows(boxscore_payload, game_row))
                skaters, goalies = self._parse_player_rows(boxscore_payload, game_row)
                skater_rows.extend(skaters)
                goalie_rows.extend(goalies)

                shift_payload = self._fetch_shiftcharts(game_id)
                shift_rows.extend(self._parse_shift_rows(shift_payload, game_row))

                summary_rows.append(
                    {
                        "season": self.season,
                        "season_type": self.season_type,
                        "game_id": game_id,
                        "status": "ok",
                        "boxscore_team_rows": len(self._parse_team_game_rows(boxscore_payload, game_row)),
                        "boxscore_skater_rows": len(skaters),
                        "boxscore_goalie_rows": len(goalies),
                        "shift_rows": len(self._parse_shift_rows(shift_payload, game_row)),
                    }
                )
            except Exception as exc:
                error_rows.append(
                    {
                        "season": self.season,
                        "season_type": self.season_type,
                        "game_id": game_id,
                        "error": str(exc),
                    }
                )
                summary_rows.append(
                    {
                        "season": self.season,
                        "season_type": self.season_type,
                        "game_id": game_id,
                        "status": "error",
                        "boxscore_team_rows": None,
                        "boxscore_skater_rows": None,
                        "boxscore_goalie_rows": None,
                        "shift_rows": None,
                    }
                )

            if idx % 25 == 0 or idx == total_games:
                print(f"Processed {idx}/{total_games} games for {self.season}.")
            time.sleep(self.sleep_seconds)

        team_df = self._finalize_df(team_rows)
        skater_df = self._finalize_df(skater_rows)
        goalie_df = self._finalize_df(goalie_rows)
        shift_df = self._finalize_df(shift_rows)
        summary_df = self._finalize_df(summary_rows)
        errors_df = self._finalize_df(error_rows)

        write_df(team_df, self.processed_root / "team_game_stats")
        write_df(skater_df, self.processed_root / "skater_game_stats")
        write_df(goalie_df, self.processed_root / "goalie_game_stats")
        write_df(shift_df, self.processed_root / "shift_charts")
        write_df(summary_df, self.processed_root / "ingest_summary")
        write_df(errors_df, self.processed_root / "ingest_errors")

        return IngestResult(
            games=schedule_df,
            team_game_stats=team_df,
            skater_game_stats=skater_df,
            goalie_game_stats=goalie_df,
            shift_charts=shift_df,
            summary=summary_df,
            errors=errors_df,
        )

    def _build_schedule_table(self) -> pd.DataFrame:
        all_games: list[dict[str, Any]] = []
        schedules_dir = self.raw_root / "schedules"
        schedules_dir.mkdir(parents=True, exist_ok=True)

        for team in self.team_abbrevs:
            url = f"{WEB_BASE}/club-schedule-season/{team}/{self.season}"
            try:
                payload = self.client.get(url, expect_json=True).json_data
                if self.save_raw_json:
                    write_json(schedules_dir / f"{team}_schedule.json", payload)

                games = payload.get("games", []) if isinstance(payload, dict) else []
                for game in games:
                    row = self._parse_schedule_game(game, requested_team=team)
                    if row is not None:
                        all_games.append(row)
            except Exception:
                # Some abbreviations will fail depending on season (e.g. UTA vs ARI). That's expected.
                continue

            time.sleep(self.sleep_seconds)

        if not all_games:
            return pd.DataFrame()

        df = pd.DataFrame(all_games).drop_duplicates(subset=["game_id"]).reset_index(drop=True)
        return df

    def _parse_schedule_game(self, game: dict[str, Any], requested_team: str) -> dict[str, Any] | None:
        game_type = game.get("gameType")
        if self.season_type is not None and game_type != self.season_type:
            return None

        away = game.get("awayTeam", {})
        home = game.get("homeTeam", {})
        venue = game.get("venue", {})
        neutral_site = game.get("neutralSite", False)
        start_time_utc = game.get("startTimeUTC")
        game_date = first_non_null(game.get("gameDate"), start_time_utc[:10] if isinstance(start_time_utc, str) else None)

        row = {
            "season": self.season,
            "season_type": game_type,
            "game_id": game.get("id"),
            "game_date": game_date,
            "start_time_utc": start_time_utc,
            "game_state": game.get("gameState"),
            "schedule_state": game.get("gameScheduleState"),
            "venue_name": coerce_name(venue),
            "neutral_site": neutral_site,
            "away_team_abbrev": safe_get(away, ["abbrev"]),
            "away_team_id": safe_get(away, ["id"]),
            "away_team_name": first_non_null(coerce_name(safe_get(away, ["commonName"])), coerce_name(safe_get(away, ["placeName"]))),
            "away_score": safe_get(away, ["score"]),
            "home_team_abbrev": safe_get(home, ["abbrev"]),
            "home_team_id": safe_get(home, ["id"]),
            "home_team_name": first_non_null(coerce_name(safe_get(home, ["commonName"])), coerce_name(safe_get(home, ["placeName"]))),
            "home_score": safe_get(home, ["score"]),
            "winning_goalie_id": game.get("winningGoalie", {}).get("playerId") if isinstance(game.get("winningGoalie"), dict) else None,
            "winning_goal_scorer_id": game.get("winningGoalScorer", {}).get("playerId") if isinstance(game.get("winningGoalScorer"), dict) else None,
            "requested_team": requested_team,
        }
        return row

    def _fetch_boxscore(self, game_id: int) -> dict[str, Any]:
        url = f"{WEB_BASE}/gamecenter/{game_id}/boxscore"
        payload = self.client.get(url, expect_json=True).json_data
        if self.save_raw_json:
            write_json(self.raw_root / "boxscores" / f"{game_id}.json", payload)
        return payload

    def _fetch_shiftcharts(self, game_id: int) -> dict[str, Any]:
        url = f"{STATS_BASE}/shiftcharts"
        params = {"cayenneExp": f"gameId={game_id}"}
        payload = self.client.get(url, expect_json=True, params=params).json_data
        if self.save_raw_json:
            write_json(self.raw_root / "shiftcharts" / f"{game_id}.json", payload)
        return payload

    def _base_game_context(self, payload: dict[str, Any], game_row: Any) -> dict[str, Any]:
        return {
            "season": self.season,
            "season_type": self.season_type,
            "game_id": int(game_row.game_id),
            "game_date": getattr(game_row, "game_date", None),
            "start_time_utc": first_non_null(payload.get("startTimeUTC"), getattr(game_row, "start_time_utc", None)),
            "game_state": first_non_null(payload.get("gameState"), getattr(game_row, "game_state", None)),
            "venue_name": first_non_null(coerce_name(payload.get("venue")), getattr(game_row, "venue_name", None)),
            "away_team_abbrev": first_non_null(safe_get(payload, ["awayTeam", "abbrev"]), getattr(game_row, "away_team_abbrev", None)),
            "home_team_abbrev": first_non_null(safe_get(payload, ["homeTeam", "abbrev"]), getattr(game_row, "home_team_abbrev", None)),
        }

    def _parse_team_game_rows(self, payload: dict[str, Any], game_row: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        context = self._base_game_context(payload, game_row)

        team_stats = payload.get("teamGameStats", [])
        team_stat_map = self._parse_team_stat_sections(team_stats)

        for side in ["awayTeam", "homeTeam"]:
            team = payload.get(side, {})
            team_side = "away" if side == "awayTeam" else "home"
            opp_side = "homeTeam" if side == "awayTeam" else "awayTeam"
            opponent = payload.get(opp_side, {})

            flat_team = flatten_scalar_dict(team, prefix="team_", max_depth=2)
            row = {
                **context,
                "team_side": team_side,
                "team_abbrev": first_non_null(team.get("abbrev"), flat_team.get("team_abbrev")),
                "team_id": team.get("id"),
                "team_name": first_non_null(coerce_name(team.get("commonName")), coerce_name(team.get("placeName"))),
                "opponent_abbrev": opponent.get("abbrev"),
                "opponent_id": opponent.get("id"),
                "opponent_name": first_non_null(coerce_name(opponent.get("commonName")), coerce_name(opponent.get("placeName"))),
                "is_home": int(team_side == "home"),
            }
            row.update(flat_team)
            row.update(team_stat_map.get(team_side, {}))
            rows.append(row)
        return rows

    def _parse_team_stat_sections(self, team_stats: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        out = {"away": {}, "home": {}}
        for item in team_stats or []:
            category = item.get("category") or item.get("label") or item.get("name")
            if not category:
                continue
            col = self._clean_stat_name(str(category))
            away_val = first_non_null(item.get("awayValue"), item.get("awayTeamValue"), item.get("away"))
            home_val = first_non_null(item.get("homeValue"), item.get("homeTeamValue"), item.get("home"))
            out["away"][f"team_stat_{col}"] = away_val
            out["home"][f"team_stat_{col}"] = home_val
        return out

    def _parse_player_rows(self, payload: dict[str, Any], game_row: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        skater_rows: list[dict[str, Any]] = []
        goalie_rows: list[dict[str, Any]] = []
        context = self._base_game_context(payload, game_row)
        pbgs = payload.get("playerByGameStats", {})

        for team_key, side_label in [("awayTeam", "away"), ("homeTeam", "home")]:
            team_meta = payload.get(team_key, {})
            team_bucket = pbgs.get(team_key, {}) if isinstance(pbgs, dict) else {}
            team_abbrev = team_meta.get("abbrev")
            opponent_abbrev = payload.get("homeTeam", {}).get("abbrev") if side_label == "away" else payload.get("awayTeam", {}).get("abbrev")

            for group_name, players in (team_bucket or {}).items():
                if not isinstance(players, list):
                    continue
                for player in players:
                    row = {
                        **context,
                        "team_side": side_label,
                        "team_abbrev": team_abbrev,
                        "opponent_abbrev": opponent_abbrev,
                        "position_group": group_name,
                        "player_id": first_non_null(player.get("playerId"), player.get("id"), safe_get(player, ["playerId"])),
                        "player_name": self._player_name(player),
                        "sweater_number": first_non_null(player.get("sweaterNumber"), player.get("sweaterNum")),
                        "position_code": first_non_null(player.get("position"), safe_get(player, ["positionCode"]), safe_get(player, ["position", "code"])),
                    }
                    row.update(flatten_scalar_dict(player, max_depth=1))

                    is_goalie = group_name.lower().startswith("goal") or str(row.get("position_code", "")).upper() == "G"
                    if is_goalie:
                        goalie_rows.append(row)
                    else:
                        skater_rows.append(row)

        return skater_rows, goalie_rows

    def _player_name(self, player: dict[str, Any]) -> str | None:
        return first_non_null(
            coerce_name(player.get("name")),
            coerce_name(player.get("fullName")),
            player.get("firstName") and player.get("lastName") and f"{player.get('firstName')} {player.get('lastName')}",
            player.get("default"),
        )

    def _parse_shift_rows(self, payload: dict[str, Any], game_row: Any) -> list[dict[str, Any]]:
        data = payload.get("data", []) if isinstance(payload, dict) else []
        rows: list[dict[str, Any]] = []
        base = {
            "season": self.season,
            "season_type": self.season_type,
            "game_id": int(game_row.game_id),
            "game_date": getattr(game_row, "game_date", None),
            "away_team_abbrev": getattr(game_row, "away_team_abbrev", None),
            "home_team_abbrev": getattr(game_row, "home_team_abbrev", None),
        }

        for shift in data:
            row = dict(base)
            row.update(flatten_scalar_dict(shift, max_depth=1))
            if row.get("first_name") or row.get("last_name"):
                row["player_name"] = " ".join([str(x) for x in [row.get("first_name"), row.get("last_name")] if x])
            rows.append(row)
        return rows

    def _clean_stat_name(self, text: str) -> str:
        return sanitize_columns([text])[0]

    def _finalize_df(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df.columns = sanitize_columns(df.columns)
        return df
