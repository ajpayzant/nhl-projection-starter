from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.utils.http import SimpleHTTPClient
from nhl_model.utils.io import write_df, write_json, write_text
from nhl_model.utils.normalize import sanitize_columns

API_WEB = "https://api-web.nhle.com/v1"
API_STATS = "https://api.nhle.com/stats/rest/en"
MP_DATA_URL = "https://www.moneypuck.com/data.htm"


def _flatten(prefix: str, obj: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not isinstance(obj, dict):
        return out
    for k, v in obj.items():
        key = f"{prefix}_{k}" if prefix else str(k)
        if isinstance(v, dict):
            out.update(_flatten(key, v))
        elif isinstance(v, list):
            continue
        else:
            out[key] = v
    return out


def _coerce_name(player: dict[str, Any]) -> str:
    first = player.get("firstName", {})
    last = player.get("lastName", {})
    first_name = first.get("default") if isinstance(first, dict) else player.get("firstName")
    last_name = last.get("default") if isinstance(last, dict) else player.get("lastName")
    return " ".join([str(x).strip() for x in [first_name, last_name] if x]).strip()


def _safe_abbrev(team_obj: dict[str, Any]) -> str | None:
    if not isinstance(team_obj, dict):
        return None
    return team_obj.get("abbrev")


def _finalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df.columns = sanitize_columns(df.columns)
    return df


def _fetch_json(client: SimpleHTTPClient, url: str) -> dict[str, Any]:
    result = client.get(url, expect_json=True)
    payload = result.json_data if result.json_data is not None else {}
    return payload if isinstance(payload, dict) else {"data": payload}


def _fetch_text(client: SimpleHTTPClient, url: str) -> str:
    result = client.get(url, expect_json=False)
    return result.text or ""


def _discover_moneypuck_links(client: SimpleHTTPClient, season: str) -> pd.DataFrame:
    html = _fetch_text(client, MP_DATA_URL)
    write_text(
        PATHS.data_raw / "audit" / "actual_tables" / f"season_{season}_regular" / "moneypuck_data_page.html",
        html,
    )
    rows: list[dict[str, Any]] = []
    for href in pd.read_html(StringIO(pd.DataFrame({"html": [html]}).to_html())):
        pass  # no-op; keeps parser imports warm in some environments

    from bs4 import BeautifulSoup  # local import keeps this file simpler to drop in

    soup = BeautifulSoup(html, "lxml")
    season_year = season[:4]
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        text = " ".join(a.get_text(" ", strip=True).split())
        absolute = href
        if href and href.startswith("moneypuck/"):
            absolute = f"https://www.moneypuck.com/{href}"

        category = "other"
        if "dictionary" in text.lower() or "dictionary" in (href or "").lower():
            category = "dictionary"
        elif f"seasonSummary/{season_year}/regular/skaters.csv" in (href or ""):
            category = "season_summary_skaters"
        elif f"seasonSummary/{season_year}/regular/goalies.csv" in (href or ""):
            category = "season_summary_goalies"
        elif f"seasonSummary/{season_year}/regular/teams.csv" in (href or ""):
            category = "season_summary_teams"
        elif f"seasonSummary/{season_year}/regular/lines.csv" in (href or ""):
            category = "season_summary_lines"
        elif "shots_" in (href or "") and href.endswith(".zip"):
            category = "shot_data_zip"

        rows.append(
            {
                "category": category,
                "link_text": text,
                "href": href,
                "absolute_url": absolute,
            }
        )

    return pd.DataFrame(rows)


def _download_csv_from_url(client: SimpleHTTPClient, url: str, raw_path: Path, processed_path: Path) -> pd.DataFrame:
    text = _fetch_text(client, url)
    write_text(raw_path, text)
    df = pd.read_csv(StringIO(text), low_memory=False)
    write_df(df, processed_path)
    return df


def download_actual_tables(season: str, teams: list[str], game_id: str) -> dict[str, pd.DataFrame]:
    client = SimpleHTTPClient(timeout=90)

    raw_root = PATHS.data_raw / "audit" / "actual_tables" / f"season_{season}_regular"
    proc_root = PATHS.data_processed / "audit" / "actual_tables" / f"season_{season}_regular"
    raw_root.mkdir(parents=True, exist_ok=True)
    proc_root.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------
    # NHL actual sample tables
    # --------------------------------------------------
    box_url = f"{API_WEB}/gamecenter/{game_id}/boxscore"
    pbp_url = f"{API_WEB}/gamecenter/{game_id}/play-by-play"
    shift_url = f"{API_STATS}/shiftcharts?cayenneExp=gameId={game_id}"

    box = _fetch_json(client, box_url)
    pbp = _fetch_json(client, pbp_url)
    shifts = _fetch_json(client, shift_url)

    write_json(raw_root / "nhl_gamecenter_boxscore.json", box)
    write_json(raw_root / "nhl_gamecenter_play_by_play.json", pbp)
    write_json(raw_root / "nhl_shiftcharts.json", shifts)

    away_team = box.get("awayTeam", {}) if isinstance(box, dict) else {}
    home_team = box.get("homeTeam", {}) if isinstance(box, dict) else {}

    game_ctx = {
        "season": season,
        "game_id": int(game_id),
        "away_team_abbrev": _safe_abbrev(away_team),
        "home_team_abbrev": _safe_abbrev(home_team),
    }

    team_rows: list[dict[str, Any]] = []
    skater_rows: list[dict[str, Any]] = []
    goalie_rows: list[dict[str, Any]] = []
    shift_rows: list[dict[str, Any]] = []
    pbp_rows: list[dict[str, Any]] = []
    roster_rows: list[dict[str, Any]] = []

    # Team rows
    for side, team_obj in [("away", away_team), ("home", home_team)]:
        if not isinstance(team_obj, dict):
            continue
        row = dict(game_ctx)
        row["team_side"] = side
        row["team_abbrev"] = team_obj.get("abbrev")
        row["team_id"] = team_obj.get("id")
        row["team_score"] = team_obj.get("score")
        row.update(_flatten("team", team_obj))
        team_rows.append(row)

    # Player rows
    player_blocks = box.get("playerByGameStats", {}) if isinstance(box, dict) else {}
    for side in ["awayTeam", "homeTeam"]:
        side_obj = player_blocks.get(side, {}) if isinstance(player_blocks, dict) else {}
        side_abbrev = away_team.get("abbrev") if side == "awayTeam" else home_team.get("abbrev")
        opp_abbrev = home_team.get("abbrev") if side == "awayTeam" else away_team.get("abbrev")

        for grp_name in ["forwards", "defense"]:
            for p in side_obj.get(grp_name, []) or []:
                row = dict(game_ctx)
                row["team_abbrev"] = side_abbrev
                row["opponent_abbrev"] = opp_abbrev
                row["position_group"] = "skater"
                row["player_name"] = _coerce_name(p)
                row["player_id"] = p.get("playerId") or p.get("id")
                row.update(_flatten("", p))
                skater_rows.append(row)

        for p in side_obj.get("goalies", []) or []:
            row = dict(game_ctx)
            row["team_abbrev"] = side_abbrev
            row["opponent_abbrev"] = opp_abbrev
            row["position_group"] = "goalie"
            row["player_name"] = _coerce_name(p)
            row["player_id"] = p.get("playerId") or p.get("id")
            row.update(_flatten("", p))
            goalie_rows.append(row)

    # Shift rows
    shift_data = shifts.get("data", []) if isinstance(shifts, dict) else []
    for item in shift_data:
        row = dict(game_ctx)
        if isinstance(item, dict):
            row.update(item)
        shift_rows.append(row)

    # Play-by-play rows
    pbp_data = pbp.get("plays", []) if isinstance(pbp, dict) else []
    for item in pbp_data:
        row = dict(game_ctx)
        if isinstance(item, dict):
            row.update(_flatten("", item))
        pbp_rows.append(row)

    # Rosters for requested teams
    for team in teams:
        roster_url = f"{API_WEB}/roster/{team}/{season}"
        roster_payload = _fetch_json(client, roster_url)
        write_json(raw_root / f"nhl_roster_{team}.json", roster_payload)

        for grp in ["forwards", "defensemen", "goalies"]:
            for p in roster_payload.get(grp, []) or []:
                roster_rows.append(
                    {
                        "season": season,
                        "team": team,
                        "group": grp,
                        "player_id": p.get("id") or p.get("playerId"),
                        "player_name": _coerce_name(p),
                        "position_code": p.get("positionCode") or p.get("position"),
                        "sweater_number": p.get("sweaterNumber"),
                    }
                )

    nhl_team_df = _finalize(pd.DataFrame(team_rows))
    nhl_skater_df = _finalize(pd.DataFrame(skater_rows))
    nhl_goalie_df = _finalize(pd.DataFrame(goalie_rows))
    nhl_shift_df = _finalize(pd.DataFrame(shift_rows))
    nhl_pbp_df = _finalize(pd.DataFrame(pbp_rows))
    nhl_roster_df = _finalize(pd.DataFrame(roster_rows))

    write_df(nhl_team_df, proc_root / "nhl_team_game_sample.csv")
    write_df(nhl_skater_df, proc_root / "nhl_skater_game_sample.csv")
    write_df(nhl_goalie_df, proc_root / "nhl_goalie_game_sample.csv")
    write_df(nhl_shift_df, proc_root / "nhl_shift_sample.csv")
    write_df(nhl_pbp_df, proc_root / "nhl_play_by_play_sample.csv")
    write_df(nhl_roster_df, proc_root / "nhl_team_rosters.csv")

    # --------------------------------------------------
    # MoneyPuck actual tables
    # --------------------------------------------------
    links_df = _discover_moneypuck_links(client, season)
    write_df(links_df, proc_root / "moneypuck_discovered_links.csv")

    category_to_filename = {
        "season_summary_skaters": "moneypuck_season_summary_skaters.csv",
        "season_summary_goalies": "moneypuck_season_summary_goalies.csv",
        "season_summary_teams": "moneypuck_season_summary_teams.csv",
        "season_summary_lines": "moneypuck_season_summary_lines.csv",
        "dictionary": "moneypuck_dictionary.csv",
    }

    mp_outputs: dict[str, pd.DataFrame] = {}
    download_rows: list[dict[str, Any]] = []

    for category, filename in category_to_filename.items():
        candidates = links_df[links_df["category"] == category].copy()
        if candidates.empty:
            continue
        url = candidates.iloc[0]["absolute_url"]
        raw_path = raw_root / filename
        proc_path = proc_root / filename
        try:
            df = _download_csv_from_url(client, url, raw_path, proc_path)
            df = _finalize(df)
            write_df(df, proc_path)
            mp_outputs[category] = df
            download_rows.append(
                {
                    "category": category,
                    "url": url,
                    "raw_path": str(raw_path),
                    "processed_path": str(proc_path),
                    "rows": len(df),
                    "cols": len(df.columns),
                    "status": "ok",
                }
            )
        except Exception as exc:
            download_rows.append(
                {
                    "category": category,
                    "url": url,
                    "raw_path": str(raw_path),
                    "processed_path": str(proc_path),
                    "rows": None,
                    "cols": None,
                    "status": f"error: {exc}",
                }
            )

    mp_manifest_df = pd.DataFrame(download_rows)
    write_df(mp_manifest_df, proc_root / "moneypuck_download_manifest.csv")

    # --------------------------------------------------
    # Summary
    # --------------------------------------------------
    summary_rows = [
        {"dataset": "nhl_team_game_sample", "rows": len(nhl_team_df), "cols": len(nhl_team_df.columns)},
        {"dataset": "nhl_skater_game_sample", "rows": len(nhl_skater_df), "cols": len(nhl_skater_df.columns)},
        {"dataset": "nhl_goalie_game_sample", "rows": len(nhl_goalie_df), "cols": len(nhl_goalie_df.columns)},
        {"dataset": "nhl_shift_sample", "rows": len(nhl_shift_df), "cols": len(nhl_shift_df.columns)},
        {"dataset": "nhl_play_by_play_sample", "rows": len(nhl_pbp_df), "cols": len(nhl_pbp_df.columns)},
        {"dataset": "nhl_team_rosters", "rows": len(nhl_roster_df), "cols": len(nhl_roster_df.columns)},
    ]
    for category, df in mp_outputs.items():
        summary_rows.append({"dataset": category, "rows": len(df), "cols": len(df.columns)})

    summary_df = pd.DataFrame(summary_rows)
    write_df(summary_df, proc_root / "actual_table_summary.csv")

    return {
        "summary": summary_df,
        "nhl_team_game_sample": nhl_team_df,
        "nhl_skater_game_sample": nhl_skater_df,
        "nhl_goalie_game_sample": nhl_goalie_df,
        "nhl_shift_sample": nhl_shift_df,
        "nhl_play_by_play_sample": nhl_pbp_df,
        "nhl_team_rosters": nhl_roster_df,
        "moneypuck_manifest": mp_manifest_df,
        **mp_outputs,
    }
