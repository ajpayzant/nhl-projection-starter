from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.utils.http import SimpleHTTPClient
from nhl_model.utils.io import write_df, write_json


API_WEB = "https://api-web.nhle.com/v1"
API_STATS = "https://api.nhle.com/stats/rest/en"


def build_urls(season: str, teams: Iterable[str], game_id: str) -> dict[str, str]:
    first_team = list(teams)[0]
    return {
        "schedule_calendar": f"{API_WEB}/schedule-calendar/{season[:4]}-10-01",
        "team_season_schedule": f"{API_WEB}/club-schedule-season/{first_team}/{season}",
        "gamecenter_boxscore": f"{API_WEB}/gamecenter/{game_id}/boxscore",
        "gamecenter_play_by_play": f"{API_WEB}/gamecenter/{game_id}/play-by-play",
        "shiftcharts": f"{API_STATS}/shiftcharts?cayenneExp=gameId={game_id}",
        "glossary": f"{API_STATS}/glossary",
        "roster_seasons": f"{API_WEB}/roster-season/{first_team}",
        "team_roster": f"{API_WEB}/roster/{first_team}/{season}",
    }


def run_audit(season: str, teams: Iterable[str], game_id: str) -> dict[str, pd.DataFrame]:
    raw_root = PATHS.data_raw / "audit" / "nhl_api" / f"season_{season}_regular"
    proc_root = PATHS.data_processed / "audit" / "nhl_api" / f"season_{season}_regular"
    client = SimpleHTTPClient()
    urls = build_urls(season, teams, game_id)
    summary_rows = []
    glossary_terms = []
    roster_rows = []

    for label, url in urls.items():
        result = client.get(url, expect_json=True)
        payload = result.json_data if result.json_data is not None else {"raw_text": result.text}
        write_json(raw_root / f"{label}.json", payload)

        notes = ""
        if label == "glossary" and isinstance(payload, dict):
            entries = payload.get("data") or payload.get("glossary") or []
            if isinstance(entries, list):
                for item in entries[:1000]:
                    glossary_terms.append(item)
                notes = f"terms={len(entries)}"
        elif label == "team_roster" and isinstance(payload, dict):
            for grp in ["forwards", "defensemen", "goalies"]:
                for p in payload.get(grp, []) or []:
                    roster_rows.append({
                        "group": grp,
                        "team": list(teams)[0],
                        "player_id": p.get("id") or p.get("playerId"),
                        "player_name": p.get("firstName", {}).get("default", "") + " " + p.get("lastName", {}).get("default", ""),
                        "position": p.get("positionCode") or p.get("position"),
                        "sweater_number": p.get("sweaterNumber"),
                        "raw_name": p.get("firstName"),
                    })
                notes = f"roster_rows={len(roster_rows)}"
        elif label == "shiftcharts" and isinstance(payload, dict):
            notes = f"rows={len(payload.get('data', []))}"
        elif label == "gamecenter_boxscore" and isinstance(payload, dict):
            away = ((payload.get('awayTeam') or {}).get('abbrev'))
            home = ((payload.get('homeTeam') or {}).get('abbrev'))
            notes = f"away={away} home={home}"
        elif label == "team_season_schedule" and isinstance(payload, dict):
            notes = f"games={len(payload.get('games', []))}"

        summary_rows.append({
            "endpoint": label,
            "status_code": result.status_code,
            "content_type": result.content_type,
            "url": result.url,
            "notes": notes,
            "ok": result.status_code == 200,
        })

    summary_df = pd.DataFrame(summary_rows)
    write_df(summary_df, proc_root / "endpoint_summary.csv")

    glossary_df = pd.DataFrame(glossary_terms)
    if not glossary_df.empty:
        write_df(glossary_df, proc_root / "glossary_terms.csv")

    roster_df = pd.DataFrame(roster_rows)
    if not roster_df.empty:
        write_df(roster_df, proc_root / "sample_roster.csv")

    return {
        "summary": summary_df,
        "glossary": glossary_df,
        "roster": roster_df,
    }
