from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from nhl_model.config import PATHS
from nhl_model.utils.http import SimpleHTTPClient, write_json


@dataclass
class EndpointSummary:
    endpoint_name: str
    url: str
    status: str
    content_type: str
    elapsed_seconds: float
    notes: str


def _safe_len(value: Any) -> int | None:
    try:
        return len(value)
    except Exception:
        return None


class NHLAPIProbe:
    def __init__(self, season: str, game_id: int, team_abbrev: str):
        self.season = season
        self.game_id = game_id
        self.team_abbrev = team_abbrev.upper()
        self.http = SimpleHTTPClient(timeout=30)
        self.out_dir = PATHS.data_raw / "probes" / "nhl_api"
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> pd.DataFrame:
        rows: list[EndpointSummary] = []

        endpoints = {
            "schedule_calendar": f"https://api-web.nhle.com/v1/schedule-calendar/{self.season[:4]}-10-01",
            "club_schedule_season": f"https://api-web.nhle.com/v1/club-schedule-season/{self.team_abbrev}/{self.season}",
            "gamecenter_boxscore": f"https://api-web.nhle.com/v1/gamecenter/{self.game_id}/boxscore",
            "shiftcharts": f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={self.game_id}",
        }

        for name, url in endpoints.items():
            try:
                result = self.http.get(url, expect_json=True)
                payload = result.json_data
                write_json(self.out_dir / f"{name}.json", payload)
                notes = self._infer_notes(name, payload)
                rows.append(
                    EndpointSummary(
                        endpoint_name=name,
                        url=url,
                        status="ok",
                        content_type=result.content_type,
                        elapsed_seconds=round(result.elapsed_seconds, 4),
                        notes=notes,
                    )
                )
            except Exception as exc:
                rows.append(
                    EndpointSummary(
                        endpoint_name=name,
                        url=url,
                        status="error",
                        content_type="",
                        elapsed_seconds=0.0,
                        notes=str(exc),
                    )
                )

        df = pd.DataFrame([asdict(r) for r in rows])
        df.to_csv(self.out_dir / "endpoint_summary.csv", index=False)
        return df

    def _infer_notes(self, name: str, payload: Any) -> str:
        if name == "schedule_calendar" and isinstance(payload, dict):
            return f"top-level keys={list(payload.keys())[:10]}"
        if name == "club_schedule_season" and isinstance(payload, dict):
            games = payload.get("games", [])
            return f"games={_safe_len(games)}"
        if name == "gamecenter_boxscore" and isinstance(payload, dict):
            away = payload.get("awayTeam", {})
            home = payload.get("homeTeam", {})
            return f"away={away.get('abbrev')} home={home.get('abbrev')}"
        if name == "shiftcharts" and isinstance(payload, dict):
            data = payload.get("data", [])
            return f"shift rows={_safe_len(data)}"
        return "payload saved"


def run_probe(season: str, game_id: int, team_abbrev: str) -> pd.DataFrame:
    return NHLAPIProbe(season=season, game_id=game_id, team_abbrev=team_abbrev).run()


if __name__ == "__main__":
    df = run_probe(season="20252026", game_id=2025020001, team_abbrev="BOS")
    print(df)
