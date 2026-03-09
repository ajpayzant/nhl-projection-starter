from __future__ import annotations

import argparse

from nhl_model.ingest.nhl_api_ingest import NHLAPIIngestor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2A NHL API historical ingest")
    parser.add_argument("--season", required=True, help="Season ID like 20252026")
    parser.add_argument("--season-type", type=int, default=2, help="2=regular season, 3=playoffs")
    parser.add_argument("--max-games", type=int, default=None, help="Optional cap for a test run")
    parser.add_argument("--sleep-seconds", type=float, default=0.15, help="Throttle between requests")
    parser.add_argument("--no-raw-json", action="store_true", help="Skip saving raw JSON payloads")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ingestor = NHLAPIIngestor(
        season=args.season,
        season_type=args.season_type,
        max_games=args.max_games,
        sleep_seconds=args.sleep_seconds,
        save_raw_json=not args.no_raw_json,
    )
    result = ingestor.run()

    print("\nPhase 2A NHL ingest finished.\n")
    print(f"games: {len(result.games):,}")
    print(f"team_game_stats: {len(result.team_game_stats):,}")
    print(f"skater_game_stats: {len(result.skater_game_stats):,}")
    print(f"goalie_game_stats: {len(result.goalie_game_stats):,}")
    print(f"shift_charts: {len(result.shift_charts):,}")
    print(f"summary rows: {len(result.summary):,}")
    print(f"error rows: {len(result.errors):,}")

    if not result.errors.empty:
        print("\nThere were ingest errors. Open data/processed/nhl_api/.../ingest_errors.csv")


if __name__ == "__main__":
    main()
