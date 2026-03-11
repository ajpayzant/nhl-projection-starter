from __future__ import annotations

import argparse

from nhl_model.ingest.dailyfaceoff_ingest import DailyFaceoffIngestor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2D Daily Faceoff operational ingest")
    parser.add_argument("--date", required=True, help="Slate date in YYYY-MM-DD format")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = DailyFaceoffIngestor(date_str=args.date).run()
    print("\nPhase 2D Daily Faceoff ingest finished.\n")
    print(f"team_links rows: {len(result.team_links):,}")
    print(f"team_meta rows: {len(result.team_meta):,}")
    print(f"forward_lines rows: {len(result.forward_lines):,}")
    print(f"defense_pairs rows: {len(result.defense_pairs):,}")
    print(f"pp_units rows: {len(result.pp_units):,}")
    print(f"pk_units rows: {len(result.pk_units):,}")
    print(f"goalies rows: {len(result.goalies):,}")
    print(f"starting_goalies rows: {len(result.starting_goalies):,}")


if __name__ == "__main__":
    main()
