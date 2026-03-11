from __future__ import annotations

import argparse

from nhl_model.silver.build_silver import build_silver_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2F build cleaned silver tables")
    parser.add_argument("--season", required=True, help="Season ID like 20252026")
    parser.add_argument("--season-type", type=int, default=2, help="2=regular season, 3=playoffs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_silver_tables(season=args.season, season_type=args.season_type)
    print("\nPhase 2F silver build finished.\n")
    print(f"skaters silver rows: {len(result.skaters):,}")
    print(f"goalies silver rows: {len(result.goalies):,}")
    print(f"teams silver rows: {len(result.teams):,}")
    print(f"shifts silver rows: {len(result.shifts):,}")


if __name__ == "__main__":
    main()
