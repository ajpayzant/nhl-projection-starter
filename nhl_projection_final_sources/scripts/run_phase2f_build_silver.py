from __future__ import annotations

import argparse

from nhl_model.silver.build_silver import build_silver_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2F build cleaned silver tables")
    parser.add_argument("--season", required=True, help="Season ID like 20252026")
    parser.add_argument("--season-type", type=int, default=2, help="2=regular season, 3=playoffs")
    parser.add_argument("--date", required=False, help="Optional Daily Faceoff date YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_silver_tables(season=args.season, season_type=args.season_type, dailyfaceoff_date=args.date)
    print("\nPhase 2F silver build finished.\n")
    print(f"NHL skaters silver rows: {len(result.nhl_skaters):,}")
    print(f"NHL goalies silver rows: {len(result.nhl_goalies):,}")
    print(f"NHL teams silver rows: {len(result.nhl_teams):,}")
    print(f"NHL shifts silver rows: {len(result.nhl_shifts):,}")
    print(f"MoneyPuck skaters silver rows: {len(result.mp_skaters):,}")
    print(f"MoneyPuck goalies silver rows: {len(result.mp_goalies):,}")
    print(f"MoneyPuck teams silver rows: {len(result.mp_teams):,}")
    print(f"MoneyPuck lines silver rows: {len(result.mp_lines):,}")
    print(f"Daily Faceoff forward lines silver rows: {len(result.dfo_forward_lines):,}")
    print(f"Daily Faceoff defense pairs silver rows: {len(result.dfo_defense_pairs):,}")
    print(f"Daily Faceoff PP units silver rows: {len(result.dfo_pp_units):,}")
    print(f"Daily Faceoff PK units silver rows: {len(result.dfo_pk_units):,}")
    print(f"Daily Faceoff starting goalies silver rows: {len(result.dfo_starting_goalies):,}")


if __name__ == "__main__":
    main()
