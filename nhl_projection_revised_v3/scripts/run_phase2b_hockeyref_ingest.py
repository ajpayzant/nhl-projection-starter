from __future__ import annotations

import argparse

from nhl_model.ingest.hockeyref_ingest import HockeyReferenceIngestor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2B Hockey-Reference enrichment ingest")
    parser.add_argument("--season", required=True, help="Season ID like 20252026")
    parser.add_argument("--season-type", type=int, default=2, help="2=regular season, 3=playoffs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = HockeyReferenceIngestor(season=args.season, season_type=args.season_type).run()
    print("\nPhase 2B Hockey-Reference ingest finished.\n")
    print(f"skaters_standard rows: {len(result.skaters_standard):,}")
    print(f"skaters_advanced rows: {len(result.skaters_advanced):,}")
    print(f"goalies_standard rows: {len(result.goalies_standard):,}")
    print(f"table_dictionary rows: {len(result.table_dictionary):,}")
    if not result.table_dictionary.empty:
        print(result.table_dictionary.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
