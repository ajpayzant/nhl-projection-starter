from __future__ import annotations

import argparse

from nhl_model.ingest.moneypuck_ingest import MoneyPuckIngestor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2C MoneyPuck advanced ingest")
    parser.add_argument("--season", required=True, help="Season ID like 20252026")
    parser.add_argument("--season-type", type=int, default=2, help="2=regular season, 3=playoffs")
    parser.add_argument("--include-shot-data", action="store_true", help="Download current-season shot data when discovered")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = MoneyPuckIngestor(
        season=args.season,
        season_type=args.season_type,
        include_shot_data=args.include_shot_data,
    ).run()
    print("\nPhase 2C MoneyPuck ingest finished.\n")
    print(f"discovered_links rows: {len(result.discovered_links):,}")
    print(f"download_manifest rows: {len(result.download_manifest):,}")
    if not result.discovered_links.empty:
        print("\nDiscovered links sample:\n")
        print(result.discovered_links.head(10).to_string(index=False))
    if not result.download_manifest.empty:
        print("\nDownload manifest sample:\n")
        print(result.download_manifest.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
