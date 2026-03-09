from __future__ import annotations

import argparse

from nhl_model.constants import SEASON_TYPE_MAP
from nhl_model.reference.data_dictionary import build_and_save_reference_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a quick Phase 2 data dictionary and reference pull")
    parser.add_argument("--season", required=True, help="Season ID like 20252026")
    parser.add_argument("--season-type", type=int, default=2, help="2=regular season, 3=playoffs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    season_type_label = SEASON_TYPE_MAP.get(args.season_type, str(args.season_type))
    paths = build_and_save_reference_outputs(season=args.season, season_type_label=season_type_label)

    print("\nPhase 2B data dictionary finished.\n")
    for label, path in paths.items():
        print(f"{label}: {path}")


if __name__ == "__main__":
    main()
