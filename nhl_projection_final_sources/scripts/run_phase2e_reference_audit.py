from __future__ import annotations

import argparse

from nhl_model.constants import SEASON_TYPE_MAP
from nhl_model.reference.data_dictionary import build_and_save_reference_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2E reference audit / data dictionary build")
    parser.add_argument("--season", required=True, help="Season ID like 20252026")
    parser.add_argument("--season-type", type=int, default=2, help="2=regular season, 3=playoffs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    season_type_label = SEASON_TYPE_MAP.get(args.season_type, str(args.season_type))
    outputs = build_and_save_reference_outputs(args.season, season_type_label)
    print("\nPhase 2E reference audit finished.\n")
    for label, path in outputs.items():
        print(f"{label}: {path}")


if __name__ == "__main__":
    main()
