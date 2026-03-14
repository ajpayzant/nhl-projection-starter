import argparse

from nhl_model.audit.actual_tables import download_actual_tables
from nhl_model.config import PATHS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True)
    parser.add_argument("--teams", nargs="+", required=True)
    parser.add_argument("--game-id", required=True)
    args = parser.parse_args()

    out = download_actual_tables(args.season, args.teams, args.game_id)

    print("\nPhase 1E actual-table download finished.\n")
    if not out["summary"].empty:
        print(out["summary"].to_string(index=False))

    print(f"\nSaved to: {PATHS.data_processed / 'audit' / 'actual_tables' / f'season_{args.season}_regular'}")


if __name__ == "__main__":
    main()
