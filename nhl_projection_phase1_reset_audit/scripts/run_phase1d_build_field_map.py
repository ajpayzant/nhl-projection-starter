import argparse

from nhl_model.audit.field_map import build_field_map
from nhl_model.config import PATHS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--season', required=True)
    parser.add_argument('--slate-date', required=True)
    args = parser.parse_args()

    out = build_field_map(args.season, args.slate_date)
    print("\nPhase 1D field map build finished.\n")
    if not out['coverage'].empty:
        print("Coverage sample:")
        print(out['coverage'].head(40).to_string(index=False))
    print(f"\nSaved to: {PATHS.data_processed / 'field_map' / f'season_{args.season}_regular'}")


if __name__ == '__main__':
    main()
