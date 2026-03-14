import argparse

from nhl_model.audit.nhl_api_audit import run_audit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--season', required=True)
    parser.add_argument('--teams', nargs='+', required=True)
    parser.add_argument('--game-id', required=True)
    args = parser.parse_args()

    out = run_audit(args.season, args.teams, args.game_id)
    print("\nPhase 1A NHL API audit finished.\n")
    print("Endpoint summary:")
    print(out['summary'].to_string(index=False))
    if not out['roster'].empty:
        print("\nSample roster rows:")
        print(out['roster'].head(10).to_string(index=False))
    if not out['glossary'].empty:
        print(f"\nGlossary rows: {len(out['glossary'])}")


if __name__ == '__main__':
    main()
