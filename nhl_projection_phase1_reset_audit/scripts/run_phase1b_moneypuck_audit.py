import argparse

from nhl_model.audit.moneypuck_audit import run_audit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--season', required=True)
    args = parser.parse_args()

    out = run_audit(args.season)
    print("\nPhase 1B MoneyPuck audit finished.\n")
    print("Summary:")
    print(out['summary'].to_string(index=False))
    print("\nCore discovered links:")
    core = out['links'][out['links']['category'] != 'other'].copy()
    print(core.to_string(index=False))


if __name__ == '__main__':
    main()
