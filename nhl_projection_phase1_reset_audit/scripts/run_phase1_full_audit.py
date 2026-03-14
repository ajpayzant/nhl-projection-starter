import argparse

from nhl_model.audit.nhl_api_audit import run_audit as run_nhl
from nhl_model.audit.moneypuck_audit import run_audit as run_mp
from nhl_model.audit.sheets_templates import generate_templates
from nhl_model.audit.actual_tables import download_actual_tables
from nhl_model.audit.field_map import build_field_map


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True)
    parser.add_argument("--teams", nargs="+", required=True)
    parser.add_argument("--game-id", required=True)
    parser.add_argument("--slate-date", required=True)
    args = parser.parse_args()

    print("[1/5] NHL API source audit")
    nhl = run_nhl(args.season, args.teams, args.game_id)
    print(nhl["summary"].to_string(index=False))

    print("\n[2/5] MoneyPuck source audit")
    mp = run_mp(args.season)
    print(mp["summary"].to_string(index=False))

    print("\n[3/5] Google Sheets templates")
    generate_templates(args.slate_date)
    print("Templates generated.")

    print("\n[4/5] Actual table download")
    actual = download_actual_tables(args.season, args.teams, args.game_id)
    print(actual["summary"].to_string(index=False))

    print("\n[5/5] Final field map from actual tables")
    field_map = build_field_map(args.season, args.slate_date)
    if not field_map["recommended_map"].empty:
        print(field_map["recommended_map"].to_string(index=False))

    print("\nPhase 1 full audit finished.")


if __name__ == "__main__":
    main()
