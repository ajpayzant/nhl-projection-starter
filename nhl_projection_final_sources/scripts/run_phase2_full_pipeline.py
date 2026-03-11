from __future__ import annotations

import argparse

from nhl_model.constants import SEASON_TYPE_MAP
from nhl_model.ingest.dailyfaceoff_ingest import DailyFaceoffIngestor
from nhl_model.ingest.moneypuck_ingest import MoneyPuckIngestor
from nhl_model.ingest.nhl_api_ingest import NHLAPIIngestor
from nhl_model.reference.data_dictionary import build_and_save_reference_outputs
from nhl_model.silver.build_silver import build_silver_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the revised Phase 2 pipeline end to end (NHL API + MoneyPuck + Daily Faceoff)')
    parser.add_argument('--season', required=True, help='Season ID like 20252026')
    parser.add_argument('--season-type', type=int, default=2, help='2=regular season, 3=playoffs')
    parser.add_argument('--max-games', type=int, default=None, help='Optional max games for NHL API test runs')
    parser.add_argument('--date', required=False, help='YYYY-MM-DD for Daily Faceoff ingest. If omitted, DFO step is skipped.')
    parser.add_argument('--include-shot-data', action='store_true', help='Download MoneyPuck shot files')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    season_type_label = SEASON_TYPE_MAP.get(args.season_type, str(args.season_type))

    print('\n[1/6] NHL API official ingest')
    NHLAPIIngestor(season=args.season, season_type=args.season_type, max_games=args.max_games).run()

    print('\n[2/6] MoneyPuck advanced ingest')
    MoneyPuckIngestor(season=args.season, season_type=args.season_type, include_shot_data=args.include_shot_data).run()

    if args.date:
        print('\n[3/6] Daily Faceoff operational ingest')
        DailyFaceoffIngestor(date_str=args.date).run()
    else:
        print('\n[3/6] Daily Faceoff operational ingest skipped (no --date provided)')

    print('\n[4/6] Reference audit build')
    build_and_save_reference_outputs(args.season, season_type_label)

    print('\n[5/6] Silver cleanup build')
    build_silver_tables(args.season, args.season_type, dailyfaceoff_date=args.date)

    print('\n[6/6] Validation summary + field inventory')
    from scripts.run_phase2g_validate_outputs import main as validate_main
    from scripts.run_phase2h_field_inventory import main as inventory_main
    import sys
    sys.argv = ['run_phase2g_validate_outputs.py', '--season', str(args.season), '--season-type', str(args.season_type)] + (['--date', args.date] if args.date else [])
    validate_main()
    sys.argv = ['run_phase2h_field_inventory.py', '--season', str(args.season), '--season-type', str(args.season_type)] + (['--date', args.date] if args.date else [])
    inventory_main()

    print('\nRevised Phase 2 pipeline finished.')


if __name__ == '__main__':
    main()
