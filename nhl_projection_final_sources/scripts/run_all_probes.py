from __future__ import annotations

import argparse
from pathlib import Path

from nhl_model.probes.dailyfaceoff_probe import run_probe as run_dailyfaceoff_probe
from nhl_model.probes.moneypuck_probe import run_probe as run_moneypuck_probe
from nhl_model.probes.nhl_api_probe import run_probe as run_nhl_api_probe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run source access probes for the NHL projection starter repo.")
    parser.add_argument("--season", default="20252026", help="Season ID, e.g. 20252026")
    parser.add_argument("--game-id", type=int, default=2025020001, help="Sample NHL game ID")
    parser.add_argument("--team", default="BOS", help="Team abbreviation for schedule probe")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print("\n[1/3] NHL API probe")
    nhl_df = run_nhl_api_probe(season=args.season, game_id=args.game_id, team_abbrev=args.team)
    print(nhl_df)

    print("\n[2/3] MoneyPuck probe")
    mp = run_moneypuck_probe()
    for name, df in mp.items():
        print(f"\n{name}\n{df.head()}\n")

    print("\n[3/3] Daily Faceoff probe")
    df_out = run_dailyfaceoff_probe()
    for name, df in df_out.items():
        print(f"\n{name}\n{df.head()}\n")

    print("All probes finished. Review CSV/HTML/JSON outputs under data/raw/probes/.")


if __name__ == "__main__":
    main()
