# NHL Projection Data Build — Revised Source Map (Phase 2)

This repository is the **corrected Phase 2 data build** for the NHL projection system.

The main change from the earlier version is that we no longer force a single source to do everything.
Instead, each source is used for the data family it is best at:

- **NHL API** → schedules, game IDs, gamecenter boxscores, shift charts, official game structure
- **Hockey-Reference** → skater and goalie season tables, EV/PP/SH splits, FOW/FOL/FO%, TSA, BLK, HIT, TAKE, GIVE
- **MoneyPuck** → advanced skater/goalie/team/line data, game-by-game advanced data, shot/xG files
- **Daily Faceoff** → expected lines, PP/PK units, goalie boards, operational same-day inputs

## Recommended workflow

1. Run **Phase 1 probes** if you have not already.
2. Run **Phase 2A** NHL API official ingest.
3. Run **Phase 2B** Hockey-Reference enrichment.
4. Run **Phase 2C** MoneyPuck advanced ingest.
5. Run **Phase 2D** Daily Faceoff operational ingest.
6. Run **Phase 2E** reference audit / dictionary build.
7. Run **Phase 2F** silver-table cleanup.

## Repository layout

```text
nhl_projection_updated_source_map/
├── .devcontainer/
├── config/
├── data/
│   ├── raw/
│   └── processed/
├── logs/
├── scripts/
└── src/nhl_model/
    ├── config.py
    ├── constants.py
    ├── ingest/
    ├── probes/
    ├── reference/
    ├── silver/
    ├── sources/
    └── utils/
```

## Setup

### Codespaces
Open the repo in Codespaces and then run:

```bash
pip install -r requirements.txt
pip install -e .
```

### Local setup
```bash
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
pip install -e .
```

## Phase 1 (optional if already completed)
```bash
python scripts/run_all_probes.py --season 20252026 --game-id 2025020001 --team BOS
```

## Phase 2A — NHL API official ingest
Small test:
```bash
python scripts/run_phase2a_nhl_api_ingest.py --season 20252026 --max-games 10
```

Full season:
```bash
python scripts/run_phase2a_nhl_api_ingest.py --season 20252026
```

## Phase 2B — Hockey-Reference enrichment
```bash
python scripts/run_phase2b_hockeyref_ingest.py --season 20252026
```

This saves:
- skaters standard season table
- skaters advanced season table
- goalie season table
- a table dictionary built from the discovered tables

## Phase 2C — MoneyPuck advanced ingest
```bash
python scripts/run_phase2c_moneypuck_ingest.py --season 20252026
```

Optional current-season shot file download:
```bash
python scripts/run_phase2c_moneypuck_ingest.py --season 20252026 --include-shot-data
```

This saves:
- discovered link manifest
- season summary downloads (skaters / goalies / lines / teams when present)
- game-by-game downloads (skaters / goalies / lines when present)
- optional shot-data links and downloads
- MoneyPuck data dictionary link manifest

## Phase 2D — Daily Faceoff operational ingest
For a specific slate date:
```bash
python scripts/run_phase2d_dailyfaceoff_ingest.py --date 2026-03-11
```

This saves:
- teams index page
- per-team line-combination HTML snapshots
- parsed line/PP/PK/goalie rows where discoverable
- starting goalies page snapshot and parsed rows where discoverable

## Phase 2E — Reference audit / data dictionary build
```bash
python scripts/run_phase2e_reference_audit.py --season 20252026
```

This builds:
- dataset dictionary from CSV outputs
- NHL glossary export
- MoneyPuck dictionary link export

## Phase 2F — Silver cleanup
```bash
python scripts/run_phase2f_build_silver.py --season 20252026
```

This standardizes the raw ingests into cleaner tables:
- drops duplicate flattened ID columns
- parses TOI strings into seconds/minutes
- splits goalie ratio fields like `27/30` into separate numeric columns
- keeps one consistent player/team/game key system

## One-command Phase 2 pipeline
You can also run everything in order:

```bash
python scripts/run_phase2_full_pipeline.py --season 20252026 --max-games 10 --date 2026-03-11
```

## Important notes

- Hockey-Reference parsing is built to handle Sports Reference HTML, including tables hidden inside comments.
- MoneyPuck downloads can be large. Shot files are optional by default.
- Daily Faceoff is an operational source; page structure can change. The script saves raw HTML even if parsing needs adjustment.
- The silver layer is where we turn source-specific fields into model-ready columns.

## What this repo does **not** do yet

This repo does **not** build the final projections yet. It builds the corrected historical + operational data layer needed before feature engineering and modeling.
