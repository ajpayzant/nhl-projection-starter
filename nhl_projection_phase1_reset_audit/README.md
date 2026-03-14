# NHL Projection System — Phase 1 Reset Audit

This repo is the **hard-reset Phase 1** for the NHL matchup projection engine.

It does **not** build the full projection model yet.
It does the source-review and field-mapping work needed before modeling.

## What this repo audits

### NHL API / NHL stats layer
- schedules
- rosters
- boxscores
- play-by-play endpoint reachability
- shift charts
- glossary

### MoneyPuck
- season summary skaters
- season summary goalies
- season summary teams
- season summary lines
- dictionary links
- optional shot-data link discovery

### Google Sheets / manual workflow
- generates CSV templates for the Google Sheets tabs you want to use
- optional Sheets API auth smoke test scaffold

## Core V1 outputs this audit is built around

### Team
- goals
- assists
- SOG
- PP TOI
- PP goals
- PP assists
- PK TOI

### Skaters
- total TOI
- PP TOI
- PK TOI
- SOG
- goals
- assists
- points
- PP goals
- PP assists
- PP points

### Goalies
- TOI
- saves
- goals against

## Recommended run order

```bash
pip install -r requirements.txt
pip install -e .
python scripts/run_debug_paths.py
python scripts/run_phase1a_nhl_api_audit.py --season 20252026 --teams BOS TOR --game-id 2025020001
python scripts/run_phase1b_moneypuck_audit.py --season 20252026
python scripts/run_phase1c_generate_sheet_templates.py --slate-date 2026-03-11
python scripts/run_phase1d_build_field_map.py --season 20252026 --slate-date 2026-03-11
```

Or run everything:

```bash
python scripts/run_phase1_full_audit.py --season 20252026 --teams BOS TOR --game-id 2025020001 --slate-date 2026-03-11
```

## Main outputs

### Source audit summaries
- `data/processed/audit/nhl_api/...`
- `data/processed/audit/moneypuck/...`

### Google Sheets templates
- `templates/google_sheets/*.csv`

### Field map and planning outputs
- `data/processed/field_map/season_<season>_regular/phase1_field_map.csv`
- `data/processed/field_map/season_<season>_regular/phase1_field_coverage.csv`
- `data/processed/field_map/season_<season>_regular/phase1_planning_notes.txt`

## Notes
- Google Sheets manual-entry tabs can be used even if all code is run from GitHub/Codespaces.
- The final intended workflow is: code in GitHub/Codespaces, manual inputs + review in Google Sheets, CSV archive for historical storage.
