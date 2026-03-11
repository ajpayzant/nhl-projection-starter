# NHL Projection Data Build — Revised v4

This repository is the corrected multi-source **data build layer** for the NHL projection system.

It is designed to do four things well before we build the actual model:

1. **pull the right data from the right working source**
2. **save raw and processed files in a stable folder structure**
3. **build cleaned silver tables for modeling**
4. **validate outputs and inventory key fields so we know what is actually available**

## Supported source map

- **NHL API** → schedules, game IDs, gamecenter boxscores, shift charts, official game structure
- **MoneyPuck** → advanced skater / goalie / team / line data, link discovery, shot/xG dictionary and file manifests
- **Daily Faceoff** → line combinations, PP/PK units, same-day goalie context

## Removed from required pipeline

- **Hockey-Reference** is no longer part of the required automated build because league pages may block automated requests depending on environment. A placeholder script remains only so old commands do not confuse you.

---

# Easiest setup

## 1. Open in GitHub Codespaces
Once the repo is uploaded, open it in Codespaces.

## 2. Open a terminal and run

```bash
pip install -r requirements.txt
pip install -e .
```

## 3. Check the project root

```bash
python scripts/run_debug_paths.py
```

You want `project_root` to equal the folder that contains:
- `requirements.txt`
- `pyproject.toml`
- `scripts/`
- `src/`
- `data/`

If that path is wrong, stop and fix it before running ingests.

---

# Recommended run order

## Step A — NHL API test ingest

```bash
python scripts/run_phase2a_nhl_api_ingest.py --season 20252026 --max-games 10
```

## Step B — MoneyPuck advanced ingest

```bash
python scripts/run_phase2c_moneypuck_ingest.py --season 20252026
```

## Step C — Daily Faceoff operational ingest

```bash
python scripts/run_phase2d_dailyfaceoff_ingest.py --date 2026-03-11
```

## Step D — Build reference audit / dictionary

```bash
python scripts/run_phase2e_reference_audit.py --season 20252026
```

## Step E — Build silver cleaned tables

```bash
python scripts/run_phase2f_build_silver.py --season 20252026 --date 2026-03-11
```

## Step F — Validate outputs

```bash
python scripts/run_phase2g_validate_outputs.py --season 20252026 --date 2026-03-11
```

## Step G — Inventory key fields

```bash
python scripts/run_phase2h_field_inventory.py --season 20252026 --date 2026-03-11
```

---

# One-command version

After the source-by-source tests are working, you can run:

```bash
python scripts/run_phase2_full_pipeline.py --season 20252026 --max-games 10 --date 2026-03-11
```

---

# What to check after each step

## NHL API
Check:

```text
data/processed/nhl_api/season_20252026_regular/
```

You should see:
- `games.csv`
- `team_game_stats.csv`
- `skater_game_stats.csv`
- `goalie_game_stats.csv`
- `shift_charts.csv`
- `ingest_summary.csv`
- `ingest_errors.csv`

## MoneyPuck
Check:

```text
data/processed/moneypuck/season_20252026_regular/
```

You should see:
- `discovered_links.csv`
- `download_manifest.csv`
- downloaded CSVs under `downloads/`

## Daily Faceoff
Check:

```text
data/processed/dailyfaceoff/2026-03-11/
```

You should see:
- `team_links.csv`
- `forward_lines.csv`
- `defense_pairs.csv`
- `pp_units.csv`
- `pk_units.csv`
- `goalies.csv`
- `starting_goalies.csv`

## Silver tables
Check:

```text
data/processed/silver/season_20252026_regular/
```

You should see subfolders for:
- `nhl/`
- `moneypuck/`
- `dailyfaceoff/`

## Validation
Check:

```text
data/processed/validation/season_20252026_regular/phase2_validation_summary.csv
```

This file tells you:
- whether each expected dataset exists
- row counts
- column counts
- whether required columns are missing

## Field inventory
Check:

```text
data/processed/validation/season_20252026_regular/phase2_field_inventory.csv
```

This file tells you which datasets currently contain candidate columns for important targets like:
- even-strength TOI
- PP TOI
- PK TOI
- shots on goal
- shot attempts
- faceoff wins / faceoffs taken
- blocked shots
- hits
- save percentage
- xG / expected goals

---

# How to know the data is “as it should be”

## 1. NHL API counts
- `games` should have the number of games you requested
- `team_game_stats` should usually be about `2 x games`
- `skater_game_stats` should be much larger than `games`
- `goalie_game_stats` should usually be several rows per game
- `ingest_errors.csv` should ideally be empty

## 2. MoneyPuck manifest
- `download_manifest.csv` should show `ok` on the main season summary rows
- `season_summary_skaters.csv`, `season_summary_goalies.csv`, `season_summary_teams.csv`, and `season_summary_lines.csv` should exist under `downloads/`

## 3. Silver tables
- NHL TOI should have parsed numeric columns like `toi_seconds` and `toi_minutes`
- goalie ratio fields should split into numeric columns
- duplicate flattened ID fields should be reduced
- MoneyPuck and Daily Faceoff silver tables should exist if their source ingests ran

## 4. Validation summary
- most required datasets should show `status = ok`
- anything marked `review` should be inspected before moving on

## 5. Field inventory
- look for matches under the target fields you care most about
- use this file to decide which source should power each feature family in the model

---

# What this repo still does not do

This repo **does not build final projections yet**.

It builds the historical + operational data layer needed before feature engineering and modeling.
