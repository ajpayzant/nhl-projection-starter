# NHL Projection Data Build — Revised v3

This repository is the corrected multi-source **data build layer** for the NHL projection system.

It is designed to do three things well before we build the actual model:

1. **pull the right data from the right source**
2. **save raw and processed files in a stable folder structure**
3. **validate the outputs so we can confirm the data is actually usable**

## Source map

- **NHL API** → schedules, game IDs, gamecenter boxscores, shift charts, official game structure
- **Hockey-Reference** → season skater / goalie tables, EV / PP / SH production fields, FOW / FOL / FO%, TSA, BLK, HIT, TAKE, GIVE
- **MoneyPuck** → advanced skater / goalie / team / line data, link discovery, shot/xG dictionary and file manifests
- **Daily Faceoff** → line combinations, PP/PK units, same-day goalie context

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

## Step B — Hockey-Reference enrichment

```bash
python scripts/run_phase2b_hockeyref_ingest.py --season 20252026
```

## Step C — MoneyPuck advanced ingest

```bash
python scripts/run_phase2c_moneypuck_ingest.py --season 20252026
```

## Step D — Daily Faceoff operational ingest

```bash
python scripts/run_phase2d_dailyfaceoff_ingest.py --date 2026-03-11
```

## Step E — Build reference audit / dictionary

```bash
python scripts/run_phase2e_reference_audit.py --season 20252026
```

## Step F — Build silver cleaned tables

```bash
python scripts/run_phase2f_build_silver.py --season 20252026
```

## Step G — Validate outputs

```bash
python scripts/run_phase2g_validate_outputs.py --season 20252026
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

## Hockey-Reference
Check:

```text
data/processed/hockey_reference/season_20252026_regular/
```

You should see:
- `skaters_standard.csv`
- `skaters_advanced.csv`
- `goalies_standard.csv`
- `table_dictionary.csv`
- `table_summary.csv`

Also inspect raw diagnostics under:

```text
data/raw/hockey_reference/season_20252026_regular/
```

Important diagnostic files:
- `skaters_standard_diagnostic.txt`
- `skaters_advanced_diagnostic.txt`
- `goalies_standard_diagnostic.txt`
- `*_table_candidates.csv`

## MoneyPuck
Check:

```text
data/processed/moneypuck/season_20252026_regular/
```

You should see:
- `discovered_links.csv`
- `download_manifest.csv`
- downloaded CSVs under `downloads/`

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

---

# How to know the data is “as it should be”

Run the validation step and inspect:

## 1. NHL API counts
- `games` should have the number of games you requested
- `team_game_stats` should usually be about `2 x games`
- `skater_game_stats` should be much larger than `games`
- `goalie_game_stats` should usually be several rows per game
- `ingest_errors.csv` should ideally be empty

## 2. Hockey-Reference tables
- `table_summary.csv` should not show all three tables as `empty`
- `table_dictionary.csv` should list real columns like `player`, `team`, `gp`, `evg`, `ppg`, `fow`, `fol`, `tsa`, `blk`, `hit`

## 3. MoneyPuck manifest
- `download_manifest.csv` should show `ok` on the main season summary rows

## 4. Silver tables
- TOI should have parsed numeric columns like `toi_seconds` and `toi_minutes`
- goalie ratio fields should split into numeric columns
- duplicate flattened ID fields should be reduced

---

# If Hockey-Reference looks empty

Look first at:

```text
data/raw/hockey_reference/season_20252026_regular/*_diagnostic.txt
```

and:

```text
data/raw/hockey_reference/season_20252026_regular/*_table_candidates.csv
```

Those files tell you:
- whether the page loaded
- whether HTML tables were present
- how many candidate tables were found
- which candidate had the strongest column match

---

# What this repo still does not do

This repo **does not build final projections yet**.

It builds the corrected historical + operational data layer needed before feature engineering and modeling.
