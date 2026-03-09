# NHL Projection System Starter (Phase 1)

This starter repository implements **Phase 1** of the NHL projection build:

1. environment setup
2. source access validation
3. raw data probe scripts
4. directory structure for later model stages

It is intentionally focused on **proving data access and schema viability before feature engineering**.

## Recommended stack

- **Core development:** GitHub + Codespaces (or local VS Code)
- **Experimentation:** Google Colab
- **Persistent storage:** Google Drive
- **Trader UI / overrides:** Google Sheets

## Repository layout

```text
nhl_projection_starter/
├── .devcontainer/
├── config/
├── data/
│   ├── raw/
│   └── processed/
├── logs/
├── scripts/
└── src/nhl_model/
    ├── probes/
    └── utils/
```

## What this phase does

### NHL API probe
Checks:
- schedule endpoint
- club schedule endpoint
- gamecenter endpoint
- shift chart endpoint

Saves:
- endpoint health summary
- small normalized samples
- raw JSON payloads for inspection

### MoneyPuck probe
Checks:
- site availability
- HTML availability of the download page
- extraction of downloadable CSV links where present
- detection of current-year or historical references in the page

Saves:
- page snapshot metadata
- extracted link table

### Daily Faceoff probe
Checks:
- starting goalies page availability
- team line combinations page availability
- basic HTML parsing for headings, links, timestamps, or textual markers

Saves:
- page metadata
- discovered links / text snippets useful for future scraper design

## Quick start

### 1. Create the environment

#### Codespaces
Open the repo in Codespaces. The dev container will install Python dependencies automatically.

#### Local
```bash
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
pip install -e .
```

### 2. Run all probes
```bash
python scripts/run_all_probes.py --season 20252026 --game-id 2025020001 --team BOS
```

### 3. Review outputs
Inspect:
- `data/raw/probes/nhl_api/`
- `data/raw/probes/moneypuck/`
- `data/raw/probes/dailyfaceoff/`
- `logs/`

## Google Workspace setup (for later phases)

Before using Drive/Sheets integration, create a Google Cloud project and enable:
- Google Drive API
- Google Sheets API

Download OAuth desktop credentials as `credentials.json` and place them in the project root only on your machine (never commit them).

## Next phases after this starter

### Phase 2
Raw ingestion tables:
- games
- teams
- players
- team_game_stats
- skater_game_stats
- goalie_game_stats
- shift_charts

### Phase 3
Pregame operational inputs:
- expected line combinations
- starting goalie expectation
- injuries / scratches
- manual override table

### Phase 4
Feature engineering:
- team rolling form
- player rolling usage
- PP share
- line correlation
- opponent adjustments
- rest / home-away / back-to-back

### Phase 5
Modeling:
- team environment model
- player TOI model
- player rate model
- goalie model
- reconciliation layer

## Notes

This repository is deliberately **modular**. The probe stage should be completed and reviewed before building feature tables so that schema issues are discovered early.
