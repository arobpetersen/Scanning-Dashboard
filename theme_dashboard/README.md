# Thematic Stock Dashboard (Local v1)

A local-first Streamlit app for **objective, formula-based theme ranking** and theme registry management.

## What this app does
- Imports themes from `themes_seed_structured.json` on first run only.
- Stores and manages themes in local DuckDB afterward (DuckDB is source of truth).
- Refreshes ticker snapshots from a provider (`mock` implemented, `live` placeholder).
- Calculates deterministic rankings from numeric ticker metrics only.
- Stores historical ticker snapshots and historical theme snapshots on every successful/partial refresh.
- Shows trend deltas between latest and prior theme snapshots.
- Provides pages for Home, Theme Detail, Theme Manager, and Diagnostics.

## Project structure

```
theme_dashboard/
  app.py
  requirements.txt
  README.md
  themes_seed_structured.json
  /src
    config.py
    database.py
    models.py
    seed_loader.py
    provider_base.py
    provider_mock.py
    provider_live.py
    fetch_data.py
    rankings.py
    queries.py
    theme_service.py
  /pages
    1_Theme_Detail.py
    2_Theme_Manager.py
    3_Diagnostics.py
```

## Setup (foolproof)

Run these commands from repo root (`/workspace/Scanning-Dashboard`):

```bash
cd theme_dashboard
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

Open the URL Streamlit prints (normally `http://localhost:8501`).

## First run behavior
1. `src/database.py` initializes local DuckDB tables automatically.
2. If the `themes` table is empty, `src/theme_service.py::seed_if_needed()` imports `themes_seed_structured.json`.
3. From that point onward, all edits happen in DuckDB via Theme Manager.

## Ongoing source of truth
- **DuckDB is the ongoing source of truth** after initialization.
- The seed JSON is only a bootstrap input file.

## Historical snapshot behavior
- Every refresh run writes ticker-level records to `ticker_snapshots` keyed by `run_id`.
- After successful/partial refresh completion, theme-level metrics are computed and stored in `theme_snapshots` with:
  - `run_id`
  - `snapshot_time`
  - `theme_id`
  - `ticker_count`
  - `avg_1w`, `avg_1m`, `avg_3m`
  - `positive_1w_breadth_pct`, `positive_1m_breadth_pct`, `positive_3m_breadth_pct`
  - `composite_score`
- Trend deltas are explicit latest-vs-previous differences:
  - `delta_avg_1w`
  - `delta_avg_1m`
  - `delta_avg_3m`
  - `delta_positive_1m_breadth_pct`
  - `delta_composite_score`

## Providers
- `mock`: deterministic sample data for all tickers so the app is usable immediately.
- `live`: placeholder provider.

### Which file to edit first for a real market provider?
Start with **`src/provider_live.py`**. That is the first file you should implement.

Then, if needed:
- update credential/config handling in `src/config.py`
- keep `src/fetch_data.py` unchanged unless response mapping requires adjustments

## Ranking formulas (auditable)
- `avg_1w = mean(perf_1w)`
- `avg_1m = mean(perf_1m)`
- `avg_3m = mean(perf_3m)`
- `positive_1w_breadth_pct = percent(perf_1w > 0)`
- `positive_1m_breadth_pct = percent(perf_1m > 0)`
- `positive_3m_breadth_pct = percent(perf_3m > 0)`
- `composite_score = 0.25*avg_1w + 0.50*avg_1m + 0.25*avg_3m`

Weights are configured in `src/config.py`.
