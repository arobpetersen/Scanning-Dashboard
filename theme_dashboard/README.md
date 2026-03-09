# Thematic Stock Dashboard (Local v1)

A local-first Streamlit app for **objective, formula-based theme ranking** and theme registry management.

## What this app does
- Imports themes from `themes_seed_structured.json` on first run only.
- Stores and manages themes in local DuckDB afterward (DuckDB is source of truth).
- Refreshes ticker snapshots from a provider (`mock` and `live` via Massive).
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

## Massive live provider setup

1. Create a Massive API key at https://massive.com/ (Polygon API).
2. Export it in your shell before launching Streamlit:

```bash
export MASSIVE_API_KEY="your_api_key_here"
```

3. In app sidebar, choose `live` provider.

If `MASSIVE_API_KEY` is not set and you choose `live`, the app shows a warning and gracefully falls back to `mock` for refresh so the app remains usable.

> Keep secrets local: do **not** hardcode API keys in source files and do **not** commit `.env` or shell files containing secrets.

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

## Live data architecture
- Live mode now uses **Massive (Polygon)** as the historical price source for return calculations and proof-of-concept quote/reference fields.
- Why this change: Finnhub `/stock/candle` returned `403 Forbidden` in this setup, so return calculations were refactored to use Massive daily aggregates.
- Massive setup is optimized for **small scoped refreshes** (selected theme or short custom ticker list), especially for free-tier testing.
- Deterministic return formulas remain unchanged and are computed from daily closes:
  - `perf_1w = ((close_latest - close_5_trading_days_ago) / close_5_trading_days_ago) * 100`
  - `perf_1m = ((close_latest - close_21_trading_days_ago) / close_21_trading_days_ago) * 100`
  - `perf_3m = ((close_latest - close_63_trading_days_ago) / close_63_trading_days_ago) * 100`
- Nullable in live mode (not faked when unavailable):
  - `short_interest_pct`
  - `float_shares`
  - `adr_pct`
  - `market_cap` (if Massive reference data is unavailable for a ticker)
- `avg_volume` is computed as simple mean of recent daily volumes (last 21 daily bars) from Massive aggregates.

## Providers
- `mock`: deterministic sample data for all tickers so the app is usable immediately.
- `live`: Massive-backed provider in `src/provider_live.py`.

## Ranking formulas (auditable)
- `avg_1w = mean(perf_1w)`
- `avg_1m = mean(perf_1m)`
- `avg_3m = mean(perf_3m)`
- `positive_1w_breadth_pct = percent(perf_1w > 0)`
- `positive_1m_breadth_pct = percent(perf_1m > 0)`
- `positive_3m_breadth_pct = percent(perf_3m > 0)`
- `composite_score = 0.25*avg_1w + 0.50*avg_1m + 0.25*avg_3m`

Weights are configured in `src/config.py`.


## Refresh safety and operational workflow
- Only one refresh run can be active at a time. If a run is already `running`, a new request is blocked and logged with status `blocked`.
- Stale run protection is enabled: runs left in `running` state beyond the timeout are auto-marked `failed` with a stale-run error message.
- Timeout is configurable via `REFRESH_STALE_TIMEOUT_MINUTES` in `src/config.py`.
- Progress is visible during refresh in the homepage (provider, completed/total tickers, success/failure counts, elapsed seconds).
- `success_count` and `failure_count` are updated incrementally during execution for better observability in Diagnostics.

## Scoped refresh behavior
To keep live runs operationally usable, the homepage supports scoped refresh:
- **Active themes**: full active ticker universe
- **Selected theme** (**default in live mode**): tickers from a single theme
- **Custom ticker list**: manual ticker subset

Large full-universe live refreshes can be slow due to API calls and may hit rate limits. Prefer scoped refresh for day-to-day live usage.

During refresh, progress is persisted incrementally (`success_count`, `failure_count`, and progress notes) so Diagnostics can show in-flight activity.

Scope observability: each run records scope metadata (`scope_type`, `scope_theme_name`) and the resolved ticker universe in `refresh_run_tickers`, which is visible in Diagnostics.

Live safeguard: the run stops early if repeated rate-limit errors are detected (configured by `LIVE_RATE_LIMIT_STOP_THRESHOLD` in `src/config.py`) and is finalized cleanly with a summary error message.


## Suggestions and review workflow
- Suggestions are stored in DuckDB (`theme_suggestions`) and are **separate** from direct theme registry edits.
- Supported suggestion types:
  - `add_ticker_to_theme`
  - `remove_ticker_from_theme`
  - `create_theme`
  - `rename_theme`
  - `move_ticker_between_themes`
- Workflow is explicit and auditable:
  1. Create suggestion (status `pending`)
  2. Review suggestion (`approved` or `rejected`, with notes)
  3. Apply approved suggestion (status becomes `applied`)
- Suggestions include source metadata (`manual`, `rules_engine`, `ai_proposal`, `imported`) so future automation engines can plug in cleanly.

- Validation rules block invalid/redundant suggestions at creation time (e.g., add existing ticker, remove missing ticker, duplicate pending proposal, blank/duplicate theme names, invalid move semantics).
- Approved suggestions are validated again at apply time to catch stale queue items after registry changes.
- Queue shows a computed `validation_status` indicator (`valid`, `stale`, `duplicate_pending`) to highlight actionability.
- Applied suggestions update the same DuckDB theme source-of-truth tables used by Theme Manager and refresh runs.
