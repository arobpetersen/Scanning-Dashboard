# Thematic Stock Dashboard (Local v1)

A local-first Streamlit app for **objective, formula-based theme ranking** and theme registry management.

## What this app does
- Imports themes from `themes_seed_structured.json` on first run only.
- Stores and manages themes in local DuckDB afterward (DuckDB is source of truth).
- Refreshes ticker snapshots from a provider (`mock` and `live` via Massive).
- Calculates deterministic rankings from numeric ticker metrics only.
- Stores historical ticker snapshots and historical theme snapshots on every successful/partial refresh.
- Shows trend deltas between latest and prior theme snapshots.
- Provides a simplified operations-centered page structure: Home (control center), Theme Explorer, Theme Registry, Operations & Diagnostics, Suggestions Queue, Theme Health, Historical Performance, and AI Proposal Assistant.

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
    3_Operations_Diagnostics.py
    4_Suggestions_Queue.py
    5_Theme_Health.py
    6_Historical_Performance.py
    7_AI_Proposal_Assistant.py
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
3. From that point onward, all edits happen in DuckDB via Theme Registry.

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


## Manual refresh efficiency model (live + mock)
- External API calls happen only in explicit **manual refresh** actions from the dashboard control center.
- All pages (Themes, Historical Performance, Suggestions, Health) read from DuckDB snapshots; navigation/filtering/chart changes do not trigger provider fetches.
- Refresh write path:
  1) fetch provider data,
  2) persist `ticker_snapshots`,
  3) persist derived `theme_snapshots`,
  4) render UI from DuckDB.
- Live refresh now defaults to daily aggregate endpoints for returns and avoids per-refresh reference endpoint fetches by default; missing `market_cap` is backfilled from the latest persisted ticker snapshot when available.
- Lightweight refresh accounting is stored per run in `refresh_runs`:
  - `api_call_count`,
  - `api_endpoint_counts` (JSON map),
  - `skipped_tickers` (failed/errored symbols).
- Homepage last-run card surfaces API call accounting in an expander for operational visibility.


Live safeguard: the run stops early if repeated rate-limit errors are detected (configured by `LIVE_RATE_LIMIT_STOP_THRESHOLD` in `src/config.py`) and is finalized cleanly with a summary error message.


## Suggestions and review workflow
- Suggestions are stored in DuckDB (`theme_suggestions`) and are **separate** from direct theme registry edits.
- Supported suggestion types:
  - `add_ticker_to_theme`
  - `remove_ticker_from_theme`
  - `create_theme`
  - `rename_theme`
  - `move_ticker_between_themes`
  - `review_theme` (rules/manual review marker; no direct registry mutation)
- Workflow is explicit and auditable:
  1. Create suggestion (status `pending`)
  2. Review suggestion (`approved` or `rejected`, with notes)
  3. Apply approved suggestion (status becomes `applied`)
- Suggestions include source metadata (`manual`, `rules_engine`, `ai_proposal`, `imported`) so future automation engines can plug in cleanly.

- Validation rules block invalid/redundant suggestions at creation time (e.g., add existing ticker, remove missing ticker, duplicate pending proposal, blank/duplicate theme names, invalid move semantics).
- Approved suggestions are validated again at apply time to catch stale queue items after registry changes.
- Queue shows a computed `validation_status` indicator (`valid`, `stale`, `duplicate_pending`) to highlight actionability.
- Suggestions page now uses database-backed selectors for existing membership actions (remove/move ticker) and shows current theme members to reduce manual-entry errors during creation.
- Applied suggestions update the same DuckDB theme source-of-truth tables used by Theme Manager and refresh runs.
- Queue cleanup tools are available on the Suggestions page and operate on the **currently filtered queue**.
- Primary cleanup action is bulk mark-as-`obsolete` (audit-preserving, no hard delete by default).
- Optional bulk reject is also available for filtered pending/approved items.
- Cleanup writes reviewer notes and timestamps so legacy/noisy suggestions can be retired without losing history.


## Deterministic rules engine
- The Suggestions page includes a manual trigger to run a deterministic rules engine.
- Rules engine outputs are inserted into the same suggestions queue with `source = rules_engine`.
- It never auto-applies changes and always uses the existing review/approve/apply governance flow.
- Suggestions now include a triage priority (`low` / `medium` / `high`) so reviewers can focus on the most actionable items first.
- First-wave low-noise rules:
  - `low_constituent_count_review` (`medium`): flags non-empty themes under the configurable member threshold (`RULE_LOW_CONSTITUENT_THRESHOLD`).
  - `empty_theme_review` (`high`): flags themes with zero members.
  - `inactive_theme_cleanup_review` (`medium`): flags inactive themes that still contain members.
  - `repeated_live_failure_review` (`high`): flags tickers only when repeated **ticker-specific** live failures are dominant/actionable (`ticker_data_missing`, `ticker_symbol_issue`, `no_candles`). Provider-wide failures are suppressed from ticker proposal generation.
- Overlap across themes is generally acceptable in this taxonomy, so duplicate-membership overlap is **not** treated as a default problem and no longer generates suggestions by default.
- Rule runs show concise output by rule (`severity`, `evaluated`, `created`, `duplicates_skipped`, per-rule cap), and proposals remain auditable in queue history.
- Live failures are categorized into: `provider_limit`, `provider_auth`, `provider_outage`, `ticker_data_missing`, `ticker_symbol_issue`, `no_candles`, `unknown`.
- Provider-level failure patterns remain visible in Diagnostics and in rules-run provider signal summaries, but do not flood ticker-level review suggestions.
- Noise guardrails are configurable in `src/config.py` (including `RULE_MAX_SUGGESTIONS_PER_RULE`).

## Theme Health / Maintenance view
- A dedicated **Theme Health / Maintenance** page provides an operational quality view without flooding the suggestions queue.
- For each theme, it surfaces:
  - name, category, active/inactive status,
  - constituent count,
  - low-count and empty flags,
  - recent live refresh failure count across member tickers,
  - latest theme snapshot timestamp,
  - simple health status (`healthy`, `watch`, `needs_attention`).
- The page supports lightweight filtering for active/inactive, low-count, and empty-theme flags.


## Simplified page structure
- **Dashboard**: operations control center for refresh, high-level rankings, and key queue signals.
- **Themes**: consolidated explore + manage experience (detail, members, create/edit/delete, ticker membership) plus Top 10 discovery leaderboards for 1W and 1M theme performance with direct row-click drill-down.
- Themes leaderboards use direct table click drill-down into the same detailed theme view (no extra button workflow, and no checkbox selection column).
- **Historical Performance**: lookback-based trend and leadership rotation analysis.
- **Suggestions**: consolidated manual creation, queue review/apply, bulk cleanup, rules trigger, and AI assistant.
- **Health**: consolidated operations diagnostics + provider/failure visibility + theme health maintenance.

## Historical theme tracking and leadership movement
- Historical page supports common windows (1 week, 1 month, 3 months) and custom days.
- Includes top-N cross-theme trend line charts for metrics such as `composite_score`, `avg_1w`, `avg_1m`, `avg_3m`, `positive_1m_breadth_pct`, and `ticker_count`.
- Shows biggest movers over selected window (`delta_composite`, breadth change), plus top-N membership changes (entered/dropped leaders).
- Provides a single-theme history panel to inspect one theme over time with line charts and raw history table.

## AI-assisted proposal generation (controlled)
- AI proposal generation is **manual trigger only** from the AI Proposal Assistant page.
- AI can only create records in `theme_suggestions` with `source = ai_proposal`; it never mutates registry tables directly.
- AI outputs must pass the existing validation and duplicate-prevention rules in `suggestions_service.py`.
- Generated suggestions still require normal human review (`approve/reject`) and optional apply flow.
- AI supports proposal types: `add_ticker_to_theme`, `remove_ticker_from_theme`, `create_theme`, `rename_theme`, `review_theme`.
- AI proposals include rationale/evidence text, and weak proposals can be skipped.


## AI proposal request serialization
- AI context is sanitized to JSON-safe primitives before request construction.
- Pandas/Datetime values (including `Timestamp`) are converted to ISO-8601 strings to avoid `Object of type Timestamp is not JSON serializable`.
- AI key messaging reflects real key presence: warning is shown only when `OPENAI_API_KEY` is missing.
- AI remains queue-only: proposals are inserted as `source = ai_proposal` and must follow normal review/approve/apply governance.


## Theme Momentum Engine
- `src/momentum_engine.py` computes deterministic momentum/leadership analytics from historical `theme_snapshots`.
- Core tracked inputs include: `composite_score`, `avg_1w`, `avg_1m`, `avg_3m`, `positive_1m_breadth_pct`, and `ticker_count`.
- Derived outputs include:
  - strongest momentum themes,
  - biggest risers / fallers,
  - breadth improvers,
  - weakening themes,
  - top-N leadership entrants and dropouts,
  - rank and composite deltas over the selected window.
- Momentum score (auditable formula):
  - `0.45*delta_composite + 0.25*delta_avg_1m + 0.20*delta_breadth + 0.10*rank_change`.

## Historical momentum views
- Historical Performance now combines single-theme history and cross-theme rotation analysis.
- Supported lookback windows: 1 week, 1 month, 3 months, and custom days.
- Includes momentum summary sections:
  - Top Momentum Themes
  - Biggest Risers
  - Biggest Fallers
  - New Leaders
  - Breadth Improvers
  - Weakening Themes
- Top-N movement analysis makes it easy to track leadership rotation and emerging strength.


## Synthetic historical backfill generator
To test momentum and historical leadership views without live API calls, use:

```bash
python tools/generate_mock_history.py --days 120 --frequency weekly --seed 42 --volatility medium --reset
```

Options:
- `--days` lookback window to synthesize (default `120`)
- `--frequency` `daily|weekly|monthly` (default `weekly`)
- `--seed` deterministic reproducibility seed
- `--volatility` `low|medium|high` movement amplitude
- `--reset` clear prior snapshots/runs before backfill
- `--shocks` optional comma-separated shock events:
  - `ai_boom`
  - `energy_crash`
  - `defense_rally`

Generated data includes archetypes (`persistent_leader`, `emerging_theme`, `weakening_theme`, `choppy_theme`, `dead_theme`) and rotation phases so charts show non-flat leadership movement.

Snapshot provenance tagging:
- `live`
- `mock`
- `synthetic_backfill`

The dashboard shows a "Synthetic historical data active" indicator when synthetic snapshots are present.


## Historical Performance readability modes
- Historical Performance is split into two clear sections:
  - **Top Theme Overview (fixed cross-window)**: side-by-side 1W/1M/3M compact leaderboards ranked independently by each window return metric; clicking a row jumps to Single Theme History for that theme.
  - **Analysis Workspace (reactive controls)**: all lookback, top-N, comparison metric, filter, display mode, and smoothing controls apply only here.
- Top-N analysis is separated from chart display:
  - **Top N analyzed** controls leadership/momentum universe.
  - **Themes shown in chart** controls how many lines are plotted for readability.
- Chart display modes:
  - **raw metric**,
  - **indexed (100=start)**,
  - **rank movement**.
- Optional smoothing is available:
  - none,
  - 3 period rolling,
  - 5 period rolling.
- Theme selection controls include category filter, theme search, and watchlist pinning so selected themes remain visible in charts.
- If too few snapshots exist for a selected window, the page shows a data sufficiency warning instead of rendering broken visuals (minimum 2 boundary snapshots for valid comparison).
- Short-window lookbacks use boundary snapshots anchored to the latest available snapshot (nearest snapshot at/before the window start) to avoid fragile 1W behavior on weekly or irregular cadence.
- Themes with fewer than 2 data points in the selected window are automatically skipped with an informational message.
- Overview and Themes leaderboards share a common window-specific ranking helper to keep sorting behavior consistent.
- Historical analytics tables include concise section descriptions and header tooltips (column help) to explain metric meaning and interpretation.
- The page includes a collapsible **Metric Guide** section summarizing momentum, breadth, rank change, and delta metrics.
- A deterministic **Theme Signals (Inflection Feed)** block highlights emerging, accelerating, rotating-in, weakening, and rotating-out themes with trigger reasons and detection timestamps.

## Theme Rotation Engine
- `src/rotation_engine.py` derives deterministic rotation signals from momentum outputs.
- Rotation sections include:
  - Rotating Into Leadership,
  - Rotating Out Of Leadership,
  - Emerging Themes,
  - Fading Themes,
  - Acceleration in Leadership,
  - Deterioration in Leadership.
- Rotation intensity metrics:
  - themes entering Top N,
  - themes exiting Top N,
  - `rotation_intensity_score = ((entered + exited) / top_n) * 100`.
- Historical page also includes a Theme Momentum Leaderboard for selected windows.

## Theme Inflection Engine
- `src/inflection_engine.py` computes deterministic inflection signals from existing momentum + rotation outputs (no duplicate snapshot model).
- Signals emitted: `emerging`, `accelerating`, `rotating_into`, `weakening`, `leadership_deterioration`, and `rotating_out`.
- Each signal includes:
  - `detected_at` snapshot timestamp,
  - signal label/type,
  - trigger reason text,
  - supporting deltas (`rank_change`, `momentum_score`, `delta_composite`, `delta_avg_1m`, `delta_breadth`).
- Noise control keeps only the highest-priority signal per theme in a run to avoid repetitive overlap.
- If history is sparse (fewer than 2 boundary snapshots), the feed is suppressed with a clear insufficiency message.
