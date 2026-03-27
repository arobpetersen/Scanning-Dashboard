# Calculated Fields Dictionary

This document describes the calculation semantics currently implemented on branch `2.9-DEV-Agenetic-Refinemnet`.

Scope:
- Themes page semantics
- Historical Performance page semantics
- Current ranking eligibility, capping, and weighting
- Historical movement provenance and boundary resolution
- Expected disagreement cases that are not bugs

Code is the source of truth. If UI wording or older notes disagree with this file, trust the current code in:
- [theme_dashboard/pages/1_Themes.py](theme_dashboard/pages/1_Themes.py)
- [theme_dashboard/pages/2_Historical_Performance.py](theme_dashboard/pages/2_Historical_Performance.py)
- [theme_dashboard/src/rankings.py](theme_dashboard/src/rankings.py)
- [theme_dashboard/src/queries.py](theme_dashboard/src/queries.py)
- [theme_dashboard/src/momentum_engine.py](theme_dashboard/src/momentum_engine.py)
- [theme_dashboard/src/rotation_engine.py](theme_dashboard/src/rotation_engine.py)
- [theme_dashboard/src/leaderboard_utils.py](theme_dashboard/src/leaderboard_utils.py)
- [theme_dashboard/src/inflection_engine.py](theme_dashboard/src/inflection_engine.py)

## Core Current Ranking Semantics

### Current ranking constituent base

Current theme-facing ranking surfaces are built from `compute_current_ranking_snapshot()` in `src/rankings.py`.

Constituent load path:
- `themes` + `theme_membership`
- latest preferred-source ticker rows from `latest_ticker_snapshots()`
- optional suppression status from `symbol_refresh_status`

Current ranking eligibility is per ticker:
- `snapshot_present = run_id not null AND snapshot_time not null`
- `price_valid = price >= CURRENT_RANKING_MIN_PRICE`
- `avg_volume_valid = avg_volume > 0`
- `dollar_volume_valid = price * avg_volume >= CURRENT_RANKING_MIN_DOLLAR_VOLUME`
- `not_refresh_suppressed = status != 'refresh_suppressed'`
- `eligible_ticker = snapshot_present AND price_valid AND avg_volume_valid AND dollar_volume_valid AND not_refresh_suppressed`

Return handling:
- `perf_1w`, `perf_1m`, `perf_3m` are capped to `[-CURRENT_RANKING_RETURN_CAP_PCT, +CURRENT_RANKING_RETURN_CAP_PCT]` before aggregation
- means are calculated only across eligible tickers with non-null return for that window
- breadth uses raw sign on eligible returns, not capped sign

Theme-level current ranking fields:
- `ticker_count = count of governed member tickers present in theme_membership`
- `eligible_ticker_count = count of current eligible tickers`
- `eligible_1w_count`, `eligible_1m_count`, `eligible_3m_count` = count of eligible non-null contributors by window
- `eligible_composite_count = count of tickers eligible for all 3 windows`
- `eligible_standardized_count = count of tickers eligible for both 1W and 1M`
- `eligible_breadth_pct = eligible_ticker_count / ticker_count * 100`
- `avg_1w`, `avg_1m`, `avg_3m` = mean of capped eligible returns
- `positive_1w_breadth_pct`, `positive_1m_breadth_pct`, `positive_3m_breadth_pct` = percent of eligible non-null raw returns above zero
- `legacy_composite_score = legacy branch composite retained for comparison`
- `standardized_base_strength_score`, `standardized_participation_ratio`, `standardized_participation_factor`, `standardized_guardrail_factor`, `standardized_recovery_factor`, `standardized_composite_score` = staged validation fields for the new standardized baseline
- `current_momentum_raw_score`, `current_momentum_quality_factor`, `current_momentum_score` = staged validation fields for the new current-thrust momentum model

Composite score:

```text
base_score =
    0.25 * avg_1w
  + 0.50 * avg_1m
  + 0.25 * avg_3m

theme_confidence_factor =
    min(1, sqrt(ticker_count / THEME_CONFIDENCE_FULL_COUNT))

composite_score =
    base_score * theme_confidence_factor
```

Important nuance:
- the confidence factor uses `ticker_count`, not `eligible_composite_count`
- `composite_score` is set to null when `eligible_composite_count == 0`

Standardized composite score validation fields:

```text
standardized_base_strength_score =
    0.30 * avg_1w
  + 0.70 * avg_1m

standardized_participation_ratio =
    eligible_standardized_count / max(ticker_count, 1)

standardized_participation_factor =
    clip(0.50 + standardized_participation_ratio, 0.50, 1.00)

standardized_guardrail_factor =
    1.00                                                         when avg_3m >= -10
    1.00 - (((-10 - avg_3m) / 10) * 0.35)                       when -20 <= avg_3m < -10
    max(0.30, 0.65 - (((-20 - avg_3m) / 10) * 0.35))            when avg_3m < -20

standardized_recovery_factor =
    1.00                                                         when avg_3m >= -10
    clip(standardized_base_strength_score / 15.0, 0.50, 1.00)   when avg_3m < -10

standardized_composite_score =
    standardized_base_strength_score
  * standardized_participation_factor
  * standardized_guardrail_factor
  * standardized_recovery_factor
```

Notes:
- 3M is not a weighted input in the staged standardized score; it acts only as a two-zone soft guardrail
- when 3M is weak, recent 1W/1M strength must clear a higher burden of proof to regain full credit
- the participation adjustment is percentage-based and reaches full credit at roughly 50% participation
- the legacy `composite_score` remains available as the comparison baseline on this branch

Current momentum validation fields:

```text
current_momentum_raw_score =
    0.70 * avg_1w
  + 0.30 * avg_1m

current_momentum_quality_factor =
    1.00                                       when standardized_composite_score >= 10
    0.60 + (((score - 5) / 5) * 0.40)         when 5 < standardized_composite_score < 10
    0.60                                       when standardized_composite_score <= 5

current_momentum_score =
    current_momentum_raw_score
  * current_momentum_quality_factor
```

Notes:
- this is a current-thrust score, not the historical start/end `momentum_score` from Historical Performance
- it stays close to raw current 1W leaders, but weak baseline themes only keep partial credit until their standardized composite improves
- the Themes page exposes it in a localized `Current Momentum Validation` expander without replacing the default current 1W table
- the Themes page now uses the standardized composite as its default visible `composite_score` / baseline-strength metric, while legacy composite remains available only in the validation/debug layer

Current ranking minimum threshold:
- `CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS` gates current leaderboard inclusion
- current leadership requires `eligible_composite_count >= threshold`
- current top-by-window requires the corresponding `eligible_1w_count` or `eligible_1m_count` or `eligible_3m_count` to meet the threshold

Leadership quality label from `current_leadership_quality_label()`:
- define `participation_ratio = eligible_contributor_count / max(ticker_count, 1)`
- `Thin / filtered` if `eligible_contributor_count <= 2` or (`eligible_contributor_count <= 3` and `participation_ratio < 0.40`)
- `Broad leader` if `eligible_contributor_count >= 4` and `participation_ratio >= 0.50` and `breadth_1m >= 60`
- otherwise `Narrow leader`

## Themes Page

### Current Market Leadership

Purpose:
- strongest current themes now, using the current ranking pipeline

View type:
- current-view

Source path:
- `compute_current_ranking_snapshot()` -> `theme_metrics`
- `build_current_leadership_table()`

Visible columns:
- `rank`
- `theme`
- `category`
- `composite_score`
- `avg_1w`
- `avg_1m`
- `avg_3m`
- `breadth_1m`
- `ticker_count`
- `eligible_contributor_count`
- `eligible_breadth_pct`
- `leadership_quality`

Sort order:
- `composite_score desc`
- `positive_1m_breadth_pct desc`
- `eligible_composite_count desc`
- `ticker_count desc`
- `theme asc`

Filters and gates:
- `is_active == True`
- `eligible_composite_count >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS`

Semantics:
- `avg_*` values are eligible-only and capped
- `breadth_1m` is renamed from `positive_1m_breadth_pct`
- `eligible_contributor_count = eligible_composite_count`
- optional per-table Themes daily-delta toggles compare only `composite_score`, `avg_1w`, and `avg_1m` against the prior daily movement endpoint from the cached 1M movement history; when enabled, the page keeps those fields inline and adds the daily delta in parentheses beside the base value; default table ranking/sorting does not change
- plain percentage fields on Themes tables render with `%` suffixes even when delta toggles are off
- a localized `Standardized Composite Validation` expander on Themes compares the staged standardized score against the legacy baseline without replacing the default leadership table yet, including the staged `guardrail_factor` and `recovery_factor`
- a localized `Current Momentum Validation` expander on Themes compares raw current 1W leaders against the staged current-thrust momentum model, alongside the standardized composite baseline and the momentum `quality_factor`
- the main current Themes tables now show `momentum` next to `composite_score` as the trader-facing thrust-now versus baseline-strength pairing, while redundant eligible-count columns stay out of the default visible table layout
- current 1W / current 1M tables expose `rank_change` only in the optional daily-delta view, where it reflects prior daily rank minus current rank for that same window metric

### Current Top Themes By Window

Purpose:
- strongest current themes for one return window at a time

View type:
- current-view

Source path:
- `compute_current_ranking_snapshot()` -> `theme_metrics`
- `build_current_performance_table()`

Major surfaces:
- `Top Themes - Current 1W`
- `Top Themes - Current 1M`

Visible columns:
- `rank`
- `theme`
- `category`
- `performance`
- `composite_score`
- `breadth_1m`
- `ticker_count`
- `eligible_contributor_count`
- `eligible_breadth_pct`
- `leadership_quality`

Sort order:
- selected `perf_col desc`
- `composite_score desc`
- `eligible_contributor_count desc`
- `eligible_breadth_pct desc`
- `theme asc`

Filters and gates:
- `is_active == True`
- `eligible_1w_count >= threshold` for 1W
- `eligible_1m_count >= threshold` for 1M
- same capped eligible-return semantics as Current Market Leadership

Semantics:
- `performance` is the capped current eligible mean for that window
- `composite_score` is still shown as context, but is not the primary ranking key here
- when the table-local daily-delta toggle is enabled, the page keeps `performance` plain and adds display-only inline parenthesis deltas only for `composite_score` and the relevant `avg_1w` / `avg_1m` fields versus the prior daily movement endpoint; ranking remains unchanged

### Theme Movement Snapshots

Purpose:
- top historical themes across a resolved 1W or 1M movement window

View type:
- boundary-window historical view

Source path:
- `theme_history_window()`
- `compute_theme_momentum()`
- `build_window_leaderboard()`
- `build_category_leaderboard()`
- `build_category_theme_breakdown()`

Major surfaces:
- `Top 10 Themes - 1W`
- `Top 10 Themes - 1M`
- `Top Categories - 1W`
- `Top Categories - 1M`
- category drill to underlying themes

Theme leaderboard visible columns:
- `rank`
- `theme`
- `category`
- `performance`
- `momentum_score`
- optional `rank_change`
- optional `breadth_1m`

Theme leaderboard sort order from `build_window_leaderboard()`:
- latest selected `perf_col desc`
- `momentum_score desc`
- `rank_change desc`
- `theme asc`

Category leaderboard visible columns:
- `rank`
- `category`
- `top_themes`
- `contributing_themes`
- `performance`
- `momentum_score`
- `breadth_1m`

Category aggregation:
- built from the full latest theme set in the selected window, not just the displayed top 10 themes
- `performance = mean(theme performance)`
- `momentum_score = mean(theme momentum_score)`
- `breadth_1m = mean(theme breadth_1m)`
- `contributing_themes = nunique(theme_id)`

Category sort order:
- `performance desc`
- `momentum_score desc`
- `breadth_1m desc`
- `contributing_themes desc`
- `category asc`

Category drill sort order:
- `category asc`
- `performance desc`
- `momentum_score desc`
- `breadth_1m desc`
- `theme asc`

Semantics:
- identity is `theme_id` where available
- visible labels remain theme names
- `performance` is historical latest-row window performance, not current-view eligible/capped performance
- latest-day historical append/materialization operates on non-suppressed governed tickers only; suppressed tickers can still retain older stored raw/history rows, but new append scope is suppression-aware
- when the section-local daily-delta toggle is enabled in Themes mode, displayed `performance` compares against each table's own prior daily movement endpoint, not the selected window start, and the page adds inline parenthesis deltas only where the displayed metric directly represents `avg_1w` or `avg_1m`; breadth remains plain

### Selected Theme Detail Panel

Purpose:
- current governed-member detail plus theme-level historical snapshot context

View type:
- mixed current-view + historical-view

Current ticker detail source path:
- `theme_ticker_metrics(theme_id, include_suppressed=True)`
- `format_theme_ticker_table()`

Current ticker detail source semantics:
- current/live ticker detail fields use the preferred-source current snapshot path
- this is the right source for current ticker detail because the table is meant to show the latest governed-member snapshot state, not reconstructed daily history
- suppressed governed members are now hidden by default in the visible table, but remain available through the local `Include suppressed tickers` toggle
- when suppressed rows are included, the table can also show a visible `suppressed` indicator column
- visible `suppressed` means overall suppression, not only manual suppression:
  - `yes` when `manual_suppressed = true`
  - or when `symbol_refresh_status.status = 'refresh_suppressed'`
- the visible `current status` field is a detail-view operational state, not a new ranking formula:
  - `healthy current coverage` = governed, unsuppressed, and currently eligible
  - `suppressed` = manually/operationally suppressed
  - `needs refresh check` = governed and unsuppressed, but no usable current preferred-source snapshot coverage is stored
  - `current but ineligible` = preferred-source snapshot coverage exists, but the row still fails current eligibility
- the visible `eligible` column uses the same effective current-ranking eligibility rules as the main current ranking pipeline:
  - snapshot present
  - price >= minimum
  - avg_volume > 0
  - dollar_volume >= minimum
  - not `refresh_suppressed`

Selected-theme ticker detail scoring:
- ticker-level `composite` now follows the same baseline-strength philosophy as the standardized theme composite, but without theme participation logic:

```text
ticker_base_strength =
    0.30 * perf_1w
  + 0.70 * perf_1m

ticker_composite =
    ticker_base_strength
  * standardized_three_month_guardrail_factor(perf_3m)
  * standardized_recovery_factor(ticker_base_strength, perf_3m)
```

- ticker-level `momentum` now follows the same thrust-now philosophy as theme current momentum:

```text
ticker_momentum_raw =
    0.70 * perf_1w
  + 0.30 * perf_1m

ticker_momentum =
    ticker_momentum_raw
  * current_momentum_quality_factor(ticker_composite)
```

- these ticker scores are display-only detail enhancements on Themes; they do not change theme-level ranking formulas
- the bottom selected-theme chart now shows ticker-level composite history for the current top 5 visible governed tickers in the selected theme
- chart history uses `ticker_history_last_n_trading_days()` first so the time series can use stored daily history when available
- only if deeper daily history is unavailable for a ticker does the chart fall back to recent preferred-source `ticker_history_last_n_snapshots()` rows
- chart plotting keeps one weekday point per ticker-day and excludes weekends from the rendered series
- the Themes selected-theme detail panel and Ticker Lookup remain read-only stored-state surfaces; they do not trigger a live refresh attempt from the view itself
- Ticker Lookup uses the same user-facing suppression truth: visible `Suppressed = yes` when suppression is manual or operational

Current summary cards:
- `Governed tickers`
- `Current eligible`
- `Eligible/capped 1W`
- `Eligible/capped 1M`
- `Eligible/capped 3M`
- `Eligible breadth`
- `Current quality`

Current summary semantics:
- summary cards now intentionally match the current ranking pipeline
- `Eligible/capped 1W/1M/3M` come from `current_theme_metrics`
- `Current quality` is computed directly from the selected row; inactive themes show `n/a (inactive theme)`

Ticker table semantics:
- governed-membership-first view
- suppression hidden by default, with local opt-in include toggle
- latest preferred-source ticker row per ticker
- market cap can backfill from the latest non-null preferred-source row
- sorted by `ticker asc`
- `dollar_volume = price * avg_volume` is UI-only

Selected-theme history source path:
- `theme_snapshot_history(theme_id, include_recent_ticker_history=False)` on Themes page

Selected-theme history precedence:
- `captured > reconstructed`

Important mismatch:
- the Themes page selected-theme history is intentionally not the same surface as movement tables above it
- movement tables may use recent `ticker_history_derived` boundary rows
- selected-theme history on this page is described as preferred-source captured/reconstructed theme history

## Historical Performance Page

### Main movement workflow

Purpose:
- audit what changed across a resolved historical window, with explicit trust/provenance context

View type:
- boundary-window historical view

Source path:
- `load_theme_momentum_cached()` -> `compute_theme_momentum()` -> `theme_history_window()`

Trust strip fields:
- `Effective window start`
- `Effective window end`
- `Boundary snapshots`
- `History depth quality`

What the trust strip summarizes:
- resolved boundary dates actually used
- number of distinct boundary timestamps in the selected history
- provenance mix for the full window and the boundary rows
- whether the requested window collapsed to a shorter effective window

`History depth quality`:
- `Too shallow` if fewer than 2 boundary snapshots or no analyzed themes
- `Mixed` if `collapsed_to_available_history` is true, or provenance contains `mixed` or `reconstructed`
- otherwise `Good`

### theme_history_window()

This is the key movement-window read path.

Source path:
- `theme_history_window()` in `src/queries.py`
- internally uses `_recent_movement_theme_snapshot_union()`
- boundary resolution from `_resolve_recent_movement_boundaries()`

Movement workflow precedence:
- when recent `ticker_history_derived` rows are available: `ticker_history_derived > captured > reconstructed`
- otherwise fallback precedence remains `captured > ticker_history_derived > reconstructed`

What can feed the union:
- `captured`
- `ticker_history_derived`
- `reconstructed`

Boundary selection logic:
- end boundary = latest derived day if any derived rows exist in the movement union
- otherwise end boundary = latest available snapshot in the union
- start boundary = nearest snapshot `<= end_time - lookback_days`
- if none exists, start boundary falls back to earliest available snapshot

Deduping identity:
- same-date winner is selected per `theme_id + snapshot_date`

Returned window:
- all rows between resolved start and end timestamps inclusive
- sorted `snapshot_time asc, composite_score desc`

`collapsed_to_available_history`:
- true when `effective_window_days < requested_lookback_days`
- means the requested lookback was longer than the currently available boundary depth
- this is expected on shallow history and is not, by itself, a bug

### compute_theme_momentum()

Purpose:
- compute start/end comparison fields for each theme over the resolved movement window

Identity:
- grouped by `theme_id`
- narrow fallback to `theme` only if `theme_id` is absent in synthetic/test frames

Per-snapshot rank:
- dense rank within `snapshot_time`
- `composite_score desc`
- rank 1 is strongest

Derived fields:
- `rank_start`, `rank_end`
- `composite_score_start`, `composite_score_end`
- `delta_composite = end - start`
- `delta_avg_1w = avg_1w_end - avg_1w_start`
- `delta_avg_1m = avg_1m_end - avg_1m_start`
- `delta_avg_3m = avg_3m_end - avg_3m_start`
- `delta_breadth = positive_1m_breadth_pct_end - positive_1m_breadth_pct_start`
- `delta_ticker_count = ticker_count_end - ticker_count_start`
- `rank_change = rank_start - rank_end`

Momentum score:

```text
momentum_score =
    0.45 * delta_composite
  + 0.25 * delta_avg_1m
  + 0.20 * delta_breadth
  + 0.10 * rank_change
```

Returned summary ordering:
- `window_summary`: `momentum_score desc`, `delta_composite desc`
- `top_momentum`: `momentum_score desc`
- `biggest_risers`: `rank_change desc`, `delta_composite desc`
- `biggest_fallers`: `rank_change asc`, `delta_composite asc`
- `breadth_improvers`: `delta_breadth desc`
- `weakening_themes`: `delta_composite asc`, `delta_breadth asc`

Metadata returned in `meta`:
- `requested_lookback_days`
- `window_start`
- `window_end`
- `boundary_snapshot_count`
- `effective_window_days`
- `collapsed_to_available_history`
- `provenance_mix`
- `boundary_provenance_mix`

### Most Improving Themes In This Window

Purpose:
- rank themes by strongest positive movement, not strongest current level

Visible columns:
- `rank`
- `theme`
- `rank_change`
- `delta_composite`
- `momentum_score`
- optional `delta_avg_1m`
- optional `delta_breadth`

Sort order:
- `momentum_score desc`
- `delta_composite desc`
- `rank_change desc`

### Top Momentum Themes

Purpose:
- clearest direct view of the page's deterministic momentum model

Visible columns:
- `theme`
- `momentum_score`
- `delta_composite`
- `rank_change`
- `delta_breadth`

Sort order:
- `momentum_score desc`

### Movement Chart

Purpose:
- visualize historical movement for selected themes in the resolved window

Displayed metric options:
- `composite_score`
- `avg_1w`
- `avg_1m`
- `avg_3m`
- `positive_1m_breadth_pct`
- `ticker_count`

Display modes:
- `raw metric`
- `indexed (100=start)`
- `rank movement`

Chart transformations:
- filtering by category and search is page-side only
- displayed theme identity is `theme_id`
- labels remain names
- indexed mode rebases to `metric / start_value * 100`, using 100 if start is missing or zero
- smoothing uses 3-period or 5-period rolling mean per `theme_id`
- `leader_tier = current leader` if `theme_id` is in `summary.sort_values("rank_end").head(3)`

Important note:
- the chart does not change ranking or stored calculations
- it is a presentation layer over the resolved movement history

### Theme Signals (Deterministic Inflection Feed)

Purpose:
- one highest-priority deterministic signal per theme for the selected window

Source path:
- `compute_theme_inflections()`
- internally reuses `compute_theme_momentum()` and `compute_theme_rotation()`

Identity:
- trend merge, rotation membership checks, and one-signal dedupe use `theme_id`

Recent trend flags from `_recent_trend_flags()`:
- looks at the last 4 rows per `theme_id`
- `accel_trend_up = last composite delta > 0 AND prior composite delta >= 0`
- `avg1m_trend_up = latest avg_1m delta > 0`

Signal thresholds:
- `MIN_SIGNAL_MOMENTUM = 1.5`
- `MIN_SIGNAL_COMPOSITE = 0.5`
- `MIN_SIGNAL_AVG1M = 0.25`
- `rank_thr = max(5, int(top_n * 0.25))`

Signal families:
- `rotating_out`
- `leadership_deterioration`
- `rotating_into`
- `emerging`
- `accelerating`
- `weakening`

Internal priority:
- `rotating_out = 5`
- `leadership_deterioration = 4`
- `rotating_into = 3`
- `emerging = 2`
- `accelerating = 1`
- `weakening = 1`

One-signal-per-theme noise control:
- sort by `theme_id`, `priority desc`, `momentum_score desc`, `rank_change desc`
- keep first row per `theme_id`
- final output sort: `priority desc`, `momentum_score desc`, `rank_change desc`, `theme asc`

### Rotation Signals

Purpose:
- classify leadership turnover and secondary rotation buckets from the same movement summary

Source path:
- `compute_theme_rotation(summary, top_n, new_leaders, dropped_leaders)`

Rotation buckets:
- `rotating_into`: `rank_end <= top_n AND rank_start > top_n`
- `rotating_out`: `rank_start <= top_n AND rank_end > top_n`
- `emerging`: `rank_change >= max(5, int(top_n * 0.25))` and `delta_composite > 0` and `delta_avg_1m > 0` and `delta_breadth > 0`
- `fading`: `rank_change <= -max(5, int(top_n * 0.25))` and `delta_composite < 0` and `delta_avg_1m < 0` and `delta_breadth < 0`
- `acceleration`: current leaders with `delta_composite > 0`, `rank_change > 0`, and `momentum_score >= 60th percentile of current leaders`
- `deterioration`: current leaders with `delta_composite < 0` and `rank_change < 0`

Sort order by bucket:
- `rotating_into`: `rank_change desc`, `momentum_score desc`
- `rotating_out`: `rank_change asc`, `momentum_score asc`
- `emerging`: `momentum_score desc`, `rank_change desc`
- `fading`: `momentum_score asc`, `rank_change asc`
- `acceleration`: `momentum_score desc`
- `deterioration`: `momentum_score asc`, `rank_change asc`

Rotation intensity:

```text
rotation_intensity_score =
    ((entered_top_n + exited_top_n) / max(1, top_n)) * 100
```

`new_leaders` and `dropped_leaders`:
- computed by `top_n_membership_changes()`
- membership diff is by `theme_id`
- returned labels are still theme names for compatibility

### Window-End Leaders

Purpose:
- historical reference tables for strongest themes at the end of a resolved 1W/1M/3M window

Source path:
- `build_window_leaderboard()`

Visible columns:
- `rank`
- `theme`
- `window_perf`
- `momentum_score`
- `rank_change`

Sort order:
- latest selected `perf_col desc`
- `momentum_score desc`
- `rank_change desc`
- `theme asc`

Important note:
- these are window-end performance leaders
- they are not the same thing as top momentum movers

### Single Theme Historical Snapshot Detail

Purpose:
- theme-level historical snapshot series for one selected theme

View type:
- historical detail view

Source path:
- `theme_snapshot_history(theme_id, include_recent_ticker_history=True)`

Selected-theme detail precedence:
- `captured > ticker_history_derived > reconstructed`

Visible table columns include:
- `run_id`
- `snapshot_time`
- `ticker_count`
- `avg_1w`
- `avg_1m`
- `avg_3m`
- `positive_1w_breadth_pct`
- `positive_1m_breadth_pct`
- `positive_3m_breadth_pct`
- `composite_score`
- `snapshot_source`
- `provenance_class`
- `provenance_source_label`

Sort order:
- table: `snapshot_time desc`, `run_id desc`
- chart: ascending by `snapshot_time`

Important note:
- this is not a current/live constituent table
- it is a theme-level historical snapshot surface

### Debug: Historical Source Lineage

Purpose:
- show which same-date source row actually won the movement boundary selection for the selected theme/window

Source path:
- `historical_theme_boundary_debug()`

Debug precedence:
- `ticker_history_derived > captured > reconstructed`

What it shows:
- resolved movement boundary start and end
- same-date candidate rows
- winner rows
- provenance classes that drove the movement boundary

Important difference:
- this debug block mirrors the movement workflow
- it does not mirror selected-theme detail precedence when those differ on recent dates

## Provenance And Precedence Summary

### Movement workflow precedence

Used by:
- Themes page movement tables
- Historical Performance movement workflow
- Historical Performance debug lineage

Rule:
- prefer `ticker_history_derived` on recent movement boundaries when available
- otherwise fall back to `captured`
- `reconstructed` is lowest precedence

Shorthand:
- recent movement mode: `ticker_history_derived > captured > reconstructed`
- fallback mode: `captured > ticker_history_derived > reconstructed`

### Selected-theme detail precedence

Themes page selected-theme history:
- `captured > reconstructed`

Historical Performance selected-theme detail:
- `captured > ticker_history_derived > reconstructed`

Why selected-theme detail can differ from movement/debug on the same recent date:
- movement/debug is boundary-window-first and may intentionally prefer recent `ticker_history_derived` rows
- selected-theme detail is a historical detail surface with its own same-date precedence
- both can be correct for their own purpose

### When `ticker_history_derived` can win

`ticker_history_derived` can win when:
- recent ticker-history-derived rows exist in the movement union
- the movement workflow is resolving recent boundaries
- the page is using the movement/debug lineage path rather than the detail-history precedence path

### When captured can still win

Captured rows still win when:
- no recent derived row is available for that theme/date in the movement boundary path
- the surface uses detail-history precedence instead of movement precedence
- the same-date comparison is on a surface where captured remains first priority

## Expected Disagreements That Are Not Bugs

- `Current Market Leadership` vs `Current Top Themes - Current 1W/1M`
  - leadership ranks by `composite_score` and requires composite-eligible contributors
  - top-by-window ranks by one window return and only requires that window's contributor threshold

- current Themes tables vs Themes `Theme Movement Snapshots`
  - current tables use latest preferred-source ticker snapshots, eligibility gates, and capped returns
  - movement tables use resolved historical boundary rows and movement-window precedence

- Historical Performance `Most Improving Themes` vs `Window-End Leaders`
  - improving themes are ranked by `momentum_score`
  - window-end leaders are ranked by latest historical window performance level

- Historical Performance `Rotation Signals` vs `Theme Signals`
  - rotation buckets are classification rules over the summary
  - inflection feed adds trend flags, thresholds, and one-signal-per-theme dedupe

- selected-theme detail history vs movement/debug lineage
  - same recent date can legitimately show different winning provenance because precedence differs by surface

- chart view vs tables on Historical Performance
  - category/search filtering, indexed mode, rank mode, and rolling smoothing change presentation only
  - they do not change the underlying movement summary

- Themes category tables vs top theme lists
  - category tables average across all underlying grouped themes in the selected latest window
  - they are not a rollup of only the previewed top rows

- selected theme ticker table vs current leaderboard values on Themes
  - ticker detail is a governed-member current view
  - leaderboard metrics are eligible/capped aggregates over that governed-member set

## Trust Checklist

When a result looks suspicious, check these in order:

1. Page/view type
- current-view, boundary-window movement view, historical detail view, or diagnostic/debug view

2. Provenance path
- current ranking pipeline
- movement workflow precedence
- selected-theme detail precedence

3. Resolved boundaries
- `window_start`
- `window_end`
- `effective_window_days`
- `collapsed_to_available_history`

4. Eligibility basis
- `eligible_*_count`
- `eligible_composite_count`
- `eligible_breadth_pct`

5. Metric basis
- capped eligible metric vs raw historical snapshot metric
- current-strength ranking vs start/end delta ranking

6. Identity basis
- `theme_id` is the stable identity
- theme name is only a display label and may be ambiguous

7. Surface mismatch sanity check
- if two sections disagree, first compare their read path and precedence before suspecting a calculation bug

## Non-Obvious But Current Behaviors

- `composite_score` confidence weighting uses `ticker_count`, not `eligible_composite_count`
- `delta_avg_1m` on current ranking pages means current vs prior preferred-source snapshot, but on Historical Performance it means window end vs window start
- `new_leaders` and `dropped_leaders` are computed by `theme_id`, but returned labels remain names for compatibility
- category `top_themes` preview strings are display-only and can still look ambiguous if two different themes share the same name
