# Theme Model Audit

This document captures the current production theme-ranking model and the main calibration concerns that motivated the comparison framework on the experimental branch.

## Current Production Logic

Theme-level return metrics are built from simple arithmetic means of member ticker returns:

- `avg_1w = mean(perf_1w)`
- `avg_1m = mean(perf_1m)`
- `avg_3m = mean(perf_3m)`

Breadth is measured separately as the percent of positive member returns:

- `positive_1w_breadth_pct`
- `positive_1m_breadth_pct`
- `positive_3m_breadth_pct`

Composite score is currently:

- `composite_score = 0.25*avg_1w + 0.50*avg_1m + 0.25*avg_3m`

Momentum score is currently:

- `momentum_score = 0.45*delta_composite + 0.25*delta_avg_1m + 0.20*delta_breadth + 0.10*rank_change`

## What The Current Model Does Well

- It is deterministic and easy to audit.
- It reacts quickly to leadership changes.
- It keeps the production logic understandable without factor-model complexity.

## Main Calibration Risks

### Small-theme bias

Because theme performance is a simple mean of member returns, small themes can move sharply on one or two names and outrank broader themes too easily.

### Outlier / concentration bias

One or two extreme tickers can distort a theme average. Breadth is measured but does not currently protect the core composite score from concentrated leadership.

### Breadth underweighting

Breadth matters operationally, but in production it is mostly contextual rather than a ranking driver.

### Momentum redundancy

`momentum_score` reuses return-based deltas and rank effects that already echo recent return strength. It is useful, but not fully orthogonal.

## High-Value Candidate Refinements

These were chosen because they are explainable and narrow:

1. Small-theme confidence adjustment
2. Winsorized or trimmed mean for theme return averages
3. Stronger breadth role in ranking
4. One combined quality-adjusted variant for comparison only

## Comparison Framework

Use `run_theme_model_comparison.py` to generate:

- `docs/THEME_MODEL_COMPARISON.md`
- `docs/theme_model_comparison.csv`

These artifacts compare the current model against a small set of explainable variants without changing production behavior.
