from __future__ import annotations

import pandas as pd

from .queries import theme_history_window, top_n_membership_changes

METRIC_COLS = [
    "composite_score",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "positive_1m_breadth_pct",
    "ticker_count",
]


def _empty_result() -> dict:
    empty = pd.DataFrame()
    return {
        "history": empty,
        "window_summary": empty,
        "top_momentum": empty,
        "biggest_risers": empty,
        "biggest_fallers": empty,
        "breadth_improvers": empty,
        "weakening_themes": empty,
        "new_leaders": [],
        "dropped_leaders": [],
        "source_preference": None,
    }


def compute_theme_momentum(conn, lookback_days: int, top_n: int = 20) -> dict:
    history = theme_history_window(conn, lookback_days)
    if history.empty:
        return _empty_result()

    source_preference = None
    if "snapshot_source" in history.columns and not history["snapshot_source"].dropna().empty:
        sources = sorted(set(history["snapshot_source"].dropna().astype(str).tolist()))
        source_preference = sources[0] if len(sources) == 1 else ", ".join(sources)

    history = history.sort_values(["theme", "snapshot_time"]).copy()
    history["rank"] = history.groupby("snapshot_time")["composite_score"].rank(method="dense", ascending=False)

    first = history.groupby("theme", as_index=False).first()
    last = history.groupby("theme", as_index=False).last()

    merged = first[["theme", "composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count", "rank"]].merge(
        last[["theme", "composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count", "rank"]],
        on="theme",
        suffixes=("_start", "_end"),
    )

    merged["delta_composite"] = merged["composite_score_end"] - merged["composite_score_start"]
    merged["delta_avg_1w"] = merged["avg_1w_end"] - merged["avg_1w_start"]
    merged["delta_avg_1m"] = merged["avg_1m_end"] - merged["avg_1m_start"]
    merged["delta_avg_3m"] = merged["avg_3m_end"] - merged["avg_3m_start"]
    merged["delta_breadth"] = merged["positive_1m_breadth_pct_end"] - merged["positive_1m_breadth_pct_start"]
    merged["delta_ticker_count"] = merged["ticker_count_end"] - merged["ticker_count_start"]
    merged["rank_change"] = merged["rank_start"] - merged["rank_end"]

    # Deterministic, auditable momentum score
    merged["momentum_score"] = (
        0.45 * merged["delta_composite"]
        + 0.25 * merged["delta_avg_1m"]
        + 0.20 * merged["delta_breadth"]
        + 0.10 * merged["rank_change"]
    )

    merged = merged.round(2)

    entered, dropped = top_n_membership_changes(conn, lookback_days, top_n=top_n)

    return {
        "history": history,
        "window_summary": merged.sort_values(["momentum_score", "delta_composite"], ascending=False),
        "top_momentum": merged.sort_values("momentum_score", ascending=False).head(top_n),
        "biggest_risers": merged.sort_values(["rank_change", "delta_composite"], ascending=False).head(top_n),
        "biggest_fallers": merged.sort_values(["rank_change", "delta_composite"], ascending=[True, True]).head(top_n),
        "breadth_improvers": merged.sort_values("delta_breadth", ascending=False).head(top_n),
        "weakening_themes": merged.sort_values(["delta_composite", "delta_breadth"], ascending=[True, True]).head(top_n),
        "new_leaders": entered,
        "dropped_leaders": dropped,
        "source_preference": source_preference,
    }
