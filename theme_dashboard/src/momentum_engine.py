from __future__ import annotations

import pandas as pd

from .config import THEME_CONFIDENCE_FULL_COUNT
from .queries import canonical_theme_history_window, theme_history_window

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
        "meta": {
            "requested_lookback_days": None,
            "window_start": None,
            "window_end": None,
            "boundary_snapshot_count": 0,
            "effective_window_days": None,
            "collapsed_to_available_history": False,
        },
    }


def _historical_theme_confidence_factor(ticker_count: int | float) -> float:
    if pd.isna(ticker_count) or float(ticker_count) <= 0:
        return 0.0
    return min(1.0, (float(ticker_count) / float(THEME_CONFIDENCE_FULL_COUNT)) ** 0.5)


def _top_n_membership_changes_from_history(history: pd.DataFrame, top_n: int = 20) -> tuple[list[str], list[str]]:
    if history.empty:
        return [], []
    boundary_times = pd.to_datetime(history["snapshot_time"]).dropna().drop_duplicates().sort_values()
    if len(boundary_times) < 2:
        return [], []
    start_time = boundary_times.iloc[0]
    end_time = boundary_times.iloc[-1]
    sort_cols = ["rank", "theme", "theme_id"] if "rank" in history.columns else ["composite_score", "theme", "theme_id"]
    ascending = [True, True, True] if "rank" in history.columns else [False, True, True]
    start_top = (
        history[pd.to_datetime(history["snapshot_time"]) == start_time]
        .sort_values(sort_cols, ascending=ascending)
        .head(top_n)
    )
    end_top = (
        history[pd.to_datetime(history["snapshot_time"]) == end_time]
        .sort_values(sort_cols, ascending=ascending)
        .head(top_n)
    )
    start_map = {
        str(row["theme_id"]): str(row["theme"])
        for _, row in start_top[["theme_id", "theme"]].drop_duplicates(subset=["theme_id"]).iterrows()
    } if not start_top.empty else {}
    end_map = {
        str(row["theme_id"]): str(row["theme"])
        for _, row in end_top[["theme_id", "theme"]].drop_duplicates(subset=["theme_id"]).iterrows()
    } if not end_top.empty else {}
    start_set = set(start_map.keys())
    end_set = set(end_map.keys())
    entered = [(end_map[theme_id], theme_id) for theme_id in sorted(end_set - start_set, key=lambda value: (end_map[value], value))]
    dropped = [(start_map[theme_id], theme_id) for theme_id in sorted(start_set - end_set, key=lambda value: (start_map[value], value))]
    return [label for label, _ in entered], [label for label, _ in dropped]


def compute_theme_momentum(conn, lookback_days: int, top_n: int = 20) -> dict:
    history = canonical_theme_history_window(conn, lookback_days)
    if history.empty:
        history = theme_history_window(conn, lookback_days)
    if history.empty:
        return _empty_result()

    history = history.copy()
    if "theme_id" not in history.columns:
        history["theme_id"] = history["theme"].astype(str)
    if "category" not in history.columns:
        history["category"] = None

    source_preference = None
    if "snapshot_source" in history.columns and not history["snapshot_source"].dropna().empty:
        sources = sorted(set(history["snapshot_source"].dropna().astype(str).tolist()))
        source_preference = sources[0] if len(sources) == 1 else ", ".join(sources)

    boundary_times = pd.to_datetime(history["snapshot_time"]).dropna().drop_duplicates().sort_values()
    window_start = boundary_times.iloc[0] if not boundary_times.empty else None
    window_end = boundary_times.iloc[-1] if not boundary_times.empty else None
    effective_window_days = int((window_end - window_start).days) if window_start is not None and window_end is not None else None
    collapsed_to_available_history = bool(effective_window_days is not None and effective_window_days < int(lookback_days))
    provenance_classes = sorted(set(history["provenance_class"].dropna().astype(str).tolist())) if "provenance_class" in history.columns else []
    provenance_mix = (
        "mixed"
        if len(provenance_classes) > 1
        else (f"{provenance_classes[0]}-only" if provenance_classes else "unknown")
    )
    boundary_rows = history[pd.to_datetime(history["snapshot_time"]).isin([window_start, window_end])].copy()
    boundary_classes = sorted(set(boundary_rows["provenance_class"].dropna().astype(str).tolist())) if "provenance_class" in boundary_rows.columns else []
    boundary_provenance_mix = (
        "mixed"
        if len(boundary_classes) > 1
        else (f"{boundary_classes[0]}-only" if boundary_classes else "unknown")
    )

    history = history.sort_values(["theme_id", "snapshot_time", "theme"]).copy()
    if "rank" not in history.columns or history["rank"].isna().all():
        history["rank"] = history.groupby("snapshot_time")["composite_score"].rank(method="dense", ascending=False)

    first = history.groupby("theme_id", as_index=False).first()
    last = history.groupby("theme_id", as_index=False).last()

    merged = first[
        ["theme_id", "theme", "category", "composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count", "rank"]
    ].merge(
        last[
            ["theme_id", "theme", "category", "composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count", "rank"]
        ],
        on="theme_id",
        suffixes=("_start", "_end"),
    )
    merged["theme"] = merged["theme_end"].where(merged["theme_end"].notna(), merged["theme_start"])
    merged["category"] = merged["category_end"].where(merged["category_end"].notna(), merged["category_start"])

    merged["delta_composite"] = merged["composite_score_end"] - merged["composite_score_start"]
    merged["delta_avg_1w"] = merged["avg_1w_end"] - merged["avg_1w_start"]
    merged["delta_avg_1m"] = merged["avg_1m_end"] - merged["avg_1m_start"]
    merged["delta_avg_3m"] = merged["avg_3m_end"] - merged["avg_3m_start"]
    merged["delta_breadth"] = merged["positive_1m_breadth_pct_end"] - merged["positive_1m_breadth_pct_start"]
    merged["delta_ticker_count"] = merged["ticker_count_end"] - merged["ticker_count_start"]
    merged["rank_change"] = merged["rank_start"] - merged["rank_end"]
    merged["breadth_confidence_factor"] = [
        min(
            _historical_theme_confidence_factor(start_count),
            _historical_theme_confidence_factor(end_count),
        )
        for start_count, end_count in zip(merged["ticker_count_start"], merged["ticker_count_end"])
    ]
    merged["effective_delta_breadth"] = merged["delta_breadth"] * merged["breadth_confidence_factor"]

    # Deterministic, auditable momentum score
    merged["momentum_score"] = (
        0.45 * merged["delta_composite"]
        + 0.25 * merged["delta_avg_1m"]
        + 0.20 * merged["effective_delta_breadth"]
        + 0.10 * merged["rank_change"]
    )

    merged = merged.round(2)

    entered, dropped = _top_n_membership_changes_from_history(history, top_n=top_n)

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
        "meta": {
            "requested_lookback_days": int(lookback_days),
            "window_start": window_start,
            "window_end": window_end,
            "boundary_snapshot_count": int(boundary_times.nunique()),
            "effective_window_days": effective_window_days,
            "collapsed_to_available_history": collapsed_to_available_history,
            "provenance_mix": provenance_mix,
            "boundary_provenance_mix": boundary_provenance_mix,
        },
    }
