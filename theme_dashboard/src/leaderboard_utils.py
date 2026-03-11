from __future__ import annotations

import pandas as pd


def build_window_leaderboard(momentum: dict, perf_col: str, top_k: int = 10) -> tuple[pd.DataFrame, str | None]:
    """Build a window-specific leaderboard from momentum output.

    Sorting is deterministic and window-specific:
    1) selected window performance column (avg_1w/avg_1m/avg_3m),
    2) momentum_score,
    3) rank_change.
    """
    history = momentum.get("history", pd.DataFrame())
    if history.empty:
        return pd.DataFrame(), "No snapshots available for this window yet."

    snapshot_count = int(history["snapshot_time"].nunique())
    if snapshot_count < 2:
        source_hint = momentum.get("source_preference") or "current"
        return (
            pd.DataFrame(),
            f"Need at least two boundary snapshots for this window (currently {snapshot_count} available under {source_hint}-preferred selection). "
            "The comparison needs one latest snapshot and one earlier boundary snapshot near the start of the selected window, so two same-day imports may still be insufficient for 1W/1M.",
        )

    latest = history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)

    summary = momentum.get("window_summary", pd.DataFrame())
    if summary.empty:
        return pd.DataFrame(), "No momentum summary available for this window."

    ranked = (
        latest[["theme", perf_col]]
        .merge(summary[["theme", "momentum_score", "rank_change"]], on="theme", how="left")
        .sort_values([perf_col, "momentum_score", "rank_change"], ascending=False)
        .head(top_k)
        .reset_index(drop=True)
    )
    ranked["rank"] = ranked.index + 1
    return ranked[["rank", "theme", perf_col, "momentum_score", "rank_change"]], None
