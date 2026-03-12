from __future__ import annotations

import pandas as pd


def _validate_window_leaderboard_inputs(momentum: dict) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    history = momentum.get("history", pd.DataFrame())
    if history.empty:
        return pd.DataFrame(), pd.DataFrame(), "No snapshots available for this window yet."

    snapshot_count = int(history["snapshot_time"].nunique())
    if snapshot_count < 2:
        source_hint = momentum.get("source_preference") or "current"
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            f"Need at least two boundary snapshots for this window (currently {snapshot_count} available under {source_hint}-preferred selection). "
            "The comparison needs one latest snapshot and one earlier boundary snapshot near the start of the selected window, so two same-day imports may still be insufficient for 1W/1M.",
        )

    summary = momentum.get("window_summary", pd.DataFrame())
    if summary.empty:
        return pd.DataFrame(), pd.DataFrame(), "No momentum summary available for this window."

    return history, summary, None


def build_window_leaderboard(momentum: dict, perf_col: str, top_k: int = 10) -> tuple[pd.DataFrame, str | None]:
    """Build a window-specific leaderboard from momentum output.

    Sorting is deterministic and window-specific:
    1) selected window performance column (avg_1w/avg_1m/avg_3m),
    2) momentum_score,
    3) rank_change.
    """
    history, summary, msg = _validate_window_leaderboard_inputs(momentum)
    if msg:
        return pd.DataFrame(), msg

    latest = history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)

    ranked = (
        latest[["theme", perf_col]]
        .merge(summary[["theme", "momentum_score", "rank_change"]], on="theme", how="left")
        .sort_values([perf_col, "momentum_score", "rank_change"], ascending=False)
        .head(top_k)
        .reset_index(drop=True)
    )
    ranked["rank"] = ranked.index + 1
    return ranked[["rank", "theme", perf_col, "momentum_score", "rank_change"]], None


def build_category_leaderboard(momentum: dict, perf_col: str, top_k: int = 10) -> tuple[pd.DataFrame, str | None]:
    """Aggregate the full eligible theme window into a category leaderboard."""
    history, summary, msg = _validate_window_leaderboard_inputs(momentum)
    if msg:
        return pd.DataFrame(), msg

    latest = history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
    grouped = latest[["theme", "category", perf_col, "positive_1m_breadth_pct"]].merge(
        summary[["theme", "momentum_score", "rank_change"]],
        on="theme",
        how="left",
    )
    grouped = grouped.rename(columns={perf_col: "performance", "positive_1m_breadth_pct": "breadth_1m"})
    grouped["category_group"] = grouped["category"].fillna("").astype(str).str.strip()
    grouped.loc[grouped["category_group"] == "", "category_group"] = grouped["theme"]
    grouped = grouped.sort_values(
        ["category_group", "performance", "momentum_score", "breadth_1m", "theme"],
        ascending=[True, False, False, False, True],
    ).reset_index(drop=True)

    preview_map = (
        grouped.groupby("category_group")["theme"]
        .apply(lambda s: _format_top_theme_preview(s.tolist()))
        .to_dict()
    )

    aggregated = (
        grouped.groupby("category_group", dropna=False)
        .agg(
            performance=("performance", "mean"),
            momentum_score=("momentum_score", "mean"),
            breadth_1m=("breadth_1m", "mean"),
            contributing_themes=("theme", "nunique"),
        )
        .reset_index()
        .rename(columns={"category_group": "category"})
        .sort_values(["performance", "momentum_score", "breadth_1m", "contributing_themes", "category"], ascending=[False, False, False, False, True])
        .head(top_k)
        .reset_index(drop=True)
    )
    for metric_col in ("performance", "momentum_score", "breadth_1m"):
        aggregated[metric_col] = aggregated[metric_col].round(2)
    aggregated["top_themes"] = aggregated["category"].map(preview_map).fillna("")
    aggregated["rank"] = aggregated.index + 1
    return aggregated[["rank", "category", "top_themes", "contributing_themes", "performance", "momentum_score", "breadth_1m"]], None


def _format_top_theme_preview(themes: list[str], preview_limit: int = 3) -> str:
    unique_themes: list[str] = []
    for theme in themes:
        label = str(theme or "").strip()
        if label and label not in unique_themes:
            unique_themes.append(label)

    if not unique_themes:
        return ""

    shown = unique_themes[:preview_limit]
    return ", ".join(shown)
