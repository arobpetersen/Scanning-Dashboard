from __future__ import annotations

import pandas as pd

from .config import CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS


def _leadership_quality_label(row: pd.Series) -> str:
    breadth = row.get("eligible_breadth_pct", row.get("positive_1m_breadth_pct"))
    breadth_value = float(breadth) if breadth is not None and not pd.isna(breadth) else None
    ticker_count = int(row.get("eligible_contributor_count", row.get("eligible_composite_count", row.get("ticker_count") or 0)) or 0)

    if breadth_value is not None and breadth_value >= 60 and ticker_count >= 8:
        return "Broad leader"
    if ticker_count <= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS + 1 or (breadth_value is not None and breadth_value < 45):
        return "Thin / filtered"
    return "Narrow leader"


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


def build_current_leadership_table(rankings: pd.DataFrame, top_k: int = 12) -> pd.DataFrame:
    if rankings.empty:
        return pd.DataFrame()

    leadership = rankings.copy()
    if "is_active" in leadership.columns:
        leadership = leadership[leadership["is_active"] == True].copy()
    if leadership.empty:
        return pd.DataFrame()

    leadership = leadership.sort_values(
        ["composite_score", "positive_1m_breadth_pct", "eligible_composite_count", "ticker_count", "theme"],
        ascending=[False, False, False, False, True],
    ).head(top_k).reset_index(drop=True)
    leadership["rank"] = leadership.index + 1
    leadership["eligible_contributor_count"] = leadership["eligible_composite_count"]
    leadership["leadership_quality"] = leadership.apply(_leadership_quality_label, axis=1)
    leadership = leadership.rename(columns={"positive_1m_breadth_pct": "breadth_1m"})
    return leadership[
        [
            "rank",
            "theme_id",
            "theme",
            "category",
            "composite_score",
            "avg_1w",
            "avg_1m",
            "avg_3m",
            "breadth_1m",
            "ticker_count",
            "eligible_contributor_count",
            "eligible_breadth_pct",
            "leadership_quality",
        ]
    ]


def build_current_performance_table(rankings: pd.DataFrame, perf_col: str, top_k: int = 10) -> pd.DataFrame:
    if rankings.empty:
        return pd.DataFrame()

    eligible_count_col = {
        "avg_1w": "eligible_1w_count",
        "avg_1m": "eligible_1m_count",
        "avg_3m": "eligible_3m_count",
    }.get(perf_col)
    if not eligible_count_col:
        raise ValueError(f"Unsupported current performance column: {perf_col}")

    current = rankings.copy()
    if "is_active" in current.columns:
        current = current[current["is_active"] == True].copy()
    current = current[current[eligible_count_col] >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS].copy()
    if current.empty:
        return pd.DataFrame()

    current["eligible_contributor_count"] = current[eligible_count_col]
    current = current.sort_values(
        [perf_col, "composite_score", "eligible_contributor_count", "eligible_breadth_pct", "theme"],
        ascending=[False, False, False, False, True],
    ).head(top_k).reset_index(drop=True)
    current["rank"] = current.index + 1
    current["leadership_quality"] = current.apply(_leadership_quality_label, axis=1)
    current = current.rename(columns={perf_col: "performance", "positive_1m_breadth_pct": "breadth_1m"})
    return current[
        [
            "rank",
            "theme_id",
            "theme",
            "category",
            "performance",
            "composite_score",
            "breadth_1m",
            "ticker_count",
            "eligible_contributor_count",
            "eligible_breadth_pct",
            "leadership_quality",
        ]
    ]


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


def build_category_theme_breakdown(momentum: dict, perf_col: str) -> tuple[pd.DataFrame, str | None]:
    """Return full eligible underlying themes for category drill views."""
    history, summary, msg = _validate_window_leaderboard_inputs(momentum)
    if msg:
        return pd.DataFrame(), msg

    latest = history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
    breakdown = latest[["theme", "category", perf_col, "positive_1m_breadth_pct"]].merge(
        summary[["theme", "momentum_score", "rank_change"]],
        on="theme",
        how="left",
    )
    breakdown = breakdown.rename(columns={perf_col: "performance", "positive_1m_breadth_pct": "breadth_1m"})
    breakdown["category"] = breakdown["category"].fillna("").astype(str).str.strip()
    breakdown.loc[breakdown["category"] == "", "category"] = breakdown["theme"]
    breakdown = breakdown.sort_values(
        ["category", "performance", "momentum_score", "breadth_1m", "theme"],
        ascending=[True, False, False, False, True],
    ).reset_index(drop=True)
    for metric_col in ("performance", "momentum_score", "breadth_1m"):
        breakdown[metric_col] = breakdown[metric_col].round(2)
    return breakdown[["category", "theme", "performance", "momentum_score", "breadth_1m", "rank_change"]], None


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
