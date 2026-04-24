from __future__ import annotations

import pandas as pd
import numpy as np

from .config import CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS

LEADERSHIP_QUALITY_LEADER_TOP_PCT = 0.08
LEADERSHIP_QUALITY_THIN_MAX_ELIGIBLE = 2
LEADERSHIP_QUALITY_THIN_BORDERLINE_MAX_ELIGIBLE = 3
LEADERSHIP_QUALITY_THIN_BORDERLINE_MIN_PARTICIPATION = 0.40
LEADERSHIP_QUALITY_BROAD_MIN_ELIGIBLE = 5
LEADERSHIP_QUALITY_BROAD_MIN_PARTICIPATION = 0.65
LEADERSHIP_QUALITY_BROAD_MIN_BREADTH = 70.0
LEADERSHIP_QUALITY_NARROW_STRENGTH_MAX_ELIGIBLE = 3
LEADERSHIP_QUALITY_NARROW_STRENGTH_MIN_PARTICIPATION = 0.40
LEADERSHIP_QUALITY_NARROW_STRENGTH_MIN_BREADTH = 60.0

def disambiguate_theme_labels(
    df: pd.DataFrame,
    *,
    theme_col: str = "theme",
    theme_id_col: str = "theme_id",
    category_col: str = "category",
    output_col: str = "theme_display",
) -> pd.DataFrame:
    if df.empty or theme_col not in df.columns:
        out = df.copy()
        if output_col not in out.columns and theme_col in out.columns:
            out[output_col] = out[theme_col]
        return out

    out = df.copy()
    out[output_col] = out[theme_col].fillna("").astype(str).str.strip()
    name_counts = out[output_col].value_counts()
    duplicate_names = set(name_counts[name_counts > 1].index.tolist())
    if not duplicate_names:
        return out

    if category_col in out.columns:
        category_text = out[category_col].fillna("").astype(str).str.strip()
        duplicate_mask = out[output_col].isin(duplicate_names) & category_text.ne("")
        out.loc[duplicate_mask, output_col] = out.loc[duplicate_mask, theme_col].astype(str).str.strip() + " (" + category_text[duplicate_mask] + ")"

    rendered_counts = out[output_col].value_counts()
    duplicate_rendered = set(rendered_counts[rendered_counts > 1].index.tolist())
    if duplicate_rendered and theme_id_col in out.columns:
        duplicate_mask = out[output_col].isin(duplicate_rendered) & out[theme_id_col].notna()
        out.loc[duplicate_mask, output_col] = (
            out.loc[duplicate_mask, output_col].astype(str).str.strip()
            + " [#"
            + out.loc[duplicate_mask, theme_id_col].astype(str).str.strip()
            + "]"
        )

    return out


def leadership_cohort_cutoff_rank(active_theme_count: int | float) -> int:
    if pd.isna(active_theme_count) or float(active_theme_count) <= 0:
        return 0
    return int(max(1, np.ceil(float(active_theme_count) * LEADERSHIP_QUALITY_LEADER_TOP_PCT)))


def annotate_current_leadership_quality(
    df: pd.DataFrame,
    *,
    rank_col: str = "rank",
    active_col: str = "is_active",
) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        if "leader_cohort_eligible" not in out.columns:
            out["leader_cohort_eligible"] = pd.Series(dtype="boolean")
        if "leadership_quality" not in out.columns:
            out["leadership_quality"] = pd.Series(dtype="object")
        return out

    out = df.copy()
    if active_col in out.columns:
        active_mask = out[active_col].astype("boolean").fillna(True).astype(bool)
    else:
        active_mask = pd.Series(True, index=out.index, dtype=bool)
    active_theme_count = int(active_mask.sum())
    cutoff_rank = leadership_cohort_cutoff_rank(active_theme_count)
    if rank_col in out.columns:
        ranks = pd.to_numeric(out[rank_col], errors="coerce")
        out["leader_cohort_eligible"] = active_mask & ranks.notna() & (ranks <= cutoff_rank)
    else:
        out["leader_cohort_eligible"] = False
    out["leader_cohort_cutoff_rank"] = cutoff_rank
    out["active_theme_count"] = active_theme_count
    out["leadership_quality"] = out.apply(current_leadership_quality_label, axis=1)
    return out


def current_leadership_quality_label(row: pd.Series) -> str:
    breadth = row.get("eligible_breadth_pct", row.get("positive_1m_breadth_pct"))
    breadth_value = float(breadth) if breadth is not None and not pd.isna(breadth) else None
    eligible_contributor_count = int(
        row.get("eligible_contributor_count", row.get("eligible_composite_count", row.get("ticker_count") or 0)) or 0
    )
    governed_ticker_count = int(row.get("ticker_count") or 0)
    participation_ratio = (
        float(eligible_contributor_count) / float(max(governed_ticker_count, 1))
        if eligible_contributor_count > 0
        else 0.0
    )
    leader_cohort_eligible = bool(row.get("leader_cohort_eligible"))

    if eligible_contributor_count <= LEADERSHIP_QUALITY_THIN_MAX_ELIGIBLE or (
        eligible_contributor_count <= LEADERSHIP_QUALITY_THIN_BORDERLINE_MAX_ELIGIBLE
        and participation_ratio < LEADERSHIP_QUALITY_THIN_BORDERLINE_MIN_PARTICIPATION
    ):
        return "Thin / filtered"
    if leader_cohort_eligible and (
        eligible_contributor_count >= LEADERSHIP_QUALITY_BROAD_MIN_ELIGIBLE
        and participation_ratio >= LEADERSHIP_QUALITY_BROAD_MIN_PARTICIPATION
        and breadth_value is not None
        and breadth_value >= LEADERSHIP_QUALITY_BROAD_MIN_BREADTH
    ):
        return "Broad leader"
    if leader_cohort_eligible:
        return "Narrow leader"
    if (
        not leader_cohort_eligible
        and eligible_contributor_count <= LEADERSHIP_QUALITY_NARROW_STRENGTH_MAX_ELIGIBLE
        and participation_ratio >= LEADERSHIP_QUALITY_NARROW_STRENGTH_MIN_PARTICIPATION
        and breadth_value is not None
        and breadth_value >= LEADERSHIP_QUALITY_NARROW_STRENGTH_MIN_BREADTH
    ):
        return "Narrow strength"
    return ""


def historical_concentration_label(row: pd.Series) -> str:
    contributor_count = next(
        (
            int(value)
            for value in [
                pd.to_numeric(row.get("covered_eligible_contributor_count"), errors="coerce"),
                pd.to_numeric(row.get("covered_eligible_constituent_count"), errors="coerce"),
                pd.to_numeric(row.get("eligible_contributor_count"), errors="coerce"),
                pd.to_numeric(row.get("eligible_constituent_count"), errors="coerce"),
                pd.to_numeric(row.get("ticker_count"), errors="coerce"),
            ]
            if value is not None and not pd.isna(value) and int(value) > 0
        ),
        0,
    )
    breadth = row.get("positive_1m_breadth_pct", row.get("breadth_1m"))
    breadth_value = float(breadth) if breadth is not None and not pd.isna(breadth) else None

    if contributor_count <= 0:
        return "Unknown"
    if contributor_count == 1:
        return "Single-name"
    if contributor_count == 2:
        return "Thin"
    if contributor_count == 3:
        return "Narrow"
    if breadth_value is not None and breadth_value >= 60:
        return "Broad"
    return "Narrow"


def _validate_window_leaderboard_inputs(momentum: dict) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    history = momentum.get("history", pd.DataFrame())
    if history.empty:
        return pd.DataFrame(), pd.DataFrame(), "No snapshots available for this window yet."

    history = history.copy()
    if "theme_id" not in history.columns and "theme" in history.columns:
        history["theme_id"] = history["theme"].astype(str)

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

    summary = summary.copy()
    if "theme_id" not in summary.columns and "theme" in summary.columns:
        summary["theme_id"] = summary["theme"].astype(str)

    return history, summary, None


def build_window_leaderboard(
    momentum: dict,
    perf_col: str,
    top_k: int = 10,
    *,
    primary_sort_col: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """Build a window-specific leaderboard from momentum output.

    Sorting is deterministic and window-specific:
    1) selected primary sort column,
    2) displayed window metric when momentum_score is primary,
    3) momentum_score,
    4) rank_change.
    """
    history, summary, msg = _validate_window_leaderboard_inputs(momentum)
    if msg:
        return pd.DataFrame(), msg

    latest = history.sort_values(["snapshot_time", "theme"]).groupby("theme_id", as_index=False).tail(1)
    if "is_active" in latest.columns:
        latest = latest[latest["is_active"] == True].copy()
    if latest.empty:
        return pd.DataFrame(), "No active themes available for this window."
    sort_col = primary_sort_col or perf_col
    sort_cols = [sort_col]
    if sort_col == "momentum_score":
        sort_cols.append(perf_col)
    else:
        sort_cols.append("momentum_score")
    sort_cols.extend(["rank_change", "theme"])
    ascending = [False] * (len(sort_cols) - 1) + [True]

    ranked = (
        latest[["theme_id", "theme", perf_col]]
        .merge(summary[["theme_id", "momentum_score", "rank_change"]], on="theme_id", how="left")
        .sort_values(sort_cols, ascending=ascending)
        .head(top_k)
        .reset_index(drop=True)
    )
    ranked["rank"] = ranked.index + 1
    return ranked[["rank", "theme_id", "theme", perf_col, "momentum_score", "rank_change"]], None


def build_current_leadership_table(
    rankings: pd.DataFrame,
    top_k: int = 12,
    *,
    score_col: str = "composite_score",
    eligible_count_col: str = "eligible_composite_count",
    output_score_col: str = "composite_score",
) -> pd.DataFrame:
    if rankings.empty:
        return pd.DataFrame()

    leadership = rankings.copy()
    if "is_active" in leadership.columns:
        leadership = leadership[leadership["is_active"] == True].copy()
    if leadership.empty:
        return pd.DataFrame()

    leadership = leadership.sort_values(
        [score_col, "positive_1m_breadth_pct", eligible_count_col, "ticker_count", "theme"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    leadership["rank"] = leadership.index + 1
    leadership["eligible_contributor_count"] = leadership[eligible_count_col]
    leadership = annotate_current_leadership_quality(leadership)
    leadership = leadership.head(top_k).reset_index(drop=True)
    if "current_momentum_score" not in leadership.columns:
        leadership["current_momentum_score"] = np.nan
    if "eligible_standardized_count" not in leadership.columns:
        leadership["eligible_standardized_count"] = np.nan
    if "eligible_momentum_count" not in leadership.columns:
        leadership["eligible_momentum_count"] = np.nan
    if "composite_atr_score" not in leadership.columns:
        leadership["composite_atr_score"] = np.nan
    if "composite_atr_rank" not in leadership.columns:
        leadership["composite_atr_rank"] = np.nan
    if score_col == "current_momentum_score" and output_score_col != "current_momentum_score":
        leadership["current_momentum_score_context"] = leadership["current_momentum_score"]
    elif score_col != "current_momentum_score" and output_score_col == "current_momentum_score":
        leadership["current_momentum_score"] = leadership[score_col]
    leadership = leadership.rename(columns={score_col: output_score_col, "positive_1m_breadth_pct": "breadth_1m"})
    if "current_momentum_score" not in leadership.columns and "current_momentum_score_context" in leadership.columns:
        leadership["current_momentum_score"] = leadership["current_momentum_score_context"]
    ordered_cols = [
        "rank",
        "theme_id",
        "theme",
        "category",
        "composite_atr_rank",
        "current_momentum_score",
        output_score_col,
        "composite_atr_score",
        "avg_1d",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "breadth_1m",
        "ticker_count",
        "eligible_contributor_count",
        "eligible_standardized_count",
        "eligible_momentum_count",
        "eligible_breadth_pct",
        "leadership_quality",
    ]
    deduped_cols: list[str] = []
    for col in ordered_cols:
        if col not in deduped_cols:
            deduped_cols.append(col)
    for col in deduped_cols:
        if col not in leadership.columns:
            leadership[col] = np.nan
    return leadership[deduped_cols]


def format_top_ticker_leaders(
    ticker_df: pd.DataFrame,
    *,
    top_k: int = 3,
    score_col: str = "ticker_composite_score",
    eligibility_col: str = "eligible",
    ticker_col: str = "ticker",
) -> str:
    if ticker_df.empty or ticker_col not in ticker_df.columns:
        return ""

    leaders = ticker_df.copy()
    if eligibility_col in leaders.columns:
        leaders = leaders[leaders[eligibility_col] == True].copy()
    if leaders.empty:
        return ""

    if score_col not in leaders.columns:
        leaders[score_col] = np.nan
    leaders[score_col] = pd.to_numeric(leaders[score_col], errors="coerce")
    leaders[ticker_col] = leaders[ticker_col].fillna("").astype(str).str.strip().str.upper()
    leaders = leaders[leaders[ticker_col] != ""].copy()
    if leaders.empty:
        return ""

    leaders = (
        leaders.sort_values([score_col, ticker_col], ascending=[False, True], na_position="last")
        .drop_duplicates(subset=[ticker_col], keep="first")
        .head(top_k)
    )
    return ", ".join(leaders[ticker_col].tolist())


def format_rank_history_delta(rank_history, *, lookback_points: int) -> str:
    if not isinstance(rank_history, (list, tuple)):
        return "-"
    cleaned = [float(value) for value in rank_history if value is not None and not pd.isna(value)]
    if len(cleaned) < 2:
        return "-"
    improvement = int(round(cleaned[0] - cleaned[-1]))
    if improvement > 0:
        delta = f"+{improvement}"
    elif improvement < 0:
        delta = str(improvement)
    else:
        delta = "0"
    return f"{int(lookback_points)}d: {delta}"


def format_rank_history_movement(rank_history, current_rank: int | float | None) -> str:
    if not isinstance(rank_history, (list, tuple)) or current_rank is None or pd.isna(current_rank):
        return "-"
    cleaned = [float(value) for value in rank_history if value is not None and not pd.isna(value)]
    if len(cleaned) < 2:
        return "-"
    start_rank = int(round(cleaned[0]))
    end_rank = int(round(float(current_rank)))
    delta = start_rank - end_rank
    return f"{start_rank} -> {end_rank} ({delta:+d})"


def build_current_rank_movers_table(
    rankings: pd.DataFrame,
    rank_history: pd.DataFrame,
    *,
    direction: str,
    top_k: int = 8,
) -> pd.DataFrame:
    if rankings.empty or rank_history.empty:
        return pd.DataFrame()

    current = rankings.copy().reset_index(drop=True)
    current["current_rank"] = current.index + 1
    history = rank_history.copy()
    history["prior_rank"] = history["rank_history"].apply(
        lambda values: (
            float([value for value in values if value is not None and not pd.isna(value)][0])
            if isinstance(values, (list, tuple)) and len([value for value in values if value is not None and not pd.isna(value)]) >= 2
            else np.nan
        )
    )
    merged = current.merge(history[["theme_id", "rank_history", "prior_rank"]], on="theme_id", how="left")
    merged = merged.dropna(subset=["prior_rank"]).copy()
    if merged.empty:
        return pd.DataFrame()

    merged["rank_move_value"] = merged["prior_rank"] - merged["current_rank"]
    if direction == "riser":
        merged = merged[merged["rank_move_value"] > 0].copy()
        merged = merged.sort_values(["rank_move_value", "current_rank", "theme"], ascending=[False, True, True])
    elif direction == "faller":
        merged = merged[merged["rank_move_value"] < 0].copy()
        merged = merged.sort_values(["rank_move_value", "current_rank", "theme"], ascending=[True, True, True])
    else:
        raise ValueError(f"Unsupported mover direction: {direction}")
    if merged.empty:
        return pd.DataFrame()

    merged["move"] = merged.apply(
        lambda row: format_rank_history_movement(row.get("rank_history"), row.get("current_rank")),
        axis=1,
    )
    out = merged.head(top_k).copy()
    return out[
        [
            "current_rank",
            "theme_id",
            "theme",
            "category",
            "move",
            "composite_score",
        ]
    ].reset_index(drop=True)


def build_current_rank_persistence_table(
    rankings: pd.DataFrame,
    rank_history: pd.DataFrame,
    *,
    direction: str,
    top_k: int = 8,
    min_transition_wins: int = 4,
    min_transition_count: int = 5,
) -> pd.DataFrame:
    if rankings.empty or rank_history.empty:
        return pd.DataFrame()

    current = rankings.copy().reset_index(drop=True)
    current["current_rank"] = current.index + 1
    history = rank_history.copy()
    history["cleaned_rank_history"] = history["rank_history"].apply(
        lambda values: [float(value) for value in values if value is not None and not pd.isna(value)]
        if isinstance(values, (list, tuple))
        else []
    )
    history["transition_count"] = history["cleaned_rank_history"].apply(lambda values: max(len(values) - 1, 0))
    history = history[history["transition_count"] >= int(min_transition_count)].copy()
    if history.empty:
        return pd.DataFrame()

    history["rank_up_days"] = history["cleaned_rank_history"].apply(
        lambda values: int(sum(1 for idx in range(1, len(values)) if values[idx - 1] > values[idx]))
    )
    history["rank_down_days"] = history["cleaned_rank_history"].apply(
        lambda values: int(sum(1 for idx in range(1, len(values)) if values[idx - 1] < values[idx]))
    )
    history["net_rank_move"] = history["cleaned_rank_history"].apply(
        lambda values: float(values[0] - values[-1]) if len(values) >= 2 else np.nan
    )
    history["persistence"] = history.apply(
        lambda row: (
            f"{int(row['rank_up_days'])}/{int(row['transition_count'])} up days"
            if int(row.get("rank_up_days", 0)) >= int(row.get("rank_down_days", 0))
            else f"{int(row['rank_down_days'])}/{int(row['transition_count'])} down days"
        ),
        axis=1,
    )

    merged = current.merge(
        history[
            [
                "theme_id",
                "rank_history",
                "rank_up_days",
                "rank_down_days",
                "transition_count",
                "net_rank_move",
                "persistence",
            ]
        ],
        on="theme_id",
        how="left",
    ).copy()

    if direction == "riser":
        merged = merged[
            (merged["rank_up_days"] >= int(min_transition_wins))
            & (merged["net_rank_move"] > 0)
        ].copy()
        merged = merged.sort_values(
            ["rank_up_days", "net_rank_move", "current_rank", "theme"],
            ascending=[False, False, True, True],
        )
    elif direction == "faller":
        merged = merged[
            (merged["rank_down_days"] >= int(min_transition_wins))
            & (merged["net_rank_move"] < 0)
        ].copy()
        merged = merged.sort_values(
            ["rank_down_days", "net_rank_move", "current_rank", "theme"],
            ascending=[False, True, True, True],
        )
    else:
        raise ValueError(f"Unsupported persistence direction: {direction}")

    if merged.empty:
        return pd.DataFrame()

    merged["move"] = merged.apply(
        lambda row: format_rank_history_movement(row.get("rank_history"), row.get("current_rank")),
        axis=1,
    )
    out = merged.head(top_k).copy()
    return out[
        [
            "current_rank",
            "theme_id",
            "theme",
            "category",
            "move",
            "persistence",
            "composite_score",
        ]
    ].reset_index(drop=True)


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
    current["performance"] = current[perf_col]
    current = current.sort_values(
        [perf_col, "composite_score", "eligible_contributor_count", "eligible_breadth_pct", "theme"],
        ascending=[False, False, False, False, True],
    ).head(top_k).reset_index(drop=True)
    current["rank"] = current.index + 1
    leadership_rank_col = "current_rank" if "current_rank" in current.columns else "rank"
    current = annotate_current_leadership_quality(current, rank_col=leadership_rank_col)
    if "current_momentum_score" not in current.columns:
        current["current_momentum_score"] = np.nan
    if "composite_atr_score" not in current.columns:
        current["composite_atr_score"] = np.nan
    if "composite_atr_rank" not in current.columns:
        current["composite_atr_rank"] = np.nan
    if "avg_3m" not in current.columns:
        current["avg_3m"] = np.nan
    current = current.rename(columns={"positive_1m_breadth_pct": "breadth_1m"})
    output_cols = [
        "rank",
        "theme_id",
        "theme",
        "category",
        "composite_atr_rank",
        "performance",
        "avg_1d",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "current_momentum_score",
        "composite_score",
        "composite_atr_score",
        "breadth_1m",
        "ticker_count",
        "eligible_contributor_count",
        "eligible_breadth_pct",
        "leadership_quality",
    ]
    for col in output_cols:
        if col not in current.columns:
            current[col] = np.nan
    return current[output_cols]


def build_category_leaderboard(momentum: dict, perf_col: str, top_k: int = 10) -> tuple[pd.DataFrame, str | None]:
    """Aggregate the full eligible theme window into a category leaderboard."""
    history, summary, msg = _validate_window_leaderboard_inputs(momentum)
    if msg:
        return pd.DataFrame(), msg

    latest = history.sort_values(["snapshot_time", "theme"]).groupby("theme_id", as_index=False).tail(1)
    if "is_active" in latest.columns:
        latest = latest[latest["is_active"] == True].copy()
    if latest.empty:
        return pd.DataFrame(), "No active themes available for this window."
    grouped = latest[["theme_id", "theme", "category", perf_col, "positive_1m_breadth_pct"]].merge(
        summary[["theme_id", "momentum_score", "rank_change"]],
        on="theme_id",
        how="left",
    )
    grouped = grouped.rename(columns={perf_col: "performance", "positive_1m_breadth_pct": "breadth_1m"})
    grouped["category_group"] = grouped["category"].fillna("").astype(str).str.strip()
    grouped.loc[grouped["category_group"] == "", "category_group"] = grouped["theme"]
    grouped = grouped.sort_values(
        ["category_group", "performance", "momentum_score", "breadth_1m", "theme"],
        ascending=[True, False, False, False, True],
    ).reset_index(drop=True)

    grouped = disambiguate_theme_labels(grouped, output_col="theme_display")
    preview_map = grouped.groupby("category_group")["theme_display"].apply(lambda s: _format_top_theme_preview(s.tolist())).to_dict()

    aggregated = (
        grouped.groupby("category_group", dropna=False)
        .agg(
            performance=("performance", "mean"),
            momentum_score=("momentum_score", "mean"),
            breadth_1m=("breadth_1m", "mean"),
            contributing_themes=("theme_id", "nunique"),
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

    latest = history.sort_values(["snapshot_time", "theme"]).groupby("theme_id", as_index=False).tail(1)
    if "is_active" in latest.columns:
        latest = latest[latest["is_active"] == True].copy()
    if latest.empty:
        return pd.DataFrame(), "No active themes available for this window."
    breakdown = latest[["theme_id", "theme", "category", perf_col, "positive_1m_breadth_pct"]].merge(
        summary[["theme_id", "momentum_score", "rank_change"]],
        on="theme_id",
        how="left",
    )
    breakdown = breakdown.rename(columns={perf_col: "performance", "positive_1m_breadth_pct": "breadth_1m"})
    breakdown["category"] = breakdown["category"].fillna("").astype(str).str.strip()
    breakdown.loc[breakdown["category"] == "", "category"] = breakdown["theme"]
    breakdown = breakdown.sort_values(
        ["category", "performance", "momentum_score", "breadth_1m", "theme"],
        ascending=[True, False, False, False, True],
    ).reset_index(drop=True)
    breakdown = disambiguate_theme_labels(breakdown, output_col="theme_display")
    for metric_col in ("performance", "momentum_score", "breadth_1m"):
        breakdown[metric_col] = breakdown[metric_col].round(2)
    return breakdown[["theme_id", "category", "theme", "theme_display", "performance", "momentum_score", "breadth_1m", "rank_change"]], None


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
