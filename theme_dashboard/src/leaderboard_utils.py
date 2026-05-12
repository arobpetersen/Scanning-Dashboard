from __future__ import annotations

import pandas as pd
import numpy as np

from .config import CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS
from .db_introspection import table_exists, table_has_column
from .queries import latest_ticker_snapshots
from .rankings import (
    current_ticker_is_eligible,
    ticker_current_momentum_score,
    ticker_standardized_composite_score,
    visible_ticker_suppressed,
)

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
TICKER_LEADERSHIP_WINDOWS = {
    "1D": "perf_1d",
    "1W": "perf_1w",
    "1M": "perf_1m",
    "3M": "perf_3m",
    "6M": "perf_6m",
}
TICKER_LEADERSHIP_SORT_COLUMNS = {
    "1D %": "perf_1d",
    "1W %": "perf_1w",
    "1M %": "perf_1m",
    "3M %": "perf_3m",
    "6M %": "perf_6m",
    "Ticker Composite Score": "ticker_composite_score",
    "Ticker Momentum Score": "ticker_momentum_score",
}


def build_top_governed_ticker_leaders(
    conn,
    *,
    window: str = "1W",
    sort_by: str | None = None,
    top_k: int = 25,
) -> pd.DataFrame:
    """Return top active governed tickers by an existing return window or ticker score."""
    window_label = str(window or "1W").upper()
    default_sort_col = TICKER_LEADERSHIP_WINDOWS.get(window_label)
    if not default_sort_col:
        raise ValueError(f"Unsupported ticker leadership window: {window}")
    sort_col = TICKER_LEADERSHIP_SORT_COLUMNS.get(str(sort_by or "").strip(), default_sort_col)

    membership = conn.execute(
        """
        SELECT
            upper(trim(m.ticker)) AS ticker,
            t.id AS theme_id,
            t.name AS theme,
            t.category
        FROM themes t
        JOIN theme_membership m ON m.theme_id = t.id
        WHERE t.is_active = TRUE
          AND length(trim(m.ticker)) > 0
        ORDER BY ticker, theme
        """
    ).df()
    empty_cols = [
        "rank",
        "ticker",
        "themes",
        "categories",
        "perf_1d",
        "perf_1w",
        "perf_1m",
        "perf_3m",
        "perf_6m",
        "ticker_composite_score",
        "ticker_momentum_score",
        "price",
        "avg_volume",
        "dollar_volume",
        "theme_count",
        "leadership_note",
        "snapshot_time",
    ]
    if membership.empty:
        return pd.DataFrame(columns=empty_cols)

    latest = latest_ticker_snapshots(conn)
    if latest.empty:
        return pd.DataFrame(columns=empty_cols)

    latest = latest.copy()
    latest["ticker"] = latest["ticker"].fillna("").astype(str).str.strip().str.upper()
    for col in ("price", "avg_volume", "perf_1d", "perf_1w", "perf_1m", "perf_3m", "perf_6m"):
        if col not in latest.columns:
            latest[col] = np.nan
        latest[col] = pd.to_numeric(latest[col], errors="coerce")

    if table_exists(conn, "symbol_refresh_status"):
        status_cols = ["ticker"]
        if table_has_column(conn, "symbol_refresh_status", "status"):
            status_cols.append("status")
        if table_has_column(conn, "symbol_refresh_status", "manual_suppressed"):
            status_cols.append("manual_suppressed")
        statuses = conn.execute(f"SELECT {', '.join(status_cols)} FROM symbol_refresh_status").df()
        statuses["ticker"] = statuses["ticker"].fillna("").astype(str).str.strip().str.upper()
        if "status" not in statuses.columns:
            statuses["status"] = "active"
        if "manual_suppressed" not in statuses.columns:
            statuses["manual_suppressed"] = False
    else:
        statuses = pd.DataFrame(columns=["ticker", "status", "manual_suppressed"])

    grouped_membership = (
        membership.groupby("ticker", as_index=False)
        .agg(
            themes=("theme", lambda values: ", ".join(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))),
            categories=("category", lambda values: ", ".join(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))),
            theme_count=("theme_id", "nunique"),
        )
    )
    leaders = grouped_membership.merge(latest, on="ticker", how="left")
    leaders = leaders.merge(statuses[["ticker", "status", "manual_suppressed"]], on="ticker", how="left")
    leaders["status"] = leaders["status"].fillna("active")
    leaders["manual_suppressed"] = leaders["manual_suppressed"].fillna(False).astype(bool)
    leaders["visible_suppressed"] = leaders.apply(
        lambda row: visible_ticker_suppressed(row.get("status"), bool(row.get("manual_suppressed"))),
        axis=1,
    )
    leaders["effective_status"] = np.where(leaders["visible_suppressed"], "refresh_suppressed", leaders["status"])
    leaders["dollar_volume"] = leaders["price"] * leaders["avg_volume"]
    leaders["eligible"] = leaders.apply(
        lambda row: current_ticker_is_eligible(
            row.get("price"),
            row.get("avg_volume"),
            row.get("effective_status"),
            snapshot_present=bool(pd.notna(row.get("run_id")) and pd.notna(row.get("snapshot_time"))),
        ),
        axis=1,
    )
    leaders = leaders[leaders["eligible"]].copy()
    if leaders.empty:
        return pd.DataFrame(columns=empty_cols)

    leaders["ticker_composite_score"] = leaders.apply(
        lambda row: ticker_standardized_composite_score(
            row.get("perf_1w"),
            row.get("perf_1m"),
            row.get("perf_3m"),
        ),
        axis=1,
    )
    leaders["ticker_momentum_score"] = leaders.apply(
        lambda row: ticker_current_momentum_score(
            row.get("perf_1w"),
            row.get("perf_1m"),
            row.get("perf_3m"),
        ),
        axis=1,
    )
    leaders[sort_col] = pd.to_numeric(leaders[sort_col], errors="coerce")
    leaders = leaders[leaders[sort_col].notna()].copy()
    if leaders.empty:
        return pd.DataFrame(columns=empty_cols)

    leaders["leadership_note"] = np.where(leaders["theme_count"] > 1, "Multi-theme leader", "Single-theme leader")
    leaders = leaders.sort_values([sort_col, "dollar_volume", "ticker"], ascending=[False, False, True]).head(int(top_k)).reset_index(drop=True)
    leaders["rank"] = leaders.index + 1
    for metric_col in ("perf_1d", "perf_1w", "perf_1m", "perf_3m", "perf_6m"):
        leaders[metric_col] = pd.to_numeric(leaders[metric_col], errors="coerce").round(2)
    for score_col in ("ticker_composite_score", "ticker_momentum_score"):
        leaders[score_col] = pd.to_numeric(leaders[score_col], errors="coerce").round(2)
    leaders["price"] = pd.to_numeric(leaders["price"], errors="coerce").round(2)
    leaders["dollar_volume"] = pd.to_numeric(leaders["dollar_volume"], errors="coerce").round(0)
    leaders["theme_count"] = leaders["theme_count"].fillna(0).astype(int)
    return leaders[empty_cols]

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


def _market_snapshot_prior_rank_lookup(prior_rank_history: pd.DataFrame) -> dict[int, float]:
    if prior_rank_history.empty or "theme_id" not in prior_rank_history.columns or "rank" not in prior_rank_history.columns:
        return {}

    history = prior_rank_history.copy()
    history["snapshot_date"] = pd.to_datetime(history.get("snapshot_date"), errors="coerce")
    history["rank"] = pd.to_numeric(history["rank"], errors="coerce")
    history = history.dropna(subset=["theme_id", "snapshot_date", "rank"])
    if history.empty:
        return {}

    latest_date = history["snapshot_date"].max()
    latest = history[history["snapshot_date"] == latest_date].copy()
    latest = latest.sort_values(["theme_id", "rank"])
    return {
        int(row["theme_id"]): float(row["rank"])
        for _, row in latest.drop_duplicates(subset=["theme_id"], keep="first").iterrows()
    }


def _market_snapshot_prior_composite_lookup(prior_rank_history: pd.DataFrame) -> dict[int, float]:
    if (
        prior_rank_history.empty
        or "theme_id" not in prior_rank_history.columns
        or "standardized_composite_score" not in prior_rank_history.columns
    ):
        return {}

    history = prior_rank_history.copy()
    history["snapshot_date"] = pd.to_datetime(history.get("snapshot_date"), errors="coerce")
    history["standardized_composite_score"] = pd.to_numeric(history["standardized_composite_score"], errors="coerce")
    history = history.dropna(subset=["theme_id", "snapshot_date", "standardized_composite_score"])
    if history.empty:
        return {}

    latest_date = history["snapshot_date"].max()
    latest = history[history["snapshot_date"] == latest_date].copy()
    latest = latest.sort_values(["theme_id", "standardized_composite_score"], ascending=[True, False])
    return {
        int(row["theme_id"]): float(row["standardized_composite_score"])
        for _, row in latest.drop_duplicates(subset=["theme_id"], keep="first").iterrows()
    }


MARKET_THEME_DRIVER_LABELS = {
    "single_name_led": "One ticker leading the move",
    "broad_advance": "Broad advance",
    "broad_fade": "Broad fade",
    "mixed_action": "Mixed action inside theme",
    "mixed_leadership": "Mixed leadership",
    "new_top_12_entrant": "New top-12 entrant",
}


def _market_snapshot_driver_stats(member_rows: pd.DataFrame) -> dict[str, object]:
    empty = {
        "positive_1d_pct": np.nan,
        "top_1d_driver": "",
        "top_1d_driver_move": np.nan,
        "top_1d_driver_share": np.nan,
        "driver_flag": "",
    }
    if member_rows.empty or "perf_1d" not in member_rows.columns:
        return empty

    members = member_rows.copy()
    members["perf_1d"] = pd.to_numeric(members["perf_1d"], errors="coerce")
    members = members[members["perf_1d"].notna()].copy()
    if members.empty:
        return empty

    if "ticker" in members.columns:
        members["ticker"] = members["ticker"].fillna("").astype(str).str.strip().str.upper()
    else:
        members["ticker"] = ""

    contributor_count = int(len(members))
    positive_count = int((members["perf_1d"] > 0).sum())
    positive_1d_pct = round((positive_count / contributor_count) * 100.0, 1) if contributor_count else np.nan
    avg_1d = float(members["perf_1d"].mean())
    min_1d = float(members["perf_1d"].min())
    max_1d = float(members["perf_1d"].max())

    if avg_1d >= 0:
        contribution_pool = members[members["perf_1d"] > 0].copy()
    else:
        contribution_pool = members[members["perf_1d"] < 0].copy()
    contribution_pool["_driver_abs"] = contribution_pool["perf_1d"].abs() if not contribution_pool.empty else pd.Series(dtype=float)

    top_driver = ""
    driver_share = np.nan
    if not contribution_pool.empty:
        contribution_pool = contribution_pool.sort_values(["_driver_abs", "ticker"], ascending=[False, True])
        total_driver_abs = float(contribution_pool["_driver_abs"].sum())
        if total_driver_abs > 0:
            top_driver = str(contribution_pool.iloc[0]["ticker"])
            top_driver_move = round(float(contribution_pool.iloc[0]["perf_1d"]), 1)
            driver_share = round((float(contribution_pool.iloc[0]["_driver_abs"]) / total_driver_abs) * 100.0, 1)
        else:
            top_driver_move = np.nan
    else:
        top_driver_move = np.nan

    flag = ""
    if contributor_count <= 1:
        flag = MARKET_THEME_DRIVER_LABELS["single_name_led"] if contributor_count == 1 else ""
    elif avg_1d > 0 and positive_1d_pct >= 70:
        flag = MARKET_THEME_DRIVER_LABELS["broad_advance"]
    elif avg_1d < 0 and positive_1d_pct <= 30:
        flag = MARKET_THEME_DRIVER_LABELS["broad_fade"]
    elif pd.notna(driver_share) and driver_share >= 65:
        flag = MARKET_THEME_DRIVER_LABELS["single_name_led"]
    elif min_1d < 0 < max_1d and 35 <= positive_1d_pct <= 65:
        flag = MARKET_THEME_DRIVER_LABELS["mixed_action"]
    elif avg_1d > 0 and positive_1d_pct >= 45:
        flag = MARKET_THEME_DRIVER_LABELS["mixed_leadership"]

    return {
        "positive_1d_pct": positive_1d_pct,
        "top_1d_driver": top_driver,
        "top_1d_driver_move": top_driver_move,
        "top_1d_driver_share": driver_share,
        "driver_flag": flag,
    }


def build_market_theme_snapshot_table(
    current_rankings: pd.DataFrame,
    member_metrics: pd.DataFrame,
    prior_rank_history: pd.DataFrame,
    *,
    top_k: int = 12,
) -> pd.DataFrame:
    if current_rankings.empty:
        return pd.DataFrame()

    current = current_rankings.copy().reset_index(drop=True)
    if "rank" not in current.columns:
        current["rank"] = current.index + 1
    current["rank"] = pd.to_numeric(current["rank"], errors="coerce")
    current = current[current["rank"].notna()].copy()
    current = current[current["rank"] <= int(top_k)].copy()
    if current.empty:
        return pd.DataFrame()

    prior_rank_by_theme = _market_snapshot_prior_rank_lookup(prior_rank_history)
    prior_composite_by_theme = _market_snapshot_prior_composite_lookup(prior_rank_history)
    members = member_metrics.copy() if not member_metrics.empty else pd.DataFrame()
    if not members.empty and "theme_id" in members.columns:
        members["theme_id"] = pd.to_numeric(members["theme_id"], errors="coerce")

    rows: list[dict[str, object]] = []
    for _, row in current.sort_values(["rank", "theme"]).iterrows():
        theme_id = int(row["theme_id"])
        prior_rank = prior_rank_by_theme.get(theme_id, np.nan)
        rank_delta = np.nan if pd.isna(prior_rank) else int(float(prior_rank) - float(row["rank"]))
        current_composite = pd.to_numeric(row.get("composite_score"), errors="coerce")
        prior_composite = prior_composite_by_theme.get(theme_id, np.nan)
        composite_delta = (
            round(float(current_composite) - float(prior_composite), 2)
            if pd.notna(current_composite) and pd.notna(prior_composite)
            else np.nan
        )
        theme_members = (
            members[members["theme_id"] == theme_id].copy()
            if not members.empty and "theme_id" in members.columns
            else pd.DataFrame()
        )
        driver_stats = _market_snapshot_driver_stats(theme_members)
        driver_flag = str(driver_stats.get("driver_flag") or "")
        if (pd.isna(prior_rank) or float(prior_rank) > int(top_k)) and int(row["rank"]) <= int(top_k):
            new_entrant_label = MARKET_THEME_DRIVER_LABELS["new_top_12_entrant"]
            driver_flag = new_entrant_label if not driver_flag else f"{driver_flag}; {new_entrant_label}"

        eligible_contributors = row.get(
            "eligible_contributor_count",
            row.get("eligible_standardized_count", row.get("eligible_ticker_count", np.nan)),
        )
        rows.append(
            {
                "current_rank": int(row["rank"]),
                "theme_id": theme_id,
                "theme": row.get("theme"),
                "category": row.get("category"),
                "prior_rank": prior_rank,
                "rank_delta_1d": rank_delta,
                "avg_1d": row.get("avg_1d"),
                "avg_1w": row.get("avg_1w"),
                "avg_1m": row.get("avg_1m"),
                "avg_6m": row.get("avg_6m"),
                "composite_score": row.get("composite_score"),
                "prior_composite_score": prior_composite,
                "delta_composite_score": composite_delta,
                "current_momentum_score": row.get("current_momentum_score"),
                "eligible_contributors": eligible_contributors,
                "positive_1d_pct": driver_stats.get("positive_1d_pct"),
                "top_1d_driver": driver_stats.get("top_1d_driver"),
                "top_1d_driver_move": driver_stats.get("top_1d_driver_move"),
                "top_1d_driver_share": driver_stats.get("top_1d_driver_share"),
                "driver_flag": driver_flag,
            }
        )

    return pd.DataFrame(rows)


def build_top_theme_baseline_snapshot(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df.empty:
        return pd.DataFrame(columns=["metric", "value"])

    top = snapshot_df.copy()
    for col in ("avg_1d", "positive_1d_pct", "delta_composite_score", "current_momentum_score"):
        top[col] = pd.to_numeric(top.get(col), errors="coerce")

    def _pct(value) -> str:
        return "-" if value is None or pd.isna(value) else f"{float(value):.1f}%"

    def _num(value) -> str:
        return "-" if value is None or pd.isna(value) else f"{float(value):.2f}"

    rows = [
        {"metric": "Top-12 avg 1D", "value": _pct(top["avg_1d"].mean())},
        {"metric": "Top-12 median 1D", "value": _pct(top["avg_1d"].median())},
        {"metric": "Top-12 avg momentum score", "value": _num(top["current_momentum_score"].mean())},
        {"metric": "Top-12 avg positive 1D breadth", "value": _pct(top["positive_1d_pct"].mean())},
    ]
    rows.insert(2, {"metric": "Top-12 avg composite Δ 1D", "value": _num(top["delta_composite_score"].mean())})
    return pd.DataFrame(rows)


THEME_ANOMALY_LABELS = {
    "relative_strength": "Theme stronger than top-12 baseline",
    "relative_weakness": "Theme weaker than top-12 baseline",
    "breadth_strength": "More broad-based buying than peers",
    "breadth_weakness": "Narrower participation than peers",
    "internal_divergence": "Mixed action inside theme",
    "single_name_concentration": "One ticker driving theme",
}


TICKER_VS_THEME_LABELS = {
    "severe_downside": "Sharp downside outlier",
    "ticker_holding_up": "Theme weak, ticker holding up",
    "ticker_weak_theme_strong": "Ticker weak while theme is strong",
    "single_name_driver": "Ticker driving theme move",
    "relative_strength": "Ticker leading theme",
    "relative_weakness": "Ticker lagging theme",
    "positive_divergence": "Theme weak, ticker holding up",
    "in_line": "In line with theme",
}


def build_theme_anomaly_snapshot_table(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["theme", "standout", "evidence", "basis"])
    if snapshot_df.empty:
        return empty

    top = snapshot_df.copy()
    for col in ("avg_1d", "positive_1d_pct", "top_1d_driver_move", "top_1d_driver_share", "current_rank"):
        top[col] = pd.to_numeric(top.get(col), errors="coerce")
    top = top[top["avg_1d"].notna()].copy()
    if top.empty:
        return empty

    peer_avg_1d = float(top["avg_1d"].mean())
    peer_median_1d = float(top["avg_1d"].median())
    peer_positive_breadth = float(top["positive_1d_pct"].dropna().mean()) if top["positive_1d_pct"].notna().any() else np.nan

    def _pct(value) -> str:
        return "-" if value is None or pd.isna(value) else f"{float(value):.1f}%"

    def _theme_label(row: pd.Series) -> str:
        theme = str(row.get("theme") or "-")
        category = str(row.get("category") or "").strip()
        return f"{theme} - {category}" if category else theme

    def _append(
        rows: list[dict[str, object]],
        row: pd.Series,
        standout: str,
        evidence: str,
        basis: str,
        priority: int,
        evidence_order: int,
    ) -> None:
        rows.append(
            {
                "theme": _theme_label(row),
                "standout": standout,
                "evidence": evidence,
                "basis": basis,
                "_priority": int(priority),
                "_evidence_order": int(evidence_order),
                "_rank": row.get("current_rank"),
            }
        )

    rows: list[dict[str, object]] = []
    for _, row in top.iterrows():
        avg_1d = float(row["avg_1d"])
        breadth = row.get("positive_1d_pct")
        driver = str(row.get("top_1d_driver") or "").strip()
        driver_move = row.get("top_1d_driver_move")
        driver_share = row.get("top_1d_driver_share")
        rank = row.get("current_rank")

        avg_diff = avg_1d - peer_avg_1d
        median_diff = avg_1d - peer_median_1d
        relative_strength_flagged = avg_diff >= 2.0 and median_diff >= 1.5
        breadth_text = _pct(breadth)
        peer_text = _pct(peer_avg_1d)
        peer_breadth_text = _pct(peer_positive_breadth)
        driver_text = f", {driver} {_pct(driver_move)}" if driver and pd.notna(driver_move) else ""

        if relative_strength_flagged:
            _append(
                rows,
                row,
                THEME_ANOMALY_LABELS["relative_strength"],
                f"avg 1D {_pct(avg_1d)} vs top-12 avg {peer_text}",
                f"avg diff {avg_diff:+.1f} pts; median diff {median_diff:+.1f} pts",
                30,
                10,
            )
        elif avg_diff <= -2.0 and median_diff <= -1.5:
            _append(
                rows,
                row,
                THEME_ANOMALY_LABELS["relative_weakness"],
                f"avg 1D {_pct(avg_1d)} vs top-12 avg {peer_text}",
                f"avg diff {avg_diff:+.1f} pts; median diff {median_diff:+.1f} pts",
                30,
                10,
            )

        breadth_strength_supported = relative_strength_flagged or avg_1d >= 0 or median_diff >= 1.0
        if (
            pd.notna(breadth)
            and pd.notna(peer_positive_breadth)
            and float(breadth) >= peer_positive_breadth + 25.0
            and breadth_strength_supported
        ):
            _append(
                rows,
                row,
                THEME_ANOMALY_LABELS["breadth_strength"],
                f"{breadth_text} positive breadth vs top-12 avg {peer_breadth_text}",
                f"breadth diff {float(breadth) - peer_positive_breadth:+.1f} pts",
                20,
                20,
            )
        elif pd.notna(breadth) and pd.notna(peer_positive_breadth) and float(breadth) <= peer_positive_breadth - 25.0:
            _append(
                rows,
                row,
                THEME_ANOMALY_LABELS["breadth_weakness"],
                f"{breadth_text} positive breadth vs top-12 avg {peer_breadth_text}",
                f"breadth diff {float(breadth) - peer_positive_breadth:+.1f} pts",
                20,
                20,
            )

        if avg_1d <= 0 and pd.notna(driver_move) and float(driver_move) >= 5.0:
            _append(
                rows,
                row,
                THEME_ANOMALY_LABELS["internal_divergence"],
                f"theme avg {_pct(avg_1d)}{driver_text}; {breadth_text} positive breadth",
                "top constituent is holding up while the theme average is flat or negative",
                10,
                30,
            )
        elif avg_1d >= 0 and pd.notna(breadth) and pd.notna(peer_positive_breadth) and float(breadth) <= peer_positive_breadth - 20.0:
            _append(
                rows,
                row,
                THEME_ANOMALY_LABELS["internal_divergence"],
                f"theme avg {_pct(avg_1d)}; {breadth_text} positive breadth vs top-12 avg {peer_breadth_text}",
                "positive/flat average with materially below-baseline breadth",
                10,
                30,
            )

        if pd.notna(driver_share) and float(driver_share) >= 70.0:
            _append(
                rows,
                row,
                THEME_ANOMALY_LABELS["single_name_concentration"],
                f"{driver or '-'} {_pct(driver_move)}; driver share {_pct(driver_share)}; breadth {breadth_text}",
                "one ticker accounts for >= 70% of the directional move",
                40,
                30,
            )

    if not rows:
        return empty

    out = pd.DataFrame(rows)
    out = (
        out.sort_values(["_priority", "_rank", "theme", "standout"], ascending=[False, True, True, True])
        .drop_duplicates(subset=["theme", "standout"], keep="first")
        .sort_values(["_rank", "_evidence_order", "theme", "standout"], ascending=[True, True, True, True])
        .reset_index(drop=True)
    )
    combined = (
        out.groupby("theme", sort=False)
        .agg(
            standout=("standout", lambda values: "; ".join(dict.fromkeys(str(value) for value in values if str(value).strip()))),
            evidence=("evidence", lambda values: "; ".join(dict.fromkeys(str(value) for value in values if str(value).strip()))),
            basis=("basis", lambda values: "; ".join(dict.fromkeys(str(value) for value in values if str(value).strip()))),
            _priority=("_priority", "max"),
            _rank=("_rank", "min"),
        )
        .reset_index()
        .sort_values(["_priority", "_rank", "theme"], ascending=[False, True, True])
        .drop(columns=["_priority", "_rank"])
        .reset_index(drop=True)
    )
    return combined


def build_theme_ticker_standout_table(
    snapshot_df: pd.DataFrame,
    member_metrics: pd.DataFrame,
) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["ticker", "theme", "standout", "ticker_1d", "theme_avg_1d", "diff_vs_theme", "basis"])
    if snapshot_df.empty or member_metrics.empty:
        return empty
    if "theme_id" not in snapshot_df.columns or "theme_id" not in member_metrics.columns or "perf_1d" not in member_metrics.columns:
        return empty

    themes = snapshot_df.copy()
    themes["theme_id"] = pd.to_numeric(themes["theme_id"], errors="coerce")
    themes["avg_1d"] = pd.to_numeric(themes.get("avg_1d"), errors="coerce")
    themes = themes.dropna(subset=["theme_id", "avg_1d"])
    if themes.empty:
        return empty
    themes["theme_label"] = themes["theme"].fillna("").astype(str)
    if "category" in themes.columns:
        category_text = themes["category"].fillna("").astype(str).str.strip()
        themes["theme_label"] = np.where(category_text.ne(""), themes["theme_label"] + " - " + category_text, themes["theme_label"])

    members = member_metrics.copy()
    members["theme_id"] = pd.to_numeric(members["theme_id"], errors="coerce")
    members["perf_1d"] = pd.to_numeric(members["perf_1d"], errors="coerce")
    if "ticker" in members.columns:
        members["ticker"] = members["ticker"].fillna("").astype(str).str.strip().str.upper()
    else:
        members["ticker"] = ""
    members = members[(members["theme_id"].notna()) & (members["perf_1d"].notna()) & members["ticker"].ne("")]
    if members.empty:
        return empty

    joined = members.merge(themes[["theme_id", "theme_label", "avg_1d"]], on="theme_id", how="inner")
    if joined.empty:
        return empty
    joined["diff_vs_theme"] = joined["perf_1d"] - joined["avg_1d"]
    joined["_same_direction"] = np.where(
        joined["avg_1d"] >= 0,
        joined["perf_1d"] > 0,
        joined["perf_1d"] < 0,
    )
    joined["_directional_abs"] = np.where(joined["_same_direction"], joined["perf_1d"].abs(), 0.0)
    totals = joined.groupby("theme_id")["_directional_abs"].sum().rename("_theme_directional_abs")
    joined = joined.merge(totals, on="theme_id", how="left")
    joined["driver_share"] = np.where(
        joined["_theme_directional_abs"] > 0,
        (joined["_directional_abs"] / joined["_theme_directional_abs"]) * 100.0,
        np.nan,
    )

    def _pct(value) -> str:
        return "-" if value is None or pd.isna(value) else f"{float(value):.1f}%"

    def _opposite_direction_basis(ticker_1d: float, theme_avg: float) -> str:
        direction = "up" if ticker_1d > 0 else "down"
        theme_direction = "down" if theme_avg < 0 else "up"
        return f"ticker {direction} while theme avg is {theme_direction} {_pct(theme_avg)}"

    rows: list[dict[str, object]] = []
    for _, row in joined.iterrows():
        basis_parts: list[str] = []
        diff = float(row["diff_vs_theme"])
        ticker_1d = float(row["perf_1d"])
        theme_avg = float(row["avg_1d"])
        driver_share = row.get("driver_share")
        opposite_direction = (ticker_1d > 0 > theme_avg) or (ticker_1d < 0 < theme_avg)
        severe_downside = diff <= -8.0 or ticker_1d <= -8.0
        single_name_driver = pd.notna(driver_share) and float(driver_share) >= 70.0
        if diff >= 5.0:
            basis_parts.append(f"ticker 1D {_pct(ticker_1d)} is {diff:+.1f} pts vs theme avg {_pct(theme_avg)}")
        elif diff <= -5.0:
            basis_parts.append(f"ticker 1D {_pct(ticker_1d)} is {diff:+.1f} pts vs theme avg {_pct(theme_avg)}")
        if opposite_direction and abs(diff) >= 3.0:
            basis_parts.append(_opposite_direction_basis(ticker_1d, theme_avg))
        if single_name_driver:
            basis_parts.append(f"driver share {_pct(driver_share)}")

        if severe_downside:
            primary_label = TICKER_VS_THEME_LABELS["severe_downside"]
        elif opposite_direction and abs(diff) >= 3.0:
            primary_label = (
                TICKER_VS_THEME_LABELS["ticker_holding_up"]
                if ticker_1d > 0
                else TICKER_VS_THEME_LABELS["ticker_weak_theme_strong"]
            )
        elif single_name_driver:
            primary_label = TICKER_VS_THEME_LABELS["single_name_driver"]
        elif diff >= 5.0:
            primary_label = TICKER_VS_THEME_LABELS["relative_strength"]
        elif diff <= -5.0:
            primary_label = TICKER_VS_THEME_LABELS["relative_weakness"]
        elif theme_avg <= 0 and diff >= 5.0:
            primary_label = TICKER_VS_THEME_LABELS["positive_divergence"]
        else:
            primary_label = ""

        if not primary_label:
            continue
        rows.append(
            {
                "ticker": row["ticker"],
                "theme": row["theme_label"],
                "standout": primary_label,
                "ticker_1d": round(ticker_1d, 1),
                "theme_avg_1d": round(theme_avg, 1),
                "diff_vs_theme": round(diff, 1),
                "basis": "; ".join(dict.fromkeys(basis_parts)),
                "_opposite_direction": bool(opposite_direction and abs(diff) >= 3.0),
                "_abs_diff": abs(diff),
                "_priority": max(abs(diff), float(driver_share) / 10.0 if pd.notna(driver_share) else 0.0),
            }
        )

    if not rows:
        return empty
    return (
        pd.DataFrame(rows)
        .sort_values(["_opposite_direction", "_abs_diff", "_priority", "theme", "ticker"], ascending=[False, False, False, True, True])
        .drop(columns=["_priority", "_opposite_direction", "_abs_diff"])
        .head(8)
        .reset_index(drop=True)
    )


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
