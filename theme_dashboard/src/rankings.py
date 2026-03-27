from __future__ import annotations

import numpy as np
import pandas as pd

from .config import (
    COMPOSITE_WEIGHTS,
    CURRENT_RANKING_MIN_DOLLAR_VOLUME,
    CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS,
    CURRENT_RANKING_MIN_PRICE,
    CURRENT_RANKING_RETURN_CAP_PCT,
    THEME_CONFIDENCE_FULL_COUNT,
)
from .db_introspection import table_exists, table_has_column
from .queries import latest_ticker_snapshots, preferred_theme_snapshot_source


METRIC_COLUMNS = [
    "ticker_count",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "positive_1w_breadth_pct",
    "positive_1m_breadth_pct",
    "positive_3m_breadth_pct",
    "composite_score",
]

STANDARDIZED_COMPOSITE_WEIGHTS = {
    "perf_1w": 0.30,
    "perf_1m": 0.70,
}
STANDARDIZED_COMPOSITE_3M_NO_PENALTY_PCT = -10.0
STANDARDIZED_COMPOSITE_3M_MODERATE_PENALTY_PCT = -20.0
STANDARDIZED_COMPOSITE_3M_MODERATE_ZONE_FLOOR = 0.65
STANDARDIZED_COMPOSITE_3M_GUARDRAIL_FLOOR = 0.30
STANDARDIZED_COMPOSITE_RECOVERY_BASE_SCORE = 15.0
CURRENT_MOMENTUM_WEIGHTS = {
    "perf_1w": 0.70,
    "perf_1m": 0.30,
}
CURRENT_MOMENTUM_QUALITY_NO_PENALTY_SCORE = 10.0
CURRENT_MOMENTUM_QUALITY_FLOOR_SCORE = 5.0
CURRENT_MOMENTUM_QUALITY_FACTOR_FLOOR = 0.60


CURRENT_RANKING_COLUMNS = [
    "theme_id",
    "theme",
    "category",
    "is_active",
    "run_id",
    "snapshot_time",
    "ticker_count",
    "eligible_ticker_count",
    "eligible_1w_count",
    "eligible_1m_count",
    "eligible_3m_count",
    "eligible_composite_count",
    "eligible_standardized_count",
    "eligible_momentum_count",
    "eligible_breadth_pct",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "positive_1w_breadth_pct",
    "positive_1m_breadth_pct",
    "positive_3m_breadth_pct",
    "composite_score",
    "legacy_composite_score",
    "standardized_base_strength_score",
    "standardized_participation_ratio",
    "standardized_participation_factor",
    "standardized_guardrail_factor",
    "standardized_recovery_factor",
    "standardized_composite_score",
    "current_momentum_raw_score",
    "current_momentum_quality_factor",
    "current_momentum_score",
]


def theme_confidence_factor(ticker_count: int | float) -> float:
    if pd.isna(ticker_count) or float(ticker_count) <= 0:
        return 0.0
    return min(1.0, (float(ticker_count) / float(THEME_CONFIDENCE_FULL_COUNT)) ** 0.5)


def standardized_participation_factor(participation_ratio: int | float) -> float:
    if pd.isna(participation_ratio):
        return 0.5
    return float(np.clip(0.5 + float(participation_ratio), 0.5, 1.0))


def standardized_three_month_guardrail_factor(avg_3m: int | float) -> float:
    if pd.isna(avg_3m):
        return 1.0
    avg_3m_value = float(avg_3m)
    if avg_3m_value >= STANDARDIZED_COMPOSITE_3M_NO_PENALTY_PCT:
        return 1.0
    if avg_3m_value >= STANDARDIZED_COMPOSITE_3M_MODERATE_PENALTY_PCT:
        progress = (STANDARDIZED_COMPOSITE_3M_NO_PENALTY_PCT - avg_3m_value) / (
            STANDARDIZED_COMPOSITE_3M_NO_PENALTY_PCT - STANDARDIZED_COMPOSITE_3M_MODERATE_PENALTY_PCT
        )
        return float(1.0 - progress * (1.0 - STANDARDIZED_COMPOSITE_3M_MODERATE_ZONE_FLOOR))

    progress = (STANDARDIZED_COMPOSITE_3M_MODERATE_PENALTY_PCT - avg_3m_value) / 10.0
    factor = STANDARDIZED_COMPOSITE_3M_MODERATE_ZONE_FLOOR - progress * (
        STANDARDIZED_COMPOSITE_3M_MODERATE_ZONE_FLOOR - STANDARDIZED_COMPOSITE_3M_GUARDRAIL_FLOOR
    )
    return float(max(STANDARDIZED_COMPOSITE_3M_GUARDRAIL_FLOOR, factor))


def standardized_recovery_factor(base_strength_score: int | float, avg_3m: int | float) -> float:
    if pd.isna(avg_3m) or float(avg_3m) >= STANDARDIZED_COMPOSITE_3M_NO_PENALTY_PCT:
        return 1.0
    if pd.isna(base_strength_score):
        return 0.5
    return float(np.clip(float(base_strength_score) / STANDARDIZED_COMPOSITE_RECOVERY_BASE_SCORE, 0.5, 1.0))


def current_momentum_quality_factor(standardized_composite_score: int | float) -> float:
    if pd.isna(standardized_composite_score):
        return CURRENT_MOMENTUM_QUALITY_FACTOR_FLOOR
    score_value = float(standardized_composite_score)
    if score_value >= CURRENT_MOMENTUM_QUALITY_NO_PENALTY_SCORE:
        return 1.0
    if score_value <= CURRENT_MOMENTUM_QUALITY_FLOOR_SCORE:
        return CURRENT_MOMENTUM_QUALITY_FACTOR_FLOOR
    progress = (score_value - CURRENT_MOMENTUM_QUALITY_FLOOR_SCORE) / (
        CURRENT_MOMENTUM_QUALITY_NO_PENALTY_SCORE - CURRENT_MOMENTUM_QUALITY_FLOOR_SCORE
    )
    return float(
        CURRENT_MOMENTUM_QUALITY_FACTOR_FLOOR
        + progress * (1.0 - CURRENT_MOMENTUM_QUALITY_FACTOR_FLOOR)
    )


def ticker_standardized_composite_score(
    perf_1w: int | float,
    perf_1m: int | float,
    perf_3m: int | float,
) -> float:
    if pd.isna(perf_1w) or pd.isna(perf_1m):
        return float("nan")
    base_strength_score = 0.30 * float(perf_1w) + 0.70 * float(perf_1m)
    guardrail_factor = standardized_three_month_guardrail_factor(perf_3m)
    recovery_factor = standardized_recovery_factor(base_strength_score, perf_3m)
    return float(base_strength_score * guardrail_factor * recovery_factor)


def ticker_current_momentum_score(
    perf_1w: int | float,
    perf_1m: int | float,
    perf_3m: int | float,
) -> float:
    if pd.isna(perf_1w) or pd.isna(perf_1m):
        return float("nan")
    standardized_composite_score = ticker_standardized_composite_score(perf_1w, perf_1m, perf_3m)
    momentum_raw_score = (
        CURRENT_MOMENTUM_WEIGHTS["perf_1w"] * float(perf_1w)
        + CURRENT_MOMENTUM_WEIGHTS["perf_1m"] * float(perf_1m)
    )
    quality_factor = current_momentum_quality_factor(standardized_composite_score)
    return float(momentum_raw_score * quality_factor)


def _compute_theme_metrics(raw: pd.DataFrame) -> pd.DataFrame:
    prepared = raw.copy()
    if "calculation_eligible" not in prepared.columns:
        prepared["calculation_eligible"] = True
    prepared["calculation_eligible"] = prepared["calculation_eligible"].fillna(False).astype(bool)
    for perf_col in ("perf_1w", "perf_1m", "perf_3m"):
        prepared[f"{perf_col}_for_calc"] = prepared[perf_col].where(prepared["calculation_eligible"])

    grouped = prepared.groupby(["theme_id", "theme", "category", "is_active"], dropna=False)
    out = grouped.agg(
        ticker_count=("calculation_eligible", "sum"),
        avg_1w=("perf_1w_for_calc", "mean"),
        avg_1m=("perf_1m_for_calc", "mean"),
        avg_3m=("perf_3m_for_calc", "mean"),
        positive_1w_breadth_pct=("perf_1w_for_calc", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
        positive_1m_breadth_pct=("perf_1m_for_calc", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
        positive_3m_breadth_pct=("perf_3m_for_calc", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
    ).reset_index()

    base_score = (
        COMPOSITE_WEIGHTS["perf_1w"] * out["avg_1w"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_1m"] * out["avg_1m"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_3m"] * out["avg_3m"].fillna(0)
    )
    out["composite_score"] = base_score * out["ticker_count"].apply(theme_confidence_factor)

    out[METRIC_COLUMNS[1:]] = out[METRIC_COLUMNS[1:]].round(2)
    return out


def compute_theme_metrics_for_run(conn, run_id: int) -> pd.DataFrame:
    status_join = "LEFT JOIN symbol_refresh_status sr ON sr.ticker = m.ticker" if table_exists(conn, "symbol_refresh_status") else ""
    calculation_eligible_expr = "COALESCE(sr.status, 'active') <> 'refresh_suppressed'" if status_join else "TRUE"
    raw = conn.execute(
        f"""
        SELECT t.id AS theme_id, t.name AS theme, t.category, t.is_active,
               m.ticker, s.perf_1w, s.perf_1m, s.perf_3m,
               {calculation_eligible_expr} AS calculation_eligible
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        LEFT JOIN ticker_snapshots s ON s.ticker = m.ticker AND s.run_id = ?
        {status_join}
        """,
        [run_id],
    ).df()

    if raw.empty:
        return pd.DataFrame(columns=["theme_id", "theme", "category", "is_active", *METRIC_COLUMNS])
    return _compute_theme_metrics(raw)


def persist_theme_snapshot_for_run(conn, run_id: int) -> None:
    metrics = compute_theme_metrics_for_run(conn, run_id)
    if metrics.empty:
        return

    metrics = metrics.copy()
    run_meta = conn.execute("SELECT finished_at, provider FROM refresh_runs WHERE run_id = ?", [run_id]).fetchone()
    snapshot_time = run_meta[0] if run_meta else None
    source = (run_meta[1] if run_meta and run_meta[1] in {"live", "mock", "synthetic_backfill"} else "live")
    metrics["run_id"] = run_id
    metrics["snapshot_time"] = snapshot_time
    metrics["snapshot_source"] = source

    conn.register("theme_snapshot_incoming", metrics)
    conn.execute(
        """
        INSERT OR REPLACE INTO theme_snapshots(
            run_id, snapshot_time, theme_id, ticker_count,
            avg_1w, avg_1m, avg_3m,
            positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
            composite_score, snapshot_source
        )
        SELECT run_id, snapshot_time, theme_id, ticker_count,
               avg_1w, avg_1m, avg_3m,
               positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
               composite_score, snapshot_source
        FROM theme_snapshot_incoming
        """
    )
    conn.unregister("theme_snapshot_incoming")


def _safe_numeric(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.where(np.isfinite(values), np.nan)


def _load_current_ranking_constituents(conn) -> pd.DataFrame:
    membership = conn.execute(
        """
        SELECT
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            m.ticker
        FROM themes t
        LEFT JOIN theme_membership m ON m.theme_id = t.id
        """
    ).df()
    if membership.empty:
        return membership

    latest = latest_ticker_snapshots(conn)
    if latest.empty:
        for col in ("run_id", "snapshot_time", "price", "avg_volume", "perf_1w", "perf_1m", "perf_3m"):
            membership[col] = np.nan
        membership["status"] = None
        return membership

    latest = latest.copy()
    for col in ("price", "avg_volume", "perf_1w", "perf_1m", "perf_3m"):
        if col not in latest.columns:
            latest[col] = np.nan

    if table_exists(conn, "symbol_refresh_status"):
        status_cols = ["ticker"]
        if table_has_column(conn, "symbol_refresh_status", "status"):
            status_cols.append("status")
        statuses = conn.execute(f"SELECT {', '.join(status_cols)} FROM symbol_refresh_status").df()
        if "status" not in statuses.columns:
            statuses["status"] = None
    else:
        statuses = pd.DataFrame(columns=["ticker", "status"])

    raw = membership.merge(latest, on="ticker", how="left")
    raw = raw.merge(statuses[["ticker", "status"]], on="ticker", how="left", suffixes=("", "_symbol"))
    return raw


def _build_current_ranking_metrics(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=CURRENT_RANKING_COLUMNS)

    prepared = raw.copy()
    for col in ("price", "avg_volume", "perf_1w", "perf_1m", "perf_3m"):
        prepared[col] = _safe_numeric(prepared.get(col))

    prepared["run_id"] = _safe_numeric(prepared.get("run_id"))
    prepared["snapshot_time"] = pd.to_datetime(prepared.get("snapshot_time"), errors="coerce")
    prepared["snapshot_present"] = prepared["run_id"].notna() & prepared["snapshot_time"].notna()
    prepared["price_valid"] = prepared["price"].notna() & (prepared["price"] >= CURRENT_RANKING_MIN_PRICE)
    prepared["avg_volume_valid"] = prepared["avg_volume"].notna() & (prepared["avg_volume"] > 0)
    prepared["dollar_volume"] = prepared["price"] * prepared["avg_volume"]
    prepared["dollar_volume_valid"] = prepared["dollar_volume"].notna() & (
        prepared["dollar_volume"] >= CURRENT_RANKING_MIN_DOLLAR_VOLUME
    )
    prepared["not_refresh_suppressed"] = prepared.get("status", pd.Series(index=prepared.index)).fillna("active") != "refresh_suppressed"
    prepared["eligible_ticker"] = (
        prepared["snapshot_present"]
        & prepared["price_valid"]
        & prepared["avg_volume_valid"]
        & prepared["dollar_volume_valid"]
        & prepared["not_refresh_suppressed"]
    )

    capped_return_cols: dict[str, str] = {}
    for perf_col in ("perf_1w", "perf_1m", "perf_3m"):
        eligible_col = f"{perf_col}_eligible"
        capped_col = f"{perf_col}_capped"
        prepared[eligible_col] = prepared["eligible_ticker"] & prepared[perf_col].notna()
        prepared[capped_col] = prepared[perf_col].clip(
            lower=-CURRENT_RANKING_RETURN_CAP_PCT,
            upper=CURRENT_RANKING_RETURN_CAP_PCT,
        )
        capped_return_cols[perf_col] = capped_col

    prepared["composite_metric_eligible"] = (
        prepared["perf_1w_eligible"] & prepared["perf_1m_eligible"] & prepared["perf_3m_eligible"]
    )
    prepared["standardized_metric_eligible"] = prepared["perf_1w_eligible"] & prepared["perf_1m_eligible"]

    prepared["ticker_present"] = prepared["ticker"].notna().astype(int)
    prepared["perf_1w_capped_for_agg"] = prepared[capped_return_cols["perf_1w"]].where(prepared["perf_1w_eligible"])
    prepared["perf_1m_capped_for_agg"] = prepared[capped_return_cols["perf_1m"]].where(prepared["perf_1m_eligible"])
    prepared["perf_3m_capped_for_agg"] = prepared[capped_return_cols["perf_3m"]].where(prepared["perf_3m_eligible"])
    prepared["perf_1w_positive"] = np.where(prepared["perf_1w_eligible"], prepared["perf_1w"] > 0, np.nan)
    prepared["perf_1m_positive"] = np.where(prepared["perf_1m_eligible"], prepared["perf_1m"] > 0, np.nan)
    prepared["perf_3m_positive"] = np.where(prepared["perf_3m_eligible"], prepared["perf_3m"] > 0, np.nan)

    grouped = prepared.groupby(["theme_id", "theme", "category", "is_active"], dropna=False)
    out = grouped.agg(
        run_id=("run_id", "max"),
        snapshot_time=("snapshot_time", "max"),
        ticker_count=("ticker_present", "sum"),
        eligible_ticker_count=("eligible_ticker", "sum"),
        eligible_1w_count=("perf_1w_eligible", "sum"),
        eligible_1m_count=("perf_1m_eligible", "sum"),
        eligible_3m_count=("perf_3m_eligible", "sum"),
        eligible_composite_count=("composite_metric_eligible", "sum"),
        eligible_standardized_count=("standardized_metric_eligible", "sum"),
        eligible_momentum_count=("standardized_metric_eligible", "sum"),
        avg_1w=("perf_1w_capped_for_agg", "mean"),
        avg_1m=("perf_1m_capped_for_agg", "mean"),
        avg_3m=("perf_3m_capped_for_agg", "mean"),
        positive_1w_breadth_pct=("perf_1w_positive", "mean"),
        positive_1m_breadth_pct=("perf_1m_positive", "mean"),
        positive_3m_breadth_pct=("perf_3m_positive", "mean"),
    ).reset_index()
    if out.empty:
        return out

    count_cols = [
        "ticker_count",
        "eligible_ticker_count",
        "eligible_1w_count",
        "eligible_1m_count",
        "eligible_3m_count",
        "eligible_composite_count",
        "eligible_standardized_count",
        "eligible_momentum_count",
    ]
    out[count_cols] = out[count_cols].fillna(0).astype(int)
    out["run_id"] = pd.to_numeric(out["run_id"], errors="coerce")
    out["run_id"] = out["run_id"].where(out["run_id"].notna(), None)
    out["positive_1w_breadth_pct"] = out["positive_1w_breadth_pct"].fillna(0.0) * 100.0
    out["positive_1m_breadth_pct"] = out["positive_1m_breadth_pct"].fillna(0.0) * 100.0
    out["positive_3m_breadth_pct"] = out["positive_3m_breadth_pct"].fillna(0.0) * 100.0
    out["eligible_breadth_pct"] = np.where(
        out["ticker_count"] > 0,
        (out["eligible_ticker_count"] / out["ticker_count"]) * 100.0,
        0.0,
    )
    base_score = (
        COMPOSITE_WEIGHTS["perf_1w"] * out["avg_1w"].fillna(0.0)
        + COMPOSITE_WEIGHTS["perf_1m"] * out["avg_1m"].fillna(0.0)
        + COMPOSITE_WEIGHTS["perf_3m"] * out["avg_3m"].fillna(0.0)
    )
    out["composite_score"] = np.where(
        out["eligible_composite_count"] > 0,
        base_score * out["ticker_count"].apply(theme_confidence_factor),
        np.nan,
    )
    out["legacy_composite_score"] = out["composite_score"]
    out["standardized_base_strength_score"] = (
        STANDARDIZED_COMPOSITE_WEIGHTS["perf_1w"] * out["avg_1w"].fillna(0.0)
        + STANDARDIZED_COMPOSITE_WEIGHTS["perf_1m"] * out["avg_1m"].fillna(0.0)
    )
    out["standardized_participation_ratio"] = np.where(
        out["ticker_count"] > 0,
        out["eligible_standardized_count"] / out["ticker_count"],
        0.0,
    )
    out["standardized_participation_factor"] = out["standardized_participation_ratio"].apply(standardized_participation_factor)
    out["standardized_guardrail_factor"] = out["avg_3m"].apply(standardized_three_month_guardrail_factor)
    out["standardized_recovery_factor"] = [
        standardized_recovery_factor(base_score, avg_3m)
        for base_score, avg_3m in zip(out["standardized_base_strength_score"], out["avg_3m"])
    ]
    out["standardized_composite_score"] = np.where(
        out["eligible_standardized_count"] > 0,
        out["standardized_base_strength_score"]
        * out["standardized_participation_factor"]
        * out["standardized_guardrail_factor"]
        * out["standardized_recovery_factor"],
        np.nan,
    )
    out["current_momentum_raw_score"] = (
        CURRENT_MOMENTUM_WEIGHTS["perf_1w"] * out["avg_1w"].fillna(0.0)
        + CURRENT_MOMENTUM_WEIGHTS["perf_1m"] * out["avg_1m"].fillna(0.0)
    )
    out["current_momentum_quality_factor"] = out["standardized_composite_score"].apply(current_momentum_quality_factor)
    out["current_momentum_score"] = np.where(
        out["eligible_momentum_count"] > 0,
        out["current_momentum_raw_score"] * out["current_momentum_quality_factor"],
        np.nan,
    )
    numeric_cols = [
        "eligible_breadth_pct",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "positive_1w_breadth_pct",
        "positive_1m_breadth_pct",
        "positive_3m_breadth_pct",
        "composite_score",
        "legacy_composite_score",
        "standardized_base_strength_score",
        "standardized_participation_ratio",
        "standardized_participation_factor",
        "standardized_guardrail_factor",
        "standardized_recovery_factor",
        "standardized_composite_score",
        "current_momentum_raw_score",
        "current_momentum_quality_factor",
        "current_momentum_score",
    ]
    out[numeric_cols] = out[numeric_cols].round(2)
    return out[CURRENT_RANKING_COLUMNS]


def _finalize_current_rankings(
    current: pd.DataFrame,
    *,
    score_col: str = "composite_score",
    eligible_count_col: str = "eligible_composite_count",
) -> pd.DataFrame:
    rankings = current[current[eligible_count_col] >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS].copy()
    rankings = rankings.sort_values(
        [score_col, "positive_1m_breadth_pct", eligible_count_col, "theme"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    return rankings


def _finalize_current_window_rankings(current: pd.DataFrame, perf_col: str) -> pd.DataFrame:
    eligible_count_col = {
        "avg_1w": "eligible_1w_count",
        "avg_1m": "eligible_1m_count",
        "avg_3m": "eligible_3m_count",
    }.get(perf_col)
    if not eligible_count_col:
        raise ValueError(f"Unsupported current performance column: {perf_col}")

    rankings = current.copy()
    if "is_active" in rankings.columns:
        rankings = rankings[rankings["is_active"] == True].copy()
    rankings = rankings[rankings[eligible_count_col] >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS].copy()
    rankings = rankings.sort_values(
        [perf_col, "composite_score", eligible_count_col, "eligible_breadth_pct", "theme"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    return rankings


def _build_standardized_composite_validation(legacy_rankings: pd.DataFrame, standardized_rankings: pd.DataFrame) -> pd.DataFrame:
    legacy = legacy_rankings[["theme_id", "theme", "category", "legacy_composite_score", "eligible_composite_count"]].copy()
    legacy["legacy_rank"] = range(1, len(legacy) + 1)

    standardized = standardized_rankings[
        [
            "theme_id",
            "theme",
            "category",
            "standardized_composite_score",
            "eligible_standardized_count",
            "standardized_participation_ratio",
            "standardized_participation_factor",
            "standardized_guardrail_factor",
            "standardized_recovery_factor",
            "avg_1w",
            "avg_1m",
            "avg_3m",
        ]
    ].copy()
    standardized["standardized_rank"] = range(1, len(standardized) + 1)

    comparison = legacy.merge(standardized, on="theme_id", how="outer", suffixes=("_legacy", "_standardized"))
    comparison["theme"] = comparison["theme_standardized"].where(comparison["theme_standardized"].notna(), comparison["theme_legacy"])
    comparison["category"] = comparison["category_standardized"].where(comparison["category_standardized"].notna(), comparison["category_legacy"])
    comparison["rank_shift_vs_legacy"] = comparison["legacy_rank"] - comparison["standardized_rank"]
    comparison["entered_standardized_view"] = comparison["legacy_rank"].isna() & comparison["standardized_rank"].notna()
    comparison["dropped_from_standardized_view"] = comparison["legacy_rank"].notna() & comparison["standardized_rank"].isna()
    comparison = comparison.drop(columns=["theme_legacy", "theme_standardized", "category_legacy", "category_standardized"])
    return comparison.sort_values(
        ["rank_shift_vs_legacy", "standardized_rank", "legacy_rank", "theme"],
        ascending=[False, True, True, True],
        na_position="last",
    ).reset_index(drop=True)


def _build_current_momentum_validation(
    current_1w_rankings: pd.DataFrame,
    current_momentum_rankings: pd.DataFrame,
    standardized_rankings: pd.DataFrame,
) -> pd.DataFrame:
    current_1w = current_1w_rankings[
        ["theme_id", "theme", "category", "avg_1w", "avg_1m", "standardized_composite_score"]
    ].copy()
    current_1w["current_1w_rank"] = range(1, len(current_1w) + 1)

    current_momentum = current_momentum_rankings[
        [
            "theme_id",
            "theme",
            "category",
            "current_momentum_raw_score",
            "current_momentum_quality_factor",
            "current_momentum_score",
            "standardized_composite_score",
            "avg_1w",
            "avg_1m",
        ]
    ].copy()
    current_momentum["current_momentum_rank"] = range(1, len(current_momentum) + 1)

    standardized = standardized_rankings[["theme_id"]].copy()
    standardized["standardized_rank"] = range(1, len(standardized) + 1)

    comparison = current_1w.merge(
        current_momentum,
        on="theme_id",
        how="outer",
        suffixes=("_1w", "_momentum"),
    ).merge(standardized, on="theme_id", how="left")
    comparison["theme"] = comparison["theme_momentum"].where(comparison["theme_momentum"].notna(), comparison["theme_1w"])
    comparison["category"] = comparison["category_momentum"].where(comparison["category_momentum"].notna(), comparison["category_1w"])
    comparison["avg_1w"] = comparison["avg_1w_momentum"].where(comparison["avg_1w_momentum"].notna(), comparison["avg_1w_1w"])
    comparison["avg_1m"] = comparison["avg_1m_momentum"].where(comparison["avg_1m_momentum"].notna(), comparison["avg_1m_1w"])
    comparison["standardized_composite_score"] = comparison["standardized_composite_score_momentum"].where(
        comparison["standardized_composite_score_momentum"].notna(),
        comparison["standardized_composite_score_1w"],
    )
    comparison["rank_shift_vs_1w"] = comparison["current_1w_rank"] - comparison["current_momentum_rank"]
    comparison["entered_momentum_view"] = comparison["current_1w_rank"].isna() & comparison["current_momentum_rank"].notna()
    comparison["dropped_from_momentum_view"] = comparison["current_1w_rank"].notna() & comparison["current_momentum_rank"].isna()
    return comparison[
        [
            "theme_id",
            "theme",
            "category",
            "current_1w_rank",
            "current_momentum_rank",
            "standardized_rank",
            "rank_shift_vs_1w",
            "avg_1w",
            "avg_1m",
            "current_momentum_raw_score",
            "current_momentum_quality_factor",
            "current_momentum_score",
            "standardized_composite_score",
            "entered_momentum_view",
            "dropped_from_momentum_view",
        ]
    ].sort_values(
        ["rank_shift_vs_1w", "current_momentum_rank", "current_1w_rank", "theme"],
        ascending=[False, True, True, True],
        na_position="last",
    ).reset_index(drop=True)


def compute_current_ranking_snapshot(conn) -> dict[str, pd.DataFrame]:
    # Current trust surfaces all derive from one prepared latest-snapshot view so
    # contributor eligibility and capped-return semantics stay consistent.
    current = _build_current_ranking_metrics(_load_current_ranking_constituents(conn))
    if current.empty:
        return {
            "theme_metrics": pd.DataFrame(columns=CURRENT_RANKING_COLUMNS),
            "rankings": pd.DataFrame(),
            "standardized_rankings": pd.DataFrame(),
            "standardized_comparison": pd.DataFrame(),
            "current_momentum_rankings": pd.DataFrame(),
            "current_momentum_comparison": pd.DataFrame(),
        }

    legacy_rankings = _finalize_current_rankings(current, score_col="legacy_composite_score", eligible_count_col="eligible_composite_count")
    standardized_rankings = _finalize_current_rankings(
        current,
        score_col="standardized_composite_score",
        eligible_count_col="eligible_standardized_count",
    )
    current_1w_rankings = _finalize_current_window_rankings(current, "avg_1w")
    current_momentum_rankings = _finalize_current_rankings(
        current,
        score_col="current_momentum_score",
        eligible_count_col="eligible_momentum_count",
    )
    standardized_comparison = _build_standardized_composite_validation(legacy_rankings, standardized_rankings)
    current_momentum_comparison = _build_current_momentum_validation(
        current_1w_rankings,
        current_momentum_rankings,
        standardized_rankings,
    )

    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        rankings = legacy_rankings.copy()
        for col in (
            "delta_avg_1w",
            "delta_avg_1m",
            "delta_avg_3m",
            "delta_positive_1m_breadth_pct",
            "delta_composite_score",
        ):
            rankings[col] = np.nan
        return {
            "theme_metrics": current,
            "rankings": rankings,
            "standardized_rankings": standardized_rankings,
            "standardized_comparison": standardized_comparison,
            "current_momentum_rankings": current_momentum_rankings,
            "current_momentum_comparison": current_momentum_comparison,
        }

    prior = conn.execute(
        """
        WITH ranked AS (
            SELECT
                ts.theme_id,
                ts.avg_1w,
                ts.avg_1m,
                ts.avg_3m,
                ts.positive_1m_breadth_pct,
                ts.composite_score,
                ROW_NUMBER() OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id DESC) AS rn
            FROM theme_snapshots ts
            WHERE ts.snapshot_source = ?
        )
        SELECT
            theme_id,
            avg_1w AS prev_avg_1w,
            avg_1m AS prev_avg_1m,
            avg_3m AS prev_avg_3m,
            positive_1m_breadth_pct AS prev_positive_1m_breadth_pct,
            composite_score AS prev_composite_score
        FROM ranked
        WHERE rn = 2
        """,
        [preferred_source],
    ).df()

    rankings = legacy_rankings.merge(prior, on="theme_id", how="left")
    rankings["delta_avg_1w"] = (rankings["avg_1w"] - rankings["prev_avg_1w"]).round(2)
    rankings["delta_avg_1m"] = (rankings["avg_1m"] - rankings["prev_avg_1m"]).round(2)
    rankings["delta_avg_3m"] = (rankings["avg_3m"] - rankings["prev_avg_3m"]).round(2)
    rankings["delta_positive_1m_breadth_pct"] = (
        rankings["positive_1m_breadth_pct"] - rankings["prev_positive_1m_breadth_pct"]
    ).round(2)
    rankings["delta_composite_score"] = (rankings["legacy_composite_score"] - rankings["prev_composite_score"]).round(2)
    return {
        "theme_metrics": current,
        "rankings": rankings,
        "standardized_rankings": standardized_rankings,
        "standardized_comparison": standardized_comparison,
        "current_momentum_rankings": current_momentum_rankings,
        "current_momentum_comparison": current_momentum_comparison,
    }


def compute_theme_rankings(conn) -> pd.DataFrame:
    return compute_current_ranking_snapshot(conn)["rankings"]
