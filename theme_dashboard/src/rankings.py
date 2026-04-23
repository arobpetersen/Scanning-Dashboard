from __future__ import annotations

from datetime import date, datetime, time

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
from .queries import latest_ticker_history_atr_companion_fields, latest_ticker_snapshots, preferred_theme_snapshot_source


METRIC_COLUMNS = [
    "ticker_count",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "avg_6m",
    "positive_1w_breadth_pct",
    "positive_1m_breadth_pct",
    "positive_3m_breadth_pct",
    "composite_score",
]

STANDARDIZED_COMPOSITE_WEIGHTS = {
    "perf_1w": 0.20,
    "perf_1m": 0.55,
    "perf_3m": 0.25,
}
COMPOSITE_ATR_BASE_WEIGHTS = {
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
    "eligible_6m_count",
    "eligible_composite_count",
    "eligible_standardized_count",
    "eligible_momentum_count",
    "eligible_breadth_pct",
    "avg_1d",
    "avg_1w",
    "avg_1m",
    "avg_1w_atr_units",
    "avg_1m_atr_units",
    "avg_3m",
    "avg_6m",
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
    "eligible_atr_count",
    "composite_atr_base_strength_score",
    "composite_atr_participation_ratio",
    "composite_atr_participation_factor",
    "composite_atr_guardrail_factor",
    "composite_atr_recovery_factor",
    "composite_atr_score",
    "composite_atr_rank",
    "current_momentum_raw_score",
    "current_momentum_quality_factor",
    "current_momentum_score",
]


CANONICAL_THEME_DAILY_COLUMNS = [
    "snapshot_date",
    "snapshot_time",
    "run_id",
    "theme_id",
    "theme",
    "category",
    "is_active",
    "snapshot_source",
    "extract_session",
    "is_canonical_daily",
    "canonical_reason",
    "ticker_count",
    "eligible_ticker_count",
    "eligible_1w_count",
    "eligible_1m_count",
    "eligible_3m_count",
    "eligible_6m_count",
    "eligible_composite_count",
    "eligible_standardized_count",
    "eligible_momentum_count",
    "eligible_breadth_pct",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "avg_6m",
    "positive_1w_breadth_pct",
    "positive_1m_breadth_pct",
    "positive_3m_breadth_pct",
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
    "canonical_rank",
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


def standardized_base_strength_score(
    perf_1w: int | float,
    perf_1m: int | float,
    perf_3m: int | float,
) -> float:
    if pd.isna(perf_1w) or pd.isna(perf_1m):
        return float("nan")
    perf_3m_value = 0.0 if pd.isna(perf_3m) else float(perf_3m)
    return float(
        STANDARDIZED_COMPOSITE_WEIGHTS["perf_1w"] * float(perf_1w)
        + STANDARDIZED_COMPOSITE_WEIGHTS["perf_1m"] * float(perf_1m)
        + STANDARDIZED_COMPOSITE_WEIGHTS["perf_3m"] * perf_3m_value
    )


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


def current_ticker_is_eligible(
    price: int | float,
    avg_volume: int | float,
    status: object = "active",
    *,
    snapshot_present: bool = True,
) -> bool:
    if not snapshot_present:
        return False
    if pd.isna(price) or float(price) < CURRENT_RANKING_MIN_PRICE:
        return False
    if pd.isna(avg_volume) or float(avg_volume) <= 0:
        return False
    if float(price) * float(avg_volume) < CURRENT_RANKING_MIN_DOLLAR_VOLUME:
        return False
    return str(status or "active") != "refresh_suppressed"


def visible_ticker_suppressed(status: object = "active", manual_suppressed: bool = False) -> bool:
    return bool(manual_suppressed) or str(status or "active") == "refresh_suppressed"


def current_ticker_coverage_status(
    *,
    governed_membership: bool,
    suppressed: bool,
    eligible: bool,
    has_current_usable_snapshot: bool,
) -> str:
    if not governed_membership:
        return "not governed"
    if suppressed:
        return "suppressed"
    if eligible:
        return "healthy current coverage"
    if not has_current_usable_snapshot:
        return "needs refresh check"
    return "current but ineligible"


def ticker_standardized_composite_score(
    perf_1w: int | float,
    perf_1m: int | float,
    perf_3m: int | float,
) -> float:
    base_strength_score = standardized_base_strength_score(perf_1w, perf_1m, perf_3m)
    if pd.isna(base_strength_score):
        return float("nan")
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
    for perf_col in ("perf_1w", "perf_1m", "perf_3m", "perf_6m"):
        prepared[f"{perf_col}_for_calc"] = prepared[perf_col].where(prepared["calculation_eligible"])

    grouped = prepared.groupby(["theme_id", "theme", "category", "is_active"], dropna=False)
    out = grouped.agg(
        ticker_count=("calculation_eligible", "sum"),
        avg_1w=("perf_1w_for_calc", "mean"),
        avg_1m=("perf_1m_for_calc", "mean"),
        avg_3m=("perf_3m_for_calc", "mean"),
        avg_6m=("perf_6m_for_calc", "mean"),
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
               m.ticker, s.perf_1w, s.perf_1m, s.perf_3m, s.perf_6m,
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
            avg_1w, avg_1m, avg_3m, avg_6m,
            positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
            composite_score, snapshot_source
        )
        SELECT run_id, snapshot_time, theme_id, ticker_count,
               avg_1w, avg_1m, avg_3m, avg_6m,
               positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
               composite_score, snapshot_source
        FROM theme_snapshot_incoming
        """
    )
    conn.unregister("theme_snapshot_incoming")


def backfill_ticker_snapshot_perf_6m_from_history(conn) -> int:
    if not table_exists(conn, "ticker_daily_history") or not table_exists(conn, "ticker_snapshots"):
        return 0
    updated_rows = conn.execute(
        """
        WITH deduped_history AS (
            SELECT
                upper(trim(ticker)) AS ticker,
                trading_date,
                market_data_source,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY upper(trim(ticker)), trading_date, market_data_source
                    ORDER BY updated_at DESC, created_at DESC, close DESC
                ) AS row_rank
            FROM ticker_daily_history
        ),
        perf_history AS (
            SELECT
                ticker,
                trading_date,
                market_data_source,
                ((close / LAG(close, 126) OVER (PARTITION BY ticker, market_data_source ORDER BY trading_date)) - 1.0) * 100.0 AS perf_6m
            FROM deduped_history
            WHERE row_rank = 1
        ),
        joined AS (
            SELECT
                ts.snapshot_id,
                ph.perf_6m
            FROM ticker_snapshots ts
            JOIN refresh_runs rr ON rr.run_id = ts.run_id
            JOIN perf_history ph
              ON ph.ticker = upper(trim(ts.ticker))
             AND ph.trading_date = CAST(rr.finished_at AS DATE)
             AND ph.market_data_source = COALESCE(NULLIF(trim(ts.snapshot_source), ''), COALESCE(rr.provider, 'live'))
            WHERE ts.perf_6m IS NULL
              AND rr.finished_at IS NOT NULL
        )
        UPDATE ticker_snapshots AS ts
        SET perf_6m = joined.perf_6m
        FROM joined
        WHERE ts.snapshot_id = joined.snapshot_id
          AND joined.perf_6m IS NOT NULL
        RETURNING ts.snapshot_id
        """
    ).fetchall()
    return int(len(updated_rows))


def backfill_theme_snapshot_avg_6m(conn) -> int:
    if not table_exists(conn, "theme_snapshots"):
        return 0
    run_rows = conn.execute(
        """
        SELECT DISTINCT run_id
        FROM theme_snapshots
        WHERE avg_6m IS NULL
        ORDER BY run_id
        """
    ).fetchall()
    updated_runs = 0
    for (run_id,) in run_rows:
        if run_id is None:
            continue
        metrics = compute_theme_metrics_for_run(conn, int(run_id))
        if metrics.empty or "avg_6m" not in metrics.columns:
            continue
        persist_theme_snapshot_for_run(conn, int(run_id))
        updated_runs += 1
    return updated_runs


def backfill_canonical_theme_daily_avg_6m(
    conn,
    *,
    recent_trading_day_limit: int = 30,
    provider: str = "live",
) -> dict[str, object]:
    if not table_exists(conn, "canonical_theme_daily_snapshots"):
        return {"status": "missing_table", "rows_updated": 0, "dates_updated": 0}

    target_dates = conn.execute(
        """
        WITH recent_dates AS (
            SELECT DISTINCT snapshot_date
            FROM canonical_theme_daily_snapshots
            ORDER BY snapshot_date DESC
            LIMIT ?
        )
        SELECT snapshot_date
        FROM recent_dates
        ORDER BY snapshot_date ASC
        """,
        [max(int(recent_trading_day_limit), 1)],
    ).df()
    if target_dates.empty:
        return {"status": "no_scope", "rows_updated": 0, "dates_updated": 0}

    rows_updated = 0
    dates_updated = 0
    for snapshot_date in target_dates["snapshot_date"].tolist():
        existing = conn.execute(
            """
            SELECT run_id
            FROM canonical_theme_daily_snapshots
            WHERE snapshot_date = ?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            [snapshot_date],
        ).fetchone()
        if existing is None:
            continue

        run_id = int(existing[0] or 0)
        if run_id > 0:
            canonical_rows = build_canonical_theme_daily_rows_for_run(
                conn,
                run_id,
                extract_session="backfill_existing_6m",
                canonical_reason="recent_trading_day_backfill",
                is_canonical_daily=True,
            )
        else:
            canonical_rows = build_canonical_theme_daily_rows_for_trading_date(
                conn,
                snapshot_date,
                market_data_source=provider,
                extract_session="backfill_existing_6m",
                canonical_reason="missing_full_theme_run_history_repair",
                is_canonical_daily=True,
            )
        if canonical_rows.empty:
            continue

        update_df = _deduplicate_canonical_rows(canonical_rows)[["snapshot_date", "theme_id", "eligible_6m_count", "avg_6m"]].copy()
        conn.register("canonical_theme_daily_avg_6m_updates", update_df)
        conn.execute(
            """
            UPDATE canonical_theme_daily_snapshots AS target
            SET eligible_6m_count = source.eligible_6m_count,
                avg_6m = source.avg_6m
            FROM canonical_theme_daily_avg_6m_updates AS source
            WHERE target.snapshot_date = source.snapshot_date
              AND target.theme_id = source.theme_id
            """
        )
        conn.unregister("canonical_theme_daily_avg_6m_updates")
        if not update_df.empty:
            dates_updated += 1
            rows_updated += len(update_df)

    return {
        "status": "success" if rows_updated else "no_op",
        "rows_updated": int(rows_updated),
        "dates_updated": int(dates_updated),
        "recent_trading_day_limit": int(recent_trading_day_limit),
    }


def _load_ranking_constituents_for_run(conn, run_id: int) -> pd.DataFrame:
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

    snapshots = conn.execute(
        """
        SELECT
            s.run_id,
            rr.finished_at AS snapshot_time,
            s.ticker,
            s.price,
            s.avg_volume,
            s.perf_1w,
            s.perf_1m,
            s.perf_3m,
            s.perf_6m
        FROM ticker_snapshots s
        JOIN refresh_runs rr ON rr.run_id = s.run_id
        WHERE s.run_id = ?
        """,
        [run_id],
    ).df()
    if snapshots.empty:
        for col in ("run_id", "snapshot_time", "price", "avg_volume", "perf_1w", "perf_1m", "perf_3m", "perf_6m"):
            membership[col] = np.nan
        membership["status"] = None
        return membership

    if table_exists(conn, "symbol_refresh_status"):
        status_cols = ["ticker"]
        if table_has_column(conn, "symbol_refresh_status", "status"):
            status_cols.append("status")
        statuses = conn.execute(f"SELECT {', '.join(status_cols)} FROM symbol_refresh_status").df()
        if "status" not in statuses.columns:
            statuses["status"] = None
    else:
        statuses = pd.DataFrame(columns=["ticker", "status"])

    raw = membership.merge(snapshots, on="ticker", how="left")
    raw = raw.merge(statuses[["ticker", "status"]], on="ticker", how="left", suffixes=("", "_symbol"))
    return raw


def compute_current_ranking_metrics_for_run(conn, run_id: int) -> pd.DataFrame:
    return _build_current_ranking_metrics(_load_ranking_constituents_for_run(conn, run_id))


def compute_current_ranking_snapshot_for_run(conn, run_id: int) -> dict[str, pd.DataFrame]:
    current = compute_current_ranking_metrics_for_run(conn, run_id)
    if current.empty:
        return _empty_current_ranking_snapshot(include_validation=True)

    legacy_rankings = _finalize_current_rankings(
        current,
        score_col="legacy_composite_score",
        eligible_count_col="eligible_composite_count",
    )
    standardized_rankings = _finalize_current_rankings(
        current,
        score_col="standardized_composite_score",
        eligible_count_col="eligible_standardized_count",
    )
    atr_rankings = _finalize_current_rankings(
        current,
        score_col="composite_atr_score",
        eligible_count_col="eligible_atr_count",
    )
    if not atr_rankings.empty:
        atr_rankings = atr_rankings.copy()
        atr_rankings["composite_atr_rank"] = range(1, len(atr_rankings) + 1)
        atr_rank_lookup = atr_rankings.set_index("theme_id")["composite_atr_rank"].to_dict()
        current = current.copy()
        current["composite_atr_rank"] = current["theme_id"].map(atr_rank_lookup)
        standardized_rankings = standardized_rankings.copy()
        standardized_rankings["composite_atr_rank"] = standardized_rankings["theme_id"].map(atr_rank_lookup)
        legacy_rankings = legacy_rankings.copy()
        legacy_rankings["composite_atr_rank"] = legacy_rankings["theme_id"].map(atr_rank_lookup)

    validation_snapshot = _build_current_ranking_validation_snapshot_from_base(
        current,
        legacy_rankings,
        standardized_rankings,
    )
    return {
        "theme_metrics": current,
        "rankings": legacy_rankings,
        "standardized_rankings": standardized_rankings,
        **validation_snapshot,
    }


def _load_ranking_constituents_for_trading_date(
    conn,
    snapshot_date: date | str,
    *,
    market_data_source: str = "live",
) -> pd.DataFrame:
    snapshot_ts = pd.Timestamp(snapshot_date)
    if pd.isna(snapshot_ts):
        return pd.DataFrame()

    membership = conn.execute(
        """
        SELECT
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            upper(trim(m.ticker)) AS ticker
        FROM themes t
        LEFT JOIN theme_membership m ON m.theme_id = t.id
        """
    ).df()
    if membership.empty:
        return membership

    history = conn.execute(
        """
        WITH deduped_history AS (
            SELECT
                upper(trim(ticker)) AS ticker,
                trading_date,
                close AS price,
                volume,
                atr_14,
                atr_pct_14,
                ROW_NUMBER() OVER (
                    PARTITION BY upper(trim(ticker)), trading_date, market_data_source
                    ORDER BY updated_at DESC, created_at DESC, close DESC
                ) AS row_rank
            FROM ticker_daily_history
            WHERE market_data_source = ?
              AND trading_date <= ?
        )
        SELECT
            ticker,
            trading_date,
            price,
            volume,
            atr_14,
            atr_pct_14
        FROM deduped_history
        WHERE row_rank = 1
        ORDER BY ticker, trading_date
        """,
        [market_data_source, snapshot_ts.date()],
    ).df()
    if history.empty:
        for col in (
            "run_id",
            "snapshot_time",
            "price",
            "avg_volume",
            "perf_1w",
            "perf_1m",
            "perf_3m",
            "perf_6m",
            "perf_1w_atr_units",
            "perf_1m_atr_units",
            "atr_14",
            "atr_pct_14",
        ):
            membership[col] = np.nan
        membership["status"] = None
        return membership

    history = history.sort_values(["ticker", "trading_date"]).copy()
    history["trading_date"] = pd.to_datetime(history["trading_date"], errors="coerce").dt.date
    grouped_close = history.groupby("ticker")["price"]
    history["perf_1w"] = ((grouped_close.transform(lambda s: s / s.shift(5))) - 1.0) * 100.0
    history["perf_1m"] = ((grouped_close.transform(lambda s: s / s.shift(21))) - 1.0) * 100.0
    history["perf_3m"] = ((grouped_close.transform(lambda s: s / s.shift(63))) - 1.0) * 100.0
    history["perf_6m"] = ((grouped_close.transform(lambda s: s / s.shift(126))) - 1.0) * 100.0
    history["avg_volume"] = history.groupby("ticker")["volume"].transform(
        lambda s: s.rolling(window=20, min_periods=1).mean()
    )
    history["perf_1w_atr_units"] = np.where(
        history["atr_14"].notna() & (history["atr_14"] != 0) & grouped_close.shift(5).notna(),
        (history["price"] - grouped_close.shift(5)) / history["atr_14"],
        np.nan,
    )
    history["perf_1m_atr_units"] = np.where(
        history["atr_14"].notna() & (history["atr_14"] != 0) & grouped_close.shift(21).notna(),
        (history["price"] - grouped_close.shift(21)) / history["atr_14"],
        np.nan,
    )
    day_rows = history[history["trading_date"] == snapshot_ts.date()][
        [
            "ticker",
            "price",
            "avg_volume",
            "perf_1w",
            "perf_1m",
            "perf_3m",
            "perf_6m",
            "perf_1w_atr_units",
            "perf_1m_atr_units",
            "atr_14",
            "atr_pct_14",
        ]
    ].copy()
    if day_rows.empty:
        for col in (
            "run_id",
            "snapshot_time",
            "price",
            "avg_volume",
            "perf_1w",
            "perf_1m",
            "perf_3m",
            "perf_6m",
            "perf_1w_atr_units",
            "perf_1m_atr_units",
            "atr_14",
            "atr_pct_14",
        ):
            membership[col] = np.nan
        membership["status"] = None
        return membership

    snapshot_time = datetime.combine(snapshot_ts.date(), time(hour=17, minute=0, second=0))
    day_rows["run_id"] = np.nan
    day_rows["snapshot_time"] = snapshot_time

    if table_exists(conn, "symbol_refresh_status"):
        status_cols = ["ticker"]
        if table_has_column(conn, "symbol_refresh_status", "status"):
            status_cols.append("status")
        statuses = conn.execute(f"SELECT {', '.join(status_cols)} FROM symbol_refresh_status").df()
        if "status" not in statuses.columns:
            statuses["status"] = None
        statuses["ticker"] = statuses["ticker"].astype(str).str.strip().str.upper()
    else:
        statuses = pd.DataFrame(columns=["ticker", "status"])

    raw = membership.merge(day_rows, on="ticker", how="left")
    raw = raw.merge(statuses[["ticker", "status"]], on="ticker", how="left", suffixes=("", "_symbol"))
    return raw


def compute_current_ranking_metrics_for_trading_date(
    conn,
    snapshot_date: date | str,
    *,
    market_data_source: str = "live",
) -> pd.DataFrame:
    raw = _load_ranking_constituents_for_trading_date(
        conn,
        snapshot_date,
        market_data_source=market_data_source,
    )
    return _build_current_ranking_metrics(raw)


def _insert_canonical_history_repair_run(
    conn,
    *,
    snapshot_date: date,
    market_data_source: str,
    ticker_count: int,
    success_count: int,
) -> int:
    finished_at = datetime.combine(snapshot_date, time(hour=17, minute=0, second=0))
    started_at = datetime.combine(snapshot_date, time(hour=16, minute=55, second=0))
    return int(
        conn.execute(
            """
            INSERT INTO refresh_runs(
                provider, started_at, finished_at, status,
                ticker_count, success_count, failure_count, scope_type, error_message
            )
            VALUES (?, ?, ?, 'success', ?, ?, 0, 'canonical_history_repair', ?)
            RETURNING run_id
            """,
            [
                "synthetic_backfill",
                started_at,
                finished_at,
                int(ticker_count),
                int(success_count),
                f"Canonical daily history repair from ticker_daily_history ({market_data_source})",
            ],
        ).fetchone()[0]
    )


def build_canonical_theme_daily_rows_for_trading_date(
    conn,
    snapshot_date: date | str,
    *,
    market_data_source: str = "live",
    extract_session: str,
    canonical_reason: str,
    is_canonical_daily: bool = True,
) -> pd.DataFrame:
    snapshot_ts = pd.Timestamp(snapshot_date)
    if pd.isna(snapshot_ts):
        return pd.DataFrame(columns=CANONICAL_THEME_DAILY_COLUMNS)

    raw = _load_ranking_constituents_for_trading_date(
        conn,
        snapshot_ts.date(),
        market_data_source=market_data_source,
    )
    if raw.empty:
        return pd.DataFrame(columns=CANONICAL_THEME_DAILY_COLUMNS)

    represented_ticker_count = int(raw["price"].notna().sum()) if "price" in raw.columns else 0
    run_id = _insert_canonical_history_repair_run(
        conn,
        snapshot_date=snapshot_ts.date(),
        market_data_source=market_data_source,
        ticker_count=represented_ticker_count,
        success_count=represented_ticker_count,
    )
    raw = raw.copy()
    raw["run_id"] = int(run_id)
    metrics = _build_current_ranking_metrics(raw)
    if metrics.empty:
        return pd.DataFrame(columns=CANONICAL_THEME_DAILY_COLUMNS)

    canonical = metrics.copy()
    canonical["snapshot_date"] = snapshot_ts.date()
    canonical["snapshot_time"] = datetime.combine(snapshot_ts.date(), time(hour=17, minute=0, second=0))
    canonical["run_id"] = int(run_id)
    canonical["snapshot_source"] = "synthetic_backfill"
    canonical["extract_session"] = extract_session
    canonical["is_canonical_daily"] = bool(is_canonical_daily)
    canonical["canonical_reason"] = canonical_reason

    ranked = _finalize_current_rankings(
        canonical,
        score_col="standardized_composite_score",
        eligible_count_col="eligible_standardized_count",
    )
    ranked = ranked.copy()
    ranked["canonical_rank"] = range(1, len(ranked) + 1)
    canonical["canonical_rank"] = canonical["theme_id"].map(ranked.set_index("theme_id")["canonical_rank"])
    return canonical[CANONICAL_THEME_DAILY_COLUMNS].copy()


def _deduplicate_canonical_rows(canonical_rows: pd.DataFrame) -> pd.DataFrame:
    if canonical_rows.empty or "theme_id" not in canonical_rows.columns or "snapshot_date" not in canonical_rows.columns:
        return canonical_rows

    working = canonical_rows.copy()
    if "canonical_rank" in working.columns:
        working["_canonical_rank_missing"] = working["canonical_rank"].isna()
    else:
        working["_canonical_rank_missing"] = True
    if "snapshot_time" in working.columns:
        working["_snapshot_time_sort"] = pd.to_datetime(working["snapshot_time"], errors="coerce")
    else:
        working["_snapshot_time_sort"] = pd.NaT
    if "run_id" in working.columns:
        working["_run_id_sort"] = pd.to_numeric(working["run_id"], errors="coerce")
    else:
        working["_run_id_sort"] = pd.NA

    working = (
        working.sort_values(
            ["snapshot_date", "theme_id", "_canonical_rank_missing", "canonical_rank", "_snapshot_time_sort", "_run_id_sort"],
            ascending=[True, True, True, True, False, False],
            na_position="last",
        )
        .drop_duplicates(subset=["snapshot_date", "theme_id"], keep="first")
        .drop(columns=["_canonical_rank_missing", "_snapshot_time_sort", "_run_id_sort"], errors="ignore")
        .reset_index(drop=True)
    )
    return working[CANONICAL_THEME_DAILY_COLUMNS].copy()


def _merge_canonical_theme_daily_incoming(conn) -> None:
    column_list = ", ".join(CANONICAL_THEME_DAILY_COLUMNS)
    update_assignments = ", ".join(
        f"{column} = source.{column}"
        for column in CANONICAL_THEME_DAILY_COLUMNS
        if column not in {"snapshot_date", "theme_id"}
    )
    value_list = ", ".join(f"source.{column}" for column in CANONICAL_THEME_DAILY_COLUMNS)
    conn.execute(
        f"""
        MERGE INTO canonical_theme_daily_snapshots AS target
        USING canonical_theme_daily_incoming AS source
          ON target.snapshot_date = source.snapshot_date
         AND target.theme_id = source.theme_id
        WHEN MATCHED THEN
            UPDATE SET {update_assignments}
        WHEN NOT MATCHED THEN
            INSERT ({column_list})
            VALUES ({value_list})
        """
    )


def persist_canonical_theme_daily_snapshot_for_trading_date(
    conn,
    snapshot_date: date | str,
    *,
    market_data_source: str = "live",
    extract_session: str = "ticker_history_repair",
    canonical_reason: str = "missing_full_theme_run_history_repair",
    is_canonical_daily: bool = True,
    overwrite_existing: bool = False,
) -> dict[str, int | str]:
    snapshot_ts = pd.Timestamp(snapshot_date)
    if pd.isna(snapshot_ts):
        return {"run_id": 0, "snapshot_date": "", "row_count": 0, "inserted_count": 0, "status": "invalid_date"}

    snapshot_date_value = snapshot_ts.date()
    existing_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM canonical_theme_daily_snapshots WHERE snapshot_date = ?",
            [snapshot_date_value],
        ).fetchone()[0]
    )
    if existing_count > 0 and not overwrite_existing:
        return {
            "run_id": 0,
            "snapshot_date": str(snapshot_date_value),
            "row_count": existing_count,
            "inserted_count": 0,
            "status": "existing_snapshot_date",
        }

    canonical_rows = build_canonical_theme_daily_rows_for_trading_date(
        conn,
        snapshot_date_value,
        market_data_source=market_data_source,
        extract_session=extract_session,
        canonical_reason=canonical_reason,
        is_canonical_daily=is_canonical_daily,
    )
    if canonical_rows.empty:
        return {
            "run_id": 0,
            "snapshot_date": str(snapshot_date_value),
            "row_count": 0,
            "inserted_count": 0,
            "status": "no_history_rows_for_date",
        }
    canonical_rows = _deduplicate_canonical_rows(canonical_rows)

    incoming = canonical_rows.copy()
    conn.register("canonical_theme_daily_incoming", incoming)
    if overwrite_existing:
        _merge_canonical_theme_daily_incoming(conn)
        inserted_count = int(len(canonical_rows))
    else:
        inserted = conn.execute(
            """
            INSERT INTO canonical_theme_daily_snapshots(
                snapshot_date, snapshot_time, run_id, theme_id, theme, category, is_active,
                snapshot_source, extract_session, is_canonical_daily, canonical_reason,
                ticker_count, eligible_ticker_count, eligible_1w_count, eligible_1m_count,
                eligible_3m_count, eligible_6m_count, eligible_composite_count, eligible_standardized_count,
                eligible_momentum_count, eligible_breadth_pct, avg_1w, avg_1m, avg_3m, avg_6m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                legacy_composite_score, standardized_base_strength_score, standardized_participation_ratio,
                standardized_participation_factor, standardized_guardrail_factor,
                standardized_recovery_factor, standardized_composite_score,
                current_momentum_raw_score, current_momentum_quality_factor,
                current_momentum_score, canonical_rank
            )
            SELECT
                i.snapshot_date, i.snapshot_time, i.run_id, i.theme_id, i.theme, i.category, i.is_active,
                i.snapshot_source, i.extract_session, i.is_canonical_daily, i.canonical_reason,
                i.ticker_count, i.eligible_ticker_count, i.eligible_1w_count, i.eligible_1m_count,
                i.eligible_3m_count, i.eligible_6m_count, i.eligible_composite_count, i.eligible_standardized_count,
                i.eligible_momentum_count, i.eligible_breadth_pct, i.avg_1w, i.avg_1m, i.avg_3m, i.avg_6m,
                i.positive_1w_breadth_pct, i.positive_1m_breadth_pct, i.positive_3m_breadth_pct,
                i.legacy_composite_score, i.standardized_base_strength_score, i.standardized_participation_ratio,
                i.standardized_participation_factor, i.standardized_guardrail_factor,
                i.standardized_recovery_factor, i.standardized_composite_score,
                i.current_momentum_raw_score, i.current_momentum_quality_factor,
                i.current_momentum_score, i.canonical_rank
            FROM canonical_theme_daily_incoming i
            WHERE NOT EXISTS (
                SELECT 1
                FROM canonical_theme_daily_snapshots existing
                WHERE existing.snapshot_date = i.snapshot_date
                  AND existing.theme_id = i.theme_id
            )
            RETURNING run_id
            """
        ).fetchall()
        inserted_count = int(len(inserted))
    conn.unregister("canonical_theme_daily_incoming")
    run_id = int(canonical_rows["run_id"].iloc[0]) if not canonical_rows.empty else 0
    return {
        "run_id": run_id,
        "snapshot_date": str(snapshot_date_value),
        "row_count": int(len(canonical_rows)),
        "inserted_count": inserted_count,
        "status": "history_repaired",
    }


def build_canonical_theme_daily_rows_for_run(
    conn,
    run_id: int,
    *,
    extract_session: str,
    canonical_reason: str,
    is_canonical_daily: bool = True,
) -> pd.DataFrame:
    run_meta = conn.execute(
        """
        SELECT finished_at, provider, status
        FROM refresh_runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()
    if run_meta is None:
        raise ValueError(f"Unknown refresh run_id: {run_id}")

    snapshot_time, provider, status = run_meta
    if status not in {"success", "partial"}:
        raise ValueError(f"Refresh run {run_id} is not complete enough for canonical persistence: status={status}")
    if snapshot_time is None:
        raise ValueError(f"Refresh run {run_id} has no finished_at timestamp")

    metrics = compute_current_ranking_metrics_for_run(conn, run_id)
    if metrics.empty:
        return pd.DataFrame(columns=CANONICAL_THEME_DAILY_COLUMNS)

    canonical = metrics.copy()
    snapshot_timestamp = pd.Timestamp(snapshot_time)
    canonical["snapshot_date"] = snapshot_timestamp.date()
    canonical["snapshot_time"] = snapshot_timestamp
    canonical["run_id"] = int(run_id)
    canonical["snapshot_source"] = provider if provider in {"live", "mock", "synthetic_backfill"} else "live"
    canonical["extract_session"] = extract_session
    canonical["is_canonical_daily"] = bool(is_canonical_daily)
    canonical["canonical_reason"] = canonical_reason

    ranked = _finalize_current_rankings(
        canonical,
        score_col="standardized_composite_score",
        eligible_count_col="eligible_standardized_count",
    )
    ranked = ranked.copy()
    ranked["canonical_rank"] = range(1, len(ranked) + 1)
    canonical["canonical_rank"] = canonical["theme_id"].map(ranked.set_index("theme_id")["canonical_rank"])
    return canonical[CANONICAL_THEME_DAILY_COLUMNS].copy()


def persist_canonical_theme_daily_snapshot_for_run(
    conn,
    run_id: int,
    *,
    extract_session: str = "after_hours_official",
    canonical_reason: str = "official_daily_refresh",
    is_canonical_daily: bool = True,
    overwrite_existing: bool = False,
) -> dict[str, int | str]:
    # Current assumption: "official daily" is asserted by the caller via
    # extract_session/canonical_reason. We do not yet infer this from an
    # upstream run-classification table, and we do not let later non-canonical
    # refreshes overwrite an existing canonical theme/date lock.
    canonical_rows = build_canonical_theme_daily_rows_for_run(
        conn,
        run_id,
        extract_session=extract_session,
        canonical_reason=canonical_reason,
        is_canonical_daily=is_canonical_daily,
    )
    if canonical_rows.empty:
        return {"run_id": int(run_id), "snapshot_date": "", "row_count": 0, "inserted_count": 0, "status": "no_rows"}
    canonical_rows = _deduplicate_canonical_rows(canonical_rows)

    rankable_row_count = int(canonical_rows["canonical_rank"].notna().sum()) if "canonical_rank" in canonical_rows.columns else 0
    if rankable_row_count <= 0:
        snapshot_date = canonical_rows["snapshot_date"].iloc[0]
        return {
            "run_id": int(run_id),
            "snapshot_date": str(snapshot_date),
            "row_count": int(len(canonical_rows)),
            "inserted_count": 0,
            "ranked_row_count": 0,
            "status": "no_rankable_rows_for_run",
        }

    snapshot_date = pd.Timestamp(canonical_rows["snapshot_date"].iloc[0]).date()

    incoming = canonical_rows.copy()
    conn.register("canonical_theme_daily_incoming", incoming)
    if overwrite_existing:
        _merge_canonical_theme_daily_incoming(conn)
        inserted_count = int(len(canonical_rows))
    else:
        inserted = conn.execute(
            """
            INSERT INTO canonical_theme_daily_snapshots(
                snapshot_date, snapshot_time, run_id, theme_id, theme, category, is_active,
                snapshot_source, extract_session, is_canonical_daily, canonical_reason,
                ticker_count, eligible_ticker_count, eligible_1w_count, eligible_1m_count,
                eligible_3m_count, eligible_6m_count, eligible_composite_count, eligible_standardized_count,
                eligible_momentum_count, eligible_breadth_pct, avg_1w, avg_1m, avg_3m, avg_6m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                legacy_composite_score, standardized_base_strength_score, standardized_participation_ratio,
                standardized_participation_factor, standardized_guardrail_factor,
                standardized_recovery_factor, standardized_composite_score,
                current_momentum_raw_score, current_momentum_quality_factor,
                current_momentum_score, canonical_rank
            )
            SELECT
                i.snapshot_date, i.snapshot_time, i.run_id, i.theme_id, i.theme, i.category, i.is_active,
                i.snapshot_source, i.extract_session, i.is_canonical_daily, i.canonical_reason,
                i.ticker_count, i.eligible_ticker_count, i.eligible_1w_count, i.eligible_1m_count,
                i.eligible_3m_count, i.eligible_6m_count, i.eligible_composite_count, i.eligible_standardized_count,
                i.eligible_momentum_count, i.eligible_breadth_pct, i.avg_1w, i.avg_1m, i.avg_3m, i.avg_6m,
                i.positive_1w_breadth_pct, i.positive_1m_breadth_pct, i.positive_3m_breadth_pct,
                i.legacy_composite_score, i.standardized_base_strength_score, i.standardized_participation_ratio,
                i.standardized_participation_factor, i.standardized_guardrail_factor,
                i.standardized_recovery_factor, i.standardized_composite_score,
                i.current_momentum_raw_score, i.current_momentum_quality_factor,
                i.current_momentum_score, i.canonical_rank
            FROM canonical_theme_daily_incoming i
            WHERE NOT EXISTS (
                SELECT 1
                FROM canonical_theme_daily_snapshots existing
                WHERE existing.snapshot_date = i.snapshot_date
                  AND existing.theme_id = i.theme_id
            )
            RETURNING theme_id
            """
        ).fetchall()
        inserted_count = int(len(inserted))
    conn.unregister("canonical_theme_daily_incoming")
    return {
        "run_id": int(run_id),
        "snapshot_date": str(snapshot_date),
        "row_count": int(len(canonical_rows)),
        "inserted_count": inserted_count,
        "ranked_row_count": rankable_row_count,
        "status": "materialized_from_run",
    }


def canonical_backfill_candidate_runs_by_date(
    conn,
    *,
    recent_trading_day_limit: int = 30,
    provider: str = "live",
) -> pd.DataFrame:
    limit_days = max(int(recent_trading_day_limit), 1)
    target_dates = conn.execute(
        """
        WITH recent_trading_dates AS (
            SELECT DISTINCT trading_date
            FROM ticker_daily_history
            WHERE market_data_source = ?
            ORDER BY trading_date DESC
            LIMIT ?
        )
        SELECT trading_date
        FROM recent_trading_dates
        ORDER BY trading_date ASC
        """,
        [provider, limit_days],
    ).df()
    if target_dates.empty:
        return pd.DataFrame(
            columns=[
                "snapshot_date",
                "run_id",
                "provider",
                "status",
                "scope_type",
                "finished_at",
                "ticker_count",
                "success_count",
                "selection_reason",
                "selection_status",
            ]
        )

    candidates = conn.execute(
        """
        WITH target_dates AS (
            SELECT trading_date AS snapshot_date
            FROM (
                SELECT DISTINCT trading_date
                FROM ticker_daily_history
                WHERE market_data_source = ?
                ORDER BY trading_date DESC
                LIMIT ?
            )
        ),
        candidate_runs AS (
            SELECT
                CAST(r.finished_at AS DATE) AS snapshot_date,
                r.run_id,
                r.provider,
                r.status,
                r.scope_type,
                r.finished_at,
                r.ticker_count,
                r.success_count,
                CASE
                    WHEN r.scope_type = 'scheduled_eod' THEN 0
                    WHEN r.scope_type = 'active_themes' THEN 1
                    ELSE 9
                END AS scope_priority
            FROM refresh_runs r
            JOIN target_dates d ON d.snapshot_date = CAST(r.finished_at AS DATE)
            WHERE r.status IN ('success', 'partial')
              AND r.finished_at IS NOT NULL
              AND r.provider = ?
              AND r.scope_type IN ('scheduled_eod', 'active_themes')
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY snapshot_date
                    ORDER BY scope_priority ASC, success_count DESC, ticker_count DESC, finished_at DESC, run_id DESC
                ) AS winner_rank
            FROM candidate_runs
        )
        SELECT
            d.snapshot_date,
            r.run_id,
            r.provider,
            r.status,
            r.scope_type,
            r.finished_at,
            r.ticker_count,
            r.success_count,
            CASE
                WHEN r.run_id IS NULL THEN 'no_completed_full_theme_run_for_trading_date'
                WHEN r.scope_type = 'scheduled_eod' THEN 'scheduled_eod_preferred'
                ELSE 'active_themes_fallback'
            END AS selection_reason,
            CASE
                WHEN r.run_id IS NULL THEN 'missing_run'
                ELSE 'selected'
            END AS selection_status
        FROM target_dates d
        LEFT JOIN ranked r
          ON r.snapshot_date = d.snapshot_date
         AND r.winner_rank = 1
        ORDER BY d.snapshot_date ASC
        """,
        [provider, limit_days, provider],
    ).df()
    return candidates


def backfill_canonical_theme_daily_snapshots_for_recent_trading_days(
    conn,
    *,
    recent_trading_day_limit: int = 30,
    provider: str = "live",
    overwrite_existing: bool = False,
) -> dict[str, object]:
    before = conn.execute(
        """
        SELECT
            COUNT(DISTINCT snapshot_date) AS date_count,
            COUNT(*) AS row_count,
            MIN(snapshot_date) AS min_date,
            MAX(snapshot_date) AS max_date
        FROM canonical_theme_daily_snapshots
        """
    ).df().iloc[0].to_dict()

    candidates = canonical_backfill_candidate_runs_by_date(
        conn,
        recent_trading_day_limit=recent_trading_day_limit,
        provider=provider,
    )
    if candidates.empty:
        return {
            "before": before,
            "after": before,
            "results": [],
            "missing_dates": [],
            "selected_run_dates": 0,
        }

    results: list[dict[str, object]] = []
    missing_dates: list[dict[str, object]] = []
    for row in candidates.itertuples(index=False):
        snapshot_date = row.snapshot_date
        run_id = row.run_id
        if run_id is None or pd.isna(run_id):
            repair_result = persist_canonical_theme_daily_snapshot_for_trading_date(
                conn,
                snapshot_date,
                market_data_source=provider,
                extract_session="ticker_history_repair",
                canonical_reason="missing_full_theme_run_history_repair",
                is_canonical_daily=True,
                overwrite_existing=overwrite_existing,
            )
            if int(repair_result.get("inserted_count", 0) or 0) > 0:
                repair_result.update(
                    {
                        "selection_reason": "ticker_history_repair_fallback",
                        "scope_type": "canonical_history_repair",
                        "finished_at": None,
                        "ticker_count": 0,
                        "success_count": 0,
                    }
                )
                results.append(repair_result)
                continue
            missing_dates.append(
                {
                    "snapshot_date": str(snapshot_date),
                    "reason": str(repair_result.get("status") or row.selection_reason),
                }
            )
            continue
        persist_result = persist_canonical_theme_daily_snapshot_for_run(
            conn,
            int(run_id),
            extract_session="backfill_selected_run",
            canonical_reason="recent_trading_day_backfill",
            is_canonical_daily=True,
            overwrite_existing=overwrite_existing,
        )
        if str(persist_result.get("status") or "") == "no_rankable_rows_for_run":
            repair_result = persist_canonical_theme_daily_snapshot_for_trading_date(
                conn,
                snapshot_date,
                market_data_source=provider,
                extract_session="ticker_history_repair",
                canonical_reason="missing_full_theme_run_history_repair",
                is_canonical_daily=True,
                overwrite_existing=overwrite_existing,
            )
            repair_result.update(
                {
                    "selection_reason": f"{row.selection_reason}_rankability_repair_fallback",
                    "scope_type": "canonical_history_repair",
                    "finished_at": row.finished_at,
                    "ticker_count": int(row.ticker_count or 0),
                    "success_count": int(row.success_count or 0),
                    "upstream_run_id": int(run_id),
                    "upstream_run_status": str(persist_result.get("status") or "unknown"),
                }
            )
            results.append(repair_result)
            continue
        persist_result.update(
            {
                "selection_reason": str(row.selection_reason),
                "scope_type": str(row.scope_type),
                "finished_at": row.finished_at,
                "ticker_count": int(row.ticker_count or 0),
                "success_count": int(row.success_count or 0),
            }
        )
        results.append(persist_result)

    after = conn.execute(
        """
        SELECT
            COUNT(DISTINCT snapshot_date) AS date_count,
            COUNT(*) AS row_count,
            MIN(snapshot_date) AS min_date,
            MAX(snapshot_date) AS max_date
        FROM canonical_theme_daily_snapshots
        """
    ).df().iloc[0].to_dict()
    return {
        "before": before,
        "after": after,
        "results": results,
        "missing_dates": missing_dates,
        "selected_run_dates": int(len(results)),
    }


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
        for col in ("run_id", "snapshot_time", "price", "avg_volume", "perf_1d", "perf_1w", "perf_1m", "perf_3m", "perf_6m"):
            membership[col] = np.nan
        membership["status"] = None
        return membership

    latest = latest.copy()
    for col in ("price", "avg_volume", "perf_1d", "perf_1w", "perf_1m", "perf_3m", "perf_6m"):
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
    atr_research = latest_ticker_history_atr_companion_fields(conn)
    if not atr_research.empty:
        raw = raw.merge(atr_research, on="ticker", how="left")
    return raw


def _build_current_ranking_metrics(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=CURRENT_RANKING_COLUMNS)

    prepared = raw.copy()
    for col in ("price", "avg_volume", "perf_1d", "perf_1w", "perf_1m", "perf_3m", "perf_6m", "perf_1w_atr_units", "perf_1m_atr_units"):
        if col in prepared.columns:
            prepared[col] = _safe_numeric(prepared[col])
        else:
            prepared[col] = pd.Series(np.nan, index=prepared.index, dtype="float64")

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

    prepared["perf_1d_eligible"] = prepared["eligible_ticker"] & prepared["perf_1d"].notna()
    capped_return_cols: dict[str, str] = {}
    for perf_col in ("perf_1w", "perf_1m", "perf_3m", "perf_6m"):
        eligible_col = f"{perf_col}_eligible"
        capped_col = f"{perf_col}_capped"
        prepared[eligible_col] = prepared["eligible_ticker"] & prepared[perf_col].notna()
        prepared[capped_col] = prepared[perf_col].clip(
            lower=-CURRENT_RANKING_RETURN_CAP_PCT,
            upper=CURRENT_RANKING_RETURN_CAP_PCT,
        )
        capped_return_cols[perf_col] = capped_col
    atr_capped_return_cols: dict[str, str] = {}
    for perf_col in ("perf_1w_atr_units", "perf_1m_atr_units"):
        eligible_col = f"{perf_col}_eligible"
        capped_col = f"{perf_col}_capped"
        prepared[eligible_col] = prepared["eligible_ticker"] & prepared[perf_col].notna()
        prepared[capped_col] = prepared[perf_col].clip(
            lower=-CURRENT_RANKING_RETURN_CAP_PCT,
            upper=CURRENT_RANKING_RETURN_CAP_PCT,
        )
        atr_capped_return_cols[perf_col] = capped_col

    prepared["composite_metric_eligible"] = (
        prepared["perf_1w_eligible"] & prepared["perf_1m_eligible"] & prepared["perf_3m_eligible"]
    )
    prepared["standardized_metric_eligible"] = prepared["perf_1w_eligible"] & prepared["perf_1m_eligible"]
    prepared["atr_metric_eligible"] = prepared["perf_1w_atr_units_eligible"] & prepared["perf_1m_atr_units_eligible"]

    prepared["ticker_present"] = prepared["ticker"].notna().astype(int)
    prepared["perf_1d_capped_for_agg"] = prepared["perf_1d"].clip(
        lower=-CURRENT_RANKING_RETURN_CAP_PCT,
        upper=CURRENT_RANKING_RETURN_CAP_PCT,
    ).where(prepared["perf_1d_eligible"])
    prepared["perf_1w_capped_for_agg"] = prepared[capped_return_cols["perf_1w"]].where(prepared["perf_1w_eligible"])
    prepared["perf_1m_capped_for_agg"] = prepared[capped_return_cols["perf_1m"]].where(prepared["perf_1m_eligible"])
    prepared["perf_3m_capped_for_agg"] = prepared[capped_return_cols["perf_3m"]].where(prepared["perf_3m_eligible"])
    prepared["perf_6m_capped_for_agg"] = prepared[capped_return_cols["perf_6m"]].where(prepared["perf_6m_eligible"])
    prepared["perf_1w_atr_units_capped_for_agg"] = prepared[atr_capped_return_cols["perf_1w_atr_units"]].where(prepared["perf_1w_atr_units_eligible"])
    prepared["perf_1m_atr_units_capped_for_agg"] = prepared[atr_capped_return_cols["perf_1m_atr_units"]].where(prepared["perf_1m_atr_units_eligible"])
    prepared["perf_1w_positive"] = np.where(prepared["perf_1w_eligible"], prepared["perf_1w"] > 0, np.nan)
    prepared["perf_1m_positive"] = np.where(prepared["perf_1m_eligible"], prepared["perf_1m"] > 0, np.nan)
    prepared["perf_3m_positive"] = np.where(prepared["perf_3m_eligible"], prepared["perf_3m"] > 0, np.nan)

    grouped = prepared.groupby(["theme_id", "theme", "category", "is_active"], dropna=False)
    out = grouped.agg(
        run_id=("run_id", "max"),
        snapshot_time=("snapshot_time", "max"),
        ticker_count=("ticker_present", "sum"),
        eligible_ticker_count=("eligible_ticker", "sum"),
        eligible_1d_count=("perf_1d_eligible", "sum"),
        eligible_1w_count=("perf_1w_eligible", "sum"),
        eligible_1m_count=("perf_1m_eligible", "sum"),
        eligible_3m_count=("perf_3m_eligible", "sum"),
        eligible_6m_count=("perf_6m_eligible", "sum"),
        eligible_composite_count=("composite_metric_eligible", "sum"),
        eligible_standardized_count=("standardized_metric_eligible", "sum"),
        eligible_atr_count=("atr_metric_eligible", "sum"),
        eligible_momentum_count=("standardized_metric_eligible", "sum"),
        avg_1d=("perf_1d_capped_for_agg", "mean"),
        avg_1w=("perf_1w_capped_for_agg", "mean"),
        avg_1m=("perf_1m_capped_for_agg", "mean"),
        avg_1w_atr_units=("perf_1w_atr_units_capped_for_agg", "mean"),
        avg_1m_atr_units=("perf_1m_atr_units_capped_for_agg", "mean"),
        avg_3m=("perf_3m_capped_for_agg", "mean"),
        avg_6m=("perf_6m_capped_for_agg", "mean"),
        positive_1w_breadth_pct=("perf_1w_positive", "mean"),
        positive_1m_breadth_pct=("perf_1m_positive", "mean"),
        positive_3m_breadth_pct=("perf_3m_positive", "mean"),
    ).reset_index()
    if out.empty:
        return out

    count_cols = [
        "ticker_count",
        "eligible_ticker_count",
        "eligible_1d_count",
        "eligible_1w_count",
        "eligible_1m_count",
        "eligible_3m_count",
        "eligible_6m_count",
        "eligible_composite_count",
        "eligible_standardized_count",
        "eligible_atr_count",
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
        + STANDARDIZED_COMPOSITE_WEIGHTS["perf_3m"] * out["avg_3m"].fillna(0.0)
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
    out["composite_atr_base_strength_score"] = (
        COMPOSITE_ATR_BASE_WEIGHTS["perf_1w"] * out["avg_1w_atr_units"].fillna(0.0)
        + COMPOSITE_ATR_BASE_WEIGHTS["perf_1m"] * out["avg_1m_atr_units"].fillna(0.0)
    )
    out["composite_atr_participation_ratio"] = np.where(
        out["ticker_count"] > 0,
        out["eligible_atr_count"] / out["ticker_count"],
        0.0,
    )
    out["composite_atr_participation_factor"] = out["composite_atr_participation_ratio"].apply(standardized_participation_factor)
    out["composite_atr_guardrail_factor"] = out["avg_3m"].apply(standardized_three_month_guardrail_factor)
    out["composite_atr_recovery_factor"] = [
        standardized_recovery_factor(base_score, avg_3m)
        for base_score, avg_3m in zip(out["composite_atr_base_strength_score"], out["avg_3m"])
    ]
    out["composite_atr_score"] = np.where(
        out["eligible_atr_count"] > 0,
        out["composite_atr_base_strength_score"]
        * out["composite_atr_participation_factor"]
        * out["composite_atr_guardrail_factor"]
        * out["composite_atr_recovery_factor"],
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
        "avg_1w_atr_units",
        "avg_1m_atr_units",
        "avg_3m",
        "avg_6m",
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
        "composite_atr_base_strength_score",
        "composite_atr_participation_ratio",
        "composite_atr_participation_factor",
        "composite_atr_guardrail_factor",
        "composite_atr_recovery_factor",
        "composite_atr_score",
        "current_momentum_raw_score",
        "current_momentum_quality_factor",
        "current_momentum_score",
    ]
    out[numeric_cols] = out[numeric_cols].round(2)
    out["composite_atr_rank"] = np.nan
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
        "avg_6m": "eligible_6m_count",
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
            "composite_atr_score",
            "composite_atr_rank",
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
        ["theme_id", "theme", "category", "avg_1w", "avg_1m", "standardized_composite_score", "composite_atr_score", "composite_atr_rank"]
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
            "composite_atr_score",
            "composite_atr_rank",
            "avg_1w",
            "avg_1m",
        ]
    ].copy()
    current_momentum["current_momentum_rank"] = range(1, len(current_momentum) + 1)

    standardized = standardized_rankings[["theme_id", "composite_atr_rank"]].copy()
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
    comparison["composite_atr_score"] = comparison["composite_atr_score_momentum"].where(
        comparison["composite_atr_score_momentum"].notna(),
        comparison["composite_atr_score_1w"],
    )
    atr_rank_fallback = comparison["composite_atr_rank"] if "composite_atr_rank" in comparison.columns else np.nan
    comparison["composite_atr_rank"] = comparison["composite_atr_rank_momentum"].where(
        comparison["composite_atr_rank_momentum"].notna(),
        comparison["composite_atr_rank_1w"],
    )
    if isinstance(atr_rank_fallback, pd.Series):
        comparison["composite_atr_rank"] = comparison["composite_atr_rank"].where(
            comparison["composite_atr_rank"].notna(),
            atr_rank_fallback,
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
            "composite_atr_rank",
            "rank_shift_vs_1w",
            "avg_1w",
            "avg_1m",
            "current_momentum_raw_score",
            "current_momentum_quality_factor",
            "current_momentum_score",
            "standardized_composite_score",
            "composite_atr_score",
            "entered_momentum_view",
            "dropped_from_momentum_view",
        ]
    ].sort_values(
        ["rank_shift_vs_1w", "current_momentum_rank", "current_1w_rank", "theme"],
        ascending=[False, True, True, True],
        na_position="last",
    ).reset_index(drop=True)


def _empty_current_ranking_snapshot(*, include_validation: bool = True) -> dict[str, pd.DataFrame]:
    snapshot = {
        "theme_metrics": pd.DataFrame(columns=CURRENT_RANKING_COLUMNS),
        "rankings": pd.DataFrame(),
        "standardized_rankings": pd.DataFrame(),
    }
    if include_validation:
        snapshot["standardized_comparison"] = pd.DataFrame()
        snapshot["current_momentum_rankings"] = pd.DataFrame()
        snapshot["current_momentum_comparison"] = pd.DataFrame()
    return snapshot


def _build_current_ranking_base_snapshot(conn) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Current trust surfaces all derive from one prepared latest-snapshot view so
    # contributor eligibility and capped-return semantics stay consistent.
    current = _build_current_ranking_metrics(_load_current_ranking_constituents(conn))
    if current.empty:
        return current, pd.DataFrame(), pd.DataFrame()

    legacy_rankings = _finalize_current_rankings(current, score_col="legacy_composite_score", eligible_count_col="eligible_composite_count")
    standardized_rankings = _finalize_current_rankings(
        current,
        score_col="standardized_composite_score",
        eligible_count_col="eligible_standardized_count",
    )
    atr_rankings = _finalize_current_rankings(
        current,
        score_col="composite_atr_score",
        eligible_count_col="eligible_atr_count",
    )
    if not atr_rankings.empty:
        atr_rankings = atr_rankings.copy()
        atr_rankings["composite_atr_rank"] = range(1, len(atr_rankings) + 1)
        atr_rank_lookup = atr_rankings.set_index("theme_id")["composite_atr_rank"].to_dict()
        current["composite_atr_rank"] = current["theme_id"].map(atr_rank_lookup)
        standardized_rankings["composite_atr_rank"] = standardized_rankings["theme_id"].map(atr_rank_lookup)
        legacy_rankings["composite_atr_rank"] = legacy_rankings["theme_id"].map(atr_rank_lookup)
    return current, legacy_rankings, standardized_rankings


def compute_current_ranking_operating_snapshot(conn) -> dict[str, pd.DataFrame]:
    current, legacy_rankings, standardized_rankings = _build_current_ranking_base_snapshot(conn)
    return _build_current_ranking_operating_snapshot_from_base(conn, current, legacy_rankings, standardized_rankings)


def _build_current_ranking_operating_snapshot_from_base(
    conn,
    current: pd.DataFrame,
    legacy_rankings: pd.DataFrame,
    standardized_rankings: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    if current.empty:
        return _empty_current_ranking_snapshot(include_validation=False)

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
    }


def compute_current_ranking_validation_snapshot(conn) -> dict[str, pd.DataFrame]:
    current, legacy_rankings, standardized_rankings = _build_current_ranking_base_snapshot(conn)
    return _build_current_ranking_validation_snapshot_from_base(current, legacy_rankings, standardized_rankings)


def _build_current_ranking_validation_snapshot_from_base(
    current: pd.DataFrame,
    legacy_rankings: pd.DataFrame,
    standardized_rankings: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    if current.empty:
        return {
            "standardized_comparison": pd.DataFrame(),
            "current_momentum_rankings": pd.DataFrame(),
            "current_momentum_comparison": pd.DataFrame(),
        }

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
    return {
        "standardized_comparison": standardized_comparison,
        "current_momentum_rankings": current_momentum_rankings,
        "current_momentum_comparison": current_momentum_comparison,
    }


def compute_current_ranking_snapshot(conn) -> dict[str, pd.DataFrame]:
    current, legacy_rankings, standardized_rankings = _build_current_ranking_base_snapshot(conn)
    if current.empty:
        return _empty_current_ranking_snapshot(include_validation=True)
    operating_snapshot = _build_current_ranking_operating_snapshot_from_base(
        conn,
        current,
        legacy_rankings,
        standardized_rankings,
    )
    validation_snapshot = _build_current_ranking_validation_snapshot_from_base(
        current,
        legacy_rankings,
        standardized_rankings,
    )
    return {
        **operating_snapshot,
        **validation_snapshot,
    }


def compute_theme_rankings(conn) -> pd.DataFrame:
    return compute_current_ranking_snapshot(conn)["rankings"]
