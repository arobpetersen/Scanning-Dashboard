from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from .config import (
    COMPOSITE_WEIGHTS,
    CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS,
    CURRENT_RANKING_RETURN_CAP_PCT,
    ENABLE_RECENT_TICKER_HISTORY_PREFERRED_RECONSTRUCTION,
    THEME_CONFIDENCE_FULL_COUNT,
)
from .db_introspection import table_exists, table_has_column


RECENT_TICKER_HISTORY_DERIVED_CALENDAR_DAYS = 45
TICKER_HISTORY_BUFFER_DAYS = 220
TICKER_HISTORY_ELIGIBLE_COVERAGE_THRESHOLD = 0.6


CORE_TABLES = [
    "themes",
    "theme_membership",
    "refresh_runs",
    "ticker_snapshots",
    "theme_snapshots",
    "refresh_failures",
    "refresh_run_tickers",
    "symbol_refresh_status",
    "theme_suggestions",
]


class QueryResultStateError(RuntimeError):
    """Raised when a required query result row is unexpectedly missing."""


def _is_duckdb_result_state_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return any(
        token in message
        for token in {
            "no open result set",
            "closed pending query result",
            "unsuccessful or closed pending query result",
            "result closed",
        }
    )


def _is_duckdb_internal_poisoned_state(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return any(
        token in message
        for token in {
            "attempted to dereference unique_ptr that is null",
            "internal error",
        }
    )


def _with_bootstrap_recovery(loader):
    try:
        return loader()
    except (duckdb.InvalidInputException, duckdb.InternalException) as exc:
        if not (_is_duckdb_result_state_error(exc) or _is_duckdb_internal_poisoned_state(exc)):
            raise
        from .database import get_bootstrap_conn

        with get_bootstrap_conn() as bootstrap_conn:
            return loader(bootstrap_conn)
    except QueryResultStateError:
        from .database import get_bootstrap_conn

        with get_bootstrap_conn() as bootstrap_conn:
            return loader(bootstrap_conn)


def _fetchone_required(result, context: str):
    row = result.fetchone()
    if row is None:
        raise QueryResultStateError(f"Query returned no row: {context}")
    return row


def _manual_suppression_enabled(conn) -> bool:
    return table_exists(conn, "symbol_refresh_status") and table_has_column(conn, "symbol_refresh_status", "manual_suppressed")


def _manual_suppression_filter_sql(conn, ticker_expr: str) -> str:
    if not _manual_suppression_enabled(conn):
        return ""
    return (
        " AND NOT EXISTS ("
        "SELECT 1 FROM symbol_refresh_status sr "
        f"WHERE upper(trim(sr.ticker)) = upper(trim({ticker_expr})) "
        "AND COALESCE(sr.manual_suppressed, FALSE)"
        ")"
    )


def _theme_snapshot_source_expr(conn) -> str:
    if table_has_column(conn, "theme_snapshots", "snapshot_source"):
        return "snapshot_source"
    if table_has_column(conn, "refresh_runs", "provider"):
        return "COALESCE((SELECT provider FROM refresh_runs rr WHERE rr.run_id = theme_snapshots.run_id), 'live')"
    return "'live'"


def _ticker_snapshot_source_expr(conn) -> str:
    if table_has_column(conn, "ticker_snapshots", "snapshot_source"):
        return "s.snapshot_source"
    if table_has_column(conn, "refresh_runs", "provider"):
        return "COALESCE(r.provider, 'live')"
    return "'live'"


def _historical_theme_snapshot_union(
    conn,
    *,
    include_recent_ticker_history: bool = False,
    theme_id: int | None = None,
    start_date: object | None = None,
    prefer_recent_ticker_history: bool = False,
) -> pd.DataFrame:
    preferred_source = preferred_theme_snapshot_source(conn) or _preferred_ticker_history_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    positive_1w_expr = "ts.positive_1w_breadth_pct" if table_has_column(conn, "theme_snapshots", "positive_1w_breadth_pct") else "NULL"
    positive_3m_expr = "ts.positive_3m_breadth_pct" if table_has_column(conn, "theme_snapshots", "positive_3m_breadth_pct") else "NULL"
    avg_6m_expr = "ts.avg_6m" if table_has_column(conn, "theme_snapshots", "avg_6m") else "NULL"
    theme_filter_sql = "AND ts.theme_id = ?" if theme_id is not None else ""
    captured_date_filter_sql = "AND CAST(ts.snapshot_time AS DATE) >= ?" if start_date is not None else ""
    captured_params: list[object] = [preferred_source]
    if theme_id is not None:
        captured_params.append(int(theme_id))
    if start_date is not None:
        captured_params.append(pd.Timestamp(start_date).date())

    captured = conn.execute(
        f"""
        SELECT
            ts.run_id,
            CAST(ts.snapshot_time AS DATE) AS snapshot_date,
            ts.snapshot_time,
            ts.theme_id,
            t.name AS theme,
            t.category,
            ts.ticker_count,
            ts.avg_1w,
            ts.avg_1m,
            ts.avg_3m,
            {avg_6m_expr} AS avg_6m,
            {positive_1w_expr} AS positive_1w_breadth_pct,
            ts.positive_1m_breadth_pct,
            {positive_3m_expr} AS positive_3m_breadth_pct,
            ts.composite_score,
            ts.snapshot_source AS snapshot_source,
            'captured' AS provenance_class,
            ts.snapshot_source AS provenance_source_label
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        WHERE ts.snapshot_source = ?
          {theme_filter_sql}
          {captured_date_filter_sql}
        """,
        captured_params,
    ).df()

    reconstructed = pd.DataFrame()
    if table_exists(conn, "reconstructed_theme_snapshots"):
        reconstructed_theme_filter_sql = "AND r.theme_id = ?" if theme_id is not None else ""
        reconstructed_date_filter_sql = "AND r.snapshot_date >= ?" if start_date is not None else ""
        reconstructed_params: list[object] = [preferred_source]
        if theme_id is not None:
            reconstructed_params.append(int(theme_id))
        if start_date is not None:
            reconstructed_params.append(pd.Timestamp(start_date).date())
        reconstructed = conn.execute(
            f"""
            SELECT
                r.run_id,
                r.snapshot_date,
                r.snapshot_time,
                r.theme_id,
                t.name AS theme,
                t.category,
                r.ticker_count,
                r.avg_1w,
                r.avg_1m,
                r.avg_3m,
                r.avg_6m,
                r.positive_1w_breadth_pct,
                r.positive_1m_breadth_pct,
                r.positive_3m_breadth_pct,
                r.composite_score,
                r.market_data_source AS snapshot_source,
                r.provenance_class,
                r.provenance_source_label
            FROM reconstructed_theme_snapshots r
            JOIN themes t ON t.id = r.theme_id
            WHERE r.market_data_source = ?
              {reconstructed_theme_filter_sql}
              {reconstructed_date_filter_sql}
            """,
            reconstructed_params,
        ).df()

    ticker_history_derived = pd.DataFrame()
    if include_recent_ticker_history and ENABLE_RECENT_TICKER_HISTORY_PREFERRED_RECONSTRUCTION:
        ticker_history_derived = _recent_ticker_history_theme_history(
            conn,
            preferred_source,
            theme_id=theme_id,
            start_date=start_date,
        )

    frames = [frame for frame in [captured, ticker_history_derived, reconstructed] if not frame.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if combined.empty:
        return combined

    # For any theme/date collision, prefer captured snapshots, then recent
    # ticker-history-derived snapshots, then deeper reconstructed history.
    combined["snapshot_time"] = pd.to_datetime(combined["snapshot_time"])
    combined["snapshot_date"] = pd.to_datetime(combined["snapshot_date"]).dt.date
    precedence_map = {"captured": 0, "ticker_history_derived": 1, "reconstructed": 2}
    if include_recent_ticker_history and prefer_recent_ticker_history:
        precedence_map = {"ticker_history_derived": 0, "captured": 1, "reconstructed": 2}
    combined["_precedence"] = combined["provenance_class"].map(precedence_map).fillna(9)
    combined = (
        combined.sort_values(["theme_id", "snapshot_date", "_precedence", "snapshot_time"], ascending=[True, True, True, False])
        .drop_duplicates(subset=["theme_id", "snapshot_date"], keep="first")
        .drop(columns=["_precedence"])
        .sort_values(["snapshot_time", "composite_score"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return combined


def _recent_movement_theme_snapshot_union(
    conn,
    *,
    theme_id: int | None = None,
    start_date: object | None = None,
) -> pd.DataFrame:
    """Build the recent movement window with ticker-history-derived rows preferred over captured rows when available."""
    return _historical_theme_snapshot_union(
        conn,
        include_recent_ticker_history=True,
        theme_id=theme_id,
        start_date=start_date,
        prefer_recent_ticker_history=True,
    )


def _resolve_recent_movement_boundaries(history: pd.DataFrame, lookback_days: int) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if history.empty:
        return None, None

    snapshot_times = pd.to_datetime(history["snapshot_time"]).dropna()
    if snapshot_times.empty:
        return None, None

    # When recent ticker-history-derived rows are available, anchor the
    # movement window to that last fully derived trading day instead of a
    # newer captured-only snapshot that cannot be reproduced from persisted
    # daily history yet.
    derived_times = pd.to_datetime(
        history.loc[history["provenance_class"].astype(str) == "ticker_history_derived", "snapshot_time"]
    ).dropna()
    end_time = derived_times.max() if not derived_times.empty else snapshot_times.max()

    eligible_times = snapshot_times[snapshot_times <= end_time].drop_duplicates().sort_values()
    if eligible_times.empty:
        return None, None

    target_start = end_time - pd.Timedelta(days=int(lookback_days))
    start_candidates = eligible_times[eligible_times <= target_start]
    start_time = start_candidates.iloc[-1] if not start_candidates.empty else eligible_times.iloc[0]
    return start_time, end_time


def _preferred_ticker_history_source(conn) -> str | None:
    if not table_exists(conn, "ticker_daily_history"):
        return None
    row = conn.execute(
        """
        SELECT market_data_source
        FROM ticker_daily_history
        ORDER BY CASE WHEN market_data_source = 'live' THEN 0 ELSE 1 END,
                 trading_date DESC,
                 updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def _theme_confidence_factor_for_history(ticker_count: int | float) -> float:
    if pd.isna(ticker_count) or float(ticker_count) <= 0:
        return 0.0
    return min(1.0, (float(ticker_count) / float(THEME_CONFIDENCE_FULL_COUNT)) ** 0.5)


def _recent_ticker_history_theme_history(
    conn,
    market_data_source: str,
    *,
    theme_id: int | None = None,
    start_date: object | None = None,
) -> pd.DataFrame:
    if not market_data_source or not table_exists(conn, "ticker_daily_history"):
        return pd.DataFrame()

    latest_row = conn.execute(
        """
        SELECT MAX(trading_date)
        FROM ticker_daily_history
        WHERE market_data_source = ?
        """,
        [market_data_source],
    ).fetchone()
    max_trading_date = latest_row[0] if latest_row and latest_row[0] else None
    if max_trading_date is None:
        return pd.DataFrame()

    recent_floor = pd.Timestamp(max_trading_date) - pd.Timedelta(days=RECENT_TICKER_HISTORY_DERIVED_CALENDAR_DAYS)
    requested_start = pd.Timestamp(start_date) if start_date is not None else None
    recent_start = max(recent_floor, requested_start) if requested_start is not None else recent_floor
    buffer_start = recent_start - pd.Timedelta(days=TICKER_HISTORY_BUFFER_DAYS)
    membership_filter_sql = "WHERE t.id = ?" if theme_id is not None else ""
    suppression_join_sql = (
        "LEFT JOIN symbol_refresh_status s ON s.ticker = m.ticker"
        if table_exists(conn, "symbol_refresh_status")
        else ""
    )
    eligibility_expr = (
        "CASE WHEN COALESCE(s.status, 'active') = 'refresh_suppressed' THEN FALSE ELSE TRUE END"
        if table_exists(conn, "symbol_refresh_status")
        else "TRUE"
    )
    params: list[object] = []
    if theme_id is not None:
        params.append(int(theme_id))
    params.extend(
        [
            market_data_source,
            pd.Timestamp(buffer_start).date(),
            pd.Timestamp(max_trading_date).date(),
            pd.Timestamp(recent_start).date(),
            market_data_source,
            TICKER_HISTORY_ELIGIBLE_COVERAGE_THRESHOLD,
        ]
    )

    metrics = conn.execute(
        f"""
        WITH membership AS (
            SELECT
                t.id AS theme_id,
                t.name AS theme,
                t.category,
                m.ticker,
                {eligibility_expr} AS is_eligible
            FROM themes t
            JOIN theme_membership m ON m.theme_id = t.id
            {suppression_join_sql}
            {membership_filter_sql}
        ),
        theme_counts AS (
            SELECT
                theme_id,
                MAX(theme) AS theme,
                MAX(category) AS category,
                SUM(CASE WHEN is_eligible THEN 1 ELSE 0 END) AS ticker_count,
                SUM(CASE WHEN is_eligible THEN 1 ELSE 0 END) AS eligible_constituent_count
            FROM membership
            GROUP BY theme_id
        ),
        governed_tickers AS (
            SELECT DISTINCT ticker
            FROM membership
            WHERE is_eligible
        ),
        deduped_history AS (
            SELECT
                upper(trim(h.ticker)) AS ticker,
                h.trading_date,
                h.close
            FROM ticker_daily_history h
            JOIN governed_tickers g ON upper(trim(g.ticker)) = upper(trim(h.ticker))
            WHERE h.market_data_source = ?
              AND h.trading_date BETWEEN ? AND ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY upper(trim(h.ticker)), h.trading_date, h.market_data_source
                ORDER BY h.updated_at DESC, h.close DESC
            ) = 1
        ),
        perf AS (
            SELECT
                h.ticker,
                h.trading_date,
                h.close,
                ((h.close / LAG(h.close, 5) OVER (PARTITION BY h.ticker ORDER BY h.trading_date)) - 1.0) * 100.0 AS perf_1w,
                ((h.close / LAG(h.close, 21) OVER (PARTITION BY h.ticker ORDER BY h.trading_date)) - 1.0) * 100.0 AS perf_1m,
                ((h.close / LAG(h.close, 63) OVER (PARTITION BY h.ticker ORDER BY h.trading_date)) - 1.0) * 100.0 AS perf_3m,
                ((h.close / LAG(h.close, 126) OVER (PARTITION BY h.ticker ORDER BY h.trading_date)) - 1.0) * 100.0 AS perf_6m
            FROM deduped_history h
        ),
        recent_perf AS (
            SELECT *
            FROM perf
            WHERE trading_date >= ?
        ),
        theme_date_metrics AS (
            SELECT
                m.theme_id,
                rp.trading_date,
                AVG(CASE WHEN m.is_eligible THEN GREATEST(LEAST(rp.perf_1w, {CURRENT_RANKING_RETURN_CAP_PCT}), -{CURRENT_RANKING_RETURN_CAP_PCT}) ELSE NULL END) AS avg_1w,
                AVG(CASE WHEN m.is_eligible THEN GREATEST(LEAST(rp.perf_1m, {CURRENT_RANKING_RETURN_CAP_PCT}), -{CURRENT_RANKING_RETURN_CAP_PCT}) ELSE NULL END) AS avg_1m,
                AVG(CASE WHEN m.is_eligible THEN GREATEST(LEAST(rp.perf_3m, {CURRENT_RANKING_RETURN_CAP_PCT}), -{CURRENT_RANKING_RETURN_CAP_PCT}) ELSE NULL END) AS avg_3m,
                AVG(CASE WHEN m.is_eligible THEN GREATEST(LEAST(rp.perf_6m, {CURRENT_RANKING_RETURN_CAP_PCT}), -{CURRENT_RANKING_RETURN_CAP_PCT}) ELSE NULL END) AS avg_6m,
                AVG(CASE WHEN NOT m.is_eligible OR rp.perf_1w IS NULL THEN NULL WHEN GREATEST(LEAST(rp.perf_1w, {CURRENT_RANKING_RETURN_CAP_PCT}), -{CURRENT_RANKING_RETURN_CAP_PCT}) > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS positive_1w_breadth_pct,
                AVG(CASE WHEN NOT m.is_eligible OR rp.perf_1m IS NULL THEN NULL WHEN GREATEST(LEAST(rp.perf_1m, {CURRENT_RANKING_RETURN_CAP_PCT}), -{CURRENT_RANKING_RETURN_CAP_PCT}) > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS positive_1m_breadth_pct,
                AVG(CASE WHEN NOT m.is_eligible OR rp.perf_3m IS NULL THEN NULL WHEN GREATEST(LEAST(rp.perf_3m, {CURRENT_RANKING_RETURN_CAP_PCT}), -{CURRENT_RANKING_RETURN_CAP_PCT}) > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS positive_3m_breadth_pct,
                SUM(CASE WHEN m.is_eligible AND rp.close IS NOT NULL THEN 1 ELSE 0 END) AS covered_eligible_constituent_count
            FROM membership m
            JOIN recent_perf rp ON rp.ticker = m.ticker
            GROUP BY m.theme_id, rp.trading_date
        )
        SELECT
            NULL AS run_id,
            tdm.trading_date AS snapshot_date,
            CAST(tdm.trading_date AS TIMESTAMP) AS snapshot_time,
            tc.theme_id,
            tc.theme,
            tc.category,
            tc.ticker_count,
            ROUND(tdm.avg_1w, 2) AS avg_1w,
            ROUND(tdm.avg_1m, 2) AS avg_1m,
            ROUND(tdm.avg_3m, 2) AS avg_3m,
            ROUND(tdm.avg_6m, 2) AS avg_6m,
            ROUND(COALESCE(tdm.positive_1w_breadth_pct, 0), 2) AS positive_1w_breadth_pct,
            ROUND(COALESCE(tdm.positive_1m_breadth_pct, 0), 2) AS positive_1m_breadth_pct,
            ROUND(COALESCE(tdm.positive_3m_breadth_pct, 0), 2) AS positive_3m_breadth_pct,
            ROUND(
                (
                    ({COMPOSITE_WEIGHTS["perf_1w"]} * COALESCE(tdm.avg_1w, 0))
                    + ({COMPOSITE_WEIGHTS["perf_1m"]} * COALESCE(tdm.avg_1m, 0))
                    + ({COMPOSITE_WEIGHTS["perf_3m"]} * COALESCE(tdm.avg_3m, 0))
                ) * LEAST(1.0, SQRT(tc.ticker_count / {float(THEME_CONFIDENCE_FULL_COUNT)})),
                2
            ) AS composite_score,
            ? AS snapshot_source,
            'ticker_history_derived' AS provenance_class,
            'ticker_daily_history_recent' AS provenance_source_label,
            tc.eligible_constituent_count AS eligible_contributor_count,
            tdm.covered_eligible_constituent_count
        FROM theme_date_metrics tdm
        JOIN theme_counts tc ON tc.theme_id = tdm.theme_id
        WHERE tc.eligible_constituent_count > 0
          AND tdm.covered_eligible_constituent_count > 0
          AND (
              CAST(tdm.covered_eligible_constituent_count AS DOUBLE)
              / CAST(tc.eligible_constituent_count AS DOUBLE)
          ) >= ?
        ORDER BY snapshot_time, theme
        """,
        params,
    ).df()
    if metrics.empty:
        return pd.DataFrame()
    return metrics[
        [
            "run_id",
            "snapshot_date",
            "snapshot_time",
            "theme_id",
            "theme",
            "category",
            "ticker_count",
            "avg_1w",
            "avg_1m",
            "avg_3m",
            "avg_6m",
            "positive_1w_breadth_pct",
            "positive_1m_breadth_pct",
            "positive_3m_breadth_pct",
            "composite_score",
            "snapshot_source",
            "provenance_class",
            "provenance_source_label",
            "eligible_contributor_count",
            "covered_eligible_constituent_count",
        ]
    ].reset_index(drop=True)


def last_refresh_run(conn) -> pd.DataFrame:
    return conn.execute("SELECT * FROM refresh_runs ORDER BY run_id DESC LIMIT 1").df()


def refresh_history(conn, limit: int = 20) -> pd.DataFrame:
    return conn.execute("SELECT * FROM refresh_runs ORDER BY run_id DESC LIMIT ?", [limit]).df()


def latest_completed_runs(conn, limit: int = 2) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT run_id, finished_at
        FROM refresh_runs
        WHERE status IN ('success', 'partial') AND finished_at IS NOT NULL
        ORDER BY run_id DESC
        LIMIT ?
        """,
        [limit],
    ).df()


def preferred_theme_snapshot_source(conn) -> str | None:
    if table_has_column(conn, "theme_snapshots", "snapshot_source"):
        row = conn.execute(
            """
            SELECT snapshot_source
            FROM theme_snapshots
            ORDER BY CASE WHEN snapshot_source = 'live' THEN 0 ELSE 1 END,
                     snapshot_time DESC,
                     run_id DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    if table_has_column(conn, "refresh_runs", "provider"):
        row = conn.execute(
            """
            SELECT COALESCE(r.provider, 'live')
            FROM theme_snapshots ts
            LEFT JOIN refresh_runs r ON r.run_id = ts.run_id
            ORDER BY CASE WHEN COALESCE(r.provider, 'live') = 'live' THEN 0 ELSE 1 END,
                     ts.snapshot_time DESC,
                     ts.run_id DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    row = conn.execute("SELECT 1 FROM theme_snapshots LIMIT 1").fetchone()
    return "live" if row else None


def preferred_ticker_snapshot_source(conn) -> str | None:
    if not table_exists(conn, "ticker_snapshots") or not table_exists(conn, "refresh_runs"):
        return None
    if table_has_column(conn, "ticker_snapshots", "snapshot_source"):
        row = conn.execute(
            """
            SELECT s.snapshot_source
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
            ORDER BY CASE WHEN s.snapshot_source = 'live' THEN 0 ELSE 1 END,
                     s.run_id DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    if table_has_column(conn, "refresh_runs", "provider"):
        row = conn.execute(
            """
            SELECT COALESCE(r.provider, 'live')
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
            ORDER BY CASE WHEN COALESCE(r.provider, 'live') = 'live' THEN 0 ELSE 1 END,
                     s.run_id DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    row = conn.execute(
        """
        SELECT 1
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE r.status IN ('success', 'partial')
        LIMIT 1
        """
    ).fetchone()
    return "live" if row else None


def theme_ticker_metrics(conn, theme_id: int, *, include_suppressed: bool = False) -> pd.DataFrame:
    preferred_source = preferred_ticker_snapshot_source(conn)
    manual_filter = "" if include_suppressed else _manual_suppression_filter_sql(conn, "m.ticker")
    suppression_table_exists = table_exists(conn, "symbol_refresh_status")
    has_manual_suppressed_col = suppression_table_exists and table_has_column(conn, "symbol_refresh_status", "manual_suppressed")
    suppression_join_membership = (
        "LEFT JOIN symbol_refresh_status sr ON upper(trim(sr.ticker)) = upper(trim(m.ticker))"
        if suppression_table_exists
        else ""
    )
    suppression_join_governed = (
        "LEFT JOIN symbol_refresh_status sr ON upper(trim(sr.ticker)) = gm.ticker"
        if suppression_table_exists
        else ""
    )
    suppression_select = (
        "COALESCE(sr.status, 'active') AS status,\n            COALESCE(sr.manual_suppressed, FALSE) AS manual_suppressed,"
        if has_manual_suppressed_col
        else "'active' AS status,\n            FALSE AS manual_suppressed,"
    )
    if not preferred_source:
        return conn.execute(
            f"""
            SELECT
                   upper(trim(m.ticker)) AS ticker,
                   {suppression_select}
            FROM theme_membership m
            {suppression_join_membership}
            WHERE theme_id BETWEEN ? AND ?
            {manual_filter}
            GROUP BY upper(trim(m.ticker)), status, manual_suppressed
            ORDER BY upper(trim(m.ticker))
            """,
            [int(theme_id), int(theme_id)],
        ).df()

    if table_has_column(conn, "ticker_snapshots", "snapshot_source"):
        ticker_source_expr = "s.snapshot_source"
    elif table_has_column(conn, "refresh_runs", "provider"):
        ticker_source_expr = "COALESCE(r.provider, 'live')"
    else:
        ticker_source_expr = "'live'"

    latest_refresh_time = conn.execute(
        f"""
        SELECT MAX(r.finished_at)
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE r.status IN ('success', 'partial')
          AND {ticker_source_expr} = ?
        """,
        [preferred_source],
    ).fetchone()[0]

    return conn.execute(
        f"""
        WITH governed_members AS (
            SELECT
                upper(trim(ticker)) AS ticker
            FROM theme_membership m
            WHERE theme_id BETWEEN ? AND ?
            {manual_filter}
            GROUP BY upper(trim(ticker))
        ),
        completed_snapshots AS (
            SELECT
                upper(trim(s.ticker)) AS ticker,
                s.price,
                s.perf_1d,
                s.perf_1w,
                s.perf_1m,
                s.perf_3m,
                s.perf_6m,
                s.market_cap,
                s.avg_volume,
                s.short_interest_pct,
                s.float_shares,
                s.adr_pct,
                s.last_updated,
                r.finished_at AS snapshot_time,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.run_id DESC) AS rn
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
              AND {ticker_source_expr} = ?
        ),
        latest_nonnull_market_caps AS (
            SELECT
                upper(trim(s.ticker)) AS ticker,
                s.market_cap,
                ROW_NUMBER() OVER (PARTITION BY upper(trim(s.ticker)) ORDER BY s.run_id DESC) AS rn
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
              AND {ticker_source_expr} = ?
              AND s.market_cap IS NOT NULL
        )
        SELECT
            gm.ticker,
            {suppression_select}
            cs.price,
            cs.perf_1d,
            cs.perf_1w,
            cs.perf_1m,
            cs.perf_3m,
            cs.perf_6m,
            COALESCE(cs.market_cap, lmc.market_cap) AS market_cap,
            cs.avg_volume,
            cs.short_interest_pct,
            cs.float_shares,
            cs.adr_pct,
            cs.last_updated,
            cs.snapshot_time,
            ? AS latest_refresh_time
        FROM governed_members gm
        {suppression_join_governed}
        LEFT JOIN completed_snapshots cs
          ON gm.ticker = cs.ticker AND cs.rn = 1
        LEFT JOIN latest_nonnull_market_caps lmc
          ON gm.ticker = lmc.ticker AND lmc.rn = 1
        ORDER BY gm.ticker
        """,
        [int(theme_id), int(theme_id), preferred_source, preferred_source, latest_refresh_time],
    ).df()


def theme_ticker_metrics_for_theme_ids(conn, theme_ids: list[int], *, include_suppressed: bool = False) -> pd.DataFrame:
    normalized_theme_ids = sorted({int(theme_id) for theme_id in theme_ids if theme_id is not None})
    if not normalized_theme_ids:
        return pd.DataFrame()

    preferred_source = preferred_ticker_snapshot_source(conn)
    manual_filter = "" if include_suppressed else _manual_suppression_filter_sql(conn, "m.ticker")
    suppression_table_exists = table_exists(conn, "symbol_refresh_status")
    has_manual_suppressed_col = suppression_table_exists and table_has_column(conn, "symbol_refresh_status", "manual_suppressed")
    placeholders = ", ".join(["?"] * len(normalized_theme_ids))
    suppression_join_membership = (
        "LEFT JOIN symbol_refresh_status sr ON upper(trim(sr.ticker)) = upper(trim(m.ticker))"
        if suppression_table_exists
        else ""
    )
    suppression_join_governed = (
        "LEFT JOIN symbol_refresh_status sr ON upper(trim(sr.ticker)) = gm.ticker"
        if suppression_table_exists
        else ""
    )
    suppression_select = (
        "COALESCE(sr.status, 'active') AS status,\n            COALESCE(sr.manual_suppressed, FALSE) AS manual_suppressed,"
        if has_manual_suppressed_col
        else "'active' AS status,\n            FALSE AS manual_suppressed,"
    )
    if not preferred_source:
        return conn.execute(
            f"""
            SELECT
                   m.theme_id,
                   upper(trim(m.ticker)) AS ticker,
                   {suppression_select}
            FROM theme_membership m
            {suppression_join_membership}
            WHERE m.theme_id IN ({placeholders})
            {manual_filter}
            GROUP BY m.theme_id, upper(trim(m.ticker)), status, manual_suppressed
            ORDER BY m.theme_id, upper(trim(m.ticker))
            """,
            normalized_theme_ids,
        ).df()

    if table_has_column(conn, "ticker_snapshots", "snapshot_source"):
        ticker_source_expr = "s.snapshot_source"
    elif table_has_column(conn, "refresh_runs", "provider"):
        ticker_source_expr = "COALESCE(r.provider, 'live')"
    else:
        ticker_source_expr = "'live'"

    latest_refresh_time = conn.execute(
        f"""
        SELECT MAX(r.finished_at)
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE r.status IN ('success', 'partial')
          AND {ticker_source_expr} = ?
        """,
        [preferred_source],
    ).fetchone()[0]

    return conn.execute(
        f"""
        WITH governed_members AS (
            SELECT
                m.theme_id,
                upper(trim(ticker)) AS ticker
            FROM theme_membership m
            WHERE m.theme_id IN ({placeholders})
            {manual_filter}
            GROUP BY m.theme_id, upper(trim(ticker))
        ),
        completed_snapshots AS (
            SELECT
                upper(trim(s.ticker)) AS ticker,
                s.price,
                s.perf_1d,
                s.perf_1w,
                s.perf_1m,
                s.perf_3m,
                s.perf_6m,
                s.market_cap,
                s.avg_volume,
                s.short_interest_pct,
                s.float_shares,
                s.adr_pct,
                s.last_updated,
                r.finished_at AS snapshot_time,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.run_id DESC) AS rn
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
              AND {ticker_source_expr} = ?
        ),
        latest_nonnull_market_caps AS (
            SELECT
                upper(trim(s.ticker)) AS ticker,
                s.market_cap,
                ROW_NUMBER() OVER (PARTITION BY upper(trim(s.ticker)) ORDER BY s.run_id DESC) AS rn
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
              AND {ticker_source_expr} = ?
              AND s.market_cap IS NOT NULL
        )
        SELECT
            gm.theme_id,
            gm.ticker,
            {suppression_select}
            cs.price,
            cs.perf_1d,
            cs.perf_1w,
            cs.perf_1m,
            cs.perf_3m,
            cs.perf_6m,
            COALESCE(cs.market_cap, lmc.market_cap) AS market_cap,
            cs.avg_volume,
            cs.short_interest_pct,
            cs.float_shares,
            cs.adr_pct,
            cs.last_updated,
            cs.snapshot_time,
            ? AS latest_refresh_time
        FROM governed_members gm
        {suppression_join_governed}
        LEFT JOIN completed_snapshots cs
          ON gm.ticker = cs.ticker AND cs.rn = 1
        LEFT JOIN latest_nonnull_market_caps lmc
          ON gm.ticker = lmc.ticker AND lmc.rn = 1
        ORDER BY gm.theme_id, gm.ticker
        """,
        [*normalized_theme_ids, preferred_source, preferred_source, latest_refresh_time],
    ).df()


def theme_snapshot_history(
    conn,
    theme_id: int,
    limit: int = 20,
    *,
    include_recent_ticker_history: bool = False,
) -> pd.DataFrame:
    def _load(active_conn=conn) -> pd.DataFrame:
        history = _historical_theme_snapshot_union(
            active_conn,
            include_recent_ticker_history=include_recent_ticker_history,
            theme_id=int(theme_id),
        )
        if history.empty:
            return pd.DataFrame()
        view = history.copy()
        if view.empty:
            return view
        return (
            view[
                [
                    "run_id",
                    "snapshot_time",
                    "ticker_count",
                    "avg_1w",
                    "avg_1m",
                    "avg_3m",
                    "positive_1w_breadth_pct",
                    "positive_1m_breadth_pct",
                    "positive_3m_breadth_pct",
                    "composite_score",
                    "snapshot_source",
                    "provenance_class",
                    "provenance_source_label",
                ]
            ]
            .sort_values(["snapshot_time", "run_id"], ascending=[False, False])
            .head(limit)
            .reset_index(drop=True)
        )

    return _with_bootstrap_recovery(_load)


def _historical_theme_boundary_debug_core(conn, theme_id: int, lookback_days: int) -> dict[str, object]:
    boundary_history = _recent_movement_theme_snapshot_union(conn)
    if boundary_history.empty:
        return {
            "resolved_window_start": None,
            "resolved_window_end": None,
            "boundary_summary": pd.DataFrame(),
            "candidate_rows": pd.DataFrame(),
        }

    start_time, end_time = _resolve_recent_movement_boundaries(boundary_history, int(lookback_days))
    if start_time is None or end_time is None:
        return {
            "resolved_window_start": None,
            "resolved_window_end": None,
            "boundary_summary": pd.DataFrame(),
            "candidate_rows": pd.DataFrame(),
        }

    theme_window = _recent_movement_theme_snapshot_union(conn, theme_id=int(theme_id), start_date=start_time)
    if theme_window.empty:
        return {
            "resolved_window_start": start_time,
            "resolved_window_end": end_time,
            "boundary_summary": pd.DataFrame(),
            "candidate_rows": pd.DataFrame(),
        }

    theme_window = theme_window[
        (pd.to_datetime(theme_window["snapshot_time"]) >= start_time)
        & (pd.to_datetime(theme_window["snapshot_time"]) <= end_time)
    ].copy()
    if theme_window.empty:
        return {
            "resolved_window_start": start_time,
            "resolved_window_end": end_time,
            "boundary_summary": pd.DataFrame(),
            "candidate_rows": pd.DataFrame(),
        }

    preferred_source = preferred_theme_snapshot_source(conn) or _preferred_ticker_history_source(conn)
    candidate_frames: list[pd.DataFrame] = []
    boundary_dates = [pd.Timestamp(start_time).date(), pd.Timestamp(end_time).date()]
    boundary_map = {
        pd.Timestamp(start_time).date(): "start",
        pd.Timestamp(end_time).date(): "end",
    }

    captured = conn.execute(
        """
        SELECT
            ts.run_id,
            CAST(ts.snapshot_time AS DATE) AS snapshot_date,
            ts.snapshot_time,
            ts.theme_id,
            ts.ticker_count,
            ts.avg_1w,
            ts.avg_1m,
            ts.avg_3m,
            ts.composite_score,
            'captured' AS provenance_class,
            ts.snapshot_source AS provenance_source_label,
            ts.snapshot_source AS market_data_source
        FROM theme_snapshots ts
        WHERE ts.theme_id = ?
          AND ts.snapshot_source = ?
          AND CAST(ts.snapshot_time AS DATE) IN (?, ?)
        """,
        [int(theme_id), preferred_source, *boundary_dates],
    ).df()
    if not captured.empty:
        candidate_frames.append(captured)

    if table_exists(conn, "reconstructed_theme_snapshots"):
        reconstructed = conn.execute(
            """
            SELECT
                r.run_id,
                r.snapshot_date,
                r.snapshot_time,
                r.theme_id,
                r.ticker_count,
                r.avg_1w,
                r.avg_1m,
                r.avg_3m,
                r.composite_score,
                r.provenance_class,
                r.provenance_source_label,
                r.market_data_source
            FROM reconstructed_theme_snapshots r
            WHERE r.theme_id = ?
              AND r.market_data_source = ?
              AND r.snapshot_date IN (?, ?)
            """,
            [int(theme_id), preferred_source, *boundary_dates],
        ).df()
        if not reconstructed.empty:
            candidate_frames.append(reconstructed)

    if ENABLE_RECENT_TICKER_HISTORY_PREFERRED_RECONSTRUCTION:
        ticker_history_derived = _recent_ticker_history_theme_history(
            conn,
            preferred_source,
            theme_id=int(theme_id),
            start_date=start_time,
        )
        if not ticker_history_derived.empty:
            ticker_history_derived = ticker_history_derived[
                pd.to_datetime(ticker_history_derived["snapshot_date"]).dt.date.isin(boundary_dates)
            ].copy()
            ticker_history_derived["market_data_source"] = ticker_history_derived["snapshot_source"]
            if not ticker_history_derived.empty:
                candidate_frames.append(
                    ticker_history_derived[
                        [
                            "run_id",
                            "snapshot_date",
                            "snapshot_time",
                            "theme_id",
                            "ticker_count",
                            "avg_1w",
                            "avg_1m",
                            "avg_3m",
                            "composite_score",
                            "provenance_class",
                            "provenance_source_label",
                            "market_data_source",
                        ]
                    ]
                )

    candidate_rows = pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()
    if candidate_rows.empty:
        return {
            "resolved_window_start": start_time,
            "resolved_window_end": end_time,
            "boundary_summary": pd.DataFrame(),
            "candidate_rows": pd.DataFrame(),
        }

    candidate_rows["snapshot_time"] = pd.to_datetime(candidate_rows["snapshot_time"], errors="coerce")
    candidate_rows["snapshot_date"] = pd.to_datetime(candidate_rows["snapshot_date"]).dt.date
    candidate_rows["precedence_rank"] = candidate_rows["provenance_class"].map(
        {"ticker_history_derived": 0, "captured": 1, "reconstructed": 2}
    ).fillna(9)
    candidate_rows["boundary_label"] = candidate_rows["snapshot_date"].map(boundary_map).fillna("other")

    winners = theme_window[pd.to_datetime(theme_window["snapshot_time"]).isin([start_time, end_time])].copy()
    winners["snapshot_time"] = pd.to_datetime(winners["snapshot_time"], errors="coerce")
    winners["snapshot_date"] = pd.to_datetime(winners["snapshot_time"]).dt.date
    winners["boundary_label"] = winners["snapshot_date"].map(boundary_map).fillna("other")
    winners = winners.rename(
        columns={
            "provenance_class": "winner_provenance_class",
            "provenance_source_label": "winner_provenance_source_label",
            "snapshot_source": "winner_market_data_source",
        }
    )

    candidate_rows = candidate_rows.merge(
        winners[
            [
                "snapshot_date",
                "boundary_label",
                "winner_provenance_class",
                "winner_provenance_source_label",
                "winner_market_data_source",
            ]
        ],
        on=["snapshot_date", "boundary_label"],
        how="left",
    )
    candidate_rows["selected"] = (
        (candidate_rows["provenance_class"] == candidate_rows["winner_provenance_class"])
        & (candidate_rows["provenance_source_label"] == candidate_rows["winner_provenance_source_label"])
    )
    candidate_rows["suppression_honored"] = candidate_rows["provenance_class"].map(
        {
            "captured": "captured snapshot (suppression unknown)",
            "ticker_history_derived": "yes",
            "reconstructed": "yes",
        }
    ).fillna("unknown")

    boundary_summary_rows: list[dict[str, object]] = []
    for boundary_date, boundary_label in boundary_map.items():
        boundary_candidates = candidate_rows[candidate_rows["snapshot_date"] == boundary_date].copy()
        winner = winners[winners["snapshot_date"] == boundary_date]
        winner_row = winner.iloc[0] if not winner.empty else None
        boundary_summary_rows.append(
            {
                "boundary_label": boundary_label,
                "resolved_snapshot_date": boundary_date,
                "resolved_snapshot_time": winner_row["snapshot_time"] if winner_row is not None else None,
                "winner_provenance_class": winner_row["winner_provenance_class"] if winner_row is not None else None,
                "winner_provenance_source_label": winner_row["winner_provenance_source_label"] if winner_row is not None else None,
                "winner_market_data_source": winner_row["winner_market_data_source"] if winner_row is not None else None,
                "captured_candidate_exists": bool((boundary_candidates["provenance_class"] == "captured").any()),
                "ticker_history_derived_candidate_exists": bool((boundary_candidates["provenance_class"] == "ticker_history_derived").any()),
                "reconstructed_candidate_exists": bool((boundary_candidates["provenance_class"] == "reconstructed").any()),
                "reconstructed_overridden": bool(
                    (boundary_candidates["provenance_class"] == "reconstructed").any()
                    and not (boundary_candidates["selected"] & (boundary_candidates["provenance_class"] == "reconstructed")).any()
                ),
                "ticker_history_derived_overridden": bool(
                    (boundary_candidates["provenance_class"] == "ticker_history_derived").any()
                    and not (boundary_candidates["selected"] & (boundary_candidates["provenance_class"] == "ticker_history_derived")).any()
                ),
                "suppression_honored_in_winner": (
                    {
                        "captured": "captured snapshot (suppression unknown)",
                        "ticker_history_derived": "yes",
                        "reconstructed": "yes",
                    }.get(winner_row["winner_provenance_class"], "unknown")
                    if winner_row is not None
                    else "unknown"
                ),
            }
        )

    boundary_summary = pd.DataFrame(boundary_summary_rows)
    candidate_rows = candidate_rows[
        [
            "boundary_label",
            "snapshot_date",
            "snapshot_time",
            "provenance_class",
            "provenance_source_label",
            "market_data_source",
            "ticker_count",
            "avg_1w",
            "avg_1m",
            "avg_3m",
            "composite_score",
            "precedence_rank",
            "selected",
            "suppression_honored",
        ]
    ].sort_values(["snapshot_date", "precedence_rank", "snapshot_time"], ascending=[True, True, False]).reset_index(drop=True)

    return {
        "resolved_window_start": start_time,
        "resolved_window_end": end_time,
        "boundary_summary": boundary_summary,
        "candidate_rows": candidate_rows,
    }


def historical_theme_boundary_debug(conn, theme_id: int, lookback_days: int) -> dict[str, object]:
    def _load(active_conn=conn) -> dict[str, object]:
        return _historical_theme_boundary_debug_core(active_conn, theme_id, lookback_days)

    return _with_bootstrap_recovery(_load)


def _historical_theme_movement_row_audit_core(conn, theme_id: int, lookback_days: int) -> dict[str, object]:
    boundary_debug = _historical_theme_boundary_debug_core(conn, theme_id, lookback_days)
    boundary_summary = boundary_debug.get("boundary_summary", pd.DataFrame())
    candidate_rows = boundary_debug.get("candidate_rows", pd.DataFrame())
    start_time = boundary_debug.get("resolved_window_start")
    end_time = boundary_debug.get("resolved_window_end")

    base = {
        "resolved_window_start": start_time,
        "resolved_window_end": end_time,
        "boundary_summary": boundary_summary,
        "candidate_rows": candidate_rows,
        "aggregate_summary": pd.DataFrame(),
        "constituent_rows": pd.DataFrame(),
        "audit_available": False,
        "audit_reason": "No resolved movement boundary rows available for this theme/window.",
        "theme_identity": pd.DataFrame(),
    }
    if boundary_summary.empty or start_time is None or end_time is None:
        return base

    selected_boundary_rows = candidate_rows[candidate_rows["selected"] == True].copy() if not candidate_rows.empty else pd.DataFrame()
    if selected_boundary_rows.empty:
        base["audit_reason"] = "No selected winning boundary rows were found for this theme/window."
        return base

    winners = set(selected_boundary_rows["provenance_class"].dropna().astype(str).tolist())
    if winners != {"ticker_history_derived"}:
        base["audit_reason"] = (
            "Constituent-level capped audit is only available when the resolved movement boundary rows are "
            "`ticker_history_derived`. Use the lineage table above for captured/reconstructed winners."
        )
        return base

    latest_winner = selected_boundary_rows.sort_values(["snapshot_time", "snapshot_date"]).tail(1).iloc[0]
    base["theme_identity"] = pd.DataFrame(
        [
            {
                "theme_id": int(theme_id),
                "theme": latest_winner.get("theme") if "theme" in selected_boundary_rows.columns else None,
                "category": latest_winner.get("category") if "category" in selected_boundary_rows.columns else None,
                "winner_provenance_class": "ticker_history_derived",
                "winner_provenance_source_label": latest_winner.get("provenance_source_label"),
            }
        ]
    )

    membership = conn.execute(
        """
        SELECT
            upper(trim(m.ticker)) AS ticker,
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            COALESCE(s.status, 'active') AS status,
            CASE WHEN COALESCE(s.status, 'active') = 'refresh_suppressed' THEN FALSE ELSE TRUE END AS is_eligible
        FROM themes t
        JOIN theme_membership m ON m.theme_id = t.id
        LEFT JOIN symbol_refresh_status s ON upper(trim(s.ticker)) = upper(trim(m.ticker))
        WHERE t.id = ?
        ORDER BY ticker
        """,
        [int(theme_id)],
    ).df()
    if membership.empty:
        base["audit_reason"] = "No governed membership rows were found for this theme."
        return base

    eligible_members = membership[membership["is_eligible"] == True].copy()
    if eligible_members.empty:
        base["audit_reason"] = "This theme has no historical-eligible governed constituents after suppression filtering."
        return base

    buffer_start = pd.Timestamp(start_time) - pd.Timedelta(days=TICKER_HISTORY_BUFFER_DAYS)
    daily_history = conn.execute(
        """
        SELECT
            upper(trim(h.ticker)) AS ticker,
            h.trading_date,
            h.close
        FROM ticker_daily_history h
        WHERE h.market_data_source = ?
          AND upper(trim(h.ticker)) IN (
              SELECT upper(trim(ticker))
              FROM theme_membership
              WHERE theme_id = ?
          )
          AND h.trading_date BETWEEN ? AND ?
        ORDER BY ticker, trading_date
        """,
        [
            str(latest_winner.get("market_data_source") or latest_winner.get("provenance_source_label") or "live"),
            int(theme_id),
            pd.Timestamp(buffer_start).date(),
            pd.Timestamp(end_time).date(),
        ],
    ).df()
    if daily_history.empty:
        base["audit_reason"] = "No stored ticker_daily_history rows were available for the winning movement source."
        return base

    daily_history = daily_history.sort_values(["ticker", "trading_date"]).copy()
    daily_history["trading_date"] = pd.to_datetime(daily_history["trading_date"]).dt.date
    grouped = daily_history.groupby("ticker")["close"]
    daily_history["perf_1w_raw"] = ((grouped.transform(lambda s: s / s.shift(5))) - 1.0) * 100.0
    daily_history["perf_1m_raw"] = ((grouped.transform(lambda s: s / s.shift(21))) - 1.0) * 100.0
    daily_history["perf_3m_raw"] = ((grouped.transform(lambda s: s / s.shift(63))) - 1.0) * 100.0
    daily_history["perf_6m_raw"] = ((grouped.transform(lambda s: s / s.shift(126))) - 1.0) * 100.0
    for perf_col in ("perf_1w", "perf_1m", "perf_3m", "perf_6m"):
        daily_history[f"{perf_col}_capped"] = pd.to_numeric(
            daily_history[f"{perf_col}_raw"], errors="coerce"
        ).clip(-CURRENT_RANKING_RETURN_CAP_PCT, CURRENT_RANKING_RETURN_CAP_PCT)

    boundary_dates = {
        "start": pd.Timestamp(start_time).date(),
        "end": pd.Timestamp(end_time).date(),
    }
    constituent_frames: list[pd.DataFrame] = []
    aggregate_rows: list[dict[str, object]] = []
    distinct_governed_tickers = int(membership["ticker"].nunique())

    for boundary_label, boundary_date in boundary_dates.items():
        rows = membership.merge(
            daily_history[daily_history["trading_date"] == boundary_date],
            on="ticker",
            how="left",
        ).copy()
        rows["boundary_label"] = boundary_label
        eligible_contributor_count = int(rows["is_eligible"].sum())
        covered_eligible_contributor_count = int(((rows["is_eligible"] == True) & rows["close"].notna()).sum())
        passed_historical_gate = bool(
            eligible_contributor_count >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS
            and covered_eligible_contributor_count >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS
        )
        rows["passed_historical_gate"] = passed_historical_gate
        constituent_frames.append(rows)

        eligible_rows = rows[(rows["is_eligible"] == True) & rows["close"].notna()].copy()
        aggregate_rows.append(
            {
                "boundary_label": boundary_label,
                "boundary_date": boundary_date,
                "theme_id": int(theme_id),
                "theme": rows["theme"].dropna().iloc[0] if rows["theme"].notna().any() else None,
                "category": rows["category"].dropna().iloc[0] if rows["category"].notna().any() else None,
                "winner_provenance_class": "ticker_history_derived",
                "winner_provenance_source_label": latest_winner.get("provenance_source_label"),
                "distinct_governed_tickers": distinct_governed_tickers,
                "eligible_contributor_count": eligible_contributor_count,
                "covered_eligible_contributor_count": covered_eligible_contributor_count,
                "historical_gate_min": int(CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS),
                "passed_historical_gate": passed_historical_gate,
                "avg_1w": round(float(eligible_rows["perf_1w_capped"].mean()), 2) if not eligible_rows.empty else None,
                "avg_1m": round(float(eligible_rows["perf_1m_capped"].mean()), 2) if not eligible_rows.empty else None,
                "avg_3m": round(float(eligible_rows["perf_3m_capped"].mean()), 2) if not eligible_rows.empty else None,
                "avg_6m": round(float(eligible_rows["perf_6m_capped"].mean()), 2) if not eligible_rows.empty else None,
                "positive_1m_breadth_pct": (
                    round(float((eligible_rows["perf_1m_capped"] > 0).mean() * 100.0), 2)
                    if not eligible_rows.empty and eligible_rows["perf_1m_capped"].notna().any()
                    else None
                ),
                "composite_score": (
                    round(
                        (
                            (COMPOSITE_WEIGHTS["perf_1w"] * float(eligible_rows["perf_1w_capped"].mean()))
                            + (COMPOSITE_WEIGHTS["perf_1m"] * float(eligible_rows["perf_1m_capped"].mean()))
                            + (COMPOSITE_WEIGHTS["perf_3m"] * float(eligible_rows["perf_3m_capped"].mean()))
                        ) * _theme_confidence_factor_for_history(int(rows["is_eligible"].sum())),
                        2,
                    )
                    if not eligible_rows.empty
                    else None
                ),
            }
        )

    aggregate_summary = pd.DataFrame(aggregate_rows)
    constituent_rows = pd.concat(constituent_frames, ignore_index=True) if constituent_frames else pd.DataFrame()
    if not constituent_rows.empty:
        constituent_rows["raw_vs_capped_1w"] = constituent_rows.apply(
            lambda row: None
            if pd.isna(row.get("perf_1w_raw"))
            else f"{float(row.get('perf_1w_raw')):.2f} -> {float(row.get('perf_1w_capped')):.2f}",
            axis=1,
        )
        constituent_rows["raw_vs_capped_1m"] = constituent_rows.apply(
            lambda row: None
            if pd.isna(row.get("perf_1m_raw"))
            else f"{float(row.get('perf_1m_raw')):.2f} -> {float(row.get('perf_1m_capped')):.2f}",
            axis=1,
        )
        constituent_rows["raw_vs_capped_3m"] = constituent_rows.apply(
            lambda row: None
            if pd.isna(row.get("perf_3m_raw"))
            else f"{float(row.get('perf_3m_raw')):.2f} -> {float(row.get('perf_3m_capped')):.2f}",
            axis=1,
        )
        constituent_rows["raw_vs_capped_6m"] = constituent_rows.apply(
            lambda row: None
            if pd.isna(row.get("perf_6m_raw"))
            else f"{float(row.get('perf_6m_raw')):.2f} -> {float(row.get('perf_6m_capped')):.2f}",
            axis=1,
        )
        constituent_rows = constituent_rows[
            [
                "boundary_label",
                "ticker",
                "status",
                "is_eligible",
                "close",
                "perf_1w_raw",
                "perf_1w_capped",
                "raw_vs_capped_1w",
                "perf_1m_raw",
                "perf_1m_capped",
                "raw_vs_capped_1m",
                "perf_3m_raw",
                "perf_3m_capped",
                "raw_vs_capped_3m",
                "perf_6m_raw",
                "perf_6m_capped",
                "raw_vs_capped_6m",
                "passed_historical_gate",
            ]
        ].sort_values(["boundary_label", "ticker"]).reset_index(drop=True)

    base.update(
        {
            "aggregate_summary": aggregate_summary,
            "constituent_rows": constituent_rows,
            "audit_available": True,
            "audit_reason": "Constituent-level audit is available because both winning boundary rows use ticker-history-derived provenance.",
        }
    )
    return base


def historical_theme_movement_row_audit(conn, theme_id: int, lookback_days: int) -> dict[str, object]:
    def _load(active_conn=conn) -> dict[str, object]:
        return _historical_theme_movement_row_audit_core(active_conn, theme_id, lookback_days)

    return _with_bootstrap_recovery(_load)


def theme_history_window(conn, lookback_days: int) -> pd.DataFrame:
    boundary_history = _recent_movement_theme_snapshot_union(conn)
    if boundary_history.empty:
        return pd.DataFrame()

    start_time, end_time = _resolve_recent_movement_boundaries(boundary_history, int(lookback_days))
    if start_time is None or end_time is None:
        return pd.DataFrame()

    history = _recent_movement_theme_snapshot_union(conn, start_date=start_time)
    if history.empty:
        return pd.DataFrame()

    window = history[(pd.to_datetime(history["snapshot_time"]) >= start_time) & (pd.to_datetime(history["snapshot_time"]) <= end_time)].copy()
    return window.sort_values(["snapshot_time", "composite_score"], ascending=[True, False]).reset_index(drop=True)


def top_theme_movers(conn, lookback_days: int, top_n: int = 20) -> pd.DataFrame:
    history = theme_history_window(conn, lookback_days)
    if history.empty:
        return pd.DataFrame()

    history = history.sort_values(["theme_id", "snapshot_time"]).copy()
    first = history.groupby("theme_id", as_index=False).first()
    last = history.groupby("theme_id", as_index=False).last()
    merged = first[["theme_id", "theme", "composite_score", "positive_1m_breadth_pct"]].merge(
        last[["theme_id", "theme", "composite_score", "positive_1m_breadth_pct"]],
        on=["theme_id", "theme"],
        suffixes=("_start", "_end"),
    )
    merged["delta_composite"] = merged["composite_score_end"] - merged["composite_score_start"]
    merged["delta_breadth"] = merged["positive_1m_breadth_pct_end"] - merged["positive_1m_breadth_pct_start"]
    merged = merged.rename(
        columns={
            "composite_score_start": "start_composite",
            "composite_score_end": "end_composite",
            "positive_1m_breadth_pct_start": "start_breadth",
            "positive_1m_breadth_pct_end": "end_breadth",
        }
    )
    merged = merged.round(2).sort_values(["end_composite", "theme"], ascending=[False, True]).head(top_n)
    return merged[["theme_id", "theme", "start_composite", "end_composite", "delta_composite", "start_breadth", "end_breadth", "delta_breadth"]].reset_index(drop=True)


def top_n_membership_changes(conn, lookback_days: int, top_n: int = 20) -> tuple[list[str], list[str]]:
    history = theme_history_window(conn, lookback_days)
    if history.empty:
        return [], []
    boundary_times = pd.to_datetime(history["snapshot_time"]).dropna().drop_duplicates().sort_values()
    if len(boundary_times) < 2:
        return [], []
    start_time = boundary_times.iloc[0]
    end_time = boundary_times.iloc[-1]
    start_top = (
        history[pd.to_datetime(history["snapshot_time"]) == start_time]
        .sort_values(["composite_score", "theme", "theme_id"], ascending=[False, True, True])
        .head(top_n)
    )
    end_top = (
        history[pd.to_datetime(history["snapshot_time"]) == end_time]
        .sort_values(["composite_score", "theme", "theme_id"], ascending=[False, True, True])
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


def theme_health_overview(conn, low_constituent_threshold: int, failure_window_days: int = 14) -> pd.DataFrame:
    preferred_source = preferred_theme_snapshot_source(conn)
    theme_source_filter = preferred_source or "__no_source__"
    theme_source_expr = _theme_snapshot_source_expr(conn)
    updated_at_expr = "t.updated_at" if table_has_column(conn, "themes", "updated_at") else "NULL"
    member_join_filter = _manual_suppression_filter_sql(conn, "m.ticker")
    return conn.execute(
        f"""
        WITH member_counts AS (
            SELECT t.id AS theme_id, COUNT(DISTINCT upper(trim(m.ticker))) AS constituent_count
            FROM themes t
            LEFT JOIN theme_membership m ON t.id = m.theme_id{member_join_filter}
            GROUP BY t.id
        ),
        latest_snap AS (
            SELECT theme_id, MAX(snapshot_time) AS latest_snapshot_time
            FROM theme_snapshots
            WHERE {theme_source_expr} = ?
            GROUP BY theme_id
        ),
        live_failures_by_theme AS (
            SELECT theme_id, COUNT(*) AS live_failure_count_recent
            FROM (
                SELECT DISTINCT
                    m.theme_id,
                    f.run_id,
                    upper(trim(f.ticker)) AS failure_ticker,
                    f.created_at
                FROM refresh_failures f
                JOIN refresh_runs r ON r.run_id = f.run_id
                JOIN theme_membership m ON upper(trim(m.ticker)) = upper(trim(f.ticker))
                WHERE r.provider = 'live'
                  AND f.created_at >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')
                  {_manual_suppression_filter_sql(conn, 'm.ticker')}
            ) deduped_failures
            GROUP BY theme_id
        )
        SELECT
            t.id AS theme_id,
            t.name AS theme_name,
            t.category,
            t.is_active,
            {updated_at_expr} AS updated_at,
            mc.constituent_count,
            (mc.constituent_count > 0 AND mc.constituent_count < ?) AS low_count_flag,
            (mc.constituent_count = 0) AS empty_theme_flag,
            COALESCE(lf.live_failure_count_recent, 0) AS live_failure_count_recent,
            ls.latest_snapshot_time,
            CASE
              WHEN mc.constituent_count = 0 THEN 'needs_attention'
              WHEN t.is_active = FALSE AND mc.constituent_count > 0 THEN 'needs_attention'
              WHEN COALESCE(lf.live_failure_count_recent, 0) >= 3 THEN 'watch'
              WHEN mc.constituent_count > 0 AND mc.constituent_count < ? THEN 'watch'
              ELSE 'healthy'
            END AS health_status
        FROM themes t
        JOIN member_counts mc ON mc.theme_id = t.id
        LEFT JOIN latest_snap ls ON ls.theme_id = t.id
        LEFT JOIN live_failures_by_theme lf ON lf.theme_id = t.id
        ORDER BY
          CASE health_status WHEN 'needs_attention' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
          theme_name
        """,
        [theme_source_filter, failure_window_days, low_constituent_threshold, low_constituent_threshold],
    ).df()


def snapshot_counts(conn) -> pd.DataFrame:
    counts = _with_bootstrap_recovery(
        lambda active_conn=conn: active_conn.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM ticker_snapshots) AS ticker_snapshot_rows,
              (SELECT COUNT(*) FROM theme_snapshots) AS theme_snapshot_rows,
              (SELECT COUNT(DISTINCT run_id) FROM theme_snapshots) AS runs_with_theme_snapshots
            """
        ).df()
    )
    if not counts.empty:
        return counts
    return pd.DataFrame(
        [
            {
                "ticker_snapshot_rows": 0,
                "theme_snapshot_rows": 0,
                "runs_with_theme_snapshots": 0,
            }
        ]
    )


def row_counts(conn) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT 'themes' AS table_name, COUNT(*) AS row_count FROM themes
        UNION ALL
        SELECT 'theme_membership', COUNT(*) FROM theme_membership
        UNION ALL
        SELECT 'ticker_snapshots', COUNT(*) FROM ticker_snapshots
        UNION ALL
        SELECT 'theme_snapshots', COUNT(*) FROM theme_snapshots
        UNION ALL
        SELECT 'refresh_runs', COUNT(*) FROM refresh_runs
        UNION ALL
        SELECT 'refresh_failures', COUNT(*) FROM refresh_failures
        UNION ALL
        SELECT 'refresh_run_tickers', COUNT(*) FROM refresh_run_tickers
        UNION ALL
        SELECT 'symbol_refresh_status', COUNT(*) FROM symbol_refresh_status
        UNION ALL
        SELECT 'theme_suggestions', COUNT(*) FROM theme_suggestions
        UNION ALL
        SELECT 'ticker_daily_history', COUNT(*) FROM ticker_daily_history
        """
    ).df()


def synthetic_data_active(conn) -> bool:
    row = conn.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM theme_snapshots WHERE snapshot_source='synthetic_backfill') +
          (SELECT COUNT(*) FROM ticker_snapshots WHERE snapshot_source='synthetic_backfill')
        """
    ).fetchone()
    return bool(row and row[0] and int(row[0]) > 0)


def theme_history_last_n_snapshots(
    conn,
    theme_id: int,
    snapshot_limit: int = 14,
    *,
    include_recent_ticker_history: bool = False,
) -> pd.DataFrame:
    return theme_snapshot_history(
        conn,
        int(theme_id),
        limit=snapshot_limit,
        include_recent_ticker_history=include_recent_ticker_history,
    )


def ticker_history_last_n_snapshots(conn, ticker: str, snapshot_limit: int = 14) -> pd.DataFrame:
    preferred_source = preferred_ticker_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT s.run_id, s.ticker, s.price, s.perf_1w, s.perf_1m, s.perf_3m, s.perf_6m,
               s.market_cap, s.avg_volume, s.last_updated,
               r.finished_at AS snapshot_time, s.snapshot_source
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE s.ticker = ?
          AND r.status IN ('success', 'partial')
          AND s.snapshot_source = ?
        ORDER BY s.run_id DESC
        LIMIT ?
        """,
        [ticker.strip().upper(), preferred_source, snapshot_limit],
    ).df()


def ticker_history_last_n_trading_days(conn, ticker: str, trading_day_limit: int = 140) -> pd.DataFrame:
    preferred_source = _preferred_ticker_history_source(conn)
    if not preferred_source or not table_exists(conn, "ticker_daily_history"):
        return pd.DataFrame()

    atr_select = "atr_14, atr_pct_14," if table_has_column(conn, "ticker_daily_history", "atr_14") else "NULL AS atr_14, NULL AS atr_pct_14,"

    history = conn.execute(
        f"""
        SELECT
            ticker,
            trading_date AS snapshot_date,
            close,
            {atr_select}
            market_data_source,
            updated_at,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, trading_date
                ORDER BY updated_at DESC
            ) AS rn
        FROM ticker_daily_history
        WHERE ticker = ?
          AND market_data_source = ?
        QUALIFY rn = 1
        ORDER BY trading_date DESC
        LIMIT ?
        """,
        [ticker.strip().upper(), preferred_source, int(trading_day_limit)],
    ).df()
    if history.empty:
        return history

    history = history.sort_values(["ticker", "snapshot_date"]).copy()
    grouped = history.groupby("ticker")["close"]
    history["perf_1w"] = ((grouped.transform(lambda s: s / s.shift(5))) - 1.0) * 100.0
    history["perf_1m"] = ((grouped.transform(lambda s: s / s.shift(21))) - 1.0) * 100.0
    history["perf_3m"] = ((grouped.transform(lambda s: s / s.shift(63))) - 1.0) * 100.0
    history["perf_6m"] = ((grouped.transform(lambda s: s / s.shift(126))) - 1.0) * 100.0
    history["atr_14"] = pd.to_numeric(history.get("atr_14"), errors="coerce")
    history["atr_pct_14"] = pd.to_numeric(history.get("atr_pct_14"), errors="coerce")
    history["perf_1w_atr_units"] = np.where(
        history["atr_14"].notna() & (history["atr_14"] != 0) & grouped.shift(5).notna(),
        (history["close"] - grouped.shift(5)) / history["atr_14"],
        np.nan,
    )
    history["perf_1m_atr_units"] = np.where(
        history["atr_14"].notna() & (history["atr_14"] != 0) & grouped.shift(21).notna(),
        (history["close"] - grouped.shift(21)) / history["atr_14"],
        np.nan,
    )
    return history[
        [
            "ticker",
            "snapshot_date",
            "close",
            "atr_14",
            "atr_pct_14",
            "perf_1w",
            "perf_1m",
            "perf_3m",
            "perf_6m",
            "perf_1w_atr_units",
            "perf_1m_atr_units",
            "market_data_source",
        ]
    ].copy()


def latest_theme_snapshots(conn) -> pd.DataFrame:
    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT *
        FROM theme_snapshots
        WHERE snapshot_source = ?
        QUALIFY ROW_NUMBER() OVER (PARTITION BY theme_id ORDER BY snapshot_time DESC, run_id DESC) = 1
        """,
        [preferred_source],
    ).df()


def latest_ticker_snapshots(conn) -> pd.DataFrame:
    preferred_source = preferred_ticker_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT s.*, r.finished_at AS snapshot_time
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE r.status IN ('success', 'partial')
          AND s.snapshot_source = ?
        QUALIFY ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.run_id DESC) = 1
        """,
        [preferred_source],
    ).df()


def latest_canonical_theme_daily_snapshots(conn) -> pd.DataFrame:
    if not table_exists(conn, "canonical_theme_daily_snapshots"):
        return pd.DataFrame()
    return conn.execute(
        """
        WITH latest_date AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM canonical_theme_daily_snapshots
        )
        SELECT *
        FROM canonical_theme_daily_snapshots
        WHERE snapshot_date = (SELECT snapshot_date FROM latest_date)
        ORDER BY canonical_rank ASC NULLS LAST, theme ASC
        """
    ).df()


def canonical_theme_rank_history(conn, theme_id: int, days: int = 10) -> pd.DataFrame:
    if not table_exists(conn, "canonical_theme_daily_snapshots"):
        return pd.DataFrame()
    limit_days = max(1, int(days))
    return conn.execute(
        """
        WITH recent_dates AS (
            SELECT DISTINCT snapshot_date
            FROM canonical_theme_daily_snapshots
            ORDER BY snapshot_date DESC
            LIMIT ?
        )
        SELECT
            c.snapshot_date,
            c.snapshot_time,
            c.theme_id,
            c.theme,
            c.category,
            c.canonical_rank,
            c.standardized_composite_score
        FROM canonical_theme_daily_snapshots c
        JOIN recent_dates d ON d.snapshot_date = c.snapshot_date
        WHERE c.theme_id = ?
        ORDER BY c.snapshot_date ASC
        """,
        [limit_days, int(theme_id)],
    ).df()


def canonical_theme_leadership_rank_history_long(
    conn,
    theme_ids: list[int],
    lookback_points: int = 10,
) -> pd.DataFrame:
    normalized_theme_ids = sorted({int(theme_id) for theme_id in theme_ids or []})
    empty = pd.DataFrame(
        columns=[
            "theme_id",
            "theme",
            "category",
            "snapshot_date",
            "snapshot_time",
            "rank",
            "standardized_composite_score",
            "snapshot_source",
            "extract_session",
            "canonical_reason",
        ]
    )
    if not normalized_theme_ids or not table_exists(conn, "canonical_theme_daily_snapshots"):
        return empty

    limit_points = max(int(lookback_points), 1)
    return conn.execute(
        """
        WITH recent_dates AS (
            SELECT DISTINCT snapshot_date
            FROM canonical_theme_daily_snapshots
            ORDER BY snapshot_date DESC
            LIMIT ?
        )
        SELECT
            c.theme_id,
            c.theme,
            c.category,
            c.snapshot_date,
            c.snapshot_time,
            c.canonical_rank AS rank,
            c.standardized_composite_score,
            c.snapshot_source,
            c.extract_session,
            c.canonical_reason
        FROM canonical_theme_daily_snapshots c
        JOIN recent_dates d ON d.snapshot_date = c.snapshot_date
        WHERE c.theme_id IN (SELECT UNNEST(?::BIGINT[]))
          AND c.canonical_rank IS NOT NULL
        ORDER BY c.snapshot_date ASC, c.canonical_rank ASC, c.theme ASC
        """,
        [limit_points, normalized_theme_ids],
    ).df()


def canonical_theme_leadership_rank_history(
    conn,
    theme_ids: list[int],
    lookback_points: int = 7,
) -> pd.DataFrame:
    normalized_theme_ids = sorted({int(theme_id) for theme_id in theme_ids or []})
    empty = pd.DataFrame(
        columns=[
            "theme_id",
            "rank_history",
            "rank_history_points",
            "rank_history_start_date",
            "rank_history_end_date",
        ]
    )
    if not normalized_theme_ids:
        return empty

    history_long = canonical_theme_leadership_rank_history_long(
        conn,
        normalized_theme_ids,
        lookback_points=int(lookback_points),
    )
    if history_long.empty:
        return empty

    history_long = history_long.copy()
    history_long["snapshot_date"] = pd.to_datetime(history_long["snapshot_date"], errors="coerce").dt.date
    history_long["rank"] = pd.to_numeric(history_long["rank"], errors="coerce")
    history_long = history_long.dropna(subset=["snapshot_date", "rank"])
    if history_long.empty:
        return empty

    recent_dates = sorted(history_long["snapshot_date"].dropna().unique().tolist())[-max(int(lookback_points), 1) :]
    if not recent_dates:
        return empty

    rows: list[dict[str, object]] = []
    for theme_id in normalized_theme_ids:
        theme_rows = history_long[history_long["theme_id"] == int(theme_id)].copy()
        series_by_date = {
            snapshot_date: float(rank)
            for snapshot_date, rank in zip(theme_rows["snapshot_date"], theme_rows["rank"])
            if snapshot_date is not None and rank is not None and not pd.isna(rank)
        }
        rank_history = [series_by_date[snapshot_date] for snapshot_date in recent_dates if snapshot_date in series_by_date]
        point_count = sum(1 for value in rank_history if value is not None and not pd.isna(value))
        rows.append(
            {
                "theme_id": int(theme_id),
                "rank_history": rank_history if point_count >= 2 else None,
                "rank_history_points": int(point_count),
                "rank_history_start_date": recent_dates[0],
                "rank_history_end_date": recent_dates[-1],
            }
        )
    return pd.DataFrame(rows)


def canonical_theme_history_window(conn, lookback_days: int) -> pd.DataFrame:
    if not table_exists(conn, "canonical_theme_daily_snapshots"):
        return pd.DataFrame()
    lookback_days = max(int(lookback_days), 1)
    latest_row = conn.execute(
        """
        SELECT MAX(snapshot_date) AS latest_snapshot_date
        FROM canonical_theme_daily_snapshots
        """
    ).fetchone()
    latest_snapshot_date = latest_row[0] if latest_row else None
    if latest_snapshot_date is None:
        return pd.DataFrame()
    start_date = pd.Timestamp(latest_snapshot_date) - pd.Timedelta(days=lookback_days)
    return conn.execute(
        """
        SELECT
            snapshot_time,
            snapshot_date,
            theme_id,
            theme,
            category,
            ticker_count,
            avg_1w,
            avg_1m,
            avg_3m,
            positive_1m_breadth_pct,
            standardized_composite_score AS composite_score,
            snapshot_source,
            'canonical_daily' AS provenance_class,
            canonical_rank AS rank,
            extract_session,
            canonical_reason
        FROM canonical_theme_daily_snapshots
        WHERE snapshot_date >= ?
          AND snapshot_date <= ?
          AND canonical_rank IS NOT NULL
        ORDER BY snapshot_time ASC, canonical_rank ASC, theme ASC
        """,
        [start_date.date(), latest_snapshot_date],
    ).df()


def canonical_theme_snapshot_counts(conn) -> pd.DataFrame:
    if not table_exists(conn, "canonical_theme_daily_snapshots"):
        return pd.DataFrame(
            [{"canonical_snapshot_dates": 0, "canonical_snapshot_rows": 0, "latest_canonical_snapshot_date": None}]
        )
    return conn.execute(
        """
        SELECT
            COUNT(DISTINCT snapshot_date) AS canonical_snapshot_dates,
            COUNT(*) AS canonical_snapshot_rows,
            MAX(snapshot_date) AS latest_canonical_snapshot_date
        FROM canonical_theme_daily_snapshots
        """
    ).df()


def canonical_daily_window_status(conn) -> pd.DataFrame:
    empty = pd.DataFrame(
        [
            {
                "latest_expected_trading_date": None,
                "latest_raw_canonical_date": None,
                "latest_ranked_canonical_date": None,
                "raw_vs_ranked_date_differs": False,
            }
        ]
    )

    def _load(active_conn=conn) -> pd.DataFrame:
        if not table_exists(active_conn, "canonical_theme_daily_snapshots"):
            if not table_exists(active_conn, "ticker_daily_history"):
                return empty.copy()

        preferred_source = _preferred_ticker_history_source(active_conn)
        latest_expected = None
        if preferred_source and table_exists(active_conn, "ticker_daily_history"):
            expected_row = active_conn.execute(
                """
                SELECT MAX(trading_date) AS latest_expected_trading_date
                FROM ticker_daily_history
                WHERE market_data_source = ?
                """,
                [preferred_source],
            ).df()
            if not expected_row.empty:
                latest_expected = expected_row.iloc[0].get("latest_expected_trading_date")

        if not table_exists(active_conn, "canonical_theme_daily_snapshots"):
            empty_row = empty.copy()
            empty_row.loc[:, "latest_expected_trading_date"] = latest_expected
            return empty_row

        dates = active_conn.execute(
            """
            SELECT
                MAX(snapshot_date) AS latest_raw_canonical_date,
                MAX(CASE WHEN canonical_rank IS NOT NULL THEN snapshot_date ELSE NULL END) AS latest_ranked_canonical_date
            FROM canonical_theme_daily_snapshots
            """
        ).df()
        if dates.empty:
            empty_row = empty.copy()
            empty_row.loc[:, "latest_expected_trading_date"] = latest_expected
            return empty_row

        row = dates.iloc[0]
        latest_raw = row.get("latest_raw_canonical_date")
        latest_ranked = row.get("latest_ranked_canonical_date")
        return pd.DataFrame(
            [
                {
                    "latest_expected_trading_date": latest_expected,
                    "latest_raw_canonical_date": latest_raw,
                    "latest_ranked_canonical_date": latest_ranked,
                    "raw_vs_ranked_date_differs": bool(
                        latest_raw is not None and latest_ranked is not None and str(latest_raw) != str(latest_ranked)
                    ),
                }
            ]
        )

    return _with_bootstrap_recovery(_load)


def canonical_daily_recent_coverage(conn, trading_day_limit: int = 30) -> pd.DataFrame:
    empty = pd.DataFrame(
        columns=[
            "expected_trading_date",
            "market_data_source",
            "has_canonical_coverage",
            "canonical_row_count",
            "ranked_canonical_row_count",
            "repair_row_count",
            "run_based_row_count",
            "coverage_origin",
            "snapshot_source_summary",
            "canonical_reason_summary",
        ]
    )

    def _load(active_conn=conn) -> pd.DataFrame:
        preferred_source = _preferred_ticker_history_source(active_conn)
        if not preferred_source or not table_exists(active_conn, "ticker_daily_history"):
            return empty.copy()

        limit_days = max(int(trading_day_limit), 1)
        coverage = active_conn.execute(
            """
            WITH expected_dates AS (
                SELECT trading_date AS expected_trading_date
                FROM (
                    SELECT DISTINCT trading_date
                    FROM ticker_daily_history
                    WHERE market_data_source = ?
                    ORDER BY trading_date DESC
                    LIMIT ?
                )
            ),
            canonical_by_date AS (
                SELECT
                    snapshot_date,
                    COUNT(*) AS canonical_row_count,
                    SUM(CASE WHEN canonical_rank IS NOT NULL THEN 1 ELSE 0 END) AS ranked_canonical_row_count,
                    SUM(
                        CASE
                            WHEN snapshot_source = 'synthetic_backfill'
                              OR canonical_reason = 'missing_full_theme_run_history_repair'
                              OR extract_session = 'ticker_history_repair'
                            THEN 1
                            ELSE 0
                        END
                    ) AS repair_row_count,
                    SUM(
                        CASE
                            WHEN snapshot_source <> 'synthetic_backfill'
                              AND canonical_reason <> 'missing_full_theme_run_history_repair'
                              AND extract_session <> 'ticker_history_repair'
                            THEN 1
                            ELSE 0
                        END
                    ) AS run_based_row_count,
                    STRING_AGG(DISTINCT snapshot_source, ', ' ORDER BY snapshot_source) AS snapshot_source_summary,
                    STRING_AGG(DISTINCT canonical_reason, ', ' ORDER BY canonical_reason) AS canonical_reason_summary
                FROM canonical_theme_daily_snapshots
                GROUP BY snapshot_date
            )
            SELECT
                e.expected_trading_date,
                ? AS market_data_source,
                COALESCE(c.canonical_row_count, 0) > 0 AS has_canonical_coverage,
                COALESCE(c.canonical_row_count, 0) AS canonical_row_count,
                COALESCE(c.ranked_canonical_row_count, 0) AS ranked_canonical_row_count,
                COALESCE(c.repair_row_count, 0) AS repair_row_count,
                COALESCE(c.run_based_row_count, 0) AS run_based_row_count,
                COALESCE(c.snapshot_source_summary, '') AS snapshot_source_summary,
                COALESCE(c.canonical_reason_summary, '') AS canonical_reason_summary
            FROM expected_dates e
            LEFT JOIN canonical_by_date c ON c.snapshot_date = e.expected_trading_date
            ORDER BY e.expected_trading_date DESC
            """,
            [preferred_source, limit_days, preferred_source],
        ).df()
        if coverage.empty:
            return empty.copy()

        coverage["coverage_origin"] = coverage.apply(
            lambda row: (
                "missing"
                if int(row.get("canonical_row_count") or 0) <= 0
                else (
                    "repair_fallback"
                    if int(row.get("repair_row_count") or 0) >= int(row.get("canonical_row_count") or 0)
                    else (
                        "run_based"
                        if int(row.get("run_based_row_count") or 0) >= int(row.get("canonical_row_count") or 0)
                        else "mixed"
                    )
                )
            ),
            axis=1,
        )
        return coverage.reset_index(drop=True)

    return _with_bootstrap_recovery(_load)


def canonical_daily_health_status(
    conn,
    trading_day_limit: int = 30,
    reconciliation_top_n: int = 10,
    *,
    coverage: pd.DataFrame | None = None,
) -> pd.DataFrame:
    empty = pd.DataFrame(
        [
            {
                "market_data_source": None,
                "latest_expected_trading_date": None,
                "latest_canonical_snapshot_date": None,
                "latest_expected_date_canonically_covered": False,
                "canonical_trading_date_gap_count": 0,
                "latest_canonical_row_count": 0,
                "latest_canonical_ranked_row_count": 0,
                "latest_canonical_repair_row_count": 0,
                "latest_canonical_run_based_row_count": 0,
                "recent_expected_trading_dates": 0,
                "recent_covered_dates": 0,
                "recent_missing_dates": 0,
                "recent_repair_involved_dates": 0,
                "reconciliation_reference_date": None,
                "reconciliation_reference_is_latest_expected": False,
                "reconciliation_top_n": int(max(int(reconciliation_top_n), 1)),
                "current_standardized_leader_count": 0,
                "canonical_leader_count": 0,
                "top_n_mismatch_count": None,
                "latest_day_leaders_match_current_standardized": False,
                "reconciliation_status": "unavailable",
            }
        ]
    )

    def _load(active_conn=conn) -> pd.DataFrame:
        coverage_df = coverage.copy() if coverage is not None else canonical_daily_recent_coverage(active_conn, trading_day_limit=trading_day_limit)
        if coverage_df.empty:
            return empty.copy()

        preferred_source = str(coverage_df.iloc[0].get("market_data_source") or "") or None
        latest_expected_date = coverage_df.iloc[0]["expected_trading_date"]
        latest_expected_covered = bool(coverage_df.iloc[0]["has_canonical_coverage"])
        latest_canonical_row = active_conn.execute(
            """
            SELECT MAX(snapshot_date) AS latest_canonical_snapshot_date
            FROM canonical_theme_daily_snapshots
            """
        ).df()
        latest_canonical_date = (
            latest_canonical_row.iloc[0]["latest_canonical_snapshot_date"] if not latest_canonical_row.empty else None
        )

        latest_counts = pd.DataFrame(
            [
                {
                    "latest_canonical_row_count": 0,
                    "latest_canonical_ranked_row_count": 0,
                    "latest_canonical_repair_row_count": 0,
                    "latest_canonical_run_based_row_count": 0,
                }
            ]
        )
        if latest_canonical_date is not None and table_exists(active_conn, "canonical_theme_daily_snapshots"):
            latest_counts = active_conn.execute(
                """
                SELECT
                    COUNT(*) AS latest_canonical_row_count,
                    SUM(CASE WHEN canonical_rank IS NOT NULL THEN 1 ELSE 0 END) AS latest_canonical_ranked_row_count,
                    SUM(
                        CASE
                            WHEN snapshot_source = 'synthetic_backfill'
                              OR canonical_reason = 'missing_full_theme_run_history_repair'
                              OR extract_session = 'ticker_history_repair'
                            THEN 1
                            ELSE 0
                        END
                    ) AS latest_canonical_repair_row_count,
                    SUM(
                        CASE
                            WHEN snapshot_source <> 'synthetic_backfill'
                              AND canonical_reason <> 'missing_full_theme_run_history_repair'
                              AND extract_session <> 'ticker_history_repair'
                            THEN 1
                            ELSE 0
                        END
                    ) AS latest_canonical_run_based_row_count
                FROM canonical_theme_daily_snapshots
                WHERE snapshot_date = ?
                """,
                [latest_canonical_date],
            ).df()

        latest_canonical_date_value = pd.Timestamp(latest_canonical_date).date() if latest_canonical_date is not None else None
        canonical_gap_count = int(
            (pd.to_datetime(coverage_df["expected_trading_date"], errors="coerce").dt.date > latest_canonical_date_value).sum()
        ) if latest_canonical_date_value is not None else int(len(coverage_df))

        reference_date = latest_expected_date if latest_expected_covered else latest_canonical_date
        reference_is_latest_expected = bool(reference_date is not None and reference_date == latest_expected_date)

        current_leaders: list[int] = []
        canonical_leaders: list[int] = []
        mismatch_count: int | None = None
        reconciliation_status = "unavailable"
        leaders_match = False

        if reference_date is not None:
            from .rankings import compute_current_ranking_snapshot, compute_current_ranking_snapshot_for_run

            reference_run_id = None
            if latest_canonical_date is not None and reference_date == latest_canonical_date:
                reference_run_id_row = active_conn.execute(
                    """
                    SELECT run_id
                    FROM canonical_theme_daily_snapshots
                    WHERE snapshot_date = ?
                      AND canonical_rank IS NOT NULL
                      AND run_id IS NOT NULL
                    GROUP BY run_id
                    ORDER BY COUNT(*) DESC, run_id DESC
                    LIMIT 1
                    """,
                    [reference_date],
                ).fetchone()
                if reference_run_id_row is not None and reference_run_id_row[0] is not None:
                    reference_run_id = int(reference_run_id_row[0])

            current_snapshot = (
                compute_current_ranking_snapshot_for_run(active_conn, reference_run_id)
                if reference_run_id is not None
                else compute_current_ranking_snapshot(active_conn)
            )
            standardized_rankings = current_snapshot.get("standardized_rankings", pd.DataFrame())
            if not standardized_rankings.empty and "theme_id" in standardized_rankings.columns:
                current_leaders = standardized_rankings["theme_id"].dropna().astype(int).tolist()

            canonical_on_reference = active_conn.execute(
                """
                SELECT theme_id
                FROM canonical_theme_daily_snapshots
                WHERE snapshot_date = ?
                  AND canonical_rank IS NOT NULL
                ORDER BY canonical_rank ASC, theme ASC
                """,
                [reference_date],
            ).df()
            if not canonical_on_reference.empty:
                canonical_leaders = canonical_on_reference["theme_id"].dropna().astype(int).tolist()

            compare_n = max(int(reconciliation_top_n), 1)
            if current_leaders and canonical_leaders:
                compared = 0
                mismatch_count = 0
                for idx in range(compare_n):
                    current_theme_id = current_leaders[idx] if idx < len(current_leaders) else None
                    canonical_theme_id = canonical_leaders[idx] if idx < len(canonical_leaders) else None
                    if current_theme_id is None and canonical_theme_id is None:
                        continue
                    compared += 1
                    if current_theme_id != canonical_theme_id:
                        mismatch_count += 1
                if compared == 0:
                    mismatch_count = None

            if mismatch_count is None:
                reconciliation_status = "unavailable"
            elif not reference_is_latest_expected:
                reconciliation_status = "stale_canonical_date"
            elif mismatch_count == 0:
                reconciliation_status = "matched"
                leaders_match = True
            else:
                reconciliation_status = "mismatch"

        recent_expected_count = int(len(coverage_df))
        recent_covered_dates = int(coverage_df["has_canonical_coverage"].fillna(False).sum()) if not coverage_df.empty else 0
        recent_missing_dates = int((~coverage_df["has_canonical_coverage"].fillna(False)).sum()) if not coverage_df.empty else 0
        recent_repair_involved_dates = int((coverage_df["repair_row_count"].fillna(0) > 0).sum()) if not coverage_df.empty else 0

        latest_counts_row = latest_counts.iloc[0] if not latest_counts.empty else {}
        return pd.DataFrame(
            [
                {
                    "market_data_source": preferred_source,
                    "latest_expected_trading_date": latest_expected_date,
                    "latest_canonical_snapshot_date": latest_canonical_date,
                    "latest_expected_date_canonically_covered": latest_expected_covered,
                    "canonical_trading_date_gap_count": canonical_gap_count,
                    "latest_canonical_row_count": int(latest_counts_row.get("latest_canonical_row_count") or 0),
                    "latest_canonical_ranked_row_count": int(latest_counts_row.get("latest_canonical_ranked_row_count") or 0),
                    "latest_canonical_repair_row_count": int(latest_counts_row.get("latest_canonical_repair_row_count") or 0),
                    "latest_canonical_run_based_row_count": int(latest_counts_row.get("latest_canonical_run_based_row_count") or 0),
                    "recent_expected_trading_dates": recent_expected_count,
                    "recent_covered_dates": recent_covered_dates,
                    "recent_missing_dates": recent_missing_dates,
                    "recent_repair_involved_dates": recent_repair_involved_dates,
                    "reconciliation_reference_date": reference_date,
                    "reconciliation_reference_is_latest_expected": reference_is_latest_expected,
                    "reconciliation_top_n": int(max(int(reconciliation_top_n), 1)),
                    "current_standardized_leader_count": int(len(current_leaders)),
                    "canonical_leader_count": int(len(canonical_leaders)),
                    "top_n_mismatch_count": mismatch_count,
                    "latest_day_leaders_match_current_standardized": bool(leaders_match),
                    "reconciliation_status": reconciliation_status,
                }
            ]
        )

    return _with_bootstrap_recovery(_load)


def latest_ticker_history_atr_companion_fields(conn) -> pd.DataFrame:
    preferred_source = _preferred_ticker_history_source(conn)
    if (
        not preferred_source
        or not table_exists(conn, "ticker_daily_history")
        or not table_has_column(conn, "ticker_daily_history", "atr_14")
    ):
        return pd.DataFrame(
            columns=[
                "ticker",
                "perf_1w_atr_units",
                "perf_1m_atr_units",
            ]
        )
    return conn.execute(
        """
        WITH deduped AS (
            SELECT
                upper(trim(ticker)) AS ticker,
                trading_date,
                close,
                atr_14,
                ROW_NUMBER() OVER (
                    PARTITION BY ticker, trading_date
                    ORDER BY updated_at DESC, provenance_source_label DESC
                ) AS rn
            FROM ticker_daily_history
            WHERE market_data_source = ?
        ),
        ordered AS (
            SELECT
                ticker,
                trading_date,
                close,
                atr_14,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trading_date DESC) AS recency_rank
            FROM deduped
            WHERE rn = 1
        )
        SELECT
            ticker,
            CASE
              WHEN MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) IS NOT NULL
               AND MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) <> 0
               AND MAX(CASE WHEN recency_rank = 6 THEN close END) IS NOT NULL THEN (
                MAX(CASE WHEN recency_rank = 1 THEN close END)
                - MAX(CASE WHEN recency_rank = 6 THEN close END)
              ) / MAX(CASE WHEN recency_rank = 1 THEN atr_14 END)
              ELSE NULL
            END AS perf_1w_atr_units,
            CASE
              WHEN MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) IS NOT NULL
               AND MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) <> 0
               AND MAX(CASE WHEN recency_rank = 22 THEN close END) IS NOT NULL THEN (
                MAX(CASE WHEN recency_rank = 1 THEN close END)
                - MAX(CASE WHEN recency_rank = 22 THEN close END)
              ) / MAX(CASE WHEN recency_rank = 1 THEN atr_14 END)
              ELSE NULL
            END AS perf_1m_atr_units
        FROM ordered
        WHERE recency_rank <= 22
        GROUP BY ticker
        ORDER BY ticker
        """,
        [preferred_source],
    ).df()


def latest_ticker_history_research_fields(conn) -> pd.DataFrame:
    preferred_source = _preferred_ticker_history_source(conn)
    if (
        not preferred_source
        or not table_exists(conn, "ticker_daily_history")
        or not table_has_column(conn, "ticker_daily_history", "atr_14")
    ):
        return pd.DataFrame(
            columns=[
                "ticker",
                "latest_history_date",
                "atr_14",
                "atr_pct_14",
                "perf_1w_atr_units",
                "perf_1m_atr_units",
            ]
        )
    return conn.execute(
        """
        WITH deduped AS (
            SELECT
                upper(trim(ticker)) AS ticker,
                trading_date,
                close,
                atr_14,
                atr_pct_14,
                ROW_NUMBER() OVER (
                    PARTITION BY ticker, trading_date
                    ORDER BY updated_at DESC, provenance_source_label DESC
                ) AS rn
            FROM ticker_daily_history
            WHERE market_data_source = ?
        ),
        ordered AS (
            SELECT
                ticker,
                trading_date,
                close,
                atr_14,
                atr_pct_14,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trading_date DESC) AS recency_rank
            FROM deduped
            WHERE rn = 1
        )
        SELECT
            ticker,
            MAX(CASE WHEN recency_rank = 1 THEN trading_date END) AS latest_history_date,
            MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) AS atr_14,
            MAX(CASE WHEN recency_rank = 1 THEN atr_pct_14 END) AS atr_pct_14,
            CASE
              WHEN MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) IS NOT NULL
               AND MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) <> 0
               AND MAX(CASE WHEN recency_rank = 6 THEN close END) IS NOT NULL THEN (
                MAX(CASE WHEN recency_rank = 1 THEN close END)
                - MAX(CASE WHEN recency_rank = 6 THEN close END)
              ) / MAX(CASE WHEN recency_rank = 1 THEN atr_14 END)
              ELSE NULL
            END AS perf_1w_atr_units,
            CASE
              WHEN MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) IS NOT NULL
               AND MAX(CASE WHEN recency_rank = 1 THEN atr_14 END) <> 0
               AND MAX(CASE WHEN recency_rank = 22 THEN close END) IS NOT NULL THEN (
                MAX(CASE WHEN recency_rank = 1 THEN close END)
                - MAX(CASE WHEN recency_rank = 22 THEN close END)
              ) / MAX(CASE WHEN recency_rank = 1 THEN atr_14 END)
              ELSE NULL
            END AS perf_1m_atr_units
        FROM ordered
        WHERE recency_rank <= 22
        GROUP BY ticker
        ORDER BY ticker
        """,
        [preferred_source],
    ).df()


def ticker_lookup_summary(conn, ticker: str) -> pd.DataFrame:
    normalized = (ticker or "").strip().upper()
    if not normalized:
        return pd.DataFrame()

    ticker_source_expr = _ticker_snapshot_source_expr(conn)
    preferred_source = preferred_ticker_snapshot_source(conn)
    preferred_snapshot_filter = f"AND {ticker_source_expr} = ?" if preferred_source else "AND 1 = 0"
    manual_suppressed_expr = "COALESCE(manual_suppressed, FALSE)" if _manual_suppression_enabled(conn) else "FALSE"
    manual_reason_expr = "manual_suppression_reason" if table_has_column(conn, "symbol_refresh_status", "manual_suppression_reason") else "NULL"
    manual_at_expr = "manual_suppressed_at" if table_has_column(conn, "symbol_refresh_status", "manual_suppressed_at") else "NULL"
    params: list[object] = [normalized, normalized, normalized, normalized]
    if preferred_source:
        params.append(preferred_source)
    params.extend([normalized, normalized, normalized])
    return conn.execute(
        f"""
        WITH membership AS (
            SELECT
                COUNT(*) AS membership_count,
                COUNT(*) FILTER (WHERE COALESCE(t.is_active, FALSE)) AS active_membership_count,
                COUNT(*) FILTER (WHERE NOT COALESCE(t.is_active, FALSE)) AS inactive_membership_count
            FROM theme_membership m
            LEFT JOIN themes t ON t.id = m.theme_id
            WHERE upper(trim(m.ticker)) = ?
        ),
        snapshots AS (
            SELECT
                COUNT(*) AS snapshot_count,
                MAX(s.run_id) AS latest_snapshot_run_id
            FROM ticker_snapshots s
            WHERE s.ticker = ?
        ),
        latest_snapshot AS (
            SELECT
                s.price AS latest_price,
                s.market_cap AS latest_market_cap,
                s.avg_volume AS latest_avg_volume,
                r.finished_at AS latest_snapshot_time,
                {ticker_source_expr} AS latest_snapshot_source
            FROM ticker_snapshots s
            LEFT JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE s.ticker = ?
              AND (r.run_id IS NULL OR r.status IN ('success', 'partial'))
            QUALIFY ROW_NUMBER() OVER (ORDER BY s.run_id DESC) = 1
        ),
        preferred_snapshot AS (
            SELECT
                s.price AS preferred_price,
                s.avg_volume AS preferred_avg_volume,
                s.perf_1w AS preferred_perf_1w,
                s.perf_1m AS preferred_perf_1m,
                s.perf_3m AS preferred_perf_3m,
                r.finished_at AS preferred_snapshot_time,
                {ticker_source_expr} AS preferred_snapshot_source
            FROM ticker_snapshots s
            LEFT JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE s.ticker = ?
              AND (r.run_id IS NULL OR r.status IN ('success', 'partial'))
              {preferred_snapshot_filter}
            QUALIFY ROW_NUMBER() OVER (ORDER BY s.run_id DESC) = 1
        ),
        refresh_seen AS (
            SELECT COUNT(*) AS refresh_run_count
            FROM refresh_run_tickers
            WHERE ticker = ?
        ),
        symbol_seen AS (
            SELECT
                COUNT(*) AS symbol_status_count,
                MAX(CASE WHEN {manual_suppressed_expr} THEN 1 ELSE 0 END) AS manual_suppressed_flag,
                MAX(CASE WHEN COALESCE(status, 'active') = 'refresh_suppressed' THEN 1 ELSE 0 END) AS refresh_suppressed_flag,
                MAX(CASE WHEN {manual_suppressed_expr} THEN {manual_reason_expr} ELSE NULL END) AS manual_suppression_reason,
                MAX(CASE WHEN {manual_suppressed_expr} THEN {manual_at_expr} ELSE NULL END) AS manual_suppressed_at
            FROM symbol_refresh_status
            WHERE upper(trim(ticker)) = ?
        )
        SELECT
            ? AS ticker,
            CAST(m.membership_count > 0 AND COALESCE(ss.manual_suppressed_flag, 0) = 0 AS BOOLEAN) AS exists_in_theme_membership,
            CAST(s.snapshot_count > 0 AS BOOLEAN) AS exists_in_ticker_snapshots,
            CAST(r.refresh_run_count > 0 AS BOOLEAN) AS exists_in_refresh_run_tickers,
            CAST(ss.symbol_status_count > 0 AS BOOLEAN) AS exists_in_symbol_refresh_status,
            COALESCE(m.membership_count, 0) AS assigned_theme_count,
            COALESCE(m.active_membership_count, 0) AS active_assigned_theme_count,
            COALESCE(m.inactive_membership_count, 0) AS inactive_assigned_theme_count,
            CAST(COALESCE(ss.manual_suppressed_flag, 0) > 0 AS BOOLEAN) AS manually_suppressed,
            CAST(COALESCE(ss.refresh_suppressed_flag, 0) > 0 AS BOOLEAN) AS operationally_suppressed,
            ss.manual_suppression_reason,
            ss.manual_suppressed_at,
            ls.latest_snapshot_time,
            ls.latest_snapshot_source,
            ls.latest_price,
            ls.latest_market_cap,
            ls.latest_avg_volume,
            ps.preferred_snapshot_time,
            ps.preferred_snapshot_source,
            ps.preferred_price,
            ps.preferred_avg_volume,
            CAST(ps.preferred_snapshot_time IS NOT NULL AS BOOLEAN) AS has_current_preferred_snapshot,
            CAST(
                ps.preferred_snapshot_time IS NOT NULL
                AND (
                    ps.preferred_price IS NOT NULL
                    OR ps.preferred_avg_volume IS NOT NULL
                    OR ps.preferred_perf_1w IS NOT NULL
                    OR ps.preferred_perf_1m IS NOT NULL
                    OR ps.preferred_perf_3m IS NOT NULL
                ) AS BOOLEAN
            ) AS has_current_usable_preferred_snapshot,
            CASE
              WHEN COALESCE(ss.manual_suppressed_flag, 0) > 0 THEN 'Suppressed operationally'
              WHEN COALESCE(m.membership_count, 0) > 0 THEN 'In DB and assigned'
              WHEN COALESCE(s.snapshot_count, 0) > 0 THEN 'Seen in snapshots only'
              WHEN COALESCE(r.refresh_run_count, 0) > 0 OR COALESCE(ss.symbol_status_count, 0) > 0 THEN 'In DB but unassigned'
              ELSE 'Not found'
            END AS lookup_status
        FROM membership m
        CROSS JOIN snapshots s
        CROSS JOIN refresh_seen r
        CROSS JOIN symbol_seen ss
        LEFT JOIN latest_snapshot ls ON TRUE
        LEFT JOIN preferred_snapshot ps ON TRUE
        """,
        params,
    ).df()


def ticker_lookup_memberships(conn, ticker: str) -> pd.DataFrame:
    normalized = (ticker or "").strip().upper()
    if not normalized:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT
            m.ticker,
            t.id AS theme_id,
            t.name AS theme_name,
            t.category,
            t.is_active
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        WHERE upper(trim(m.ticker)) = ?
        ORDER BY t.name
        """,
        [normalized],
    ).df()


def theme_member_hygiene_context(conn, theme_id: int) -> pd.DataFrame:
    return conn.execute(
        """
        WITH governed_members AS (
            SELECT
                upper(trim(ticker)) AS ticker
            FROM theme_membership m
            WHERE theme_id BETWEEN ? AND ?
            """
            + _manual_suppression_filter_sql(conn, "m.ticker")
            + """
            GROUP BY upper(trim(ticker))
        )
        SELECT
            gm.ticker,
            s.last_failure_category,
            s.last_failure_at,
            s.consecutive_failure_count,
            s.status AS symbol_hygiene_status
        FROM governed_members gm
        LEFT JOIN symbol_refresh_status s ON upper(trim(s.ticker)) = gm.ticker
        ORDER BY
            CASE WHEN s.last_failure_at IS NULL THEN 1 ELSE 0 END,
            s.last_failure_at DESC,
            gm.ticker
        """,
        [int(theme_id), int(theme_id)],
    ).df()


def themes_dimension(conn) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            id AS theme_id,
            name AS theme_name,
            category,
            is_active
        FROM themes
        ORDER BY name
        """
    ).df()


def historical_reconstruction_runs(conn, limit: int = 20) -> pd.DataFrame:
    if not table_exists(conn, "historical_reconstruction_runs"):
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT
            run_id,
            run_kind,
            provenance_source_label,
            market_data_source,
            status,
            start_date,
            end_date,
            ticker_count,
            theme_count,
            ticker_history_rows_written,
            ticker_history_rows_skipped,
            snapshot_rows_written,
            snapshot_rows_skipped,
            failed_tickers,
            started_at,
            finished_at,
            error_message
        FROM historical_reconstruction_runs
        ORDER BY run_id DESC
        LIMIT ?
        """,
        [limit],
    ).df()


def classify_ticker_history_readiness(
    available_trading_days: int,
    ready_coverage_pct: float,
    *,
    target_trading_days: int = 30,
) -> str:
    if available_trading_days >= target_trading_days and ready_coverage_pct >= 70.0:
        return "ready"
    if available_trading_days >= 20 or ready_coverage_pct >= 40.0:
        return "near ready"
    return "accumulating"


def ticker_history_readiness(conn, target_trading_days: int = 30) -> pd.DataFrame:
    suppression_table_exists = table_exists(conn, "symbol_refresh_status")
    has_status = suppression_table_exists and table_has_column(conn, "symbol_refresh_status", "status")
    has_manual_suppressed = suppression_table_exists and table_has_column(conn, "symbol_refresh_status", "manual_suppressed")
    suppressed_expr_parts: list[str] = []
    if has_status:
        suppressed_expr_parts.append("COALESCE(sr.status, 'active') = 'refresh_suppressed'")
    if has_manual_suppressed:
        suppressed_expr_parts.append("COALESCE(sr.manual_suppressed, FALSE)")
    suppressed_expr = " OR ".join(suppressed_expr_parts) if suppressed_expr_parts else "FALSE"

    governed_active_tickers = conn.execute(
        f"""
        WITH governed AS (
            SELECT DISTINCT upper(trim(m.ticker)) AS ticker
            FROM theme_membership m
            JOIN themes t ON t.id = m.theme_id
            WHERE t.is_active = TRUE
        ),
        status_rollup AS (
            SELECT
                upper(trim(ticker)) AS ticker,
                MAX(CASE WHEN {suppressed_expr} THEN 1 ELSE 0 END) AS operationally_suppressed
            FROM symbol_refresh_status sr
            GROUP BY 1
        )
        SELECT
            g.ticker,
            COALESCE(sr.operationally_suppressed, 0) AS operationally_suppressed
        FROM governed g
        LEFT JOIN status_rollup sr ON sr.ticker = g.ticker
        ORDER BY g.ticker
        """
        if suppression_table_exists
        else """
        SELECT DISTINCT upper(trim(m.ticker)) AS ticker, 0 AS operationally_suppressed
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        WHERE t.is_active = TRUE
        ORDER BY upper(trim(m.ticker))
        """
    ).df()
    raw_governed_count = int(len(governed_active_tickers))
    expected_governed = governed_active_tickers[governed_active_tickers["operationally_suppressed"] != 1].copy()
    governed_count = int(len(expected_governed))
    suppressed_count = int(raw_governed_count - governed_count)

    if not table_exists(conn, "ticker_daily_history"):
        return pd.DataFrame(
            [
                {
                    "target_trading_days": int(target_trading_days),
                    "market_data_source": None,
                    "available_trading_days": 0,
                    "remaining_trading_days": int(target_trading_days),
                    "governed_active_tickers": governed_count,
                    "governed_active_tickers_raw": raw_governed_count,
                    "governed_active_tickers_suppressed": suppressed_count,
                    "governed_active_tickers_ready": 0,
                    "governed_ready_pct": 0.0,
                    "min_ticker_depth": 0,
                    "median_ticker_depth": 0.0,
                    "max_ticker_depth": 0,
                    "earliest_trading_date": None,
                    "latest_trading_date": None,
                    "status_label": "accumulating",
                }
            ]
        )

    preferred_source = conn.execute(
        """
        SELECT market_data_source
        FROM ticker_daily_history
        ORDER BY CASE WHEN market_data_source = 'live' THEN 0 ELSE 1 END,
                 trading_date DESC,
                 updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    market_data_source = str(preferred_source[0]) if preferred_source and preferred_source[0] else None

    if not market_data_source:
        return pd.DataFrame(
            [
                {
                    "target_trading_days": int(target_trading_days),
                    "market_data_source": None,
                    "available_trading_days": 0,
                    "remaining_trading_days": int(target_trading_days),
                    "governed_active_tickers": governed_count,
                    "governed_active_tickers_raw": raw_governed_count,
                    "governed_active_tickers_suppressed": suppressed_count,
                    "governed_active_tickers_ready": 0,
                    "governed_ready_pct": 0.0,
                    "min_ticker_depth": 0,
                    "median_ticker_depth": 0.0,
                    "max_ticker_depth": 0,
                    "earliest_trading_date": None,
                    "latest_trading_date": None,
                    "status_label": "accumulating",
                }
            ]
        )

    conn.register("expected_governed_tickers", expected_governed[["ticker"]])
    try:
        coverage = conn.execute(
            """
            SELECT
                g.ticker,
                COUNT(DISTINCT h.trading_date) AS trading_day_rows
            FROM expected_governed_tickers g
            LEFT JOIN ticker_daily_history h
              ON h.ticker = g.ticker
             AND h.market_data_source = ?
            GROUP BY g.ticker
            ORDER BY g.ticker
            """,
            [market_data_source],
        ).df()
    finally:
        conn.unregister("expected_governed_tickers")
    overall = conn.execute(
        """
        SELECT
            COUNT(DISTINCT trading_date) AS available_trading_days,
            MIN(trading_date) AS earliest_trading_date,
            MAX(trading_date) AS latest_trading_date
        FROM ticker_daily_history
        WHERE market_data_source = ?
        """,
        [market_data_source],
    ).df()

    available_trading_days = int(overall.iloc[0]["available_trading_days"] or 0) if not overall.empty else 0
    remaining_trading_days = max(0, int(target_trading_days) - available_trading_days)
    ready_count = int((coverage["trading_day_rows"] >= int(target_trading_days)).sum()) if not coverage.empty else 0
    ready_pct = round((ready_count / governed_count) * 100.0, 1) if governed_count > 0 else 0.0
    min_depth = int(coverage["trading_day_rows"].min()) if not coverage.empty else 0
    median_depth = float(coverage["trading_day_rows"].median()) if not coverage.empty else 0.0
    max_depth = int(coverage["trading_day_rows"].max()) if not coverage.empty else 0
    status_label = classify_ticker_history_readiness(
        available_trading_days,
        ready_pct,
        target_trading_days=int(target_trading_days),
    )

    return pd.DataFrame(
        [
            {
                "target_trading_days": int(target_trading_days),
                "market_data_source": market_data_source,
                "available_trading_days": available_trading_days,
                "remaining_trading_days": remaining_trading_days,
                "governed_active_tickers": governed_count,
                "governed_active_tickers_raw": raw_governed_count,
                "governed_active_tickers_suppressed": suppressed_count,
                "governed_active_tickers_ready": ready_count,
                "governed_ready_pct": ready_pct,
                "min_ticker_depth": min_depth,
                "median_ticker_depth": median_depth,
                "max_ticker_depth": max_depth,
                "earliest_trading_date": overall.iloc[0]["earliest_trading_date"] if not overall.empty else None,
                "latest_trading_date": overall.iloc[0]["latest_trading_date"] if not overall.empty else None,
                "status_label": status_label,
            }
        ]
    )


def theme_snapshot_history_recent(conn, snapshot_limit: int = 14) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            ts.theme_id,
            ts.snapshot_time,
            ts.run_id,
            ts.ticker_count,
            ts.avg_1w,
            ts.avg_1m,
            ts.avg_3m,
            ts.positive_1w_breadth_pct,
            ts.positive_1m_breadth_pct,
            ts.positive_3m_breadth_pct,
            ts.composite_score,
            ts.snapshot_source
        FROM theme_snapshots ts
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ts.theme_id
            ORDER BY ts.snapshot_time DESC, ts.run_id DESC
        ) <= ?
        ORDER BY ts.theme_id, ts.snapshot_time DESC, ts.run_id DESC
        """,
        [snapshot_limit],
    ).df()


def theme_leadership_rank_history(
    conn,
    theme_ids: list[int],
    lookback_points: int = 7,
    *,
    ranking_theme_ids: list[int] | None = None,
) -> pd.DataFrame:
    normalized_theme_ids = sorted({int(theme_id) for theme_id in theme_ids or []})
    ranking_universe_ids = sorted({int(theme_id) for theme_id in (ranking_theme_ids or normalized_theme_ids)})
    empty = pd.DataFrame(
        columns=[
            "theme_id",
            "rank_history",
            "rank_history_points",
            "rank_history_start_date",
            "rank_history_end_date",
        ]
    )
    if not normalized_theme_ids:
        return empty

    def _load(active_conn=conn) -> pd.DataFrame:
        calendar_lookback_days = max(int(lookback_points) + 7, int(lookback_points) * 3)
        start_date = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=calendar_lookback_days)
        history = _recent_movement_theme_snapshot_union(active_conn, start_date=start_date)
        if history.empty:
            return empty.copy()

        ranked = history.copy()
        ranked["snapshot_time"] = pd.to_datetime(ranked["snapshot_time"], errors="coerce")
        ranked["snapshot_date"] = pd.to_datetime(ranked["snapshot_time"], errors="coerce").dt.date
        ranked["composite_score"] = pd.to_numeric(ranked["composite_score"], errors="coerce")
        ranked = ranked.dropna(subset=["snapshot_time", "snapshot_date", "composite_score"])
        if ranked.empty:
            return empty.copy()

        # Keep one deterministic daily winner per theme using the latest
        # resolved snapshot row already selected by the historical union.
        ranked = (
            ranked.sort_values(["theme_id", "snapshot_date", "snapshot_time"], ascending=[True, True, False])
            .drop_duplicates(subset=["theme_id", "snapshot_date"], keep="first")
            .reset_index(drop=True)
        )

        recent_dates = sorted(ranked["snapshot_date"].dropna().unique().tolist())[-max(int(lookback_points), 1) :]
        if not recent_dates:
            return empty.copy()

        ranked = ranked[ranked["snapshot_date"].isin(recent_dates)].copy()
        ranked = ranked[ranked["theme_id"].isin(ranking_universe_ids)].copy()
        if ranked.empty:
            return empty.copy()
        ranked["rank"] = ranked.groupby("snapshot_date")["composite_score"].rank(method="dense", ascending=False)
        ranked = ranked[ranked["theme_id"].isin(normalized_theme_ids)].copy()

        rows: list[dict[str, object]] = []
        for theme_id in normalized_theme_ids:
            theme_rows = ranked[ranked["theme_id"] == int(theme_id)].copy()
            series_by_date = {
                snapshot_date: float(rank)
                for snapshot_date, rank in zip(theme_rows["snapshot_date"], theme_rows["rank"])
                if snapshot_date is not None and rank is not None and not pd.isna(rank)
            }
            rank_history = [series_by_date[snapshot_date] for snapshot_date in recent_dates if snapshot_date in series_by_date]
            point_count = sum(1 for value in rank_history if value is not None and not pd.isna(value))
            rows.append(
                {
                    "theme_id": int(theme_id),
                    "rank_history": rank_history if point_count >= 2 else None,
                    "rank_history_points": int(point_count),
                    "rank_history_start_date": recent_dates[0],
                    "rank_history_end_date": recent_dates[-1],
                }
            )
        return pd.DataFrame(rows)

    return _with_bootstrap_recovery(_load)


def theme_leadership_rank_history_long(
    conn,
    theme_ids: list[int],
    lookback_points: int = 10,
) -> pd.DataFrame:
    normalized_theme_ids = sorted({int(theme_id) for theme_id in theme_ids or []})
    empty = pd.DataFrame(columns=["theme_id", "theme", "category", "snapshot_date", "rank"])
    if not normalized_theme_ids:
        return empty

    def _load(active_conn=conn) -> pd.DataFrame:
        calendar_lookback_days = max(int(lookback_points) + 7, int(lookback_points) * 3)
        start_date = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=calendar_lookback_days)
        history = _recent_movement_theme_snapshot_union(active_conn, start_date=start_date)
        if history.empty:
            return empty.copy()

        ranked = history.copy()
        ranked["snapshot_date"] = pd.to_datetime(ranked["snapshot_time"], errors="coerce").dt.date
        ranked["composite_score"] = pd.to_numeric(ranked["composite_score"], errors="coerce")
        ranked = ranked.dropna(subset=["snapshot_date", "composite_score"])
        if ranked.empty:
            return empty.copy()

        recent_dates = sorted(ranked["snapshot_date"].dropna().unique().tolist())[-max(int(lookback_points), 1) :]
        if not recent_dates:
            return empty.copy()

        ranked = ranked[ranked["snapshot_date"].isin(recent_dates)].copy()
        ranked["rank"] = ranked.groupby("snapshot_date")["composite_score"].rank(method="dense", ascending=False)
        ranked = ranked[ranked["theme_id"].isin(normalized_theme_ids)].copy()
        if ranked.empty:
            return empty.copy()
        ranked["rank"] = pd.to_numeric(ranked["rank"], errors="coerce")
        return ranked[["theme_id", "theme", "category", "snapshot_date", "rank"]].sort_values(
            ["snapshot_date", "rank", "theme"],
            ascending=[True, True, True],
        ).reset_index(drop=True)

    return _with_bootstrap_recovery(_load)


def tickers_dimension(conn) -> pd.DataFrame:
    return conn.execute(
        """
        WITH latest_completed AS (
            SELECT
                s.ticker,
                s.avg_volume,
                s.last_updated,
                r.finished_at AS latest_snapshot_time,
                ROW_NUMBER() OVER (PARTITION BY upper(trim(s.ticker)) ORDER BY s.run_id DESC) AS rn
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
        ),
        latest_nonnull_caps AS (
            SELECT
                s.ticker,
                s.market_cap,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.run_id DESC) AS rn
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
              AND s.market_cap IS NOT NULL
        )
        SELECT
            lc.ticker,
            lmc.market_cap AS latest_market_cap,
            lc.avg_volume AS latest_avg_volume,
            lc.last_updated AS latest_last_updated,
            lc.latest_snapshot_time
        FROM latest_completed lc
        LEFT JOIN latest_nonnull_caps lmc
          ON lc.ticker = lmc.ticker AND lmc.rn = 1
        WHERE lc.rn = 1
        ORDER BY lc.ticker
        """
    ).df()


def ticker_snapshot_history_recent(conn, snapshot_limit: int = 14) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            s.ticker,
            r.finished_at AS snapshot_time,
            s.run_id,
            s.price,
            s.perf_1w,
            s.perf_1m,
            s.perf_3m,
            s.market_cap,
            s.avg_volume,
            s.last_updated,
            s.snapshot_source
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE r.status IN ('success', 'partial')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY s.ticker
            ORDER BY s.run_id DESC
        ) <= ?
        ORDER BY s.ticker, s.run_id DESC
        """,
        [snapshot_limit],
    ).df()


def core_table_status(conn) -> pd.DataFrame:
    expected = pd.DataFrame({"table_name": CORE_TABLES})
    existing = conn.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE schema_name = 'main'
        """
    ).df()
    out = expected.merge(existing.assign(exists=True), on="table_name", how="left")
    out["exists"] = out["exists"].fillna(False)
    return out


def baseline_status(conn, recent_limit: int = 50) -> pd.DataFrame:
    preferred_theme = preferred_theme_snapshot_source(conn)
    preferred_ticker = preferred_ticker_snapshot_source(conn)
    theme_source_filter = preferred_theme or "__no_source__"
    ticker_source_filter = preferred_ticker or "__no_source__"
    theme_source_expr = _theme_snapshot_source_expr(conn)
    ticker_source_expr = _ticker_snapshot_source_expr(conn)
    return conn.execute(
        f"""
        WITH last_run AS (
            SELECT run_id, provider, status, finished_at
            FROM refresh_runs
            ORDER BY run_id DESC
            LIMIT 1
        ),
        preferred_theme_view AS (
            SELECT MAX(snapshot_time) AS latest_theme_snapshot_time,
                   COUNT(DISTINCT snapshot_time) AS theme_snapshot_sets
            FROM theme_snapshots
            WHERE {theme_source_expr} = ?
        ),
        preferred_ticker_view AS (
            SELECT MAX(r.finished_at) AS latest_ticker_snapshot_time,
                   COUNT(DISTINCT r.finished_at) AS ticker_snapshot_sets
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
              AND {ticker_source_expr} = ?
        ),
        recent_theme_sources AS (
            SELECT STRING_AGG(snapshot_source, ', ' ORDER BY snapshot_source) AS recent_theme_sources
            FROM (
                SELECT DISTINCT snapshot_source
                FROM (
                    SELECT snapshot_source
                    FROM theme_snapshots
                    ORDER BY snapshot_time DESC, run_id DESC
                    LIMIT ?
                )
            )
        ),
        recent_ticker_sources AS (
            SELECT STRING_AGG(snapshot_source, ', ' ORDER BY snapshot_source) AS recent_ticker_sources
            FROM (
                SELECT DISTINCT snapshot_source
                FROM (
                    SELECT s.snapshot_source
                    FROM ticker_snapshots s
                    JOIN refresh_runs r ON r.run_id = s.run_id
                    WHERE r.status IN ('success', 'partial')
                    ORDER BY s.run_id DESC
                    LIMIT ?
                )
            )
        )
        SELECT
            (SELECT COUNT(*) FROM themes) AS themes_count,
            (SELECT COUNT(*) FROM ticker_snapshots) AS ticker_snapshot_rows,
            (SELECT COUNT(*) FROM theme_snapshots) AS theme_snapshot_rows,
            (SELECT COUNT(DISTINCT run_id) FROM theme_snapshots) AS runs_with_theme_snapshots,
            lr.run_id AS latest_run_id,
            lr.provider AS latest_run_provider,
            lr.status AS latest_run_status,
            lr.finished_at AS latest_run_finished_at,
            lt.latest_theme_snapshot_time,
            lk.latest_ticker_snapshot_time,
            lt.theme_snapshot_sets,
            lk.ticker_snapshot_sets,
            COALESCE(rts.recent_theme_sources, '') AS recent_theme_sources,
            COALESCE(rks.recent_ticker_sources, '') AS recent_ticker_sources
        FROM preferred_theme_view lt
        CROSS JOIN preferred_ticker_view lk
        CROSS JOIN recent_theme_sources rts
        CROSS JOIN recent_ticker_sources rks
        LEFT JOIN last_run lr ON TRUE
        """,
        [theme_source_filter, ticker_source_filter, recent_limit, recent_limit],
    ).df()


def source_audit_status(conn, recent_limit: int = 50) -> pd.DataFrame:
    def _load(active_conn=conn) -> pd.DataFrame:
        preferred_theme = preferred_theme_snapshot_source(active_conn)
        preferred_ticker = preferred_ticker_snapshot_source(active_conn)

        recent_theme_sources = _fetchone_required(
            active_conn.execute(
                """
                SELECT COALESCE(STRING_AGG(snapshot_source, ', ' ORDER BY snapshot_source), '')
                FROM (
                    SELECT DISTINCT snapshot_source
                    FROM (
                        SELECT snapshot_source
                        FROM theme_snapshots
                        ORDER BY snapshot_time DESC, run_id DESC
                        LIMIT ?
                    )
                )
                """,
                [recent_limit],
            ),
            "recent theme snapshot sources",
        )[0]
        recent_ticker_sources = _fetchone_required(
            active_conn.execute(
                """
                SELECT COALESCE(STRING_AGG(snapshot_source, ', ' ORDER BY snapshot_source), '')
                FROM (
                    SELECT DISTINCT snapshot_source
                    FROM (
                        SELECT s.snapshot_source
                        FROM ticker_snapshots s
                        JOIN refresh_runs r ON r.run_id = s.run_id
                        WHERE r.status IN ('success', 'partial')
                        ORDER BY s.run_id DESC
                        LIMIT ?
                    )
                )
                """,
                [recent_limit],
            ),
            "recent ticker snapshot sources",
        )[0]

        latest_theme_view = latest_theme_snapshots(active_conn)
        latest_ticker_view = latest_ticker_snapshots(active_conn)
        latest_theme_view_sources = ", ".join(sorted(set(latest_theme_view["snapshot_source"].dropna().astype(str).tolist()))) if not latest_theme_view.empty and "snapshot_source" in latest_theme_view.columns else ""
        latest_ticker_view_sources = ", ".join(sorted(set(latest_ticker_view["snapshot_source"].dropna().astype(str).tolist()))) if not latest_ticker_view.empty and "snapshot_source" in latest_ticker_view.columns else ""

        def _mixed(text: str) -> bool:
            return bool(text and "," in text)

        theme_view_live_only = bool(preferred_theme == "live" and latest_theme_view_sources == "live")
        ticker_view_live_only = bool(preferred_ticker == "live" and latest_ticker_view_sources == "live")
        active_contamination = bool(
            (preferred_theme == "live" and latest_theme_view_sources and latest_theme_view_sources != "live")
            or (preferred_ticker == "live" and latest_ticker_view_sources and latest_ticker_view_sources != "live")
        )
        historical_residue_only = bool(
            not active_contamination
            and ((_mixed(recent_theme_sources) and preferred_theme == "live") or (_mixed(recent_ticker_sources) and preferred_ticker == "live"))
        )

        return pd.DataFrame(
            [
                {
                    "preferred_theme_source": preferred_theme,
                    "preferred_ticker_source": preferred_ticker,
                    "recent_theme_sources": recent_theme_sources or "",
                    "recent_ticker_sources": recent_ticker_sources or "",
                    "latest_theme_view_sources": latest_theme_view_sources or "",
                    "latest_ticker_view_sources": latest_ticker_view_sources or "",
                    "theme_history_mixed": _mixed(recent_theme_sources or ""),
                    "ticker_history_mixed": _mixed(recent_ticker_sources or ""),
                    "theme_current_live_only": theme_view_live_only,
                    "ticker_current_live_only": ticker_view_live_only,
                    "active_contamination": active_contamination,
                    "historical_residue_only": historical_residue_only,
                }
            ]
        )

    return _with_bootstrap_recovery(_load)
