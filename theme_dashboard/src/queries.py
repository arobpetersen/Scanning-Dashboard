from __future__ import annotations

import pandas as pd

from .config import COMPOSITE_WEIGHTS, THEME_CONFIDENCE_FULL_COUNT


RECENT_TICKER_HISTORY_DERIVED_CALENDAR_DAYS = 45
TICKER_HISTORY_BUFFER_DAYS = 120
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


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_columns()
        WHERE table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_tables()
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _theme_snapshot_source_expr(conn) -> str:
    if _table_has_column(conn, "theme_snapshots", "snapshot_source"):
        return "snapshot_source"
    if _table_has_column(conn, "refresh_runs", "provider"):
        return "COALESCE((SELECT provider FROM refresh_runs rr WHERE rr.run_id = theme_snapshots.run_id), 'live')"
    return "'live'"


def _ticker_snapshot_source_expr(conn) -> str:
    if _table_has_column(conn, "ticker_snapshots", "snapshot_source"):
        return "s.snapshot_source"
    if _table_has_column(conn, "refresh_runs", "provider"):
        return "COALESCE(r.provider, 'live')"
    return "'live'"


def _historical_theme_snapshot_union(conn) -> pd.DataFrame:
    preferred_source = preferred_theme_snapshot_source(conn) or _preferred_ticker_history_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    positive_1w_expr = "ts.positive_1w_breadth_pct" if _table_has_column(conn, "theme_snapshots", "positive_1w_breadth_pct") else "NULL"
    positive_3m_expr = "ts.positive_3m_breadth_pct" if _table_has_column(conn, "theme_snapshots", "positive_3m_breadth_pct") else "NULL"

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
        """,
        [preferred_source],
    ).df()

    reconstructed = pd.DataFrame()
    if _table_exists(conn, "reconstructed_theme_snapshots"):
        reconstructed = conn.execute(
            """
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
            """,
            [preferred_source],
        ).df()

    ticker_history_derived = _recent_ticker_history_theme_history(conn, preferred_source)

    frames = [frame for frame in [captured, ticker_history_derived, reconstructed] if not frame.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if combined.empty:
        return combined

    combined["snapshot_time"] = pd.to_datetime(combined["snapshot_time"])
    combined["snapshot_date"] = pd.to_datetime(combined["snapshot_date"]).dt.date
    combined["_precedence"] = combined["provenance_class"].map(
        {"captured": 0, "ticker_history_derived": 1, "reconstructed": 2}
    ).fillna(9)
    combined = (
        combined.sort_values(["theme_id", "snapshot_date", "_precedence", "snapshot_time"], ascending=[True, True, True, False])
        .drop_duplicates(subset=["theme_id", "snapshot_date"], keep="first")
        .drop(columns=["_precedence"])
        .sort_values(["snapshot_time", "composite_score"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return combined


def _preferred_ticker_history_source(conn) -> str | None:
    if not _table_exists(conn, "ticker_daily_history"):
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


def _compute_ticker_history_perf(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=["ticker", "trading_date", "close", "perf_1w", "perf_1m", "perf_3m"])

    enriched = history.sort_values(["ticker", "trading_date"]).copy()
    enriched["trading_date"] = pd.to_datetime(enriched["trading_date"]).dt.date
    grouped = enriched.groupby("ticker")["close"]
    enriched["perf_1w"] = ((grouped.transform(lambda s: s / s.shift(5))) - 1.0) * 100.0
    enriched["perf_1m"] = ((grouped.transform(lambda s: s / s.shift(21))) - 1.0) * 100.0
    enriched["perf_3m"] = ((grouped.transform(lambda s: s / s.shift(63))) - 1.0) * 100.0
    return enriched


def _recent_ticker_history_theme_history(conn, market_data_source: str) -> pd.DataFrame:
    if not market_data_source or not _table_exists(conn, "ticker_daily_history"):
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

    recent_start = pd.Timestamp(max_trading_date) - pd.Timedelta(days=RECENT_TICKER_HISTORY_DERIVED_CALENDAR_DAYS)
    buffer_start = recent_start - pd.Timedelta(days=TICKER_HISTORY_BUFFER_DAYS)

    membership = conn.execute(
        """
        SELECT
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            m.ticker,
            CASE WHEN COALESCE(s.status, 'active') = 'refresh_suppressed' THEN FALSE ELSE TRUE END AS is_eligible
        FROM themes t
        JOIN theme_membership m ON m.theme_id = t.id
        LEFT JOIN symbol_refresh_status s ON s.ticker = m.ticker
        ORDER BY t.id, m.ticker
        """
    ).df()
    if membership.empty:
        return pd.DataFrame()

    history = conn.execute(
        """
        WITH governed_tickers AS (
            SELECT DISTINCT ticker
            FROM theme_membership
        )
        SELECT
            h.ticker,
            h.trading_date,
            h.close
        FROM ticker_daily_history h
        JOIN governed_tickers g ON g.ticker = h.ticker
        WHERE h.market_data_source = ?
          AND h.trading_date BETWEEN ? AND ?
        ORDER BY h.ticker, h.trading_date
        """,
        [market_data_source, pd.Timestamp(buffer_start).date(), pd.Timestamp(max_trading_date).date()],
    ).df()
    if history.empty:
        return pd.DataFrame()

    perf = _compute_ticker_history_perf(history)
    perf = perf[pd.to_datetime(perf["trading_date"]).dt.date >= pd.Timestamp(recent_start).date()].copy()
    if perf.empty:
        return pd.DataFrame()

    recent_dates = pd.DataFrame({"trading_date": sorted(pd.to_datetime(perf["trading_date"]).dt.date.unique().tolist())})
    membership = membership.copy()
    membership["_cross_key"] = 1
    recent_dates["_cross_key"] = 1
    raw = membership.merge(recent_dates, on="_cross_key", how="inner").drop(columns=["_cross_key"])
    raw = raw.merge(perf, on=["ticker", "trading_date"], how="left")
    if raw.empty:
        return pd.DataFrame()

    raw["eligible_member"] = raw["is_eligible"].astype(bool).astype(int)
    raw["covered_eligible_member"] = ((raw["is_eligible"].astype(bool)) & raw["close"].notna()).astype(int)

    grouped = raw.groupby(["theme_id", "theme", "category", "is_active", "trading_date"], dropna=False)
    metrics = grouped.agg(
        ticker_count=("ticker", "count"),
        avg_1w=("perf_1w", "mean"),
        avg_1m=("perf_1m", "mean"),
        avg_3m=("perf_3m", "mean"),
        positive_1w_breadth_pct=("perf_1w", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
        positive_1m_breadth_pct=("perf_1m", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
        positive_3m_breadth_pct=("perf_3m", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
        eligible_constituent_count=("eligible_member", "sum"),
        covered_eligible_constituent_count=("covered_eligible_member", "sum"),
    ).reset_index()
    metrics["eligible_coverage_pct"] = (
        metrics["covered_eligible_constituent_count"] / metrics["eligible_constituent_count"].replace({0: pd.NA})
    ) * 100.0
    base_score = (
        COMPOSITE_WEIGHTS["perf_1w"] * metrics["avg_1w"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_1m"] * metrics["avg_1m"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_3m"] * metrics["avg_3m"].fillna(0)
    )
    metrics["composite_score"] = base_score * metrics["ticker_count"].apply(_theme_confidence_factor_for_history)
    metrics = metrics[
        (metrics["eligible_constituent_count"] > 0)
        & (metrics["covered_eligible_constituent_count"] > 0)
        & (
            (
                metrics["covered_eligible_constituent_count"]
                / metrics["eligible_constituent_count"].replace({0: pd.NA})
            )
            >= TICKER_HISTORY_ELIGIBLE_COVERAGE_THRESHOLD
        )
    ].copy()
    if metrics.empty:
        return pd.DataFrame()

    metrics["snapshot_date"] = pd.to_datetime(metrics["trading_date"]).dt.date
    metrics["snapshot_time"] = pd.to_datetime(metrics["snapshot_date"])
    metrics["run_id"] = pd.NA
    metrics["snapshot_source"] = market_data_source
    metrics["provenance_class"] = "ticker_history_derived"
    metrics["provenance_source_label"] = "ticker_daily_history_recent"
    metrics[
        [
            "avg_1w",
            "avg_1m",
            "avg_3m",
            "positive_1w_breadth_pct",
            "positive_1m_breadth_pct",
            "positive_3m_breadth_pct",
            "composite_score",
            "eligible_coverage_pct",
        ]
    ] = metrics[
        [
            "avg_1w",
            "avg_1m",
            "avg_3m",
            "positive_1w_breadth_pct",
            "positive_1m_breadth_pct",
            "positive_3m_breadth_pct",
            "composite_score",
            "eligible_coverage_pct",
        ]
    ].round(2)
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
            "positive_1w_breadth_pct",
            "positive_1m_breadth_pct",
            "positive_3m_breadth_pct",
            "composite_score",
            "snapshot_source",
            "provenance_class",
            "provenance_source_label",
        ]
    ].sort_values(["snapshot_time", "theme"]).reset_index(drop=True)


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
    if _table_has_column(conn, "theme_snapshots", "snapshot_source"):
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
    else:
        if _table_has_column(conn, "refresh_runs", "provider"):
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
        else:
            row = ("live",)
    return str(row[0]) if row and row[0] else None


def preferred_ticker_snapshot_source(conn) -> str | None:
    if _table_has_column(conn, "ticker_snapshots", "snapshot_source"):
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
    else:
        if _table_has_column(conn, "refresh_runs", "provider"):
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
        else:
            row = ("live",)
    return str(row[0]) if row and row[0] else None


def theme_ticker_metrics(conn, theme_id: int) -> pd.DataFrame:
    preferred_source = preferred_ticker_snapshot_source(conn)
    if not preferred_source:
        return conn.execute(
            "SELECT ticker FROM theme_membership WHERE theme_id = ? ORDER BY ticker", [theme_id]
        ).df()

    if _table_has_column(conn, "ticker_snapshots", "snapshot_source"):
        ticker_source_expr = "s.snapshot_source"
    elif _table_has_column(conn, "refresh_runs", "provider"):
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
        WITH completed_snapshots AS (
            SELECT
                s.ticker,
                s.price,
                s.perf_1w,
                s.perf_1m,
                s.perf_3m,
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
                s.ticker,
                s.market_cap,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.run_id DESC) AS rn
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
              AND {ticker_source_expr} = ?
              AND s.market_cap IS NOT NULL
        )
        SELECT
            m.ticker,
            cs.price,
            cs.perf_1w,
            cs.perf_1m,
            cs.perf_3m,
            COALESCE(cs.market_cap, lmc.market_cap) AS market_cap,
            cs.avg_volume,
            cs.short_interest_pct,
            cs.float_shares,
            cs.adr_pct,
            cs.last_updated,
            cs.snapshot_time,
            ? AS latest_refresh_time
        FROM theme_membership m
        LEFT JOIN completed_snapshots cs
          ON m.ticker = cs.ticker AND cs.rn = 1
        LEFT JOIN latest_nonnull_market_caps lmc
          ON m.ticker = lmc.ticker AND lmc.rn = 1
        WHERE m.theme_id = ?
        ORDER BY m.ticker
        """,
        [preferred_source, preferred_source, latest_refresh_time, theme_id],
    ).df()


def theme_snapshot_history(conn, theme_id: int, limit: int = 20) -> pd.DataFrame:
    history = _historical_theme_snapshot_union(conn)
    if history.empty:
        return pd.DataFrame()
    view = history[history["theme_id"] == int(theme_id)].copy()
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


def theme_history_window(conn, lookback_days: int) -> pd.DataFrame:
    history = _historical_theme_snapshot_union(conn)
    if history.empty:
        return pd.DataFrame()

    boundary_times = pd.to_datetime(history["snapshot_time"]).dropna().drop_duplicates().sort_values()
    if boundary_times.empty:
        return pd.DataFrame()

    end_time = boundary_times.iloc[-1]
    target_start = end_time - pd.Timedelta(days=int(lookback_days))
    start_candidates = boundary_times[boundary_times <= target_start]
    start_time = start_candidates.iloc[-1] if not start_candidates.empty else boundary_times.iloc[0]

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
        .sort_values(["composite_score", "theme"], ascending=[False, True])
        .head(top_n)
    )
    end_top = (
        history[pd.to_datetime(history["snapshot_time"]) == end_time]
        .sort_values(["composite_score", "theme"], ascending=[False, True])
        .head(top_n)
    )
    start_set = set(start_top["theme"].astype(str).tolist()) if not start_top.empty else set()
    end_set = set(end_top["theme"].astype(str).tolist()) if not end_top.empty else set()
    return sorted(end_set - start_set), sorted(start_set - end_set)


def theme_health_overview(conn, low_constituent_threshold: int, failure_window_days: int = 14) -> pd.DataFrame:
    preferred_source = preferred_theme_snapshot_source(conn)
    theme_source_filter = preferred_source or "__no_source__"
    theme_source_expr = _theme_snapshot_source_expr(conn)
    return conn.execute(
        f"""
        WITH member_counts AS (
            SELECT t.id AS theme_id, COUNT(m.ticker) AS constituent_count
            FROM themes t
            LEFT JOIN theme_membership m ON t.id = m.theme_id
            GROUP BY t.id
        ),
        latest_snap AS (
            SELECT theme_id, MAX(snapshot_time) AS latest_snapshot_time
            FROM theme_snapshots
            WHERE {theme_source_expr} = ?
            GROUP BY theme_id
        ),
        live_failures_by_theme AS (
            SELECT m.theme_id, COUNT(*) AS live_failure_count_recent
            FROM refresh_failures f
            JOIN refresh_runs r ON r.run_id = f.run_id
            JOIN theme_membership m ON m.ticker = f.ticker
            WHERE r.provider = 'live'
              AND f.created_at >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')
            GROUP BY m.theme_id
        )
        SELECT
            t.id AS theme_id,
            t.name AS theme_name,
            t.category,
            t.is_active,
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
    return conn.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM ticker_snapshots) AS ticker_snapshot_rows,
          (SELECT COUNT(*) FROM theme_snapshots) AS theme_snapshot_rows,
          (SELECT COUNT(DISTINCT run_id) FROM theme_snapshots) AS runs_with_theme_snapshots
        """
    ).df()


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


def theme_history_last_n_snapshots(conn, theme_id: int, snapshot_limit: int = 14) -> pd.DataFrame:
    history = _historical_theme_snapshot_union(conn)
    if history.empty:
        return pd.DataFrame()
    view = history[history["theme_id"] == int(theme_id)].copy()
    if view.empty:
        return view
    return (
        view[
            [
                "run_id",
                "snapshot_time",
                "theme_id",
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
        .head(snapshot_limit)
        .reset_index(drop=True)
    )


def ticker_history_last_n_snapshots(conn, ticker: str, snapshot_limit: int = 14) -> pd.DataFrame:
    preferred_source = preferred_ticker_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT s.run_id, s.ticker, s.price, s.perf_1w, s.perf_1m, s.perf_3m,
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


def ticker_lookup_summary(conn, ticker: str) -> pd.DataFrame:
    normalized = (ticker or "").strip().upper()
    if not normalized:
        return pd.DataFrame()

    ticker_source_expr = _ticker_snapshot_source_expr(conn)
    return conn.execute(
        f"""
        WITH membership AS (
            SELECT COUNT(*) AS membership_count
            FROM theme_membership
            WHERE ticker = ?
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
        refresh_seen AS (
            SELECT COUNT(*) AS refresh_run_count
            FROM refresh_run_tickers
            WHERE ticker = ?
        ),
        symbol_seen AS (
            SELECT COUNT(*) AS symbol_status_count
            FROM symbol_refresh_status
            WHERE ticker = ?
        )
        SELECT
            ? AS ticker,
            CAST(m.membership_count > 0 AS BOOLEAN) AS exists_in_theme_membership,
            CAST(s.snapshot_count > 0 AS BOOLEAN) AS exists_in_ticker_snapshots,
            CAST(r.refresh_run_count > 0 AS BOOLEAN) AS exists_in_refresh_run_tickers,
            CAST(ss.symbol_status_count > 0 AS BOOLEAN) AS exists_in_symbol_refresh_status,
            COALESCE(m.membership_count, 0) AS assigned_theme_count,
            ls.latest_snapshot_time,
            ls.latest_snapshot_source,
            ls.latest_price,
            ls.latest_market_cap,
            ls.latest_avg_volume,
            CASE
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
        """,
        [normalized, normalized, normalized, normalized, normalized, normalized],
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
        WHERE m.ticker = ?
        ORDER BY t.name
        """,
        [normalized],
    ).df()


def theme_member_hygiene_context(conn, theme_id: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            m.ticker,
            s.last_failure_category,
            s.last_failure_at,
            s.consecutive_failure_count,
            s.status AS symbol_hygiene_status
        FROM theme_membership m
        LEFT JOIN symbol_refresh_status s ON s.ticker = m.ticker
        WHERE m.theme_id = ?
        ORDER BY
            CASE WHEN s.last_failure_at IS NULL THEN 1 ELSE 0 END,
            s.last_failure_at DESC,
            m.ticker
        """,
        [theme_id],
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
    if not _table_exists(conn, "historical_reconstruction_runs"):
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
    governed_active_tickers = conn.execute(
        """
        SELECT DISTINCT m.ticker
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        WHERE t.is_active = TRUE
        ORDER BY m.ticker
        """
    ).df()
    governed_count = int(len(governed_active_tickers))

    if not _table_exists(conn, "ticker_daily_history"):
        return pd.DataFrame(
            [
                {
                    "target_trading_days": int(target_trading_days),
                    "market_data_source": None,
                    "available_trading_days": 0,
                    "remaining_trading_days": int(target_trading_days),
                    "governed_active_tickers": governed_count,
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

    coverage = conn.execute(
        """
        WITH governed AS (
            SELECT DISTINCT m.ticker
            FROM theme_membership m
            JOIN themes t ON t.id = m.theme_id
            WHERE t.is_active = TRUE
        )
        SELECT
            g.ticker,
            COUNT(DISTINCT h.trading_date) AS trading_day_rows
        FROM governed g
        LEFT JOIN ticker_daily_history h
          ON h.ticker = g.ticker
         AND h.market_data_source = ?
        GROUP BY g.ticker
        ORDER BY g.ticker
        """,
        [market_data_source],
    ).df()
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


def tickers_dimension(conn) -> pd.DataFrame:
    return conn.execute(
        """
        WITH latest_completed AS (
            SELECT
                s.ticker,
                s.avg_volume,
                s.last_updated,
                r.finished_at AS latest_snapshot_time,
                ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.run_id DESC) AS rn
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
    preferred_theme = preferred_theme_snapshot_source(conn)
    preferred_ticker = preferred_ticker_snapshot_source(conn)

    recent_theme_sources = conn.execute(
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
    ).fetchone()[0]
    recent_ticker_sources = conn.execute(
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
    ).fetchone()[0]

    latest_theme_view = latest_theme_snapshots(conn)
    latest_ticker_view = latest_ticker_snapshots(conn)
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
