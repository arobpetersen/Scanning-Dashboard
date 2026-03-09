from __future__ import annotations

import pandas as pd


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


def latest_completed_run_id(conn) -> int | None:
    runs = latest_completed_runs(conn, limit=1)
    if runs.empty:
        return None
    return int(runs.iloc[0]["run_id"])


def theme_ticker_metrics(conn, theme_id: int) -> pd.DataFrame:
    run_id = latest_completed_run_id(conn)
    if run_id is None:
        return conn.execute(
            "SELECT ticker FROM theme_membership WHERE theme_id = ? ORDER BY ticker", [theme_id]
        ).df()

    return conn.execute(
        """
        SELECT m.ticker, s.price, s.perf_1w, s.perf_1m, s.perf_3m,
               s.market_cap, s.avg_volume, s.short_interest_pct, s.float_shares,
               s.adr_pct, s.last_updated
        FROM theme_membership m
        LEFT JOIN ticker_snapshots s
          ON m.ticker = s.ticker AND s.run_id = ?
        WHERE m.theme_id = ?
        ORDER BY m.ticker
        """,
        [run_id, theme_id],
    ).df()


def theme_snapshot_history(conn, theme_id: int, limit: int = 20) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT ts.run_id, ts.snapshot_time, ts.ticker_count,
               ts.avg_1w, ts.avg_1m, ts.avg_3m,
               ts.positive_1w_breadth_pct, ts.positive_1m_breadth_pct, ts.positive_3m_breadth_pct,
               ts.composite_score
        FROM theme_snapshots ts
        WHERE ts.theme_id = ?
        ORDER BY ts.run_id DESC
        LIMIT ?
        """,
        [theme_id, limit],
    ).df()


def theme_health_overview(conn, low_constituent_threshold: int, failure_window_days: int = 14) -> pd.DataFrame:
    return conn.execute(
        """
        WITH member_counts AS (
            SELECT t.id AS theme_id, COUNT(m.ticker) AS constituent_count
            FROM themes t
            LEFT JOIN theme_membership m ON t.id = m.theme_id
            GROUP BY t.id
        ),
        latest_snap AS (
            SELECT theme_id, MAX(snapshot_time) AS latest_snapshot_time
            FROM theme_snapshots
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
        [failure_window_days, low_constituent_threshold, low_constituent_threshold],
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
        SELECT 'theme_suggestions', COUNT(*) FROM theme_suggestions
        """
    ).df()
