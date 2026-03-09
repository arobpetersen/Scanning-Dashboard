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
def latest_completed_run_id(conn) -> int | None:
    row = conn.execute(
        """
        SELECT run_id
        FROM refresh_runs
        WHERE status IN ('success', 'partial') AND finished_at IS NOT NULL
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return row[0] if row else None


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
        """
    ).df()
