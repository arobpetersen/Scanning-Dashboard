from __future__ import annotations

import pandas as pd


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


def theme_ticker_metrics(conn, theme_id: int) -> pd.DataFrame:
    latest_run = latest_completed_runs(conn, limit=1)
    if latest_run.empty:
        return conn.execute(
            "SELECT ticker FROM theme_membership WHERE theme_id = ? ORDER BY ticker", [theme_id]
        ).df()

    latest_refresh_time = latest_run.iloc[0]["finished_at"]

    return conn.execute(
        """
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
        ),
        latest_nonnull_market_caps AS (
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
        [latest_refresh_time, theme_id],
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


def theme_history_window(conn, lookback_days: int) -> pd.DataFrame:
    # Boundary-based windowing keeps short lookbacks (especially 1W) stable on sparse/weekly cadence.
    # We anchor to the latest available snapshot and pick the nearest snapshot at or before start target.
    return conn.execute(
        """
        WITH latest AS (
            SELECT MAX(snapshot_time) AS latest_time
            FROM theme_snapshots
        ),
        bounds AS (
            SELECT
                latest_time AS end_time,
                latest_time - (? * INTERVAL '1 day') AS target_start
            FROM latest
            WHERE latest_time IS NOT NULL
        ),
        start_pick AS (
            SELECT MAX(ts.snapshot_time) AS start_time
            FROM theme_snapshots ts
            JOIN bounds b ON TRUE
            WHERE ts.snapshot_time <= b.target_start
        ),
        effective AS (
            SELECT
                COALESCE(sp.start_time, (SELECT MIN(snapshot_time) FROM theme_snapshots)) AS start_time,
                b.end_time AS end_time
            FROM bounds b
            LEFT JOIN start_pick sp ON TRUE
        )
        SELECT ts.run_id, ts.snapshot_time, ts.theme_id, t.name AS theme, t.category,
               ts.ticker_count, ts.avg_1w, ts.avg_1m, ts.avg_3m,
               ts.positive_1m_breadth_pct, ts.composite_score
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        JOIN effective e ON ts.snapshot_time BETWEEN e.start_time AND e.end_time
        ORDER BY ts.snapshot_time ASC, ts.composite_score DESC
        """,
        [lookback_days],
    ).df()


def top_theme_movers(conn, lookback_days: int, top_n: int = 20) -> pd.DataFrame:
    return conn.execute(
        """
        WITH in_window AS (
            SELECT ts.theme_id, t.name AS theme, ts.snapshot_time, ts.composite_score, ts.positive_1m_breadth_pct,
                   ROW_NUMBER() OVER (PARTITION BY ts.theme_id ORDER BY ts.snapshot_time ASC) AS first_rn,
                   ROW_NUMBER() OVER (PARTITION BY ts.theme_id ORDER BY ts.snapshot_time DESC) AS last_rn
            FROM theme_snapshots ts
            JOIN themes t ON t.id = ts.theme_id
            WHERE ts.snapshot_time >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')
        ),
        first_last AS (
            SELECT
                w.theme_id,
                MAX(CASE WHEN w.first_rn = 1 THEN w.theme END) AS theme,
                MAX(CASE WHEN w.first_rn = 1 THEN w.composite_score END) AS start_composite,
                MAX(CASE WHEN w.last_rn = 1 THEN w.composite_score END) AS end_composite,
                MAX(CASE WHEN w.first_rn = 1 THEN w.positive_1m_breadth_pct END) AS start_breadth,
                MAX(CASE WHEN w.last_rn = 1 THEN w.positive_1m_breadth_pct END) AS end_breadth
            FROM in_window w
            GROUP BY w.theme_id
        ),
        ranks AS (
            SELECT
                ts.theme_id,
                ROW_NUMBER() OVER (ORDER BY CASE WHEN ts.snapshot_time = (SELECT MIN(snapshot_time) FROM theme_snapshots WHERE snapshot_time >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')) THEN ts.composite_score END DESC) AS start_rank,
                ROW_NUMBER() OVER (ORDER BY CASE WHEN ts.snapshot_time = (SELECT MAX(snapshot_time) FROM theme_snapshots WHERE snapshot_time >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')) THEN ts.composite_score END DESC) AS end_rank
            FROM theme_snapshots ts
            WHERE ts.snapshot_time IN (
                SELECT MIN(snapshot_time) FROM theme_snapshots WHERE snapshot_time >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')
                UNION ALL
                SELECT MAX(snapshot_time) FROM theme_snapshots WHERE snapshot_time >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')
            )
        )
        SELECT f.theme_id, f.theme,
               ROUND(f.start_composite,2) AS start_composite,
               ROUND(f.end_composite,2) AS end_composite,
               ROUND(f.end_composite - f.start_composite,2) AS delta_composite,
               ROUND(f.start_breadth,2) AS start_breadth,
               ROUND(f.end_breadth,2) AS end_breadth,
               ROUND(f.end_breadth - f.start_breadth,2) AS delta_breadth
        FROM first_last f
        QUALIFY ROW_NUMBER() OVER (ORDER BY end_composite DESC) <= ?
        """,
        [lookback_days, lookback_days, lookback_days, lookback_days, lookback_days, top_n],
    ).df()


def top_n_membership_changes(conn, lookback_days: int, top_n: int = 20) -> tuple[list[str], list[str]]:
    first_top = conn.execute(
        """
        WITH latest AS (
            SELECT MAX(snapshot_time) AS latest_time
            FROM theme_snapshots
        ),
        bounds AS (
            SELECT
                latest_time AS end_time,
                latest_time - (? * INTERVAL '1 day') AS target_start
            FROM latest
            WHERE latest_time IS NOT NULL
        ),
        start_pick AS (
            SELECT MAX(ts.snapshot_time) AS start_time
            FROM theme_snapshots ts
            JOIN bounds b ON TRUE
            WHERE ts.snapshot_time <= b.target_start
        ),
        effective AS (
            SELECT
                COALESCE(sp.start_time, (SELECT MIN(snapshot_time) FROM theme_snapshots)) AS start_time,
                b.end_time AS end_time
            FROM bounds b
            LEFT JOIN start_pick sp ON TRUE
        )
        SELECT t.name
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        JOIN effective e ON ts.snapshot_time = e.start_time
        ORDER BY ts.composite_score DESC
        LIMIT ?
        """,
        [lookback_days, top_n],
    ).df()
    last_top = conn.execute(
        """
        WITH latest AS (
            SELECT MAX(snapshot_time) AS latest_time
            FROM theme_snapshots
        )
        SELECT t.name
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        JOIN latest l ON ts.snapshot_time = l.latest_time
        ORDER BY ts.composite_score DESC
        LIMIT ?
        """,
        [top_n],
    ).df()
    start_set = set(first_top["name"].tolist()) if not first_top.empty else set()
    end_set = set(last_top["name"].tolist()) if not last_top.empty else set()
    return sorted(end_set - start_set), sorted(start_set - end_set)


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
        SELECT 'symbol_refresh_status', COUNT(*) FROM symbol_refresh_status
        UNION ALL
        SELECT 'theme_suggestions', COUNT(*) FROM theme_suggestions
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
    return conn.execute(
        """
        SELECT ts.run_id, ts.snapshot_time, ts.theme_id,
               ts.ticker_count, ts.avg_1w, ts.avg_1m, ts.avg_3m,
               ts.positive_1w_breadth_pct, ts.positive_1m_breadth_pct, ts.positive_3m_breadth_pct,
               ts.composite_score, ts.snapshot_source
        FROM theme_snapshots ts
        WHERE ts.theme_id = ?
        ORDER BY ts.snapshot_time DESC
        LIMIT ?
        """,
        [theme_id, snapshot_limit],
    ).df()


def ticker_history_last_n_snapshots(conn, ticker: str, snapshot_limit: int = 14) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT s.run_id, s.ticker, s.price, s.perf_1w, s.perf_1m, s.perf_3m,
               s.market_cap, s.avg_volume, s.last_updated,
               r.finished_at AS snapshot_time, s.snapshot_source
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE s.ticker = ?
          AND r.status IN ('success', 'partial')
        ORDER BY s.run_id DESC
        LIMIT ?
        """,
        [ticker.strip().upper(), snapshot_limit],
    ).df()


def latest_theme_snapshots(conn) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT *
        FROM theme_snapshots
        QUALIFY ROW_NUMBER() OVER (PARTITION BY theme_id ORDER BY snapshot_time DESC, run_id DESC) = 1
        """
    ).df()


def latest_ticker_snapshots(conn) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT s.*, r.finished_at AS snapshot_time
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE r.status IN ('success', 'partial')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.run_id DESC) = 1
        """
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
    return conn.execute(
        """
        WITH last_run AS (
            SELECT run_id, provider, status, finished_at
            FROM refresh_runs
            ORDER BY run_id DESC
            LIMIT 1
        ),
        latest_theme AS (
            SELECT MAX(snapshot_time) AS latest_theme_snapshot_time,
                   COUNT(DISTINCT snapshot_time) AS theme_snapshot_sets
            FROM theme_snapshots
        ),
        latest_ticker AS (
            SELECT MAX(r.finished_at) AS latest_ticker_snapshot_time,
                   COUNT(DISTINCT r.finished_at) AS ticker_snapshot_sets
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
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
        FROM latest_theme lt
        CROSS JOIN latest_ticker lk
        CROSS JOIN recent_theme_sources rts
        CROSS JOIN recent_ticker_sources rks
        LEFT JOIN last_run lr ON TRUE
        """
        ,
        [recent_limit, recent_limit],
    ).df()
