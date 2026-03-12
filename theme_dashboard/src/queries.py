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
    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT ts.run_id, ts.snapshot_time, ts.ticker_count,
               ts.avg_1w, ts.avg_1m, ts.avg_3m,
               ts.positive_1w_breadth_pct, ts.positive_1m_breadth_pct, ts.positive_3m_breadth_pct,
               ts.composite_score
        FROM theme_snapshots ts
        WHERE ts.theme_id = ?
          AND ts.snapshot_source = ?
        ORDER BY ts.run_id DESC
        LIMIT ?
        """,
        [theme_id, preferred_source, limit],
    ).df()


def theme_history_window(conn, lookback_days: int) -> pd.DataFrame:
    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    # Boundary-based windowing keeps short lookbacks (especially 1W) stable on sparse/weekly cadence.
    # We anchor to the latest available snapshot and pick the nearest snapshot at or before start target.
    return conn.execute(
        """
        WITH latest AS (
            SELECT MAX(snapshot_time) AS latest_time
            FROM theme_snapshots
            WHERE snapshot_source = ?
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
              AND ts.snapshot_source = ?
        ),
        effective AS (
            SELECT
                COALESCE(sp.start_time, (SELECT MIN(snapshot_time) FROM theme_snapshots WHERE snapshot_source = ?)) AS start_time,
                b.end_time AS end_time
            FROM bounds b
            LEFT JOIN start_pick sp ON TRUE
        )
        SELECT ts.run_id, ts.snapshot_time, ts.theme_id, t.name AS theme, t.category,
               ts.ticker_count, ts.avg_1w, ts.avg_1m, ts.avg_3m,
               ts.snapshot_source,
               ts.positive_1m_breadth_pct, ts.composite_score
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        JOIN effective e ON ts.snapshot_time BETWEEN e.start_time AND e.end_time
        WHERE ts.snapshot_source = ?
        ORDER BY ts.snapshot_time ASC, ts.composite_score DESC
        """,
        [preferred_source, lookback_days, preferred_source, preferred_source, preferred_source],
    ).df()


def top_theme_movers(conn, lookback_days: int, top_n: int = 20) -> pd.DataFrame:
    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    theme_source_expr = _theme_snapshot_source_expr(conn)
    return conn.execute(
        f"""
        WITH latest AS (
            SELECT MAX(snapshot_time) AS latest_time
            FROM theme_snapshots
            WHERE {theme_source_expr} = ?
        ),
        bounds AS (
            SELECT
                latest_time AS end_time,
                latest_time - (? * INTERVAL '1 day') AS target_start
            FROM latest
            WHERE latest_time IS NOT NULL
        ),
        start_pick AS (
            SELECT MAX(theme_snapshots.snapshot_time) AS start_time
            FROM theme_snapshots
            JOIN bounds b ON TRUE
            WHERE theme_snapshots.snapshot_time <= b.target_start
              AND {theme_source_expr} = ?
        ),
        effective AS (
            SELECT
                COALESCE(
                    sp.start_time,
                    (SELECT MIN(snapshot_time) FROM theme_snapshots WHERE {theme_source_expr} = ?)
                ) AS start_time,
                b.end_time AS end_time
            FROM bounds b
            LEFT JOIN start_pick sp ON TRUE
        ),
        in_window AS (
            SELECT
                   ts.theme_id,
                   t.name AS theme,
                   ts.snapshot_time,
                   ts.run_id,
                   ts.composite_score,
                   ts.positive_1m_breadth_pct,
                   ROW_NUMBER() OVER (PARTITION BY ts.theme_id ORDER BY ts.snapshot_time ASC, ts.run_id ASC) AS first_rn,
                   ROW_NUMBER() OVER (PARTITION BY ts.theme_id ORDER BY ts.snapshot_time DESC, ts.run_id DESC) AS last_rn
            FROM theme_snapshots ts
            JOIN themes t ON t.id = ts.theme_id
            JOIN effective e ON ts.snapshot_time BETWEEN e.start_time AND e.end_time
            WHERE {theme_source_expr} = ?
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
        )
        SELECT f.theme_id, f.theme,
               ROUND(f.start_composite,2) AS start_composite,
               ROUND(f.end_composite,2) AS end_composite,
               ROUND(f.end_composite - f.start_composite,2) AS delta_composite,
               ROUND(f.start_breadth,2) AS start_breadth,
               ROUND(f.end_breadth,2) AS end_breadth,
               ROUND(f.end_breadth - f.start_breadth,2) AS delta_breadth
        FROM first_last f
        ORDER BY end_composite DESC, theme
        LIMIT ?
        """,
        [preferred_source, lookback_days, preferred_source, preferred_source, preferred_source, top_n],
    ).df()


def top_n_membership_changes(conn, lookback_days: int, top_n: int = 20) -> tuple[list[str], list[str]]:
    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        return [], []
    first_top = conn.execute(
        """
        WITH latest AS (
            SELECT MAX(snapshot_time) AS latest_time
            FROM theme_snapshots
            WHERE snapshot_source = ?
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
              AND ts.snapshot_source = ?
        ),
        effective AS (
            SELECT
                COALESCE(sp.start_time, (SELECT MIN(snapshot_time) FROM theme_snapshots WHERE snapshot_source = ?)) AS start_time,
                b.end_time AS end_time
            FROM bounds b
            LEFT JOIN start_pick sp ON TRUE
        )
        SELECT t.name
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        JOIN effective e ON ts.snapshot_time = e.start_time
        WHERE ts.snapshot_source = ?
        ORDER BY ts.composite_score DESC
        LIMIT ?
        """,
        [preferred_source, lookback_days, preferred_source, preferred_source, preferred_source, top_n],
    ).df()
    last_top = conn.execute(
        """
        WITH latest AS (
            SELECT MAX(snapshot_time) AS latest_time
            FROM theme_snapshots
            WHERE snapshot_source = ?
        )
        SELECT t.name
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        JOIN latest l ON ts.snapshot_time = l.latest_time
        WHERE ts.snapshot_source = ?
        ORDER BY ts.composite_score DESC
        LIMIT ?
        """,
        [preferred_source, preferred_source, top_n],
    ).df()
    start_set = set(first_top["name"].tolist()) if not first_top.empty else set()
    end_set = set(last_top["name"].tolist()) if not last_top.empty else set()
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
    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT ts.run_id, ts.snapshot_time, ts.theme_id,
               ts.ticker_count, ts.avg_1w, ts.avg_1m, ts.avg_3m,
               ts.positive_1w_breadth_pct, ts.positive_1m_breadth_pct, ts.positive_3m_breadth_pct,
               ts.composite_score, ts.snapshot_source
        FROM theme_snapshots ts
        WHERE ts.theme_id = ?
          AND ts.snapshot_source = ?
        ORDER BY ts.snapshot_time DESC
        LIMIT ?
        """,
        [theme_id, preferred_source, snapshot_limit],
    ).df()


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
