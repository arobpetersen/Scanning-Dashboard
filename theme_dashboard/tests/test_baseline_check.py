import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from run_baseline_check import collect_baseline_check, format_baseline_check
from src.leaderboard_utils import format_top_ticker_leaders
from src.queries import (
    baseline_status,
    core_table_status,
    preferred_theme_snapshot_source,
    preferred_ticker_snapshot_source,
    source_audit_status,
    theme_ticker_metrics,
    theme_ticker_metrics_for_theme_ids,
)
from src.rankings import compute_theme_rankings, ticker_standardized_composite_score


class TestBaselineQueries(unittest.TestCase):
    def test_baseline_status_reports_recent_sources_and_latest_times(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table refresh_runs(
                run_id bigint,
                provider varchar,
                started_at timestamp,
                finished_at timestamp,
                status varchar
            )
            """
        )
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp, snapshot_source varchar
            )
            """
        )
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint, snapshot_time timestamp, theme_id bigint, ticker_count bigint, avg_1w double, avg_1m double, avg_3m double,
                positive_1w_breadth_pct double, positive_1m_breadth_pct double, positive_3m_breadth_pct double,
                composite_score double, snapshot_source varchar
            )
            """
        )
        conn.execute("create table refresh_failures(run_id bigint, ticker varchar, error_message varchar, failure_category varchar, created_at timestamp)")
        conn.execute("create table refresh_run_tickers(run_id bigint, ticker varchar)")
        conn.execute("create table symbol_refresh_status(ticker varchar)")
        conn.execute("create table theme_suggestions(suggestion_id bigint)")

        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
        conn.execute("insert into refresh_runs values (7, 'live', '2026-03-10 20:00:00', '2026-03-10 22:00:00', 'success')")
        conn.execute(
            "insert into ticker_snapshots values (7, 'NVDA', 100, 1, 2, 3, 1000, 2000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )
        conn.execute(
            "insert into theme_snapshots values (7, '2026-03-10 22:00:00', 1, 1, 1, 2, 3, 40, 50, 60, 10, 'synthetic_backfill')"
        )

        tables = core_table_status(conn)
        status = baseline_status(conn)

        self.assertTrue(bool(tables["exists"].all()))
        self.assertEqual(int(status.iloc[0]["latest_run_id"]), 7)
        self.assertEqual(status.iloc[0]["recent_ticker_sources"], "live")
        self.assertEqual(status.iloc[0]["recent_theme_sources"], "synthetic_backfill")
        conn.close()

    def test_baseline_status_latest_times_follow_preferred_current_view_source(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table refresh_runs(
                run_id bigint,
                provider varchar,
                started_at timestamp,
                finished_at timestamp,
                status varchar,
                ticker_count bigint,
                success_count bigint,
                failure_count bigint
            )
            """
        )
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp, snapshot_source varchar
            )
            """
        )
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint, snapshot_time timestamp, theme_id bigint, ticker_count bigint, avg_1w double, avg_1m double, avg_3m double,
                positive_1w_breadth_pct double, positive_1m_breadth_pct double, positive_3m_breadth_pct double,
                composite_score double, snapshot_source varchar
            )
            """
        )
        conn.execute("create table refresh_failures(run_id bigint, ticker varchar, error_message varchar, failure_category varchar, created_at timestamp)")
        conn.execute("create table refresh_run_tickers(run_id bigint, ticker varchar)")
        conn.execute("create table symbol_refresh_status(ticker varchar)")
        conn.execute("create table theme_suggestions(suggestion_id bigint)")

        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership values (1, 'NVDA')")
        conn.execute("insert into theme_membership values (1, 'MSFT')")
        conn.execute("insert into theme_membership values (1, 'AAPL')")
        conn.execute("insert into refresh_runs values (1, 'live', '2026-03-10 20:00:00', '2026-03-10 22:00:00', 'success', 1, 1, 0)")
        conn.execute("insert into refresh_runs values (2, 'mock', '2026-03-11 20:00:00', '2026-03-11 22:00:00', 'success', 1, 1, 0)")
        conn.execute(
            "insert into ticker_snapshots values (1, 'NVDA', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )
        conn.execute(
            "insert into ticker_snapshots values (1, 'MSFT', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )
        conn.execute(
            "insert into ticker_snapshots values (1, 'AAPL', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )
        conn.execute(
            "insert into ticker_snapshots values (2, 'NVDA', 200, 9, 9, 9, 2000, 300000, null, null, null, '2026-03-11 21:00:00', 'mock')"
        )
        conn.execute(
            "insert into theme_snapshots values (1, '2026-03-10 22:00:00', 1, 3, 1, 2, 3, 40, 50, 60, 10, 'live')"
        )
        conn.execute(
            "insert into theme_snapshots values (2, '2026-03-11 22:00:00', 1, 3, 9, 9, 9, 90, 90, 90, 99, 'mock')"
        )

        status = baseline_status(conn).iloc[0]

        self.assertEqual(str(status["latest_theme_snapshot_time"]), "2026-03-10 22:00:00")
        self.assertEqual(str(status["latest_ticker_snapshot_time"]), "2026-03-10 22:00:00")
        self.assertEqual(int(status["theme_snapshot_sets"]), 1)
        self.assertEqual(int(status["ticker_snapshot_sets"]), 1)
        conn.close()


class TestBaselineCheckCommand(unittest.TestCase):
    def test_collect_baseline_check_warns_when_history_is_shallow(self):
        db_path = Path(__file__).resolve().parent / "test_baseline_check.duckdb"
        if db_path.exists():
            db_path.unlink()
        try:
            with patch("src.database.DB_PATH", db_path), patch("src.config.DB_PATH", db_path), patch("run_baseline_check.DB_PATH", db_path):
                from src.database import get_conn, init_db

                init_db()
                with get_conn() as conn:
                    run_id = conn.execute(
                        """
                        insert into refresh_runs(provider, started_at, finished_at, status, ticker_count, success_count, failure_count)
                        values ('mock', '2026-03-10 20:00:00', '2026-03-10 22:00:00', 'success', 1, 1, 0)
                        returning run_id
                        """
                    ).fetchone()[0]
                    conn.execute(
                        """
                        insert into ticker_snapshots(
                            run_id, ticker, price, perf_1w, perf_1m, perf_3m, market_cap, avg_volume,
                            short_interest_pct, float_shares, adr_pct, last_updated, snapshot_source
                        )
                        values (?, 'ABC', 10, 1, 2, 3, 1000, 2000, null, null, null, '2026-03-10 21:00:00', 'mock')
                        """,
                        [run_id],
                    )
                    conn.execute(
                        """
                        insert into theme_snapshots(
                            run_id, snapshot_time, theme_id, ticker_count, avg_1w, avg_1m, avg_3m,
                            positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                            composite_score, snapshot_source
                        )
                        select ?, '2026-03-10 22:00:00', id, 1, 1, 2, 3, 40, 50, 60, 10, 'mock'
                        from themes
                        limit 1
                        """,
                        [run_id],
                    )

                report, exit_code = collect_baseline_check()
                rendered = format_baseline_check(report)

                self.assertEqual(exit_code, 0)
                self.assertIn("Only 1 theme snapshot set(s) available", rendered)
                self.assertIn("Recent ticker snapshot sources: mock", rendered)
        finally:
            if db_path.exists():
                db_path.unlink()


class TestLivePreferredSourceSelection(unittest.TestCase):
    def test_current_views_prefer_live_over_newer_mock_rows(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table refresh_runs(
                run_id bigint,
                provider varchar,
                started_at timestamp,
                finished_at timestamp,
                status varchar,
                ticker_count bigint,
                success_count bigint,
                failure_count bigint
            )
            """
        )
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp, snapshot_source varchar
            )
            """
        )
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint, snapshot_time timestamp, theme_id bigint, ticker_count bigint, avg_1w double, avg_1m double, avg_3m double,
                positive_1w_breadth_pct double, positive_1m_breadth_pct double, positive_3m_breadth_pct double,
                composite_score double, snapshot_source varchar
            )
            """
        )

        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership values (1, 'NVDA')")
        conn.execute("insert into theme_membership values (1, 'MSFT')")
        conn.execute("insert into theme_membership values (1, 'AAPL')")
        conn.execute("insert into refresh_runs values (1, 'live', '2026-03-10 20:00:00', '2026-03-10 22:00:00', 'success', 1, 1, 0)")
        conn.execute("insert into refresh_runs values (2, 'mock', '2026-03-11 20:00:00', '2026-03-11 22:00:00', 'success', 1, 1, 0)")
        conn.execute(
            "insert into ticker_snapshots values (1, 'NVDA', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )
        conn.execute(
            "insert into ticker_snapshots values (1, 'MSFT', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )
        conn.execute(
            "insert into ticker_snapshots values (1, 'AAPL', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )
        conn.execute(
            "insert into ticker_snapshots values (2, 'NVDA', 200, 9, 9, 9, 2000, 300000, null, null, null, '2026-03-11 21:00:00', 'mock')"
        )
        conn.execute(
            "insert into theme_snapshots values (1, '2026-03-10 22:00:00', 1, 3, 1, 2, 3, 40, 50, 60, 10, 'live')"
        )
        conn.execute(
            "insert into theme_snapshots values (2, '2026-03-11 22:00:00', 1, 3, 9, 9, 9, 90, 90, 90, 99, 'mock')"
        )

        self.assertEqual(preferred_theme_snapshot_source(conn), "live")
        self.assertEqual(preferred_ticker_snapshot_source(conn), "live")

        theme_view = theme_ticker_metrics(conn, 1)
        rankings = compute_theme_rankings(conn)
        audit = source_audit_status(conn).iloc[0]

        self.assertEqual(float(theme_view.iloc[0]["price"]), 100.0)
        self.assertEqual(float(rankings.iloc[0]["avg_1m"]), 2.0)
        self.assertEqual(audit["latest_theme_view_sources"], "live")
        self.assertEqual(audit["latest_ticker_view_sources"], "live")
        self.assertTrue(bool(audit["historical_residue_only"]))
        self.assertFalse(bool(audit["active_contamination"]))
        conn.close()

    def test_batched_theme_ticker_metrics_matches_single_theme_helper(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table refresh_runs(
                run_id bigint,
                provider varchar,
                started_at timestamp,
                finished_at timestamp,
                status varchar,
                ticker_count bigint,
                success_count bigint,
                failure_count bigint
            )
            """
        )
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp, snapshot_source varchar
            )
            """
        )

        conn.execute("insert into themes values (1, 'AI', 'Tech', true), (2, 'Power', 'Infra', true)")
        conn.execute("insert into theme_membership values (1, 'NVDA'), (1, 'MSFT'), (2, 'VRT')")
        conn.execute("insert into refresh_runs values (1, 'live', '2026-03-10 20:00:00', '2026-03-10 22:00:00', 'success', 3, 3, 0)")
        conn.execute(
            """
            insert into ticker_snapshots values
            (1, 'NVDA', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-10 21:00:00', 'live'),
            (1, 'MSFT', 101, 2, 3, 4, 1001, 200001, null, null, null, '2026-03-10 21:00:00', 'live'),
            (1, 'VRT', 102, 3, 4, 5, 1002, 200002, null, null, null, '2026-03-10 21:00:00', 'live')
            """
        )

        single_theme = theme_ticker_metrics(conn, 1).sort_values("ticker").reset_index(drop=True)
        batched = theme_ticker_metrics_for_theme_ids(conn, [1, 2]).sort_values(["theme_id", "ticker"]).reset_index(drop=True)

        self.assertEqual(sorted(batched["theme_id"].unique().tolist()), [1, 2])
        batched_theme_one = batched[batched["theme_id"] == 1].drop(columns=["theme_id"]).reset_index(drop=True)
        self.assertTrue(single_theme.equals(batched_theme_one))
        single_scored = single_theme.copy()
        single_scored["ticker_composite_score"] = single_scored.apply(
            lambda row: ticker_standardized_composite_score(row.get("perf_1w"), row.get("perf_1m"), row.get("perf_3m")),
            axis=1,
        )
        batched_scored = batched_theme_one.copy()
        batched_scored["ticker_composite_score"] = batched_scored.apply(
            lambda row: ticker_standardized_composite_score(row.get("perf_1w"), row.get("perf_1m"), row.get("perf_3m")),
            axis=1,
        )
        self.assertEqual(
            format_top_ticker_leaders(single_scored, top_k=3),
            format_top_ticker_leaders(batched_scored, top_k=3),
        )
        self.assertEqual(format_top_ticker_leaders(single_scored, top_k=2), "MSFT, NVDA")
        conn.close()


if __name__ == "__main__":
    unittest.main()
