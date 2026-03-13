import unittest
from unittest.mock import patch

import duckdb
import pandas as pd

from src.database import SCHEMA_SQL
from src.historical_backfill import rebuild_recent_reconstructed_history, reconstruct_theme_history_range
from src.momentum_engine import compute_theme_momentum
from src.queries import (
    classify_ticker_history_readiness,
    theme_history_window,
    theme_snapshot_history,
    ticker_history_readiness,
)
from src.rankings import compute_theme_rankings
from src.ticker_history import persist_ticker_daily_history, ticker_daily_history_rows


class TestHistoricalBackfill(unittest.TestCase):
    def _conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        return conn

    def test_reconstructed_backfill_is_idempotent_for_same_date_theme_source(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")

        first = reconstruct_theme_history_range(
            conn,
            provider_name="mock",
            start_date="2026-02-01",
            end_date="2026-02-05",
            provenance_source_label="historical_backfill",
        )
        row_count_after_first = int(conn.execute("select count(*) from reconstructed_theme_snapshots").fetchone()[0])
        ticker_history_count_after_first = int(conn.execute("select count(*) from ticker_daily_history").fetchone()[0])

        second = reconstruct_theme_history_range(
            conn,
            provider_name="mock",
            start_date="2026-02-01",
            end_date="2026-02-05",
            provenance_source_label="historical_backfill",
        )
        row_count_after_second = int(conn.execute("select count(*) from reconstructed_theme_snapshots").fetchone()[0])
        ticker_history_count_after_second = int(conn.execute("select count(*) from ticker_daily_history").fetchone()[0])

        self.assertGreater(int(first["snapshot_rows_written"]), 0)
        self.assertGreater(int(first["ticker_history_rows_written"]), 0)
        self.assertEqual(int(second["snapshot_rows_written"]), 0)
        self.assertGreater(int(second["snapshot_rows_skipped"]), 0)
        self.assertEqual(int(second["ticker_history_rows_written"]), 0)
        self.assertGreater(int(second["ticker_history_rows_skipped"]), 0)
        self.assertEqual(row_count_after_first, row_count_after_second)
        self.assertEqual(ticker_history_count_after_first, ticker_history_count_after_second)
        conn.close()

    def test_ticker_daily_history_write_is_idempotent_and_replaceable(self):
        conn = self._conn()
        history = pd.DataFrame(
            [
                {
                    "ticker": "NVDA",
                    "snapshot_date": "2026-03-10",
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 105.0,
                    "volume": 1000.0,
                    "vwap": 103.0,
                    "trade_count": 50,
                }
            ]
        )

        first = persist_ticker_daily_history(
            conn,
            history,
            ticker="NVDA",
            provenance_source_label="ticker_intake_backfill",
            market_data_source="live",
            run_id=1,
            replace_existing=False,
        )
        second = persist_ticker_daily_history(
            conn,
            history,
            ticker="NVDA",
            provenance_source_label="ticker_intake_backfill",
            market_data_source="live",
            run_id=2,
            replace_existing=False,
        )
        updated = history.copy()
        updated.loc[0, "close"] = 111.0
        third = persist_ticker_daily_history(
            conn,
            updated,
            ticker="NVDA",
            provenance_source_label="ticker_intake_backfill",
            market_data_source="live",
            run_id=3,
            replace_existing=True,
        )

        stored = ticker_daily_history_rows(conn, tickers=["NVDA"], provenance_source_label="ticker_intake_backfill")

        self.assertEqual(first, {"rows_written": 1, "rows_skipped": 0})
        self.assertEqual(second, {"rows_written": 0, "rows_skipped": 1})
        self.assertEqual(third, {"rows_written": 1, "rows_skipped": 0})
        self.assertEqual(len(stored), 1)
        self.assertEqual(float(stored.iloc[0]["close"]), 111.0)
        conn.close()

    def test_theme_history_window_uses_mixed_captured_and_reconstructed_history(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute(
            """
            insert into theme_snapshots(
                run_id, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, snapshot_source
            ) values
            (1, '2026-03-10 22:00:00', 1, 4, 1, 2, 3, 40, 50, 60, 10, 'live'),
            (2, '2026-03-11 22:00:00', 1, 4, 2, 3, 4, 50, 60, 70, 12, 'live')
            """
        )
        conn.execute(
            """
            insert into reconstructed_theme_snapshots(
                run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
            ) values
            (101, '2026-03-09', '2026-03-09 00:00:00', 1, 4, 0.5, 1.5, 2.5, 30, 40, 50, 8, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership'),
            (102, '2026-03-10', '2026-03-10 00:00:00', 1, 4, 9.0, 9.0, 9.0, 90, 90, 90, 99, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership')
            """
        )

        history = theme_history_window(conn, 30)
        momentum = compute_theme_momentum(conn, 30)

        self.assertEqual(sorted(pd.to_datetime(history["snapshot_time"]).dt.date.astype(str).unique().tolist()), ["2026-03-09", "2026-03-10", "2026-03-11"])
        ten_mar = history[pd.to_datetime(history["snapshot_time"]).dt.date.astype(str) == "2026-03-10"]
        self.assertEqual(ten_mar.iloc[0]["provenance_class"], "captured")
        self.assertEqual(momentum["meta"]["provenance_mix"], "mixed")
        self.assertEqual(momentum["meta"]["boundary_provenance_mix"], "mixed")
        conn.close()

    def test_targeted_ticker_backfill_replaces_existing_rows_for_affected_theme_dates(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA')")

        first = reconstruct_theme_history_range(
            conn,
            provider_name="mock",
            start_date="2026-02-10",
            end_date="2026-02-12",
            provenance_source_label="ticker_intake_backfill",
            replace_existing=True,
        )
        first_count = int(conn.execute("select count(*) from reconstructed_theme_snapshots where provenance_source_label='ticker_intake_backfill'").fetchone()[0])
        first_ticker_count = int(conn.execute("select max(ticker_count) from reconstructed_theme_snapshots where provenance_source_label='ticker_intake_backfill'").fetchone()[0])
        first_history_count = int(conn.execute("select count(*) from ticker_daily_history where provenance_source_label='ticker_intake_backfill'").fetchone()[0])

        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'BBB')")
        second = reconstruct_theme_history_range(
            conn,
            provider_name="mock",
            start_date="2026-02-10",
            end_date="2026-02-12",
            tickers=["BBB"],
            provenance_source_label="ticker_intake_backfill",
            run_kind="ticker_intake_backfill",
            replace_existing=True,
        )
        second_count = int(conn.execute("select count(*) from reconstructed_theme_snapshots where provenance_source_label='ticker_intake_backfill'").fetchone()[0])
        second_ticker_count = int(conn.execute("select max(ticker_count) from reconstructed_theme_snapshots where provenance_source_label='ticker_intake_backfill'").fetchone()[0])
        second_history_count = int(conn.execute("select count(*) from ticker_daily_history where provenance_source_label='ticker_intake_backfill'").fetchone()[0])

        self.assertGreater(int(first["snapshot_rows_written"]), 0)
        self.assertGreater(int(second["snapshot_rows_written"]), 0)
        self.assertGreater(int(first["ticker_history_rows_written"]), 0)
        self.assertGreater(int(second["ticker_history_rows_written"]), 0)
        self.assertEqual(first_count, second_count)
        self.assertEqual(first_ticker_count, 1)
        self.assertEqual(second_ticker_count, 2)
        self.assertGreaterEqual(second_history_count, first_history_count)
        conn.close()

    def test_current_rankings_ignore_reconstructed_history(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Energy', 'Macro', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'MSFT')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAPL')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'XOM')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'CVX')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'SLB')")
        conn.execute(
            """
            insert into refresh_runs(run_id, provider, started_at, finished_at, status, ticker_count, success_count, failure_count) values
            (1, 'live', '2026-03-11 20:00:00', '2026-03-11 22:00:00', 'success', 6, 6, 0)
            """
        )
        conn.execute(
            """
            insert into ticker_snapshots(
                run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated, snapshot_source
            ) values
            (1, 'NVDA', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-11 21:00:00', 'live'),
            (1, 'MSFT', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-11 21:00:00', 'live'),
            (1, 'AAPL', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-11 21:00:00', 'live'),
            (1, 'XOM', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-11 21:00:00', 'live'),
            (1, 'CVX', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-11 21:00:00', 'live'),
            (1, 'SLB', 100, 1, 2, 3, 1000, 200000, null, null, null, '2026-03-11 21:00:00', 'live')
            """
        )
        conn.execute(
            """
            insert into theme_snapshots(
                run_id, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, snapshot_source
            ) values
            (1, '2026-03-11 22:00:00', 1, 8, 1, 2, 3, 40, 50, 60, 10, 'live'),
            (1, '2026-03-11 22:00:00', 2, 8, 1, 2, 3, 40, 50, 60, 5, 'live')
            """
        )
        conn.execute(
            """
            insert into reconstructed_theme_snapshots(
                run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
            ) values
            (101, '2026-03-11', '2026-03-11 00:00:00', 2, 8, 9, 9, 9, 90, 90, 90, 999, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership')
            """
        )

        rankings = compute_theme_rankings(conn)
        self.assertEqual(rankings.iloc[0]["theme"], "AI")
        self.assertEqual(float(rankings.iloc[0]["avg_1m"]), 2.0)
        conn.close()

    def test_ticker_history_readiness_handles_no_rows(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")

        readiness = ticker_history_readiness(conn, target_trading_days=30)

        self.assertEqual(int(readiness.iloc[0]["available_trading_days"]), 0)
        self.assertEqual(int(readiness.iloc[0]["remaining_trading_days"]), 30)
        self.assertEqual(int(readiness.iloc[0]["governed_active_tickers"]), 1)
        self.assertEqual(float(readiness.iloc[0]["governed_ready_pct"]), 0.0)
        self.assertEqual(readiness.iloc[0]["status_label"], "accumulating")
        conn.close()

    def test_ticker_history_readiness_reports_partial_depth_and_sparse_coverage(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cloud', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'MSFT')")

        dates = pd.bdate_range("2026-02-02", periods=22)
        nvda_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
        )
        msft_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 200 + idx, "volume": 2000 + idx} for idx, ts in enumerate(dates[:10])]
        )
        persist_ticker_daily_history(conn, nvda_history, ticker="NVDA", provenance_source_label="ticker_intake_backfill", market_data_source="live")
        persist_ticker_daily_history(conn, msft_history, ticker="MSFT", provenance_source_label="ticker_intake_backfill", market_data_source="live")

        readiness = ticker_history_readiness(conn, target_trading_days=30)

        self.assertEqual(int(readiness.iloc[0]["available_trading_days"]), 22)
        self.assertEqual(int(readiness.iloc[0]["remaining_trading_days"]), 8)
        self.assertEqual(int(readiness.iloc[0]["governed_active_tickers"]), 2)
        self.assertEqual(int(readiness.iloc[0]["governed_active_tickers_ready"]), 0)
        self.assertEqual(float(readiness.iloc[0]["governed_ready_pct"]), 0.0)
        self.assertEqual(readiness.iloc[0]["status_label"], "near ready")
        conn.close()

    def test_ticker_history_readiness_reports_ready_when_depth_and_coverage_are_strong(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cloud', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (3, 'Energy', 'Macro', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'MSFT')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (3, 'XOM')")

        dates = pd.bdate_range("2026-01-15", periods=30)
        for ticker, base in [("NVDA", 100), ("MSFT", 200), ("XOM", 50)]:
            history = pd.DataFrame(
                [{"snapshot_date": ts.date(), "close": base + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
            )
            persist_ticker_daily_history(conn, history, ticker=ticker, provenance_source_label="daily_historical_append", market_data_source="live")

        readiness = ticker_history_readiness(conn, target_trading_days=30)

        self.assertEqual(int(readiness.iloc[0]["available_trading_days"]), 30)
        self.assertEqual(int(readiness.iloc[0]["remaining_trading_days"]), 0)
        self.assertEqual(int(readiness.iloc[0]["governed_active_tickers_ready"]), 3)
        self.assertEqual(float(readiness.iloc[0]["governed_ready_pct"]), 100.0)
        self.assertEqual(readiness.iloc[0]["status_label"], "ready")
        conn.close()

    def test_classify_ticker_history_readiness_thresholds(self):
        self.assertEqual(classify_ticker_history_readiness(10, 0.0, target_trading_days=30), "accumulating")
        self.assertEqual(classify_ticker_history_readiness(24, 10.0, target_trading_days=30), "near ready")
        self.assertEqual(classify_ticker_history_readiness(30, 75.0, target_trading_days=30), "ready")

    def test_recent_theme_history_prefers_ticker_daily_history_when_coverage_is_sufficient(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'BBB')")

        dates = pd.bdate_range("2026-01-05", periods=70)
        for ticker, base in [("AAA", 100), ("BBB", 120)]:
            history = pd.DataFrame(
                [{"snapshot_date": ts.date(), "close": base + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
            )
            persist_ticker_daily_history(conn, history, ticker=ticker, provenance_source_label="daily_historical_append", market_data_source="live")

        history = theme_history_window(conn, 30)
        momentum = compute_theme_momentum(conn, 30)

        self.assertFalse(history.empty)
        self.assertIn("ticker_history_derived", set(history["provenance_class"].astype(str)))
        self.assertEqual(momentum["meta"]["provenance_mix"], "ticker_history_derived-only")
        conn.close()

    def test_recent_theme_history_falls_back_when_ticker_history_coverage_is_insufficient(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'BBB')")
        conn.execute(
            """
            insert into reconstructed_theme_snapshots(
                run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
            ) values
            (101, '2026-03-10', '2026-03-10 00:00:00', 1, 2, 1, 2, 3, 50, 60, 70, 2, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership'),
            (102, '2026-03-11', '2026-03-11 00:00:00', 1, 2, 2, 3, 4, 55, 65, 75, 3, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership')
            """
        )

        sparse_dates = pd.bdate_range("2026-03-01", periods=5)
        history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 1000 + idx} for idx, ts in enumerate(sparse_dates)]
        )
        persist_ticker_daily_history(conn, history, ticker="AAA", provenance_source_label="ticker_intake_backfill", market_data_source="live")

        out = theme_history_window(conn, 30)

        self.assertFalse(out.empty)
        self.assertNotIn("ticker_history_derived", set(out["provenance_class"].astype(str)))
        self.assertEqual(set(out["provenance_class"].astype(str)), {"reconstructed"})
        conn.close()

    def test_suppressed_tickers_do_not_block_recent_ticker_history_reconstruction(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Biotech', 'Health', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'CRSP')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'CRSPR')")
        conn.execute(
            """
            insert into symbol_refresh_status(
                ticker, status, consecutive_failure_count, rolling_failure_count, updated_at
            ) values ('CRSPR', 'refresh_suppressed', 5, 8, CURRENT_TIMESTAMP)
            """
        )

        dates = pd.bdate_range("2026-01-05", periods=70)
        history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 50 + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, history, ticker="CRSP", provenance_source_label="daily_historical_append", market_data_source="live")

        out = theme_history_window(conn, 30)

        self.assertFalse(out.empty)
        self.assertIn("ticker_history_derived", set(out["provenance_class"].astype(str)))
        conn.close()

    def test_suppressed_tickers_are_excluded_from_recent_ticker_history_aggregation(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Biotech', 'Health', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'GOOD')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'BBIG')")
        conn.execute(
            """
            insert into symbol_refresh_status(
                ticker, status, consecutive_failure_count, rolling_failure_count, updated_at
            ) values ('BBIG', 'refresh_suppressed', 1, 1, CURRENT_TIMESTAMP)
            """
        )

        dates = pd.bdate_range("2026-01-05", periods=70)
        good_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 100000 + idx} for idx, ts in enumerate(dates)]
        )
        bad_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 1 + idx, "volume": 500 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, good_history, ticker="GOOD", provenance_source_label="daily_historical_append", market_data_source="live")
        persist_ticker_daily_history(conn, bad_history, ticker="BBIG", provenance_source_label="daily_historical_append", market_data_source="live")

        out = theme_history_window(conn, 30)
        derived = out[out["provenance_class"] == "ticker_history_derived"].sort_values("snapshot_time")

        self.assertFalse(derived.empty)
        self.assertTrue((derived["ticker_count"] == 1).all())
        self.assertLess(float(derived["avg_1w"].abs().max()), 10.0)
        conn.close()

    def test_reconstructed_theme_history_excludes_suppressed_members_from_calculation_rows(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Biotech', 'Health', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'GOOD')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'BBIG')")
        conn.execute(
            """
            insert into symbol_refresh_status(
                ticker, status, consecutive_failure_count, rolling_failure_count, updated_at
            ) values ('BBIG', 'refresh_suppressed', 1, 1, CURRENT_TIMESTAMP)
            """
        )

        result = reconstruct_theme_history_range(
            conn,
            provider_name="mock",
            start_date="2026-02-10",
            end_date="2026-02-12",
            tickers=["GOOD", "BBIG"],
            provenance_source_label="historical_backfill",
            replace_existing=True,
        )
        stored = conn.execute(
            """
            select distinct ticker_count
            from reconstructed_theme_snapshots
            where provenance_source_label = 'historical_backfill'
            """
        ).fetchall()

        self.assertGreater(int(result["snapshot_rows_written"]), 0)
        self.assertEqual(stored, [(1,)])
        conn.close()

    def test_targeted_recent_rebuild_replaces_only_reconstructed_rows_in_scope(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Meme Stocks', 'Spec', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'GOOD')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'BBIG')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'NVDA')")
        conn.execute(
            """
            insert into symbol_refresh_status(
                ticker, status, consecutive_failure_count, rolling_failure_count, updated_at
            ) values ('BBIG', 'refresh_suppressed', 1, 1, CURRENT_TIMESTAMP)
            """
        )
        conn.execute(
            """
            insert into theme_snapshots(
                run_id, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, snapshot_source
            ) values
            (1, '2026-03-12 22:00:00', 1, 2, 5, 6, 7, 50, 60, 70, 8, 'live')
            """
        )
        conn.execute(
            """
            insert into reconstructed_theme_snapshots(
                run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
            ) values
            (100, '2026-03-10', '2026-03-10 00:00:00', 1, 2, 75, 90, 95, 100, 100, 100, 80, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership'),
            (101, '2026-03-10', '2026-03-10 00:00:00', 2, 1, 4, 5, 6, 70, 80, 90, 7, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership')
            """
        )

        dates = pd.bdate_range("2026-01-05", periods=70)
        good_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 100000 + idx} for idx, ts in enumerate(dates)]
        )
        bad_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 1 + idx, "volume": 500 + idx} for idx, ts in enumerate(dates)]
        )
        nvda_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 300 + idx, "volume": 200000 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, good_history, ticker="GOOD", provenance_source_label="daily_historical_append", market_data_source="live")
        persist_ticker_daily_history(conn, bad_history, ticker="BBIG", provenance_source_label="daily_historical_append", market_data_source="live")
        persist_ticker_daily_history(conn, nvda_history, ticker="NVDA", provenance_source_label="daily_historical_append", market_data_source="live")

        result = rebuild_recent_reconstructed_history(conn, tickers=["BBIG"])
        meme_row = conn.execute(
            """
            select ticker_count, avg_1w
            from reconstructed_theme_snapshots
            where theme_id = 1 and provenance_source_label = 'historical_backfill' and snapshot_date = '2026-03-10'
            """
        ).fetchone()
        ai_row = conn.execute(
            """
            select ticker_count, avg_1w
            from reconstructed_theme_snapshots
            where theme_id = 2 and provenance_source_label = 'historical_backfill' and snapshot_date = '2026-03-10'
            """
        ).fetchone()
        captured_row = conn.execute(
            """
            select ticker_count, avg_1w
            from theme_snapshots
            where theme_id = 1 and snapshot_time = '2026-03-12 22:00:00'
            """
        ).fetchone()

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["affected_theme_ids"], [1])
        self.assertEqual(int(result["rows_replaced"]), 1)
        self.assertGreater(int(result["rows_written"]), 0)
        self.assertEqual(int(meme_row[0]), 1)
        self.assertLess(float(meme_row[1]), 10.0)
        self.assertEqual(ai_row, (1, 4.0))
        self.assertEqual(captured_row, (2, 5.0))
        conn.close()

    def test_targeted_recent_rebuild_reintroduces_restored_ticker(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Meme Stocks', 'Spec', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'GOOD')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'BBIG')")
        conn.execute(
            """
            insert into symbol_refresh_status(
                ticker, status, consecutive_failure_count, rolling_failure_count, updated_at
            ) values ('BBIG', 'refresh_suppressed', 1, 1, CURRENT_TIMESTAMP)
            """
        )
        conn.execute(
            """
            insert into reconstructed_theme_snapshots(
                run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
            ) values
            (100, '2026-03-10', '2026-03-10 00:00:00', 1, 1, 4, 5, 6, 100, 100, 100, 5, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership')
            """
        )

        dates = pd.bdate_range("2026-01-05", periods=70)
        good_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 100000 + idx} for idx, ts in enumerate(dates)]
        )
        bad_history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 1 + idx, "volume": 500 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, good_history, ticker="GOOD", provenance_source_label="daily_historical_append", market_data_source="live")
        persist_ticker_daily_history(conn, bad_history, ticker="BBIG", provenance_source_label="daily_historical_append", market_data_source="live")

        first = rebuild_recent_reconstructed_history(conn, tickers=["BBIG"])
        first_row = conn.execute(
            """
            select ticker_count
            from reconstructed_theme_snapshots
            where theme_id = 1 and provenance_source_label = 'historical_backfill' and snapshot_date = '2026-03-10'
            """
        ).fetchone()
        conn.execute(
            """
            update symbol_refresh_status
            set status = 'active', suggested_status = null, suggested_reason = null, last_failure_category = null
            where ticker = 'BBIG'
            """
        )
        second = rebuild_recent_reconstructed_history(conn, tickers=["BBIG"])
        second_row = conn.execute(
            """
            select ticker_count
            from reconstructed_theme_snapshots
            where theme_id = 1 and provenance_source_label = 'historical_backfill' and snapshot_date = '2026-03-10'
            """
        ).fetchone()

        self.assertEqual(first["status"], "success")
        self.assertEqual(int(first_row[0]), 1)
        self.assertEqual(second["status"], "success")
        self.assertEqual(int(second_row[0]), 2)
        conn.close()

    def test_captured_history_still_wins_over_ticker_history_derived_on_same_date(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA')")
        conn.execute(
            """
            insert into theme_snapshots(
                run_id, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, snapshot_source
            ) values
            (1, '2026-03-11 22:00:00', 1, 1, 1, 2, 3, 40, 50, 60, 10, 'live')
            """
        )
        dates = pd.bdate_range("2026-01-05", periods=70)
        history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, history, ticker="AAA", provenance_source_label="daily_historical_append", market_data_source="live")

        out = theme_history_window(conn, 30)
        same_day = out[pd.to_datetime(out["snapshot_time"]).dt.date.astype(str) == "2026-03-11"]

        self.assertFalse(same_day.empty)
        self.assertEqual(str(same_day.iloc[0]["provenance_class"]), "captured")
        conn.close()

    def test_theme_snapshot_history_default_path_skips_recent_ticker_history_reconstruction(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA')")
        conn.execute(
            """
            insert into theme_snapshots(
                run_id, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, snapshot_source
            ) values
            (1, '2026-03-11 22:00:00', 1, 1, 1, 2, 3, 40, 50, 60, 10, 'live')
            """
        )
        dates = pd.bdate_range("2026-01-05", periods=70)
        history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, history, ticker="AAA", provenance_source_label="daily_historical_append", market_data_source="live")

        out = theme_snapshot_history(conn, 1, limit=50)

        self.assertFalse(out.empty)
        self.assertEqual(set(out["provenance_class"].astype(str)), {"captured"})
        conn.close()

    def test_theme_snapshot_history_can_opt_into_recent_ticker_history_reconstruction(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA')")

        dates = pd.bdate_range("2026-01-05", periods=70)
        history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, history, ticker="AAA", provenance_source_label="daily_historical_append", market_data_source="live")

        default_out = theme_snapshot_history(conn, 1, limit=50)
        opted_in = theme_snapshot_history(conn, 1, limit=50, include_recent_ticker_history=True)

        self.assertTrue(default_out.empty)
        self.assertFalse(opted_in.empty)
        self.assertIn("ticker_history_derived", set(opted_in["provenance_class"].astype(str)))
        conn.close()

    def test_recent_ticker_history_reconstruction_kill_switch_preserves_reconstructed_fallback(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA')")
        conn.execute(
            """
            insert into reconstructed_theme_snapshots(
                run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
            ) values
            (101, '2026-03-10', '2026-03-10 00:00:00', 1, 1, 1, 2, 3, 50, 60, 70, 2, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership'),
            (102, '2026-03-11', '2026-03-11 00:00:00', 1, 1, 2, 3, 4, 55, 65, 75, 3, 'reconstructed', 'historical_backfill', 'live', 'current_governed_membership')
            """
        )
        dates = pd.bdate_range("2026-01-05", periods=70)
        history = pd.DataFrame(
            [{"snapshot_date": ts.date(), "close": 100 + idx, "volume": 1000 + idx} for idx, ts in enumerate(dates)]
        )
        persist_ticker_daily_history(conn, history, ticker="AAA", provenance_source_label="daily_historical_append", market_data_source="live")

        with patch("src.queries.ENABLE_RECENT_TICKER_HISTORY_PREFERRED_RECONSTRUCTION", False):
            out = theme_history_window(conn, 30)

        self.assertFalse(out.empty)
        self.assertEqual(set(out["provenance_class"].astype(str)), {"reconstructed"})
        conn.close()


if __name__ == "__main__":
    unittest.main()
