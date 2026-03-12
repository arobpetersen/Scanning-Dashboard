import unittest

import duckdb
import pandas as pd

from src.database import SCHEMA_SQL
from src.historical_backfill import reconstruct_theme_history_range
from src.momentum_engine import compute_theme_momentum
from src.queries import theme_history_window
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
        self.assertEqual(float(rankings.iloc[0]["composite_score"]), 10.0)
        conn.close()


if __name__ == "__main__":
    unittest.main()
