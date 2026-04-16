import unittest

import duckdb

from src.database import SCHEMA_SQL
from src.queries import canonical_theme_rank_history, latest_canonical_theme_daily_snapshots
from src.rankings import (
    compute_current_ranking_snapshot,
    persist_canonical_theme_daily_snapshot_for_run,
)


class TestCanonicalThemeDailySnapshots(unittest.TestCase):
    def _build_conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Alpha', 'Compute', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Beta', 'Compute', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (3, 'Dormant', 'Legacy', false)")
        return conn

    def test_persist_canonical_theme_daily_snapshot_for_run_is_idempotent_and_ranked(self):
        conn = self._build_conn()
        try:
            conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA'), (1, 'AAB'), (1, 'AAC')")
            conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'BBB'), (2, 'BBC'), (2, 'BBD')")
            conn.execute(
                """
                insert into refresh_runs(run_id, provider, started_at, finished_at, status, ticker_count, success_count, failure_count)
                values (7, 'live', '2026-04-14 20:00:00', '2026-04-14 22:05:00', 'success', 6, 6, 0)
                """
            )
            conn.execute(
                """
                insert into ticker_snapshots(
                    run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                    market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated, snapshot_source
                ) values
                (7, 'AAA', 10, 14, 20, -8, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (7, 'AAB', 11, 13, 19, -8, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (7, 'AAC', 12, 12, 18, -8, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (7, 'BBB', 10, 8, 10, 12, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (7, 'BBC', 10, 7, 9, 12, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (7, 'BBD', 10, 6, 8, 12, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live')
                """
            )

            result_first = persist_canonical_theme_daily_snapshot_for_run(conn, 7)
            result_second = persist_canonical_theme_daily_snapshot_for_run(conn, 7)

            self.assertEqual(result_first["inserted_count"], 3)
            self.assertEqual(result_second["inserted_count"], 0)
            row_count = conn.execute("select count(*) from canonical_theme_daily_snapshots").fetchone()[0]
            self.assertEqual(row_count, 3)

            latest = latest_canonical_theme_daily_snapshots(conn)
            self.assertEqual(latest["theme"].tolist()[:2], ["Alpha", "Beta"])
            self.assertEqual(int(latest.iloc[0]["canonical_rank"]), 1)
            self.assertEqual(int(latest.iloc[1]["canonical_rank"]), 2)
            self.assertTrue(latest["canonical_rank"].isna().iloc[2])
        finally:
            conn.close()

    def test_latest_canonical_rows_match_current_standardized_order_for_same_run_date(self):
        conn = self._build_conn()
        try:
            conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA'), (1, 'AAB'), (1, 'AAC')")
            conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'BBB'), (2, 'BBC'), (2, 'BBD')")
            conn.execute(
                """
                insert into refresh_runs(run_id, provider, started_at, finished_at, status, ticker_count, success_count, failure_count)
                values (8, 'live', '2026-04-14 20:00:00', '2026-04-14 22:05:00', 'success', 6, 6, 0)
                """
            )
            conn.execute(
                """
                insert into ticker_snapshots(
                    run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                    market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated, snapshot_source
                ) values
                (8, 'AAA', 10, 14, 20, -8, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'AAB', 11, 13, 19, -8, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'AAC', 12, 12, 18, -8, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'BBB', 10, 8, 10, 12, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'BBC', 10, 7, 9, 12, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'BBD', 10, 6, 8, 12, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live')
                """
            )

            snapshot = compute_current_ranking_snapshot(conn)
            persist_canonical_theme_daily_snapshot_for_run(conn, 8)
            latest = latest_canonical_theme_daily_snapshots(conn)

            expected = snapshot["standardized_rankings"]["theme"].tolist()
            observed = latest[latest["canonical_rank"].notna()].sort_values("canonical_rank")["theme"].tolist()
            self.assertEqual(observed, expected)

            history = canonical_theme_rank_history(conn, 1, days=5)
            self.assertEqual(history["theme"].tolist(), ["Alpha"])
            self.assertEqual(int(history.iloc[0]["canonical_rank"]), 1)
        finally:
            conn.close()

    def test_persist_canonical_theme_daily_snapshot_for_run_skips_unrankable_run_rows(self):
        conn = self._build_conn()
        try:
            conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA'), (2, 'BBB')")
            conn.execute(
                """
                insert into refresh_runs(run_id, provider, started_at, finished_at, status, ticker_count, success_count, failure_count)
                values (9, 'live', '2026-04-15 20:00:00', '2026-04-15 22:05:00', 'partial', 2, 0, 2)
                """
            )

            result = persist_canonical_theme_daily_snapshot_for_run(conn, 9)

            self.assertEqual(result["status"], "no_rankable_rows_for_run")
            self.assertEqual(int(result["inserted_count"]), 0)
            row_count = conn.execute("select count(*) from canonical_theme_daily_snapshots").fetchone()[0]
            self.assertEqual(row_count, 0)
        finally:
            conn.close()
