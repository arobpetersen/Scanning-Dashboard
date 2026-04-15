import unittest

import duckdb

from src.database import SCHEMA_SQL
from src.rankings import (
    backfill_canonical_theme_daily_snapshots_for_recent_trading_days,
    canonical_backfill_candidate_runs_by_date,
)


class TestCanonicalBackfill(unittest.TestCase):
    def _build_conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Alpha', 'Compute', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Beta', 'Compute', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAA'), (1, 'AAB'), (1, 'AAC')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'BBB'), (2, 'BBC'), (2, 'BBD')")
        return conn

    def _insert_history(self, conn):
        conn.execute(
            """
            insert into ticker_daily_history(
                run_id, ticker, trading_date, open, high, low, close, atr_14, atr_pct_14, volume, vwap, trade_count,
                provenance_class, provenance_source_label, market_data_source, created_at, updated_at
            ) values
            (1, 'AAA', '2026-04-10', 10, 10, 10, 10, 1, 0.1, 1000, 10, 1, 'reconstructed', 'test', 'live', '2026-04-10 22:00:00', '2026-04-10 22:00:00'),
            (1, 'BBB', '2026-04-10', 10, 10, 10, 10, 1, 0.1, 1000, 10, 1, 'reconstructed', 'test', 'live', '2026-04-10 22:00:00', '2026-04-10 22:00:00'),
            (1, 'AAA', '2026-04-11', 11, 11, 11, 11, 1, 0.1, 1000, 11, 1, 'reconstructed', 'test', 'live', '2026-04-11 22:00:00', '2026-04-11 22:00:00'),
            (1, 'BBB', '2026-04-11', 11, 11, 11, 11, 1, 0.1, 1000, 11, 1, 'reconstructed', 'test', 'live', '2026-04-11 22:00:00', '2026-04-11 22:00:00')
            """
        )

    def _insert_run_payload(self, conn, run_id: int, finished_at: str, scope_type: str, aaa: tuple[float, float, float], bbb: tuple[float, float, float]):
        conn.execute(
            """
            insert into refresh_runs(run_id, provider, started_at, finished_at, status, ticker_count, success_count, failure_count, scope_type)
            values (?, 'live', ?, ?, 'success', 6, 6, 0, ?)
            """,
            [run_id, finished_at, finished_at, scope_type],
        )
        conn.execute(
            """
            insert into ticker_snapshots(
                run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated, snapshot_source
            ) values
            (?, 'AAA', 10, ?, ?, ?, null, 2000000, null, null, null, ?, 'live'),
            (?, 'AAB', 10, ?, ?, ?, null, 2000000, null, null, null, ?, 'live'),
            (?, 'AAC', 10, ?, ?, ?, null, 2000000, null, null, null, ?, 'live'),
            (?, 'BBB', 10, ?, ?, ?, null, 2000000, null, null, null, ?, 'live'),
            (?, 'BBC', 10, ?, ?, ?, null, 2000000, null, null, null, ?, 'live'),
            (?, 'BBD', 10, ?, ?, ?, null, 2000000, null, null, null, ?, 'live')
            """,
            [
                run_id, aaa[0], aaa[1], aaa[2], finished_at,
                run_id, aaa[0], aaa[1], aaa[2], finished_at,
                run_id, aaa[0], aaa[1], aaa[2], finished_at,
                run_id, bbb[0], bbb[1], bbb[2], finished_at,
                run_id, bbb[0], bbb[1], bbb[2], finished_at,
                run_id, bbb[0], bbb[1], bbb[2], finished_at,
            ],
        )

    def test_canonical_backfill_candidate_runs_by_date_prefers_scheduled_eod_then_latest(self):
        conn = self._build_conn()
        try:
            self._insert_history(conn)
            self._insert_run_payload(conn, 10, "2026-04-10 18:00:00", "active_themes", (12, 18, -5), (6, 8, 10))
            self._insert_run_payload(conn, 11, "2026-04-10 20:00:00", "scheduled_eod", (12, 18, -5), (6, 8, 10))
            self._insert_run_payload(conn, 12, "2026-04-11 19:00:00", "active_themes", (13, 19, -5), (7, 9, 10))
            out = canonical_backfill_candidate_runs_by_date(conn, recent_trading_day_limit=2)
            self.assertEqual(out["run_id"].tolist(), [11, 12])
            self.assertEqual(out["selection_reason"].tolist(), ["scheduled_eod_preferred", "active_themes_fallback"])
        finally:
            conn.close()

    def test_backfill_canonical_theme_daily_snapshots_for_recent_trading_days_is_idempotent(self):
        conn = self._build_conn()
        try:
            self._insert_history(conn)
            self._insert_run_payload(conn, 10, "2026-04-10 20:00:00", "active_themes", (12, 18, -5), (6, 8, 10))
            self._insert_run_payload(conn, 12, "2026-04-11 19:00:00", "active_themes", (13, 19, -5), (7, 9, 10))
            first = backfill_canonical_theme_daily_snapshots_for_recent_trading_days(conn, recent_trading_day_limit=2)
            second = backfill_canonical_theme_daily_snapshots_for_recent_trading_days(conn, recent_trading_day_limit=2)
            self.assertEqual(first["selected_run_dates"], 2)
            self.assertEqual(first["after"]["date_count"], 2)
            self.assertEqual(second["after"]["date_count"], 2)
            self.assertEqual(
                conn.execute("select count(*) from canonical_theme_daily_snapshots").fetchone()[0],
                conn.execute("select count(distinct snapshot_date || ':' || theme_id::varchar) from canonical_theme_daily_snapshots").fetchone()[0],
            )
        finally:
            conn.close()

    def test_backfill_reports_missing_dates_without_candidates(self):
        conn = self._build_conn()
        try:
            self._insert_history(conn)
            self._insert_run_payload(conn, 10, "2026-04-10 20:00:00", "active_themes", (12, 18, -5), (6, 8, 10))
            out = backfill_canonical_theme_daily_snapshots_for_recent_trading_days(conn, recent_trading_day_limit=2)
            self.assertEqual(out["selected_run_dates"], 2)
            self.assertEqual(len(out["missing_dates"]), 0)
            repaired = [row for row in out["results"] if row["selection_reason"] == "ticker_history_repair_fallback"]
            self.assertEqual(len(repaired), 1)
            self.assertEqual(repaired[0]["snapshot_date"], "2026-04-11")
            self.assertGreater(repaired[0]["inserted_count"], 0)
            self.assertGreater(
                conn.execute(
                    "select max(ticker_count) from canonical_theme_daily_snapshots where snapshot_date = '2026-04-11'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "select count(*) from refresh_runs where provider = 'synthetic_backfill' and scope_type = 'canonical_history_repair'"
                ).fetchone()[0],
                1,
            )
        finally:
            conn.close()

    def test_history_repair_backfill_is_idempotent_on_rerun(self):
        conn = self._build_conn()
        try:
            self._insert_history(conn)
            self._insert_run_payload(conn, 10, "2026-04-10 20:00:00", "active_themes", (12, 18, -5), (6, 8, 10))
            first = backfill_canonical_theme_daily_snapshots_for_recent_trading_days(conn, recent_trading_day_limit=2)
            second = backfill_canonical_theme_daily_snapshots_for_recent_trading_days(conn, recent_trading_day_limit=2)
            self.assertEqual(first["after"]["date_count"], 2)
            self.assertEqual(second["after"]["date_count"], 2)
            self.assertEqual(
                conn.execute("select count(*) from canonical_theme_daily_snapshots where snapshot_date = '2026-04-11'").fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "select count(*) from refresh_runs where provider = 'synthetic_backfill' and scope_type = 'canonical_history_repair'"
                ).fetchone()[0],
                1,
            )
        finally:
            conn.close()
