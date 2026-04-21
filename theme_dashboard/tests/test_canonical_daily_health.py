import unittest

import duckdb
import pandas as pd

from src.database import SCHEMA_SQL
from src.queries import canonical_daily_health_status, canonical_daily_recent_coverage, canonical_daily_window_status


class TestCanonicalDailyHealth(unittest.TestCase):
    def _build_conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Alpha', 'Compute', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Beta', 'Compute', true)")
        return conn

    def _seed_current_snapshot_state(self, conn):
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

    def _seed_expected_trading_dates(self, conn):
        conn.execute(
            """
            insert into ticker_daily_history(
                ticker, trading_date, open, high, low, close, volume,
                market_data_source, provenance_source_label, created_at, updated_at
            ) values
            ('AAA', '2026-04-12', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp),
            ('AAA', '2026-04-13', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp),
            ('AAA', '2026-04-14', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp)
            """
        )

    def _insert_canonical_row(
        self,
        conn,
        *,
        snapshot_date: str,
        run_id: int,
        theme_id: int,
        theme: str,
        snapshot_source: str,
        extract_session: str,
        canonical_reason: str,
        standardized_score: float,
        canonical_rank: int | None,
    ):
        canonical_columns = [row[0] for row in conn.execute("describe canonical_theme_daily_snapshots").fetchall()]
        row = {column: None for column in canonical_columns}
        row.update(
            {
                "snapshot_date": snapshot_date,
                "snapshot_time": f"{snapshot_date} 22:05:00",
                "run_id": run_id,
                "theme_id": theme_id,
                "theme": theme,
                "category": "Compute",
                "is_active": True,
                "snapshot_source": snapshot_source,
                "extract_session": extract_session,
                "is_canonical_daily": True,
                "canonical_reason": canonical_reason,
                "ticker_count": 3,
                "eligible_ticker_count": 3,
                "eligible_1w_count": 3,
                "eligible_1m_count": 3,
                "eligible_3m_count": 3,
                "eligible_composite_count": 3,
                "eligible_standardized_count": 3,
                "eligible_momentum_count": 3,
                "eligible_breadth_pct": 100.0,
                "avg_1w": 10.0,
                "avg_1m": 10.0,
                "avg_3m": 10.0,
                "positive_1w_breadth_pct": 100.0,
                "positive_1m_breadth_pct": 100.0,
                "positive_3m_breadth_pct": 100.0,
                "legacy_composite_score": standardized_score,
                "standardized_base_strength_score": standardized_score,
                "standardized_participation_ratio": 1.0,
                "standardized_participation_factor": 1.0,
                "standardized_guardrail_factor": 1.0,
                "standardized_recovery_factor": 1.0,
                "standardized_composite_score": standardized_score,
                "current_momentum_raw_score": standardized_score,
                "current_momentum_quality_factor": 1.0,
                "current_momentum_score": standardized_score,
                "canonical_rank": canonical_rank,
                "created_at": f"{snapshot_date} 22:06:00",
                "updated_at": f"{snapshot_date} 22:06:00",
            }
        )
        payload = pd.DataFrame([[row[column] for column in canonical_columns]], columns=canonical_columns)
        conn.register("canonical_test_row", payload)
        try:
            conn.execute(
                f"""
                insert into canonical_theme_daily_snapshots({", ".join(canonical_columns)})
                select {", ".join(canonical_columns)}
                from canonical_test_row
                """
            )
        finally:
            conn.unregister("canonical_test_row")

    def test_canonical_daily_health_status_reports_latest_day_match(self):
        conn = self._build_conn()
        try:
            self._seed_current_snapshot_state(conn)
            self._seed_expected_trading_dates(conn)
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=15.0,
                canonical_rank=1,
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=2,
                theme="Beta",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=9.0,
                canonical_rank=2,
            )

            status = canonical_daily_health_status(conn, trading_day_limit=1, reconciliation_top_n=2).iloc[0]

            self.assertTrue(str(status["latest_expected_trading_date"]).startswith("2026-04-14"))
            self.assertTrue(str(status["latest_canonical_snapshot_date"]).startswith("2026-04-14"))
            self.assertTrue(bool(status["latest_expected_date_canonically_covered"]))
            self.assertEqual(int(status["top_n_mismatch_count"]), 0)
            self.assertTrue(bool(status["latest_day_leaders_match_current_standardized"]))
            self.assertEqual(str(status["reconciliation_status"]), "matched")
        finally:
            conn.close()

    def test_canonical_daily_recent_coverage_flags_missing_and_repair_dates(self):
        conn = self._build_conn()
        try:
            self._seed_expected_trading_dates(conn)
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-12",
                run_id=1,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=11.0,
                canonical_rank=1,
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=2,
                theme_id=1,
                theme="Alpha",
                snapshot_source="synthetic_backfill",
                extract_session="ticker_history_repair",
                canonical_reason="missing_full_theme_run_history_repair",
                standardized_score=12.0,
                canonical_rank=1,
            )

            coverage = canonical_daily_recent_coverage(conn, trading_day_limit=3)
            origins = {
                str(row["expected_trading_date"]): str(row["coverage_origin"])
                for _, row in coverage.iterrows()
            }

            self.assertEqual(origins[[key for key in origins if key.startswith("2026-04-14")][0]], "repair_fallback")
            self.assertEqual(origins[[key for key in origins if key.startswith("2026-04-13")][0]], "missing")
            self.assertEqual(origins[[key for key in origins if key.startswith("2026-04-12")][0]], "run_based")
        finally:
            conn.close()

    def test_canonical_daily_health_status_reports_leader_mismatch_for_latest_day(self):
        conn = self._build_conn()
        try:
            self._seed_current_snapshot_state(conn)
            self._seed_expected_trading_dates(conn)
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=2,
                theme="Beta",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=9.0,
                canonical_rank=1,
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=15.0,
                canonical_rank=2,
            )

            status = canonical_daily_health_status(conn, trading_day_limit=1, reconciliation_top_n=2).iloc[0]

            self.assertEqual(int(status["top_n_mismatch_count"]), 2)
            self.assertFalse(bool(status["latest_day_leaders_match_current_standardized"]))
            self.assertEqual(str(status["reconciliation_status"]), "mismatch")
        finally:
            conn.close()

    def test_canonical_daily_window_status_distinguishes_raw_vs_ranked_latest_dates(self):
        conn = self._build_conn()
        try:
            self._seed_expected_trading_dates(conn)
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=7,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=10.0,
                canonical_rank=1,
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-15",
                run_id=8,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=11.0,
                canonical_rank=None,
            )

            status = canonical_daily_window_status(conn).iloc[0]

            self.assertTrue(str(status["latest_expected_trading_date"]).startswith("2026-04-14"))
            self.assertTrue(str(status["latest_raw_canonical_date"]).startswith("2026-04-15"))
            self.assertTrue(str(status["latest_ranked_canonical_date"]).startswith("2026-04-14"))
            self.assertTrue(bool(status["raw_vs_ranked_date_differs"]))
        finally:
            conn.close()

    def test_canonical_daily_window_status_uses_latest_trading_date_not_wall_clock_day(self):
        conn = self._build_conn()
        try:
            conn.execute(
                """
                insert into ticker_daily_history(
                    ticker, trading_date, open, high, low, close, volume,
                    market_data_source, provenance_source_label, created_at, updated_at
                ) values
                ('AAA', '2026-04-16', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp),
                ('AAA', '2026-04-17', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp)
                """
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-17",
                run_id=9,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=11.0,
                canonical_rank=1,
            )

            status = canonical_daily_window_status(conn).iloc[0]

            self.assertTrue(str(status["latest_expected_trading_date"]).startswith("2026-04-17"))
            self.assertTrue(str(status["latest_ranked_canonical_date"]).startswith("2026-04-17"))
            self.assertFalse(bool(status["raw_vs_ranked_date_differs"]))
        finally:
            conn.close()

    def test_canonical_daily_health_status_matches_when_reusing_precomputed_coverage(self):
        conn = self._build_conn()
        try:
            self._seed_current_snapshot_state(conn)
            self._seed_expected_trading_dates(conn)
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=15.0,
                canonical_rank=1,
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=2,
                theme="Beta",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=9.0,
                canonical_rank=2,
            )

            coverage = canonical_daily_recent_coverage(conn, trading_day_limit=3)
            direct = canonical_daily_health_status(conn, trading_day_limit=3, reconciliation_top_n=2)
            reused = canonical_daily_health_status(conn, trading_day_limit=3, reconciliation_top_n=2, coverage=coverage)

            self.assertTrue(direct.equals(reused))
        finally:
            conn.close()

    def test_canonical_daily_health_uses_canonical_run_basis_over_newer_live_hydration(self):
        conn = self._build_conn()
        try:
            self._seed_expected_trading_dates(conn)
            conn.execute("delete from theme_membership")
            conn.execute("delete from refresh_runs")
            conn.execute("delete from ticker_snapshots")
            conn.execute(
                """
                insert into themes(id, name, category, is_active)
                values (3, 'Gamma', 'Compute', true)
                on conflict do nothing
                """
            )
            conn.execute(
                """
                insert into theme_membership(theme_id, ticker) values
                (1, 'AAA'), (1, 'AAB'), (1, 'AAC'),
                (2, 'BBB'), (2, 'BBC'), (2, 'BBD'),
                (3, 'CCC'), (3, 'CCD'), (3, 'CCE')
                """
            )
            conn.execute(
                """
                insert into refresh_runs(run_id, provider, started_at, finished_at, status, ticker_count, success_count, failure_count, scope_type)
                values
                (8, 'live', '2026-04-14 20:00:00', '2026-04-14 22:05:00', 'success', 9, 9, 0, 'scheduled_eod'),
                (9, 'live', '2026-04-14 22:06:00', '2026-04-14 22:07:00', 'success', 1, 1, 0, 'governed_ticker_current_hydration')
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
                (8, 'BBD', 10, 6, 8, 12, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'CCC', 10, 9, 11, 10, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'CCD', 10, 9, 11, 10, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (8, 'CCE', 10, 9, 11, 10, null, 2000000, null, null, null, '2026-04-14 22:00:00', 'live'),
                (9, 'BBB', 10, 30, 30, 12, null, 2000000, null, null, null, '2026-04-14 22:06:30', 'live')
                """
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=1,
                theme="Alpha",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=15.0,
                canonical_rank=1,
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=3,
                theme="Gamma",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=10.6,
                canonical_rank=2,
            )
            self._insert_canonical_row(
                conn,
                snapshot_date="2026-04-14",
                run_id=8,
                theme_id=2,
                theme="Beta",
                snapshot_source="live",
                extract_session="after_hours_official",
                canonical_reason="official_daily_refresh",
                standardized_score=9.0,
                canonical_rank=3,
            )

            status = canonical_daily_health_status(conn, trading_day_limit=1, reconciliation_top_n=3).iloc[0]

            self.assertEqual(int(status["top_n_mismatch_count"]), 0)
            self.assertTrue(bool(status["latest_day_leaders_match_current_standardized"]))
            self.assertEqual(str(status["reconciliation_status"]), "matched")
        finally:
            conn.close()
