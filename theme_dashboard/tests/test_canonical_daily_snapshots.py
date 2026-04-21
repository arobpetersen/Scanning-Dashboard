import unittest

import duckdb
import pandas as pd
from unittest.mock import patch

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

    def test_persist_canonical_theme_daily_snapshot_for_run_overwrite_deduplicates_same_day_rows(self):
        conn = self._build_conn()
        try:
            conn.execute(
                """
                insert into canonical_theme_daily_snapshots(
                    snapshot_date, snapshot_time, run_id, theme_id, theme, category, is_active,
                    snapshot_source, extract_session, is_canonical_daily, canonical_reason,
                    ticker_count, eligible_ticker_count, eligible_1w_count, eligible_1m_count,
                    eligible_3m_count, eligible_composite_count, eligible_standardized_count,
                    eligible_momentum_count, eligible_breadth_pct, avg_1w, avg_1m, avg_3m,
                    positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                    legacy_composite_score, standardized_base_strength_score, standardized_participation_ratio,
                    standardized_participation_factor, standardized_guardrail_factor,
                    standardized_recovery_factor, standardized_composite_score,
                    current_momentum_raw_score, current_momentum_quality_factor,
                    current_momentum_score, canonical_rank
                ) values
                ('2026-04-21', '2026-04-21 16:00:00', 40, 1, 'Alpha', 'Compute', true,
                 'live', 'after_hours_official', true, 'official_daily_refresh',
                 3, 3, 3, 3, 3, 3, 3, 3, 100, 9, 9, 9,
                 100, 100, 100, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 1)
                """
            )
            duplicate_rows = pd.DataFrame(
                [
                    {
                        "snapshot_date": pd.Timestamp("2026-04-21").date(),
                        "snapshot_time": pd.Timestamp("2026-04-21 16:00:00"),
                        "run_id": 41,
                        "theme_id": 1,
                        "theme": "Alpha",
                        "category": "Compute",
                        "is_active": True,
                        "snapshot_source": "live",
                        "extract_session": "after_hours_official",
                        "is_canonical_daily": True,
                        "canonical_reason": "official_daily_refresh",
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
                        "legacy_composite_score": 10.0,
                        "standardized_base_strength_score": 10.0,
                        "standardized_participation_ratio": 1.0,
                        "standardized_participation_factor": 1.0,
                        "standardized_guardrail_factor": 1.0,
                        "standardized_recovery_factor": 1.0,
                        "standardized_composite_score": 10.0,
                        "current_momentum_raw_score": 10.0,
                        "current_momentum_quality_factor": 1.0,
                        "current_momentum_score": 10.0,
                        "canonical_rank": 1,
                    },
                    {
                        "snapshot_date": pd.Timestamp("2026-04-21").date(),
                        "snapshot_time": pd.Timestamp("2026-04-21 16:00:00"),
                        "run_id": 41,
                        "theme_id": 1,
                        "theme": "Alpha",
                        "category": "Compute",
                        "is_active": True,
                        "snapshot_source": "live",
                        "extract_session": "after_hours_official",
                        "is_canonical_daily": True,
                        "canonical_reason": "official_daily_refresh",
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
                        "legacy_composite_score": 10.0,
                        "standardized_base_strength_score": 10.0,
                        "standardized_participation_ratio": 1.0,
                        "standardized_participation_factor": 1.0,
                        "standardized_guardrail_factor": 1.0,
                        "standardized_recovery_factor": 1.0,
                        "standardized_composite_score": 10.0,
                        "current_momentum_raw_score": 10.0,
                        "current_momentum_quality_factor": 1.0,
                        "current_momentum_score": 10.0,
                        "canonical_rank": 1,
                    },
                ]
            )

            with patch("src.rankings.build_canonical_theme_daily_rows_for_run", return_value=duplicate_rows):
                result = persist_canonical_theme_daily_snapshot_for_run(conn, 41, overwrite_existing=True)

            self.assertEqual(result["status"], "materialized_from_run")
            self.assertEqual(int(result["inserted_count"]), 1)
            self.assertEqual(
                conn.execute(
                    "select count(*) from canonical_theme_daily_snapshots where snapshot_date = '2026-04-21' and theme_id = 1"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute("select max(run_id) from canonical_theme_daily_snapshots where snapshot_date = '2026-04-21'").fetchone()[0],
                41,
            )
        finally:
            conn.close()
