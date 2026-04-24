import unittest

import duckdb

from src.database import SCHEMA_SQL
from src.momentum_engine import compute_theme_momentum
from src.queries import (
    canonical_theme_history_window,
    canonical_theme_snapshot_counts,
    canonical_theme_leadership_rank_history,
    canonical_theme_leadership_rank_history_long,
)


class TestCanonicalRankHistory(unittest.TestCase):
    def _build_conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        return conn

    def test_canonical_theme_leadership_rank_history_long_returns_one_row_per_theme_date(self):
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
                ('2026-04-12', '2026-04-12 22:00:00', 1, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 10, 11, 12, 100, 100, 100, 9, 10, 1, 1, 1, 1, 10, 10, 1, 10, 1),
                ('2026-04-12', '2026-04-12 22:00:00', 1, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 9, 10, 100, 100, 100, 7, 8, 1, 1, 1, 1, 8, 8, 1, 8, 2),
                ('2026-04-13', '2026-04-13 22:00:00', 2, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 11, 12, 13, 100, 100, 100, 10, 11, 1, 1, 1, 1, 11, 11, 1, 11, 2),
                ('2026-04-13', '2026-04-13 22:00:00', 2, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 12, 13, 14, 100, 100, 100, 11, 12, 1, 1, 1, 1, 12, 12, 1, 12, 1)
                """
            )
            out = canonical_theme_leadership_rank_history_long(conn, [101, 102], lookback_points=5)
            self.assertEqual(len(out), 4)
            self.assertEqual(out[["theme_id", "snapshot_date"]].drop_duplicates().shape[0], 4)
            alpha_latest = out[out["theme_id"] == 101].sort_values("snapshot_date").iloc[-1]
            beta_latest = out[out["theme_id"] == 102].sort_values("snapshot_date").iloc[-1]
            self.assertEqual(int(alpha_latest["rank"]), 2)
            self.assertEqual(int(beta_latest["rank"]), 1)
        finally:
            conn.close()

    def test_canonical_theme_leadership_rank_history_long_uses_latest_canonical_dates(self):
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
                ('2026-04-10', '2026-04-10 22:00:00', 1, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 7, 7, 100, 100, 100, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 3),
                ('2026-04-11', '2026-04-11 22:00:00', 2, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 100, 100, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 2),
                ('2026-04-12', '2026-04-12 22:00:00', 3, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 9, 9, 9, 100, 100, 100, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 1)
                """
            )
            out = canonical_theme_leadership_rank_history_long(conn, [101], lookback_points=2)
            self.assertEqual(out["snapshot_date"].astype(str).tolist(), ["2026-04-11", "2026-04-12"])
        finally:
            conn.close()

    def test_canonical_theme_leadership_rank_history_builds_rank_series_from_canonical_ranks(self):
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
                ('2026-04-10', '2026-04-10 22:00:00', 1, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 7, 7, 100, 100, 100, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 3),
                ('2026-04-11', '2026-04-11 22:00:00', 2, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 100, 100, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 2),
                ('2026-04-12', '2026-04-12 22:00:00', 3, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 9, 9, 9, 100, 100, 100, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 1),
                ('2026-04-10', '2026-04-10 22:00:00', 1, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 10, 10, 10, 100, 100, 100, 10, 10, 1, 1, 1, 1, 10, 10, 1, 10, 1),
                ('2026-04-11', '2026-04-11 22:00:00', 2, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 9, 9, 9, 100, 100, 100, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 3),
                ('2026-04-12', '2026-04-12 22:00:00', 3, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 100, 100, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 4)
                """
            )
            out = canonical_theme_leadership_rank_history(conn, [101, 102], lookback_points=3).sort_values("theme_id")
            self.assertEqual(out["rank_history_points"].tolist(), [3, 3])
            self.assertEqual(out.iloc[0]["rank_history"], [3.0, 2.0, 1.0])
            self.assertEqual(out.iloc[1]["rank_history"], [1.0, 3.0, 4.0])
        finally:
            conn.close()

    def test_compute_theme_momentum_prefers_canonical_daily_rank_change(self):
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
                ('2026-04-10', '2026-04-10 22:00:00', 1, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 7, 7, 100, 100, 100, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 3),
                ('2026-04-10', '2026-04-10 22:00:00', 1, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 10, 10, 10, 100, 100, 100, 10, 10, 1, 1, 1, 1, 10, 10, 1, 10, 1),
                ('2026-04-12', '2026-04-12 22:00:00', 2, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 9, 9, 9, 100, 100, 100, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 1),
                ('2026-04-12', '2026-04-12 22:00:00', 2, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 100, 100, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 4)
                """
            )
            out = compute_theme_momentum(conn, 7, top_n=1)
            history = out["history"].sort_values(["snapshot_time", "rank", "theme"]).reset_index(drop=True)
            self.assertEqual(history["provenance_class"].drop_duplicates().tolist(), ["canonical_daily"])
            latest = out["window_summary"].set_index("theme")
            self.assertEqual(float(latest.loc["Alpha", "rank_change"]), 2.0)
            self.assertEqual(float(latest.loc["Beta", "rank_change"]), -3.0)
            self.assertEqual(out["new_leaders"], ["Alpha"])
            self.assertEqual(out["dropped_leaders"], ["Beta"])
        finally:
            conn.close()

    def test_compute_theme_momentum_falls_back_when_canonical_history_is_unavailable(self):
        conn = self._build_conn()
        try:
            conn.execute("create table themes_legacy(id bigint, name varchar)")
        except Exception:
            pass
            # no-op: schema already provides the needed core tables, and this test
            # only needs enough legacy history rows for the fallback path
            # to return a non-empty result.
        conn.execute("insert into themes(id, name, category, is_active) values (201, 'Fallback', 'Legacy', true)")
        conn.execute(
            """
            insert into theme_snapshots(
                run_id, snapshot_time, theme_id, ticker_count,
                avg_1w, avg_1m, avg_3m,
                positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                composite_score, snapshot_source
            ) values
            (1, '2026-04-01 22:00:00', 201, 4, 1, 2, 3, 50, 50, 50, 5, 'live'),
            (2, '2026-04-08 22:00:00', 201, 4, 2, 3, 4, 60, 60, 60, 8, 'live')
            """
        )
        try:
            out = compute_theme_momentum(conn, 30, top_n=5)
            self.assertFalse(out["history"].empty)
            self.assertNotIn("canonical_daily", out["history"]["provenance_class"].dropna().astype(str).tolist())
        finally:
            conn.close()

    def test_compute_theme_momentum_adds_persistent_behavior_flags(self):
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
                ('2026-04-10', '2026-04-10 22:00:00', 1, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 7, 7, 100, 40, 40, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 4),
                ('2026-04-10', '2026-04-10 22:00:00', 1, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 6, 6, 6, 100, 40, 40, 6, 6, 1, 1, 1, 1, 6, 6, 1, 6, 3),
                ('2026-04-10', '2026-04-10 22:00:00', 1, 103, 'Gamma', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 9, 9, 9, 100, 60, 60, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 2),
                ('2026-04-11', '2026-04-11 22:00:00', 2, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 50, 50, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 3),
                ('2026-04-11', '2026-04-11 22:00:00', 2, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 7, 7, 100, 42, 42, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 2),
                ('2026-04-11', '2026-04-11 22:00:00', 2, 103, 'Gamma', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 55, 55, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 4),
                ('2026-04-12', '2026-04-12 22:00:00', 3, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 9, 9, 9, 100, 60, 60, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 2),
                ('2026-04-12', '2026-04-12 22:00:00', 3, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 43, 43, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 1),
                ('2026-04-12', '2026-04-12 22:00:00', 3, 103, 'Gamma', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 7, 7, 100, 50, 50, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 5),
                ('2026-04-13', '2026-04-13 22:00:00', 4, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 10, 10, 10, 100, 70, 70, 10, 10, 1, 1, 1, 1, 10, 10, 1, 10, 1),
                ('2026-04-13', '2026-04-13 22:00:00', 4, 102, 'Beta', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 9, 9, 9, 100, 44, 44, 9, 9, 1, 1, 1, 1, 9, 9, 1, 9, 1),
                ('2026-04-13', '2026-04-13 22:00:00', 4, 103, 'Gamma', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 6, 6, 6, 100, 45, 45, 6, 6, 1, 1, 1, 1, 6, 6, 1, 6, 6)
                """
            )
            out = compute_theme_momentum(conn, 7, top_n=3)
            summary = out["window_summary"].set_index("theme")
            self.assertEqual(summary.loc["Alpha", "persistent_behavior"], "Broadening persistence")
            self.assertEqual(summary.loc["Beta", "persistent_behavior"], "Narrow persistent move")
            self.assertEqual(summary.loc["Gamma", "persistent_behavior"], "Persistent fade")
            self.assertIn("straight sessions", summary.loc["Alpha", "persistent_behavior_reason"])
            self.assertAlmostEqual(float(summary.loc["Alpha", "persistent_breadth_total"]), 30.0, places=2)
            self.assertAlmostEqual(float(summary.loc["Beta", "persistent_breadth_total"]), 4.0, places=2)
            self.assertAlmostEqual(float(summary.loc["Gamma", "persistent_rank_total"]), -4.0, places=2)
        finally:
            conn.close()

    def test_canonical_theme_history_window_carries_avg_6m_and_is_active(self):
        conn = self._build_conn()
        try:
            conn.execute(
                """
                insert into canonical_theme_daily_snapshots(
                    snapshot_date, snapshot_time, run_id, theme_id, theme, category, is_active,
                    snapshot_source, extract_session, is_canonical_daily, canonical_reason,
                    ticker_count, eligible_ticker_count, eligible_1w_count, eligible_1m_count,
                    eligible_3m_count, eligible_composite_count, eligible_standardized_count,
                    eligible_momentum_count, eligible_breadth_pct, avg_1w, avg_1m, avg_3m, avg_6m,
                    positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                    legacy_composite_score, standardized_base_strength_score, standardized_participation_ratio,
                    standardized_participation_factor, standardized_guardrail_factor,
                    standardized_recovery_factor, standardized_composite_score,
                    current_momentum_raw_score, current_momentum_quality_factor,
                    current_momentum_score, canonical_rank
                ) values
                ('2026-04-10', '2026-04-10 22:00:00', 1, 101, 'Alpha', 'Compute', false, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 8, 9, 10, 100, 100, 100, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 1)
                """
            )
            out = canonical_theme_history_window(conn, 7)
            self.assertEqual(float(out.iloc[0]["avg_6m"]), 10.0)
            self.assertFalse(bool(out.iloc[0]["is_active"]))
        finally:
            conn.close()

    def test_canonical_theme_snapshot_counts_reports_canonical_daily_inventory(self):
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
                ('2026-04-10', '2026-04-10 22:00:00', 1, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 7, 7, 7, 100, 100, 100, 7, 7, 1, 1, 1, 1, 7, 7, 1, 7, 1),
                ('2026-04-11', '2026-04-11 22:00:00', 2, 101, 'Alpha', 'Compute', true, 'live', 'after_hours_official', true, 'scheduled_eod_refresh', 4, 4, 4, 4, 4, 4, 4, 4, 100, 8, 8, 8, 100, 100, 100, 8, 8, 1, 1, 1, 1, 8, 8, 1, 8, 1)
                """
            )
            counts = canonical_theme_snapshot_counts(conn)
            self.assertEqual(int(counts.iloc[0]["canonical_snapshot_dates"]), 2)
            self.assertEqual(int(counts.iloc[0]["canonical_snapshot_rows"]), 2)
            self.assertTrue(str(counts.iloc[0]["latest_canonical_snapshot_date"]).startswith("2026-04-11"))
        finally:
            conn.close()
