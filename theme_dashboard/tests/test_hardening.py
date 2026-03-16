import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from src.fetch_data import run_refresh
from src.failure_classification import categorize_failure_message
from src.inflection_engine import compute_theme_inflections
from src.leaderboard_utils import (
    build_category_leaderboard,
    build_category_theme_breakdown,
    build_current_leadership_table,
    build_current_performance_table,
    build_window_leaderboard,
)
from src.metric_formatting import format_theme_ticker_table, human_readable_number, short_timestamp
from src.momentum_engine import compute_theme_momentum
from src.queries import (
    latest_ticker_snapshots,
    ticker_lookup_memberships,
    ticker_lookup_summary,
    theme_health_overview,
    theme_history_last_n_snapshots,
    theme_history_window,
    theme_member_hygiene_context,
    theme_ticker_metrics,
    ticker_history_last_n_snapshots,
    top_theme_movers,
)
from src.symbol_hygiene import (
    OVERRIDE_ACTIONS,
    STAGED_ACTIONS,
    apply_refresh_failure,
    apply_refresh_success,
    apply_staged_symbol_hygiene_actions,
    clear_symbol_hygiene_staged_state,
    filter_symbol_hygiene_queue,
    resolve_staged_symbol_hygiene_action,
    sync_symbol_hygiene_staged_action,
    sort_symbol_hygiene_queue,
    symbol_hygiene_queue,
)
from src.suggestions_service import list_suggestions, review_suggestion
from src.theme_service import refresh_active_ticker_universe, replace_ticker_in_theme, seed_if_needed
from src.theme_service import set_ticker_theme_assignments
from src.provider_live import LiveProvider
from src.eod_refresh import has_eod_run_for_date, run_scheduled_eod_refresh
from src.rankings import _build_current_ranking_metrics, _compute_theme_metrics, compute_theme_rankings, theme_confidence_factor


class TestLeaderboardUtils(unittest.TestCase):
    def test_window_specific_sorting_prefers_window_metric(self):
        history = pd.DataFrame(
            [
                {"snapshot_time": "2026-01-01", "theme": "A", "avg_1w": 1.0, "avg_1m": 2.0, "avg_3m": 3.0},
                {"snapshot_time": "2026-01-08", "theme": "A", "avg_1w": 2.0, "avg_1m": 3.0, "avg_3m": 4.0},
                {"snapshot_time": "2026-01-01", "theme": "B", "avg_1w": 5.0, "avg_1m": 1.0, "avg_3m": 1.0},
                {"snapshot_time": "2026-01-08", "theme": "B", "avg_1w": 4.0, "avg_1m": 2.0, "avg_3m": 2.0},
            ]
        )
        summary = pd.DataFrame(
            [
                {"theme": "A", "momentum_score": 10, "rank_change": 1},
                {"theme": "B", "momentum_score": 1, "rank_change": 1},
            ]
        )
        momentum = {"history": history, "window_summary": summary}

        ranked_1w, _ = build_window_leaderboard(momentum, "avg_1w", top_k=2)
        ranked_1m, _ = build_window_leaderboard(momentum, "avg_1m", top_k=2)

        self.assertEqual(ranked_1w.iloc[0]["theme"], "B")
        self.assertEqual(ranked_1m.iloc[0]["theme"], "A")

    def test_window_leaderboard_explains_true_boundary_requirement(self):
        history = pd.DataFrame(
            [
                {"snapshot_time": "2026-03-11", "theme": "A", "avg_1w": 1.0},
                {"snapshot_time": "2026-03-11", "theme": "B", "avg_1w": 2.0},
            ]
        )
        momentum = {"history": history, "window_summary": pd.DataFrame(), "source_preference": "live"}

        ranked, msg = build_window_leaderboard(momentum, "avg_1w", top_k=10)

        self.assertTrue(ranked.empty)
        self.assertIn("two boundary snapshots", msg)
        self.assertIn("currently 1 available", msg)
        self.assertIn("two same-day imports may still be insufficient", msg)

    def test_category_leaderboard_groups_full_window_and_falls_back_to_theme_name(self):
        history = pd.DataFrame(
            [
                {"snapshot_time": "2026-03-01", "theme": "AI Infra", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 50.0},
                {"snapshot_time": "2026-03-08", "theme": "AI Infra", "category": "Tech", "avg_1w": 8.0, "positive_1m_breadth_pct": 70.0},
                {"snapshot_time": "2026-03-01", "theme": "Semis", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 40.0},
                {"snapshot_time": "2026-03-08", "theme": "Semis", "category": "Tech", "avg_1w": 6.0, "positive_1m_breadth_pct": 60.0},
                {"snapshot_time": "2026-03-01", "theme": "Oil Services", "category": "", "avg_1w": 1.0, "positive_1m_breadth_pct": 30.0},
                {"snapshot_time": "2026-03-08", "theme": "Oil Services", "category": "", "avg_1w": 7.0, "positive_1m_breadth_pct": 50.0},
            ]
        )
        summary = pd.DataFrame(
            [
                {"theme": "AI Infra", "momentum_score": 5.0, "rank_change": 2},
                {"theme": "Semis", "momentum_score": 4.0, "rank_change": 1},
                {"theme": "Oil Services", "momentum_score": 3.0, "rank_change": 1},
            ]
        )
        momentum = {"history": history, "window_summary": summary, "source_preference": "live"}

        out, msg = build_category_leaderboard(momentum, "avg_1w", top_k=10)

        self.assertIsNone(msg)
        self.assertEqual(out.iloc[0]["category"], "Tech")
        self.assertEqual(int(out.iloc[0]["contributing_themes"]), 2)
        self.assertEqual(float(out.iloc[0]["performance"]), 7.0)
        self.assertEqual(float(out.iloc[0]["momentum_score"]), 4.5)
        self.assertEqual(float(out.iloc[0]["breadth_1m"]), 65.0)
        self.assertEqual(out.iloc[0]["top_themes"], "AI Infra, Semis")
        self.assertIn("Oil Services", out["category"].tolist())

    def test_category_leaderboard_uses_full_eligible_theme_universe_not_top_theme_sample(self):
        history = pd.DataFrame(
            [
                {"snapshot_time": "2026-03-01", "theme": "Alpha", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 40.0},
                {"snapshot_time": "2026-03-08", "theme": "Alpha", "category": "Tech", "avg_1w": 10.0, "positive_1m_breadth_pct": 80.0},
                {"snapshot_time": "2026-03-01", "theme": "Beta", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 40.0},
                {"snapshot_time": "2026-03-08", "theme": "Beta", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 40.0},
                {"snapshot_time": "2026-03-01", "theme": "Gamma", "category": "Energy", "avg_1w": 1.0, "positive_1m_breadth_pct": 70.0},
                {"snapshot_time": "2026-03-08", "theme": "Gamma", "category": "Energy", "avg_1w": 9.0, "positive_1m_breadth_pct": 70.0},
            ]
        )
        summary = pd.DataFrame(
            [
                {"theme": "Alpha", "momentum_score": 5.0, "rank_change": 2},
                {"theme": "Beta", "momentum_score": 0.0, "rank_change": 0},
                {"theme": "Gamma", "momentum_score": 4.0, "rank_change": 1},
            ]
        )
        momentum = {"history": history, "window_summary": summary, "source_preference": "live"}

        theme_ranked, msg = build_window_leaderboard(momentum, "avg_1w", top_k=2)
        self.assertIsNone(msg)
        self.assertEqual(theme_ranked["theme"].tolist(), ["Alpha", "Gamma"])

        category_ranked, category_msg = build_category_leaderboard(momentum, "avg_1w", top_k=10)
        self.assertIsNone(category_msg)
        self.assertEqual(category_ranked.iloc[0]["category"], "Energy")
        self.assertEqual(float(category_ranked.iloc[0]["performance"]), 9.0)
        self.assertEqual(category_ranked.iloc[0]["top_themes"], "Gamma")

    def test_category_leaderboard_top_theme_preview_truncates_cleanly(self):
        history = pd.DataFrame(
            [
                {"snapshot_time": "2026-03-01", "theme": "A", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 10.0},
                {"snapshot_time": "2026-03-08", "theme": "A", "category": "Tech", "avg_1w": 9.0, "positive_1m_breadth_pct": 90.0},
                {"snapshot_time": "2026-03-01", "theme": "B", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 10.0},
                {"snapshot_time": "2026-03-08", "theme": "B", "category": "Tech", "avg_1w": 8.0, "positive_1m_breadth_pct": 80.0},
                {"snapshot_time": "2026-03-01", "theme": "C", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 10.0},
                {"snapshot_time": "2026-03-08", "theme": "C", "category": "Tech", "avg_1w": 7.0, "positive_1m_breadth_pct": 70.0},
                {"snapshot_time": "2026-03-01", "theme": "D", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 10.0},
                {"snapshot_time": "2026-03-08", "theme": "D", "category": "Tech", "avg_1w": 6.0, "positive_1m_breadth_pct": 60.0},
            ]
        )
        summary = pd.DataFrame(
            [
                {"theme": "A", "momentum_score": 9.0, "rank_change": 4},
                {"theme": "B", "momentum_score": 8.0, "rank_change": 3},
                {"theme": "C", "momentum_score": 7.0, "rank_change": 2},
                {"theme": "D", "momentum_score": 6.0, "rank_change": 1},
            ]
        )
        momentum = {"history": history, "window_summary": summary, "source_preference": "live"}

        out, msg = build_category_leaderboard(momentum, "avg_1w", top_k=10)

        self.assertIsNone(msg)
        self.assertEqual(out.iloc[0]["top_themes"], "A, B, C")
        self.assertNotIn("+", out.iloc[0]["top_themes"])

    def test_category_theme_breakdown_returns_ranked_underlying_themes(self):
        history = pd.DataFrame(
            [
                {"snapshot_time": "2026-03-01", "theme": "Alpha", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 10.0},
                {"snapshot_time": "2026-03-08", "theme": "Alpha", "category": "Tech", "avg_1w": 9.0, "positive_1m_breadth_pct": 90.0},
                {"snapshot_time": "2026-03-01", "theme": "Beta", "category": "Tech", "avg_1w": 1.0, "positive_1m_breadth_pct": 10.0},
                {"snapshot_time": "2026-03-08", "theme": "Beta", "category": "Tech", "avg_1w": 7.0, "positive_1m_breadth_pct": 70.0},
                {"snapshot_time": "2026-03-01", "theme": "Gamma", "category": "", "avg_1w": 1.0, "positive_1m_breadth_pct": 10.0},
                {"snapshot_time": "2026-03-08", "theme": "Gamma", "category": "", "avg_1w": 8.0, "positive_1m_breadth_pct": 80.0},
            ]
        )
        summary = pd.DataFrame(
            [
                {"theme": "Alpha", "momentum_score": 5.0, "rank_change": 2},
                {"theme": "Beta", "momentum_score": 4.0, "rank_change": 1},
                {"theme": "Gamma", "momentum_score": 4.5, "rank_change": 1},
            ]
        )
        momentum = {"history": history, "window_summary": summary, "source_preference": "live"}

        out, msg = build_category_theme_breakdown(momentum, "avg_1w")

        self.assertIsNone(msg)
        tech = out[out["category"] == "Tech"]
        self.assertEqual(tech["theme"].tolist(), ["Alpha", "Beta"])
        self.assertIn("Gamma", out[out["category"] == "Gamma"]["theme"].tolist())

    def test_current_leadership_table_uses_composite_strength_and_quality_context(self):
        rankings = pd.DataFrame(
            [
                {
                    "theme_id": 1,
                    "theme": "Broad Tech",
                    "category": "Tech",
                    "is_active": True,
                    "composite_score": 12.0,
                    "avg_1w": 4.0,
                    "avg_1m": 8.0,
                    "avg_3m": 6.0,
                    "positive_1m_breadth_pct": 72.0,
                    "ticker_count": 10,
                    "eligible_composite_count": 9,
                    "eligible_breadth_pct": 90.0,
                },
                {
                    "theme_id": 2,
                    "theme": "Narrow Spike",
                    "category": "Spec",
                    "is_active": True,
                    "composite_score": 11.5,
                    "avg_1w": 10.0,
                    "avg_1m": 9.0,
                    "avg_3m": 2.0,
                    "positive_1m_breadth_pct": 30.0,
                    "ticker_count": 3,
                    "eligible_composite_count": 3,
                    "eligible_breadth_pct": 100.0,
                },
                {
                    "theme_id": 3,
                    "theme": "Turning Up",
                    "category": "Macro",
                    "is_active": True,
                    "composite_score": 10.0,
                    "avg_1w": 3.0,
                    "avg_1m": 6.0,
                    "avg_3m": 5.0,
                    "positive_1m_breadth_pct": 55.0,
                    "ticker_count": 6,
                    "eligible_composite_count": 5,
                    "eligible_breadth_pct": 83.3,
                },
            ]
        )

        out = build_current_leadership_table(rankings, top_k=10)

        self.assertEqual(out["theme"].tolist(), ["Broad Tech", "Narrow Spike", "Turning Up"])
        self.assertEqual(out.iloc[0]["leadership_quality"], "Broad leader")
        self.assertEqual(out.iloc[1]["leadership_quality"], "Thin / filtered")
        self.assertEqual(out.iloc[2]["leadership_quality"], "Narrow leader")
        self.assertEqual(int(out.iloc[0]["eligible_contributor_count"]), 9)

    def test_current_performance_table_uses_metric_specific_eligible_counts(self):
        rankings = pd.DataFrame(
            [
                {
                    "theme_id": 1,
                    "theme": "Deep Bench",
                    "category": "Tech",
                    "is_active": True,
                    "avg_1w": 8.0,
                    "avg_1m": 12.0,
                    "composite_score": 10.0,
                    "positive_1m_breadth_pct": 70.0,
                    "ticker_count": 8,
                    "eligible_1w_count": 6,
                    "eligible_1m_count": 5,
                    "eligible_3m_count": 5,
                    "eligible_composite_count": 5,
                    "eligible_breadth_pct": 75.0,
                },
                {
                    "theme_id": 2,
                    "theme": "Thin Bench",
                    "category": "Spec",
                    "is_active": True,
                    "avg_1w": 30.0,
                    "avg_1m": 15.0,
                    "composite_score": 8.0,
                    "positive_1m_breadth_pct": 60.0,
                    "ticker_count": 4,
                    "eligible_1w_count": 2,
                    "eligible_1m_count": 4,
                    "eligible_3m_count": 4,
                    "eligible_composite_count": 4,
                    "eligible_breadth_pct": 100.0,
                },
            ]
        )

        out = build_current_performance_table(rankings, "avg_1w", top_k=10)

        self.assertEqual(out["theme"].tolist(), ["Deep Bench"])
        self.assertEqual(int(out.iloc[0]["eligible_contributor_count"]), 6)


class TestThemeConfidenceAdjustment(unittest.TestCase):
    def test_theme_confidence_factor_has_no_penalty_at_threshold(self):
        self.assertEqual(theme_confidence_factor(8), 1.0)
        self.assertEqual(theme_confidence_factor(12), 1.0)

    def test_theme_confidence_factor_softly_penalizes_small_themes(self):
        self.assertEqual(theme_confidence_factor(2), 0.5)
        self.assertAlmostEqual(theme_confidence_factor(4), 0.70710678, places=6)

    def test_compute_theme_metrics_applies_small_theme_confidence_adjustment(self):
        raw = pd.DataFrame(
            [
                {"theme_id": 1, "theme": "Small", "category": "Tech", "is_active": True, "ticker": "A", "perf_1w": 10.0, "perf_1m": 10.0, "perf_3m": 10.0},
                {"theme_id": 1, "theme": "Small", "category": "Tech", "is_active": True, "ticker": "B", "perf_1w": 10.0, "perf_1m": 10.0, "perf_3m": 10.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "C", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "D", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "E", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "F", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "G", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "H", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "I", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
                {"theme_id": 2, "theme": "Broad", "category": "Tech", "is_active": True, "ticker": "J", "perf_1w": 9.0, "perf_1m": 9.0, "perf_3m": 9.0},
            ]
        )

        out = _compute_theme_metrics(raw).sort_values("composite_score", ascending=False).reset_index(drop=True)

        self.assertEqual(out.iloc[0]["theme"], "Broad")
        self.assertEqual(float(out[out["theme"] == "Small"]["composite_score"].iloc[0]), 5.0)
        self.assertEqual(float(out[out["theme"] == "Broad"]["composite_score"].iloc[0]), 9.0)


class TestCurrentThemeRankingHardening(unittest.TestCase):
    def _build_conn(self):
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
                run_id bigint,
                ticker varchar,
                price double,
                perf_1w double,
                perf_1m double,
                perf_3m double,
                market_cap double,
                avg_volume double,
                short_interest_pct double,
                float_shares double,
                adr_pct double,
                last_updated timestamp,
                snapshot_source varchar
            )
            """
        )
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint,
                snapshot_time timestamp,
                theme_id bigint,
                ticker_count bigint,
                avg_1w double,
                avg_1m double,
                avg_3m double,
                positive_1w_breadth_pct double,
                positive_1m_breadth_pct double,
                positive_3m_breadth_pct double,
                composite_score double,
                snapshot_source varchar
            )
            """
        )
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar,
                status varchar
            )
            """
        )
        conn.execute("insert into refresh_runs values (1, 'live', '2026-03-12 20:00:00', '2026-03-12 22:00:00', 'success')")
        conn.execute("insert into themes values (1, 'Quality', 'Tech', true)")
        conn.execute("insert into themes values (2, 'Thin', 'Spec', true)")
        return conn

    def test_build_current_ranking_metrics_excludes_low_price_low_volume_and_suppressed_names(self):
        raw = pd.DataFrame(
            [
                {
                    "theme_id": 1,
                    "theme": "Quality",
                    "category": "Tech",
                    "is_active": True,
                    "ticker": "AAA",
                    "run_id": 1,
                    "snapshot_time": "2026-03-12 22:00:00",
                    "price": 10.0,
                    "avg_volume": 2_000_000.0,
                    "perf_1w": 5.0,
                    "perf_1m": 10.0,
                    "perf_3m": 15.0,
                    "status": "active",
                },
                {
                    "theme_id": 1,
                    "theme": "Quality",
                    "category": "Tech",
                    "is_active": True,
                    "ticker": "PENNY",
                    "run_id": 1,
                    "snapshot_time": "2026-03-12 22:00:00",
                    "price": 0.5,
                    "avg_volume": 50_000_000.0,
                    "perf_1w": 200.0,
                    "perf_1m": 250.0,
                    "perf_3m": 300.0,
                    "status": "active",
                },
                {
                    "theme_id": 1,
                    "theme": "Quality",
                    "category": "Tech",
                    "is_active": True,
                    "ticker": "ILLIQ",
                    "run_id": 1,
                    "snapshot_time": "2026-03-12 22:00:00",
                    "price": 2.0,
                    "avg_volume": 1_000_000.0,
                    "perf_1w": 40.0,
                    "perf_1m": 40.0,
                    "perf_3m": 40.0,
                    "status": "active",
                },
                {
                    "theme_id": 1,
                    "theme": "Quality",
                    "category": "Tech",
                    "is_active": True,
                    "ticker": "SUPR",
                    "run_id": 1,
                    "snapshot_time": "2026-03-12 22:00:00",
                    "price": 8.0,
                    "avg_volume": 3_000_000.0,
                    "perf_1w": 20.0,
                    "perf_1m": 20.0,
                    "perf_3m": 20.0,
                    "status": "refresh_suppressed",
                },
            ]
        )

        out = _build_current_ranking_metrics(raw)

        self.assertEqual(int(out.iloc[0]["ticker_count"]), 4)
        self.assertEqual(int(out.iloc[0]["eligible_ticker_count"]), 1)
        self.assertEqual(int(out.iloc[0]["eligible_composite_count"]), 1)
        self.assertEqual(float(out.iloc[0]["avg_1w"]), 5.0)

    def test_build_current_ranking_metrics_caps_outlier_returns_before_aggregation(self):
        raw = pd.DataFrame(
            [
                {
                    "theme_id": 1,
                    "theme": "Quality",
                    "category": "Tech",
                    "is_active": True,
                    "ticker": "AAA",
                    "run_id": 1,
                    "snapshot_time": "2026-03-12 22:00:00",
                    "price": 10.0,
                    "avg_volume": 2_000_000.0,
                    "perf_1w": 10.0,
                    "perf_1m": 10.0,
                    "perf_3m": 10.0,
                    "status": "active",
                },
                {
                    "theme_id": 1,
                    "theme": "Quality",
                    "category": "Tech",
                    "is_active": True,
                    "ticker": "BBB",
                    "run_id": 1,
                    "snapshot_time": "2026-03-12 22:00:00",
                    "price": 12.0,
                    "avg_volume": 2_000_000.0,
                    "perf_1w": 200.0,
                    "perf_1m": 200.0,
                    "perf_3m": 200.0,
                    "status": "active",
                },
                {
                    "theme_id": 1,
                    "theme": "Quality",
                    "category": "Tech",
                    "is_active": True,
                    "ticker": "CCC",
                    "run_id": 1,
                    "snapshot_time": "2026-03-12 22:00:00",
                    "price": 14.0,
                    "avg_volume": 2_000_000.0,
                    "perf_1w": -20.0,
                    "perf_1m": -20.0,
                    "perf_3m": -20.0,
                    "status": "active",
                },
            ]
        )

        out = _build_current_ranking_metrics(raw)

        self.assertEqual(float(out.iloc[0]["avg_1w"]), 13.33)
        self.assertEqual(float(out.iloc[0]["avg_1m"]), 13.33)

    def test_compute_theme_rankings_excludes_themes_with_too_few_eligible_contributors(self):
        conn = self._build_conn()
        try:
            conn.execute("insert into theme_membership values (1, 'AAA'), (1, 'BBB'), (1, 'CCC')")
            conn.execute("insert into theme_membership values (2, 'XXX'), (2, 'YYY')")
            conn.execute(
                """
                insert into ticker_snapshots values
                (1, 'AAA', 10, 5, 6, 7, null, 2000000, null, null, null, '2026-03-12 21:00:00', 'live'),
                (1, 'BBB', 12, 6, 7, 8, null, 2000000, null, null, null, '2026-03-12 21:00:00', 'live'),
                (1, 'CCC', 14, 7, 8, 9, null, 2000000, null, null, null, '2026-03-12 21:00:00', 'live'),
                (1, 'XXX', 9, 30, 30, 30, null, 2000000, null, null, null, '2026-03-12 21:00:00', 'live'),
                (1, 'YYY', 9, 25, 25, 25, null, 2000000, null, null, null, '2026-03-12 21:00:00', 'live')
                """
            )
            conn.execute(
                """
                insert into theme_snapshots values
                (1, '2026-03-12 22:00:00', 1, 3, 1, 1, 1, 50, 50, 50, 1, 'live'),
                (1, '2026-03-12 22:00:00', 2, 2, 1, 1, 1, 50, 50, 50, 1, 'live')
                """
            )

            out = compute_theme_rankings(conn)

            self.assertEqual(out["theme"].tolist(), ["Quality"])
            self.assertEqual(int(out.iloc[0]["eligible_composite_count"]), 3)
        finally:
            conn.close()

class TestBoundarySelection(unittest.TestCase):
    def test_theme_history_window_uses_boundary_snapshot(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar)")
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint,
                snapshot_time timestamp,
                theme_id bigint,
                ticker_count bigint,
                avg_1w double,
                avg_1m double,
                avg_3m double,
                positive_1m_breadth_pct double,
                composite_score double,
                snapshot_source varchar
            )
            """
        )
        conn.execute("insert into themes values (1, 'A', 'Cat')")
        conn.execute("insert into themes values (2, 'B', 'Cat')")

        # Weekly cadence: latest and prior boundary should both be included for 7d window.
        for run_id, ts in [(1, "2026-03-01"), (2, "2026-03-08")]:
            conn.execute(
                "insert into theme_snapshots values (?, ?, 1, 10, 1, 1, 1, 50, 1, 'live')",
                [run_id, ts],
            )
            conn.execute(
                "insert into theme_snapshots values (?, ?, 2, 10, 1, 1, 1, 50, 1, 'live')",
                [run_id, ts],
            )

        out = theme_history_window(conn, 7)
        self.assertEqual(int(out["snapshot_time"].nunique()), 2)
        conn.close()

    def test_compute_theme_momentum_reports_effective_boundary_window_meta(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar)")
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint,
                snapshot_time timestamp,
                theme_id bigint,
                ticker_count bigint,
                avg_1w double,
                avg_1m double,
                avg_3m double,
                positive_1m_breadth_pct double,
                composite_score double,
                snapshot_source varchar
            )
            """
        )
        conn.execute("insert into themes values (1, 'A', 'Cat')")
        conn.execute("insert into themes values (2, 'B', 'Cat')")

        for run_id, ts in [(1, "2026-03-01"), (2, "2026-03-08")]:
            conn.execute(
                "insert into theme_snapshots values (?, ?, 1, 10, 1, 1, 1, 50, 1, 'live')",
                [run_id, ts],
            )
            conn.execute(
                "insert into theme_snapshots values (?, ?, 2, 10, 1, 1, 1, 50, 1, 'live')",
                [run_id, ts],
            )

        out = compute_theme_momentum(conn, 30)
        self.assertEqual(int(out["meta"]["boundary_snapshot_count"]), 2)
        self.assertEqual(str(pd.to_datetime(out["meta"]["window_start"]).date()), "2026-03-01")
        self.assertEqual(str(pd.to_datetime(out["meta"]["window_end"]).date()), "2026-03-08")
        self.assertTrue(out["meta"]["collapsed_to_available_history"])
        conn.close()

    def test_top_theme_movers_uses_preferred_source_boundary_window(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar)")
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint,
                snapshot_time timestamp,
                theme_id bigint,
                ticker_count bigint,
                avg_1w double,
                avg_1m double,
                avg_3m double,
                positive_1w_breadth_pct double,
                positive_1m_breadth_pct double,
                positive_3m_breadth_pct double,
                composite_score double,
                snapshot_source varchar
            )
            """
        )
        conn.execute("insert into themes values (1, 'AI', 'Tech')")
        conn.execute("insert into themes values (2, 'Energy', 'Macro')")

        conn.execute(
            "insert into theme_snapshots values (1, '2026-03-01 22:00:00', 1, 10, 1, 2, 3, 40, 40, 40, 10, 'live')"
        )
        conn.execute(
            "insert into theme_snapshots values (2, '2026-03-10 22:00:00', 1, 10, 1, 2, 3, 50, 50, 50, 20, 'live')"
        )
        conn.execute(
            "insert into theme_snapshots values (1, '2026-03-01 22:00:00', 2, 10, 1, 2, 3, 20, 20, 20, 5, 'live')"
        )
        conn.execute(
            "insert into theme_snapshots values (2, '2026-03-10 22:00:00', 2, 10, 1, 2, 3, 30, 30, 30, 15, 'live')"
        )
        # Later mock residue should not override the current live-facing movers view.
        conn.execute(
            "insert into theme_snapshots values (3, '2026-03-11 22:00:00', 1, 10, 1, 2, 3, 99, 99, 99, 999, 'mock')"
        )

        out = top_theme_movers(conn, 7, top_n=5)

        self.assertEqual(out.iloc[0]["theme"], "AI")
        self.assertEqual(float(out.iloc[0]["start_composite"]), 10.0)
        self.assertEqual(float(out.iloc[0]["end_composite"]), 20.0)
        self.assertEqual(float(out.iloc[0]["delta_composite"]), 10.0)
        conn.close()

    def test_theme_health_overview_uses_preferred_source_snapshot_time(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table theme_snapshots(
                run_id bigint,
                snapshot_time timestamp,
                theme_id bigint,
                ticker_count bigint,
                avg_1w double,
                avg_1m double,
                avg_3m double,
                positive_1w_breadth_pct double,
                positive_1m_breadth_pct double,
                positive_3m_breadth_pct double,
                composite_score double,
                snapshot_source varchar
            )
            """
        )
        conn.execute("create table refresh_runs(run_id bigint, provider varchar)")
        conn.execute(
            "create table refresh_failures(run_id bigint, ticker varchar, error_message varchar, failure_category varchar, created_at timestamp)"
        )

        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership values (1, 'NVDA')")
        conn.execute(
            "insert into theme_snapshots values (1, '2026-03-10 22:00:00', 1, 1, 1, 2, 3, 50, 60, 70, 10, 'live')"
        )
        conn.execute(
            "insert into theme_snapshots values (2, '2026-03-11 22:00:00', 1, 1, 1, 2, 3, 50, 60, 70, 10, 'mock')"
        )

        out = theme_health_overview(conn, low_constituent_threshold=3, failure_window_days=14)

        self.assertEqual(str(out.iloc[0]["latest_snapshot_time"]), "2026-03-10 22:00:00")
        conn.close()

    def test_theme_member_hygiene_context_sorts_recent_failures_first(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp
            )
            """
        )
        conn.execute("insert into theme_membership values (1, 'AAA')")
        conn.execute("insert into theme_membership values (1, 'BBB')")
        conn.execute("insert into theme_membership values (1, 'CCC')")
        conn.execute(
            """
            insert into symbol_refresh_status(
                ticker, status, last_failure_category, consecutive_failure_count, last_failure_at
            ) values
            ('BBB', 'watch', 'TIMEOUT', 2, '2026-03-11 22:00:00'),
            ('AAA', 'inactive_candidate', 'NO_CANDLES', 5, '2026-03-10 22:00:00')
            """
        )

        out = theme_member_hygiene_context(conn, 1)

        self.assertEqual(out["ticker"].tolist(), ["BBB", "AAA", "CCC"])
        self.assertEqual(str(out.iloc[0]["last_failure_category"]), "TIMEOUT")
        self.assertTrue(pd.isna(out.iloc[2]["last_failure_at"]))
        conn.close()


class TestInflectionEngine(unittest.TestCase):
    @patch("src.inflection_engine.compute_theme_rotation")
    @patch("src.inflection_engine.compute_theme_momentum")
    def test_signal_dedup_keeps_single_highest_priority_per_theme(self, mock_momentum, mock_rotation):
        history = pd.DataFrame(
            [
                {"snapshot_time": "2026-01-01", "theme": "A", "composite_score": 1.0, "avg_1m": 1.0},
                {"snapshot_time": "2026-01-08", "theme": "A", "composite_score": 1.4, "avg_1m": 1.4},
                {"snapshot_time": "2026-01-01", "theme": "B", "composite_score": 1.0, "avg_1m": 1.0},
                {"snapshot_time": "2026-01-08", "theme": "B", "composite_score": 0.6, "avg_1m": 0.6},
            ]
        )
        summary = pd.DataFrame(
            [
                {
                    "theme": "A",
                    "rank_change": 6,
                    "momentum_score": 2.5,
                    "delta_composite": 1.0,
                    "delta_avg_1m": 0.5,
                    "delta_breadth": 0.8,
                },
                {
                    "theme": "B",
                    "rank_change": -6,
                    "momentum_score": -2.5,
                    "delta_composite": -1.0,
                    "delta_avg_1m": -0.5,
                    "delta_breadth": -0.8,
                },
            ]
        )
        mock_momentum.return_value = {
            "history": history,
            "window_summary": summary,
            "new_leaders": ["A"],
            "dropped_leaders": ["B"],
        }
        mock_rotation.return_value = {
            "rotating_into": pd.DataFrame([{"theme": "A"}]),
            "rotating_out": pd.DataFrame([{"theme": "B"}]),
            "emerging": pd.DataFrame(),
            "fading": pd.DataFrame(),
            "acceleration": pd.DataFrame(),
            "deterioration": pd.DataFrame([{"theme": "B"}]),
            "rotation_intensity": {},
        }

        out = compute_theme_inflections(conn=None, lookback_days=30, top_n=20)
        self.assertFalse(out["signals"].empty)
        self.assertEqual(len(out["signals"]["theme"].unique()), len(out["signals"]))
        self.assertIn("window_start", out["meta"])
        self.assertIn("window_end", out["meta"])


class TestFailureClassificationAndHygiene(unittest.TestCase):
    def test_no_candles_classification_is_deterministic(self):
        self.assertEqual(categorize_failure_message("Massive fetch failed: NO_CANDLES: Massive returned no daily aggregates"), "NO_CANDLES")

    def test_symbol_hygiene_flags_then_auto_suppresses_on_repeated_no_candles(self):
        conn = duckdb.connect(":memory:")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp default current_timestamp
            )
            """
        )

        for i in range(1, 4):
            apply_refresh_failure(conn, "BAD1", i, "NO_CANDLES: Massive returned no daily aggregates")
        flagged = conn.execute("select status, suggested_status, consecutive_failure_count from symbol_refresh_status where ticker='BAD1'").fetchone()
        self.assertEqual(flagged[0], "inactive_candidate")
        self.assertEqual(flagged[1], "refresh_suppressed")
        self.assertEqual(int(flagged[2]), 3)

        for i in range(4, 6):
            apply_refresh_failure(conn, "BAD1", i, "NO_CANDLES: Massive returned no daily aggregates")
        suppressed = conn.execute("select status, suggested_status, consecutive_failure_count from symbol_refresh_status where ticker='BAD1'").fetchone()
        self.assertEqual(suppressed[0], "refresh_suppressed")
        self.assertIsNone(suppressed[1])
        self.assertEqual(int(suppressed[2]), 5)

        apply_refresh_success(conn, "BAD1", 6)
        recovered = conn.execute("select status, consecutive_failure_count, last_failure_category from symbol_refresh_status where ticker='BAD1'").fetchone()
        self.assertEqual(recovered[0], "active")
        self.assertEqual(int(recovered[1]), 0)
        self.assertIsNone(recovered[2])
        conn.close()

    def test_symbol_hygiene_queue_includes_last_valid_market_data_context(self):
        conn = duckdb.connect(":memory:")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp default current_timestamp
            )
            """
        )
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
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
            insert into symbol_refresh_status(
                ticker, status, suggested_status, suggested_reason, last_failure_category,
                consecutive_failure_count, rolling_failure_count, last_failure_at, last_success_at, last_run_id
            )
            values ('XYZ', 'inactive_candidate', 'refresh_suppressed', 'Repeated no candles.', 'NO_CANDLES', 4, 4, '2026-03-11 22:00:00', '2026-03-01 22:00:00', 7)
            """
        )
        conn.execute("insert into refresh_runs values (7, 'success', '2026-03-01 22:00:00')")
        conn.execute(
            "insert into ticker_snapshots values (7, 'XYZ', 10, 1, 2, 3, 1000000, 10000, null, null, null, '2026-02-28 21:00:00', 'live')"
        )

        out = symbol_hygiene_queue(conn, limit=50)

        self.assertEqual(str(out.iloc[0]["last_market_data_at"]), "2026-02-28 21:00:00")
        self.assertGreaterEqual(int(out.iloc[0]["days_since_last_valid_data"]), 0)
        conn.close()

    def test_sort_symbol_hygiene_queue_supports_operational_priorities(self):
        queue = pd.DataFrame(
            [
                {
                    "ticker": "AAA",
                    "status": "inactive_candidate",
                    "suggested_status": "refresh_suppressed",
                    "suggested_reason": "review",
                    "last_failure_category": "NO_CANDLES",
                    "consecutive_failure_count": 5,
                    "rolling_failure_count": 8,
                    "last_success_at": None,
                    "last_failure_at": "2026-03-10 22:00:00",
                    "last_run_id": 5,
                    "last_market_data_at": "2026-02-01 21:00:00",
                    "days_since_last_valid_data": 40,
                },
                {
                    "ticker": "BBB",
                    "status": "watch",
                    "suggested_status": None,
                    "suggested_reason": None,
                    "last_failure_category": "TIMEOUT",
                    "consecutive_failure_count": 1,
                    "rolling_failure_count": 12,
                    "last_success_at": "2026-03-10 22:00:00",
                    "last_failure_at": "2026-03-11 22:00:00",
                    "last_run_id": 6,
                    "last_market_data_at": "2026-03-10 21:00:00",
                    "days_since_last_valid_data": 2,
                },
            ]
        )

        by_confidence = sort_symbol_hygiene_queue(queue, "Highest confidence")
        by_rolling = sort_symbol_hygiene_queue(queue, "Most rolling failures")

        self.assertEqual(by_confidence.iloc[0]["ticker"], "AAA")
        self.assertEqual(by_rolling.iloc[0]["ticker"], "BBB")

    def test_filter_symbol_hygiene_queue_hides_resolved_suppressions_by_default(self):
        queue = pd.DataFrame(
            [
                {"ticker": "AAA", "status": "inactive_candidate", "suggested_status": "refresh_suppressed"},
                {"ticker": "BBB", "status": "watch", "suggested_status": None},
                {"ticker": "CCC", "status": "refresh_suppressed", "suggested_status": None},
            ]
        )

        pending = filter_symbol_hygiene_queue(queue, "Pending review")
        resolved = filter_symbol_hygiene_queue(queue, "Suppressed / resolved")

        self.assertEqual(pending["ticker"].tolist(), ["AAA", "BBB"])
        self.assertEqual(resolved["ticker"].tolist(), ["CCC"])

    def test_apply_staged_symbol_hygiene_actions_updates_multiple_rows(self):
        conn = duckdb.connect(":memory:")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp
            )
            """
        )
        conn.execute(
            """
            insert into symbol_refresh_status(ticker, status, suggested_status, consecutive_failure_count, rolling_failure_count)
            values
            ('AAA', 'inactive_candidate', 'refresh_suppressed', 5, 8),
            ('BBB', 'watch', null, 1, 3),
            ('CCC', 'inactive_candidate', 'refresh_suppressed', 4, 6)
            """
        )

        out = apply_staged_symbol_hygiene_actions(
            conn,
            {"AAA": "suppress", "BBB": "keep_active", "CCC": "watch", "DDD": "none"},
        )
        rows = conn.execute(
            "select ticker, status, suggested_status from symbol_refresh_status order by ticker"
        ).fetchall()

        self.assertEqual(int(out["applied_count"]), 3)
        self.assertEqual(out["by_action"]["suppress"], 1)
        self.assertEqual(out["by_action"]["keep_active"], 1)
        self.assertEqual(out["by_action"]["watch"], 1)
        self.assertEqual(rows, [("AAA", "refresh_suppressed", None), ("BBB", "active", None), ("CCC", "watch", None)])
        self.assertEqual(STAGED_ACTIONS["reset"], "Stage reset history")
        conn.close()

    def test_symbol_hygiene_queue_includes_membership_context(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
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
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp
            )
            """
        )
        conn.execute("insert into themes values (1, '3D Printing', 'Emerging Tech', true)")
        conn.execute("insert into theme_membership values (1, 'DDD')")
        conn.execute(
            """
            insert into symbol_refresh_status(
                ticker, status, suggested_status, last_failure_category, consecutive_failure_count, rolling_failure_count
            ) values ('DDD', 'inactive_candidate', 'refresh_suppressed', 'NO_CANDLES', 4, 7)
            """
        )

        out = symbol_hygiene_queue(conn, limit=20)

        self.assertEqual(out.iloc[0]["current_theme_names"], "3D Printing")
        self.assertEqual(out.iloc[0]["current_categories"], "Emerging Tech")
        conn.close()

    def test_symbol_hygiene_queue_flags_calculation_outliers_and_keeps_membership_visible(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
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
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp default current_timestamp
            )
            """
        )
        conn.execute("insert into themes values (1, 'Meme Stocks', 'Spec', true)")
        conn.execute("insert into theme_membership values (1, 'BBIG')")
        conn.execute("insert into refresh_runs values (7, 'success', '2026-03-12 22:00:00')")
        conn.execute(
            """
            insert into ticker_snapshots values
            (7, 'BBIG', 2.0, 180.0, 240.0, 300.0, 10000000, 1000000, null, null, null, '2026-03-12 21:00:00', 'live')
            """
        )

        queue = symbol_hygiene_queue(conn, limit=20)

        self.assertEqual(queue.iloc[0]["ticker"], "BBIG")
        self.assertEqual(queue.iloc[0]["suggested_status"], "refresh_suppressed")
        self.assertEqual(queue.iloc[0]["last_failure_category"], "CALC_OUTLIER")
        self.assertIn("Meme Stocks", str(queue.iloc[0]["current_theme_names"]))
        self.assertIn("current rankings, historical movement", str(queue.iloc[0]["affected_calculation_surfaces"]))
        self.assertIn("Extreme", str(queue.iloc[0]["outlier_reason"]))

        apply_staged_symbol_hygiene_actions(conn, {"BBIG": "suppress"})
        member_context = theme_member_hygiene_context(conn, 1)

        self.assertEqual(member_context.iloc[0]["ticker"], "BBIG")
        self.assertEqual(member_context.iloc[0]["symbol_hygiene_status"], "refresh_suppressed")
        conn.close()

    def test_resolve_staged_symbol_hygiene_action_prefers_override(self):
        self.assertEqual(resolve_staged_symbol_hygiene_action(True, "none"), "suppress")
        self.assertEqual(resolve_staged_symbol_hygiene_action(False, "keep_active"), "keep_active")
        self.assertEqual(resolve_staged_symbol_hygiene_action(True, "watch"), "watch")
        self.assertEqual(resolve_staged_symbol_hygiene_action(False, "none"), "none")
        self.assertEqual(OVERRIDE_ACTIONS["reset"], "Reset history")

    def test_sync_and_clear_symbol_hygiene_staged_state_use_one_source_of_truth(self):
        session_state = {
            "symbol_hygiene_staged": {},
            "stage_approve_AAA": True,
            "stage_override_AAA": "none",
            "stage_approve_BBB": True,
            "stage_override_BBB": "watch",
        }

        first = sync_symbol_hygiene_staged_action(session_state, "AAA")
        second = sync_symbol_hygiene_staged_action(session_state, "BBB")

        self.assertEqual(first, "suppress")
        self.assertEqual(second, "watch")
        self.assertEqual(session_state["symbol_hygiene_staged"], {"AAA": "suppress", "BBB": "watch"})

        clear_symbol_hygiene_staged_state(session_state, ["AAA", "BBB"])

        self.assertEqual(session_state["symbol_hygiene_staged"], {})
        self.assertFalse(bool(session_state["stage_approve_AAA"]))
        self.assertEqual(session_state["stage_override_AAA"], "none")
        self.assertFalse(bool(session_state["stage_approve_BBB"]))
        self.assertEqual(session_state["stage_override_BBB"], "none")


class TestMetricFormattingAndReturnSafety(unittest.TestCase):
    def test_human_readable_number(self):
        self.assertEqual(human_readable_number(125900000000), "125.9B")
        self.assertEqual(human_readable_number(50000000), "50.0M")
        self.assertEqual(human_readable_number(55825862), "55.8M")

    def test_short_timestamp_is_cross_platform(self):
        self.assertEqual(short_timestamp("2026-03-09T21:00:00Z"), "Mar 9 21:00")

    def test_theme_ticker_table_adds_dollar_volume_and_formats_time(self):
        df = pd.DataFrame([
            {
                "ticker": "ABC",
                "price": 10.1234,
                "avg_volume": 55825862,
                "market_cap": 125900000000,
                "perf_1w": 1.2345,
                "perf_1m": 2.3456,
                "perf_3m": 3.4567,
                "last_updated": "2026-03-09T21:00:00Z",
            }
        ])
        out = format_theme_ticker_table(df)
        self.assertEqual(out.iloc[0]["market_cap"], "125.9B")
        self.assertEqual(out.iloc[0]["avg_volume"], "55.8M")
        self.assertEqual(out.iloc[0]["dollar_volume"], "565.1M")
        self.assertEqual(float(out.iloc[0]["perf_1w"]), 1.23)
        self.assertTrue(str(out.iloc[0]["last_updated"]).startswith("Mar"))

    def test_live_calc_return_returns_none_when_history_insufficient(self):
        self.assertIsNone(LiveProvider._calc_return([1, 2, 3], 5))


class TestTickerLookup(unittest.TestCase):
    def test_ticker_lookup_reports_assigned_membership(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp, snapshot_source varchar
            )
            """
        )
        conn.execute("create table refresh_run_tickers(run_id bigint, ticker varchar)")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp
            )
            """
        )

        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership values (1, 'NVDA')")
        conn.execute("insert into refresh_runs values (1, 'success', '2026-03-10 22:00:00')")
        conn.execute(
            "insert into ticker_snapshots values (1, 'NVDA', 120, 1, 2, 3, 1000000000, 5000000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )

        summary = ticker_lookup_summary(conn, " nvda ")
        memberships = ticker_lookup_memberships(conn, "nvda")

        self.assertEqual(summary.iloc[0]["lookup_status"], "In DB and assigned")
        self.assertTrue(bool(summary.iloc[0]["exists_in_theme_membership"]))
        self.assertTrue(bool(summary.iloc[0]["exists_in_ticker_snapshots"]))
        self.assertEqual(memberships.iloc[0]["theme_name"], "AI")
        conn.close()

    def test_ticker_lookup_reports_snapshots_only(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp, snapshot_source varchar
            )
            """
        )
        conn.execute("create table refresh_run_tickers(run_id bigint, ticker varchar)")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp
            )
            """
        )

        conn.execute("insert into refresh_runs values (1, 'success', '2026-03-10 22:00:00')")
        conn.execute(
            "insert into ticker_snapshots values (1, 'PLTR', 25, 1, 2, 3, 25000000000, 10000000, null, null, null, '2026-03-10 21:00:00', 'live')"
        )

        summary = ticker_lookup_summary(conn, "PLTR")

        self.assertEqual(summary.iloc[0]["lookup_status"], "Seen in snapshots only")
        self.assertFalse(bool(summary.iloc[0]["exists_in_theme_membership"]))
        self.assertTrue(bool(summary.iloc[0]["exists_in_ticker_snapshots"]))
        conn.close()

    def test_ticker_lookup_reports_not_found(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp, snapshot_source varchar
            )
            """
        )
        conn.execute("create table refresh_run_tickers(run_id bigint, ticker varchar)")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp
            )
            """
        )

        summary = ticker_lookup_summary(conn, "ZZZZ")
        memberships = ticker_lookup_memberships(conn, "ZZZZ")

        self.assertEqual(summary.iloc[0]["lookup_status"], "Not found")
        self.assertTrue(memberships.empty)
        conn.close()


class TestTickerAssignmentEditing(unittest.TestCase):
    def test_replace_ticker_in_theme_swaps_membership_for_selected_theme_only(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar, primary key(theme_id, ticker))")
        conn.execute("insert into theme_membership values (1, 'CRSPR')")
        conn.execute("insert into theme_membership values (2, 'CRSPR')")

        result = replace_ticker_in_theme(conn, 1, " crspr ", " crsp ")
        theme_one = conn.execute(
            "select ticker from theme_membership where theme_id = 1 order by ticker"
        ).fetchall()
        theme_two = conn.execute(
            "select ticker from theme_membership where theme_id = 2 order by ticker"
        ).fetchall()

        self.assertEqual(result["removed_ticker"], "CRSPR")
        self.assertEqual(result["added_ticker"], "CRSP")
        self.assertEqual(theme_one, [("CRSP",)])
        self.assertEqual(theme_two, [("CRSPR",)])
        conn.close()

    def test_replace_ticker_in_theme_rejects_duplicate_or_unchanged_replacement(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar, primary key(theme_id, ticker))")
        conn.execute("insert into theme_membership values (1, 'CRSPR')")
        conn.execute("insert into theme_membership values (1, 'CRSP')")

        with self.assertRaisesRegex(ValueError, "already assigned to this theme"):
            replace_ticker_in_theme(conn, 1, "CRSPR", "CRSP")

        with self.assertRaisesRegex(ValueError, "must be different"):
            replace_ticker_in_theme(conn, 1, "CRSPR", "crspr")
        conn.close()

    def test_set_ticker_theme_assignments_requires_at_least_one_theme(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar, primary key(theme_id, ticker))")
        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")

        with self.assertRaisesRegex(ValueError, "Select at least one theme assignment"):
            set_ticker_theme_assignments(conn, "nvda", [])
        conn.close()

    def test_set_ticker_theme_assignments_upserts_without_duplicates(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar, primary key(theme_id, ticker))")
        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
        conn.execute("insert into themes values (2, 'Robotics', 'Tech', true)")
        conn.execute("insert into theme_membership values (1, 'NVDA')")

        first = set_ticker_theme_assignments(conn, " nvda ", [1, 2, 2])
        members = conn.execute(
            "select theme_id, ticker from theme_membership where ticker='NVDA' order by theme_id"
        ).fetchall()

        self.assertEqual(first["ticker"], "NVDA")
        self.assertEqual(int(first["added_count"]), 1)
        self.assertEqual(int(first["removed_count"]), 0)
        self.assertEqual(members, [(1, "NVDA"), (2, "NVDA")])

        second = set_ticker_theme_assignments(conn, "NVDA", [2])
        members_after = conn.execute(
            "select theme_id, ticker from theme_membership where ticker='NVDA' order by theme_id"
        ).fetchall()

        self.assertEqual(int(second["added_count"]), 0)
        self.assertEqual(int(second["removed_count"]), 1)
        self.assertEqual(members_after, [(2, "NVDA")])
        conn.close()


class TestRefreshUniverseSemantics(unittest.TestCase):
    def test_refresh_active_ticker_universe_excludes_suppressed_symbols(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table symbol_refresh_status(
                ticker varchar primary key,
                status varchar,
                suggested_status varchar,
                suggested_reason varchar,
                suppression_reason varchar,
                last_failure_category varchar,
                consecutive_failure_count bigint,
                rolling_failure_count bigint,
                last_failure_at timestamp,
                last_success_at timestamp,
                last_run_id bigint,
                updated_at timestamp
            )
            """
        )
        conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership values (1, 'NVDA')")
        conn.execute("insert into theme_membership values (1, 'PLTR')")
        conn.execute(
            "insert into symbol_refresh_status(ticker, status, consecutive_failure_count, rolling_failure_count) values ('PLTR', 'refresh_suppressed', 5, 8)"
        )

        out = refresh_active_ticker_universe(conn)

        self.assertEqual(out, ["NVDA"])
        conn.close()


class TestSuggestionsWorkflow(unittest.TestCase):
    def test_review_suggestion_reports_change_and_noop(self):
        conn = duckdb.connect(":memory:")
        conn.execute(
            """
            create table theme_suggestions(
                suggestion_id bigint primary key,
                suggestion_type varchar,
                status varchar,
                source varchar,
                rationale varchar,
                priority varchar,
                proposed_theme_name varchar,
                proposed_ticker varchar,
                existing_theme_id bigint,
                proposed_target_theme_id bigint,
                reviewed_at timestamp,
                reviewer_notes varchar
            )
            """
        )
        conn.execute(
            """
            insert into theme_suggestions(
                suggestion_id, suggestion_type, status, source, rationale, priority
            ) values (1, 'review_theme', 'pending', 'manual', '', 'medium')
            """
        )

        changed = review_suggestion(conn, 1, "approved", "looks good")
        stored = conn.execute("select status, reviewer_notes from theme_suggestions where suggestion_id = 1").fetchone()
        noop = review_suggestion(conn, 1, "approved", "looks good")

        self.assertTrue(bool(changed["changed"]))
        self.assertEqual(changed["old_status"], "pending")
        self.assertEqual(changed["new_status"], "approved")
        self.assertEqual(stored, ("approved", "looks good"))
        self.assertFalse(bool(noop["changed"]))
        self.assertIn("already approved", str(noop["message"]))
        conn.close()

    def test_list_suggestions_includes_ticker_membership_context(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table theme_suggestions(
                suggestion_id bigint primary key,
                suggestion_type varchar,
                status varchar,
                source varchar,
                rationale varchar,
                priority varchar,
                proposed_theme_name varchar,
                proposed_ticker varchar,
                existing_theme_id bigint,
                proposed_target_theme_id bigint,
                reviewed_at timestamp,
                reviewer_notes varchar,
                created_at timestamp
            )
            """
        )
        conn.execute("insert into themes values (1, 'Edge Computing', 'Emerging Tech', true)")
        conn.execute("insert into themes values (2, 'Cloud Security', 'Technology - Software', true)")
        conn.execute("insert into theme_membership values (1, 'CRWD')")
        conn.execute("insert into theme_membership values (2, 'CRWD')")
        conn.execute(
            """
            insert into theme_suggestions(
                suggestion_id, suggestion_type, status, source, rationale, priority, proposed_ticker, created_at
            ) values (1, 'review_theme', 'pending', 'manual', '', 'medium', 'CRWD', '2026-03-12 12:00:00')
            """
        )

        out = list_suggestions(conn, status="pending")

        self.assertEqual(out.iloc[0]["current_theme_names"], "Cloud Security, Edge Computing")
        self.assertIn("Cloud Security (Technology - Software)", str(out.iloc[0]["current_membership_context"]))
        self.assertIn("Edge Computing (Emerging Tech)", str(out.iloc[0]["current_membership_context"]))
        self.assertIn("Emerging Tech", str(out.iloc[0]["current_categories"]))
        conn.close()


class TestThemeSeedBackfill(unittest.TestCase):
    @patch("src.theme_service.load_seed_file")
    def test_seed_backfills_membership_when_themes_exist(self, mock_load_seed):
        mock_load_seed.return_value = [
            {"name": "AI", "category": "Tech", "tickers": ["NVDA", "MSFT"]},
            {"name": "Energy", "category": "Macro", "tickers": ["XOM"]},
        ]

        conn = duckdb.connect(":memory:")
        conn.execute("create sequence if not exists themes_id_seq")
        conn.execute("create table themes(id bigint primary key default nextval('themes_id_seq'), name varchar unique, category varchar, is_active boolean default true, created_at timestamp default current_timestamp, updated_at timestamp default current_timestamp)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar, created_at timestamp default current_timestamp, primary key(theme_id, ticker))")

        conn.execute("insert into themes(name, category, is_active) values ('AI','Tech', true)")
        changed = seed_if_needed(conn)

        self.assertTrue(changed)
        members = conn.execute("select t.name, m.ticker from theme_membership m join themes t on t.id=m.theme_id order by t.name, m.ticker").fetchall()
        self.assertEqual(members, [("AI", "MSFT"), ("AI", "NVDA"), ("Energy", "XOM")])

        changed_again = seed_if_needed(conn)
        self.assertFalse(changed_again)
        members_again = conn.execute("select t.name, m.ticker from theme_membership m join themes t on t.id=m.theme_id order by t.name, m.ticker").fetchall()
        self.assertEqual(members_again, members)
        conn.close()


if __name__ == "__main__":
    unittest.main()


class TestBootstrapAndHistoryFramework(unittest.TestCase):
    def test_get_conn_surfaces_friendly_database_locked_error(self):
        with patch(
            "src.database.duckdb.connect",
            side_effect=duckdb.IOException(
                "IO Error: Cannot open file 'theme_dashboard.duckdb': The process cannot access the file because it is being used by another process."
            ),
        ) as mock_connect, patch("src.database.time.sleep"):
            from src.database import DatabaseLockedError, get_conn

            with self.assertRaises(DatabaseLockedError) as ctx:
                with get_conn():
                    pass

        self.assertEqual(mock_connect.call_count, 3)
        message = str(ctx.exception)
        self.assertIn("locked by another process", message)
        self.assertIn("another Streamlit dashboard instance", message)
        self.assertIn("Database path:", message)

    def test_init_db_bootstrap_after_file_delete_and_seed_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_theme_dashboard.duckdb"
            with patch("src.database.DB_PATH", db_path), patch("src.config.DB_PATH", db_path):
                from src.database import get_conn, init_db

                init_db()
                self.assertTrue(db_path.exists())
                with get_conn() as conn:
                    first_seed = seed_if_needed(conn)
                    second_seed = seed_if_needed(conn)
                    self.assertIn(first_seed, [True, False])
                    self.assertFalse(second_seed)

                db_path.unlink()
                init_db()
                with get_conn() as conn:
                    themes_count = int(conn.execute("select count(*) from themes").fetchone()[0])
                    self.assertGreater(themes_count, 0)

    def test_history_helpers_return_recent_snapshots_and_latest(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
        conn.execute("create table themes(id bigint, name varchar, category varchar)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
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

        conn.execute("insert into themes values (1, 'AI', 'Tech')")
        conn.execute("insert into theme_membership values (1, 'NVDA')")

        for i in range(1, 17):
            ts = f"2026-03-{i:02d} 22:00:00"
            conn.execute("insert into refresh_runs values (?, 'success', ?)", [i, ts])
            conn.execute(
                "insert into ticker_snapshots values (?, 'NVDA', 100 + ?, 1, 2, 3, 1000000000 + ?, 1000000 + ?, null, null, null, ?, 'live')",
                [i, i, i, i, ts],
            )
            conn.execute(
                "insert into theme_snapshots values (?, ?, 1, 1, 1, 2, 3, 50, 60, 70, 10 + ?, 'live')",
                [i, ts, i],
            )

        recent_theme = theme_history_last_n_snapshots(conn, 1, snapshot_limit=14)
        recent_ticker = ticker_history_last_n_snapshots(conn, "nvda", snapshot_limit=14)
        latest_ticker = latest_ticker_snapshots(conn)

        self.assertEqual(len(recent_theme), 14)
        self.assertEqual(len(recent_ticker), 14)
        self.assertEqual(int(latest_ticker.iloc[0]["run_id"]), 16)
        conn.close()

    def test_theme_ticker_metrics_uses_latest_available_snapshot_for_market_cap(self):
        conn = duckdb.connect(":memory:")
        conn.execute("create table refresh_runs(run_id bigint, status varchar, finished_at timestamp)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
        conn.execute(
            """
            create table ticker_snapshots(
                run_id bigint, ticker varchar, price double, perf_1w double, perf_1m double, perf_3m double,
                market_cap double, avg_volume double, short_interest_pct double, float_shares double, adr_pct double,
                last_updated timestamp
            )
            """
        )

        conn.execute("insert into theme_membership values (1, 'ABC')")
        conn.execute("insert into refresh_runs values (1, 'success', '2026-03-10 22:00:00')")
        conn.execute("insert into refresh_runs values (2, 'partial', '2026-03-11 22:00:00')")
        conn.execute("insert into ticker_snapshots values (1, 'ABC', 10, 1, 2, 3, 125900000000, 50000000, null, null, null, '2026-03-10 21:00:00')")
        conn.execute("insert into ticker_snapshots values (2, 'ABC', 11, 1.5, 2.5, 3.5, null, 51000000, null, null, null, '2026-03-11 21:00:00')")

        out = theme_ticker_metrics(conn, 1)
        self.assertEqual(float(out.iloc[0]["market_cap"]), 125900000000)
        formatted = format_theme_ticker_table(out)
        self.assertEqual(formatted.iloc[0]["market_cap"], "125.9B")
        self.assertEqual(str(out.iloc[0]["latest_refresh_time"]), "2026-03-11 22:00:00")
        conn.close()

    def test_run_refresh_backfills_market_cap_from_latest_nonnull_snapshot(self):
        class NullCapProvider:
            name = "live"

            def fetch_ticker_data(self, tickers):
                return (
                    pd.DataFrame(
                        [
                            {
                                "ticker": "ABC",
                                "price": 11.0,
                                "perf_1w": 1.5,
                                "perf_1m": 2.5,
                                "perf_3m": 3.5,
                                "market_cap": None,
                                "avg_volume": 51000000.0,
                                "short_interest_pct": None,
                                "float_shares": None,
                                "adr_pct": None,
                                "last_updated": "2026-03-11 21:00:00",
                            }
                        ]
                    ),
                    [],
                )

            def get_call_accounting(self):
                return {"api_call_count": 1, "endpoint_counts": {"aggs_daily": 1}}

        db_path = Path(__file__).resolve().parent / "test_market_cap_refresh.duckdb"
        if db_path.exists():
            db_path.unlink()
        try:
            with patch("src.database.DB_PATH", db_path), patch("src.config.DB_PATH", db_path):
                from src.database import get_conn, init_db

                init_db()
                with get_conn() as conn:
                    conn.execute("delete from themes")
                    conn.execute("delete from theme_membership")
                    conn.execute("insert into themes(id, name, category, is_active) values (1, 'Test Theme', 'Tech', true)")
                    conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'ABC')")
                    prior_run_id = conn.execute(
                        """
                        insert into refresh_runs(provider, started_at, finished_at, status, ticker_count, success_count, failure_count)
                        values ('live', '2026-03-10 20:00:00', '2026-03-10 22:00:00', 'success', 1, 1, 0)
                        returning run_id
                        """
                    ).fetchone()[0]
                    conn.execute(
                        """
                        insert into ticker_snapshots(
                            run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                            market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated, snapshot_source
                        )
                        values (?, 'ABC', 10, 1, 2, 3, 125900000000, 50000000, null, null, null, '2026-03-10 21:00:00', 'live')
                        """,
                        [prior_run_id],
                    )

                    with patch("src.fetch_data.get_provider", return_value=NullCapProvider()), patch(
                        "src.fetch_data.persist_theme_snapshot_for_run", return_value=None
                    ):
                        run_id = run_refresh(conn, provider_name="live", tickers=["ABC"])

                    stored = conn.execute(
                        "select market_cap from ticker_snapshots where run_id = ? and ticker = 'ABC'",
                        [run_id],
                    ).fetchone()
                    self.assertIsNotNone(stored)
                    self.assertEqual(float(stored[0]), 125900000000)
        finally:
            if db_path.exists():
                db_path.unlink()


class TestEODRefreshFramework(unittest.TestCase):
    def test_has_eod_run_for_date_and_force_runner(self):
        conn = duckdb.connect(":memory:")
        conn.execute(
            "create table refresh_runs(run_id bigint, status varchar, finished_at timestamp, scope_type varchar)"
        )
        conn.execute("insert into refresh_runs values (1, 'success', '2026-03-10 22:30:00', 'scheduled_eod')")

        dt_et = datetime(2026, 3, 10, 19, 0, tzinfo=UTC)
        self.assertTrue(has_eod_run_for_date(conn, dt_et))

        with patch("src.eod_refresh.active_ticker_universe", return_value=["AAPL"]), patch("src.eod_refresh.run_refresh", return_value=42) as mock_run:
            run_id = run_scheduled_eod_refresh(conn, provider_name="live", force=True)
            self.assertEqual(run_id, 42)
            mock_run.assert_called_once()
        conn.close()
