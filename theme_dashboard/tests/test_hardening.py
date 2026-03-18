import json
import inspect
from contextlib import contextmanager
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from src.fetch_data import run_refresh
from src.database import get_conn, get_db_connection
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
from src.suggestions_service import (
    bulk_update_filtered_status,
    can_apply_queue_suggestion_row,
    can_fast_path_create_governed_theme_row,
    can_follow_up_applied_scanner_audit_review_row,
    fast_path_create_governed_theme_and_assign_ticker,
    list_suggestions,
    review_suggestion,
    update_suggestion_status,
)
from src.scanner_audit import (
    apply_scanner_candidate_selected_themes,
    promote_scanner_candidate_to_theme_review,
    scanner_candidate_summary,
    send_preserved_applied_scanner_audit_theme_to_review,
)
from src.suggestions_page_state import (
    apply_generated_theme_idea_checkbox_selection,
    add_theme_to_selected_existing,
    finalize_possible_new_theme_category_state,
    finalize_possible_new_theme_state,
    has_meaningful_theme_review_selection,
    join_possible_new_theme_ideas,
    merge_generated_theme_ideas_with_custom,
    merge_suggested_and_custom_theme_ids,
    normalize_theme_id_list,
    prepare_possible_new_theme_category_prefill,
    prepare_possible_new_theme_prefill,
    reconcile_possible_new_theme_from_generated_checkbox_state,
    resolve_active_suggestions_tab,
    resolve_scanner_audit_ticker,
    split_possible_new_theme_ideas,
    split_selected_existing_theme_ids,
    sync_generated_theme_idea_checkbox_state,
    sync_suggested_theme_checkbox_state,
)
from src.suggestions_service import recent_applied_suggestions
from src.theme_service import refresh_active_ticker_universe, replace_ticker_in_theme, seed_if_needed
from src.theme_service import set_ticker_theme_assignments
from src.provider_live import LiveProvider
from src.scanner_research import (
    _description_theme_generation_draft,
    get_scanner_research_review,
    save_scanner_research_review,
    scanner_research_review_summary,
)
from src.streamlit_utils import _load_theme_inflections_cached, _load_theme_momentum_cached
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


class TestHistoricalAnalyticsConnectionIsolation(unittest.TestCase):
    def test_theme_momentum_cached_loader_uses_fresh_connections(self):
        _load_theme_momentum_cached.clear()
        seen_connections: list[object] = []

        class _FreshConn:
            pass

        class _FreshConnContext:
            def __init__(self):
                self.conn = _FreshConn()

            def __enter__(self):
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch("src.streamlit_utils.get_conn", side_effect=AssertionError("shared connection should not be used")):
            with patch("src.streamlit_utils.get_fresh_read_conn", side_effect=lambda: _FreshConnContext()):
                with patch("src.streamlit_utils.compute_theme_momentum", side_effect=lambda conn, lookback_days, top_n=20: seen_connections.append(conn) or {"history": pd.DataFrame(), "window_summary": pd.DataFrame()}):
                    _load_theme_momentum_cached(("db", 1), 30, 20)
                    _load_theme_momentum_cached(("db", 1), 31, 20)

        self.assertEqual(len(seen_connections), 2)
        self.assertIsNot(seen_connections[0], seen_connections[1])

    def test_theme_inflections_cached_loader_uses_fresh_connections(self):
        _load_theme_inflections_cached.clear()
        seen_connections: list[object] = []

        class _FreshConn:
            pass

        class _FreshConnContext:
            def __init__(self):
                self.conn = _FreshConn()

            def __enter__(self):
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch("src.streamlit_utils.get_conn", side_effect=AssertionError("shared connection should not be used")):
            with patch("src.streamlit_utils.get_fresh_read_conn", side_effect=lambda: _FreshConnContext()):
                with patch("src.streamlit_utils.compute_theme_inflections", side_effect=lambda conn, lookback_days, top_n=20: {"leaders": pd.DataFrame(), "laggards": pd.DataFrame(), "turnarounds": pd.DataFrame(), "momentum": seen_connections.append(conn) or {"history": pd.DataFrame(), "window_summary": pd.DataFrame()}}):
                    _load_theme_inflections_cached(("db", 1), 30, 20)
                    _load_theme_inflections_cached(("db", 1), 31, 20)

        self.assertEqual(len(seen_connections), 2)
        self.assertIsNot(seen_connections[0], seen_connections[1])


class TestSuggestionsPageState(unittest.TestCase):
    def test_resolve_active_suggestions_tab_preserves_valid_selection(self):
        options = ["Manual", "Queue", "Rules", "AI", "Scanner Audit"]
        self.assertEqual(
            resolve_active_suggestions_tab("Scanner Audit", options, "Manual"),
            "Scanner Audit",
        )

    def test_resolve_active_suggestions_tab_falls_back_to_default(self):
        options = ["Manual", "Queue", "Rules", "AI", "Scanner Audit"]
        self.assertEqual(
            resolve_active_suggestions_tab("Unknown", options, "Manual"),
            "Manual",
        )

    def test_resolve_scanner_audit_ticker_preserves_valid_selection(self):
        self.assertEqual(
            resolve_scanner_audit_ticker("NVDA", ["AAPL", "NVDA", "PLTR"]),
            "NVDA",
        )

    def test_resolve_scanner_audit_ticker_falls_back_to_first_option(self):
        self.assertEqual(
            resolve_scanner_audit_ticker("MISSING", ["AAPL", "NVDA", "PLTR"]),
            "AAPL",
        )

    def test_clicking_suggested_theme_adds_it_to_selected_existing_state(self):
        selected = add_theme_to_selected_existing([], 7, {1, 7, 9})
        self.assertEqual(selected, [7])

    def test_duplicate_clicks_do_not_create_duplicate_selected_existing_themes(self):
        selected = add_theme_to_selected_existing([7], 7, {1, 7, 9})
        self.assertEqual(selected, [7])

    def test_selected_existing_theme_state_persists_cleanly_across_rerun_normalization(self):
        normalized = normalize_theme_id_list([7, "9", 7, "bad"], {7, 9, 10})
        self.assertEqual(normalized, [7, 9])

    def test_manual_selection_and_click_to_add_work_together(self):
        selected = add_theme_to_selected_existing([3], 7, {3, 7, 9})
        suggested_ids, custom_ids = split_selected_existing_theme_ids(selected, [7, 9])
        self.assertEqual(suggested_ids, [7])
        self.assertEqual(custom_ids, [3])

    def test_unchecking_suggested_theme_removes_it_from_selected_existing_state(self):
        merged = merge_suggested_and_custom_theme_ids([], [3, 7], [7, 9], {3, 7, 9})
        self.assertEqual(merged, [3])

    def test_suggestion_checkboxes_sync_from_multiselect_selection(self):
        synced = sync_suggested_theme_checkbox_state([3, 7], [7, 9])
        self.assertEqual(synced, {7: True, 9: False})

    def test_possible_new_theme_prefills_input_when_present(self):
        value, state = prepare_possible_new_theme_prefill(None, "Optical Interconnects", None)
        self.assertEqual(value, "Optical Interconnects")
        self.assertEqual(state["auto_value"], "Optical Interconnects")
        self.assertFalse(bool(state["user_edited"]))

    def test_no_possible_new_theme_leaves_input_blank(self):
        value, state = prepare_possible_new_theme_prefill(None, None, None)
        self.assertEqual(value, "")
        self.assertEqual(state["auto_value"], "")
        self.assertFalse(bool(state["user_edited"]))

    def test_manual_possible_new_theme_edit_persists_across_reruns(self):
        value, state = prepare_possible_new_theme_prefill(None, "Optical Interconnects", None)
        self.assertEqual(value, "Optical Interconnects")
        state = finalize_possible_new_theme_state("Custom Theme", state)
        rerun_value, rerun_state = prepare_possible_new_theme_prefill("Custom Theme", "Data Center Optics", state)
        self.assertEqual(rerun_value, "Custom Theme")
        self.assertTrue(bool(rerun_state["user_edited"]))

    def test_regenerate_updates_possible_new_theme_if_field_is_still_untouched(self):
        value, state = prepare_possible_new_theme_prefill(None, "Optical Interconnects", None)
        self.assertEqual(value, "Optical Interconnects")
        state = finalize_possible_new_theme_state("Optical Interconnects", state)
        rerun_value, rerun_state = prepare_possible_new_theme_prefill("Optical Interconnects", "Data Center Optics", state)
        self.assertEqual(rerun_value, "Data Center Optics")
        self.assertEqual(rerun_state["auto_value"], "Data Center Optics")
        self.assertFalse(bool(rerun_state["user_edited"]))

    def test_checking_first_generated_theme_idea_adds_it(self):
        value, state = prepare_possible_new_theme_prefill(None, "Optical Interconnects", None)
        self.assertEqual(value, "Optical Interconnects")
        selected_value, selected_state = apply_generated_theme_idea_checkbox_selection(
            "",
            ["Data Center Optics"],
            ["Data Center Optics", "Optical Interconnects"],
            state,
        )
        finalized_state = finalize_possible_new_theme_state(selected_value, selected_state)

        self.assertEqual(selected_value, "Data Center Optics")
        self.assertTrue(bool(finalized_state["user_edited"]))
        self.assertTrue(bool(finalized_state["forced_user_edited"]))
        self.assertEqual(split_possible_new_theme_ideas(selected_value), ["Data Center Optics"])

    def test_checking_second_generated_theme_idea_adds_it(self):
        selected_value, selected_state = apply_generated_theme_idea_checkbox_selection(
            "Data Center Optics",
            ["Data Center Optics", "Optical Interconnects"],
            ["Data Center Optics", "Optical Interconnects"],
            {"auto_value": "Optical Interconnects", "user_edited": True},
        )

        self.assertEqual(
            split_possible_new_theme_ideas(selected_value),
            ["Data Center Optics", "Optical Interconnects"],
        )
        self.assertEqual(
            join_possible_new_theme_ideas(split_possible_new_theme_ideas(selected_value)),
            "Data Center Optics, Optical Interconnects",
        )
        self.assertTrue(bool(selected_state["user_edited"]))

    def test_unchecking_generated_theme_idea_removes_it(self):
        selected_value, selected_state = apply_generated_theme_idea_checkbox_selection(
            "Data Center Optics, Optical Interconnects",
            ["Optical Interconnects"],
            ["Data Center Optics", "Optical Interconnects"],
            {"auto_value": "Optical Interconnects", "user_edited": True},
        )

        self.assertEqual(split_possible_new_theme_ideas(selected_value), ["Optical Interconnects"])
        self.assertTrue(bool(selected_state["user_edited"]))

    def test_generated_theme_menu_add_updates_canonical_proposed_new_theme_value(self):
        updated_value, updated_state = reconcile_possible_new_theme_from_generated_checkbox_state(
            "",
            ["Data Center Optics", "Optical Interconnects"],
            {"Data Center Optics": True, "Optical Interconnects": False},
            {},
        )

        self.assertEqual(updated_value, "Data Center Optics")
        self.assertTrue(bool(updated_state["user_edited"]))
        self.assertTrue(bool(updated_state["forced_user_edited"]))

    def test_generated_theme_menu_add_is_visible_in_input_value_after_rerun(self):
        updated_value, updated_state = reconcile_possible_new_theme_from_generated_checkbox_state(
            "",
            ["Data Center Optics", "Optical Interconnects"],
            {"Data Center Optics": True, "Optical Interconnects": False},
            {},
        )

        rerun_value, rerun_state = prepare_possible_new_theme_prefill(
            updated_value,
            "Optical Interconnects",
            updated_state,
        )

        self.assertEqual(rerun_value, "Data Center Optics")
        self.assertTrue(bool(rerun_state["user_edited"]))

    def test_manual_text_and_generated_theme_menu_merge_without_duplicates(self):
        updated_value, _ = reconcile_possible_new_theme_from_generated_checkbox_state(
            "Custom Theme, Data Center Optics",
            ["Data Center Optics", "Optical Interconnects"],
            {"Data Center Optics": True, "Optical Interconnects": True},
            {"auto_value": "Optical Interconnects", "user_edited": True},
        )

        self.assertEqual(
            split_possible_new_theme_ideas(updated_value),
            ["Data Center Optics", "Optical Interconnects", "Custom Theme"],
        )

    def test_generated_theme_menu_remove_updates_same_canonical_value(self):
        updated_value, _ = reconcile_possible_new_theme_from_generated_checkbox_state(
            "Custom Theme, Data Center Optics, Optical Interconnects",
            ["Data Center Optics", "Optical Interconnects"],
            {"Data Center Optics": False, "Optical Interconnects": True},
            {"auto_value": "Optical Interconnects", "user_edited": True},
        )

        self.assertEqual(
            split_possible_new_theme_ideas(updated_value),
            ["Optical Interconnects", "Custom Theme"],
        )

    def test_generated_theme_idea_checkbox_state_syncs_from_existing_input_on_rerun(self):
        synced = sync_generated_theme_idea_checkbox_state(
            "Data Center Optics, Custom Theme",
            ["Data Center Optics", "Optical Interconnects"],
        )

        self.assertEqual(
            synced,
            {
                "Data Center Optics": True,
                "Optical Interconnects": False,
            },
        )

    def test_generated_theme_idea_checkbox_merge_preserves_custom_manual_items(self):
        merged = merge_generated_theme_ideas_with_custom(
            "Custom Theme, Data Center Optics",
            ["Optical Interconnects"],
            ["Data Center Optics", "Optical Interconnects"],
        )

        self.assertEqual(merged, "Optical Interconnects, Custom Theme")

    def test_regenerate_still_respects_explicit_generated_theme_checkbox_selections(self):
        value, state = prepare_possible_new_theme_prefill(None, "Optical Interconnects", None)
        self.assertEqual(value, "Optical Interconnects")
        selected_value, selected_state = apply_generated_theme_idea_checkbox_selection(
            "Optical Interconnects",
            ["Optical Interconnects", "Data Center Optics"],
            ["Data Center Optics", "Optical Interconnects"],
            state,
        )
        finalized_state = finalize_possible_new_theme_state(selected_value, selected_state)
        rerun_value, rerun_state = prepare_possible_new_theme_prefill(selected_value, "Semiconductor Substrates", finalized_state)

        self.assertEqual(rerun_value, "Data Center Optics, Optical Interconnects")
        self.assertTrue(bool(rerun_state["user_edited"]))

    def test_possible_new_theme_category_prefills_input_when_present(self):
        value, state = prepare_possible_new_theme_category_prefill(None, "Optical Networking", None)
        self.assertEqual(value, "Optical Networking")
        self.assertEqual(state["auto_value"], "Optical Networking")
        self.assertFalse(bool(state["user_edited"]))

    def test_manual_possible_new_theme_category_edit_persists_across_reruns(self):
        value, state = prepare_possible_new_theme_category_prefill(None, "Optical Networking", None)
        self.assertEqual(value, "Optical Networking")
        state = finalize_possible_new_theme_category_state("Communications Infrastructure", state)
        rerun_value, rerun_state = prepare_possible_new_theme_category_prefill(
            "Communications Infrastructure",
            "Financial Technology",
            state,
        )
        self.assertEqual(rerun_value, "Communications Infrastructure")
        self.assertTrue(bool(rerun_state["user_edited"]))

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

    def test_symbol_hygiene_queue_still_renders_when_outlier_calculation_fails(self):
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

        with patch("src.symbol_hygiene._calculation_outlier_candidates", side_effect=duckdb.InternalException("Attempted to dereference unique_ptr that is NULL!")):
            out = symbol_hygiene_queue(conn, limit=50)

        self.assertEqual(out.iloc[0]["ticker"], "XYZ")
        self.assertTrue(out.attrs.get("warnings"))
        self.assertIn("temporarily unavailable", str(out.attrs["warnings"][0]))
        conn.close()

    def test_symbol_hygiene_queue_falls_back_when_isolated_read_path_fails(self):
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

        @contextmanager
        def failing_read_conn():
            raise duckdb.InternalException("isolated read failed")
            yield conn

        queue = symbol_hygiene_queue(conn, limit=20, outlier_read_conn_factory=failing_read_conn)

        self.assertEqual(queue.iloc[0]["ticker"], "BBIG")
        self.assertEqual(queue.iloc[0]["suggested_status"], "refresh_suppressed")
        self.assertIn("falling back to the shared connection", " ".join(queue.attrs.get("warnings", [])))
        self.assertIn("Extreme", str(queue.iloc[0]["outlier_reason"]))
        conn.close()

    def test_symbol_hygiene_queue_isolated_read_path_does_not_regress_outlier_behavior(self):
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

        @contextmanager
        def isolated_read_conn():
            try:
                yield conn
            finally:
                pass

        queue = symbol_hygiene_queue(conn, limit=20, outlier_read_conn_factory=isolated_read_conn)

        self.assertEqual(queue.iloc[0]["ticker"], "BBIG")
        self.assertEqual(queue.iloc[0]["last_failure_category"], "CALC_OUTLIER")
        self.assertIn("Meme Stocks", str(queue.iloc[0]["current_theme_names"]))
        self.assertIn("Extreme", str(queue.iloc[0]["outlier_reason"]))
        self.assertFalse(queue.attrs.get("warnings"))
        conn.close()

    def test_live_health_page_source_explicitly_wires_symbol_hygiene_queue_to_fresh_read_path(self):
        health_page_source = Path("pages/4_Health.py").read_text()

        self.assertIn(
            "symbol_hygiene_queue(conn, limit=250, outlier_read_conn_factory=get_fresh_read_conn)",
            health_page_source,
        )

    def test_live_symbol_hygiene_queue_path_has_no_df_materialization_in_queue_or_outlier_helpers(self):
        import src.symbol_hygiene as symbol_hygiene_module

        outlier_source = inspect.getsource(symbol_hygiene_module._calculation_outlier_candidates)
        base_queue_source = inspect.getsource(symbol_hygiene_module._base_symbol_hygiene_queue)

        self.assertNotIn(".df(", outlier_source)
        self.assertNotIn(".df(", base_queue_source)

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


class TestDescriptionFirstResearchRefinements(unittest.TestCase):
    @staticmethod
    def _candidate(**overrides):
        candidate = {
            "ticker": "TEST",
            "recommendation": "review for addition",
            "recommendation_reason": "description-first audit",
            "persistence_score": 3.2,
            "observed_days": 7,
            "observations_last_10d": 4,
            "current_streak": 2,
        }
        candidate.update(overrides)
        return candidate

    @staticmethod
    def _theme(theme_id: int, name: str, category: str, description: str, tickers: list[str] | None = None):
        return {
            "theme_id": theme_id,
            "theme_name": name,
            "category": category,
            "theme_description": description,
            "representative_tickers": tickers or [],
        }

    def test_description_first_prefers_direct_governed_theme_over_new_theme(self):
        profile = {
            "company_name": "Photon Fabric",
            "description": "Designs optical interconnect systems and co-packaged optics for hyperscale data-center networking.",
            "sic_description": "Networking equipment",
        }
        catalog = [
            self._theme(
                1,
                "Optical Interconnects",
                "Networking",
                "Suppliers of optical interconnect components and systems used in data-center and AI networking.",
                ["CIEN"],
            ),
            self._theme(
                2,
                "Digital Payments",
                "Fintech",
                "Platforms and networks that process digital payments and merchant transactions.",
                ["SQ"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "add_to_existing_theme_review")
        self.assertIsNone(draft["possible_new_theme"])
        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Optical Interconnects")
        self.assertIn("Direct business-role fit", draft["suggested_existing_themes"][0]["why_it_might_fit"])
        self.assertEqual(
            draft["validation_debug"]["possible_new_theme_decision"]["status"],
            "suppressed_by_direct_governed_match",
        )
        self.assertTrue(draft["validation_debug"]["evaluated_matches"][0]["actionable"])

    def test_description_first_keeps_new_theme_tentative_when_existing_match_is_only_adjacent(self):
        profile = {
            "company_name": "Affirmed Credit",
            "description": "Provides installment lending and buy-now-pay-later financing through a consumer finance platform.",
            "sic_description": "Consumer lending",
        }
        catalog = [
            self._theme(
                1,
                "Digital Payments",
                "Fintech",
                "Payment processing networks, merchant acceptance, and digital wallet infrastructure.",
                ["PYPL"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Consumer Lending")
        self.assertTrue(draft["suggested_existing_themes"])
        self.assertEqual(draft["suggested_existing_themes"][0]["fit_label"], "adjacent_fit")
        self.assertIn("adjacent rather than direct", " ".join(draft["caveats"]))
        self.assertEqual(
            draft["validation_debug"]["possible_new_theme_decision"]["status"],
            "kept_tentative",
        )

    def test_description_first_filters_obviously_unrelated_governed_matches(self):
        profile = {
            "company_name": "Photon Fabric",
            "description": "Designs optical interconnect systems and co-packaged optics for hyperscale data-center networking.",
            "sic_description": "Networking equipment",
        }
        catalog = [
            self._theme(
                1,
                "Digital Payments",
                "Fintech",
                "Payment processing networks, merchant acceptance, and digital wallet infrastructure.",
                ["PYPL"],
            ),
            self._theme(
                2,
                "Consumer Lending",
                "Fintech",
                "Platforms focused on installment loans, personal lending, and credit underwriting.",
                ["AFRM"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["suggested_existing_themes"], [])
        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Optical Interconnects")
        self.assertTrue(draft["validation_debug"]["evaluated_matches"])
        self.assertFalse(any(item["actionable"] for item in draft["validation_debug"]["evaluated_matches"]))

    def test_description_first_does_not_promote_generic_business_model_language_to_direct_fit(self):
        profile = {
            "company_name": "Workflow Grid",
            "description": "Provides a mission-critical analytics platform and infrastructure software for enterprise workflow integration and observability.",
            "sic_description": "Enterprise software",
        }
        catalog = [
            self._theme(
                1,
                "Cloud Software",
                "Software",
                "Cloud software platforms, observability tooling, and enterprise workflow applications.",
                ["DDOG"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertNotEqual(draft["recommended_action"], "add_to_existing_theme_review")
        self.assertFalse(
            any(item.get("fit_label") == "direct_fit" for item in draft.get("suggested_existing_themes") or [])
        )
        self.assertFalse(bool(draft["validation_debug"].get("strong_role_evidence")))

    def test_description_first_generates_drone_phrase_ideas_even_when_anchors_are_weak(self):
        profile = {
            "company_name": "Aero Parts Co",
            "description": "Engaged in serving the American drone industry by building and selling drone components.",
            "sic_description": "Electronic components",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["domain_anchor"], "unclear")
        self.assertEqual(draft["dominant_business_role"], "unclear")
        self.assertIn("Drone Components", draft["candidate_theme_ideas"])
        self.assertIn("Drones", draft["candidate_theme_ideas"])

    def test_description_first_component_bucket_outranks_broader_product_bucket(self):
        profile = {
            "company_name": "Rotor Parts",
            "description": "Builds UAV modules and drone components for unmanned aircraft systems manufacturers.",
            "sic_description": "Electronic components",
        }
        catalog = [
            self._theme(
                1,
                "Drones",
                "Autonomous Systems",
                "Drone manufacturers, UAV product platforms, and unmanned aircraft systems.",
                ["AVAV"],
            ),
            self._theme(
                2,
                "Drone Components",
                "Autonomous Systems",
                "Suppliers of drone components, UAV modules, and unmanned aircraft parts.",
                ["KTOS"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["candidate_theme_ideas"][0], "Drone Components")
        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Drone Components")

    def test_description_first_sales_language_variant_collapses_to_canonical_component_label(self):
        profile = {
            "company_name": "Aero Parts Co",
            "description": "Engaged in serving the American drone industry by building and selling drone components.",
            "sic_description": "Electronic components",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertIn("Drone Components", draft["candidate_theme_ideas"])
        self.assertNotIn("Selling Drone Components", draft["candidate_theme_ideas"])
        self.assertEqual(draft["candidate_theme_ideas"][0], "Drone Components")

    def test_description_first_plain_component_maker_suppresses_broad_ai_adjacency(self):
        profile = {
            "company_name": "Rotor Parts",
            "description": "Builds UAV modules and drone components for unmanned aircraft systems manufacturers.",
            "sic_description": "Electronic components",
        }
        catalog = [
            self._theme(
                1,
                "AI - Robotics",
                "AI",
                "Artificial intelligence robotics platforms, autonomous software, and factory robotics systems.",
                ["SYM1"],
            ),
            self._theme(
                2,
                "AI - Edge Computing",
                "AI",
                "Edge AI infrastructure, distributed inference, and intelligent edge systems.",
                ["SYM2"],
            ),
            self._theme(
                3,
                "AI - Semiconductors",
                "AI",
                "AI semiconductor platforms, accelerator chips, and compute silicon.",
                ["SYM3"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertFalse(draft["suggested_existing_themes"])
        self.assertEqual(draft["possible_new_theme"], "Drone Components")

    def test_description_first_generic_adjacent_equipment_themes_do_not_survive_without_direct_support(self):
        profile = {
            "company_name": "Rotor Parts",
            "description": "Builds UAV modules and drone components for unmanned aircraft systems manufacturers.",
            "sic_description": "Electronic components",
        }
        catalog = [
            self._theme(
                1,
                "Industrial Equipment",
                "Industrial",
                "Industrial equipment, machinery systems, and factory hardware.",
                ["SYM4"],
            ),
            self._theme(
                2,
                "Robotics Equipment",
                "Industrial",
                "Robotics equipment, automation hardware, and industrial machines.",
                ["SYM5"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertFalse(draft["suggested_existing_themes"])
        self.assertEqual(draft["possible_new_theme"], "Drone Components")

    def test_description_first_phrase_fallback_survives_weak_anchor_inference(self):
        profile = {
            "company_name": "UAS Fabrication",
            "description": "Builds UAV components and unmanned systems equipment for domestic industrial customers.",
            "sic_description": "Industrial components",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertTrue(draft["candidate_theme_ideas"])
        self.assertTrue(
            {"UAV Components", "Unmanned Systems", "Unmanned Systems Equipment"}
            & set(draft["candidate_theme_ideas"])
        )

    def test_description_first_module_phrase_generates_component_style_idea(self):
        profile = {
            "company_name": "Airframe Modules",
            "description": "Builds UAV modules for unmanned aircraft systems integrators.",
            "sic_description": "Industrial modules",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertIn("UAV Components", draft["candidate_theme_ideas"])

    def test_description_first_extracts_generic_product_phrases_when_anchors_are_weak(self):
        profile = {
            "company_name": "WaterWorks Fabrication",
            "description": "Builds desalination modules and water treatment equipment for industrial plants.",
            "sic_description": "Industrial equipment",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertTrue(
            {"Desalination Components", "Water Treatment Equipment"}
            & set(draft["candidate_theme_ideas"])
        )

    def test_description_first_ignores_generic_business_model_product_phrases(self):
        profile = {
            "company_name": "Workflow Core",
            "description": "Provides mission critical systems and workflow platforms for enterprise operations.",
            "sic_description": "Enterprise software",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertNotIn("Mission Critical Systems", draft["candidate_theme_ideas"])
        self.assertNotIn("Workflow Systems", draft["candidate_theme_ideas"])

    def test_description_first_does_not_treat_generic_fintech_platform_as_direct_fit(self):
        profile = {
            "company_name": "Broad Finance Platform",
            "description": "Operates a fintech platform for consumers and merchants.",
            "sic_description": "Financial technology platform",
        }
        catalog = [
            self._theme(
                1,
                "Digital Payments",
                "Fintech",
                "Payment processing networks, merchant acceptance, and digital wallet infrastructure.",
                ["PYPL"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertFalse(
            any(item.get("fit_label") == "direct_fit" for item in draft.get("suggested_existing_themes") or [])
        )
        self.assertNotEqual(draft["recommended_action"], "add_to_existing_theme_review")

    def test_description_first_optical_component_descriptors_beat_ai_and_5g_adjacency(self):
        profile = {
            "company_name": "Photon Link",
            "description": "Designs optical modules, optical interconnect engines, and fiber-optic networking products for hyperscale networking.",
            "sic_description": "Networking components",
        }
        catalog = [
            self._theme(
                1,
                "AI Infrastructure",
                "Compute",
                "Suppliers of AI data-center compute infrastructure, hyperscale servers, and GPU cluster systems.",
                ["NVDA"],
            ),
            self._theme(
                2,
                "5G Infrastructure",
                "Telecom",
                "5G radios, carrier deployments, telecom infrastructure, and broadband networking rollouts.",
                ["ERIC"],
            ),
            self._theme(
                3,
                "Optical Interconnects",
                "Networking",
                "Optical modules, interconnect engines, and fiber networking components for high-speed data links.",
                ["CIEN"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Optical Interconnects")
        self.assertEqual(draft["suggested_existing_themes"][0]["fit_label"], "direct_fit")
        self.assertIn("Optical Interconnects", draft["validation_debug"]["business_descriptors"])

    def test_description_first_optical_platform_with_process_language_does_not_imply_materials(self):
        profile = {
            "company_name": "Interposer Optics",
            "description": "Develops optical interposers and photonics platforms using advanced semiconductor packaging and wafer-level manufacturing techniques.",
            "sic_description": "Optical networking equipment",
        }
        catalog = [
            self._theme(
                1,
                "Semiconductor Materials",
                "Materials",
                "Makers of semiconductor materials, wafers, substrates, compounds, coatings, and consumable inputs.",
                ["ENTG"],
            ),
            self._theme(
                2,
                "Optical Interconnects",
                "Networking",
                "Optical interposers, photonic interconnect platforms, and fiber networking modules.",
                ["COHR"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Optical Interconnects")
        self.assertFalse(any(item["theme_name"] == "Semiconductor Materials" for item in draft["suggested_existing_themes"]))

    def test_description_first_digital_asset_infrastructure_prefers_missing_category_over_cloud_or_cyber(self):
        profile = {
            "company_name": "Atlas Exchange Infra",
            "description": "Provides digital asset exchange infrastructure, institutional crypto custody, and market infrastructure software for trading venues.",
            "sic_description": "Financial software",
        }
        catalog = [
            self._theme(
                1,
                "Cybersecurity",
                "Software",
                "Cybersecurity platforms, identity security, and enterprise threat management.",
                ["PANW"],
            ),
            self._theme(
                2,
                "Cloud Software",
                "Software",
                "Cloud software platforms, workflow tooling, and enterprise observability applications.",
                ["DDOG"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Digital Asset Market Infrastructure")
        self.assertFalse(draft["suggested_existing_themes"])
        self.assertIn("Digital Asset Market Infrastructure", draft["validation_debug"]["business_descriptors"])

    def test_description_first_stablecoin_infrastructure_beats_generic_payments_as_primary_descriptor(self):
        profile = {
            "company_name": "Ledger Dollar Network",
            "description": "Builds stablecoin infrastructure and tokenized dollar settlement networks for enterprise treasury and cross-border payment flows.",
            "sic_description": "Payments infrastructure",
        }
        catalog = [
            self._theme(1, "Digital Payments", "Fintech", "Digital payment platforms, merchant checkout, and transaction processing.", ["PYPL"]),
            self._theme(2, "Fintech Payments", "Fintech", "Fintech payment applications and merchant transaction software.", ["SQ"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Stablecoins / Digital Assets Infrastructure")
        self.assertEqual(draft["candidate_theme_ideas"][0], "Stablecoins / Digital Assets Infrastructure")
        self.assertNotIn("Digital Payments", draft["candidate_theme_ideas"][:2])

    def test_description_first_blockchain_payment_rails_suppress_cloud_devops_drift(self):
        profile = {
            "company_name": "Chain Settlement Rail",
            "description": "Provides blockchain payments rails and digital asset settlement infrastructure for enterprises moving funds across on-chain payment networks.",
            "sic_description": "Blockchain payments infrastructure",
        }
        catalog = [
            self._theme(1, "Cloud Software", "Software", "Cloud software platforms, workflow tooling, and enterprise observability applications.", ["DDOG"]),
            self._theme(2, "Cloud DevOps", "Software", "Cloud-native DevOps, monitoring, and observability platforms.", ["DDOG"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIn(draft["possible_new_theme"], {"Blockchain Payments", "Crypto Payments Infrastructure"})
        self.assertFalse(draft["suggested_existing_themes"])
        self.assertNotIn("Cloud Software", draft["candidate_theme_ideas"])

    def test_description_first_stablecoin_infrastructure_beats_fintech_banking_adjacency(self):
        profile = {
            "company_name": "Reserve Rail",
            "description": "Operates stablecoin infrastructure and tokenized dollar rails that connect exchanges, wallets, and enterprise settlement counterparties.",
            "sic_description": "Digital asset infrastructure",
        }
        catalog = [
            self._theme(1, "Digital Banking", "Fintech", "Digital banking apps, neobanks, and consumer account services.", ["SOFI"]),
            self._theme(2, "Consumer Fintech", "Fintech", "Consumer financial services platforms, neobanks, and personal finance apps.", ["NU"]),
            self._theme(3, "Fintech Payments", "Fintech", "Merchant payments, checkout software, and fintech transaction platforms.", ["SQ"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Stablecoins / Digital Assets Infrastructure")
        self.assertFalse(draft["suggested_existing_themes"])

    def test_description_first_digital_asset_infrastructure_beats_weak_generic_fintech_and_software_when_governed_coverage_is_weak(self):
        profile = {
            "company_name": "Token Rail Systems",
            "description": "Provides digital asset financial infrastructure, blockchain payments connectivity, and settlement infrastructure for tokenized dollar transfers.",
            "sic_description": "Financial infrastructure software",
        }
        catalog = [
            self._theme(1, "Digital Payments", "Fintech", "Digital payment platforms and merchant transaction processing.", ["PYPL"]),
            self._theme(2, "Cloud Software", "Software", "Cloud workflow software and observability tooling for enterprises.", ["DDOG"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIn(draft["possible_new_theme"], {"Stablecoins / Digital Assets Infrastructure", "Blockchain Payments", "Crypto Payments Infrastructure"})
        self.assertFalse(draft["suggested_existing_themes"])
        self.assertTrue(
            {"Stablecoins / Digital Assets Infrastructure", "Blockchain Payments", "Crypto Payments Infrastructure"}
            & set(draft["validation_debug"]["business_descriptors"])
        )

    def test_description_first_upstream_ep_beats_lng_and_energy_transition_drift(self):
        profile = {
            "company_name": "Frontier Basin Energy",
            "description": "Acquires, explores, develops, and produces oil and natural gas assets with operated interests, reserves, wells, and acreage in onshore basins.",
            "sic_description": "Oil and gas exploration and production",
        }
        catalog = [
            self._theme(1, "LNG", "Energy", "Liquefied natural gas export infrastructure and LNG terminals.", ["LNG"]),
            self._theme(2, "Energy Transition", "Energy", "Energy transition platforms across decarbonization, renewables, and clean energy.", ["NEE"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Oil & Gas Exploration & Production")
        self.assertFalse(draft["suggested_existing_themes"])

    def test_description_first_acquisition_exploration_development_and_production_surfaces_upstream_missing_category(self):
        profile = {
            "company_name": "Onshore Resource Partners",
            "description": "Focuses on the acquisition, exploration, development, and production of oil and gas properties, operated interests, and working interests across onshore assets.",
            "sic_description": "Oil and gas properties",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Oil & Gas Exploration & Production")
        self.assertEqual(draft["possible_new_theme_category"], "Oil & Gas / Upstream")

    def test_description_first_unsupported_regional_buckets_do_not_survive_without_explicit_support(self):
        profile = {
            "company_name": "General Upstream Energy",
            "description": "Explores, develops, and produces oil and natural gas reserves from onshore acreage, operated wells, and producing properties.",
            "sic_description": "Upstream oil and gas",
        }
        catalog = [
            self._theme(1, "Permian Basin", "Energy", "Permian Basin producers and acreage operators.", ["FANG"]),
            self._theme(2, "Appalachia Gas", "Energy", "Appalachia basin natural gas producers and gathering assets.", ["EQT"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertFalse(draft["suggested_existing_themes"])
        self.assertEqual(draft["possible_new_theme"], "Oil & Gas Exploration & Production")

    def test_description_first_natural_gas_can_be_secondary_without_replacing_broader_upstream_ep(self):
        profile = {
            "company_name": "Heritage Upstream",
            "description": "Acquires and develops oil and natural gas assets and produces from operated wells, reserves, and working interests across onshore properties.",
            "sic_description": "Oil and gas production",
        }
        catalog = [
            self._theme(1, "Natural Gas", "Energy", "Natural gas production and gas-focused upstream operators.", ["EQT"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["possible_new_theme"], "Oil & Gas Exploration & Production")
        self.assertFalse(any(item.get("theme_name") == "Natural Gas" and item.get("fit_label") == "direct_fit" for item in draft["suggested_existing_themes"]))

    def test_description_first_consumer_banking_app_surfaces_missing_fintech_category(self):
        profile = {
            "company_name": "PocketBank",
            "description": "Offers a consumer banking app with overdraft protection, credit building, short-term liquidity, and financial management tools.",
            "sic_description": "Consumer financial services",
        }
        catalog = []

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIn(draft["possible_new_theme"], {"Consumer Fintech", "Digital Banking"})
        self.assertTrue({"Consumer Fintech", "Digital Banking"} & set(draft["candidate_theme_ideas"]))

    def test_description_first_memory_and_storage_suppresses_materials_and_robotics_adjacency(self):
        profile = {
            "company_name": "FlashCore",
            "description": "Designs NAND flash memory semiconductors, storage controllers, and SSD storage devices for enterprise systems.",
            "sic_description": "Memory and storage products",
        }
        catalog = [
            self._theme(
                1,
                "Semiconductor Materials",
                "Materials",
                "Makers of semiconductor wafers, chemicals, substrates, and consumables.",
                ["ENTG"],
            ),
            self._theme(
                2,
                "Industrial Robotics",
                "Automation",
                "Industrial robotics, factory automation, and autonomous manufacturing systems.",
                ["ROK"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIn(draft["possible_new_theme"], {"Memory & Storage", "Semiconductor Memory", "Data Storage"})
        self.assertFalse(draft["suggested_existing_themes"])

    def test_description_first_sdr_and_autonomous_systems_suppress_cloud_clusters(self):
        profile = {
            "company_name": "Spectrum Autonomous",
            "description": "Builds software-defined radio systems and autonomous systems hardware for wireless communications infrastructure and unmanned platforms.",
            "sic_description": "Wireless communications equipment",
        }
        catalog = [
            self._theme(1, "Cloud Computing", "Software", "Cloud computing platforms and enterprise infrastructure software.", ["MSFT"]),
            self._theme(2, "Cloud Infrastructure", "Software", "Cloud infrastructure, observability, and DevOps tooling.", ["AMZN"]),
            self._theme(3, "Cloud Software", "Software", "Cloud software applications and workflow analytics.", ["DDOG"]),
            self._theme(4, "Cloud DevOps", "Software", "Cloud-native DevOps, monitoring, and observability platforms.", ["DDOG"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertFalse(draft["suggested_existing_themes"])
        self.assertTrue({"Software-Defined Radio", "Autonomous Systems", "Wireless Communications Infrastructure"} & set(draft["candidate_theme_ideas"]))

    def test_description_first_generic_manufacturing_language_alone_does_not_unlock_materials(self):
        profile = {
            "company_name": "Process Optics",
            "description": "Uses proprietary semiconductor manufacturing processes, packaging techniques, and precision fabrication to build optical networking engines.",
            "sic_description": "Optical engines",
        }
        catalog = [
            self._theme(
                1,
                "Semiconductor Materials",
                "Materials",
                "Suppliers of semiconductor wafers, substrates, coatings, compounds, and process chemicals.",
                ["ENTG"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertNotIn("Semiconductor Materials", draft["candidate_theme_ideas"])
        self.assertFalse(draft["suggested_existing_themes"])

    def test_description_first_damps_same_family_weak_umbrella_clusters(self):
        profile = {
            "company_name": "Spectrum Autonomous",
            "description": "Builds software-defined radio systems and autonomous systems hardware for wireless communications infrastructure and unmanned platforms.",
            "sic_description": "Wireless communications equipment",
        }
        catalog = [
            self._theme(1, "Cloud Computing", "Software", "Cloud computing platforms and enterprise infrastructure software.", ["MSFT"]),
            self._theme(2, "Cloud Infrastructure", "Software", "Cloud infrastructure, observability, and DevOps tooling.", ["AMZN"]),
            self._theme(3, "Cloud Software", "Software", "Cloud software applications and workflow analytics.", ["DDOG"]),
            self._theme(4, "Cybersecurity", "Software", "Cybersecurity platforms, threat analytics, and security tooling.", ["PANW"]),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)
        evaluated = draft["validation_debug"]["evaluated_matches"]
        cloud_actionable = [item for item in evaluated if item["actionable"] and "Cloud" in item["theme_name"]]

        self.assertLessEqual(len(cloud_actionable), 1)

    def test_description_first_clear_descriptor_surfaces_missing_category_instead_of_unclear(self):
        profile = {
            "company_name": "Retail Water Systems",
            "description": "Builds water treatment equipment and desalination modules for municipal and industrial customers.",
            "sic_description": "Water equipment",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertTrue(draft["possible_new_theme"])
        self.assertTrue(draft["validation_debug"]["business_descriptors"])
        self.assertEqual(draft["validation_debug"]["possible_new_theme_decision"]["status"], "selected_no_actionable_governed_match")

    def test_description_first_does_not_award_direct_fit_for_broad_end_market_adjacency(self):
        profile = {
            "company_name": "Photon Link",
            "description": "Designs optical modules and fiber-optic networking products for hyperscale data-center networking.",
            "sic_description": "Networking components",
        }
        catalog = [
            self._theme(
                1,
                "AI Infrastructure",
                "Compute",
                "AI data-center servers, GPU systems, and hyperscale compute infrastructure.",
                ["NVDA"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertFalse(any(item.get("fit_label") == "direct_fit" for item in draft.get("suggested_existing_themes") or []))
        self.assertEqual(draft["possible_new_theme"], "Optical Interconnects")

    def test_description_first_mining_processing_and_shipping_does_not_return_domain_unclear(self):
        profile = {
            "company_name": "Frontier Tungsten",
            "description": "Advances mine assets, mineral processing, and shipping of tungsten concentrate from its tungsten project portfolio.",
            "sic_description": "Mining projects",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertNotEqual(draft["domain_anchor"], "unclear")
        self.assertEqual(draft["domain_anchor"], "mining/resources")
        self.assertTrue(draft["possible_new_theme"])

    def test_description_first_extracts_commodity_specific_mining_descriptor(self):
        profile = {
            "company_name": "Frontier Tungsten",
            "description": "Develops tungsten mine projects and processes tungsten ore into concentrate for shipment.",
            "sic_description": "Mining and processing",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertIn("Tungsten Mining", draft["candidate_theme_ideas"])
        self.assertIn("Tungsten Mining", draft["validation_debug"]["business_descriptors"])

    def test_description_first_mining_process_language_does_not_collapse_into_generic_materials(self):
        profile = {
            "company_name": "Frontier Tungsten",
            "description": "Owns tungsten mine assets, mineral processing facilities, and ships tungsten concentrate from operating projects.",
            "sic_description": "Mining operations",
        }
        catalog = [
            self._theme(
                1,
                "Semiconductor Materials",
                "Materials",
                "Makers of semiconductor wafers, substrates, coatings, compounds, and process chemicals.",
                ["ENTG"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertNotIn("Semiconductor Materials", draft["candidate_theme_ideas"])
        self.assertFalse(draft["suggested_existing_themes"])

    def test_description_first_clear_mining_descriptor_beats_domain_unclear_when_governed_coverage_is_weak(self):
        profile = {
            "company_name": "Frontier Tungsten",
            "description": "Operates mine projects, mineral processing, and concentrate shipping tied to tungsten ore production.",
            "sic_description": "Metals mining",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIn(draft["possible_new_theme"], {"Tungsten Mining", "Critical Minerals Mining", "Metals & Mining"})
        self.assertEqual(draft["validation_debug"]["possible_new_theme_decision"]["status"], "selected_no_actionable_governed_match")

    def test_description_first_emits_proposed_category_for_clear_missing_theme_case(self):
        profile = {
            "company_name": "PocketBank",
            "description": "Offers a consumer banking app with overdraft protection, credit building, short-term liquidity, and financial management tools.",
            "sic_description": "Consumer financial services",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme_category"], "Financial Technology")

    def test_description_first_additive_manufacturing_surfaces_coherent_missing_category(self):
        profile = {
            "company_name": "LayerForge",
            "description": "Builds additive manufacturing systems and industrial 3D printers for production-scale metal part fabrication.",
            "sic_description": "Industrial printer systems",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertEqual(draft["possible_new_theme"], "Additive Manufacturing")
        self.assertEqual(draft["possible_new_theme_category"], "Additive Manufacturing / Industrial 3D Printing")
        self.assertIn("Industrial 3D Printing", draft["validation_debug"]["business_descriptors"])

    def test_description_first_named_printer_system_does_not_stay_product_name_only(self):
        profile = {
            "company_name": "Forge Systems",
            "description": "Markets the Falcon production system and Atlas printer systems for additive manufacturing and industrial 3D printing customers.",
            "sic_description": "Manufacturing systems",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["possible_new_theme"], "Additive Manufacturing")
        self.assertFalse(str(draft["possible_new_theme"]).startswith("Falcon"))
        self.assertFalse(str(draft["possible_new_theme"]).startswith("Atlas"))
        self.assertIn("Industrial Manufacturing Systems", draft["validation_debug"]["business_descriptors"])

    def test_description_first_industrial_manufacturing_suppresses_geography_and_luxury_drift(self):
        profile = {
            "company_name": "PowderPrint",
            "description": "Provides additive manufacturing software and industrial printer systems used in factory production workflows.",
            "sic_description": "Industrial manufacturing software",
        }
        catalog = [
            self._theme(
                1,
                "European Luxury",
                "Geography",
                "European luxury brands, premium fashion houses, and high-end consumer goods.",
                ["LVMUY"],
            ),
            self._theme(
                2,
                "Asia Luxury",
                "Geography",
                "Asian luxury demand, premium retail, and upscale consumer brands.",
                ["N/A"],
            ),
        ]

        draft = _description_theme_generation_draft(self._candidate(), catalog, profile)

        self.assertFalse(draft["suggested_existing_themes"])
        self.assertEqual(draft["possible_new_theme_category"], "Additive Manufacturing / Industrial 3D Printing")
        self.assertTrue(all("Luxury" not in item["theme_name"] for item in draft["validation_debug"]["evaluated_matches"] if item["actionable"]))

    def test_description_first_additive_manufacturing_descriptor_beats_domain_unclear_when_coverage_is_weak(self):
        profile = {
            "company_name": "PrintFlow",
            "description": "Develops manufacturing software and printer control systems for industrial 3D printers and additive manufacturing factories.",
            "sic_description": "Manufacturing software",
        }

        draft = _description_theme_generation_draft(self._candidate(), [], profile)

        self.assertEqual(draft["domain_anchor"], "industrial manufacturing/additive")
        self.assertEqual(draft["possible_new_theme"], "Additive Manufacturing")
        self.assertEqual(draft["validation_debug"]["possible_new_theme_decision"]["status"], "selected_no_actionable_governed_match")


class TestScannerResearchReviewPersistence(unittest.TestCase):
    def test_save_scanner_research_review_inserts_and_updates_same_draft_context(self):
        conn = duckdb.connect(":memory:")
        draft = {
            "ticker": "NVDA",
            "generated_at": "2026-03-17 12:00:00",
            "theme_generation_strategy": "description_theme_generation",
            "research_mode": "heuristic_fallback",
            "recommended_action": "watch_only",
            "confidence": "low",
            "possible_new_theme": "Optical Interconnects",
            "domain_anchor": "networking/communications",
            "dominant_business_role": "component_supplier",
            "candidate_theme_ideas": ["Optical Interconnects", "Optical Networking"],
            "suggested_existing_themes": [
                {"theme_id": 7, "theme_name": "Optical Networking", "fit_label": "adjacent_fit"}
            ],
        }

        inserted = save_scanner_research_review(
            conn,
            "NVDA",
            draft,
            outcome_class="false_positive",
            reviewer_notes="Too broad for this role.",
        )
        updated = save_scanner_research_review(
            conn,
            "NVDA",
            draft,
            outcome_class="should_have_been_tentative",
            reviewer_notes="Existing theme should stay secondary.",
        )

        stored = get_scanner_research_review(conn, "NVDA", draft)
        count = conn.execute("select count(*) from scanner_research_reviews").fetchone()[0]

        self.assertEqual(count, 1)
        self.assertEqual(inserted["review_id"], updated["review_id"])
        self.assertEqual(stored["outcome_class"], "should_have_been_tentative")
        self.assertEqual(stored["reviewer_notes"], "Existing theme should stay secondary.")

    def test_scanner_research_review_summary_returns_counts_and_recent_rows(self):
        conn = duckdb.connect(":memory:")
        first_draft = {
            "ticker": "NVDA",
            "generated_at": "2026-03-17 12:00:00",
            "theme_generation_strategy": "description_theme_generation",
            "research_mode": "heuristic_fallback",
            "recommended_action": "watch_only",
            "confidence": "low",
        }
        second_draft = {
            "ticker": "PLTR",
            "generated_at": "2026-03-17 12:05:00",
            "theme_generation_strategy": "description_theme_generation",
            "research_mode": "openai",
            "recommended_action": "add_to_existing_theme_review",
            "confidence": "medium",
        }

        save_scanner_research_review(conn, "NVDA", first_draft, outcome_class="false_positive", reviewer_notes="Too generic.")
        save_scanner_research_review(conn, "PLTR", second_draft, outcome_class="direct_fit_correct", reviewer_notes="")

        summary = scanner_research_review_summary(conn, limit=5)

        self.assertEqual(summary["counts_by_outcome"]["false_positive"], 1)
        self.assertEqual(summary["counts_by_outcome"]["direct_fit_correct"], 1)
        self.assertEqual(len(summary["recent_reviews"]), 2)
        self.assertEqual(summary["recent_reviews"][0]["ticker"], "PLTR")

    def test_streamlit_cached_connection_keeps_research_review_queries_usable_across_reruns(self):
        db_path = Path(__file__).resolve().parent / f"_tmp_research_review_cache_{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}.duckdb"
        get_db_connection.clear()
        try:
            with patch("src.database.DB_PATH", db_path), patch("src.config.DB_PATH", db_path), patch("src.database._has_streamlit_script_run_context", return_value=True):
                from src.database import init_db

                init_db()
                draft = {
                    "ticker": "NVDA",
                    "generated_at": "2026-03-17 12:00:00",
                    "theme_generation_strategy": "description_theme_generation",
                    "research_mode": "heuristic_fallback",
                    "recommended_action": "watch_only",
                    "confidence": "low",
                }
                with get_conn() as conn:
                    save_scanner_research_review(conn, "NVDA", draft, outcome_class="false_positive", reviewer_notes="First pass")
                with get_conn() as conn:
                    first = get_scanner_research_review(conn, "NVDA", draft)
                with get_conn() as conn:
                    second = get_scanner_research_review(conn, "NVDA", draft)

                self.assertIsNotNone(first)
                self.assertEqual(first["review_id"], second["review_id"])
        finally:
            get_db_connection.clear()


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

    def test_queue_page_source_exposes_single_row_selection_and_obsolete_action(self):
        page_source = Path(__file__).resolve().parents[1] / "pages" / "3_Suggestions.py"
        content = page_source.read_text(encoding="utf-8")

        self.assertIn("Selected suggestion", content)
        self.assertIn("Mark obsolete", content)
        self.assertIn("Apply approved", content)
        self.assertIn("Send preserved proposed theme to Theme Review", content)
        self.assertIn("there is nothing governed to apply to membership yet", content)
        self.assertIn("Create governed theme and assign ticker", content)
        self.assertIn("clear_scanner_candidate_summary_cache()", content)

    def test_update_suggestion_status_supports_obsolete_from_selected_row_path(self):
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
            ) values (1, 'review_theme', 'rejected', 'manual', '', 'medium')
            """
        )

        changed = update_suggestion_status(conn, 1, "obsolete", "superseded")
        stored = conn.execute("select status, reviewer_notes from theme_suggestions where suggestion_id = 1").fetchone()

        self.assertTrue(bool(changed["changed"]))
        self.assertEqual(changed["new_status"], "obsolete")
        self.assertEqual(stored, ("obsolete", "superseded"))
        conn.close()

    def test_follow_up_action_is_not_available_without_preserved_proposed_new_theme(self):
        row_without_preserved_theme = {
            "source": "scanner_audit",
            "suggestion_type": "review_theme",
            "status": "applied",
            "custom_new_theme_names": "",
            "proposed_theme_name": "",
        }
        row_with_preserved_theme = {
            "source": "scanner_audit",
            "suggestion_type": "review_theme",
            "status": "applied",
            "custom_new_theme_names": "Data Center Optics",
        }

        self.assertFalse(can_follow_up_applied_scanner_audit_review_row(row_without_preserved_theme))
        self.assertTrue(can_follow_up_applied_scanner_audit_review_row(row_with_preserved_theme))

    def test_can_apply_queue_suggestion_row_requires_existing_governed_themes_for_review_theme(self):
        approved_governed_row = {
            "status": "approved",
            "suggestion_type": "review_theme",
            "selected_existing_theme_names": "AI - Infrastructure",
            "custom_new_theme_names": "Data Center Optics",
        }
        approved_new_theme_only_row = {
            "status": "approved",
            "suggestion_type": "review_theme",
            "selected_existing_theme_names": "",
            "custom_new_theme_names": "Data Center Optics",
        }

        self.assertTrue(can_apply_queue_suggestion_row(approved_governed_row))
        self.assertFalse(can_apply_queue_suggestion_row(approved_new_theme_only_row))

    def test_fast_path_action_requires_approved_review_theme_with_proposed_theme_and_ticker(self):
        eligible_row = {
            "status": "approved",
            "suggestion_type": "review_theme",
            "proposed_ticker": "AAOI",
            "custom_new_theme_names": "Data Center Optics",
        }
        missing_ticker = {
            "status": "approved",
            "suggestion_type": "review_theme",
            "custom_new_theme_names": "Data Center Optics",
        }
        pending_row = {
            "status": "pending",
            "suggestion_type": "review_theme",
            "proposed_ticker": "AAOI",
            "custom_new_theme_names": "Data Center Optics",
        }

        self.assertTrue(can_fast_path_create_governed_theme_row(eligible_row))
        self.assertFalse(can_fast_path_create_governed_theme_row(missing_ticker))
        self.assertFalse(can_fast_path_create_governed_theme_row(pending_row))


class TestScannerAuditProposedNewThemePersistence(unittest.TestCase):
    @staticmethod
    def _candidate_summary_df(ticker: str = "AAOI") -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "source_labels": "scanner_audit",
                    "recommendation": "review for addition",
                    "recommendation_reason": "description-first audit",
                    "persistence_score": 3,
                    "observed_days": 7,
                    "observations_last_5d": 3,
                    "observations_last_10d": 4,
                    "current_streak": 2,
                    "distinct_scanner_count": 1,
                    "first_seen": "2026-03-10",
                    "last_seen": "2026-03-17",
                    "scanners": "Growth",
                    "metadata_basis": "test",
                }
            ]
        )

    @staticmethod
    def _setup_scanner_audit_tables(conn):
        conn.execute("create sequence if not exists suggestion_id_seq")
        conn.execute("create sequence if not exists themes_id_seq")
        conn.execute("create table themes(id bigint primary key, name varchar, category varchar, is_active boolean)")
        conn.execute("create table theme_membership(theme_id bigint, ticker varchar, created_at timestamp default current_timestamp, primary key(theme_id, ticker))")
        conn.execute(
            """
            create table scanner_hit_history(
                normalized_ticker varchar,
                observed_date date,
                scanner_name varchar,
                source_label varchar,
                scanner_name_inferred boolean,
                scanner_name_basis varchar,
                observed_date_inferred boolean,
                observed_date_basis varchar
            )
            """
        )
        conn.execute(
            """
            create table scanner_candidate_review_state(
                normalized_ticker varchar primary key,
                review_state varchar,
                review_note varchar,
                updated_at timestamp
            )
            """
        )
        conn.execute(
            """
            create table theme_suggestions(
                suggestion_id bigint primary key default nextval('suggestion_id_seq'),
                suggestion_type varchar not null,
                status varchar not null default 'pending',
                created_at timestamp not null default current_timestamp,
                reviewed_at timestamp,
                source varchar not null,
                rationale varchar,
                proposed_theme_name varchar,
                proposed_theme_category varchar,
                proposed_ticker varchar,
                existing_theme_id bigint,
                proposed_target_theme_id bigint,
                reviewer_notes varchar,
                priority varchar not null default 'medium',
                source_context_json varchar,
                source_updated_at timestamp
            )
            """
        )
        conn.execute(
            """
            create table governed_ticker_onboarding(
                ticker varchar primary key,
                added_at timestamp not null default current_timestamp,
                onboarding_source varchar not null,
                history_readiness_status varchar not null default 'unknown',
                backfill_status varchar not null default 'not_needed',
                last_backfill_attempt_at timestamp,
                last_backfill_error varchar,
                downstream_refresh_needed boolean not null default false,
                history_row_count bigint not null default 0,
                history_target_days bigint not null default 30,
                history_market_data_source varchar,
                history_latest_trading_date date,
                updated_at timestamp not null default current_timestamp
            )
            """
        )

    @staticmethod
    def _insert_scanner_hit_history(conn, ticker: str = "AAOI") -> None:
        conn.execute(
            """
            insert into scanner_hit_history(
                normalized_ticker, observed_date, scanner_name, source_label,
                scanner_name_inferred, scanner_name_basis, observed_date_inferred, observed_date_basis
            ) values
                (?, '2026-03-15', 'Growth', 'scanner_audit', false, 'file_column', false, 'file_column'),
                (?, '2026-03-16', 'Growth', 'scanner_audit', false, 'file_column', false, 'file_column'),
                (?, '2026-03-17', 'Growth', 'scanner_audit', false, 'file_column', false, 'file_column')
            """,
            [ticker, ticker, ticker],
        )

    def test_manual_proposed_new_theme_persists_through_send_to_theme_review(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        row = conn.execute(
            "select proposed_theme_name, proposed_theme_category, source_context_json from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()
        queue = list_suggestions(conn, status="pending")

        self.assertEqual(row[0], "Data Center Optics")
        self.assertEqual(row[1], "Optical Networking")
        self.assertIn("Data Center Optics", str(row[2]))
        self.assertEqual(str(queue.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(queue.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
        conn.close()

    def test_promotion_succeeds_with_only_a_proposed_new_theme(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics"},
                custom_new_themes=["Data Center Optics"],
            )

        row = conn.execute(
            "select proposed_theme_name, proposed_theme_category from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()

        self.assertEqual(row, ("Data Center Optics", None))
        conn.close()

    def test_promotion_succeeds_with_proposed_new_theme_and_category_without_existing_theme(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        queue = list_suggestions(conn, status="pending")

        self.assertEqual(str(queue.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(queue.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
        self.assertFalse(str(queue.iloc[0]["selected_existing_theme_names"] or "").strip())
        conn.close()

    def test_send_to_theme_review_with_proposed_additions_surfaces_same_saved_values_in_pending_and_approved_views(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        saved_row = conn.execute(
            "select proposed_theme_name, proposed_theme_category from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()
        pending = list_suggestions(conn, status="pending")

        review_suggestion(conn, int(result["suggestion_id"]), "approved", "approve staged review")
        approved = list_suggestions(conn, status="approved")

        self.assertEqual(saved_row, ("Data Center Optics", "Optical Networking"))
        self.assertEqual(str(pending.iloc[0]["selected_existing_theme_names"]), "AI - Infrastructure")
        self.assertEqual(str(pending.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(pending.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
        self.assertEqual(str(approved.iloc[0]["selected_existing_theme_names"]), "AI - Infrastructure")
        self.assertEqual(str(approved.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(approved.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
        conn.close()

    def test_selected_queue_row_details_surface_saved_proposed_theme_fields(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        queue = list_suggestions(conn, status="pending", suggestion_type="review_theme", source="scanner_audit")
        selected_row = queue.iloc[0]

        self.assertEqual(str(selected_row["proposed_ticker"]), "AAOI")
        self.assertEqual(str(selected_row["source"]), "scanner_audit")
        self.assertEqual(str(selected_row["suggestion_type"]), "review_theme")
        self.assertEqual(str(selected_row["selected_existing_theme_names"]), "AI - Infrastructure")
        self.assertEqual(str(selected_row["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(selected_row["proposed_new_theme_category"]), "Optical Networking")
        conn.close()

    def test_approved_new_theme_only_row_does_not_allow_apply_approved(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )
        review_suggestion(conn, int(result["suggestion_id"]), "approved", "approve new theme only")
        approved_queue = list_suggestions(conn, status="approved", suggestion_type="review_theme", source="scanner_audit")
        approved_row = approved_queue.iloc[0]

        self.assertFalse(can_apply_queue_suggestion_row(approved_row))
        self.assertFalse(str(approved_row["selected_existing_theme_names"] or "").strip())
        self.assertEqual(str(approved_row["custom_new_theme_names"]), "Data Center Optics")
        conn.close()

    def test_approved_governed_review_row_still_allows_apply_approved(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
            )
        review_suggestion(conn, int(result["suggestion_id"]), "approved", "approve governed review")
        approved_queue = list_suggestions(conn, status="approved", suggestion_type="review_theme", source="scanner_audit")
        approved_row = approved_queue.iloc[0]

        self.assertTrue(can_apply_queue_suggestion_row(approved_row))
        self.assertEqual(str(approved_row["selected_existing_theme_names"]), "AI - Infrastructure")
        conn.close()

    def test_fast_path_can_create_governed_theme_and_assign_ticker_in_one_action(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )
        review_suggestion(conn, int(result["suggestion_id"]), "approved", "reviewed and approved")
        fast_path = fast_path_create_governed_theme_and_assign_ticker(conn, int(result["suggestion_id"]), "fast path")

        themes = conn.execute("select name, category from themes").fetchall()
        membership = conn.execute("select ticker from theme_membership where theme_id = ?", [int(fast_path["theme_id"])]).fetchall()
        suggestion_row = conn.execute(
            "select status, source_context_json from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()

        self.assertIn(("Data Center Optics", "Optical Networking"), themes)
        self.assertEqual(membership, [("AAOI",)])
        self.assertEqual(suggestion_row[0], "applied")
        self.assertIn('"applied_via_fast_path": true', str(suggestion_row[1]).lower())
        self.assertIn('"created_governed_theme_name": "Data Center Optics"', str(suggestion_row[1]))
        conn.close()

    def test_fast_path_applied_ticker_no_longer_appears_uncovered_in_scanner_audit_coverage_logic(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        self._insert_scanner_hit_history(conn, "AXTI")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AXTI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AXTI",
                research_draft={"ticker": "AXTI", "possible_new_theme": "Compound Semiconductor Materials", "possible_new_theme_category": "Semiconductor Materials"},
                custom_new_themes=["Compound Semiconductor Materials"],
                proposed_new_theme_category="Semiconductor Materials",
            )
        review_suggestion(conn, int(result["suggestion_id"]), "approved", "reviewed and approved")
        fast_path_create_governed_theme_and_assign_ticker(conn, int(result["suggestion_id"]), "fast path")

        summary = scanner_candidate_summary(conn)
        row = summary[summary["ticker"] == "AXTI"].iloc[0]
        uncovered = summary[summary["governed_status"] == "uncovered"]

        self.assertEqual(str(row["governed_status"]), "already governed")
        self.assertEqual(int(row["active_theme_count"]), 1)
        self.assertEqual(str(row["current_theme_names"]), "Compound Semiconductor Materials")
        self.assertNotIn("AXTI", uncovered["ticker"].tolist())
        conn.close()

    def test_direct_apply_coverage_refresh_source_of_truth_does_not_regress(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        self._insert_scanner_hit_history(conn, "AAOI")
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")), patch(
            "src.fetch_data.run_targeted_current_snapshot_hydration",
            return_value={"refreshed": ["AAOI"]},
        ):
            result = apply_scanner_candidate_selected_themes(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        summary = scanner_candidate_summary(conn)
        row = summary[summary["ticker"] == "AAOI"].iloc[0]

        self.assertEqual(int(result["suggestion_id"]) > 0, True)
        self.assertEqual(str(row["governed_status"]), "already governed")
        self.assertEqual(int(row["active_theme_count"]), 1)
        self.assertEqual(str(row["current_theme_names"]), "AI - Infrastructure")
        conn.close()

    def test_fast_path_reuses_existing_governed_theme_instead_of_creating_duplicate(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'Data Center Optics', 'Optical Networking', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )
        review_suggestion(conn, int(result["suggestion_id"]), "approved", "reviewed and approved")
        fast_path = fast_path_create_governed_theme_and_assign_ticker(conn, int(result["suggestion_id"]), "fast path")

        theme_rows = conn.execute("select id, name from themes where lower(name) = lower('Data Center Optics')").fetchall()
        membership = conn.execute("select theme_id, ticker from theme_membership where ticker = 'AAOI'").fetchall()

        self.assertEqual(len(theme_rows), 1)
        self.assertFalse(bool(fast_path["created_theme"]))
        self.assertEqual(membership, [(1, "AAOI")])
        conn.close()

    def test_fast_path_ticker_assignment_is_idempotent_and_not_duplicated(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )
        review_suggestion(conn, int(result["suggestion_id"]), "approved", "reviewed and approved")
        first = fast_path_create_governed_theme_and_assign_ticker(conn, int(result["suggestion_id"]), "fast path")

        conn.execute("update theme_suggestions set status = 'approved' where suggestion_id = ?", [int(result["suggestion_id"])])
        second = fast_path_create_governed_theme_and_assign_ticker(conn, int(result["suggestion_id"]), "fast path again")
        membership_count = conn.execute("select count(*) from theme_membership where ticker = 'AAOI'").fetchone()[0]

        self.assertTrue(bool(first["ticker_added_to_theme"]))
        self.assertFalse(bool(second["ticker_added_to_theme"]))
        self.assertEqual(int(membership_count), 1)
        conn.close()

    def test_fast_path_not_available_when_required_fields_are_missing(self):
        row_missing_theme = {
            "status": "approved",
            "suggestion_type": "review_theme",
            "proposed_ticker": "AAOI",
            "custom_new_theme_names": "",
            "proposed_theme_name": "",
        }
        row_missing_ticker = {
            "status": "approved",
            "suggestion_type": "review_theme",
            "custom_new_theme_names": "Data Center Optics",
            "proposed_ticker": "",
        }

        self.assertFalse(can_fast_path_create_governed_theme_row(row_missing_theme))
        self.assertFalse(can_fast_path_create_governed_theme_row(row_missing_ticker))

    def test_manual_proposed_new_theme_persists_when_existing_themes_are_also_selected(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        row = conn.execute(
            "select proposed_theme_name, proposed_theme_category, source_context_json from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()

        self.assertEqual(row[0], "Data Center Optics")
        self.assertEqual(row[1], "Optical Networking")
        self.assertIn("AI - Infrastructure", str(row[2]))
        self.assertIn("Data Center Optics", str(row[2]))
        conn.close()

    def test_promotion_succeeds_with_both_existing_theme_and_proposed_new_theme(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
            )

        queue = list_suggestions(conn, status="pending")

        self.assertEqual(int(result["suggestion_id"]), int(queue.iloc[0]["suggestion_id"]))
        self.assertEqual(str(queue.iloc[0]["selected_existing_theme_names"]), "AI - Infrastructure")
        self.assertEqual(str(queue.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        conn.close()

    def test_generated_checkbox_selected_proposed_new_theme_persists(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        updated_value, _ = apply_generated_theme_idea_checkbox_selection(
            "",
            ["Data Center Optics"],
            ["Data Center Optics", "Optical Networking"],
            {},
        )

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=split_possible_new_theme_ideas(updated_value),
                proposed_new_theme_category="Optical Networking",
            )

        row = conn.execute(
            "select proposed_theme_name, proposed_theme_category, source_context_json from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()

        self.assertEqual(row[0], "Data Center Optics")
        self.assertEqual(row[1], "Optical Networking")
        self.assertIn("Data Center Optics", str(row[2]))
        conn.close()

    def test_generated_theme_menu_add_remove_persists_canonical_proposed_new_theme_value(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        added_value, added_state = reconcile_possible_new_theme_from_generated_checkbox_state(
            "Custom Theme",
            ["Data Center Optics", "Optical Networking"],
            {"Data Center Optics": True, "Optical Networking": False},
            {},
        )
        updated_value, _ = reconcile_possible_new_theme_from_generated_checkbox_state(
            added_value,
            ["Data Center Optics", "Optical Networking"],
            {"Data Center Optics": False, "Optical Networking": True},
            added_state,
        )

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_new_themes=split_possible_new_theme_ideas(updated_value),
                proposed_new_theme_category="Optical Networking",
            )

        row = conn.execute(
            "select proposed_theme_name, proposed_theme_category, source_context_json from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()
        source_context = json.loads(str(row[2]))

        self.assertEqual(row[0], "Optical Networking, Custom Theme")
        self.assertEqual(row[1], "Optical Networking")
        self.assertEqual(source_context.get("custom_new_themes"), ["Optical Networking", "Custom Theme"])
        conn.close()

    def test_direct_apply_still_requires_an_existing_governed_theme(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            with self.assertRaisesRegex(ValueError, "Select at least one existing theme to apply now"):
                apply_scanner_candidate_selected_themes(
                    conn,
                    "AAOI",
                    research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics"},
                    custom_new_themes=["Data Center Optics"],
                    proposed_new_theme_category="Optical Networking",
                )
        conn.close()

    def test_canonical_proposed_new_theme_field_is_used_for_promotion_validation(self):
        selected_value, _ = reconcile_possible_new_theme_from_generated_checkbox_state(
            "Transient Theme",
            ["Data Center Optics", "Optical Networking"],
            {"Data Center Optics": True, "Optical Networking": False},
            {},
        )
        self.assertTrue(has_meaningful_theme_review_selection([], selected_value))
        self.assertFalse(has_meaningful_theme_review_selection([], ""))
        self.assertFalse(has_meaningful_theme_review_selection([], None))

    def test_direct_apply_preserves_proposed_new_theme_context_while_only_applying_existing_themes(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")), patch(
            "src.fetch_data.run_targeted_current_snapshot_hydration",
            return_value={"refreshed": ["AAOI"]},
        ):
            result = apply_scanner_candidate_selected_themes(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        membership = conn.execute("select theme_id, ticker from theme_membership").fetchall()
        applied_row = conn.execute(
            "select status, proposed_theme_name, proposed_theme_category, source_context_json from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()
        applied_queue = list_suggestions(conn, status="applied")
        recent = recent_applied_suggestions(conn, limit=5)

        self.assertEqual(membership, [(1, "AAOI")])
        self.assertEqual(applied_row[0], "applied")
        self.assertEqual(applied_row[1], "Data Center Optics")
        self.assertEqual(applied_row[2], "Optical Networking")
        self.assertIn("Data Center Optics", str(applied_row[3]))
        self.assertEqual(result["proposed_new_theme_names"], ["Data Center Optics"])
        self.assertEqual(result["proposed_new_theme_category"], "Optical Networking")
        self.assertEqual(str(applied_queue.iloc[0]["selected_existing_theme_names"]), "AI - Infrastructure")
        self.assertEqual(str(applied_queue.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(applied_queue.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
        self.assertEqual(str(recent.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(recent.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
        conn.close()

    def test_applied_scanner_audit_row_with_preserved_theme_can_spawn_pending_theme_review_candidate(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")), patch(
            "src.fetch_data.run_targeted_current_snapshot_hydration",
            return_value={"refreshed": ["AAOI"]},
        ):
            applied = apply_scanner_candidate_selected_themes(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )
            follow_up = send_preserved_applied_scanner_audit_theme_to_review(conn, int(applied["suggestion_id"]))

        applied_row = conn.execute(
            "select status from theme_suggestions where suggestion_id = ?",
            [int(applied["suggestion_id"])],
        ).fetchone()
        pending_row = conn.execute(
            "select status, proposed_theme_name, proposed_theme_category, source_context_json from theme_suggestions where suggestion_id = ?",
            [int(follow_up["suggestion_id"])],
        ).fetchone()

        self.assertEqual(applied_row[0], "applied")
        self.assertEqual(pending_row[0], "pending")
        self.assertEqual(pending_row[1], "Data Center Optics")
        self.assertEqual(pending_row[2], "Optical Networking")
        self.assertIn('"follow_up_source_suggestion_id": %d' % int(applied["suggestion_id"]), str(pending_row[3]))
        conn.close()

    def test_applied_scanner_audit_follow_up_preserves_category_and_refreshes_pending_candidate(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")), patch(
            "src.fetch_data.run_targeted_current_snapshot_hydration",
            return_value={"refreshed": ["AAOI"]},
        ):
            applied = apply_scanner_candidate_selected_themes(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )
            first_follow_up = send_preserved_applied_scanner_audit_theme_to_review(conn, int(applied["suggestion_id"]))
            second_follow_up = send_preserved_applied_scanner_audit_theme_to_review(conn, int(applied["suggestion_id"]))

        pending_rows = conn.execute(
            """
            select suggestion_id, status, proposed_theme_name, proposed_theme_category
            from theme_suggestions
            where proposed_ticker = 'AAOI' and status = 'pending'
            order by suggestion_id
            """
        ).fetchall()

        self.assertEqual(int(first_follow_up["suggestion_id"]), int(second_follow_up["suggestion_id"]))
        self.assertEqual(len(pending_rows), 1)
        self.assertEqual(pending_rows[0][2], "Data Center Optics")
        self.assertEqual(pending_rows[0][3], "Optical Networking")
        conn.close()

    def test_scanner_audit_review_theme_rows_can_be_acted_on_from_queue_status_path(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            result = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics"},
                custom_new_themes=["Data Center Optics"],
            )

        approved = review_suggestion(conn, int(result["suggestion_id"]), "approved", "queue approve")
        obsolete = update_suggestion_status(conn, int(result["suggestion_id"]), "obsolete", "queue obsolete")
        stored = conn.execute(
            "select status, reviewer_notes from theme_suggestions where suggestion_id = ?",
            [int(result["suggestion_id"])],
        ).fetchone()

        self.assertEqual(approved["new_status"], "approved")
        self.assertEqual(obsolete["new_status"], "obsolete")
        self.assertEqual(stored, ("obsolete", "queue obsolete"))
        conn.close()

    def test_queue_interaction_path_does_not_regress_filtered_queue_or_bulk_actions(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")):
            first = promote_scanner_candidate_to_theme_review(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics"},
                custom_new_themes=["Data Center Optics"],
            )
        second_df = self._candidate_summary_df("LITE")
        with patch("src.scanner_audit.scanner_candidate_summary", return_value=second_df):
            second = promote_scanner_candidate_to_theme_review(
                conn,
                "LITE",
                research_draft={"ticker": "LITE", "possible_new_theme": "Optical Interconnects"},
                custom_new_themes=["Optical Interconnects"],
            )

        scanner_queue = list_suggestions(conn, status="pending", suggestion_type="review_theme", source="scanner_audit")
        changed = bulk_update_filtered_status(
            conn,
            "rejected",
            "bulk queue action",
            "pending",
            "review_theme",
            "scanner_audit",
            "",
            ["pending", "approved"],
        )
        rejected_queue = list_suggestions(conn, status="rejected", suggestion_type="review_theme", source="scanner_audit")

        self.assertEqual({int(value) for value in scanner_queue["suggestion_id"].tolist()}, {int(first["suggestion_id"]), int(second["suggestion_id"])})
        self.assertEqual(changed, 2)
        self.assertEqual(set(rejected_queue["proposed_ticker"].tolist()), {"AAOI", "LITE"})
        conn.close()

    def test_direct_apply_with_multiple_existing_themes_preserves_existing_governed_theme_persistence(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")
        conn.execute("insert into themes values (2, 'Optical Components', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")), patch(
            "src.fetch_data.run_targeted_current_snapshot_hydration",
            return_value={"refreshed": ["AAOI"]},
        ):
            result = apply_scanner_candidate_selected_themes(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1, 2],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        membership = conn.execute(
            "select theme_id, ticker from theme_membership where ticker = 'AAOI' order by theme_id"
        ).fetchall()
        applied_queue = list_suggestions(conn, status="applied")
        row = applied_queue[applied_queue["suggestion_id"] == int(result["suggestion_id"])].iloc[0]

        self.assertEqual(membership, [(1, "AAOI"), (2, "AAOI")])
        self.assertEqual(
            set(str(row["selected_existing_theme_names"]).split(", ")),
            {"AI - Infrastructure", "Optical Components"},
        )
        self.assertEqual(str(row["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(row["proposed_new_theme_category"]), "Optical Networking")
        conn.close()

    def test_review_and_recent_views_fall_back_to_saved_row_values_when_structured_context_is_missing(self):
        conn = duckdb.connect(":memory:")
        self._setup_scanner_audit_tables(conn)
        conn.execute("insert into themes values (1, 'AI - Infrastructure', 'Technology', true)")

        with patch("src.scanner_audit.scanner_candidate_summary", return_value=self._candidate_summary_df("AAOI")), patch(
            "src.fetch_data.run_targeted_current_snapshot_hydration",
            return_value={"refreshed": ["AAOI"]},
        ):
            result = apply_scanner_candidate_selected_themes(
                conn,
                "AAOI",
                research_draft={"ticker": "AAOI", "possible_new_theme": "Data Center Optics", "possible_new_theme_category": "Optical Networking"},
                custom_existing_theme_ids=[1],
                custom_new_themes=["Data Center Optics"],
                proposed_new_theme_category="Optical Networking",
            )

        conn.execute(
            "update theme_suggestions set source_context_json = null where suggestion_id = ?",
            [int(result["suggestion_id"])],
        )
        applied_queue = list_suggestions(conn, status="applied")
        recent = recent_applied_suggestions(conn, limit=5)

        self.assertEqual(str(applied_queue.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(applied_queue.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
        self.assertEqual(str(recent.iloc[0]["custom_new_theme_names"]), "Data Center Optics")
        self.assertEqual(str(recent.iloc[0]["proposed_new_theme_category"]), "Optical Networking")
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
    def test_init_db_uses_fresh_bootstrap_connection_instead_of_shared_cached_connection(self):
        real_conn = duckdb.connect(":memory:")

        class PoisonedSharedConn:
            def execute(self, sql, params=None):
                raise duckdb.InvalidInputException("Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result")

        @contextmanager
        def fake_bootstrap_conn():
            try:
                yield real_conn
            finally:
                pass

        with patch("src.database.get_conn", side_effect=AssertionError("shared connection should not be used for init_db")), patch(
            "src.database.get_bootstrap_conn",
            fake_bootstrap_conn,
        ), patch("src.database.get_db_connection", return_value=PoisonedSharedConn()), patch(
            "src.theme_service.seed_if_needed",
            return_value=False,
        ):
            from src.database import init_db

            init_db()

        self.assertEqual(
            real_conn.execute("select count(*) from duckdb_tables() where table_name = 'themes'").fetchone()[0],
            1,
        )
        real_conn.close()

    def test_get_conn_recovers_from_invalid_cached_connection(self):
        healthy_conn = duckdb.connect(":memory:")

        class PoisonedSharedConn:
            def execute(self, sql, params=None):
                raise duckdb.InvalidInputException("Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result")

        poisoned_conn = PoisonedSharedConn()
        cached_calls = []

        def fake_cached_getter(_db_path):
            cached_calls.append("call")
            return poisoned_conn if len(cached_calls) == 1 else healthy_conn

        fake_cached_getter.clear = unittest.mock.Mock()

        with patch("src.database._has_streamlit_script_run_context", return_value=True), patch(
            "src.database.get_db_connection",
            fake_cached_getter,
        ):
            from src.database import get_conn

            with get_conn() as conn:
                self.assertIs(conn, healthy_conn)
                self.assertEqual(conn.execute("select 1").fetchone()[0], 1)

        self.assertEqual(len(cached_calls), 2)
        fake_cached_getter.clear.assert_called_once()
        healthy_conn.close()

    def test_suggestions_style_init_flow_recovers_after_poisoned_cached_connection(self):
        bootstrap_conn = duckdb.connect(":memory:")
        healthy_shared_conn = duckdb.connect(":memory:")

        class PoisonedSharedConn:
            def execute(self, sql, params=None):
                raise duckdb.InvalidInputException("Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result")

        @contextmanager
        def fake_bootstrap_conn():
            try:
                yield bootstrap_conn
            finally:
                pass

        cached_calls = []

        def fake_cached_getter(_db_path):
            cached_calls.append("call")
            return PoisonedSharedConn() if len(cached_calls) == 1 else healthy_shared_conn

        fake_cached_getter.clear = unittest.mock.Mock()

        with patch("src.database._has_streamlit_script_run_context", return_value=True), patch(
            "src.database.get_bootstrap_conn",
            fake_bootstrap_conn,
        ), patch("src.database.get_db_connection", fake_cached_getter), patch(
            "src.theme_service.seed_if_needed",
            return_value=False,
        ):
            from src.database import get_conn, init_db

            init_db()
            with get_conn() as conn:
                self.assertIs(conn, healthy_shared_conn)
                self.assertEqual(conn.execute("select 1").fetchone()[0], 1)

        fake_cached_getter.clear.assert_called_once()
        bootstrap_conn.close()
        healthy_shared_conn.close()

    def test_init_db_bootstrap_still_runs_expected_schema_and_seed_flow(self):
        real_conn = duckdb.connect(":memory:")

        @contextmanager
        def fake_bootstrap_conn():
            try:
                yield real_conn
            finally:
                pass

        with patch("src.database.get_bootstrap_conn", fake_bootstrap_conn), patch(
            "src.theme_service.seed_if_needed",
            return_value=False,
        ) as mock_seed:
            from src.database import init_db

            init_db()

        self.assertEqual(
            real_conn.execute("select count(*) from duckdb_tables() where table_name = 'theme_suggestions'").fetchone()[0],
            1,
        )
        mock_seed.assert_called_once()
        real_conn.close()

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

    def test_init_db_skips_noncritical_symbol_refresh_status_conflict(self):
        real_conn = duckdb.connect(":memory:")
        conflict_calls = {"count": 0}

        class WrappedConn:
            def __init__(self, inner):
                self.inner = inner

            def execute(self, sql, params=None):
                normalized_sql = " ".join(str(sql).split())
                if normalized_sql.startswith("UPDATE symbol_refresh_status SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"):
                    conflict_calls["count"] += 1
                    raise duckdb.TransactionException("TransactionContext Error: Conflict on update")
                if params is None:
                    return self.inner.execute(sql)
                return self.inner.execute(sql, params)

        @contextmanager
        def fake_bootstrap_conn():
            try:
                yield WrappedConn(real_conn)
            finally:
                pass

        with patch("src.database.get_bootstrap_conn", fake_bootstrap_conn), patch("src.theme_service.seed_if_needed", return_value=False):
            from src.database import init_db

            init_db()

        self.assertEqual(conflict_calls["count"], 1)
        self.assertEqual(
            real_conn.execute("select count(*) from duckdb_tables() where table_name = 'symbol_refresh_status'").fetchone()[0],
            1,
        )
        real_conn.close()

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
