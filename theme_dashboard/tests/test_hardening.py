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
from src.leaderboard_utils import build_window_leaderboard
from src.metric_formatting import format_theme_ticker_table, human_readable_number, short_timestamp
from src.queries import (
    latest_ticker_snapshots,
    ticker_lookup_memberships,
    ticker_lookup_summary,
    theme_health_overview,
    theme_history_last_n_snapshots,
    theme_history_window,
    theme_ticker_metrics,
    ticker_history_last_n_snapshots,
    top_theme_movers,
)
from src.symbol_hygiene import apply_refresh_failure, apply_refresh_success
from src.theme_service import seed_if_needed
from src.theme_service import set_ticker_theme_assignments
from src.provider_live import LiveProvider
from src.eod_refresh import has_eod_run_for_date, run_scheduled_eod_refresh


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
