import unittest
from unittest.mock import patch

import duckdb
import pandas as pd

from src.failure_classification import categorize_failure_message
from src.inflection_engine import compute_theme_inflections
from src.leaderboard_utils import build_window_leaderboard
from src.metric_formatting import format_theme_ticker_table, human_readable_number, short_timestamp
from src.queries import theme_history_window
from src.symbol_hygiene import apply_refresh_failure, apply_refresh_success
from src.provider_live import LiveProvider


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
                composite_score double
            )
            """
        )
        conn.execute("insert into themes values (1, 'A', 'Cat')")
        conn.execute("insert into themes values (2, 'B', 'Cat')")

        # Weekly cadence: latest and prior boundary should both be included for 7d window.
        for run_id, ts in [(1, "2026-03-01"), (2, "2026-03-08")]:
            conn.execute(
                "insert into theme_snapshots values (?, ?, 1, 10, 1, 1, 1, 50, 1)",
                [run_id, ts],
            )
            conn.execute(
                "insert into theme_snapshots values (?, ?, 2, 10, 1, 1, 1, 50, 1)",
                [run_id, ts],
            )

        out = theme_history_window(conn, 7)
        self.assertEqual(int(out["snapshot_time"].nunique()), 2)
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
        self.assertEqual(out.iloc[0]["dollar_volume"], "565.0M")
        self.assertEqual(float(out.iloc[0]["perf_1w"]), 1.23)
        self.assertTrue(str(out.iloc[0]["last_updated"]).startswith("Mar"))

    def test_live_calc_return_returns_none_when_history_insufficient(self):
        self.assertIsNone(LiveProvider._calc_return([1, 2, 3], 5))


if __name__ == "__main__":
    unittest.main()
