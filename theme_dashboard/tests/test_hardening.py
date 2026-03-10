import unittest
from unittest.mock import patch

import duckdb
import pandas as pd

from src.inflection_engine import compute_theme_inflections
from src.leaderboard_utils import build_window_leaderboard
from src.queries import theme_history_window


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


if __name__ == "__main__":
    unittest.main()
