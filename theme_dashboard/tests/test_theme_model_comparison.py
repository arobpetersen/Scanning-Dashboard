import unittest

import pandas as pd

from src.theme_model_comparison import (
    _confidence_factor,
    _winsorized_mean,
    build_theme_model_comparison,
)


class TestThemeModelComparison(unittest.TestCase):
    def test_confidence_factor_penalizes_small_themes(self):
        self.assertAlmostEqual(_confidence_factor(0), 0.0)
        self.assertAlmostEqual(_confidence_factor(2), 0.5)
        self.assertAlmostEqual(_confidence_factor(8), 1.0)
        self.assertAlmostEqual(_confidence_factor(20), 1.0)

    def test_winsorized_mean_reduces_outlier_impact(self):
        series = pd.Series([1.0, 1.0, 1.0, 1.0, 100.0])
        raw_mean = float(series.mean())
        winsorized = _winsorized_mean(series)

        self.assertLess(winsorized, raw_mean)
        self.assertGreaterEqual(winsorized, 1.0)

    def test_build_theme_model_comparison_deemphasizes_small_outlier_theme(self):
        raw = pd.DataFrame(
            [
                {"theme_id": 1, "theme": "Tiny Heat", "category": "Spec", "is_active": True, "ticker": "A", "perf_1w": 5.0, "perf_1m": 80.0, "perf_3m": 60.0},
                {"theme_id": 1, "theme": "Tiny Heat", "category": "Spec", "is_active": True, "ticker": "B", "perf_1w": -2.0, "perf_1m": -10.0, "perf_3m": -5.0},
                {"theme_id": 2, "theme": "Broad Strength", "category": "Quality", "is_active": True, "ticker": "C", "perf_1w": 4.0, "perf_1m": 16.0, "perf_3m": 14.0},
                {"theme_id": 2, "theme": "Broad Strength", "category": "Quality", "is_active": True, "ticker": "D", "perf_1w": 5.0, "perf_1m": 15.0, "perf_3m": 13.0},
                {"theme_id": 2, "theme": "Broad Strength", "category": "Quality", "is_active": True, "ticker": "E", "perf_1w": 4.0, "perf_1m": 14.0, "perf_3m": 12.0},
                {"theme_id": 2, "theme": "Broad Strength", "category": "Quality", "is_active": True, "ticker": "F", "perf_1w": 3.0, "perf_1m": 17.0, "perf_3m": 15.0},
            ]
        )

        compare = build_theme_model_comparison(raw)
        tiny = compare.loc[compare["theme"] == "Tiny Heat"].iloc[0]
        broad = compare.loc[compare["theme"] == "Broad Strength"].iloc[0]

        self.assertLess(float(tiny["confidence_factor"]), 1.0)
        self.assertGreater(float(tiny["baseline_score"]), float(broad["baseline_score"]))
        self.assertLess(float(tiny["confidence_adjusted_score"]), float(tiny["baseline_score"]))
        self.assertGreater(float(tiny["top_abs_share_1m_pct"]), float(broad["top_abs_share_1m_pct"]))


if __name__ == "__main__":
    unittest.main()
