import unittest

import pandas as pd

from src.ticker_lookup_ui import (
    compact_membership_theme_labels,
    compact_ticker_lookup_lines,
    normalize_ticker_lookup_input,
)


class TestTickerLookupUi(unittest.TestCase):
    def test_normalize_ticker_lookup_input_trims_and_uppercases(self):
        self.assertEqual(normalize_ticker_lookup_input(" nvda "), "NVDA")
        self.assertEqual(normalize_ticker_lookup_input(None), "")

    def test_compact_membership_theme_labels_sorts_and_disambiguates_duplicates(self):
        memberships = pd.DataFrame(
            [
                {"theme_id": 7, "theme_name": "AI", "category": "Software", "is_active": True},
                {"theme_id": 3, "theme_name": "AI", "category": "Hardware", "is_active": True},
                {"theme_id": 9, "theme_name": "AI", "category": "Hardware", "is_active": False},
            ]
        )

        self.assertEqual(
            compact_membership_theme_labels(memberships),
            ["AI (Hardware) [#3]", "AI (Hardware) [#9] inactive", "AI (Software)"],
        )

    def test_compact_ticker_lookup_lines_reports_assignments_with_context(self):
        summary = pd.DataFrame(
            [
                {
                    "lookup_status": "In DB and assigned",
                    "exists_in_ticker_snapshots": True,
                    "exists_in_refresh_run_tickers": False,
                    "exists_in_symbol_refresh_status": False,
                    "manually_suppressed": False,
                    "operationally_suppressed": False,
                    "preferred_perf_1w": 1.25,
                    "preferred_perf_1m": -2.5,
                    "preferred_snapshot_time": "2026-03-10 22:00:00",
                    "latest_snapshot_time": None,
                }
            ]
        )
        memberships = pd.DataFrame(
            [{"theme_id": 1, "theme_name": "AI", "category": "Tech", "is_active": True}]
        )

        lines = compact_ticker_lookup_lines(summary, memberships, " nvda ")

        self.assertEqual(lines[0], "**NVDA:** AI (Tech)")
        self.assertIn("1W +1.2%", lines[1])
        self.assertIn("1M -2.5%", lines[1])
        self.assertIn("In DB and assigned", lines[1])

    def test_compact_ticker_lookup_lines_distinguishes_unassigned_and_not_found(self):
        snapshots_only = pd.DataFrame(
            [
                {
                    "lookup_status": "Seen in snapshots only",
                    "exists_in_ticker_snapshots": True,
                    "exists_in_refresh_run_tickers": False,
                    "exists_in_symbol_refresh_status": False,
                    "manually_suppressed": False,
                    "operationally_suppressed": False,
                    "preferred_perf_1w": None,
                    "preferred_perf_1m": None,
                    "preferred_snapshot_time": None,
                    "latest_snapshot_time": None,
                }
            ]
        )
        not_found = pd.DataFrame(
            [
                {
                    "lookup_status": "Not found",
                    "exists_in_ticker_snapshots": False,
                    "exists_in_refresh_run_tickers": False,
                    "exists_in_symbol_refresh_status": False,
                    "manually_suppressed": False,
                    "operationally_suppressed": False,
                    "preferred_perf_1w": None,
                    "preferred_perf_1m": None,
                    "preferred_snapshot_time": None,
                    "latest_snapshot_time": None,
                }
            ]
        )

        self.assertEqual(
            compact_ticker_lookup_lines(snapshots_only, pd.DataFrame(), "PLTR")[0],
            "**PLTR:** no governed theme assignment.",
        )
        self.assertEqual(
            compact_ticker_lookup_lines(not_found, pd.DataFrame(), "ZZZZ")[0],
            "`ZZZZ` not found in governed membership or stored snapshots.",
        )


if __name__ == "__main__":
    unittest.main()
