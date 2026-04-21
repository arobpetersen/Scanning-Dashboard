from __future__ import annotations

from datetime import datetime
import unittest

from src.theme_sync_status import ranked_canonical_sync_status
from src.trading_day_status import EASTERN_TZ, finalization_eligible, latest_finalizable_trading_date


class ThemesSyncStatusTests(unittest.TestCase):
    def test_intraday_status_distinguishes_live_current_from_finalized_canonical(self):
        as_of_et = datetime(2026, 4, 21, 13, 0, tzinfo=EASTERN_TZ)
        label = ranked_canonical_sync_status(
            "2026-04-21 13:42:00",
            "2026-04-20",
            latest_finalizable_value="2026-04-20",
            as_of_et=as_of_et,
        )
        self.assertEqual(
            label,
            "Live current through 2026-04-21; canonical finalized through 2026-04-20",
        )

    def test_after_hours_status_reports_latest_trading_day_sync(self):
        as_of_et = datetime(2026, 4, 21, 16, 0, tzinfo=EASTERN_TZ)
        label = ranked_canonical_sync_status(
            "2026-04-21 16:00:00",
            "2026-04-21",
            latest_finalizable_value="2026-04-21",
            as_of_et=as_of_et,
        )
        self.assertEqual(label, "In sync with latest trading day")

    def test_intraday_does_not_report_in_sync_when_finalizable_and_live_inputs_differ(self):
        as_of_et = datetime(2026, 4, 21, 13, 42, tzinfo=EASTERN_TZ)
        label = ranked_canonical_sync_status(
            "2026-04-21 13:42:00",
            "2026-04-20",
            latest_finalizable_value="2026-04-20",
            as_of_et=as_of_et,
        )
        self.assertNotEqual(label, "In sync with latest trading day")

    def test_finalization_becomes_eligible_at_market_close(self):
        self.assertFalse(bool(finalization_eligible(datetime(2026, 4, 21, 15, 59, tzinfo=EASTERN_TZ))))
        self.assertTrue(bool(finalization_eligible(datetime(2026, 4, 21, 16, 0, tzinfo=EASTERN_TZ))))

    def test_latest_finalizable_trading_date_switches_at_market_close(self):
        self.assertEqual(
            str(latest_finalizable_trading_date(datetime(2026, 4, 21, 15, 59, tzinfo=EASTERN_TZ))),
            "2026-04-20",
        )
        self.assertEqual(
            str(latest_finalizable_trading_date(datetime(2026, 4, 21, 16, 0, tzinfo=EASTERN_TZ))),
            "2026-04-21",
        )
