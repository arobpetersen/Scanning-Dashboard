from pathlib import Path
import unittest


class AppDailySyncUiTests(unittest.TestCase):
    def test_apps_page_tracks_all_daily_sync_stages_in_running_status(self):
        content = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
        self.assertIn('DAILY_SYNC_STAGE_ORDER = ["live_refresh", "market_context", "historical_append", "canonical_materialization"]', content)
        self.assertIn('stage_cols = container.columns(len(DAILY_SYNC_STAGE_ORDER))', content)
        self.assertIn('stages": stages,', content)
        self.assertIn('st.markdown(f"**{DAILY_SYNC_STAGE_LABELS[stage_key]}**")', content)
        self.assertIn('Updated `', content)
        self.assertIn('Waiting for prior stages.', content)
        self.assertNotIn('st.progress(stage_fraction, text=f"{stage_completed}/{stage_total} tickers")', content)
        self.assertIn('container.progress(progress_value, text=f"{completed}/{total} tickers completed")', content)
        self.assertIn('st.caption(f"Live completion: `{stage_fraction * 100:.0f}%`")', content)
        self.assertIn('Historical append progress: stage-state only until row totals are finalized.', content)
        self.assertIn('Canonical progress: stage-state only until materialization finishes.', content)
        self.assertIn('Historical current for target day', content)
        self.assertIn('QQQ latest day', content)
        self.assertIn('Reused existing same-day state: `Yes`', content)
