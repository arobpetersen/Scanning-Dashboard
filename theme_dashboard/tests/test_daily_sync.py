import unittest
from datetime import UTC, datetime
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch

import duckdb

from src.database import SCHEMA_SQL
from src.eod_refresh import materialize_latest_canonical_day, refresh_run_stage_summary, run_latest_daily_sync


class TestLatestDailySync(unittest.TestCase):
    @patch("src.eod_refresh.refresh_run_stage_summary", return_value={
        "tickers_requested": 12,
        "tickers_updated": 10,
        "tickers_failed": 2,
        "tickers_suppressed": 1,
        "tickers_skipped": 3,
        "flagged_tickers": 1,
        "api_call_count": 12,
        "failed_tickers": ["AAA", "BBB"],
    })
    @patch("src.eod_refresh.latest_historical_append_date", side_effect=["2026-04-14", "2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.latest_canonical_snapshot_date")
    @patch("src.eod_refresh.latest_expected_trading_date")
    @patch("src.eod_refresh.materialize_latest_canonical_day")
    @patch("src.eod_refresh.run_scheduled_historical_append")
    @patch("src.eod_refresh.has_historical_append_for_date")
    @patch("src.eod_refresh.run_scheduled_eod_refresh")
    @patch("src.eod_refresh.has_eod_run_for_date")
    @patch("src.eod_refresh.is_trading_day", return_value=True)
    @patch("src.eod_refresh.current_et")
    def test_run_latest_daily_sync_runs_stages_in_order(
        self,
        mock_current_et,
        _mock_is_trading_day,
        mock_has_eod,
        mock_run_eod,
        mock_has_append,
        mock_run_append,
        mock_materialize,
        mock_latest_expected,
        mock_latest_canonical,
        _mock_latest_historical,
        mock_refresh_summary,
    ):
        event_order: list[str] = []
        mock_current_et.return_value = datetime(2026, 4, 15, 19, 0, tzinfo=UTC)
        mock_has_eod.side_effect = lambda *_args, **_kwargs: event_order.append("has_eod") or False
        mock_run_eod.side_effect = lambda *_args, **_kwargs: event_order.append("run_eod") or 101
        mock_has_append.side_effect = lambda *_args, **_kwargs: event_order.append("has_append") or False
        mock_run_append.side_effect = lambda *_args, **_kwargs: event_order.append("run_append") or {
            "status": "success",
            "snapshot_rows_written": 5,
            "ticker_history_rows_written": 10,
        }
        mock_materialize.side_effect = lambda *_args, **_kwargs: event_order.append("materialize") or {
            "status": "history_repaired",
            "latest_expected_trading_date": "2026-04-15",
            "latest_canonical_snapshot_date_before": "2026-04-14",
            "latest_canonical_snapshot_date_after": "2026-04-15",
            "advanced": True,
            "row_count": 10,
            "inserted_count": 10,
        }
        mock_latest_expected.side_effect = ["2026-04-14", "2026-04-15"]
        mock_latest_canonical.side_effect = ["2026-04-14", "2026-04-15"]

        result = run_latest_daily_sync(MagicMock(), provider_name="live")

        self.assertEqual(event_order, ["has_eod", "run_eod", "has_append", "run_append", "materialize"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["stages"]["live_refresh"]["status"], "refreshed")
        self.assertEqual(result["stages"]["live_refresh"]["refresh_run_summary"]["tickers_updated"], 10)
        self.assertEqual(result["stages"]["historical_append"]["status"], "success")
        self.assertTrue(bool(result["stages"]["historical_append"]["advanced"]))
        self.assertEqual(result["stages"]["canonical_materialization"]["status"], "history_repaired")
        mock_refresh_summary.assert_called_once()

    @patch("src.eod_refresh.refresh_run_stage_summary", return_value={
        "tickers_requested": 0,
        "tickers_updated": 0,
        "tickers_failed": 0,
        "tickers_suppressed": 0,
        "tickers_skipped": 0,
        "flagged_tickers": 0,
        "api_call_count": 0,
        "failed_tickers": [],
    })
    @patch("src.eod_refresh.latest_historical_append_date", side_effect=["2026-04-15", "2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.latest_canonical_snapshot_date", side_effect=["2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.latest_expected_trading_date", side_effect=["2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.materialize_latest_canonical_day", return_value={
        "status": "already_current",
        "latest_expected_trading_date": "2026-04-15",
        "latest_canonical_snapshot_date_before": "2026-04-15",
        "latest_canonical_snapshot_date_after": "2026-04-15",
        "advanced": False,
        "row_count": 10,
        "inserted_count": 0,
    })
    @patch("src.eod_refresh.has_finalized_historical_append_for_date", return_value=True)
    @patch("src.eod_refresh.has_historical_append_for_date", return_value=True)
    @patch("src.eod_refresh.has_eod_run_for_date", return_value=True)
    @patch("src.eod_refresh.is_trading_day", return_value=True)
    @patch("src.eod_refresh.current_et", return_value=datetime(2026, 4, 15, 19, 0, tzinfo=UTC))
    def test_run_latest_daily_sync_is_idempotent_when_all_stages_are_current(
        self,
        _mock_current_et,
        _mock_is_trading_day,
        _mock_has_eod,
        _mock_has_finalized_append,
        _mock_has_append,
        mock_materialize,
        _mock_latest_expected,
        _mock_latest_canonical,
        _mock_latest_historical,
        _mock_refresh_summary,
    ):
        result = run_latest_daily_sync(MagicMock(), provider_name="live")

        self.assertEqual(result["status"], "no_change")
        self.assertEqual(result["stages"]["live_refresh"]["status"], "already_current")
        self.assertEqual(result["stages"]["historical_append"]["status"], "already_current")
        self.assertFalse(bool(result["stages"]["historical_append"]["advanced"]))
        self.assertEqual(result["stages"]["canonical_materialization"]["status"], "already_current")
        mock_materialize.assert_called_once()

    @patch("src.eod_refresh.refresh_run_stage_summary", return_value={
        "tickers_requested": 12,
        "tickers_updated": 11,
        "tickers_failed": 1,
        "tickers_suppressed": 0,
        "tickers_skipped": 0,
        "flagged_tickers": 0,
        "api_call_count": 12,
        "failed_tickers": ["BBB"],
    })
    @patch("src.eod_refresh.latest_historical_append_date", side_effect=["2026-04-14", "2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.latest_canonical_snapshot_date", side_effect=["2026-04-14", "2026-04-15"])
    @patch("src.eod_refresh.latest_expected_trading_date", side_effect=["2026-04-14", "2026-04-15"])
    @patch("src.eod_refresh.materialize_latest_canonical_day")
    @patch("src.eod_refresh.run_scheduled_historical_append")
    @patch("src.eod_refresh.has_finalized_historical_append_for_date", return_value=False)
    @patch("src.eod_refresh.has_historical_append_for_date", return_value=False)
    @patch("src.eod_refresh.run_refresh", return_value=202)
    @patch("src.eod_refresh.active_ticker_universe", return_value=["AAA", "BBB"])
    @patch("src.eod_refresh.run_scheduled_eod_refresh")
    @patch("src.eod_refresh.has_eod_run_for_date")
    @patch("src.eod_refresh.is_trading_day", return_value=True)
    @patch("src.eod_refresh.current_et", return_value=datetime(2026, 4, 21, 0, 21, tzinfo=ZoneInfo("America/New_York")))
    def test_run_latest_daily_sync_finalizes_previous_trading_day_before_next_eod(
        self,
        _mock_current_et,
        _mock_is_trading_day,
        mock_has_eod,
        mock_run_eod,
        _mock_active_ticker_universe,
        mock_run_refresh,
        mock_has_append,
        mock_has_finalized_append,
        mock_run_append,
        mock_materialize,
        _mock_latest_expected,
        _mock_latest_canonical,
        _mock_latest_historical,
        _mock_refresh_summary,
    ):
        mock_run_append.return_value = {
            "status": "success",
            "snapshot_rows_written": 5,
            "ticker_history_rows_written": 10,
        }
        mock_materialize.return_value = {
            "status": "history_repaired",
            "latest_expected_trading_date": "2026-04-15",
            "latest_canonical_snapshot_date_before": "2026-04-14",
            "latest_canonical_snapshot_date_after": "2026-04-15",
            "advanced": True,
            "row_count": 10,
            "inserted_count": 10,
            "ranked_row_count": 10,
        }

        result = run_latest_daily_sync(MagicMock(), provider_name="live")

        self.assertEqual(result["status"], "success")
        self.assertFalse(bool(result["finalization_eligible"]))
        self.assertEqual(str(result["finalization_target_date"]), "2026-04-20")
        self.assertEqual(result["stages"]["live_refresh"]["status"], "refreshed")
        self.assertEqual(result["stages"]["live_refresh"]["scope_type"], "daily_sync_live")
        self.assertEqual(result["stages"]["historical_append"]["status"], "success")
        self.assertEqual(str(result["stages"]["historical_append"]["target_date"]), "2026-04-20")
        self.assertEqual(result["stages"]["canonical_materialization"]["status"], "history_repaired")
        mock_has_eod.assert_not_called()
        mock_run_eod.assert_not_called()
        mock_has_finalized_append.assert_called_once()
        mock_has_append.assert_called_once()
        self.assertEqual(mock_run_append.call_count, 1)
        self.assertEqual(str(mock_run_append.call_args.kwargs.get("target_date")), "2026-04-20")
        mock_materialize.assert_called_once()
        mock_run_refresh.assert_called_once()

    @patch("src.eod_refresh.refresh_run_stage_summary", return_value={
        "tickers_requested": 12,
        "tickers_updated": 12,
        "tickers_failed": 0,
        "tickers_suppressed": 0,
        "tickers_skipped": 0,
        "flagged_tickers": 0,
        "api_call_count": 12,
        "failed_tickers": [],
    })
    @patch("src.eod_refresh.latest_historical_append_date", side_effect=["2026-04-15", "2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.latest_canonical_snapshot_date", side_effect=["2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.latest_expected_trading_date", side_effect=["2026-04-15", "2026-04-15"])
    @patch("src.eod_refresh.materialize_latest_canonical_day", return_value={
        "status": "repaired_intraday_same_day",
        "latest_expected_trading_date": "2026-04-15",
        "latest_canonical_snapshot_date_before": "2026-04-15",
        "latest_canonical_snapshot_date_after": "2026-04-15",
        "advanced": False,
        "row_count": 10,
        "inserted_count": 10,
        "ranked_row_count": 10,
        "same_day_repair_performed": True,
        "finalization_eligible": True,
    })
    @patch("src.eod_refresh.run_scheduled_historical_append", return_value={
        "status": "success",
        "snapshot_rows_written": 0,
        "ticker_history_rows_written": 0,
        "failed_tickers": ["AAA", "BBB"],
    })
    @patch("src.eod_refresh.has_historical_append_for_date", return_value=True)
    @patch("src.eod_refresh.has_finalized_historical_append_for_date", return_value=False)
    @patch("src.eod_refresh.run_scheduled_eod_refresh", return_value=303)
    @patch("src.eod_refresh.has_eod_run_for_date", return_value=False)
    @patch("src.eod_refresh.is_trading_day", return_value=True)
    @patch("src.eod_refresh.current_et", return_value=datetime(2026, 4, 15, 19, 0, tzinfo=UTC))
    def test_run_latest_daily_sync_rebuilds_same_day_finalization_after_intraday_artifacts(
        self,
        _mock_current_et,
        _mock_is_trading_day,
        _mock_has_eod,
        _mock_run_eod,
        mock_has_finalized_append,
        mock_has_append,
        mock_run_append,
        mock_materialize,
        _mock_latest_expected,
        _mock_latest_canonical,
        _mock_latest_historical,
        _mock_refresh_summary,
    ):
        result = run_latest_daily_sync(MagicMock(), provider_name="live")

        self.assertTrue(bool(result["finalization_eligible"]))
        self.assertEqual(result["stages"]["historical_append"]["status"], "repaired_intraday_same_day")
        self.assertTrue(bool(result["stages"]["historical_append"]["same_day_repair_performed"]))
        self.assertTrue(bool(result["stages"]["historical_append"]["historical_current_for_target_day"]))
        self.assertTrue(bool(result["stages"]["historical_append"]["reused_existing_same_day_state"]))
        self.assertEqual(result["stages"]["historical_append"]["failed_tickers"], [])
        self.assertEqual(result["stages"]["historical_append"]["raw_failed_tickers"], ["AAA", "BBB"])
        self.assertEqual(result["stages"]["canonical_materialization"]["status"], "repaired_intraday_same_day")
        self.assertTrue(bool(result["stages"]["canonical_materialization"]["same_day_repair_performed"]))
        mock_has_finalized_append.assert_called_once()
        mock_has_append.assert_called_once()
        self.assertEqual(mock_run_append.call_count, 1)
        self.assertTrue(bool(mock_run_append.call_args.kwargs.get("replace_existing")))
        self.assertEqual(mock_run_append.call_args.kwargs.get("provider_name"), "live")
        self.assertTrue(bool(mock_run_append.call_args.kwargs.get("force")))
        mock_materialize.assert_called_once()


class TestRefreshRunStageSummary(unittest.TestCase):
    def test_refresh_run_stage_summary_reads_operational_counts(self):
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("create table refresh_runs(run_id bigint, ticker_count bigint, success_count bigint, failure_count bigint, skipped_tickers varchar, flagged_symbol_count bigint, suppressed_symbol_count bigint, api_call_count bigint)")
            conn.execute("create table refresh_failures(run_id bigint, ticker varchar)")
            conn.execute(
                """
                insert into refresh_runs values
                (101, 12, 9, 2, 'AAA,BBB,SUPR', 1, 1, 12)
                """
            )
            conn.execute(
                """
                insert into refresh_failures values
                (101, 'AAA'),
                (101, 'BBB')
                """
            )

            summary = refresh_run_stage_summary(conn, 101)

            self.assertEqual(summary["tickers_requested"], 12)
            self.assertEqual(summary["tickers_updated"], 9)
            self.assertEqual(summary["tickers_failed"], 2)
            self.assertEqual(summary["tickers_suppressed"], 1)
            self.assertEqual(summary["tickers_skipped"], 3)
            self.assertEqual(summary["flagged_tickers"], 1)
            self.assertEqual(summary["api_call_count"], 12)
            self.assertEqual(summary["failed_tickers"], ["AAA", "BBB"])
        finally:
            conn.close()


class TestCanonicalMaterializationStage(unittest.TestCase):
    def _build_conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        return conn

    def test_materialize_latest_canonical_day_uses_recent_day_backfill_path(self):
        conn = self._build_conn()
        try:
            conn.execute(
                """
                insert into ticker_daily_history(
                    ticker, trading_date, open, high, low, close, volume,
                    market_data_source, provenance_source_label, created_at, updated_at
                ) values
                ('AAA', '2026-04-15', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp)
                """
            )

            with patch(
                "src.eod_refresh.backfill_canonical_theme_daily_snapshots_for_recent_trading_days",
                return_value={
                    "results": [
                        {
                            "snapshot_date": "2026-04-15",
                            "status": "history_repaired",
                            "row_count": 12,
                            "inserted_count": 12,
                        }
                    ],
                    "missing_dates": [],
                },
            ) as mock_backfill, patch(
                "src.eod_refresh.latest_canonical_snapshot_date",
                side_effect=[None, "2026-04-15"],
            ):
                result = materialize_latest_canonical_day(conn, provider_name="live")

            mock_backfill.assert_called_once_with(
                conn,
                recent_trading_day_limit=1,
                provider="live",
                overwrite_existing=False,
            )
            self.assertEqual(result["status"], "history_repaired")
            self.assertEqual(result["inserted_count"], 12)
            self.assertTrue(result["advanced"])
        finally:
            conn.close()

    def test_materialize_latest_canonical_day_repairs_existing_unranked_latest_day(self):
        conn = self._build_conn()
        try:
            conn.execute(
                """
                insert into ticker_daily_history(
                    ticker, trading_date, open, high, low, close, volume,
                    market_data_source, provenance_source_label, created_at, updated_at
                ) values
                ('AAA', '2026-04-15', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp)
                """
            )
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
                ) values (
                    '2026-04-15', '2026-04-15 22:00:00', 9, 1, 'Alpha', 'Compute', true,
                    'live', 'after_hours_official', true, 'scheduled_eod_refresh',
                    3, 0, 0, 0, 0, 0, 0,
                    0, 0, null, null, null,
                    null, null, null,
                    null, null, null,
                    null, null, null, null,
                    null, null, null, null
                )
                """
            )

            with patch(
                "src.eod_refresh.backfill_canonical_theme_daily_snapshots_for_recent_trading_days",
                return_value={
                    "results": [
                        {
                            "snapshot_date": "2026-04-15",
                            "status": "history_repaired",
                            "row_count": 12,
                            "ranked_row_count": 11,
                            "inserted_count": 12,
                        }
                    ],
                    "missing_dates": [],
                },
            ) as mock_backfill, patch(
                "src.eod_refresh.latest_canonical_snapshot_date",
                side_effect=["2026-04-15", "2026-04-15"],
            ):
                result = materialize_latest_canonical_day(conn, provider_name="live")

            mock_backfill.assert_called_once_with(
                conn,
                recent_trading_day_limit=1,
                provider="live",
                overwrite_existing=True,
            )
            self.assertEqual(result["status"], "repaired_unranked_existing")
            self.assertTrue(bool(result["repaired_unranked_existing"]))
            self.assertEqual(int(result["ranked_row_count"]), 11)
        finally:
            conn.close()

    def test_materialize_latest_canonical_day_rebuilds_same_day_intraday_snapshot_after_eod(self):
        conn = self._build_conn()
        try:
            conn.execute(
                """
                insert into ticker_daily_history(
                    ticker, trading_date, open, high, low, close, volume,
                    market_data_source, provenance_source_label, created_at, updated_at
                ) values
                ('AAA', '2026-04-15', 10, 10, 10, 10, 1000, 'live', 'seed', current_timestamp, current_timestamp)
                """
            )
            conn.execute(
                """
                insert into refresh_runs(
                    run_id, provider, started_at, finished_at, status, ticker_count, success_count, failure_count, scope_type
                ) values (
                    9, 'live', '2026-04-15 18:00:00', '2026-04-15 19:00:00', 'success', 1, 1, 0, 'scheduled_eod'
                )
                """
            )
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
                ) values (
                    '2026-04-15', '2026-04-15 19:00:00', 9, 1, 'Alpha', 'Compute', true,
                    'live', 'after_hours_official', true, 'scheduled_eod_refresh',
                    3, 3, 3, 3, 3, 3, 3,
                    3, 75, 1, 2, 3,
                    50, 75, 100,
                    10, 10, 1,
                    1, 1, 1, 10,
                    10, 1, 10, 1
                )
                """
            )

            with patch(
                "src.eod_refresh.backfill_canonical_theme_daily_snapshots_for_recent_trading_days",
                return_value={
                    "results": [
                        {
                            "snapshot_date": "2026-04-15",
                            "status": "materialized_from_run",
                            "row_count": 12,
                            "ranked_row_count": 12,
                            "inserted_count": 12,
                        }
                    ],
                    "missing_dates": [],
                },
            ) as mock_backfill, patch(
                "src.eod_refresh.latest_canonical_snapshot_date",
                side_effect=["2026-04-15", "2026-04-15"],
            ):
                result = materialize_latest_canonical_day(
                    conn,
                    provider_name="live",
                    as_of_et=datetime(2026, 4, 15, 19, 0, tzinfo=UTC),
                )

            mock_backfill.assert_called_once_with(
                conn,
                recent_trading_day_limit=1,
                provider="live",
                overwrite_existing=True,
            )
            self.assertEqual(result["status"], "repaired_intraday_same_day")
            self.assertTrue(bool(result["same_day_repair_performed"]))
        finally:
            conn.close()
