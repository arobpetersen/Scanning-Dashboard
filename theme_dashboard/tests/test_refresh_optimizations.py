from datetime import datetime
from unittest.mock import patch

import duckdb
import pandas as pd

from src.database import SCHEMA_SQL
from src.fetch_data import run_refresh


class CountingProvider:
    name = "mock"

    def __init__(self):
        self.calls = 0

    def fetch_ticker_data(self, tickers):
        self.calls += 1
        ticker = str(list(tickers)[0]).strip().upper()
        return (
            pd.DataFrame(
                [
                    {
                        "ticker": ticker,
                        "price": 25.0,
                        "perf_1d": 0.5,
                        "perf_1w": 1.0,
                        "perf_1m": 2.0,
                        "perf_3m": 3.0,
                        "perf_6m": 4.0,
                        "market_cap": 1_000_000_000.0,
                        "avg_volume": 1_000_000.0,
                        "short_interest_pct": None,
                        "float_shares": None,
                        "adr_pct": None,
                        "last_updated": datetime(2026, 4, 24, 20, 0, 0),
                    }
                ]
            ),
            [],
        )

    def get_call_accounting(self):
        return {"api_call_count": self.calls, "endpoint_counts": {"aggs_daily": self.calls}}


def test_run_refresh_batches_scope_rows_and_throttles_progress_callbacks():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(SCHEMA_SQL)
        tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
        provider = CountingProvider()
        progress_updates: list[dict[str, object]] = []

        with patch("src.fetch_data.get_provider", return_value=provider), patch(
            "src.fetch_data.persist_theme_snapshot_for_run",
            return_value=None,
        ), patch("src.fetch_data.REFRESH_PROGRESS_CALLBACK_INTERVAL_TICKERS", 5), patch(
            "src.fetch_data.REFRESH_PROGRESS_DB_UPDATE_INTERVAL_TICKERS",
            5,
        ):
            run_id = run_refresh(
                conn,
                provider_name="mock",
                tickers=tickers,
                progress_callback=lambda payload: progress_updates.append(dict(payload)),
            )

        scoped_count = conn.execute(
            "select count(*) from refresh_run_tickers where run_id = ?",
            [run_id],
        ).fetchone()[0]
        final_run = conn.execute(
            "select success_count, failure_count, status from refresh_runs where run_id = ?",
            [run_id],
        ).fetchone()

        assert int(scoped_count) == len(tickers)
        assert [int(update["completed"]) for update in progress_updates] == [1, 5, 6]
        assert final_run == (6, 0, "success")
    finally:
        conn.close()
