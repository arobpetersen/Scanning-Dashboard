from __future__ import annotations

from datetime import datetime

from .provider_live import LiveProvider
from .provider_mock import MockProvider
from .theme_service import active_ticker_universe


def get_provider(provider_name: str):
    return LiveProvider() if provider_name == "live" else MockProvider()


def run_refresh(conn, provider_name: str) -> int:
    provider = get_provider(provider_name)
    tickers = active_ticker_universe(conn)

    run_id = conn.execute(
        """
        INSERT INTO refresh_runs(provider, started_at, status, ticker_count)
        VALUES (?, ?, 'running', ?)
        RETURNING run_id
        """,
        [provider.name, datetime.utcnow(), len(tickers)],
    ).fetchone()[0]

    try:
        if not tickers:
            conn.execute(
                """
                UPDATE refresh_runs
                SET finished_at = CURRENT_TIMESTAMP, status = 'success', success_count = 0, failure_count = 0
                WHERE run_id = ?
                """,
                [run_id],
            )
            return run_id

        df, failures = provider.fetch_ticker_data(tickers)

        if not df.empty:
            payload = df.copy()
            payload["run_id"] = run_id
            conn.register("incoming_snapshots", payload)
            conn.execute(
                """
                INSERT INTO ticker_snapshots(
                    run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                    market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated
                )
                SELECT run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                       market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated
                FROM incoming_snapshots
                """
            )
            conn.unregister("incoming_snapshots")

        for failure in failures:
            conn.execute(
                "INSERT INTO refresh_failures(run_id, ticker, error_message) VALUES (?, ?, ?)",
                [run_id, failure.get("ticker"), failure.get("error_message", "Unknown error")],
            )

        conn.execute(
            """
            UPDATE refresh_runs
            SET finished_at = CURRENT_TIMESTAMP,
                status = ?,
                success_count = ?,
                failure_count = ?
            WHERE run_id = ?
            """,
            ["success" if not failures else "partial", len(df), len(failures), run_id],
        )
    except Exception as exc:
        conn.execute(
            """
            UPDATE refresh_runs
            SET finished_at = CURRENT_TIMESTAMP,
                status = 'failed',
                success_count = 0,
                failure_count = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            [len(tickers), str(exc), run_id],
        )
        raise

    return run_id
