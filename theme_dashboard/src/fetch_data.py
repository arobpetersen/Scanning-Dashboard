from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Iterable

from .config import REFRESH_STALE_TIMEOUT_MINUTES
from .provider_live import LiveProvider
from .provider_mock import MockProvider
from .rankings import persist_theme_snapshot_for_run
from .theme_service import active_ticker_universe


class RefreshBlockedError(RuntimeError):
    def __init__(self, message: str, running_run_id: int):
        super().__init__(message)
        self.running_run_id = running_run_id


def get_provider(provider_name: str):
    if provider_name == "live":
        live = LiveProvider()
        if live.is_configured:
            return live
        return MockProvider()
    return MockProvider()


def mark_stale_running_runs(conn, stale_minutes: int = REFRESH_STALE_TIMEOUT_MINUTES) -> int:
    stale_before = datetime.utcnow() - timedelta(minutes=stale_minutes)
    stale_count = conn.execute(
        "SELECT COUNT(*) FROM refresh_runs WHERE status = 'running' AND started_at < ?",
        [stale_before],
    ).fetchone()[0]
    if stale_count:
        conn.execute(
            """
            UPDATE refresh_runs
            SET status = 'failed',
                finished_at = CURRENT_TIMESTAMP,
                error_message = COALESCE(error_message, ?)
            WHERE status = 'running' AND started_at < ?
            """,
            [f"Run marked stale after exceeding {stale_minutes} minutes.", stale_before],
        )
    return int(stale_count)


def _current_running_run(conn):
    return conn.execute(
        """
        SELECT run_id, provider, started_at, ticker_count, success_count, failure_count
        FROM refresh_runs
        WHERE status = 'running'
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()


def run_refresh(
    conn,
    provider_name: str,
    tickers: Iterable[str] | None = None,
    progress_callback: Callable[[dict], None] | None = None,
) -> int:
    mark_stale_running_runs(conn)

    running = _current_running_run(conn)
    if running is not None:
        run_id = int(running[0])
        conn.execute(
            """
            INSERT INTO refresh_runs(provider, started_at, finished_at, status, ticker_count, error_message)
            VALUES (?, ?, CURRENT_TIMESTAMP, 'blocked', 0, ?)
            """,
            [provider_name, datetime.utcnow(), f"Refresh blocked: run {run_id} is already running."],
        )
        raise RefreshBlockedError(f"Refresh already running (run_id={run_id}).", run_id)

    provider = get_provider(provider_name)
    universe = list(tickers) if tickers is not None else active_ticker_universe(conn)
    clean_tickers = sorted({(t or "").strip().upper() for t in universe if (t or "").strip()})

    run_id = conn.execute(
        """
        INSERT INTO refresh_runs(provider, started_at, status, ticker_count)
        VALUES (?, ?, 'running', ?)
        RETURNING run_id
        """,
        [provider.name, datetime.utcnow(), len(clean_tickers)],
    ).fetchone()[0]

    success_count = 0
    failure_count = 0
    started = datetime.utcnow()

    try:
        if not clean_tickers:
            conn.execute(
                """
                UPDATE refresh_runs
                SET finished_at = CURRENT_TIMESTAMP, status = 'success', success_count = 0, failure_count = 0
                WHERE run_id = ?
                """,
                [run_id],
            )
            persist_theme_snapshot_for_run(conn, run_id)
            if progress_callback:
                progress_callback(
                    {
                        "run_id": run_id,
                        "provider": provider.name,
                        "total": 0,
                        "completed": 0,
                        "success": 0,
                        "failure": 0,
                        "elapsed_seconds": 0.0,
                    }
                )
            return run_id

        for idx, ticker in enumerate(clean_tickers, start=1):
            df, failures = provider.fetch_ticker_data([ticker])

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
                success_count += int(len(df))

            for failure in failures:
                conn.execute(
                    "INSERT INTO refresh_failures(run_id, ticker, error_message) VALUES (?, ?, ?)",
                    [run_id, failure.get("ticker", ticker), failure.get("error_message", "Unknown error")],
                )
                failure_count += 1

            conn.execute(
                """
                UPDATE refresh_runs
                SET success_count = ?, failure_count = ?
                WHERE run_id = ?
                """,
                [success_count, failure_count, run_id],
            )

            if progress_callback:
                progress_callback(
                    {
                        "run_id": run_id,
                        "provider": provider.name,
                        "total": len(clean_tickers),
                        "completed": idx,
                        "success": success_count,
                        "failure": failure_count,
                        "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
                    }
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
            ["success" if failure_count == 0 else "partial", success_count, failure_count, run_id],
        )
        persist_theme_snapshot_for_run(conn, run_id)
    except Exception as exc:
        conn.execute(
            """
            UPDATE refresh_runs
            SET finished_at = CURRENT_TIMESTAMP,
                status = 'failed',
                failure_count = GREATEST(failure_count, ?),
                error_message = ?
            WHERE run_id = ?
            """,
            [max(1, len(clean_tickers) - success_count), str(exc), run_id],
        )
        raise

    return run_id
