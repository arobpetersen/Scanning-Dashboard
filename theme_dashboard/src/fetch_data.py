from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Iterable

from .config import LIVE_FETCH_REFERENCE_ON_REFRESH, LIVE_RATE_LIMIT_STOP_THRESHOLD, REFRESH_STALE_TIMEOUT_MINUTES
from .failure_classification import categorize_failure_message
from .provider_live import LiveProvider
from .provider_mock import MockProvider
from .rankings import persist_theme_snapshot_for_run
from .symbol_hygiene import apply_refresh_failure, apply_refresh_success, refresh_eligible_tickers
from .theme_service import active_ticker_universe


class RefreshBlockedError(RuntimeError):
    def __init__(self, message: str, running_run_id: int):
        super().__init__(message)
        self.running_run_id = running_run_id


def get_provider(provider_name: str):
    if provider_name == "live":
        live = LiveProvider(include_reference=LIVE_FETCH_REFERENCE_ON_REFRESH)
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


def _is_rate_limit_error(message: str) -> bool:
    return categorize_failure_message(message) == "RATE_LIMIT"


def run_refresh(
    conn,
    provider_name: str,
    tickers: Iterable[str] | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    scope_type: str | None = None,
    scope_theme_name: str | None = None,
    persist_theme_snapshots: bool = True,
) -> int:
    mark_stale_running_runs(conn)

    running = _current_running_run(conn)
    if running is not None:
        run_id = int(running[0])
        conn.execute(
            """
            INSERT INTO refresh_runs(provider, started_at, finished_at, status, ticker_count, scope_type, scope_theme_name, error_message)
            VALUES (?, ?, CURRENT_TIMESTAMP, 'blocked', 0, ?, ?, ?)
            """,
            [provider_name, datetime.utcnow(), scope_type, scope_theme_name, f"Refresh blocked: run {run_id} is already running."],
        )
        raise RefreshBlockedError(f"Refresh already running (run_id={run_id}).", run_id)

    provider = get_provider(provider_name)
    universe = list(tickers) if tickers is not None else active_ticker_universe(conn)
    clean_tickers = sorted({(t or "").strip().upper() for t in universe if (t or "").strip()})
    eligible_tickers, suppressed_tickers = refresh_eligible_tickers(conn, clean_tickers)

    run_id = conn.execute(
        """
        INSERT INTO refresh_runs(provider, started_at, status, ticker_count, scope_type, scope_theme_name)
        VALUES (?, ?, 'running', ?, ?, ?)
        RETURNING run_id
        """,
        [provider.name, datetime.utcnow(), len(clean_tickers), scope_type, scope_theme_name],
    ).fetchone()[0]

    for ticker in clean_tickers:
        conn.execute("INSERT INTO refresh_run_tickers(run_id, ticker) VALUES (?, ?)", [run_id, ticker])

    success_count = 0
    failure_count = 0
    started = datetime.utcnow()
    consecutive_rate_limit_failures = 0
    early_stop_reason: str | None = None
    failed_tickers: set[str] = set()
    failure_categories: dict[str, int] = defaultdict(int)
    flagged_count = 0
    auto_suppressed_count = 0

    try:
        if not eligible_tickers:
            conn.execute(
                """
                UPDATE refresh_runs
                SET finished_at = CURRENT_TIMESTAMP,
                    status = 'success',
                    success_count = 0,
                    failure_count = 0,
                    api_call_count = 0,
                    api_endpoint_counts = '{}',
                    skipped_tickers = ?,
                    failure_category_counts = '{}',
                    flagged_symbol_count = 0,
                    suppressed_symbol_count = ?
                WHERE run_id = ?
                """,
                [",".join(sorted(suppressed_tickers)) if suppressed_tickers else None, len(suppressed_tickers), run_id],
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

        for idx, ticker in enumerate(eligible_tickers, start=1):
            df, failures = provider.fetch_ticker_data([ticker])

            if not df.empty:
                payload = df.copy()
                if "market_cap" in payload.columns and payload["market_cap"].isna().any():
                    prior_caps = conn.execute(
                        """
                        SELECT ticker, market_cap
                        FROM ticker_snapshots
                        WHERE market_cap IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY run_id DESC) = 1
                        """
                    ).df()
                    if not prior_caps.empty:
                        payload = payload.merge(prior_caps, on="ticker", how="left", suffixes=("", "_prev"))
                        payload["market_cap"] = payload["market_cap"].combine_first(payload["market_cap_prev"])
                        payload = payload.drop(columns=["market_cap_prev"], errors="ignore")
                payload["run_id"] = run_id
                conn.register("incoming_snapshots", payload)
                conn.execute(
                    """
                    INSERT INTO ticker_snapshots(
                        run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                        market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated, snapshot_source
                    )
                    SELECT run_id, ticker, price, perf_1w, perf_1m, perf_3m,
                           market_cap, avg_volume, short_interest_pct, float_shares, adr_pct, last_updated, ?
                    FROM incoming_snapshots
                    """,
                    [provider.name],
                )
                conn.unregister("incoming_snapshots")
                success_count += int(len(df))
                consecutive_rate_limit_failures = 0
                for row in df.itertuples(index=False):
                    hygiene = apply_refresh_success(conn, str(row.ticker).upper(), int(run_id))
                    if hygiene.get("auto_suppressed"):
                        auto_suppressed_count += 1

            for failure in failures:
                error_message = failure.get("error_message", "Unknown error")
                failed_symbol = str(failure.get("ticker", ticker) or ticker).strip().upper()
                failure_category = categorize_failure_message(error_message)
                conn.execute(
                    "INSERT INTO refresh_failures(run_id, ticker, error_message, failure_category) VALUES (?, ?, ?, ?)",
                    [run_id, failed_symbol, error_message, failure_category],
                )
                failed_tickers.add(failed_symbol)
                failure_count += 1
                failure_categories[failure_category] += 1

                hygiene = apply_refresh_failure(conn, failed_symbol, int(run_id), error_message)
                if hygiene.get("flagged"):
                    flagged_count += 1
                if hygiene.get("auto_suppressed"):
                    auto_suppressed_count += 1

                if provider.name == "live" and _is_rate_limit_error(error_message):
                    consecutive_rate_limit_failures += 1
                else:
                    consecutive_rate_limit_failures = 0

            progress_note = f"Progress {idx}/{len(eligible_tickers)} | success={success_count} | failures={failure_count}"
            conn.execute(
                """
                UPDATE refresh_runs
                SET success_count = ?, failure_count = ?, error_message = ?
                WHERE run_id = ?
                """,
                [success_count, failure_count, progress_note, run_id],
            )

            if progress_callback:
                progress_callback(
                    {
                        "run_id": run_id,
                        "provider": provider.name,
                        "total": len(eligible_tickers),
                        "completed": idx,
                        "success": success_count,
                        "failure": failure_count,
                        "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
                    }
                )

            if provider.name == "live" and consecutive_rate_limit_failures >= LIVE_RATE_LIMIT_STOP_THRESHOLD:
                early_stop_reason = (
                    f"Stopped early due to repeated Massive rate-limit failures ({LIVE_RATE_LIMIT_STOP_THRESHOLD} consecutive tickers). "
                    "Use a smaller live scope and retry later."
                )
                break

        final_status = "success" if failure_count == 0 else "partial"
        if early_stop_reason and success_count == 0:
            final_status = "failed"

        accounting = provider.get_call_accounting() if hasattr(provider, "get_call_accounting") else {"api_call_count": 0, "endpoint_counts": {}}
        endpoint_json = json.dumps(accounting.get("endpoint_counts", {}), sort_keys=True)
        skipped_symbols = sorted(set(suppressed_tickers) | failed_tickers)
        skipped_csv = ",".join(skipped_symbols) if skipped_symbols else None

        summary_bits = [
            early_stop_reason or "",
            f"failures={failure_count}",
            f"flagged={flagged_count}",
            f"suppressed={len(suppressed_tickers) + auto_suppressed_count}",
        ]
        if failure_categories:
            summary_bits.append(f"by_category={json.dumps(dict(sorted(failure_categories.items())), sort_keys=True)}")
        summary_message = " | ".join([b for b in summary_bits if b])

        conn.execute(
            """
            UPDATE refresh_runs
            SET finished_at = CURRENT_TIMESTAMP,
                status = ?,
                success_count = ?,
                failure_count = ?,
                error_message = ?,
                api_call_count = ?,
                api_endpoint_counts = ?,
                skipped_tickers = ?,
                failure_category_counts = ?,
                flagged_symbol_count = ?,
                suppressed_symbol_count = ?
            WHERE run_id = ?
            """,
            [
                final_status,
                success_count,
                failure_count,
                summary_message,
                int(accounting.get("api_call_count", 0)),
                endpoint_json,
                skipped_csv,
                json.dumps(dict(sorted(failure_categories.items())), sort_keys=True),
                int(flagged_count),
                int(len(suppressed_tickers) + auto_suppressed_count),
                run_id,
            ],
        )

        if success_count > 0 and persist_theme_snapshots:
            persist_theme_snapshot_for_run(conn, run_id)
    except Exception as exc:
        accounting = provider.get_call_accounting() if hasattr(provider, "get_call_accounting") else {"api_call_count": 0, "endpoint_counts": {}}
        conn.execute(
            """
            UPDATE refresh_runs
            SET finished_at = CURRENT_TIMESTAMP,
                status = 'failed',
                failure_count = GREATEST(failure_count, ?),
                error_message = ?,
                api_call_count = ?,
                api_endpoint_counts = ?,
                skipped_tickers = ?,
                failure_category_counts = ?,
                flagged_symbol_count = ?,
                suppressed_symbol_count = ?
            WHERE run_id = ?
            """,
            [
                max(1, len(eligible_tickers) - success_count),
                str(exc),
                int(accounting.get("api_call_count", 0)),
                json.dumps(accounting.get("endpoint_counts", {}), sort_keys=True),
                ",".join(sorted(set(suppressed_tickers) | failed_tickers)) if (suppressed_tickers or failed_tickers) else None,
                json.dumps(dict(sorted(failure_categories.items())), sort_keys=True),
                int(flagged_count),
                int(len(suppressed_tickers) + auto_suppressed_count),
                run_id,
            ],
        )
        raise

    return run_id


def run_targeted_current_snapshot_hydration(
    conn,
    tickers: Iterable[str],
    *,
    provider_name: str,
    scope_type: str = "governed_ticker_current_hydration",
) -> dict[str, object]:
    normalized_tickers = sorted({(ticker or "").strip().upper() for ticker in tickers if (ticker or "").strip()})
    if not normalized_tickers:
        return {"status": "no_scope", "tickers": [], "run_id": None}
    required_tables = {"refresh_runs", "refresh_run_tickers", "ticker_snapshots"}
    existing_tables = {
        str(row[0]).strip()
        for row in conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    }
    if not required_tables.issubset(existing_tables):
        return {
            "status": "unavailable",
            "tickers": normalized_tickers,
            "run_id": None,
            "message": "Current snapshot hydration requires refresh tracking tables.",
        }
    try:
        run_id = run_refresh(
            conn,
            provider_name=provider_name,
            tickers=normalized_tickers,
            scope_type=scope_type,
            scope_theme_name=None,
            persist_theme_snapshots=False,
        )
    except RefreshBlockedError as exc:
        return {
            "status": "blocked",
            "tickers": normalized_tickers,
            "run_id": int(exc.running_run_id),
            "message": str(exc),
        }
    return {
        "status": "success",
        "tickers": normalized_tickers,
        "run_id": int(run_id),
    }
