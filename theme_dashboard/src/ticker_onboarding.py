from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from .config import DEFAULT_PROVIDER

ONBOARDING_HISTORY_TARGET_DAYS = 30
ONBOARDING_BACKFILL_WINDOW_DAYS = 90


def _normalize_ticker(ticker: str) -> str:
    return str(ticker or "").strip().upper()


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM duckdb_tables() WHERE table_name = ? LIMIT 1",
        [str(table_name or "").strip()],
    ).fetchone()
    return bool(row)


def _preferred_history_source(conn) -> str | None:
    if not _table_exists(conn, "ticker_daily_history"):
        return None
    row = conn.execute(
        """
        SELECT market_data_source
        FROM ticker_daily_history
        ORDER BY CASE WHEN market_data_source = 'live' THEN 0 ELSE 1 END,
                 trading_date DESC,
                 updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def assess_ticker_history_readiness(
    conn,
    ticker: str,
    *,
    target_trading_days: int = ONBOARDING_HISTORY_TARGET_DAYS,
) -> dict[str, object]:
    normalized_ticker = _normalize_ticker(ticker)
    source = _preferred_history_source(conn)
    if not source:
        return {
            "ticker": normalized_ticker,
            "history_row_count": 0,
            "history_target_days": int(target_trading_days),
            "history_market_data_source": None,
            "history_latest_trading_date": None,
            "history_readiness_status": "needs_backfill",
            "backfill_status": "needed",
            "downstream_refresh_needed": True,
        }

    row = conn.execute(
        """
        SELECT COUNT(DISTINCT trading_date) AS history_row_count,
               MAX(trading_date) AS history_latest_trading_date
        FROM ticker_daily_history
        WHERE ticker = ? AND market_data_source = ?
        """,
        [normalized_ticker, source],
    ).fetchone()
    history_row_count = int(row[0] or 0) if row else 0
    latest_trading_date = row[1] if row else None
    is_ready = history_row_count >= int(target_trading_days)
    return {
        "ticker": normalized_ticker,
        "history_row_count": history_row_count,
        "history_target_days": int(target_trading_days),
        "history_market_data_source": source,
        "history_latest_trading_date": latest_trading_date,
        "history_readiness_status": "ready" if is_ready else "needs_backfill",
        "backfill_status": "not_needed" if is_ready else "needed",
        "downstream_refresh_needed": not is_ready,
    }


def record_new_governed_ticker_onboarding(
    conn,
    ticker: str,
    *,
    onboarding_source: str,
    target_trading_days: int = ONBOARDING_HISTORY_TARGET_DAYS,
) -> dict[str, object]:
    from .fetch_data import run_targeted_current_snapshot_hydration

    readiness = assess_ticker_history_readiness(conn, ticker, target_trading_days=target_trading_days)
    downstream_refresh_needed = readiness["history_readiness_status"] == "ready"
    readiness["downstream_refresh_needed"] = bool(downstream_refresh_needed)
    readiness["current_snapshot_result"] = run_targeted_current_snapshot_hydration(
        conn,
        [ticker],
        provider_name=DEFAULT_PROVIDER,
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    if not _table_exists(conn, "governed_ticker_onboarding"):
        return readiness
    updated = conn.execute(
        """
        UPDATE governed_ticker_onboarding
        SET onboarding_source = ?,
            history_readiness_status = ?,
            backfill_status = ?,
            downstream_refresh_needed = ?,
            history_row_count = ?,
            history_target_days = ?,
            history_market_data_source = ?,
            history_latest_trading_date = ?,
            updated_at = ?
        WHERE ticker = ?
        RETURNING ticker
        """,
        [
            str(onboarding_source or "governed_add"),
            readiness["history_readiness_status"],
            readiness["backfill_status"],
            bool(downstream_refresh_needed),
            int(readiness["history_row_count"]),
            int(readiness["history_target_days"]),
            readiness["history_market_data_source"],
            readiness["history_latest_trading_date"],
            now,
            readiness["ticker"],
        ],
    ).fetchone()
    if updated is None:
        conn.execute(
            """
            INSERT INTO governed_ticker_onboarding(
                ticker, onboarding_source, history_readiness_status, backfill_status,
                downstream_refresh_needed, history_row_count, history_target_days,
                history_market_data_source, history_latest_trading_date, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                readiness["ticker"],
                str(onboarding_source or "governed_add"),
                readiness["history_readiness_status"],
                readiness["backfill_status"],
                bool(downstream_refresh_needed),
                int(readiness["history_row_count"]),
                int(readiness["history_target_days"]),
                readiness["history_market_data_source"],
                readiness["history_latest_trading_date"],
                now,
            ],
        )
    return readiness


def list_governed_ticker_onboarding(conn, limit: int = 100) -> pd.DataFrame:
    if not _table_exists(conn, "governed_ticker_onboarding"):
        return pd.DataFrame()
    return conn.execute(
        """
        WITH membership AS (
            SELECT
                m.ticker,
                COUNT(*) AS governed_assignment_count,
                STRING_AGG(t.name, ', ' ORDER BY t.name) AS governed_themes
            FROM theme_membership m
            JOIN themes t ON t.id = m.theme_id
            GROUP BY m.ticker
        )
        SELECT
            o.ticker,
            o.added_at,
            o.onboarding_source,
            o.history_readiness_status,
            o.backfill_status,
            o.last_backfill_attempt_at,
            o.last_backfill_error,
            o.downstream_refresh_needed,
            o.history_row_count,
            o.history_target_days,
            o.history_market_data_source,
            o.history_latest_trading_date,
            COALESCE(m.governed_assignment_count, 0) AS governed_assignment_count,
            m.governed_themes,
            o.updated_at
        FROM governed_ticker_onboarding o
        LEFT JOIN membership m ON m.ticker = o.ticker
        ORDER BY o.added_at DESC, o.ticker
        LIMIT ?
        """,
        [limit],
    ).df()


def governed_ticker_onboarding_counts(conn) -> pd.DataFrame:
    if not _table_exists(conn, "governed_ticker_onboarding"):
        return pd.DataFrame()
    return conn.execute(
        """
        SELECT
            history_readiness_status,
            backfill_status,
            COUNT(*) AS cnt
        FROM governed_ticker_onboarding
        GROUP BY history_readiness_status, backfill_status
        ORDER BY history_readiness_status, backfill_status
        """
    ).df()


def run_governed_ticker_onboarding_backfill(
    conn,
    tickers: list[str],
    *,
    provider_name: str = DEFAULT_PROVIDER,
    target_trading_days: int = ONBOARDING_HISTORY_TARGET_DAYS,
    lookback_days: int = ONBOARDING_BACKFILL_WINDOW_DAYS,
) -> dict[str, object]:
    normalized_tickers = sorted({_normalize_ticker(ticker) for ticker in tickers if _normalize_ticker(ticker)})
    if not normalized_tickers:
        return {"status": "no_scope", "tickers": [], "backfill_result": None}
    if not _table_exists(conn, "governed_ticker_onboarding"):
        return {"status": "no_tracking_table", "tickers": normalized_tickers, "backfill_result": None, "updated_rows": []}

    now = datetime.now(UTC).replace(tzinfo=None)
    conn.execute(
        f"""
        UPDATE governed_ticker_onboarding
        SET last_backfill_attempt_at = ?,
            last_backfill_error = NULL,
            backfill_status = 'running',
            updated_at = CURRENT_TIMESTAMP
        WHERE ticker IN ({", ".join(["?"] * len(normalized_tickers))})
        """,
        [now, *normalized_tickers],
    )

    from .fetch_data import get_provider
    from .fetch_data import run_targeted_current_snapshot_hydration
    from .ticker_history import persist_ticker_daily_history

    provider = get_provider(provider_name)
    fetch_start = (datetime.now(UTC) - timedelta(days=int(lookback_days))).date()
    fetch_end = datetime.now(UTC).date()
    result = {
        "status": "success",
        "ticker_history_rows_written": 0,
        "ticker_history_rows_skipped": 0,
        "failed_tickers": [],
    }
    for ticker in normalized_tickers:
        try:
            history = provider.fetch_ticker_history_range(ticker, fetch_start, fetch_end)
            if history.empty:
                result["failed_tickers"].append(ticker)
                result["status"] = "partial"
                continue
            persisted = persist_ticker_daily_history(
                conn,
                history,
                ticker=ticker,
                provenance_source_label="governed_ticker_onboarding",
                market_data_source=provider.name,
                run_id=None,
                replace_existing=False,
            )
            result["ticker_history_rows_written"] += int(persisted["rows_written"])
            result["ticker_history_rows_skipped"] += int(persisted["rows_skipped"])
        except Exception as exc:
            conn.execute(
                """
                UPDATE governed_ticker_onboarding
                SET backfill_status = 'failed',
                    last_backfill_error = ?,
                    downstream_refresh_needed = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE ticker = ?
                """,
                [str(exc), ticker],
            )
            result["failed_tickers"].append(ticker)
            result["status"] = "partial"

    updated_rows: list[dict[str, object]] = []
    for ticker in normalized_tickers:
        readiness = assess_ticker_history_readiness(conn, ticker, target_trading_days=target_trading_days)
        backfill_status = "completed" if readiness["history_readiness_status"] == "ready" else "insufficient_after_attempt"
        downstream_refresh_needed = readiness["history_readiness_status"] == "ready"
        conn.execute(
            """
            UPDATE governed_ticker_onboarding
            SET history_readiness_status = ?,
                backfill_status = ?,
                last_backfill_error = NULL,
                downstream_refresh_needed = ?,
                history_row_count = ?,
                history_target_days = ?,
                history_market_data_source = ?,
                history_latest_trading_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE ticker = ?
            """,
            [
                readiness["history_readiness_status"],
                backfill_status,
                downstream_refresh_needed,
                int(readiness["history_row_count"]),
                int(readiness["history_target_days"]),
                readiness["history_market_data_source"],
                readiness["history_latest_trading_date"],
                ticker,
            ],
        )
        updated_rows.append(
            {
                "ticker": ticker,
                "history_readiness_status": readiness["history_readiness_status"],
                "backfill_status": backfill_status,
                "history_row_count": int(readiness["history_row_count"]),
                "downstream_refresh_needed": downstream_refresh_needed,
            }
        )

    current_snapshot_result = run_targeted_current_snapshot_hydration(
        conn,
        normalized_tickers,
        provider_name=provider_name,
    )
    return {
        "status": result.get("status") or "success",
        "tickers": normalized_tickers,
        "backfill_result": result,
        "updated_rows": updated_rows,
        "current_snapshot_result": current_snapshot_result,
    }


def run_governed_ticker_onboarding_theme_reconstruction(
    conn,
    tickers: list[str],
    *,
    provider_name: str = DEFAULT_PROVIDER,
    lookback_days: int = ONBOARDING_BACKFILL_WINDOW_DAYS,
) -> dict[str, object]:
    normalized_tickers = sorted({_normalize_ticker(ticker) for ticker in tickers if _normalize_ticker(ticker)})
    if not normalized_tickers:
        return {"status": "no_scope", "tickers": [], "reconstruction_result": None}
    if not _table_exists(conn, "governed_ticker_onboarding"):
        return {"status": "no_tracking_table", "tickers": normalized_tickers, "reconstruction_result": None, "updated_rows": []}

    from .historical_backfill import reconstruct_theme_history_range

    result = reconstruct_theme_history_range(
        conn,
        provider_name=provider_name,
        start_date=(datetime.now(UTC) - timedelta(days=int(lookback_days))).date(),
        end_date=datetime.now(UTC).date(),
        tickers=normalized_tickers,
        provenance_source_label="governed_ticker_onboarding_theme_reconstruction",
        run_kind="governed_ticker_onboarding_theme_reconstruction",
        replace_existing=False,
        persist_ticker_history=False,
    )
    updated_rows: list[dict[str, object]] = []
    if result.get("status") in {"success", "partial"}:
        conn.execute(
            f"""
            UPDATE governed_ticker_onboarding
            SET downstream_refresh_needed = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE ticker IN ({", ".join(["?"] * len(normalized_tickers))})
            """,
            normalized_tickers,
        )
        updated_rows = [{"ticker": ticker, "downstream_refresh_needed": False} for ticker in normalized_tickers]
    return {
        "status": result.get("status") or "success",
        "tickers": normalized_tickers,
        "reconstruction_result": result,
        "updated_rows": updated_rows,
    }
