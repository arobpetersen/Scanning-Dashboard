from __future__ import annotations

from datetime import datetime

import pandas as pd

from .failure_classification import categorize_failure_message

ACTIVE = "active"
WATCH = "watch"
REFRESH_SUPPRESSED = "refresh_suppressed"
INACTIVE_CANDIDATE = "inactive_candidate"

NO_CANDLES_FLAG_THRESHOLD = 3
NO_CANDLES_AUTO_SUPPRESS_THRESHOLD = 5


def _load_state(conn, ticker: str):
    return conn.execute(
        """
        SELECT ticker, status, suggested_status, suppression_reason,
               last_failure_category, consecutive_failure_count, rolling_failure_count,
               last_failure_at, last_success_at, last_run_id
        FROM symbol_refresh_status
        WHERE ticker = ?
        """,
        [ticker],
    ).fetchone()


def ensure_symbol_row(conn, ticker: str) -> None:
    conn.execute(
        """
        INSERT INTO symbol_refresh_status(ticker, status, consecutive_failure_count, rolling_failure_count)
        VALUES (?, 'active', 0, 0)
        ON CONFLICT (ticker) DO NOTHING
        """,
        [ticker],
    )


def refresh_eligible_tickers(conn, tickers: list[str]) -> tuple[list[str], list[str]]:
    if not tickers:
        return [], []
    rows = conn.execute(
        """
        SELECT ticker, status
        FROM symbol_refresh_status
        WHERE ticker IN ({})
        """.format(
            ",".join(["?"] * len(tickers))
        ),
        tickers,
    ).df()
    status_map = {str(r["ticker"]): str(r["status"]) for _, r in rows.iterrows()}

    eligible: list[str] = []
    suppressed: list[str] = []
    for t in tickers:
        status = status_map.get(t, ACTIVE)
        if status == REFRESH_SUPPRESSED:
            suppressed.append(t)
        else:
            eligible.append(t)
    return eligible, suppressed


def apply_refresh_success(conn, ticker: str, run_id: int) -> dict:
    ensure_symbol_row(conn, ticker)
    conn.execute(
        """
        UPDATE symbol_refresh_status
        SET status='active',
            suggested_status=NULL,
            suggested_reason=NULL,
            last_failure_category=NULL,
            consecutive_failure_count=0,
            last_success_at=CURRENT_TIMESTAMP,
            last_run_id=?,
            updated_at=CURRENT_TIMESTAMP
        WHERE ticker=?
        """,
        [run_id, ticker],
    )
    return {"flagged": False, "auto_suppressed": False}


def apply_refresh_failure(conn, ticker: str, run_id: int, error_message: str) -> dict:
    ensure_symbol_row(conn, ticker)
    category = categorize_failure_message(error_message)
    prev = _load_state(conn, ticker)
    prev_category = str(prev[4]) if prev and prev[4] else None
    prev_consecutive = int(prev[5]) if prev and prev[5] is not None else 0
    prev_rolling = int(prev[6]) if prev and prev[6] is not None else 0
    prev_status = str(prev[1]) if prev and prev[1] else ACTIVE

    consecutive = (prev_consecutive + 1) if prev_category == category else 1
    rolling = prev_rolling + 1

    status = prev_status
    suggested_status = None
    suggested_reason = None
    flagged = False
    auto_suppressed = False

    # Conservative deterministic policy: first flag repeated no-candles for manual review,
    # only auto-suppress after a stronger threshold.
    if category == "NO_CANDLES" and consecutive >= NO_CANDLES_AUTO_SUPPRESS_THRESHOLD:
        status = REFRESH_SUPPRESSED
        suggested_status = None
        suggested_reason = f"Auto-suppressed after {consecutive} consecutive NO_CANDLES failures."
        auto_suppressed = True
    elif category == "NO_CANDLES" and consecutive >= NO_CANDLES_FLAG_THRESHOLD:
        if status != REFRESH_SUPPRESSED:
            status = INACTIVE_CANDIDATE
        suggested_status = REFRESH_SUPPRESSED
        suggested_reason = f"{consecutive} consecutive NO_CANDLES failures. Review suppression."
        flagged = True
    elif category in {"TIMEOUT", "RATE_LIMIT"} and status == ACTIVE:
        status = WATCH

    conn.execute(
        """
        UPDATE symbol_refresh_status
        SET status=?,
            suggested_status=?,
            suggested_reason=?,
            last_failure_category=?,
            consecutive_failure_count=?,
            rolling_failure_count=?,
            last_failure_at=CURRENT_TIMESTAMP,
            last_run_id=?,
            updated_at=CURRENT_TIMESTAMP
        WHERE ticker=?
        """,
        [status, suggested_status, suggested_reason, category, consecutive, rolling, run_id, ticker],
    )
    return {"flagged": flagged, "auto_suppressed": auto_suppressed, "category": category}


def symbol_hygiene_queue(conn, limit: int = 200) -> pd.DataFrame:
    return conn.execute(
        """
        WITH latest_market_data AS (
            SELECT
                ts.ticker,
                MAX(ts.last_updated) AS last_market_data_at
            FROM ticker_snapshots ts
            JOIN refresh_runs r ON r.run_id = ts.run_id
            WHERE r.status IN ('success', 'partial')
            GROUP BY ts.ticker
        )
        SELECT
            s.ticker,
            s.status,
            s.suggested_status,
            s.suggested_reason,
            s.last_failure_category,
            s.consecutive_failure_count,
            s.rolling_failure_count,
            s.last_success_at,
            s.last_failure_at,
            s.last_run_id,
            lmd.last_market_data_at,
            CASE
              WHEN lmd.last_market_data_at IS NULL THEN NULL
              ELSE DATE_DIFF('day', CAST(lmd.last_market_data_at AS DATE), CURRENT_DATE)
            END AS days_since_last_valid_data
        FROM symbol_refresh_status s
        LEFT JOIN latest_market_data lmd ON lmd.ticker = s.ticker
        WHERE s.suggested_status IS NOT NULL
           OR s.status IN ('inactive_candidate', 'refresh_suppressed', 'watch')
        ORDER BY
          CASE s.status
            WHEN 'inactive_candidate' THEN 0
            WHEN 'refresh_suppressed' THEN 1
            WHEN 'watch' THEN 2
            ELSE 3
          END,
          s.consecutive_failure_count DESC,
          s.rolling_failure_count DESC,
          s.ticker
        LIMIT ?
        """,
        [limit],
    ).df()


def approve_suppression(conn, ticker: str, note: str | None = None) -> None:
    ensure_symbol_row(conn, ticker)
    reason = note or "Suppression approved in Health review queue."
    conn.execute(
        """
        UPDATE symbol_refresh_status
        SET status='refresh_suppressed',
            suggested_status=NULL,
            suggested_reason=?,
            updated_at=CURRENT_TIMESTAMP
        WHERE ticker=?
        """,
        [reason, ticker],
    )


def reject_keep_active(conn, ticker: str) -> None:
    ensure_symbol_row(conn, ticker)
    conn.execute(
        """
        UPDATE symbol_refresh_status
        SET status='active',
            suggested_status=NULL,
            suggested_reason='Suppression rejected; kept active by reviewer.',
            updated_at=CURRENT_TIMESTAMP
        WHERE ticker=?
        """,
        [ticker],
    )


def reset_failure_history(conn, ticker: str, to_watch: bool = False) -> None:
    ensure_symbol_row(conn, ticker)
    conn.execute(
        """
        UPDATE symbol_refresh_status
        SET status=?,
            suggested_status=NULL,
            suggested_reason='Failure history reset by reviewer.',
            last_failure_category=NULL,
            consecutive_failure_count=0,
            rolling_failure_count=0,
            updated_at=CURRENT_TIMESTAMP
        WHERE ticker=?
        """,
        [WATCH if to_watch else ACTIVE, ticker],
    )
