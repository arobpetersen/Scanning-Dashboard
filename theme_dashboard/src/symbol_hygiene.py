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
STAGED_ACTIONS = {
    "none": "No staged action",
    "suppress": "Stage suppress",
    "keep_active": "Stage keep active",
    "watch": "Stage return to watch",
    "reset": "Stage reset history",
}
OVERRIDE_ACTIONS = {
    "none": "No override",
    "keep_active": "Keep active",
    "watch": "Return to watch",
    "reset": "Reset history",
}


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_tables()
        WHERE schema_name = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


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


def hygiene_decision_context(row) -> dict[str, str]:
    status = str(row.get("status") or "")
    suggested = str(row.get("suggested_status") or "")
    category = str(row.get("last_failure_category") or "")
    consecutive = int(row.get("consecutive_failure_count") or 0)

    if status == REFRESH_SUPPRESSED:
        return {
            "recommended_action": "Keep suppressed",
            "confidence": "high",
            "explanation": "Refreshes are already suppressed. This preserves lineage/history while keeping the symbol out of active refresh.",
        }
    if suggested == REFRESH_SUPPRESSED and category == "NO_CANDLES" and consecutive >= NO_CANDLES_AUTO_SUPPRESS_THRESHOLD:
        return {
            "recommended_action": "Approve suppression",
            "confidence": "high",
            "explanation": "Repeated NO_CANDLES failures have reached a strong threshold. Suppress from active refresh; review theme membership separately.",
        }
    if suggested == REFRESH_SUPPRESSED and category == "NO_CANDLES" and consecutive >= NO_CANDLES_FLAG_THRESHOLD:
        return {
            "recommended_action": "Approve suppression",
            "confidence": "medium",
            "explanation": "Repeated NO_CANDLES failures suggest this symbol may no longer provide usable data. Suppression is preferred to deletion.",
        }
    if status == WATCH:
        return {
            "recommended_action": "Keep active / watch",
            "confidence": "medium",
            "explanation": "Operational issue pattern exists, but evidence is not strong enough for suppression. Continue refresh with monitoring.",
        }
    return {
        "recommended_action": "Review manually",
        "confidence": "low",
        "explanation": "Use failure streaks and data recency as context. Suppression controls refresh eligibility; it does not delete DB lineage or theme history.",
    }


def resolve_staged_symbol_hygiene_action(approve_recommended: bool, override_action: str | None) -> str:
    normalized_override = str(override_action or "none").strip().lower()
    if normalized_override in OVERRIDE_ACTIONS and normalized_override != "none":
        return normalized_override
    return "suppress" if approve_recommended else "none"


def symbol_hygiene_queue(conn, limit: int = 200) -> pd.DataFrame:
    membership_join = ""
    membership_columns = "NULL AS current_theme_names, NULL AS current_categories,"
    membership_cte = ""
    if _table_exists(conn, "theme_membership") and _table_exists(conn, "themes"):
        membership_cte = """
        ,
        membership_context AS (
            SELECT
                m.ticker,
                STRING_AGG(t.name, ', ' ORDER BY t.name) AS current_theme_names,
                STRING_AGG(
                    DISTINCT COALESCE(NULLIF(t.category, ''), 'Uncategorized'),
                    ', '
                    ORDER BY COALESCE(NULLIF(t.category, ''), 'Uncategorized')
                ) AS current_categories
            FROM theme_membership m
            JOIN themes t ON t.id = m.theme_id
            GROUP BY m.ticker
        )
        """
        membership_columns = "mc.current_theme_names, mc.current_categories,"
        membership_join = "LEFT JOIN membership_context mc ON mc.ticker = s.ticker"

    return conn.execute(
        f"""
        WITH latest_market_data AS (
            SELECT
                ts.ticker,
                MAX(ts.last_updated) AS last_market_data_at
            FROM ticker_snapshots ts
            JOIN refresh_runs r ON r.run_id = ts.run_id
            WHERE r.status IN ('success', 'partial')
            GROUP BY ts.ticker
        )
        {membership_cte}
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
            {membership_columns}
            CASE
              WHEN lmd.last_market_data_at IS NULL THEN NULL
              ELSE DATE_DIFF('day', CAST(lmd.last_market_data_at AS DATE), CURRENT_DATE)
            END AS days_since_last_valid_data
        FROM symbol_refresh_status s
        LEFT JOIN latest_market_data lmd ON lmd.ticker = s.ticker
        {membership_join}
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


def filter_symbol_hygiene_queue(queue: pd.DataFrame, queue_view: str) -> pd.DataFrame:
    if queue.empty:
        return queue

    out = queue.copy()
    if queue_view == "Pending review":
        mask = out["suggested_status"].notna() | out["status"].isin([INACTIVE_CANDIDATE, WATCH])
        out = out[mask & (out["status"] != REFRESH_SUPPRESSED)]
    elif queue_view == "Suppressed / resolved":
        out = out[out["status"] == REFRESH_SUPPRESSED]
    return out.reset_index(drop=True)


def sort_symbol_hygiene_queue(queue: pd.DataFrame, sort_mode: str) -> pd.DataFrame:
    if queue.empty:
        return queue

    out = queue.copy()
    decision = out.apply(hygiene_decision_context, axis=1, result_type="expand")
    out["recommended_action"] = decision["recommended_action"]
    out["confidence"] = decision["confidence"]
    out["recommendation_explanation"] = decision["explanation"]
    out["_confidence_rank"] = out["confidence"].map({"high": 2, "medium": 1, "low": 0}).fillna(0)
    out["_days_since_sort"] = out["days_since_last_valid_data"].fillna(10**9)

    if sort_mode == "Longest invalid period":
        sort_cols = ["_days_since_sort", "_confidence_rank", "consecutive_failure_count", "rolling_failure_count", "ticker"]
    elif sort_mode == "Most consecutive failures":
        sort_cols = ["consecutive_failure_count", "_confidence_rank", "_days_since_sort", "rolling_failure_count", "ticker"]
    elif sort_mode == "Most rolling failures":
        sort_cols = ["rolling_failure_count", "_confidence_rank", "_days_since_sort", "consecutive_failure_count", "ticker"]
    else:
        sort_cols = ["_confidence_rank", "_days_since_sort", "consecutive_failure_count", "rolling_failure_count", "ticker"]

    ascending = [False, False, False, False, True]
    return out.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)


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


def apply_symbol_hygiene_action(conn, ticker: str, action: str) -> None:
    normalized = (action or "").strip().lower()
    if normalized == "suppress":
        approve_suppression(conn, ticker)
    elif normalized == "keep_active":
        reject_keep_active(conn, ticker)
    elif normalized == "watch":
        reset_failure_history(conn, ticker, to_watch=True)
    elif normalized == "reset":
        reset_failure_history(conn, ticker, to_watch=False)
    else:
        raise ValueError(f"Unknown symbol hygiene action: {action}")


def apply_staged_symbol_hygiene_actions(conn, staged_actions: dict[str, str]) -> dict[str, object]:
    normalized_actions = {
        str(ticker).strip().upper(): str(action).strip().lower()
        for ticker, action in staged_actions.items()
        if str(ticker).strip() and str(action).strip().lower() in STAGED_ACTIONS and str(action).strip().lower() != "none"
    }
    if not normalized_actions:
        return {"applied_count": 0, "by_action": {}, "tickers": []}

    by_action: dict[str, int] = {}
    tickers = sorted(normalized_actions.keys())
    conn.execute("BEGIN TRANSACTION")
    try:
        for ticker, action in normalized_actions.items():
            apply_symbol_hygiene_action(conn, ticker, action)
            by_action[action] = by_action.get(action, 0) + 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "applied_count": len(normalized_actions),
        "by_action": by_action,
        "tickers": tickers,
    }


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
