from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from .config import DEFAULT_PROVIDER
from .db_introspection import table_exists, table_has_column

ONBOARDING_HISTORY_TARGET_DAYS = 30
ONBOARDING_BACKFILL_WINDOW_DAYS = 90


def _manual_suppression_filter_sql(conn, ticker_expr: str) -> str:
    if not table_exists(conn, "symbol_refresh_status") or not table_has_column(conn, "symbol_refresh_status", "manual_suppressed"):
        return ""
    return (
        " AND NOT EXISTS ("
        "SELECT 1 FROM symbol_refresh_status s "
        f"WHERE upper(trim(s.ticker)) = upper(trim({ticker_expr})) "
        "AND COALESCE(s.manual_suppressed, FALSE)"
        ")"
    )


def _normalize_ticker(ticker: str) -> str:
    return str(ticker or "").strip().upper()


def _preferred_history_source(conn) -> str | None:
    if not table_exists(conn, "ticker_daily_history"):
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


def _preferred_ticker_snapshot_source(conn) -> str | None:
    if not table_exists(conn, "ticker_snapshots") or not table_exists(conn, "refresh_runs"):
        return None
    if table_has_column(conn, "ticker_snapshots", "snapshot_source"):
        row = conn.execute(
            """
            SELECT s.snapshot_source
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
            ORDER BY CASE WHEN s.snapshot_source = 'live' THEN 0 ELSE 1 END,
                     s.run_id DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    if table_has_column(conn, "refresh_runs", "provider"):
        row = conn.execute(
            """
            SELECT COALESCE(r.provider, 'live')
            FROM ticker_snapshots s
            JOIN refresh_runs r ON r.run_id = s.run_id
            WHERE r.status IN ('success', 'partial')
            ORDER BY CASE WHEN COALESCE(r.provider, 'live') = 'live' THEN 0 ELSE 1 END,
                     s.run_id DESC
            LIMIT 1
            """
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    row = conn.execute(
        """
        SELECT 1
        FROM ticker_snapshots s
        JOIN refresh_runs r ON r.run_id = s.run_id
        WHERE r.status IN ('success', 'partial')
        LIMIT 1
        """
    ).fetchone()
    return "live" if row else None


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
    if not table_exists(conn, "governed_ticker_onboarding"):
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
    if not table_exists(conn, "governed_ticker_onboarding"):
        return pd.DataFrame()
    preferred_snapshot_source = _preferred_ticker_snapshot_source(conn)
    if table_exists(conn, "ticker_snapshots") and table_exists(conn, "refresh_runs") and preferred_snapshot_source:
        snapshot_source_expr = (
            "s.snapshot_source"
            if table_has_column(conn, "ticker_snapshots", "snapshot_source")
            else ("COALESCE(r.provider, 'live')" if table_has_column(conn, "refresh_runs", "provider") else "'live'")
        )
        snapshot_cte = f"""
        ,
        preferred_snapshots AS (
            SELECT
                snapshot_rows.ticker,
                snapshot_rows.latest_current_snapshot_time,
                snapshot_rows.current_snapshot_source,
                snapshot_rows.price,
                snapshot_rows.avg_volume,
                snapshot_rows.perf_1w,
                snapshot_rows.perf_1m,
                snapshot_rows.perf_3m
            FROM (
                SELECT
                    upper(trim(s.ticker)) AS ticker,
                    r.finished_at AS latest_current_snapshot_time,
                    {snapshot_source_expr} AS current_snapshot_source,
                    s.price,
                    s.avg_volume,
                    s.perf_1w,
                    s.perf_1m,
                    s.perf_3m,
                    ROW_NUMBER() OVER (
                        PARTITION BY upper(trim(s.ticker))
                        ORDER BY s.run_id DESC
                    ) AS snapshot_rank
                FROM ticker_snapshots s
                JOIN refresh_runs r ON r.run_id = s.run_id
                WHERE r.status IN ('success', 'partial')
                  AND {snapshot_source_expr} = ?
            ) snapshot_rows
            WHERE snapshot_rows.snapshot_rank = 1
        )
        """
        snapshot_params: list[object] = [preferred_snapshot_source]
        snapshot_select = """
            ps.latest_current_snapshot_time,
            ps.current_snapshot_source,
            ps.price AS current_price,
            ps.avg_volume AS current_avg_volume,
            ps.perf_1w AS current_perf_1w,
            ps.perf_1m AS current_perf_1m,
            ps.perf_3m AS current_perf_3m,
        """
        snapshot_join = "LEFT JOIN preferred_snapshots ps ON ps.ticker = upper(trim(o.ticker))"
    else:
        snapshot_cte = ""
        snapshot_params = []
        snapshot_select = """
            NULL AS latest_current_snapshot_time,
            NULL AS current_snapshot_source,
            NULL AS current_price,
            NULL AS current_avg_volume,
            NULL AS current_perf_1w,
            NULL AS current_perf_1m,
            NULL AS current_perf_3m,
        """
        snapshot_join = ""

    df = conn.execute(
        """
        WITH membership AS (
            SELECT
                upper(trim(m.ticker)) AS ticker,
                COUNT(*) AS governed_assignment_count,
                STRING_AGG(t.name, ', ' ORDER BY t.name) AS governed_themes
            FROM theme_membership m
            JOIN themes t ON t.id = m.theme_id
            WHERE 1=1
        """
        + _manual_suppression_filter_sql(conn, "m.ticker")
        + """
            GROUP BY upper(trim(m.ticker))
        )
        """
        + snapshot_cte
        + """
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
        """
        + snapshot_select
        + """
            COALESCE(m.governed_assignment_count, 0) AS governed_assignment_count,
            m.governed_themes,
            o.updated_at
        FROM governed_ticker_onboarding o
        LEFT JOIN membership m ON m.ticker = upper(trim(o.ticker))
        """
        + snapshot_join
        + """
        WHERE COALESCE(m.governed_assignment_count, 0) > 0
        ORDER BY o.added_at DESC, o.ticker
        LIMIT ?
        """,
        [*snapshot_params, limit],
    ).df()
    if df.empty:
        return df

    out = df.copy()
    out["has_current_preferred_snapshot"] = pd.to_datetime(
        out.get("latest_current_snapshot_time"),
        errors="coerce",
    ).notna()
    current_value_cols = [
        "current_price",
        "current_avg_volume",
        "current_perf_1w",
        "current_perf_1m",
        "current_perf_3m",
    ]
    usable_snapshot = pd.Series(False, index=out.index)
    for column in current_value_cols:
        usable_snapshot = usable_snapshot | pd.to_numeric(out.get(column), errors="coerce").notna()
    out["has_current_usable_preferred_snapshot"] = out["has_current_preferred_snapshot"] & usable_snapshot

    def _propagation_status(row) -> str:
        if not bool(row.get("has_current_usable_preferred_snapshot")):
            return "needs_current_snapshot"
        if str(row.get("history_readiness_status") or "") != "ready":
            return "needs_history_backfill"
        if bool(row.get("downstream_refresh_needed")):
            return "needs_theme_reconstruction"
        return "ready_for_current_and_history"

    out["propagation_status"] = out.apply(_propagation_status, axis=1)
    return out


def governed_ticker_onboarding_counts(conn) -> pd.DataFrame:
    if not table_exists(conn, "governed_ticker_onboarding"):
        return pd.DataFrame()
    return conn.execute(
        """
        WITH visible_tickers AS (
            SELECT DISTINCT upper(trim(m.ticker)) AS ticker
            FROM theme_membership m
            WHERE 1=1
        """
        + _manual_suppression_filter_sql(conn, "m.ticker")
        + """
        )
        SELECT
            o.history_readiness_status,
            o.backfill_status,
            COUNT(*) AS cnt
        FROM governed_ticker_onboarding o
        JOIN visible_tickers v ON v.ticker = upper(trim(o.ticker))
        GROUP BY o.history_readiness_status, o.backfill_status
        ORDER BY o.history_readiness_status, o.backfill_status
        """
    ).df()


def run_governed_ticker_onboarding_backfill(
    conn,
    tickers: list[str],
    *,
    provider_name: str = DEFAULT_PROVIDER,
    target_trading_days: int = ONBOARDING_HISTORY_TARGET_DAYS,
    lookback_days: int = ONBOARDING_BACKFILL_WINDOW_DAYS,
    perform_current_snapshot_hydration: bool = True,
) -> dict[str, object]:
    normalized_tickers = sorted({_normalize_ticker(ticker) for ticker in tickers if _normalize_ticker(ticker)})
    if not normalized_tickers:
        return {"status": "no_scope", "tickers": [], "backfill_result": None}
    if not table_exists(conn, "governed_ticker_onboarding"):
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

    if perform_current_snapshot_hydration:
        current_snapshot_result = run_targeted_current_snapshot_hydration(
            conn,
            normalized_tickers,
            provider_name=provider_name,
        )
    else:
        current_snapshot_result = {
            "status": "skipped_by_caller",
            "tickers": normalized_tickers,
            "run_id": None,
        }
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
    if not table_exists(conn, "governed_ticker_onboarding"):
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


def complete_governed_ticker_onboarding(
    conn,
    tickers: list[str],
    *,
    provider_name: str = DEFAULT_PROVIDER,
    target_trading_days: int = ONBOARDING_HISTORY_TARGET_DAYS,
    lookback_days: int = ONBOARDING_BACKFILL_WINDOW_DAYS,
) -> dict[str, object]:
    from .fetch_data import run_targeted_current_snapshot_hydration

    normalized_tickers = sorted({_normalize_ticker(ticker) for ticker in tickers if _normalize_ticker(ticker)})
    if not normalized_tickers:
        return {
            "status": "no_scope",
            "tickers": [],
            "results": [],
            "completed_count": 0,
        }
    if not table_exists(conn, "governed_ticker_onboarding"):
        return {
            "status": "no_tracking_table",
            "tickers": normalized_tickers,
            "results": [],
            "completed_count": 0,
        }

    onboarding_view = list_governed_ticker_onboarding(conn, limit=max(1000, len(normalized_tickers) * 10))
    onboarding_by_ticker = {
        str(row["ticker"]).strip().upper(): row
        for _, row in onboarding_view.iterrows()
        if str(row.get("ticker") or "").strip()
    }

    results: list[dict[str, object]] = []
    completed_count = 0
    advanced_any = False
    partial_any = False

    for ticker in normalized_tickers:
        initial_row = onboarding_by_ticker.get(ticker)
        if initial_row is None:
            results.append(
                {
                    "ticker": ticker,
                    "status": "not_tracked",
                    "current_hydration": {"status": "not_tracked", "advanced": False},
                    "history_backfill": {"status": "not_tracked", "advanced": False},
                    "theme_reconstruction": {"status": "not_tracked", "advanced": False},
                    "initial_propagation_status": None,
                    "final_propagation_status": None,
                    "completed": False,
                }
            )
            partial_any = True
            continue

        initial_propagation_status = str(initial_row.get("propagation_status") or "")
        current_stage = {"status": "already_current", "advanced": False, "run_id": None}
        history_stage = {"status": "already_sufficient", "advanced": False}
        reconstruction_stage = {"status": "already_current", "advanced": False}

        ran_current_stage = False
        if not bool(initial_row.get("has_current_usable_preferred_snapshot")):
            current_result = run_targeted_current_snapshot_hydration(
                conn,
                [ticker],
                provider_name=provider_name,
            )
            ran_current_stage = True
            current_stage = {
                "status": "hydrated" if str(current_result.get("status") or "") == "success" else str(current_result.get("status") or "unknown"),
                "advanced": str(current_result.get("status") or "") == "success",
                "run_id": current_result.get("run_id"),
            }

        row_after_current_df = list_governed_ticker_onboarding(conn, limit=max(1000, len(normalized_tickers) * 10))
        row_after_current_match = row_after_current_df[row_after_current_df["ticker"].astype(str).str.upper() == ticker]
        row_after_current = row_after_current_match.iloc[0] if not row_after_current_match.empty else initial_row

        history_ready = str(row_after_current.get("history_readiness_status") or "") == "ready"
        if not history_ready:
            history_result = run_governed_ticker_onboarding_backfill(
                conn,
                [ticker],
                provider_name=provider_name,
                target_trading_days=target_trading_days,
                lookback_days=lookback_days,
                perform_current_snapshot_hydration=not ran_current_stage,
            )
            row_after_history_df = list_governed_ticker_onboarding(conn, limit=max(1000, len(normalized_tickers) * 10))
            row_after_history_match = row_after_history_df[row_after_history_df["ticker"].astype(str).str.upper() == ticker]
            row_after_history = row_after_history_match.iloc[0] if not row_after_history_match.empty else row_after_current
            history_now_ready = str(row_after_history.get("history_readiness_status") or "") == "ready"
            history_stage = {
                "status": (
                    "completed"
                    if history_now_ready
                    else str(history_result.get("status") or "unknown")
                ),
                "advanced": history_now_ready,
                "backfill_status": str(row_after_history.get("backfill_status") or ""),
            }
        else:
            row_after_history = row_after_current

        can_reconstruct = (
            str(row_after_history.get("history_readiness_status") or "") == "ready"
            and bool(row_after_history.get("downstream_refresh_needed"))
        )
        if can_reconstruct:
            reconstruction_result = run_governed_ticker_onboarding_theme_reconstruction(
                conn,
                [ticker],
                provider_name=provider_name,
                lookback_days=lookback_days,
            )
            row_after_reconstruction_df = list_governed_ticker_onboarding(conn, limit=max(1000, len(normalized_tickers) * 10))
            row_after_reconstruction_match = row_after_reconstruction_df[row_after_reconstruction_df["ticker"].astype(str).str.upper() == ticker]
            row_after_reconstruction = row_after_reconstruction_match.iloc[0] if not row_after_reconstruction_match.empty else row_after_history
            reconstruction_cleared = not bool(row_after_reconstruction.get("downstream_refresh_needed"))
            reconstruction_stage = {
                "status": "completed" if reconstruction_cleared else str(reconstruction_result.get("status") or "unknown"),
                "advanced": reconstruction_cleared,
            }
        else:
            row_after_reconstruction = row_after_history
            if str(row_after_history.get("history_readiness_status") or "") != "ready":
                reconstruction_stage = {"status": "blocked_needs_history", "advanced": False}

        final_propagation_status = str(row_after_reconstruction.get("propagation_status") or "")
        completed = final_propagation_status == "ready_for_current_and_history"
        if completed:
            completed_count += 1
        if any(bool(stage.get("advanced")) for stage in [current_stage, history_stage, reconstruction_stage]):
            advanced_any = True
        if not completed and initial_propagation_status != "ready_for_current_and_history":
            partial_any = True

        row_status = "already_complete" if initial_propagation_status == "ready_for_current_and_history" else ("completed" if completed else "partial")
        results.append(
            {
                "ticker": ticker,
                "status": row_status,
                "current_hydration": current_stage,
                "history_backfill": history_stage,
                "theme_reconstruction": reconstruction_stage,
                "initial_propagation_status": initial_propagation_status,
                "final_propagation_status": final_propagation_status,
                "completed": completed,
            }
        )

    if completed_count == len(normalized_tickers) and not advanced_any:
        overall_status = "no_change"
    elif completed_count == len(normalized_tickers):
        overall_status = "completed"
    elif partial_any or advanced_any:
        overall_status = "partial"
    else:
        overall_status = "no_change"
    return {
        "status": overall_status,
        "tickers": normalized_tickers,
        "results": results,
        "completed_count": completed_count,
    }
