from __future__ import annotations

from datetime import UTC, date, datetime

from typing import Callable

from .fetch_data import run_refresh
from .historical_backfill import run_daily_historical_append
from .rankings import backfill_canonical_theme_daily_snapshots_for_recent_trading_days, persist_canonical_theme_daily_snapshot_for_run
from .theme_service import active_ticker_universe
from .trading_day_status import (
    EASTERN_TZ,
    current_et,
    finalization_eligible,
    is_trading_day,
    latest_finalizable_trading_date,
    reached_eod_window,
)


def _as_et(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(EASTERN_TZ)


def _finished_at_is_finalized_for_date(finished_at: datetime | None, target_date) -> bool:
    finished_et = _as_et(finished_at)
    if finished_et is None:
        return False
    return bool(finished_et.date() == target_date and reached_eod_window(finished_et))


def has_eod_run_for_date(conn, as_of_et: datetime) -> bool:
    target_date = as_of_et.date()
    rows = conn.execute(
        """
        SELECT finished_at
        FROM refresh_runs
        WHERE status IN ('success', 'partial')
          AND scope_type = 'scheduled_eod'
          AND finished_at IS NOT NULL
        ORDER BY run_id DESC
        """
    ).fetchall()
    for (finished_at,) in rows:
        if finished_at is None:
            continue
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=UTC)
        if finished_at.astimezone(EASTERN_TZ).date() == target_date:
            return True
    return False


def has_historical_append_for_date(
    conn,
    as_of_et: datetime | None = None,
    provenance_source_label: str = "daily_historical_append",
    *,
    target_date: date | None = None,
) -> bool:
    if conn.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'historical_reconstruction_runs'").fetchone()[0] == 0:
        return False
    effective_target_date = target_date or (as_of_et.date() if as_of_et is not None else None)
    if effective_target_date is None:
        return False
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM historical_reconstruction_runs
        WHERE status IN ('success', 'partial')
          AND provenance_source_label = ?
          AND start_date = ?
          AND end_date = ?
        """,
        [provenance_source_label, effective_target_date, effective_target_date],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def has_finalized_historical_append_for_date(
    conn,
    as_of_et: datetime | None = None,
    provenance_source_label: str = "daily_historical_append",
    *,
    target_date: date | None = None,
) -> bool:
    if conn.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'historical_reconstruction_runs'").fetchone()[0] == 0:
        return False
    effective_target_date = target_date or (as_of_et.date() if as_of_et is not None else None)
    if effective_target_date is None:
        return False
    rows = conn.execute(
        """
        SELECT finished_at
        FROM historical_reconstruction_runs
        WHERE status IN ('success', 'partial')
          AND provenance_source_label = ?
          AND start_date = ?
          AND end_date = ?
        ORDER BY run_id DESC
        """,
        [provenance_source_label, effective_target_date, effective_target_date],
    ).fetchall()
    return any(_finished_at_is_finalized_for_date(finished_at, effective_target_date) for (finished_at,) in rows)


def run_scheduled_eod_refresh(
    conn,
    provider_name: str = "live",
    force: bool = False,
    *,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> int | None:
    now_et = current_et()
    if not force:
        if not is_trading_day(now_et) or not reached_eod_window(now_et):
            return None
        if has_eod_run_for_date(conn, now_et):
            return None

    tickers = active_ticker_universe(conn)
    if not tickers:
        return None

    run_id = run_refresh(
        conn,
        provider_name,
        tickers=tickers,
        progress_callback=progress_callback,
        scope_type="scheduled_eod",
        scope_theme_name=None,
    )
    if run_id is not None:
        persist_canonical_theme_daily_snapshot_for_run(
            conn,
            run_id,
            extract_session="after_hours_official",
            canonical_reason="scheduled_eod_refresh",
            is_canonical_daily=True,
        )
    return run_id


def run_scheduled_historical_append(
    conn,
    provider_name: str = "live",
    force: bool = False,
    *,
    target_date: date | None = None,
    replace_existing: bool = False,
) -> dict[str, object] | None:
    now_et = current_et()
    effective_target_date = target_date or now_et.date()
    if not force:
        if not is_trading_day(now_et) or not reached_eod_window(now_et):
            return None
        if has_historical_append_for_date(conn, now_et, target_date=effective_target_date):
            return None

    return run_daily_historical_append(
        conn,
        provider_name=provider_name,
        target_date=effective_target_date,
        replace_existing=bool(replace_existing),
    )


def latest_expected_trading_date(conn, provider_name: str = "live"):
    row = conn.execute(
        """
        SELECT MAX(trading_date)
        FROM ticker_daily_history
        WHERE market_data_source = ?
        """,
        [provider_name],
    ).fetchone()
    return row[0] if row and row[0] else None


def latest_canonical_snapshot_date(conn):
    row = conn.execute(
        """
        SELECT MAX(snapshot_date)
        FROM canonical_theme_daily_snapshots
        """
    ).fetchone()
    return row[0] if row and row[0] else None


def latest_historical_append_date(conn):
    if conn.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'historical_reconstruction_runs'").fetchone()[0] == 0:
        return None
    row = conn.execute(
        """
        SELECT MAX(end_date)
        FROM historical_reconstruction_runs
        WHERE status IN ('success', 'partial')
          AND provenance_source_label = 'daily_historical_append'
        """
    ).fetchone()
    return row[0] if row and row[0] else None


def canonical_snapshot_finalization_state(conn, snapshot_date) -> dict[str, object]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(*) FILTER (WHERE canonical_rank IS NOT NULL) AS ranked_row_count
        FROM canonical_theme_daily_snapshots
        WHERE snapshot_date = ?
        """,
        [snapshot_date],
    ).fetchone()
    existing_count = int((row[0] if row else 0) or 0)
    ranked_row_count = int((row[1] if row else 0) or 0)
    run_rows = conn.execute(
        """
        SELECT DISTINCT r.scope_type, r.finished_at
        FROM canonical_theme_daily_snapshots c
        LEFT JOIN refresh_runs r ON r.run_id = c.run_id
        WHERE c.snapshot_date = ?
        """,
        [snapshot_date],
    ).fetchall()
    finalized_via_refresh_run = any(
        str(scope_type or "") == "scheduled_eod" and _finished_at_is_finalized_for_date(finished_at, snapshot_date)
        for scope_type, finished_at in run_rows
    )
    latest_run_finished_at = next(
        (finished_at for _scope_type, finished_at in run_rows if finished_at is not None),
        None,
    )
    return {
        "row_count": existing_count,
        "ranked_row_count": ranked_row_count,
        "finalized_via_refresh_run": finalized_via_refresh_run,
        "latest_run_finished_at": latest_run_finished_at,
    }


def refresh_run_stage_summary(conn, run_id: int | None) -> dict[str, object]:
    if run_id is None:
        return {
            "tickers_requested": 0,
            "tickers_updated": 0,
            "tickers_failed": 0,
            "tickers_suppressed": 0,
            "tickers_skipped": 0,
            "flagged_tickers": 0,
            "api_call_count": 0,
            "failed_tickers": [],
        }

    run_row = conn.execute(
        """
        SELECT
            ticker_count,
            success_count,
            failure_count,
            skipped_tickers,
            flagged_symbol_count,
            suppressed_symbol_count,
            api_call_count
        FROM refresh_runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()
    failed_rows = conn.execute(
        """
        SELECT DISTINCT upper(trim(ticker)) AS ticker
        FROM refresh_failures
        WHERE run_id = ?
          AND ticker IS NOT NULL
          AND trim(ticker) <> ''
        ORDER BY 1
        """,
        [run_id],
    ).fetchall()
    failed_tickers = [str(row[0]) for row in failed_rows if row and row[0]]
    if run_row is None:
        return {
            "tickers_requested": 0,
            "tickers_updated": 0,
            "tickers_failed": len(failed_tickers),
            "tickers_suppressed": 0,
            "tickers_skipped": len(failed_tickers),
            "flagged_tickers": 0,
            "api_call_count": 0,
            "failed_tickers": failed_tickers,
        }

    skipped_csv = str(run_row[3] or "").strip()
    skipped_tickers = [ticker.strip().upper() for ticker in skipped_csv.split(",") if ticker.strip()]
    return {
        "tickers_requested": int(run_row[0] or 0),
        "tickers_updated": int(run_row[1] or 0),
        "tickers_failed": int(run_row[2] or 0),
        "tickers_suppressed": int(run_row[5] or 0),
        "tickers_skipped": len(skipped_tickers),
        "flagged_tickers": int(run_row[4] or 0),
        "api_call_count": int(run_row[6] or 0),
        "failed_tickers": failed_tickers,
    }


def materialize_latest_canonical_day(
    conn,
    provider_name: str = "live",
    *,
    overwrite_existing: bool = False,
    as_of_et: datetime | None = None,
) -> dict[str, object]:
    latest_expected_date = latest_expected_trading_date(conn, provider_name=provider_name)
    latest_canonical_before = latest_canonical_snapshot_date(conn)
    effective_as_of = as_of_et or current_et()
    if latest_expected_date is None:
        return {
            "status": "no_ticker_history",
            "latest_expected_trading_date": None,
            "latest_canonical_snapshot_date_before": latest_canonical_before,
            "latest_canonical_snapshot_date_after": latest_canonical_before,
            "advanced": False,
            "row_count": 0,
            "inserted_count": 0,
            "same_day_repair_performed": False,
            "finalization_eligible": finalization_eligible(effective_as_of),
        }

    finalization_ready = finalization_eligible(effective_as_of)
    existing_state = canonical_snapshot_finalization_state(conn, latest_expected_date)
    existing_count = int(existing_state.get("row_count") or 0)
    existing_ranked_count = int(existing_state.get("ranked_row_count") or 0)
    needs_rankability_repair = existing_count > 0 and existing_ranked_count <= 0
    same_day_intraday_repair_needed = bool(
        finalization_ready
        and str(latest_expected_date) == str(effective_as_of.date())
        and existing_count > 0
        and not bool(existing_state.get("finalized_via_refresh_run"))
    )
    effective_overwrite_existing = bool(overwrite_existing or needs_rankability_repair or same_day_intraday_repair_needed)
    if existing_count > 0 and existing_ranked_count > 0 and not effective_overwrite_existing:
        latest_canonical_after = latest_canonical_snapshot_date(conn)
        return {
            "status": "already_current",
            "latest_expected_trading_date": latest_expected_date,
            "latest_canonical_snapshot_date_before": latest_canonical_before,
            "latest_canonical_snapshot_date_after": latest_canonical_after,
            "advanced": False,
            "row_count": existing_count,
            "inserted_count": 0,
            "ranked_row_count": existing_ranked_count,
            "repaired_unranked_existing": False,
            "same_day_repair_performed": False,
            "finalization_eligible": finalization_ready,
        }

    materialize_result = backfill_canonical_theme_daily_snapshots_for_recent_trading_days(
        conn,
        recent_trading_day_limit=1,
        provider=provider_name,
        overwrite_existing=effective_overwrite_existing,
    )
    latest_canonical_after = latest_canonical_snapshot_date(conn)
    result_rows = list(materialize_result.get("results") or [])
    matching_result = next(
        (
            row
            for row in result_rows
            if str(row.get("snapshot_date") or "") == str(latest_expected_date)
        ),
        None,
    )
    missing_match = next(
        (
            row
            for row in list(materialize_result.get("missing_dates") or [])
            if str(row.get("snapshot_date") or "") == str(latest_expected_date)
        ),
        None,
    )
    if matching_result is not None:
        inserted_count = int(matching_result.get("inserted_count") or 0)
        row_count = int(matching_result.get("row_count") or 0)
        ranked_row_count = int(matching_result.get("ranked_row_count") or 0)
        status = str(matching_result.get("status") or "materialized")
    elif missing_match is not None:
        inserted_count = 0
        row_count = 0
        ranked_row_count = 0
        status = str(missing_match.get("reason") or "missing")
    else:
        inserted_count = 0
        row_count = existing_count
        ranked_row_count = existing_ranked_count
        status = "no_change"

    if same_day_intraday_repair_needed and status in {"history_repaired", "materialized_from_run"}:
        status = "repaired_intraday_same_day"
    elif needs_rankability_repair and status in {"history_repaired", "materialized_from_run"}:
        status = "repaired_unranked_existing"

    latest_expected_text = str(latest_expected_date) if latest_expected_date is not None else None
    latest_canonical_before_text = str(latest_canonical_before) if latest_canonical_before is not None else None
    latest_canonical_after_text = str(latest_canonical_after) if latest_canonical_after is not None else None
    return {
        "status": status,
        "latest_expected_trading_date": latest_expected_date,
        "latest_canonical_snapshot_date_before": latest_canonical_before,
        "latest_canonical_snapshot_date_after": latest_canonical_after,
        "advanced": bool(
            latest_canonical_after_text == latest_expected_text
            and latest_canonical_before_text != latest_canonical_after_text
        ),
        "row_count": row_count,
        "inserted_count": inserted_count,
        "ranked_row_count": ranked_row_count,
        "repaired_unranked_existing": needs_rankability_repair,
        "same_day_repair_performed": same_day_intraday_repair_needed,
        "finalization_eligible": finalization_ready,
        "backfill_result": materialize_result,
    }


def run_latest_daily_sync(
    conn,
    provider_name: str = "live",
    *,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    now_et = current_et()
    trading_day = is_trading_day(now_et)
    can_finalize_today = finalization_eligible(now_et)
    target_date = now_et.date()
    finalization_target_date = latest_finalizable_trading_date(now_et)
    latest_expected_before = latest_expected_trading_date(conn, provider_name=provider_name)
    latest_canonical_before = latest_canonical_snapshot_date(conn)
    latest_historical_before = latest_historical_append_date(conn)

    stages: dict[str, dict[str, object]] = {}

    def emit_progress(stage: str, **fields) -> None:
        if progress_callback is None:
            return
        progress_callback({"stage": stage, **fields})

    if not trading_day:
        emit_progress(
            "live_refresh",
            stage_label="Live refresh",
            stage_status="skipped_non_trading_day",
            summary="Live refresh skipped because today is not a trading day.",
            detail=f"Target date `{target_date}`.",
        )
        stages["live_refresh"] = {
            "status": "skipped_non_trading_day",
            "run_id": None,
            "target_date": target_date,
            "finalization_eligible": can_finalize_today,
            "scope_type": None,
        }
    elif can_finalize_today and has_eod_run_for_date(conn, now_et):
        emit_progress(
            "live_refresh",
            stage_label="Live refresh",
            stage_status="already_current",
            summary="Live refresh already finalized for today.",
            detail=f"Target date `{target_date}`.",
        )
        stages["live_refresh"] = {
            "status": "already_current",
            "run_id": None,
            "target_date": target_date,
            "finalization_eligible": can_finalize_today,
            "scope_type": "scheduled_eod",
            "refresh_run_summary": refresh_run_stage_summary(conn, None),
        }
    else:
        try:
            if can_finalize_today:
                emit_progress(
                    "live_refresh",
                    stage_label="Live refresh",
                    stage_status="running",
                    summary="Running live refresh for the full active ticker universe.",
                    detail="Progress will update as each ticker completes.",
                )
                run_id = run_scheduled_eod_refresh(
                    conn,
                    provider_name=provider_name,
                    force=True,
                    progress_callback=lambda payload: emit_progress(
                        "live_refresh",
                        stage_label="Live refresh",
                        stage_status="running",
                        **payload,
                    ),
                )
                scope_type = "scheduled_eod"
            else:
                tickers = active_ticker_universe(conn)
                if tickers:
                    emit_progress(
                        "live_refresh",
                        stage_label="Live refresh",
                        stage_status="running",
                        summary="Running live refresh for the full active ticker universe.",
                        detail="Progress will update as each ticker completes.",
                    )
                run_id = (
                    run_refresh(
                        conn,
                        provider_name,
                        tickers=tickers,
                        progress_callback=lambda payload: emit_progress(
                            "live_refresh",
                            stage_label="Live refresh",
                            stage_status="running",
                            **payload,
                        ),
                        scope_type="daily_sync_live",
                        scope_theme_name=None,
                    )
                    if tickers
                    else None
                )
                scope_type = "daily_sync_live"
            stages["live_refresh"] = {
                "status": "refreshed" if run_id is not None else "not_run",
                "run_id": run_id,
                "target_date": target_date,
                "finalization_eligible": can_finalize_today,
                "scope_type": scope_type,
                "refresh_run_summary": refresh_run_stage_summary(conn, run_id),
            }
            emit_progress(
                "live_refresh",
                stage_label="Live refresh",
                stage_status=stages["live_refresh"]["status"],
                summary="Live refresh stage finished.",
                detail=f"Run #{int(run_id)} complete." if run_id is not None else "No live refresh run was needed.",
                run_id=run_id,
                refresh_run_summary=stages["live_refresh"]["refresh_run_summary"],
            )
        except Exception as exc:
            stages["live_refresh"] = {
                "status": "failed",
                "run_id": None,
                "target_date": target_date,
                "finalization_eligible": can_finalize_today,
                "scope_type": "scheduled_eod" if can_finalize_today else "daily_sync_live",
                "error": str(exc),
                "refresh_run_summary": refresh_run_stage_summary(conn, None),
            }
            emit_progress(
                "live_refresh",
                stage_label="Live refresh",
                stage_status="failed",
                summary="Live refresh failed.",
                detail=str(exc),
            )

    if not trading_day:
        emit_progress(
            "historical_append",
            stage_label="Historical append",
            stage_status="skipped_non_trading_day",
            summary="Historical append skipped because today is not a trading day.",
            detail=f"Target date `{target_date}`.",
        )
        stages["historical_append"] = {
            "status": "skipped_non_trading_day",
            "target_date": target_date,
            "latest_historical_snapshot_date_before": latest_historical_before,
            "latest_historical_snapshot_date_after": latest_historical_before,
            "advanced": False,
            "finalization_eligible": can_finalize_today,
            "same_day_repair_performed": False,
        }
    elif finalization_target_date is None:
        emit_progress(
            "historical_append",
            stage_label="Historical append",
            stage_status="deferred_until_eod",
            summary="Historical append deferred until the EOD window.",
            detail=f"Target date `{target_date}`.",
        )
        stages["historical_append"] = {
            "status": "deferred_until_eod",
            "target_date": target_date,
            "latest_historical_snapshot_date_before": latest_historical_before,
            "latest_historical_snapshot_date_after": latest_historical_before,
            "advanced": False,
            "finalization_eligible": can_finalize_today,
            "same_day_repair_performed": False,
        }
    elif has_finalized_historical_append_for_date(conn, now_et, target_date=finalization_target_date):
        emit_progress(
            "historical_append",
            stage_label="Historical append",
            stage_status="already_current",
            summary="Historical append already finalized for today.",
            detail=f"Target date `{finalization_target_date}`.",
        )
        stages["historical_append"] = {
            "status": "already_current",
            "target_date": finalization_target_date,
            "latest_historical_snapshot_date_before": latest_historical_before,
            "latest_historical_snapshot_date_after": latest_historical_before,
            "advanced": False,
            "finalization_eligible": can_finalize_today,
            "same_day_repair_performed": False,
        }
    else:
        try:
            emit_progress(
                "historical_append",
                stage_label="Historical append",
                stage_status="running",
                summary="Building latest-day historical rows.",
                detail=f"Target date `{finalization_target_date}`.",
            )
            same_day_intraday_append_exists = has_historical_append_for_date(conn, now_et, target_date=finalization_target_date)
            append_result = run_scheduled_historical_append(
                conn,
                provider_name=provider_name,
                force=True,
                target_date=finalization_target_date,
                replace_existing=same_day_intraday_append_exists,
            )
            latest_historical_after = latest_historical_append_date(conn)
            append_status = str((append_result or {}).get("status") or "not_run")
            raw_failed_tickers = list((append_result or {}).get("failed_tickers") or [])
            ticker_history_rows_written = int((append_result or {}).get("ticker_history_rows_written") or 0)
            snapshot_rows_written = int((append_result or {}).get("snapshot_rows_written") or 0)
            historical_current_for_target_day = bool(str(latest_historical_after or "") == str(finalization_target_date))
            reused_existing_same_day_state = bool(
                same_day_intraday_append_exists
                and historical_current_for_target_day
                and ticker_history_rows_written == 0
                and snapshot_rows_written == 0
                and bool(raw_failed_tickers)
            )
            reported_failed_tickers = [] if reused_existing_same_day_state else raw_failed_tickers
            if same_day_intraday_append_exists and append_status in {"success", "partial"}:
                append_status = "repaired_intraday_same_day"
            stages["historical_append"] = {
                "status": append_status,
                "target_date": finalization_target_date,
                "snapshot_rows_written": snapshot_rows_written,
                "ticker_history_rows_written": ticker_history_rows_written,
                "ticker_history_rows_skipped": int((append_result or {}).get("ticker_history_rows_skipped") or 0),
                "snapshot_rows_skipped": int((append_result or {}).get("snapshot_rows_skipped") or 0),
                "failed_tickers": reported_failed_tickers,
                "raw_failed_tickers": raw_failed_tickers,
                "available_snapshot_dates": list((append_result or {}).get("available_snapshot_dates") or []),
                "latest_historical_snapshot_date_before": latest_historical_before,
                "latest_historical_snapshot_date_after": latest_historical_after,
                "finalization_eligible": can_finalize_today,
                "same_day_repair_performed": same_day_intraday_append_exists,
                "reused_existing_same_day_state": reused_existing_same_day_state,
                "historical_current_for_target_day": historical_current_for_target_day,
                "advanced": bool(
                    str(latest_historical_after or "") == str(finalization_target_date)
                    and str(latest_historical_before or "") != str(latest_historical_after or "")
                ),
            }
            emit_progress(
                "historical_append",
                stage_label="Historical append",
                stage_status=append_status,
                summary="Historical append stage finished.",
                detail=(
                    f"Ticker rows `{int((append_result or {}).get('ticker_history_rows_written') or 0)}` | "
                    f"Theme rows `{int((append_result or {}).get('snapshot_rows_written') or 0)}`."
                ),
            )
        except Exception as exc:
            stages["historical_append"] = {
                "status": "failed",
                "target_date": finalization_target_date,
                "error": str(exc),
                "latest_historical_snapshot_date_before": latest_historical_before,
                "latest_historical_snapshot_date_after": latest_historical_append_date(conn),
                "advanced": False,
                "finalization_eligible": can_finalize_today,
                "same_day_repair_performed": False,
            }
            emit_progress(
                "historical_append",
                stage_label="Historical append",
                stage_status="failed",
                summary="Historical append failed.",
                detail=str(exc),
            )

    if not trading_day:
        emit_progress(
            "canonical_materialization",
            stage_label="Canonical materialization",
            stage_status="skipped_non_trading_day",
            summary="Canonical materialization skipped because today is not a trading day.",
            detail=f"Target date `{target_date}`.",
        )
        stages["canonical_materialization"] = {
            "status": "skipped_non_trading_day",
            "latest_expected_trading_date": latest_expected_before,
            "latest_canonical_snapshot_date_before": latest_canonical_before,
            "latest_canonical_snapshot_date_after": latest_canonical_before,
            "advanced": False,
            "row_count": 0,
            "inserted_count": 0,
            "finalization_eligible": can_finalize_today,
            "same_day_repair_performed": False,
        }
    elif finalization_target_date is None:
        emit_progress(
            "canonical_materialization",
            stage_label="Canonical materialization",
            stage_status="deferred_until_eod",
            summary="Canonical materialization deferred until the EOD window.",
            detail=f"Target date `{target_date}`.",
        )
        stages["canonical_materialization"] = {
            "status": "deferred_until_eod",
            "latest_expected_trading_date": latest_expected_before,
            "latest_canonical_snapshot_date_before": latest_canonical_before,
            "latest_canonical_snapshot_date_after": latest_canonical_before,
            "advanced": False,
            "row_count": 0,
            "inserted_count": 0,
            "finalization_eligible": can_finalize_today,
            "same_day_repair_performed": False,
        }
    else:
        try:
            emit_progress(
                "canonical_materialization",
                stage_label="Canonical materialization",
                stage_status="running",
                summary="Materializing the latest canonical day.",
                detail=f"Target date `{finalization_target_date}`.",
            )
            stages["canonical_materialization"] = materialize_latest_canonical_day(
                conn,
                provider_name=provider_name,
                as_of_et=now_et,
            )
            emit_progress(
                "canonical_materialization",
                stage_label="Canonical materialization",
                stage_status=stages["canonical_materialization"].get("status"),
                summary="Canonical materialization stage finished.",
                detail=(
                    f"Inserted rows `{int(stages['canonical_materialization'].get('inserted_count') or 0)}` | "
                    f"Ranked rows `{int(stages['canonical_materialization'].get('ranked_row_count') or 0)}`."
                ),
            )
        except Exception as exc:
            stages["canonical_materialization"] = {
                "status": "failed",
                "latest_expected_trading_date": latest_expected_trading_date(conn, provider_name=provider_name),
                "latest_canonical_snapshot_date_before": latest_canonical_before,
                "latest_canonical_snapshot_date_after": latest_canonical_snapshot_date(conn),
                "advanced": False,
                "row_count": 0,
                "inserted_count": 0,
                "finalization_eligible": can_finalize_today,
                "same_day_repair_performed": False,
                "error": str(exc),
            }
            emit_progress(
                "canonical_materialization",
                stage_label="Canonical materialization",
                stage_status="failed",
                summary="Canonical materialization failed.",
                detail=str(exc),
            )

    latest_expected_after = latest_expected_trading_date(conn, provider_name=provider_name)
    latest_canonical_after = latest_canonical_snapshot_date(conn)
    latest_historical_after = latest_historical_append_date(conn)
    stage_statuses = [str(stage.get("status") or "") for stage in stages.values()]
    if any(status == "failed" for status in stage_statuses):
        overall_status = "partial"
    elif any(status in {"refreshed", "success", "partial", "history_repaired", "materialized", "repaired_intraday_same_day"} for status in stage_statuses):
        overall_status = "success"
    else:
        overall_status = "no_change"

    return {
        "status": overall_status,
        "provider_name": provider_name,
        "target_date": target_date,
        "finalization_target_date": finalization_target_date,
        "trading_day": trading_day,
        "finalization_eligible": can_finalize_today,
        "latest_expected_trading_date_before": latest_expected_before,
        "latest_expected_trading_date_after": latest_expected_after,
        "latest_historical_snapshot_date_before": latest_historical_before,
        "latest_historical_snapshot_date_after": latest_historical_after,
        "latest_canonical_snapshot_date_before": latest_canonical_before,
        "latest_canonical_snapshot_date_after": latest_canonical_after,
        "stages": stages,
    }
