from __future__ import annotations

from datetime import UTC, datetime, time
from zoneinfo import ZoneInfo

from .fetch_data import run_refresh
from .historical_backfill import run_daily_historical_append
from .rankings import backfill_canonical_theme_daily_snapshots_for_recent_trading_days, persist_canonical_theme_daily_snapshot_for_run
from .theme_service import active_ticker_universe

EASTERN_TZ = ZoneInfo("America/New_York")


def current_et(now_utc: datetime | None = None) -> datetime:
    now = now_utc or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return now.astimezone(EASTERN_TZ)


def is_trading_day(dt_et: datetime) -> bool:
    return dt_et.weekday() < 5


def reached_eod_window(dt_et: datetime, target_hour: int = 18, target_minute: int = 0) -> bool:
    return dt_et.time() >= time(hour=target_hour, minute=target_minute)


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


def has_historical_append_for_date(conn, as_of_et: datetime, provenance_source_label: str = "daily_historical_append") -> bool:
    if conn.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'historical_reconstruction_runs'").fetchone()[0] == 0:
        return False
    target_date = as_of_et.date()
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM historical_reconstruction_runs
        WHERE status IN ('success', 'partial')
          AND provenance_source_label = ?
          AND start_date = ?
          AND end_date = ?
        """,
        [provenance_source_label, target_date, target_date],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def run_scheduled_eod_refresh(conn, provider_name: str = "live", force: bool = False) -> int | None:
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


def run_scheduled_historical_append(conn, provider_name: str = "live", force: bool = False) -> dict[str, object] | None:
    now_et = current_et()
    target_date = now_et.date()
    if not force:
        if not is_trading_day(now_et) or not reached_eod_window(now_et):
            return None
        if has_historical_append_for_date(conn, now_et):
            return None

    return run_daily_historical_append(
        conn,
        provider_name=provider_name,
        target_date=target_date,
        replace_existing=False,
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


def materialize_latest_canonical_day(
    conn,
    provider_name: str = "live",
    *,
    overwrite_existing: bool = False,
) -> dict[str, object]:
    latest_expected_date = latest_expected_trading_date(conn, provider_name=provider_name)
    latest_canonical_before = latest_canonical_snapshot_date(conn)
    if latest_expected_date is None:
        return {
            "status": "no_ticker_history",
            "latest_expected_trading_date": None,
            "latest_canonical_snapshot_date_before": latest_canonical_before,
            "latest_canonical_snapshot_date_after": latest_canonical_before,
            "advanced": False,
            "row_count": 0,
            "inserted_count": 0,
        }

    existing_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM canonical_theme_daily_snapshots
            WHERE snapshot_date = ?
            """,
            [latest_expected_date],
        ).fetchone()[0]
    )
    existing_ranked_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM canonical_theme_daily_snapshots
            WHERE snapshot_date = ?
              AND canonical_rank IS NOT NULL
            """,
            [latest_expected_date],
        ).fetchone()[0]
    )
    needs_rankability_repair = existing_count > 0 and existing_ranked_count <= 0
    if existing_count > 0 and existing_ranked_count > 0 and not overwrite_existing:
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
        }

    materialize_result = backfill_canonical_theme_daily_snapshots_for_recent_trading_days(
        conn,
        recent_trading_day_limit=1,
        provider=provider_name,
        overwrite_existing=bool(overwrite_existing or needs_rankability_repair),
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

    if needs_rankability_repair and status in {"history_repaired", "materialized_from_run"}:
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
        "backfill_result": materialize_result,
    }


def run_latest_daily_sync(conn, provider_name: str = "live") -> dict[str, object]:
    now_et = current_et()
    trading_day = is_trading_day(now_et)
    target_date = now_et.date()
    latest_expected_before = latest_expected_trading_date(conn, provider_name=provider_name)
    latest_canonical_before = latest_canonical_snapshot_date(conn)

    stages: dict[str, dict[str, object]] = {}

    if not trading_day:
        stages["live_refresh"] = {
            "status": "skipped_non_trading_day",
            "run_id": None,
            "target_date": target_date,
        }
    elif has_eod_run_for_date(conn, now_et):
        stages["live_refresh"] = {
            "status": "already_current",
            "run_id": None,
            "target_date": target_date,
        }
    else:
        try:
            run_id = run_scheduled_eod_refresh(conn, provider_name=provider_name, force=True)
            stages["live_refresh"] = {
                "status": "refreshed" if run_id is not None else "not_run",
                "run_id": run_id,
                "target_date": target_date,
            }
        except Exception as exc:
            stages["live_refresh"] = {
                "status": "failed",
                "run_id": None,
                "target_date": target_date,
                "error": str(exc),
            }

    if not trading_day:
        stages["historical_append"] = {
            "status": "skipped_non_trading_day",
            "target_date": target_date,
        }
    elif has_historical_append_for_date(conn, now_et):
        stages["historical_append"] = {
            "status": "already_current",
            "target_date": target_date,
        }
    else:
        try:
            append_result = run_scheduled_historical_append(conn, provider_name=provider_name, force=True)
            stages["historical_append"] = {
                "status": str((append_result or {}).get("status") or "not_run"),
                "target_date": target_date,
                "snapshot_rows_written": int((append_result or {}).get("snapshot_rows_written") or 0),
                "ticker_history_rows_written": int((append_result or {}).get("ticker_history_rows_written") or 0),
                "failed_tickers": list((append_result or {}).get("failed_tickers") or []),
                "available_snapshot_dates": list((append_result or {}).get("available_snapshot_dates") or []),
            }
        except Exception as exc:
            stages["historical_append"] = {
                "status": "failed",
                "target_date": target_date,
                "error": str(exc),
            }

    try:
        stages["canonical_materialization"] = materialize_latest_canonical_day(conn, provider_name=provider_name)
    except Exception as exc:
        stages["canonical_materialization"] = {
            "status": "failed",
            "latest_expected_trading_date": latest_expected_trading_date(conn, provider_name=provider_name),
            "latest_canonical_snapshot_date_before": latest_canonical_before,
            "latest_canonical_snapshot_date_after": latest_canonical_snapshot_date(conn),
            "advanced": False,
            "row_count": 0,
            "inserted_count": 0,
            "error": str(exc),
        }

    latest_expected_after = latest_expected_trading_date(conn, provider_name=provider_name)
    latest_canonical_after = latest_canonical_snapshot_date(conn)
    stage_statuses = [str(stage.get("status") or "") for stage in stages.values()]
    if any(status == "failed" for status in stage_statuses):
        overall_status = "partial"
    elif any(status in {"refreshed", "success", "partial", "history_repaired", "materialized"} for status in stage_statuses):
        overall_status = "success"
    else:
        overall_status = "no_change"

    return {
        "status": overall_status,
        "provider_name": provider_name,
        "target_date": target_date,
        "trading_day": trading_day,
        "latest_expected_trading_date_before": latest_expected_before,
        "latest_expected_trading_date_after": latest_expected_after,
        "latest_canonical_snapshot_date_before": latest_canonical_before,
        "latest_canonical_snapshot_date_after": latest_canonical_after,
        "stages": stages,
    }
