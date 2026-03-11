from __future__ import annotations

from datetime import UTC, datetime, time
from zoneinfo import ZoneInfo

from .fetch_data import run_refresh
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

    return run_refresh(
        conn,
        provider_name,
        tickers=tickers,
        scope_type="scheduled_eod",
        scope_theme_name=None,
    )
