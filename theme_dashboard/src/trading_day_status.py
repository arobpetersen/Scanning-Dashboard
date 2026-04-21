from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

EASTERN_TZ = ZoneInfo("America/New_York")
MARKET_CLOSE_HOUR_ET = 16
MARKET_CLOSE_MINUTE_ET = 0


def current_et(now_utc: datetime | None = None) -> datetime:
    now = now_utc or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return now.astimezone(EASTERN_TZ)


def is_trading_day(dt_et: datetime) -> bool:
    return dt_et.weekday() < 5


def reached_eod_window(
    dt_et: datetime,
    target_hour: int = MARKET_CLOSE_HOUR_ET,
    target_minute: int = MARKET_CLOSE_MINUTE_ET,
) -> bool:
    return dt_et.time() >= time(hour=target_hour, minute=target_minute)


def finalization_eligible(dt_et: datetime) -> bool:
    return bool(is_trading_day(dt_et) and reached_eod_window(dt_et))


def previous_trading_date(anchor_date: date) -> date:
    candidate = anchor_date - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def latest_finalizable_trading_date(as_of_et: datetime) -> date | None:
    if finalization_eligible(as_of_et):
        return as_of_et.date()
    return previous_trading_date(as_of_et.date())
