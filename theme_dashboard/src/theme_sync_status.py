from __future__ import annotations

import pandas as pd

from .trading_day_status import current_et, latest_finalizable_trading_date


def short_date_label(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return "-"
    stamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(stamp):
        return str(value)
    return stamp.strftime("%Y-%m-%d")


def short_capture_label(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return "-"
    stamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(stamp):
        return str(value)
    return stamp.strftime("%b %d %H:%M")


def effective_latest_finalizable_trading_day(latest_live_trading_value, as_of_et=None):
    live_ts = pd.to_datetime(latest_live_trading_value, errors="coerce")
    if pd.isna(live_ts):
        return pd.NaT
    effective_as_of = as_of_et or current_et()
    finalizable_date = latest_finalizable_trading_date(effective_as_of)
    if finalizable_date is None:
        return live_ts.normalize()
    finalizable_ts = pd.Timestamp(finalizable_date)
    return min(live_ts.normalize(), finalizable_ts.normalize())


def live_current_trading_day(live_current_value) -> pd.Timestamp:
    live_ts = pd.to_datetime(live_current_value, errors="coerce")
    if pd.isna(live_ts):
        return pd.NaT
    return live_ts.normalize()


def ranked_canonical_sync_status(
    live_current_value,
    ranked_canonical_value,
    *,
    latest_finalizable_value=None,
    as_of_et=None,
) -> str:
    live_day = live_current_trading_day(live_current_value)
    ranked_ts = pd.to_datetime(ranked_canonical_value, errors="coerce")
    finalizable_ts = (
        pd.to_datetime(latest_finalizable_value, errors="coerce")
        if latest_finalizable_value is not None
        else effective_latest_finalizable_trading_day(live_current_value, as_of_et=as_of_et)
    )
    if not pd.isna(finalizable_ts):
        finalizable_ts = finalizable_ts.normalize()
    if pd.isna(live_day) and pd.isna(ranked_ts):
        return "Status unavailable"
    if pd.isna(live_day):
        return "Latest market-data trading date unavailable"
    if pd.isna(ranked_ts):
        return "Ranked canonical unavailable"
    if pd.isna(finalizable_ts):
        day_gap = int((live_day - ranked_ts.normalize()).days)
        if day_gap == 0:
            return "Market data and ranked canonical share latest trading day"
        if day_gap > 0:
            suffix = "day" if day_gap == 1 else "days"
            return f"Market-data trading day ahead of ranked canonical by {day_gap} {suffix}"
        day_gap = abs(day_gap)
        suffix = "day" if day_gap == 1 else "days"
        return f"Ranked canonical ahead of market-data trading day by {day_gap} {suffix}"

    ranked_day = ranked_ts.normalize()
    finalizable_day = finalizable_ts
    if ranked_day == finalizable_day:
        if live_day > finalizable_day:
            return (
                f"Latest refresh captured {short_capture_label(live_current_value)}; "
                f"canonical finalized through {short_date_label(finalizable_day)}"
            )
        return (
            f"Market data current through {short_date_label(finalizable_day)}; "
            f"canonical finalized through {short_date_label(finalizable_day)}"
        )

    day_gap = int((finalizable_day - ranked_day).days)
    if day_gap > 0:
        suffix = "day" if day_gap == 1 else "days"
        return f"Canonical trails latest finalized trading day by {day_gap} {suffix}"

    day_gap = abs(day_gap)
    suffix = "day" if day_gap == 1 else "days"
    return f"Ranked canonical ahead of latest finalized trading day by {day_gap} {suffix}"
