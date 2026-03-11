from __future__ import annotations

from datetime import datetime

import pandas as pd


def human_readable_number(value: float | int | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    n = float(value)
    abs_n = abs(n)
    if abs_n >= 1_000_000_000_000:
        return f"{n / 1_000_000_000_000:.1f}T"
    if abs_n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if abs_n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if abs_n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return f"{n:.0f}"


def short_timestamp(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if isinstance(ts, pd.Timestamp):
        dt = ts.to_pydatetime()
    elif isinstance(ts, datetime):
        dt = ts
    else:
        dt = pd.Timestamp(ts).to_pydatetime()
    return f"{dt.strftime('%b')} {dt.day} {dt.strftime('%H:%M')}"




def format_price(value: float | int | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    price = float(value)
    abs_price = abs(price)
    if abs_price >= 100:
        return f"{price:,.2f}"
    if abs_price >= 1:
        return f"{price:,.2f}"
    return f"{price:,.4f}"


def display_or_dash(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    text = str(value).strip()
    return text if text else "—"
def format_theme_ticker_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    for col in ("perf_1w", "perf_1m", "perf_3m"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    numeric_price = None
    if "price" in out.columns:
        numeric_price = pd.to_numeric(out["price"], errors="coerce")
        out["price"] = numeric_price.apply(format_price)

    if "avg_volume" in out.columns:
        numeric_avg = pd.to_numeric(out["avg_volume"], errors="coerce")
        if numeric_price is not None:
            out["dollar_volume"] = (numeric_price * numeric_avg).round(2)
        out["avg_volume"] = numeric_avg.apply(human_readable_number)

    if "market_cap" in out.columns:
        out["market_cap"] = out["market_cap"].apply(human_readable_number)
    if "dollar_volume" in out.columns:
        out["dollar_volume"] = out["dollar_volume"].apply(human_readable_number)

    for ts_col in ("last_updated", "snapshot_time", "latest_refresh_time"):
        if ts_col in out.columns:
            out[ts_col] = out[ts_col].apply(short_timestamp)

    for readable_col in ("market_cap", "avg_volume", "dollar_volume", "price", "last_updated", "snapshot_time", "latest_refresh_time"):
        if readable_col in out.columns:
            out[readable_col] = out[readable_col].apply(display_or_dash)

    return out
