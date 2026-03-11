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
    return dt.strftime("%b %-d %H:%M")


def format_theme_ticker_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    for col in ("perf_1w", "perf_1m", "perf_3m"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    if "price" in out.columns:
        out["price"] = pd.to_numeric(out["price"], errors="coerce").round(2)

    if "avg_volume" in out.columns and "price" in out.columns:
        out["dollar_volume"] = (pd.to_numeric(out["price"], errors="coerce") * pd.to_numeric(out["avg_volume"], errors="coerce")).round(2)

    if "market_cap" in out.columns:
        out["market_cap"] = out["market_cap"].apply(human_readable_number)
    if "avg_volume" in out.columns:
        out["avg_volume"] = out["avg_volume"].apply(human_readable_number)
    if "dollar_volume" in out.columns:
        out["dollar_volume"] = out["dollar_volume"].apply(human_readable_number)
    if "last_updated" in out.columns:
        out["last_updated"] = out["last_updated"].apply(short_timestamp)

    return out
