from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from .db_introspection import table_exists, table_has_column
from .provider_live import LiveProvider
from .queries import _preferred_ticker_history_source
from .ticker_history import persist_ticker_daily_history


MARKET_PROXY_TICKER = "QQQ"
ATR_PERIOD = 14
QQQ_HISTORY_BACKFILL_DAYS = 120
QQQ_HISTORY_PROVENANCE_SOURCE_LABEL = "market_context_bootstrap"
QQQ_UNAVAILABLE_MESSAGE = "QQQ tape unavailable — run market context backfill or include QQQ in daily history refresh."

STRONG_UP_DAY = "Strong Up Day"
UP_DAY = "Up Day"
FLAT_MIXED = "Flat / Mixed"
DOWN_DAY = "Down Day"
STRONG_DOWN_DAY = "Strong Down Day"

VOLATILE_RECOVERY = "Volatile Recovery"
RECOVERY = "Recovery"
VOLATILE_FADE = "Volatile Fade"
FADE = "Fade"
TREND_UP = "Trend Up"
TREND_DOWN = "Trend Down"
VOLATILE_CHOP = "Volatile Chop"
QUIET = "Quiet"
MIXED = "Mixed"


def classify_move_label(qqq_pct_change: float | int | None) -> str:
    pct_change = _to_float(qqq_pct_change)
    if pct_change is None:
        return FLAT_MIXED
    if pct_change >= 1.50:
        return STRONG_UP_DAY
    if pct_change >= 0.50:
        return UP_DAY
    if pct_change <= -1.50:
        return STRONG_DOWN_DAY
    if pct_change <= -0.50:
        return DOWN_DAY
    return FLAT_MIXED


def classify_character_tag(
    *,
    qqq_pct_change: float | int | None,
    gap_pct: float | int | None,
    range_pct: float | int | None,
    close_position: float | int | None,
    range_x_atr_14: float | int | None,
) -> str:
    pct_change = _to_float(qqq_pct_change)
    gap = _to_float(gap_pct)
    range_percent = _to_float(range_pct)
    close_pos = _to_float(close_position)
    range_atr = _to_float(range_x_atr_14)

    if pct_change is None or gap is None or close_pos is None:
        return MIXED

    atr_available = range_atr is not None
    volatile = bool(atr_available and range_atr >= 1.20)
    atr_below_volatile = (not atr_available) or range_atr < 1.20

    if volatile and close_pos >= 0.70 and (pct_change <= 0.0 or gap <= -0.50):
        return VOLATILE_RECOVERY
    if close_pos >= 0.70 and (pct_change <= 0.0 or gap <= -0.50) and atr_below_volatile:
        return RECOVERY
    if volatile and close_pos <= 0.30 and (pct_change >= 0.0 or gap >= 0.50):
        return VOLATILE_FADE
    if close_pos <= 0.30 and (pct_change >= 0.0 or gap >= 0.50) and atr_below_volatile:
        return FADE
    if pct_change >= 0.75 and close_pos >= 0.70:
        return TREND_UP
    if pct_change <= -0.75 and close_pos <= 0.30:
        return TREND_DOWN
    if volatile and 0.35 <= close_pos <= 0.65:
        return VOLATILE_CHOP
    if abs(pct_change) < 0.40:
        if atr_available and range_atr < 0.80:
            return QUIET
        if not atr_available and range_percent is not None and range_percent < 0.90:
            return QUIET
    return MIXED


def latest_qqq_market_context_from_ohlcv(ohlcv: pd.DataFrame) -> dict[str, object] | None:
    if ohlcv.empty:
        return None

    history = ohlcv.copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col not in history.columns:
            history[col] = np.nan
        history[col] = pd.to_numeric(history[col], errors="coerce")
    if "trading_date" not in history.columns:
        return None

    history["trading_date"] = pd.to_datetime(history["trading_date"], errors="coerce")
    history = history.dropna(subset=["trading_date", "open", "high", "low", "close"]).copy()
    if history.empty:
        return None

    sort_cols = ["trading_date", "updated_at"] if "updated_at" in history.columns else ["trading_date"]
    history = (
        history.sort_values(sort_cols)
        .drop_duplicates(subset=["trading_date"], keep="last")
        .sort_values("trading_date")
        .reset_index(drop=True)
    )
    if len(history) < 2:
        return None

    history["previous_close"] = history["close"].shift(1)
    true_range_parts = pd.concat(
        [
            history["high"] - history["low"],
            (history["high"] - history["previous_close"]).abs(),
            (history["low"] - history["previous_close"]).abs(),
        ],
        axis=1,
    )
    history["true_range"] = true_range_parts.max(axis=1)
    history["atr_14"] = history["true_range"].shift(1).rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    latest = history.iloc[-1]
    previous_close = _to_float(latest.get("previous_close"))
    open_value = _to_float(latest.get("open"))
    high = _to_float(latest.get("high"))
    low = _to_float(latest.get("low"))
    close = _to_float(latest.get("close"))
    atr_14 = _to_float(latest.get("atr_14"))

    if previous_close is None or open_value is None or high is None or low is None or close is None:
        return None

    intraday_range = high - low
    qqq_pct_change = ((close / previous_close) - 1.0) * 100.0 if previous_close != 0 else None
    gap_pct = ((open_value / previous_close) - 1.0) * 100.0 if previous_close != 0 else None
    range_pct = (intraday_range / previous_close) * 100.0 if previous_close != 0 else None
    close_position = (close - low) / intraday_range if intraday_range > 0 else 0.50
    close_position_pct = close_position * 100.0
    range_x_atr_14 = intraday_range / atr_14 if atr_14 not in {None, 0.0} else None
    move_label = classify_move_label(qqq_pct_change)
    character_tag = classify_character_tag(
        qqq_pct_change=qqq_pct_change,
        gap_pct=gap_pct,
        range_pct=range_pct,
        close_position=close_position,
        range_x_atr_14=range_x_atr_14,
    )
    context_label = f"{move_label} | {character_tag}"
    display_summary = _display_summary(
        qqq_pct_change=qqq_pct_change,
        move_label=move_label,
        character_tag=character_tag,
        gap_pct=gap_pct,
        close_position_pct=close_position_pct,
        range_x_atr_14=range_x_atr_14,
    )

    return {
        "trading_date": latest["trading_date"].date(),
        "proxy_ticker": MARKET_PROXY_TICKER,
        "open": round(open_value, 4),
        "high": round(high, 4),
        "low": round(low, 4),
        "close": round(close, 4),
        "previous_close": round(previous_close, 4),
        "atr_14": round(atr_14, 4) if atr_14 is not None else None,
        "qqq_pct_change": round(qqq_pct_change, 2) if qqq_pct_change is not None else None,
        "gap_pct": round(gap_pct, 2) if gap_pct is not None else None,
        "range_pct": round(range_pct, 2) if range_pct is not None else None,
        "close_position": round(close_position, 4),
        "close_position_pct": round(close_position_pct, 1),
        "range_x_atr_14": round(range_x_atr_14, 2) if range_x_atr_14 is not None else None,
        "move_label": move_label,
        "character_tag": character_tag,
        "context_label": context_label,
        "display_summary": display_summary,
    }


def latest_qqq_market_context(conn) -> dict[str, object] | None:
    if not table_exists(conn, "ticker_daily_history"):
        return None
    required_cols = {"open", "high", "low", "close", "volume", "trading_date", "market_data_source", "created_at", "updated_at"}
    if any(not table_has_column(conn, "ticker_daily_history", col) for col in required_cols):
        return None

    preferred_source = _preferred_ticker_history_source(conn)
    if not preferred_source:
        return None

    history = conn.execute(
        """
        SELECT
            upper(trim(ticker)) AS ticker,
            trading_date,
            open,
            high,
            low,
            close,
            volume,
            market_data_source,
            updated_at
        FROM ticker_daily_history
        WHERE upper(trim(ticker)) = ?
          AND market_data_source = ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY upper(trim(ticker)), trading_date, market_data_source
            ORDER BY updated_at DESC, created_at DESC, close DESC
        ) = 1
        ORDER BY trading_date DESC
        LIMIT ?
        """,
        [MARKET_PROXY_TICKER, preferred_source, 80],
    ).df()
    if history.empty:
        return None
    return latest_qqq_market_context_from_ohlcv(history)


def qqq_market_context_unavailable_message(
    *,
    latest_qqq_history_date_value: date | datetime | str | None = None,
    target_date: date | datetime | str | None = None,
    provider_data_unavailable: bool = False,
) -> str:
    if provider_data_unavailable and target_date is not None:
        return f"QQQ context unavailable: provider data unavailable for {_normalize_date(target_date)}."
    if latest_qqq_history_date_value is not None and target_date is not None:
        return (
            f"QQQ context unavailable: latest stored QQQ history is "
            f"{_normalize_date(latest_qqq_history_date_value)}, target is {_normalize_date(target_date)}."
        )
    if latest_qqq_history_date_value is not None:
        return f"QQQ context unavailable: latest stored QQQ history is {_normalize_date(latest_qqq_history_date_value)}."
    return QQQ_UNAVAILABLE_MESSAGE


def _normalize_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def latest_qqq_history_date(conn, *, market_data_source: str = "live") -> date | None:
    if not table_exists(conn, "ticker_daily_history"):
        return None
    row = conn.execute(
        """
        SELECT MAX(trading_date)
        FROM ticker_daily_history
        WHERE upper(trim(ticker)) = ?
          AND market_data_source = ?
        """,
        [MARKET_PROXY_TICKER, market_data_source],
    ).fetchone()
    return row[0] if row and row[0] else None


def backfill_qqq_market_context_history(
    conn,
    *,
    days: int = QQQ_HISTORY_BACKFILL_DAYS,
    provider: LiveProvider | None = None,
    target_date: date | datetime | str | None = None,
    replace_existing: bool = True,
) -> dict[str, object]:
    active_provider = provider or LiveProvider(include_reference=False)
    if not active_provider.is_configured:
        return {
            "status": "missing_api_key",
            "rows_written": 0,
            "rows_skipped": 0,
            "message": "Massive API key is not configured.",
        }

    end_date = _normalize_date(target_date) if target_date is not None else date.today()
    start_date = end_date - timedelta(days=max(int(days), ATR_PERIOD + 5))
    latest_before = latest_qqq_history_date(conn, market_data_source=active_provider.name)
    try:
        history = active_provider.fetch_ticker_history_range(MARKET_PROXY_TICKER, start_date, end_date)
    except Exception as exc:
        return {
            "status": "provider_data_unavailable",
            "ticker": MARKET_PROXY_TICKER,
            "start_date": start_date,
            "end_date": end_date,
            "latest_qqq_history_date_before": latest_before,
            "latest_qqq_history_date_after": latest_before,
            "rows_written": 0,
            "rows_skipped": 0,
            "message": f"Provider data unavailable for {MARKET_PROXY_TICKER} target date {end_date}.",
            "error": str(exc),
        }
    available_dates = sorted(pd.to_datetime(history.get("snapshot_date"), errors="coerce").dropna().dt.date.unique().tolist())
    latest_available_date = available_dates[-1] if available_dates else None
    persisted = persist_ticker_daily_history(
        conn,
        history,
        ticker=MARKET_PROXY_TICKER,
        provenance_source_label=QQQ_HISTORY_PROVENANCE_SOURCE_LABEL,
        market_data_source=active_provider.name,
        replace_existing=replace_existing,
    )
    latest_after = latest_qqq_history_date(conn, market_data_source=active_provider.name)
    if target_date is not None and (latest_available_date is None or latest_available_date < end_date):
        return {
            "status": "provider_data_unavailable",
            "ticker": MARKET_PROXY_TICKER,
            "start_date": start_date,
            "end_date": end_date,
            "latest_available_date": latest_available_date,
            "latest_qqq_history_date_before": latest_before,
            "latest_qqq_history_date_after": latest_after,
            "rows_written": int(persisted.get("rows_written") or 0),
            "rows_skipped": int(persisted.get("rows_skipped") or 0),
            "available_snapshot_dates": available_dates,
            "message": f"Provider data unavailable for {MARKET_PROXY_TICKER} target date {end_date}.",
        }
    return {
        "status": "success",
        "ticker": MARKET_PROXY_TICKER,
        "start_date": start_date,
        "end_date": end_date,
        "latest_available_date": latest_available_date,
        "latest_qqq_history_date_before": latest_before,
        "latest_qqq_history_date_after": latest_after,
        "rows_written": int(persisted.get("rows_written") or 0),
        "rows_skipped": int(persisted.get("rows_skipped") or 0),
        "available_snapshot_dates": available_dates,
    }


def ensure_qqq_market_context_history(
    conn,
    *,
    target_date: date | datetime | str | None,
    provider: LiveProvider | None = None,
    days: int = QQQ_HISTORY_BACKFILL_DAYS,
) -> dict[str, object]:
    provider_name = provider.name if provider is not None else "live"
    latest_before = latest_qqq_history_date(conn, market_data_source=provider_name)
    if target_date is None:
        return {
            "status": "no_valid_target_trading_day",
            "ticker": MARKET_PROXY_TICKER,
            "target_date": None,
            "latest_qqq_history_date_before": latest_before,
            "latest_qqq_history_date_after": latest_before,
            "advanced": False,
            "message": "QQQ context skipped because no valid target trading day exists.",
        }

    target = _normalize_date(target_date)
    if latest_before is not None and latest_before >= target:
        return {
            "status": "already_current",
            "ticker": MARKET_PROXY_TICKER,
            "target_date": target,
            "latest_qqq_history_date_before": latest_before,
            "latest_qqq_history_date_after": latest_before,
            "advanced": False,
            "message": f"QQQ context current through {target}.",
        }

    result = backfill_qqq_market_context_history(
        conn,
        days=days,
        provider=provider,
        target_date=target,
        replace_existing=False,
    )
    latest_after = latest_qqq_history_date(conn, market_data_source=provider_name)
    status = str(result.get("status") or "")
    if status == "success" and latest_after is not None and latest_after >= target:
        status = "updated"
        message = f"QQQ context updated through {target}."
    elif status == "provider_data_unavailable":
        message = f"QQQ context unavailable: provider data unavailable for {target}."
    else:
        message = str(result.get("message") or f"QQQ context unavailable for {target}.")
    return {
        **result,
        "status": status,
        "target_date": target,
        "latest_qqq_history_date_after": latest_after,
        "advanced": bool(latest_after is not None and latest_before != latest_after),
        "message": message,
    }


def _display_summary(
    *,
    qqq_pct_change: float | None,
    move_label: str,
    character_tag: str,
    gap_pct: float | None,
    close_position_pct: float | None,
    range_x_atr_14: float | None,
) -> str:
    qqq_text = _format_pct(qqq_pct_change, decimals=1)
    gap_text = _format_pct(gap_pct, decimals=1)
    close_text = f"{close_position_pct:.0f}%" if close_position_pct is not None else "n/a"
    range_text = f"{range_x_atr_14:.2f}x ATR(14)" if range_x_atr_14 is not None else "ATR(14) n/a"
    return (
        f"QQQ {qqq_text} | {move_label} | {character_tag} | "
        f"Gap {gap_text} | Close Position {close_text} | Range {range_text}"
    )


def _format_pct(value: float | None, *, decimals: int) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.{decimals}f}%"


def _to_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
