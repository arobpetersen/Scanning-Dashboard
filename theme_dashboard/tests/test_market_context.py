from datetime import date, timedelta

import duckdb
import pandas as pd

from src.database import SCHEMA_SQL
from src.market_context import (
    DOWN_DAY,
    FADE,
    FLAT_MIXED,
    MIXED,
    QUIET,
    RECOVERY,
    STRONG_DOWN_DAY,
    STRONG_UP_DAY,
    TREND_DOWN,
    TREND_UP,
    UP_DAY,
    VOLATILE_CHOP,
    VOLATILE_FADE,
    VOLATILE_RECOVERY,
    classify_character_tag,
    classify_move_label,
    ensure_qqq_market_context_history,
    latest_qqq_history_date,
    latest_qqq_market_context_from_ohlcv,
    qqq_market_context_unavailable_message,
)


def test_move_label_strong_up_day():
    assert classify_move_label(1.50) == STRONG_UP_DAY


def test_move_label_up_day():
    assert classify_move_label(0.75) == UP_DAY


def test_move_label_flat_mixed():
    assert classify_move_label(0.0) == FLAT_MIXED


def test_move_label_down_day():
    assert classify_move_label(-0.75) == DOWN_DAY


def test_move_label_strong_down_day():
    assert classify_move_label(-1.50) == STRONG_DOWN_DAY


def test_character_tag_volatile_recovery_priority():
    assert (
        classify_character_tag(
            qqq_pct_change=-0.2,
            gap_pct=-0.7,
            range_pct=1.5,
            close_position=0.78,
            range_x_atr_14=1.30,
        )
        == VOLATILE_RECOVERY
    )


def test_character_tag_recovery():
    assert (
        classify_character_tag(
            qqq_pct_change=-0.1,
            gap_pct=-0.2,
            range_pct=0.8,
            close_position=0.75,
            range_x_atr_14=0.95,
        )
        == RECOVERY
    )


def test_character_tag_volatile_fade_priority():
    assert (
        classify_character_tag(
            qqq_pct_change=0.2,
            gap_pct=0.7,
            range_pct=1.5,
            close_position=0.22,
            range_x_atr_14=1.30,
        )
        == VOLATILE_FADE
    )


def test_character_tag_fade():
    assert (
        classify_character_tag(
            qqq_pct_change=0.1,
            gap_pct=0.2,
            range_pct=0.8,
            close_position=0.25,
            range_x_atr_14=0.95,
        )
        == FADE
    )


def test_character_tag_trend_up():
    assert (
        classify_character_tag(
            qqq_pct_change=0.90,
            gap_pct=0.2,
            range_pct=0.9,
            close_position=0.80,
            range_x_atr_14=0.95,
        )
        == TREND_UP
    )


def test_character_tag_trend_down():
    assert (
        classify_character_tag(
            qqq_pct_change=-0.90,
            gap_pct=-0.2,
            range_pct=0.9,
            close_position=0.20,
            range_x_atr_14=0.95,
        )
        == TREND_DOWN
    )


def test_character_tag_volatile_chop():
    assert (
        classify_character_tag(
            qqq_pct_change=0.2,
            gap_pct=0.1,
            range_pct=1.5,
            close_position=0.50,
            range_x_atr_14=1.30,
        )
        == VOLATILE_CHOP
    )


def test_character_tag_quiet_using_atr():
    assert (
        classify_character_tag(
            qqq_pct_change=0.2,
            gap_pct=0.1,
            range_pct=0.8,
            close_position=0.50,
            range_x_atr_14=0.70,
        )
        == QUIET
    )


def test_character_tag_quiet_fallback_using_range_pct_when_atr_unavailable():
    assert (
        classify_character_tag(
            qqq_pct_change=0.2,
            gap_pct=0.1,
            range_pct=0.80,
            close_position=0.50,
            range_x_atr_14=None,
        )
        == QUIET
    )


def test_character_tag_mixed_fallback():
    assert (
        classify_character_tag(
            qqq_pct_change=0.6,
            gap_pct=0.1,
            range_pct=0.8,
            close_position=0.50,
            range_x_atr_14=0.95,
        )
        == MIXED
    )


def test_atr_14_excludes_current_day():
    history = _history_with_prior_true_ranges([1.0] * 14, current_range=20.0)

    out = latest_qqq_market_context_from_ohlcv(history)

    assert out is not None
    assert float(out["atr_14"]) == 1.0
    assert float(out["range_x_atr_14"]) == 20.0


def test_display_summary_formatting():
    rows = _history_with_prior_true_ranges([1.0] * 14, current_range=1.38)
    previous_close = rows.iloc[-2]["close"]
    current_idx = rows.index[-1]
    rows.loc[current_idx, "open"] = previous_close * 0.993
    rows.loc[current_idx, "low"] = previous_close - 1.00
    rows.loc[current_idx, "high"] = previous_close + 0.38
    rows.loc[current_idx, "close"] = rows.loc[current_idx, "low"] + (1.38 * 0.78)

    out = latest_qqq_market_context_from_ohlcv(rows)

    assert out is not None
    assert out["display_summary"] == "QQQ 0.1% | Flat / Mixed | Volatile Recovery | Gap -0.7% | Close Position 78% | Range 1.38x ATR(14)"


def test_missing_qqq_context_message_is_actionable():
    assert qqq_market_context_unavailable_message() == "QQQ tape unavailable — run market context backfill or include QQQ in daily history refresh."


def test_missing_qqq_context_message_can_include_specific_reason():
    assert (
        qqq_market_context_unavailable_message(
            latest_qqq_history_date_value=date(2026, 5, 14),
            target_date=date(2026, 5, 15),
        )
        == "QQQ context unavailable: latest stored QQQ history is 2026-05-14, target is 2026-05-15."
    )
    assert (
        qqq_market_context_unavailable_message(provider_data_unavailable=True, target_date=date(2026, 5, 15))
        == "QQQ context unavailable: provider data unavailable for 2026-05-15."
    )


def test_ensure_qqq_market_context_history_backfills_missing_target_date():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(SCHEMA_SQL)
        provider = _FakeQqqProvider(_qqq_history(date(2026, 5, 15)))

        result = ensure_qqq_market_context_history(conn, target_date=date(2026, 5, 15), provider=provider, days=20)

        assert result["status"] == "updated"
        assert result["message"] == "QQQ context updated through 2026-05-15."
        assert latest_qqq_history_date(conn) == date(2026, 5, 15)
        assert provider.calls == [(date(2026, 4, 25), date(2026, 5, 15))]
    finally:
        conn.close()


def test_ensure_qqq_market_context_history_noops_when_current():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(SCHEMA_SQL)
        provider = _FakeQqqProvider(_qqq_history(date(2026, 5, 15)))
        ensure_qqq_market_context_history(conn, target_date=date(2026, 5, 15), provider=provider, days=20)
        provider.calls.clear()

        result = ensure_qqq_market_context_history(conn, target_date=date(2026, 5, 15), provider=provider, days=20)

        assert result["status"] == "already_current"
        assert result["message"] == "QQQ context current through 2026-05-15."
        assert provider.calls == []
    finally:
        conn.close()


def test_ensure_qqq_market_context_history_reports_provider_data_unavailable():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(SCHEMA_SQL)
        provider = _FakeQqqProvider(_qqq_history(date(2026, 5, 14)))

        result = ensure_qqq_market_context_history(conn, target_date=date(2026, 5, 15), provider=provider, days=20)

        assert result["status"] == "provider_data_unavailable"
        assert result["message"] == "QQQ context unavailable: provider data unavailable for 2026-05-15."
        assert latest_qqq_history_date(conn) == date(2026, 5, 14)
    finally:
        conn.close()


class _FakeQqqProvider:
    name = "live"
    is_configured = True

    def __init__(self, history: pd.DataFrame):
        self.history = history
        self.calls: list[tuple[date, date]] = []

    def fetch_ticker_history_range(self, _ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        self.calls.append((start_date, end_date))
        history = self.history.copy()
        mask = (pd.to_datetime(history["snapshot_date"]).dt.date >= start_date) & (
            pd.to_datetime(history["snapshot_date"]).dt.date <= end_date
        )
        return history.loc[mask].copy()


def _qqq_history(end_date: date) -> pd.DataFrame:
    rows = []
    close = 100.0
    start = end_date - timedelta(days=20)
    for offset in range(21):
        trading_date = start + timedelta(days=offset)
        close += 0.25
        rows.append(
            {
                "snapshot_date": trading_date,
                "open": close - 0.1,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 1_000_000,
                "vwap": close,
                "trade_count": 1000,
            }
        )
    return pd.DataFrame(rows)


def _history_with_prior_true_ranges(prior_ranges: list[float], *, current_range: float) -> pd.DataFrame:
    rows = []
    start = date(2026, 1, 1)
    close = 100.0
    rows.append(_row(start, close, close + 0.5, close - 0.5, close))
    for idx, true_range in enumerate(prior_ranges, start=1):
        trading_date = start + timedelta(days=idx)
        open_value = close
        low = close - true_range / 2.0
        high = close + true_range / 2.0
        close = close + 0.1
        rows.append(_row(trading_date, open_value, high, low, close))
    trading_date = start + timedelta(days=len(prior_ranges) + 1)
    open_value = close
    low = close - current_range / 2.0
    high = close + current_range / 2.0
    close = high
    rows.append(_row(trading_date, open_value, high, low, close))
    return pd.DataFrame(rows)


def _row(trading_date, open_value, high, low, close):
    return {
        "trading_date": trading_date,
        "open": open_value,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1_000_000,
        "updated_at": f"{trading_date} 16:00:00",
    }
