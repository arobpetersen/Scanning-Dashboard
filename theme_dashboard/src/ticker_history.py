from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd

from .db_introspection import table_has_column, table_exists

ATR_PERIOD = 14


def _none_if_missing(value):
    return value if pd.notna(value) else None


def _ticker_daily_history_has_atr_columns(conn) -> bool:
    return table_exists(conn, "ticker_daily_history") and table_has_column(conn, "ticker_daily_history", "atr_14")


def _compute_atr_history_frame(history: pd.DataFrame, *, period: int = ATR_PERIOD) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=["trading_date", "atr_14", "atr_pct_14"])

    working = history.copy()
    working["trading_date"] = pd.to_datetime(working["trading_date"]).dt.date
    for column in ("high", "low", "close"):
        working[column] = pd.to_numeric(working.get(column), errors="coerce")
    working = working.sort_values("trading_date").reset_index(drop=True)

    prev_close = working["close"].shift(1)
    true_range = pd.concat(
        [
            working["high"] - working["low"],
            (working["high"] - prev_close).abs(),
            (working["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1, skipna=True)
    true_range = true_range.where(
        working["high"].notna() & working["low"].notna() & working["close"].notna(),
        np.nan,
    )
    atr_14 = true_range.rolling(window=int(period), min_periods=int(period)).mean()
    atr_pct_14 = np.where(
        working["close"].notna() & (working["close"] != 0) & atr_14.notna(),
        atr_14 / working["close"],
        np.nan,
    )

    return pd.DataFrame(
        {
            "trading_date": working["trading_date"],
            "atr_14": atr_14,
            "atr_pct_14": atr_pct_14,
        }
    )


def _load_existing_history_for_atr(conn, *, ticker: str, market_data_source: str, max_trading_date) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            trading_date,
            open,
            high,
            low,
            close
        FROM ticker_daily_history
        WHERE ticker = ?
          AND market_data_source = ?
          AND trading_date <= ?
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY trading_date
            ORDER BY updated_at DESC, provenance_source_label DESC
        ) = 1
        ORDER BY trading_date
        """,
        [ticker, market_data_source, pd.Timestamp(max_trading_date).date()],
    ).df()


def _attach_atr_metrics(
    conn,
    normalized: pd.DataFrame,
    *,
    ticker: str,
    market_data_source: str,
) -> pd.DataFrame:
    if normalized.empty or not _ticker_daily_history_has_atr_columns(conn):
        return normalized

    max_trading_date = pd.to_datetime(normalized["trading_date"]).dt.date.max()
    existing = _load_existing_history_for_atr(
        conn,
        ticker=ticker,
        market_data_source=market_data_source,
        max_trading_date=max_trading_date,
    )
    if not existing.empty:
        existing["trading_date"] = pd.to_datetime(existing["trading_date"]).dt.date
    incoming = normalized[["trading_date", "open", "high", "low", "close"]].copy()
    incoming["trading_date"] = pd.to_datetime(incoming["trading_date"]).dt.date
    incoming["_incoming_priority"] = 1
    existing["_incoming_priority"] = 0
    combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    combined = (
        combined.sort_values(["trading_date", "_incoming_priority"])
        .drop_duplicates(subset=["trading_date"], keep="last")
        .drop(columns=["_incoming_priority"], errors="ignore")
    )
    atr_metrics = _compute_atr_history_frame(combined, period=ATR_PERIOD)

    enriched = normalized.copy()
    enriched["trading_date"] = pd.to_datetime(enriched["trading_date"]).dt.date
    enriched = enriched.merge(atr_metrics, on="trading_date", how="left")
    return enriched


def _normalize_ticker_history_frame(
    history: pd.DataFrame,
    *,
    ticker: str,
    provenance_source_label: str,
    market_data_source: str,
    run_id: int | None,
) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(
            columns=[
                "run_id",
                "ticker",
                "trading_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
                "trade_count",
                "provenance_class",
                "provenance_source_label",
                "market_data_source",
            ]
        )

    normalized = history.copy()
    normalized["ticker"] = str(ticker or "").strip().upper()
    normalized["trading_date"] = pd.to_datetime(normalized["snapshot_date"]).dt.date
    for column in ["open", "high", "low", "close", "volume", "vwap", "trade_count"]:
        if column not in normalized.columns:
            normalized[column] = None

    normalized["run_id"] = run_id
    normalized["provenance_class"] = "reconstructed"
    normalized["provenance_source_label"] = provenance_source_label
    normalized["market_data_source"] = market_data_source
    base = normalized[
        [
            "run_id",
            "ticker",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
            "provenance_class",
            "provenance_source_label",
            "market_data_source",
        ]
    ].drop_duplicates(subset=["ticker", "trading_date"], keep="last")
    base["trading_date"] = pd.to_datetime(base["trading_date"]).dt.date
    return base


def persist_ticker_daily_history(
    conn,
    history: pd.DataFrame,
    *,
    ticker: str,
    provenance_source_label: str,
    market_data_source: str,
    run_id: int | None = None,
    replace_existing: bool = False,
) -> dict[str, int]:
    normalized = _normalize_ticker_history_frame(
        history,
        ticker=ticker,
        provenance_source_label=provenance_source_label,
        market_data_source=market_data_source,
        run_id=run_id,
    )
    if normalized.empty:
        return {"rows_written": 0, "rows_skipped": 0}
    normalized = _attach_atr_metrics(
        conn,
        normalized,
        ticker=str(ticker or "").strip().upper(),
        market_data_source=market_data_source,
    )

    rows_written = 0
    rows_skipped = 0
    has_atr_columns = _ticker_daily_history_has_atr_columns(conn)
    for row in normalized.itertuples(index=False):
        exists = conn.execute(
            """
            SELECT 1
            FROM ticker_daily_history
            WHERE ticker = ?
              AND trading_date = ?
              AND market_data_source = ?
              AND provenance_source_label = ?
            LIMIT 1
            """,
            [row.ticker, row.trading_date, row.market_data_source, row.provenance_source_label],
        ).fetchone()
        if exists and not replace_existing:
            rows_skipped += 1
            continue
        if exists and replace_existing:
            conn.execute(
                """
                DELETE FROM ticker_daily_history
                WHERE ticker = ?
                  AND trading_date = ?
                  AND market_data_source = ?
                  AND provenance_source_label = ?
                """,
                [row.ticker, row.trading_date, row.market_data_source, row.provenance_source_label],
            )
        if has_atr_columns:
            conn.execute(
                """
                INSERT INTO ticker_daily_history(
                    run_id, ticker, trading_date, open, high, low, close, atr_14, atr_pct_14, volume, vwap, trade_count,
                    provenance_class, provenance_source_label, market_data_source, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    row.run_id,
                    row.ticker,
                    row.trading_date,
                    _none_if_missing(row.open),
                    _none_if_missing(row.high),
                    _none_if_missing(row.low),
                    _none_if_missing(row.close),
                    _none_if_missing(getattr(row, "atr_14", None)),
                    _none_if_missing(getattr(row, "atr_pct_14", None)),
                    _none_if_missing(row.volume),
                    _none_if_missing(row.vwap),
                    int(row.trade_count) if pd.notna(row.trade_count) else None,
                    row.provenance_class,
                    row.provenance_source_label,
                    row.market_data_source,
                ],
            )
        else:
            conn.execute(
                """
                INSERT INTO ticker_daily_history(
                    run_id, ticker, trading_date, open, high, low, close, volume, vwap, trade_count,
                    provenance_class, provenance_source_label, market_data_source, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    row.run_id,
                    row.ticker,
                    row.trading_date,
                    _none_if_missing(row.open),
                    _none_if_missing(row.high),
                    _none_if_missing(row.low),
                    _none_if_missing(row.close),
                    _none_if_missing(row.volume),
                    _none_if_missing(row.vwap),
                    int(row.trade_count) if pd.notna(row.trade_count) else None,
                    row.provenance_class,
                    row.provenance_source_label,
                    row.market_data_source,
                ],
            )
        rows_written += 1
    return {"rows_written": rows_written, "rows_skipped": rows_skipped}


def recompute_ticker_daily_history_atr(
    conn,
    *,
    tickers: list[str] | None = None,
    market_data_source: str | None = None,
    start_after_ticker: str | None = None,
    limit: int | None = None,
    progress_callback=None,
) -> dict[str, int]:
    if not _ticker_daily_history_has_atr_columns(conn):
        return {"series_recomputed": 0, "rows_updated": 0}

    clauses: list[str] = []
    params: list[object] = []
    if tickers:
        normalized_tickers = sorted({str(t or "").strip().upper() for t in tickers if str(t or "").strip()})
        if normalized_tickers:
            placeholders = ", ".join(["?"] * len(normalized_tickers))
            clauses.append(f"ticker IN ({placeholders})")
            params.extend(normalized_tickers)
    if start_after_ticker:
        clauses.append("ticker > ?")
        params.append(str(start_after_ticker).strip().upper())
    if market_data_source:
        clauses.append("market_data_source = ?")
        params.append(str(market_data_source))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_sql = ""
    if limit is not None and int(limit) > 0:
        limit_sql = "LIMIT ?"
        params.append(int(limit))

    series = conn.execute(
        f"""
        SELECT DISTINCT ticker, market_data_source
        FROM ticker_daily_history
        {where}
        ORDER BY ticker, market_data_source
        {limit_sql}
        """,
        params,
    ).fetchall()

    series_recomputed = 0
    rows_updated = 0
    total_series = len(series)
    for index, (ticker_value, source_value) in enumerate(series, start=1):
        history = conn.execute(
            """
            SELECT
                trading_date,
                open,
                high,
                low,
                close
            FROM ticker_daily_history
            WHERE ticker = ?
              AND market_data_source = ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY trading_date
                ORDER BY updated_at DESC, provenance_source_label DESC
            ) = 1
            ORDER BY trading_date
            """,
            [ticker_value, source_value],
        ).df()
        if history.empty:
            continue

        atr_metrics = _compute_atr_history_frame(history, period=ATR_PERIOD)
        if atr_metrics.empty:
            continue

        atr_metrics = atr_metrics.copy()
        atr_metrics["trading_date"] = pd.to_datetime(atr_metrics["trading_date"]).dt.date
        atr_metrics["atr_14"] = pd.to_numeric(atr_metrics["atr_14"], errors="coerce")
        atr_metrics["atr_pct_14"] = pd.to_numeric(atr_metrics["atr_pct_14"], errors="coerce")

        staged_name = "ticker_daily_history_atr_stage"
        conn.register(staged_name, atr_metrics)
        rows_updated_for_series = 0
        try:
            rows_updated_for_series = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM ticker_daily_history AS target
                    INNER JOIN ticker_daily_history_atr_stage AS stage
                        ON target.trading_date = stage.trading_date
                    WHERE target.ticker = ?
                      AND target.market_data_source = ?
                    """,
                    [ticker_value, source_value],
                ).fetchone()[0]
            )
            conn.execute(
                """
                UPDATE ticker_daily_history AS target
                SET
                    atr_14 = stage.atr_14,
                    atr_pct_14 = stage.atr_pct_14,
                    updated_at = CURRENT_TIMESTAMP
                FROM ticker_daily_history_atr_stage AS stage
                WHERE target.ticker = ?
                  AND target.market_data_source = ?
                  AND target.trading_date = stage.trading_date
                """,
                [ticker_value, source_value],
            )
        finally:
            conn.unregister(staged_name)

        rows_updated += rows_updated_for_series
        series_recomputed += 1
        if progress_callback is not None:
            progress_callback(
                {
                    "index": index,
                    "total_series": total_series,
                    "ticker": str(ticker_value),
                    "market_data_source": str(source_value),
                    "rows_updated_for_series": int(rows_updated_for_series),
                    "series_recomputed": int(series_recomputed),
                    "rows_updated": int(rows_updated),
                }
            )

    return {"series_recomputed": int(series_recomputed), "rows_updated": int(rows_updated)}


def ticker_daily_history_rows(
    conn,
    *,
    tickers: list[str] | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    market_data_source: str | None = None,
    provenance_source_label: str | None = None,
) -> pd.DataFrame:
    clauses: list[str] = []
    params: list[object] = []

    if tickers:
        normalized_tickers = sorted({str(t or "").strip().upper() for t in tickers if str(t or "").strip()})
        if normalized_tickers:
            placeholders = ", ".join(["?"] * len(normalized_tickers))
            clauses.append(f"ticker IN ({placeholders})")
            params.extend(normalized_tickers)
    if start_date is not None:
        clauses.append("trading_date >= ?")
        params.append(pd.Timestamp(start_date).date())
    if end_date is not None:
        clauses.append("trading_date <= ?")
        params.append(pd.Timestamp(end_date).date())
    if market_data_source:
        clauses.append("market_data_source = ?")
        params.append(market_data_source)
    if provenance_source_label:
        clauses.append("provenance_source_label = ?")
        params.append(provenance_source_label)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    select_cols = [
        "run_id",
        "ticker",
        "trading_date",
        "open",
        "high",
        "low",
        "close",
    ]
    if _ticker_daily_history_has_atr_columns(conn):
        select_cols.extend(["atr_14", "atr_pct_14"])
    select_cols.extend(
        [
            "volume",
            "vwap",
            "trade_count",
            "provenance_class",
            "provenance_source_label",
            "market_data_source",
            "created_at",
            "updated_at",
        ]
    )
    return conn.execute(
        f"""
        SELECT
            {", ".join(select_cols)}
        FROM ticker_daily_history
        {where}
        ORDER BY trading_date DESC, ticker
        """,
        params,
    ).df()
