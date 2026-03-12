from __future__ import annotations

from datetime import date, datetime

import pandas as pd


def _none_if_missing(value):
    return value if pd.notna(value) else None


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
    return normalized[
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

    rows_written = 0
    rows_skipped = 0
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
    return conn.execute(
        f"""
        SELECT
            run_id,
            ticker,
            trading_date,
            open,
            high,
            low,
            close,
            volume,
            vwap,
            trade_count,
            provenance_class,
            provenance_source_label,
            market_data_source,
            created_at,
            updated_at
        FROM ticker_daily_history
        {where}
        ORDER BY trading_date DESC, ticker
        """,
        params,
    ).df()
