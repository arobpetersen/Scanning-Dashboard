from datetime import date, timedelta

import duckdb

from src.queries import latest_ticker_history_atr_companion_fields, latest_ticker_history_research_fields


def test_latest_ticker_history_research_fields_uses_latest_row_and_boundary_anchors():
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE ticker_daily_history (
            run_id BIGINT,
            ticker VARCHAR,
            trading_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            trade_count BIGINT,
            provenance_class VARCHAR,
            provenance_source_label VARCHAR,
            market_data_source VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            atr_14 DOUBLE,
            atr_pct_14 DOUBLE
        );
        """
    )

    start = date(2026, 3, 16)
    rows: list[tuple] = []
    for offset in range(22):
        trading_date = start + timedelta(days=offset)
        close = 100.0 + offset
        rows.append(
            (
                1,
                "AAA",
                trading_date,
                close,
                close,
                close,
                close,
                1000.0,
                close,
                10,
                "canonical_daily",
                "live",
                "live",
                "2026-04-15 17:00:00",
                f"2026-04-15 17:{offset:02d}:00",
                2.0,
                0.02,
            )
        )
    rows.append(
        (
            1,
            "AAA",
            start + timedelta(days=21),
            999.0,
            999.0,
            999.0,
            999.0,
            1000.0,
            999.0,
            10,
            "canonical_daily",
            "live",
            "live",
            "2026-04-15 17:00:00",
            "2026-04-15 16:00:00",
            9.0,
            0.09,
        )
    )
    conn.executemany(
        """
        INSERT INTO ticker_daily_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    out = latest_ticker_history_research_fields(conn)

    assert out["ticker"].tolist() == ["AAA"]
    row = out.iloc[0]
    assert str(row["latest_history_date"])[:10] == "2026-04-06"
    assert float(row["atr_14"]) == 2.0
    assert round(float(row["perf_1w_atr_units"]), 6) == 2.5
    assert round(float(row["perf_1m_atr_units"]), 6) == 10.5


def test_latest_ticker_history_atr_companion_fields_returns_only_operating_columns():
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE ticker_daily_history (
            run_id BIGINT,
            ticker VARCHAR,
            trading_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            trade_count BIGINT,
            provenance_class VARCHAR,
            provenance_source_label VARCHAR,
            market_data_source VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            atr_14 DOUBLE,
            atr_pct_14 DOUBLE
        );
        """
    )
    conn.execute(
        """
        INSERT INTO ticker_daily_history VALUES
            (1, 'AAA', DATE '2026-03-16', 100, 100, 100, 100, 1000, 100, 10, 'canonical_daily', 'live', 'live', TIMESTAMP '2026-04-15 17:00:00', TIMESTAMP '2026-04-15 17:00:00', 2.0, 0.02),
            (1, 'AAA', DATE '2026-03-21', 105, 105, 105, 105, 1000, 105, 10, 'canonical_daily', 'live', 'live', TIMESTAMP '2026-04-15 17:00:00', TIMESTAMP '2026-04-15 17:01:00', 2.0, 0.02),
            (1, 'AAA', DATE '2026-04-06', 121, 121, 121, 121, 1000, 121, 10, 'canonical_daily', 'live', 'live', TIMESTAMP '2026-04-15 17:00:00', TIMESTAMP '2026-04-15 17:02:00', 2.0, 0.02);
        """
    )

    out = latest_ticker_history_atr_companion_fields(conn)

    assert out.columns.tolist() == ["ticker", "perf_1w_atr_units", "perf_1m_atr_units"]
