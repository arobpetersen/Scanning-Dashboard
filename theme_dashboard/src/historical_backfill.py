from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd

from .fetch_data import get_provider
from .rankings import _compute_theme_metrics
from .ticker_history import persist_ticker_daily_history

HISTORICAL_LOOKBACK_BUFFER_DAYS = 120
SUPPRESSION_REBUILD_LOOKBACK_DAYS = 45


def _normalize_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _target_snapshot_time(snapshot_date: date) -> datetime:
    return datetime.combine(snapshot_date, datetime.min.time(), tzinfo=UTC).replace(tzinfo=None)


def _preferred_stored_history_source(conn) -> str | None:
    row = conn.execute(
        """
        SELECT market_data_source
        FROM ticker_daily_history
        ORDER BY CASE WHEN market_data_source = 'live' THEN 0 ELSE 1 END,
                 trading_date DESC,
                 updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def _scope_membership(conn, tickers: list[str] | None = None, theme_ids: list[int] | None = None) -> pd.DataFrame:
    clauses = []
    params: list[object] = []

    if tickers:
        placeholders = ", ".join(["?"] * len(tickers))
        clauses.append(f"m.theme_id IN (SELECT DISTINCT theme_id FROM theme_membership WHERE ticker IN ({placeholders}))")
        params.extend(tickers)
    if theme_ids:
        placeholders = ", ".join(["?"] * len(theme_ids))
        clauses.append(f"m.theme_id IN ({placeholders})")
        params.extend(theme_ids)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(
        f"""
        SELECT
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            m.ticker
        FROM themes t
        JOIN theme_membership m ON m.theme_id = t.id
        {where}
        ORDER BY t.id, m.ticker
        """,
        params,
    ).df()


def _compute_daily_perf(history: pd.DataFrame, requested_start: date, requested_end: date) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=["ticker", "snapshot_date", "perf_1w", "perf_1m", "perf_3m"])

    enriched = history.sort_values(["ticker", "snapshot_date"]).copy()
    grouped = enriched.groupby("ticker")["close"]
    enriched["perf_1w"] = ((grouped.transform(lambda s: s / s.shift(5))) - 1.0) * 100.0
    enriched["perf_1m"] = ((grouped.transform(lambda s: s / s.shift(21))) - 1.0) * 100.0
    enriched["perf_3m"] = ((grouped.transform(lambda s: s / s.shift(63))) - 1.0) * 100.0
    mask = (pd.to_datetime(enriched["snapshot_date"]).dt.date >= requested_start) & (
        pd.to_datetime(enriched["snapshot_date"]).dt.date <= requested_end
    )
    return enriched.loc[mask, ["ticker", "snapshot_date", "perf_1w", "perf_1m", "perf_3m"]].copy()


def _insert_reconstruction_run(
    conn,
    *,
    run_kind: str,
    provenance_source_label: str,
    market_data_source: str,
    start_date: date,
    end_date: date,
    tickers: list[str],
    theme_ids: list[int],
) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO historical_reconstruction_runs(
                run_kind, provenance_class, provenance_source_label, market_data_source,
                started_at, status, start_date, end_date, target_tickers, target_theme_ids
            )
            VALUES (?, 'reconstructed', ?, ?, CURRENT_TIMESTAMP, 'running', ?, ?, ?, ?)
            RETURNING run_id
            """,
            [
                run_kind,
                provenance_source_label,
                market_data_source,
                start_date,
                end_date,
                ",".join(tickers) if tickers else None,
                ",".join(str(theme_id) for theme_id in theme_ids) if theme_ids else None,
            ],
        ).fetchone()[0]
    )


def _finalize_reconstruction_run(conn, run_id: int, **fields) -> None:
    assignments = ", ".join(f"{column} = ?" for column in fields)
    conn.execute(
        f"""
        UPDATE historical_reconstruction_runs
        SET finished_at = CURRENT_TIMESTAMP,
            {assignments}
        WHERE run_id = ?
        """,
        [*fields.values(), run_id],
    )


def reconstruct_theme_history_range(
    conn,
    *,
    provider_name: str = "live",
    start_date: date | datetime | str,
    end_date: date | datetime | str,
    tickers: list[str] | None = None,
    theme_ids: list[int] | None = None,
    provenance_source_label: str = "historical_backfill",
    run_kind: str = "historical_backfill",
    replace_existing: bool = False,
    persist_ticker_history: bool = True,
) -> dict[str, object]:
    requested_start = _normalize_date(start_date)
    requested_end = _normalize_date(end_date)
    if requested_end < requested_start:
        raise ValueError("end_date must be on or after start_date.")

    normalized_tickers = sorted({str(t or "").strip().upper() for t in (tickers or []) if str(t or "").strip()})
    normalized_theme_ids = sorted({int(theme_id) for theme_id in (theme_ids or [])})
    membership = _scope_membership(conn, tickers=normalized_tickers or None, theme_ids=normalized_theme_ids or None)
    if membership.empty:
        return {
            "run_id": None,
            "status": "no_scope",
            "ticker_history_rows_written": 0,
            "ticker_history_rows_skipped": 0,
            "snapshot_rows_written": 0,
            "snapshot_rows_skipped": 0,
            "failed_tickers": [],
            "available_snapshot_dates": [],
        }

    scoped_tickers = sorted(membership["ticker"].astype(str).str.strip().str.upper().unique().tolist())
    scoped_theme_ids = sorted(membership["theme_id"].astype(int).unique().tolist())
    provider = get_provider(provider_name)
    run_id = _insert_reconstruction_run(
        conn,
        run_kind=run_kind,
        provenance_source_label=provenance_source_label,
        market_data_source=provider.name,
        start_date=requested_start,
        end_date=requested_end,
        tickers=normalized_tickers,
        theme_ids=scoped_theme_ids,
    )

    fetch_start = requested_start - timedelta(days=HISTORICAL_LOOKBACK_BUFFER_DAYS)
    ticker_history_frames: list[pd.DataFrame] = []
    failed_tickers: list[str] = []
    ticker_history_rows_written = 0
    ticker_history_rows_skipped = 0

    try:
        for ticker in scoped_tickers:
            try:
                history = provider.fetch_ticker_history_range(ticker, fetch_start, requested_end)
                if history.empty:
                    failed_tickers.append(ticker)
                    continue
                if persist_ticker_history:
                    ticker_history_result = persist_ticker_daily_history(
                        conn,
                        history,
                        ticker=ticker,
                        provenance_source_label=provenance_source_label,
                        market_data_source=provider.name,
                        run_id=run_id,
                        replace_existing=replace_existing,
                    )
                    ticker_history_rows_written += int(ticker_history_result["rows_written"])
                    ticker_history_rows_skipped += int(ticker_history_result["rows_skipped"])
                ticker_history_frames.append(history)
            except Exception:
                failed_tickers.append(ticker)

        history_df = pd.concat(ticker_history_frames, ignore_index=True) if ticker_history_frames else pd.DataFrame()
        perf_df = _compute_daily_perf(history_df, requested_start, requested_end)
        if perf_df.empty:
            _finalize_reconstruction_run(
                conn,
                run_id,
                status="success",
                ticker_count=len(scoped_tickers),
                theme_count=len(scoped_theme_ids),
                ticker_history_rows_written=ticker_history_rows_written,
                ticker_history_rows_skipped=ticker_history_rows_skipped,
                snapshot_rows_written=0,
                snapshot_rows_skipped=0,
                failed_tickers=",".join(failed_tickers) if failed_tickers else None,
            )
            return {
                "run_id": run_id,
                "status": "success",
                "ticker_history_rows_written": ticker_history_rows_written,
                "ticker_history_rows_skipped": ticker_history_rows_skipped,
                "snapshot_rows_written": 0,
                "snapshot_rows_skipped": 0,
                "failed_tickers": failed_tickers,
                "available_snapshot_dates": [],
            }

        snapshot_dates = sorted(pd.to_datetime(perf_df["snapshot_date"]).dt.date.unique().tolist())
        rows_written = 0
        rows_skipped = 0

        membership_base = membership[["theme_id", "theme", "category", "is_active", "ticker"]].copy()
        status_df = conn.execute(
            """
            SELECT ticker, COALESCE(status, 'active') <> 'refresh_suppressed' AS calculation_eligible
            FROM symbol_refresh_status
            """
        ).df()
        if status_df.empty:
            membership_base["calculation_eligible"] = True
        else:
            membership_base = membership_base.merge(status_df, on="ticker", how="left")
            membership_base["calculation_eligible"] = membership_base["calculation_eligible"].combine_first(
                pd.Series(True, index=membership_base.index, dtype="boolean")
            ).astype(bool)
        for snapshot_date in snapshot_dates:
            daily_perf = perf_df[pd.to_datetime(perf_df["snapshot_date"]).dt.date == snapshot_date][
                ["ticker", "perf_1w", "perf_1m", "perf_3m"]
            ].copy()
            raw = membership_base.merge(daily_perf, on="ticker", how="left")
            metrics = _compute_theme_metrics(raw)
            if metrics.empty:
                continue

            metrics["run_id"] = run_id
            metrics["snapshot_date"] = snapshot_date
            metrics["snapshot_time"] = _target_snapshot_time(snapshot_date)
            metrics["provenance_class"] = "reconstructed"
            metrics["provenance_source_label"] = provenance_source_label
            metrics["market_data_source"] = provider.name
            metrics["membership_basis"] = "current_governed_membership"

            for row in metrics.itertuples(index=False):
                exists = conn.execute(
                    """
                    SELECT 1
                    FROM reconstructed_theme_snapshots
                    WHERE snapshot_date = ? AND theme_id = ? AND provenance_source_label = ?
                    LIMIT 1
                    """,
                    [row.snapshot_date, int(row.theme_id), provenance_source_label],
                ).fetchone()
                if exists and not replace_existing:
                    rows_skipped += 1
                    continue
                if exists and replace_existing:
                    conn.execute(
                        """
                        DELETE FROM reconstructed_theme_snapshots
                        WHERE snapshot_date = ? AND theme_id = ? AND provenance_source_label = ?
                        """,
                        [row.snapshot_date, int(row.theme_id), provenance_source_label],
                    )

                conn.execute(
                    """
                    INSERT INTO reconstructed_theme_snapshots(
                        run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                        avg_1w, avg_1m, avg_3m,
                        positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                        composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        int(row.run_id),
                        row.snapshot_date,
                        row.snapshot_time,
                        int(row.theme_id),
                        int(row.ticker_count),
                        row.avg_1w,
                        row.avg_1m,
                        row.avg_3m,
                        row.positive_1w_breadth_pct,
                        row.positive_1m_breadth_pct,
                        row.positive_3m_breadth_pct,
                        row.composite_score,
                        row.provenance_class,
                        row.provenance_source_label,
                        row.market_data_source,
                        row.membership_basis,
                    ],
                )
                rows_written += 1

        _finalize_reconstruction_run(
            conn,
            run_id,
            status="partial" if failed_tickers else "success",
            ticker_count=len(scoped_tickers),
            theme_count=len(scoped_theme_ids),
            ticker_history_rows_written=ticker_history_rows_written,
            ticker_history_rows_skipped=ticker_history_rows_skipped,
            snapshot_rows_written=rows_written,
            snapshot_rows_skipped=rows_skipped,
            failed_tickers=",".join(failed_tickers) if failed_tickers else None,
        )
        return {
            "run_id": run_id,
            "status": "partial" if failed_tickers else "success",
            "ticker_history_rows_written": ticker_history_rows_written,
            "ticker_history_rows_skipped": ticker_history_rows_skipped,
            "snapshot_rows_written": rows_written,
            "snapshot_rows_skipped": rows_skipped,
            "failed_tickers": failed_tickers,
            "available_snapshot_dates": snapshot_dates,
            "market_data_source": provider.name,
            "theme_ids": scoped_theme_ids,
        }
    except Exception as exc:
        _finalize_reconstruction_run(
            conn,
            run_id,
            status="failed",
            ticker_count=len(scoped_tickers),
            theme_count=len(scoped_theme_ids),
            ticker_history_rows_written=ticker_history_rows_written,
            ticker_history_rows_skipped=ticker_history_rows_skipped,
            snapshot_rows_written=0,
            snapshot_rows_skipped=0,
            failed_tickers=",".join(failed_tickers) if failed_tickers else None,
            error_message=str(exc),
        )
        raise


def run_daily_historical_append(
    conn,
    *,
    provider_name: str = "live",
    target_date: date | datetime | str,
    replace_existing: bool = False,
) -> dict[str, object]:
    target = _normalize_date(target_date)
    return reconstruct_theme_history_range(
        conn,
        provider_name=provider_name,
        start_date=target,
        end_date=target,
        provenance_source_label="daily_historical_append",
        run_kind="daily_historical_append",
        replace_existing=replace_existing,
    )


def rebuild_recent_reconstructed_history(
    conn,
    *,
    tickers: list[str] | None = None,
    theme_ids: list[int] | None = None,
    lookback_days: int = SUPPRESSION_REBUILD_LOOKBACK_DAYS,
) -> dict[str, object]:
    normalized_tickers = sorted({str(t or "").strip().upper() for t in (tickers or []) if str(t or "").strip()})
    normalized_theme_ids = sorted({int(theme_id) for theme_id in (theme_ids or [])})
    membership = _scope_membership(conn, tickers=normalized_tickers or None, theme_ids=normalized_theme_ids or None)
    if membership.empty:
        return {
            "status": "no_scope",
            "affected_theme_ids": [],
            "affected_tickers": normalized_tickers,
            "labels_rebuilt": [],
            "rows_replaced": 0,
            "rows_written": 0,
        }

    scoped_theme_ids = sorted(membership["theme_id"].astype(int).unique().tolist())
    scoped_tickers = sorted(membership["ticker"].astype(str).str.strip().str.upper().unique().tolist())
    market_data_source = _preferred_stored_history_source(conn)
    if not market_data_source:
        return {
            "status": "no_ticker_history",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "labels_rebuilt": [],
            "rows_replaced": 0,
            "rows_written": 0,
        }

    latest_row = conn.execute(
        """
        SELECT MAX(trading_date)
        FROM ticker_daily_history
        WHERE market_data_source = ?
        """,
        [market_data_source],
    ).fetchone()
    latest_trading_date = latest_row[0] if latest_row and latest_row[0] else None
    if latest_trading_date is None:
        return {
            "status": "no_ticker_history",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "labels_rebuilt": [],
            "rows_replaced": 0,
            "rows_written": 0,
        }

    end_date = _normalize_date(latest_trading_date)
    start_date = end_date - timedelta(days=int(lookback_days))
    label_rows = conn.execute(
        f"""
        SELECT DISTINCT provenance_source_label
        FROM reconstructed_theme_snapshots
        WHERE theme_id IN ({", ".join(["?"] * len(scoped_theme_ids))})
          AND market_data_source = ?
          AND snapshot_date BETWEEN ? AND ?
        ORDER BY provenance_source_label
        """,
        [*scoped_theme_ids, market_data_source, start_date, end_date],
    ).fetchall()
    labels = [str(row[0]) for row in label_rows if row and row[0]]
    if not labels:
        return {
            "status": "no_reconstructed_scope",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "labels_rebuilt": [],
            "rows_replaced": 0,
            "rows_written": 0,
            "window_start": start_date,
            "window_end": end_date,
            "market_data_source": market_data_source,
        }

    stored_history = conn.execute(
        f"""
        SELECT
            ticker,
            trading_date AS snapshot_date,
            close
        FROM ticker_daily_history
        WHERE market_data_source = ?
          AND ticker IN ({", ".join(["?"] * len(scoped_tickers))})
          AND trading_date BETWEEN ? AND ?
        ORDER BY ticker, trading_date
        """,
        [
            market_data_source,
            *scoped_tickers,
            start_date - timedelta(days=HISTORICAL_LOOKBACK_BUFFER_DAYS),
            end_date,
        ],
    ).df()
    if stored_history.empty:
        return {
            "status": "no_history_rows",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "labels_rebuilt": labels,
            "rows_replaced": 0,
            "rows_written": 0,
            "window_start": start_date,
            "window_end": end_date,
            "market_data_source": market_data_source,
        }

    perf_df = _compute_daily_perf(stored_history, start_date, end_date)
    if perf_df.empty:
        return {
            "status": "no_history_rows",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "labels_rebuilt": labels,
            "rows_replaced": 0,
            "rows_written": 0,
            "window_start": start_date,
            "window_end": end_date,
            "market_data_source": market_data_source,
        }

    membership_base = membership[["theme_id", "theme", "category", "is_active", "ticker"]].copy()
    status_df = conn.execute(
        """
        SELECT ticker, COALESCE(status, 'active') <> 'refresh_suppressed' AS calculation_eligible
        FROM symbol_refresh_status
        """
    ).df()
    if status_df.empty:
        membership_base["calculation_eligible"] = True
    else:
        membership_base = membership_base.merge(status_df, on="ticker", how="left")
        membership_base["calculation_eligible"] = membership_base["calculation_eligible"].combine_first(
            pd.Series(True, index=membership_base.index, dtype="boolean")
        ).astype(bool)

    snapshot_dates = sorted(pd.to_datetime(perf_df["snapshot_date"]).dt.date.unique().tolist())
    rows_replaced = 0
    rows_written = 0
    affected_theme_names = sorted(membership["theme"].astype(str).unique().tolist())
    run_id = _insert_reconstruction_run(
        conn,
        run_kind="suppression_rebuild",
        provenance_source_label="suppression_rebuild",
        market_data_source=market_data_source,
        start_date=start_date,
        end_date=end_date,
        tickers=normalized_tickers,
        theme_ids=scoped_theme_ids,
    )

    conn.execute("BEGIN TRANSACTION")
    try:
        for label in labels:
            deleted = conn.execute(
                f"""
                DELETE FROM reconstructed_theme_snapshots
                WHERE theme_id IN ({", ".join(["?"] * len(scoped_theme_ids))})
                  AND market_data_source = ?
                  AND provenance_source_label = ?
                  AND snapshot_date BETWEEN ? AND ?
                RETURNING theme_id
                """,
                [*scoped_theme_ids, market_data_source, label, start_date, end_date],
            ).fetchall()
            rows_replaced += len(deleted)

            for snapshot_date in snapshot_dates:
                daily_perf = perf_df[pd.to_datetime(perf_df["snapshot_date"]).dt.date == snapshot_date][
                    ["ticker", "perf_1w", "perf_1m", "perf_3m"]
                ].copy()
                raw = membership_base.merge(daily_perf, on="ticker", how="left")
                metrics = _compute_theme_metrics(raw)
                if metrics.empty:
                    continue

                metrics["run_id"] = run_id
                metrics["snapshot_date"] = snapshot_date
                metrics["snapshot_time"] = _target_snapshot_time(snapshot_date)
                metrics["provenance_class"] = "reconstructed"
                metrics["provenance_source_label"] = label
                metrics["market_data_source"] = market_data_source
                metrics["membership_basis"] = "current_governed_membership"

                for row in metrics.itertuples(index=False):
                    conn.execute(
                        """
                        INSERT INTO reconstructed_theme_snapshots(
                            run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                            avg_1w, avg_1m, avg_3m,
                            positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                            composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            row.run_id,
                            row.snapshot_date,
                            row.snapshot_time,
                            int(row.theme_id),
                            int(row.ticker_count),
                            row.avg_1w,
                            row.avg_1m,
                            row.avg_3m,
                            row.positive_1w_breadth_pct,
                            row.positive_1m_breadth_pct,
                            row.positive_3m_breadth_pct,
                            row.composite_score,
                            row.provenance_class,
                            row.provenance_source_label,
                            row.market_data_source,
                            row.membership_basis,
                        ],
                    )
                    rows_written += 1
        conn.execute("COMMIT")
        _finalize_reconstruction_run(
            conn,
            run_id,
            status="success" if (rows_replaced or rows_written) else "success",
            ticker_count=len(scoped_tickers),
            theme_count=len(scoped_theme_ids),
            ticker_history_rows_written=0,
            ticker_history_rows_skipped=0,
            snapshot_rows_written=rows_written,
            snapshot_rows_skipped=0,
            failed_tickers=None,
        )
    except Exception:
        conn.execute("ROLLBACK")
        _finalize_reconstruction_run(
            conn,
            run_id,
            status="failed",
            ticker_count=len(scoped_tickers),
            theme_count=len(scoped_theme_ids),
            ticker_history_rows_written=0,
            ticker_history_rows_skipped=0,
            snapshot_rows_written=0,
            snapshot_rows_skipped=0,
            failed_tickers=None,
        )
        raise

    return {
        "run_id": run_id,
        "status": "success" if (rows_replaced or rows_written) else "no_op",
        "affected_theme_ids": scoped_theme_ids,
        "affected_theme_names": affected_theme_names,
        "affected_tickers": scoped_tickers,
        "labels_rebuilt": labels,
        "rows_replaced": rows_replaced,
        "rows_written": rows_written,
        "window_start": start_date,
        "window_end": end_date,
        "market_data_source": market_data_source,
    }
