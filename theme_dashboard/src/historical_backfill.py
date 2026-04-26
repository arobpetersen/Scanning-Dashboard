from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd

from .config import HISTORICAL_APPEND_PROGRESS_DB_UPDATE_INTERVAL_TICKERS
from .fetch_data import get_provider
from .db_introspection import table_exists, table_has_column
from .rankings import _compute_theme_metrics
from .ticker_history import persist_ticker_daily_history

HISTORICAL_LOOKBACK_BUFFER_DAYS = 220
SUPPRESSION_REBUILD_LOOKBACK_DAYS = 45


def _progress_update_due(index: int, total: int, interval: int) -> bool:
    if total <= 0:
        return False
    if index <= 1 or index >= total:
        return True
    return int(interval) > 0 and index % int(interval) == 0


def _suppressed_ticker_filter_sql(conn, ticker_expr: str) -> str:
    if not table_exists(conn, "symbol_refresh_status"):
        return ""
    if table_has_column(conn, "symbol_refresh_status", "status"):
        return (
            " AND NOT EXISTS ("
            "SELECT 1 FROM symbol_refresh_status s "
            f"WHERE upper(trim(s.ticker)) = upper(trim({ticker_expr})) "
            "AND COALESCE(s.status, 'active') = 'refresh_suppressed'"
            ")"
        )
    if not table_has_column(conn, "symbol_refresh_status", "manual_suppressed"):
        return ""
    return (
        " AND NOT EXISTS ("
        "SELECT 1 FROM symbol_refresh_status s "
        f"WHERE upper(trim(s.ticker)) = upper(trim({ticker_expr})) "
        "AND COALESCE(s.manual_suppressed, FALSE)"
        ")"
    )


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
        clauses.append(
            f"m.theme_id IN (SELECT DISTINCT theme_id FROM theme_membership WHERE upper(trim(ticker)) IN ({placeholders}))"
        )
        params.extend(tickers)
    if theme_ids:
        placeholders = ", ".join(["?"] * len(theme_ids))
        clauses.append(f"m.theme_id IN ({placeholders})")
        params.extend(theme_ids)
    suppression_clause = _suppressed_ticker_filter_sql(conn, "m.ticker")
    if suppression_clause:
        clauses.append(suppression_clause.replace(" AND ", "", 1))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return conn.execute(
        f"""
        SELECT
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            upper(trim(m.ticker)) AS ticker
        FROM themes t
        JOIN theme_membership m ON m.theme_id = t.id
        {where}
        ORDER BY t.id, m.ticker
        """,
        params,
    ).df()


def _compute_daily_perf(history: pd.DataFrame, requested_start: date, requested_end: date) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=["ticker", "snapshot_date", "perf_1w", "perf_1m", "perf_3m", "perf_6m"])

    enriched = history.sort_values(["ticker", "snapshot_date"]).copy()
    grouped = enriched.groupby("ticker")["close"]
    enriched["perf_1w"] = ((grouped.transform(lambda s: s / s.shift(5))) - 1.0) * 100.0
    enriched["perf_1m"] = ((grouped.transform(lambda s: s / s.shift(21))) - 1.0) * 100.0
    enriched["perf_3m"] = ((grouped.transform(lambda s: s / s.shift(63))) - 1.0) * 100.0
    enriched["perf_6m"] = ((grouped.transform(lambda s: s / s.shift(126))) - 1.0) * 100.0
    mask = (pd.to_datetime(enriched["snapshot_date"]).dt.date >= requested_start) & (
        pd.to_datetime(enriched["snapshot_date"]).dt.date <= requested_end
    )
    return enriched.loc[mask, ["ticker", "snapshot_date", "perf_1w", "perf_1m", "perf_3m", "perf_6m"]].copy()


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


def _update_reconstruction_run_progress(conn, run_id: int, **fields) -> None:
    if not fields:
        return
    assignments = ", ".join(f"{column} = ?" for column in fields)
    conn.execute(
        f"""
        UPDATE historical_reconstruction_runs
        SET {assignments}
        WHERE run_id = ?
          AND status = 'running'
        """,
        [*fields.values(), run_id],
    )


def _persist_reconstructed_theme_metrics(
    conn,
    metrics: pd.DataFrame,
    *,
    provenance_source_label: str,
    replace_existing: bool,
) -> tuple[int, int]:
    if metrics.empty:
        return 0, 0

    insert_columns = [
        "run_id",
        "snapshot_date",
        "snapshot_time",
        "theme_id",
        "ticker_count",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "positive_1w_breadth_pct",
        "positive_1m_breadth_pct",
        "positive_3m_breadth_pct",
        "composite_score",
        "provenance_class",
        "provenance_source_label",
        "market_data_source",
        "membership_basis",
    ]
    incoming = metrics[insert_columns].copy()
    incoming["theme_id"] = incoming["theme_id"].astype(int)
    snapshot_date = incoming["snapshot_date"].iloc[0]
    theme_ids = incoming["theme_id"].dropna().astype(int).unique().tolist()
    if not theme_ids:
        return 0, 0

    placeholders = ", ".join(["?"] * len(theme_ids))
    existing_count = int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM reconstructed_theme_snapshots
            WHERE snapshot_date = ?
              AND provenance_source_label = ?
              AND theme_id IN ({placeholders})
            """,
            [snapshot_date, provenance_source_label, *theme_ids],
        ).fetchone()[0]
        or 0
    )

    conn.register("incoming_reconstructed_theme_metrics", incoming)
    quoted_columns = ", ".join(insert_columns)
    selected_columns = ", ".join(f"i.{column}" for column in insert_columns)
    try:
        if replace_existing:
            conn.execute(
                f"""
                DELETE FROM reconstructed_theme_snapshots
                WHERE snapshot_date = ?
                  AND provenance_source_label = ?
                  AND theme_id IN ({placeholders})
                """,
                [snapshot_date, provenance_source_label, *theme_ids],
            )
            inserted = conn.execute(
                f"""
                INSERT INTO reconstructed_theme_snapshots({quoted_columns})
                SELECT {selected_columns}
                FROM incoming_reconstructed_theme_metrics i
                RETURNING theme_id
                """
            ).fetchall()
            return len(inserted), 0

        inserted = conn.execute(
            f"""
            INSERT INTO reconstructed_theme_snapshots({quoted_columns})
            SELECT {selected_columns}
            FROM incoming_reconstructed_theme_metrics i
            WHERE NOT EXISTS (
                SELECT 1
                FROM reconstructed_theme_snapshots existing
                WHERE existing.snapshot_date = i.snapshot_date
                  AND existing.theme_id = i.theme_id
                  AND existing.provenance_source_label = i.provenance_source_label
            )
            RETURNING theme_id
            """
        ).fetchall()
        return len(inserted), existing_count
    finally:
        conn.unregister("incoming_reconstructed_theme_metrics")


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
    _update_reconstruction_run_progress(
        conn,
        run_id,
        ticker_count=len(scoped_tickers),
        theme_count=len(scoped_theme_ids),
        ticker_history_rows_written=0,
        ticker_history_rows_skipped=0,
        snapshot_rows_written=0,
        snapshot_rows_skipped=0,
    )

    try:
        total_tickers = len(scoped_tickers)
        for ticker_index, ticker in enumerate(scoped_tickers, start=1):
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
            if _progress_update_due(
                ticker_index,
                total_tickers,
                HISTORICAL_APPEND_PROGRESS_DB_UPDATE_INTERVAL_TICKERS,
            ):
                _update_reconstruction_run_progress(
                    conn,
                    run_id,
                    ticker_history_rows_written=ticker_history_rows_written,
                    ticker_history_rows_skipped=ticker_history_rows_skipped,
                    failed_tickers=",".join(failed_tickers) if failed_tickers else None,
                )

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
                ["ticker", "perf_1w", "perf_1m", "perf_3m", "perf_6m"]
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

            written, skipped = _persist_reconstructed_theme_metrics(
                conn,
                metrics,
                provenance_source_label=provenance_source_label,
                replace_existing=replace_existing,
            )
            rows_written += int(written)
            rows_skipped += int(skipped)
            _update_reconstruction_run_progress(
                conn,
                run_id,
                snapshot_rows_written=rows_written,
                snapshot_rows_skipped=rows_skipped,
            )

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
                    ["ticker", "perf_1w", "perf_1m", "perf_3m", "perf_6m"]
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
                        INSERT OR REPLACE INTO reconstructed_theme_snapshots(
                            run_id, snapshot_date, snapshot_time, theme_id, ticker_count,
                            avg_1w, avg_1m, avg_3m, avg_6m,
                            positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                            composite_score, provenance_class, provenance_source_label, market_data_source, membership_basis
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            row.avg_6m,
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


def backfill_reconstructed_theme_snapshot_avg_6m(
    conn,
    *,
    tickers: list[str] | None = None,
    theme_ids: list[int] | None = None,
    lookback_days: int = SUPPRESSION_REBUILD_LOOKBACK_DAYS,
) -> dict[str, object]:
    if not table_exists(conn, "reconstructed_theme_snapshots"):
        return {"status": "missing_table", "rows_updated": 0}

    normalized_tickers = sorted({str(t or "").strip().upper() for t in (tickers or []) if str(t or "").strip()})
    normalized_theme_ids = sorted({int(theme_id) for theme_id in (theme_ids or [])})
    membership = _scope_membership(conn, tickers=normalized_tickers or None, theme_ids=normalized_theme_ids or None)
    if membership.empty:
        return {
            "status": "no_scope",
            "affected_theme_ids": [],
            "affected_tickers": normalized_tickers,
            "rows_updated": 0,
        }

    scoped_theme_ids = sorted(membership["theme_id"].astype(int).unique().tolist())
    scoped_tickers = sorted(membership["ticker"].astype(str).str.strip().str.upper().unique().tolist())
    market_data_source = _preferred_stored_history_source(conn)
    if not market_data_source:
        return {
            "status": "no_ticker_history",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "rows_updated": 0,
        }

    latest_row = conn.execute(
        """
        SELECT MAX(snapshot_date)
        FROM reconstructed_theme_snapshots
        WHERE market_data_source = ?
          AND avg_6m IS NULL
        """,
        [market_data_source],
    ).fetchone()
    latest_snapshot_date = latest_row[0] if latest_row and latest_row[0] else None
    if latest_snapshot_date is None:
        return {
            "status": "no_missing_rows",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "rows_updated": 0,
        }

    end_date = _normalize_date(latest_snapshot_date)
    start_date = end_date - timedelta(days=int(lookback_days))
    target_rows = conn.execute(
        f"""
        SELECT snapshot_date, theme_id
        FROM reconstructed_theme_snapshots
        WHERE market_data_source = ?
          AND avg_6m IS NULL
          AND snapshot_date BETWEEN ? AND ?
          AND theme_id IN ({", ".join(["?"] * len(scoped_theme_ids))})
        GROUP BY snapshot_date, theme_id
        ORDER BY snapshot_date, theme_id
        """,
        [market_data_source, start_date, end_date, *scoped_theme_ids],
    ).df()
    if target_rows.empty:
        return {
            "status": "no_missing_rows_in_window",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "rows_updated": 0,
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
            "rows_updated": 0,
            "window_start": start_date,
            "window_end": end_date,
            "market_data_source": market_data_source,
        }

    perf_df = _compute_daily_perf(stored_history, start_date, end_date)
    if perf_df.empty:
        return {
            "status": "no_perf_rows",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "rows_updated": 0,
            "window_start": start_date,
            "window_end": end_date,
            "market_data_source": market_data_source,
        }

    membership_base = membership[["theme_id", "theme", "category", "is_active", "ticker"]].copy()
    if table_exists(conn, "symbol_refresh_status"):
        status_df = conn.execute(
            """
            SELECT ticker, COALESCE(status, 'active') <> 'refresh_suppressed' AS calculation_eligible
            FROM symbol_refresh_status
            """
        ).df()
        if not status_df.empty:
            membership_base = membership_base.merge(status_df, on="ticker", how="left")
    membership_base["calculation_eligible"] = membership_base.get(
        "calculation_eligible",
        pd.Series(True, index=membership_base.index, dtype="boolean"),
    ).combine_first(pd.Series(True, index=membership_base.index, dtype="boolean")).astype(bool)

    updates: list[pd.DataFrame] = []
    target_dates = sorted(pd.to_datetime(target_rows["snapshot_date"]).dt.date.unique().tolist())
    for snapshot_date in target_dates:
        daily_perf = perf_df[pd.to_datetime(perf_df["snapshot_date"]).dt.date == snapshot_date][
            ["ticker", "perf_1w", "perf_1m", "perf_3m", "perf_6m"]
        ].copy()
        raw = membership_base.merge(daily_perf, on="ticker", how="left")
        metrics = _compute_theme_metrics(raw)
        if metrics.empty:
            continue
        metrics["snapshot_date"] = snapshot_date
        updates.append(metrics[["snapshot_date", "theme_id", "avg_6m"]].copy())

    if not updates:
        return {
            "status": "no_updates_computed",
            "affected_theme_ids": scoped_theme_ids,
            "affected_tickers": scoped_tickers,
            "rows_updated": 0,
            "window_start": start_date,
            "window_end": end_date,
            "market_data_source": market_data_source,
        }

    update_df = pd.concat(updates, ignore_index=True).drop_duplicates(subset=["snapshot_date", "theme_id"], keep="last")
    conn.register("reconstructed_avg_6m_updates", update_df)
    updated_rows = conn.execute(
        """
        UPDATE reconstructed_theme_snapshots AS r
        SET avg_6m = u.avg_6m
        FROM reconstructed_avg_6m_updates u
        WHERE r.snapshot_date = u.snapshot_date
          AND r.theme_id = u.theme_id
          AND r.market_data_source = ?
          AND r.avg_6m IS NULL
        RETURNING r.snapshot_date, r.theme_id, r.provenance_source_label
        """,
        [market_data_source],
    ).fetchall()
    conn.unregister("reconstructed_avg_6m_updates")

    return {
        "status": "success" if updated_rows else "no_op",
        "affected_theme_ids": scoped_theme_ids,
        "affected_tickers": scoped_tickers,
        "rows_updated": int(len(updated_rows)),
        "window_start": start_date,
        "window_end": end_date,
        "market_data_source": market_data_source,
    }
