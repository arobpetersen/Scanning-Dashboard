from __future__ import annotations

import numpy as np
import pandas as pd

from .config import (
    COMPOSITE_WEIGHTS,
    CURRENT_RANKING_MIN_DOLLAR_VOLUME,
    CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS,
    CURRENT_RANKING_MIN_PRICE,
    CURRENT_RANKING_RETURN_CAP_PCT,
    THEME_CONFIDENCE_FULL_COUNT,
)
from .queries import latest_ticker_snapshots, preferred_theme_snapshot_source


METRIC_COLUMNS = [
    "ticker_count",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "positive_1w_breadth_pct",
    "positive_1m_breadth_pct",
    "positive_3m_breadth_pct",
    "composite_score",
]


CURRENT_RANKING_COLUMNS = [
    "theme_id",
    "theme",
    "category",
    "is_active",
    "run_id",
    "snapshot_time",
    "ticker_count",
    "eligible_ticker_count",
    "eligible_1w_count",
    "eligible_1m_count",
    "eligible_3m_count",
    "eligible_composite_count",
    "eligible_breadth_pct",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "positive_1w_breadth_pct",
    "positive_1m_breadth_pct",
    "positive_3m_breadth_pct",
    "composite_score",
]


def theme_confidence_factor(ticker_count: int | float) -> float:
    if pd.isna(ticker_count) or float(ticker_count) <= 0:
        return 0.0
    return min(1.0, (float(ticker_count) / float(THEME_CONFIDENCE_FULL_COUNT)) ** 0.5)


def _compute_theme_metrics(raw: pd.DataFrame) -> pd.DataFrame:
    grouped = raw.groupby(["theme_id", "theme", "category", "is_active"], dropna=False)
    out = grouped.agg(
        ticker_count=("ticker", "count"),
        avg_1w=("perf_1w", "mean"),
        avg_1m=("perf_1m", "mean"),
        avg_3m=("perf_3m", "mean"),
        positive_1w_breadth_pct=("perf_1w", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
        positive_1m_breadth_pct=("perf_1m", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
        positive_3m_breadth_pct=("perf_3m", lambda s: (s.dropna().gt(0).mean() * 100) if len(s.dropna()) else 0),
    ).reset_index()

    base_score = (
        COMPOSITE_WEIGHTS["perf_1w"] * out["avg_1w"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_1m"] * out["avg_1m"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_3m"] * out["avg_3m"].fillna(0)
    )
    out["composite_score"] = base_score * out["ticker_count"].apply(theme_confidence_factor)

    out[METRIC_COLUMNS[1:]] = out[METRIC_COLUMNS[1:]].round(2)
    return out


def compute_theme_metrics_for_run(conn, run_id: int) -> pd.DataFrame:
    raw = conn.execute(
        """
        SELECT t.id AS theme_id, t.name AS theme, t.category, t.is_active,
               m.ticker, s.perf_1w, s.perf_1m, s.perf_3m
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        LEFT JOIN ticker_snapshots s ON s.ticker = m.ticker AND s.run_id = ?
        """,
        [run_id],
    ).df()

    if raw.empty:
        return pd.DataFrame(columns=["theme_id", "theme", "category", "is_active", *METRIC_COLUMNS])
    return _compute_theme_metrics(raw)


def persist_theme_snapshot_for_run(conn, run_id: int) -> None:
    metrics = compute_theme_metrics_for_run(conn, run_id)
    if metrics.empty:
        return

    metrics = metrics.copy()
    run_meta = conn.execute("SELECT finished_at, provider FROM refresh_runs WHERE run_id = ?", [run_id]).fetchone()
    snapshot_time = run_meta[0] if run_meta else None
    source = (run_meta[1] if run_meta and run_meta[1] in {"live", "mock", "synthetic_backfill"} else "live")
    metrics["run_id"] = run_id
    metrics["snapshot_time"] = snapshot_time
    metrics["snapshot_source"] = source

    conn.register("theme_snapshot_incoming", metrics)
    conn.execute(
        """
        INSERT OR REPLACE INTO theme_snapshots(
            run_id, snapshot_time, theme_id, ticker_count,
            avg_1w, avg_1m, avg_3m,
            positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
            composite_score, snapshot_source
        )
        SELECT run_id, snapshot_time, theme_id, ticker_count,
               avg_1w, avg_1m, avg_3m,
               positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
               composite_score, snapshot_source
        FROM theme_snapshot_incoming
        """
    )
    conn.unregister("theme_snapshot_incoming")


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_columns()
        WHERE table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_tables()
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _safe_numeric(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.where(np.isfinite(values), np.nan)


def _load_current_ranking_constituents(conn) -> pd.DataFrame:
    membership = conn.execute(
        """
        SELECT
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            m.ticker
        FROM themes t
        LEFT JOIN theme_membership m ON m.theme_id = t.id
        """
    ).df()
    if membership.empty:
        return membership

    latest = latest_ticker_snapshots(conn)
    if latest.empty:
        for col in ("run_id", "snapshot_time", "price", "avg_volume", "perf_1w", "perf_1m", "perf_3m"):
            membership[col] = np.nan
        membership["status"] = None
        return membership

    latest = latest.copy()
    for col in ("price", "avg_volume", "perf_1w", "perf_1m", "perf_3m"):
        if col not in latest.columns:
            latest[col] = np.nan

    if _table_exists(conn, "symbol_refresh_status"):
        status_cols = ["ticker"]
        if _table_has_column(conn, "symbol_refresh_status", "status"):
            status_cols.append("status")
        statuses = conn.execute(f"SELECT {', '.join(status_cols)} FROM symbol_refresh_status").df()
        if "status" not in statuses.columns:
            statuses["status"] = None
    else:
        statuses = pd.DataFrame(columns=["ticker", "status"])

    raw = membership.merge(latest, on="ticker", how="left")
    raw = raw.merge(statuses[["ticker", "status"]], on="ticker", how="left", suffixes=("", "_symbol"))
    return raw


def _build_current_ranking_metrics(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=CURRENT_RANKING_COLUMNS)

    prepared = raw.copy()
    for col in ("price", "avg_volume", "perf_1w", "perf_1m", "perf_3m"):
        prepared[col] = _safe_numeric(prepared.get(col))

    prepared["run_id"] = _safe_numeric(prepared.get("run_id"))
    prepared["snapshot_time"] = pd.to_datetime(prepared.get("snapshot_time"), errors="coerce")
    prepared["snapshot_present"] = prepared["run_id"].notna() & prepared["snapshot_time"].notna()
    prepared["price_valid"] = prepared["price"].notna() & (prepared["price"] >= CURRENT_RANKING_MIN_PRICE)
    prepared["avg_volume_valid"] = prepared["avg_volume"].notna() & (prepared["avg_volume"] > 0)
    prepared["dollar_volume"] = prepared["price"] * prepared["avg_volume"]
    prepared["dollar_volume_valid"] = prepared["dollar_volume"].notna() & (
        prepared["dollar_volume"] >= CURRENT_RANKING_MIN_DOLLAR_VOLUME
    )
    prepared["not_refresh_suppressed"] = prepared.get("status", pd.Series(index=prepared.index)).fillna("active") != "refresh_suppressed"
    prepared["eligible_ticker"] = (
        prepared["snapshot_present"]
        & prepared["price_valid"]
        & prepared["avg_volume_valid"]
        & prepared["dollar_volume_valid"]
        & prepared["not_refresh_suppressed"]
    )

    capped_return_cols: dict[str, str] = {}
    for perf_col in ("perf_1w", "perf_1m", "perf_3m"):
        eligible_col = f"{perf_col}_eligible"
        capped_col = f"{perf_col}_capped"
        prepared[eligible_col] = prepared["eligible_ticker"] & prepared[perf_col].notna()
        prepared[capped_col] = prepared[perf_col].clip(
            lower=-CURRENT_RANKING_RETURN_CAP_PCT,
            upper=CURRENT_RANKING_RETURN_CAP_PCT,
        )
        capped_return_cols[perf_col] = capped_col

    prepared["composite_metric_eligible"] = (
        prepared["perf_1w_eligible"] & prepared["perf_1m_eligible"] & prepared["perf_3m_eligible"]
    )

    rows: list[dict[str, object]] = []
    group_cols = ["theme_id", "theme", "category", "is_active"]
    for (theme_id, theme, category, is_active), group in prepared.groupby(group_cols, dropna=False):
        ticker_count = int(group["ticker"].notna().sum())
        eligible_ticker_count = int(group["eligible_ticker"].sum())
        eligible_1w_count = int(group["perf_1w_eligible"].sum())
        eligible_1m_count = int(group["perf_1m_eligible"].sum())
        eligible_3m_count = int(group["perf_3m_eligible"].sum())
        eligible_composite_count = int(group["composite_metric_eligible"].sum())

        avg_1w = group.loc[group["perf_1w_eligible"], capped_return_cols["perf_1w"]].mean()
        avg_1m = group.loc[group["perf_1m_eligible"], capped_return_cols["perf_1m"]].mean()
        avg_3m = group.loc[group["perf_3m_eligible"], capped_return_cols["perf_3m"]].mean()

        positive_1w = group.loc[group["perf_1w_eligible"], "perf_1w"].gt(0).mean() * 100 if eligible_1w_count else 0.0
        positive_1m = group.loc[group["perf_1m_eligible"], "perf_1m"].gt(0).mean() * 100 if eligible_1m_count else 0.0
        positive_3m = group.loc[group["perf_3m_eligible"], "perf_3m"].gt(0).mean() * 100 if eligible_3m_count else 0.0

        snapshot_time = group["snapshot_time"].dropna().max() if "snapshot_time" in group.columns else pd.NaT
        run_id_series = group["run_id"].dropna() if "run_id" in group.columns else pd.Series(dtype=float)
        run_id = int(run_id_series.max()) if not run_id_series.empty else None
        eligible_breadth_pct = (eligible_ticker_count / ticker_count) * 100.0 if ticker_count > 0 else 0.0

        base_score = (
            COMPOSITE_WEIGHTS["perf_1w"] * (0.0 if pd.isna(avg_1w) else float(avg_1w))
            + COMPOSITE_WEIGHTS["perf_1m"] * (0.0 if pd.isna(avg_1m) else float(avg_1m))
            + COMPOSITE_WEIGHTS["perf_3m"] * (0.0 if pd.isna(avg_3m) else float(avg_3m))
        )
        composite_score = base_score * theme_confidence_factor(ticker_count) if eligible_composite_count else np.nan

        rows.append(
            {
                "theme_id": theme_id,
                "theme": theme,
                "category": category,
                "is_active": is_active,
                "run_id": run_id,
                "snapshot_time": snapshot_time,
                "ticker_count": ticker_count,
                "eligible_ticker_count": eligible_ticker_count,
                "eligible_1w_count": eligible_1w_count,
                "eligible_1m_count": eligible_1m_count,
                "eligible_3m_count": eligible_3m_count,
                "eligible_composite_count": eligible_composite_count,
                "eligible_breadth_pct": eligible_breadth_pct,
                "avg_1w": avg_1w,
                "avg_1m": avg_1m,
                "avg_3m": avg_3m,
                "positive_1w_breadth_pct": positive_1w,
                "positive_1m_breadth_pct": positive_1m,
                "positive_3m_breadth_pct": positive_3m,
                "composite_score": composite_score,
            }
        )

    out = pd.DataFrame(rows, columns=CURRENT_RANKING_COLUMNS)
    if out.empty:
        return out

    numeric_cols = [
        "eligible_breadth_pct",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "positive_1w_breadth_pct",
        "positive_1m_breadth_pct",
        "positive_3m_breadth_pct",
        "composite_score",
    ]
    out[numeric_cols] = out[numeric_cols].round(2)
    return out


def compute_current_theme_metrics(conn) -> pd.DataFrame:
    current_raw = _load_current_ranking_constituents(conn)
    return _build_current_ranking_metrics(current_raw)


def compute_theme_rankings(conn) -> pd.DataFrame:
    current = compute_current_theme_metrics(conn)
    if current.empty:
        return pd.DataFrame()

    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        rankings = current.copy()
        for col in (
            "delta_avg_1w",
            "delta_avg_1m",
            "delta_avg_3m",
            "delta_positive_1m_breadth_pct",
            "delta_composite_score",
        ):
            rankings[col] = np.nan
        rankings = rankings[rankings["eligible_composite_count"] >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS].copy()
        rankings = rankings.sort_values(
            ["composite_score", "positive_1m_breadth_pct", "eligible_composite_count", "theme"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        return rankings

    prior = conn.execute(
        """
        WITH ranked AS (
            SELECT
                ts.theme_id,
                ts.avg_1w,
                ts.avg_1m,
                ts.avg_3m,
                ts.positive_1m_breadth_pct,
                ts.composite_score,
                ROW_NUMBER() OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id DESC) AS rn
            FROM theme_snapshots ts
            WHERE ts.snapshot_source = ?
        )
        SELECT
            theme_id,
            avg_1w AS prev_avg_1w,
            avg_1m AS prev_avg_1m,
            avg_3m AS prev_avg_3m,
            positive_1m_breadth_pct AS prev_positive_1m_breadth_pct,
            composite_score AS prev_composite_score
        FROM ranked
        WHERE rn = 2
        """,
        [preferred_source],
    ).df()

    rankings = current.merge(prior, on="theme_id", how="left")
    rankings["delta_avg_1w"] = (rankings["avg_1w"] - rankings["prev_avg_1w"]).round(2)
    rankings["delta_avg_1m"] = (rankings["avg_1m"] - rankings["prev_avg_1m"]).round(2)
    rankings["delta_avg_3m"] = (rankings["avg_3m"] - rankings["prev_avg_3m"]).round(2)
    rankings["delta_positive_1m_breadth_pct"] = (
        rankings["positive_1m_breadth_pct"] - rankings["prev_positive_1m_breadth_pct"]
    ).round(2)
    rankings["delta_composite_score"] = (rankings["composite_score"] - rankings["prev_composite_score"]).round(2)

    rankings = rankings[rankings["eligible_composite_count"] >= CURRENT_RANKING_MIN_ELIGIBLE_CONSTITUENTS].copy()
    rankings = rankings.sort_values(
        ["composite_score", "positive_1m_breadth_pct", "eligible_composite_count", "theme"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    return rankings
