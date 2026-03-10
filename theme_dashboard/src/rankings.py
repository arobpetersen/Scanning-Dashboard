from __future__ import annotations

import pandas as pd

from .config import COMPOSITE_WEIGHTS


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

    out["composite_score"] = (
        COMPOSITE_WEIGHTS["perf_1w"] * out["avg_1w"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_1m"] * out["avg_1m"].fillna(0)
        + COMPOSITE_WEIGHTS["perf_3m"] * out["avg_3m"].fillna(0)
    )

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


def compute_theme_rankings(conn) -> pd.DataFrame:
    rankings = conn.execute(
        """
        WITH ranked AS (
            SELECT
                ts.theme_id,
                ts.run_id,
                ts.snapshot_time,
                ts.ticker_count,
                ts.avg_1w,
                ts.avg_1m,
                ts.avg_3m,
                ts.positive_1w_breadth_pct,
                ts.positive_1m_breadth_pct,
                ts.positive_3m_breadth_pct,
                ts.composite_score,
                LAG(ts.avg_1w) OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id) AS prev_avg_1w,
                LAG(ts.avg_1m) OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id) AS prev_avg_1m,
                LAG(ts.avg_3m) OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id) AS prev_avg_3m,
                LAG(ts.positive_1m_breadth_pct) OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id) AS prev_positive_1m_breadth_pct,
                LAG(ts.composite_score) OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id) AS prev_composite_score,
                ROW_NUMBER() OVER (PARTITION BY ts.theme_id ORDER BY ts.run_id DESC) AS rn
            FROM theme_snapshots ts
        )
        SELECT
            r.theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            r.run_id,
            r.snapshot_time,
            r.ticker_count,
            ROUND(r.avg_1w, 2) AS avg_1w,
            ROUND(r.avg_1m, 2) AS avg_1m,
            ROUND(r.avg_3m, 2) AS avg_3m,
            ROUND(r.positive_1w_breadth_pct, 2) AS positive_1w_breadth_pct,
            ROUND(r.positive_1m_breadth_pct, 2) AS positive_1m_breadth_pct,
            ROUND(r.positive_3m_breadth_pct, 2) AS positive_3m_breadth_pct,
            ROUND(r.composite_score, 2) AS composite_score,
            ROUND(r.avg_1w - r.prev_avg_1w, 2) AS delta_avg_1w,
            ROUND(r.avg_1m - r.prev_avg_1m, 2) AS delta_avg_1m,
            ROUND(r.avg_3m - r.prev_avg_3m, 2) AS delta_avg_3m,
            ROUND(r.positive_1m_breadth_pct - r.prev_positive_1m_breadth_pct, 2) AS delta_positive_1m_breadth_pct,
            ROUND(r.composite_score - r.prev_composite_score, 2) AS delta_composite_score
        FROM ranked r
        JOIN themes t ON t.id = r.theme_id
        WHERE r.rn = 1
        ORDER BY composite_score DESC
        """
    ).df()
    return rankings
