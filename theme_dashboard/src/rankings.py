from __future__ import annotations

import pandas as pd

from .config import COMPOSITE_WEIGHTS
from .queries import latest_completed_runs


METRIC_COLUMNS = [
from .queries import latest_completed_run_id


RANKING_COLUMNS = [
    "theme_id",
    "theme",
    "category",
    "is_active",
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
def compute_theme_rankings(conn) -> pd.DataFrame:
    run_id = latest_completed_run_id(conn)
    if run_id is None:
        return pd.DataFrame(columns=RANKING_COLUMNS)

    raw = conn.execute(
        """
        SELECT t.id AS theme_id, t.name AS theme, t.category, t.is_active,
               m.ticker, s.perf_1w, s.perf_1m, s.perf_3m
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        LEFT JOIN ticker_snapshots s
          ON m.ticker = s.ticker AND s.run_id = ?
        """,
        [run_id],
    ).df()

    if raw.empty:
        return pd.DataFrame(columns=RANKING_COLUMNS)

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
    snapshot_time = conn.execute("SELECT finished_at FROM refresh_runs WHERE run_id = ?", [run_id]).fetchone()[0]
    metrics["run_id"] = run_id
    metrics["snapshot_time"] = snapshot_time

    conn.register("theme_snapshot_incoming", metrics)
    conn.execute(
        """
        INSERT OR REPLACE INTO theme_snapshots(
            run_id, snapshot_time, theme_id, ticker_count,
            avg_1w, avg_1m, avg_3m,
            positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
            composite_score
        )
        SELECT run_id, snapshot_time, theme_id, ticker_count,
               avg_1w, avg_1m, avg_3m,
               positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
               composite_score
        FROM theme_snapshot_incoming
        """
    )
    conn.unregister("theme_snapshot_incoming")


def compute_theme_rankings(conn) -> pd.DataFrame:
    runs = latest_completed_runs(conn, limit=2)
    if runs.empty:
        return pd.DataFrame()

    latest_run_id = int(runs.iloc[0]["run_id"])
    latest = conn.execute(
        """
        SELECT ts.theme_id, t.name AS theme, t.category, t.is_active,
               ts.ticker_count, ts.avg_1w, ts.avg_1m, ts.avg_3m,
               ts.positive_1w_breadth_pct, ts.positive_1m_breadth_pct, ts.positive_3m_breadth_pct,
               ts.composite_score
        FROM theme_snapshots ts
        JOIN themes t ON t.id = ts.theme_id
        WHERE ts.run_id = ?
        """,
        [latest_run_id],
    ).df()

    if latest.empty:
        return pd.DataFrame()

    if len(runs) > 1:
        prev_run_id = int(runs.iloc[1]["run_id"])
        prev = conn.execute(
            """
            SELECT theme_id, avg_1w, avg_1m, avg_3m, positive_1m_breadth_pct, composite_score
            FROM theme_snapshots
            WHERE run_id = ?
            """,
            [prev_run_id],
        ).df()
        prev = prev.rename(
            columns={
                "avg_1w": "prev_avg_1w",
                "avg_1m": "prev_avg_1m",
                "avg_3m": "prev_avg_3m",
                "positive_1m_breadth_pct": "prev_positive_1m_breadth_pct",
                "composite_score": "prev_composite_score",
            }
        )
        latest = latest.merge(prev, on="theme_id", how="left")
    else:
        latest["prev_avg_1w"] = pd.NA
        latest["prev_avg_1m"] = pd.NA
        latest["prev_avg_3m"] = pd.NA
        latest["prev_positive_1m_breadth_pct"] = pd.NA
        latest["prev_composite_score"] = pd.NA

    latest["delta_avg_1w"] = (latest["avg_1w"] - latest["prev_avg_1w"]).round(2)
    latest["delta_avg_1m"] = (latest["avg_1m"] - latest["prev_avg_1m"]).round(2)
    latest["delta_avg_3m"] = (latest["avg_3m"] - latest["prev_avg_3m"]).round(2)
    latest["delta_positive_1m_breadth_pct"] = (
        latest["positive_1m_breadth_pct"] - latest["prev_positive_1m_breadth_pct"]
    ).round(2)
    latest["delta_composite_score"] = (latest["composite_score"] - latest["prev_composite_score"]).round(2)

    return latest.sort_values("composite_score", ascending=False)
    numeric_cols = [
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "positive_1w_breadth_pct",
        "positive_1m_breadth_pct",
        "positive_3m_breadth_pct",
        "composite_score",
    ]
    out[numeric_cols] = out[numeric_cols].round(2)
    return out[RANKING_COLUMNS].sort_values("composite_score", ascending=False)
