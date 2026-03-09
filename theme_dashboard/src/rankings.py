from __future__ import annotations

import pandas as pd

from .config import COMPOSITE_WEIGHTS
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
