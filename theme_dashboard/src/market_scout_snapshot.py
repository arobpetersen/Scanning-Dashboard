from __future__ import annotations

import numpy as np
import pandas as pd

from .market_context import (
    DOWN_DAY,
    FADE,
    QUIET,
    STRONG_DOWN_DAY,
    STRONG_UP_DAY,
    TREND_UP,
    UP_DAY,
    VOLATILE_CHOP,
    VOLATILE_FADE,
)
from .rankings import (
    _load_current_ranking_constituents,
    compute_current_ranking_operating_snapshot,
    current_ticker_is_eligible,
    ticker_current_momentum_score,
)


def build_top_database_tickers(conn, *, limit: int = 10) -> pd.DataFrame:
    return top_database_ticker_snapshot(_load_current_ranking_constituents(conn), limit=limit)


def build_top_theme_snapshot(conn, *, limit: int = 5) -> pd.DataFrame:
    snapshot = compute_current_ranking_operating_snapshot(conn)
    return top_theme_snapshot(
        snapshot.get("theme_metrics", pd.DataFrame()),
        snapshot.get("standardized_rankings", pd.DataFrame()),
        limit=limit,
    )


def top_database_ticker_snapshot(raw: pd.DataFrame, *, limit: int = 10) -> pd.DataFrame:
    columns = ["Ticker", "Theme", "1W", "1M", "Read"]
    if raw.empty:
        return pd.DataFrame(columns=columns)

    prepared = raw.copy()
    for col in ("ticker", "theme", "status"):
        if col not in prepared.columns:
            prepared[col] = ""
        prepared[col] = prepared[col].fillna("").astype(str)
    if "is_active" not in prepared.columns:
        prepared["is_active"] = True
    for col in ("run_id", "price", "avg_volume", "perf_1d", "perf_1w", "perf_1m", "perf_3m"):
        if col not in prepared.columns:
            prepared[col] = np.nan
        prepared[col] = pd.to_numeric(prepared[col], errors="coerce")

    prepared["ticker"] = prepared["ticker"].str.upper().str.strip()
    prepared["theme"] = prepared["theme"].str.strip()
    prepared["eligible"] = prepared.apply(
        lambda row: current_ticker_is_eligible(
            row.get("price"),
            row.get("avg_volume"),
            row.get("status"),
            snapshot_present=pd.notna(row.get("run_id")),
        ),
        axis=1,
    )
    prepared = prepared[
        prepared["is_active"].astype("boolean").fillna(True).astype(bool)
        & prepared["eligible"]
        & prepared["ticker"].ne("")
        & prepared["theme"].ne("")
    ].copy()
    if prepared.empty:
        return pd.DataFrame(columns=columns)

    prepared["momentum"] = prepared.apply(
        lambda row: ticker_current_momentum_score(
            row.get("perf_1w"),
            row.get("perf_1m"),
            row.get("perf_3m"),
            cap_return_inputs=True,
        ),
        axis=1,
    )
    prepared["sort_score"] = prepared["momentum"].fillna(-np.inf)
    prepared = prepared.sort_values(
        ["sort_score", "perf_1m", "perf_1w", "ticker"],
        ascending=[False, False, False, True],
    )

    rows: list[dict[str, object]] = []
    for ticker, group in prepared.groupby("ticker", sort=False):
        leader = group.iloc[0]
        themes = sorted({str(theme).strip() for theme in group["theme"].tolist() if str(theme).strip()})
        row = {
            "Ticker": ticker,
            "Theme": _theme_context(themes),
            "1W": _format_pct(leader.get("perf_1w")),
            "1M": _format_pct(leader.get("perf_1m")),
            "Read": _ticker_snapshot_read(leader, theme_count=len(themes)),
        }
        if prepared["perf_1d"].notna().any():
            row["1D"] = _format_pct(leader.get("perf_1d"))
        rows.append(row)
        if len(rows) >= int(limit):
            break

    out = pd.DataFrame(rows)
    ordered = ["Ticker", "Theme"]
    if "1D" in out.columns:
        ordered.append("1D")
    ordered.extend(["1W", "1M", "Read"])
    return out[ordered]


def top_theme_snapshot(theme_metrics: pd.DataFrame, rankings: pd.DataFrame | None = None, *, limit: int = 5) -> pd.DataFrame:
    columns = ["Theme", "Rank", "1W", "1M", "Breadth", "Quality / Read"]
    if theme_metrics.empty:
        return pd.DataFrame(columns=columns)

    metrics = theme_metrics.copy()
    for col in (
        "theme_id",
        "rank",
        "avg_1w",
        "avg_1m",
        "positive_1w_breadth_pct",
        "eligible_breadth_pct",
        "standardized_composite_score",
        "current_momentum_score",
    ):
        if col not in metrics.columns:
            metrics[col] = np.nan
        metrics[col] = pd.to_numeric(metrics[col], errors="coerce")
    if "theme" not in metrics.columns:
        metrics["theme"] = ""
    if "is_active" not in metrics.columns:
        metrics["is_active"] = True

    metrics["theme"] = metrics["theme"].fillna("").astype(str).str.strip()
    metrics = metrics[metrics["is_active"].astype("boolean").fillna(True).astype(bool) & metrics["theme"].ne("")].copy()
    if metrics.empty:
        return pd.DataFrame(columns=columns)

    rank_lookup = _rank_lookup(rankings if rankings is not None else pd.DataFrame())
    if rank_lookup:
        metrics["rank"] = metrics["theme_id"].map(rank_lookup).fillna(metrics["rank"])
    if metrics["rank"].isna().all():
        score_col = "standardized_composite_score"
        if metrics[score_col].isna().all():
            score_col = "current_momentum_score"
        metrics = metrics.sort_values([score_col, "theme"], ascending=[False, True]).copy()
        metrics["rank"] = range(1, len(metrics) + 1)

    metrics = metrics.sort_values(["rank", "standardized_composite_score", "theme"], ascending=[True, False, True])
    rows = []
    for _idx, row in metrics.head(max(int(limit), 0)).iterrows():
        breadth = row.get("positive_1w_breadth_pct")
        if pd.isna(breadth):
            breadth = row.get("eligible_breadth_pct")
        rows.append(
            {
                "Theme": row.get("theme") or "-",
                "Rank": _format_rank(row.get("rank")),
                "1W": _format_pct(row.get("avg_1w")),
                "1M": _format_pct(row.get("avg_1m")),
                "Breadth": _format_pct(breadth, decimals=0),
                "Quality / Read": _theme_snapshot_read(row),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def market_backdrop_read_line(context: dict[str, object] | None) -> str:
    if not context:
        return "QQQ context unavailable; read the snapshot without tape confirmation."
    move_label = str(context.get("move_label") or "").strip()
    character_tag = str(context.get("character_tag") or "").strip()
    if move_label in {DOWN_DAY, STRONG_DOWN_DAY}:
        return "Weak QQQ tape means positive clusters deserve more attention."
    if move_label in {UP_DAY, STRONG_UP_DAY} or character_tag == TREND_UP:
        return "Strong QQQ tape means broad strength may be more market-beta driven."
    if character_tag == QUIET:
        return "Quiet QQQ tape means theme clusters may be more group-specific."
    if character_tag in {VOLATILE_CHOP, FADE, VOLATILE_FADE} or "Chop" in character_tag or "Fade" in character_tag:
        return "Choppy or fading QQQ tape adds caution; require cleaner follow-through."
    return "QQQ backdrop is mixed; use breadth and repeated-name checks to prioritize review."


def _theme_context(themes: list[str]) -> str:
    if not themes:
        return "-"
    if len(themes) == 1:
        return themes[0]
    return f"{themes[0]} +{len(themes) - 1}"


def _ticker_snapshot_read(row: pd.Series, *, theme_count: int = 1) -> str:
    perf_1w = _safe_float(row.get("perf_1w"))
    perf_1m = _safe_float(row.get("perf_1m"))
    perf_3m = _safe_float(row.get("perf_3m"))
    if theme_count > 1:
        return "Repeated scout name"
    if perf_1m is not None and perf_1m >= 35.0 and perf_1w is not None and perf_1w >= 12.0:
        return "Extreme 1M leader"
    if perf_1m is not None and perf_1m >= 25.0 and (perf_1w is None or perf_1w < 8.0):
        return "Outlier-led; verify group"
    if perf_1w is not None and perf_1w >= 12.0:
        return "Theme confirmation name"
    if perf_3m is not None and perf_3m >= 30.0:
        return "High momentum; check extension"
    return "Current standout"


def _theme_snapshot_read(row: pd.Series) -> str:
    breadth = _safe_float(row.get("positive_1w_breadth_pct"))
    avg_1w = _safe_float(row.get("avg_1w"))
    avg_1m = _safe_float(row.get("avg_1m"))
    eligible_breadth = _safe_float(row.get("eligible_breadth_pct"))
    if breadth is not None and breadth >= 65.0:
        if avg_1m is not None and avg_1m >= 15.0:
            return "Broad, strong 1M"
        return "Broad leader"
    if eligible_breadth is not None and eligible_breadth < 45.0:
        return "Outlier risk"
    if avg_1w is not None and avg_1w > 0:
        return "Positive current trend."
    return "Watch breadth"


def _rank_lookup(rankings: pd.DataFrame) -> dict[float, int]:
    if rankings is None or rankings.empty or "theme_id" not in rankings.columns:
        return {}
    ordered = rankings.copy()
    if "rank" in ordered.columns:
        return {
            float(row["theme_id"]): int(row["rank"])
            for _idx, row in ordered.dropna(subset=["theme_id", "rank"]).iterrows()
        }
    if "canonical_rank" in ordered.columns:
        return {
            float(row["theme_id"]): int(row["canonical_rank"])
            for _idx, row in ordered.dropna(subset=["theme_id", "canonical_rank"]).iterrows()
        }
    ordered = ordered.reset_index(drop=True)
    return {
        float(row["theme_id"]): int(idx + 1)
        for idx, row in ordered.dropna(subset=["theme_id"]).iterrows()
    }


def _safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _format_pct(value: object, *, decimals: int = 1) -> str:
    number = _safe_float(value)
    if number is None:
        return "-"
    return f"{number:.{int(decimals)}f}%"


def _format_number(value: object, *, decimals: int = 1) -> str:
    number = _safe_float(value)
    if number is None:
        return "-"
    return f"{number:.{int(decimals)}f}"


def _format_rank(value: object) -> str:
    number = _safe_float(value)
    if number is None:
        return "-"
    return f"#{int(number)}"
