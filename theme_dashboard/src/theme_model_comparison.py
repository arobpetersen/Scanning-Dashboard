from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .queries import preferred_theme_snapshot_source


CONFIDENCE_FULL_COUNT = 8
WINSORIZE_LOWER_Q = 0.10
WINSORIZE_UPPER_Q = 0.90
BREADTH_SIGNAL_MULTIPLIER = 0.50


@dataclass(frozen=True)
class ComparisonMeta:
    source: str
    run_id: int
    snapshot_time: object
    output_csv: Path
    output_markdown: Path


def _safe_mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.mean())


def _winsorized_mean(series: pd.Series, lower_q: float = WINSORIZE_LOWER_Q, upper_q: float = WINSORIZE_UPPER_Q) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    if len(valid) < 5:
        return float(valid.mean())
    lower = float(valid.quantile(lower_q))
    upper = float(valid.quantile(upper_q))
    return float(valid.clip(lower=lower, upper=upper).mean())


def _positive_breadth_pct(series: pd.Series) -> float:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return 0.0
    return float(valid.gt(0).mean() * 100.0)


def _confidence_factor(ticker_count: int, full_count: int = CONFIDENCE_FULL_COUNT) -> float:
    if ticker_count <= 0:
        return 0.0
    return min(1.0, float(ticker_count / full_count) ** 0.5)


def _breadth_signal(positive_1m_breadth_pct: float | int | None) -> float:
    breadth = 0.0 if positive_1m_breadth_pct is None or pd.isna(positive_1m_breadth_pct) else float(positive_1m_breadth_pct)
    return (breadth - 50.0) / 10.0


def _top_abs_share_pct(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna().abs()
    if valid.empty:
        return None
    total = float(valid.sum())
    if total <= 0:
        return None
    return float(valid.max() / total * 100.0)


def load_latest_theme_model_input(conn) -> tuple[pd.DataFrame, str, int, object]:
    preferred_source = preferred_theme_snapshot_source(conn)
    if not preferred_source:
        raise ValueError("No preferred theme snapshot source is available.")

    run_meta = conn.execute(
        """
        SELECT run_id, snapshot_time
        FROM theme_snapshots
        WHERE snapshot_source = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        [preferred_source],
    ).fetchone()
    if not run_meta:
        raise ValueError(f"No theme snapshots found for preferred source `{preferred_source}`.")

    run_id = int(run_meta[0])
    snapshot_time = run_meta[1]
    raw = conn.execute(
        """
        SELECT
            t.id AS theme_id,
            t.name AS theme,
            t.category,
            t.is_active,
            m.ticker,
            s.perf_1w,
            s.perf_1m,
            s.perf_3m
        FROM themes t
        LEFT JOIN theme_membership m ON m.theme_id = t.id
        LEFT JOIN ticker_snapshots s ON s.ticker = m.ticker AND s.run_id = ?
        ORDER BY t.name, m.ticker
        """,
        [run_id],
    ).df()
    return raw, preferred_source, run_id, snapshot_time


def _build_metrics(raw: pd.DataFrame, mean_fn) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    grouped = raw.groupby(["theme_id", "theme", "category", "is_active"], dropna=False)
    for (theme_id, theme, category, is_active), group in grouped:
        perf_1w = pd.to_numeric(group["perf_1w"], errors="coerce")
        perf_1m = pd.to_numeric(group["perf_1m"], errors="coerce")
        perf_3m = pd.to_numeric(group["perf_3m"], errors="coerce")
        ticker_count = int(group["ticker"].notna().sum())
        avg_1w = mean_fn(perf_1w)
        avg_1m = mean_fn(perf_1m)
        avg_3m = mean_fn(perf_3m)
        breadth_1m = _positive_breadth_pct(perf_1m)
        rows.append(
            {
                "theme_id": int(theme_id),
                "theme": theme,
                "category": category,
                "is_active": bool(is_active),
                "ticker_count": ticker_count,
                "avg_1w": avg_1w,
                "avg_1m": avg_1m,
                "avg_3m": avg_3m,
                "positive_1m_breadth_pct": breadth_1m,
                "valid_1m_count": int(perf_1m.dropna().shape[0]),
                "median_1m": float(perf_1m.median()) if not perf_1m.dropna().empty else None,
                "std_1m": float(perf_1m.std()) if perf_1m.dropna().shape[0] >= 2 else None,
                "max_member_1m": float(perf_1m.max()) if not perf_1m.dropna().empty else None,
                "top_abs_share_1m_pct": _top_abs_share_pct(perf_1m),
            }
        )

    out = pd.DataFrame(rows)
    out["composite_score"] = (
        0.25 * out["avg_1w"].fillna(0.0)
        + 0.50 * out["avg_1m"].fillna(0.0)
        + 0.25 * out["avg_3m"].fillna(0.0)
    )
    out["confidence_factor"] = out["ticker_count"].apply(_confidence_factor)
    out["breadth_signal"] = out["positive_1m_breadth_pct"].apply(_breadth_signal)
    out["mean_median_gap_1m"] = out["avg_1m"] - out["median_1m"]
    return out


def _rank_scores(scores: pd.DataFrame, score_col: str, rank_col: str) -> pd.DataFrame:
    ranked = scores.sort_values([score_col, "positive_1m_breadth_pct", "ticker_count", "theme"], ascending=[False, False, False, True]).reset_index(drop=True)
    rank_map = ranked.reset_index()[["theme_id", "index"]].copy()
    rank_map[rank_col] = rank_map["index"] + 1
    return scores.merge(rank_map[["theme_id", rank_col]], on="theme_id", how="left")


def build_theme_model_comparison(raw: pd.DataFrame) -> pd.DataFrame:
    baseline = _build_metrics(raw, _safe_mean).rename(
        columns={
            "avg_1w": "baseline_avg_1w",
            "avg_1m": "baseline_avg_1m",
            "avg_3m": "baseline_avg_3m",
            "composite_score": "baseline_score",
        }
    )
    winsorized = _build_metrics(raw, _winsorized_mean).rename(
        columns={
            "avg_1w": "winsorized_avg_1w",
            "avg_1m": "winsorized_avg_1m",
            "avg_3m": "winsorized_avg_3m",
            "composite_score": "winsorized_score",
        }
    )

    compare = baseline.merge(
        winsorized[["theme_id", "winsorized_avg_1w", "winsorized_avg_1m", "winsorized_avg_3m", "winsorized_score"]],
        on="theme_id",
        how="left",
    )
    compare["confidence_adjusted_score"] = compare["baseline_score"] * compare["confidence_factor"]
    compare["breadth_adjusted_score"] = compare["baseline_score"] + (BREADTH_SIGNAL_MULTIPLIER * compare["breadth_signal"])
    compare["combined_score"] = (
        compare["winsorized_score"] + (BREADTH_SIGNAL_MULTIPLIER * compare["breadth_signal"])
    ) * compare["confidence_factor"]

    compare = _rank_scores(compare, "baseline_score", "baseline_rank")
    compare = _rank_scores(compare, "confidence_adjusted_score", "confidence_rank")
    compare = _rank_scores(compare, "winsorized_score", "winsorized_rank")
    compare = _rank_scores(compare, "breadth_adjusted_score", "breadth_rank")
    compare = _rank_scores(compare, "combined_score", "combined_rank")

    compare["confidence_rank_delta_vs_baseline"] = compare["baseline_rank"] - compare["confidence_rank"]
    compare["winsorized_rank_delta_vs_baseline"] = compare["baseline_rank"] - compare["winsorized_rank"]
    compare["breadth_rank_delta_vs_baseline"] = compare["baseline_rank"] - compare["breadth_rank"]
    compare["combined_rank_delta_vs_baseline"] = compare["baseline_rank"] - compare["combined_rank"]

    rounded_cols = [
        "baseline_avg_1w",
        "baseline_avg_1m",
        "baseline_avg_3m",
        "baseline_score",
        "winsorized_avg_1w",
        "winsorized_avg_1m",
        "winsorized_avg_3m",
        "winsorized_score",
        "positive_1m_breadth_pct",
        "confidence_factor",
        "breadth_signal",
        "confidence_adjusted_score",
        "breadth_adjusted_score",
        "combined_score",
        "median_1m",
        "std_1m",
        "max_member_1m",
        "top_abs_share_1m_pct",
        "mean_median_gap_1m",
    ]
    for col in rounded_cols:
        if col in compare.columns:
            compare[col] = pd.to_numeric(compare[col], errors="coerce").round(2)
    return compare


def _table_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows_"
    render = df.copy().fillna("")
    headers = [str(col) for col in render.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in render.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in render.columns) + " |")
    return "\n".join(lines)


def _top_table(compare: pd.DataFrame, rank_col: str, score_col: str, top_n: int = 10) -> pd.DataFrame:
    cols = [rank_col, "theme", "category", "ticker_count", score_col, "positive_1m_breadth_pct", "top_abs_share_1m_pct"]
    out = compare.sort_values(rank_col).head(top_n)[cols].copy()
    return out.rename(
        columns={
            rank_col: "rank",
            score_col: "score",
            "positive_1m_breadth_pct": "breadth_1m",
            "top_abs_share_1m_pct": "top_abs_share_1m_pct",
        }
    )


def _mover_table(compare: pd.DataFrame, delta_col: str, top_n: int = 10) -> pd.DataFrame:
    cols = ["theme", "category", "ticker_count", "baseline_rank", delta_col, "top_abs_share_1m_pct", "positive_1m_breadth_pct"]
    return compare.sort_values(delta_col, ascending=False).head(top_n)[cols].copy()


def render_comparison_markdown(compare: pd.DataFrame, source: str, run_id: int, snapshot_time: object) -> str:
    low_count = compare[compare["ticker_count"] <= 3].sort_values("combined_rank_delta_vs_baseline").head(10)
    concentrated = compare[compare["top_abs_share_1m_pct"].fillna(0) >= 60].sort_values("combined_rank_delta_vs_baseline").head(10)

    parts = [
        "# Theme Model Comparison",
        "",
        f"- Preferred source: `{source}`",
        f"- Run id: `{run_id}`",
        f"- Snapshot time: `{snapshot_time}`",
        "",
        "## Variant Definitions",
        "",
        "- `baseline`: current production-style composite score using simple mean returns.",
        f"- `confidence_adjusted`: `baseline_score * min(1, sqrt(ticker_count / {CONFIDENCE_FULL_COUNT}))`.",
        f"- `winsorized`: recompute average returns with {int(WINSORIZE_LOWER_Q * 100)}/{int(WINSORIZE_UPPER_Q * 100)} winsorized means when a theme has at least 5 valid members.",
        f"- `breadth_adjusted`: `baseline_score + {BREADTH_SIGNAL_MULTIPLIER:.2f} * ((breadth_1m - 50) / 10)`.",
        f"- `combined`: `(winsorized_score + {BREADTH_SIGNAL_MULTIPLIER:.2f} * breadth_signal) * confidence_factor`.",
        "",
        "## Top 10 By Variant",
        "",
        "### Baseline",
        _table_markdown(_top_table(compare, "baseline_rank", "baseline_score")),
        "",
        "### Confidence Adjusted",
        _table_markdown(_top_table(compare, "confidence_rank", "confidence_adjusted_score")),
        "",
        "### Winsorized",
        _table_markdown(_top_table(compare, "winsorized_rank", "winsorized_score")),
        "",
        "### Breadth Adjusted",
        _table_markdown(_top_table(compare, "breadth_rank", "breadth_adjusted_score")),
        "",
        "### Combined",
        _table_markdown(_top_table(compare, "combined_rank", "combined_score")),
        "",
        "## Largest Upward Movers Vs Baseline",
        "",
        "### Confidence Adjusted",
        _table_markdown(_mover_table(compare, "confidence_rank_delta_vs_baseline")),
        "",
        "### Winsorized",
        _table_markdown(_mover_table(compare, "winsorized_rank_delta_vs_baseline")),
        "",
        "### Breadth Adjusted",
        _table_markdown(_mover_table(compare, "breadth_rank_delta_vs_baseline")),
        "",
        "### Combined",
        _table_markdown(_mover_table(compare, "combined_rank_delta_vs_baseline")),
        "",
        "## Small Themes De-emphasized By Combined Variant",
        "",
        _table_markdown(low_count[["theme", "category", "ticker_count", "baseline_rank", "combined_rank", "combined_rank_delta_vs_baseline"]]),
        "",
        "## Concentrated Themes De-emphasized By Combined Variant",
        "",
        _table_markdown(concentrated[["theme", "category", "ticker_count", "baseline_rank", "combined_rank", "combined_rank_delta_vs_baseline", "top_abs_share_1m_pct"]]),
        "",
    ]
    return "\n".join(parts)


def build_and_write_theme_model_comparison(conn, output_dir: Path) -> ComparisonMeta:
    raw, source, run_id, snapshot_time = load_latest_theme_model_input(conn)
    compare = build_theme_model_comparison(raw)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "theme_model_comparison.csv"
    output_markdown = output_dir / "THEME_MODEL_COMPARISON.md"
    compare.sort_values("baseline_rank").to_csv(output_csv, index=False)
    output_markdown.write_text(render_comparison_markdown(compare, source, run_id, snapshot_time), encoding="utf-8")
    return ComparisonMeta(
        source=source,
        run_id=run_id,
        snapshot_time=snapshot_time,
        output_csv=output_csv,
        output_markdown=output_markdown,
    )
