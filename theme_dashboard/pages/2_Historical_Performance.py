import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.leaderboard_utils import (
    annotate_current_leadership_quality,
    current_leadership_quality_label,
    disambiguate_theme_labels,
    format_top_ticker_leaders,
)
from src.queries import (
    canonical_theme_snapshot_counts,
    theme_ticker_metrics_for_theme_ids,
)
from src.rankings import ticker_standardized_composite_score
from src.streamlit_utils import (
    db_cache_token,
    load_current_ranking_operating_snapshot_cached,
    load_theme_momentum_cached,
    render_dataframe,
    render_feedback_message,
    reset_perf_timings,
    stop_for_database_error,
)
from src.theme_selection import (
    set_theme_selection_state,
)
from src.theme_service import list_themes, seed_if_needed


TABLE_HELP = {
    "theme": "Theme name.",
    "category": "Theme category from the registry.",
    "ticker_count": "Governed member count in the current view.",
    "eligible_contributor_count": "Current eligible preferred-source contributors supporting the theme metrics.",
    "participation_ratio": "Eligible contributors divided by governed ticker count.",
    "rank": "Current rank in the selected snapshot (1 is strongest).",
    "composite_score": "Current standardized composite score used on the Themes page.",
    "comp_atr": "ATR-standardized companion composite score for research comparison.",
    "perf_1d": "Current average 1-day return snapshot value for this theme.",
    "perf_1w": "Current average 1-week return snapshot value for this theme.",
    "perf_1m": "Current average 1-month return snapshot value for this theme.",
    "perf_3m": "Current average 3-month return snapshot value for this theme.",
    "perf_6m": "Current average 6-month return snapshot value for this theme.",
    "breadth_1m": "Current positive 1M breadth across eligible contributors.",
    "leadership_quality": "Current-state leadership quality label aligned to Themes using current eligible contributor count, participation, and breadth.",
    "start_rank": "Theme rank at the start of the selected lookback window.",
    "end_rank": "Theme rank at the end of the selected lookback window.",
    "start_composite": "Composite score at the beginning of the selected window.",
    "end_composite": "Composite score at the end of the selected window.",
    "rank_start": "Theme rank at the start of the selected lookback window.",
    "rank_end": "Theme rank at the end of the selected lookback window.",
    "rank_change": "Start rank minus end rank. Positive values mean rank improved.",
    "momentum_score": "Composite momentum metric combining performance, breadth, and rank change.",
    "delta_composite": "Change in confidence-adjusted composite score from window start to end. Positive means strengthening.",
    "delta_breadth": "Change in positive-breadth participation. Positive means more constituents are contributing.",
    "delta_avg_1w": "Change in average 1-week return over the window.",
    "delta_avg_1m": "Change in average 1-month return over the window.",
    "delta_avg_3m": "Change in average 3-month return over the window.",
    "delta_avg_6m": "Change in average 6-month return over the window.",
    "delta_ticker_count": "Change in constituent count over the selected window.",
    "composite_score_start": "Confidence-adjusted composite score at the beginning of the selected window.",
    "composite_score_end": "Confidence-adjusted composite score at the end of the selected window.",
    "avg_1w": "Average 1-week return snapshot value for this theme.",
    "avg_1m": "Average 1-month return snapshot value for this theme.",
    "avg_3m": "Average 3-month return snapshot value for this theme.",
    "avg_6m": "Average 6-month return snapshot value for this theme.",
    "window_perf": "Primary return metric for this overview window.",
    "signal_label": "Detected inflection category for this theme.",
    "reason": "Why the signal was triggered.",
    "detected_at": "Snapshot timestamp when signal was detected.",
    "priority": "Internal confidence/priority score (higher = stronger).",
}


def _config_for_columns(columns: list[str]) -> dict:
    return {
        col: st.column_config.Column(
            col.replace("_", " ").title(),
            help=TABLE_HELP.get(col, "Computed analytics field for this section."),
        )
        for col in columns
    }


def _display_theme_name_from_row(row, label_by_id: dict[int, str], ids_by_name: dict[str, list[int]]) -> str:
    return _theme_label_for_display(row.get("theme_id"), row.get("theme"), label_by_id, ids_by_name)

def _normalize_theme_identifier(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value).strip()


def _resolve_theme_id(theme_id, fallback_theme_name: str | None, ids_by_name: dict[str, list[int]]) -> int | None:
    normalized_id = _normalize_theme_identifier(theme_id)
    if normalized_id is not None:
        try:
            return int(float(normalized_id))
        except (TypeError, ValueError):
            pass

    normalized_name = str(fallback_theme_name or "").strip()
    matching_ids = ids_by_name.get(normalized_name, [])
    if len(matching_ids) == 1:
        return int(matching_ids[0])
    return None


def _open_theme_in_themes(theme_id, fallback_theme_name: str | None, label_by_id: dict[int, str], ids_by_name: dict[str, list[int]], source: str) -> None:
    resolved_id = _resolve_theme_id(theme_id, fallback_theme_name, ids_by_name)
    if resolved_id is None or resolved_id not in label_by_id:
        fallback_label = fallback_theme_name or "selected theme"
        st.warning(f"Unable to open `{fallback_label}` in Themes because its theme id could not be resolved from the current theme registry.")
        return
    set_theme_selection_state(st.session_state, resolved_id, label_by_id[resolved_id], source)
    st.switch_page("pages/1_Themes.py")


def _theme_option_maps(themes: pd.DataFrame) -> tuple[dict[int, str], dict[str, list[int]]]:
    ids_by_name: dict[str, list[int]] = {}
    base_label_by_id: dict[int, str] = {}
    base_counts: dict[str, int] = {}
    for _, row in themes.iterrows():
        theme_id = int(row["id"])
        name = str(row["name"])
        category = str(row["category"])
        ids_by_name.setdefault(name, []).append(theme_id)
        base_label = f"{name} ({category})"
        base_label_by_id[theme_id] = base_label
        base_counts[base_label] = base_counts.get(base_label, 0) + 1

    label_by_id: dict[int, str] = {}
    for _, row in themes.iterrows():
        theme_id = int(row["id"])
        base_label = base_label_by_id[theme_id]
        if base_counts.get(base_label, 0) > 1:
            label_by_id[theme_id] = f"{base_label} [{theme_id}]"
        else:
            label_by_id[theme_id] = base_label

    return label_by_id, ids_by_name


def _theme_label_for_display(theme_id, fallback_theme_name: str | None, label_by_id: dict[int, str], ids_by_name: dict[str, list[int]]) -> str:
    resolved_id = _resolve_theme_id(theme_id, fallback_theme_name, ids_by_name)
    if resolved_id is not None and resolved_id in label_by_id:
        return label_by_id[resolved_id]
    return str(fallback_theme_name or resolved_id or "Unknown theme")


def _render_gain_filter(label: str, *, min_value: float, max_value: float, default: tuple[float, float], key: str) -> tuple[float, float]:
    st.caption(label)
    return st.slider(
        label,
        min_value=min_value,
        max_value=max_value,
        value=default,
        step=1.0,
        key=key,
        label_visibility="collapsed",
    )


def _build_master_research_grid(
    themes: pd.DataFrame,
    summary: pd.DataFrame,
    history: pd.DataFrame,
    current_snapshot: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    summary_base = summary.copy()
    if summary_base.empty:
        return pd.DataFrame()

    theme_registry = themes.rename(
        columns={
            "id": "theme_id",
            "name": "theme",
            "is_active": "is_active_registry",
        }
    )[["theme_id", "theme", "category", "is_active_registry"]].copy()

    current_metrics = current_snapshot.get("theme_metrics", pd.DataFrame()).copy()
    current_rankings = current_snapshot.get("standardized_rankings", pd.DataFrame()).copy()
    if not current_metrics.empty and "standardized_composite_score" in current_metrics.columns:
        current_metrics["composite_score"] = current_metrics["standardized_composite_score"]
    if not current_rankings.empty:
        rank_lookup = current_rankings.copy()
        if "rank" not in rank_lookup.columns:
            rank_lookup = rank_lookup.reset_index(drop=True)
            rank_lookup["rank"] = rank_lookup.index + 1
        rank_lookup = rank_lookup[["theme_id", "rank"]].copy()
    else:
        rank_lookup = pd.DataFrame(columns=["theme_id", "rank"])

    current_cols = [
        "theme_id",
        "ticker_count",
        "eligible_standardized_count",
        "eligible_contributor_count",
        "eligible_breadth_pct",
        "avg_1d",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "composite_score",
        "composite_atr_score",
    ]
    available_current_cols = [col for col in current_cols if col in current_metrics.columns]
    current_base = current_metrics[available_current_cols].copy() if available_current_cols else pd.DataFrame(columns=["theme_id"])
    if not current_base.empty:
        current_base = current_base.merge(rank_lookup, on="theme_id", how="left")
    else:
        current_base = rank_lookup.copy()

    latest_movement_context = pd.DataFrame(columns=["theme_id", "covered_eligible_constituent_count"])
    if not history.empty:
        covered_count_col = next(
            (
                col
                for col in ["covered_eligible_constituent_count", "covered_eligible_contributor_count"]
                if col in history.columns
            ),
            None,
        )
        latest_movement_context = (
            history.sort_values(["snapshot_time", "theme"])
            .groupby("theme_id", as_index=False)
            .tail(1)[["theme_id", covered_count_col]].rename(
                columns={covered_count_col: "covered_eligible_constituent_count"}
            )
            .copy()
            if covered_count_col is not None
            else pd.DataFrame(columns=["theme_id", "covered_eligible_constituent_count"])
        )

    grid = summary_base.merge(theme_registry, on="theme_id", how="outer", suffixes=("", "_registry"))
    grid = grid.merge(current_base, on="theme_id", how="left", suffixes=("", "_current"))
    grid = grid.merge(latest_movement_context, on="theme_id", how="left")

    if "theme_registry" in grid.columns:
        grid["theme"] = grid["theme"].where(grid["theme"].notna(), grid["theme_registry"])
    if "category_registry" in grid.columns:
        grid["category"] = grid["category"].where(grid["category"].notna(), grid["category_registry"])
    if "theme_current" in grid.columns:
        grid["theme"] = grid["theme"].where(grid["theme"].notna(), grid["theme_current"])
    if "category_current" in grid.columns:
        grid["category"] = grid["category"].where(grid["category"].notna(), grid["category_current"])

    grid["is_active"] = (
        grid["is_active_registry"]
        .astype("boolean")
        .fillna(True)
        .astype(bool)
    )
    grid = grid[grid["is_active"]].copy()
    grid["eligible_standardized_count"] = pd.to_numeric(grid.get("eligible_standardized_count"), errors="coerce")
    grid["eligible_contributor_count"] = pd.to_numeric(grid.get("eligible_contributor_count"), errors="coerce")
    covered_counts = pd.to_numeric(grid.get("covered_eligible_constituent_count"), errors="coerce")
    grid["eligible_contributor_count"] = (
        grid["eligible_contributor_count"]
        .where(grid["eligible_contributor_count"].notna(), grid["eligible_standardized_count"])
        .where(
            grid["eligible_contributor_count"].notna() | grid["eligible_standardized_count"].notna(),
            covered_counts,
        )
    )
    grid["ticker_count"] = pd.to_numeric(grid.get("ticker_count"), errors="coerce")
    grid["eligible_contributor_count"] = grid["eligible_contributor_count"].fillna(0)
    grid["ticker_count"] = grid["ticker_count"].fillna(0)
    grid["participation_ratio"] = (
        grid["eligible_contributor_count"] / grid["ticker_count"].replace(0, pd.NA)
    )
    grid["composite_score"] = pd.to_numeric(grid.get("composite_score"), errors="coerce")
    grid["comp_atr"] = pd.to_numeric(grid.get("composite_atr_score"), errors="coerce")
    grid["perf_1d"] = pd.to_numeric(grid.get("avg_1d"), errors="coerce")
    grid["perf_1w"] = pd.to_numeric(grid.get("avg_1w"), errors="coerce")
    grid["perf_1m"] = pd.to_numeric(grid.get("avg_1m"), errors="coerce")
    grid["perf_3m"] = pd.to_numeric(grid.get("avg_3m"), errors="coerce")
    grid["perf_6m"] = pd.to_numeric(grid.get("avg_6m"), errors="coerce")
    grid["breadth_1m"] = pd.to_numeric(grid.get("eligible_breadth_pct"), errors="coerce")
    grid["start_rank"] = pd.to_numeric(grid.get("rank_start"), errors="coerce")
    grid["end_rank"] = pd.to_numeric(grid.get("rank_end"), errors="coerce")
    grid["start_composite"] = pd.to_numeric(grid.get("composite_score_start"), errors="coerce")
    grid["end_composite"] = pd.to_numeric(grid.get("composite_score_end"), errors="coerce")
    grid["delta_composite"] = pd.to_numeric(grid.get("delta_composite"), errors="coerce")
    grid["delta_breadth"] = pd.to_numeric(grid.get("delta_breadth"), errors="coerce")
    grid["momentum_score"] = pd.to_numeric(grid.get("momentum_score"), errors="coerce")
    grid["rank"] = pd.to_numeric(grid.get("rank"), errors="coerce")
    grid = annotate_current_leadership_quality(grid)
    grid["atr_ready"] = grid["comp_atr"].notna()

    ordered_cols = [
        "theme_id",
        "theme",
        "category",
        "ticker_count",
        "eligible_contributor_count",
        "participation_ratio",
        "rank",
        "composite_score",
        "comp_atr",
        "perf_1w",
        "perf_1m",
        "perf_3m",
        "perf_6m",
        "perf_1d",
        "breadth_1m",
        "leadership_quality",
        "start_rank",
        "end_rank",
        "rank_change",
        "start_composite",
        "end_composite",
        "delta_composite",
        "delta_breadth",
        "momentum_score",
        "atr_ready",
    ]
    for col in ordered_cols:
        if col not in grid.columns:
            grid[col] = pd.NA

    return grid[ordered_cols].copy()


def _attach_grid_top_tickers(grid: pd.DataFrame, *, top_k: int = 4) -> pd.DataFrame:
    if grid.empty or "theme_id" not in grid.columns:
        out = grid.copy()
        if "leaders" not in out.columns:
            out["leaders"] = ""
        return out

    out = grid.copy()
    theme_ids = sorted({int(value) for value in out["theme_id"].dropna().astype(int).tolist()})
    if not theme_ids:
        out["leaders"] = ""
        return out

    leaders_by_theme_id: dict[int, str] = {}
    with get_conn() as conn:
        ticker_df = theme_ticker_metrics_for_theme_ids(conn, theme_ids)
    if ticker_df.empty:
        out["leaders"] = ""
        return out

    scored = ticker_df.copy()
    scored["perf_1w"] = pd.to_numeric(scored.get("perf_1w"), errors="coerce")
    scored["perf_1m"] = pd.to_numeric(scored.get("perf_1m"), errors="coerce")
    scored["perf_3m"] = pd.to_numeric(scored.get("perf_3m"), errors="coerce")
    scored["perf_6m"] = pd.to_numeric(scored.get("perf_6m"), errors="coerce")
    scored["ticker_composite_score"] = scored.apply(
        lambda row: ticker_standardized_composite_score(row.get("perf_1w"), row.get("perf_1m"), row.get("perf_3m")),
        axis=1,
    )
    for theme_id, theme_rows in scored.groupby("theme_id", sort=False):
        leaders_by_theme_id[int(theme_id)] = format_top_ticker_leaders(theme_rows, top_k=top_k)

    out["leaders"] = out["theme_id"].map(lambda value: leaders_by_theme_id.get(int(value), "") if pd.notna(value) else "")
    return out


st.set_page_config(page_title="Historical Performance", layout="wide")
st.title("Historical Performance Research Grid")
st.caption("Research workflow: audit current, historical, and ATR-companion theme behavior in one dense grid.")
st.caption(
    "Themes remains the curated operating page. Use this page as the primary historical comparison workbench; "
    "lineage and audit helpers remain separate from the main rank-interpretation surface."
)
reset_perf_timings("historical_performance")

try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        themes = list_themes(conn, active_only=False)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()
theme_label_by_id, theme_ids_by_name = _theme_option_maps(themes)

render_feedback_message(st.session_state, "historical_refresh_feedback")

current_snapshot = load_current_ranking_operating_snapshot_cached(db_token)
lookback_days = 30
analysis_top_n = 20
momentum = load_theme_momentum_cached(db_token, int(lookback_days), top_n=analysis_top_n)
with get_conn() as conn:
    canonical_counts = canonical_theme_snapshot_counts(conn)
    canonical_count_row = canonical_counts.iloc[0] if not canonical_counts.empty else {}
    total_canonical_snapshot_dates = int((canonical_count_row.get("canonical_snapshot_dates") if canonical_count_row is not None else 0) or 0)
    latest_canonical_snapshot_date = canonical_count_row.get("latest_canonical_snapshot_date") if canonical_count_row is not None else None
    snapshot_count_row = conn.execute("SELECT COUNT(DISTINCT snapshot_time) FROM theme_snapshots").fetchone()
    total_theme_snapshot_sets = int((snapshot_count_row[0] if snapshot_count_row else 0) or 0)

history = momentum["history"]
if history.empty:
    st.info(
        f"No snapshots available in the default 30-day research window. Canonical daily dates currently available: {total_canonical_snapshot_dates}; legacy theme snapshot sets currently available: {total_theme_snapshot_sets}. "
        "At least 2 boundary snapshots are required for comparisons. Run another refresh if history is still being seeded."
    )
    st.stop()

snapshot_count = int(history["snapshot_time"].nunique())
if snapshot_count < 2:
    st.warning(
        f"Not enough historical snapshots for the default 30-day research window (have {snapshot_count}, need at least 2 boundary snapshots). "
        f"Canonical daily dates currently stored: {total_canonical_snapshot_dates}; legacy theme snapshot sets currently stored: {total_theme_snapshot_sets}. Run another refresh if appropriate."
    )
    st.stop()

summary = momentum["window_summary"]
window_meta = momentum.get("meta", {})
window_source_preference = str(momentum.get("source_preference") or "unknown")
window_provenance_mix = str(window_meta.get("provenance_mix") or "unknown")
using_canonical_primary = "canonical_daily" in window_provenance_mix

master_grid = _build_master_research_grid(themes, summary, history, current_snapshot)
master_grid = _attach_grid_top_tickers(master_grid, top_k=4)
master_grid = master_grid.reset_index(drop=True)
st.subheader("Master Theme Research Grid")
st.caption(
    "Primary research surface: active themes only, combining current ranking context, ATR companion comparison, and historical start-to-end movement. "
    "This workflow now prefers canonical daily standardized history when available."
)
if using_canonical_primary:
    st.caption(
        f"Primary window source: `canonical_daily` | latest canonical date: `{latest_canonical_snapshot_date or '-'}` | source label: `{window_source_preference}`."
    )
else:
    st.caption(
        f"Primary window source fallback: `{window_provenance_mix}` | source label: `{window_source_preference}`. "
        "Canonical daily coverage was not available for this window, so the older movement-history path was used."
    )
st.caption(
    "Lineage/debug tools are intentionally outside the main workflow. Treat the grid and summary tables above as the primary view; "
    "use audit helpers only when you need to inspect winner selection or provenance edge cases."
)
st.caption("Current-context columns in the master grid are `Rank`, `Composite Score`, `Comp ATR`, and `Perf 1D/1W/1M/3M`. Historical-window movement fields remain separate to the right.")
fg1, fg2, fg3 = st.columns([1.2, 1.2, 1.6])
with fg1:
    category_options = sorted(master_grid["category"].dropna().astype(str).unique().tolist()) if not master_grid.empty else []
    grid_category_filter = st.multiselect(
        "Include Category",
        options=category_options,
        default=[],
        key="historical_grid_category",
        placeholder="All categories",
    )
with fg2:
    theme_options = sorted(master_grid["theme"].dropna().astype(str).unique().tolist()) if not master_grid.empty else []
    theme_filter = st.multiselect(
        "Include Theme",
        options=theme_options,
        default=[],
        key="historical_grid_theme",
        placeholder="All themes",
    )
with fg3:
    quality_options = sorted(master_grid["leadership_quality"].dropna().astype(str).unique().tolist()) if not master_grid.empty else []
    leadership_quality_filter = st.multiselect(
        "Include Leadership Quality",
        options=quality_options,
        default=[],
        key="historical_grid_quality",
        placeholder="All quality labels",
    )

gain_c1, gain_c2, gain_c3 = st.columns(3)
with gain_c1:
    perf_1w_range = _render_gain_filter(
        "1W gain filter",
        min_value=-100.0,
        max_value=100.0,
        default=(-100.0, 100.0),
        key="historical_grid_perf_1w",
    )
with gain_c2:
    perf_1m_range = _render_gain_filter(
        "1M gain filter",
        min_value=-100.0,
        max_value=200.0,
        default=(-100.0, 200.0),
        key="historical_grid_perf_1m",
    )
with gain_c3:
    perf_3m_range = _render_gain_filter(
        "3M gain filter",
        min_value=-100.0,
        max_value=300.0,
        default=(-100.0, 300.0),
        key="historical_grid_perf_3m",
    )

with st.expander("Advanced filters", expanded=False):
    st.caption("Exclude category, theme, or leadership-quality slices here without cluttering the main filter row.")
    advanced_c1, advanced_c2, advanced_c3 = st.columns(3)
    with advanced_c1:
        exclude_category_filter = st.multiselect(
            "Exclude Category",
            options=category_options,
            default=[],
            key="historical_grid_exclude_category",
            placeholder="Exclude none",
        )
    with advanced_c2:
        exclude_theme_filter = st.multiselect(
            "Exclude Theme",
            options=theme_options,
            default=[],
            key="historical_grid_exclude_theme",
            placeholder="Exclude none",
        )
    with advanced_c3:
        exclude_quality_filter = st.multiselect(
            "Exclude Leadership Quality",
            options=quality_options,
            default=[],
            key="historical_grid_exclude_quality",
            placeholder="Exclude none",
        )

filtered_grid = master_grid.copy()
if grid_category_filter:
    filtered_grid = filtered_grid[filtered_grid["category"].astype(str).isin([str(value) for value in grid_category_filter])]
if exclude_category_filter:
    filtered_grid = filtered_grid[~filtered_grid["category"].astype(str).isin([str(value) for value in exclude_category_filter])]
if leadership_quality_filter:
    filtered_grid = filtered_grid[filtered_grid["leadership_quality"].astype(str).isin([str(value) for value in leadership_quality_filter])]
if exclude_quality_filter:
    filtered_grid = filtered_grid[~filtered_grid["leadership_quality"].astype(str).isin([str(value) for value in exclude_quality_filter])]
if theme_filter:
    filtered_grid = filtered_grid[filtered_grid["theme"].astype(str).isin([str(value) for value in theme_filter])]
if exclude_theme_filter:
    filtered_grid = filtered_grid[~filtered_grid["theme"].astype(str).isin([str(value) for value in exclude_theme_filter])]
perf_1w_values = pd.to_numeric(filtered_grid["perf_1w"], errors="coerce")
perf_1m_values = pd.to_numeric(filtered_grid["perf_1m"], errors="coerce")
perf_3m_values = pd.to_numeric(filtered_grid["perf_3m"], errors="coerce")
filtered_grid = filtered_grid[perf_1w_values.between(float(perf_1w_range[0]), float(perf_1w_range[1]), inclusive="both") | perf_1w_values.isna()]
filtered_grid = filtered_grid[perf_1m_values.between(float(perf_1m_range[0]), float(perf_1m_range[1]), inclusive="both") | perf_1m_values.isna()]
filtered_grid = filtered_grid[perf_3m_values.between(float(perf_3m_range[0]), float(perf_3m_range[1]), inclusive="both") | perf_3m_values.isna()]
filtered_grid = filtered_grid.sort_values(
    ["rank", "momentum_score", "delta_composite", "theme"],
    ascending=[True, False, False, True],
    na_position="last",
).reset_index(drop=True)

st.caption(
    f"Filtered themes: `{len(filtered_grid)}` of `{len(master_grid)}` | "
    f"window: `{pd.to_datetime(window_meta.get('window_start')).strftime('%Y-%m-%d') if window_meta.get('window_start') is not None else '-'}` "
    f"to `{pd.to_datetime(window_meta.get('window_end')).strftime('%Y-%m-%d') if window_meta.get('window_end') is not None else '-'}`"
)
master_grid_cols = [
    "theme",
    "category",
    "leaders",
    "rank",
    "composite_score",
    "comp_atr",
    "momentum_score",
    "perf_1d",
    "perf_1w",
    "perf_1m",
    "perf_3m",
    "perf_6m",
    "breadth_1m",
    "leadership_quality",
    "ticker_count",
]
master_display = disambiguate_theme_labels(filtered_grid)
if "theme_display" in master_display.columns:
    master_display["theme"] = master_display["theme_display"]
master_grid_height = max(120, 35 * (len(master_display) + 1) + 2)
render_dataframe(
    "historical_master_research_grid",
    master_display[master_grid_cols],
    width="stretch",
    height=master_grid_height,
    hide_index=True,
    column_config=_config_for_columns(master_grid_cols) | {
        "leaders": st.column_config.TextColumn("Top Tickers", width="medium"),
        "perf_1d": st.column_config.NumberColumn("Perf 1D", format="%.1f%%"),
        "perf_1w": st.column_config.NumberColumn("Perf 1W", format="%.1f%%"),
        "perf_1m": st.column_config.NumberColumn("Perf 1M", format="%.1f%%"),
        "perf_3m": st.column_config.NumberColumn("Perf 3M", format="%.1f%%"),
        "perf_6m": st.column_config.NumberColumn("Perf 6M", format="%.1f%%"),
        "breadth_1m": st.column_config.NumberColumn("Breadth 1M", format="%.1f%%"),
    },
    key="historical_master_research_grid",
)
if not filtered_grid.empty:
    open_theme_options = (
        filtered_grid[["theme_id", "theme"]]
        .dropna(subset=["theme_id"])
        .drop_duplicates(subset=["theme_id"])
        .copy()
    )
    open_theme_options["theme_label"] = open_theme_options.apply(
        lambda row: _display_theme_name_from_row(row, theme_label_by_id, theme_ids_by_name),
        axis=1,
    )
    open_theme_options = open_theme_options.sort_values("theme_label", kind="stable").reset_index(drop=True)
    selected_theme_label = st.selectbox(
        "Open theme in Themes detail",
        options=open_theme_options["theme_label"].tolist(),
        index=0,
        key="historical_open_theme_label",
    )
    if st.button(f"Open `{selected_theme_label}` in Themes detail", key="open_historical_grid_theme"):
        selected_theme_row = open_theme_options.loc[open_theme_options["theme_label"] == selected_theme_label].iloc[0]
        _open_theme_in_themes(
            selected_theme_row.get("theme_id"),
            selected_theme_row.get("theme"),
            theme_label_by_id,
            theme_ids_by_name,
            "historical_grid",
        )
