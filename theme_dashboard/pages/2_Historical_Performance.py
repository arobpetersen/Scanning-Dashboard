import altair as alt
import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.leaderboard_utils import (
    build_window_leaderboard,
    current_leadership_quality_label,
    disambiguate_theme_labels,
    format_top_ticker_leaders,
    historical_concentration_label,
)
from src.queries import (
    baseline_status,
    historical_theme_boundary_debug,
    historical_theme_movement_row_audit,
    theme_ticker_metrics,
    theme_snapshot_history,
)
from src.rankings import ticker_standardized_composite_score
from src.rotation_engine import compute_theme_rotation
from src.streamlit_utils import (
    clear_current_market_view_caches,
    db_cache_token,
    extract_selected_row,
    load_current_ranking_snapshot_cached,
    load_theme_inflections_cached,
    load_theme_momentum_cached,
    queue_feedback_message,
    render_dataframe,
    render_feedback_message,
    reset_perf_timings,
    show_perf_summary,
    stop_for_database_error,
)
from src.theme_selection import (
    set_theme_selection_state,
)
from src.theme_service import list_themes, seed_if_needed


TABLE_HELP = {
    "theme": "Theme name.",
    "category": "Theme category from the registry.",
    "active_status": "Whether the theme is currently active in the registry.",
    "ticker_count": "Governed member count in the current view.",
    "eligible_contributor_count": "Current eligible preferred-source contributors supporting the theme metrics.",
    "participation_ratio": "Eligible contributors divided by governed ticker count.",
    "rank": "Current rank in the selected snapshot (1 is strongest).",
    "composite_score": "Current standardized composite score used on the Themes page.",
    "comp_atr": "ATR-standardized companion composite score for research comparison.",
    "perf_1w": "Current average 1-week return snapshot value for this theme.",
    "perf_1m": "Current average 1-month return snapshot value for this theme.",
    "perf_3m": "Current average 3-month return snapshot value for this theme.",
    "breadth_1m": "Current positive 1M breadth across eligible contributors.",
    "leadership_quality": "Compact current-state label based on contributor count, participation, and breadth.",
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
    "delta_ticker_count": "Change in constituent count over the selected window.",
    "composite_score_start": "Confidence-adjusted composite score at the beginning of the selected window.",
    "composite_score_end": "Confidence-adjusted composite score at the end of the selected window.",
    "avg_1w": "Average 1-week return snapshot value for this theme.",
    "avg_1m": "Average 1-month return snapshot value for this theme.",
    "avg_3m": "Average 3-month return snapshot value for this theme.",
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


def _render_explained_table(title: str, description: str, df: pd.DataFrame, columns: list[str], *, limit: int | None = 10):
    st.write(f"**{title}**")
    st.caption(description)
    display_df = disambiguate_theme_labels(df)
    if "theme_display" in display_df.columns and "theme" in display_df.columns:
        display_df["theme"] = display_df["theme_display"]
    shaped = display_df.reindex(columns=columns)
    show_df = shaped if limit is None else shaped.head(limit)
    render_dataframe(f"explained_{title}", show_df, width="stretch", column_config=_config_for_columns(columns))


def _display_theme_name_from_row(row, label_by_id: dict[int, str], ids_by_name: dict[str, list[int]]) -> str:
    return _theme_label_for_display(row.get("theme_id"), row.get("theme"), label_by_id, ids_by_name)


def _signal_reason_text(row: pd.Series) -> str:
    return (
        f"rank_change {row.get('rank_change', 0):+.0f}, "
        f"momentum_score {row.get('momentum_score', 0):+.2f}, "
        f"delta_composite {row.get('delta_composite', 0):+.2f}, "
        f"delta_breadth {row.get('delta_breadth', 0):+.2f}"
    )


def _format_theme_list(df: pd.DataFrame, preview_limit: int = 5) -> str:
    if df.empty or "theme" not in df.columns:
        return "none"
    display_df = disambiguate_theme_labels(df)
    labels = []
    for label in display_df.get("theme_display", display_df["theme"]).astype(str).tolist():
        cleaned = label.strip()
        if cleaned and cleaned not in labels:
            labels.append(cleaned)
    if not labels:
        return "none"
    shown = labels[:preview_limit]
    if len(labels) > preview_limit:
        shown.append(f"+{len(labels) - preview_limit} more")
    return ", ".join(shown)


def _history_depth_quality(window_meta: dict, summary: pd.DataFrame) -> str:
    snapshot_count = int(window_meta.get("boundary_snapshot_count") or 0)
    provenance_mix = str(window_meta.get("provenance_mix") or "unknown")
    collapsed = bool(window_meta.get("collapsed_to_available_history"))
    theme_count = int(summary.shape[0]) if not summary.empty else 0

    if snapshot_count < 2 or theme_count == 0:
        return "Too shallow"
    if collapsed or "mixed" in provenance_mix or "reconstructed" in provenance_mix:
        return "Mixed"
    return "Good"


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


def _theme_option_maps(themes: pd.DataFrame) -> tuple[dict[int, str], dict[str, int], dict[str, list[int]]]:
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

    id_by_label = {label: theme_id for theme_id, label in label_by_id.items()}
    return label_by_id, id_by_label, ids_by_name


def _theme_label_for_display(theme_id, fallback_theme_name: str | None, label_by_id: dict[int, str], ids_by_name: dict[str, list[int]]) -> str:
    resolved_id = _resolve_theme_id(theme_id, fallback_theme_name, ids_by_name)
    if resolved_id is not None and resolved_id in label_by_id:
        return label_by_id[resolved_id]
    return str(fallback_theme_name or resolved_id or "Unknown theme")


def _build_overview_leaders(momentum: dict, perf_col: str, top_k: int = 10) -> tuple[pd.DataFrame, str | None]:
    return build_window_leaderboard(momentum, perf_col, top_k=top_k)


def _render_overview_panel(title: str, leaders: pd.DataFrame, perf_col: str, message: str | None, key_prefix: str):
    st.markdown(f"**{title}**")
    st.caption("Ranked by end-of-window performance first, with momentum and rank change only as secondary context. This is not a top-momentum table.")
    if message:
        st.info(message)
        return

    display = disambiguate_theme_labels(leaders.rename(columns={perf_col: "window_perf"}))
    if "theme_display" in display.columns and "theme" in display.columns:
        display["theme"] = display["theme_display"]
    cols = ["rank", "theme", "window_perf", "momentum_score", "rank_change"]
    event = render_dataframe(
        f"{key_prefix}_overview",
        display[cols],
        hide_index=True,
        width="stretch",
        column_config=_config_for_columns(cols),
        on_select="rerun",
        selection_mode="single-cell",
        key=f"{key_prefix}_table",
    )

    row_idx = extract_selected_row(event)
    if row_idx is not None and 0 <= row_idx < len(display):
        picked_row = display.iloc[int(row_idx)]
        st.session_state["historical_selected_theme_id"] = _normalize_theme_identifier(picked_row.get("theme_id"))
        st.session_state["historical_selected_theme_name"] = str(picked_row.get("theme") or "")


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
        "eligible_contributor_count",
        "eligible_breadth_pct",
        "avg_1w",
        "avg_1m",
        "avg_3m",
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
        latest_movement_context = (
            history.sort_values(["snapshot_time", "theme"])
            .groupby("theme_id", as_index=False)
            .tail(1)[["theme_id", "covered_eligible_constituent_count"]]
            .copy()
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

    grid["is_active"] = grid["is_active_registry"].fillna(True)
    grid["active_status"] = grid["is_active"].map(lambda value: "Active" if bool(value) else "Inactive")
    grid["eligible_contributor_count"] = pd.to_numeric(grid.get("eligible_contributor_count"), errors="coerce")
    covered_counts = pd.to_numeric(grid.get("covered_eligible_constituent_count"), errors="coerce")
    grid["eligible_contributor_count"] = grid["eligible_contributor_count"].where(grid["eligible_contributor_count"].notna(), covered_counts)
    grid["ticker_count"] = pd.to_numeric(grid.get("ticker_count"), errors="coerce")
    grid["eligible_contributor_count"] = grid["eligible_contributor_count"].fillna(0)
    grid["ticker_count"] = grid["ticker_count"].fillna(0)
    grid["participation_ratio"] = (
        grid["eligible_contributor_count"] / grid["ticker_count"].replace(0, pd.NA)
    )
    grid["composite_score"] = pd.to_numeric(grid.get("composite_score"), errors="coerce")
    grid["comp_atr"] = pd.to_numeric(grid.get("composite_atr_score"), errors="coerce")
    grid["perf_1w"] = pd.to_numeric(grid.get("avg_1w"), errors="coerce")
    grid["perf_1m"] = pd.to_numeric(grid.get("avg_1m"), errors="coerce")
    grid["perf_3m"] = pd.to_numeric(grid.get("avg_3m"), errors="coerce")
    grid["breadth_1m"] = pd.to_numeric(grid.get("eligible_breadth_pct"), errors="coerce")
    grid["start_rank"] = pd.to_numeric(grid.get("rank_start"), errors="coerce")
    grid["end_rank"] = pd.to_numeric(grid.get("rank_end"), errors="coerce")
    grid["start_composite"] = pd.to_numeric(grid.get("composite_score_start"), errors="coerce")
    grid["end_composite"] = pd.to_numeric(grid.get("composite_score_end"), errors="coerce")
    grid["delta_composite"] = pd.to_numeric(grid.get("delta_composite"), errors="coerce")
    grid["delta_breadth"] = pd.to_numeric(grid.get("delta_breadth"), errors="coerce")
    grid["momentum_score"] = pd.to_numeric(grid.get("momentum_score"), errors="coerce")
    grid["rank"] = pd.to_numeric(grid.get("rank"), errors="coerce")
    grid["leadership_quality"] = grid.apply(current_leadership_quality_label, axis=1)
    grid["atr_ready"] = grid["comp_atr"].notna()

    ordered_cols = [
        "theme_id",
        "theme",
        "category",
        "active_status",
        "ticker_count",
        "eligible_contributor_count",
        "participation_ratio",
        "rank",
        "composite_score",
        "comp_atr",
        "perf_1w",
        "perf_1m",
        "perf_3m",
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
    leaders_by_theme_id: dict[int, str] = {}
    with get_conn() as conn:
        for theme_id in out["theme_id"].dropna().astype(int).tolist():
            ticker_df = theme_ticker_metrics(conn, int(theme_id))
            if ticker_df.empty:
                leaders_by_theme_id[int(theme_id)] = ""
                continue
            scored = ticker_df.copy()
            scored["perf_1w"] = pd.to_numeric(scored.get("perf_1w"), errors="coerce")
            scored["perf_1m"] = pd.to_numeric(scored.get("perf_1m"), errors="coerce")
            scored["perf_3m"] = pd.to_numeric(scored.get("perf_3m"), errors="coerce")
            scored["ticker_composite_score"] = scored.apply(
                lambda row: ticker_standardized_composite_score(row.get("perf_1w"), row.get("perf_1m"), row.get("perf_3m")),
                axis=1,
            )
            leaders_by_theme_id[int(theme_id)] = format_top_ticker_leaders(scored, top_k=top_k)

    out["leaders"] = out["theme_id"].map(lambda value: leaders_by_theme_id.get(int(value), "") if pd.notna(value) else "")
    return out


st.set_page_config(page_title="Historical Performance", layout="wide")
st.title("Historical Performance Research Grid")
st.caption("Research workflow: audit current, historical, and ATR-companion theme behavior in one dense grid.")
st.caption("Themes remains the curated operating page. Use this page as the cross-theme experimentation and movement-analysis workbench.")
reset_perf_timings("historical_performance")

try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        themes = list_themes(conn, active_only=False)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()
theme_label_by_id, theme_id_by_label, theme_ids_by_name = _theme_option_maps(themes)

render_feedback_message(st.session_state, "historical_refresh_feedback")

current_snapshot = load_current_ranking_snapshot_cached(db_token)
lookback_days = 30
analysis_top_n = max(20, len(themes))
momentum = load_theme_momentum_cached(db_token, int(lookback_days), top_n=analysis_top_n)
with get_conn() as conn:
    snapshot_count_row = conn.execute("SELECT COUNT(DISTINCT snapshot_time) FROM theme_snapshots").fetchone()
    total_theme_snapshot_sets = int((snapshot_count_row[0] if snapshot_count_row else 0) or 0)

history = momentum["history"]
if history.empty:
    st.info(
        f"No snapshots available in the default 30-day research window. Theme snapshot sets currently available: {total_theme_snapshot_sets}. "
        "At least 2 boundary snapshots are required for comparisons. Run another refresh if history is still being seeded."
    )
    st.stop()

snapshot_count = int(history["snapshot_time"].nunique())
if snapshot_count < 2:
    st.warning(
        f"Not enough historical snapshots for the default 30-day research window (have {snapshot_count}, need at least 2 boundary snapshots). "
        f"Total theme snapshot sets currently stored: {total_theme_snapshot_sets}. Run another refresh if appropriate."
    )
    st.stop()

summary = momentum["window_summary"]
window_meta = momentum.get("meta", {})

master_grid = _build_master_research_grid(themes, summary, history, current_snapshot)
master_grid = _attach_grid_top_tickers(master_grid, top_k=4)
st.subheader("Master Theme Research Grid")
st.caption(
    "Primary research surface: one row per theme combining current ranking context, ATR companion comparison, and historical start-to-end movement."
)
fg1, fg2, fg3 = st.columns(3)
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
    active_filter = st.selectbox("Active state", ["all", "active only", "inactive only"], index=0, key="historical_grid_active")
with fg3:
    quality_options = sorted(master_grid["leadership_quality"].dropna().astype(str).unique().tolist()) if not master_grid.empty else []
    leadership_quality_filter = st.multiselect(
        "Include Leadership Quality",
        options=quality_options,
        default=[],
        key="historical_grid_quality",
        placeholder="All quality labels",
    )

fg4, fg5, fg6 = st.columns(3)
with fg4:
    exclude_category_filter = st.multiselect(
        "Exclude Category",
        options=category_options,
        default=[],
        key="historical_grid_exclude_category",
        placeholder="Exclude none",
    )
with fg5:
    theme_options = sorted(master_grid["theme"].dropna().astype(str).unique().tolist()) if not master_grid.empty else []
    theme_filter = st.multiselect(
        "Include Theme",
        options=theme_options,
        default=[],
        key="historical_grid_theme",
        placeholder="All themes",
    )
with fg6:
    exclude_theme_filter = st.multiselect(
        "Exclude Theme",
        options=theme_options,
        default=[],
        key="historical_grid_exclude_theme",
        placeholder="Exclude none",
    )

fg7, fg8, fg9, fg10 = st.columns(4)
with fg7:
    exclude_quality_filter = st.multiselect(
        "Exclude Leadership Quality",
        options=quality_options,
        default=[],
        key="historical_grid_exclude_quality",
        placeholder="Exclude none",
    )
with fg8:
    perf_1w_range = st.slider(
        "1W gain",
        min_value=-100.0,
        max_value=100.0,
        value=(-100.0, 100.0),
        step=1.0,
        key="historical_grid_perf_1w",
    )
with fg9:
    perf_1m_range = st.slider(
        "1M gain",
        min_value=-100.0,
        max_value=200.0,
        value=(-100.0, 200.0),
        step=1.0,
        key="historical_grid_perf_1m",
    )
with fg10:
    perf_3m_range = st.slider(
        "3M gain",
        min_value=-100.0,
        max_value=300.0,
        value=(-100.0, 300.0),
        step=1.0,
        key="historical_grid_perf_3m",
    )

filtered_grid = master_grid.copy()
if grid_category_filter:
    filtered_grid = filtered_grid[filtered_grid["category"].astype(str).isin([str(value) for value in grid_category_filter])]
if exclude_category_filter:
    filtered_grid = filtered_grid[~filtered_grid["category"].astype(str).isin([str(value) for value in exclude_category_filter])]
if active_filter == "active only":
    filtered_grid = filtered_grid[filtered_grid["active_status"] == "Active"]
elif active_filter == "inactive only":
    filtered_grid = filtered_grid[filtered_grid["active_status"] == "Inactive"]
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
    "ticker_count",
    "rank",
    "composite_score",
    "comp_atr",
    "momentum_score",
    "perf_1w",
    "perf_1m",
    "perf_3m",
    "breadth_1m",
    "leadership_quality",
    "start_rank",
    "end_rank",
]
master_display = disambiguate_theme_labels(filtered_grid)
if "theme_display" in master_display.columns:
    master_display["theme"] = master_display["theme_display"]
master_event = render_dataframe(
    "historical_master_research_grid",
    master_display[master_grid_cols],
    width="stretch",
    height=1200,
    hide_index=True,
    column_config=_config_for_columns(master_grid_cols) | {
        "leaders": st.column_config.TextColumn("Top Tickers", width="medium"),
        "perf_1w": st.column_config.NumberColumn("Perf 1W", format="%.1f%%"),
        "perf_1m": st.column_config.NumberColumn("Perf 1M", format="%.1f%%"),
        "perf_3m": st.column_config.NumberColumn("Perf 3M", format="%.1f%%"),
        "breadth_1m": st.column_config.NumberColumn("Breadth 1M", format="%.1f%%"),
    },
    on_select="rerun",
    selection_mode="single-row",
    key="historical_master_research_grid",
)
master_idx = extract_selected_row(master_event)
if master_idx is not None and 0 <= master_idx < len(filtered_grid):
    picked_row = filtered_grid.iloc[int(master_idx)]
    st.session_state["historical_selected_theme_id"] = _normalize_theme_identifier(picked_row.get("theme_id"))
    st.session_state["historical_selected_theme_name"] = str(picked_row.get("theme") or "")
    picked_theme = _display_theme_name_from_row(picked_row, theme_label_by_id, theme_ids_by_name)
    if st.button(f"Open grid-selected theme `{picked_theme}` in Themes detail", key="open_historical_grid_theme"):
        _open_theme_in_themes(picked_row.get("theme_id"), picked_theme, theme_label_by_id, theme_ids_by_name, "historical_grid")
