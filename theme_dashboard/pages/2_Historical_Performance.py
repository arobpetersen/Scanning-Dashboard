import altair as alt
import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.leaderboard_utils import build_window_leaderboard, disambiguate_theme_labels
from src.queries import historical_theme_boundary_debug, theme_snapshot_history
from src.rotation_engine import compute_theme_rotation
from src.streamlit_utils import (
    db_cache_token,
    extract_selected_row,
    load_theme_inflections_cached,
    load_theme_momentum_cached,
    render_dataframe,
    reset_perf_timings,
    show_perf_summary,
    stop_for_database_error,
)
from src.theme_selection import set_theme_selection_state
from src.theme_service import list_themes, seed_if_needed


TABLE_HELP = {
    "theme": "Theme name.",
    "rank": "Current rank in the selected snapshot (1 is strongest).",
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


st.set_page_config(page_title="Historical Performance", layout="wide")
st.title("Historical Performance & Theme Momentum")
st.caption("Audit historical theme movement, leadership rotation, and provenance-aware change across resolved boundary windows.")
st.caption("Use the Themes page for current leadership and strongest-now views. This page is for what changed across a historical window and how trustworthy that change is.")
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

overview_1w = load_theme_momentum_cached(db_token, 7, top_n=10)
overview_1m = load_theme_momentum_cached(db_token, 30, top_n=10)
overview_3m = load_theme_momentum_cached(db_token, 90, top_n=10)

st.subheader("Theme Movement Analysis")
st.caption(
    "Use this section to understand which themes are improving, weakening, or rotating over the selected window. "
    "This is a movement/rotation workflow, not a simple current-strength leaderboard."
)

c1, c2, c3, c4 = st.columns(4)
with c1:
    window_label = st.selectbox("Lookback window", ["1 week", "1 month", "3 months", "Custom"], index=1)
with c2:
    analysis_top_n = st.slider("Top N analyzed", min_value=5, max_value=50, value=20, step=5)
with c3:
    metric = st.selectbox(
        "Chart metric",
        ["composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count"],
        index=0,
    )
with c4:
    display_mode = st.selectbox("Chart display", ["raw metric", "indexed (100=start)", "rank movement"], index=0)

lookback_days = {"1 week": 7, "1 month": 30, "3 months": 90}.get(window_label, 30)
if window_label == "Custom":
    lookback_days = st.number_input("Custom lookback days", min_value=3, max_value=365, value=45)

st.caption(
    "The movement leaderboard below is always ranked by momentum score for the selected window. "
    "The chart controls only change how the chart/filtering view is built."
)

momentum = load_theme_momentum_cached(db_token, int(lookback_days), top_n=analysis_top_n)
with get_conn() as conn:
    total_theme_snapshot_sets = int(conn.execute("SELECT COUNT(DISTINCT snapshot_time) FROM theme_snapshots").fetchone()[0] or 0)

history = momentum["history"]
if history.empty:
    st.info(
        f"No snapshots available in the selected window. Theme snapshot sets currently available: {total_theme_snapshot_sets}. "
        "At least 2 boundary snapshots are required for comparisons. Run another refresh if history is still being seeded."
    )
    st.stop()

snapshot_count = int(history["snapshot_time"].nunique())
if snapshot_count < 2:
    st.warning(
        f"Not enough historical snapshots for this lookback window (have {snapshot_count}, need at least 2 boundary snapshots). "
        f"Total theme snapshot sets currently stored: {total_theme_snapshot_sets}. Run another refresh if appropriate."
    )
    st.stop()

summary = momentum["window_summary"]
rotation = compute_theme_rotation(summary, analysis_top_n, momentum["new_leaders"], momentum["dropped_leaders"])
inflections = load_theme_inflections_cached(db_token, int(lookback_days), top_n=analysis_top_n)
window_meta = momentum.get("meta", {})
history_depth_quality = _history_depth_quality(window_meta, summary)

w1, w2, w3, w4 = st.columns(4)
w1.metric("Effective window start", str(pd.to_datetime(window_meta.get("window_start")).strftime("%Y-%m-%d")) if window_meta.get("window_start") is not None else "-")
w2.metric("Effective window end", str(pd.to_datetime(window_meta.get("window_end")).strftime("%Y-%m-%d")) if window_meta.get("window_end") is not None else "-")
w3.metric("Boundary snapshots", int(window_meta.get("boundary_snapshot_count") or 0))
w4.metric("History depth quality", history_depth_quality)

c1, c2 = st.columns([2, 1])
with c1:
    st.caption(
        f"Resolved boundaries: `{pd.to_datetime(window_meta.get('window_start')).strftime('%Y-%m-%d') if window_meta.get('window_start') is not None else '-'}` "
        f"to `{pd.to_datetime(window_meta.get('window_end')).strftime('%Y-%m-%d') if window_meta.get('window_end') is not None else '-'}` | "
        f"boundary provenance: `{window_meta.get('boundary_provenance_mix') or 'unknown'}` | "
        f"window provenance: `{window_meta.get('provenance_mix') or 'unknown'}`"
    )
with c2:
    st.caption(f"Themes analyzed: `{int(summary.shape[0])}` | effective days: `{int(window_meta.get('effective_window_days') or 0)}`")

st.caption(
    "Use this strip to judge window trust before reading the tables below. Movement windows resolve boundaries with precedence "
    "`ticker_history_derived > captured > reconstructed` when recent derived rows are available; otherwise the fallback precedence is `captured > ticker_history_derived > reconstructed`."
)
if window_meta.get("collapsed_to_available_history"):
    st.info(
        f"Selected {int(window_meta.get('requested_lookback_days') or 0)}d lookback currently resolves to an effective "
        f"{int(window_meta.get('effective_window_days') or 0)}d boundary window because older snapshots are not yet available."
    )
if any(
    token in str(window_meta.get("provenance_mix") or "")
    for token in ["reconstructed", "mixed", "ticker_history_derived"]
):
    st.caption(
        "Historical movement windows may use captured theme history, reconstructed theme history, or recent ticker-history-derived reconstruction. "
        "Non-captured history applies current governed membership to historical market data and is not a true point-in-time membership record."
    )

st.subheader("Most Improving Themes In This Window")
leaders_tbl = summary.sort_values(["momentum_score", "delta_composite", "rank_change"], ascending=[False, False, False]).head(10).copy()
leaders_tbl["rank"] = leaders_tbl.index + 1
st.caption(
    "Ranks themes by momentum score first, then confidence-adjusted composite improvement, then rank improvement. "
    "This is a change leaderboard built from start-to-end window deltas, not a strongest-at-end table."
)
show_leaderboard_advanced = st.checkbox("Show advanced movement fields", value=False, key="historical_show_leaderboard_advanced")
leaders_cols = ["rank", "theme", "rank_change", "delta_composite", "momentum_score"]
if show_leaderboard_advanced:
    leaders_cols.extend(["delta_avg_1m", "delta_breadth"])
leaders_display = disambiguate_theme_labels(leaders_tbl)
if "theme_display" in leaders_display.columns:
    leaders_display["theme"] = leaders_display["theme_display"]
leaders_display = leaders_display[leaders_cols]
leaders_event = render_dataframe(
    "historical_momentum_leaderboard",
    leaders_display,
    width="stretch",
    column_config=_config_for_columns(leaders_tbl.columns.tolist()),
    on_select="rerun",
    selection_mode="single-row",
    key="historical_momentum_leaderboard",
)
leader_idx = extract_selected_row(leaders_event)
if leader_idx is not None and 0 <= leader_idx < len(leaders_tbl):
    picked_row = leaders_tbl.iloc[leader_idx]
    picked_theme = _display_theme_name_from_row(picked_row, theme_label_by_id, theme_ids_by_name)
    if st.button(f"Open `{picked_theme}` in Themes detail", key="open_historical_momentum_theme"):
        _open_theme_in_themes(picked_row.get("theme_id"), picked_theme, theme_label_by_id, theme_ids_by_name, "historical_table")

st.subheader("Top Momentum Themes")
st.caption(
    "These are the strongest themes by the page's deterministic momentum model for the selected window. "
    "Use this as the clearest model-based companion to the improving-themes leaderboard; unlike Window-End Leaders below, this table is sorted by `momentum_score`, not end-of-window return level."
)
top_momentum_display = disambiguate_theme_labels(momentum["top_momentum"])
if "theme_display" in top_momentum_display.columns:
    top_momentum_display["theme"] = top_momentum_display["theme_display"]
top_momentum_event = render_dataframe(
    "historical_top_momentum",
    top_momentum_display[["theme", "momentum_score", "delta_composite", "rank_change", "delta_breadth"]].head(analysis_top_n),
    width="stretch",
    column_config=_config_for_columns(["theme", "momentum_score", "delta_composite", "rank_change", "delta_breadth"]),
    on_select="rerun",
    selection_mode="single-row",
    key="historical_top_momentum_table",
)
top_momentum_idx = extract_selected_row(top_momentum_event)
if top_momentum_idx is not None and 0 <= top_momentum_idx < len(top_momentum_display.head(analysis_top_n)):
    picked_row = top_momentum_display.head(analysis_top_n).reset_index(drop=True).iloc[int(top_momentum_idx)]
    st.session_state["historical_selected_theme_id"] = _normalize_theme_identifier(picked_row.get("theme_id"))
    st.session_state["historical_selected_theme_name"] = str(picked_row.get("theme") or "")
    picked_theme = _display_theme_name_from_row(picked_row, theme_label_by_id, theme_ids_by_name)
    if st.button(f"Open top momentum theme `{picked_theme}` in Themes detail", key="open_historical_top_momentum_theme"):
        _open_theme_in_themes(picked_row.get("theme_id"), picked_theme, theme_label_by_id, theme_ids_by_name, "historical_top_momentum")

with st.expander("Advanced chart controls", expanded=False):
    fc1, fc2 = st.columns(2)
    with fc1:
        category_filter = st.selectbox("Category filter", ["all"] + sorted(history["category"].dropna().unique().tolist()))
    with fc2:
        search_filter = st.text_input("Theme search", value="")

    fc3, fc4 = st.columns(2)
    with fc3:
        smoothing = st.selectbox("Smoothing", ["none", "3 period rolling", "5 period rolling"], index=0)
    with fc4:
        chart_series_count = st.slider("Themes shown in chart", min_value=2, max_value=12, value=5)

filtered_history = history.copy()
if category_filter != "all":
    filtered_history = filtered_history[filtered_history["category"] == category_filter]
if search_filter.strip():
    filtered_history = filtered_history[filtered_history["theme"].str.contains(search_filter.strip(), case=False, na=False)]

latest = filtered_history.sort_values(["snapshot_time", "theme"]).groupby("theme_id", as_index=False).tail(1)
movement_leaders = summary.sort_values(["momentum_score", "delta_composite", "rank_change"], ascending=[False, False, False])[
    ["theme_id", "theme"]
].drop_duplicates(subset=["theme_id"])
analysis_leaders_df = movement_leaders[movement_leaders["theme_id"].isin(latest["theme_id"].tolist())].head(analysis_top_n).copy()

if analysis_leaders_df.empty:
    st.warning("No themes match current filter for this lookback window.")
    st.stop()

analysis_leader_ids = analysis_leaders_df["theme_id"].tolist()
chart_option_by_id = {
    int(theme_id): _theme_label_for_display(theme_id, analysis_leaders_df.loc[analysis_leaders_df["theme_id"] == theme_id, "theme"].iloc[0], theme_label_by_id, theme_ids_by_name)
    for theme_id in analysis_leader_ids
}
chart_options = [chart_option_by_id[int(theme_id)] for theme_id in analysis_leader_ids]
chart_id_by_option = {label: theme_id for theme_id, label in chart_option_by_id.items()}
default_chart_themes = chart_options[: min(chart_series_count, len(chart_options))]
with st.expander("Advanced theme selection", expanded=False):
    watchlist = st.multiselect("Pinned watchlist themes", options=chart_options, default=[])
    chart_themes = st.multiselect(
        "Themes to display",
        options=chart_options,
        default=sorted(set(default_chart_themes + watchlist), key=lambda x: chart_options.index(x))[:12],
    )

if not chart_themes:
    st.warning("Select at least one theme to display in chart.")
    st.stop()

selected_chart_theme_ids = [int(chart_id_by_option[label]) for label in chart_themes]
trend = filtered_history[filtered_history["theme_id"].isin(selected_chart_theme_ids)][["snapshot_time", "theme_id", "theme", metric, "rank"]].copy()
trend["theme_label"] = trend.apply(lambda row: _theme_label_for_display(row.get("theme_id"), row.get("theme"), theme_label_by_id, theme_ids_by_name), axis=1)
trend = trend.sort_values(["theme_id", "snapshot_time", "theme"])

points_per_theme = trend.groupby("theme_id")["snapshot_time"].nunique()
valid_themes = points_per_theme[points_per_theme >= 2].index.tolist()
if not valid_themes:
    st.warning("Selected themes do not have enough points in this window to plot trends.")
    st.stop()
if len(valid_themes) < len(selected_chart_theme_ids):
    dropped = [chart_option_by_id[int(theme_id)] for theme_id in selected_chart_theme_ids if theme_id not in set(valid_themes)]
    st.info(f"Skipping themes with insufficient history: {', '.join(dropped)}")
    trend = trend[trend["theme_id"].isin(valid_themes)]

if len(valid_themes) > 8:
    st.caption("Showing many lines can reduce readability; consider narrowing to roughly 5-8 themes.")

if display_mode == "rank movement":
    trend["display_value"] = trend["rank"]
    y_title = "Rank (lower is better)"
else:
    trend["display_value"] = trend[metric]
    if display_mode == "indexed (100=start)":
        start_vals = trend.groupby("theme_id")["display_value"].transform("first")
        trend["display_value"] = (trend["display_value"] / start_vals.replace(0, pd.NA)) * 100.0
        trend["display_value"] = trend["display_value"].fillna(100.0)
        y_title = f"{metric} indexed"
    else:
        y_title = metric

window = 0
if smoothing == "3 period rolling":
    window = 3
elif smoothing == "5 period rolling":
    window = 5
if window > 1:
    trend["display_value"] = trend.groupby("theme_id")["display_value"].transform(lambda s: s.rolling(window, min_periods=1).mean())

leaders_now = set(summary.sort_values("rank_end").head(3)["theme_id"].tolist()) if "theme_id" in summary.columns else set()
trend["leader_tier"] = trend["theme_id"].apply(lambda x: "current leader" if x in leaders_now else "other")

y_min = float(trend["display_value"].min())
y_max = float(trend["display_value"].max())
if pd.isna(y_min) or pd.isna(y_max):
    st.warning("Unable to determine chart scale due to missing values after filtering.")
    st.stop()

if y_min == y_max:
    pad = max(0.5, abs(y_min) * 0.05)
else:
    pad = max(0.5, (y_max - y_min) * 0.08)
scale = alt.Scale(domain=[y_min - pad, y_max + pad], reverse=(display_mode == "rank movement"))

chart = (
    alt.Chart(trend)
    .mark_line()
    .encode(
        x=alt.X("snapshot_time:T", title="Snapshot time"),
        y=alt.Y("display_value:Q", title=y_title, scale=scale),
        color=alt.Color("theme_label:N", title="Theme"),
        strokeWidth=alt.condition(alt.datum.leader_tier == "current leader", alt.value(3), alt.value(1.6)),
        tooltip=["snapshot_time:T", "theme_label:N", alt.Tooltip("display_value:Q", format=".2f"), "rank:Q"],
    )
    .properties(height=420)
)
st.subheader("Movement Chart")
if display_mode == "rank movement":
    st.caption("This chart plots cross-theme rank over time for the selected themes. Lower values are stronger.")
elif display_mode == "indexed (100=start)":
    st.caption(f"This chart rebases `{metric}` to 100 at the start of the selected window so relative movement is easier to compare.")
else:
    st.caption(f"This chart plots the raw `{metric}` snapshot values over time for the selected themes.")
st.altair_chart(chart, width="stretch")

st.caption(
    f"Analyzed top N={analysis_top_n}; displaying {trend['theme_id'].nunique()} movement-selected theme lines "
    f"from {pd.to_datetime(window_meta.get('window_start')).strftime('%Y-%m-%d') if window_meta.get('window_start') is not None else '-'} "
    f"to {pd.to_datetime(window_meta.get('window_end')).strftime('%Y-%m-%d') if window_meta.get('window_end') is not None else '-'}."
)

st.subheader("Theme Signals (Deterministic Inflection Feed)")
st.caption(
    "Deterministic event triage derived from momentum + rotation rules for the selected historical window. "
    "These are heuristic flags, not predictive signals."
)
if inflections["meta"]["insufficient"]:
    st.info(inflections["meta"]["message"])
elif inflections["signals"].empty:
    st.info("No high-confidence inflection signals for this analysis window.")
else:
    signal_cols = [
        "detected_at",
        "theme",
        "signal_label",
        "reason",
        "rank_change",
        "momentum_score",
        "delta_composite",
        "delta_avg_1m",
        "delta_breadth",
    ]
    signal_df = inflections["signals"][["theme_id", *signal_cols]].head(30).reset_index(drop=True)
    signal_display = disambiguate_theme_labels(signal_df)
    if "theme_display" in signal_display.columns:
        signal_display["theme"] = signal_display["theme_display"]
    signal_display = signal_display[signal_cols]
    signal_event = render_dataframe(
        "historical_signal_table",
        signal_display,
        width="stretch",
        hide_index=True,
        column_config=_config_for_columns(signal_cols),
        on_select="rerun",
        selection_mode="single-row",
        key="historical_signal_table",
    )
    signal_idx = extract_selected_row(signal_event)
    if signal_idx is not None and 0 <= signal_idx < len(signal_df):
        picked_row = signal_df.iloc[signal_idx]
        picked_theme = _display_theme_name_from_row(picked_row, theme_label_by_id, theme_ids_by_name)
        if st.button(f"Open signal theme `{picked_theme}` in Themes detail", key="open_historical_signal_theme"):
            _open_theme_in_themes(picked_row.get("theme_id"), picked_theme, theme_label_by_id, theme_ids_by_name, "historical_signal")
    st.caption(f"Showing top {min(30, len(inflections['signals']))} signals by priority and momentum.")

st.subheader("Rotation Signals")
st.caption("Leadership transition is kept prominent here; overlap-heavy secondary diagnostics have been moved lower.")
rotation_meta = rotation["rotation_intensity"]
rt1, rt2, rt3 = st.columns(3)
rt1.metric("Entered top N", int(rotation_meta.get("entered_top_n") or 0))
rt2.metric("Exited top N", int(rotation_meta.get("exited_top_n") or 0))
rt3.metric("Rotation intensity", f"{float(rotation_meta.get('rotation_intensity_score') or 0):.2f}%")
st.caption(
    f"Entered: {_format_theme_list(rotation['rotating_into'])} | Exited: {_format_theme_list(rotation['rotating_out'])}. "
    "Duplicate names get a minimal suffix only when needed so turnover stays visually attributable."
)
r1, r2 = st.columns(2)
with r1:
    _render_explained_table(
        "Rotating Into Leadership",
        "Themes that moved into the analyzed top-N set during the selected window.",
        rotation["rotating_into"],
        ["theme", "rank_start", "rank_end", "rank_change", "delta_composite", "momentum_score"],
    )
with r2:
    _render_explained_table(
        "Rotating Out Of Leadership",
        "Themes that fell out of the analyzed top-N set during the selected window.",
        rotation["rotating_out"],
        ["theme", "rank_start", "rank_end", "rank_change", "delta_composite", "momentum_score"],
    )

st.subheader("Cross-theme Detail Table")
st.caption("Full start/end comparison across themes for the selected window. Use this table to audit every major movement component.")
detail_cols = [
    "theme",
    "composite_score_start",
    "composite_score_end",
    "delta_composite",
    "rank_start",
    "rank_end",
    "rank_change",
    "delta_avg_1w",
    "delta_avg_1m",
    "delta_avg_3m",
    "delta_breadth",
    "delta_ticker_count",
    "momentum_score",
]
detail_df = summary[detail_cols].reset_index(drop=True)
detail_source = summary.reset_index(drop=True)
detail_display = disambiguate_theme_labels(detail_df)
if "theme_display" in detail_display.columns:
    detail_display["theme"] = detail_display["theme_display"]
detail_event = render_dataframe(
    "historical_detail_table",
    detail_display,
    width="stretch",
    column_config=_config_for_columns(detail_cols),
    on_select="rerun",
    selection_mode="single-row",
    key="historical_detail_table",
)
detail_idx = extract_selected_row(detail_event)
if detail_idx is not None and 0 <= detail_idx < len(detail_df):
    picked_row = detail_source.iloc[detail_idx]
    picked_theme = _display_theme_name_from_row(picked_row, theme_label_by_id, theme_ids_by_name)
    if st.button(f"Open detail theme `{picked_theme}` in Themes detail", key="open_historical_detail_theme"):
        _open_theme_in_themes(picked_row.get("theme_id"), picked_theme, theme_label_by_id, theme_ids_by_name, "historical_table")

with st.expander("Advanced historical diagnostics", expanded=False):
    st.caption(
        "Secondary and overlap-prone diagnostics live here so the main page stays focused on movement audit, rotation, and provenance-aware drilldown."
    )
    st.write("**Historical Snapshot Reference**")
    st.caption(
        "These window-end leader tables are retained as secondary historical reference only. "
        "They are not intended to compete with the Themes page current leadership surfaces."
    )
    ov1, ov2 = st.columns(2)
    with ov1:
        leaders_1w, msg_1w = _build_overview_leaders(overview_1w, "avg_1w")
        _render_overview_panel("Window-End Leaders - 1W", leaders_1w, "avg_1w", msg_1w, "ov_1w")
    with ov2:
        leaders_1m, msg_1m = _build_overview_leaders(overview_1m, "avg_1m")
        _render_overview_panel("Window-End Leaders - 1M", leaders_1m, "avg_1m", msg_1m, "ov_1m")
    leaders_3m, msg_3m = _build_overview_leaders(overview_3m, "avg_3m")
    _render_overview_panel("Window-End Leaders - 3M", leaders_3m, "avg_3m", msg_3m, "ov_3m")

    st.divider()
    ad1, ad2 = st.columns(2)
    with ad1:
        _render_explained_table(
            "Emerging Themes",
            "Themes with rapid rank improvement plus improving momentum and breadth.",
            rotation["emerging"],
            ["theme", "rank_change", "delta_composite", "delta_avg_1m", "delta_breadth", "momentum_score"],
        )
        if not rotation["emerging"].empty:
            reasons = rotation["emerging"].head(5).copy()
            reasons["trigger_reason"] = reasons.apply(_signal_reason_text, axis=1)
            reasons = disambiguate_theme_labels(reasons)
            if "theme_display" in reasons.columns:
                reasons["theme"] = reasons["theme_display"]
            with st.expander("Why these themes are marked Emerging"):
                render_dataframe("historical_emerging_reasons", reasons[["theme", "trigger_reason"]], width="stretch")
        _render_explained_table(
            "Largest Rank Improvers",
            "Themes with the largest positive rank change over the selected lookback window.",
            momentum["biggest_risers"],
            ["theme", "rank_change", "delta_composite", "momentum_score"],
            limit=analysis_top_n,
        )
        _render_explained_table(
            "Breadth Improvers",
            "Themes where a larger share of constituent tickers is contributing positively.",
            momentum["breadth_improvers"],
            ["theme", "delta_breadth", "delta_composite", "momentum_score"],
            limit=analysis_top_n,
        )
    with ad2:
        _render_explained_table(
            "Acceleration In Leadership",
            "Themes already in leadership that are still gaining rank and momentum.",
            rotation["acceleration"],
            ["theme", "rank_end", "rank_change", "delta_composite", "momentum_score"],
        )
        _render_explained_table(
            "Largest Rank Decliners",
            "Themes with the largest negative rank change over the selected lookback window.",
            momentum["biggest_fallers"],
            ["theme", "rank_change", "delta_composite", "momentum_score"],
            limit=analysis_top_n,
        )
        _render_explained_table(
            "Leadership Deterioration",
            "Current leaders that are losing momentum and slipping in rank.",
            rotation["deterioration"],
            ["theme", "rank_end", "rank_change", "delta_composite", "momentum_score"],
        )
        if not momentum["weakening_themes"].empty:
            weak_reasons = momentum["weakening_themes"].head(5).copy()
            weak_reasons["trigger_reason"] = weak_reasons.apply(_signal_reason_text, axis=1)
            weak_reasons = disambiguate_theme_labels(weak_reasons)
            if "theme_display" in weak_reasons.columns:
                weak_reasons["theme"] = weak_reasons["theme_display"]
            with st.expander("Why these themes are marked Weakening"):
                render_dataframe("historical_weakening_reasons", weak_reasons[["theme", "trigger_reason"]], width="stretch")

show_perf_summary()

st.subheader("Single Theme Historical Snapshot Detail")
if themes.empty:
    st.info("No themes found.")
else:
    options = {label: theme_id for theme_id, label in theme_label_by_id.items()}
    selected_theme_id_default = _resolve_theme_id(
        st.session_state.get("historical_selected_theme_id"),
        st.session_state.get("historical_selected_theme_name"),
        theme_ids_by_name,
    )
    labels = list(options.keys())
    default_index = 0
    if selected_theme_id_default is not None:
        selected_label_default = theme_label_by_id.get(int(selected_theme_id_default))
        if selected_label_default in options:
            default_index = labels.index(selected_label_default)
    sel = st.selectbox("Theme", labels, index=default_index)
    st.caption(
        "Historical table basis: resolved historical snapshot window above. "
        "Detail basis below: historical theme snapshot rows for the selected theme, not a current/live constituent member table."
    )
    st.caption(
        "If you want current/live constituent rows for this theme, use the Themes page. "
        "This section is for historical theme-level snapshot behavior across time."
    )
    st.caption(
        "Detail history precedence for same-date rows is `captured > ticker_history_derived > reconstructed`. "
        "The movement/debug workflow above can prefer `ticker_history_derived > captured > reconstructed` for recent boundaries, so recent dates can differ without being a bug."
    )
    with get_conn() as conn:
        single = theme_snapshot_history(conn, options[sel], limit=250, include_recent_ticker_history=True)
        boundary_debug = historical_theme_boundary_debug(conn, options[sel], int(lookback_days))
    if single.empty:
        st.info("No history for selected theme.")
    else:
        single = single.sort_values("snapshot_time")
        single_points = int(single["snapshot_time"].nunique())
        d1, d2 = st.columns(2)
        d1.metric("Detail basis", "Historical snapshots")
        d2.metric("Constituent basis", "Not current/live members")
        if single_points < 2:
            st.caption(
                f"Selected theme currently has {single_points} snapshot point(s). "
                "At least 2 are needed for meaningful before/after comparison."
            )
        st.caption(
            "These rows summarize the theme at each historical snapshot boundary. "
            "They should not be compared directly to a current/live member tape as if both describe the same time basis."
        )
        st.line_chart(single.set_index("snapshot_time")[["composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct"]])
        st.dataframe(single, width="stretch")
        with st.expander("Debug: Historical Source Lineage", expanded=False):
            st.caption(
                "Use this to verify which layer actually drove the selected theme's resolved movement boundary rows for the current lookback window."
            )
            st.caption(
                f"Resolved boundary window: `{pd.to_datetime(boundary_debug.get('resolved_window_start')).strftime('%Y-%m-%d') if boundary_debug.get('resolved_window_start') is not None else '-'}` "
                f"to `{pd.to_datetime(boundary_debug.get('resolved_window_end')).strftime('%Y-%m-%d') if boundary_debug.get('resolved_window_end') is not None else '-'}`"
            )
            st.caption(
                f"Movement boundary precedence here is `ticker_history_derived > captured > reconstructed`; requested window=`{int(lookback_days)}`d, "
                f"effective window=`{int(window_meta.get('effective_window_days') or 0)}`d, collapsed=`{'yes' if bool(window_meta.get('collapsed_to_available_history')) else 'no'}`."
            )
            boundary_summary = boundary_debug.get("boundary_summary", pd.DataFrame())
            if boundary_summary.empty:
                st.info("No boundary lineage rows are available for this selected theme/window.")
            else:
                st.dataframe(boundary_summary, width="stretch", hide_index=True)
                winners = boundary_summary["winner_provenance_class"].dropna().astype(str).tolist()
                if winners:
                    st.caption(f"Displayed boundary driver(s): `{', '.join(winners)}`")
            candidate_rows = boundary_debug.get("candidate_rows", pd.DataFrame())
            if not candidate_rows.empty:
                st.caption(
                    "Candidate rows below include same-date source layers that either won precedence (`selected=True`) or were overridden by a higher-precedence row."
                )
                st.dataframe(candidate_rows, width="stretch", hide_index=True)
        selected_theme_name = str(themes.loc[themes["id"] == options[sel], "name"].iloc[0])
        if st.button(f"Open current/live constituent view for `{selected_theme_name}`", key="open_historical_theme_current_view"):
            _open_theme_in_themes(options[sel], selected_theme_name, theme_label_by_id, theme_ids_by_name, "historical_detail")

with st.expander("Momentum score formula (deterministic)"):
    st.code(
        """
momentum_score =
    0.45 * delta_composite
  + 0.25 * delta_avg_1m
  + 0.20 * delta_breadth
  + 0.10 * rank_change

rank_change = start_rank - end_rank  (positive means rank improved)
rotation_intensity_score = ((entered_top_n + exited_top_n) / top_n) * 100
        """.strip()
    )

with st.expander("Metric Guide"):
    st.markdown(
        """
- **Momentum Score**: Composite metric combining performance changes, breadth change, and rank movement.
- **Composite Score**: Base weighted return score (`0.25*avg_1w + 0.50*avg_1m + 0.25*avg_3m`) multiplied by a small-theme confidence factor `min(1, sqrt(ticker_count / 8))`.
- **Breadth (positive_1m_breadth_pct)**: Percent of theme constituents with positive 1M contribution; higher means participation is broader.
- **Rank / Rank Change**: Rank is cross-theme standing (1 is strongest). Rank change is start rank minus end rank.
- **Delta Composite**: Change in composite score between start and end snapshots; positive implies improving momentum.
- **Delta Breadth**: Change in participation breadth; positive implies more constituents are supporting the move.
- **Delta Avg 1W / 1M / 3M**: Change in average return contribution over the selected window.
        """
    )
