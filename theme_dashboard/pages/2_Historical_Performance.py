import altair as alt
import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.inflection_engine import compute_theme_inflections
from src.leaderboard_utils import build_window_leaderboard
from src.momentum_engine import compute_theme_momentum
from src.queries import theme_snapshot_history
from src.rotation_engine import compute_theme_rotation
from src.streamlit_utils import extract_selected_row
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
    shaped = df.reindex(columns=columns)
    show_df = shaped if limit is None else shaped.head(limit)
    st.dataframe(show_df, width="stretch", column_config=_config_for_columns(columns))


def _signal_reason_text(row: pd.Series) -> str:
    return (
        f"rank_change {row.get('rank_change', 0):+.0f}, "
        f"momentum_score {row.get('momentum_score', 0):+.2f}, "
        f"delta_composite {row.get('delta_composite', 0):+.2f}, "
        f"delta_breadth {row.get('delta_breadth', 0):+.2f}"
    )


def _open_theme_in_themes(theme_name: str, id_by_name: dict[str, int], label_by_name: dict[str, str], source: str) -> None:
    if theme_name not in id_by_name:
        st.warning(f"Unable to open `{theme_name}` in Themes because it is not present in the current theme registry.")
        return
    set_theme_selection_state(st.session_state, id_by_name[theme_name], label_by_name[theme_name], source)
    st.switch_page("pages/1_Themes.py")


def _build_overview_leaders(momentum: dict, perf_col: str, top_k: int = 10) -> tuple[pd.DataFrame, str | None]:
    return build_window_leaderboard(momentum, perf_col, top_k=top_k)


def _render_overview_panel(title: str, leaders: pd.DataFrame, perf_col: str, message: str | None, key_prefix: str):
    st.markdown(f"**{title}**")
    if message:
        st.info(message)
        return

    display = leaders.rename(columns={perf_col: "window_perf"})
    cols = ["rank", "theme", "window_perf", "momentum_score", "rank_change"]
    event = st.dataframe(
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
        picked = display.iloc[int(row_idx)]["theme"]
        st.session_state["historical_selected_theme_name"] = picked


st.set_page_config(page_title="Historical Performance", layout="wide")
st.title("Historical Performance & Theme Momentum")
st.caption("Audit historical theme movement, leadership rotation, and provenance-aware change across resolved boundary windows.")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)
theme_label_by_name = {str(r["name"]): f"{r['name']} ({r['category']})" for _, r in themes.iterrows()}
theme_id_by_name = {str(r["name"]): int(r["id"]) for _, r in themes.iterrows()}

with get_conn() as conn:
    overview_1w = compute_theme_momentum(conn, 7, top_n=10)
    overview_1m = compute_theme_momentum(conn, 30, top_n=10)
    overview_3m = compute_theme_momentum(conn, 90, top_n=10)

st.subheader("Window-End Leadership Snapshot")
st.caption(
    "Fixed cross-window historical snapshot. These panels show which themes were strongest at the end of each resolved window, "
    "not which themes are strongest in the current/live view."
)

ov1, ov2 = st.columns(2)
with ov1:
    leaders_1w, msg_1w = _build_overview_leaders(overview_1w, "avg_1w")
    _render_overview_panel("Window-End Leaders - 1W", leaders_1w, "avg_1w", msg_1w, "ov_1w")
with ov2:
    leaders_1m, msg_1m = _build_overview_leaders(overview_1m, "avg_1m")
    _render_overview_panel("Window-End Leaders - 1M", leaders_1m, "avg_1m", msg_1m, "ov_1m")

with st.expander("Advanced historical snapshot leaders", expanded=False):
    st.caption("Longer-horizon and secondary window-end leader cuts live here so the main page stays focused on movement audit.")
    leaders_3m, msg_3m = _build_overview_leaders(overview_3m, "avg_3m")
    _render_overview_panel("Window-End Leaders - 3M", leaders_3m, "avg_3m", msg_3m, "ov_3m")

st.divider()
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

with get_conn() as conn:
    momentum = compute_theme_momentum(conn, int(lookback_days), top_n=analysis_top_n)
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
with get_conn() as conn:
    inflections = compute_theme_inflections(conn, int(lookback_days), top_n=analysis_top_n)
window_meta = momentum.get("meta", {})

w1, w2, w3 = st.columns(3)
w1.metric("Window start", str(pd.to_datetime(window_meta.get("window_start")).strftime("%Y-%m-%d")) if window_meta.get("window_start") is not None else "-")
w2.metric("Window end", str(pd.to_datetime(window_meta.get("window_end")).strftime("%Y-%m-%d")) if window_meta.get("window_end") is not None else "-")
w3.metric("Boundary snapshots", int(window_meta.get("boundary_snapshot_count") or 0))
st.caption(
    f"Resolved boundary provenance: `{window_meta.get('boundary_provenance_mix') or 'unknown'}` | "
    f"overall window provenance: `{window_meta.get('provenance_mix') or 'unknown'}`"
)
st.caption("Treat the outputs below as conditional on this resolved window shape and provenance mix. Shallow or mixed windows are useful for audit, but less trustworthy for strong inference.")
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

m1, m2, m3, m4 = st.columns(4)
m1.metric("Themes in window", int(summary.shape[0]))
m2.metric("New leaders", len(momentum["new_leaders"]))
m3.metric("Dropped leaders", len(momentum["dropped_leaders"]))
m4.metric("Rotation intensity", f"{rotation['rotation_intensity']['rotation_intensity_score']:.1f}")

st.subheader("Most Improving Themes In This Window")
leaders_tbl = summary.sort_values(["momentum_score", "delta_composite", "rank_change"], ascending=[False, False, False]).head(10).copy()
leaders_tbl["rank"] = leaders_tbl.index + 1
st.caption(
    "Ranks themes by momentum score first, then confidence-adjusted composite improvement, then rank improvement. "
    "This is a change leaderboard: it highlights themes improving the most over the selected window, not simply the strongest themes right now."
)
show_leaderboard_advanced = st.checkbox("Show advanced movement fields", value=False, key="historical_show_leaderboard_advanced")
leaders_cols = ["rank", "theme", "rank_change", "delta_composite", "momentum_score"]
if show_leaderboard_advanced:
    leaders_cols.extend(["delta_avg_1m", "delta_breadth"])
leaders_tbl = leaders_tbl[leaders_cols]
leaders_event = st.dataframe(
    leaders_tbl,
    width="stretch",
    column_config=_config_for_columns(leaders_tbl.columns.tolist()),
    on_select="rerun",
    selection_mode="single-row",
    key="historical_momentum_leaderboard",
)
leader_idx = extract_selected_row(leaders_event)
if leader_idx is not None and 0 <= leader_idx < len(leaders_tbl):
    picked_theme = str(leaders_tbl.iloc[leader_idx]["theme"])
    if st.button(f"Open `{picked_theme}` in Themes detail", key="open_historical_momentum_theme"):
        _open_theme_in_themes(picked_theme, theme_id_by_name, theme_label_by_name, "historical_table")

st.subheader("Top Momentum Themes")
st.caption(
    "These are the strongest themes by the page's deterministic momentum model for the selected window. "
    "Use this as the clearest model-based companion to the improving-themes leaderboard."
)
st.dataframe(
    momentum["top_momentum"][["theme", "momentum_score", "delta_composite", "rank_change", "delta_breadth"]].head(analysis_top_n),
    width="stretch",
    column_config=_config_for_columns(["theme", "momentum_score", "delta_composite", "rank_change", "delta_breadth"]),
)

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

latest = filtered_history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
movement_leaders = summary.sort_values(["momentum_score", "delta_composite", "rank_change"], ascending=[False, False, False])["theme"].tolist()
analysis_leaders = [theme for theme in movement_leaders if theme in latest["theme"].tolist()][:analysis_top_n]

if not analysis_leaders:
    st.warning("No themes match current filter for this lookback window.")
    st.stop()

default_chart_themes = analysis_leaders[: min(chart_series_count, len(analysis_leaders))]
with st.expander("Advanced theme selection", expanded=False):
    watchlist = st.multiselect("Pinned watchlist themes", options=analysis_leaders, default=[])
    chart_themes = st.multiselect(
        "Themes to display",
        options=analysis_leaders,
        default=sorted(set(default_chart_themes + watchlist), key=lambda x: analysis_leaders.index(x))[:12],
    )

if not chart_themes:
    st.warning("Select at least one theme to display in chart.")
    st.stop()

trend = filtered_history[filtered_history["theme"].isin(chart_themes)][["snapshot_time", "theme", metric, "rank"]].copy()
trend = trend.sort_values(["theme", "snapshot_time"])

points_per_theme = trend.groupby("theme")["snapshot_time"].nunique()
valid_themes = points_per_theme[points_per_theme >= 2].index.tolist()
if not valid_themes:
    st.warning("Selected themes do not have enough points in this window to plot trends.")
    st.stop()
if len(valid_themes) < len(chart_themes):
    dropped = sorted(set(chart_themes) - set(valid_themes))
    st.info(f"Skipping themes with insufficient history: {', '.join(dropped)}")
    trend = trend[trend["theme"].isin(valid_themes)]

if len(valid_themes) > 8:
    st.caption("Showing many lines can reduce readability; consider narrowing to roughly 5-8 themes.")

if display_mode == "rank movement":
    trend["display_value"] = trend["rank"]
    y_title = "Rank (lower is better)"
else:
    trend["display_value"] = trend[metric]
    if display_mode == "indexed (100=start)":
        start_vals = trend.groupby("theme")["display_value"].transform("first")
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
    trend["display_value"] = trend.groupby("theme")["display_value"].transform(lambda s: s.rolling(window, min_periods=1).mean())

leaders_now = summary.sort_values("rank_end").head(3)["theme"].tolist()
trend["leader_tier"] = trend["theme"].apply(lambda x: "current leader" if x in leaders_now else "other")

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
        color=alt.Color("theme:N", title="Theme"),
        strokeWidth=alt.condition(alt.datum.leader_tier == "current leader", alt.value(3), alt.value(1.6)),
        tooltip=["snapshot_time:T", "theme:N", alt.Tooltip("display_value:Q", format=".2f"), "rank:Q"],
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
    f"Analyzed top N={analysis_top_n}; displaying {trend['theme'].nunique()} movement-selected theme lines "
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
    signal_df = inflections["signals"][signal_cols].head(30).reset_index(drop=True)
    signal_event = st.dataframe(
        signal_df,
        width="stretch",
        hide_index=True,
        column_config=_config_for_columns(signal_cols),
        on_select="rerun",
        selection_mode="single-row",
        key="historical_signal_table",
    )
    signal_idx = extract_selected_row(signal_event)
    if signal_idx is not None and 0 <= signal_idx < len(signal_df):
        picked_theme = str(signal_df.iloc[signal_idx]["theme"])
        if st.button(f"Open signal theme `{picked_theme}` in Themes detail", key="open_historical_signal_theme"):
            _open_theme_in_themes(picked_theme, theme_id_by_name, theme_label_by_name, "historical_signal")
    st.caption(f"Showing top {min(30, len(inflections['signals']))} signals by priority and momentum.")

st.subheader("Rotation Signals")
st.caption("Leadership transition is kept prominent here; overlap-heavy secondary diagnostics have been moved lower.")
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
detail_event = st.dataframe(
    detail_df,
    width="stretch",
    column_config=_config_for_columns(detail_cols),
    on_select="rerun",
    selection_mode="single-row",
    key="historical_detail_table",
)
detail_idx = extract_selected_row(detail_event)
if detail_idx is not None and 0 <= detail_idx < len(detail_df):
    picked_theme = str(detail_df.iloc[detail_idx]["theme"])
    if st.button(f"Open detail theme `{picked_theme}` in Themes detail", key="open_historical_detail_theme"):
        _open_theme_in_themes(picked_theme, theme_id_by_name, theme_label_by_name, "historical_table")

with st.expander("Advanced historical diagnostics", expanded=False):
    st.caption(
        "Secondary and overlap-prone diagnostics live here so the main page stays focused on movement audit, rotation, and provenance-aware drilldown."
    )
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
            with st.expander("Why these themes are marked Emerging"):
                st.dataframe(reasons[["theme", "trigger_reason"]], width="stretch")
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
            with st.expander("Why these themes are marked Weakening"):
                st.dataframe(weak_reasons[["theme", "trigger_reason"]], width="stretch")

st.subheader("Single Theme History")
if themes.empty:
    st.info("No themes found.")
else:
    options = {f"{r['name']} ({r['category']})": int(r['id']) for _, r in themes.iterrows()}
    theme_name_default = st.session_state.get("historical_selected_theme_name")
    labels = list(options.keys())
    default_index = 0
    if theme_name_default:
        for i, label in enumerate(labels):
            if label.startswith(f"{theme_name_default} ("):
                default_index = i
                break
    sel = st.selectbox("Theme", labels, index=default_index)
    with get_conn() as conn:
        single = theme_snapshot_history(conn, options[sel], limit=250, include_recent_ticker_history=True)
    if single.empty:
        st.info("No history for selected theme.")
    else:
        single = single.sort_values("snapshot_time")
        single_points = int(single["snapshot_time"].nunique())
        if single_points < 2:
            st.caption(
                f"Selected theme currently has {single_points} snapshot point(s). "
                "At least 2 are needed for meaningful before/after comparison."
            )
        st.line_chart(single.set_index("snapshot_time")[["composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct"]])
        st.dataframe(single, width="stretch")

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
