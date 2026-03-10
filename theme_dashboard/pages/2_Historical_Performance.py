import altair as alt
import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.momentum_engine import compute_theme_momentum
from src.queries import theme_snapshot_history
from src.rotation_engine import compute_theme_rotation
from src.theme_service import list_themes, seed_if_needed

st.set_page_config(page_title="Historical Performance", layout="wide")
st.title("Historical Performance & Theme Momentum")
st.caption("Track leadership, rotation, emerging strength, and weakening themes over configurable windows.")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

window_label = st.selectbox("Lookback window", ["1 week", "1 month", "3 months", "Custom"], index=1)
lookback_days = {"1 week": 7, "1 month": 30, "3 months": 90}.get(window_label, 30)
if window_label == "Custom":
    lookback_days = st.number_input("Custom lookback days", min_value=3, max_value=365, value=45)

analysis_top_n = st.slider("Top N analyzed", min_value=5, max_value=50, value=20, step=5)
metric = st.selectbox(
    "Comparison metric",
    ["composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count"],
    index=0,
)

with get_conn() as conn:
    momentum = compute_theme_momentum(conn, int(lookback_days), top_n=analysis_top_n)

history = momentum["history"]
if history.empty:
    st.info("No snapshots available in selected window. Run refreshes first.")
    st.stop()

snapshot_count = int(history["snapshot_time"].nunique())
min_snapshots = 2
if window_label == "1 week":
    min_snapshots = 2
elif window_label == "1 month":
    min_snapshots = 3
elif window_label == "3 months":
    min_snapshots = 4

if snapshot_count < min_snapshots:
    st.warning(
        f"Not enough historical snapshots for this lookback window (have {snapshot_count}, need at least {min_snapshots})."
    )
    st.stop()

summary = momentum["window_summary"]
rotation = compute_theme_rotation(summary, analysis_top_n, momentum["new_leaders"], momentum["dropped_leaders"])

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Themes in window", int(summary.shape[0]))
m2.metric("New leaders", len(momentum["new_leaders"]))
m3.metric("Dropped leaders", len(momentum["dropped_leaders"]))
m4.metric("Avg momentum score", f"{summary['momentum_score'].mean():.2f}")
m5.metric("Rotation intensity", f"{rotation['rotation_intensity']['rotation_intensity_score']:.1f}")

st.subheader("Theme Momentum Leaderboard")
leaders_tbl = summary.sort_values(["rank_end", "momentum_score"], ascending=[True, False]).head(10)
st.dataframe(leaders_tbl[["rank_end", "theme", "momentum_score", "delta_composite", "rank_change"]].rename(columns={"rank_end": "rank"}), width="stretch")

st.subheader("Top-N Theme Movement")
fc1, fc2, fc3 = st.columns(3)
with fc1:
    category_filter = st.selectbox("Category filter", ["all"] + sorted(history["category"].dropna().unique().tolist()))
with fc2:
    search_filter = st.text_input("Theme search", value="")
with fc3:
    display_mode = st.selectbox("Display mode", ["raw metric", "indexed (100=start)", "rank movement"], index=0)

fc4, fc5 = st.columns(2)
with fc4:
    smoothing = st.selectbox("Smoothing", ["none", "3 period rolling", "5 period rolling"], index=0)
with fc5:
    chart_series_count = st.slider("Themes shown in chart", min_value=2, max_value=12, value=5)

filtered_history = history.copy()
if category_filter != "all":
    filtered_history = filtered_history[filtered_history["category"] == category_filter]
if search_filter.strip():
    filtered_history = filtered_history[filtered_history["theme"].str.contains(search_filter.strip(), case=False, na=False)]

latest = filtered_history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
analysis_leaders = latest.sort_values(metric, ascending=False).head(analysis_top_n)["theme"].tolist()

if not analysis_leaders:
    st.warning("No themes match current filter for this lookback window.")
    st.stop()

default_chart_themes = analysis_leaders[: min(chart_series_count, len(analysis_leaders))]
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
    st.caption("Showing many lines can reduce readability; consider narrowing to ~5-8 themes.")

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
st.altair_chart(chart, width="stretch")

st.caption(f"Analyzed top N={analysis_top_n}; displaying {trend['theme'].nunique()} theme lines.")

st.subheader("Rotation Signals")
r1, r2 = st.columns(2)
with r1:
    st.write("**Rotating Into Leadership**")
    st.dataframe(rotation["rotating_into"][["theme", "rank_start", "rank_end", "rank_change", "delta_composite", "momentum_score"]].head(10), width="stretch")
    st.write("**Emerging Themes**")
    st.dataframe(rotation["emerging"][["theme", "rank_change", "delta_composite", "delta_avg_1m", "delta_breadth", "momentum_score"]].head(10), width="stretch")
    st.write("**Acceleration in Leadership**")
    st.dataframe(rotation["acceleration"][["theme", "rank_end", "rank_change", "delta_composite", "momentum_score"]].head(10), width="stretch")
with r2:
    st.write("**Rotating Out Of Leadership**")
    st.dataframe(rotation["rotating_out"][["theme", "rank_start", "rank_end", "rank_change", "delta_composite", "momentum_score"]].head(10), width="stretch")
    st.write("**Fading Themes**")
    st.dataframe(rotation["fading"][["theme", "rank_change", "delta_composite", "delta_avg_1m", "delta_breadth", "momentum_score"]].head(10), width="stretch")
    st.write("**Deterioration in Leadership**")
    st.dataframe(rotation["deterioration"][["theme", "rank_end", "rank_change", "delta_composite", "momentum_score"]].head(10), width="stretch")

st.subheader("Momentum Summary Sections")
sec1, sec2 = st.columns(2)
with sec1:
    st.write("**Top Momentum Themes**")
    st.dataframe(momentum["top_momentum"][["theme", "momentum_score", "delta_composite", "rank_change", "delta_breadth"]], width="stretch")
    st.write("**Biggest Risers**")
    st.dataframe(momentum["biggest_risers"][["theme", "rank_change", "delta_composite", "momentum_score"]], width="stretch")
    st.write("**Breadth Improvers**")
    st.dataframe(momentum["breadth_improvers"][["theme", "delta_breadth", "delta_composite", "momentum_score"]], width="stretch")
with sec2:
    st.write("**Biggest Fallers**")
    st.dataframe(momentum["biggest_fallers"][["theme", "rank_change", "delta_composite", "momentum_score"]], width="stretch")
    st.write("**Weakening Themes**")
    st.dataframe(momentum["weakening_themes"][["theme", "delta_composite", "delta_breadth", "rank_change"]], width="stretch")

c1, c2 = st.columns(2)
with c1:
    st.write(f"**New leaders in top {analysis_top_n}**")
    st.write(", ".join(momentum["new_leaders"]) if momentum["new_leaders"] else "None")
with c2:
    st.write(f"**Dropped from top {analysis_top_n}**")
    st.write(", ".join(momentum["dropped_leaders"]) if momentum["dropped_leaders"] else "None")

st.subheader("Cross-theme Detail Table")
st.dataframe(
    summary[
        [
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
    ],
    width="stretch",
)

st.subheader("Single Theme History")
if themes.empty:
    st.info("No themes found.")
else:
    options = {f"{r['name']} ({r['category']})": int(r['id']) for _, r in themes.iterrows()}
    sel = st.selectbox("Theme", list(options.keys()))
    with get_conn() as conn:
        single = theme_snapshot_history(conn, options[sel], limit=250)
    if single.empty:
        st.info("No history for selected theme.")
    else:
        single = single.sort_values("snapshot_time")
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
