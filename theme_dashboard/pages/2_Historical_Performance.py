import streamlit as st

from src.database import get_conn, init_db
from src.momentum_engine import compute_theme_momentum
from src.queries import theme_snapshot_history
from src.theme_service import list_themes, seed_if_needed

st.set_page_config(page_title="Historical Performance", layout="wide")
st.title("Historical Performance & Theme Momentum")
st.caption("Track leadership, rotation, strengthening, and weakening themes over configurable windows.")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

window_label = st.selectbox("Lookback window", ["1 week", "1 month", "3 months", "Custom"], index=1)
lookback_days = {"1 week": 7, "1 month": 30, "3 months": 90}.get(window_label, 30)
if window_label == "Custom":
    lookback_days = st.number_input("Custom lookback days", min_value=3, max_value=365, value=45)

top_n = st.slider("Top N focus", min_value=5, max_value=50, value=20, step=5)
metric = st.selectbox(
    "Comparison metric",
    ["composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count"],
    index=0,
)

with get_conn() as conn:
    momentum = compute_theme_momentum(conn, int(lookback_days), top_n=top_n)

history = momentum["history"]
if history.empty:
    st.info("No snapshots available in selected window. Run refreshes first.")
    st.stop()

summary = momentum["window_summary"]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Themes in window", int(summary.shape[0]))
m2.metric("New leaders", len(momentum["new_leaders"]))
m3.metric("Dropped leaders", len(momentum["dropped_leaders"]))
m4.metric("Avg momentum score", f"{summary['momentum_score'].mean():.2f}")

st.subheader("Top-N Theme Movement")
latest = history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
leaders = latest.sort_values(metric, ascending=False).head(top_n)["theme"].tolist()
trend = history[history["theme"].isin(leaders)][["snapshot_time", "theme", metric]].copy()
pivot = trend.pivot_table(index="snapshot_time", columns="theme", values=metric)
st.line_chart(pivot)

c1, c2 = st.columns(2)
with c1:
    st.write(f"**New leaders in top {top_n}**")
    st.write(", ".join(momentum["new_leaders"]) if momentum["new_leaders"] else "None")
with c2:
    st.write(f"**Dropped from top {top_n}**")
    st.write(", ".join(momentum["dropped_leaders"]) if momentum["dropped_leaders"] else "None")

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
        """.strip()
    )
