import streamlit as st

from src.database import get_conn, init_db
from src.queries import theme_history_window, theme_snapshot_history, top_n_membership_changes, top_theme_movers
from src.theme_service import list_themes, seed_if_needed

st.set_page_config(page_title="Historical Performance", layout="wide")
st.title("Historical Theme Performance")
st.caption("Inspect single-theme history and cross-theme leadership rotation over time.")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

window_label = st.selectbox("Lookback window", ["1 week", "1 month", "3 months", "Custom"], index=1)
lookback_days = {"1 week": 7, "1 month": 30, "3 months": 90}.get(window_label, 30)
if window_label == "Custom":
    lookback_days = st.number_input("Custom lookback days", min_value=3, max_value=365, value=45)

top_n = st.slider("Top N", min_value=5, max_value=50, value=20, step=5)
metric = st.selectbox("Metric", ["composite_score", "avg_1w", "avg_1m", "avg_3m", "positive_1m_breadth_pct", "ticker_count"], index=0)

with get_conn() as conn:
    history = theme_history_window(conn, int(lookback_days))
    movers = top_theme_movers(conn, int(lookback_days), top_n=top_n)
    entered, dropped = top_n_membership_changes(conn, int(lookback_days), top_n=top_n)

if history.empty:
    st.info("No snapshots available for the selected window. Run refreshes first.")
    st.stop()

st.subheader("Top-N Metric Trend")
latest = history.sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
leaders = latest.sort_values(metric, ascending=False).head(top_n)["theme"].tolist()
trend = history[history["theme"].isin(leaders)][["snapshot_time", "theme", metric]].copy()
pivot = trend.pivot_table(index="snapshot_time", columns="theme", values=metric)
st.line_chart(pivot)

c1, c2 = st.columns(2)
with c1:
    st.write(f"**Entered top {top_n}:**")
    st.write(", ".join(entered) if entered else "None")
with c2:
    st.write(f"**Dropped from top {top_n}:**")
    st.write(", ".join(dropped) if dropped else "None")

st.subheader("Biggest Movers (window)")
if movers.empty:
    st.info("Not enough data for mover analysis in this window.")
else:
    st.dataframe(movers.sort_values("delta_composite", ascending=False), width="stretch")

st.subheader("Single Theme History")
if themes.empty:
    st.info("No themes found.")
else:
    options = {f"{r['name']} ({r['category']})": int(r['id']) for _, r in themes.iterrows()}
    sel = st.selectbox("Theme", list(options.keys()))
    with get_conn() as conn:
        single = theme_snapshot_history(conn, options[sel], limit=200)
    if single.empty:
        st.info("No history for selected theme.")
    else:
        single = single.sort_values("snapshot_time")
        st.line_chart(single.set_index("snapshot_time")[["composite_score", "avg_1m", "positive_1m_breadth_pct"]])
        st.dataframe(single, width="stretch")
