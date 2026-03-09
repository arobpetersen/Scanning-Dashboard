import streamlit as st

from src.database import get_conn, init_db
from src.queries import theme_snapshot_history, theme_ticker_metrics
from src.theme_service import list_themes, seed_if_needed

st.set_page_config(page_title="Theme Explorer", layout="wide")
st.title("Theme Explorer")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn)

if themes.empty:
    st.info("No themes found.")
    st.stop()

options = {f"{r['name']} ({r['category']})": int(r["id"]) for _, r in themes.iterrows()}
selection = st.selectbox("Choose a theme", list(options.keys()))
theme_id = options[selection]

with get_conn() as conn:
    ticker_df = theme_ticker_metrics(conn, theme_id)
    history_df = theme_snapshot_history(conn, theme_id, limit=20)

if ticker_df.empty:
    st.warning("No tickers found for the selected theme.")
    st.stop()

if "perf_1w" not in ticker_df.columns:
    st.info("No snapshot data yet for this theme. Run a refresh on the Home page.")
    st.dataframe(ticker_df, width="stretch")
    st.stop()

s1, s2, s3, s4 = st.columns(4)
s1.metric("Ticker count", int(ticker_df.shape[0]))
s2.metric("Avg 1W", f"{ticker_df['perf_1w'].mean():.2f}%")
s3.metric("Avg 1M", f"{ticker_df['perf_1m'].mean():.2f}%")
s4.metric("Avg 3M", f"{ticker_df['perf_3m'].mean():.2f}%")

if len(history_df) >= 2:
    latest = history_df.iloc[0]
    previous = history_df.iloc[1]
    t1, t2, t3 = st.columns(3)
    t1.metric("Composite score", f"{latest['composite_score']:.2f}", f"{latest['composite_score'] - previous['composite_score']:.2f}")
    t2.metric("Avg 1M", f"{latest['avg_1m']:.2f}%", f"{latest['avg_1m'] - previous['avg_1m']:.2f}")
    t3.metric(
        "Positive 1M breadth",
        f"{latest['positive_1m_breadth_pct']:.2f}%",
        f"{latest['positive_1m_breadth_pct'] - previous['positive_1m_breadth_pct']:.2f}",
    )

filter_ticker = st.text_input("Filter ticker")
if filter_ticker:
    ticker_df = ticker_df[ticker_df["ticker"].str.contains(filter_ticker.upper(), na=False)]

st.subheader("Latest ticker metrics")
st.dataframe(ticker_df, width="stretch")

st.subheader("Recent theme snapshot history")
st.dataframe(history_df, width="stretch")

if not history_df.empty:
    chart_df = history_df[["snapshot_time", "avg_1m", "composite_score"]].copy()
    chart_df = chart_df.sort_values("snapshot_time")
    chart_df = chart_df.set_index("snapshot_time")
    st.line_chart(chart_df)
