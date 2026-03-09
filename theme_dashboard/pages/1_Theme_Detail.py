import streamlit as st

from src.database import get_conn, init_db
from src.queries import theme_ticker_metrics
from src.theme_service import list_themes, seed_if_needed

st.set_page_config(page_title="Theme Detail", layout="wide")
st.title("Theme Detail")

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
    df = theme_ticker_metrics(conn, theme_id)

if df.empty:
    st.warning("No tickers found for the selected theme.")
    st.stop()

if "perf_1w" not in df.columns:
    st.info("No snapshot data yet for this theme. Run a refresh on the Home page.")
    st.dataframe(df, width="stretch")
    st.stop()

s1, s2, s3, s4 = st.columns(4)
s1.metric("Ticker count", int(df.shape[0]))
s2.metric("Avg 1W", f"{df['perf_1w'].mean():.2f}%")
s3.metric("Avg 1M", f"{df['perf_1m'].mean():.2f}%")
s4.metric("Avg 3M", f"{df['perf_3m'].mean():.2f}%")

filter_ticker = st.text_input("Filter ticker")
if filter_ticker:
    df = df[df["ticker"].str.contains(filter_ticker.upper(), na=False)]

st.dataframe(df, width="stretch")
