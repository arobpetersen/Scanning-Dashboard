import streamlit as st

from src.config import DEFAULT_PROVIDER
from src.database import get_conn, init_db
from src.fetch_data import run_refresh
from src.queries import last_refresh_run
from src.rankings import compute_theme_rankings
from src.theme_service import seed_if_needed

st.set_page_config(page_title="Thematic Stock Dashboard", layout="wide")
st.title("Thematic Stock Dashboard (Local v1)")

init_db()
with get_conn() as conn:
    seeded = seed_if_needed(conn)

if seeded:
    st.success("Theme registry imported from themes_seed_structured.json (one-time). DuckDB is now source of truth.")

provider_name = st.sidebar.selectbox("Data provider", ["mock", "live"], index=0 if DEFAULT_PROVIDER == "mock" else 1)

col1, col2, col3 = st.columns(3)
with col1:
    st.write(f"**Provider:** `{provider_name}`")
with col2:
    if st.button("Refresh now", type="primary"):
        with get_conn() as conn:
            run_id = run_refresh(conn, provider_name)
        st.success(f"Refresh completed. Run ID: {run_id}")
        st.rerun()

with get_conn() as conn:
    last_run = last_refresh_run(conn)
    rankings = compute_theme_rankings(conn)

with col3:
    if not last_run.empty:
        st.write(f"**Last refresh:** {last_run.iloc[0]['finished_at']}")
    else:
        st.write("**Last refresh:** never")

if rankings.empty:
    st.warning("No ranking data yet. Click 'Refresh now' to populate snapshots.")
    st.stop()

summary_cols = st.columns(4)
summary_cols[0].metric("Themes", int(rankings.shape[0]))
summary_cols[1].metric("Active themes", int((rankings["is_active"] == True).sum()))
summary_cols[2].metric("Average 1M", f"{rankings['avg_1m'].mean():.2f}%")
summary_cols[3].metric("Avg positive 1M breadth", f"{rankings['positive_1m_breadth_pct'].mean():.1f}%")

search = st.text_input("Filter themes")
if search:
    rankings = rankings[rankings["theme"].str.contains(search, case=False, na=False)]

view = rankings[
    [
        "theme",
        "category",
        "ticker_count",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "positive_1m_breadth_pct",
        "composite_score",
        "is_active",
    ]
].rename(
    columns={
        "avg_1w": "avg 1W %",
        "avg_1m": "avg 1M %",
        "avg_3m": "avg 3M %",
        "positive_1m_breadth_pct": "positive 1M breadth %",
        "composite_score": "composite score",
    }
)
st.dataframe(view, width="stretch")

with st.expander("Ranking formulas (auditable)"):
    st.code(
        """
avg_1w = mean(perf_1w)
avg_1m = mean(perf_1m)
avg_3m = mean(perf_3m)
positive_1w_breadth_pct = percent(perf_1w > 0)
positive_1m_breadth_pct = percent(perf_1m > 0)
positive_3m_breadth_pct = percent(perf_3m > 0)
composite_score = 0.25 * avg_1w + 0.50 * avg_1m + 0.25 * avg_3m
        """.strip()
    )
