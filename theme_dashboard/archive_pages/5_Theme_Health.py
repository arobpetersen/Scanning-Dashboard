import streamlit as st

from src.config import RULE_LIVE_FAILURE_WINDOW_DAYS, RULE_LOW_CONSTITUENT_THRESHOLD
from src.database import get_conn, init_db
from src.queries import theme_health_overview
from src.theme_service import seed_if_needed

st.set_page_config(page_title="Theme Health / Maintenance", layout="wide")
st.title("Theme Health / Maintenance")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)

st.caption("Operational health view for taxonomy oversight without queue flooding.")

c1, c2 = st.columns(2)
with c1:
    low_threshold = st.number_input("Low constituent threshold", min_value=1, max_value=25, value=RULE_LOW_CONSTITUENT_THRESHOLD)
with c2:
    failure_window = st.number_input("Live failure lookback (days)", min_value=1, max_value=90, value=RULE_LIVE_FAILURE_WINDOW_DAYS)

with get_conn() as conn:
    health = theme_health_overview(conn, int(low_threshold), int(failure_window))

if health.empty:
    st.info("No theme data available.")
    st.stop()

f1, f2, f3 = st.columns(3)
with f1:
    active_filter = st.selectbox("Active status", ["all", "active", "inactive"], index=0)
with f2:
    low_filter = st.selectbox("Low-count flag", ["all", "only low-count", "exclude low-count"], index=0)
with f3:
    empty_filter = st.selectbox("Empty theme flag", ["all", "only empty", "exclude empty"], index=0)

view = health.copy()
if active_filter == "active":
    view = view[view["is_active"] == True]
elif active_filter == "inactive":
    view = view[view["is_active"] == False]

if low_filter == "only low-count":
    view = view[view["low_count_flag"] == True]
elif low_filter == "exclude low-count":
    view = view[view["low_count_flag"] == False]

if empty_filter == "only empty":
    view = view[view["empty_theme_flag"] == True]
elif empty_filter == "exclude empty":
    view = view[view["empty_theme_flag"] == False]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Themes", int(view.shape[0]))
m2.metric("Needs attention", int((view["health_status"] == "needs_attention").sum()))
m3.metric("Watch", int((view["health_status"] == "watch").sum()))
m4.metric("Healthy", int((view["health_status"] == "healthy").sum()))

st.dataframe(
    view[
        [
            "theme_name",
            "category",
            "is_active",
            "constituent_count",
            "low_count_flag",
            "empty_theme_flag",
            "live_failure_count_recent",
            "latest_snapshot_time",
            "health_status",
        ]
    ],
    width="stretch",
)
