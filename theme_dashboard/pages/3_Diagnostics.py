from datetime import datetime, timezone

import streamlit as st

from src.config import DEFAULT_PROVIDER, STALE_DATA_HOURS
from src.database import get_conn, init_db
from src.queries import last_refresh_run, refresh_history, row_counts
from src.theme_service import seed_if_needed

st.set_page_config(page_title="Diagnostics", layout="wide")
st.title("Diagnostics")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    last_run = last_refresh_run(conn)
    history = refresh_history(conn, limit=30)
    counts = row_counts(conn)

st.write(f"Current provider setting: `{DEFAULT_PROVIDER}`")

if last_run.empty:
    st.warning("No refresh runs found.")
else:
    run = last_run.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Last run id", int(run["run_id"]))
    c2.metric("Status", str(run["status"]))
    c3.metric("Success", int(run["success_count"]))
    c4.metric("Failures", int(run["failure_count"]))

    if int(run["failure_count"]) > 0 or str(run["status"]) == "failed":
        st.error("Latest refresh has failures. Review failed tickers below.")

    finished_at = run["finished_at"]
    if finished_at is not None:
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - finished_at).total_seconds() / 3600
        if age_hours > STALE_DATA_HOURS:
            st.warning(f"Data appears stale ({age_hours:.1f} hours since last refresh).")

    st.subheader("Last refresh run details")
    st.dataframe(last_run, width="stretch")

st.subheader("Failures for most recent run")
if last_run.empty:
    st.info("No runs yet.")
else:
    with get_conn() as conn:
        recent_failures = conn.execute(
            """
            SELECT ticker, error_message, created_at
            FROM refresh_failures
            WHERE run_id = ?
            ORDER BY created_at DESC
            """,
            [int(last_run.iloc[0]["run_id"])],
        ).df()
    if recent_failures.empty:
        st.success("No failures in latest run.")
    else:
        st.dataframe(recent_failures, width="stretch")

st.subheader("Refresh history")
st.dataframe(history, width="stretch")

st.subheader("Table row counts")
st.dataframe(counts, width="stretch")
