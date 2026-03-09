from datetime import datetime, timezone

import streamlit as st

from src.config import (
    DEFAULT_PROVIDER,
    FINNHUB_API_KEY_ENV,
    REFRESH_STALE_TIMEOUT_MINUTES,
    STALE_DATA_HOURS,
    finnhub_api_key,
)
from src.database import get_conn, init_db
from src.fetch_data import mark_stale_running_runs
from src.queries import last_refresh_run, refresh_history, row_counts, snapshot_counts
from src.theme_service import seed_if_needed

st.set_page_config(page_title="Diagnostics", layout="wide")
st.title("Diagnostics")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    stale_marked = mark_stale_running_runs(conn)
    last_run = last_refresh_run(conn)
    history = refresh_history(conn, limit=30)
    counts = row_counts(conn)
    snaps = snapshot_counts(conn)

st.write(f"Default provider setting: `{DEFAULT_PROVIDER}`")
key_present = bool(finnhub_api_key())
st.write(f"Finnhub live configured: `{key_present}` (env: `{FINNHUB_API_KEY_ENV}`)")
st.write(f"Stale running timeout: `{REFRESH_STALE_TIMEOUT_MINUTES}` minutes")
if stale_marked:
    st.warning(f"Marked {stale_marked} stale running run(s) as failed during this page load.")

sc1, sc2, sc3 = st.columns(3)
sc1.metric("Ticker snapshot rows", int(snaps.iloc[0]["ticker_snapshot_rows"]))
sc2.metric("Theme snapshot rows", int(snaps.iloc[0]["theme_snapshot_rows"]))
sc3.metric("Runs with theme snapshots", int(snaps.iloc[0]["runs_with_theme_snapshots"]))

if not history.empty:
    status_counts = history["status"].value_counts().to_dict()
    st.caption(
        "Run status counts: "
        + ", ".join(
            f"{status}={count}" for status, count in sorted(status_counts.items(), key=lambda x: x[0])
        )
    )

running = history[history["status"] == "running"] if not history.empty else history
if not running.empty:
    st.error("A refresh is currently running. New refresh attempts are blocked until it finishes or goes stale.")

if last_run.empty:
    st.warning("No refresh runs found.")
else:
    run = last_run.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Last run id", int(run["run_id"]))
    c2.metric("Provider used", str(run["provider"]))
    c3.metric("Status", str(run["status"]))
    c4.metric("Success", int(run["success_count"]))
    c5.metric("Failures", int(run["failure_count"]))

    if int(run["failure_count"]) > 0 or str(run["status"]) in {"failed", "partial", "blocked"}:
        st.error("Latest refresh has issues. Review run details and failed tickers below.")

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
