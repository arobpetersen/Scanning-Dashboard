from datetime import datetime, timezone

import streamlit as st

from src.config import (
    DEFAULT_PROVIDER,
    MASSIVE_API_KEY_ENV,
    LIVE_HISTORICAL_SOURCE,
    LIVE_QUOTE_PROFILE_SOURCE,
    REFRESH_STALE_TIMEOUT_MINUTES,
    STALE_DATA_HOURS,
    massive_api_key,
)
from src.database import get_conn, init_db
from src.failure_classification import categorize_failure_message
from src.fetch_data import mark_stale_running_runs
from src.provider_live import LiveProvider
from src.queries import last_refresh_run, refresh_history, row_counts, snapshot_counts
from src.suggestions_service import suggestion_status_counts
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
    sugg_counts = suggestion_status_counts(conn)

st.write(f"Default provider setting: `{DEFAULT_PROVIDER}`")
key_present = bool(massive_api_key())
st.write(f"Massive live configured: `{key_present}` (env: `{MASSIVE_API_KEY_ENV}`)")
live_provider = LiveProvider()
st.write(f"Live quote/profile source: `{LIVE_QUOTE_PROFILE_SOURCE}`")
st.write(f"Live historical price source: `{LIVE_HISTORICAL_SOURCE}`")
st.write(f"Historical source available: `{live_provider.historical_source_available}`")
st.write(f"Stale running timeout: `{REFRESH_STALE_TIMEOUT_MINUTES}` minutes")
if stale_marked:
    st.warning(f"Marked {stale_marked} stale running run(s) as failed during this page load.")

sc1, sc2, sc3 = st.columns(3)
sc1.metric("Ticker snapshot rows", int(snaps.iloc[0]["ticker_snapshot_rows"]))
sc2.metric("Theme snapshot rows", int(snaps.iloc[0]["theme_snapshot_rows"]))
sc3.metric("Runs with theme snapshots", int(snaps.iloc[0]["runs_with_theme_snapshots"]))

sc4, sc5, sc6, sc7 = st.columns(4)
sc4.metric("Suggestions pending", int(sugg_counts[sugg_counts["status"] == "pending"]["cnt"].sum()) if not sugg_counts.empty else 0)
sc5.metric("Suggestions approved", int(sugg_counts[sugg_counts["status"] == "approved"]["cnt"].sum()) if not sugg_counts.empty else 0)
sc6.metric("Suggestions rejected", int(sugg_counts[sugg_counts["status"] == "rejected"]["cnt"].sum()) if not sugg_counts.empty else 0)
sc7.metric("Suggestions applied", int(sugg_counts[sugg_counts["status"] == "applied"]["cnt"].sum()) if not sugg_counts.empty else 0)

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

    scope_type = run.get("scope_type") if "scope_type" in last_run.columns else None
    scope_theme_name = run.get("scope_theme_name") if "scope_theme_name" in last_run.columns else None
    st.write(f"**Last run scope:** `{scope_type or 'n/a'}`")
    st.write(f"**Last run selected theme:** `{scope_theme_name or 'n/a'}`")
    st.write(f"**Last run attempted tickers:** `{int(run['ticker_count'])}`")

    if int(run["failure_count"]) > 0 or str(run["status"]) in {"failed", "partial", "blocked"}:
        st.error("Latest refresh has issues. Review run details and failed tickers below.")

    finished_at = run["finished_at"]
    if finished_at is not None:
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - finished_at).total_seconds() / 3600
        if age_hours > STALE_DATA_HOURS:
            st.warning(f"Data appears stale ({age_hours:.1f} hours since last refresh).")

    with get_conn() as conn:
        tickers_used = conn.execute(
            "SELECT ticker FROM refresh_run_tickers WHERE run_id = ? ORDER BY ticker LIMIT 50",
            [int(run["run_id"])],
        ).df()
    with st.expander("Last run ticker sample (up to 50)"):
        if tickers_used.empty:
            st.info("No ticker universe recorded for this run.")
        else:
            st.dataframe(tickers_used, width="stretch")

    st.subheader("Last refresh run details")
    st.dataframe(last_run, width="stretch")

st.subheader("Failures for most recent run")
if last_run.empty:
    st.info("No runs yet.")
else:
    run_id = int(last_run.iloc[0]["run_id"])
    with get_conn() as conn:
        recent_failures = conn.execute(
            """
            SELECT ticker, error_message, created_at
            FROM refresh_failures
            WHERE run_id = ?
            ORDER BY created_at DESC
            LIMIT 100
            """,
            [run_id],
        ).df()

        live_failure_sample = conn.execute(
            """
            SELECT r.run_id, f.ticker, f.error_message, f.created_at
            FROM refresh_failures f
            JOIN refresh_runs r ON r.run_id = f.run_id
            WHERE r.provider = 'live'
            ORDER BY f.created_at DESC
            LIMIT 50
            """
        ).df()


    top_error_reasons = (
        recent_failures.assign(error_category=recent_failures["error_message"].apply(categorize_failure_message))
        .groupby("error_category", as_index=False)
        .size()
        .rename(columns={"size": "cnt"})
        .sort_values("cnt", ascending=False)
    ) if not recent_failures.empty else recent_failures

    provider_level_cats = {"provider_limit", "provider_auth", "provider_outage"}
    provider_issue_count = (
        int(top_error_reasons[top_error_reasons["error_category"].isin(provider_level_cats)]["cnt"].sum())
        if not top_error_reasons.empty
        else 0
    )
    if recent_failures.empty:
        st.success("No failures in latest run.")
    else:
        if not top_error_reasons.empty:
            st.write("**Most common failure categories (latest run)**")
            st.dataframe(top_error_reasons, width="stretch")
            if provider_issue_count > 0:
                st.warning(f"Provider-level failures in latest run: {provider_issue_count}. These should not drive ticker-level review suggestions.")
        st.dataframe(recent_failures, width="stretch")

    st.subheader("Recent live failure samples")
    if live_failure_sample.empty:
        st.info("No live failures recorded yet.")
    else:
        st.dataframe(live_failure_sample, width="stretch")

st.subheader("Refresh history")
st.dataframe(history, width="stretch")

st.subheader("Table row counts")
st.dataframe(counts, width="stretch")
