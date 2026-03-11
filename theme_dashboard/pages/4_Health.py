from datetime import datetime, timezone

import streamlit as st

from src.config import (
    DEFAULT_PROVIDER,
    LIVE_HISTORICAL_SOURCE,
    LIVE_QUOTE_PROFILE_SOURCE,
    MASSIVE_API_KEY_ENV,
    REFRESH_STALE_TIMEOUT_MINUTES,
    RULE_LIVE_FAILURE_WINDOW_DAYS,
    RULE_LOW_CONSTITUENT_THRESHOLD,
    STALE_DATA_HOURS,
    massive_api_key,
)
from src.database import get_conn, init_db
from src.failure_classification import categorize_failure_message
from src.fetch_data import mark_stale_running_runs
from src.metric_formatting import short_timestamp
from src.queries import baseline_status, last_refresh_run, refresh_history, row_counts, snapshot_counts, theme_health_overview
from src.suggestions_service import suggestion_status_counts
from src.symbol_hygiene import approve_suppression, reject_keep_active, reset_failure_history, symbol_hygiene_queue
from src.theme_service import seed_if_needed

st.set_page_config(page_title="Health", layout="wide")
st.title("Health & Operations")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    stale_marked = mark_stale_running_runs(conn)
    last_run = last_refresh_run(conn)
    history = refresh_history(conn, limit=30)
    counts = row_counts(conn)
    snaps = snapshot_counts(conn)
    baseline = baseline_status(conn)
    sugg_counts = suggestion_status_counts(conn)

ops_tab, themes_tab = st.tabs(["Operations", "Theme Health"])

with ops_tab:
    st.write(f"Default provider: `{DEFAULT_PROVIDER}`")
    st.write(f"Massive configured: `{bool(massive_api_key())}` ({MASSIVE_API_KEY_ENV})")
    st.write(f"Live sources: quote/profile=`{LIVE_QUOTE_PROFILE_SOURCE}`, historical=`{LIVE_HISTORICAL_SOURCE}`")
    st.write(f"Stale timeout: `{REFRESH_STALE_TIMEOUT_MINUTES}` minutes")
    if stale_marked:
        st.warning(f"Marked {stale_marked} stale run(s) failed on page load.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ticker snapshots", int(snaps.iloc[0]["ticker_snapshot_rows"]))
    c2.metric("Theme snapshots", int(snaps.iloc[0]["theme_snapshot_rows"]))
    c3.metric("Runs w/theme snapshots", int(snaps.iloc[0]["runs_with_theme_snapshots"]))
    c4.metric("Pending suggestions", int(sugg_counts[sugg_counts["status"] == "pending"]["cnt"].sum()) if not sugg_counts.empty else 0)

    if not baseline.empty:
        state = baseline.iloc[0]
        st.subheader("Current data state")
        st.caption(
            f"Latest refresh #{int(state['latest_run_id']) if state['latest_run_id'] is not None else 'n/a'} | "
            f"status=`{state.get('latest_run_status') or 'n/a'}` | provider=`{state.get('latest_run_provider') or 'n/a'}` | "
            f"finished_at=`{state.get('latest_run_finished_at') or 'n/a'}`"
        )
        d1, d2 = st.columns(2)
        with d1:
            st.write(f"Latest theme snapshot: `{short_timestamp(state.get('latest_theme_snapshot_time')) or 'â€”'}`")
            st.write(f"Recent theme sources: `{state.get('recent_theme_sources') or 'none'}`")
        with d2:
            st.write(f"Latest ticker snapshot: `{short_timestamp(state.get('latest_ticker_snapshot_time')) or 'â€”'}`")
            st.write(f"Recent ticker sources: `{state.get('recent_ticker_sources') or 'none'}`")

        theme_sets = int(state.get("theme_snapshot_sets") or 0)
        ticker_sets = int(state.get("ticker_snapshot_sets") or 0)
        if theme_sets <= 1 or ticker_sets <= 1:
            st.warning(
                f"History is still shallow: theme snapshot sets={theme_sets}, ticker snapshot sets={ticker_sets}. "
                "At least 2 boundary snapshots are needed for reliable comparisons."
            )

    if not last_run.empty:
        run = last_run.iloc[0]
        st.info(
            f"Last run #{int(run['run_id'])} provider={run['provider']} status={run['status']} "
            f"success={int(run['success_count'])} fail={int(run['failure_count'])} "
            f"flagged={int(run.get('flagged_symbol_count') or 0)} suppressed={int(run.get('suppressed_symbol_count') or 0)}"
        )
        if run.get("failure_category_counts"):
            st.caption(f"Failure categories: {run.get('failure_category_counts')}")
        finished_at = run["finished_at"]
        if finished_at is not None:
            if finished_at.tzinfo is None:
                finished_at = finished_at.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - finished_at).total_seconds() / 3600
            if age_hours > STALE_DATA_HOURS:
                st.warning(f"Data appears stale: {age_hours:.1f} hours since last refresh.")

    st.subheader("Recent failure categories (latest run)")
    if last_run.empty:
        st.info("No runs yet.")
    else:
        run_id = int(last_run.iloc[0]["run_id"])
        with get_conn() as conn:
            recent_failures = conn.execute(
                "SELECT ticker, error_message, failure_category, created_at FROM refresh_failures WHERE run_id=? ORDER BY created_at DESC LIMIT 200",
                [run_id],
            ).df()
        if recent_failures.empty:
            st.success("No failures in latest run.")
        else:
            if "failure_category" not in recent_failures.columns or recent_failures["failure_category"].isna().any():
                recent_failures["failure_category"] = recent_failures["error_message"].apply(categorize_failure_message)
            cats = (
                recent_failures.groupby("failure_category", as_index=False)
                .size()
                .rename(columns={"size": "cnt"})
                .sort_values("cnt", ascending=False)
            )
            st.dataframe(cats, width="stretch")
            st.dataframe(recent_failures, width="stretch")

    st.subheader("Symbol hygiene review queue")
    with get_conn() as conn:
        queue = symbol_hygiene_queue(conn, limit=250)

    if queue.empty:
        st.success("No flagged/suppressed/watch symbols currently in queue.")
    else:
        st.caption("Review context and actions are inline per symbol for fast triage.")
        for _, row in queue.iterrows():
            ticker = str(row["ticker"])
            with st.container(border=True):
                c1, c2, c3, c4, c5, c6, c7 = st.columns([1, 1.1, 0.9, 0.9, 1.2, 1.1, 1.8])
                c1.write(f"**{ticker}**")
                c2.write(f"cat: `{row.get('last_failure_category') or 'n/a'}`")
                c3.write(f"consec: `{int(row.get('consecutive_failure_count') or 0)}`")
                c4.write(f"rolling: `{int(row.get('rolling_failure_count') or 0)}`")
                c5.write(f"last success: `{row.get('last_success_at') or 'never'}`")
                c6.write(f"status: `{row.get('status')}`")
                c7.write(f"suggested: `{row.get('suggested_status') or 'none'}`")
                st.caption(str(row.get("suggested_reason") or ""))

                a1, a2, a3, a4 = st.columns(4)
                if a1.button("Approve suppression", key=f"approve_{ticker}"):
                    with get_conn() as conn:
                        approve_suppression(conn, ticker)
                    st.rerun()
                if a2.button("Reject / keep active", key=f"reject_{ticker}"):
                    with get_conn() as conn:
                        reject_keep_active(conn, ticker)
                    st.rerun()
                if a3.button("Return to watch", key=f"watch_{ticker}"):
                    with get_conn() as conn:
                        reset_failure_history(conn, ticker, to_watch=True)
                    st.rerun()
                if a4.button("Reset history", key=f"reset_{ticker}"):
                    with get_conn() as conn:
                        reset_failure_history(conn, ticker, to_watch=False)
                    st.rerun()

    st.subheader("Refresh history")
    st.dataframe(history, width="stretch")
    st.subheader("Table row counts")
    st.dataframe(counts, width="stretch")

with themes_tab:
    c1, c2 = st.columns(2)
    with c1:
        low_threshold = st.number_input("Low constituent threshold", min_value=1, max_value=25, value=RULE_LOW_CONSTITUENT_THRESHOLD)
    with c2:
        failure_window = st.number_input("Live failure lookback (days)", min_value=1, max_value=90, value=RULE_LIVE_FAILURE_WINDOW_DAYS)

    with get_conn() as conn:
        health = theme_health_overview(conn, int(low_threshold), int(failure_window))

    if health.empty:
        st.info("No theme health data.")
    else:
        f1, f2, f3 = st.columns(3)
        with f1:
            active_filter = st.selectbox("Active status", ["all", "active", "inactive"], index=0)
        with f2:
            low_filter = st.selectbox("Low-count flag", ["all", "only low-count", "exclude low-count"], index=0)
        with f3:
            empty_filter = st.selectbox("Empty flag", ["all", "only empty", "exclude empty"], index=0)

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

        health_view = view[[
            "theme_name",
            "category",
            "is_active",
            "constituent_count",
            "low_count_flag",
            "empty_theme_flag",
            "live_failure_count_recent",
            "latest_snapshot_time",
            "health_status",
        ]].copy()
        health_view["latest_snapshot_time"] = health_view["latest_snapshot_time"].apply(
            lambda v: short_timestamp(v) or "—"
        )
        st.dataframe(health_view, width="stretch")
