from datetime import datetime, timedelta, timezone
import time

import pandas as pd
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
from src.database import get_conn, get_fresh_read_conn, init_db
from src.failure_classification import categorize_failure_message
from src.fetch_data import mark_refresh_run_interrupted, mark_stale_running_runs
from src.historical_backfill import (
    SUPPRESSION_REBUILD_LOOKBACK_DAYS,
    reconstruct_theme_history_range,
    rebuild_recent_reconstructed_history,
)
from src.metric_formatting import short_timestamp
from src.queries import (
    baseline_status,
    historical_reconstruction_runs,
    last_refresh_run,
    refresh_history,
    row_counts,
    snapshot_counts,
    source_audit_status,
    ticker_history_readiness,
    theme_member_hygiene_context,
)
from src.streamlit_utils import (
    clear_current_market_view_caches,
    db_cache_token,
    extract_selected_row,
    get_canonical_multiselect_values,
    load_theme_health_overview_cached,
    prepare_post_mutation_refresh,
    queue_feedback_message,
    render_dataframe,
    render_feedback_message,
    reset_perf_timings,
    show_perf_summary,
    sync_valid_multiselect_state,
    stop_for_database_error,
)
from src.theme_health_audit import (
    AUDIT_PRESETS,
    AUDIT_SORT_OPTIONS,
    apply_theme_health_audit_preset,
    enrich_theme_health_for_audit,
    sort_theme_health_audit,
    theme_health_action_eligibility,
    theme_health_audit_counts,
)
from src.suggestions_service import suggestion_status_counts
from src.symbol_hygiene import (
    OVERRIDE_ACTIONS,
    STAGED_ACTIONS,
    apply_staged_symbol_hygiene_actions,
    clear_symbol_hygiene_staged_state,
    filter_symbol_hygiene_queue,
    hygiene_decision_context,
    approve_suppression,
    reject_keep_active,
    sync_symbol_hygiene_staged_action,
    sort_symbol_hygiene_queue,
    symbol_hygiene_queue,
)
from src.theme_selection import set_theme_selection_state
from src.theme_service import get_theme_members, replace_ticker_in_theme, seed_if_needed, theme_membership_export, update_theme
from src.ticker_onboarding import (
    governed_ticker_onboarding_counts,
    list_governed_ticker_onboarding,
    run_governed_ticker_onboarding_backfill,
    run_governed_ticker_onboarding_theme_reconstruction,
)

st.set_page_config(page_title="Health", layout="wide")
st.title("Health & Operations")
reset_perf_timings("health")


def _display_placeholder(value) -> str:
    return "-" if value is None or value != value else str(value)


def _queue_label(theme_id: int, theme_name: str) -> str:
    return f"{str(theme_name)} [{int(theme_id)}]"


def _first_metric_value(df: pd.DataFrame, column: str) -> int:
    if df is None or getattr(df, "empty", True) or column not in df.columns:
        return 0
    try:
        return int(df.iloc[0].get(column) or 0)
    except (TypeError, ValueError, IndexError):
        return 0


def _add_theme_to_audit_queue(session_state, theme_id: int, theme_name: str) -> tuple[bool, str]:
    label = _queue_label(theme_id, theme_name)
    current = list(get_canonical_multiselect_values(session_state, "health_theme_audit_queue"))
    if label in current:
        return False, label
    current.append(label)
    session_state["health_theme_audit_queue"] = current
    return True, label


try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        stale_marked = mark_stale_running_runs(conn)
        last_run = last_refresh_run(conn)
        history = refresh_history(conn, limit=30)
        counts = row_counts(conn)
        snaps = snapshot_counts(conn)
        baseline = baseline_status(conn)
        source_audit = source_audit_status(conn)
        ticker_history_ready = ticker_history_readiness(conn, target_trading_days=30)
        sugg_counts = suggestion_status_counts(conn)
        governed_onboarding = list_governed_ticker_onboarding(conn, limit=100)
        governed_onboarding_counts_df = governed_ticker_onboarding_counts(conn)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()

ops_tab, themes_tab = st.tabs(["Operations", "Theme Health"])

with ops_tab:
    render_feedback_message(st.session_state, "refresh_recovery_feedback")

    st.write(f"Default provider: `{DEFAULT_PROVIDER}`")
    st.write(f"Massive configured: `{bool(massive_api_key())}` ({MASSIVE_API_KEY_ENV})")
    st.write(f"Live sources: quote/profile=`{LIVE_QUOTE_PROFILE_SOURCE}`, historical=`{LIVE_HISTORICAL_SOURCE}`")
    st.caption("Current dashboard views resolve against live-preferred data only; mock fallback is no longer used in app workflows.")
    st.write(f"Stale timeout: `{REFRESH_STALE_TIMEOUT_MINUTES}` minutes")
    if stale_marked:
        st.warning(f"Marked {stale_marked} stale run(s) failed on page load.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ticker snapshots", _first_metric_value(snaps, "ticker_snapshot_rows"))
    c2.metric("Theme snapshots", _first_metric_value(snaps, "theme_snapshot_rows"))
    c3.metric("Runs w/theme snapshots", _first_metric_value(snaps, "runs_with_theme_snapshots"))
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
            st.write(f"Latest theme snapshot: `{short_timestamp(state.get('latest_theme_snapshot_time')) or '-'}`")
            st.write(f"Recent theme sources: `{state.get('recent_theme_sources') or 'none'}`")
        with d2:
            st.write(f"Latest ticker snapshot: `{short_timestamp(state.get('latest_ticker_snapshot_time')) or '-'}`")
            st.write(f"Recent ticker sources: `{state.get('recent_ticker_sources') or 'none'}`")

        theme_sets = int(state.get("theme_snapshot_sets") or 0)
        ticker_sets = int(state.get("ticker_snapshot_sets") or 0)
        if theme_sets <= 1 or ticker_sets <= 1:
            st.warning(
                f"History is still shallow: theme snapshot sets={theme_sets}, ticker snapshot sets={ticker_sets}. "
                "At least 2 boundary snapshots are needed for reliable comparisons."
            )
    if not source_audit.empty:
        audit = source_audit.iloc[0]
        st.subheader("Source audit")
        st.caption(
            f"Preferred current sources: theme=`{audit.get('preferred_theme_source') or 'none'}` | "
            f"ticker=`{audit.get('preferred_ticker_source') or 'none'}`"
        )
        a1, a2 = st.columns(2)
        with a1:
            st.write(f"Current theme view sources: `{audit.get('latest_theme_view_sources') or 'none'}`")
            st.write(f"Recent theme history sources: `{audit.get('recent_theme_sources') or 'none'}`")
        with a2:
            st.write(f"Current ticker view sources: `{audit.get('latest_ticker_view_sources') or 'none'}`")
            st.write(f"Recent ticker history sources: `{audit.get('recent_ticker_sources') or 'none'}`")
        if bool(audit.get("active_contamination")):
            st.error("Active source contamination detected: current live-facing views are mixed.")
        elif bool(audit.get("historical_residue_only")):
            st.info("Mixed source history exists as residue, but current live-facing views are using live-preferred data.")
        else:
            st.success("Current live-facing views are source-pure under live-preferred selection.")

    if not ticker_history_ready.empty:
        readiness = ticker_history_ready.iloc[0]
        st.subheader("Ticker history readiness")
        st.caption(
            "Tracks progress toward using persisted ticker-day history as the baseline for recent historical reconstruction. "
            "This is a trading-day target and does not change current/live semantics."
        )
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Target", f"{int(readiness['target_trading_days'])} trading days")
        r2.metric("Current progress", int(readiness["available_trading_days"]))
        r3.metric("Remaining", int(readiness["remaining_trading_days"]))
        r4.metric("Status", str(readiness["status_label"]).title())
        c1, c2, c3 = st.columns(3)
        c1.metric("Governed active tickers", int(readiness["governed_active_tickers"]))
        c2.metric(
            f"Governed tickers with >={int(readiness['target_trading_days'])} rows",
            int(readiness["governed_active_tickers_ready"]),
        )
        c3.metric("Ready coverage", f"{float(readiness['governed_ready_pct']):.1f}%")
        st.caption(
            f"Source=`{readiness.get('market_data_source') or 'none'}` | "
            f"Depth range=`{int(readiness['min_ticker_depth'])}` / `{float(readiness['median_ticker_depth']):.1f}` / `{int(readiness['max_ticker_depth'])}` "
            "(min / median / max rows across governed active tickers)"
        )
        if readiness.get("earliest_trading_date") or readiness.get("latest_trading_date"):
            st.caption(
                f"Stored trading-date range: `{readiness.get('earliest_trading_date') or 'n/a'}` to "
                f"`{readiness.get('latest_trading_date') or 'n/a'}`"
            )

    st.subheader("Newly governed ticker onboarding")
    st.caption(
        "Tracks post-addition history readiness for newly governed tickers. "
        "This does not run on advisory review actions; it starts only when governed membership is actually written."
    )
    render_feedback_message(st.session_state, "governed_onboarding_feedback")
    if governed_onboarding.empty:
        st.success("No newly governed tickers are currently being tracked for onboarding.")
    else:
        onboarding_count = int(len(governed_onboarding))
        needs_backfill = int(
            len(
                governed_onboarding[
                    governed_onboarding["backfill_status"].isin(["needed", "running", "failed", "insufficient_after_attempt"])
                ]
            )
        )
        ready_count = int(len(governed_onboarding[governed_onboarding["history_readiness_status"] == "ready"]))
        downstream_needed = int(len(governed_onboarding[governed_onboarding["downstream_refresh_needed"] == True]))
        o1, o2, o3, o4 = st.columns(4)
        o1.metric("Tracked tickers", onboarding_count)
        o2.metric("History ready", ready_count)
        o3.metric("Needs backfill", needs_backfill)
        o4.metric("Downstream refresh needed", downstream_needed)
        if not governed_onboarding_counts_df.empty:
            st.caption(
                "Status mix: "
                + "; ".join(
                    f"{row['history_readiness_status']}/{row['backfill_status']}={int(row['cnt'])}"
                    for _, row in governed_onboarding_counts_df.iterrows()
                )
            )
        pending_backfill_options = (
            governed_onboarding[
                governed_onboarding["backfill_status"].isin(["needed", "failed", "insufficient_after_attempt"])
            ]["ticker"]
            .astype(str)
            .tolist()
        )
        selected_onboarding_tickers = sync_valid_multiselect_state(
            st.session_state,
            "governed_onboarding_tickers",
            pending_backfill_options,
            default=pending_backfill_options[:5],
        )
        st.multiselect(
            "Tickers for onboarding history hydration",
            options=pending_backfill_options,
            key="governed_onboarding_tickers",
            help="Fetches and persists ticker daily history only for newly governed tickers that still need stored history depth.",
        )
        if st.button(
            "Hydrate ticker history for onboarding",
            type="primary",
            disabled=not bool(get_canonical_multiselect_values(st.session_state, "governed_onboarding_tickers")),
            key="run_governed_onboarding_backfill",
        ):
            try:
                selected_onboarding_tickers = get_canonical_multiselect_values(st.session_state, "governed_onboarding_tickers")
                started = time.perf_counter()
                with get_conn() as conn:
                    result = run_governed_ticker_onboarding_backfill(conn, selected_onboarding_tickers)
                updated_rows = result.get("updated_rows") or []
                updated_summary = ", ".join(
                    f"{row['ticker']}={row['backfill_status']}" for row in updated_rows[:5]
                )
                feedback_message = (
                    f"Onboarding history hydration finished with status `{result.get('status')}` for "
                    f"{len(result.get('tickers') or [])} ticker(s). {updated_summary}"
                )
                current_snapshot_result = result.get("current_snapshot_result") or {}
                if current_snapshot_result:
                    feedback_message += (
                        " Targeted current snapshot hydration: "
                        f"status=`{current_snapshot_result.get('status') or 'unknown'}` | "
                        f"run_id=`{current_snapshot_result.get('run_id') or 'n/a'}`."
                    )
                prepare_post_mutation_refresh(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level="success",
                    message=feedback_message + f" Completed in {time.perf_counter() - started:.1f}s.",
                    clear_market=True,
                    clear_scanner_summary=True,
                )
                st.rerun()
            except Exception as exc:
                queue_feedback_message(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level="error",
                    message=f"Onboarding history hydration failed: {exc}",
                )
                st.rerun()
        downstream_options = (
            governed_onboarding[governed_onboarding["downstream_refresh_needed"] == True]["ticker"]
            .astype(str)
            .tolist()
        )
        selected_reconstruction_tickers = sync_valid_multiselect_state(
            st.session_state,
            "governed_onboarding_reconstruction_tickers",
            downstream_options,
            default=downstream_options[:5],
        )
        st.multiselect(
            "Tickers for affected-theme reconstruction",
            options=downstream_options,
            key="governed_onboarding_reconstruction_tickers",
            help="Rebuilds reconstructed theme history for themes affected by these newly governed tickers after ticker history is ready.",
        )
        if st.button(
            "Run affected-theme reconstruction",
            disabled=not bool(get_canonical_multiselect_values(st.session_state, "governed_onboarding_reconstruction_tickers")),
            key="run_governed_onboarding_theme_reconstruction",
        ):
            try:
                selected_reconstruction_tickers = get_canonical_multiselect_values(
                    st.session_state,
                    "governed_onboarding_reconstruction_tickers",
                )
                started = time.perf_counter()
                with get_conn() as conn:
                    result = run_governed_ticker_onboarding_theme_reconstruction(conn, selected_reconstruction_tickers)
                reconstruction_result = result.get("reconstruction_result") or {}
                snapshot_rows_written = int(reconstruction_result.get("snapshot_rows_written") or 0)
                snapshot_rows_skipped = int(reconstruction_result.get("snapshot_rows_skipped") or 0)
                failed_tickers = list(reconstruction_result.get("failed_tickers") or [])
                detail_parts = [
                    f"snapshot rows written={snapshot_rows_written}",
                    f"skipped={snapshot_rows_skipped}",
                ]
                if failed_tickers:
                    detail_parts.append("failed tickers=" + ", ".join(failed_tickers))
                prepare_post_mutation_refresh(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level="success",
                    message=(
                        f"Affected-theme reconstruction finished with status `{result.get('status')}` for "
                        f"{len(result.get('tickers') or [])} ticker(s): " + "; ".join(detail_parts) + ". "
                        f"Completed in {time.perf_counter() - started:.1f}s."
                    ),
                    clear_market=True,
                    clear_scanner_summary=True,
                )
                st.rerun()
            except Exception as exc:
                queue_feedback_message(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level="error",
                    message=f"Affected-theme reconstruction failed: {exc}",
                )
                st.rerun()
        st.caption(
            "History hydration is ticker-scoped. Affected-theme reconstruction is a separate explicit step once ticker history is ready."
        )
        render_dataframe(
            "governed_ticker_onboarding",
            governed_onboarding[
                [
                    "ticker",
                    "added_at",
                    "onboarding_source",
                    "history_readiness_status",
                    "backfill_status",
                    "history_row_count",
                    "history_target_days",
                    "history_market_data_source",
                    "history_latest_trading_date",
                    "downstream_refresh_needed",
                    "last_backfill_attempt_at",
                    "last_backfill_error",
                    "governed_assignment_count",
                    "governed_themes",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    with get_conn() as conn:
        reconstruction_runs = historical_reconstruction_runs(conn, limit=10)
    if not reconstruction_runs.empty:
        st.subheader("Historical reconstruction runs")
        st.caption(
            "Reconstructed history runs now log both stored ticker-day history and downstream reconstructed theme refresh results. "
            "This layer is additive, used for deeper movement analysis only, and never treated as true captured point-in-time composition."
        )
        render_dataframe("health_reconstruction_runs", reconstruction_runs, width="stretch", hide_index=True)

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
            render_dataframe("health_failure_categories", cats, width="stretch")
            render_dataframe("health_recent_failures", recent_failures, width="stretch")

    st.subheader("Symbol hygiene review queue")
    render_feedback_message(st.session_state, "symbol_hygiene_feedback")
    queue_warning_messages: list[str] = []
    try:
        with get_conn() as conn:
            # Authoritative Health-page queue call: explicitly wire the outlier path to a fresh read connection.
            queue = symbol_hygiene_queue(conn, limit=250, outlier_read_conn_factory=get_fresh_read_conn)
        queue_warning_messages = [str(message) for message in queue.attrs.get("warnings", []) if str(message).strip()]
    except Exception as exc:
        queue = pd.DataFrame()
        queue_warning_messages = [
            "Symbol hygiene queue loaded without calculation outlier context because that subquery failed. "
            f"Details: {exc}"
        ]

    for message in queue_warning_messages:
        st.warning(message)

    staged_actions = st.session_state.setdefault("symbol_hygiene_staged", {})

    if queue.empty:
        st.success("No flagged/suppressed/watch symbols currently in queue.")
    else:
        queue_view = st.selectbox(
            "Queue view",
            ["Pending review", "Suppressed / resolved", "All"],
            index=0,
            key="symbol_hygiene_queue_view",
            help="Pending review focuses on actionable items. Suppressed / resolved shows symbols already moved out of active refresh.",
        )
        queue_sort = st.selectbox(
            "Queue sort",
            [
                "Highest confidence",
                "Longest invalid period",
                "Most consecutive failures",
                "Most rolling failures",
            ],
            index=0,
            key="symbol_hygiene_queue_sort",
            help="Prioritize the review queue by confidence, data staleness, or failure streak intensity.",
        )
        queue = filter_symbol_hygiene_queue(queue, queue_view)
        queue = sort_symbol_hygiene_queue(queue, queue_sort)
        st.caption(
            "Suppression is a calculation-control decision, not a delete action. "
            "Preferred policy: keep symbol lineage/history in DuckDB, suppress high-confidence non-viable symbols from refresh and theme calculations, and review theme membership separately."
        )
        if queue_view == "Pending review":
            st.caption("Default view shows actionable review items. Already suppressed symbols move to `Suppressed / resolved` after approval.")
        elif queue_view == "Suppressed / resolved":
            st.caption("This view shows symbols already removed from active refresh. They remain in DuckDB for lineage/history and can be reviewed separately from theme membership.")

        queue_tickers = [str(row["ticker"]).strip().upper() for _, row in queue.iterrows()]
        staged_visible = {ticker: action for ticker, action in staged_actions.items() if action in STAGED_ACTIONS and action != "none"}
        s1, s2, s3 = st.columns([3, 1, 1])
        with s1:
            if staged_visible:
                action_counts: dict[str, int] = {}
                for action in staged_visible.values():
                    action_counts[action] = action_counts.get(action, 0) + 1
                counts_text = ", ".join(f"{STAGED_ACTIONS[action]}: {count}" for action, count in sorted(action_counts.items()))
                st.info(f"Staged changes: {len(staged_visible)} symbol(s). {counts_text}")
            else:
                st.caption("No staged hygiene actions yet. Review rows below and staged selections will appear here immediately.")
        with s2:
            if st.button("Clear staged changes", key="clear_hygiene_staged", disabled=not bool(staged_visible)):
                clear_symbol_hygiene_staged_state(st.session_state, queue_tickers)
                st.session_state["symbol_hygiene_feedback"] = {"level": "success", "message": "Cleared staged hygiene actions."}
                st.rerun()
        with s3:
            if st.button("Apply staged changes", key="apply_hygiene_staged", type="primary", disabled=not bool(staged_visible)):
                try:
                    with get_conn() as conn:
                        result = apply_staged_symbol_hygiene_actions(conn, staged_visible)
                    by_action = result.get("by_action") or {}
                    summary_bits = ", ".join(
                        f"{STAGED_ACTIONS[action]}: {count}" for action, count in sorted(by_action.items())
                    )
                    clear_symbol_hygiene_staged_state(st.session_state, queue_tickers)
                    st.session_state["symbol_hygiene_feedback"] = {
                        "level": "success",
                        "message": (
                            f"Applied staged hygiene changes for {int(result.get('applied_count') or 0)} symbol(s). "
                            f"{summary_bits}".strip()
                        ),
                    }
                except Exception as exc:
                    st.session_state["symbol_hygiene_feedback"] = {
                        "level": "error",
                        "message": f"Applying staged hygiene changes failed: {exc}",
                    }
                st.rerun()

        if queue.empty:
            st.success("No symbols match the current queue view.")
        else:
            for _, row in queue.iterrows():
                ticker = str(row["ticker"]).strip().upper()
                decision = hygiene_decision_context(row)
                recommendation = decision["recommended_action"]
                confidence = decision["confidence"]
                recommendation_help = decision["explanation"]
                last_market_data = short_timestamp(row.get("last_market_data_at")) or "none"
                days_since_valid = row.get("days_since_last_valid_data")
                days_since_valid_text = "unknown" if days_since_valid is None else f"{int(days_since_valid)}d"
                staged_action = staged_visible.get(ticker, "none")
                default_approve = staged_action == "suppress"
                default_override = staged_action if staged_action in OVERRIDE_ACTIONS and staged_action != "none" else "none"
                approve_key = f"stage_approve_{ticker}"
                override_key = f"stage_override_{ticker}"
                if approve_key not in st.session_state:
                    st.session_state[approve_key] = default_approve
                if override_key not in st.session_state:
                    st.session_state[override_key] = default_override

                with st.container(border=True):
                    c1, c2, c3, c4, c5, c6, c7 = st.columns([1, 1.1, 1.2, 1.3, 0.9, 1.1, 1.4])
                    c1.write(f"**{ticker}**")
                    c2.write(f"cat: `{row.get('last_failure_category') or 'n/a'}`")
                    c3.write(f"status: `{row.get('status')}`")
                    c4.write(f"recommended: `{recommendation}`")
                    c5.write(f"confidence: `{confidence}`")
                    c6.write(f"last valid data: `{last_market_data}`")
                    c7.write(f"days since valid: `{days_since_valid_text}`")
                    st.caption(
                        f"consecutive={int(row.get('consecutive_failure_count') or 0)} | "
                        f"rolling={int(row.get('rolling_failure_count') or 0)} | "
                        f"last success={row.get('last_success_at') or 'never'} | "
                        f"suggested_status={row.get('suggested_status') or 'none'}"
                    )
                    st.caption(str(row.get("suggested_reason") or recommendation_help))
                    if row.get("outlier_reason"):
                        st.caption(
                            f"Calculation outlier: {row.get('outlier_reason')} "
                            f"| surfaces={row.get('affected_calculation_surfaces') or 'n/a'}"
                        )
                        st.caption(
                            f"price={_display_placeholder(row.get('price'))} | "
                            f"dollar_volume={_display_placeholder(row.get('dollar_volume'))} | "
                            f"1W={_display_placeholder(row.get('perf_1w'))} | "
                            f"1M={_display_placeholder(row.get('perf_1m'))}"
                        )
                    current_themes = str(row.get("current_theme_names") or "").strip()
                    current_categories = str(row.get("current_categories") or "").strip()
                    if current_themes:
                        st.caption(f"Themes: {current_themes}")
                        st.caption(f"Categories: {current_categories or 'Uncategorized'}")
                    else:
                        st.caption("Not currently assigned to any theme.")
                    if staged_action != "none":
                        st.info(f"Staged: {STAGED_ACTIONS[staged_action]}")
                    approve_help = (
                        "Check to stage the common approve-suppression action. "
                        "If you choose an override below, the override wins."
                    )
                    st.checkbox(
                        "Approve recommended action",
                        key=approve_key,
                        help=approve_help,
                        on_change=sync_symbol_hygiene_staged_action,
                        args=(st.session_state, ticker),
                    )
                    st.selectbox(
                        f"Override action for {ticker}",
                        options=list(OVERRIDE_ACTIONS.keys()),
                        format_func=lambda key: OVERRIDE_ACTIONS[key],
                        key=override_key,
                        help="Optional override for less common actions. Overrides the checkbox if selected.",
                        on_change=sync_symbol_hygiene_staged_action,
                        args=(st.session_state, ticker),
                    )

    st.subheader("Refresh history")
    refresh_event = render_dataframe(
        "health_refresh_history",
        history,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        key="health_refresh_history_table",
    )
    selected_refresh_row = extract_selected_row(refresh_event)
    selected_refresh = None
    if selected_refresh_row is not None and 0 <= selected_refresh_row < len(history):
        selected_refresh = history.reset_index(drop=True).iloc[int(selected_refresh_row)]
        selected_run_id = int(selected_refresh["run_id"])
        st.session_state["health_selected_refresh_run_id"] = selected_run_id
    else:
        selected_run_id = st.session_state.get("health_selected_refresh_run_id")
        if selected_run_id is not None:
            matching_refresh = history[history["run_id"] == int(selected_run_id)]
            if not matching_refresh.empty:
                selected_refresh = matching_refresh.reset_index(drop=True).iloc[0]

    if selected_refresh is not None:
        selected_run_id = int(selected_refresh["run_id"])
        selected_status = str(selected_refresh.get("status") or "")
        started_at = pd.to_datetime(selected_refresh.get("started_at"), errors="coerce")
        age_minutes = None
        if pd.notna(started_at):
            age_minutes = (datetime.now(timezone.utc).replace(tzinfo=None) - started_at.to_pydatetime()).total_seconds() / 60.0
        stale_hint = age_minutes is not None and age_minutes >= REFRESH_STALE_TIMEOUT_MINUTES
        st.caption(
            f"Selected run #{selected_run_id} | status=`{selected_status or 'n/a'}`"
            + (f" | age_minutes=`{age_minutes:.1f}`" if age_minutes is not None else "")
        )
        if selected_status == "running":
            if stale_hint:
                st.warning(
                    f"Run #{selected_run_id} looks stale based on the `{REFRESH_STALE_TIMEOUT_MINUTES}` minute timeout. "
                    "Use the recovery action below only if you know the process is no longer active."
                )
            if st.button(
                "Mark selected running run interrupted",
                key="mark_selected_refresh_run_interrupted",
                type="secondary",
            ):
                try:
                    with get_conn() as conn:
                        changed = mark_refresh_run_interrupted(
                            conn,
                            selected_run_id,
                            note="Run manually marked interrupted from Refresh history.",
                        )
                    if changed:
                        queue_feedback_message(
                            st.session_state,
                            "refresh_recovery_feedback",
                            level="success",
                            message=f"Marked refresh run #{selected_run_id} interrupted. New refreshes are unblocked.",
                        )
                    else:
                        queue_feedback_message(
                            st.session_state,
                            "refresh_recovery_feedback",
                            level="warning",
                            message=f"Refresh run #{selected_run_id} was no longer running, so nothing was changed.",
                        )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to mark run interrupted: {exc}")
    st.subheader("Table row counts")
    render_dataframe("health_row_counts", counts, width="stretch")

with themes_tab:
    c1, c2 = st.columns(2)
    with c1:
        low_threshold = st.number_input("Low constituent threshold", min_value=1, max_value=25, value=RULE_LOW_CONSTITUENT_THRESHOLD)
    with c2:
        failure_window = st.number_input("Live failure lookback (days)", min_value=1, max_value=90, value=RULE_LIVE_FAILURE_WINDOW_DAYS)

    health = load_theme_health_overview_cached(db_token, int(low_threshold), int(failure_window))

    if health.empty:
        st.info("No theme health data.")
    else:
        audit = enrich_theme_health_for_audit(health, stale_hours=STALE_DATA_HOURS)
        audit_counts = theme_health_audit_counts(audit)

        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Empty themes", audit_counts.empty_themes)
        a2.metric("Low-count themes", audit_counts.low_count_themes)
        a3.metric("Stale / no snapshot", audit_counts.stale_themes)
        a4.metric("Recent failure themes", audit_counts.recent_failure_themes)
        a5, a6, a7 = st.columns(3)
        a5.metric("Active with zero members", audit_counts.active_zero_member_themes)
        a6.metric("Inactive with members", audit_counts.inactive_with_members)
        a7.metric("Recently changed", audit_counts.recently_changed_themes)
        st.caption(
            "Primary empty definition: zero governed members. Missing or stale snapshots are tracked separately so audit triage can distinguish structural membership problems from market-history gaps."
        )
        with get_conn() as conn:
            theme_export_df = theme_membership_export(conn)
        export_csv = theme_export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download themes membership CSV",
            data=export_csv,
            file_name="theme_membership_audit_export.csv",
            mime="text/csv",
            help="Exports all themes with category, active status, normalized governed member count, and governed member list.",
            key="theme_health_membership_export",
        )

        f1, f2, f3, f4 = st.columns([1.8, 1.4, 1.3, 1.1])
        with f1:
            preset = st.radio("Audit preset", AUDIT_PRESETS, horizontal=True, index=0)
        with f2:
            audit_sort = st.selectbox("Sort", AUDIT_SORT_OPTIONS, index=0)
        with f3:
            search_text = st.text_input("Search", placeholder="theme, category, reason")
        with f4:
            flagged_only = st.checkbox("Only flagged", value=False)

        view = apply_theme_health_audit_preset(audit, preset)
        if str(search_text or "").strip():
            query = str(search_text).strip().casefold()
            view = view[
                view["theme_name"].astype(str).str.casefold().str.contains(query, na=False)
                | view["category"].astype(str).str.casefold().str.contains(query, na=False)
                | view["why_flagged"].astype(str).str.casefold().str.contains(query, na=False)
                | view["next_action"].astype(str).str.casefold().str.contains(query, na=False)
            ]
        if flagged_only:
            view = view[view["why_flagged"] != "healthy"]
        view = sort_theme_health_audit(view, audit_sort)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Themes in audit view", int(view.shape[0]))
        m2.metric("Needs attention", int((view["audit_status"] == "needs_attention").sum()))
        m3.metric("Watch", int((view["audit_status"] == "watch").sum()))
        m4.metric("Healthy", int((view["audit_status"] == "healthy").sum()))

        health_view = view[[
            "theme_name",
            "category",
            "is_active",
            "constituent_count",
            "empty_theme_flag",
            "low_count_flag",
            "stale_theme_flag",
            "live_failure_count_recent",
            "latest_snapshot_time",
            "why_flagged",
            "next_action",
            "audit_status",
        ]].copy()
        health_view["latest_snapshot_time"] = health_view["latest_snapshot_time"].apply(
            lambda v: short_timestamp(v) or "—"
        )
        st.caption(
            "`latest_snapshot_time` uses the preferred current-view theme source. `why_flagged` explains the audit signal; `next_action` is the recommended operator follow-up."
        )
        health_event = render_dataframe(
            "health_theme_table",
            health_view,
            width="stretch",
            on_select="rerun",
            selection_mode="single-row",
            key="health_theme_table",
        )
        picked_idx = extract_selected_row(health_event)
        view_reset = view.reset_index(drop=True)
        selected_theme_id = st.session_state.get("health_selected_theme_id")
        if picked_idx is not None and 0 <= picked_idx < len(view_reset):
            selected_theme_id = int(view_reset.iloc[picked_idx]["theme_id"])
            st.session_state["health_selected_theme_id"] = selected_theme_id

        picked = None
        if selected_theme_id is not None:
            matching = view_reset[view_reset["theme_id"] == int(selected_theme_id)]
            if not matching.empty:
                picked = matching.iloc[0]

        queue_options = [_queue_label(int(row["theme_id"]), str(row["theme_name"])) for _, row in view_reset.iterrows()]
        selected_queue_labels = sync_valid_multiselect_state(
            st.session_state,
            "health_theme_audit_queue",
            queue_options,
            default=[],
        )
        st.multiselect(
            "Audit queue selection",
            options=queue_options,
            key="health_theme_audit_queue",
            help="Select one or more themes from the current audit view for queue actions.",
        )
        selected_queue_ids = [
            int(label.rsplit("[", 1)[1].rstrip("]"))
            for label in get_canonical_multiselect_values(st.session_state, "health_theme_audit_queue")
            if label in queue_options and "[" in label and label.endswith("]")
        ]
        selected_queue = view_reset[view_reset["theme_id"].isin(selected_queue_ids)].copy()
        action_state = theme_health_action_eligibility(selected_queue)
        if action_state["selected_count"]:
            st.caption(
                f"Queue selected: `{int(action_state['selected_count'])}` theme(s) | "
                f"rebuild-ready=`{len(action_state['rebuild_theme_ids'])}` | "
                f"backfill-ready=`{len(action_state['backfill_theme_ids'])}` | "
                f"deactivate-ready=`{len(action_state['deactivate_theme_ids'])}`"
            )
            q1, q2, q3, q4 = st.columns(4)
            rebuild_clicked = q1.button(
                f"Rebuild recent selected ({SUPPRESSION_REBUILD_LOOKBACK_DAYS}d)",
                disabled=not bool(action_state["rebuild_theme_ids"]),
                help="Rebuilds recent reconstructed history for the selected themes using stored ticker history.",
                key="health_theme_queue_rebuild",
            )
            backfill_clicked = q2.button(
                "Backfill + reconstruct selected",
                disabled=not bool(action_state["backfill_theme_ids"]) or not bool(massive_api_key()),
                help="Fetches recent live ticker history and rebuilds selected themes. Requires live API configuration.",
                key="health_theme_queue_backfill",
            )
            deactivate_clicked = q3.button(
                "Deactivate selected empty active",
                disabled=not bool(action_state["deactivate_theme_ids"]),
                help="Only deactivates selected themes that are active and currently have zero governed members.",
                key="health_theme_queue_deactivate",
            )
            clear_queue_clicked = q4.button(
                "Clear queue",
                key="health_theme_queue_clear",
            )
            deactivate_confirmed = st.checkbox(
                "Confirm deactivation of selected empty active themes",
                value=False,
                disabled=not bool(action_state["deactivate_theme_ids"]),
                key="health_theme_queue_deactivate_confirm",
                help="Required before the deactivate action will run.",
            )

            if rebuild_clicked:
                try:
                    with get_conn() as conn:
                        rebuild_result = rebuild_recent_reconstructed_history(
                            conn,
                            theme_ids=[int(theme_id) for theme_id in action_state["rebuild_theme_ids"]],
                        )
                    prepare_post_mutation_refresh(
                        st.session_state,
                        "governed_onboarding_feedback",
                        level="success" if str(rebuild_result.get("status") or "") not in {"no_scope", "no_ticker_history", "no_reconstructed_scope", "no_history_rows", "no_op"} else "warning",
                        message=(
                            f"Queue rebuild result: status={rebuild_result.get('status')} | "
                            f"themes={len(rebuild_result.get('affected_theme_ids', []))} | "
                            f"replaced={int(rebuild_result.get('rows_replaced') or 0)} | "
                            f"written={int(rebuild_result.get('rows_written') or 0)}."
                        ),
                        clear_market=True,
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not rebuild selected themes. {exc}")

            if backfill_clicked:
                try:
                    end_date = datetime.now(timezone.utc).replace(tzinfo=None).date()
                    start_date = end_date - timedelta(days=45)
                    with get_conn() as conn:
                        backfill_result = reconstruct_theme_history_range(
                            conn,
                            provider_name=DEFAULT_PROVIDER,
                            start_date=start_date,
                            end_date=end_date,
                            theme_ids=[int(theme_id) for theme_id in action_state["backfill_theme_ids"]],
                            provenance_source_label="theme_health_queue_backfill",
                            run_kind="theme_health_queue_backfill",
                            replace_existing=False,
                            persist_ticker_history=True,
                        )
                    level = "success" if str(backfill_result.get("status") or "") in {"success", "partial"} else "warning"
                    prepare_post_mutation_refresh(
                        st.session_state,
                        "governed_onboarding_feedback",
                        level=level,
                        message=(
                            f"Queue backfill result: status={backfill_result.get('status')} | "
                            f"themes={len(backfill_result.get('theme_ids', []))} | "
                            f"ticker_history_written={int(backfill_result.get('ticker_history_rows_written') or 0)} | "
                            f"snapshot_rows_written={int(backfill_result.get('snapshot_rows_written') or 0)} | "
                            f"failed_tickers={len(backfill_result.get('failed_tickers') or [])}."
                        ),
                        clear_market=True,
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not backfill selected themes. {exc}")

            if deactivate_clicked:
                if not deactivate_confirmed:
                    st.warning("Confirm deactivation before running this action.")
                else:
                    try:
                        with get_conn() as conn:
                            target_rows = selected_queue[selected_queue["theme_id"].isin(action_state["deactivate_theme_ids"])].copy()
                            for _, row in target_rows.iterrows():
                                update_theme(conn, int(row["theme_id"]), str(row["theme_name"]), str(row["category"] or ""), False)
                        prepare_post_mutation_refresh(
                            st.session_state,
                            "governed_onboarding_feedback",
                            level="success",
                            message=f"Deactivated {len(action_state['deactivate_theme_ids'])} selected empty active theme(s).",
                            clear_market=True,
                            clear_scanner_summary=True,
                            clear_research=True,
                        )
                        st.session_state["health_theme_queue_deactivate_confirm"] = False
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Could not deactivate selected themes. {exc}")

            if clear_queue_clicked:
                st.session_state["health_theme_audit_queue"] = []
                st.rerun()

        if picked is not None:
            theme_id = int(picked["theme_id"])
            theme_name = str(picked["theme_name"])
            theme_label = f"{theme_name} ({picked['category']})"
            with get_conn() as conn:
                member_rows = theme_member_hygiene_context(conn, theme_id)
                governed_members = get_theme_members(conn, theme_id)
            members = member_rows["ticker"].tolist() if not member_rows.empty else []
            governed_members_list = governed_members["ticker"].tolist() if not governed_members.empty else []

            st.subheader("Selected theme detail")
            st.caption("Compact audit detail for the selected theme. Use this to confirm why the theme is flagged and the next best operator step.")
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("Theme", theme_name)
            d2.metric("Category", str(picked["category"] or "Uncategorized"))
            d3.metric("Active", "Yes" if bool(picked["is_active"]) else "No")
            d4.metric("Ticker count", int(picked["constituent_count"] or 0))
            d5, d6, d7, d8 = st.columns(4)
            d5.metric("Latest snapshot", short_timestamp(picked.get("latest_snapshot_time")) or "none")
            d6.metric("Recent failures", int(picked.get("live_failure_count_recent") or 0))
            d7.metric("Audit status", str(picked.get("audit_status") or "healthy").replace("_", " ").title())
            d8.metric("Governed members", len(governed_members_list))
            st.caption(f"Why flagged: `{picked.get('why_flagged') or 'healthy'}`")
            st.caption(f"Next action: `{picked.get('next_action') or 'monitor'}`")
            if governed_members_list:
                preview = ", ".join(governed_members_list[:8])
                if len(governed_members_list) > 8:
                    preview = f"{preview}, +{len(governed_members_list) - 8} more"
                st.caption(f"Current governed members: `{preview}`")
            else:
                st.caption("Current governed members: `none`")
            detail_queue_cols = st.columns(3)
            queue_selected_clicked = detail_queue_cols[0].button("Add selected theme to queue", key=f"health_theme_add_queue_{theme_id}")
            recommended_action = str(picked.get("next_action") or "").strip().lower()
            queue_recommended_label = None
            if any(token in recommended_action for token in ["reconstruct", "refresh snapshots"]):
                queue_recommended_label = "Queue recommended: rebuild"
            elif "deactivate" in recommended_action:
                queue_recommended_label = "Queue recommended: deactivate"
            elif "review failing members" in recommended_action or "inspect failures" in recommended_action:
                queue_recommended_label = "Queue recommended: backfill/rebuild"
            queue_recommended_clicked = detail_queue_cols[1].button(
                queue_recommended_label or "Queue recommended action",
                disabled=queue_recommended_label is None,
                key=f"health_theme_add_recommended_queue_{theme_id}",
            )
            open_queue_hint = detail_queue_cols[2]
            open_queue_hint.caption("Use the audit queue bar above to run actions after adding this theme.")

            if queue_selected_clicked:
                added, label = _add_theme_to_audit_queue(st.session_state, theme_id, theme_name)
                queue_feedback_message(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level="success" if added else "warning",
                    message=f"Added `{label}` to the audit queue." if added else f"`{label}` is already in the audit queue.",
                )
                st.rerun()

            if queue_recommended_clicked and queue_recommended_label is not None:
                added, label = _add_theme_to_audit_queue(st.session_state, theme_id, theme_name)
                queue_feedback_message(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level="success" if added else "warning",
                    message=(
                        f"Queued `{label}` for the recommended follow-up: {queue_recommended_label.replace('Queue recommended: ', '')}."
                        if added
                        else f"`{label}` is already in the audit queue."
                    ),
                )
                st.rerun()
            if members:
                failed_count = int(member_rows["last_failure_at"].notna().sum()) if "last_failure_at" in member_rows.columns else 0
                st.metric("Members with recent failures", failed_count)
                member_view = member_rows.copy()
                member_view["calculation_status"] = member_rows["symbol_hygiene_status"].apply(
                    lambda value: "Suppressed" if str(value or "").strip().lower() == "refresh_suppressed" else "Active"
                )
                member_view["last_failure_category"] = member_view["last_failure_category"].map(_display_placeholder)
                member_view["last_failure_at"] = member_view["last_failure_at"].apply(lambda v: short_timestamp(v) or "-")
                member_view["consecutive_failure_count"] = member_view["consecutive_failure_count"].map(_display_placeholder)
                member_view["symbol_hygiene_status"] = member_view["symbol_hygiene_status"].map(_display_placeholder)
                st.caption("Member ticker failure context. Tickers with the most recent failures are listed first.")
                render_dataframe("health_member_view", member_view, width="stretch", hide_index=True)

                with st.form(f"health_theme_member_suppression_{theme_id}"):
                    st.write("Manual member calculation control")
                    suppression_member = st.selectbox(
                        "Member ticker",
                        options=members,
                        help="This changes calculation eligibility only. Theme membership remains intact.",
                    )
                    selected_member_row = member_rows[member_rows["ticker"] == suppression_member]
                    selected_member_status = (
                        str(selected_member_row.iloc[0]["symbol_hygiene_status"] or "").strip().lower()
                        if not selected_member_row.empty
                        else ""
                    )
                    is_suppressed = selected_member_status == "refresh_suppressed"
                    current_state_label = "Suppressed" if is_suppressed else "Active"
                    action_label = "Return to active calculations" if is_suppressed else "Suppress from calculations"
                    st.caption(f"Current calculation state: {current_state_label}")
                    if is_suppressed:
                        st.caption("Suppressed members are excluded from rankings and historical movement. Theme membership is retained.")
                    rebuild_after_suppression = st.checkbox(
                        f"Rebuild recent reconstructed history for affected themes ({SUPPRESSION_REBUILD_LOOKBACK_DAYS}d)",
                        value=True,
                        help="Rewrites only recent reconstructed rows for affected themes. Captured history is untouched.",
                    )
                    suppression_submitted = st.form_submit_button(action_label)

                if suppression_submitted:
                    try:
                        with get_conn() as conn:
                            if not is_suppressed:
                                approve_suppression(
                                    conn,
                                    suppression_member,
                                    note="Manually suppressed from calculations in Theme Health. Theme membership preserved.",
                                )
                                success_message = (
                                    f"Suppressed `{suppression_member}` from refresh, ranking, and movement calculations. "
                                    "Theme membership was left unchanged."
                                )
                            else:
                                reject_keep_active(conn, suppression_member)
                                success_message = (
                                    f"Returned `{suppression_member}` to active calculations. "
                                    "Theme membership was left unchanged."
                                )
                            rebuild_result = None
                            if rebuild_after_suppression:
                                rebuild_result = rebuild_recent_reconstructed_history(conn, tickers=[suppression_member])
                        st.session_state["health_selected_theme_id"] = theme_id
                        if rebuild_result is not None:
                            rebuild_bits = (
                                f" Rebuilt recent reconstructed history for {len(rebuild_result.get('affected_theme_ids', []))} affected theme(s) "
                                f"over {rebuild_result.get('window_start')} to {rebuild_result.get('window_end')} "
                                f"| replaced={int(rebuild_result.get('rows_replaced') or 0)} "
                                f"| written={int(rebuild_result.get('rows_written') or 0)}."
                            )
                        else:
                            rebuild_bits = ""
                        clear_current_market_view_caches()
                        queue_feedback_message(
                            st.session_state,
                            "governed_onboarding_feedback",
                            level="success",
                            message=f"{success_message}{rebuild_bits}",
                        )
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Could not update ticker calculation suppression. {exc}")

                with st.form(f"health_theme_member_rebuild_{theme_id}"):
                    st.write("Manual recent reconstructed-history rebuild")
                    rebuild_member = st.selectbox(
                        "Ticker to rebuild around",
                        options=members,
                        help="Scopes the rebuild to themes currently containing this ticker.",
                    )
                    rebuild_submitted = st.form_submit_button(
                        f"Rebuild recent reconstructed history ({SUPPRESSION_REBUILD_LOOKBACK_DAYS}d)"
                    )

                if rebuild_submitted:
                    try:
                        with get_conn() as conn:
                            rebuild_result = rebuild_recent_reconstructed_history(conn, tickers=[rebuild_member])
                        st.session_state["health_selected_theme_id"] = theme_id
                        status = str(rebuild_result.get("status") or "unknown")
                        if status in {"no_scope", "no_ticker_history", "no_reconstructed_scope", "no_history_rows", "no_op"}:
                            queue_feedback_message(
                                st.session_state,
                                "governed_onboarding_feedback",
                                level="warning",
                                message=(
                                    f"Rebuild result for `{rebuild_member}`: {status}. "
                                    f"Affected themes={len(rebuild_result.get('affected_theme_ids', []))}, "
                                    f"replaced={int(rebuild_result.get('rows_replaced') or 0)}, "
                                    f"written={int(rebuild_result.get('rows_written') or 0)}."
                                ),
                            )
                        else:
                            queue_feedback_message(
                                st.session_state,
                                "governed_onboarding_feedback",
                                level="success",
                                message=(
                                    f"Rebuilt recent reconstructed history for `{rebuild_member}` "
                                    f"over {rebuild_result.get('window_start')} to {rebuild_result.get('window_end')} "
                                    f"| labels={', '.join(rebuild_result.get('labels_rebuilt') or []) or 'none'} "
                                    f"| replaced={int(rebuild_result.get('rows_replaced') or 0)} "
                                    f"| written={int(rebuild_result.get('rows_written') or 0)}."
                                ),
                            )
                        clear_current_market_view_caches()
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Recent reconstructed-history rebuild failed. {exc}")

                with st.form(f"health_theme_replace_ticker_{theme_id}"):
                    st.write("Correct member ticker")
                    current_member = st.selectbox(
                        "Current ticker",
                        options=members,
                        help="Pick the existing member ticker to replace within this theme only.",
                    )
                    replacement_member = st.text_input(
                        "Replacement ticker",
                        help="Required. Replacement is normalized to uppercase and only updates this theme membership.",
                    )
                    replace_submitted = st.form_submit_button("Replace ticker in this theme")

                if replace_submitted:
                    try:
                        with get_conn() as conn:
                            result = replace_ticker_in_theme(conn, theme_id, current_member, replacement_member)
                        st.session_state["health_selected_theme_id"] = theme_id
                        prepare_post_mutation_refresh(
                            st.session_state,
                            "governed_onboarding_feedback",
                            level="success",
                            message=f"Removed {result['removed_ticker']} from {theme_name} and added {result['added_ticker']}.",
                            clear_market=True,
                            clear_scanner_summary=True,
                            clear_research=True,
                        )
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Could not replace ticker in this theme. {exc}")
            else:
                st.info("This theme currently has no member tickers.")

            with st.form(f"health_theme_edit_{theme_id}"):
                st.write("Edit theme fields")
                edit_name = st.text_input("Theme name", value=theme_name, help="Required. Theme names must remain unique.")
                edit_category = st.text_input(
                    "Category",
                    value=str(picked["category"] or ""),
                    help="Optional. Blank values will be normalized to 'Uncategorized'.",
                )
                edit_active = st.checkbox(
                    "Active status (editable)",
                    value=bool(picked["is_active"]),
                    help="Toggle whether this theme remains active in normal operations.",
                )
                submitted = st.form_submit_button("Save theme changes")

            if submitted:
                intended_name = edit_name.strip()
                intended_category = edit_category.strip() or "Uncategorized"
                current_category = str(picked["category"] or "Uncategorized")
                current_active = bool(picked["is_active"])

                if not intended_name:
                    st.error("Theme name cannot be blank.")
                elif (
                    intended_name == theme_name
                    and intended_category == current_category
                    and edit_active == current_active
                ):
                    st.info("No changes to save.")
                else:
                    try:
                        with get_conn() as conn:
                            update_theme(conn, theme_id, edit_name, edit_category, edit_active)
                        st.session_state["health_selected_theme_id"] = theme_id
                        prepare_post_mutation_refresh(
                            st.session_state,
                            "governed_onboarding_feedback",
                            level="success",
                            message=f"Updated theme `{intended_name}`.",
                            clear_market=True,
                            clear_scanner_summary=True,
                            clear_research=True,
                        )
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Could not update theme. {exc}")

            if st.button(f"Open `{theme_name}` in Themes detail", key="open_health_theme_detail"):
                st.session_state["manage_theme"] = f"{theme_name} [{theme_id}]"
                set_theme_selection_state(st.session_state, theme_id, theme_label, "health_theme")
                st.switch_page("pages/1_Themes.py")

show_perf_summary()
