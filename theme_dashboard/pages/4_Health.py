from datetime import datetime, timedelta, timezone
import time

import pandas as pd
import streamlit as st

from src.config import (
    DEFAULT_PROVIDER,
    REFRESH_STALE_TIMEOUT_MINUTES,
    RULE_LIVE_FAILURE_WINDOW_DAYS,
    RULE_LOW_CONSTITUENT_THRESHOLD,
    STALE_DATA_HOURS,
    massive_api_key,
)
from src.database import get_conn, get_fresh_read_conn, init_db
from src.fetch_data import mark_stale_running_runs
from src.historical_backfill import (
    SUPPRESSION_REBUILD_LOOKBACK_DAYS,
    reconstruct_theme_history_range,
    rebuild_recent_reconstructed_history,
)
from src.metric_formatting import short_timestamp
from src.queries import (
    canonical_daily_health_status,
    canonical_daily_recent_coverage,
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
    complete_governed_ticker_onboarding,
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


def _remove_theme_from_audit_queue(session_state, theme_id: int, theme_name: str) -> tuple[bool, str]:
    label = _queue_label(theme_id, theme_name)
    current = list(get_canonical_multiselect_values(session_state, "health_theme_audit_queue"))
    if label not in current:
        return False, label
    session_state["health_theme_audit_queue"] = [value for value in current if value != label]
    return True, label


def _focus_audit_queue_on_theme(session_state, theme_id: int, theme_name: str) -> str:
    label = _queue_label(theme_id, theme_name)
    session_state["health_theme_audit_queue"] = [label]
    return label


def _queue_status_summary(session_state, theme_id: int, theme_name: str) -> tuple[bool, int]:
    label = _queue_label(theme_id, theme_name)
    current = list(get_canonical_multiselect_values(session_state, "health_theme_audit_queue"))
    return label in current, len(current)


def _scope_preview(values: list[str], *, limit: int = 5) -> str:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    if not cleaned:
        return "none"
    shown = cleaned[:limit]
    if len(cleaned) > limit:
        shown.append(f"+{len(cleaned) - limit} more")
    return ", ".join(shown)


def _feedback_level_for_status(status: object) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"success", "completed"}:
        return "success"
    if normalized in {"partial", "no_scope", "no_op", "insufficient_after_attempt", "no_tracking_table"}:
        return "warning"
    return "error"


try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        mark_stale_running_runs(conn)
        canonical_daily_recent = canonical_daily_recent_coverage(conn, trading_day_limit=30)
        canonical_daily_health = canonical_daily_health_status(
            conn,
            trading_day_limit=30,
            reconciliation_top_n=10,
            coverage=canonical_daily_recent,
        )
        ticker_history_ready = ticker_history_readiness(conn, target_trading_days=30)
        governed_onboarding = list_governed_ticker_onboarding(conn, limit=100)
        governed_onboarding_counts_df = governed_ticker_onboarding_counts(conn)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()
THEME_HEALTH_FEEDBACK_KEY = "theme_health_feedback"

ops_tab, themes_tab = st.tabs(["Operations", "Theme Health"])

with ops_tab:
    render_feedback_message(st.session_state, "refresh_recovery_feedback")

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
        c1.metric("Expected active tickers", int(readiness["governed_active_tickers"]))
        c2.metric(
            f"Expected tickers with >={int(readiness['target_trading_days'])} rows",
            int(readiness["governed_active_tickers_ready"]),
        )
        c3.metric("Ready coverage", f"{float(readiness['governed_ready_pct']):.1f}%")
        st.caption(
            f"Source=`{readiness.get('market_data_source') or 'none'}` | "
            f"Raw governed=`{int(readiness.get('governed_active_tickers_raw') or readiness['governed_active_tickers'])}` | "
            f"Suppressed exclusions=`{int(readiness.get('governed_active_tickers_suppressed') or 0)}` | "
            f"Depth range=`{int(readiness['min_ticker_depth'])}` / `{float(readiness['median_ticker_depth']):.1f}` / `{int(readiness['max_ticker_depth'])}` "
            "(min / median / max rows across expected unsuppressed governed tickers)"
        )
        if readiness.get("earliest_trading_date") or readiness.get("latest_trading_date"):
            st.caption(
                f"Stored trading-date range: `{readiness.get('earliest_trading_date') or 'n/a'}` to "
                f"`{readiness.get('latest_trading_date') or 'n/a'}`"
            )

    if not canonical_daily_health.empty:
        canonical = canonical_daily_health.iloc[0]
        st.subheader("Canonical daily health")
        st.caption(
            "Operational guardrail for canonical daily rankings. This checks latest expected trading-date coverage, recent continuity, "
            "and whether the latest canonical leaders still reconcile to the current standardized leaders."
        )
        g1, g2, g3, g4, g5, g6 = st.columns(6)
        g1.metric("Latest expected date", str(canonical.get("latest_expected_trading_date") or "n/a"))
        g2.metric("Latest canonical date", str(canonical.get("latest_canonical_snapshot_date") or "n/a"))
        g3.metric("Date gap", int(canonical.get("canonical_trading_date_gap_count") or 0))
        g4.metric(
            "Latest date covered",
            "yes" if bool(canonical.get("latest_expected_date_canonically_covered")) else "no",
        )
        g5.metric("Top-10 mismatch count", canonical.get("top_n_mismatch_count") if canonical.get("top_n_mismatch_count") is not None else "n/a")
        g6.metric("Missing dates (30d)", int(canonical.get("recent_missing_dates") or 0))

        st.caption(
            f"Latest canonical rows=`{int(canonical.get('latest_canonical_row_count') or 0)}` | "
            f"ranked=`{int(canonical.get('latest_canonical_ranked_row_count') or 0)}` | "
            f"run-based=`{int(canonical.get('latest_canonical_run_based_row_count') or 0)}` | "
            f"repair=`{int(canonical.get('latest_canonical_repair_row_count') or 0)}` | "
            f"recent repair-involved dates=`{int(canonical.get('recent_repair_involved_dates') or 0)}` | "
            f"reconciliation status=`{canonical.get('reconciliation_status') or 'unknown'}`"
        )
        if bool(canonical.get("latest_day_leaders_match_current_standardized")):
            st.success("Latest expected trading date is canonically covered and the latest canonical leaders match current standardized leaders.")
        elif str(canonical.get("reconciliation_status") or "") == "stale_canonical_date":
            st.warning(
                "Latest canonical coverage is stale versus the latest expected trading date. Leader reconciliation below is against the most recent canonical date, not the latest expected date."
            )
        elif str(canonical.get("reconciliation_status") or "") == "mismatch":
            st.warning("Latest expected trading date is covered, but the canonical leader order does not fully reconcile to current standardized leaders.")
        else:
            st.info("Canonical daily reconciliation is unavailable until canonical coverage and ranked leaders are both present.")

        if not canonical_daily_recent.empty:
            exceptions = canonical_daily_recent[
                canonical_daily_recent["coverage_origin"].astype(str).isin(["missing", "repair_fallback", "mixed"])
            ].copy()
            if exceptions.empty:
                st.caption("Recent canonical continuity: all expected trading dates in the last 30-day window are covered by run-based canonical rows.")
            else:
                render_dataframe(
                    "canonical_daily_recent_exceptions",
                    exceptions[
                        [
                            "expected_trading_date",
                            "coverage_origin",
                            "canonical_row_count",
                            "ranked_canonical_row_count",
                            "repair_row_count",
                            "run_based_row_count",
                            "snapshot_source_summary",
                            "canonical_reason_summary",
                        ]
                    ],
                    width="stretch",
                    hide_index=True,
                )

    st.subheader("Newly governed ticker onboarding")
    st.caption(
        "Tracks post-addition history readiness for newly governed tickers. "
        "This does not run on advisory review actions; it starts only when governed membership is actually written."
    )
    st.caption(
        "This is the ticker-level propagation surface for current live hydration plus recent-history readiness. "
        "Canonical daily inclusion remains a separate day-finalized system and is guarded above."
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
        current_live_ready = int(len(governed_onboarding[governed_onboarding["has_current_usable_preferred_snapshot"] == True]))
        o1, o2, o3, o4, o5 = st.columns(5)
        o1.metric("Tracked tickers", onboarding_count)
        o2.metric("Current live ready", current_live_ready)
        o3.metric("History ready", ready_count)
        o4.metric("Needs backfill", needs_backfill)
        o5.metric("Downstream refresh needed", downstream_needed)
        if not governed_onboarding_counts_df.empty:
            st.caption(
                "Status mix: "
                + "; ".join(
                    f"{row['history_readiness_status']}/{row['backfill_status']}={int(row['cnt'])}"
                    for _, row in governed_onboarding_counts_df.iterrows()
                )
            )
        incomplete_onboarding_options = (
            governed_onboarding[
                governed_onboarding["propagation_status"].astype(str) != "ready_for_current_and_history"
            ]["ticker"]
            .astype(str)
            .tolist()
        )
        sync_valid_multiselect_state(
            st.session_state,
            "governed_onboarding_completion_tickers",
            incomplete_onboarding_options,
            default=incomplete_onboarding_options[:5],
        )
        st.caption(
            "Use this for normal onboarding completion. It tries current/live hydration, history backfill, and affected-theme reconstruction in order."
        )
        st.multiselect(
            "Tickers for one-click onboarding completion",
            options=incomplete_onboarding_options,
            key="governed_onboarding_completion_tickers",
            help="Attempts current hydration, history backfill, and affected-theme reconstruction in order for newly governed tickers that are not fully propagated yet.",
        )
        st.caption(
            f"Completion scope: selected=`{len(get_canonical_multiselect_values(st.session_state, 'governed_onboarding_completion_tickers'))}` | "
            f"eligible now=`{len(incomplete_onboarding_options)}` | tickers=`{_scope_preview(get_canonical_multiselect_values(st.session_state, 'governed_onboarding_completion_tickers'))}`"
        )
        if st.button(
            "Complete selected onboarding",
            type="primary",
            disabled=not bool(get_canonical_multiselect_values(st.session_state, "governed_onboarding_completion_tickers")),
            key="run_governed_onboarding_completion",
        ):
            try:
                selected_completion_tickers = get_canonical_multiselect_values(
                    st.session_state,
                    "governed_onboarding_completion_tickers",
                )
                started = time.perf_counter()
                with get_conn() as conn:
                    result = complete_governed_ticker_onboarding(conn, selected_completion_tickers)
                result_rows = list(result.get("results") or [])
                completed_rows = [row for row in result_rows if bool(row.get("completed"))]
                incomplete_rows = [row for row in result_rows if not bool(row.get("completed"))]
                stage_summary = _scope_preview(
                    [
                        (
                            f"{row.get('ticker')}:"
                            f"current={row.get('current_hydration', {}).get('status')},"
                            f"history={row.get('history_backfill', {}).get('status')},"
                            f"rebuild={row.get('theme_reconstruction', {}).get('status')}"
                        )
                        for row in result_rows
                    ],
                    limit=4,
                )
                feedback_bits = [
                    f"Onboarding completion finished with status `{result.get('status')}` for {len(result.get('tickers') or [])} ticker(s).",
                    f"Completed now=`{len(completed_rows)}` | still incomplete=`{len(incomplete_rows)}`.",
                    f"Stages: `{stage_summary}`.",
                ]
                if incomplete_rows:
                    feedback_bits.append(
                        f"Remaining gaps: `{_scope_preview([f'{row.get('ticker')}={row.get('final_propagation_status') or 'unknown'}' for row in incomplete_rows])}`."
                    )
                feedback_bits.append(
                    "This action completes current/live hydration, history readiness, and affected-theme reconstruction only. It does not finalize canonical daily inclusion."
                )
                feedback_bits.append("Current market/scanner caches were cleared; the onboarding table below reruns against refreshed state.")
                prepare_post_mutation_refresh(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level=_feedback_level_for_status(result.get("status")),
                    message=" ".join(feedback_bits) + f" Completed in {time.perf_counter() - started:.1f}s.",
                    clear_market=True,
                    clear_scanner_summary=True,
                )
                st.rerun()
            except Exception as exc:
                queue_feedback_message(
                    st.session_state,
                    "governed_onboarding_feedback",
                    level="error",
                    message=f"Onboarding completion failed: {exc}",
                )
                st.rerun()
        st.caption(
            "This onboarding flow completes current/live hydration, history readiness, and affected-theme reconstruction. It does not finalize canonical daily inclusion."
        )
        with st.expander("Recovery tools", expanded=False):
            st.caption("Use these only when the normal onboarding completion action does not finish the job.")
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
            st.caption("Use only if a ticker still needs more history after the normal onboarding action.")
            st.multiselect(
                "Tickers for onboarding history hydration",
                options=pending_backfill_options,
                key="governed_onboarding_tickers",
                help="Use only if a ticker still needs more history after the normal onboarding action.",
            )
            st.caption(
                f"Hydration scope: selected=`{len(get_canonical_multiselect_values(st.session_state, 'governed_onboarding_tickers'))}` | "
                f"eligible now=`{len(pending_backfill_options)}` | tickers=`{_scope_preview(get_canonical_multiselect_values(st.session_state, 'governed_onboarding_tickers'))}`"
            )
            if st.button(
                "Hydrate ticker history for onboarding",
                disabled=not bool(get_canonical_multiselect_values(st.session_state, "governed_onboarding_tickers")),
                key="run_governed_onboarding_backfill",
            ):
                try:
                    selected_onboarding_tickers = get_canonical_multiselect_values(st.session_state, "governed_onboarding_tickers")
                    started = time.perf_counter()
                    with get_conn() as conn:
                        result = run_governed_ticker_onboarding_backfill(conn, selected_onboarding_tickers)
                    updated_rows = result.get("updated_rows") or []
                    ready_rows = [row for row in updated_rows if str(row.get("history_readiness_status") or "") == "ready"]
                    pending_rows = [row for row in updated_rows if str(row.get("history_readiness_status") or "") != "ready"]
                    current_snapshot_result = result.get("current_snapshot_result") or {}
                    feedback_bits = [
                        f"Onboarding history hydration finished with status `{result.get('status')}` for {len(result.get('tickers') or [])} ticker(s).",
                        f"Ready now=`{len(ready_rows)}` | still pending=`{len(pending_rows)}`.",
                        f"Backfill states: `{_scope_preview([f'{row['ticker']}={row['backfill_status']}' for row in updated_rows])}`.",
                        (
                            "Targeted current snapshot hydration: "
                            f"status=`{current_snapshot_result.get('status') or 'unknown'}` | "
                            f"run_id=`{current_snapshot_result.get('run_id') or 'n/a'}`."
                        ),
                    ]
                    if pending_rows:
                        feedback_bits.append(
                            f"Next step: remaining tickers still need history depth before downstream reconstruction. Pending=`{_scope_preview([str(row.get('ticker')) for row in pending_rows])}`."
                        )
                    else:
                        feedback_bits.append("Next step: any ready tickers with downstream refresh still flagged can move to affected-theme reconstruction.")
                    feedback_bits.append("Current market/scanner caches were cleared; the onboarding table below reruns against refreshed state.")
                    prepare_post_mutation_refresh(
                        st.session_state,
                        "governed_onboarding_feedback",
                        level=_feedback_level_for_status(result.get("status")),
                        message=" ".join(feedback_bits) + f" Completed in {time.perf_counter() - started:.1f}s.",
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
            st.caption("Use only if history is ready but affected themes still have not refreshed.")
            st.multiselect(
                "Tickers for affected-theme reconstruction",
                options=downstream_options,
                key="governed_onboarding_reconstruction_tickers",
                help="Use only if history is ready but affected themes still have not refreshed.",
            )
            st.caption(
                f"Reconstruction scope: selected=`{len(get_canonical_multiselect_values(st.session_state, 'governed_onboarding_reconstruction_tickers'))}` | "
                f"eligible now=`{len(downstream_options)}` | tickers=`{_scope_preview(get_canonical_multiselect_values(st.session_state, 'governed_onboarding_reconstruction_tickers'))}`"
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
                    detail_parts = [f"snapshot rows written={snapshot_rows_written}", f"skipped={snapshot_rows_skipped}"]
                    if failed_tickers:
                        detail_parts.append("failed tickers=" + ", ".join(failed_tickers))
                    prepare_post_mutation_refresh(
                        st.session_state,
                        "governed_onboarding_feedback",
                        level=_feedback_level_for_status(result.get("status")),
                        message=(
                            f"Affected-theme reconstruction finished with status `{result.get('status')}` for "
                            f"{len(result.get('tickers') or [])} ticker(s): " + "; ".join(detail_parts) + ". "
                            f"Downstream refresh cleared for `{len(result.get('updated_rows') or [])}` ticker(s). "
                            "Current market/scanner caches were cleared; the onboarding table below reruns against refreshed state. "
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
                    "latest_current_snapshot_time",
                    "current_snapshot_source",
                    "has_current_usable_preferred_snapshot",
                    "propagation_status",
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
            ["Pending review", "History gaps", "Suppressed / resolved", "All"],
            index=0,
            key="symbol_hygiene_queue_view",
            help="Pending review mixes actionable suppression items with advisory history-gap reviews. History gaps isolates zero-history / stale-history names that still need manual investigation.",
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
        elif queue_view == "History gaps":
            st.caption("This view isolates governed tickers with zero stored history or stale current snapshot data so you can review mapping, delisting, retry, or manual suppression decisions without mixing in the broader suppression queue.")
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
                    history_rows = row.get("history_row_count")
                    history_rows_text = "-" if pd.isna(history_rows) else str(int(history_rows))
                    history_latest = row.get("history_latest_trading_date")
                    history_latest_text = _display_placeholder(history_latest)
                    history_focus = str(row.get("history_review_focus") or "").strip()
                    if bool(row.get("history_gap_flag")):
                        st.caption(
                            f"History gap advisory: stored rows=`{history_rows_text}` | "
                            f"latest trading date=`{history_latest_text}` | "
                            f"review focus=`{history_focus or 'review manually'}`"
                        )
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
                    if bool(row.get("history_gap_flag")):
                        if st.button(f"Open `{ticker}` in Themes lookup", key=f"open_history_gap_{ticker}"):
                            st.session_state["manage_ticker_lookup"] = ticker
                            st.switch_page("pages/1_Themes.py")
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

with themes_tab:
    render_feedback_message(st.session_state, THEME_HEALTH_FEEDBACK_KEY)

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
                        THEME_HEALTH_FEEDBACK_KEY,
                        level="success" if str(rebuild_result.get("status") or "") not in {"no_scope", "no_ticker_history", "no_reconstructed_scope", "no_history_rows", "no_op"} else "warning",
                        message=(
                            f"Queue rebuild result: status={rebuild_result.get('status')} | "
                            f"themes={len(rebuild_result.get('affected_theme_ids', []))} | "
                            f"replaced={int(rebuild_result.get('rows_replaced') or 0)} | "
                            f"written={int(rebuild_result.get('rows_written') or 0)}. "
                            "Current market caches were cleared; the audit table reruns against refreshed state."
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
                        THEME_HEALTH_FEEDBACK_KEY,
                        level=level,
                        message=(
                            f"Queue backfill result: status={backfill_result.get('status')} | "
                            f"themes={len(backfill_result.get('theme_ids', []))} | "
                            f"ticker_history_written={int(backfill_result.get('ticker_history_rows_written') or 0)} | "
                            f"snapshot_rows_written={int(backfill_result.get('snapshot_rows_written') or 0)} | "
                            f"failed_tickers={len(backfill_result.get('failed_tickers') or [])}. "
                            "Current market caches were cleared; rerun tables should now reflect any refreshed scope."
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
                            THEME_HEALTH_FEEDBACK_KEY,
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
            in_queue, queue_size = _queue_status_summary(st.session_state, theme_id, theme_name)
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
            st.caption(
                f"Audit queue status: in_queue=`{'yes' if in_queue else 'no'}` | current_queue_size=`{queue_size}`. "
                "Use the shortcuts below to add/remove/focus this theme without leaving the detail context."
            )
            if governed_members_list:
                preview = ", ".join(governed_members_list[:8])
                if len(governed_members_list) > 8:
                    preview = f"{preview}, +{len(governed_members_list) - 8} more"
                st.caption(f"Current governed members: `{preview}`")
            else:
                st.caption("Current governed members: `none`")
            detail_queue_cols = st.columns(4)
            queue_selected_clicked = detail_queue_cols[0].button(
                "Add to queue",
                key=f"health_theme_add_queue_{theme_id}",
                disabled=in_queue,
            )
            remove_from_queue_clicked = detail_queue_cols[1].button(
                "Remove from queue",
                key=f"health_theme_remove_queue_{theme_id}",
                disabled=not in_queue,
            )
            recommended_action = str(picked.get("next_action") or "").strip().lower()
            queue_recommended_label = None
            if any(token in recommended_action for token in ["reconstruct", "refresh snapshots"]):
                queue_recommended_label = "Queue recommended: rebuild"
            elif "deactivate" in recommended_action:
                queue_recommended_label = "Queue recommended: deactivate"
            elif "review failing members" in recommended_action or "inspect failures" in recommended_action:
                queue_recommended_label = "Queue recommended: backfill/rebuild"
            queue_recommended_clicked = detail_queue_cols[2].button(
                queue_recommended_label or "Queue recommended action",
                disabled=queue_recommended_label is None,
                key=f"health_theme_add_recommended_queue_{theme_id}",
            )
            queue_focus_clicked = detail_queue_cols[3].button(
                "Queue only this theme",
                key=f"health_theme_focus_queue_{theme_id}",
            )
            st.caption("Queue only this theme resets the current audit queue to this theme, which is the fastest path into queue actions above.")

            if queue_selected_clicked:
                added, label = _add_theme_to_audit_queue(st.session_state, theme_id, theme_name)
                queue_feedback_message(
                    st.session_state,
                    THEME_HEALTH_FEEDBACK_KEY,
                    level="success" if added else "warning",
                    message=f"Added `{label}` to the audit queue." if added else f"`{label}` is already in the audit queue.",
                )
                st.rerun()

            if remove_from_queue_clicked:
                removed, label = _remove_theme_from_audit_queue(st.session_state, theme_id, theme_name)
                queue_feedback_message(
                    st.session_state,
                    THEME_HEALTH_FEEDBACK_KEY,
                    level="success" if removed else "warning",
                    message=f"Removed `{label}` from the audit queue." if removed else f"`{label}` is not currently in the audit queue.",
                )
                st.rerun()

            if queue_recommended_clicked and queue_recommended_label is not None:
                added, label = _add_theme_to_audit_queue(st.session_state, theme_id, theme_name)
                queue_feedback_message(
                    st.session_state,
                    THEME_HEALTH_FEEDBACK_KEY,
                    level="success" if added else "warning",
                    message=(
                        f"Queued `{label}` for the recommended follow-up: {queue_recommended_label.replace('Queue recommended: ', '')}."
                        if added
                        else f"`{label}` is already in the audit queue."
                    ),
                )
                st.rerun()
            if queue_focus_clicked:
                label = _focus_audit_queue_on_theme(st.session_state, theme_id, theme_name)
                queue_feedback_message(
                    st.session_state,
                    THEME_HEALTH_FEEDBACK_KEY,
                    level="success",
                    message=f"Focused the audit queue on `{label}`. Queue actions above now apply to this theme only.",
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
                            THEME_HEALTH_FEEDBACK_KEY,
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
                                THEME_HEALTH_FEEDBACK_KEY,
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
                                THEME_HEALTH_FEEDBACK_KEY,
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
                            THEME_HEALTH_FEEDBACK_KEY,
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
                            THEME_HEALTH_FEEDBACK_KEY,
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
