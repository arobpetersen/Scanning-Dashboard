from datetime import datetime

import streamlit as st

from src.config import MASSIVE_API_KEY_ENV, massive_api_key
from src.database import get_bootstrap_conn, get_conn, init_db
from src.eod_refresh import run_latest_daily_sync
from src.fetch_data import running_refresh_runs
from src.queries import last_refresh_run, synthetic_data_active
from src.suggestions_service import suggestion_status_counts
from src.symbol_hygiene import refresh_eligible_tickers
from src.streamlit_utils import (
    prepare_post_mutation_refresh,
    queue_feedback_message,
    render_feedback_message,
    reset_perf_timings,
    show_perf_summary,
    stop_for_database_error,
)
from src.theme_service import active_ticker_universe, refresh_active_ticker_universe, seed_if_needed, theme_registry_counts


def _stage_status_label(status: object) -> str:
    mapping = {
        "queued": "Queued",
        "running": "Running",
        "refreshed": "Refreshed",
        "success": "Success",
        "partial": "Partial",
        "failed": "Failed",
        "already_current": "Already current",
        "skipped_non_trading_day": "Skipped: non-trading day",
        "not_run": "Not run",
        "history_repaired": "History repaired",
        "materialized": "Materialized",
        "materialized_from_run": "Materialized from run",
        "repaired_unranked_existing": "Repaired latest day",
        "repaired_intraday_same_day": "Rebuilt same-day final",
        "deferred_until_eod": "Deferred until EOD",
        "no_change": "No change",
        "no_ticker_history": "No ticker history",
        "missing": "Missing",
    }
    return mapping.get(str(status or "").strip(), str(status or "Unknown").replace("_", " ").title())


def _yes_no(value: object) -> str:
    return "Yes" if bool(value) else "No"


DAILY_SYNC_STAGE_LABELS = {
    "live_refresh": "Live refresh",
    "historical_append": "Historical append",
    "canonical_materialization": "Canonical",
}

DAILY_SYNC_STAGE_ORDER = ["live_refresh", "historical_append", "canonical_materialization"]
DAILY_SYNC_TERMINAL_SUCCESS_STATUSES = {
    "already_current",
    "success",
    "partial",
    "refreshed",
    "history_repaired",
    "materialized",
    "materialized_from_run",
    "repaired_unranked_existing",
    "repaired_intraday_same_day",
    "deferred_until_eod",
    "skipped_non_trading_day",
    "not_run",
    "no_change",
    "no_ticker_history",
    "missing",
}


def _status_timestamp_label() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _initial_running_stage_state() -> dict[str, object]:
    stages: dict[str, dict[str, object]] = {}
    for stage_key in DAILY_SYNC_STAGE_ORDER:
        stages[stage_key] = {
            "stage": stage_key,
            "stage_label": DAILY_SYNC_STAGE_LABELS[stage_key],
            "stage_status": "queued",
            "summary": "Waiting for prior stages.",
            "detail": "",
            "updated_at_label": _status_timestamp_label(),
        }
    return {
        "stage_label": "Daily sync",
        "stage_status": "running",
        "summary": "Run daily sync/finalize now: started.",
        "detail": "Stages run in order: live refresh, historical append, then canonical materialization.",
        "updated_at_label": _status_timestamp_label(),
        "active_stage": None,
        "stages": stages,
    }


def _stage_progress_index(stage_status: str) -> int:
    normalized = str(stage_status or "").strip()
    if normalized in {"running"}:
        return 1
    if normalized in DAILY_SYNC_TERMINAL_SUCCESS_STATUSES or normalized == "failed":
        return 2
    return 0


def _workflow_stage_progress(running_status: dict[str, object]) -> tuple[int, int, str]:
    stages = running_status.get("stages") or {}
    completed_stages = 0
    active_stage_idx = 1
    active_stage_label = "Live refresh"
    for idx, stage_key in enumerate(DAILY_SYNC_STAGE_ORDER, start=1):
        stage_payload = stages.get(stage_key) or {}
        stage_status = str(stage_payload.get("stage_status") or "").strip()
        progress_index = _stage_progress_index(stage_status)
        if progress_index >= 2:
            completed_stages += 1
            continue
        active_stage_idx = idx
        active_stage_label = DAILY_SYNC_STAGE_LABELS[stage_key]
        break
    else:
        active_stage_idx = len(DAILY_SYNC_STAGE_ORDER)
        active_stage_label = DAILY_SYNC_STAGE_LABELS[DAILY_SYNC_STAGE_ORDER[-1]]
    return completed_stages, active_stage_idx, active_stage_label


def _sync_status_summary(sync_result: dict[str, object]) -> tuple[str, str, str]:
    stages = sync_result.get("stages", {}) or {}
    stage_statuses = [str((stage or {}).get("status") or "") for stage in stages.values()]
    latest_expected = sync_result.get("latest_expected_trading_date_after")
    latest_canonical = sync_result.get("latest_canonical_snapshot_date_after")
    canonical_current = bool(latest_expected) and str(latest_expected) == str(latest_canonical)
    finalization_eligible = bool(sync_result.get("finalization_eligible"))
    finalization_deferred = any(status == "deferred_until_eod" for status in stage_statuses)
    has_failures = any(status == "failed" for status in stage_statuses)
    has_changes = any(status in {"refreshed", "success", "partial", "history_repaired", "materialized", "materialized_from_run", "repaired_unranked_existing", "repaired_intraday_same_day"} for status in stage_statuses)
    has_live_refresh = str(((stages.get("live_refresh") or {}).get("status") or "")) == "refreshed"

    if has_failures and not canonical_current and not has_changes:
        state = "Failed"
        level = "error"
    elif finalization_deferred and has_failures:
        state = "Intraday live refresh with failures"
        level = "warning"
    elif finalization_deferred and has_live_refresh:
        state = "Live refreshed; finalization deferred"
        level = "info"
    elif finalization_deferred:
        state = "Live current; finalization deferred"
        level = "info"
    elif has_failures:
        state = "Partial sync"
        level = "warning"
    elif str(sync_result.get("status") or "") == "no_change":
        state = "Already current"
        level = "info"
    elif canonical_current:
        state = "Current through latest trading day"
        level = "success"
    elif has_changes:
        state = "Partial sync"
        level = "warning"
    else:
        state = "Failed"
        level = "error"

    detail = (
        f"Expected `{latest_expected or '-'}` | "
        f"Historical `{sync_result.get('latest_historical_snapshot_date_after') or '-'}` | "
        f"Canonical `{latest_canonical or '-'}`"
    )
    if finalization_deferred:
        detail = f"{detail} | Finalization `deferred until EOD window`"
    elif not finalization_eligible and sync_result.get("finalization_target_date"):
        detail = f"{detail} | Finalized target `{sync_result.get('finalization_target_date')}`"
    return state, level, detail


def _render_daily_sync_status(container, sync_result: dict[str, object]) -> None:
    stages = sync_result.get("stages", {}) or {}
    live_stage = stages.get("live_refresh", {}) or {}
    append_stage = stages.get("historical_append", {}) or {}
    canonical_stage = stages.get("canonical_materialization", {}) or {}
    live_summary = live_stage.get("refresh_run_summary", {}) or {}
    final_state, level, detail = _sync_status_summary(sync_result)

    container.markdown("**Latest daily sync/finalize status**")
    summary_line = f"{final_state}."
    if level == "success":
        container.success(summary_line)
    elif level == "warning":
        container.warning(summary_line)
    elif level == "info":
        container.info(summary_line)
    else:
        container.error(summary_line)
    container.caption(detail)

    live_col, append_col, canonical_col = container.columns(3)
    with live_col:
        st.markdown("**Live refresh**")
        st.caption(_stage_status_label(live_stage.get("status")))
        if live_stage.get("scope_type") == "daily_sync_live":
            st.caption("Intraday live-only refresh")
        st.write(f"Requested: `{int(live_summary.get('tickers_requested') or 0)}`")
        st.write(f"Updated/passed: `{int(live_summary.get('tickers_updated') or 0)}`")
        st.write(f"Failed: `{int(live_summary.get('tickers_failed') or 0)}`")
        st.write(f"Suppressed: `{int(live_summary.get('tickers_suppressed') or 0)}`")
        if live_stage.get("run_id"):
            st.caption(f"Run #{int(live_stage['run_id'])} | API calls `{int(live_summary.get('api_call_count') or 0)}`")

    with append_col:
        st.markdown("**Historical append**")
        st.caption(_stage_status_label(append_stage.get("status")))
        if append_stage.get("target_date"):
            st.caption(f"Target date `{append_stage.get('target_date')}`")
        st.write(f"Ticker rows written: `{int(append_stage.get('ticker_history_rows_written') or 0)}`")
        st.write(f"Theme rows written: `{int(append_stage.get('snapshot_rows_written') or 0)}`")
        st.write(f"Failed tickers: `{len(list(append_stage.get('failed_tickers') or []))}`")
        st.write(f"Historical current for target day: `{_yes_no(append_stage.get('historical_current_for_target_day'))}`")
        if append_stage.get("same_day_repair_performed"):
            st.write("Same-day rebuild: `Yes`")
        if append_stage.get("reused_existing_same_day_state"):
            st.write("Reused existing same-day state: `Yes`")
        st.caption(f"Latest historical day `{append_stage.get('latest_historical_snapshot_date_after') or '-'}`")

    with canonical_col:
        st.markdown("**Canonical**")
        st.caption(_stage_status_label(canonical_stage.get("status")))
        if sync_result.get("finalization_target_date"):
            st.caption(f"Target date `{sync_result.get('finalization_target_date')}`")
        st.write(f"Latest expected day: `{canonical_stage.get('latest_expected_trading_date') or sync_result.get('latest_expected_trading_date_after') or '-'}`")
        st.write(f"Latest canonical day: `{canonical_stage.get('latest_canonical_snapshot_date_after') or '-'}`")
        st.write(f"Inserted rows: `{int(canonical_stage.get('inserted_count') or 0)}`")
        st.write(f"Ranked rows: `{int(canonical_stage.get('ranked_row_count') or 0)}`")
        if canonical_stage.get("same_day_repair_performed"):
            st.write("Same-day rebuild: `Yes`")
        st.write(
            f"Canonical now current: `{_yes_no((canonical_stage.get('latest_expected_trading_date') or sync_result.get('latest_expected_trading_date_after')) and str(canonical_stage.get('latest_expected_trading_date') or sync_result.get('latest_expected_trading_date_after')) == str(canonical_stage.get('latest_canonical_snapshot_date_after') or sync_result.get('latest_canonical_snapshot_date_after') or ''))}`"
        )

    live_failed = list(live_summary.get("failed_tickers") or [])
    append_failed = list(append_stage.get("failed_tickers") or [])
    if live_failed or append_failed:
        with container.expander("Failure detail", expanded=False):
            if live_failed:
                st.write(f"Live refresh failed tickers: {', '.join(live_failed)}")
            if append_failed:
                st.write(f"Historical append failed tickers: {', '.join(append_failed)}")
    if canonical_stage.get("error"):
        container.caption(f"Canonical note: {canonical_stage.get('error')}")


def _render_daily_sync_running_status(container, running_status: dict[str, object]) -> None:
    container.markdown("**Latest daily sync/finalize status**")
    stage_label = str(running_status.get("stage_label") or "Daily sync")
    stage_status = str(running_status.get("stage_status") or "running")
    summary = str(running_status.get("summary") or "Run daily sync/finalize now: started.")
    message = f"{stage_label}: {summary}"
    if stage_status in {"failed"}:
        container.error(message)
    elif stage_status in {"already_current", "success", "refreshed", "partial"}:
        container.success(message)
    else:
        container.info(message)
    detail = str(running_status.get("detail") or "").strip()
    if detail:
        container.caption(detail)
    updated_at = str(running_status.get("updated_at_label") or "").strip()
    active_stage = str(running_status.get("active_stage") or "").strip()
    if updated_at:
        last_update_text = f"Last update `{updated_at}`"
        if active_stage and active_stage in DAILY_SYNC_STAGE_LABELS:
            last_update_text = f"{last_update_text} | Active stage `{DAILY_SYNC_STAGE_LABELS[active_stage]}`"
        container.caption(last_update_text)

    stages = running_status.get("stages") or {}
    completed_stages, _active_stage_idx, active_stage_label = _workflow_stage_progress(running_status)
    active_payload = stages.get(active_stage) if active_stage else None
    progress_source = active_payload if active_payload else running_status
    live_stage_payload = stages.get("live_refresh") or {}
    live_total = int(live_stage_payload.get("total") or 0)
    live_completed = int(live_stage_payload.get("completed") or 0)

    workflow_c1, workflow_c2, workflow_c3 = container.columns(3)
    workflow_c1.metric("Stages complete", f"{completed_stages}/{len(DAILY_SYNC_STAGE_ORDER)}")
    workflow_c2.metric("Active stage", active_stage_label)
    workflow_c3.metric("Run alive", "Yes")

    stage_cols = container.columns(3)
    for idx, stage_key in enumerate(DAILY_SYNC_STAGE_ORDER):
        stage_payload = stages.get(stage_key) or {}
        with stage_cols[idx]:
            st.markdown(f"**{DAILY_SYNC_STAGE_LABELS[stage_key]}**")
            st.caption(_stage_status_label(stage_payload.get("stage_status")))
            stage_summary = str(stage_payload.get("summary") or "").strip()
            stage_detail = str(stage_payload.get("detail") or "").strip()
            stage_updated_at = str(stage_payload.get("updated_at_label") or "").strip()
            if stage_summary:
                st.write(stage_summary)
            if stage_detail:
                st.caption(stage_detail)
            if stage_updated_at:
                st.caption(f"Updated `{stage_updated_at}`")
            stage_total = int(stage_payload.get("total") or 0)
            stage_completed = int(stage_payload.get("completed") or 0)
            if stage_key == "live_refresh" and stage_total > 0:
                stage_fraction = min(max(stage_completed / max(stage_total, 1), 0.0), 1.0)
                st.caption(f"Live completion: `{stage_fraction * 100:.0f}%`")
            elif stage_key == "historical_append" and str(stage_payload.get("stage_status") or "") == "running":
                st.caption("Historical append progress: stage-state only until row totals are finalized.")
            elif stage_key == "canonical_materialization" and str(stage_payload.get("stage_status") or "") == "running":
                st.caption("Canonical progress: stage-state only until materialization finishes.")
    total = int(progress_source.get("total") or 0)
    completed = int(progress_source.get("completed") or 0)
    if total > 0:
        progress_value = min(max(completed / max(total, 1), 0.0), 1.0)
        container.progress(progress_value, text=f"{completed}/{total} tickers completed")
        c1, c2, c3, c4 = container.columns(4)
        c1.metric("Completed", completed)
        c2.metric("Succeeded", int(progress_source.get("success") or 0))
        c3.metric("Failed", int(progress_source.get("failure") or 0))
        c4.metric("API hits", int(progress_source.get("api_call_count") or 0))
        current_ticker = str(progress_source.get("current_ticker") or "").strip()
        if current_ticker:
            container.caption(f"Current ticker: `{current_ticker}`")
        endpoint_counts = progress_source.get("api_endpoint_counts") or {}
        if isinstance(endpoint_counts, dict) and endpoint_counts:
            endpoint_text = " | ".join(
                f"{str(name).replace('_', ' ')}=`{int(count or 0)}`"
                for name, count in sorted(endpoint_counts.items())
            )
            container.caption(f"Endpoint hits: {endpoint_text}")
    elif progress_source.get("api_call_count") is not None:
        container.caption(f"API hits so far: `{int(progress_source.get('api_call_count') or 0)}`")


st.set_page_config(page_title="Theme Ops Dashboard", layout="wide")
st.title("Theme Operations Dashboard")
st.caption("Control center for daily sync/finalization, refresh, rankings, review queue, and health signals.")
reset_perf_timings("app")

try:
    init_db()
    with get_conn() as conn:
        seeded = seed_if_needed(conn)
        registry_counts = theme_registry_counts(conn)
except Exception as exc:
    stop_for_database_error(exc)

if seeded:
    st.success("Theme registry imported from themes_seed_structured.json. DuckDB is source of truth.")

render_feedback_message(st.session_state, "app_refresh_feedback")
if st.session_state.get("app_daily_sync_running"):
    _render_daily_sync_running_status(st, st.session_state["app_daily_sync_running"])
elif st.session_state.get("app_daily_sync_status"):
    _render_daily_sync_status(st, st.session_state["app_daily_sync_status"])

try:
    with get_conn() as conn:
        requested_tickers = active_ticker_universe(conn)
        resolved_tickers = refresh_active_ticker_universe(conn)
        eligible_tickers, _suppressed_scope_tickers = refresh_eligible_tickers(conn, requested_tickers)
        last_run = last_refresh_run(conn)
        running_runs = running_refresh_runs(conn)
        sugg_counts = suggestion_status_counts(conn)
        synthetic_active = synthetic_data_active(conn)
except Exception as exc:
    stop_for_database_error(exc)

live_configured = bool(massive_api_key())
if not live_configured:
    st.error(f"Live refresh is unavailable until `{MASSIVE_API_KEY_ENV}` is configured.")

if synthetic_active:
    st.info("Synthetic historical data active")

pending = int(sugg_counts[sugg_counts["status"] == "pending"]["cnt"].sum()) if not sugg_counts.empty else 0
obsolete = int(sugg_counts[sugg_counts["status"] == "obsolete"]["cnt"].sum()) if not sugg_counts.empty else 0

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Themes", int(registry_counts["themes_count"]))
m2.metric("Active themes", int(registry_counts["active_themes_count"]))
m3.metric("Pending suggestions", pending)
m4.metric("Obsolete suggestions", obsolete)
m5.metric("Refresh-eligible tickers", len(eligible_tickers))

st.subheader("Refresh Control")
st.caption("Apps owns daily sync/finalization. Run the authoritative refresh here, review the status, then use the rest of the app against the refreshed DB state.")
if not running_runs.empty:
    active_run = running_runs.iloc[0]
    stale_note = " likely stale" if bool(active_run.get("likely_stale")) else ""
    st.warning(
        f"Refresh run #{int(active_run['run_id'])} is still marked `running`{stale_note}. "
        "The daily sync stays blocked until it finishes or is cleared from Health > Refresh history."
    )
if st.button("Run daily sync/finalize now", type="primary", disabled=not live_configured, key="app_run_daily_sync"):
    st.session_state["app_daily_sync_running"] = _initial_running_stage_state()
    running_status_slot = st.empty()
    with running_status_slot.container():
        _render_daily_sync_running_status(st, st.session_state["app_daily_sync_running"])
    try:
        def _daily_sync_progress(payload: dict[str, object]) -> None:
            status = dict(st.session_state.get("app_daily_sync_running") or {})
            payload = dict(payload or {})
            stage_key = str(payload.get("stage") or "").strip()
            stages = dict(status.get("stages") or {})
            timestamp_label = _status_timestamp_label()
            if stage_key:
                stage_state = dict(stages.get(stage_key) or {})
                stage_state.update(payload)
                stage_state["updated_at_label"] = timestamp_label
                stages[stage_key] = stage_state
                status["stages"] = stages
                status["active_stage"] = stage_key if str(stage_state.get("stage_status") or "") == "running" else status.get("active_stage")
                if str(stage_state.get("stage_status") or "") != "running" and status.get("active_stage") == stage_key:
                    status["active_stage"] = None
            status.update(
                {
                    "stage_label": payload.get("stage_label") or status.get("stage_label"),
                    "stage_status": payload.get("stage_status") or status.get("stage_status"),
                    "summary": payload.get("summary") or status.get("summary"),
                    "detail": payload.get("detail") or status.get("detail"),
                    "updated_at_label": timestamp_label,
                }
            )
            st.session_state["app_daily_sync_running"] = status
            with running_status_slot.container():
                _render_daily_sync_running_status(st, status)

        with get_bootstrap_conn() as conn:
            sync_result = run_latest_daily_sync(conn, provider_name="live", progress_callback=_daily_sync_progress)
        st.session_state["app_daily_sync_status"] = sync_result
        st.session_state.pop("app_daily_sync_running", None)
        final_state, feedback_level, detail = _sync_status_summary(sync_result)
        prepare_post_mutation_refresh(
            st.session_state,
            "app_refresh_feedback",
            level=feedback_level,
            message=f"{final_state}. {detail}. Caches were cleared and the app reran against refreshed DB state.",
            clear_market=True,
        )
        st.rerun()
    except Exception as exc:
        st.session_state.pop("app_daily_sync_running", None)
        queue_feedback_message(
            st.session_state,
            "app_refresh_feedback",
            level="error",
            message=(
                "Run daily sync/finalize now failed before completion. "
                f"No full-current guarantee: {exc}"
            ),
        )
        st.rerun()
if not last_run.empty:
    run = last_run.iloc[0]
    with st.expander("Recent run note", expanded=False):
        st.caption(
            f"Last run #{int(run['run_id'])} | status=`{run['status']}` | "
            f"success=`{int(run.get('success_count') or 0)}` | failures=`{int(run.get('failure_count') or 0)}`"
        )

st.caption("Apps is the authoritative sync/control page. Themes, Historical Performance, Suggestions, and Health should reflect stored state after refresh.")
show_perf_summary()
