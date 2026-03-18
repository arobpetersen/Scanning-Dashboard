import streamlit as st

import html

from src.ai_proposals import generate_ai_suggestions, sanitize_context
from src.config import (
    AI_MAX_PROPOSALS,
    OPENAI_API_KEY_ENV,
    TC2000_DEFAULT_SOURCE_LABEL,
    TC2000_FILE_GLOB,
    TC2000_IMPORT_DIR,
    openai_api_key,
)
from src.database import get_conn, init_db
from src.rules_engine import run_rules_engine
from src.scanner_audit import (
    apply_scanner_candidate_selected_themes,
    import_tc2000_exports,
    promote_scanner_candidate_to_theme_review,
    recent_scanner_import_runs,
    reset_scanner_audit_data,
    scanner_import_overview,
    send_preserved_applied_scanner_audit_theme_to_review,
    set_scanner_candidate_review_state,
)
from src.scanner_research import (
    get_or_create_scanner_research_draft,
    get_scanner_research_review,
    save_scanner_research_review,
    scanner_research_review_summary,
)
from src.suggestions_page_state import (
    apply_generated_theme_idea_checkbox_selection,
    build_scanner_research_debug_entry,
    finalize_possible_new_theme_category_state,
    finalize_possible_new_theme_state,
    has_meaningful_theme_review_selection,
    join_possible_new_theme_ideas,
    normalize_theme_id_list,
    prepare_possible_new_theme_category_prefill,
    prepare_possible_new_theme_prefill,
    reconcile_possible_new_theme_from_generated_checkbox_state,
    resolve_active_suggestions_tab,
    resolve_scanner_audit_ticker,
    split_possible_new_theme_ideas,
    sync_generated_theme_idea_checkbox_state,
    split_selected_existing_theme_ids,
    sync_suggested_theme_checkbox_state,
)
from src.streamlit_utils import (
    clear_scanner_candidate_summary_cache,
    clear_current_market_view_caches,
    db_cache_token,
    load_scanner_candidate_summary_cached,
    load_theme_health_overview_cached,
    load_theme_rankings_cached,
    render_dataframe,
    reset_perf_timings,
    show_perf_summary,
    stop_for_database_error,
)
from src.suggestions_service import (
    SuggestionPayload,
    apply_suggestion,
    bulk_update_filtered_status,
    can_apply_queue_suggestion_row,
    can_fast_path_create_governed_theme_row,
    count_filtered_suggestions,
    create_suggestion,
    fast_path_create_governed_theme_and_assign_ticker,
    list_suggestions,
    recent_applied_suggestions,
    review_suggestion,
    suggestion_status_counts,
    can_follow_up_applied_scanner_audit_review_row,
    update_suggestion_status,
)
from src.theme_service import get_theme_members, list_themes, seed_if_needed

st.set_page_config(page_title="Suggestions", layout="wide")
st.title("Suggestions")
reset_perf_timings("suggestions")
research_feedback = st.session_state.pop("scanner_research_feedback", None)
if research_feedback:
    level = str(research_feedback.get("level") or "info")
    message = str(research_feedback.get("message") or "")
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    else:
        st.error(message)

feedback = st.session_state.pop("suggestions_feedback", None)
if feedback:
    level = str(feedback.get("level") or "info")
    message = str(feedback.get("message") or "")
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    else:
        st.error(message)


def _render_ticker_membership_context(row) -> None:
    ticker = str(row.get("proposed_ticker") or "").strip().upper()
    if not ticker:
        return

    context = str(row.get("current_membership_context") or "").strip()
    themes = str(row.get("current_theme_names") or "").strip()
    categories = str(row.get("current_categories") or "").strip()

    with st.container(border=True):
        st.write(f"**Ticker context: `{ticker}`**")
        if context:
            st.caption(f"In themes: {context}")
        else:
            st.caption("Not currently assigned to any theme.")
        if themes:
            st.write(f"Themes: `{themes}`")
        if categories:
            st.write(f"Categories: `{categories}`")


def _render_structured_review_context(row) -> None:
    selected_existing = str(row.get("selected_existing_theme_names") or "").strip()
    custom_new = str(row.get("custom_new_theme_names") or "").strip()
    proposed_category = str(row.get("proposed_new_theme_category") or row.get("proposed_theme_category") or "").strip()
    promotion_note = str(row.get("promotion_note") or "").strip()
    scanner_recommendation = str(row.get("scanner_audit_recommendation") or "").strip()
    scanner_reason = str(row.get("scanner_audit_reason") or "").strip()
    research_summary = str(row.get("research_summary") or "").strip()
    if not any([selected_existing, custom_new, proposed_category, promotion_note, scanner_recommendation, research_summary]):
        return

    with st.container(border=True):
        st.write("**Structured Review Context**")
        if selected_existing:
            st.write(f"Selected existing themes: `{selected_existing}`")
        if custom_new:
            st.write(f"Custom new themes: `{custom_new}`")
        if proposed_category:
            st.write(f"Proposed category: `{proposed_category}`")
        if promotion_note:
            st.write(f"Promotion note: {promotion_note}")
        if scanner_recommendation or scanner_reason:
            st.caption(
                "Scanner Audit: "
                + " | ".join(part for part in [scanner_recommendation, scanner_reason] if part)
            )
        if research_summary:
            st.caption(f"Research summary: {research_summary}")


def _queue_selection_label(row) -> str:
    suggestion_id = int(row.get("suggestion_id") or 0)
    status = str(row.get("status") or "").strip() or "unknown"
    suggestion_type = str(row.get("suggestion_type") or "").strip() or "n/a"
    ticker = str(row.get("proposed_ticker") or "").strip().upper()
    proposed_theme_name = str(row.get("proposed_theme_name") or "").strip()
    context_label = ticker or proposed_theme_name or str(row.get("existing_theme_name") or "").strip() or "no context"
    return f"#{suggestion_id} | {status} | {suggestion_type} | {context_label}"


def _queue_visibility_note(status_filter: str, new_status: str) -> str:
    if status_filter not in {"all", new_status}:
        return f" It will disappear on rerun if your current filter excludes {new_status} items."
    return " It remains visible because the current filter still includes that status."


def _render_queue_detail_panel(row) -> None:
    with st.container(border=True):
        st.write(
            f"**Suggestion #{int(row['suggestion_id'])}** "
            f"`{row.get('suggestion_type')}` from `{row.get('source')}`"
        )
        meta_parts = [
            f"status=`{row.get('status')}`",
            f"priority=`{row.get('priority')}`",
            f"validation=`{row.get('validation_status')}`",
        ]
        st.caption(" | ".join(meta_parts))
        if str(row.get("proposed_ticker") or "").strip():
            st.write(f"Ticker: `{str(row.get('proposed_ticker') or '').strip().upper()}`")
        proposed_theme = str(row.get("custom_new_theme_names") or row.get("proposed_theme_name") or "").strip()
        proposed_category = str(row.get("proposed_new_theme_category") or row.get("proposed_theme_category") or "").strip()
        selected_existing = str(row.get("selected_existing_theme_names") or row.get("existing_theme_name") or "").strip()
        if selected_existing:
            st.write(f"Selected existing themes: `{selected_existing}`")
        if proposed_theme:
            st.write(f"Proposed new themes: `{proposed_theme}`")
        if proposed_category:
            st.write(f"Proposed category: `{proposed_category}`")
        if str(row.get("rationale") or "").strip():
            st.write(f"Rationale: {str(row.get('rationale') or '').strip()}")
        if str(row.get("reviewer_notes") or "").strip():
            st.write(f"Reviewer notes: {str(row.get('reviewer_notes') or '').strip()}")
        if str(row.get("proposed_ticker") or "").strip():
            _render_ticker_membership_context(row)
        _render_structured_review_context(row)


def _sync_selected_existing_from_suggested_checkbox(
    selected_existing_key: str,
    checkbox_key: str,
    theme_id: int,
    valid_ids: set[int],
) -> None:
    selected_existing = normalize_theme_id_list(st.session_state.get(selected_existing_key, []), valid_ids)
    checked = bool(st.session_state.get(checkbox_key))
    if checked:
        if theme_id not in selected_existing:
            selected_existing.append(theme_id)
    else:
        selected_existing = [value for value in selected_existing if value != theme_id]
    st.session_state[selected_existing_key] = normalize_theme_id_list(selected_existing, valid_ids)


def _sync_possible_new_theme_from_generated_checkboxes(
    custom_new_key: str,
    custom_new_state_key: str,
    generated_ideas: list[str],
    checkbox_keys: dict[str, str],
) -> None:
    checked_generated = [
        idea
        for idea in generated_ideas
        if bool(st.session_state.get(checkbox_keys.get(idea, ""), False))
    ]
    updated_value, updated_state = apply_generated_theme_idea_checkbox_selection(
        st.session_state.get(custom_new_key),
        checked_generated,
        generated_ideas,
        st.session_state.get(custom_new_state_key),
    )
    st.session_state[custom_new_key] = updated_value
    st.session_state[custom_new_state_key] = updated_state


def _render_compact_checkbox_content_row(
    checkbox_key: str,
    *,
    checkbox_label: str,
    title: str,
    detail: str = "",
    on_change=None,
    args: tuple = (),
) -> None:
    checkbox_col, content_col, spacer_col = st.columns([0.03, 0.32, 0.65], gap="small", vertical_alignment="top")
    with checkbox_col:
        st.checkbox(
            checkbox_label,
            key=checkbox_key,
            label_visibility="collapsed",
            on_change=on_change,
            args=args,
        )
    with content_col:
        title_html = f"<div class='scanner-checkbox-row-title'>{html.escape(str(title or ''))}</div>"
        detail_html = ""
        if detail:
            detail_html = f"<div class='scanner-checkbox-row-detail'>{html.escape(str(detail))}</div>"
        st.markdown(
            f"<div class='scanner-checkbox-row'><div class='scanner-checkbox-row-content scanner-checkbox-text'>{title_html}{detail_html}</div></div>",
            unsafe_allow_html=True,
        )
    with spacer_col:
        st.empty()

try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        themes = list_themes(conn, active_only=False)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()

theme_options = themes[["id", "name"]].to_dict("records")
theme_option_by_id = {int(row["id"]): row for row in theme_options}

suggestions_tab_options = ["Manual", "Queue", "Rules", "AI", "Scanner Audit"]
st.session_state["suggestions_active_tab"] = resolve_active_suggestions_tab(
    st.session_state.get("suggestions_active_tab"),
    suggestions_tab_options,
    "Manual",
)
active_suggestions_tab = st.radio(
    "Suggestions section",
    suggestions_tab_options,
    horizontal=True,
    key="suggestions_active_tab",
    label_visibility="collapsed",
)

if active_suggestions_tab == "Manual":
    suggestion_type = st.selectbox("Suggestion type", ["add_ticker_to_theme", "remove_ticker_from_theme", "create_theme", "rename_theme", "move_ticker_between_themes", "review_theme"])
    source = st.selectbox("Source", ["manual", "rules_engine", "ai_proposal", "imported", "scanner_audit"], index=0)
    priority = st.selectbox("Priority", ["low", "medium", "high"], index=1)
    rationale = st.text_area("Rationale", value="")

    selected_existing_theme = None
    selected_target_theme = None
    proposed_theme_name = None
    proposed_ticker = None
    current_members: list[str] = []

    if suggestion_type in {"add_ticker_to_theme", "remove_ticker_from_theme", "rename_theme", "move_ticker_between_themes", "review_theme"}:
        selected_existing_theme = st.selectbox("Existing theme", options=theme_options, format_func=lambda t: f"{t['name']} [{t['id']}]", key="manual_existing")
        with get_conn() as conn:
            current_members = get_theme_members(conn, int(selected_existing_theme["id"]))["ticker"].tolist()

    if suggestion_type == "add_ticker_to_theme":
        proposed_ticker = st.text_input("Ticker to add", value="")
    if suggestion_type == "remove_ticker_from_theme" and current_members:
        proposed_ticker = st.selectbox("Ticker to remove", options=current_members)
    if suggestion_type == "move_ticker_between_themes":
        if current_members:
            proposed_ticker = st.selectbox("Ticker to move", options=current_members)
        selected_target_theme = st.selectbox("Target theme", options=theme_options, format_func=lambda t: f"{t['name']} [{t['id']}]", key="manual_target")
    if suggestion_type in {"create_theme", "rename_theme"}:
        proposed_theme_name = st.text_input("Proposed theme name", value="")
    if suggestion_type == "review_theme":
        proposed_ticker = st.text_input("Ticker context (optional)", value="")

    if st.button("Create manual suggestion"):
        try:
            payload = SuggestionPayload(
                suggestion_type=suggestion_type,
                source=source,
                priority=priority,
                rationale=rationale,
                proposed_theme_name=proposed_theme_name,
                proposed_ticker=proposed_ticker,
                existing_theme_id=int(selected_existing_theme["id"]) if selected_existing_theme else None,
                proposed_target_theme_id=int(selected_target_theme["id"]) if selected_target_theme else None,
            )
            with get_conn() as conn:
                sid = create_suggestion(conn, payload)
            st.success(f"Created suggestion #{sid}")
            st.rerun()
        except Exception as exc:
            st.error(f"Create failed: {exc}")

if active_suggestions_tab == "Queue":
    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        status_filter = st.selectbox("Status", ["all", "pending", "approved", "rejected", "applied", "obsolete"], index=0)
    with fc2:
        type_filter = st.selectbox("Type", ["all", "add_ticker_to_theme", "remove_ticker_from_theme", "create_theme", "rename_theme", "move_ticker_between_themes", "review_theme"], index=0)
    with fc3:
        source_filter = st.selectbox("Source", ["all", "manual", "rules_engine", "ai_proposal", "imported", "scanner_audit"], index=0)
    with fc4:
        search_filter = st.text_input("Search", value="")

    with get_conn() as conn:
        queue = list_suggestions(conn, status_filter, type_filter, source_filter, search_filter)
        counts = suggestion_status_counts(conn)
        recent_applied = recent_applied_suggestions(conn, limit=10)
        cleanup_count = count_filtered_suggestions(conn, status_filter, type_filter, source_filter, search_filter, statuses_subset=["pending", "approved", "rejected"])

    cmap = {row["status"]: int(row["cnt"]) for _, row in counts.iterrows()}
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Pending", cmap.get("pending", 0))
    m2.metric("Approved", cmap.get("approved", 0))
    m3.metric("Rejected", cmap.get("rejected", 0))
    m4.metric("Applied", cmap.get("applied", 0))
    m5.metric("Obsolete", cmap.get("obsolete", 0))

    st.caption(f"Filtered bulk actions affect {cleanup_count} suggestion(s) (pending/approved/rejected).")
    notes = st.text_input("Bulk notes", value="Marked obsolete via filtered cleanup")
    confirm = st.checkbox("Confirm bulk action on filtered queue")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Mark filtered obsolete") and confirm:
            with get_conn() as conn:
                changed = bulk_update_filtered_status(conn, "obsolete", notes, status_filter, type_filter, source_filter, search_filter, ["pending", "approved", "rejected"])
            st.success(f"Marked {changed} obsolete")
            st.rerun()
    with c2:
        if st.button("Bulk reject filtered") and confirm:
            with get_conn() as conn:
                changed = bulk_update_filtered_status(conn, "rejected", notes, status_filter, type_filter, source_filter, search_filter, ["pending", "approved"])
            st.success(f"Rejected {changed}")
            st.rerun()

    if queue.empty:
        st.info("No queue rows for filters.")
    else:
        render_dataframe(
            "suggestions_queue",
            queue[["suggestion_id", "status", "priority", "validation_status", "suggestion_type", "source", "proposed_ticker", "existing_theme_name", "proposed_theme_name", "rationale", "created_at", "reviewer_notes"]],
            width="stretch",
        )
        queue_rows = queue.to_dict("records")
        queue_row_by_id = {int(row["suggestion_id"]): row for row in queue_rows}
        selected_queue_id = st.selectbox(
            "Selected suggestion",
            options=list(queue_row_by_id.keys()),
            format_func=lambda suggestion_id: _queue_selection_label(queue_row_by_id[int(suggestion_id)]),
            key="queue_selected_suggestion_id",
        )
        selected_row = queue_row_by_id[int(selected_queue_id)]
        _render_queue_detail_panel(selected_row)

        action_notes = st.text_input(
            "Row action notes",
            value=str(selected_row.get("reviewer_notes") or ""),
            key=f"queue_action_notes_{int(selected_queue_id)}",
        )
        selected_status = str(selected_row.get("status") or "").strip().lower()
        action_c1, action_c2, action_c3 = st.columns(3)
        if selected_status in {"pending", "approved"}:
            with action_c1:
                if st.button("Approve", key=f"queue_approve_{int(selected_queue_id)}"):
                    try:
                        with get_conn() as conn:
                            result = review_suggestion(conn, int(selected_queue_id), "approved", action_notes)
                        if bool(result.get("changed")):
                            st.session_state["suggestions_feedback"] = {
                                "level": "success",
                                "message": f"{result['message']}{_queue_visibility_note(status_filter, 'approved')}",
                            }
                        else:
                            st.session_state["suggestions_feedback"] = {
                                "level": "warning",
                                "message": str(result.get("message") or f"Suggestion #{selected_queue_id} is already approved."),
                            }
                    except Exception as exc:
                        st.session_state["suggestions_feedback"] = {
                            "level": "error",
                            "message": f"Approve failed: {exc}",
                        }
                    st.rerun()
            with action_c2:
                if st.button("Reject", key=f"queue_reject_{int(selected_queue_id)}"):
                    try:
                        with get_conn() as conn:
                            result = review_suggestion(conn, int(selected_queue_id), "rejected", action_notes)
                        if bool(result.get("changed")):
                            st.session_state["suggestions_feedback"] = {
                                "level": "success",
                                "message": f"{result['message']}{_queue_visibility_note(status_filter, 'rejected')}",
                            }
                        else:
                            st.session_state["suggestions_feedback"] = {
                                "level": "warning",
                                "message": str(result.get("message") or f"Suggestion #{selected_queue_id} is already rejected."),
                            }
                    except Exception as exc:
                        st.session_state["suggestions_feedback"] = {
                            "level": "error",
                            "message": f"Reject failed: {exc}",
                        }
                    st.rerun()
        if selected_status in {"pending", "approved", "rejected"}:
            with action_c3:
                if st.button("Mark obsolete", key=f"queue_obsolete_{int(selected_queue_id)}"):
                    try:
                        with get_conn() as conn:
                            result = update_suggestion_status(conn, int(selected_queue_id), "obsolete", action_notes)
                        if bool(result.get("changed")):
                            st.session_state["suggestions_feedback"] = {
                                "level": "success",
                                "message": f"{result['message']}{_queue_visibility_note(status_filter, 'obsolete')}",
                            }
                        else:
                            st.session_state["suggestions_feedback"] = {
                                "level": "warning",
                                "message": str(result.get("message") or f"Suggestion #{selected_queue_id} is already obsolete."),
                            }
                    except Exception as exc:
                        st.session_state["suggestions_feedback"] = {
                            "level": "error",
                            "message": f"Mark obsolete failed: {exc}",
                        }
                    st.rerun()
        if selected_status == "approved" and can_apply_queue_suggestion_row(selected_row):
            apply_notes = st.text_input(
                "Apply notes",
                value="",
                key=f"queue_apply_notes_{int(selected_queue_id)}",
            )
            if st.button("Apply approved", key=f"queue_apply_{int(selected_queue_id)}"):
                try:
                    with get_conn() as conn:
                        apply_suggestion(conn, int(selected_queue_id), apply_notes)
                    st.session_state["suggestions_feedback"] = {
                        "level": "success",
                        "message": f"Suggestion #{int(selected_queue_id)} applied.{_queue_visibility_note(status_filter, 'applied')}",
                    }
                except Exception as exc:
                    st.session_state["suggestions_feedback"] = {
                        "level": "error",
                        "message": f"Apply failed: {exc}",
                    }
                st.rerun()
        elif selected_status == "approved" and str(selected_row.get("suggestion_type") or "").strip().lower() == "review_theme":
            st.caption(
                "This approved review item only contains proposed new-theme context and no governed theme selection, "
                "so there is nothing governed to apply to membership yet."
            )
        if can_fast_path_create_governed_theme_row(selected_row):
            fast_path_notes = st.text_input(
                "Fast-path notes",
                value="",
                key=f"queue_fast_path_notes_{int(selected_queue_id)}",
            )
            if st.button(
                "Create governed theme and assign ticker",
                key=f"queue_fast_path_create_theme_{int(selected_queue_id)}",
            ):
                try:
                    with get_conn() as conn:
                        result = fast_path_create_governed_theme_and_assign_ticker(conn, int(selected_queue_id), fast_path_notes)
                    clear_scanner_candidate_summary_cache()
                    clear_current_market_view_caches()
                    st.session_state["suggestions_feedback"] = {
                        "level": "success",
                        "message": f"{result['message']}{_queue_visibility_note(status_filter, 'applied')}",
                    }
                except Exception as exc:
                    st.session_state["suggestions_feedback"] = {
                        "level": "error",
                        "message": f"Fast-path theme creation failed: {exc}",
                    }
                st.rerun()
        if can_follow_up_applied_scanner_audit_review_row(selected_row):
            st.caption("This applied Scanner Audit row still has preserved proposed new-theme context that can be staged back into Theme Review.")
            if st.button(
                "Send preserved proposed theme to Theme Review",
                key=f"queue_follow_up_theme_review_{int(selected_queue_id)}",
            ):
                try:
                    with get_conn() as conn:
                        result = send_preserved_applied_scanner_audit_theme_to_review(conn, int(selected_queue_id))
                    st.session_state["suggestions_feedback"] = {
                        "level": "success",
                        "message": (
                            f"{result['message']}"
                            + _queue_visibility_note(status_filter, "pending")
                        ),
                    }
                except Exception as exc:
                    st.session_state["suggestions_feedback"] = {
                        "level": "error",
                        "message": f"Could not send preserved proposed theme to Theme Review. {exc}",
                    }
                st.rerun()

    if not recent_applied.empty:
        st.subheader("Recently applied")
        render_dataframe("suggestions_recent_applied", recent_applied, width="stretch")

if active_suggestions_tab == "Rules":
    if st.button("Run deterministic rules engine", type="primary"):
        with get_conn() as conn:
            summary = run_rules_engine(conn)
        st.success(f"Rules run: created={summary['created']} evaluated={summary['evaluated']} duplicates={summary['duplicates_skipped']} invalid={summary['invalid_or_skipped']}")
        render_dataframe("rules_engine_results", summary.get("rule_results", []), width="stretch", hide_index=True)
        signal = summary.get("provider_failure_signal") or {}
        if signal:
            st.caption(f"Provider signal: {signal}")

if active_suggestions_tab == "AI":
    key_present = bool(openai_api_key())
    if key_present:
        st.success(f"AI key detected via {OPENAI_API_KEY_ENV}.")
    else:
        st.warning(f"{OPENAI_API_KEY_ENV} not set. AI generation is disabled until configured.")

    scope = st.selectbox("Scope", ["Top-ranked themes", "Selected theme", "Custom tickers"], index=0)
    selected_theme = None
    custom_tickers = []
    if scope == "Selected theme":
        selected_theme = st.selectbox("Theme", options=themes[["id", "name"]].to_dict("records"), format_func=lambda r: f"{r['name']} [{r['id']}]", key="ai_theme")
    elif scope == "Custom tickers":
        raw = st.text_input("Tickers", value="AAPL, MSFT, NVDA")
        custom_tickers = sorted(set([x.strip().upper() for x in raw.replace(",", " ").split(" ") if x.strip()]))

    proposal_types = st.multiselect("Allowed types", ["add_ticker_to_theme", "remove_ticker_from_theme", "create_theme", "rename_theme", "review_theme"], default=["review_theme", "add_ticker_to_theme", "remove_ticker_from_theme"])
    instruction = st.text_area("AI instruction", value="Focus on high-quality actionable proposals with evidence.")
    max_props = st.slider("Max proposals", min_value=1, max_value=20, value=AI_MAX_PROPOSALS)

    rankings = load_theme_rankings_cached(db_token)
    health = load_theme_health_overview_cached(db_token, low_constituent_threshold=3, failure_window_days=14)

    if st.button("Generate AI proposals", type="primary", disabled=not key_present):
        context = sanitize_context(
            {
                "scope": scope,
                "selected_theme": selected_theme,
                "custom_tickers": custom_tickers,
                "allowed_suggestion_types": proposal_types,
                "top_rankings": rankings.head(30).to_dict("records") if not rankings.empty else [],
                "theme_health": health.head(50).to_dict("records") if not health.empty else [],
            }
        )
        prompt = f"{instruction}\nOnly use: {proposal_types}. Skip weak suggestions."
        try:
            with get_conn() as conn:
                summary = generate_ai_suggestions(conn, prompt=prompt, context=context, max_proposals=max_props)
            st.success(f"AI done: attempted={summary['attempted']} created={summary['created']} duplicates={summary['duplicates']} invalid={summary['invalid']}")
            if summary["errors"]:
                st.code("\n".join(summary["errors"]))
        except Exception as exc:
            st.error(f"AI generation failed: {exc}")

if active_suggestions_tab == "Scanner Audit":
    st.subheader("TC2000 Universe Audit Foundation")
    st.caption("Manual import only for now. This ingests TC2000 export files, tracks recurrence over time, and surfaces uncovered scanner names for review.")

    sc1, sc2, sc3 = st.columns(3)
    sc1.metric("Import folder", str(TC2000_IMPORT_DIR))
    sc2.metric("File pattern", TC2000_FILE_GLOB)
    sc3.metric("Default source label", TC2000_DEFAULT_SOURCE_LABEL)

    reset_feedback = st.session_state.pop("scanner_reset_feedback", None)
    if reset_feedback:
        st.warning(reset_feedback["message"])

    import_feedback = st.session_state.pop("scanner_import_feedback", None)
    if import_feedback:
        if import_feedback["status"] == "success":
            st.success(import_feedback["message"])
        elif import_feedback["status"] == "partial":
            st.warning(import_feedback["message"])
        elif import_feedback["status"] == "no_files":
            st.info(import_feedback["message"])
        else:
            st.error(import_feedback["message"])
        file_results = import_feedback.get("file_results") or []
        if file_results:
            render_dataframe("scanner_import_file_results", file_results, width="stretch", hide_index=True)

    if st.button("Import latest TC2000 exports", type="primary"):
        with get_conn() as conn:
            result = import_tc2000_exports(conn)
        clear_scanner_candidate_summary_cache()
        st.session_state["scanner_import_feedback"] = result
        st.rerun()

    if st.session_state.get("scanner_reset_armed", False):
        st.warning(
            "Reset Scanner Audit Data will remove scanner hit history, scanner import run history, "
            "scanner candidate review states, and imported file ledger entries. "
            "Use this only to rebuild Scanner Audit imports after fixing import metadata inference."
        )
        rc1, rc2 = st.columns(2)
        with rc1:
            if st.button("Confirm Reset Scanner Audit Data", type="secondary"):
                with get_conn() as conn:
                    result = reset_scanner_audit_data(conn)
                clear_scanner_candidate_summary_cache()
                st.session_state["scanner_reset_feedback"] = result
                st.session_state["scanner_reset_armed"] = False
                st.session_state.pop("scanner_import_feedback", None)
                st.rerun()
        with rc2:
            if st.button("Cancel Scanner Audit Reset"):
                st.session_state["scanner_reset_armed"] = False
                st.rerun()
    elif st.button("Reset Scanner Audit Data", type="secondary"):
        st.session_state["scanner_reset_armed"] = True
        st.rerun()

    with get_conn() as conn:
        import_runs = recent_scanner_import_runs(conn, limit=10)
        overview = scanner_import_overview(conn)
    candidates = load_scanner_candidate_summary_cached(db_token)

    o1, o2, o3, o4, o5, o6 = st.columns(6)
    o1.metric("Last import", str(overview["last_import_time"] or "—"))
    o2.metric("Files seen", int(overview["files_seen"] or 0))
    o3.metric("Files processed", int(overview["files_processed"] or 0))
    o4.metric("Files skipped", int(overview["files_skipped"] or 0))
    o5.metric("Files failed", int(overview["files_failed"] or 0))
    o6.metric("Rows imported", int(overview["rows_imported"] or 0))
    st.caption(
        f"Uncovered surfacing: {int(overview['uncovered_candidates'] or 0)} | "
        f"Ignored: {int(overview['ignored_candidates'] or 0)}"
    )

    if not import_runs.empty:
        st.caption("Recent import runs")
        render_dataframe("scanner_import_runs", import_runs, width="stretch", hide_index=True)

    if candidates.empty:
        st.info("No scanner-hit history yet. Import TC2000 exports to start building recurrence evidence.")
    else:
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            recommendation_filter = st.selectbox(
                "Recommendation filter",
                ["all", "high-persistence uncovered", "review for addition", "monitor", "already covered"],
                index=0,
            )
        with fc2:
            coverage_filter = st.selectbox("Coverage filter", ["all", "uncovered", "already governed"], index=1)
        with fc3:
            review_state_filter = st.selectbox("Review state", ["active only", "all", "ignored", "reviewed"], index=0)
        with fc4:
            scanner_filter = st.selectbox(
                "Scanner/source",
                ["all"] + sorted(
                    {
                        item.strip()
                        for value in candidates["scanners"].astype(str).tolist()
                        for item in value.split(",")
                        if item.strip()
                    }
                ),
                index=0,
            )

        mc1, mc2, mc3, mc4 = st.columns(4)
        with mc1:
            min_persistence_score = st.number_input("Min persistence score", min_value=0, value=3, step=1)
        with mc2:
            min_observed_days = st.number_input("Min observed days", min_value=0, value=1, step=1)
        with mc3:
            min_last_5d = st.number_input("Min obs last 5d", min_value=0, value=0, step=1)
        with mc4:
            min_last_10d = st.number_input("Min obs last 10d", min_value=0, value=1, step=1)

        sc1, sc2 = st.columns(2)
        with sc1:
            sort_by = st.selectbox(
                "Sort by",
                ["persistence_score", "last_seen", "observed_days", "current_streak", "distinct_scanner_count"],
                index=0,
            )
        with sc2:
            sort_desc = st.checkbox("Descending", value=True)

        view = candidates.copy()
        if recommendation_filter != "all":
            view = view[view["recommendation"] == recommendation_filter]
        if coverage_filter == "uncovered":
            view = view[view["is_governed"] == False]
        elif coverage_filter == "already governed":
            view = view[view["is_governed"] == True]
        if review_state_filter == "active only":
            view = view[view["review_state"] == "active"]
        elif review_state_filter == "ignored":
            view = view[view["review_state"] == "ignored"]
        elif review_state_filter == "reviewed":
            view = view[view["review_state"] == "reviewed"]
        if scanner_filter != "all":
            view = view[view["scanners"].astype(str).str.contains(scanner_filter, case=False, na=False)]
        view = view[
            (view["persistence_score"] >= int(min_persistence_score))
            & (view["observed_days"] >= int(min_observed_days))
            & (view["observations_last_5d"] >= int(min_last_5d))
            & (view["observations_last_10d"] >= int(min_last_10d))
        ]
        view = view.sort_values([sort_by, "ticker"], ascending=[not sort_desc, True]).reset_index(drop=True)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Observed tickers", int(view.shape[0]))
        m2.metric("High-persistence uncovered", int((view["recommendation"] == "high-persistence uncovered").sum()))
        m3.metric("Review for addition", int((view["recommendation"] == "review for addition").sum()))
        m4.metric("Ignored in view", int((view["review_state"] == "ignored").sum()))

        display = view[
            [
                "ticker",
                "recommendation",
                "recommendation_reason",
                "review_state",
                "persistence_score",
                "observed_days",
                "observations_last_5d",
                "observations_last_10d",
                "current_streak",
                "distinct_scanner_count",
                "first_seen",
                "last_seen",
                "governed_status",
                "active_theme_count",
                "current_theme_names",
                "current_categories",
                "scanners",
                "source_labels",
                "metadata_basis",
            ]
        ].copy()
        render_dataframe("scanner_candidate_display", display, width="stretch", hide_index=True)

        if not view.empty:
            scanner_ticker_options = view["ticker"].tolist()
            st.session_state["scanner_audit_selected_ticker"] = resolve_scanner_audit_ticker(
                st.session_state.get("scanner_audit_selected_ticker"),
                scanner_ticker_options,
            )
            selected_audit_ticker = st.selectbox(
                "Selected scanner candidate",
                options=scanner_ticker_options,
                key="scanner_audit_selected_ticker",
            )
            selected_audit_row = view[view["ticker"] == selected_audit_ticker].iloc[0]
            st.caption(
                f"{selected_audit_ticker}: {selected_audit_row['recommendation_reason']} | "
                f"days={int(selected_audit_row['observed_days'])}, last10={int(selected_audit_row['observations_last_10d'])}, "
                f"streak={int(selected_audit_row['current_streak'])}, scanners=`{selected_audit_row['scanners']}`"
            )
            rs1, rs2 = st.columns(2)
            with rs1:
                review_action = st.selectbox(
                    "Candidate review state",
                    ["active", "ignored", "reviewed"],
                    index=["active", "ignored", "reviewed"].index(str(selected_audit_row["review_state"])),
                )
            with rs2:
                review_note = st.text_input("Review note", value=str(selected_audit_row.get("review_note") or ""))
            if st.button("Save candidate review state"):
                try:
                    with get_conn() as conn:
                        result = set_scanner_candidate_review_state(conn, selected_audit_ticker, review_action, review_note)
                    clear_scanner_candidate_summary_cache()
                    st.success(f"Saved `{result['review_state']}` state for {result['ticker']}.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not save candidate review state. {exc}")

            draft_store = st.session_state.setdefault("scanner_research_drafts", {})
            debug_store = st.session_state.setdefault("scanner_research_debug", {})
            strategy_options = {
                "Legacy direct match": "legacy_direct_match",
                "Description-first generation": "description_theme_generation",
            }
            selected_strategy_label = st.selectbox(
                "Research strategy",
                options=list(strategy_options.keys()),
                key=f"scanner_research_strategy_{selected_audit_ticker}",
            )
            selected_strategy = strategy_options[selected_strategy_label]
            existing_draft = draft_store.get(selected_audit_ticker)
            if existing_draft:
                debug_store[selected_audit_ticker] = build_scanner_research_debug_entry(
                    selected_audit_ticker,
                    existing_draft,
                    prior_debug=debug_store.get(selected_audit_ticker),
                )
            draft_action_cols = st.columns(2 if existing_draft else 1)
            with draft_action_cols[0]:
                generate_clicked = st.button("Generate Research Draft")
            regenerate_clicked = False
            if existing_draft:
                with draft_action_cols[1]:
                    regenerate_clicked = st.button("Regenerate Research Draft", type="secondary")
            if generate_clicked or regenerate_clicked:
                try:
                    with get_conn() as conn:
                        draft, reused = get_or_create_scanner_research_draft(
                            conn,
                            selected_audit_ticker,
                            existing_draft=existing_draft,
                            force_refresh=regenerate_clicked,
                            strategy=selected_strategy,
                        )
                    draft_source = "reused_session_draft" if reused else ("forced_regeneration" if regenerate_clicked else "fresh_generation")
                    draft["draft_source"] = draft_source
                    draft_store[selected_audit_ticker] = draft
                    debug_store[selected_audit_ticker] = build_scanner_research_debug_entry(
                        selected_audit_ticker,
                        draft,
                        prior_debug=debug_store.get(selected_audit_ticker),
                        draft_source=draft_source,
                    )
                    st.session_state["scanner_research_feedback"] = {
                        "level": "success",
                        "message": (
                            f"Reused existing advisory research draft for {selected_audit_ticker}."
                            if reused
                            else (
                                f"Regenerated advisory research draft for {selected_audit_ticker}."
                                if regenerate_clicked
                                else f"Generated advisory research draft for {selected_audit_ticker}."
                            )
                        ),
                    }
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not generate research draft. {exc}")

            if existing_draft:
                st.markdown("**Research Draft (Advisory Only)**")
                st.caption(
                    "This is an agent-assisted advisory draft grounded on the current governed theme catalog where practical. "
                    "It is not a governed-theme assignment and still requires human review."
                )
                research_mode = str(existing_draft.get("research_mode") or "heuristic_fallback")
                mode_label = "OpenAI" if research_mode == "openai" else "Heuristic fallback"
                meta_caption = (
                    f"Generated at `{existing_draft.get('generated_at') or 'n/a'}` | "
                    f"strategy=`{existing_draft.get('theme_generation_strategy') or selected_strategy}` | "
                    f"Research Mode: `{mode_label}` | "
                    f"recommended_action=`{existing_draft.get('recommended_action') or 'watch_only'}` | "
                    f"confidence=`{existing_draft.get('confidence') or 'low'}`"
                )
                review_gutter_col, review_shell_col, review_tail_col = st.columns([0.015, 0.935, 0.05], gap="small")
                with review_shell_col:
                    st.caption(meta_caption)
                    main_review_col, side_review_col = st.columns([0.76, 0.24], gap="large")
                debug_entry = debug_store.get(selected_audit_ticker) or {}
                timing_summary = existing_draft.get("research_timing_summary") or {}
                decision_trace = existing_draft.get("research_decision_trace") or {}
                validation_debug = existing_draft.get("validation_debug") or {}
                context_meta = existing_draft.get("research_context_meta") or {}
                research_error = existing_draft.get("research_error") or {}
                with side_review_col:
                    st.markdown("<div class='scanner-audit-side-block'>", unsafe_allow_html=True)
                    st.write(f"**Rationale:** {existing_draft.get('rationale') or 'No rationale provided.'}")
                    caveats = existing_draft.get("caveats") or []
                    st.write("**Caveats:** " + (" | ".join(caveats) if caveats else "None"))
                    if context_meta:
                        st.caption(
                            "Context: "
                            f"themes_sent=`{context_meta.get('filtered_theme_count')}` / "
                            f"catalog_total=`{context_meta.get('full_catalog_theme_count')}` | "
                            f"prefiltered=`{context_meta.get('catalog_was_prefiltered')}` | "
                            f"estimated_chars=`{context_meta.get('estimated_context_chars', 'n/a')}`"
                        )
                    if research_mode != "openai" and existing_draft.get("fallback_reason"):
                        st.caption(f"Heuristic fallback: {existing_draft.get('fallback_reason')}")
                    if research_error:
                        debug_parts = []
                        if research_error.get("status_code") is not None:
                            debug_parts.append(f"status=`{research_error.get('status_code')}`")
                        if research_error.get("error_type"):
                            debug_parts.append(f"type=`{research_error.get('error_type')}`")
                        if research_error.get("error_class"):
                            debug_parts.append(f"class=`{research_error.get('error_class')}`")
                        if research_error.get("model"):
                            debug_parts.append(f"model=`{research_error.get('model')}`")
                        if research_error.get("error_message"):
                            debug_parts.append(f"message=`{research_error.get('error_message')}`")
                        if debug_parts:
                            st.caption("OpenAI error: " + " | ".join(debug_parts))
                    st.markdown("</div>", unsafe_allow_html=True)
                with main_review_col:
                    st.write(f"**Company:** {existing_draft.get('company_name') or selected_audit_ticker}")
                    st.write(f"**Description:** {existing_draft.get('short_company_description') or 'No description available.'}")
                    st.write(
                        "**Similar tickers:** "
                        + (", ".join(existing_draft.get("possible_similar_tickers") or []) or "None suggested")
                    )
                custom_new_key = f"scanner_custom_new_{selected_audit_ticker}"
                custom_new_state_key = f"scanner_custom_new_state_{selected_audit_ticker}"
                custom_new_category_key = f"scanner_custom_new_category_{selected_audit_ticker}"
                custom_new_category_state_key = f"scanner_custom_new_category_state_{selected_audit_ticker}"
                generated_theme_ideas = existing_draft.get("candidate_theme_ideas") or []
                generated_checkbox_keys = {
                    str(idea): f"scanner_generated_theme_checkbox_{selected_audit_ticker}_{idx}"
                    for idx, idea in enumerate(generated_theme_ideas)
                    if str(idea or "").strip()
                }
                generated_checkbox_session_state = {
                    idea: bool(st.session_state.get(checkbox_key))
                    for idea, checkbox_key in generated_checkbox_keys.items()
                    if checkbox_key in st.session_state
                }
                if generated_checkbox_session_state:
                    reconciled_new_theme_value, reconciled_new_theme_state = reconcile_possible_new_theme_from_generated_checkbox_state(
                        st.session_state.get(custom_new_key),
                        generated_theme_ideas,
                        generated_checkbox_session_state,
                        st.session_state.get(custom_new_state_key),
                    )
                    st.session_state[custom_new_key] = reconciled_new_theme_value
                    st.session_state[custom_new_state_key] = reconciled_new_theme_state
                prepared_new_theme_value, prepared_new_theme_state = prepare_possible_new_theme_prefill(
                    st.session_state.get(custom_new_key),
                    existing_draft.get("possible_new_theme"),
                    st.session_state.get(custom_new_state_key),
                )
                st.session_state[custom_new_key] = prepared_new_theme_value
                st.session_state[custom_new_state_key] = prepared_new_theme_state
                prepared_new_category_value, prepared_new_category_state = prepare_possible_new_theme_category_prefill(
                    st.session_state.get(custom_new_category_key),
                    existing_draft.get("possible_new_theme_category"),
                    st.session_state.get(custom_new_category_state_key),
                )
                st.session_state[custom_new_category_key] = prepared_new_category_value
                st.session_state[custom_new_category_state_key] = prepared_new_category_state
                st.markdown(
                    """
                    <style>
                    .scanner-audit-side-block {
                        border-left: 1px solid rgba(49, 51, 63, 0.08);
                        padding-left: 0.9rem;
                    }
                    .scanner-audit-main-divider {
                        margin-top: 0.52rem;
                        padding-top: 0.42rem;
                        border-top: 1px solid rgba(49, 51, 63, 0.08);
                    }
                    .scanner-audit-helper {
                        color: rgba(49, 51, 63, 0.8);
                        font-size: 0.84rem;
                        line-height: 1.28;
                        margin: 0.08rem 0 0.22rem 0;
                    }
                    .scanner-checkbox-block {
                        margin-top: 0.12rem;
                    }
                    .scanner-checkbox-block .scanner-checkbox-row {
                        margin: 0.03rem 0 0.38rem 0;
                    }
                    .scanner-checkbox-block .scanner-checkbox-row-title {
                        line-height: 1.1;
                        margin: 0;
                    }
                    .scanner-checkbox-block .scanner-checkbox-text {
                        padding-left: 0.18rem;
                    }
                    .scanner-checkbox-block .scanner-checkbox-row-detail {
                        color: rgba(49, 51, 63, 0.78);
                        font-size: 0.82rem;
                        line-height: 1.24;
                        margin-top: 0.08rem;
                    }
                    .scanner-suggested-theme-block {
                        margin-top: 0.24rem;
                        padding-top: 0.18rem;
                        border-top: 1px solid rgba(49, 51, 63, 0.08);
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                with main_review_col:
                    if generated_theme_ideas:
                        generated_checkbox_state = sync_generated_theme_idea_checkbox_state(
                            st.session_state.get(custom_new_key),
                            generated_theme_ideas,
                        )
                        for idea, is_checked in generated_checkbox_state.items():
                            st.session_state[generated_checkbox_keys[idea]] = bool(is_checked)
                        st.markdown("**Generated Theme Ideas**")
                        st.markdown(
                            "<div class='scanner-audit-helper'>Use these as fast candidate labels for the proposed new-theme field.</div>",
                            unsafe_allow_html=True,
                        )
                        st.markdown("<div class='scanner-checkbox-block'>", unsafe_allow_html=True)
                        for idea in generated_checkbox_state.keys():
                            checkbox_key = generated_checkbox_keys[idea]
                            _render_compact_checkbox_content_row(
                                checkbox_key,
                                checkbox_label="Select generated theme idea",
                                title=str(idea),
                                on_change=_sync_possible_new_theme_from_generated_checkboxes,
                                args=(
                                    custom_new_key,
                                    custom_new_state_key,
                                    list(generated_checkbox_state.keys()),
                                    generated_checkbox_keys,
                                ),
                            )
                        st.markdown("</div>", unsafe_allow_html=True)
                        selected_idea_text = join_possible_new_theme_ideas(
                            [
                                idea
                                for idea in split_possible_new_theme_ideas(st.session_state.get(custom_new_key))
                                if idea.casefold() in {generated.casefold() for generated in generated_checkbox_state.keys()}
                            ]
                        )
                        if selected_idea_text:
                            st.caption("Selected generated ideas: " + selected_idea_text)
                review_key = f"scanner_research_review_outcome_{selected_audit_ticker}"
                review_notes_key = f"scanner_research_review_notes_{selected_audit_ticker}"
                review_stamp_key = f"scanner_research_review_stamp_{selected_audit_ticker}"
                draft_review_stamp = "|".join(
                    [
                        str(existing_draft.get("ticker") or selected_audit_ticker),
                        str(existing_draft.get("generated_at") or ""),
                        str(existing_draft.get("theme_generation_strategy") or selected_strategy),
                    ]
                )
                saved_review = get_scanner_research_review(conn, selected_audit_ticker, existing_draft)
                if st.session_state.get(review_stamp_key) != draft_review_stamp:
                    st.session_state[review_key] = str((saved_review or {}).get("outcome_class") or "")
                    st.session_state[review_notes_key] = str((saved_review or {}).get("reviewer_notes") or "")
                    st.session_state[review_stamp_key] = draft_review_stamp
                with side_review_col:
                    st.caption("Supporting Context")
                    review_summary = scanner_research_review_summary(conn, limit=6)
                    counts_by_outcome = review_summary.get("counts_by_outcome") or {}
                    if counts_by_outcome:
                        st.caption(
                            "Review counts: "
                            + " | ".join(f"{outcome}=`{count}`" for outcome, count in counts_by_outcome.items())
                        )
                    recent_reviews = review_summary.get("recent_reviews") or []
                    if recent_reviews:
                        st.caption("Recent reviews:")
                        for item in recent_reviews[:4]:
                            recent_text = f"{item.get('ticker')} -> {item.get('outcome_class')}"
                            if item.get("reviewer_notes"):
                                recent_text += f" | {item.get('reviewer_notes')}"
                            st.caption(recent_text)

                with main_review_col:
                    suggested_themes = existing_draft.get("suggested_existing_themes") or []
                    all_theme_ids = {int(option["id"]) for option in theme_options}
                    selected_existing_key = f"scanner_selected_existing_theme_ids_{selected_audit_ticker}"
                    st.session_state[selected_existing_key] = normalize_theme_id_list(
                        st.session_state.get(selected_existing_key, []),
                        all_theme_ids,
                    )
                    suggested_theme_ids = [int(item.get("theme_id")) for item in suggested_themes if item.get("theme_id") not in (None, "")]
                    checkbox_state = sync_suggested_theme_checkbox_state(
                        st.session_state[selected_existing_key],
                        suggested_theme_ids,
                    )
                    for theme_id, is_checked in checkbox_state.items():
                        checkbox_key = f"scanner_suggested_theme_{selected_audit_ticker}_{theme_id}"
                        st.session_state[checkbox_key] = bool(is_checked)
                    if suggested_themes:
                        st.markdown("**Suggested Existing Themes**")
                        st.markdown(
                            "<div class='scanner-audit-helper'>Check the strongest governed-theme fits first, then fine-tune below.</div>",
                            unsafe_allow_html=True,
                        )
                        st.markdown("<div class='scanner-checkbox-block scanner-suggested-theme-block'>", unsafe_allow_html=True)
                        for item in suggested_themes:
                            theme_id = int(item.get("theme_id"))
                            fit_label = str(item.get("fit_label") or "adjacent_fit")
                            checkbox_key = f"scanner_suggested_theme_{selected_audit_ticker}_{theme_id}"
                            supporting_parts = []
                            if item.get("why_it_might_fit"):
                                supporting_parts.append(str(item.get("why_it_might_fit")))
                            if item.get("representative_tickers"):
                                supporting_parts.append("representative tickers: " + ", ".join(item.get("representative_tickers") or []))
                            _render_compact_checkbox_content_row(
                                checkbox_key,
                                checkbox_label="Select suggested theme",
                                title=f"{item.get('theme_name')} [{fit_label}]",
                                detail=" | ".join(supporting_parts),
                                on_change=_sync_selected_existing_from_suggested_checkbox,
                                args=(selected_existing_key, checkbox_key, theme_id, all_theme_ids),
                            )
                        st.markdown("</div>", unsafe_allow_html=True)
                    else:
                        st.info("No strong existing governed-theme match was suggested from the available context.")

                    selected_existing_theme_ids = st.multiselect(
                        "Selected existing themes",
                        options=sorted(all_theme_ids),
                        format_func=lambda theme_id: (
                            f"{theme_option_by_id[int(theme_id)]['name']} [{int(theme_id)}]"
                            + (" (suggested)" if int(theme_id) in set(suggested_theme_ids) else "")
                        ),
                        key=selected_existing_key,
                    )
                    selected_suggested_theme_ids, custom_existing_theme_ids = split_selected_existing_theme_ids(
                        selected_existing_theme_ids,
                        suggested_theme_ids,
                    )
                    custom_new_theme_raw = st.text_input(
                        "Proposed new theme ideas",
                        placeholder="Comma-separated new theme ideas",
                        key=custom_new_key,
                    )
                    st.session_state[custom_new_state_key] = finalize_possible_new_theme_state(
                        custom_new_theme_raw,
                        st.session_state.get(custom_new_state_key),
                    )
                    custom_new_themes = split_possible_new_theme_ideas(st.session_state.get(custom_new_key))
                    proposed_new_theme_category = st.text_input(
                        "Proposed category",
                        placeholder="Advisory category for the proposed new theme",
                        key=custom_new_category_key,
                    )
                    st.session_state[custom_new_category_state_key] = finalize_possible_new_theme_category_state(
                        proposed_new_theme_category,
                        st.session_state.get(custom_new_category_state_key),
                    )
                    if selected_suggested_theme_ids or custom_existing_theme_ids or custom_new_themes:
                        st.caption(
                            "Selected for staged review: "
                            + ", ".join(
                                [theme_option_by_id[theme_id]["name"] for theme_id in custom_existing_theme_ids if theme_id in theme_option_by_id]
                                + [item.get("theme_name") for item in suggested_themes if int(item.get("theme_id")) in selected_suggested_theme_ids]
                                + custom_new_themes
                            )
                        )
                        if proposed_new_theme_category:
                            st.caption(f"Proposed category: `{proposed_new_theme_category}`")
                    else:
                        st.caption("No theme ideas selected yet. You can check suggested themes, search/select existing themes manually, or enter proposed new-theme labels.")
                    st.markdown("<div class='scanner-audit-main-divider'>", unsafe_allow_html=True)
                    promotion_note = st.text_area(
                        "Promotion note (optional)",
                        value="",
                        placeholder="Why it looks interesting, suspected theme/category, or any caution/uncertainty.",
                        key=f"scanner_audit_promotion_note_{selected_audit_ticker}",
                    )
                    st.markdown("**Reviewer Outcome**")
                    st.markdown(
                        "<div class='scanner-audit-helper'>Record whether the overall draft looked right after reviewing the selections above.</div>",
                        unsafe_allow_html=True,
                    )
                    selected_outcome = st.selectbox(
                        "Reviewer outcome",
                        options=[
                            "",
                            "direct_fit_correct",
                            "adjacent_fit_acceptable",
                            "should_have_been_tentative",
                            "false_positive",
                            "missed_obvious_theme",
                        ],
                        format_func=lambda value: value or "Select outcome",
                        key=review_key,
                    )
                    reviewer_notes = st.text_input(
                        "Reviewer notes (optional)",
                        placeholder="What was wrong, or what should have happened instead?",
                        key=review_notes_key,
                    )
                    save_review_clicked = st.button(
                        "Save review outcome",
                        key=f"save_scanner_research_review_{selected_audit_ticker}",
                    )
                    if save_review_clicked:
                        if not selected_outcome:
                            st.warning("Select a reviewer outcome before saving.")
                        else:
                            try:
                                saved_review = save_scanner_research_review(
                                    conn,
                                    selected_audit_ticker,
                                    existing_draft,
                                    outcome_class=selected_outcome,
                                    reviewer_notes=reviewer_notes,
                                )
                                st.success(
                                    "Saved review outcome: "
                                    f"{saved_review.get('outcome_class') or selected_outcome}"
                                )
                            except Exception as exc:
                                st.error(f"Could not save review outcome. {exc}")
                    if saved_review:
                        st.caption(
                            "Saved review: "
                            f"outcome=`{saved_review.get('outcome_class')}` | "
                            f"updated_at=`{saved_review.get('updated_at') or 'n/a'}`"
                        )
                    st.markdown("<div class='scanner-audit-main-divider'>", unsafe_allow_html=True)
                    st.markdown("**Promotion & Apply**")
                    st.markdown(
                        "<div class='scanner-audit-helper'>These actions use the current existing-theme selections and proposed new-theme notes above.</div>",
                        unsafe_allow_html=True,
                    )
                    can_promote = not bool(selected_audit_row["is_governed"])
                    st.caption(
                        "Promotion creates or refreshes a staged review candidate only. "
                        "It carries forward the advisory draft and Scanner Audit evidence, and it does not modify governed theme membership."
                    )
                    if not can_promote:
                        st.info("This candidate is already governed. Promotion is reserved for uncovered candidates.")
                    can_send_review = (
                        can_promote
                        and existing_draft is not None
                        and has_meaningful_theme_review_selection(
                            selected_existing_theme_ids,
                            st.session_state.get(custom_new_key),
                        )
                    )
                    send_disabled = not can_send_review
                    apply_now_disabled = send_disabled or not bool(selected_suggested_theme_ids or custom_existing_theme_ids)
                    if can_promote and existing_draft is not None and send_disabled:
                        st.caption("Theme Review promotion requires at least one selected existing theme or a proposed new theme. Category alone is not enough.")
                    if apply_now_disabled and existing_draft is not None:
                        st.caption("Direct apply requires at least one selected existing theme. Custom new-theme ideas can still be staged for later review.")
                    action_c1, action_c2 = st.columns(2)
                    if action_c1.button("Send Selected Suggestions to Theme Review", disabled=send_disabled):
                        try:
                            with get_conn() as conn:
                                result = promote_scanner_candidate_to_theme_review(
                                    conn,
                                    selected_audit_ticker,
                                    promotion_note,
                                    research_draft=existing_draft,
                                    selected_suggested_theme_ids=selected_suggested_theme_ids,
                                    custom_existing_theme_ids=custom_existing_theme_ids,
                                    custom_new_themes=custom_new_themes,
                                    proposed_new_theme_category=proposed_new_theme_category,
                                )
                            st.success(str(result["message"]))
                            st.caption("Selected ideas were sent to staged Theme Review only. No governed membership was changed.")
                        except Exception as exc:
                            st.error(f"Could not send candidate to Theme Review. {exc}")
                    if action_c2.button("Apply Selected Themes & Start Onboarding", disabled=apply_now_disabled, type="primary"):
                        try:
                            with get_conn() as conn:
                                result = apply_scanner_candidate_selected_themes(
                                    conn,
                                    selected_audit_ticker,
                                    promotion_note,
                                    research_draft=existing_draft,
                                    selected_suggested_theme_ids=selected_suggested_theme_ids,
                                    custom_existing_theme_ids=custom_existing_theme_ids,
                                    custom_new_themes=custom_new_themes,
                                    proposed_new_theme_category=proposed_new_theme_category,
                                )
                            clear_scanner_candidate_summary_cache()
                            clear_current_market_view_caches()
                            onboarding_state = result.get("onboarding_state") or {}
                            theme_summary = ", ".join(result.get("applied_theme_names") or [])
                            st.success(str(result["message"]))
                            if theme_summary:
                                st.caption(f"Applied themes: `{theme_summary}`")
                            proposed_new_theme_summary = ", ".join(result.get("proposed_new_theme_names") or [])
                            if proposed_new_theme_summary:
                                st.caption(f"Preserved proposed new themes: `{proposed_new_theme_summary}`")
                            if result.get("proposed_new_theme_category"):
                                st.caption(f"Preserved proposed category: `{result.get('proposed_new_theme_category')}`")
                            if onboarding_state:
                                st.caption(
                                    "Onboarding: "
                                    f"history=`{onboarding_state.get('history_readiness_status') or 'unknown'}` | "
                                    f"backfill=`{onboarding_state.get('backfill_status') or 'unknown'}` | "
                                    f"downstream_refresh_needed=`{bool(onboarding_state.get('downstream_refresh_needed'))}`"
                                )
                            st.caption(
                                "An auditable Scanner Audit review record was preserved automatically. "
                                "Use Theme Review only when you want to defer the final apply decision."
                            )
                        except Exception as exc:
                            st.error(f"Could not apply selected themes from Scanner Audit. {exc}")
                    st.markdown("<div class='scanner-audit-main-divider'>", unsafe_allow_html=True)
                    with st.expander("Validation Signals", expanded=False):
                        if debug_entry:
                            st.caption(
                                "Draft: "
                                f"ticker=`{debug_entry.get('ticker') or selected_audit_ticker}` | "
                                f"generated_at=`{debug_entry.get('generated_at') or existing_draft.get('generated_at') or 'n/a'}` | "
                                f"mode=`{debug_entry.get('research_mode') or research_mode}` | "
                                f"draft_source=`{debug_entry.get('draft_source') or existing_draft.get('draft_source') or 'reused_session_draft'}`"
                            )
                        st.caption(
                            "Anchors: "
                            f"strategy=`{validation_debug.get('strategy') or existing_draft.get('theme_generation_strategy') or selected_strategy}` | "
                            f"domain=`{validation_debug.get('domain_anchor') or existing_draft.get('domain_anchor') or 'unclear'}` | "
                            f"dominant_role=`{validation_debug.get('dominant_business_role') or existing_draft.get('dominant_business_role') or 'unclear'}` | "
                            f"strong_role_evidence=`{validation_debug.get('strong_role_evidence', 'n/a')}`"
                        )
                        generated_ideas = validation_debug.get("generated_theme_ideas") or existing_draft.get("candidate_theme_ideas") or []
                        st.caption("Generated ideas: " + (", ".join(generated_ideas) if generated_ideas else "None"))
                        new_theme_decision = validation_debug.get("possible_new_theme_decision") or {}
                        if new_theme_decision:
                            st.caption(
                                "New-theme decision: "
                                f"candidate=`{new_theme_decision.get('candidate') or 'None'}` | "
                                f"selected=`{new_theme_decision.get('selected') or 'None'}` | "
                                f"category=`{new_theme_decision.get('selected_category') or existing_draft.get('possible_new_theme_category') or 'None'}` | "
                                f"status=`{new_theme_decision.get('status') or 'n/a'}` | "
                                f"reason=`{new_theme_decision.get('reason') or 'n/a'}`"
                            )
                        elif decision_trace:
                            st.caption(
                                "New-theme decision: "
                                f"candidate=`{decision_trace.get('candidate_new_theme') or 'None'}` | "
                                f"adjacent_only_existing=`{decision_trace.get('adjacency_only_existing_fit')}` | "
                                f"heuristic_prefers_new=`{decision_trace.get('heuristic_prefers_new_theme')}` | "
                                f"should_promote_new=`{decision_trace.get('should_promote_new_theme')}`"
                            )
                        evaluated_matches = validation_debug.get("evaluated_matches") or []
                        if evaluated_matches:
                            st.markdown("**Evaluated Governed Matches**")
                            for item in evaluated_matches:
                                overlap_parts = []
                                if item.get("role_overlap"):
                                    overlap_parts.append("roles=" + ", ".join(item.get("role_overlap") or []))
                                if item.get("economic_role_overlap"):
                                    overlap_parts.append("economic=" + ", ".join(item.get("economic_role_overlap") or []))
                                if item.get("specific_overlap"):
                                    overlap_parts.append("concepts=" + ", ".join(item.get("specific_overlap") or []))
                                if item.get("market_overlap"):
                                    overlap_parts.append("markets=" + ", ".join(item.get("market_overlap") or []))
                                st.caption(
                                    f"{'PASS' if item.get('actionable') else 'FAIL'} | "
                                    f"score=`{item.get('score')}` | "
                                    f"idea=`{item.get('idea')}` -> theme=`{item.get('theme_name')}` | "
                                    f"fit=`{item.get('fit_label')}` | "
                                    f"anchor=`{item.get('theme_anchor_alignment')}` | "
                                    f"why=`{item.get('why')}`"
                                )
                                st.caption(
                                    "Gate: "
                                    f"{item.get('gate_reason') or 'n/a'}"
                                    + (f" | overlaps=`{' ; '.join(overlap_parts)}`" if overlap_parts else "")
                                )
                        elif existing_draft.get("matched_theme_candidates"):
                            st.caption(
                                "Evaluated matches: "
                                + "; ".join(
                                    f"{item.get('idea')} -> {item.get('theme_name')} [{item.get('fit_label')}]"
                                    for item in (existing_draft.get("matched_theme_candidates") or [])[:5]
                                )
                            )
                        if timing_summary:
                            timing_parts = []
                            for key in [
                                "candidate_context_ms",
                                "catalog_query_ms",
                                "catalog_preprocess_ms",
                                "profile_lookup_ms",
                                "domain_anchor_ms",
                                "dominant_business_role_ms",
                                "candidate_theme_ideas_ms",
                                "governed_theme_matching_ms",
                                "catalog_prefilter_ms",
                                "ai_request_ms",
                                "merge_ms",
                                "total_ms",
                            ]:
                                if key in timing_summary:
                                    timing_parts.append(f"{key}=`{timing_summary.get(key)}`")
                            if timing_parts:
                                st.caption("Timing: " + " | ".join(timing_parts))
            else:
                selected_existing_key = f"scanner_selected_existing_theme_ids_{selected_audit_ticker}"
                st.session_state[selected_existing_key] = normalize_theme_id_list(st.session_state.get(selected_existing_key, []))
                selected_suggested_theme_ids = []
                custom_existing_theme_ids = []
                custom_new_themes = []
                st.info("Generate a research draft first to promote an agent-assisted review candidate.")

show_perf_summary()
