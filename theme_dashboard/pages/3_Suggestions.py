import streamlit as st

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
    import_tc2000_exports,
    promote_scanner_candidate_to_theme_review,
    recent_scanner_import_runs,
    reset_scanner_audit_data,
    scanner_import_overview,
    set_scanner_candidate_review_state,
)
from src.scanner_research import get_or_create_scanner_research_draft
from src.streamlit_utils import (
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
    count_filtered_suggestions,
    create_suggestion,
    list_suggestions,
    recent_applied_suggestions,
    review_suggestion,
    suggestion_status_counts,
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

manual_tab, queue_tab, rules_tab, ai_tab, scanner_tab = st.tabs(["Manual", "Queue", "Rules", "AI", "Scanner Audit"])

with manual_tab:
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

with queue_tab:
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

        pending = queue[queue["status"] == "pending"]
        approved = queue[queue["status"] == "approved"]
        if not pending.empty:
            selected = st.selectbox("Pending suggestion", options=pending["suggestion_id"].tolist())
            selected_row = pending[pending["suggestion_id"] == selected].iloc[0]
            st.caption(
                f"Selected pending suggestion #{int(selected_row['suggestion_id'])}: "
                f"`{selected_row['suggestion_type']}` from `{selected_row['source']}`"
            )
            if str(selected_row.get("proposed_ticker") or "").strip():
                _render_ticker_membership_context(selected_row)
            rnotes = st.text_input("Review notes", value="", key="rnotes")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Approve"):
                    try:
                        with get_conn() as conn:
                            result = review_suggestion(conn, int(selected), "approved", rnotes)
                        new_status = str(result.get("new_status") or "approved")
                        if bool(result.get("changed")):
                            visible_note = (
                                " It will disappear on rerun if your current filter excludes approved items."
                                if status_filter not in {"all", "approved"}
                                else " It remains visible because the current filter still includes approved items."
                            )
                            st.session_state["suggestions_feedback"] = {
                                "level": "success",
                                "message": f"{result['message']}{visible_note}",
                            }
                        else:
                            st.session_state["suggestions_feedback"] = {
                                "level": "warning",
                                "message": str(result.get("message") or f"Suggestion #{selected} is already {new_status}."),
                            }
                    except Exception as exc:
                        st.session_state["suggestions_feedback"] = {
                            "level": "error",
                            "message": f"Approve failed: {exc}",
                        }
                    st.rerun()
            with c2:
                if st.button("Reject"):
                    try:
                        with get_conn() as conn:
                            result = review_suggestion(conn, int(selected), "rejected", rnotes)
                        if bool(result.get("changed")):
                            visible_note = (
                                " It will disappear on rerun if your current filter excludes rejected items."
                                if status_filter not in {"all", "rejected"}
                                else " It remains visible because the current filter still includes rejected items."
                            )
                            st.session_state["suggestions_feedback"] = {
                                "level": "success",
                                "message": f"{result['message']}{visible_note}",
                            }
                        else:
                            st.session_state["suggestions_feedback"] = {
                                "level": "warning",
                                "message": str(result.get("message") or f"Suggestion #{selected} is already rejected."),
                            }
                    except Exception as exc:
                        st.session_state["suggestions_feedback"] = {
                            "level": "error",
                            "message": f"Reject failed: {exc}",
                        }
                    st.rerun()
        if not approved.empty:
            aid = st.selectbox("Approved suggestion", options=approved["suggestion_id"].tolist())
            approved_row = approved[approved["suggestion_id"] == aid].iloc[0]
            st.caption(
                f"Selected approved suggestion #{int(approved_row['suggestion_id'])}: "
                f"`{approved_row['suggestion_type']}` from `{approved_row['source']}`"
            )
            if str(approved_row.get("proposed_ticker") or "").strip():
                _render_ticker_membership_context(approved_row)
            anotes = st.text_input("Apply notes", value="", key="anotes")
            if st.button("Apply approved"):
                with get_conn() as conn:
                    apply_suggestion(conn, int(aid), anotes)
                st.rerun()

    if not recent_applied.empty:
        st.subheader("Recently applied")
        render_dataframe("suggestions_recent_applied", recent_applied, width="stretch")

with rules_tab:
    if st.button("Run deterministic rules engine", type="primary"):
        with get_conn() as conn:
            summary = run_rules_engine(conn)
        st.success(f"Rules run: created={summary['created']} evaluated={summary['evaluated']} duplicates={summary['duplicates_skipped']} invalid={summary['invalid_or_skipped']}")
        render_dataframe("rules_engine_results", summary.get("rule_results", []), width="stretch", hide_index=True)
        signal = summary.get("provider_failure_signal") or {}
        if signal:
            st.caption(f"Provider signal: {signal}")

with ai_tab:
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

with scanner_tab:
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
            selected_audit_ticker = st.selectbox("Selected scanner candidate", options=view["ticker"].tolist(), key="scanner_audit_selected_ticker")
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
                    st.success(f"Saved `{result['review_state']}` state for {result['ticker']}.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not save candidate review state. {exc}")

            draft_store = st.session_state.setdefault("scanner_research_drafts", {})
            debug_store = st.session_state.setdefault("scanner_research_debug", {})
            existing_draft = draft_store.get(selected_audit_ticker)
            generate_label = "Regenerate Research Draft" if existing_draft else "Generate Research Draft"
            if existing_draft:
                debug_entry = dict(debug_store.get(selected_audit_ticker) or {})
                debug_entry.update(
                    {
                        "ticker": selected_audit_ticker,
                        "generated_at": existing_draft.get("generated_at"),
                        "research_mode": existing_draft.get("research_mode"),
                        "draft_source": "reused_session_draft",
                    }
                )
                debug_store[selected_audit_ticker] = debug_entry
            if st.button(generate_label):
                try:
                    with get_conn() as conn:
                        draft, reused = get_or_create_scanner_research_draft(
                            conn,
                            selected_audit_ticker,
                            existing_draft=existing_draft,
                            force_refresh=bool(existing_draft),
                        )
                    draft_store[selected_audit_ticker] = draft
                    debug_store[selected_audit_ticker] = {
                        "ticker": selected_audit_ticker,
                        "generated_at": draft.get("generated_at"),
                        "research_mode": draft.get("research_mode"),
                        "draft_source": "reused_session_draft" if reused else "fresh_generation",
                    }
                    st.session_state["scanner_research_feedback"] = {
                        "level": "success",
                        "message": (
                            f"Reused existing advisory research draft for {selected_audit_ticker}."
                            if reused
                            else f"Generated advisory research draft for {selected_audit_ticker}."
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
                    f"Research Mode: `{mode_label}` | "
                    f"recommended_action=`{existing_draft.get('recommended_action') or 'watch_only'}` | "
                    f"confidence=`{existing_draft.get('confidence') or 'low'}`"
                )
                st.caption(meta_caption)
                debug_entry = debug_store.get(selected_audit_ticker) or {}
                if debug_entry:
                    st.caption(
                        "Debug: "
                        f"ticker=`{debug_entry.get('ticker') or selected_audit_ticker}` | "
                        f"generated_at=`{debug_entry.get('generated_at') or existing_draft.get('generated_at') or 'n/a'}` | "
                        f"mode=`{debug_entry.get('research_mode') or research_mode}` | "
                        f"draft_source=`{debug_entry.get('draft_source') or 'reused_session_draft'}`"
                    )
                if research_mode != "openai" and existing_draft.get("fallback_reason"):
                    st.info(f"Used heuristic fallback: {existing_draft.get('fallback_reason')}")
                rd1, rd2 = st.columns(2)
                with rd1:
                    st.write(f"**Company:** {existing_draft.get('company_name') or selected_audit_ticker}")
                    st.write(f"**Description:** {existing_draft.get('short_company_description') or 'No description available.'}")
                    st.write(
                        "**Similar tickers:** "
                        + (", ".join(existing_draft.get("possible_similar_tickers") or []) or "None suggested")
                    )
                    st.write(f"**Possible new theme:** {existing_draft.get('possible_new_theme') or 'None suggested'}")
                with rd2:
                    st.write(f"**Rationale:** {existing_draft.get('rationale') or 'No rationale provided.'}")
                    caveats = existing_draft.get("caveats") or []
                    st.write("**Caveats:** " + (" | ".join(caveats) if caveats else "None"))

                suggested_themes = existing_draft.get("suggested_existing_themes") or []
                if suggested_themes:
                    st.markdown("**Suggested Existing Themes**")
                    selected_suggested_theme_ids: list[int] = []
                    for item in suggested_themes:
                        theme_id = int(item.get("theme_id"))
                        label = f"{item.get('theme_name')} ({item.get('category')})"
                        checked = st.checkbox(
                            label,
                            value=False,
                            key=f"scanner_suggested_theme_{selected_audit_ticker}_{theme_id}",
                        )
                        st.caption(
                            (str(item.get("why_it_might_fit") or "Possible governed-theme fit.") + " | ")
                            + ("representative tickers: " + ", ".join(item.get("representative_tickers") or []) if item.get("representative_tickers") else "no representative tickers shown")
                        )
                        if checked:
                            selected_suggested_theme_ids.append(theme_id)
                else:
                    selected_suggested_theme_ids = []
                    st.info("No strong existing governed-theme match was suggested from the available context.")

                available_custom_existing = [
                    option for option in theme_options if int(option["id"]) not in {int(item.get("theme_id")) for item in suggested_themes}
                ]
                custom_existing_selection = st.multiselect(
                    "Add custom existing themes",
                    options=available_custom_existing,
                    format_func=lambda option: f"{option['name']} [{option['id']}]",
                    key=f"scanner_custom_existing_{selected_audit_ticker}",
                )
                custom_existing_theme_ids = [int(item["id"]) for item in custom_existing_selection]
                custom_new_theme_raw = st.text_input(
                    "Add custom proposed new themes",
                    value="",
                    placeholder="Comma-separated new theme ideas",
                    key=f"scanner_custom_new_{selected_audit_ticker}",
                )
                custom_new_themes = sorted(
                    {
                        item.strip()
                        for item in custom_new_theme_raw.replace(";", ",").split(",")
                        if item.strip()
                    }
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
                else:
                    st.caption("No theme ideas selected yet. You can choose suggested themes, add custom existing themes, or enter custom new-theme labels.")
            else:
                selected_suggested_theme_ids = []
                custom_existing_theme_ids = []
                custom_new_themes = []

            promotion_note = st.text_area(
                "Promotion note (optional)",
                value="",
                placeholder="Why it looks interesting, suspected theme/category, or any caution/uncertainty.",
                key=f"scanner_audit_promotion_note_{selected_audit_ticker}",
            )
            can_promote = not bool(selected_audit_row["is_governed"])
            st.caption(
                "Promotion creates or refreshes a staged review candidate only. "
                "It carries forward the advisory draft and Scanner Audit evidence, and it does not modify governed theme membership."
            )
            if not can_promote:
                st.info("This candidate is already governed. Promotion is reserved for uncovered candidates.")
            send_disabled = (not can_promote) or (existing_draft is None)
            if existing_draft is None:
                st.info("Generate a research draft first to promote an agent-assisted review candidate.")
            if st.button("Send Selected Suggestions to Theme Review", disabled=send_disabled):
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
                        )
                    st.success(str(result["message"]))
                    st.caption("Selected ideas were sent to staged Theme Review only. No governed membership was changed.")
                except Exception as exc:
                    st.error(f"Could not send candidate to Theme Review. {exc}")

show_perf_summary()
