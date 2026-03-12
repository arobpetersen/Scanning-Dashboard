import streamlit as st

from src.ai_proposals import generate_ai_suggestions, sanitize_context
from src.config import AI_MAX_PROPOSALS, OPENAI_API_KEY_ENV, openai_api_key
from src.database import get_conn, init_db
from src.queries import theme_health_overview
from src.rankings import compute_theme_rankings
from src.rules_engine import run_rules_engine
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

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

theme_options = themes[["id", "name"]].to_dict("records")

manual_tab, queue_tab, rules_tab, ai_tab = st.tabs(["Manual", "Queue", "Rules", "AI"])

with manual_tab:
    suggestion_type = st.selectbox("Suggestion type", ["add_ticker_to_theme", "remove_ticker_from_theme", "create_theme", "rename_theme", "move_ticker_between_themes", "review_theme"])
    source = st.selectbox("Source", ["manual", "rules_engine", "ai_proposal", "imported"], index=0)
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
        source_filter = st.selectbox("Source", ["all", "manual", "rules_engine", "ai_proposal", "imported"], index=0)
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
        st.dataframe(queue[["suggestion_id", "status", "priority", "validation_status", "suggestion_type", "source", "proposed_ticker", "existing_theme_name", "proposed_theme_name", "rationale", "created_at", "reviewer_notes"]], width="stretch")

        pending = queue[queue["status"] == "pending"]
        approved = queue[queue["status"] == "approved"]
        if not pending.empty:
            selected = st.selectbox("Pending suggestion", options=pending["suggestion_id"].tolist())
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
            anotes = st.text_input("Apply notes", value="", key="anotes")
            if st.button("Apply approved"):
                with get_conn() as conn:
                    apply_suggestion(conn, int(aid), anotes)
                st.rerun()

    if not recent_applied.empty:
        st.subheader("Recently applied")
        st.dataframe(recent_applied, width="stretch")

with rules_tab:
    if st.button("Run deterministic rules engine", type="primary"):
        with get_conn() as conn:
            summary = run_rules_engine(conn)
        st.success(f"Rules run: created={summary['created']} evaluated={summary['evaluated']} duplicates={summary['duplicates_skipped']} invalid={summary['invalid_or_skipped']}")
        st.dataframe(summary.get("rule_results", []), width="stretch", hide_index=True)
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

    with get_conn() as conn:
        rankings = compute_theme_rankings(conn)
        health = theme_health_overview(conn, low_constituent_threshold=3, failure_window_days=14)

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
