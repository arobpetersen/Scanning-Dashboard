import streamlit as st

from src.database import get_conn, init_db
from src.rules_engine import run_rules_engine
from src.suggestions_service import (
    SuggestionPayload,
    apply_suggestion,
    create_suggestion,
    list_suggestions,
    recent_applied_suggestions,
    review_suggestion,
    suggestion_status_counts,
)
from src.theme_service import get_theme_members, list_themes, seed_if_needed

st.set_page_config(page_title="Suggestions", layout="wide")
st.title("Suggestions / Review Queue")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

theme_options = themes[["id", "name"]].to_dict("records")

st.subheader("Rules Engine")
if st.button("Run deterministic rules engine"):
    with get_conn() as conn:
        summary = run_rules_engine(conn)
    st.success(
        f"Rules run complete: evaluated={summary['evaluated']}, created={summary['created']}, duplicates_skipped={summary['duplicates_skipped']}, invalid_or_skipped={summary['invalid_or_skipped']}"
    )
    if summary["rule_results"]:
        st.caption("Rule-level outcomes")
        st.dataframe(summary["rule_results"], width="stretch", hide_index=True)
    if summary["errors"]:
        st.warning("Some rule outputs were skipped:")
        st.code("\n".join(summary["errors"]))
    st.rerun()

st.subheader("Create Suggestion")
suggestion_type = st.selectbox(
    "Suggestion type",
    [
        "add_ticker_to_theme",
        "remove_ticker_from_theme",
        "create_theme",
        "rename_theme",
        "move_ticker_between_themes",
        "review_theme",
    ],
)
source = st.selectbox("Source", ["manual", "rules_engine", "ai_proposal", "imported"], index=0)
rationale = st.text_area("Rationale", value="")

selected_existing_theme = None
selected_target_theme = None
proposed_theme_name = None
proposed_ticker = None
current_members: list[str] = []

if suggestion_type in {"add_ticker_to_theme", "remove_ticker_from_theme", "rename_theme", "move_ticker_between_themes", "review_theme"}:
    selected_existing_theme = st.selectbox(
        "Existing theme",
        options=theme_options,
        format_func=lambda t: f"{t['name']} [{t['id']}]",
    )
    with get_conn() as conn:
        current_members = get_theme_members(conn, int(selected_existing_theme["id"]))["ticker"].tolist()

if suggestion_type == "rename_theme" and selected_existing_theme:
    st.caption(f"Current theme name: **{selected_existing_theme['name']}**")

if suggestion_type == "add_ticker_to_theme":
    proposed_ticker = st.text_input("Ticker to add", value="")
    with st.expander("Current theme members", expanded=False):
        if not current_members:
            st.info("Theme currently has no members.")
        else:
            st.write(", ".join(current_members))

if suggestion_type == "remove_ticker_from_theme":
    if not current_members:
        st.warning("Selected theme has no members to remove.")
    else:
        proposed_ticker = st.selectbox("Ticker to remove", options=current_members)

if suggestion_type == "move_ticker_between_themes":
    if not current_members:
        st.warning("Selected source theme has no members to move.")
    else:
        proposed_ticker = st.selectbox("Ticker to move", options=current_members)
    selected_target_theme = st.selectbox(
        "Target theme",
        options=theme_options,
        format_func=lambda t: f"{t['name']} [{t['id']}]",
    )

if suggestion_type in {"create_theme", "rename_theme"}:
    proposed_theme_name = st.text_input("Proposed theme name", value="")
if suggestion_type == "review_theme":
    proposed_ticker = st.text_input("Ticker context (optional)", value="")

if st.button("Create suggestion"):
    try:
        payload = SuggestionPayload(
            suggestion_type=suggestion_type,
            source=source,
            rationale=rationale,
            proposed_theme_name=proposed_theme_name,
            proposed_ticker=proposed_ticker,
            existing_theme_id=int(selected_existing_theme["id"]) if selected_existing_theme else None,
            proposed_target_theme_id=int(selected_target_theme["id"]) if selected_target_theme else None,
        )
        with get_conn() as conn:
            suggestion_id = create_suggestion(conn, payload)
        st.success(f"Suggestion created: {suggestion_id}")
        st.rerun()
    except Exception as exc:
        st.error(f"Create failed: {exc}")

st.subheader("Queue Filters")
fc1, fc2, fc3, fc4 = st.columns(4)
with fc1:
    status_filter = st.selectbox("Status", ["all", "pending", "approved", "rejected", "applied"], index=0)
with fc2:
    type_filter = st.selectbox(
        "Type",
        [
            "all",
            "add_ticker_to_theme",
            "remove_ticker_from_theme",
            "create_theme",
            "rename_theme",
            "move_ticker_between_themes",
            "review_theme",
        ],
        index=0,
    )
with fc3:
    source_filter = st.selectbox("Source", ["all", "manual", "rules_engine", "ai_proposal", "imported"], index=0)
with fc4:
    search_filter = st.text_input("Search ticker/theme", value="")

with get_conn() as conn:
    queue = list_suggestions(conn, status_filter, type_filter, source_filter, search_filter)
    counts = suggestion_status_counts(conn)
    recent_applied = recent_applied_suggestions(conn, limit=10)

cmap = {row["status"]: int(row["cnt"]) for _, row in counts.iterrows()}
mc1, mc2, mc3, mc4 = st.columns(4)
mc1.metric("Pending", cmap.get("pending", 0))
mc2.metric("Approved", cmap.get("approved", 0))
mc3.metric("Rejected", cmap.get("rejected", 0))
mc4.metric("Applied", cmap.get("applied", 0))

st.subheader("Suggestions")
if queue.empty:
    st.info("No suggestions for selected filters.")
else:
    view_cols = [
        "suggestion_id",
        "status",
        "validation_status",
        "suggestion_type",
        "source",
        "proposed_ticker",
        "existing_theme_name",
        "target_theme_name",
        "proposed_theme_name",
        "rationale",
        "created_at",
        "reviewed_at",
        "reviewer_notes",
    ]
    st.dataframe(queue[view_cols], width="stretch")

    pending = queue[queue["status"] == "pending"]
    approved = queue[queue["status"] == "approved"]

    st.subheader("Review Pending Suggestion")
    if pending.empty:
        st.info("No pending suggestions in current filter.")
    else:
        pending_options = pending[["suggestion_id", "suggestion_type", "source", "validation_status"]].to_dict("records")
        selected_pending = st.selectbox(
            "Pending suggestion",
            options=pending_options,
            format_func=lambda r: f"#{r['suggestion_id']} | {r['suggestion_type']} | {r['source']} | {r['validation_status']}",
        )
        notes = st.text_area("Reviewer notes", value="", key="review_notes")
        rc1, rc2 = st.columns(2)
        with rc1:
            if st.button("Approve"):
                with get_conn() as conn:
                    review_suggestion(conn, int(selected_pending["suggestion_id"]), "approved", notes)
                st.success("Approved")
                st.rerun()
        with rc2:
            if st.button("Reject"):
                with get_conn() as conn:
                    review_suggestion(conn, int(selected_pending["suggestion_id"]), "rejected", notes)
                st.success("Rejected")
                st.rerun()

    st.subheader("Apply Approved Suggestion")
    if approved.empty:
        st.info("No approved suggestions available to apply.")
    else:
        approved_options = approved[["suggestion_id", "suggestion_type", "validation_status"]].to_dict("records")
        selected_approved = st.selectbox(
            "Approved suggestion",
            options=approved_options,
            format_func=lambda r: f"#{r['suggestion_id']} | {r['suggestion_type']} | {r['validation_status']}",
        )
        apply_notes = st.text_area("Application notes (optional)", value="", key="apply_notes")
        if st.button("Apply approved suggestion"):
            try:
                with get_conn() as conn:
                    apply_suggestion(conn, int(selected_approved["suggestion_id"]), apply_notes)
                st.success("Suggestion applied to theme registry")
                st.rerun()
            except Exception as exc:
                st.error(f"Apply failed: {exc}")

st.subheader("Recently Applied Suggestions")
if recent_applied.empty:
    st.info("No applied suggestions yet.")
else:
    st.dataframe(recent_applied, width="stretch")
