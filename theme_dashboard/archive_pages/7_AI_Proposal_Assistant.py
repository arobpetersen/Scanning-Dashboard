import streamlit as st

from src.ai_proposals import generate_ai_suggestions
from src.config import AI_MAX_PROPOSALS, OPENAI_API_KEY_ENV
from src.database import get_conn, init_db
from src.queries import theme_health_overview
from src.rankings import compute_theme_rankings
from src.theme_service import list_themes, seed_if_needed

st.set_page_config(page_title="AI Proposal Assistant", layout="wide")
st.title("AI Proposal Assistant")
st.caption("Manual-only AI workflow. AI can only create suggestions (`source=ai_proposal`) into review queue.")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

scope = st.selectbox("Scope", ["Top-ranked themes", "Selected theme", "Custom tickers"], index=0)
selected_theme = None
custom_tickers = []

if scope == "Selected theme":
    selected_theme = st.selectbox(
        "Theme",
        options=themes[["id", "name"]].to_dict("records"),
        format_func=lambda r: f"{r['name']} [{r['id']}]",
    )
elif scope == "Custom tickers":
    raw = st.text_input("Tickers", value="AAPL, MSFT, NVDA")
    custom_tickers = sorted(set([x.strip().upper() for x in raw.replace(",", " ").split(" ") if x.strip()]))

proposal_types = st.multiselect(
    "Allowed suggestion types",
    ["add_ticker_to_theme", "remove_ticker_from_theme", "create_theme", "rename_theme", "review_theme"],
    default=["review_theme", "add_ticker_to_theme", "remove_ticker_from_theme"],
)

instruction = st.text_area(
    "AI instruction",
    value="Focus on high-quality, actionable taxonomy maintenance proposals with explicit evidence.",
)
max_props = st.slider("Max proposals", min_value=1, max_value=20, value=AI_MAX_PROPOSALS)

with get_conn() as conn:
    rankings = compute_theme_rankings(conn)
    health = theme_health_overview(conn, low_constituent_threshold=3, failure_window_days=14)

st.info(f"AI key required: `{OPENAI_API_KEY_ENV}`. If missing, generation is blocked for safety and transparency.")

if st.button("Generate AI proposals", type="primary"):
    context = {
        "scope": scope,
        "selected_theme": selected_theme,
        "custom_tickers": custom_tickers,
        "allowed_suggestion_types": proposal_types,
        "top_rankings": rankings.head(30).to_dict("records") if not rankings.empty else [],
        "theme_health": health.head(50).to_dict("records") if not health.empty else [],
    }
    prompt = (
        f"{instruction}\n"
        f"Only use suggestion types from: {proposal_types}.\n"
        "Do not produce weak suggestions; skip if evidence is insufficient."
    )
    try:
        with get_conn() as conn:
            summary = generate_ai_suggestions(conn, prompt=prompt, context=context, max_proposals=max_props)
        st.success(
            f"AI generation complete: attempted={summary['attempted']}, created={summary['created']}, duplicates={summary['duplicates']}, invalid={summary['invalid']}"
        )
        if summary["errors"]:
            st.warning("Some proposals were skipped:")
            st.code("\n".join(summary["errors"]))
    except Exception as exc:
        st.error(f"AI generation failed: {exc}")
