import streamlit as st

from src.config import DEFAULT_PROVIDER, MASSIVE_API_KEY_ENV, massive_api_key
from src.database import get_conn, init_db
from src.fetch_data import RefreshBlockedError, run_refresh
from src.queries import last_refresh_run, synthetic_data_active
from src.rankings import compute_theme_rankings
from src.suggestions_service import suggestion_status_counts
from src.theme_service import active_ticker_universe, get_theme_members, list_themes, seed_if_needed

st.set_page_config(page_title="Theme Ops Dashboard", layout="wide")
st.title("Theme Operations Dashboard")
st.caption("Control center for refresh, rankings, review queue, and health signals.")

init_db()
with get_conn() as conn:
    seeded = seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

if seeded:
    st.success("Theme registry imported from themes_seed_structured.json. DuckDB is source of truth.")

provider_name = st.sidebar.selectbox("Provider", ["mock", "live"], index=0 if DEFAULT_PROVIDER == "mock" else 1)
scope_mode = st.sidebar.radio("Refresh scope", ["Active themes", "Selected theme", "Custom ticker list"], index=1 if provider_name == "live" else 0)

selected_theme_name: str | None = None
selected_tickers: list[str] | None = None
if scope_mode == "Selected theme":
    selected_theme = st.sidebar.selectbox(
        "Theme",
        options=themes[["id", "name", "is_active"]].sort_values("name").to_dict("records"),
        format_func=lambda t: f"{t['name']} ({'active' if t['is_active'] else 'inactive'})",
    )
    selected_theme_name = str(selected_theme["name"])
    with get_conn() as conn:
        selected_tickers = get_theme_members(conn, int(selected_theme["id"]))["ticker"].tolist()
elif scope_mode == "Custom ticker list":
    raw = st.sidebar.text_area("Tickers", value="AAPL, MSFT, NVDA")
    selected_tickers = sorted(set([p.strip().upper() for p in raw.replace("\n", " ").replace(",", " ").split(" ") if p.strip()]))

with get_conn() as conn:
    resolved_tickers = active_ticker_universe(conn) if scope_mode == "Active themes" else sorted(set(selected_tickers or []))
    last_run = last_refresh_run(conn)
    rankings = compute_theme_rankings(conn)
    sugg_counts = suggestion_status_counts(conn)
    synthetic_active = synthetic_data_active(conn)

if provider_name == "live" and not massive_api_key():
    st.warning(f"Live selected but {MASSIVE_API_KEY_ENV} is missing. Refresh will fall back to mock provider behavior.")

if synthetic_active:
    st.info("Synthetic historical data active")

if rankings.empty:
    st.warning("No ranking data yet. Run a refresh to generate snapshots.")

pending = int(sugg_counts[sugg_counts["status"] == "pending"]["cnt"].sum()) if not sugg_counts.empty else 0
obsolete = int(sugg_counts[sugg_counts["status"] == "obsolete"]["cnt"].sum()) if not sugg_counts.empty else 0

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Themes", int(rankings.shape[0]) if not rankings.empty else 0)
m2.metric("Active themes", int((rankings["is_active"] == True).sum()) if not rankings.empty else 0)
m3.metric("Pending suggestions", pending)
m4.metric("Obsolete suggestions", obsolete)
m5.metric("Scope tickers", len(resolved_tickers))

st.subheader("Refresh Control")
rc1, rc2 = st.columns([2, 3])
with rc1:
    st.write(f"**Provider:** `{provider_name}`")
    st.write(f"**Scope:** {scope_mode}")
    if selected_theme_name:
        st.write(f"**Theme:** {selected_theme_name}")
    st.write(f"**Tickers in scope:** {len(resolved_tickers)}")
    if st.button("Run refresh now", type="primary"):
        pb = st.progress(0)
        status = st.empty()

        def _progress(update: dict):
            total = update["total"] or 1
            pct = int((update["completed"] / total) * 100)
            pb.progress(min(100, pct))
            status.info(f"Run {update['run_id']} | {update['completed']}/{update['total']} | success={update['success']} fail={update['failure']}")

        try:
            with get_conn() as conn:
                run_id = run_refresh(
                    conn,
                    provider_name,
                    tickers=resolved_tickers,
                    progress_callback=_progress,
                    scope_type={"Active themes": "active_themes", "Selected theme": "selected_theme", "Custom ticker list": "custom_tickers"}[scope_mode],
                    scope_theme_name=selected_theme_name,
                )
            pb.progress(100)
            st.success(f"Refresh completed. Run ID: {run_id}")
            st.rerun()
        except RefreshBlockedError as exc:
            st.warning(f"Refresh blocked: {exc}")
        except Exception as exc:
            st.error(f"Refresh failed: {exc}")

with rc2:
    if not last_run.empty:
        run = last_run.iloc[0]
        st.info(f"Last run #{int(run['run_id'])} | provider={run['provider']} | status={run['status']} | success={int(run['success_count'])} | failures={int(run['failure_count'])}")
    else:
        st.info("No runs yet.")
    with st.expander("Resolved ticker universe"):
        st.code(", ".join(resolved_tickers) if resolved_tickers else "(none)")

st.subheader("Current Rankings")
if rankings.empty:
    st.stop()

top_n = st.slider("Top themes to display", min_value=5, max_value=50, value=20, step=5)
view = rankings.head(top_n).copy()
st.dataframe(
    view[["theme", "category", "ticker_count", "avg_1m", "positive_1m_breadth_pct", "composite_score", "delta_composite_score"]],
    width="stretch",
)
st.line_chart(view[["theme", "composite_score"]].set_index("theme"))

st.caption("Navigation: Themes, Historical Performance, Suggestions, and Health.")
