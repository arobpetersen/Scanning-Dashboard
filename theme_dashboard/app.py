import json

import streamlit as st

from src.config import DEFAULT_PROVIDER, MASSIVE_API_KEY_ENV, massive_api_key
from src.database import get_conn, init_db
from src.fetch_data import RefreshBlockedError, run_refresh
from src.queries import last_refresh_run, synthetic_data_active
from src.streamlit_utils import db_cache_token, load_theme_rankings_cached, reset_perf_timings, render_dataframe, show_perf_summary, stop_for_database_error
from src.symbol_hygiene import refresh_eligible_tickers
from src.suggestions_service import suggestion_status_counts
from src.theme_service import active_ticker_universe, get_theme_members, list_themes, refresh_active_ticker_universe, seed_if_needed

st.set_page_config(page_title="Theme Ops Dashboard", layout="wide")
st.title("Theme Operations Dashboard")
st.caption("Control center for refresh, rankings, review queue, and health signals.")
reset_perf_timings("app")

try:
    init_db()
    with get_conn() as conn:
        seeded = seed_if_needed(conn)
        themes = list_themes(conn, active_only=False)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()

if seeded:
    st.success("Theme registry imported from themes_seed_structured.json. DuckDB is source of truth.")

provider_name = st.sidebar.selectbox("Provider", ["live", "mock"], index=0 if DEFAULT_PROVIDER == "live" else 1)
active_scope_label = "Live Active Themes" if provider_name == "live" else "Active Themes"
scope_mode = st.sidebar.radio("Refresh scope", [active_scope_label, "Selected theme", "Custom ticker list"], index=0)

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

try:
    with get_conn() as conn:
        requested_tickers = active_ticker_universe(conn) if scope_mode == active_scope_label else sorted(set(selected_tickers or []))
        resolved_tickers = refresh_active_ticker_universe(conn) if scope_mode == active_scope_label else sorted(set(selected_tickers or []))
        eligible_tickers, suppressed_scope_tickers = refresh_eligible_tickers(conn, requested_tickers)
        last_run = last_refresh_run(conn)
        sugg_counts = suggestion_status_counts(conn)
        synthetic_active = synthetic_data_active(conn)
except Exception as exc:
    stop_for_database_error(exc)
rankings = load_theme_rankings_cached(db_token)

if provider_name == "live" and not massive_api_key():
    st.warning(f"Live selected but {MASSIVE_API_KEY_ENV} is missing. Refresh will fall back to mock provider behavior, which is intended mainly for development/testing.")

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
m5.metric("Refresh-eligible tickers", len(eligible_tickers))

st.subheader("Refresh Control")
rc1, rc2 = st.columns([2, 3])
with rc1:
    st.write(f"**Provider:** `{provider_name}`")
    st.write(f"**Scope:** {scope_mode}")
    if selected_theme_name:
        st.write(f"**Theme:** {selected_theme_name}")
    st.write(f"**Requested scope tickers:** {len(requested_tickers)}")
    st.write(f"**Refresh-eligible tickers:** {len(eligible_tickers)}")
    if suppressed_scope_tickers:
        st.caption(
            f"{len(suppressed_scope_tickers)} ticker(s) are currently refresh-suppressed and excluded from active refresh scope."
        )
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
                    scope_type={active_scope_label: "active_themes", "Selected theme": "selected_theme", "Custom ticker list": "custom_tickers"}[scope_mode],
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
        api_calls = int(run.get('api_call_count') or 0)
        st.info(
            f"Last run #{int(run['run_id'])} | provider={run['provider']} | status={run['status']} "
            f"| success={int(run['success_count'])} | failures={int(run['failure_count'])} | api_calls={api_calls} "
            f"| flagged={int(run.get('flagged_symbol_count') or 0)} | suppressed={int(run.get('suppressed_symbol_count') or 0)}"
        )
        with st.expander("Refresh accounting (last run)"):
            endpoint_counts = {}
            raw = run.get('api_endpoint_counts')
            if raw:
                try:
                    endpoint_counts = json.loads(raw)
                except Exception:
                    endpoint_counts = {"raw": str(raw)}
            st.write("**Endpoint counts**", endpoint_counts if endpoint_counts else "None")
            st.write("**Skipped/failed tickers**", run.get('skipped_tickers') or "None")
            st.write("**Failure categories**", run.get('failure_category_counts') or "{}")
    else:
        st.info("No runs yet.")
    with st.expander("Resolved ticker universe"):
        st.write("Refresh-eligible tickers")
        st.code(", ".join(resolved_tickers) if resolved_tickers else "(none)")
        if suppressed_scope_tickers:
            st.write("Refresh-suppressed tickers excluded from this run")
            st.code(", ".join(sorted(suppressed_scope_tickers)))

st.subheader("Current Rankings")
if rankings.empty:
    st.stop()

top_n = st.slider("Top themes to display", min_value=5, max_value=50, value=20, step=5)
view = rankings.head(top_n).copy()
render_dataframe(
    "app_current_rankings",
    view[["theme", "category", "ticker_count", "avg_1m", "positive_1m_breadth_pct", "composite_score", "delta_composite_score"]],
    width="stretch",
)
st.line_chart(view[["theme", "composite_score"]].set_index("theme"))

st.caption("Navigation: Themes, Historical Performance, Suggestions, and Health.")
show_perf_summary()
