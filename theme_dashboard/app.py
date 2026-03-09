import streamlit as st

from src.config import DEFAULT_PROVIDER, FINNHUB_API_KEY_ENV, finnhub_api_key
from src.database import get_conn, init_db
from src.fetch_data import RefreshBlockedError, run_refresh
from src.queries import last_refresh_run
from src.rankings import compute_theme_rankings
from src.theme_service import active_ticker_universe, get_theme_members, list_themes, seed_if_needed

st.set_page_config(page_title="Thematic Stock Dashboard", layout="wide")
st.title("Thematic Stock Dashboard (Local v1)")

init_db()
with get_conn() as conn:
    seeded = seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

if seeded:
    st.success("Theme registry imported from themes_seed_structured.json (one-time). DuckDB is now source of truth.")

provider_name = st.sidebar.selectbox("Data provider", ["mock", "live"], index=0 if DEFAULT_PROVIDER == "mock" else 1)
show_trends = st.sidebar.checkbox("Show trend deltas vs prior snapshot", value=True)

scope_options = ["Active themes", "Selected theme", "Custom ticker list"]
default_scope_index = 1 if provider_name == "live" else 0
scope_mode = st.sidebar.radio("Refresh scope", scope_options, index=default_scope_index)

selected_theme_name: str | None = None
selected_tickers: list[str] | None = None
if scope_mode == "Selected theme":
    theme_options = themes[["id", "name", "is_active"]].sort_values("name")
    selected_theme = st.sidebar.selectbox(
        "Theme",
        options=theme_options.to_dict("records"),
        format_func=lambda t: f"{t['name']} ({'active' if t['is_active'] else 'inactive'})",
    )
    selected_theme_name = str(selected_theme["name"])
    with get_conn() as conn:
        selected_tickers = get_theme_members(conn, int(selected_theme["id"]))["ticker"].tolist()
elif scope_mode == "Custom ticker list":
    raw = st.sidebar.text_area("Tickers (comma or space separated)", value="AAPL, MSFT, NVDA")
    parts = [p.strip().upper() for p in raw.replace("\n", " ").replace(",", " ").split(" ") if p.strip()]
    selected_tickers = sorted(set(parts))

with get_conn() as conn:
    if scope_mode == "Active themes":
        resolved_tickers = active_ticker_universe(conn)
    else:
        resolved_tickers = sorted(set(selected_tickers or []))

live_key_present = bool(finnhub_api_key())
if provider_name == "live" and not live_key_present:
    st.warning(
        f"Live provider selected but {FINNHUB_API_KEY_ENV} is not set. Refresh will gracefully fall back to mock data."
    )

scope_type = {
    "Active themes": "active_themes",
    "Selected theme": "selected_theme",
    "Custom ticker list": "custom_tickers",
}[scope_mode]

col1, col2, col3 = st.columns(3)
with col1:
    st.write(f"**Provider selection:** `{provider_name}`")
    st.write(f"**Scope:** {scope_mode}")
    if selected_theme_name:
        st.write(f"**Selected theme:** {selected_theme_name}")
    st.write(f"**Tickers in scope:** {len(resolved_tickers)}")
    if provider_name == "live" and scope_mode == "Active themes":
        st.info("Live + Active themes can be slow and may hit rate limits. Prefer Selected theme or Custom ticker list.")
    with st.expander("Show resolved ticker universe"):
        st.code(", ".join(resolved_tickers) if resolved_tickers else "(none)")

with col2:
    if st.button("Refresh now", type="primary"):
        progress_bar = st.progress(0)
        status_box = st.empty()

        def _progress(update: dict):
            total = update["total"] if update["total"] else 1
            pct = int((update["completed"] / total) * 100)
            progress_bar.progress(min(100, pct))
            status_box.info(
                (
                    f"Run {update['run_id']} | provider={update['provider']} | "
                    f"{update['completed']}/{update['total']} tickers | "
                    f"success={update['success']} | failures={update['failure']} | "
                    f"elapsed={update['elapsed_seconds']:.1f}s"
                )
            )

        try:
            with get_conn() as conn:
                run_id = run_refresh(
                    conn,
                    provider_name,
                    tickers=resolved_tickers,
                    progress_callback=_progress,
                    scope_type=scope_type,
                    scope_theme_name=selected_theme_name,
                )
            progress_bar.progress(100)
            st.success(f"Refresh completed. Run ID: {run_id}")
            st.rerun()
        except RefreshBlockedError as exc:
            st.warning(f"Refresh not started: {exc}")
        except Exception as exc:
            st.error(f"Refresh failed: {exc}")

with get_conn() as conn:
    last_run = last_refresh_run(conn)
    rankings = compute_theme_rankings(conn)

with col3:
    if not last_run.empty:
        st.write(f"**Last refresh:** {last_run.iloc[0]['finished_at']}")
        st.write(f"**Last run provider used:** `{last_run.iloc[0]['provider']}`")
        st.write(f"**Last run status:** `{last_run.iloc[0]['status']}`")
    else:
        st.write("**Last refresh:** never")

if rankings.empty:
    st.warning("No ranking data yet. Click 'Refresh now' to populate snapshots.")
    st.stop()

summary_cols = st.columns(4)
summary_cols[0].metric("Themes", int(rankings.shape[0]))
summary_cols[1].metric("Active themes", int((rankings["is_active"] == True).sum()))
summary_cols[2].metric("Average 1M", f"{rankings['avg_1m'].mean():.2f}%")
summary_cols[3].metric("Avg positive 1M breadth", f"{rankings['positive_1m_breadth_pct'].mean():.1f}%")

search = st.text_input("Filter themes")
if search:
    rankings = rankings[rankings["theme"].str.contains(search, case=False, na=False)]

base_cols = [
    "theme",
    "category",
    "ticker_count",
    "avg_1w",
    "avg_1m",
    "avg_3m",
    "positive_1m_breadth_pct",
    "composite_score",
    "is_active",
]

trend_cols = ["delta_avg_1m", "delta_positive_1m_breadth_pct", "delta_composite_score"]
selected_cols = base_cols + trend_cols if show_trends else base_cols

view = rankings[selected_cols].rename(
    columns={
        "avg_1w": "avg 1W %",
        "avg_1m": "avg 1M %",
        "avg_3m": "avg 3M %",
        "positive_1m_breadth_pct": "positive 1M breadth %",
        "composite_score": "composite score",
        "delta_avg_1m": "Δ avg 1M",
        "delta_positive_1m_breadth_pct": "Δ positive 1M breadth",
        "delta_composite_score": "Δ composite score",
    }
)
st.dataframe(view, width="stretch")

with st.expander("Ranking and trend formulas (auditable)"):
    st.code(
        """
avg_1w = mean(perf_1w)
avg_1m = mean(perf_1m)
avg_3m = mean(perf_3m)
positive_1w_breadth_pct = percent(perf_1w > 0)
positive_1m_breadth_pct = percent(perf_1m > 0)
positive_3m_breadth_pct = percent(perf_3m > 0)
composite_score = 0.25 * avg_1w + 0.50 * avg_1m + 0.25 * avg_3m

delta_avg_1m = latest.avg_1m - previous.avg_1m
delta_positive_1m_breadth_pct = latest.positive_1m_breadth_pct - previous.positive_1m_breadth_pct
delta_composite_score = latest.composite_score - previous.composite_score
        """.strip()
    )
