import streamlit as st

from src.database import get_conn, init_db
from src.momentum_engine import compute_theme_momentum
from src.queries import theme_snapshot_history, theme_ticker_metrics
from src.theme_service import (
    add_ticker,
    create_theme,
    delete_theme,
    get_theme_members,
    list_themes,
    remove_ticker,
    seed_if_needed,
    update_theme,
)

st.set_page_config(page_title="Themes", layout="wide")
st.title("Themes")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)
    themes = list_themes(conn, active_only=False)

if themes.empty:
    st.info("No themes found.")
    st.stop()


def _build_leaderboard(momentum: dict, metric_col: str, metric_label: str) -> tuple[object, str | None]:
    history = momentum["history"]
    if history.empty:
        return None, "No snapshots available yet. Run refreshes first."

    snapshot_count = int(history["snapshot_time"].nunique())
    if snapshot_count < 2:
        return None, "Not enough snapshots in this window (need at least 2)."

    latest = history.sort_values("snapshot_time").groupby("theme_id", as_index=False).tail(1)
    ranked = latest.sort_values(metric_col, ascending=False).head(10).copy()

    summary_cols = ["theme", "momentum_score", "delta_breadth"]
    summary = momentum["window_summary"][summary_cols].copy() if not momentum["window_summary"].empty else None
    if summary is not None:
        ranked = ranked.merge(summary, on="theme", how="left")
    else:
        ranked["momentum_score"] = None
        ranked["delta_breadth"] = None

    ranked = ranked.reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    ranked = ranked[["rank", "theme_id", "theme", "category", metric_col, "momentum_score", "positive_1m_breadth_pct"]].rename(
        columns={metric_col: metric_label, "positive_1m_breadth_pct": "breadth_1m"}
    )
    return ranked, None


def _render_leaderboard(title: str, key_prefix: str, leaderboard_df):
    st.markdown(f"**{title}**")
    st.dataframe(
        leaderboard_df[["rank", "theme", "category", "performance", "momentum_score", "breadth_1m"]],
        width="stretch",
        hide_index=True,
    )

    for row in leaderboard_df.itertuples(index=False):
        if st.button(
            f"Open #{row.rank} — {row.theme} ({row.performance:.2f}%)",
            key=f"{key_prefix}_{int(row.theme_id)}",
            use_container_width=True,
        ):
            st.session_state["selected_theme_id"] = int(row.theme_id)
            st.rerun()


explore_tab, manage_tab = st.tabs(["Explore", "Manage"])

with explore_tab:
    with get_conn() as conn:
        momentum_1w = compute_theme_momentum(conn, 7, top_n=20)
        momentum_1m = compute_theme_momentum(conn, 30, top_n=20)

    lb1, lb1_msg = _build_leaderboard(momentum_1w, "avg_1w", "performance")
    lb2, lb2_msg = _build_leaderboard(momentum_1m, "avg_1m", "performance")

    c1, c2 = st.columns(2)
    with c1:
        if lb1 is None:
            st.warning(f"Top 10 Themes — 1W: {lb1_msg}")
        else:
            _render_leaderboard("Top 10 Themes — 1W", "open_1w", lb1)
    with c2:
        if lb2 is None:
            st.warning(f"Top 10 Themes — 1M: {lb2_msg}")
        else:
            _render_leaderboard("Top 10 Themes — 1M", "open_1m", lb2)

    st.divider()

    options = {f"{r['name']} ({r['category']})": int(r["id"]) for _, r in themes.iterrows()}
    label_by_id = {v: k for k, v in options.items()}

    default_theme_id = st.session_state.get("selected_theme_id")
    if default_theme_id not in label_by_id:
        default_theme_id = int(themes.iloc[0]["id"])
        st.session_state["selected_theme_id"] = default_theme_id

    labels = list(options.keys())
    default_label = label_by_id[default_theme_id]
    selection = st.selectbox("Choose theme", labels, index=labels.index(default_label), key="explore_theme")
    theme_id = options[selection]
    st.session_state["selected_theme_id"] = theme_id

    with get_conn() as conn:
        ticker_df = theme_ticker_metrics(conn, theme_id)
        history_df = theme_snapshot_history(conn, theme_id, limit=50)

    if ticker_df.empty:
        st.warning("No tickers for selected theme.")
    else:
        if "perf_1w" in ticker_df.columns:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Ticker count", int(ticker_df.shape[0]))
            c2.metric("Avg 1W", f"{ticker_df['perf_1w'].mean():.2f}%")
            c3.metric("Avg 1M", f"{ticker_df['perf_1m'].mean():.2f}%")
            c4.metric("Avg 3M", f"{ticker_df['perf_3m'].mean():.2f}%")

        st.dataframe(ticker_df, width="stretch")
        if not history_df.empty:
            hist = history_df.sort_values("snapshot_time")
            st.line_chart(hist.set_index("snapshot_time")[["composite_score", "avg_1m", "positive_1m_breadth_pct"]])
            st.dataframe(history_df, width="stretch")

with manage_tab:
    st.subheader("Create Theme")
    with st.form("create_theme", clear_on_submit=True):
        new_name = st.text_input("Name")
        new_category = st.text_input("Category", value="Custom")
        new_is_active = st.checkbox("Active", value=True)
        create_submitted = st.form_submit_button("Create")

    if create_submitted:
        try:
            with get_conn() as conn:
                create_theme(conn, new_name, new_category, new_is_active)
            st.success("Theme created")
            st.rerun()
        except Exception as exc:
            st.error(f"Create failed: {exc}")

    labels = {f"{r['name']} [{r['id']}]": int(r["id"]) for _, r in themes.iterrows()}
    selected_label = st.selectbox("Select theme to manage", list(labels.keys()), key="manage_theme")
    selected_id = labels[selected_label]
    selected = themes[themes["id"] == selected_id].iloc[0]

    with st.form("edit_theme"):
        edit_name = st.text_input("Theme name", value=selected["name"])
        edit_category = st.text_input("Category", value=selected["category"])
        edit_active = st.checkbox("Active", value=bool(selected["is_active"]))
        c1, c2 = st.columns(2)
        with c1:
            save = st.form_submit_button("Save")
        with c2:
            remove = st.form_submit_button("Delete")

    if save:
        try:
            with get_conn() as conn:
                update_theme(conn, selected_id, edit_name, edit_category, edit_active)
            st.success("Theme updated")
            st.rerun()
        except Exception as exc:
            st.error(f"Update failed: {exc}")

    if remove:
        try:
            with get_conn() as conn:
                delete_theme(conn, selected_id)
            st.success("Theme deleted")
            st.rerun()
        except Exception as exc:
            st.error(f"Delete failed: {exc}")

    with get_conn() as conn:
        members = get_theme_members(conn, selected_id)

    c1, c2 = st.columns(2)
    with c1:
        with st.form("add_ticker_form", clear_on_submit=True):
            new_ticker = st.text_input("Add ticker")
            add_submitted = st.form_submit_button("Add")
        if add_submitted:
            try:
                with get_conn() as conn:
                    add_ticker(conn, selected_id, new_ticker)
                st.success(f"Added {new_ticker.strip().upper()}")
                st.rerun()
            except Exception as exc:
                st.error(f"Add ticker failed: {exc}")

    with c2:
        if members.empty:
            st.info("No members to remove.")
        else:
            with st.form("remove_ticker_form"):
                remove_tkr = st.selectbox("Remove ticker", members["ticker"].tolist())
                remove_submitted = st.form_submit_button("Remove")
            if remove_submitted:
                try:
                    with get_conn() as conn:
                        remove_ticker(conn, selected_id, remove_tkr)
                    st.success(f"Removed {remove_tkr}")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Remove ticker failed: {exc}")

    st.dataframe(members, width="stretch")
