import streamlit as st

from src.database import get_conn, init_db
from src.leaderboard_utils import build_window_leaderboard
from src.metric_formatting import display_or_dash, format_theme_ticker_table
from src.momentum_engine import compute_theme_momentum
from src.queries import theme_snapshot_history, theme_ticker_metrics
from src.theme_selection import describe_selection_source, resolve_theme_selection
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


SELECTED_THEME_ID_KEY = "selected_theme_id"
SELECTED_THEME_LABEL_KEY = "explore_theme"
SELECTED_THEME_SOURCE_KEY = "selected_theme_source"


def _extract_selected_row(event) -> int | None:
    """Best-effort extraction of selected row index across Streamlit selection payload shapes."""
    selection = {}
    if isinstance(event, dict):
        selection = event.get("selection", {}) or {}
    elif hasattr(event, "selection"):
        selection = event.selection

    row_candidates = []

    rows = selection.get("rows", []) if isinstance(selection, dict) else getattr(selection, "rows", [])
    row_candidates.extend(rows or [])

    cells = selection.get("cells", []) if isinstance(selection, dict) else getattr(selection, "cells", [])
    for cell in cells or []:
        if isinstance(cell, dict):
            row_candidates.append(cell.get("row"))
        elif isinstance(cell, (tuple, list)) and cell:
            row_candidates.append(cell[0])
        elif hasattr(cell, "row"):
            row_candidates.append(getattr(cell, "row"))

    for value in row_candidates:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _build_leaderboard(momentum: dict, metric_col: str, metric_label: str) -> tuple[object, str | None]:
    ranked, msg = build_window_leaderboard(momentum, metric_col, top_k=10)
    if ranked.empty:
        return None, msg

    latest = momentum["history"].sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
    ranked = ranked.merge(latest[["theme", "theme_id", "category", "positive_1m_breadth_pct"]], on="theme", how="left")
    ranked = ranked.rename(columns={metric_col: metric_label, "positive_1m_breadth_pct": "breadth_1m"})
    return ranked[["rank", "theme_id", "theme", "category", metric_label, "momentum_score", "breadth_1m"]], None


def _set_theme_selection(theme_id: int, label: str, source: str) -> None:
    st.session_state[SELECTED_THEME_ID_KEY] = int(theme_id)
    st.session_state[SELECTED_THEME_LABEL_KEY] = label
    st.session_state[SELECTED_THEME_SOURCE_KEY] = source


def _apply_dropdown_selection(id_by_label: dict[str, int]) -> None:
    label = st.session_state.get(SELECTED_THEME_LABEL_KEY)
    if label in id_by_label:
        _set_theme_selection(int(id_by_label[str(label)]), str(label), "manual_dropdown")


def _render_leaderboard(title: str, key_prefix: str, leaderboard_df, label_by_id: dict[int, str]):
    st.markdown(f"**{title}**")
    event = st.dataframe(
        leaderboard_df[["rank", "theme", "category", "performance", "momentum_score", "breadth_1m"]],
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-cell",
        key=f"{key_prefix}_table",
    )

    row_idx = _extract_selected_row(event)
    if row_idx is not None and 0 <= row_idx < len(leaderboard_df):
        picked_theme_id = int(leaderboard_df.iloc[row_idx]["theme_id"])
        picked_label = label_by_id.get(
            picked_theme_id,
            f"{leaderboard_df.iloc[row_idx]['theme']} ({leaderboard_df.iloc[row_idx]['category']})",
        )
        _set_theme_selection(picked_theme_id, picked_label, key_prefix)


explore_tab, manage_tab = st.tabs(["Explore", "Manage"])

with explore_tab:
    options = {f"{r['name']} ({r['category']})": int(r["id"]) for _, r in themes.iterrows()}
    label_by_id = {v: k for k, v in options.items()}
    id_by_label = dict(options)
    fallback_theme_id = int(themes.iloc[0]["id"])
    selected_theme_id, selected_theme_label = resolve_theme_selection(
        st.session_state.get(SELECTED_THEME_ID_KEY),
        st.session_state.get(SELECTED_THEME_LABEL_KEY),
        label_by_id,
        id_by_label,
        fallback_theme_id,
    )
    if st.session_state.get(SELECTED_THEME_ID_KEY) != selected_theme_id:
        st.session_state[SELECTED_THEME_ID_KEY] = selected_theme_id
    if st.session_state.get(SELECTED_THEME_LABEL_KEY) != selected_theme_label:
        st.session_state[SELECTED_THEME_LABEL_KEY] = selected_theme_label
    if SELECTED_THEME_SOURCE_KEY not in st.session_state:
        st.session_state[SELECTED_THEME_SOURCE_KEY] = "default"

    with get_conn() as conn:
        momentum_1w = compute_theme_momentum(conn, 7, top_n=20)
        momentum_1m = compute_theme_momentum(conn, 30, top_n=20)

    lb1, lb1_msg = _build_leaderboard(momentum_1w, "avg_1w", "performance")
    lb2, lb2_msg = _build_leaderboard(momentum_1m, "avg_1m", "performance")

    c1, c2 = st.columns(2)
    with c1:
        if lb1 is None:
            st.warning(f"Top 10 Themes - 1W: {lb1_msg}")
        else:
            _render_leaderboard("Top 10 Themes - 1W", "top_1w", lb1, label_by_id)
    with c2:
        if lb2 is None:
            st.warning(f"Top 10 Themes - 1M: {lb2_msg}")
        else:
            _render_leaderboard("Top 10 Themes - 1M", "top_1m", lb2, label_by_id)

    st.divider()

    labels = list(options.keys())
    selection = st.selectbox(
        "Theme detail view",
        labels,
        key=SELECTED_THEME_LABEL_KEY,
        on_change=_apply_dropdown_selection,
        args=(id_by_label,),
    )
    theme_id = int(options[selection])
    st.caption(f"Selected from: {describe_selection_source(st.session_state.get(SELECTED_THEME_SOURCE_KEY))}")

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

        display_ticker_df = format_theme_ticker_table(ticker_df)
        for perf_col in ("perf_1w", "perf_1m", "perf_3m"):
            if perf_col in display_ticker_df.columns:
                display_ticker_df[perf_col] = display_ticker_df[perf_col].apply(
                    lambda v: display_or_dash(None) if v is None else (display_or_dash(None) if str(v) == "nan" else f"{float(v):.2f}%")
                )

        cols = [
            c
            for c in [
                "ticker",
                "price",
                "perf_1w",
                "perf_1m",
                "perf_3m",
                "market_cap",
                "avg_volume",
                "dollar_volume",
                "short_interest_pct",
                "float_shares",
                "adr_pct",
                "last_updated",
                "snapshot_time",
                "latest_refresh_time",
            ]
            if c in display_ticker_df.columns
        ]

        rename_map = {
            "last_updated": "market_data_time",
            "snapshot_time": "snapshot_time",
            "latest_refresh_time": "last_refresh_time",
        }
        view_df = display_ticker_df[cols].rename(columns=rename_map) if cols else display_ticker_df

        for nullable_col in ("short_interest_pct", "float_shares", "adr_pct"):
            if nullable_col in view_df.columns:
                view_df[nullable_col] = view_df[nullable_col].apply(display_or_dash)

        st.caption(
            "`market_data_time` is the provider market-data timestamp. "
            "`snapshot_time` is when the preferred-source ticker snapshot row was captured. "
            "`last_refresh_time` is the latest completed refresh in the current ticker-source view."
        )
        st.dataframe(view_df, width="stretch")
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
