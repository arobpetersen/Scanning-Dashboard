import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.historical_backfill import reconstruct_theme_history_range
from src.leaderboard_utils import (
    build_category_leaderboard,
    build_category_theme_breakdown,
    build_current_leadership_table,
    build_current_performance_table,
    build_window_leaderboard,
)
from src.metric_formatting import display_or_dash, format_price, format_theme_ticker_table, human_readable_number, short_timestamp
from src.momentum_engine import compute_theme_momentum
from src.rankings import compute_current_ranking_snapshot
from src.queries import ticker_lookup_memberships, ticker_lookup_summary, theme_snapshot_history, theme_ticker_metrics
from src.streamlit_utils import extract_selected_row
from src.theme_selection import (
    SELECTED_THEME_ID_KEY,
    SELECTED_THEME_LABEL_KEY,
    SELECTED_THEME_SOURCE_KEY,
    describe_selection_source,
    resolve_theme_selection,
    set_theme_selection_state,
    should_apply_selection_token,
)
from src.theme_service import (
    add_ticker,
    active_ticker_universe,
    create_theme,
    delete_theme,
    get_theme_members,
    list_themes,
    remove_ticker,
    set_ticker_theme_assignments,
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

def _handled_selection_key(source: str) -> str:
    return f"{source}_handled_selection_token"


def _build_historical_leaderboard(momentum: dict, metric_col: str, metric_label: str) -> tuple[object, str | None]:
    ranked, msg = build_window_leaderboard(momentum, metric_col, top_k=10)
    if ranked.empty:
        return None, msg

    latest = momentum["history"].sort_values("snapshot_time").groupby("theme", as_index=False).tail(1)
    ranked = ranked.merge(latest[["theme", "theme_id", "category", "positive_1m_breadth_pct"]], on="theme", how="left")
    ranked = ranked.rename(columns={metric_col: metric_label, "positive_1m_breadth_pct": "breadth_1m"})
    return ranked[["rank", "theme_id", "theme", "category", metric_label, "momentum_score", "rank_change", "breadth_1m"]], None


def _set_theme_selection(theme_id: int, label: str, source: str) -> None:
    set_theme_selection_state(st.session_state, theme_id, label, source)


def _apply_dropdown_selection(id_by_label: dict[str, int]) -> None:
    label = st.session_state.get(SELECTED_THEME_LABEL_KEY)
    if label in id_by_label:
        _set_theme_selection(int(id_by_label[str(label)]), str(label), "manual_dropdown")


def _render_leaderboard(title: str, key_prefix: str, leaderboard_df, label_by_id: dict[int, str], show_advanced: bool):
    st.markdown(f"**{title}**")
    st.caption(
        "Ranked by performance first, then momentum score, then rank improvement. "
        "Breadth is contextual only and does not determine rank."
    )
    visible_cols = ["rank", "theme", "category", "performance", "momentum_score"]
    if show_advanced:
        visible_cols.extend(["rank_change", "breadth_1m"])
    event = st.dataframe(
        leaderboard_df[visible_cols],
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-cell",
        key=f"{key_prefix}_table",
    )

    row_idx = extract_selected_row(event)
    if row_idx is not None and 0 <= row_idx < len(leaderboard_df):
        picked_theme_id = int(leaderboard_df.iloc[row_idx]["theme_id"])
        picked_label = label_by_id.get(
            picked_theme_id,
            f"{leaderboard_df.iloc[row_idx]['theme']} ({leaderboard_df.iloc[row_idx]['category']})",
        )
        selection_token = f"{key_prefix}:{picked_theme_id}"
        handled_key = _handled_selection_key(key_prefix)
        if should_apply_selection_token(selection_token, st.session_state.get(handled_key)):
            _set_theme_selection(picked_theme_id, picked_label, key_prefix)
            st.session_state[handled_key] = selection_token


def _render_category_leaderboard(title: str, leaderboard_df) -> None:
    st.markdown(f"**{title}**")
    st.dataframe(
        leaderboard_df[["rank", "category", "top_themes", "contributing_themes", "performance", "momentum_score", "breadth_1m"]],
        width="stretch",
        hide_index=True,
    )
    st.caption(
        "Category rows are built by grouping the full eligible theme set for the selected window by category. "
        "`performance`, `momentum_score`, and `breadth_1m` are category-level averages across those grouped theme rows. "
        "`top_themes` previews the strongest underlying themes in that category for the same window. "
        "`contributing_themes` is the number of grouped theme rows included in the category summary and is informational; "
        "sorting is driven primarily by performance, then momentum and breadth, with `contributing_themes` only as a lower-priority tie-breaker."
    )


def _render_category_theme_drill(title: str, breakdown_df) -> None:
    if breakdown_df.empty:
        return

    with st.expander(f"Underlying themes — {title}", expanded=False):
        category_options = breakdown_df["category"].dropna().astype(str).tolist()
        picked_category = st.selectbox(
            f"Inspect category ({title})",
            options=category_options,
            key=f"category_drill_{title}",
        )
        category_rows = breakdown_df[breakdown_df["category"] == picked_category].copy().reset_index(drop=True)
        category_rows["rank"] = category_rows.index + 1
        st.dataframe(
            category_rows[["rank", "theme", "performance", "momentum_score", "breadth_1m"]],
            width="stretch",
            hide_index=True,
        )
        st.caption("These are the underlying eligible themes for the selected category/window, sorted by the same theme-level metrics used to build the category summary.")


def _render_current_leadership(leadership_df, label_by_id: dict[int, str]) -> None:
    st.subheader("Current Market Leadership")
    st.caption(
        "Ranks active themes by current confidence-adjusted composite strength using only eligible preferred-source contributors. "
        "`eligible_contributors` shows how many names actually fed the current rank, while `eligible_breadth_pct` shows the share of governed members that passed live ranking filters."
    )
    event = st.dataframe(
        leadership_df[
            [
                "rank",
                "theme",
                "category",
                "composite_score",
                "avg_1w",
                "avg_1m",
                "avg_3m",
                "breadth_1m",
                "ticker_count",
                "eligible_contributor_count",
                "eligible_breadth_pct",
                "leadership_quality",
            ]
        ],
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-cell",
        key="current_leadership_table",
    )
    row_idx = extract_selected_row(event)
    if row_idx is not None and 0 <= row_idx < len(leadership_df):
        picked_theme_id = int(leadership_df.iloc[row_idx]["theme_id"])
        picked_label = label_by_id.get(
            picked_theme_id,
            f"{leadership_df.iloc[row_idx]['theme']} ({leadership_df.iloc[row_idx]['category']})",
        )
        selection_token = f"current_leadership:{picked_theme_id}"
        handled_key = _handled_selection_key("current_leadership")
        if should_apply_selection_token(selection_token, st.session_state.get(handled_key)):
            _set_theme_selection(picked_theme_id, picked_label, "current_leadership")
            st.session_state[handled_key] = selection_token


def _render_current_performance(title: str, key_prefix: str, leaderboard_df, label_by_id: dict[int, str]) -> None:
    st.markdown(f"**{title}**")
    st.caption(
        "Ranks current active themes on the selected window return using eligible preferred-source contributors only. "
        "Displayed performance uses capped constituent returns for aggregation, but raw ticker rows remain unchanged in the detail table."
    )
    event = st.dataframe(
        leaderboard_df[
            [
                "rank",
                "theme",
                "category",
                "performance",
                "composite_score",
                "breadth_1m",
                "ticker_count",
                "eligible_contributor_count",
                "eligible_breadth_pct",
                "leadership_quality",
            ]
        ],
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-cell",
        key=f"{key_prefix}_current_table",
    )
    row_idx = extract_selected_row(event)
    if row_idx is not None and 0 <= row_idx < len(leaderboard_df):
        picked_theme_id = int(leaderboard_df.iloc[row_idx]["theme_id"])
        picked_label = label_by_id.get(
            picked_theme_id,
            f"{leaderboard_df.iloc[row_idx]['theme']} ({leaderboard_df.iloc[row_idx]['category']})",
        )
        selection_token = f"{key_prefix}:{picked_theme_id}"
        handled_key = _handled_selection_key(key_prefix)
        if should_apply_selection_token(selection_token, st.session_state.get(handled_key)):
            _set_theme_selection(picked_theme_id, picked_label, key_prefix)
            st.session_state[handled_key] = selection_token


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
        current_snapshot = compute_current_ranking_snapshot(conn)
        current_theme_metrics = current_snapshot["theme_metrics"]
        current_rankings = current_snapshot["rankings"]
        momentum_1w = compute_theme_momentum(conn, 7, top_n=20)
        momentum_1m = compute_theme_momentum(conn, 30, top_n=20)
    leadership_df = build_current_leadership_table(current_rankings, top_k=12)
    current_1w_df = build_current_performance_table(current_theme_metrics, "avg_1w", top_k=10)
    current_1m_df = build_current_performance_table(current_theme_metrics, "avg_1m", top_k=10)

    if leadership_df.empty:
        st.info("No active theme leadership data is available yet.")
    else:
        _render_current_leadership(leadership_df, label_by_id)

    st.divider()
    st.subheader("Current Top Themes By Window")
    st.caption("These are current live/preferred-source theme rankings, hardened for constituent eligibility, outlier control, and minimum contributor count.")
    current_c1, current_c2 = st.columns(2)
    with current_c1:
        if current_1w_df.empty:
            st.warning("Top Themes - Current 1W: No themes currently meet the eligible-contributor threshold.")
        else:
            _render_current_performance("Top Themes - Current 1W", "current_top_1w", current_1w_df, label_by_id)
    with current_c2:
        if current_1m_df.empty:
            st.warning("Top Themes - Current 1M: No themes currently meet the eligible-contributor threshold.")
        else:
            _render_current_performance("Top Themes - Current 1M", "current_top_1m", current_1m_df, label_by_id)

    lb1, lb1_msg = _build_historical_leaderboard(momentum_1w, "avg_1w", "performance")
    lb2, lb2_msg = _build_historical_leaderboard(momentum_1m, "avg_1m", "performance")
    st.divider()
    st.subheader("Theme Movement Snapshots")
    st.caption("These tables remain historical movement views built from snapshot windows. Use them to spot rotation and momentum change, not current live leadership.")
    leaderboard_mode = st.radio("Top table view", ["Themes", "Categories"], horizontal=True, key="themes_leaderboard_mode")
    show_advanced_leaderboard = st.checkbox(
        "Show advanced leaderboard context",
        value=False,
        key="themes_leaderboard_advanced",
        help="Adds secondary context columns beyond the default performance-first leaderboard view.",
    )
    category_lb1, category_lb1_msg = build_category_leaderboard(momentum_1w, "avg_1w", top_k=10)
    category_lb2, category_lb2_msg = build_category_leaderboard(momentum_1m, "avg_1m", top_k=10)
    category_breakdown_1w, _ = build_category_theme_breakdown(momentum_1w, "avg_1w")
    category_breakdown_1m, _ = build_category_theme_breakdown(momentum_1m, "avg_1m")

    c1, c2 = st.columns(2)
    with c1:
        if lb1 is None:
            st.warning(f"Top 10 Themes - 1W: {lb1_msg}")
        elif leaderboard_mode == "Categories":
            if category_lb1.empty:
                st.warning(f"Top Categories — 1W: {category_lb1_msg}")
            else:
                _render_category_leaderboard("Top Categories — 1W", category_lb1)
                _render_category_theme_drill("1W", category_breakdown_1w)
        else:
            _render_leaderboard("Top 10 Themes - 1W", "top_1w", lb1, label_by_id, show_advanced_leaderboard)
    with c2:
        if lb2 is None:
            st.warning(f"Top 10 Themes - 1M: {lb2_msg}")
        elif leaderboard_mode == "Categories":
            if category_lb2.empty:
                st.warning(f"Top Categories — 1M: {category_lb2_msg}")
            else:
                _render_category_leaderboard("Top Categories — 1M", category_lb2)
                _render_category_theme_drill("1M", category_breakdown_1m)
        else:
            _render_leaderboard("Top 10 Themes - 1M", "top_1m", lb2, label_by_id, show_advanced_leaderboard)
    if leaderboard_mode == "Categories":
        st.caption(
            "Category mode ranks categories from the full eligible theme set for the selected window, then shows the top category rows. "
            "Switch back to Themes mode to click a row into the detail view."
        )

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
        theme_current_row = current_theme_metrics[current_theme_metrics["theme_id"] == theme_id].copy()

    if ticker_df.empty:
        st.warning("No tickers for selected theme.")
    else:
        if not theme_current_row.empty:
            current_row = theme_current_row.iloc[0]
            qc1, qc2, qc3, qc4 = st.columns(4)
            qc1.metric("Governed tickers", int(current_row.get("ticker_count") or 0))
            qc2.metric("Current eligible", int(current_row.get("eligible_composite_count") or 0))
            qc3.metric("Eligible breadth", f"{float(current_row.get('eligible_breadth_pct') or 0):.1f}%")
            qc4.metric("Current quality", str(build_current_leadership_table(theme_current_row, top_k=1).iloc[0]["leadership_quality"]))
            st.caption(
                "Current theme ranking eligibility is separate from membership: governed members remain in the theme, "
                "but only eligible preferred-source contributors feed current ranking calculations."
            )
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
    ticker_feedback = st.session_state.pop("manage_ticker_feedback", None)
    if ticker_feedback:
        level = str(ticker_feedback.get("level") or "info")
        message = str(ticker_feedback.get("message") or "")
        if level == "success":
            st.success(message)
        elif level == "warning":
            st.warning(message)
        else:
            st.error(message)

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

    st.divider()
    st.subheader("Ticker Lookup")
    st.caption("Search one ticker at a time to inspect database presence, current assignments, and recent snapshot context.")
    lookup_raw = st.text_input("Ticker symbol", key="manage_ticker_lookup", placeholder="e.g. NVDA")
    lookup_ticker = lookup_raw.strip().upper()

    if not lookup_ticker:
        st.info("Enter a ticker to inspect membership, snapshot presence, and next manual action.")
    else:
        with get_conn() as conn:
            lookup = ticker_lookup_summary(conn, lookup_ticker)
            memberships = ticker_lookup_memberships(conn, lookup_ticker)

        if lookup.empty:
            st.warning("Ticker lookup did not return any rows.")
        else:
            row = lookup.iloc[0]
            st.write(f"**Status:** `{row['lookup_status']}` for `{lookup_ticker}`")
            l1, l2, l3, l4 = st.columns(4)
            l1.metric("Assigned themes", int(row.get("assigned_theme_count") or 0))
            l2.metric("In membership", "yes" if bool(row.get("exists_in_theme_membership")) else "no")
            l3.metric("In snapshots", "yes" if bool(row.get("exists_in_ticker_snapshots")) else "no")
            l4.metric("Seen elsewhere", "yes" if bool(row.get("exists_in_refresh_run_tickers") or row.get("exists_in_symbol_refresh_status")) else "no")

            detail = {
                "ticker": lookup_ticker,
                "latest_snapshot_time": short_timestamp(row.get("latest_snapshot_time")) or display_or_dash(None),
                "latest_snapshot_source": row.get("latest_snapshot_source") or "n/a",
                "latest_price": format_price(row.get("latest_price")) or display_or_dash(None),
                "latest_market_cap": human_readable_number(row.get("latest_market_cap")) or display_or_dash(None),
                "latest_avg_volume": human_readable_number(row.get("latest_avg_volume")) or display_or_dash(None),
            }
            st.dataframe([detail], width="stretch", hide_index=True)

            if not memberships.empty:
                st.caption("Assigned themes")
                st.dataframe(
                    memberships[["theme_name", "category", "is_active"]],
                    width="stretch",
                    hide_index=True,
                )
            elif str(row.get("lookup_status")) == "Not found":
                st.warning(f"`{lookup_ticker}` was not found in theme membership, ticker snapshots, refresh-run tickers, or symbol status.")
            else:
                st.info(f"`{lookup_ticker}` is present in the database but is not currently assigned to any theme.")

            if str(row.get("lookup_status")) == "Not found":
                st.caption("Next action: add this ticker with at least one theme assignment to create a managed membership record.")
            elif bool(row.get("exists_in_theme_membership")):
                st.caption("Next action: edit the ticker's current theme assignments below.")
            else:
                st.caption("Next action: assign this existing ticker to one or more themes below.")

            theme_options = {
                f"{theme_row['name']} ({theme_row['category']})": int(theme_row["id"])
                for _, theme_row in themes.iterrows()
            }
            selected_theme_ids = set(memberships["theme_id"].astype(int).tolist()) if not memberships.empty else set()
            selected_theme_labels = [label for label, theme_id in theme_options.items() if theme_id in selected_theme_ids]
            action_label = "Add ticker" if str(row.get("lookup_status")) == "Not found" else "Update ticker"

            st.markdown("**Ticker intake / edit**")
            st.caption(
                "Required: `ticker` and at least one theme assignment. "
                "Optional manual fields are not stored yet. Provider market data shown above remains read-only context."
            )
            with st.form("ticker_intake_edit_form"):
                form_ticker = st.text_input("Ticker (required)", value=lookup_ticker)
                form_theme_labels = st.multiselect(
                    "Theme assignments (required)",
                    list(theme_options.keys()),
                    default=selected_theme_labels,
                )
                backfill_recent_history = st.checkbox(
                    "Backfill recent ticker history (30d) and refresh affected theme history",
                    value=False,
                    help="Fetches and stores recent daily ticker history first, then refreshes only the affected reconstructed theme history.",
                )
                form_submitted = st.form_submit_button(action_label)

            if form_submitted:
                normalized_form_ticker = form_ticker.strip().upper()
                if not normalized_form_ticker:
                    st.error("Ticker is required.")
                elif not form_theme_labels:
                    st.error("Select at least one theme assignment.")
                else:
                    chosen_theme_ids = [int(theme_options[label]) for label in form_theme_labels]
                    try:
                        with get_conn() as conn:
                            result = set_ticker_theme_assignments(conn, normalized_form_ticker, chosen_theme_ids)
                            backfill_result = None
                            if backfill_recent_history and int(result["added_count"]) > 0:
                                backfill_result = reconstruct_theme_history_range(
                                    conn,
                                    provider_name="live",
                                    start_date=(pd.Timestamp.utcnow() - pd.Timedelta(days=30)).date().isoformat(),
                                    end_date=pd.Timestamp.utcnow().date().isoformat(),
                                    tickers=[normalized_form_ticker],
                                    theme_ids=list(result.get("affected_theme_ids", [])),
                                    provenance_source_label="ticker_intake_backfill",
                                    run_kind="ticker_intake_backfill",
                                    replace_existing=True,
                                )
                        if int(result["added_count"]) == 0 and int(result["removed_count"]) == 0:
                            st.session_state["manage_ticker_feedback"] = {
                                "level": "warning",
                                "message": (
                                    f"No membership changes were needed for `{result['ticker']}`. "
                                    f"It is already assigned to {int(result['assigned_theme_count'])} theme(s)."
                                ),
                            }
                        else:
                            extra = ""
                            if backfill_result is not None:
                                extra = (
                                    f" Stored {int(backfill_result.get('ticker_history_rows_written', 0))} ticker-day rows "
                                    f"(skipped {int(backfill_result.get('ticker_history_rows_skipped', 0))}) and refreshed "
                                    f"{int(backfill_result.get('snapshot_rows_written', 0))} reconstructed theme rows "
                                    f"(skipped {int(backfill_result.get('snapshot_rows_skipped', 0))})."
                                )
                            st.session_state["manage_ticker_feedback"] = {
                                "level": "success",
                                "message": (
                                    f"Saved `{result['ticker']}`: "
                                    f"{int(result['added_count'])} assignment(s) added, "
                                    f"{int(result['removed_count'])} removed."
                                    f"{extra}"
                                ),
                            }
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Ticker save failed: {exc}")

    st.divider()
    st.markdown("**Manual recent ticker-history backfill**")
    st.caption(
        "Use this after bulk or multi-ticker additions to batch-store recent ticker-day history and then refresh only the affected reconstructed theme history."
    )
    with get_conn() as conn:
        governed_tickers = active_ticker_universe(conn)

    with st.form("manual_ticker_history_backfill_form"):
        selected_backfill_tickers = st.multiselect(
            "Tickers to backfill (30d)",
            governed_tickers,
            help="Stores recent daily ticker history for the selected governed tickers, then refreshes affected reconstructed theme history in scope.",
        )
        manual_backfill_submitted = st.form_submit_button("Backfill recent ticker history")

    if manual_backfill_submitted:
        if not selected_backfill_tickers:
            st.warning("Select at least one ticker to backfill.")
        else:
            try:
                with get_conn() as conn:
                    manual_backfill_result = reconstruct_theme_history_range(
                        conn,
                        provider_name="live",
                        start_date=(pd.Timestamp.utcnow() - pd.Timedelta(days=30)).date().isoformat(),
                        end_date=pd.Timestamp.utcnow().date().isoformat(),
                        tickers=list(selected_backfill_tickers),
                        provenance_source_label="ticker_intake_backfill",
                        run_kind="ticker_intake_backfill_manual",
                        replace_existing=True,
                    )
                st.success(
                    "Recent ticker history stored and affected reconstructed theme history refreshed. "
                    f"Ticker-day rows written: {int(manual_backfill_result.get('ticker_history_rows_written', 0))}, "
                    f"skipped: {int(manual_backfill_result.get('ticker_history_rows_skipped', 0))}. "
                    f"Theme rows written: {int(manual_backfill_result.get('snapshot_rows_written', 0))}, "
                    f"skipped: {int(manual_backfill_result.get('snapshot_rows_skipped', 0))}."
                )
            except Exception as exc:
                st.error(f"Manual ticker-history backfill failed: {exc}")
