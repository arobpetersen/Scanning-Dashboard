import time

import pandas as pd
import streamlit as st

from src.database import get_bootstrap_conn, get_conn, init_db
from src.eod_refresh import run_scheduled_historical_append
from src.historical_backfill import reconstruct_theme_history_range
from src.leaderboard_utils import (
    build_category_leaderboard,
    build_category_theme_breakdown,
    build_current_leadership_table,
    build_current_performance_table,
    build_window_leaderboard,
    current_leadership_quality_label,
    disambiguate_theme_labels,
)
from src.metric_formatting import display_or_dash, format_price, format_theme_ticker_table, human_readable_number, short_timestamp
from src.queries import baseline_status, ticker_lookup_memberships, ticker_lookup_summary, theme_snapshot_history, theme_ticker_metrics
from src.streamlit_utils import (
    clear_current_market_view_caches,
    db_cache_token,
    extract_selected_row,
    load_current_ranking_snapshot_cached,
    load_theme_momentum_cached,
    prepare_post_mutation_refresh,
    queue_feedback_message,
    render_dataframe,
    render_feedback_message,
    reset_perf_timings,
    resolve_valid_selectbox_value,
    show_perf_summary,
    stop_for_database_error,
    unique_normalized_select_options,
)
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
    clear_manual_ticker_suppression,
    create_theme,
    delete_theme,
    get_theme_members,
    list_themes,
    remove_ticker,
    set_manual_ticker_suppression,
    set_ticker_theme_assignments,
    seed_if_needed,
    ticker_manual_suppression_state,
    update_theme,
)

st.set_page_config(page_title="Themes", layout="wide")
st.title("Themes")
reset_perf_timings("themes")

DAILY_HISTORICAL_APPEND_STALE_MINUTES = 45
try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        themes = list_themes(conn, active_only=False)
        baseline = baseline_status(conn)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()

if themes.empty:
    st.info("No themes found.")
    st.stop()

def _handled_selection_key(source: str) -> str:
    return f"{source}_handled_selection_token"


def _theme_option_maps(themes_df: pd.DataFrame) -> tuple[dict[str, int], dict[int, str], dict[str, int]]:
    base_label_by_id: dict[int, str] = {}
    base_counts: dict[str, int] = {}
    for _, row in themes_df.iterrows():
        theme_id = int(row["id"])
        base_label = f"{row['name']} ({row['category']})"
        base_label_by_id[theme_id] = base_label
        base_counts[base_label] = base_counts.get(base_label, 0) + 1

    label_by_id: dict[int, str] = {}
    for theme_id, base_label in base_label_by_id.items():
        label_by_id[theme_id] = f"{base_label} [#{theme_id}]" if base_counts.get(base_label, 0) > 1 else base_label

    options = {label: theme_id for theme_id, label in label_by_id.items()}
    return options, label_by_id, dict(options)


def _display_theme_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "theme" not in df.columns:
        return df
    out = disambiguate_theme_labels(df)
    if "theme_display" in out.columns:
        out["theme"] = out["theme_display"]
    return out


def _active_daily_historical_append_runs() -> pd.DataFrame:
    with get_bootstrap_conn() as conn:
        table_exists_row = conn.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'historical_reconstruction_runs'"
        ).fetchone()
        if not table_exists_row or int(table_exists_row[0]) == 0:
            return pd.DataFrame()
        active = conn.execute(
            """
            SELECT run_id, started_at, status, start_date, end_date, market_data_source
            FROM historical_reconstruction_runs
            WHERE status = 'running'
              AND run_kind = 'daily_historical_append'
            ORDER BY run_id DESC
            """
        ).df()
    if active.empty:
        return active
    started = pd.to_datetime(active["started_at"], errors="coerce")
    now = pd.Timestamp.utcnow().tz_localize(None)
    age_minutes = ((now - started).dt.total_seconds() / 60.0).round(1)
    active["age_minutes"] = age_minutes
    active["likely_stale"] = age_minutes >= float(DAILY_HISTORICAL_APPEND_STALE_MINUTES)
    return active


def _resolve_prior_daily_endpoint(history: pd.DataFrame) -> tuple[pd.DataFrame, object | None, object | None]:
    if history.empty or "theme_id" not in history.columns or "snapshot_time" not in history.columns:
        return pd.DataFrame(
            columns=[
                "theme_id",
                "prior_composite_score",
                "prior_avg_1w",
                "prior_avg_1m",
                "prior_breadth_1m",
            ]
        ), None, None

    dated = history.copy()
    dated["snapshot_date"] = pd.to_datetime(dated["snapshot_time"], errors="coerce").dt.date
    available_dates = sorted([value for value in dated["snapshot_date"].dropna().unique().tolist()], reverse=True)
    if len(available_dates) < 2:
        return pd.DataFrame(
            columns=[
                "theme_id",
                "prior_composite_score",
                "prior_avg_1w",
                "prior_avg_1m",
                "prior_breadth_1m",
            ]
        ), available_dates[0] if available_dates else None, None

    latest_date = available_dates[0]
    prior_date = available_dates[1]
    prior_rows = (
        dated[dated["snapshot_date"] == prior_date]
        .sort_values(["theme_id", "snapshot_time"])
        .groupby("theme_id", as_index=False)
        .tail(1)
        .rename(
            columns={
                "composite_score": "prior_composite_score",
                "avg_1w": "prior_avg_1w",
                "avg_1m": "prior_avg_1m",
                "positive_1m_breadth_pct": "prior_breadth_1m",
            }
        )
    )
    return prior_rows[
        [
            "theme_id",
            "prior_composite_score",
            "prior_avg_1w",
            "prior_avg_1m",
            "prior_breadth_1m",
        ]
    ], latest_date, prior_date


def _format_daily_delta_value(value, prior_value, *, is_percent: bool = False) -> str:
    if value is None or pd.isna(value):
        return "-"
    suffix = "%" if is_percent else ""
    rendered = f"{float(value):.2f}{suffix}"
    if prior_value is None or pd.isna(prior_value):
        return rendered
    delta = float(value) - float(prior_value)
    return f"{rendered} ({delta:+.2f}{suffix})"


def _apply_daily_delta_display(
    display_df: pd.DataFrame,
    prior_lookup: pd.DataFrame,
    *,
    value_map: dict[str, str],
    percent_cols: set[str] | None = None,
) -> pd.DataFrame:
    if display_df.empty:
        return display_df

    out = display_df.copy()
    if not prior_lookup.empty:
        out = out.merge(prior_lookup, on="theme_id", how="left")
    else:
        for prior_col in value_map.values():
            out[prior_col] = pd.NA

    percent_cols = percent_cols or set()
    for value_col, prior_col in value_map.items():
        if value_col not in out.columns:
            continue
        out[value_col] = out.apply(
            lambda row: _format_daily_delta_value(
                row.get(value_col),
                row.get(prior_col),
                is_percent=value_col in percent_cols,
            ),
            axis=1,
        )

    return out.drop(columns=[col for col in set(value_map.values()) if col in out.columns])


def _format_plain_value(value, *, is_percent: bool = False):
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, str):
        return value
    suffix = "%" if is_percent else ""
    return f"{float(value):.2f}{suffix}"


def _apply_plain_value_formatting(display_df: pd.DataFrame, *, percent_cols: set[str]) -> pd.DataFrame:
    if display_df.empty:
        return display_df
    out = display_df.copy()
    for col in percent_cols:
        if col not in out.columns:
            continue
        out[col] = out[col].apply(lambda value: _format_plain_value(value, is_percent=True))
    return out


def _build_historical_leaderboard(momentum: dict, metric_col: str, metric_label: str) -> tuple[object, str | None]:
    ranked, msg = build_window_leaderboard(momentum, metric_col, top_k=10)
    if ranked.empty:
        return None, msg

    latest = momentum["history"].sort_values(["snapshot_time", "theme"]).groupby("theme_id", as_index=False).tail(1)
    ranked = ranked.merge(
        latest[["theme_id", "category", "positive_1m_breadth_pct"]],
        on="theme_id",
        how="left",
        suffixes=("", "_latest"),
    )
    if "category_latest" in ranked.columns:
        ranked["category"] = ranked["category_latest"].where(ranked["category_latest"].notna(), ranked.get("category"))
        ranked = ranked.drop(columns=["category_latest"])
    ranked = ranked.rename(columns={metric_col: metric_label, "positive_1m_breadth_pct": "breadth_1m"})
    return ranked[["rank", "theme_id", "theme", "category", metric_label, "momentum_score", "rank_change", "breadth_1m"]], None


def _set_theme_selection(theme_id: int, label: str, source: str) -> None:
    set_theme_selection_state(st.session_state, theme_id, label, source)


def _apply_dropdown_selection(id_by_label: dict[str, int]) -> None:
    label = st.session_state.get(SELECTED_THEME_LABEL_KEY)
    if label in id_by_label:
        _set_theme_selection(int(id_by_label[str(label)]), str(label), "manual_dropdown")


def _render_leaderboard(
    title: str,
    key_prefix: str,
    leaderboard_df,
    label_by_id: dict[int, str],
    show_advanced: bool,
    *,
    show_daily_deltas: bool = False,
    prior_lookup: pd.DataFrame | None = None,
    performance_prior_col: str | None = None,
):
    st.markdown(f"**{title}**")
    st.caption(
        "Ranked by performance first, then momentum score, then rank improvement. "
        "This is a historical end-of-window table, so `performance` is the selected boundary-window snapshot metric, not a current eligible/capped rank metric. "
        "Breadth is contextual only and does not determine rank."
    )
    display_base = leaderboard_df
    if show_daily_deltas:
        value_map = {}
        if performance_prior_col:
            value_map["performance"] = performance_prior_col
        if value_map:
            display_base = _apply_daily_delta_display(
                leaderboard_df,
                prior_lookup if prior_lookup is not None else pd.DataFrame(),
                value_map=value_map,
                percent_cols={"performance"},
            )
    display_df = _apply_plain_value_formatting(
        _display_theme_table(display_base),
        percent_cols={"performance", "breadth_1m"},
    )
    visible_cols = ["rank", "theme", "category", "performance", "momentum_score"]
    if show_advanced:
        visible_cols.extend(["rank_change", "breadth_1m"])
    event = render_dataframe(
        f"{key_prefix}_leaderboard",
        display_df[visible_cols],
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
    render_dataframe(
        title,
        leaderboard_df[["rank", "category", "top_themes", "contributing_themes", "performance", "momentum_score", "breadth_1m"]],
        width="stretch",
        hide_index=True,
    )
    st.caption(
        "Category rows are built by grouping the full eligible theme set for the selected window by category. "
        "`performance`, `momentum_score`, and `breadth_1m` are category-level averages across those grouped theme rows. "
        "`top_themes` previews the strongest underlying themes in that category for the same window, with a minimal suffix only when duplicate names would otherwise look collapsed. "
        "`contributing_themes` is the number of grouped theme rows included in the category summary and is informational; "
        "sorting is driven primarily by performance, then momentum and breadth, with `contributing_themes` only as a lower-priority tie-breaker."
    )


def _render_category_theme_drill(title: str, breakdown_df) -> None:
    if breakdown_df.empty:
        return

    with st.expander(f"Underlying themes — {title}", expanded=False):
        category_options = unique_normalized_select_options(breakdown_df["category"].tolist())
        picked_category = st.selectbox(
            f"Inspect category ({title})",
            options=category_options,
            key=f"category_drill_{title}",
        )
        picked_category_key = str(picked_category or "").strip().casefold()
        category_rows = (
            breakdown_df[
                breakdown_df["category"].fillna("").astype(str).str.strip().str.casefold() == picked_category_key
            ]
            .copy()
            .reset_index(drop=True)
        )
        category_rows["rank"] = category_rows.index + 1
        display_rows = category_rows.copy()
        if "theme_display" in display_rows.columns:
            display_rows["theme"] = display_rows["theme_display"]
        render_dataframe(
            f"{title}_category_drill",
            display_rows[["rank", "theme", "performance", "momentum_score", "breadth_1m"]],
            width="stretch",
            hide_index=True,
        )
        st.caption("These are the underlying eligible themes for the selected category/window, sorted by the same theme-level metrics used to build the category summary.")


def _render_current_leadership(leadership_df, label_by_id: dict[int, str], *, show_daily_deltas: bool = False, prior_lookup: pd.DataFrame | None = None) -> None:
    st.subheader("Current Market Leadership")
    st.caption(
        "Ranks active themes by current confidence-adjusted composite strength using only eligible preferred-source contributors. "
        "`eligible_contributors` shows how many names actually fed the current rank, while `eligible_breadth_pct` shows the share of governed members that passed live ranking filters."
    )
    prior_daily_lookup = prior_lookup if prior_lookup is not None else pd.DataFrame()
    display_df = _display_theme_table(
        _apply_daily_delta_display(
            leadership_df,
            prior_daily_lookup,
            value_map={
                "composite_score": "prior_composite_score",
                "avg_1w": "prior_avg_1w",
                "avg_1m": "prior_avg_1m",
            },
            percent_cols={"avg_1w", "avg_1m"},
        )
        if show_daily_deltas
        else leadership_df
    )
    display_df = _apply_plain_value_formatting(
        display_df,
        percent_cols={"avg_1w", "avg_1m", "avg_3m", "breadth_1m", "eligible_breadth_pct"},
    )
    visible_cols = [
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
    event = render_dataframe(
        "current_leadership",
        display_df[visible_cols],
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


def _render_current_performance(
    title: str,
    key_prefix: str,
    leaderboard_df,
    label_by_id: dict[int, str],
    *,
    show_daily_deltas: bool = False,
    prior_lookup: pd.DataFrame | None = None,
    metric_col: str,
) -> None:
    st.markdown(f"**{title}**")
    st.caption(
        "Ranks current active themes on the selected window return using eligible preferred-source contributors only. "
        "Displayed performance uses capped constituent returns for aggregation, but raw ticker rows remain unchanged in the detail table."
    )
    prior_daily_lookup = prior_lookup if prior_lookup is not None else pd.DataFrame()
    display_base = (
        _apply_daily_delta_display(
            leaderboard_df,
            prior_daily_lookup,
            value_map={
                "composite_score": "prior_composite_score",
                "avg_1w": "prior_avg_1w",
                "avg_1m": "prior_avg_1m",
            },
            percent_cols={"avg_1w", "avg_1m"},
        )
        if show_daily_deltas
        else leaderboard_df
    )
    display_df = _display_theme_table(display_base)
    visible_cols = [
        "rank",
        "theme",
        "category",
        "performance",
        "composite_score",
    ]
    if show_daily_deltas:
        extra_cols: list[str] = []
        if "avg_1w" in display_df.columns and metric_col != "avg_1w":
            extra_cols.append("avg_1w")
        if "avg_1m" in display_df.columns and metric_col != "avg_1m":
            extra_cols.append("avg_1m")
        insert_at = len(visible_cols)
        for col in extra_cols:
            visible_cols.insert(insert_at, col)
            insert_at += 1
    display_df = _apply_plain_value_formatting(
        display_df,
        percent_cols={"performance", "avg_1w", "avg_1m", "breadth_1m", "eligible_breadth_pct"},
    )
    visible_cols.extend(
        [
            "breadth_1m",
            "ticker_count",
            "eligible_contributor_count",
            "eligible_breadth_pct",
            "leadership_quality",
        ]
    )
    event = render_dataframe(
        f"{key_prefix}_current",
        display_df[visible_cols],
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
    render_feedback_message(st.session_state, "themes_refresh_feedback")

    options, label_by_id, id_by_label = _theme_option_maps(themes)
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

    current_snapshot = load_current_ranking_snapshot_cached(db_token)
    current_theme_metrics = current_snapshot["theme_metrics"]
    current_rankings = current_snapshot["rankings"]
    momentum_1w = load_theme_momentum_cached(db_token, 7, top_n=20)
    momentum_1m = load_theme_momentum_cached(db_token, 30, top_n=20)
    baseline_row = baseline.iloc[0] if not baseline.empty else None
    current_driver_time = pd.to_datetime(current_theme_metrics["snapshot_time"]).dropna().max() if not current_theme_metrics.empty and "snapshot_time" in current_theme_metrics.columns else None
    movement_1w_end = momentum_1w.get("meta", {}).get("window_end")
    movement_1m_end = momentum_1m.get("meta", {}).get("window_end")
    freshness_c1, freshness_c2, freshness_c3, freshness_c4, freshness_c5 = st.columns([1.1, 1.1, 1.1, 1.0, 1.3])
    current_snapshot_label = short_timestamp(current_driver_time)
    if not current_snapshot_label and baseline_row is not None:
        current_snapshot_label = short_timestamp(baseline_row.get("latest_ticker_snapshot_time"))
    freshness_c1.metric("Current tables snapshot", current_snapshot_label or "-")
    freshness_c2.metric("1W movement end", short_timestamp(movement_1w_end) or "-")
    freshness_c3.metric("1M movement end", short_timestamp(movement_1m_end) or "-")
    active_append_runs = _active_daily_historical_append_runs()
    if not active_append_runs.empty and bool(active_append_runs.iloc[0].get("likely_stale")):
        stale_run = active_append_runs.iloc[0]
        st.warning(
            "A daily historical append run appears likely stale/orphaned. "
            f"run_id=`{int(stale_run['run_id'])}` has been marked `running` since `{short_timestamp(stale_run.get('started_at')) or '-'}` "
            f"({float(stale_run.get('age_minutes') or 0):.1f} minutes). "
            "The duplicate-run guard will block new Themes materialization attempts until this stale run is cleaned up manually."
        )
    with freshness_c4:
        if st.button("Reload latest DB state", key="themes_force_refresh"):
            clear_current_market_view_caches()
            queue_feedback_message(
                st.session_state,
                "themes_refresh_feedback",
                level="success",
                message=(
                    "Cleared cached Themes/Historical analytics and reran this page against the latest DB state. "
                    "This recomputes current ranking and movement tables in-memory from stored data only; it does not run upstream refreshes or rebuild snapshots."
                ),
            )
            st.rerun()
    with freshness_c5:
        if st.button("Materialize latest historical day", key="themes_force_latest_day_refresh"):
            if not active_append_runs.empty:
                active_run = active_append_runs.iloc[0]
                if bool(active_run.get("likely_stale")):
                    st.warning(
                        "Materialize latest historical day did not start because a daily historical append run looks stale/orphaned. "
                        f"Active run_id=`{int(active_run['run_id'])}` started=`{short_timestamp(active_run.get('started_at')) or '-'}` "
                        f"and has been running for about `{float(active_run.get('age_minutes') or 0):.1f}` minutes. "
                        "The duplicate-run guard will continue blocking new materialization attempts until the stale run is cleaned up manually."
                    )
                else:
                    st.warning(
                        "Materialize latest historical day did not start because a daily historical append run is already active. "
                        f"Active run_id=`{int(active_run['run_id'])}` started=`{short_timestamp(active_run.get('started_at')) or '-'}`. "
                        "Wait for the current append to finish before starting another one from Themes."
                    )
            else:
                status_container = st.status("Materialize latest historical day: started.", expanded=True)
                try:
                    status_container.write("Historical append step running against provider historical data for the latest trading day.")
                    with get_bootstrap_conn() as conn:
                        historical_append_result = run_scheduled_historical_append(conn, provider_name="live", force=True)

                    append_status = str((historical_append_result or {}).get("status") or "not_run")
                    append_rows_written = int((historical_append_result or {}).get("snapshot_rows_written") or 0)
                    append_ticker_rows_written = int((historical_append_result or {}).get("ticker_history_rows_written") or 0)
                    append_failed_tickers = list((historical_append_result or {}).get("failed_tickers") or [])
                    historical_append_ran = historical_append_result is not None
                    movement_likely_advanced = historical_append_ran and append_status in {"success", "partial", "no_op"}
                    feedback_level = "success"
                    if not historical_append_ran or append_status in {"failed", "no_scope"}:
                        feedback_level = "warning"
                    if append_status == "failed":
                        feedback_level = "error"
                    final_state = "success"
                    if append_status in {"no_op", "no_scope"}:
                        final_state = "no-op"
                    elif append_status == "partial":
                        final_state = "partial"
                    elif append_status == "failed":
                        final_state = "error"

                    status_container.write(
                        f"Historical append completed with status `{append_status}` | "
                        f"theme_rows_written=`{append_rows_written}` | "
                        f"ticker_rows_written=`{append_ticker_rows_written}`."
                    )
                    status_container.write("Cache clear and rerun preparation running.")

                    feedback_bits = [
                        (
                            f"Latest-day historical append ran with status=`{append_status}` | "
                            f"theme_rows_written=`{append_rows_written}` | "
                            f"ticker_rows_written=`{append_ticker_rows_written}`."
                            if historical_append_ran
                            else "Latest-day historical append did not run."
                        ),
                        (
                            "Movement-history layers were likely advanced to the latest trading day when provider history was available."
                            if movement_likely_advanced
                            else "Movement-history layers were not clearly advanced; inspect append status and source data availability."
                        ),
                        "Themes/Historical analytics caches were cleared and the page reran against refreshed state.",
                        "Verify next: 1W and 1M movement end should now reflect the latest available historical day if the append materialized successfully.",
                    ]
                    if append_failed_tickers:
                        feedback_bits.append(f"Append failed for tickers=`{', '.join(append_failed_tickers[:8])}`" + (f" +{len(append_failed_tickers) - 8} more." if len(append_failed_tickers) > 8 else "."))

                    status_container.update(
                        label=f"Materialize latest historical day: {final_state}.",
                        state="error" if feedback_level == "error" else "complete",
                        expanded=True,
                    )
                    prepare_post_mutation_refresh(
                        st.session_state,
                        "themes_refresh_feedback",
                        level=feedback_level,
                        message=" ".join(feedback_bits),
                        clear_market=True,
                    )
                    st.rerun()
                except Exception as exc:
                    status_container.update(
                        label="Materialize latest historical day: error.",
                        state="error",
                        expanded=True,
                    )
                    status_container.write(f"Historical append failed before completion: {exc}")
                    queue_feedback_message(
                        st.session_state,
                        "themes_refresh_feedback",
                        level="error",
                        message=(
                            "Materialize latest historical day failed before completion. "
                            f"No refresh guarantee: {exc}"
                        ),
                    )
                    st.rerun()
    st.caption(
        "Current Market Leadership and Current Top Themes use the latest preferred-source ticker snapshot shown above. "
        "Theme Movement tables use resolved historical window ends shown above, which can differ from the current snapshot clock."
    )
    st.caption(
        "Reload latest DB state clears cached page analytics and rereads the database. "
        "It does not fetch market data, rerun refresh_runs, or rebuild historical snapshots."
    )
    st.caption(
        "Materialize latest historical day is a heavier movement-history action: it runs the existing one-day historical append path for the latest trading day, then clears analytics caches and reruns the page. "
        "It does not rerun current/live snapshot refresh and does not intentionally rebuild the full recent window."
    )

    leadership_df = build_current_leadership_table(current_rankings, top_k=12)
    current_1w_df = build_current_performance_table(current_theme_metrics, "avg_1w", top_k=10)
    current_1m_df = build_current_performance_table(current_theme_metrics, "avg_1m", top_k=10)
    current_delta_lookup, current_delta_latest_date, current_delta_prior_date = _resolve_prior_daily_endpoint(momentum_1m.get("history", pd.DataFrame()))
    movement_1w_delta_lookup, movement_1w_latest_date, movement_1w_prior_date = _resolve_prior_daily_endpoint(momentum_1w.get("history", pd.DataFrame()))
    movement_1m_delta_lookup, movement_1m_latest_date, movement_1m_prior_date = _resolve_prior_daily_endpoint(momentum_1m.get("history", pd.DataFrame()))

    if leadership_df.empty:
        st.info("No active theme leadership data is available yet.")
    else:
        show_leadership_deltas = st.toggle("Show daily deltas", value=False, key="themes_show_daily_deltas_leadership")
        if show_leadership_deltas:
            if current_delta_prior_date is not None:
                st.caption(f"Current Market Leadership deltas compare against the prior daily movement endpoint `{current_delta_prior_date}`.")
            else:
                st.caption("Current Market Leadership deltas need two distinct daily endpoints; missing prior-day comparisons are left blank.")
        _render_current_leadership(
            leadership_df,
            label_by_id,
            show_daily_deltas=show_leadership_deltas,
            prior_lookup=current_delta_lookup,
        )

    st.divider()
    st.subheader("Current Top Themes By Window")
    st.caption("These are current live/preferred-source theme rankings, hardened for constituent eligibility, outlier control, and minimum contributor count. They answer strongest-now by one window, not strongest historical movement.")
    current_c1, current_c2 = st.columns(2)
    with current_c1:
        show_current_1w_deltas = st.toggle("Show daily deltas", value=False, key="themes_show_daily_deltas_current_1w")
        if show_current_1w_deltas:
            if current_delta_prior_date is not None:
                st.caption(f"Current 1W deltas compare against the prior daily movement endpoint `{current_delta_prior_date}`.")
            else:
                st.caption("Current 1W deltas need two distinct daily endpoints; missing prior-day comparisons are left blank.")
        if current_1w_df.empty:
            st.warning("Top Themes - Current 1W: No themes currently meet the eligible-contributor threshold.")
        else:
            _render_current_performance(
                "Top Themes - Current 1W",
                "current_top_1w",
                current_1w_df,
                label_by_id,
                show_daily_deltas=show_current_1w_deltas,
                prior_lookup=current_delta_lookup,
                metric_col="avg_1w",
            )
    with current_c2:
        show_current_1m_deltas = st.toggle("Show daily deltas", value=False, key="themes_show_daily_deltas_current_1m")
        if show_current_1m_deltas:
            if current_delta_prior_date is not None:
                st.caption(f"Current 1M deltas compare against the prior daily movement endpoint `{current_delta_prior_date}`.")
            else:
                st.caption("Current 1M deltas need two distinct daily endpoints; missing prior-day comparisons are left blank.")
        if current_1m_df.empty:
            st.warning("Top Themes - Current 1M: No themes currently meet the eligible-contributor threshold.")
        else:
            _render_current_performance(
                "Top Themes - Current 1M",
                "current_top_1m",
                current_1m_df,
                label_by_id,
                show_daily_deltas=show_current_1m_deltas,
                prior_lookup=current_delta_lookup,
                metric_col="avg_1m",
            )

    lb1, lb1_msg = _build_historical_leaderboard(momentum_1w, "avg_1w", "performance")
    lb2, lb2_msg = _build_historical_leaderboard(momentum_1m, "avg_1m", "performance")
    st.divider()
    st.subheader("Theme Movement Snapshots")
    st.caption("These tables remain historical movement views built from snapshot windows. Use them to spot rotation and momentum change, not current live leadership; displayed `performance` is the end-of-window historical metric for that view.")
    show_movement_deltas = st.toggle("Show daily deltas", value=False, key="themes_show_daily_deltas_movement")
    if show_movement_deltas:
        st.caption(
            "Theme Movement Snapshot deltas compare each theme table against its own prior daily movement endpoint "
            f"(1W: `{movement_1w_prior_date or '-'}` from latest `{movement_1w_latest_date or '-'}` | "
            f"1M: `{movement_1m_prior_date or '-'}` from latest `{movement_1m_latest_date or '-'}`)."
        )
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
            _render_leaderboard(
                "Top 10 Themes - 1W",
                "top_1w",
                lb1,
                label_by_id,
                show_advanced_leaderboard,
                show_daily_deltas=show_movement_deltas,
                prior_lookup=movement_1w_delta_lookup,
                performance_prior_col="prior_avg_1w",
            )
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
            _render_leaderboard(
                "Top 10 Themes - 1M",
                "top_1m",
                lb2,
                label_by_id,
                show_advanced_leaderboard,
                show_daily_deltas=show_movement_deltas,
                prior_lookup=movement_1m_delta_lookup,
                performance_prior_col="prior_avg_1m",
            )
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

    current_row = theme_current_row.iloc[0] if not theme_current_row.empty else None
    governed_count = int(current_row.get("ticker_count") or 0) if current_row is not None else int(len(ticker_df))
    visible_member_rows = int(len(ticker_df))
    enriched_basis_cols = [col for col in ["price", "perf_1w", "perf_1m", "perf_3m", "avg_volume", "snapshot_time"] if col in ticker_df.columns]
    enriched_row_count = int(ticker_df[enriched_basis_cols].notna().any(axis=1).sum()) if enriched_basis_cols else 0

    if current_row is not None:
        qc1, qc2, qc3, qc4 = st.columns(4)
        qc1.metric("Governed tickers", int(current_row.get("ticker_count") or 0))
        qc2.metric("Current eligible", int(current_row.get("eligible_composite_count") or 0))
        qc3.metric("Eligible breadth", f"{float(current_row.get('eligible_breadth_pct') or 0):.1f}%")
        quality_label = "n/a (inactive theme)" if not bool(current_row.get("is_active")) else current_leadership_quality_label(current_row)
        qc4.metric("Current quality", str(quality_label))
        st.caption(
            "Current theme ranking eligibility is separate from membership: governed members remain in the theme, "
            "but only eligible preferred-source contributors feed current ranking calculations."
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Current eligible contribs", int(current_row.get("eligible_composite_count") or 0))
        c2.metric("Eligible/capped 1W", f"{float(current_row.get('avg_1w') or 0):.2f}%")
        c3.metric("Eligible/capped 1M", f"{float(current_row.get('avg_1m') or 0):.2f}%")
        c4.metric("Eligible/capped 3M", f"{float(current_row.get('avg_3m') or 0):.2f}%")
        st.caption(
            "These current summary metrics match the current ranking pipeline: preferred-source contributors only, "
            "eligibility-filtered, and constituent returns capped before aggregation. Raw governed-member ticker rows remain below for inspection."
        )
        st.caption("Ticker rows below are current governed-member snapshot rows. They are not recapped or re-filtered to match the summary cards exactly.")

    if governed_count <= 0:
        st.info("This theme currently has no governed members, so there are no current member rows to display.")
    elif ticker_df.empty:
        st.warning(
            "This theme still has governed membership in current metrics, but no current governed-member rows are visible in the detail table. "
            "Manual suppression can remove tickers from this current member view while preserving governed membership/history semantics."
        )
    elif enriched_row_count <= 0:
        st.warning(
            f"This theme has `{visible_member_rows}` visible governed member row(s), but none currently have preferred-source enriched snapshot fields populated. "
            "Current snapshot coverage, recent refresh state, or sparse provider data can make the table look thin without changing governed membership."
        )
    elif enriched_row_count < visible_member_rows:
        st.info(
            f"Current enriched coverage is partial for this theme: `{enriched_row_count}` of `{visible_member_rows}` visible governed member row(s) currently have preferred-source snapshot values."
        )

    if ticker_df.empty and governed_count > 0:
        st.caption(
            "If this looks unexpectedly empty, check ticker-level suppression/refresh status in the Themes management tools. "
            "Governed membership can still exist even when the current detail table has no visible member rows."
        )

    if not ticker_df.empty:
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
        render_dataframe("theme_ticker_view", view_df, width="stretch")

    if not history_df.empty:
        hist = history_df.sort_values("snapshot_time")
        st.caption(
            "Selected-theme history shows preferred-source captured/reconstructed theme history for this theme. "
            "The movement tables above may prefer recent ticker-history-derived boundary rows when available, so short-window movement can differ without being a bug."
        )
        st.line_chart(hist.set_index("snapshot_time")[["composite_score", "avg_1m", "positive_1m_breadth_pct"]])
        render_dataframe("theme_history_table", history_df, width="stretch")

with manage_tab:
    render_feedback_message(st.session_state, "manage_ticker_feedback")

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
            prepare_post_mutation_refresh(
                st.session_state,
                "manage_ticker_feedback",
                level="success",
                message="Theme created.",
                clear_market=True,
                clear_scanner_summary=True,
                clear_research=True,
            )
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
            prepare_post_mutation_refresh(
                st.session_state,
                "manage_ticker_feedback",
                level="success",
                message="Theme updated.",
                clear_market=True,
                clear_scanner_summary=True,
                clear_research=True,
            )
            st.rerun()
        except Exception as exc:
            st.error(f"Update failed: {exc}")

    if remove:
        try:
            with get_conn() as conn:
                delete_theme(conn, selected_id)
            prepare_post_mutation_refresh(
                st.session_state,
                "manage_ticker_feedback",
                level="success",
                message="Theme deleted.",
                clear_market=True,
                clear_scanner_summary=True,
                clear_research=True,
            )
            st.rerun()
        except Exception as exc:
            st.error(f"Delete failed: {exc}")

    with get_conn() as conn:
        members = get_theme_members(conn, selected_id)

    st.caption("Manage Theme membership reflects normalized governed membership for the selected theme.")

    c1, c2 = st.columns(2)
    with c1:
        with st.form("add_ticker_form", clear_on_submit=True):
            new_ticker = st.text_input("Add ticker")
            add_submitted = st.form_submit_button("Add")
        if add_submitted:
            try:
                with get_conn() as conn:
                    add_ticker(conn, selected_id, new_ticker, onboarding_source="themes_page_manual_add")
                prepare_post_mutation_refresh(
                    st.session_state,
                    "manage_ticker_feedback",
                    level="success",
                    message=f"Added {new_ticker.strip().upper()}.",
                    clear_market=True,
                    clear_scanner_summary=True,
                    clear_research=True,
                )
                st.rerun()
            except Exception as exc:
                st.error(f"Add ticker failed: {exc}")

    with c2:
        if members.empty:
            st.info("No members to remove.")
        else:
            remove_select_key = f"remove_ticker_{selected_id}"
            remove_options = members["ticker"].tolist()
            next_remove_value = resolve_valid_selectbox_value(st.session_state.get(remove_select_key), remove_options)
            if next_remove_value is None:
                st.session_state.pop(remove_select_key, None)
            else:
                st.session_state[remove_select_key] = next_remove_value
            with st.form("remove_ticker_form"):
                remove_tkr = st.selectbox("Remove ticker", remove_options, key=remove_select_key)
                remove_submitted = st.form_submit_button("Remove")
            if remove_submitted:
                try:
                    with get_conn() as conn:
                        remove_result = remove_ticker(conn, selected_id, remove_tkr)
                    members = remove_result["members"]
                    if remove_result["removed"]:
                        prepare_post_mutation_refresh(
                            st.session_state,
                            "manage_ticker_feedback",
                            level="success",
                            message=f"Removed {remove_result['ticker']} from {selected['name']} [{selected_id}].",
                            clear_market=True,
                            clear_scanner_summary=True,
                            clear_research=True,
                        )
                        st.rerun()
                    else:
                        st.warning(f"No membership row was removed for {remove_result['ticker']} in {selected['name']} [{selected_id}].")
                except Exception as exc:
                    st.error(f"Remove ticker failed: {exc}")

    render_dataframe("manage_theme_members", members, width="stretch")

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
            suppression_state = ticker_manual_suppression_state(conn, lookup_ticker)

        if lookup.empty:
            st.warning("Ticker lookup did not return any rows.")
        else:
            row = lookup.iloc[0]
            st.write(f"**Status:** `{row['lookup_status']}` for `{lookup_ticker}`")
            l1, l2, l3, l4, l5 = st.columns(5)
            l1.metric("Assigned themes", int(row.get("assigned_theme_count") or 0))
            l2.metric("In governed membership", "yes" if bool(row.get("exists_in_theme_membership")) else "no")
            l3.metric("In snapshots", "yes" if bool(row.get("exists_in_ticker_snapshots")) else "no")
            l4.metric("Seen elsewhere", "yes" if bool(row.get("exists_in_refresh_run_tickers") or row.get("exists_in_symbol_refresh_status")) else "no")
            l5.metric("Suppressed", "yes" if bool(row.get("manually_suppressed")) else "no")
            active_assignment_count = int(row.get("active_assigned_theme_count") or 0)
            inactive_assignment_count = int(row.get("inactive_assigned_theme_count") or 0)
            if int(row.get("assigned_theme_count") or 0):
                assignment_bits = [f"active=`{active_assignment_count}`"]
                if inactive_assignment_count:
                    assignment_bits.append(f"inactive=`{inactive_assignment_count}`")
                st.caption("Governed membership assignment breakdown: " + " | ".join(assignment_bits))
            if bool(row.get("manually_suppressed")):
                st.caption(
                    "This ticker remains visible in raw lookup context but is excluded operationally from governed-membership-driven workflows."
                )

            detail = {
                "ticker": lookup_ticker,
                "suppressed": "yes" if bool(row.get("manually_suppressed")) else "no",
                "suppression_reason": row.get("manual_suppression_reason") or display_or_dash(None),
                "suppressed_at": short_timestamp(row.get("manual_suppressed_at")) or display_or_dash(None),
                "latest_snapshot_time": short_timestamp(row.get("latest_snapshot_time")) or display_or_dash(None),
                "latest_snapshot_source": row.get("latest_snapshot_source") or "n/a",
                "latest_price": format_price(row.get("latest_price")) or display_or_dash(None),
                "latest_market_cap": human_readable_number(row.get("latest_market_cap")) or display_or_dash(None),
                "latest_avg_volume": human_readable_number(row.get("latest_avg_volume")) or display_or_dash(None),
            }
            render_dataframe("ticker_lookup_detail", [detail], width="stretch", hide_index=True)

            if not memberships.empty:
                caption = "Raw assigned themes"
                if bool(row.get("manually_suppressed")):
                    caption += " (suppressed tickers remain assigned here for edit/history context)"
                st.caption(caption)
                render_dataframe(
                    "ticker_lookup_memberships",
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
                        if not bool(result.get("changed")):
                            prepare_post_mutation_refresh(
                                st.session_state,
                                "manage_ticker_feedback",
                                level="warning",
                                message=(
                                    f"No changes detected for `{result['ticker']}`. "
                                    f"It already matches the selected {int(result['assigned_theme_count'])} theme assignment(s)."
                                ),
                                clear_market=True,
                                clear_scanner_summary=True,
                                clear_research=True,
                            )
                        else:
                            extra = ""
                            onboarding_state = result.get("onboarding_state") or {}
                            onboarding_bits = []
                            if onboarding_state:
                                onboarding_bits.append(
                                    " Onboarding status: "
                                    f"history=`{onboarding_state.get('history_readiness_status') or 'unknown'}`"
                                )
                                if onboarding_state.get("backfill_status"):
                                    onboarding_bits.append(
                                        f", backfill=`{onboarding_state.get('backfill_status')}`"
                                    )
                                if onboarding_state.get("downstream_refresh_needed") is not None:
                                    onboarding_bits.append(
                                        f", downstream refresh needed=`{'yes' if bool(onboarding_state.get('downstream_refresh_needed')) else 'no'}`"
                                    )
                            if backfill_result is not None:
                                extra = (
                                    f" Stored {int(backfill_result.get('ticker_history_rows_written', 0))} ticker-day rows "
                                    f"(skipped {int(backfill_result.get('ticker_history_rows_skipped', 0))}) and refreshed "
                                    f"{int(backfill_result.get('snapshot_rows_written', 0))} reconstructed theme rows "
                                    f"(skipped {int(backfill_result.get('snapshot_rows_skipped', 0))})."
                                )
                            extra += "".join(onboarding_bits)
                            prepare_post_mutation_refresh(
                                st.session_state,
                                "manage_ticker_feedback",
                                level="success",
                                message=(
                                    f"Saved `{result['ticker']}`: "
                                    f"{int(result['added_count'])} assignment(s) added, "
                                    f"{int(result['removed_count'])} removed."
                                    f"{extra}"
                                ),
                                clear_market=True,
                                clear_scanner_summary=True,
                                clear_research=True,
                            )
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Ticker save failed: {exc}")

            st.markdown("**Operational suppression**")
            st.caption(
                "Manual suppression excludes this ticker from governed-membership-driven workflows without deleting raw assignment or historical presence."
            )
            with st.form("ticker_suppression_form"):
                suppress_ticker = st.checkbox(
                    "Suppress ticker from governed workflows",
                    value=bool(suppression_state.get("manual_suppressed")),
                )
                suppression_reason = st.text_input(
                    "Suppression reason",
                    value=str(suppression_state.get("manual_suppression_reason") or ""),
                    help="Required when suppressing. Example: moved to pink sheets / OTC, bad symbol, manual cleanup.",
                )
                suppression_submitted = st.form_submit_button("Save suppression override")

            if suppression_submitted:
                try:
                    with get_conn() as conn:
                        if suppress_ticker:
                            suppression_result = set_manual_ticker_suppression(conn, lookup_ticker, suppression_reason)
                            changed = bool(suppression_result.get("changed"))
                            level = "success" if changed else "warning"
                            message = (
                                f"Suppressed `{lookup_ticker}` from governed workflows. Reason: {suppression_result.get('manual_suppression_reason')}"
                                if changed
                                else f"`{lookup_ticker}` is already suppressed with the same reason."
                            )
                        else:
                            suppression_result = clear_manual_ticker_suppression(conn, lookup_ticker)
                            changed = bool(suppression_result.get("changed"))
                            level = "success" if changed else "warning"
                            message = (
                                f"Removed manual suppression for `{lookup_ticker}`."
                                if changed
                                else f"`{lookup_ticker}` was not manually suppressed."
                            )
                    prepare_post_mutation_refresh(
                        st.session_state,
                        "manage_ticker_feedback",
                        level=level,
                        message=message,
                        clear_market=True,
                        clear_scanner_summary=True,
                        clear_research=True,
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Suppression update failed: {exc}")

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
                backfill_started = time.perf_counter()
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
                prepare_post_mutation_refresh(
                    st.session_state,
                    "manage_ticker_feedback",
                    level="success",
                    message=(
                        "Recent ticker history stored and affected reconstructed theme history refreshed. "
                        f"Ticker-day rows written: {int(manual_backfill_result.get('ticker_history_rows_written', 0))}, "
                        f"skipped: {int(manual_backfill_result.get('ticker_history_rows_skipped', 0))}. "
                        f"Theme rows written: {int(manual_backfill_result.get('snapshot_rows_written', 0))}, "
                        f"skipped: {int(manual_backfill_result.get('snapshot_rows_skipped', 0))}. "
                        f"Completed in {time.perf_counter() - backfill_started:.1f}s."
                    ),
                    clear_market=True,
                    clear_scanner_summary=True,
                )
                st.rerun()
            except Exception as exc:
                st.error(f"Manual ticker-history backfill failed: {exc}")

show_perf_summary()
