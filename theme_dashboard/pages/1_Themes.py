import time

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.historical_backfill import reconstruct_theme_history_range
from src.leaderboard_utils import (
    annotate_current_leadership_quality,
    build_category_leaderboard,
    build_category_theme_breakdown,
    build_current_leadership_table,
    build_current_performance_table,
    build_current_rank_movers_table,
    build_window_leaderboard,
    current_leadership_quality_label,
    disambiguate_theme_labels,
    format_top_ticker_leaders,
)
from src.metric_formatting import display_or_dash, format_price, format_theme_ticker_table, human_readable_number, short_timestamp
from src.queries import (
    baseline_status,
    canonical_daily_window_status,
    canonical_theme_leadership_rank_history,
    canonical_theme_leadership_rank_history_long,
    theme_snapshot_history,
    theme_ticker_metrics,
    ticker_history_last_n_snapshots,
    ticker_history_last_n_trading_days,
    ticker_lookup_memberships,
    ticker_lookup_summary,
)
from src.rankings import (
    CURRENT_MOMENTUM_WEIGHTS,
    current_momentum_quality_factor,
    current_ticker_coverage_status,
    current_ticker_is_eligible,
    ticker_current_momentum_score,
    ticker_standardized_composite_score,
    standardized_participation_factor,
    standardized_recovery_factor,
    standardized_three_month_guardrail_factor,
    visible_ticker_suppressed,
)
from src.theme_sync_status import ranked_canonical_sync_status
from src.streamlit_utils import (
    clear_current_market_view_caches,
    db_cache_token,
    extract_selected_row,
    load_current_ranking_operating_snapshot_cached,
    load_current_ranking_validation_snapshot_cached,
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
    prepare_replaceable_selectbox_widget_key,
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

TICKER_COMPOSITE_CHART_TARGET_DAILY_POINTS = 20
TICKER_COMPOSITE_CHART_TRADING_DAY_LOOKBACK = 140
TICKER_COMPOSITE_CHART_RAW_SNAPSHOT_LIMIT = 160
CURRENT_LEADERSHIP_TREND_POINTS = 10
CURRENT_RANK_MOVER_LOOKBACK_POINTS = 5
try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        themes = list_themes(conn, active_only=False)
        baseline = baseline_status(conn)
        canonical_window_status = canonical_daily_window_status(conn)
except Exception as exc:
    stop_for_database_error(exc)
db_token = db_cache_token()

if themes.empty:
    st.info("No themes found.")
    st.stop()

def _handled_selection_key(source: str) -> str:
    return f"{source}_handled_selection_token"


def _short_date_label(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return "-"
    stamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(stamp):
        return str(value)
    return stamp.strftime("%Y-%m-%d")


def _ranked_canonical_sync_status(latest_live_trading_value, ranked_canonical_value, as_of_et=None) -> str:
    return ranked_canonical_sync_status(
        latest_live_trading_value,
        ranked_canonical_value,
        latest_finalizable_value=None,
        as_of_et=as_of_et,
    )


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


def _resolve_prior_daily_endpoint(history: pd.DataFrame) -> tuple[pd.DataFrame, object | None, object | None]:
    if history.empty or "theme_id" not in history.columns or "snapshot_time" not in history.columns:
        return pd.DataFrame(
            columns=[
                "theme_id",
                "prior_composite_score",
                "prior_avg_1w",
                "prior_avg_1m",
                "prior_avg_3m",
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
                "prior_avg_3m",
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
                "avg_3m": "prior_avg_3m",
                "positive_1m_breadth_pct": "prior_breadth_1m",
            }
        )
    )
    ranked_prior = prior_rows.copy()
    ranked_prior["prior_rank_composite"] = ranked_prior["prior_composite_score"].rank(method="dense", ascending=False)
    ranked_prior["prior_rank_1w"] = ranked_prior["prior_avg_1w"].rank(method="dense", ascending=False)
    ranked_prior["prior_rank_1m"] = ranked_prior["prior_avg_1m"].rank(method="dense", ascending=False)
    return ranked_prior[
        [
            "theme_id",
            "prior_composite_score",
            "prior_avg_1w",
            "prior_avg_1m",
            "prior_avg_3m",
            "prior_breadth_1m",
            "prior_rank_composite",
            "prior_rank_1w",
            "prior_rank_1m",
        ]
    ], latest_date, prior_date


def _format_daily_delta_value(
    value,
    prior_value,
    *,
    is_percent: bool = False,
    value_decimals: int = 2,
    delta_decimals: int = 2,
) -> str:
    if value is None or pd.isna(value):
        return "-"
    suffix = "%" if is_percent else ""
    rendered = f"{float(value):.{value_decimals}f}{suffix}"
    if prior_value is None or pd.isna(prior_value):
        return rendered
    delta = float(value) - float(prior_value)
    return f"{rendered} ({delta:+.{delta_decimals}f}{suffix})"


def _apply_daily_delta_display(
    display_df: pd.DataFrame,
    prior_lookup: pd.DataFrame,
    *,
    value_map: dict[str, str],
    percent_cols: set[str] | None = None,
    value_decimals: int = 2,
    percent_decimals: int = 2,
) -> pd.DataFrame:
    if display_df.empty:
        return display_df

    out = display_df.copy()
    if not prior_lookup.empty:
        merge_cols = ["theme_id"] + [col for col in prior_lookup.columns if col != "theme_id" and col not in out.columns]
        if len(merge_cols) > 1:
            out = out.merge(prior_lookup[merge_cols], on="theme_id", how="left")
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
                value_decimals=percent_decimals if value_col in percent_cols else value_decimals,
                delta_decimals=percent_decimals if value_col in percent_cols else value_decimals,
            ),
            axis=1,
        )

    return out.drop(columns=[col for col in set(value_map.values()) if col in out.columns])


def _apply_window_delta_display(
    display_df: pd.DataFrame,
    *,
    delta_map: dict[str, str],
    percent_cols: set[str] | None = None,
    value_decimals: int = 2,
    percent_decimals: int = 2,
) -> pd.DataFrame:
    if display_df.empty:
        return display_df

    out = display_df.copy()
    percent_cols = percent_cols or set()
    for value_col, delta_col in delta_map.items():
        if value_col not in out.columns:
            continue
        decimals = percent_decimals if value_col in percent_cols else value_decimals
        suffix = "%" if value_col in percent_cols else ""
        out[value_col] = out.apply(
            lambda row: (
                _format_plain_value(row.get(value_col), is_percent=value_col in percent_cols, decimals=decimals)
                if delta_col not in out.columns or row.get(delta_col) is None or pd.isna(row.get(delta_col))
                else f"{float(row.get(value_col)):.{decimals}f}{suffix} ({float(row.get(delta_col)):+.{decimals}f}{suffix})"
            ),
            axis=1,
        )
    return out


def _format_plain_value(value, *, is_percent: bool = False, decimals: int = 2):
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, str):
        return value
    suffix = "%" if is_percent else ""
    return f"{float(value):.{decimals}f}{suffix}"


def _apply_plain_value_formatting(
    display_df: pd.DataFrame,
    *,
    percent_cols: set[str],
    percent_decimals: int = 2,
) -> pd.DataFrame:
    if display_df.empty:
        return display_df
    out = display_df.copy()
    for col in percent_cols:
        if col not in out.columns:
            continue
        out[col] = out[col].apply(
            lambda value: _format_plain_value(value, is_percent=True, decimals=percent_decimals)
        )
    return out


def _current_table_column_config(columns: list[str], *, text_columns: set[str] | None = None) -> dict[str, object]:
    configs: dict[str, object] = {}
    text_columns = text_columns or set()
    for column in columns:
        if column in text_columns:
            configs[column] = st.column_config.TextColumn(column, width="small")
        elif column == "rank":
            configs[column] = st.column_config.NumberColumn(column, format="%d", width="small")
        elif column in {"theme"}:
            configs[column] = st.column_config.TextColumn(column, width="small")
        elif column in {"category"}:
            configs[column] = st.column_config.TextColumn(column, width="small")
        elif column in {"leadership_quality", "quality"}:
            configs[column] = st.column_config.TextColumn(column, width="medium")
        elif column in {"ticker_count", "tickers", "rank_change"}:
            configs[column] = st.column_config.NumberColumn(column, format="%d", width="small")
        elif column in {"current_momentum_score", "composite_score", "composite_atr_score", "momentum", "composite", "Comp ATR"}:
            configs[column] = st.column_config.NumberColumn(column, format="%.2f", width="small")
        elif column in {"performance", "avg_1d", "avg_1w", "avg_1m", "breadth_1m", "eligible_breadth_pct", "eligible %"}:
            # These values are already rendered in percentage-point form like
            # "17.9%" before they reach the dataframe, so keep them as text to
            # avoid Streamlit applying fractional-percent scaling again.
            configs[column] = st.column_config.TextColumn(column, width="small")
    return configs


def _historical_table_column_config(columns: list[str], *, text_columns: set[str] | None = None) -> dict[str, object]:
    configs: dict[str, object] = {}
    text_columns = text_columns or set()
    for column in columns:
        if column in text_columns:
            configs[column] = st.column_config.TextColumn(column, width="small")
        elif column == "rank":
            configs[column] = st.column_config.NumberColumn(column, format="%d", width="small")
        elif column in {"theme", "category"}:
            configs[column] = st.column_config.TextColumn(column, width="small")
        elif column in {"top_themes", "quality"}:
            configs[column] = st.column_config.TextColumn(column, width="medium")
        elif column in {"contributing_themes", "themes", "rank_change", "Δ rank"}:
            configs[column] = st.column_config.NumberColumn(column, format="%d", width="small")
        elif column in {"momentum_score", "momentum", "composite_score", "composite"}:
            configs[column] = st.column_config.NumberColumn(column, format="%.2f", width="small")
        elif column in {"performance", "avg_1d", "avg_1w", "avg_1m", "avg_3m", "breadth_1m", "breadth", "eligible_breadth_pct", "eligible %"}:
            configs[column] = st.column_config.TextColumn(column, width="small")
    return configs


def _format_rank_with_change(rank_value, prior_rank_value) -> str:
    if rank_value is None or pd.isna(rank_value):
        return "-"
    rank_int = int(float(rank_value))
    if prior_rank_value is None or pd.isna(prior_rank_value):
        return f"{rank_int}"
    rank_change = int(float(prior_rank_value) - float(rank_value))
    return f"{rank_int} ({rank_change:+d})"


def _apply_prior_current_model_scores(display_df: pd.DataFrame) -> pd.DataFrame:
    if display_df.empty:
        return display_df

    out = display_df.copy()
    required_cols = {
        "prior_avg_1w",
        "prior_avg_1m",
        "prior_avg_3m",
        "ticker_count",
        "eligible_standardized_count",
        "eligible_momentum_count",
    }
    if not required_cols.issubset(out.columns):
        return out

    prior_avg_1w = pd.to_numeric(out["prior_avg_1w"], errors="coerce")
    prior_avg_1m = pd.to_numeric(out["prior_avg_1m"], errors="coerce")
    prior_avg_3m = pd.to_numeric(out["prior_avg_3m"], errors="coerce")
    ticker_count = pd.to_numeric(out["ticker_count"], errors="coerce").fillna(0.0)
    eligible_standardized_count = pd.to_numeric(out["eligible_standardized_count"], errors="coerce").fillna(0.0)
    eligible_momentum_count = pd.to_numeric(out["eligible_momentum_count"], errors="coerce").fillna(0.0)

    prior_base_strength = 0.20 * prior_avg_1w + 0.55 * prior_avg_1m + 0.25 * prior_avg_3m
    participation_ratio = np.where(
        ticker_count > 0,
        eligible_standardized_count / ticker_count,
        0.0,
    )
    participation_factor = pd.Series(participation_ratio, index=out.index).apply(standardized_participation_factor)
    guardrail_factor = prior_avg_3m.apply(standardized_three_month_guardrail_factor)
    recovery_factor = pd.DataFrame({"base": prior_base_strength, "avg_3m": prior_avg_3m}).apply(
        lambda row: standardized_recovery_factor(row["base"], row["avg_3m"]),
        axis=1,
    )
    out["prior_standardized_composite_score"] = np.where(
        eligible_standardized_count > 0,
        prior_base_strength * participation_factor * guardrail_factor * recovery_factor,
        np.nan,
    )

    prior_momentum_raw = (
        CURRENT_MOMENTUM_WEIGHTS["perf_1w"] * prior_avg_1w
        + CURRENT_MOMENTUM_WEIGHTS["perf_1m"] * prior_avg_1m
    )
    prior_momentum_quality = pd.to_numeric(out["prior_standardized_composite_score"], errors="coerce").apply(
        current_momentum_quality_factor
    )
    out["prior_current_momentum_score"] = np.where(
        eligible_momentum_count > 0,
        prior_momentum_raw * prior_momentum_quality,
        np.nan,
    )
    out["prior_standardized_composite_score"] = pd.to_numeric(
        out["prior_standardized_composite_score"], errors="coerce"
    ).round(2)
    out["prior_current_momentum_score"] = pd.to_numeric(
        out["prior_current_momentum_score"], errors="coerce"
    ).round(2)
    return out


def _apply_ticker_model_scores(ticker_df: pd.DataFrame) -> pd.DataFrame:
    if ticker_df.empty:
        return ticker_df

    out = ticker_df.copy()
    out["perf_1w"] = pd.to_numeric(out.get("perf_1w"), errors="coerce")
    out["perf_1m"] = pd.to_numeric(out.get("perf_1m"), errors="coerce")
    out["perf_3m"] = pd.to_numeric(out.get("perf_3m"), errors="coerce")
    out["ticker_composite_score"] = out.apply(
        lambda row: ticker_standardized_composite_score(row.get("perf_1w"), row.get("perf_1m"), row.get("perf_3m")),
        axis=1,
    )
    out["ticker_momentum_score"] = out.apply(
        lambda row: ticker_current_momentum_score(row.get("perf_1w"), row.get("perf_1m"), row.get("perf_3m")),
        axis=1,
    )
    out["has_current_usable_snapshot"] = out.apply(
        lambda row: bool(
            pd.notna(row.get("snapshot_time"))
            and any(
                pd.notna(row.get(col))
                for col in ["price", "perf_1w", "perf_1m", "perf_3m", "avg_volume"]
            )
        ),
        axis=1,
    )
    out["suppressed"] = out.apply(
        lambda row: bool(
            visible_ticker_suppressed(
                row.get("status", "active"),
                bool(row.get("manual_suppressed", False)),
            )
        ),
        axis=1,
    )
    out["eligible"] = out.apply(
        lambda row: bool(
            current_ticker_is_eligible(
                row.get("price"),
                row.get("avg_volume"),
                row.get("status", "active"),
                snapshot_present=(
                    (row.get("snapshot_time") is not None and not pd.isna(row.get("snapshot_time")))
                    or (row.get("price") is not None and not pd.isna(row.get("price")))
                ),
            )
        ),
        axis=1,
    )
    out["current_status"] = out.apply(
        lambda row: current_ticker_coverage_status(
            governed_membership=True,
            suppressed=bool(row.get("suppressed", False)),
            eligible=bool(row.get("eligible", False)),
            has_current_usable_snapshot=bool(row.get("has_current_usable_snapshot", False)),
        ),
        axis=1,
    )
    return out


def _attach_current_leadership_tickers(leadership_df: pd.DataFrame) -> pd.DataFrame:
    if leadership_df.empty or "theme_id" not in leadership_df.columns:
        return leadership_df

    out = leadership_df.copy()
    leaders_by_theme_id: dict[int, str] = {}
    with get_conn() as conn:
        for theme_id in out["theme_id"].dropna().astype(int).tolist():
            ticker_df = theme_ticker_metrics(conn, theme_id)
            scored = _apply_ticker_model_scores(ticker_df)
            leaders_by_theme_id[int(theme_id)] = format_top_ticker_leaders(scored, top_k=3)

    out["leaders"] = out["theme_id"].map(lambda value: leaders_by_theme_id.get(int(value), "") if pd.notna(value) else "")
    return out


def _render_current_leadership_rank_study(
    leadership_df: pd.DataFrame,
    label_by_id: dict[int, str],
    *,
    lookback_points: int = CURRENT_LEADERSHIP_TREND_POINTS,
) -> None:
    if leadership_df.empty or "theme_id" not in leadership_df.columns:
        return

    with st.expander("Leadership Trend Study", expanded=False):
        st.caption(
            "Overlay of the currently visible leadership themes using true recent historical rank. Lower rank is better, and rank 1 is shown at the top. Themes may appear well outside the current visible cohort in earlier days."
        )
        top_n_choice = st.segmented_control(
            "Study depth",
            options=[3, 5, 10],
            default=5,
            format_func=lambda value: f"Top {int(value)}",
            key="current_leadership_trend_study_top_n",
        )
        study_top_n = int(top_n_choice or 5)
        study_df = leadership_df.head(study_top_n).copy()
        theme_ids = study_df["theme_id"].dropna().astype(int).tolist()
        with get_conn() as conn:
            rank_history_long = canonical_theme_leadership_rank_history_long(
                conn,
                theme_ids,
                lookback_points=int(lookback_points),
            )

        if rank_history_long.empty:
            st.info("Not enough recent canonical daily rank data is available for the visible leadership themes.")
            return

        chart_df = rank_history_long.copy()
        chart_df["theme_label"] = chart_df["theme_id"].map(
            lambda theme_id: label_by_id.get(int(theme_id), str(theme_id))
        )
        chart_df["snapshot_date"] = pd.to_datetime(chart_df["snapshot_date"], errors="coerce")
        chart_df = chart_df.dropna(subset=["snapshot_date", "rank", "theme_label"])
        if chart_df.empty:
            st.info("Not enough recent canonical daily rank data is available for the visible leadership themes.")
            return

        chart_df = chart_df.sort_values(["snapshot_date", "rank", "theme_label"]).copy()
        date_order = (
            chart_df["snapshot_date"]
            .dropna()
            .drop_duplicates()
            .sort_values()
            .dt.strftime("%m/%d")
            .tolist()
        )
        chart_df["snapshot_label"] = chart_df["snapshot_date"].dt.strftime("%m/%d")
        rank_max = float(pd.to_numeric(chart_df["rank"], errors="coerce").dropna().max())
        chart = (
            alt.Chart(chart_df)
            .mark_line(point=alt.OverlayMarkDef(size=45, filled=True))
            .encode(
                x=alt.X(
                    "snapshot_label:O",
                    sort=date_order,
                    axis=alt.Axis(
                        title=None,
                        labelAngle=0,
                        labelOverlap=False,
                    ),
                ),
                y=alt.Y(
                    "rank:Q",
                    title="Historical rank",
                    scale=alt.Scale(domain=[1, max(rank_max, 1.0)], reverse=True),
                    axis=alt.Axis(tickMinStep=1),
                ),
                color=alt.Color("theme_label:N", title="Theme"),
                order=alt.Order("snapshot_date:T"),
                tooltip=[
                    alt.Tooltip("theme_label:N", title="Theme"),
                    alt.Tooltip("yearmonthdate(snapshot_date):T", title="Date"),
                    alt.Tooltip("rank:Q", title="Rank", format=".0f"),
                ],
            )
            .interactive()
        )
        st.altair_chart(chart, width="stretch")
        latest_chart_date = pd.to_datetime(chart_df["snapshot_date"], errors="coerce").max()
        st.caption("Scroll to zoom. Drag to pan. Double-click to reset.")
        st.caption(
            f"Showing the current top `{study_top_n}` leadership themes across the last `{int(lookback_points)}` available canonical daily dates from standardized theme snapshots. Latest chart date: `{short_timestamp(latest_chart_date) or '-'}`."
        )


def _render_current_rank_movers(
    current_rankings: pd.DataFrame,
    label_by_id: dict[int, str],
    *,
    lookback_points: int = CURRENT_RANK_MOVER_LOOKBACK_POINTS,
) -> None:
    if current_rankings.empty or "theme_id" not in current_rankings.columns:
        return

    theme_ids = current_rankings["theme_id"].dropna().astype(int).tolist()
    with get_conn() as conn:
        rank_history = canonical_theme_leadership_rank_history(
            conn,
            theme_ids,
            lookback_points=int(lookback_points),
        )

    risers = build_current_rank_movers_table(current_rankings, rank_history, direction="riser", top_k=8)
    fallers = build_current_rank_movers_table(current_rankings, rank_history, direction="faller", top_k=8)
    if risers.empty and fallers.empty:
        return

    st.markdown("**Fast Risers / Fast Fallers**")
    st.caption(
        f"These tables use canonical daily standardized ranks from the same Themes ranking model. `move` compares the current rank against the earliest point in the last `{int(lookback_points)}` available canonical daily dates."
    )
    col1, col2 = st.columns(2)

    def _render_mover_table(title: str, mover_df: pd.DataFrame, key: str) -> None:
        st.markdown(f"**{title}**")
        if mover_df.empty:
            st.info(f"No {title.lower()} for the current 5-day window.")
            return
        display_df = _display_theme_table(mover_df)
        visible_df = display_df[
            ["current_rank", "theme", "category", "move", "composite_score"]
        ].rename(
            columns={
                "current_rank": "rank",
                "composite_score": "composite",
            }
        )
        render_dataframe(
            key,
            visible_df,
            width="stretch",
            hide_index=True,
            column_config=_current_table_column_config(
                list(visible_df.columns),
                text_columns={"move"},
            ) | {
                "theme": st.column_config.TextColumn("theme", width="small"),
                "category": st.column_config.TextColumn("category", width="small"),
                "move": st.column_config.TextColumn("move", width="small"),
            },
        )

    with col1:
        _render_mover_table("Fast Risers", risers, "fast_risers_table")
    with col2:
        _render_mover_table("Fast Fallers", fallers, "fast_fallers_table")


def _build_ticker_composite_history_chart_df(
    conn,
    ticker_df: pd.DataFrame,
    *,
    top_k: int = 5,
    trading_day_lookback: int = TICKER_COMPOSITE_CHART_TRADING_DAY_LOOKBACK,
) -> tuple[pd.DataFrame, list[str]]:
    if ticker_df.empty or "ticker" not in ticker_df.columns or "ticker_composite_score" not in ticker_df.columns:
        return pd.DataFrame(), []

    top_tickers = (
        ticker_df.dropna(subset=["ticker_composite_score"])
        .sort_values(["ticker_composite_score", "ticker"], ascending=[False, True])
        .head(top_k)["ticker"]
        .astype(str)
        .tolist()
    )
    if not top_tickers:
        return pd.DataFrame(), []

    history_frames: list[pd.DataFrame] = []
    for ticker in top_tickers:
        hist = ticker_history_last_n_trading_days(conn, ticker, trading_day_limit=trading_day_lookback)
        using_daily_history = not hist.empty
        if hist.empty:
            hist = ticker_history_last_n_snapshots(conn, ticker, snapshot_limit=TICKER_COMPOSITE_CHART_RAW_SNAPSHOT_LIMIT)
        if hist.empty:
            continue
        hist = _apply_ticker_model_scores(hist)
        hist = hist.dropna(subset=["ticker_composite_score"]).copy()
        if hist.empty:
            continue
        if using_daily_history:
            hist["snapshot_date"] = pd.to_datetime(hist["snapshot_date"], errors="coerce").dt.date
            hist = hist.dropna(subset=["snapshot_date"])
            if hist.empty:
                continue
            hist = hist.sort_values(["ticker", "snapshot_date"])
        else:
            hist["snapshot_time"] = pd.to_datetime(hist["snapshot_time"], errors="coerce")
            hist = hist.dropna(subset=["snapshot_time"])
            if hist.empty:
                continue
            hist["snapshot_date"] = hist["snapshot_time"].dt.date
            hist = (
                hist.sort_values(["ticker", "snapshot_time"])
                .groupby("snapshot_date", as_index=False)
                .tail(1)
                .sort_values("snapshot_time")
            )
        hist = hist[pd.to_datetime(hist["snapshot_date"], errors="coerce").dt.dayofweek < 5].copy()
        if hist.empty:
            continue
        hist = hist.tail(TICKER_COMPOSITE_CHART_TARGET_DAILY_POINTS)
        hist["ticker"] = str(ticker)
        history_frames.append(hist[["snapshot_date", "ticker", "ticker_composite_score"]])

    if not history_frames:
        return pd.DataFrame(), top_tickers

    combined = pd.concat(history_frames, ignore_index=True).sort_values(["snapshot_date", "ticker"])
    if combined.empty:
        return pd.DataFrame(), top_tickers
    chart_df = combined.rename(columns={"snapshot_date": "date", "ticker_composite_score": "composite"}).copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"], errors="coerce")
    chart_df = chart_df.dropna(subset=["date"])
    return chart_df[["date", "ticker", "composite"]].sort_values(["date", "ticker"]), top_tickers


def _render_ticker_composite_history_chart(chart_df: pd.DataFrame) -> None:
    if chart_df.empty:
        return
    chart_display_df = chart_df.copy()
    chart_display_df["date"] = pd.to_datetime(chart_display_df["date"], errors="coerce").dt.normalize()
    chart_display_df = chart_display_df.dropna(subset=["date"])
    if chart_display_df.empty:
        return
    chart = (
        alt.Chart(chart_display_df)
        .mark_line()
        .encode(
            x=alt.X(
                "yearmonthdate(date):O",
                sort=alt.SortField(field="date", order="ascending"),
                axis=alt.Axis(title=None, labelAngle=0),
            ),
            y=alt.Y("composite:Q", title="Composite"),
            color=alt.Color("ticker:N", title="Ticker"),
            tooltip=[
                alt.Tooltip("yearmonthdate(date):T", title="Date"),
                alt.Tooltip("ticker:N", title="Ticker"),
                alt.Tooltip("composite:Q", title="Composite", format=".2f"),
            ],
        )
    )
    st.altair_chart(chart, width="stretch")


def _build_historical_leaderboard(
    momentum: dict,
    metric_col: str,
    metric_label: str,
    *,
    primary_sort_col: str | None = None,
) -> tuple[object, str | None]:
    ranked, msg = build_window_leaderboard(momentum, metric_col, top_k=10, primary_sort_col=primary_sort_col)
    if ranked.empty:
        return None, msg
    ranked = ranked.rename(columns={metric_col: metric_label})
    summary = momentum.get("window_summary", pd.DataFrame())
    if not summary.empty:
        ranked = ranked.merge(
            summary[["theme_id", "delta_avg_1w", "delta_avg_1m", "delta_avg_3m"]],
            on="theme_id",
            how="left",
        )

    latest = momentum["history"].sort_values(["snapshot_time", "theme"]).groupby("theme_id", as_index=False).tail(1)
    latest_merge_cols = [
        "theme_id",
        "category",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "composite_score",
        "positive_1m_breadth_pct",
        "ticker_count",
    ]
    latest = latest.reindex(columns=latest_merge_cols)
    ranked = ranked.merge(
        latest,
        on="theme_id",
        how="left",
        suffixes=("", "_latest"),
    )
    if "category_latest" in ranked.columns:
        ranked["category"] = ranked["category_latest"].where(ranked["category_latest"].notna(), ranked.get("category"))
        ranked = ranked.drop(columns=["category_latest"])
    if "avg_3m" not in ranked.columns:
        ranked["avg_3m"] = np.nan
    if "avg_6m" not in ranked.columns:
        ranked["avg_6m"] = np.nan
    if "composite_score" not in ranked.columns:
        ranked["composite_score"] = np.nan
    if "positive_1m_breadth_pct" not in ranked.columns:
        ranked["positive_1m_breadth_pct"] = np.nan
    if "ticker_count" not in ranked.columns:
        ranked["ticker_count"] = np.nan
    ranked["eligible_breadth_pct"] = ranked["positive_1m_breadth_pct"]
    ranked["eligible_contributor_count"] = ranked["ticker_count"]
    ranked["leadership_quality"] = ranked.apply(current_leadership_quality_label, axis=1)
    return ranked[
        [
            "rank",
            "theme_id",
            "theme",
            "category",
            metric_label,
            "avg_1w",
            "avg_1m",
            "avg_3m",
            "avg_6m",
            "momentum_score",
            "composite_score",
            "rank_change",
            "delta_avg_1w",
            "delta_avg_1m",
            "delta_avg_3m",
            "eligible_breadth_pct",
            "leadership_quality",
        ]
    ], None


def _set_theme_selection(theme_id: int, label: str, source: str) -> None:
    set_theme_selection_state(st.session_state, theme_id, label, source)


def _render_leaderboard(
    title: str,
    key_prefix: str,
    leaderboard_df,
    label_by_id: dict[int, str],
    show_advanced: bool,
    *,
    show_window_deltas: bool = False,
    ranking_caption: str | None = None,
):
    st.markdown(f"**{title}**")
    st.caption(
        ranking_caption
        or "Ranked by performance first, then momentum score, then rank improvement. "
        "This is a historical end-of-window table, so the selected window metric still drives rank even though it is shown through the aligned avg columns rather than a separate `performance` column. "
        "Breadth is contextual only and does not determine rank."
    )
    display_base = leaderboard_df
    if show_window_deltas:
        display_base = _apply_window_delta_display(
            leaderboard_df,
            delta_map={
                "avg_1w": "delta_avg_1w",
                "avg_1m": "delta_avg_1m",
                "avg_3m": "delta_avg_3m",
            },
            percent_cols={"avg_1w", "avg_1m", "avg_3m"},
            percent_decimals=1,
        )
    display_df = _display_theme_table(display_base)
    if show_advanced and "rank_change" in display_df.columns:
        display_df["rank"] = display_df.apply(
            lambda row: _format_rank_with_change(row.get("rank"), float(row.get("rank")) + float(row.get("rank_change")) if row.get("rank_change") is not None and not pd.isna(row.get("rank_change")) else pd.NA),
            axis=1,
        )
    display_df = _apply_plain_value_formatting(
        display_df,
        percent_cols={"performance", "avg_1w", "avg_1m", "avg_3m", "avg_6m", "eligible_breadth_pct"},
        percent_decimals=1,
    )
    visible_cols = [
        "rank",
        "theme",
        "category",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "momentum_score",
        "composite_score",
        "eligible_breadth_pct",
        "leadership_quality",
    ]
    if show_advanced:
        visible_cols.extend(["rank_change"])
    visible_df = display_df[visible_cols].rename(
        columns={
            "momentum_score": "momentum",
            "rank_change": "Δ rank",
            "composite_score": "composite",
            "eligible_breadth_pct": "eligible %",
            "leadership_quality": "quality",
        }
    )
    event = render_dataframe(
        f"{key_prefix}_leaderboard",
        visible_df,
        width="stretch",
        hide_index=True,
        column_config=_historical_table_column_config(
            list(visible_df.columns),
            text_columns={"rank"} if show_advanced else None,
        ),
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
    display_df = _apply_plain_value_formatting(
        leaderboard_df.copy(),
        percent_cols={"performance", "breadth_1m"},
        percent_decimals=1,
    ).rename(
        columns={
            "momentum_score": "momentum",
            "breadth_1m": "breadth",
            "contributing_themes": "themes",
        }
    )
    render_dataframe(
        title,
        display_df[["rank", "category", "performance", "momentum", "top_themes", "themes", "breadth"]],
        width="stretch",
        hide_index=True,
        column_config=_historical_table_column_config(["rank", "category", "performance", "momentum", "top_themes", "themes", "breadth"]),
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
        display_rows = _apply_plain_value_formatting(
            display_rows,
            percent_cols={"performance", "breadth_1m"},
            percent_decimals=1,
        ).rename(columns={"momentum_score": "momentum", "breadth_1m": "breadth"})
        render_dataframe(
            f"{title}_category_drill",
            display_rows[["rank", "theme", "performance", "momentum", "breadth"]],
            width="stretch",
            hide_index=True,
            column_config=_historical_table_column_config(["rank", "theme", "performance", "momentum", "breadth"]),
        )
        st.caption("These are the underlying eligible themes for the selected category/window, sorted by the same theme-level metrics used to build the category summary.")


def _render_current_leadership(leadership_df, label_by_id: dict[int, str], *, show_daily_deltas: bool = False, prior_lookup: pd.DataFrame | None = None) -> None:
    st.subheader("Current Market Leadership")
    st.caption(
        "Ranks active themes by the current standardized composite baseline using only eligible preferred-source contributors. "
        "This table keeps baseline strength, current momentum, and short-window performance adjacent so you can scan strength-now versus thrust-now without extra clutter. "
        "`avg_1d` is context-only and does not affect ranking."
    )
    prior_daily_lookup = prior_lookup if prior_lookup is not None else pd.DataFrame()
    display_base = leadership_df
    if show_daily_deltas and not prior_daily_lookup.empty:
        display_base = leadership_df.merge(prior_daily_lookup, on="theme_id", how="left")
        display_base = _apply_prior_current_model_scores(display_base)
        display_base = _apply_daily_delta_display(
            display_base,
            display_base[["theme_id", "prior_current_momentum_score", "prior_standardized_composite_score"]],
            value_map={
                "current_momentum_score": "prior_current_momentum_score",
                "composite_score": "prior_standardized_composite_score",
            },
        )
        display_base = _apply_daily_delta_display(
            display_base,
            display_base[["theme_id", "prior_avg_1w", "prior_avg_1m"]],
            value_map={
                "avg_1w": "prior_avg_1w",
                "avg_1m": "prior_avg_1m",
            },
            percent_cols={"avg_1w", "avg_1m"},
            percent_decimals=1,
        )
    display_df = _display_theme_table(display_base)
    if show_daily_deltas and "prior_rank_composite" in display_df.columns:
        display_df["rank"] = display_df.apply(
            lambda row: _format_rank_with_change(row.get("rank"), row.get("prior_rank_composite")),
            axis=1,
        )
    display_df = _apply_plain_value_formatting(
        display_df,
        percent_cols={"avg_1d", "avg_1w", "avg_1m", "avg_3m", "avg_6m", "eligible_breadth_pct"},
        percent_decimals=1,
    )
    if "avg_6m" not in display_df.columns:
        display_df["avg_6m"] = np.nan
    visible_cols = [
        "rank",
        "theme",
        "category",
        "leaders",
        "current_momentum_score",
        "composite_score",
        "composite_atr_score",
        "avg_1d",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "eligible_breadth_pct",
        "leadership_quality",
    ]
    visible_df = display_df[visible_cols].rename(
        columns={
            "current_momentum_score": "momentum",
            "composite_score": "composite",
            "composite_atr_score": "Comp ATR",
            "eligible_breadth_pct": "eligible %",
            "leadership_quality": "quality",
        }
    )
    event = render_dataframe(
        "current_leadership",
        visible_df,
        width="stretch",
        hide_index=True,
        column_config=_current_table_column_config(
            list(visible_df.columns),
            text_columns={"rank"} if show_daily_deltas else None,
        ) | {
            "theme": st.column_config.TextColumn("theme", width="small"),
            "category": st.column_config.TextColumn("category", width="small"),
            "leaders": st.column_config.TextColumn("leaders", width="medium"),
            "quality": st.column_config.TextColumn("quality", width="small"),
        },
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
    _render_current_leadership_rank_study(leadership_df, label_by_id)


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
        "The table is still ranked by the selected window return using capped constituent aggregation, while nearby avg, momentum, composite, and `avg_1d` columns give thrust-now and baseline-strength context without relying on extra count columns. "
        "`avg_1d` is informational only."
    )
    prior_daily_lookup = prior_lookup if prior_lookup is not None else pd.DataFrame()
    display_base = (
        _apply_daily_delta_display(
            leaderboard_df,
            prior_daily_lookup,
            value_map={
                "composite_score": "prior_standardized_composite_score",
                "avg_1w": "prior_avg_1w",
                "avg_1m": "prior_avg_1m",
            },
            percent_cols={"avg_1w", "avg_1m"},
            percent_decimals=1,
        )
        if show_daily_deltas
        else leaderboard_df
    )
    display_df = _display_theme_table(display_base)
    visible_cols = [
        "rank",
        "theme",
        "category",
        "avg_1d",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "avg_6m",
        "current_momentum_score",
        "composite_score",
        "composite_atr_score",
    ]
    if show_daily_deltas:
        prior_rank_col = {"avg_1w": "prior_rank_1w", "avg_1m": "prior_rank_1m"}.get(metric_col)
        if prior_rank_col and prior_rank_col in display_df.columns:
            display_df["rank"] = display_df.apply(
                lambda row: _format_rank_with_change(row.get("rank"), row.get(prior_rank_col)),
                axis=1,
            )
    display_df = _apply_plain_value_formatting(
        display_df,
        percent_cols={"avg_1d", "avg_1w", "avg_1m", "avg_3m", "avg_6m", "eligible_breadth_pct"},
        percent_decimals=1,
    )
    if "avg_6m" not in display_df.columns:
        display_df["avg_6m"] = np.nan
    visible_cols.extend(
        [
            "eligible_breadth_pct",
            "leadership_quality",
        ]
    )
    visible_df = display_df[visible_cols].rename(
        columns={
            "current_momentum_score": "momentum",
            "composite_score": "composite",
            "composite_atr_score": "Comp ATR",
            "eligible_breadth_pct": "eligible %",
            "ticker_count": "tickers",
            "leadership_quality": "quality",
        }
    )
    event = render_dataframe(
        f"{key_prefix}_current",
        visible_df,
        width="stretch",
        hide_index=True,
        column_config=_current_table_column_config(
            list(visible_df.columns),
            text_columns={"rank"} if show_daily_deltas else None,
        ),
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


def _render_standardized_composite_validation(
    db_token: tuple[str, int],
    standardized_rankings: pd.DataFrame,
) -> None:
    with st.expander("Standardized Composite Validation", expanded=False):
        st.caption(
            "Validation view only: the Themes page above now uses the standardized composite as its default baseline, while this expander preserves legacy-vs-standardized comparison for debugging. "
            "The standardized score uses 20% avg_1w, 55% avg_1m, 25% avg_3m, a soft 3M guardrail, a recovery-strength burden of proof for weak 3M backdrops, and a participation-based adjustment instead of the fixed-size confidence baseline."
        )
        load_validation = st.toggle(
            "Load standardized validation tables",
            value=False,
            key="themes_load_standardized_validation",
            help="Loads the heavier validation/comparison snapshot only when you need the debug view.",
        )
        if not load_validation:
            st.caption("Current operating tables above stay on the faster base snapshot. Turn this on only when auditing the model.")
            return
        validation_snapshot = load_current_ranking_validation_snapshot_cached(db_token)
        standardized_comparison = validation_snapshot.get("standardized_comparison", pd.DataFrame())

        if standardized_rankings.empty:
            st.info("No standardized composite rankings are available yet.")
        else:
            standardized_rankings = standardized_rankings.copy()
            for col in (
                "legacy_composite_score",
                "composite_atr_score",
                "composite_atr_rank",
                "standardized_participation_ratio",
                "standardized_guardrail_factor",
                "standardized_recovery_factor",
            ):
                if col not in standardized_rankings.columns:
                    standardized_rankings[col] = np.nan
            preview = build_current_leadership_table(
                standardized_rankings,
                top_k=12,
                score_col="standardized_composite_score",
                eligible_count_col="eligible_standardized_count",
                output_score_col="standardized_composite_score",
            ).merge(
                standardized_rankings[
                    [
                        "theme_id",
                        "legacy_composite_score",
                        "standardized_participation_ratio",
                        "standardized_guardrail_factor",
                        "standardized_recovery_factor",
                    ]
                ],
                on="theme_id",
                how="left",
            )
            preview["standardized_participation_pct"] = (preview["standardized_participation_ratio"] * 100.0).round(2)
            preview = _display_theme_table(preview)
            preview = _apply_plain_value_formatting(
                preview,
                percent_cols={"avg_1w", "avg_1m", "avg_3m", "breadth_1m", "eligible_breadth_pct", "standardized_participation_pct"},
            )
            render_dataframe(
                "standardized_composite_preview",
                preview[
                    [
                        "rank",
                        "theme",
                        "category",
                        "standardized_composite_score",
                        "composite_atr_score",
                        "legacy_composite_score",
                        "avg_1w",
                        "avg_1m",
                        "avg_3m",
                        "standardized_participation_pct",
                        "standardized_guardrail_factor",
                        "standardized_recovery_factor",
                    ]
                ].rename(
                    columns={
                        "standardized_composite_score": "std_composite",
                        "composite_atr_score": "comp_atr",
                        "legacy_composite_score": "legacy_composite",
                        "standardized_participation_pct": "participation_pct",
                        "standardized_guardrail_factor": "guardrail_factor",
                        "standardized_recovery_factor": "recovery_factor",
                    }
                ),
                width="stretch",
                hide_index=True,
            )

        if standardized_comparison.empty:
            return

        comparison = standardized_comparison.copy()
        for col in (
            "composite_atr_rank",
            "composite_atr_score",
            "standardized_participation_ratio",
            "standardized_guardrail_factor",
            "standardized_recovery_factor",
        ):
            if col not in comparison.columns:
                comparison[col] = np.nan
        comparison["standardized_participation_pct"] = (comparison["standardized_participation_ratio"] * 100.0).round(2)
        comparison["rank_shift_vs_legacy_abs"] = comparison["rank_shift_vs_legacy"].abs()
        comparison = comparison.sort_values(
            ["rank_shift_vs_legacy_abs", "rank_shift_vs_legacy", "theme"],
            ascending=[False, False, True],
            na_position="last",
        ).head(15)
        comparison = _display_theme_table(comparison)
        comparison = _apply_plain_value_formatting(
            comparison,
            percent_cols={"avg_1w", "avg_1m", "avg_3m", "standardized_participation_pct"},
        )
        st.caption("Biggest standardized-vs-legacy rank differences in the currently eligible leadership set.")
        render_dataframe(
            "standardized_composite_comparison",
            comparison[
                [
                    "theme",
                    "category",
                    "legacy_rank",
                    "standardized_rank",
                    "rank_shift_vs_legacy",
                    "legacy_composite_score",
                    "standardized_composite_score",
                    "composite_atr_score",
                    "avg_1w",
                    "avg_1m",
                    "avg_3m",
                    "standardized_participation_pct",
                    "standardized_guardrail_factor",
                    "standardized_recovery_factor",
                ]
            ].rename(
                columns={
                    "rank_shift_vs_legacy": "rank_shift",
                    "legacy_composite_score": "legacy_composite",
                    "standardized_composite_score": "std_composite",
                    "composite_atr_score": "comp_atr",
                    "standardized_participation_pct": "participation_pct",
                    "standardized_guardrail_factor": "guardrail_factor",
                    "standardized_recovery_factor": "recovery_factor",
                }
            ),
            width="stretch",
            hide_index=True,
        )


def _render_current_momentum_validation(
    db_token: tuple[str, int],
) -> None:
    with st.expander("Current Momentum Validation", expanded=False):
        st.caption(
            "Validation view only: current momentum is a current-thrust model, not the historical start/end momentum engine. "
            "It uses 70% avg_1w, 30% avg_1m, then applies a soft standardized-composite quality factor so weak baseline themes need stronger recent thrust to rank cleanly."
        )
        load_validation = st.toggle(
            "Load current momentum validation tables",
            value=False,
            key="themes_load_current_momentum_validation",
            help="Loads the heavier validation/comparison snapshot only when you need the debug view.",
        )
        if not load_validation:
            st.caption("Current operating tables above stay on the faster base snapshot. Turn this on only when auditing the model.")
            return
        validation_snapshot = load_current_ranking_validation_snapshot_cached(db_token)
        current_momentum_rankings = validation_snapshot.get("current_momentum_rankings", pd.DataFrame())
        current_momentum_comparison = validation_snapshot.get("current_momentum_comparison", pd.DataFrame())

        if current_momentum_rankings.empty:
            st.info("No current momentum rankings are available yet.")
        else:
            current_momentum_rankings = current_momentum_rankings.copy()
            for col in (
                "current_momentum_raw_score",
                "current_momentum_quality_factor",
                "standardized_composite_score",
                "composite_atr_score",
                "composite_atr_rank",
            ):
                if col not in current_momentum_rankings.columns:
                    current_momentum_rankings[col] = np.nan
            preview = build_current_leadership_table(
                current_momentum_rankings,
                top_k=12,
                score_col="current_momentum_score",
                eligible_count_col="eligible_momentum_count",
                output_score_col="momentum_leadership_score",
            ).merge(
                current_momentum_rankings[
                    [
                        "theme_id",
                        "current_momentum_raw_score",
                        "current_momentum_quality_factor",
                        "standardized_composite_score",
                    ]
                ],
                on="theme_id",
                how="left",
            )
            preview = _display_theme_table(preview)
            preview = _apply_plain_value_formatting(
                preview,
                percent_cols={"avg_1w", "avg_1m", "avg_3m", "breadth_1m", "eligible_breadth_pct"},
            )
            render_dataframe(
                "current_momentum_preview",
                preview[
                    [
                        "rank",
                        "theme",
                        "category",
                        "momentum_leadership_score",
                        "current_momentum_raw_score",
                        "standardized_composite_score",
                        "composite_atr_score",
                        "current_momentum_quality_factor",
                        "avg_1w",
                        "avg_1m",
                    ]
                ].rename(
                    columns={
                        "momentum_leadership_score": "momentum_score",
                        "current_momentum_raw_score": "momentum_raw",
                        "standardized_composite_score": "std_composite",
                        "composite_atr_score": "comp_atr",
                        "current_momentum_quality_factor": "quality_factor",
                    }
                ),
                width="stretch",
                hide_index=True,
            )

        if current_momentum_comparison.empty:
            return

        comparison = current_momentum_comparison.copy()
        for col in (
            "current_momentum_raw_score",
            "current_momentum_quality_factor",
            "current_momentum_score",
            "standardized_composite_score",
            "composite_atr_score",
            "composite_atr_rank",
        ):
            if col not in comparison.columns:
                comparison[col] = np.nan
        comparison["rank_shift_vs_1w_abs"] = comparison["rank_shift_vs_1w"].abs()
        comparison = comparison.sort_values(
            ["rank_shift_vs_1w_abs", "rank_shift_vs_1w", "theme"],
            ascending=[False, False, True],
            na_position="last",
        ).head(15)
        comparison = _display_theme_table(comparison)
        comparison = _apply_plain_value_formatting(
            comparison,
            percent_cols={"avg_1w", "avg_1m"},
        )
        st.caption("Biggest current-momentum vs raw-current-1W rank differences in the currently eligible thrust set.")
        render_dataframe(
            "current_momentum_comparison",
            comparison[
                [
                    "theme",
                    "category",
                    "current_1w_rank",
                    "current_momentum_rank",
                    "standardized_rank",
                    "rank_shift_vs_1w",
                    "avg_1w",
                    "avg_1m",
                    "current_momentum_score",
                    "standardized_composite_score",
                    "composite_atr_score",
                    "current_momentum_quality_factor",
                ]
            ].rename(
                columns={
                    "current_1w_rank": "raw_1w_rank",
                    "current_momentum_rank": "momentum_rank",
                    "rank_shift_vs_1w": "rank_shift",
                    "standardized_composite_score": "std_composite",
                    "composite_atr_score": "comp_atr",
                    "current_momentum_score": "momentum_score",
                    "current_momentum_quality_factor": "quality_factor",
                }
            ),
            width="stretch",
            hide_index=True,
        )


explore_tab, manage_tab = st.tabs(["Explore Themes", "Manage & Ops"])

with explore_tab:
    render_feedback_message(st.session_state, "themes_refresh_feedback")
    st.info("Use Themes for review and drilldown. Run daily sync/finalization from the Apps page, then come here to inspect current leadership and open detail.")
    with st.expander("Internal testing quick guide", expanded=False):
        st.markdown(
            "- Start in `Themes` for current leadership, drilldown, and ticker-level inspection.\n"
            "- Run daily sync/finalization from `Apps`; this page now assumes the DB has already been refreshed there.\n"
            "- Use `Historical Performance` only when you want to audit historical movement, trust, or provenance for a theme that already looks interesting.\n"
            "- Most useful feedback: confusing labels/states, drilldown or selection bugs, trust/reconciliation issues, and places where the app feels broken even when data is present.\n"
            "- Known limitations: some advanced/debug tools remain available for internal trust work, and thin themes can still appear when their historical row contract is valid."
        )

    options, label_by_id, id_by_label = _theme_option_maps(themes)
    selected_theme_id = st.session_state.get(SELECTED_THEME_ID_KEY)
    selected_theme_label = st.session_state.get(SELECTED_THEME_LABEL_KEY)
    if selected_theme_id in label_by_id:
        selected_theme_label = label_by_id[int(selected_theme_id)]
    elif selected_theme_label in id_by_label:
        selected_theme_id = int(id_by_label[str(selected_theme_label)])
        selected_theme_label = str(selected_theme_label)
    else:
        selected_theme_id = None
        selected_theme_label = None
    if SELECTED_THEME_SOURCE_KEY not in st.session_state:
        st.session_state[SELECTED_THEME_SOURCE_KEY] = "default"

    current_snapshot = load_current_ranking_operating_snapshot_cached(db_token)
    current_theme_metrics = current_snapshot["theme_metrics"].copy()
    if "standardized_composite_score" in current_theme_metrics.columns:
        current_theme_metrics["composite_score"] = current_theme_metrics["standardized_composite_score"]
    current_rankings = current_snapshot.get("standardized_rankings", current_snapshot["rankings"]).copy()
    if "standardized_composite_score" in current_rankings.columns:
        current_rankings["composite_score"] = current_rankings["standardized_composite_score"]
    if not current_rankings.empty:
        current_rankings = current_rankings.reset_index(drop=True).copy()
        current_rankings["rank"] = current_rankings.index + 1
        current_rankings = annotate_current_leadership_quality(current_rankings)
        current_theme_metrics = current_theme_metrics.merge(
            current_rankings[["theme_id", "rank", "leader_cohort_eligible", "leader_cohort_cutoff_rank", "active_theme_count"]].rename(
                columns={"rank": "current_rank"}
            ),
            on="theme_id",
            how="left",
        )
    standardized_rankings = current_snapshot.get("standardized_rankings", pd.DataFrame())
    momentum_1w = load_theme_momentum_cached(db_token, 7, top_n=20)
    momentum_1m = load_theme_momentum_cached(db_token, 30, top_n=20)
    baseline_row = baseline.iloc[0] if not baseline.empty else None
    canonical_window_row = canonical_window_status.iloc[0] if not canonical_window_status.empty else None
    current_driver_time = pd.to_datetime(current_theme_metrics["snapshot_time"]).dropna().max() if not current_theme_metrics.empty and "snapshot_time" in current_theme_metrics.columns else None
    movement_1w_end = momentum_1w.get("meta", {}).get("window_end")
    movement_1m_end = momentum_1m.get("meta", {}).get("window_end")
    ranked_window_end_candidates = pd.to_datetime(
        [value for value in [movement_1w_end, movement_1m_end] if value is not None],
        errors="coerce",
    )
    latest_ranked_window_end = ranked_window_end_candidates.max() if len(ranked_window_end_candidates) else None
    current_snapshot_label = short_timestamp(current_driver_time)
    if not current_snapshot_label and baseline_row is not None:
        current_snapshot_label = short_timestamp(baseline_row.get("latest_ticker_snapshot_time"))
    current_live_reference = current_driver_time
    if current_live_reference is None and baseline_row is not None:
        current_live_reference = baseline_row.get("latest_ticker_snapshot_time")
    latest_expected_trading_date = canonical_window_row.get("latest_expected_trading_date") if canonical_window_row is not None else None
    latest_raw_canonical_date = canonical_window_row.get("latest_raw_canonical_date") if canonical_window_row is not None else None
    latest_ranked_canonical_date = canonical_window_row.get("latest_ranked_canonical_date") if canonical_window_row is not None else None
    current_live_trading_date = _short_date_label(current_live_reference)
    sync_status_label = ranked_canonical_sync_status(
        current_live_reference,
        latest_ranked_canonical_date,
        latest_finalizable_value=latest_expected_trading_date,
    )
    freshness_c1, freshness_c2, freshness_c3, freshness_c4 = st.columns([1.1, 1.0, 1.35, 1.15])
    freshness_c1.metric("Current live snapshot", current_snapshot_label or "-")
    freshness_c2.metric("Latest ranked canonical date", _short_date_label(latest_ranked_canonical_date))
    freshness_c3.metric("Sync status", sync_status_label)
    with freshness_c4:
        if st.button("Reload latest DB state", key="themes_force_refresh"):
            clear_current_market_view_caches()
            queue_feedback_message(
                st.session_state,
                "themes_refresh_feedback",
                level="success",
                message=(
                    "Reloaded Themes against the latest stored DB state. This page does not run sync/finalization; use Apps for the daily refresh workflow."
                ),
            )
            st.rerun()
        st.caption("Light reload only. Use Apps for daily sync/finalization.")
    with st.expander("Snapshot details", expanded=False):
        detail_c1, detail_c2, detail_c3 = st.columns(3)
        detail_c1.metric("Latest live trading date", current_live_trading_date)
        detail_c2.metric("Latest raw canonical date", _short_date_label(latest_raw_canonical_date))
        detail_c3.metric("Latest ranked canonical window end", short_timestamp(latest_ranked_window_end) or "-")
        if canonical_window_row is not None and bool(canonical_window_row.get("raw_vs_ranked_date_differs")):
            st.info(
                f"Latest canonical rows exist for `{_short_date_label(latest_raw_canonical_date)}`, "
                f"but movement windows still end on `{_short_date_label(latest_ranked_canonical_date)}` "
                "because the newer date does not have usable ranked canonical rows yet."
            )

    with st.expander("Refresh guidance", expanded=False):
        st.caption(
            "Themes reads stored state only. Use `Apps` for daily sync/finalization, then use `Reload latest DB state` here if this page needs a clean rerun against the refreshed database."
        )

    leadership_df = _attach_current_leadership_tickers(build_current_leadership_table(current_rankings, top_k=12))
    current_1w_df = build_current_performance_table(current_theme_metrics, "avg_1w", top_k=10)
    current_1m_df = build_current_performance_table(current_theme_metrics, "avg_1m", top_k=10)
    current_delta_lookup, current_delta_latest_date, current_delta_prior_date = _resolve_prior_daily_endpoint(momentum_1m.get("history", pd.DataFrame()))

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
        _render_current_rank_movers(current_rankings, label_by_id)
        _render_standardized_composite_validation(db_token, standardized_rankings)

    st.divider()
    st.subheader("Current Top Themes By Window")
    st.caption("These are current live/preferred-source theme rankings, hardened for constituent eligibility, outlier control, and minimum contributor count. Their composite context now uses the standardized baseline, while the main ranking axis remains the selected current window.")
    current_c1, current_c2 = st.columns(2)
    with current_c1:
        show_current_1w_deltas = st.toggle("Show daily deltas", value=False, key="themes_show_daily_deltas_current_1w")
        if show_current_1w_deltas:
            if current_delta_prior_date is not None:
                st.caption(f"Current 1W deltas compare against the prior daily movement endpoint `{current_delta_prior_date}`.")
                st.caption("Optional `rank_change` appears here only in delta view and uses prior daily 1W rank versus current 1W rank.")
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
                st.caption("Optional `rank_change` appears here only in delta view and uses prior daily 1M rank versus current 1M rank.")
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
    _render_current_momentum_validation(db_token)

    lb1, lb1_msg = _build_historical_leaderboard(
        momentum_1w,
        "avg_1w",
        "performance",
        primary_sort_col="momentum_score",
    )
    lb2, lb2_msg = _build_historical_leaderboard(momentum_1m, "avg_1m", "performance")
    st.divider()
    st.subheader("Theme Movement Snapshots")
    st.caption("These tables are historical improvement and rotation views built from snapshot windows. Use them to spot strengthening participation and leadership change, not current live leadership; displayed `performance` is the end-of-window historical metric for that view.")
    show_movement_deltas = st.toggle("Show window deltas", value=False, key="themes_show_daily_deltas_movement")
    if show_movement_deltas:
        st.caption(
            "Theme Movement Snapshot deltas compare the resolved window start against the resolved window end for that table. "
            f"(1W window: `{short_timestamp(momentum_1w.get('meta', {}).get('window_start')) or '-'}` to `{short_timestamp(momentum_1w.get('meta', {}).get('window_end')) or '-'}` | "
            f"1M window: `{short_timestamp(momentum_1m.get('meta', {}).get('window_start')) or '-'}` to `{short_timestamp(momentum_1m.get('meta', {}).get('window_end')) or '-'}`)."
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
                show_window_deltas=show_movement_deltas,
                ranking_caption=(
                    "Ranked by momentum score first, then displayed 1W return, then rank improvement. "
                    "This remains a historical improvement and rotation table, so `avg_1w` is the end-of-window value while optional delta overlays show start-to-end movement across the selected window."
                ),
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
                show_window_deltas=show_movement_deltas,
            )
    if leaderboard_mode == "Categories":
        st.caption(
            "Category mode ranks categories from the full eligible theme set for the selected window, then shows the top category rows. "
            "Switch back to Themes mode to click a row into the detail view."
        )

    st.divider()

    selected_theme_id = st.session_state.get(SELECTED_THEME_ID_KEY)
    selected_theme_label = st.session_state.get(SELECTED_THEME_LABEL_KEY)
    if selected_theme_id in label_by_id:
        selected_theme_label = label_by_id[int(selected_theme_id)]
    elif selected_theme_label in id_by_label:
        selected_theme_id = int(id_by_label[str(selected_theme_label)])
        selected_theme_label = str(selected_theme_label)
    else:
        selected_theme_id = None
        selected_theme_label = None

    labels = list(options.keys())
    default_theme_index = labels.index(selected_theme_label) if selected_theme_label in labels else None
    selection = labels[default_theme_index] if default_theme_index is not None else None
    picker_col, source_col = st.columns([3.5, 1.5])
    with picker_col:
        st.caption("Current theme")
        if selection:
            st.markdown(f"### {selection}")
        else:
            st.markdown("### Search and select a theme to view detail.")
        st.markdown("")
        theme_search_widget_key = prepare_replaceable_selectbox_widget_key(
            st.session_state,
            "theme_detail_view_search",
            labels,
            selection,
        )
        selected_search = st.selectbox(
            "Theme detail view",
            labels,
            index=None,
            placeholder="Type to search and select a different theme",
            key=theme_search_widget_key,
            label_visibility="collapsed",
        )
        if selected_search and str(selected_search) != str(selection or ""):
            selection = str(selected_search)
            _set_theme_selection(int(options[selection]), selection, "manual_dropdown")
    with source_col:
        if selection:
            st.caption(f"Selected from: {describe_selection_source(st.session_state.get(SELECTED_THEME_SOURCE_KEY))}")

    if not selection:
        ticker_df = pd.DataFrame()
        history_df = pd.DataFrame()
        theme_current_row = pd.DataFrame()
        include_suppressed_tickers = False
        visible_ticker_df = pd.DataFrame()
        ticker_composite_history_chart_df = pd.DataFrame()
        top_composite_tickers = []
        current_row = None
        governed_count = 0
        visible_member_rows = 0
        suppressed_hidden_count = 0
        enriched_row_count = 0
    else:
        theme_id = int(options[selection])

        with get_conn() as conn:
            ticker_df = theme_ticker_metrics(conn, theme_id, include_suppressed=True)
            history_df = theme_snapshot_history(conn, theme_id, limit=50)
            theme_current_row = current_theme_metrics[current_theme_metrics["theme_id"] == theme_id].copy()

    ticker_df = _apply_ticker_model_scores(ticker_df)
    if not ticker_df.empty:
        ticker_df = ticker_df.copy()
        ticker_df["manual_suppressed"] = ticker_df.get("manual_suppressed", False).fillna(False).astype(bool)
        ticker_df["suppressed"] = ticker_df.get("suppressed", False).fillna(False).astype(bool)

    if selection:
        include_suppressed_tickers = st.checkbox(
            "Include suppressed tickers",
            value=False,
            key="themes_include_suppressed_tickers",
            help="Show governed members that are suppressed either manually or operationally from the default current detail view.",
        )
        visible_ticker_df = (
            ticker_df.copy()
            if include_suppressed_tickers
            else ticker_df[~ticker_df.get("suppressed", pd.Series(False, index=ticker_df.index)).fillna(False)].copy()
        )
        if not visible_ticker_df.empty:
            visible_ticker_df = visible_ticker_df.sort_values(
                ["eligible", "has_current_usable_snapshot", "ticker_composite_score", "ticker"],
                ascending=[False, False, False, True],
                na_position="last",
            ).reset_index(drop=True)
        with get_conn() as conn:
            ticker_composite_history_chart_df, top_composite_tickers = _build_ticker_composite_history_chart_df(conn, visible_ticker_df)

        current_row = theme_current_row.iloc[0] if not theme_current_row.empty else None
        governed_count = int(current_row.get("ticker_count") or 0) if current_row is not None else int(len(ticker_df))
        visible_member_rows = int(len(visible_ticker_df))
        suppressed_hidden_count = max(int(len(ticker_df)) - visible_member_rows, 0)
        enriched_basis_cols = [col for col in ["price", "perf_1d", "perf_1w", "perf_1m", "perf_3m", "perf_6m", "avg_volume", "snapshot_time"] if col in visible_ticker_df.columns]
        enriched_row_count = int(visible_ticker_df[enriched_basis_cols].notna().any(axis=1).sum()) if enriched_basis_cols else 0

    if current_row is not None:
        def _metric_value(value, *, suffix: str = "", decimals: int = 2) -> str:
            if value is None or pd.isna(value):
                return "—"
            return f"{float(value):.{decimals}f}{suffix}"

        def _render_summary_metric(label: str, value: object) -> None:
            rendered_value = "â€”" if value is None else str(value)
            if rendered_value == "Ã¢â‚¬â€":
                rendered_value = "&mdash;"
            rendered_value = "&mdash;" if value is None else str(value)
            st.markdown(
                (
                    "<div style='padding:0.02rem 0.04rem; line-height:1.0;'>"
                    f"<div style='font-size:0.76rem; font-weight:500; color:var(--text-color); opacity:0.82; margin-bottom:0.08rem;'>{label}</div>"
                    f"<div style='font-size:1.14rem; font-weight:625; color:var(--text-color);'>{rendered_value}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

        current_rank = "-"
        if not current_rankings.empty and "theme_id" in current_rankings.columns:
            current_rank_lookup = current_rankings.reset_index(drop=True).copy()
            current_rank_lookup["current_rank"] = current_rank_lookup.index + 1
            current_rank_row = current_rank_lookup[current_rank_lookup["theme_id"] == theme_id]
            if not current_rank_row.empty:
                current_rank = int(current_rank_row.iloc[0]["current_rank"])
        window_rank_change = "—"
        movement_summary = momentum_1w.get("window_summary", pd.DataFrame())
        if not movement_summary.empty and "theme_id" in movement_summary.columns:
            movement_row = movement_summary[movement_summary["theme_id"] == theme_id]
            if not movement_row.empty and pd.notna(movement_row.iloc[0].get("rank_change")):
                window_rank_change = f"{float(movement_row.iloc[0]['rank_change']):+.0f}"

        summary_left, summary_mid, summary_right = st.columns([1.95, 4.1, 1.95], gap="small")
        with summary_mid:
            with st.container(border=True):
                s1, s2, s3, s4 = st.columns(4, gap="small")
                with s1:
                    _render_summary_metric("Composite", _metric_value(current_row.get("composite_score")))
                with s2:
                    _render_summary_metric("Momentum", _metric_value(current_row.get("current_momentum_score")))
                with s3:
                    _render_summary_metric("Rank", current_rank)
                with s4:
                    _render_summary_metric("1W hist rank Δ", window_rank_change)

                st.markdown("<div style='height:0.25rem;'></div>", unsafe_allow_html=True)
                s5, s6, s7, s8 = st.columns(4, gap="small")
                with s5:
                    _render_summary_metric(
                        "Contributors",
                        "—" if pd.isna(current_row.get("eligible_composite_count")) else int(current_row.get("eligible_composite_count") or 0),
                    )
                with s6:
                    _render_summary_metric("Avg 1D", _metric_value(current_row.get("avg_1d"), suffix="%"))
                with s7:
                    _render_summary_metric("Avg 1W", _metric_value(current_row.get("avg_1w"), suffix="%"))
                with s8:
                    _render_summary_metric("Avg 1M", _metric_value(current_row.get("avg_1m"), suffix="%"))
                st.markdown("<div style='height:0.18rem;'></div>", unsafe_allow_html=True)
                s9, s10, s11, s12 = st.columns(4, gap="small")
                with s9:
                    _render_summary_metric("Avg 3M", _metric_value(current_row.get("avg_3m"), suffix="%"))
                with s10:
                    _render_summary_metric("Avg 6M", _metric_value(current_row.get("avg_6m"), suffix="%"))
                with s11:
                    _render_summary_metric("Snapshot", short_timestamp(current_row.get("snapshot_time")) or "â€”")
                with s12:
                    _render_summary_metric("Workflow", "Sync in Apps")
                st.markdown("<div style='height:0.18rem;'></div>", unsafe_allow_html=True)
                s13, s14, s15, s16 = st.columns(4, gap="small")
                with s13:
                    quality_label = "n/a (inactive theme)" if not bool(current_row.get("is_active")) else current_leadership_quality_label(current_row)
                    _render_summary_metric("Quality", quality_label)
                with s14:
                    _render_summary_metric("", "")
                with s15:
                    _render_summary_metric("", "")
                with s16:
                    _render_summary_metric("", "")

    if suppressed_hidden_count > 0 and not include_suppressed_tickers:
        st.caption(f"{suppressed_hidden_count} suppressed ticker(s) are hidden from the default detail table.")

    if governed_count <= 0:
        st.info("This theme currently has no governed members, so there are no current member rows to display.")
    elif visible_ticker_df.empty:
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

    if visible_ticker_df.empty and governed_count > 0:
        st.caption(
            "If this looks unexpectedly empty, check ticker-level suppression/refresh status in the Themes management tools. "
            "If suppressed names are being hidden, turn on `Include suppressed tickers` to inspect them. "
            "Governed membership can still exist even when the current detail table has no visible member rows."
        )

    if not visible_ticker_df.empty:
        display_ticker_df = format_theme_ticker_table(visible_ticker_df)
        for perf_col in ("perf_1d", "perf_1w", "perf_1m", "perf_3m", "perf_6m"):
            if perf_col in display_ticker_df.columns:
                display_ticker_df[perf_col] = display_ticker_df[perf_col].apply(
                    lambda v: display_or_dash(None) if v is None else (display_or_dash(None) if str(v) == "nan" else f"{float(v):.2f}%")
                )
        for score_col in ("ticker_composite_score", "ticker_momentum_score"):
            if score_col in display_ticker_df.columns:
                display_ticker_df[score_col] = display_ticker_df[score_col].apply(
                    lambda v: display_or_dash(None) if v is None or str(v) == "nan" else f"{float(v):.2f}"
                )

        cols = [
            c
            for c in [
                "ticker",
                "eligible",
                "suppressed" if include_suppressed_tickers else None,
                "price",
                "perf_1d",
                "perf_1w",
                "perf_1m",
                "perf_3m",
                "perf_6m",
                "ticker_composite_score",
                "ticker_momentum_score",
                "market_cap",
                "avg_volume",
                "dollar_volume",
                "short_interest_pct",
                "float_shares",
                "adr_pct",
                "current_status",
                "last_updated",
                "snapshot_time",
                "latest_refresh_time",
            ]
            if c in display_ticker_df.columns
        ]

        rename_map = {
            "current_status": "current status",
            "eligible": "eligible",
            "suppressed": "suppressed",
            "ticker_composite_score": "composite",
            "ticker_momentum_score": "momentum",
            "last_updated": "market_data_time",
            "snapshot_time": "snapshot_time",
            "latest_refresh_time": "last_refresh_time",
        }
        view_df = display_ticker_df[cols].rename(columns=rename_map) if cols else display_ticker_df

        for nullable_col in ("short_interest_pct", "float_shares", "adr_pct"):
            if nullable_col in view_df.columns:
                view_df[nullable_col] = view_df[nullable_col].apply(display_or_dash)

        st.caption("Timestamps: `market_data_time` = provider market data, `snapshot_time` = captured snapshot row, `last_refresh_time` = latest completed refresh.")
        render_dataframe("theme_ticker_view", view_df, width="stretch")

    if not ticker_composite_history_chart_df.empty:
        st.caption(
            "Bottom chart shows ticker-level composite history for the current top 5 visible governed tickers in this theme, ranked by current ticker composite. "
            "Ticker composite uses the same baseline-strength philosophy as the standardized theme composite: 30% 1W, 70% 1M, with a 3M skepticism layer."
        )
        _render_ticker_composite_history_chart(ticker_composite_history_chart_df)
        st.caption(
            f"Top composite tickers used for the chart: {', '.join(top_composite_tickers)}."
        )
    elif not visible_ticker_df.empty:
        st.info(
            "Ticker composite history is unavailable for the current top governed tickers in this theme. "
            "This can happen when recent ticker snapshot history is sparse or missing for the preferred source."
        )

    if not history_df.empty:
        st.caption(
            "Selected-theme history table below still shows preferred-source captured/reconstructed theme history for this theme. "
            "The movement tables above may prefer recent ticker-history-derived boundary rows when available, so short-window movement can differ without being a bug."
        )
        render_dataframe("theme_history_table", history_df, width="stretch")
    elif selection:
        st.info("No preferred-source theme history rows are available yet for this selected theme.")

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
    manage_theme_options = list(labels.keys())
    next_manage_label = resolve_valid_selectbox_value(st.session_state.get("manage_theme"), manage_theme_options)
    if next_manage_label is not None and st.session_state.get("manage_theme") != next_manage_label:
        st.session_state["manage_theme"] = next_manage_label
    selected_label = st.selectbox("Select theme to manage", manage_theme_options, key="manage_theme")
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
            st.session_state.pop("manage_theme", None)
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
            visible_lookup_suppressed = bool(
                visible_ticker_suppressed(
                    "refresh_suppressed" if bool(row.get("operationally_suppressed")) else "active",
                    bool(row.get("manually_suppressed")),
                )
            )
            current_lookup_eligible = current_ticker_is_eligible(
                row.get("preferred_price"),
                row.get("preferred_avg_volume"),
                "refresh_suppressed" if visible_lookup_suppressed else "active",
                snapshot_present=bool(row.get("has_current_preferred_snapshot")),
            )
            current_coverage_status = current_ticker_coverage_status(
                governed_membership=bool(row.get("exists_in_theme_membership")),
                suppressed=visible_lookup_suppressed,
                eligible=bool(current_lookup_eligible),
                has_current_usable_snapshot=bool(row.get("has_current_usable_preferred_snapshot")),
            )
            st.write(f"**Status:** `{row['lookup_status']}` for `{lookup_ticker}`")
            l1, l2, l3, l4, l5, l6 = st.columns(6)
            l1.metric("Assigned themes", int(row.get("assigned_theme_count") or 0))
            l2.metric("In governed membership", "yes" if bool(row.get("exists_in_theme_membership")) else "no")
            l3.metric("In snapshots", "yes" if bool(row.get("exists_in_ticker_snapshots")) else "no")
            l4.metric("Seen elsewhere", "yes" if bool(row.get("exists_in_refresh_run_tickers") or row.get("exists_in_symbol_refresh_status")) else "no")
            l5.metric("Suppressed", "yes" if visible_lookup_suppressed else "no")
            l6.metric("Current coverage", str(current_coverage_status))
            active_assignment_count = int(row.get("active_assigned_theme_count") or 0)
            inactive_assignment_count = int(row.get("inactive_assigned_theme_count") or 0)
            if int(row.get("assigned_theme_count") or 0):
                assignment_bits = [f"active=`{active_assignment_count}`"]
                if inactive_assignment_count:
                    assignment_bits.append(f"inactive=`{inactive_assignment_count}`")
                st.caption("Governed membership assignment breakdown: " + " | ".join(assignment_bits))
            if visible_lookup_suppressed:
                st.caption(
                    "This ticker remains visible in raw lookup context but is excluded operationally from governed-membership-driven workflows."
                )
            elif str(current_coverage_status) == "needs refresh check":
                refresh_note = (
                    "No refresh attempt is recorded yet for this ticker."
                    if not bool(row.get("exists_in_refresh_run_tickers"))
                    else "A refresh trail exists, but no usable current preferred-source snapshot row is stored."
                )
                st.caption(
                    "This ticker is governed and unsuppressed but has no usable current preferred-source snapshot coverage. "
                    "Ticker Lookup shows stored state only and does not trigger a live refresh attempt from this view. "
                    f"{refresh_note}"
                )

            detail = {
                "ticker": lookup_ticker,
                "current_coverage": current_coverage_status,
                "preferred_snapshot_present": "yes" if bool(row.get("has_current_preferred_snapshot")) else "no",
                "suppressed": "yes" if visible_lookup_suppressed else "no",
                "suppression_reason": row.get("manual_suppression_reason") or display_or_dash(None),
                "suppressed_at": short_timestamp(row.get("manual_suppressed_at")) or display_or_dash(None),
                "preferred_snapshot_time": short_timestamp(row.get("preferred_snapshot_time")) or display_or_dash(None),
                "preferred_snapshot_source": row.get("preferred_snapshot_source") or display_or_dash(None),
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
