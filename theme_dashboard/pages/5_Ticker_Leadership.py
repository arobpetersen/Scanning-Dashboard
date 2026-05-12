import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.leaderboard_utils import build_top_governed_ticker_leaders
from src.metric_formatting import display_or_dash, format_price, human_readable_number, short_timestamp
from src.streamlit_utils import (
    render_dataframe,
    reset_perf_timings,
    show_perf_summary,
    stop_for_database_error,
)
from src.theme_service import seed_if_needed


WINDOW_OPTIONS = ["1D", "1W", "1M", "3M", "6M"]
SORT_OPTIONS = [
    "1D %",
    "1W %",
    "1M %",
    "3M %",
    "6M %",
    "Ticker Composite Score",
    "Ticker Momentum Score",
]
SORT_METRIC_COLUMNS = {
    "1D %": "perf_1d",
    "1W %": "perf_1w",
    "1M %": "perf_1m",
    "3M %": "perf_3m",
    "6M %": "perf_6m",
    "Ticker Composite Score": "ticker_composite_score",
    "Ticker Momentum Score": "ticker_momentum_score",
}
DEFAULT_TOP_K = 25
DEFAULT_SORT = "1W %"
SORT_STATE_KEY = "ticker_leadership_sort_by"
TOP_N_STATE_KEY = "ticker_leadership_top_n"


st.set_page_config(page_title="Ticker Leadership", layout="wide")
st.title("Ticker Leadership")
st.caption("Top individual ticker leaders across active governed theme membership, using current preferred ticker snapshots.")
reset_perf_timings("ticker_leadership")

try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
except Exception as exc:
    stop_for_database_error(exc)


def _split_distribution(leaders: pd.DataFrame, column: str, output_col: str) -> pd.DataFrame:
    if leaders.empty or column not in leaders.columns:
        return pd.DataFrame(columns=[output_col, "ticker_count", "tickers"])

    rows: list[dict[str, str]] = []
    for _, row in leaders.iterrows():
        ticker = str(row.get("ticker") or "").strip().upper()
        for value in str(row.get(column) or "").split(","):
            label = value.strip()
            if label:
                rows.append({output_col: label, "ticker": ticker})
    if not rows:
        return pd.DataFrame(columns=[output_col, "ticker_count", "tickers"])

    return (
        pd.DataFrame(rows)
        .groupby(output_col, as_index=False)
        .agg(
            ticker_count=("ticker", "nunique"),
            tickers=("ticker", lambda values: ", ".join(dict.fromkeys(values))),
        )
        .sort_values(["ticker_count", output_col], ascending=[False, True])
        .reset_index(drop=True)
    )


def _format_leaderboard(leaders: pd.DataFrame) -> pd.DataFrame:
    if leaders.empty:
        return leaders
    out = leaders.copy()
    out = out.rename(
        columns={
            "rank": "Rank",
            "ticker": "Ticker",
            "themes": "Themes",
            "categories": "Categories",
            "perf_1d": "1D %",
            "perf_1w": "1W %",
            "perf_1m": "1M %",
            "perf_3m": "3M %",
            "perf_6m": "6M %",
            "ticker_composite_score": "Composite Score",
            "ticker_momentum_score": "Momentum Score",
            "price": "Price",
            "dollar_volume": "Dollar Volume",
            "theme_count": "Theme Count",
            "leadership_note": "Leadership Note",
        }
    )
    out["Price"] = out["Price"].apply(format_price).apply(display_or_dash)
    out["Dollar Volume"] = out["Dollar Volume"].apply(human_readable_number).apply(display_or_dash)
    return out[
        [
            "Rank",
            "Ticker",
            "Themes",
            "Categories",
            "1D %",
            "1W %",
            "1M %",
            "3M %",
            "6M %",
            "Composite Score",
            "Momentum Score",
            "Price",
            "Dollar Volume",
            "Theme Count",
            "Leadership Note",
        ]
    ]


def _column_config(columns: list[str]) -> dict[str, object]:
    configs: dict[str, object] = {}
    for column in columns:
        if column == "Rank":
            configs[column] = st.column_config.NumberColumn(column, format="%d", width="small")
        elif column in {"Ticker", "Price", "Dollar Volume", "Theme Count"}:
            configs[column] = st.column_config.TextColumn(column, width="small")
        elif column in {"1D %", "1W %", "1M %", "3M %", "6M %", "Composite Score", "Momentum Score"}:
            configs[column] = st.column_config.NumberColumn(column, format="%.2f", width="small")
        elif column in {"Themes", "Categories", "Leadership Note"}:
            configs[column] = st.column_config.TextColumn(column, width="medium")
    return configs


def _state_key_fragment(value: object) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip()).strip("_") or "all"


def _load_ticker_leaders(sort_by: str, top_k: int) -> pd.DataFrame:
    selected_window = sort_by.replace(" %", "") if sort_by.endswith(" %") else "1W"
    with get_conn() as conn:
        return build_top_governed_ticker_leaders(
            conn,
            window=selected_window,
            sort_by=sort_by,
            top_k=int(top_k),
        )


if st.session_state.get(SORT_STATE_KEY) not in SORT_OPTIONS:
    st.session_state[SORT_STATE_KEY] = DEFAULT_SORT
if TOP_N_STATE_KEY not in st.session_state:
    st.session_state[TOP_N_STATE_KEY] = DEFAULT_TOP_K

control_cols = st.columns([2, 1])
with control_cols[0]:
    selected_sort = st.selectbox(
        "Sort by",
        SORT_OPTIONS,
        index=SORT_OPTIONS.index(st.session_state[SORT_STATE_KEY]),
        key=SORT_STATE_KEY,
        help=(
            "Raw window sorts show the best return over that period. "
            "Composite and momentum sorts use ticker-level versions of existing app scoring helpers. "
            "These page scores are display-only and do not change theme rankings."
        ),
    )
with control_cols[1]:
    top_n = st.number_input("Top N", min_value=5, max_value=100, step=5, key=TOP_N_STATE_KEY)

ticker_leadership_view_key = f"{_state_key_fragment(selected_sort)}_{int(top_n)}"
leaders = _load_ticker_leaders(selected_sort, int(top_n))

if leaders.empty:
    st.info("No eligible governed active ticker leaders are available for the selected window.")
    show_perf_summary()
    st.stop()

theme_distribution = _split_distribution(leaders, "themes", "theme")
category_distribution = _split_distribution(leaders, "categories", "category")
top_row = leaders.iloc[0]
most_theme = theme_distribution.iloc[0]["theme"] if not theme_distribution.empty else "-"
most_category = category_distribution.iloc[0]["category"] if not category_distribution.empty else "-"
multi_theme_count = int((pd.to_numeric(leaders["theme_count"], errors="coerce") > 1).sum())
snapshot_time = short_timestamp(top_row.get("snapshot_time")) or "-"

c1, c2, c3, c4 = st.columns(4)
sort_metric_col = SORT_METRIC_COLUMNS[selected_sort]
sort_metric_value = pd.to_numeric(top_row.get(sort_metric_col), errors="coerce")
sort_suffix = "%" if selected_sort.endswith("%") else ""
c1.metric(f"Top ticker by {selected_sort}", str(top_row["ticker"]))
c1.caption(
    (
        f"{float(sort_metric_value):.2f}{sort_suffix} | snapshot {snapshot_time}"
        if pd.notna(sort_metric_value)
        else f"snapshot {snapshot_time}"
    )
)
c2.metric("Most represented theme", str(most_theme))
c2.caption(f"{int(theme_distribution.iloc[0]['ticker_count']) if not theme_distribution.empty else 0} top-{len(leaders)} ticker(s)")
c3.metric("Most represented category", str(most_category))
c3.caption(f"{int(category_distribution.iloc[0]['ticker_count']) if not category_distribution.empty else 0} top-{len(leaders)} ticker(s)")
c4.metric("Multi-theme leaders", multi_theme_count)
c4.caption(f"Out of {len(leaders)} displayed ticker(s)")

st.subheader(f"Top {len(leaders)} Ticker Leaderboard")
leaderboard_display = _format_leaderboard(leaders)
render_dataframe(
    "ticker_leadership_leaderboard",
    leaderboard_display,
    width="stretch",
    hide_index=True,
    key=f"ticker_leadership_leaderboard_{ticker_leadership_view_key}",
    column_config=_column_config(list(leaderboard_display.columns)),
)

st.subheader("Distribution Context")
d1, d2 = st.columns(2)
with d1:
    st.markdown(f"**Theme distribution among top {len(leaders)}**")
    render_dataframe(
        "ticker_leadership_theme_distribution",
        theme_distribution.rename(columns={"theme": "Theme", "ticker_count": "Ticker Count", "tickers": "Tickers"}),
        width="stretch",
        hide_index=True,
        key=f"ticker_leadership_theme_distribution_{ticker_leadership_view_key}",
    )
with d2:
    st.markdown(f"**Category distribution among top {len(leaders)}**")
    render_dataframe(
        "ticker_leadership_category_distribution",
        category_distribution.rename(columns={"category": "Category", "ticker_count": "Ticker Count", "tickers": "Tickers"}),
        width="stretch",
        hide_index=True,
        key=f"ticker_leadership_category_distribution_{ticker_leadership_view_key}",
    )

show_perf_summary()
