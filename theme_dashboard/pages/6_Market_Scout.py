from __future__ import annotations

from collections import Counter
from html import escape

import pandas as pd
import streamlit as st

from src.database import get_conn, init_db
from src.market_context import (
    backfill_qqq_market_context_history,
    latest_qqq_history_date,
    latest_qqq_market_context,
    qqq_market_context_unavailable_message,
)
from src.market_scout_snapshot import (
    build_top_database_tickers,
    build_top_theme_snapshot,
    market_backdrop_read_line,
)
from src.market_scout_navigation import build_market_scout_theme_lookup, market_scout_theme_candidates
from src.queries import latest_ticker_snapshots
from src.streamlit_utils import reset_perf_timings, show_perf_summary, stop_for_database_error
from src.theme_pattern_audit import (
    build_market_scout_items,
    build_theme_pattern_audit,
    format_theme_pattern_signal_evidence,
    no_market_scout_items_message,
)
from src.theme_selection import set_theme_selection_state
from src.theme_service import list_themes, seed_if_needed


DEFAULT_VISIBLE_SCOUT_ITEMS = 3
SCOUT_ITEM_LIMIT = 8
CANDIDATE_SIGNAL_LIMIT = 50
CLUSTER_SECTION_LABEL = "Theme / Cluster Reads"


def _install_report_styles() -> None:
    st.markdown(
        """
        <style>
        .market-scout-backdrop {
            border: 1px solid rgba(148, 163, 184, 0.22);
            background: rgba(15, 23, 42, 0.42);
            border-radius: 8px;
            padding: 10px 12px;
            margin: 8px 0 10px 0;
            color: #e2e8f0;
            font-size: 0.92rem;
            font-weight: 650;
        }
        .market-scout-kicker {
            color: #94a3b8;
            font-size: 0.76rem;
            text-transform: uppercase;
            letter-spacing: 0;
            margin-bottom: 4px;
        }
        .market-scout-headline {
            font-size: 1.04rem;
            font-weight: 700;
            margin-bottom: 10px;
        }
        .market-scout-value {
            font-size: 0.98rem;
            font-weight: 650;
            overflow-wrap: anywhere;
        }
        .market-scout-card {
            border: 1px solid rgba(148, 163, 184, 0.20);
            background: rgba(15, 23, 42, 0.32);
            border-radius: 8px;
        }
        .market-scout-card {
            padding: 14px 16px;
            margin: 10px 0;
        }
        .market-scout-card-title {
            font-size: 1.05rem;
            font-weight: 720;
            margin: 3px 0 6px 0;
        }
        .market-scout-card-copy {
            color: #cbd5e1;
            line-height: 1.42;
            margin-top: 8px;
        }
        .market-scout-card-subtle {
            color: #94a3b8;
            font-size: 0.82rem;
        }
        .market-scout-read {
            border-left: 2px solid rgba(96, 165, 250, 0.75);
            padding: 4px 0 4px 13px;
            margin: 8px 0 16px 0;
        }
        .market-scout-read div {
            margin: 5px 0;
            color: #cbd5e1;
            line-height: 1.38;
        }
        .market-scout-bullet {
            color: #60a5fa;
            font-weight: 700;
            margin-right: 7px;
        }
        .market-scout-chip {
            display: inline-block;
            border: 1px solid rgba(148, 163, 184, 0.28);
            border-radius: 999px;
            padding: 2px 8px;
            margin: 0 5px 5px 0;
            color: #dbeafe;
            background: rgba(59, 130, 246, 0.10);
            font-size: 0.74rem;
            white-space: nowrap;
        }
        .market-scout-ticker-chip {
            display: inline-block;
            border: 1px solid rgba(34, 197, 94, 0.30);
            border-radius: 6px;
            padding: 3px 8px;
            margin: 0 6px 6px 0;
            color: #dcfce7;
            background: rgba(34, 197, 94, 0.10);
            font-size: 0.80rem;
            font-weight: 700;
        }
        @media (max-width: 900px) {
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _signal_table(signals: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for signal in signals:
        rows.append(
            {
                "Signal Type": signal.get("signal_type") or "",
                "Theme": signal.get("theme") or "",
                "Tickers": ", ".join(str(ticker) for ticker in signal.get("tickers") or []),
                "Why Notable": signal.get("why_notable") or "",
                "Read": signal.get("read") or "",
            }
        )
    return pd.DataFrame(rows, columns=["Signal Type", "Theme", "Tickers", "Why Notable", "Read"])


def _latest_snapshot_label(conn) -> str:
    latest = latest_ticker_snapshots(conn)
    if latest.empty or "snapshot_time" not in latest.columns:
        return "-"
    snapshot_time = pd.to_datetime(latest["snapshot_time"], errors="coerce").dropna()
    if snapshot_time.empty:
        return "-"
    return snapshot_time.max().strftime("%Y-%m-%d %H:%M")


def _dominant_signal_type(signals: list[dict[str, object]]) -> str:
    counts = Counter(str(signal.get("signal_type") or "") for signal in signals if signal.get("signal_type"))
    if not counts:
        return "-"
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _top_repeated_tickers(signals: list[dict[str, object]], *, limit: int = 4) -> list[str]:
    repeated_tickers = Counter(
        str(ticker).strip().upper()
        for signal in signals
        for ticker in (signal.get("tickers") or [])
        if str(ticker or "").strip()
    )
    return [ticker for ticker, _count in repeated_tickers.most_common(limit)]


def _format_pct_metric(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}%"


def _format_pct_brief(value: object, *, decimals: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{int(decimals)}f}%"


def _format_ratio_metric(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}x"


def _chip(label: object) -> str:
    text = str(label or "").strip()
    if not text:
        return ""
    return f'<span class="market-scout-chip">{escape(text)}</span>'


def _ticker_chips(tickers: list[str]) -> str:
    if not tickers:
        return '<span class="market-scout-card-subtle">-</span>'
    return "".join(f'<span class="market-scout-ticker-chip">{escape(ticker)}</span>' for ticker in tickers)


def _market_backdrop_read(context: dict[str, object] | None) -> str:
    if not context:
        return "QQQ tape unavailable"
    move_label = str(context.get("move_label") or "QQQ").strip()
    character_tag = str(context.get("character_tag") or "").strip()
    pct = _format_pct_metric(context.get("qqq_pct_change"))
    if character_tag:
        return f"QQQ {pct}: {move_label} / {character_tag}"
    return f"QQQ {pct}: {move_label}"


def _market_backdrop_line(context: dict[str, object] | None) -> str:
    if not context:
        return "QQQ unavailable"
    move_label = str(context.get("move_label") or "-")
    character_tag = str(context.get("character_tag") or "-")
    return (
        f"QQQ {_format_pct_metric(context.get('qqq_pct_change'))} · "
        f"{move_label} / {character_tag} character · "
        f"Gap {_format_pct_metric(context.get('gap_pct'))} · "
        f"Close position {_format_pct_brief(context.get('close_position_pct'), decimals=0)} · "
        f"Range {_format_ratio_metric(context.get('range_x_atr_14'))} ATR(14)"
    )


def _render_qqq_market_tape_strip(context: dict[str, object] | None, unavailable_message: str | None = None) -> None:
    if not context:
        st.markdown(
            """
            <div class="market-scout-backdrop">
              <div class="market-scout-kicker">Market Backdrop</div>
              <div class="market-scout-headline">QQQ tape unavailable</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.warning(unavailable_message or qqq_market_context_unavailable_message())
        return

    st.markdown(
        f'<div class="market-scout-backdrop">{escape(_market_backdrop_line(context))}</div>',
        unsafe_allow_html=True,
    )
    st.caption(market_backdrop_read_line(context))
    with st.expander("Market backdrop details", expanded=False):
        detail_cols = st.columns(6)
        detail_cols[0].metric("QQQ % Change", _format_pct_metric(context.get("qqq_pct_change")))
        detail_cols[1].metric("Move Label", str(context.get("move_label") or "-"))
        detail_cols[2].metric("Character Tag", str(context.get("character_tag") or "-"))
        detail_cols[3].metric("Gap %", _format_pct_metric(context.get("gap_pct")))
        detail_cols[4].metric("Close Position", _format_pct_metric(context.get("close_position_pct")))
        detail_cols[5].metric("Range x ATR(14)", _format_ratio_metric(context.get("range_x_atr_14")))


def _item_tags(item: dict[str, object], qqq_market_context: dict[str, object] | None) -> list[str]:
    pattern = str(item.get("pattern") or "").strip()
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    tags: list[str] = []
    if "Overlap" in pattern:
        tags.append("Cross-Theme Overlap")
    if pattern == "Overlap Warning":
        tags.append("Scout Quality Note")
    if pattern == "Broad Same-Theme Thrust":
        tags.append("Broadly Confirmed")
        if bool(metadata.get("widespread")):
            tags.append("Market-Wide Breadth")
    if "Outlier-Led" in pattern:
        tags.append("Outlier-Led")
        if "narrow" in pattern.lower():
            tags.append("Narrow/Fragile")
        else:
            tags.append("Broadly Confirmed")
    if pattern == "Emerging Cluster":
        tags.append("Emerging Cluster")
    if qqq_market_context:
        move_label = str(qqq_market_context.get("move_label") or "").lower()
        character_tag = str(qqq_market_context.get("character_tag") or "").lower()
        if "down" in move_label or "fade" in character_tag:
            tags.append("Weak QQQ Tape")
        if "quiet" in character_tag or "down" in move_label:
            tags.append("Relative Strength")
    return list(dict.fromkeys(tags or [pattern or "Scout Item"]))


def _is_quality_note(item: dict[str, object]) -> bool:
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    return bool(metadata.get("quality_note")) or str(item.get("pattern") or "") == "Overlap Warning"


def _section_for_item(item: dict[str, object]) -> str:
    pattern = str(item.get("pattern") or "")
    if pattern in {"Coherent Overlap Cluster", "Broad Same-Theme Thrust", "Emerging Cluster"}:
        return CLUSTER_SECTION_LABEL
    if pattern.startswith("Outlier-Led") or pattern == "Extreme Ticker Standout":
        return "Outlier-Led Themes To Verify"
    return CLUSTER_SECTION_LABEL


def _dominant_signal_interpretation(signals: list[dict[str, object]]) -> str:
    dominant = _dominant_signal_type(signals)
    if dominant == "Extreme Ticker Standout":
        return "Strength is concentrated in standout leaders, so breadth confirmation matters."
    if dominant == "Broad Same-Theme Thrust":
        return "Multiple names are moving together inside themes, so confirm the cluster quality."
    if dominant.startswith("Outlier-Led Theme"):
        return "Outlier-led moves need supporting-name confirmation before trusting the group read."
    if dominant == "Emerging Cluster":
        return "Lower-ranked groups are starting to show clustered strength."
    if dominant == "-":
        return "No dominant deterministic signal type is available."
    return f"{dominant} signals are most common; use the cards below for the concrete review path."


def _review_path_sentence(items: list[dict[str, object]]) -> str:
    if not items:
        return "No primary review cards are available for the current filters."
    if len(items) >= 3 and str(items[0].get("pattern") or "") == "Coherent Overlap Cluster":
        outlier_leaders = [
            _primary_ticker_label(item)
            for item in items[1:]
            if str(item.get("pattern") or "").startswith("Outlier-Led")
        ]
        outlier_leaders = [leader for leader in outlier_leaders if leader]
        if len(outlier_leaders) >= 2:
            return (
                f"Start with the {_overlap_review_label(items[0])}, then verify "
                f"{outlier_leaders[0]} and {outlier_leaders[1]}-led groups for breadth."
            )
    first = _display_headline(items[0])
    second = _display_headline(items[1]) if len(items) > 1 else ""
    if second:
        return f"Start with {first}, then verify {second}."
    return f"Start with {first}."


def _render_scout_read(
    *,
    scout_items: list[dict[str, object]],
    filtered_signals: list[dict[str, object]],
    qqq_market_context: dict[str, object] | None,
    quality_notes: list[dict[str, object]],
) -> None:
    repeated = _top_repeated_tickers(filtered_signals)
    note = quality_notes[0] if quality_notes else None
    bullets = [
        _market_backdrop_implication(qqq_market_context),
        _dominant_signal_interpretation(filtered_signals),
        (
            f"Repeated names to keep in view: {', '.join(repeated)}."
            if repeated and not note
            else str((note or {}).get("why_it_matters") or "No repeated-name quality warning is active.")
        ),
        _review_path_sentence(scout_items),
    ]
    clean_bullets = [str(bullet).strip().lstrip(". ").strip() for bullet in bullets if str(bullet or "").strip()]
    bullet_html = "".join(
        f'<div><span class="market-scout-bullet">&bull;</span>{escape(bullet)}</div>'
        for bullet in clean_bullets
    )
    st.markdown(f'<div class="market-scout-read">{bullet_html}</div>', unsafe_allow_html=True)


def _market_backdrop_implication(context: dict[str, object] | None) -> str:
    if not context:
        return "QQQ backdrop is unavailable, so review cards without tape context."
    move_label = str(context.get("move_label") or "QQQ").strip()
    character_tag = str(context.get("character_tag") or "").strip()
    if "Down" in move_label:
        return "Weak QQQ tape makes positive clusters more notable."
    if character_tag == "Quiet":
        return "Quiet QQQ tape makes repeated theme-level strength more likely to be group-specific."
    if "Up" in move_label:
        return "Strong QQQ tape raises the bar; favor cards with clear outperformance or breadth."
    pct = _format_pct_metric(context.get("qqq_pct_change"))
    return f"Market backdrop: QQQ {pct}, {move_label} / {character_tag or '-'}."


def _display_headline(item: dict[str, object]) -> str:
    pattern = str(item.get("pattern") or "")
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    themes = [str(theme).strip() for theme in metadata.get("themes") or [] if str(theme).strip()]
    if pattern == "Coherent Overlap Cluster":
        confidence = str(metadata.get("coherence_confidence") or "").strip()
        family = str(metadata.get("overlap_family") or "").strip()
        if _is_broad_ai_software_overlap(metadata):
            return "Overlapping AI/software/infrastructure strength needs breadth confirmation"
        if confidence == "strong" and family and family != "Mixed":
            return f"{family} strength is overlapping across multiple themes"
        return "Shared high-momentum names are driving multiple theme alerts"
    if pattern == "Overlap Warning":
        return "Several alerts overlap through the same standout tickers"
    if pattern == "Outlier-Led Theme (Broadly Confirmed)":
        theme = str(metadata.get("theme") or "").strip()
        return f"{theme} has an extreme leader with supporting participation" if theme else "Extreme leader has supporting participation"
    if pattern == "Outlier-Led Theme (Narrow/Fragile)":
        theme = str(metadata.get("theme") or "").strip()
        return f"{theme} depends heavily on one standout name" if theme else "One standout name may be distorting the read"
    if pattern == "Emerging Cluster":
        theme = str(metadata.get("theme") or "").strip()
        return f"{theme} is building early clustered strength" if theme else "Early clustered strength is forming"
    if themes:
        return f"{themes[0]} is the first review area"
    return str(item.get("headline") or pattern or "Scout item")


def _primary_ticker_label(item: dict[str, object]) -> str:
    tickers = [str(ticker).strip().upper() for ticker in (item.get("tickers_to_inspect") or []) if str(ticker).strip()]
    return tickers[0] if tickers else ""


def _is_broad_ai_software_overlap(metadata: dict[str, object]) -> bool:
    themes = " ".join(str(theme).lower() for theme in metadata.get("themes") or [])
    family = str(metadata.get("overlap_family") or "").lower()
    return "ai" in themes and any(token in themes or token in family for token in ("software", "infrastructure", "cloud"))


def _overlap_review_label(item: dict[str, object]) -> str:
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    if _is_broad_ai_software_overlap(metadata):
        return "overlapping AI/software/infrastructure cluster"
    family = str(metadata.get("overlap_family") or "").strip()
    if family and family != "Mixed":
        return f"overlapping {family.lower()} cluster"
    return "overlapping repeated-name cluster"


def _review_action(item: dict[str, object]) -> str:
    text = str(item.get("next_look") or "").strip()
    text = text.replace("Inspect", "Check", 1) if text.startswith("Inspect") else text
    return text or "Open the listed tickers first, then confirm breadth and follow-through."


def _render_scout_item(
    item: dict[str, object],
    *,
    idx: int,
    filtered_signals: list[dict[str, object]],
    qqq_market_context: dict[str, object] | None,
    theme_lookup: dict[str, dict[str, object]],
) -> None:
    headline = _display_headline(item)
    pattern = str(item.get("pattern") or "").strip()
    tickers = [str(ticker).strip().upper() for ticker in (item.get("tickers_to_inspect") or []) if str(ticker or "").strip()]
    tag_html = "".join(_chip(tag) for tag in _item_tags(item, qqq_market_context))
    ticker_html = _ticker_chips(tickers)
    st.markdown(
        f"""
        <div class="market-scout-card">
          <div class="market-scout-card-subtle">#{idx} {escape(pattern)}</div>
          <div class="market-scout-card-title">{escape(headline)}</div>
          <div>{tag_html}</div>
          <div class="market-scout-card-copy">{ticker_html}</div>
          <div class="market-scout-card-copy">{escape(str(item.get("why_it_matters") or "-"))}</div>
          <div class="market-scout-card-copy"><strong>Review action:</strong> {escape(_review_action(item))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _render_theme_open_actions(item, theme_lookup, key_prefix=f"scout_item_{idx}_{pattern}")
    signal_indices = [int(value) for value in item.get("signal_indices") or []]
    if signal_indices:
        with st.expander("Trigger evidence", expanded=False):
            for signal_idx in signal_indices:
                if signal_idx < 0 or signal_idx >= len(filtered_signals):
                    continue
                signal = filtered_signals[signal_idx]
                st.caption(f"{signal.get('signal_type')} - {signal.get('theme')}")
                for evidence in format_theme_pattern_signal_evidence(signal):
                    st.caption(f"- {evidence}")


def _open_theme(theme_id: int, label: str) -> None:
    set_theme_selection_state(st.session_state, int(theme_id), str(label), "market_scout")
    st.switch_page("pages/1_Themes.py")


def _render_theme_open_actions(
    item: dict[str, object],
    theme_lookup: dict[str, dict[str, object]],
    *,
    key_prefix: str,
) -> None:
    candidates = market_scout_theme_candidates(item, theme_lookup)
    if not candidates:
        return
    if len(candidates) == 1:
        candidate = candidates[0]
        if st.button("Open theme", key=f"{key_prefix}_open_theme_{candidate['theme_id']}"):
            _open_theme(int(candidate["theme_id"]), str(candidate["label"]))
        return

    st.caption("Related themes")
    cols = st.columns(min(len(candidates), 4))
    for col, candidate in zip(cols, candidates, strict=False):
        with col:
            if st.button(str(candidate["name"]), key=f"{key_prefix}_open_theme_{candidate['theme_id']}"):
                _open_theme(int(candidate["theme_id"]), str(candidate["label"]))


def _render_five_minute_snapshot(
    top_tickers: pd.DataFrame,
    top_themes: pd.DataFrame,
    theme_lookup: dict[str, dict[str, object]],
) -> None:
    st.subheader("Opening Brief")
    ticker_col, theme_col = st.columns([1.35, 1])
    with ticker_col:
        st.caption("Ticker Standouts")
        if top_tickers.empty:
            st.info("No eligible current ticker snapshot is available.")
        else:
            st.dataframe(top_tickers, use_container_width=True, hide_index=True)
    with theme_col:
        st.caption("Theme Leadership")
        if top_themes.empty:
            st.info("No current theme snapshot is available.")
        else:
            st.dataframe(top_themes, use_container_width=True, hide_index=True)
            theme_options = [
                str(candidate["label"])
                for theme_name in top_themes["Theme"].tolist()
                if (candidate := theme_lookup.get(str(theme_name).casefold()))
            ]
            if theme_options:
                selected_theme = st.selectbox("Open theme", options=theme_options, index=None, placeholder="Select theme")
                if selected_theme:
                    candidate = theme_lookup[str(selected_theme).casefold()]
                    _open_theme(int(candidate["theme_id"]), str(candidate["label"]))


st.set_page_config(page_title="Market Scout", layout="wide")
_install_report_styles()
st.title("Market Scout")
st.caption(
    "Triage layer from deterministic stored signals. Use it to choose what to open first, not as a replacement for manual review."
)
reset_perf_timings("theme_pattern_audit")

try:
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        theme_catalog = list_themes(conn, active_only=False)
        theme_lookup = build_market_scout_theme_lookup(theme_catalog)
        qqq_market_context = latest_qqq_market_context(conn)
        latest_qqq_date = latest_qqq_history_date(conn)
        all_signals = build_theme_pattern_audit(
            conn,
            limit=CANDIDATE_SIGNAL_LIMIT,
            qqq_market_context=qqq_market_context,
        )
        top_database_tickers = build_top_database_tickers(conn, limit=10)
        top_theme_snapshot = build_top_theme_snapshot(conn, limit=5)
        latest_snapshot = _latest_snapshot_label(conn)
except Exception as exc:
    stop_for_database_error(exc)

_render_qqq_market_tape_strip(
    qqq_market_context,
    qqq_market_context_unavailable_message(latest_qqq_history_date_value=latest_qqq_date),
)
if not qqq_market_context:
    if st.button("Backfill QQQ market context", key="market_scout_backfill_qqq"):
        try:
            with get_conn() as conn:
                result = backfill_qqq_market_context_history(conn)
            if result.get("status") == "success":
                st.success(
                    f"QQQ market context backfill complete. Rows written `{int(result.get('rows_written') or 0)}`; skipped `{int(result.get('rows_skipped') or 0)}`."
                )
                st.rerun()
            else:
                st.warning(str(result.get("message") or qqq_market_context_unavailable_message()))
        except Exception as exc:
                st.error(f"QQQ market context backfill failed: {exc}")

_render_five_minute_snapshot(top_database_tickers, top_theme_snapshot, theme_lookup)

signal_types = sorted({str(signal.get("signal_type") or "") for signal in all_signals if signal.get("signal_type")})
selected_signal_types = st.session_state.get("market_scout_signal_types", signal_types)

filtered_signals = [
    signal
    for signal in all_signals
    if not selected_signal_types or str(signal.get("signal_type") or "") in set(selected_signal_types)
]
scout_items = build_market_scout_items(
    filtered_signals,
    qqq_market_context=qqq_market_context,
    limit=SCOUT_ITEM_LIMIT,
)
quality_notes = [item for item in scout_items if _is_quality_note(item)]
review_items = [item for item in scout_items if not _is_quality_note(item)]

st.subheader("Scout Read")
if scout_items:
    _render_scout_read(
        scout_items=review_items or scout_items,
        filtered_signals=filtered_signals,
        qqq_market_context=qqq_market_context,
        quality_notes=quality_notes,
    )
else:
    st.info(no_market_scout_items_message())

if not review_items:
    st.info(no_market_scout_items_message())
else:
    visible_items = review_items[:DEFAULT_VISIBLE_SCOUT_ITEMS]
    additional_items = review_items[DEFAULT_VISIBLE_SCOUT_ITEMS:]
    for section_name in (CLUSTER_SECTION_LABEL, "Outlier-Led Themes To Verify"):
        section_items = [item for item in visible_items if _section_for_item(item) == section_name]
        if not section_items:
            continue
        st.subheader(section_name)
        for idx, item in enumerate(section_items, start=1):
            _render_scout_item(
                item,
                idx=idx,
                filtered_signals=filtered_signals,
                qqq_market_context=qqq_market_context,
                theme_lookup=theme_lookup,
            )
    if additional_items:
        with st.expander(f"Show more scout items ({len(additional_items)})", expanded=False):
            for section_name in (CLUSTER_SECTION_LABEL, "Outlier-Led Themes To Verify"):
                section_items = [item for item in additional_items if _section_for_item(item) == section_name]
                if not section_items:
                    continue
                st.markdown(f"**{section_name}**")
                for idx, item in enumerate(section_items, start=DEFAULT_VISIBLE_SCOUT_ITEMS + 1):
                    _render_scout_item(
                        item,
                        idx=idx,
                        filtered_signals=filtered_signals,
                        qqq_market_context=qqq_market_context,
                        theme_lookup=theme_lookup,
                    )

if quality_notes:
    st.subheader("Scout Quality Notes")
    for idx, item in enumerate(quality_notes, start=1):
        _render_scout_item(
            item,
            idx=idx,
            filtered_signals=filtered_signals,
            qqq_market_context=qqq_market_context,
            theme_lookup=theme_lookup,
        )

with st.expander("Advanced filters", expanded=False):
    control_cols = st.columns([2, 1])
    with control_cols[0]:
        st.multiselect(
            "Signal type",
            options=signal_types,
            default=selected_signal_types,
            placeholder="All signal types",
            key="market_scout_signal_types",
        )
    with control_cols[1]:
        st.caption(f"Showing top {DEFAULT_VISIBLE_SCOUT_ITEMS} first; additional items stay collapsed.")

with st.expander("Underlying Evidence", expanded=False):
    st.caption(f"Latest snapshot: `{latest_snapshot}`")
    if not filtered_signals:
        st.info("No deterministic rule-trigger signals are available for the current filters.")
    else:
        st.dataframe(
            _signal_table(filtered_signals),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Signal Type": st.column_config.TextColumn(width="medium"),
                "Theme": st.column_config.TextColumn(width="medium"),
                "Tickers": st.column_config.TextColumn(width="medium"),
                "Why Notable": st.column_config.TextColumn(width="large"),
                "Read": st.column_config.TextColumn(width="large"),
            },
        )

show_perf_summary()
