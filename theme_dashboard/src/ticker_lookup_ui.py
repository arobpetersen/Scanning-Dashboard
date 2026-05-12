from __future__ import annotations

import pandas as pd

from src.metric_formatting import short_timestamp
from src.queries import ticker_lookup_memberships, ticker_lookup_summary


def normalize_ticker_lookup_input(value: object) -> str:
    return str(value or "").strip().upper()


def _format_percent(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    try:
        return f"{float(value):+.1f}%"
    except (TypeError, ValueError):
        return None


def compact_membership_theme_labels(memberships: pd.DataFrame) -> list[str]:
    if memberships.empty:
        return []

    rows = memberships.copy()
    if "theme_id" in rows.columns:
        rows["theme_id"] = pd.to_numeric(rows["theme_id"], errors="coerce")
    rows["theme_name"] = rows.get("theme_name", "").fillna("").astype(str)
    rows["category"] = rows.get("category", "").fillna("").astype(str)
    rows = rows.sort_values(["theme_name", "category", "theme_id"], kind="stable")

    base_labels = rows.apply(lambda row: f"{row['theme_name']} ({row['category']})", axis=1)
    duplicate_bases = set(base_labels[base_labels.duplicated(keep=False)].tolist())

    labels: list[str] = []
    for idx, row in rows.iterrows():
        base_label = str(base_labels.loc[idx])
        theme_id = row.get("theme_id")
        label = f"{base_label} [#{int(theme_id)}]" if base_label in duplicate_bases and pd.notna(theme_id) else base_label
        is_active = row.get("is_active", True)
        if pd.notna(is_active) and not bool(is_active):
            label = f"{label} inactive"
        labels.append(label)
    return labels


def compact_ticker_lookup_lines(summary: pd.DataFrame, memberships: pd.DataFrame, ticker: str) -> list[str]:
    normalized = normalize_ticker_lookup_input(ticker)
    if not normalized:
        return []

    if summary.empty:
        return [f"`{normalized}` not found in governed membership or stored snapshots."]

    row = summary.iloc[0]
    labels = compact_membership_theme_labels(memberships)
    exists_in_snapshots = bool(row.get("exists_in_ticker_snapshots"))
    exists_elsewhere = bool(row.get("exists_in_refresh_run_tickers")) or bool(row.get("exists_in_symbol_refresh_status"))

    if labels:
        lines = [f"**{normalized}:** " + ", ".join(labels)]
    elif exists_in_snapshots or exists_elsewhere:
        lines = [f"**{normalized}:** no governed theme assignment."]
    else:
        lines = [f"`{normalized}` not found in governed membership or stored snapshots."]

    context_bits: list[str] = []
    perf_1w = _format_percent(row.get("preferred_perf_1w"))
    perf_1m = _format_percent(row.get("preferred_perf_1m"))
    if perf_1w is not None:
        context_bits.append(f"1W {perf_1w}")
    if perf_1m is not None:
        context_bits.append(f"1M {perf_1m}")

    status = str(row.get("lookup_status") or "").strip()
    if bool(row.get("manually_suppressed")):
        status = "manual suppression"
    elif bool(row.get("operationally_suppressed")):
        status = "refresh suppressed"
    if status:
        context_bits.append(status)

    snapshot_time = row.get("preferred_snapshot_time") if pd.notna(row.get("preferred_snapshot_time")) else row.get("latest_snapshot_time")
    snapshot_label = short_timestamp(snapshot_time)
    if snapshot_label:
        context_bits.append(snapshot_label)

    if context_bits:
        lines.append(" | ".join(context_bits))
    return lines


def render_compact_ticker_lookup(st_runtime, conn_factory, *, key: str, label: str = "Ticker Lookup") -> None:
    raw_ticker = st_runtime.text_input(
        label,
        key=key,
        placeholder="NVDA",
        label_visibility="visible",
    )
    ticker = normalize_ticker_lookup_input(raw_ticker)
    if not ticker:
        return

    try:
        with conn_factory() as conn:
            summary = ticker_lookup_summary(conn, ticker)
            memberships = ticker_lookup_memberships(conn, ticker)
    except Exception as exc:
        st_runtime.caption(f"Lookup failed: {exc}")
        return

    for idx, line in enumerate(compact_ticker_lookup_lines(summary, memberships, ticker)):
        if idx == 0:
            st_runtime.markdown(line)
        else:
            st_runtime.caption(line)
