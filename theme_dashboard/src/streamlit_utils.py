from __future__ import annotations

import time
from pathlib import Path

import streamlit as st

from .database import DB_PATH, DatabaseLockedError, database_path_str, get_conn, get_fresh_read_conn
from .inflection_engine import compute_theme_inflections
from .momentum_engine import compute_theme_momentum
from .queries import theme_health_overview
from .rankings import compute_current_ranking_snapshot, compute_theme_rankings
from .scanner_audit import scanner_candidate_summary

def extract_selected_row(event) -> int | None:
    """Best-effort extraction of a selected row index across Streamlit event payload shapes."""
    selection = {}
    if isinstance(event, dict):
        selection = event.get("selection", {}) or {}
    elif hasattr(event, "selection"):
        selection = event.selection

    rows = selection.get("rows", []) if isinstance(selection, dict) else getattr(selection, "rows", [])
    for row in rows or []:
        if row is not None:
            try:
                return int(row)
            except (TypeError, ValueError):
                continue

    cells = selection.get("cells", []) if isinstance(selection, dict) else getattr(selection, "cells", [])
    for cell in cells or []:
        row_value = None
        if isinstance(cell, dict):
            row_value = cell.get("row")
        elif isinstance(cell, (tuple, list)) and cell:
            row_value = cell[0]
        elif hasattr(cell, "row"):
            row_value = getattr(cell, "row")
        if row_value is None:
            continue
        try:
            return int(row_value)
        except (TypeError, ValueError):
            continue
    return None


def db_cache_token() -> tuple[str, int]:
    path = Path(DB_PATH)
    try:
        stamp = path.stat().st_mtime_ns
    except FileNotFoundError:
        stamp = 0
    return str(path), int(stamp)


def _perf_enabled() -> bool:
    try:
        return str(st.query_params.get("perf", "0")).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def reset_perf_timings(page_key: str) -> None:
    if not _perf_enabled():
        return
    active_key = str(st.session_state.get("_perf_page_key") or "")
    if active_key != page_key:
        st.session_state["_perf_page_key"] = page_key
        st.session_state["_perf_timings"] = []


def _record_perf(stage: str, label: str, seconds: float, *, rows: int | None = None, cols: int | None = None) -> None:
    if not _perf_enabled():
        return
    timings = st.session_state.setdefault("_perf_timings", [])
    timings.append(
        {
            "stage": stage,
            "label": label,
            "seconds": round(float(seconds), 4),
            "rows": rows,
            "cols": cols,
        }
    )


def show_perf_summary() -> None:
    if not _perf_enabled():
        return
    timings = st.session_state.get("_perf_timings", [])
    if not timings:
        return
    with st.expander("Performance Trace", expanded=False):
        st.caption("Enable with `?perf=1`. Timings are per rerun and help separate cached load vs render cost.")
        st.dataframe(timings, width="stretch", hide_index=True)


def stop_for_database_error(exc: Exception) -> None:
    if isinstance(exc, DatabaseLockedError):
        st.error(str(exc))
        st.caption(
            "Close the other process using the database and refresh this page. "
            f"Expected DB file: `{database_path_str()}`"
        )
        st.stop()
    raise exc


def _shape_of(value) -> tuple[int | None, int | None]:
    shape = getattr(value, "shape", None)
    if not shape or len(shape) != 2:
        return None, None
    return int(shape[0]), int(shape[1])


def render_dataframe(label: str, df, **kwargs):
    started = time.perf_counter()
    event = st.dataframe(df, **kwargs)
    elapsed = time.perf_counter() - started
    rows, cols = _shape_of(df)
    _record_perf("render", label, elapsed, rows=rows, cols=cols)
    return event


def _timed_cached_load(label: str, loader, *args):
    started = time.perf_counter()
    value = loader(*args)
    elapsed = time.perf_counter() - started
    rows, cols = (None, None)
    if not isinstance(value, dict):
        rows, cols = _shape_of(value)
    _record_perf("load", label, elapsed, rows=rows, cols=cols)
    return value


@st.cache_data(show_spinner=False)
def _load_current_ranking_snapshot_cached(_db_token: tuple[str, int]):
    with get_conn() as conn:
        return compute_current_ranking_snapshot(conn)


def load_current_ranking_snapshot_cached(db_token: tuple[str, int]):
    return _timed_cached_load("current_ranking_snapshot", _load_current_ranking_snapshot_cached, db_token)


def clear_current_market_view_caches() -> None:
    _load_current_ranking_snapshot_cached.clear()
    _load_theme_rankings_cached.clear()
    _load_theme_health_overview_cached.clear()


@st.cache_data(show_spinner=False)
def _load_theme_rankings_cached(_db_token: tuple[str, int]):
    with get_conn() as conn:
        return compute_theme_rankings(conn)


def load_theme_rankings_cached(db_token: tuple[str, int]):
    return _timed_cached_load("theme_rankings", _load_theme_rankings_cached, db_token)


@st.cache_data(show_spinner=False)
def _load_theme_momentum_cached(_db_token: tuple[str, int], lookback_days: int, top_n: int):
    with get_fresh_read_conn() as conn:
        return compute_theme_momentum(conn, int(lookback_days), top_n=int(top_n))


def load_theme_momentum_cached(db_token: tuple[str, int], lookback_days: int, top_n: int = 20):
    return _timed_cached_load("theme_momentum", _load_theme_momentum_cached, db_token, int(lookback_days), int(top_n))


@st.cache_data(show_spinner=False)
def _load_theme_inflections_cached(_db_token: tuple[str, int], lookback_days: int, top_n: int):
    with get_fresh_read_conn() as conn:
        return compute_theme_inflections(conn, int(lookback_days), top_n=int(top_n))


def load_theme_inflections_cached(db_token: tuple[str, int], lookback_days: int, top_n: int = 20):
    return _timed_cached_load("theme_inflections", _load_theme_inflections_cached, db_token, int(lookback_days), int(top_n))


@st.cache_data(show_spinner=False)
def _load_theme_health_overview_cached(_db_token: tuple[str, int], low_constituent_threshold: int, failure_window_days: int):
    with get_conn() as conn:
        return theme_health_overview(conn, int(low_constituent_threshold), int(failure_window_days))


def load_theme_health_overview_cached(db_token: tuple[str, int], low_constituent_threshold: int, failure_window_days: int = 14):
    return _timed_cached_load(
        "theme_health_overview",
        _load_theme_health_overview_cached,
        db_token,
        int(low_constituent_threshold),
        int(failure_window_days),
    )


@st.cache_data(show_spinner=False)
def _load_scanner_candidate_summary_cached(_db_token: tuple[str, int]):
    with get_conn() as conn:
        return scanner_candidate_summary(conn)


def load_scanner_candidate_summary_cached(db_token: tuple[str, int]):
    return _timed_cached_load("scanner_candidate_summary", _load_scanner_candidate_summary_cached, db_token)


def clear_scanner_candidate_summary_cache() -> None:
    _load_scanner_candidate_summary_cached.clear()
