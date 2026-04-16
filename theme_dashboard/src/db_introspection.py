from __future__ import annotations

import duckdb


def _is_duckdb_result_state_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return any(
        token in message
        for token in {
            "no open result set",
            "closed pending query result",
            "unsuccessful or closed pending query result",
        }
    )


def _is_duckdb_internal_poisoned_state(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return any(
        token in message
        for token in {
            "attempted to dereference unique_ptr that is null",
            "internal error",
        }
    )


def _should_retry_with_bootstrap(exc: Exception) -> bool:
    return _is_duckdb_result_state_error(exc) or _is_duckdb_internal_poisoned_state(exc)


def _fetch_exists_row(conn, sql: str, params: list[str]) -> bool:
    try:
        row = conn.execute(sql, params).fetchone()
    except (duckdb.InvalidInputException, duckdb.InternalException) as exc:
        if not _should_retry_with_bootstrap(exc):
            raise
        from .database import get_bootstrap_conn

        with get_bootstrap_conn() as bootstrap_conn:
            row = bootstrap_conn.execute(sql, params).fetchone()
    return bool(row)


def _should_use_isolated_metadata_conn(conn) -> bool:
    try:
        row = conn.execute("PRAGMA database_list").fetchone()
    except duckdb.Error:
        return False
    if not row:
        return False
    return str(row[1] or "").strip().lower() != "memory"


def table_has_column(conn, table_name: str, column_name: str) -> bool:
    sql = """
        SELECT 1
        FROM duckdb_columns()
        WHERE table_name = ?
          AND column_name = ?
        LIMIT 1
        """
    params = [str(table_name or "").strip(), str(column_name or "").strip()]
    if _should_use_isolated_metadata_conn(conn):
        from .database import get_bootstrap_conn

        # Avoid querying schema metadata on the live shared Streamlit connection.
        with get_bootstrap_conn() as bootstrap_conn:
            return _fetch_exists_row(bootstrap_conn, sql, params)
    return _fetch_exists_row(conn, sql, params)


def table_exists(conn, table_name: str) -> bool:
    sql = """
        SELECT 1
        FROM duckdb_tables()
        WHERE table_name = ?
        LIMIT 1
        """
    params = [str(table_name or "").strip()]
    if _should_use_isolated_metadata_conn(conn):
        from .database import get_bootstrap_conn

        # Avoid querying schema metadata on the live shared Streamlit connection.
        with get_bootstrap_conn() as bootstrap_conn:
            return _fetch_exists_row(bootstrap_conn, sql, params)
    return _fetch_exists_row(conn, sql, params)
