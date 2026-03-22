from __future__ import annotations


def table_has_column(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM duckdb_columns()
        WHERE table_name = ?
          AND column_name = ?
        LIMIT 1
        """,
        [str(table_name or "").strip(), str(column_name or "").strip()],
    ).fetchone()
    return bool(row)


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM duckdb_tables()
        WHERE table_name = ?
        LIMIT 1
        """,
        [str(table_name or "").strip()],
    ).fetchone()
    return bool(row)
