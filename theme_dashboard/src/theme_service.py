from __future__ import annotations

import duckdb
import pandas as pd

from .config import SEED_PATH
from .db_introspection import table_exists, table_has_column
from .seed_loader import load_seed_file
from .ticker_onboarding import record_new_governed_ticker_onboarding


def _normalize_theme_name(name: str) -> str:
    value = name.strip()
    if not value:
        raise ValueError("Theme name cannot be empty.")
    return value


def _normalize_category(category: str) -> str:
    value = category.strip()
    return value if value else "Uncategorized"


def _normalize_ticker(ticker: str) -> str:
    value = ticker.strip().upper()
    if not value:
        raise ValueError("Ticker cannot be empty.")
    return value


def _theme_membership_theme_predicate(column: str = "theme_id") -> str:
    # DuckDB has shown a live-path edge case where `theme_membership.theme_id = ?`
    # can miss rows that are still visible through joins/range predicates.
    # Keep membership reads/writes on a single exact-id range predicate instead.
    return f"{column} BETWEEN ? AND ?"


def _theme_membership_theme_params(theme_id: int) -> list[int]:
    normalized_theme_id = int(theme_id)
    return [normalized_theme_id, normalized_theme_id]


def _manual_suppression_enabled(conn) -> bool:
    return table_exists(conn, "symbol_refresh_status") and table_has_column(conn, "symbol_refresh_status", "manual_suppressed")


def _manual_suppression_filter_sql(conn, ticker_expr: str) -> str:
    if not _manual_suppression_enabled(conn):
        return ""
    return (
        " AND NOT EXISTS ("
        "SELECT 1 FROM symbol_refresh_status s "
        f"WHERE upper(trim(s.ticker)) = upper(trim({ticker_expr})) "
        "AND COALESCE(s.manual_suppressed, FALSE)"
        ")"
    )


def _ensure_symbol_refresh_row(conn, ticker: str) -> None:
    if not table_exists(conn, "symbol_refresh_status"):
        return
    normalized_ticker = _normalize_ticker(ticker)
    conn.execute(
        """
        INSERT INTO symbol_refresh_status(ticker, status, updated_at)
        VALUES (?, 'active', CURRENT_TIMESTAMP)
        ON CONFLICT(ticker) DO NOTHING
        """,
        [normalized_ticker],
    )


def ticker_manual_suppression_state(conn, ticker: str) -> dict[str, object]:
    normalized_ticker = _normalize_ticker(ticker)
    if not _manual_suppression_enabled(conn):
        return {
            "ticker": normalized_ticker,
            "manual_suppressed": False,
            "manual_suppression_reason": None,
            "manual_suppressed_at": None,
        }
    row = conn.execute(
        """
        SELECT
            COALESCE(manual_suppressed, FALSE) AS manual_suppressed,
            manual_suppression_reason,
            manual_suppressed_at
        FROM symbol_refresh_status
        WHERE upper(trim(ticker)) = ?
        QUALIFY ROW_NUMBER() OVER (ORDER BY updated_at DESC NULLS LAST) = 1
        """,
        [normalized_ticker],
    ).fetchone()
    if not row:
        return {
            "ticker": normalized_ticker,
            "manual_suppressed": False,
            "manual_suppression_reason": None,
            "manual_suppressed_at": None,
        }
    return {
        "ticker": normalized_ticker,
        "manual_suppressed": bool(row[0]),
        "manual_suppression_reason": row[1],
        "manual_suppressed_at": row[2],
    }


def set_manual_ticker_suppression(conn, ticker: str, reason: str) -> dict[str, object]:
    normalized_ticker = _normalize_ticker(ticker)
    suppression_reason = str(reason or "").strip()
    if not suppression_reason:
        raise ValueError("Suppression reason is required.")
    current = ticker_manual_suppression_state(conn, normalized_ticker)
    if bool(current.get("manual_suppressed")) and str(current.get("manual_suppression_reason") or "") == suppression_reason:
        return {**current, "changed": False}
    _ensure_symbol_refresh_row(conn, normalized_ticker)
    now = pd.Timestamp.utcnow().to_pydatetime().replace(tzinfo=None)
    conn.execute(
        """
        UPDATE symbol_refresh_status
        SET status = 'refresh_suppressed',
            manual_suppressed = TRUE,
            manual_suppression_reason = ?,
            manual_suppressed_at = ?,
            suppression_reason = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE upper(trim(ticker)) = ?
        """,
        [suppression_reason, now, suppression_reason, normalized_ticker],
    )
    return {
        "ticker": normalized_ticker,
        "manual_suppressed": True,
        "manual_suppression_reason": suppression_reason,
        "manual_suppressed_at": now,
        "changed": True,
    }


def clear_manual_ticker_suppression(conn, ticker: str) -> dict[str, object]:
    normalized_ticker = _normalize_ticker(ticker)
    current = ticker_manual_suppression_state(conn, normalized_ticker)
    if not bool(current.get("manual_suppressed")):
        return {**current, "changed": False}
    _ensure_symbol_refresh_row(conn, normalized_ticker)
    conn.execute(
        """
        UPDATE symbol_refresh_status
        SET status = 'active',
            manual_suppressed = FALSE,
            manual_suppression_reason = NULL,
            manual_suppressed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE upper(trim(ticker)) = ?
        """,
        [normalized_ticker],
    )
    return {
        "ticker": normalized_ticker,
        "manual_suppressed": False,
        "manual_suppression_reason": None,
        "manual_suppressed_at": None,
        "changed": True,
    }


def _is_duckdb_result_state_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return any(
        token in message
        for token in {
            "no open result set",
            "closed pending query result",
            "unsuccessful or closed pending query result",
            "result closed",
        }
    )


class SeedQueryResultStateError(RuntimeError):
    """Raised when a required seed query returns no row from a shared connection."""


def _fetchone_required(result, context: str):
    row = result.fetchone()
    if row is None:
        raise SeedQueryResultStateError(f"Seed query returned no row: {context}")
    return row


def _query_df_with_bootstrap_recovery(loader) -> pd.DataFrame:
    try:
        return loader()
    except duckdb.InvalidInputException as exc:
        if not _is_duckdb_result_state_error(exc):
            raise
        from .database import get_bootstrap_conn

        with get_bootstrap_conn() as bootstrap_conn:
            return loader(bootstrap_conn)


def _seed_if_needed_core(conn) -> bool:
    """Idempotent seed/backfill.

    Seeds themes and membership when DB is empty, backfills membership when the membership
    table is empty, and seeds memberships for newly inserted themes. Once governed
    membership has been established, it intentionally does not recreate missing seed themes
    or re-add missing membership rows for already-existing themes, so manual deletions and
    removals remain removed.
    """
    seed_themes = load_seed_file(SEED_PATH)

    prepared_themes: list[tuple[str, str, list[str]]] = []
    for theme in seed_themes:
        name = theme.get("name", "").strip()
        if not name:
            continue

        category = _normalize_category(theme.get("category", "Uncategorized"))
        tickers = sorted({_normalize_ticker(t) for t in theme.get("tickers", []) if t and t.strip()})
        prepared_themes.append((name, category, tickers))

    if not prepared_themes:
        return False

    themes_count = int(_fetchone_required(conn.execute("SELECT COUNT(*) FROM themes"), "themes count")[0])
    membership_count = int(_fetchone_required(conn.execute("SELECT COUNT(*) FROM theme_membership"), "theme membership count")[0])
    seed_all_memberships = themes_count == 0 or membership_count == 0
    if themes_count > 0 and not seed_all_memberships:
        return False

    seed_theme_names = {name for name, _, _ in prepared_themes}
    membership_seed_themes = set(seed_theme_names)

    changed = False
    conn.execute("BEGIN TRANSACTION")
    try:
        for name, category, tickers in prepared_themes:
            existing = conn.execute("SELECT id, category FROM themes WHERE name = ?", [name]).fetchone()
            if existing:
                theme_id = int(existing[0])
                if str(existing[1] or "") != category:
                    conn.execute(
                        "UPDATE themes SET category = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        [category, theme_id],
                    )
                    changed = True
            else:
                theme_id = _fetchone_required(
                    conn.execute(
                        "INSERT INTO themes(name, category, is_active) VALUES (?, ?, TRUE) RETURNING id",
                        [name, category],
                    ),
                    f"insert theme id for {name}",
                )[0]
                changed = True

            if name not in membership_seed_themes:
                continue

            for ticker in tickers:
                before = conn.execute(
                    f"SELECT 1 FROM theme_membership WHERE {_theme_membership_theme_predicate()} AND ticker = ? LIMIT 1",
                    [*_theme_membership_theme_params(theme_id), ticker],
                ).fetchone()
                conn.execute(
                    "INSERT OR IGNORE INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
                    [theme_id, ticker],
                )
                if before is None:
                    changed = True

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return changed


def seed_if_needed(conn) -> bool:
    try:
        return _seed_if_needed_core(conn)
    except duckdb.InvalidInputException as exc:
        if not _is_duckdb_result_state_error(exc):
            raise
        from .database import get_bootstrap_conn

        with get_bootstrap_conn() as bootstrap_conn:
            return _seed_if_needed_core(bootstrap_conn)
    except SeedQueryResultStateError:
        from .database import get_bootstrap_conn

        with get_bootstrap_conn() as bootstrap_conn:
            return _seed_if_needed_core(bootstrap_conn)


def list_themes(conn, active_only: bool = False) -> pd.DataFrame:
    def _load(active_conn=conn) -> pd.DataFrame:
        where = "WHERE t.is_active = TRUE" if active_only else ""
        sql = f"""
            SELECT t.id, t.name, t.category, t.is_active,
                   COUNT(m.ticker) AS ticker_count,
                   t.created_at, t.updated_at
            FROM themes t
            LEFT JOIN theme_membership m ON t.id = m.theme_id{_manual_suppression_filter_sql(active_conn, 'm.ticker')}
            {where}
            GROUP BY t.id, t.name, t.category, t.is_active, t.created_at, t.updated_at
            ORDER BY t.name
            """
        return active_conn.execute(sql).df()

    return _query_df_with_bootstrap_recovery(_load)


def theme_registry_counts(conn) -> dict[str, int]:
    themes = list_themes(conn, active_only=False)
    if themes.empty:
        return {"themes_count": 0, "active_themes_count": 0}
    return {
        "themes_count": int(themes.shape[0]),
        "active_themes_count": int((themes["is_active"] == True).sum()),
    }


def theme_membership_export(conn) -> pd.DataFrame:
    def _load(active_conn=conn) -> pd.DataFrame:
        manual_filter = _manual_suppression_filter_sql(active_conn, "m.ticker")
        return active_conn.execute(
            f"""
            WITH normalized_members AS (
                SELECT DISTINCT
                    m.theme_id,
                    upper(trim(m.ticker)) AS normalized_ticker
                FROM theme_membership m
                WHERE TRUE
                {manual_filter}
            ),
            member_lists AS (
                SELECT
                    theme_id,
                    COUNT(*) AS governed_member_count,
                    string_agg(normalized_ticker, ', ' ORDER BY normalized_ticker) AS governed_members
                FROM normalized_members
                GROUP BY theme_id
            )
            SELECT
                t.id AS theme_id,
                t.name AS theme_name,
                t.category,
                t.is_active,
                COALESCE(ml.governed_member_count, 0) AS governed_member_count,
                COALESCE(ml.governed_members, '') AS governed_members
            FROM themes t
            LEFT JOIN member_lists ml ON ml.theme_id = t.id
            ORDER BY t.name, t.id
            """
        ).df()

    return _query_df_with_bootstrap_recovery(_load)


def get_theme_members(conn, theme_id: int) -> pd.DataFrame:
    def _load(active_conn=conn) -> pd.DataFrame:
        return active_conn.execute(
            f"""
            SELECT
                upper(trim(ticker)) AS ticker
            FROM theme_membership m
            WHERE {_theme_membership_theme_predicate()}
            {_manual_suppression_filter_sql(active_conn, 'm.ticker')}
            GROUP BY upper(trim(ticker))
            ORDER BY upper(trim(ticker))
            """,
            _theme_membership_theme_params(theme_id),
        ).df()

    return _query_df_with_bootstrap_recovery(_load)


def create_theme(conn, name: str, category: str, is_active: bool) -> None:
    conn.execute(
        "INSERT INTO themes(name, category, is_active) VALUES (?, ?, ?)",
        [_normalize_theme_name(name), _normalize_category(category), is_active],
    )


def update_theme(conn, theme_id: int, name: str, category: str, is_active: bool) -> None:
    conn.execute(
        """
        UPDATE themes
        SET name = ?, category = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        [_normalize_theme_name(name), _normalize_category(category), is_active, theme_id],
    )


def delete_theme(conn, theme_id: int) -> None:
    conn.execute(
        f"DELETE FROM theme_membership WHERE {_theme_membership_theme_predicate()}",
        _theme_membership_theme_params(theme_id),
    )
    conn.execute("DELETE FROM themes WHERE id = ?", [theme_id])


def add_ticker(conn, theme_id: int, ticker: str, *, onboarding_source: str = "governed_add") -> dict[str, object]:
    normalized_ticker = _normalize_ticker(ticker)
    existing_any = int(
        conn.execute(
            "SELECT COUNT(*) FROM theme_membership WHERE upper(trim(ticker)) = ?",
            [normalized_ticker],
        ).fetchone()[0]
        or 0
    )
    existed_in_theme = conn.execute(
        f"SELECT 1 FROM theme_membership WHERE {_theme_membership_theme_predicate()} AND upper(trim(ticker)) = ? LIMIT 1",
        [*_theme_membership_theme_params(theme_id), normalized_ticker],
    ).fetchone()
    conn.execute(
        "INSERT OR IGNORE INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
        [theme_id, normalized_ticker],
    )
    added_to_theme = existed_in_theme is None
    onboarding_created = False
    onboarding_state = None
    if added_to_theme and existing_any == 0 and not bool(ticker_manual_suppression_state(conn, normalized_ticker).get("manual_suppressed")):
        onboarding_state = record_new_governed_ticker_onboarding(
            conn,
            normalized_ticker,
            onboarding_source=onboarding_source,
        )
        onboarding_created = True
    return {
        "ticker": normalized_ticker,
        "theme_id": int(theme_id),
        "added_to_theme": added_to_theme,
        "newly_governed": bool(added_to_theme and existing_any == 0),
        "onboarding_created": onboarding_created,
        "onboarding_state": onboarding_state,
    }


def remove_ticker(conn, theme_id: int, ticker: str) -> dict[str, object]:
    normalized_ticker = _normalize_ticker(ticker)
    removed_row = conn.execute(
        f"DELETE FROM theme_membership WHERE {_theme_membership_theme_predicate()} AND upper(trim(ticker)) = ? RETURNING ticker",
        [*_theme_membership_theme_params(theme_id), normalized_ticker],
    ).fetchone()
    members = get_theme_members(conn, theme_id)
    remaining_tickers = members["ticker"].tolist() if not members.empty else []
    return {
        "ticker": normalized_ticker,
        "theme_id": int(theme_id),
        "removed": removed_row is not None,
        "removed_count": 1 if removed_row is not None else 0,
        "members": members,
        "remaining_tickers": remaining_tickers,
    }


def replace_ticker_in_theme(conn, theme_id: int, current_ticker: str, replacement_ticker: str) -> dict[str, str | int]:
    current = _normalize_ticker(current_ticker)
    replacement = _normalize_ticker(replacement_ticker)
    if current == replacement:
        raise ValueError("Replacement ticker must be different from the current ticker.")

    current_row = conn.execute(
        f"SELECT 1 FROM theme_membership WHERE {_theme_membership_theme_predicate()} AND upper(trim(ticker)) = ? LIMIT 1",
        [*_theme_membership_theme_params(theme_id), current],
    ).fetchone()
    if current_row is None:
        raise ValueError(f"{current} is not currently assigned to this theme.")

    replacement_row = conn.execute(
        f"SELECT 1 FROM theme_membership WHERE {_theme_membership_theme_predicate()} AND upper(trim(ticker)) = ? LIMIT 1",
        [*_theme_membership_theme_params(theme_id), replacement],
    ).fetchone()
    if replacement_row is not None:
        raise ValueError(f"{replacement} is already assigned to this theme.")

    replacement_existing_any = int(conn.execute("SELECT COUNT(*) FROM theme_membership WHERE ticker = ?", [replacement]).fetchone()[0] or 0)
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            f"DELETE FROM theme_membership WHERE {_theme_membership_theme_predicate()} AND ticker = ?",
            [*_theme_membership_theme_params(theme_id), current],
        )
        conn.execute(
            "INSERT INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
            [theme_id, replacement],
        )
        if replacement_existing_any == 0 and not bool(ticker_manual_suppression_state(conn, replacement).get("manual_suppressed")):
            record_new_governed_ticker_onboarding(conn, replacement, onboarding_source="ticker_replacement")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "theme_id": int(theme_id),
        "removed_ticker": current,
        "added_ticker": replacement,
    }


def set_ticker_theme_assignments(conn, ticker: str, theme_ids: list[int]) -> dict[str, int | str]:
    normalized_ticker = _normalize_ticker(ticker)
    normalized_theme_ids = sorted({int(theme_id) for theme_id in theme_ids if theme_id is not None})
    if not normalized_theme_ids:
        raise ValueError("Select at least one theme assignment.")

    placeholders = ", ".join(["?"] * len(normalized_theme_ids))
    existing_theme_rows = conn.execute(
        f"""
        SELECT id
        FROM themes
        WHERE id IN ({placeholders})
        """,
        normalized_theme_ids,
    ).fetchall()
    existing_theme_ids = {int(row[0]) for row in existing_theme_rows}
    missing_theme_ids = [theme_id for theme_id in normalized_theme_ids if theme_id not in existing_theme_ids]
    if missing_theme_ids:
        raise ValueError(f"Unknown theme id(s): {', '.join(str(theme_id) for theme_id in missing_theme_ids)}")

    current_theme_ids = {
        int(row[0])
        for row in conn.execute(
            "SELECT theme_id FROM theme_membership WHERE upper(trim(ticker)) = ?",
            [normalized_ticker],
        ).fetchall()
    }
    to_add = [theme_id for theme_id in normalized_theme_ids if theme_id not in current_theme_ids]
    to_remove = [theme_id for theme_id in current_theme_ids if theme_id not in normalized_theme_ids]

    was_ungoverned = not bool(current_theme_ids)
    onboarding_state = None
    conn.execute("BEGIN TRANSACTION")
    try:
        for theme_id in to_add:
            conn.execute(
                "INSERT OR IGNORE INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
                [theme_id, normalized_ticker],
            )
        for theme_id in to_remove:
            conn.execute(
                f"DELETE FROM theme_membership WHERE {_theme_membership_theme_predicate()} AND upper(trim(ticker)) = ?",
                [*_theme_membership_theme_params(theme_id), normalized_ticker],
            )
        if was_ungoverned and to_add and not bool(ticker_manual_suppression_state(conn, normalized_ticker).get("manual_suppressed")):
            onboarding_state = record_new_governed_ticker_onboarding(conn, normalized_ticker, onboarding_source="theme_assignment_update")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "ticker": normalized_ticker,
        "assigned_theme_count": len(normalized_theme_ids),
        "added_count": len(to_add),
        "removed_count": len(to_remove),
        "changed": bool(to_add or to_remove),
        "onboarding_state": onboarding_state,
        "affected_theme_ids": sorted(set(to_add + to_remove + normalized_theme_ids)),
    }


def active_ticker_universe(conn) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT DISTINCT m.ticker
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        WHERE t.is_active = TRUE
        {_manual_suppression_filter_sql(conn, 'm.ticker')}
        ORDER BY m.ticker
        """
    ).fetchall()
    return [r[0] for r in rows]


def refresh_active_ticker_universe(conn) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT DISTINCT m.ticker
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        LEFT JOIN symbol_refresh_status s ON s.ticker = m.ticker
        WHERE t.is_active = TRUE
          AND COALESCE(s.status, 'active') <> 'refresh_suppressed'
          {_manual_suppression_filter_sql(conn, 'm.ticker')}
        ORDER BY m.ticker
        """
    ).fetchall()
    return [r[0] for r in rows]
