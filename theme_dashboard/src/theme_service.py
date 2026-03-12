from __future__ import annotations

import pandas as pd

from .config import SEED_PATH
from .seed_loader import load_seed_file


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


def seed_if_needed(conn) -> bool:
    """Idempotent seed/backfill.

    Seeds themes and membership when DB is empty, and also backfills membership if themes
    exist but `theme_membership` is empty or partially missing.
    """
    seed_themes = load_seed_file(SEED_PATH)

    prepared_themes: list[tuple[str, str, list[str]]] = []
    expected_pairs: set[tuple[str, str]] = set()
    for theme in seed_themes:
        name = theme.get("name", "").strip()
        if not name:
            continue

        category = _normalize_category(theme.get("category", "Uncategorized"))
        tickers = sorted({_normalize_ticker(t) for t in theme.get("tickers", []) if t and t.strip()})
        prepared_themes.append((name, category, tickers))

        for ticker in tickers:
            expected_pairs.add((name, ticker))

    if not prepared_themes:
        return False

    themes_count = int(conn.execute("SELECT COUNT(*) FROM themes").fetchone()[0])
    seed_theme_names = {name for name, _, _ in prepared_themes}

    existing_theme_names = {row[0] for row in conn.execute("SELECT name FROM themes").fetchall()}
    missing_theme_names = seed_theme_names - existing_theme_names

    existing_pairs: set[tuple[str, str]] = set()
    existing_rows = conn.execute(
        """
        SELECT t.name, m.ticker
        FROM themes t
        JOIN theme_membership m ON m.theme_id = t.id
        """
    ).fetchall()
    for theme_name, ticker in existing_rows:
        if theme_name in seed_theme_names:
            existing_pairs.add((theme_name, ticker))

    missing_memberships = expected_pairs - existing_pairs

    if themes_count > 0 and not missing_theme_names and not missing_memberships:
        return False

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
                theme_id = conn.execute(
                    "INSERT INTO themes(name, category, is_active) VALUES (?, ?, TRUE) RETURNING id",
                    [name, category],
                ).fetchone()[0]
                changed = True

            for ticker in tickers:
                before = conn.execute(
                    "SELECT 1 FROM theme_membership WHERE theme_id = ? AND ticker = ? LIMIT 1",
                    [theme_id, ticker],
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


def list_themes(conn, active_only: bool = False) -> pd.DataFrame:
    where = "WHERE t.is_active = TRUE" if active_only else ""
    return conn.execute(
        f"""
        SELECT t.id, t.name, t.category, t.is_active,
               COUNT(m.ticker) AS ticker_count,
               t.created_at, t.updated_at
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        {where}
        GROUP BY t.id, t.name, t.category, t.is_active, t.created_at, t.updated_at
        ORDER BY t.name
        """
    ).df()


def get_theme_members(conn, theme_id: int) -> pd.DataFrame:
    return conn.execute(
        "SELECT ticker FROM theme_membership WHERE theme_id = ? ORDER BY ticker", [theme_id]
    ).df()


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
    conn.execute("DELETE FROM theme_membership WHERE theme_id = ?", [theme_id])
    conn.execute("DELETE FROM themes WHERE id = ?", [theme_id])


def add_ticker(conn, theme_id: int, ticker: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
        [theme_id, _normalize_ticker(ticker)],
    )


def remove_ticker(conn, theme_id: int, ticker: str) -> None:
    conn.execute(
        "DELETE FROM theme_membership WHERE theme_id = ? AND ticker = ?",
        [theme_id, _normalize_ticker(ticker)],
    )


def replace_ticker_in_theme(conn, theme_id: int, current_ticker: str, replacement_ticker: str) -> dict[str, str | int]:
    current = _normalize_ticker(current_ticker)
    replacement = _normalize_ticker(replacement_ticker)
    if current == replacement:
        raise ValueError("Replacement ticker must be different from the current ticker.")

    current_row = conn.execute(
        "SELECT 1 FROM theme_membership WHERE theme_id = ? AND ticker = ? LIMIT 1",
        [theme_id, current],
    ).fetchone()
    if current_row is None:
        raise ValueError(f"{current} is not currently assigned to this theme.")

    replacement_row = conn.execute(
        "SELECT 1 FROM theme_membership WHERE theme_id = ? AND ticker = ? LIMIT 1",
        [theme_id, replacement],
    ).fetchone()
    if replacement_row is not None:
        raise ValueError(f"{replacement} is already assigned to this theme.")

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "DELETE FROM theme_membership WHERE theme_id = ? AND ticker = ?",
            [theme_id, current],
        )
        conn.execute(
            "INSERT INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
            [theme_id, replacement],
        )
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
            "SELECT theme_id FROM theme_membership WHERE ticker = ?",
            [normalized_ticker],
        ).fetchall()
    }
    to_add = [theme_id for theme_id in normalized_theme_ids if theme_id not in current_theme_ids]
    to_remove = [theme_id for theme_id in current_theme_ids if theme_id not in normalized_theme_ids]

    conn.execute("BEGIN TRANSACTION")
    try:
        for theme_id in to_add:
            conn.execute(
                "INSERT OR IGNORE INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
                [theme_id, normalized_ticker],
            )
        for theme_id in to_remove:
            conn.execute(
                "DELETE FROM theme_membership WHERE theme_id = ? AND ticker = ?",
                [theme_id, normalized_ticker],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "ticker": normalized_ticker,
        "assigned_theme_count": len(normalized_theme_ids),
        "added_count": len(to_add),
        "removed_count": len(to_remove),
        "affected_theme_ids": sorted(set(to_add + to_remove + normalized_theme_ids)),
    }


def active_ticker_universe(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT m.ticker
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        WHERE t.is_active = TRUE
        ORDER BY m.ticker
        """
    ).fetchall()
    return [r[0] for r in rows]


def refresh_active_ticker_universe(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT m.ticker
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        LEFT JOIN symbol_refresh_status s ON s.ticker = m.ticker
        WHERE t.is_active = TRUE
          AND COALESCE(s.status, 'active') <> 'refresh_suppressed'
        ORDER BY m.ticker
        """
    ).fetchall()
    return [r[0] for r in rows]
