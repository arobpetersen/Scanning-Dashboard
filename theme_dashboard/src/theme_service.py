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
    """Idempotent seed/backfill for themes + membership.

    - If DB is empty: seed themes and membership.
    - If themes exist but membership is empty/partial: backfill missing memberships.
    """
    seed_themes = load_seed_file(SEED_PATH)

    themes_count = int(conn.execute("SELECT COUNT(*) FROM themes").fetchone()[0])
    membership_count = int(conn.execute("SELECT COUNT(*) FROM theme_membership").fetchone()[0])

    if themes_count > 0 and membership_count > 0:
        # Fast path only when all seeded memberships already exist.
        missing_membership = False
        for theme in seed_themes:
            name = theme.get("name", "").strip()
            if not name:
                continue
            existing = conn.execute("SELECT id FROM themes WHERE name = ?", [name]).fetchone()
            if not existing:
                missing_membership = True
                break
            theme_id = int(existing[0])
            tickers = sorted({_normalize_ticker(t) for t in theme.get("tickers", []) if t and t.strip()})
            for ticker in tickers:
                row = conn.execute(
                    "SELECT 1 FROM theme_membership WHERE theme_id = ? AND ticker = ? LIMIT 1",
                    [theme_id, ticker],
                ).fetchone()
                if row is None:
                    missing_membership = True
                    break
            if missing_membership:
                break

        if not missing_membership:
            return False

    changed = False
    conn.execute("BEGIN TRANSACTION")
    try:
        for theme in seed_themes:
            name = theme.get("name", "").strip()
            if not name:
                continue
            category = _normalize_category(theme.get("category", "Uncategorized"))
            tickers = sorted({_normalize_ticker(t) for t in theme.get("tickers", []) if t and t.strip()})

            existing = conn.execute("SELECT id FROM themes WHERE name = ?", [name]).fetchone()
            if existing:
                theme_id = int(existing[0])
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
