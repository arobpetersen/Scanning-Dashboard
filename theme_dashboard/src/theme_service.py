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
    if conn.execute("SELECT COUNT(*) FROM themes").fetchone()[0] > 0:
        return False

    seed_themes = load_seed_file(SEED_PATH)
    conn.execute("BEGIN TRANSACTION")
    try:
        for theme in seed_themes:
            name = theme.get("name", "").strip()
            if not name:
                continue
            category = _normalize_category(theme.get("category", "Uncategorized"))
            tickers = sorted({_normalize_ticker(t) for t in theme.get("tickers", []) if t and t.strip()})

            theme_id = conn.execute(
                "INSERT INTO themes(name, category, is_active) VALUES (?, ?, TRUE) RETURNING id",
                [name, category],
            ).fetchone()[0]

            for ticker in tickers:
                conn.execute(
                    "INSERT INTO theme_membership(theme_id, ticker) VALUES (?, ?)",
                    [theme_id, ticker],
                )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return True


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
