from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .theme_service import add_ticker, create_theme, remove_ticker, update_theme


VALID_TYPES = {
    "add_ticker_to_theme",
    "remove_ticker_from_theme",
    "create_theme",
    "rename_theme",
    "move_ticker_between_themes",
}
VALID_STATUSES = {"pending", "approved", "rejected", "applied"}
VALID_SOURCES = {"manual", "rules_engine", "ai_proposal", "imported"}


@dataclass
class SuggestionPayload:
    suggestion_type: str
    source: str
    rationale: str = ""
    proposed_theme_name: str | None = None
    proposed_ticker: str | None = None
    existing_theme_id: int | None = None
    proposed_target_theme_id: int | None = None


def _norm_source(source: str) -> str:
    value = source.strip().lower()
    if value not in VALID_SOURCES:
        raise ValueError(f"Invalid source: {source}")
    return value


def _norm_type(suggestion_type: str) -> str:
    value = suggestion_type.strip()
    if value not in VALID_TYPES:
        raise ValueError(f"Invalid suggestion type: {suggestion_type}")
    return value


def create_suggestion(conn, payload: SuggestionPayload) -> int:
    suggestion_type = _norm_type(payload.suggestion_type)
    source = _norm_source(payload.source)

    suggestion_id = conn.execute(
        """
        INSERT INTO theme_suggestions(
            suggestion_type, status, source, rationale,
            proposed_theme_name, proposed_ticker, existing_theme_id, proposed_target_theme_id
        )
        VALUES (?, 'pending', ?, ?, ?, ?, ?, ?)
        RETURNING suggestion_id
        """,
        [
            suggestion_type,
            source,
            (payload.rationale or "").strip(),
            payload.proposed_theme_name.strip() if payload.proposed_theme_name else None,
            payload.proposed_ticker.strip().upper() if payload.proposed_ticker else None,
            payload.existing_theme_id,
            payload.proposed_target_theme_id,
        ],
    ).fetchone()[0]
    return int(suggestion_id)


def list_suggestions(
    conn,
    status: str | None = None,
    suggestion_type: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    clauses = []
    params: list[object] = []

    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)
    if suggestion_type and suggestion_type != "all":
        clauses.append("suggestion_type = ?")
        params.append(suggestion_type)
    if source and source != "all":
        clauses.append("source = ?")
        params.append(source)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    return conn.execute(
        f"""
        SELECT s.*, t.name AS existing_theme_name, tt.name AS target_theme_name
        FROM theme_suggestions s
        LEFT JOIN themes t ON t.id = s.existing_theme_id
        LEFT JOIN themes tt ON tt.id = s.proposed_target_theme_id
        {where}
        ORDER BY s.suggestion_id DESC
        """,
        params,
    ).df()


def review_suggestion(conn, suggestion_id: int, new_status: str, reviewer_notes: str) -> None:
    if new_status not in {"approved", "rejected"}:
        raise ValueError("Review status must be approved or rejected")

    conn.execute(
        """
        UPDATE theme_suggestions
        SET status = ?,
            reviewed_at = CURRENT_TIMESTAMP,
            reviewer_notes = ?
        WHERE suggestion_id = ?
        """,
        [new_status, reviewer_notes.strip(), suggestion_id],
    )


def apply_suggestion(conn, suggestion_id: int, reviewer_notes: str = "") -> None:
    row = conn.execute(
        """
        SELECT suggestion_id, suggestion_type, status, proposed_theme_name, proposed_ticker,
               existing_theme_id, proposed_target_theme_id
        FROM theme_suggestions
        WHERE suggestion_id = ?
        """,
        [suggestion_id],
    ).fetchone()
    if row is None:
        raise ValueError("Suggestion not found")

    _, suggestion_type, status, proposed_theme_name, proposed_ticker, existing_theme_id, target_theme_id = row
    if status != "approved":
        raise ValueError("Only approved suggestions can be applied")

    if suggestion_type == "add_ticker_to_theme":
        if existing_theme_id is None or not proposed_ticker:
            raise ValueError("Missing theme or ticker")
        add_ticker(conn, int(existing_theme_id), proposed_ticker)
    elif suggestion_type == "remove_ticker_from_theme":
        if existing_theme_id is None or not proposed_ticker:
            raise ValueError("Missing theme or ticker")
        remove_ticker(conn, int(existing_theme_id), proposed_ticker)
    elif suggestion_type == "create_theme":
        if not proposed_theme_name:
            raise ValueError("Missing proposed theme name")
        create_theme(conn, proposed_theme_name, "Custom", True)
    elif suggestion_type == "rename_theme":
        if existing_theme_id is None or not proposed_theme_name:
            raise ValueError("Missing theme id or new name")
        theme = conn.execute("SELECT name, category, is_active FROM themes WHERE id = ?", [existing_theme_id]).fetchone()
        if theme is None:
            raise ValueError("Theme not found")
        _, category, is_active = theme
        update_theme(conn, int(existing_theme_id), proposed_theme_name, category, bool(is_active))
    elif suggestion_type == "move_ticker_between_themes":
        if existing_theme_id is None or target_theme_id is None or not proposed_ticker:
            raise ValueError("Missing source theme, target theme, or ticker")
        remove_ticker(conn, int(existing_theme_id), proposed_ticker)
        add_ticker(conn, int(target_theme_id), proposed_ticker)
    else:
        raise ValueError(f"Unsupported suggestion type: {suggestion_type}")

    conn.execute(
        """
        UPDATE theme_suggestions
        SET status = 'applied',
            reviewed_at = COALESCE(reviewed_at, CURRENT_TIMESTAMP),
            reviewer_notes = CASE WHEN ? = '' THEN reviewer_notes ELSE ? END
        WHERE suggestion_id = ?
        """,
        [reviewer_notes.strip(), reviewer_notes.strip(), suggestion_id],
    )


def suggestion_status_counts(conn) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT status, COUNT(*) AS cnt
        FROM theme_suggestions
        GROUP BY status
        ORDER BY status
        """
    ).df()
