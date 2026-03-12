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
    "review_theme",
}
VALID_SOURCES = {"manual", "rules_engine", "ai_proposal", "imported"}
VALID_PRIORITIES = {"low", "medium", "high"}
VALID_STATUSES = {"pending", "approved", "rejected", "applied", "obsolete"}


@dataclass
class SuggestionPayload:
    suggestion_type: str
    source: str
    rationale: str = ""
    proposed_theme_name: str | None = None
    proposed_ticker: str | None = None
    existing_theme_id: int | None = None
    proposed_target_theme_id: int | None = None
    priority: str = "medium"


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


def _norm_priority(priority: str | None) -> str:
    value = (priority or "medium").strip().lower()
    if value not in VALID_PRIORITIES:
        raise ValueError(f"Invalid priority: {priority}")
    return value


def _theme_name_exists(conn, name: str) -> bool:
    return conn.execute("SELECT COUNT(*) FROM themes WHERE lower(name)=lower(?)", [name]).fetchone()[0] > 0


def _ticker_in_theme(conn, theme_id: int, ticker: str) -> bool:
    return (
        conn.execute("SELECT COUNT(*) FROM theme_membership WHERE theme_id=? AND ticker=?", [theme_id, ticker]).fetchone()[0]
        > 0
    )


def validate_payload(conn, payload: SuggestionPayload) -> tuple[bool, str]:
    t = _norm_type(payload.suggestion_type)
    theme_name = (payload.proposed_theme_name or "").strip()
    ticker = (payload.proposed_ticker or "").strip().upper()
    theme_id = payload.existing_theme_id
    target_id = payload.proposed_target_theme_id

    if t == "add_ticker_to_theme":
        if not ticker:
            return False, "Ticker cannot be blank."
        if theme_id is None:
            return False, "Existing theme is required."
        if _ticker_in_theme(conn, int(theme_id), ticker):
            return False, "Ticker already exists in the selected theme."
    elif t == "remove_ticker_from_theme":
        if not ticker:
            return False, "Ticker cannot be blank."
        if theme_id is None:
            return False, "Existing theme is required."
        if not _ticker_in_theme(conn, int(theme_id), ticker):
            return False, "Ticker is not currently in the selected theme."
    elif t == "create_theme":
        if not theme_name:
            return False, "Theme name cannot be blank."
        if _theme_name_exists(conn, theme_name):
            return False, "Theme name already exists."
    elif t == "rename_theme":
        if theme_id is None:
            return False, "Existing theme is required."
        if not theme_name:
            return False, "New theme name cannot be blank."
        current = conn.execute("SELECT name FROM themes WHERE id=?", [theme_id]).fetchone()
        if current is None:
            return False, "Existing theme not found."
        if current[0].strip().lower() == theme_name.lower():
            return False, "New theme name matches current theme name."
        if _theme_name_exists(conn, theme_name):
            return False, "Theme name already exists."
    elif t == "move_ticker_between_themes":
        if not ticker:
            return False, "Ticker cannot be blank."
        if theme_id is None or target_id is None:
            return False, "Source and target themes are required."
        if int(theme_id) == int(target_id):
            return False, "Source and target themes must be different."
        if not _ticker_in_theme(conn, int(theme_id), ticker):
            return False, "Ticker is not in the source theme."
        if _ticker_in_theme(conn, int(target_id), ticker):
            return False, "Ticker already exists in the target theme."
    elif t == "review_theme":
        if theme_id is None and not ticker and not theme_name:
            return False, "Review suggestion needs theme and/or ticker context."

    return True, "valid"


def _is_duplicate_pending(conn, payload: SuggestionPayload) -> bool:
    t = _norm_type(payload.suggestion_type)
    theme_name = (payload.proposed_theme_name or "").strip()
    ticker = (payload.proposed_ticker or "").strip().upper()

    return (
        conn.execute(
            """
            SELECT COUNT(*)
            FROM theme_suggestions
            WHERE status='pending'
              AND suggestion_type=?
              AND COALESCE(existing_theme_id,-1)=COALESCE(?, -1)
              AND COALESCE(proposed_target_theme_id,-1)=COALESCE(?, -1)
              AND COALESCE(upper(proposed_ticker),'')=COALESCE(upper(?),'')
              AND COALESCE(lower(proposed_theme_name),'')=COALESCE(lower(?),'')
            """,
            [t, payload.existing_theme_id, payload.proposed_target_theme_id, ticker or None, theme_name or None],
        ).fetchone()[0]
        > 0
    )


def create_suggestion(conn, payload: SuggestionPayload) -> int:
    suggestion_type = _norm_type(payload.suggestion_type)
    source = _norm_source(payload.source)
    priority = _norm_priority(payload.priority)

    ok, message = validate_payload(conn, payload)
    if not ok:
        raise ValueError(message)

    if _is_duplicate_pending(conn, payload):
        raise ValueError("Equivalent pending suggestion already exists.")

    suggestion_id = conn.execute(
        """
        INSERT INTO theme_suggestions(
            suggestion_type, status, source, rationale, priority,
            proposed_theme_name, proposed_ticker, existing_theme_id, proposed_target_theme_id
        )
        VALUES (?, 'pending', ?, ?, ?, ?, ?, ?, ?)
        RETURNING suggestion_id
        """,
        [
            suggestion_type,
            source,
            (payload.rationale or "").strip(),
            priority,
            payload.proposed_theme_name.strip() if payload.proposed_theme_name else None,
            payload.proposed_ticker.strip().upper() if payload.proposed_ticker else None,
            payload.existing_theme_id,
            payload.proposed_target_theme_id,
        ],
    ).fetchone()[0]
    return int(suggestion_id)


def _compute_validation_status(conn, row: pd.Series) -> str:
    payload = SuggestionPayload(
        suggestion_type=str(row["suggestion_type"]),
        source=str(row["source"]),
        rationale=str(row.get("rationale") or ""),
        proposed_theme_name=row.get("proposed_theme_name"),
        proposed_ticker=row.get("proposed_ticker"),
        existing_theme_id=int(row["existing_theme_id"]) if pd.notna(row.get("existing_theme_id")) else None,
        proposed_target_theme_id=int(row["proposed_target_theme_id"]) if pd.notna(row.get("proposed_target_theme_id")) else None,
        priority=str(row.get("priority") or "medium"),
    )

    if str(row["status"]) == "pending" and _is_duplicate_pending(conn, payload):
        dup_count = conn.execute(
            """
            SELECT COUNT(*) FROM theme_suggestions
            WHERE status='pending' AND suggestion_type=?
              AND COALESCE(existing_theme_id,-1)=COALESCE(?, -1)
              AND COALESCE(proposed_target_theme_id,-1)=COALESCE(?, -1)
              AND COALESCE(upper(proposed_ticker),'')=COALESCE(upper(?),'')
              AND COALESCE(lower(proposed_theme_name),'')=COALESCE(lower(?),'')
            """,
            [
                payload.suggestion_type,
                payload.existing_theme_id,
                payload.proposed_target_theme_id,
                payload.proposed_ticker,
                payload.proposed_theme_name,
            ],
        ).fetchone()[0]
        if dup_count > 1:
            return "duplicate_pending"

    ok, _ = validate_payload(conn, payload)
    if str(row["status"]) in {"approved", "pending"} and not ok:
        return "stale"
    return "valid"


def _build_filter_clauses(
    status: str | None,
    suggestion_type: str | None,
    source: str | None,
    search_text: str | None,
) -> tuple[list[str], list[object]]:
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
    if search_text:
        clauses.append(
            "(upper(COALESCE(proposed_ticker,'')) LIKE upper(?) OR lower(COALESCE(proposed_theme_name,'')) LIKE lower(?) OR lower(COALESCE(t.name,'')) LIKE lower(?) OR lower(COALESCE(tt.name,'')) LIKE lower(?))"
        )
        needle = f"%{search_text.strip()}%"
        params.extend([needle, needle, needle, needle])

    return clauses, params


def list_suggestions(
    conn,
    status: str | None = None,
    suggestion_type: str | None = None,
    source: str | None = None,
    search_text: str | None = None,
) -> pd.DataFrame:
    clauses, params = _build_filter_clauses(status, suggestion_type, source, search_text)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    df = conn.execute(
        f"""
        WITH ticker_membership_context AS (
            SELECT
                m.ticker,
                STRING_AGG(t.name, ', ' ORDER BY t.name) AS current_theme_names,
                STRING_AGG(DISTINCT t.category, ', ' ORDER BY t.category) AS current_categories,
                STRING_AGG(
                    t.name || ' (' || COALESCE(NULLIF(t.category, ''), 'Uncategorized') || ')',
                    ', '
                    ORDER BY t.name
                ) AS current_membership_context
            FROM theme_membership m
            JOIN themes t ON t.id = m.theme_id
            GROUP BY m.ticker
        )
        SELECT
            s.*,
            t.name AS existing_theme_name,
            tt.name AS target_theme_name,
            tmc.current_theme_names,
            tmc.current_categories,
            tmc.current_membership_context
        FROM theme_suggestions s
        LEFT JOIN themes t ON t.id = s.existing_theme_id
        LEFT JOIN themes tt ON tt.id = s.proposed_target_theme_id
        LEFT JOIN ticker_membership_context tmc ON tmc.ticker = s.proposed_ticker
        {where}
        ORDER BY s.suggestion_id DESC
        """,
        params,
    ).df()

    if df.empty:
        return df

    df = df.copy()
    df["validation_status"] = df.apply(lambda r: _compute_validation_status(conn, r), axis=1)
    return df


def count_filtered_suggestions(
    conn,
    status: str | None,
    suggestion_type: str | None,
    source: str | None,
    search_text: str | None,
    statuses_subset: list[str] | None = None,
) -> int:
    clauses, params = _build_filter_clauses(status, suggestion_type, source, search_text)
    if statuses_subset:
        placeholders = ", ".join(["?"] * len(statuses_subset))
        clauses.append(f"status IN ({placeholders})")
        params.extend(statuses_subset)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM theme_suggestions s
            LEFT JOIN themes t ON t.id = s.existing_theme_id
            LEFT JOIN themes tt ON tt.id = s.proposed_target_theme_id
            {where}
            """,
            params,
        ).fetchone()[0]
    )


def bulk_update_filtered_status(
    conn,
    new_status: str,
    reviewer_notes: str,
    status: str | None,
    suggestion_type: str | None,
    source: str | None,
    search_text: str | None,
    allowed_current_statuses: list[str] | None = None,
) -> int:
    if new_status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {new_status}")

    clauses, params = _build_filter_clauses(status, suggestion_type, source, search_text)
    if allowed_current_statuses:
        placeholders = ", ".join(["?"] * len(allowed_current_statuses))
        clauses.append(f"status IN ({placeholders})")
        params.extend(allowed_current_statuses)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    ids = conn.execute(
        f"""
        SELECT s.suggestion_id
        FROM theme_suggestions s
        LEFT JOIN themes t ON t.id = s.existing_theme_id
        LEFT JOIN themes tt ON tt.id = s.proposed_target_theme_id
        {where}
        """,
        params,
    ).df()
    if ids.empty:
        return 0

    id_list = [int(x) for x in ids["suggestion_id"].tolist()]
    placeholders = ", ".join(["?"] * len(id_list))
    note = reviewer_notes.strip()
    conn.execute(
        f"""
        UPDATE theme_suggestions
        SET status = ?,
            reviewed_at = CURRENT_TIMESTAMP,
            reviewer_notes = CASE WHEN ? = '' THEN reviewer_notes ELSE ? END
        WHERE suggestion_id IN ({placeholders})
        """,
        [new_status, note, note, *id_list],
    )
    return len(id_list)


def review_suggestion(conn, suggestion_id: int, new_status: str, reviewer_notes: str) -> dict[str, object]:
    if new_status not in {"approved", "rejected"}:
        raise ValueError("Review status must be approved or rejected")

    existing = conn.execute(
        """
        SELECT status
        FROM theme_suggestions
        WHERE suggestion_id = ?
        """,
        [suggestion_id],
    ).fetchone()
    if existing is None:
        raise ValueError("Suggestion not found")

    current_status = str(existing[0] or "")
    if current_status == new_status:
        return {
            "suggestion_id": int(suggestion_id),
            "changed": False,
            "old_status": current_status,
            "new_status": new_status,
            "message": f"Suggestion #{int(suggestion_id)} is already {new_status}.",
        }
    if current_status not in {"pending", "approved", "rejected"}:
        raise ValueError(f"Suggestion #{int(suggestion_id)} cannot be reviewed from status '{current_status}'.")

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
    return {
        "suggestion_id": int(suggestion_id),
        "changed": True,
        "old_status": current_status,
        "new_status": new_status,
        "message": f"Suggestion #{int(suggestion_id)} moved from {current_status} to {new_status}.",
    }


def apply_suggestion(conn, suggestion_id: int, reviewer_notes: str = "") -> None:
    row = conn.execute(
        """
        SELECT suggestion_id, suggestion_type, status, source, proposed_theme_name, proposed_ticker,
               existing_theme_id, proposed_target_theme_id, priority
        FROM theme_suggestions
        WHERE suggestion_id = ?
        """,
        [suggestion_id],
    ).fetchone()
    if row is None:
        raise ValueError("Suggestion not found")

    _, suggestion_type, status, source, proposed_theme_name, proposed_ticker, existing_theme_id, target_theme_id, priority = row
    if status != "approved":
        raise ValueError("Only approved suggestions can be applied")

    payload = SuggestionPayload(
        suggestion_type=suggestion_type,
        source=source,
        proposed_theme_name=proposed_theme_name,
        proposed_ticker=proposed_ticker,
        existing_theme_id=existing_theme_id,
        proposed_target_theme_id=target_theme_id,
        priority=priority,
    )
    ok, reason = validate_payload(conn, payload)
    if not ok:
        raise ValueError(f"Suggestion is no longer applicable: {reason}")

    if suggestion_type == "add_ticker_to_theme":
        add_ticker(conn, int(existing_theme_id), proposed_ticker)
    elif suggestion_type == "remove_ticker_from_theme":
        remove_ticker(conn, int(existing_theme_id), proposed_ticker)
    elif suggestion_type == "create_theme":
        create_theme(conn, proposed_theme_name, "Custom", True)
    elif suggestion_type == "rename_theme":
        theme = conn.execute("SELECT name, category, is_active FROM themes WHERE id = ?", [existing_theme_id]).fetchone()
        if theme is None:
            raise ValueError("Theme not found")
        _, category, is_active = theme
        update_theme(conn, int(existing_theme_id), proposed_theme_name, category, bool(is_active))
    elif suggestion_type == "move_ticker_between_themes":
        remove_ticker(conn, int(existing_theme_id), proposed_ticker)
        add_ticker(conn, int(target_theme_id), proposed_ticker)
    elif suggestion_type == "review_theme":
        pass
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


def recent_applied_suggestions(conn, limit: int = 10) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT s.suggestion_id, s.suggestion_type, s.source, s.priority, s.proposed_ticker, s.proposed_theme_name,
               t.name AS existing_theme_name, tt.name AS target_theme_name,
               s.reviewer_notes, s.reviewed_at
        FROM theme_suggestions s
        LEFT JOIN themes t ON t.id = s.existing_theme_id
        LEFT JOIN themes tt ON tt.id = s.proposed_target_theme_id
        WHERE s.status = 'applied'
        ORDER BY s.reviewed_at DESC, s.suggestion_id DESC
        LIMIT ?
        """,
        [limit],
    ).df()
