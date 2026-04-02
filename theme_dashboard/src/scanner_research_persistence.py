from __future__ import annotations

"""Persistence helpers for scanner research reviews.

The orchestrator owns draft generation. Persistence helpers own review storage
for the current-state workflow tables. They intentionally leave room for a
future append-only audit/event layer without requiring it today.
"""

import json
import re
from collections.abc import Mapping

from .scanner_research_models import ResearchDraft


RESEARCH_REVIEW_OUTCOMES = {
    "direct_fit_correct",
    "adjacent_fit_acceptable",
    "should_have_been_tentative",
    "false_positive",
    "missed_obvious_theme",
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_research_strategy(value: object, fallback: str = "description_theme_generation") -> str:
    normalized = _normalize_text(value) or fallback
    return normalized if normalized == "description_theme_generation" else fallback


def _normalize_research_review_outcome(value: object) -> str:
    normalized = _normalize_text(value)
    return normalized if normalized in RESEARCH_REVIEW_OUTCOMES else ""


def _sanitize_error_text(text: object, *, limit: int = 200) -> str:
    value = _normalize_text(text)
    if not value:
        return ""
    value = value.replace("\n", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value[:limit]


def _as_draft(draft: ResearchDraft | Mapping[str, object] | None) -> ResearchDraft:
    return draft if isinstance(draft, ResearchDraft) else ResearchDraft.from_mapping(draft)


def ensure_scanner_research_review_table(conn) -> None:
    conn.execute("CREATE SEQUENCE IF NOT EXISTS scanner_research_review_id_seq")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scanner_research_reviews (
            review_id BIGINT PRIMARY KEY DEFAULT nextval('scanner_research_review_id_seq'),
            ticker VARCHAR NOT NULL,
            generated_at TIMESTAMP NOT NULL,
            theme_generation_strategy VARCHAR NOT NULL,
            research_mode VARCHAR,
            outcome_class VARCHAR NOT NULL,
            reviewer_notes VARCHAR,
            recommended_action VARCHAR,
            confidence VARCHAR,
            possible_new_theme VARCHAR,
            draft_context_json VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (ticker, generated_at, theme_generation_strategy),
            CHECK (length(trim(ticker)) > 0),
            CHECK (outcome_class IN (
                'direct_fit_correct',
                'adjacent_fit_acceptable',
                'should_have_been_tentative',
                'false_positive',
                'missed_obvious_theme'
            ))
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scanner_research_reviews_outcome ON scanner_research_reviews(outcome_class)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scanner_research_reviews_generated_at ON scanner_research_reviews(generated_at)")


def scanner_research_review_context(draft: ResearchDraft | Mapping[str, object] | None) -> dict[str, object]:
    draft_model = _as_draft(draft)
    suggested_existing = []
    for item in list(draft_model.suggested_existing_themes or [])[:5]:
        suggested_existing.append(
            {
                "theme_id": item.get("theme_id"),
                "theme_name": item.get("theme_name"),
                "fit_label": item.get("fit_label"),
            }
        )
    return {
        "recommended_action": draft_model.recommended_action or "watch_only",
        "confidence": draft_model.confidence or "low",
        "possible_new_theme": draft_model.possible_new_theme or "",
        "possible_new_theme_category": draft_model.possible_new_theme_category or "",
        "domain_anchor": draft_model.domain_anchor or "unclear",
        "dominant_business_role": draft_model.dominant_business_role or "unclear",
        "generated_theme_ideas": list(draft_model.candidate_theme_ideas or [])[:5],
        "suggested_existing_themes": suggested_existing,
    }


def get_scanner_research_review(conn, ticker: str, draft: ResearchDraft | Mapping[str, object] | None) -> dict[str, object] | None:
    draft_model = _as_draft(draft)
    generated_at = draft_model.generated_at
    strategy = _normalize_research_strategy(draft_model.theme_generation_strategy)
    normalized_ticker = _normalize_text(ticker).upper()
    if not normalized_ticker or not generated_at:
        return None
    ensure_scanner_research_review_table(conn)
    row = conn.execute(
        """
        SELECT review_id, ticker, generated_at, theme_generation_strategy, research_mode,
               outcome_class, reviewer_notes, recommended_action, confidence,
               possible_new_theme, draft_context_json, created_at, updated_at
        FROM scanner_research_reviews
        WHERE ticker = ? AND generated_at = ? AND theme_generation_strategy = ?
        LIMIT 1
        """,
        [normalized_ticker, generated_at, strategy],
    ).fetchone()
    if not row:
        return None
    return {
        "review_id": int(row[0]),
        "ticker": str(row[1]),
        "generated_at": str(row[2]),
        "theme_generation_strategy": str(row[3]),
        "research_mode": _normalize_text(row[4]),
        "outcome_class": str(row[5]),
        "reviewer_notes": _normalize_text(row[6]),
        "recommended_action": _normalize_text(row[7]),
        "confidence": _normalize_text(row[8]),
        "possible_new_theme": _normalize_text(row[9]),
        "draft_context_json": _normalize_text(row[10]),
        "created_at": str(row[11]),
        "updated_at": str(row[12]),
    }


def save_scanner_research_review(
    conn,
    ticker: str,
    draft: ResearchDraft | Mapping[str, object] | None,
    *,
    outcome_class: object,
    reviewer_notes: object = "",
) -> dict[str, object]:
    draft_model = _as_draft(draft)
    normalized_ticker = _normalize_text(ticker).upper()
    generated_at = draft_model.generated_at
    strategy = _normalize_research_strategy(draft_model.theme_generation_strategy)
    outcome = _normalize_research_review_outcome(outcome_class)
    if not normalized_ticker or not generated_at:
        raise ValueError("Research review requires ticker and generated_at draft context.")
    if not outcome:
        raise ValueError("Research review outcome must be one of the supported outcome classes.")
    ensure_scanner_research_review_table(conn)
    note = _sanitize_error_text(reviewer_notes, limit=500)
    context_json = json.dumps(scanner_research_review_context(draft_model), sort_keys=True)
    existing = get_scanner_research_review(conn, normalized_ticker, draft_model)
    if existing:
        conn.execute(
            """
            UPDATE scanner_research_reviews
            SET research_mode = ?,
                outcome_class = ?,
                reviewer_notes = ?,
                recommended_action = ?,
                confidence = ?,
                possible_new_theme = ?,
                draft_context_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE review_id = ?
            """,
            [
                draft_model.research_mode,
                outcome,
                note,
                draft_model.recommended_action or "watch_only",
                draft_model.confidence or "low",
                draft_model.possible_new_theme or "",
                context_json,
                int(existing["review_id"]),
            ],
        )
    else:
        conn.execute(
            """
            INSERT INTO scanner_research_reviews(
                ticker, generated_at, theme_generation_strategy, research_mode,
                outcome_class, reviewer_notes, recommended_action, confidence,
                possible_new_theme, draft_context_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                normalized_ticker,
                generated_at,
                strategy,
                draft_model.research_mode,
                outcome,
                note,
                draft_model.recommended_action or "watch_only",
                draft_model.confidence or "low",
                draft_model.possible_new_theme or "",
                context_json,
            ],
        )
    saved = get_scanner_research_review(conn, normalized_ticker, draft_model)
    return saved or {}


def scanner_research_review_summary(conn, *, limit: int = 8) -> dict[str, object]:
    ensure_scanner_research_review_table(conn)
    outcome_rows = conn.execute(
        """
        SELECT outcome_class, COUNT(*) AS review_count
        FROM scanner_research_reviews
        GROUP BY outcome_class
        ORDER BY review_count DESC, outcome_class
        """
    ).fetchall()
    recent_rows = conn.execute(
        """
        SELECT ticker, outcome_class, reviewer_notes, theme_generation_strategy, generated_at, updated_at
        FROM scanner_research_reviews
        ORDER BY updated_at DESC, review_id DESC
        LIMIT ?
        """,
        [int(limit)],
    ).fetchall()
    return {
        "counts_by_outcome": {str(row[0]): int(row[1]) for row in outcome_rows},
        "recent_reviews": [
            {
                "ticker": str(row[0]),
                "outcome_class": str(row[1]),
                "reviewer_notes": _normalize_text(row[2]),
                "theme_generation_strategy": str(row[3]),
                "generated_at": str(row[4]),
                "updated_at": str(row[5]),
            }
            for row in recent_rows
        ],
    }

