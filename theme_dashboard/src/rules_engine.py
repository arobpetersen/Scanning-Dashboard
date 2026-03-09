from __future__ import annotations

from collections import defaultdict

from .config import RULE_LOW_CONSTITUENT_THRESHOLD
from .suggestions_service import SuggestionPayload, create_suggestion


def _try_create(conn, payload: SuggestionPayload, stats: dict, rule_name: str) -> None:
    stats["evaluated"] += 1
    stats["by_rule_evaluated"][rule_name] += 1
    try:
        create_suggestion(conn, payload)
        stats["created"] += 1
        stats["by_rule_created"][rule_name] += 1
    except Exception as exc:
        msg = str(exc).lower()
        if "equivalent pending suggestion" in msg:
            stats["duplicates_skipped"] += 1
            stats["by_rule_duplicates"][rule_name] += 1
        else:
            stats["invalid_or_skipped"] += 1
            stats["errors"].append(f"{rule_name}: {exc}")


def run_rules_engine(conn, low_constituent_threshold: int = RULE_LOW_CONSTITUENT_THRESHOLD) -> dict:
    stats: dict = {
        "evaluated": 0,
        "created": 0,
        "duplicates_skipped": 0,
        "invalid_or_skipped": 0,
        "errors": [],
        "by_rule_evaluated": defaultdict(int),
        "by_rule_created": defaultdict(int),
        "by_rule_duplicates": defaultdict(int),
    }

    # A) low_constituent_count_review
    thin = conn.execute(
        """
        SELECT t.id, t.name, COUNT(m.ticker) AS ticker_count
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        GROUP BY t.id, t.name
        HAVING COUNT(m.ticker) < ?
        ORDER BY ticker_count ASC, t.name
        """,
        [low_constituent_threshold],
    ).df()
    for _, r in thin.iterrows():
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                rationale=f"Rule low_constituent_count_review: theme has {int(r['ticker_count'])} members (< {low_constituent_threshold}).",
                existing_theme_id=int(r["id"]),
            ),
            stats,
            "low_constituent_count_review",
        )

    # B) duplicate_membership_review
    overlaps = conn.execute(
        """
        SELECT m.ticker,
               COUNT(DISTINCT m.theme_id) AS theme_count,
               string_agg(t.name, ', ' ORDER BY t.name) AS theme_names
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        GROUP BY m.ticker
        HAVING COUNT(DISTINCT m.theme_id) > 1
        ORDER BY m.ticker
        """
    ).df()
    for _, r in overlaps.iterrows():
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                rationale=f"Rule duplicate_membership_review: ticker appears in {int(r['theme_count'])} themes ({r['theme_names']}).",
                proposed_ticker=str(r["ticker"]),
            ),
            stats,
            "duplicate_membership_review",
        )

    # C) inactive_or_empty_theme_review
    inactive_or_empty = conn.execute(
        """
        SELECT t.id, t.name, t.is_active, COUNT(m.ticker) AS ticker_count
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        GROUP BY t.id, t.name, t.is_active
        HAVING t.is_active = FALSE OR COUNT(m.ticker) = 0
        ORDER BY t.name
        """
    ).df()
    for _, r in inactive_or_empty.iterrows():
        reasons = []
        if not bool(r["is_active"]):
            reasons.append("theme is inactive")
        if int(r["ticker_count"]) == 0:
            reasons.append("theme has zero members")
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                rationale=f"Rule inactive_or_empty_theme_review: {' and '.join(reasons)}.",
                existing_theme_id=int(r["id"]),
            ),
            stats,
            "inactive_or_empty_theme_review",
        )

    stats["by_rule_evaluated"] = dict(stats["by_rule_evaluated"])
    stats["by_rule_created"] = dict(stats["by_rule_created"])
    stats["by_rule_duplicates"] = dict(stats["by_rule_duplicates"])
    rule_names = sorted(
        set(stats["by_rule_evaluated"]) | set(stats["by_rule_created"]) | set(stats["by_rule_duplicates"])
    )
    stats["rule_results"] = [
        {
            "rule": name,
            "evaluated": int(stats["by_rule_evaluated"].get(name, 0)),
            "created": int(stats["by_rule_created"].get(name, 0)),
            "duplicates_skipped": int(stats["by_rule_duplicates"].get(name, 0)),
        }
        for name in rule_names
    ]
    return stats
