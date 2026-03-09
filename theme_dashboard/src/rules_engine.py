from __future__ import annotations

from collections import defaultdict

from .config import (
    RULE_LIVE_FAILURE_MIN_COUNT,
    RULE_LIVE_FAILURE_WINDOW_DAYS,
    RULE_LOW_CONSTITUENT_THRESHOLD,
    RULE_MAX_SUGGESTIONS_PER_RULE,
)
from .suggestions_service import SuggestionPayload, create_suggestion


RULE_SEVERITY = {
    "low_constituent_count_review": "medium",
    "empty_theme_review": "high",
    "inactive_theme_cleanup_review": "medium",
    "repeated_live_failure_review": "high",
}


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


def _cap(df, limit: int):
    if limit <= 0:
        return df
    return df.head(limit)


def run_rules_engine(
    conn,
    low_constituent_threshold: int = RULE_LOW_CONSTITUENT_THRESHOLD,
    max_suggestions_per_rule: int = RULE_MAX_SUGGESTIONS_PER_RULE,
    live_failure_min_count: int = RULE_LIVE_FAILURE_MIN_COUNT,
    live_failure_window_days: int = RULE_LIVE_FAILURE_WINDOW_DAYS,
) -> dict:
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

    # A) low_constituent_count_review: non-empty but thin themes
    thin = conn.execute(
        """
        SELECT t.id, t.name, COUNT(m.ticker) AS ticker_count
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        GROUP BY t.id, t.name
        HAVING COUNT(m.ticker) > 0 AND COUNT(m.ticker) < ?
        ORDER BY ticker_count ASC, t.name
        """,
        [low_constituent_threshold],
    ).df()
    for _, r in _cap(thin, max_suggestions_per_rule).iterrows():
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                priority=RULE_SEVERITY["low_constituent_count_review"],
                rationale=f"Rule low_constituent_count_review: theme has {int(r['ticker_count'])} members (< {low_constituent_threshold}). Consider expanding, merging, or retiring.",
                existing_theme_id=int(r["id"]),
            ),
            stats,
            "low_constituent_count_review",
        )

    # B) empty_theme_review
    empty_themes = conn.execute(
        """
        SELECT t.id, t.name
        FROM themes t
        LEFT JOIN theme_membership m ON t.id = m.theme_id
        GROUP BY t.id, t.name
        HAVING COUNT(m.ticker) = 0
        ORDER BY t.name
        """
    ).df()
    for _, r in _cap(empty_themes, max_suggestions_per_rule).iterrows():
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                priority=RULE_SEVERITY["empty_theme_review"],
                rationale="Rule empty_theme_review: theme has zero members. Consider populating it or retiring it.",
                existing_theme_id=int(r["id"]),
            ),
            stats,
            "empty_theme_review",
        )

    # C) inactive_theme_cleanup_review
    inactive_with_members = conn.execute(
        """
        SELECT t.id, t.name, COUNT(m.ticker) AS ticker_count
        FROM themes t
        JOIN theme_membership m ON t.id = m.theme_id
        WHERE t.is_active = FALSE
        GROUP BY t.id, t.name
        ORDER BY ticker_count DESC, t.name
        """
    ).df()
    for _, r in _cap(inactive_with_members, max_suggestions_per_rule).iterrows():
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                priority=RULE_SEVERITY["inactive_theme_cleanup_review"],
                rationale=f"Rule inactive_theme_cleanup_review: inactive theme still has {int(r['ticker_count'])} members. Consider reactivating, migrating, or clearing membership.",
                existing_theme_id=int(r["id"]),
            ),
            stats,
            "inactive_theme_cleanup_review",
        )

    # D) repeated_live_failure_review (if live failure data exists)
    repeated_failures = conn.execute(
        """
        SELECT f.ticker, COUNT(*) AS fail_count, MAX(f.created_at) AS last_failure_at
        FROM refresh_failures f
        JOIN refresh_runs r ON r.run_id = f.run_id
        WHERE r.provider = 'live'
          AND f.ticker IS NOT NULL
          AND f.created_at >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')
        GROUP BY f.ticker
        HAVING COUNT(*) >= ?
        ORDER BY fail_count DESC, f.ticker
        """,
        [live_failure_window_days, live_failure_min_count],
    ).df()
    for _, r in _cap(repeated_failures, max_suggestions_per_rule).iterrows():
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                priority=RULE_SEVERITY["repeated_live_failure_review"],
                rationale=(
                    f"Rule repeated_live_failure_review: ticker {str(r['ticker'])} failed live refresh "
                    f"{int(r['fail_count'])} times in the last {live_failure_window_days} days."
                ),
                proposed_ticker=str(r["ticker"]),
            ),
            stats,
            "repeated_live_failure_review",
        )

    stats["by_rule_evaluated"] = dict(stats["by_rule_evaluated"])
    stats["by_rule_created"] = dict(stats["by_rule_created"])
    stats["by_rule_duplicates"] = dict(stats["by_rule_duplicates"])
    rule_names = sorted(set(RULE_SEVERITY) | set(stats["by_rule_evaluated"]) | set(stats["by_rule_created"]))
    stats["rule_results"] = [
        {
            "rule": name,
            "severity": RULE_SEVERITY.get(name, "medium"),
            "evaluated": int(stats["by_rule_evaluated"].get(name, 0)),
            "created": int(stats["by_rule_created"].get(name, 0)),
            "duplicates_skipped": int(stats["by_rule_duplicates"].get(name, 0)),
            "cap_per_run": int(max_suggestions_per_rule),
        }
        for name in rule_names
    ]
    return stats
