from __future__ import annotations

from collections import defaultdict

import pandas as pd

from .config import (
    RULE_LIVE_FAILURE_MIN_COUNT,
    RULE_LIVE_FAILURE_WINDOW_DAYS,
    RULE_LOW_CONSTITUENT_THRESHOLD,
    RULE_MAX_SUGGESTIONS_PER_RULE,
)
from .failure_classification import categorize_failure_message
from .suggestions_service import SuggestionPayload, create_suggestion


RULE_SEVERITY = {
    "low_constituent_count_review": "medium",
    "empty_theme_review": "high",
    "inactive_theme_cleanup_review": "medium",
    "repeated_live_failure_review": "high",
}
PROVIDER_LEVEL_FAILURE_CATEGORIES = {"provider_limit", "provider_auth", "provider_outage"}
TICKER_ACTIONABLE_FAILURE_CATEGORIES = {"ticker_data_missing", "ticker_symbol_issue", "no_candles"}


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


def _ticker_repeated_live_failure_candidates(conn, window_days: int, min_count: int) -> tuple[pd.DataFrame, dict]:
    failures = conn.execute(
        """
        SELECT f.ticker, f.error_message
        FROM refresh_failures f
        JOIN refresh_runs r ON r.run_id = f.run_id
        WHERE r.provider = 'live'
          AND f.ticker IS NOT NULL
          AND f.created_at >= CURRENT_TIMESTAMP - (? * INTERVAL '1 day')
        """,
        [window_days],
    ).df()

    if failures.empty:
        return failures, {"provider_level_failures": 0, "ticker_actionable_failures": 0, "suppressed_tickers": 0}

    failures = failures.copy()
    failures["failure_category"] = failures["error_message"].apply(categorize_failure_message)

    provider_level = int(failures[failures["failure_category"].isin(PROVIDER_LEVEL_FAILURE_CATEGORIES)].shape[0])
    ticker_actionable = int(failures[failures["failure_category"].isin(TICKER_ACTIONABLE_FAILURE_CATEGORIES)].shape[0])

    grouped = (
        failures.groupby("ticker")
        .agg(
            total_failures=("failure_category", "size"),
            actionable_failures=("failure_category", lambda s: int(s.isin(TICKER_ACTIONABLE_FAILURE_CATEGORIES).sum())),
            provider_failures=("failure_category", lambda s: int(s.isin(PROVIDER_LEVEL_FAILURE_CATEGORIES).sum())),
            actionable_categories=("failure_category", lambda s: ", ".join(sorted(set(c for c in s if c in TICKER_ACTIONABLE_FAILURE_CATEGORIES)))),
            all_categories=("failure_category", lambda s: ", ".join(sorted(set(s)))),
        )
        .reset_index()
    )
    grouped["actionable_share"] = grouped["actionable_failures"] / grouped["total_failures"].clip(lower=1)

    candidates = grouped[
        (grouped["actionable_failures"] >= min_count)
        & (grouped["actionable_share"] >= 0.6)
    ].sort_values(["actionable_failures", "actionable_share", "ticker"], ascending=[False, False, True])

    suppressed_tickers = int(
        grouped[
            (grouped["total_failures"] >= min_count)
            & (~grouped["ticker"].isin(candidates["ticker"]))
            & (grouped["provider_failures"] >= grouped["actionable_failures"])
        ].shape[0]
    )
    return candidates, {
        "provider_level_failures": provider_level,
        "ticker_actionable_failures": ticker_actionable,
        "suppressed_tickers": suppressed_tickers,
    }


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
        "provider_failure_signal": {},
    }

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

    repeated_failures, provider_signal = _ticker_repeated_live_failure_candidates(
        conn,
        window_days=live_failure_window_days,
        min_count=live_failure_min_count,
    )
    stats["provider_failure_signal"] = provider_signal

    for _, r in _cap(repeated_failures, max_suggestions_per_rule).iterrows():
        _try_create(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="rules_engine",
                priority=RULE_SEVERITY["repeated_live_failure_review"],
                rationale=(
                    f"Rule repeated_live_failure_review: ticker {str(r['ticker'])} had {int(r['actionable_failures'])} "
                    f"ticker-specific live failures in the last {live_failure_window_days} days "
                    f"({r['actionable_categories']}; total failures={int(r['total_failures'])}, provider-related={int(r['provider_failures'])})."
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
