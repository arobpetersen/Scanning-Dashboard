from __future__ import annotations

"""Profile and context loading helpers for scanner research.

This module owns cache-aware profile retrieval and scanner/theme context loading.
It intentionally keeps the current lightweight in-memory cache policy and leaves
workflow orchestration to scanner_research_service.py.
"""

from .provider_live import LiveProvider
from .scanner_audit import scanner_candidate_summary
from .scanner_research_cache import _PROFILE_CACHE


def theme_catalog_context(conn, representative_limit: int = 5) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            t.id AS theme_id,
            t.name AS theme_name,
            t.category,
            t.is_active,
            m.ticker
        FROM themes t
        LEFT JOIN theme_membership m ON m.theme_id = t.id
        WHERE t.is_active = TRUE
        ORDER BY t.name, m.ticker
        """
    ).df()
    if rows.empty:
        return []

    catalog: list[dict[str, object]] = []
    for (theme_id, theme_name, category), frame in rows.groupby(["theme_id", "theme_name", "category"], dropna=False):
        members = [str(value).strip().upper() for value in frame["ticker"].tolist() if str(value or "").strip()]
        member_count = len(members)
        representative_tickers = members[:representative_limit]
        catalog.append(
            {
                "theme_id": int(theme_id),
                "theme_name": str(theme_name),
                "category": str(category or "Uncategorized"),
                "representative_tickers": representative_tickers,
                "member_count": member_count,
                "theme_description": (
                    f"{theme_name} ({category or 'Uncategorized'}) with representative tickers "
                    + (", ".join(representative_tickers) if members else "none")
                ),
                "theme_identity_summary": (
                    f"Governed theme {theme_name} in category {category or 'Uncategorized'} "
                    f"with {member_count} governed member{'s' if member_count != 1 else ''}. "
                    + (
                        f"Representative governed tickers: {', '.join(representative_tickers)}."
                        if representative_tickers
                        else "No representative tickers are currently available."
                    )
                ),
            }
        )
    return catalog


def load_company_profile(ticker: str) -> dict[str, object]:
    from . import scanner_research as legacy

    provider = LiveProvider(include_reference=True)
    if not provider.is_configured:
        return {}
    try:
        ref = provider._fetch_reference(str(ticker).strip().upper())
    except Exception:
        return {}
    if not isinstance(ref, dict):
        return {}
    return {
        "ticker": str(ticker).strip().upper(),
        "company_name": legacy._normalize_text(ref.get("name")),
        "description": legacy._normalize_text(ref.get("description")),
        "sic_description": legacy._normalize_text(ref.get("sic_description")),
        "sic_code": legacy._normalize_text(ref.get("sic_code")),
        "primary_exchange": legacy._normalize_text(ref.get("primary_exchange")),
        "market": legacy._normalize_text(ref.get("market")),
        "locale": legacy._normalize_text(ref.get("locale")),
        "security_type": legacy._normalize_text(ref.get("type")),
        "active": ref.get("active"),
        "currency_name": legacy._normalize_text(ref.get("currency_name")),
        "list_date": legacy._normalize_text(ref.get("list_date")),
        "market_cap": ref.get("market_cap"),
    }


def profile_has_research_value(profile: dict[str, object] | None) -> bool:
    from . import scanner_research as legacy

    if not isinstance(profile, dict):
        return False
    return bool(
        legacy._normalize_text(profile.get("company_name"))
        or legacy._normalize_text(profile.get("description"))
        or legacy._normalize_text(profile.get("sic_description"))
    )


def load_company_profile_with_cache(ticker: str) -> dict[str, object]:
    from . import scanner_research as legacy

    normalized_ticker = str(ticker or "").strip().upper()
    cached = _PROFILE_CACHE.get(normalized_ticker)
    fresh = legacy._load_company_profile(normalized_ticker)
    if profile_has_research_value(fresh):
        profile = dict(fresh)
        profile["_profile_source"] = "live_lookup"
        _PROFILE_CACHE[normalized_ticker] = profile
        return profile
    if profile_has_research_value(cached):
        profile = dict(cached)
        profile["_profile_source"] = "cached_live_lookup"
        return profile
    profile = dict(fresh) if isinstance(fresh, dict) else {}
    if profile:
        profile["_profile_source"] = "live_lookup_empty"
    return profile


def candidate_context(conn, ticker: str) -> dict[str, object]:
    candidates = scanner_candidate_summary(conn)
    if candidates.empty:
        raise ValueError("No Scanner Audit candidates are available.")
    match = candidates[candidates["ticker"] == str(ticker).strip().upper()]
    if match.empty:
        raise ValueError(f"Scanner Audit candidate not found for {ticker}.")
    row = match.iloc[0]
    return {
        "ticker": str(row["ticker"]),
        "recommendation": str(row["recommendation"]),
        "recommendation_reason": str(row["recommendation_reason"]),
        "persistence_score": int(row["persistence_score"]),
        "observed_days": int(row["observed_days"]),
        "observations_last_5d": int(row["observations_last_5d"]),
        "observations_last_10d": int(row["observations_last_10d"]),
        "current_streak": int(row["current_streak"]),
        "distinct_scanner_count": int(row["distinct_scanner_count"]),
        "first_seen": str(row["first_seen"]),
        "last_seen": str(row["last_seen"]),
        "scanners": str(row["scanners"]),
        "source_labels": str(row["source_labels"]),
        "metadata_basis": str(row["metadata_basis"]),
        "governed_status": str(row["governed_status"]),
    }
