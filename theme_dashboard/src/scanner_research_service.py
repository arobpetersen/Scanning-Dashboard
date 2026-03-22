from __future__ import annotations

"""Research orchestrator service.

What the orchestrator owns:
- loading scanner candidate, theme catalog, and profile inputs
- running description-first heuristic generation
- merging OpenAI output when configured
- normalizing to the stable ResearchDraft contract
- handing persistence requests off to dedicated persistence helpers

What pages should do:
- collect user input
- call this service
- store/render returned results

What pages should not do:
- assemble domain payloads inline
- infer draft provenance rules
- own research persistence details
"""

from .scanner_research_models import ResearchDraft, ResearchDraftRunResult
from .scanner_research_persistence import (
    get_scanner_research_review,
    save_scanner_research_review,
    scanner_research_review_summary,
)
from .scanner_research_profiles import candidate_context, load_company_profile_with_cache, theme_catalog_context
from .scanner_research_analysis import preprocessed_catalog
from .scanner_research_merge import ai_research_draft_for_strategy, baseline_research_draft


DEFAULT_RESEARCH_STRATEGY = "description_theme_generation"


def _normalize_strategy(value: object) -> str:
    normalized = str(value or "").strip()
    return DEFAULT_RESEARCH_STRATEGY if normalized != DEFAULT_RESEARCH_STRATEGY else normalized


def generate_research_draft(conn, ticker: str, *, strategy: str = DEFAULT_RESEARCH_STRATEGY) -> ResearchDraft:
    from . import scanner_research as legacy

    normalized_ticker = str(ticker or "").strip().upper()
    normalized_strategy = _normalize_strategy(strategy)
    total_start = legacy._now_perf()
    candidate_start = legacy._now_perf()
    candidate = candidate_context(conn, normalized_ticker)
    candidate_ms = legacy._elapsed_ms(candidate_start)
    catalog_start = legacy._now_perf()
    catalog = theme_catalog_context(conn)
    catalog_ms = legacy._elapsed_ms(catalog_start)
    preprocess_start = legacy._now_perf()
    preprocessed_theme_catalog = preprocessed_catalog(catalog)
    catalog_preprocess_ms = legacy._elapsed_ms(preprocess_start)
    profile_start = legacy._now_perf()
    profile = load_company_profile_with_cache(candidate["ticker"])
    profile_ms = legacy._elapsed_ms(profile_start)
    generated_at = legacy.datetime.now(legacy.UTC).replace(tzinfo=None).isoformat(sep=" ")

    research_mode = "heuristic_fallback"
    fallback_reason = None
    research_error = None
    try:
        draft_payload = ai_research_draft_for_strategy(
            candidate,
            preprocessed_theme_catalog,
            profile,
            strategy=normalized_strategy,
        )
        research_mode = "openai"
    except Exception as exc:
        draft_payload = baseline_research_draft(candidate, preprocessed_theme_catalog, profile)
        research_error = legacy._extract_openai_error_details(exc)
        fallback_reason = legacy._format_openai_error_summary(research_error)

    draft = ResearchDraft.from_mapping(draft_payload)
    draft.ticker = candidate["ticker"]
    draft.generated_at = generated_at
    draft.source = "scanner_audit"
    draft.research_mode = research_mode
    draft.theme_generation_strategy = normalized_strategy
    draft.fallback_reason = fallback_reason
    draft.research_error = dict(research_error or {})
    timing = dict(draft.research_timing_summary or {})
    timing.update(
        {
            "candidate_context_ms": candidate_ms,
            "catalog_query_ms": catalog_ms,
            "catalog_preprocess_ms": catalog_preprocess_ms,
            "profile_lookup_ms": profile_ms,
            "total_ms": legacy._elapsed_ms(total_start),
        }
    )
    draft.research_timing_summary = timing
    if fallback_reason:
        draft.fallback_reason = fallback_reason
    if research_error:
        draft.research_error = research_error
    return draft


def get_or_create_research_draft(
    conn,
    ticker: str,
    existing_draft: dict[str, object] | ResearchDraft | None = None,
    *,
    force_refresh: bool = False,
    strategy: str = DEFAULT_RESEARCH_STRATEGY,
) -> ResearchDraftRunResult:
    normalized_ticker = str(ticker or "").strip().upper()
    normalized_strategy = _normalize_strategy(strategy)
    existing = existing_draft if isinstance(existing_draft, ResearchDraft) else ResearchDraft.from_mapping(existing_draft)
    reused = bool(
        not force_refresh
        and existing.ticker == normalized_ticker
        and existing.theme_generation_strategy == normalized_strategy
        and existing.generated_at
    )
    if reused:
        draft = existing
        draft_source = "reused_session_draft"
        feedback_message = f"Reused existing advisory research draft for {normalized_ticker}."
    else:
        draft = generate_research_draft(conn, normalized_ticker, strategy=normalized_strategy)
        draft_source = "forced_regeneration" if force_refresh else "fresh_generation"
        feedback_message = (
            f"Regenerated advisory research draft for {normalized_ticker}."
            if force_refresh
            else f"Generated advisory research draft for {normalized_ticker}."
        )
    draft.draft_source = draft_source
    return ResearchDraftRunResult(
        draft=draft,
        reused=reused,
        draft_source=draft_source,
        feedback_message=feedback_message,
    )


def load_research_review(conn, ticker: str, draft: dict[str, object] | ResearchDraft | None) -> dict[str, object] | None:
    return get_scanner_research_review(conn, ticker, draft)


def persist_research_review(
    conn,
    ticker: str,
    draft: dict[str, object] | ResearchDraft | None,
    *,
    outcome_class: object,
    reviewer_notes: object = "",
) -> dict[str, object]:
    return save_scanner_research_review(
        conn,
        ticker,
        draft,
        outcome_class=outcome_class,
        reviewer_notes=reviewer_notes,
    )


def load_research_review_summary(conn, *, limit: int = 8) -> dict[str, object]:
    return scanner_research_review_summary(conn, limit=limit)
