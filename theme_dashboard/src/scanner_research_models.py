from __future__ import annotations

"""Stable contract types for scanner research.

ResearchDraft is the domain contract for an advisory research result. It is not a
page-specific payload. The service layer may attach UI/session metadata around a
draft, but those values are non-authoritative and should not be treated as
domain inputs.
"""

from dataclasses import dataclass, field
from typing import Any, Mapping


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_str_list(values: object) -> list[str]:
    return [str(value).strip() for value in list(values or []) if str(value or "").strip()]


def _normalize_mapping_list(values: object) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for value in list(values or []):
        if isinstance(value, Mapping):
            normalized.append(dict(value))
    return normalized


@dataclass(slots=True)
class ResearchDraft:
    """Stable domain workflow contract for scanner research generation.

    Field groups:
    - Domain fields: advisory content a reviewer evaluates.
    - Workflow fields: provenance and generation state.
    - Debug fields: analysis traces, timing, and audit helpers.
    - UI/session fields: display conveniences only; not authoritative inputs.
    """

    ticker: str = ""
    company_name: str = ""
    short_company_description: str = ""
    possible_similar_tickers: list[str] = field(default_factory=list)
    suggested_existing_themes: list[dict[str, object]] = field(default_factory=list)
    possible_new_theme: str | None = None
    possible_new_theme_category: str | None = None
    confidence: str = "low"
    rationale: str = ""
    caveats: list[str] = field(default_factory=list)
    recommended_action: str = "watch_only"

    generated_at: str = ""
    source: str = ""
    research_mode: str = ""
    theme_generation_strategy: str = "description_theme_generation"
    fallback_reason: str | None = None
    research_error: dict[str, object] = field(default_factory=dict)

    domain_anchor: str = ""
    dominant_business_role: str = ""
    candidate_theme_ideas: list[str] = field(default_factory=list)
    business_descriptors: list[str] = field(default_factory=list)
    matched_theme_candidates: list[dict[str, object]] = field(default_factory=list)
    validation_debug: dict[str, object] = field(default_factory=dict)
    research_timing_summary: dict[str, object] = field(default_factory=dict)
    research_context_meta: dict[str, object] = field(default_factory=dict)
    research_decision_trace: dict[str, object] = field(default_factory=dict)

    draft_source: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "ResearchDraft":
        payload = payload if isinstance(payload, Mapping) else {}
        return cls(
            ticker=_normalize_text(payload.get("ticker")).upper(),
            company_name=_normalize_text(payload.get("company_name")),
            short_company_description=_normalize_text(payload.get("short_company_description")),
            possible_similar_tickers=[value.upper() for value in _normalize_str_list(payload.get("possible_similar_tickers"))],
            suggested_existing_themes=_normalize_mapping_list(payload.get("suggested_existing_themes")),
            possible_new_theme=_normalize_text(payload.get("possible_new_theme")) or None,
            possible_new_theme_category=_normalize_text(payload.get("possible_new_theme_category")) or None,
            confidence=_normalize_text(payload.get("confidence")) or "low",
            rationale=_normalize_text(payload.get("rationale")),
            caveats=_normalize_str_list(payload.get("caveats")),
            recommended_action=_normalize_text(payload.get("recommended_action")) or "watch_only",
            generated_at=_normalize_text(payload.get("generated_at")),
            source=_normalize_text(payload.get("source")),
            research_mode=_normalize_text(payload.get("research_mode")),
            theme_generation_strategy=_normalize_text(payload.get("theme_generation_strategy")) or "description_theme_generation",
            fallback_reason=_normalize_text(payload.get("fallback_reason")) or None,
            research_error=dict(payload.get("research_error") or {}) if isinstance(payload.get("research_error"), Mapping) else {},
            domain_anchor=_normalize_text(payload.get("domain_anchor")),
            dominant_business_role=_normalize_text(payload.get("dominant_business_role")),
            candidate_theme_ideas=_normalize_str_list(payload.get("candidate_theme_ideas")),
            business_descriptors=_normalize_str_list(payload.get("business_descriptors")),
            matched_theme_candidates=_normalize_mapping_list(payload.get("matched_theme_candidates")),
            validation_debug=dict(payload.get("validation_debug") or {}) if isinstance(payload.get("validation_debug"), Mapping) else {},
            research_timing_summary=dict(payload.get("research_timing_summary") or {}) if isinstance(payload.get("research_timing_summary"), Mapping) else {},
            research_context_meta=dict(payload.get("research_context_meta") or {}) if isinstance(payload.get("research_context_meta"), Mapping) else {},
            research_decision_trace=dict(payload.get("research_decision_trace") or {}) if isinstance(payload.get("research_decision_trace"), Mapping) else {},
            draft_source=_normalize_text(payload.get("draft_source")) or None,
        )

    def domain_payload(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "company_name": self.company_name,
            "short_company_description": self.short_company_description,
            "possible_similar_tickers": list(self.possible_similar_tickers),
            "suggested_existing_themes": [dict(item) for item in self.suggested_existing_themes],
            "possible_new_theme": self.possible_new_theme,
            "possible_new_theme_category": self.possible_new_theme_category,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "caveats": list(self.caveats),
            "recommended_action": self.recommended_action,
        }

    def workflow_payload(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "source": self.source,
            "research_mode": self.research_mode,
            "theme_generation_strategy": self.theme_generation_strategy,
            "fallback_reason": self.fallback_reason,
            "research_error": dict(self.research_error),
        }

    def debug_payload(self) -> dict[str, object]:
        return {
            "domain_anchor": self.domain_anchor,
            "dominant_business_role": self.dominant_business_role,
            "candidate_theme_ideas": list(self.candidate_theme_ideas),
            "business_descriptors": list(self.business_descriptors),
            "matched_theme_candidates": [dict(item) for item in self.matched_theme_candidates],
            "validation_debug": dict(self.validation_debug),
            "research_timing_summary": dict(self.research_timing_summary),
            "research_context_meta": dict(self.research_context_meta),
            "research_decision_trace": dict(self.research_decision_trace),
        }

    def ui_payload(self) -> dict[str, object]:
        return {
            "draft_source": self.draft_source,
        }

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {}
        payload.update(self.domain_payload())
        payload.update(self.workflow_payload())
        payload.update(self.debug_payload())
        payload.update(self.ui_payload())
        return payload


@dataclass(slots=True)
class ResearchDraftRunResult:
    """Page-facing result for generate/reuse actions.

    Pages should collect user input, call the service, persist the returned draft
    in session state if desired, and render the already-normalized result.
    """

    draft: ResearchDraft
    reused: bool
    draft_source: str
    feedback_message: str

