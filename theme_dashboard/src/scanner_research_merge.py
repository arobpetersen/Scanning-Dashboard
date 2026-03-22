from __future__ import annotations

"""Merge and normalization helpers for scanner research outputs.

This module keeps AI/heuristic merge behavior together without owning the
workflow orchestration that decides when each stage runs. Some helper calls
still route through legacy wrappers intentionally so existing patch points keep
working during the staged cleanup.
"""

import json

import requests

from .ai_proposals import sanitize_context
from .config import AI_MODEL, OPENAI_API_KEY_ENV, openai_api_key
from .scanner_research_heuristics import (
    candidate_roles,
    has_strong_role_evidence,
    is_generic_factor_theme,
)


def call_openai_research(api_key: str, context: dict[str, object], *, max_output_tokens: int = 550) -> dict[str, object]:
    from . import scanner_research as legacy

    payload = {
        "model": AI_MODEL,
        "max_output_tokens": max_output_tokens,
        "input": [
            {"role": "system", "content": legacy.RESEARCH_DRAFT_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Generate one concise advisory research draft using the provided context only. "
                    "Populate every required field. If existing themes are weak, say so explicitly, provide a useful rationale, "
                    "and suggest a concise possible_new_theme when justified. Compare the best existing governed-theme fit against the best narrow business-role label and choose the more precise answer. "
                    f"Context JSON: {json.dumps(sanitize_context(context))[:16000]}"
                ),
            },
        ],
    }
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}"},
        json=payload,
        timeout=45,
    )
    response.raise_for_status()
    data = response.json()
    text = data.get("output_text", "")
    parsed = json.loads(text) if text else {}
    return parsed if isinstance(parsed, dict) else {}


def estimate_context_size_chars(context: dict[str, object]) -> int:
    return len(json.dumps(sanitize_context(context)))


def normalize_action(value: object, fallback: str = "watch_only") -> str:
    from . import scanner_research as legacy

    normalized = legacy._normalize_text(value) or fallback
    allowed = {"add_to_existing_theme_review", "consider_new_theme", "watch_only", "reject_for_now"}
    return normalized if normalized in allowed else fallback


def existing_theme_fit_is_adjacent_only(best_fit: dict[str, object]) -> bool:
    if not isinstance(best_fit, dict):
        return False
    if bool(best_fit.get("direct_role_fit")):
        return False
    if bool(best_fit.get("indirect_only_fit")):
        return True
    return int(best_fit.get("score") or 0) < 12


def best_suggested_theme_fit_details(
    suggested_existing: list[dict[str, object]],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, object]:
    from . import scanner_research as legacy

    candidate_analysis = legacy._candidate_analysis(profile, candidate)
    by_id = {int(item["theme_id"]): item for item in legacy._preprocessed_catalog(catalog)}
    best: dict[str, object] = {"score": 0, "direct_role_fit": False, "indirect_only_fit": False}
    for suggestion in suggested_existing:
        try:
            theme_id = int(suggestion.get("theme_id"))
        except Exception:
            continue
        entry = by_id.get(theme_id)
        if entry is None:
            continue
        details = legacy._theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis)
        if int(details["score"]) > int(best.get("score") or 0):
            best = details
    return best


def precision_override_reason(possible_new_theme: str, suggested_existing: list[dict[str, object]]) -> str:
    if not possible_new_theme:
        return ""
    if suggested_existing:
        best_existing = suggested_existing[0]
        return (
            f"{possible_new_theme} is a more precise business-role label than {best_existing.get('theme_name')}, "
            "which looks more like a broad adjacency fit."
        )
    return f"{possible_new_theme} is more precise than the current governed-theme coverage."


def rationale_signals_precision_gap(rationale: str) -> bool:
    normalized = str(rationale or "").strip().lower()
    if not normalized:
        return False
    precision_markers = [
        "more precise",
        "narrower",
        "actual role",
        "business role",
        "value-chain position",
        "value chain position",
        "operating role",
        "better reflects",
    ]
    adjacency_markers = [
        "adjacent",
        "indirect",
        "end-market adjacency",
        "end market adjacency",
        "adjacency fit",
        "adjacency fits",
        "end-market based",
        "end market based",
        "weaker",
        "broad alternatives",
        "secondary",
        "broader adjacency fit",
        "broader adjacency fits",
        "broader fit",
    ]
    return any(marker in normalized for marker in precision_markers) and any(marker in normalized for marker in adjacency_markers)


def build_ai_context(
    candidate: dict[str, object],
    profile: dict[str, object],
    filtered_catalog: list[dict[str, object]],
    heuristic_baseline: dict[str, object],
) -> dict[str, object]:
    return {
        "candidate": candidate,
        "company_profile": profile,
        "governed_theme_catalog": filtered_catalog,
        "heuristic_baseline": {
            "suggested_existing_themes": heuristic_baseline.get("suggested_existing_themes") or [],
            "possible_new_theme": heuristic_baseline.get("possible_new_theme"),
            "possible_new_theme_category": heuristic_baseline.get("possible_new_theme_category"),
            "recommended_action": heuristic_baseline.get("recommended_action"),
            "rationale_summary": heuristic_baseline.get("rationale"),
            "domain_anchor": heuristic_baseline.get("domain_anchor"),
            "dominant_business_role": heuristic_baseline.get("dominant_business_role"),
            "candidate_theme_ideas": list(heuristic_baseline.get("candidate_theme_ideas") or [])[:5],
            "matched_theme_candidates": list(heuristic_baseline.get("matched_theme_candidates") or [])[:5],
        },
    }


def normalize_ai_draft_payload(
    raw: dict[str, object],
    *,
    candidate: dict[str, object],
    profile: dict[str, object],
    catalog: list[dict[str, object]],
    context_meta: dict[str, object],
) -> dict[str, object]:
    from . import scanner_research as legacy

    return {
        "ticker": candidate["ticker"],
        "company_name": legacy._normalize_text(raw.get("company_name")) or legacy._normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": legacy._normalize_text(raw.get("short_company_description")) or legacy._normalize_text(profile.get("description")) or "No verified company description available.",
        "possible_similar_tickers": [str(value).strip().upper() for value in raw.get("possible_similar_tickers") or [] if str(value).strip()][:5],
        "suggested_existing_themes": normalize_ai_theme_suggestions(raw.get("suggested_existing_themes"), catalog),
        "possible_new_theme": legacy._normalize_optional_theme_label(raw.get("possible_new_theme")),
        "possible_new_theme_category": legacy._normalize_text(raw.get("possible_new_theme_category")),
        "confidence": legacy._normalize_text(raw.get("confidence")) or "low",
        "rationale": legacy._normalize_text(raw.get("rationale")),
        "caveats": [str(value).strip() for value in raw.get("caveats") or [] if str(value).strip()],
        "recommended_action": normalize_action(raw.get("recommended_action"), "watch_only"),
        "research_context_meta": context_meta,
    }


def baseline_research_draft(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
) -> dict[str, object]:
    from . import scanner_research as legacy

    baseline_start = legacy._now_perf()
    draft = legacy._description_theme_generation_draft(candidate, catalog, profile)
    timing = dict(draft.get("research_timing_summary") or {})
    timing["baseline_total_ms"] = legacy._elapsed_ms(baseline_start)
    draft["research_timing_summary"] = timing
    return draft


def ai_research_draft_for_strategy(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    strategy: str,
) -> dict[str, object]:
    from . import scanner_research as legacy

    ai_total_start = legacy._now_perf()
    normalized_strategy = legacy._normalize_research_strategy(strategy)
    api_key = openai_api_key()
    if not api_key:
        raise ValueError(f"{OPENAI_API_KEY_ENV} is not set.")
    baseline_start = legacy._now_perf()
    heuristic_baseline = legacy._baseline_research_draft(candidate, catalog, profile)
    baseline_ms = legacy._elapsed_ms(baseline_start)
    prefilter_start = legacy._now_perf()
    filtered_catalog, context_meta = legacy._prefilter_ai_theme_catalog(
        candidate,
        catalog,
        profile,
        max_themes=8,
    )
    prefilter_ms = legacy._elapsed_ms(prefilter_start)
    context = build_ai_context(candidate, profile, filtered_catalog, heuristic_baseline)
    context_meta["estimated_context_chars"] = legacy._estimate_context_size_chars(context)
    request_start = legacy._now_perf()
    raw = legacy._call_openai_research(
        api_key,
        context,
        max_output_tokens=400,
    )
    ai_request_ms = legacy._elapsed_ms(request_start)
    normalize_start = legacy._now_perf()
    ai_draft = normalize_ai_draft_payload(
        raw,
        candidate=candidate,
        profile=profile,
        catalog=catalog,
        context_meta=context_meta,
    )
    ai_normalize_ms = legacy._elapsed_ms(normalize_start)
    merge_start = legacy._now_perf()
    draft = merge_ai_with_heuristic_draft(ai_draft, heuristic_baseline, catalog, profile, candidate)
    merge_ms = legacy._elapsed_ms(merge_start)
    draft["research_context_meta"] = context_meta
    draft["theme_generation_strategy"] = normalized_strategy
    draft["domain_anchor"] = heuristic_baseline.get("domain_anchor") or legacy._domain_anchor(profile, candidate)
    draft["dominant_business_role"] = heuristic_baseline.get("dominant_business_role") or (legacy._dominant_economic_role(profile, candidate) or "unclear")
    draft["candidate_theme_ideas"] = list(heuristic_baseline.get("candidate_theme_ideas") or [])
    draft["matched_theme_candidates"] = list(heuristic_baseline.get("matched_theme_candidates") or [])
    draft["validation_debug"] = dict(heuristic_baseline.get("validation_debug") or {})
    timing = dict(heuristic_baseline.get("research_timing_summary") or {})
    timing.update(
        {
            "baseline_total_ms": baseline_ms,
            "catalog_prefilter_ms": prefilter_ms,
            "ai_request_ms": ai_request_ms,
            "ai_normalize_ms": ai_normalize_ms,
            "merge_ms": merge_ms,
            "model_attempts": 1,
            "strategy_total_ms": legacy._elapsed_ms(ai_total_start),
        }
    )
    draft["research_timing_summary"] = timing
    if not draft["suggested_existing_themes"] and not draft["possible_new_theme"]:
        draft["caveats"] = list(draft.get("caveats") or [])
        draft["caveats"].append("AI did not find a strong grounded theme fit in the current governed catalog.")
    return draft


def merge_ai_with_heuristic_draft(
    ai_draft: dict[str, object],
    heuristic_draft: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, object]:
    from . import scanner_research as legacy
    from .scanner_research_profiles import profile_has_research_value

    merged = dict(heuristic_draft)
    merged.update({k: v for k, v in ai_draft.items() if v not in (None, "", [], {})})

    merged["company_name"] = legacy._normalize_text(ai_draft.get("company_name")) or heuristic_draft.get("company_name")
    merged["short_company_description"] = legacy._normalize_text(ai_draft.get("short_company_description")) or heuristic_draft.get("short_company_description")

    ai_similar = [str(value).strip().upper() for value in ai_draft.get("possible_similar_tickers") or [] if str(value).strip()]
    merged["possible_similar_tickers"] = ai_similar[:5] if ai_similar else list(heuristic_draft.get("possible_similar_tickers") or [])

    ai_suggested = list(ai_draft.get("suggested_existing_themes") or [])
    merged["suggested_existing_themes"] = ai_suggested if ai_suggested else list(heuristic_draft.get("suggested_existing_themes") or [])

    merged["possible_new_theme"] = legacy._normalize_optional_theme_label(ai_draft.get("possible_new_theme")) or legacy._normalize_optional_theme_label(heuristic_draft.get("possible_new_theme"))
    merged["possible_new_theme_category"] = legacy._normalize_text(ai_draft.get("possible_new_theme_category")) or legacy._normalize_text(heuristic_draft.get("possible_new_theme_category"))
    merged["confidence"] = legacy._normalize_text(ai_draft.get("confidence")) or heuristic_draft.get("confidence") or "low"
    merged["recommended_action"] = normalize_action(ai_draft.get("recommended_action"), heuristic_draft.get("recommended_action") or "watch_only")

    ai_rationale = legacy._normalize_text(ai_draft.get("rationale"))
    heuristic_rationale = legacy._normalize_text(heuristic_draft.get("rationale"))
    merged["rationale"] = ai_rationale or heuristic_rationale or "No grounded rationale was available."

    ai_caveats = [str(value).strip() for value in ai_draft.get("caveats") or [] if str(value).strip()]
    heuristic_caveats = [str(value).strip() for value in heuristic_draft.get("caveats") or [] if str(value).strip()]
    merged["caveats"] = ai_caveats or heuristic_caveats

    if not merged["possible_new_theme"] and merged["recommended_action"] == "consider_new_theme":
        merged["possible_new_theme"] = legacy._normalize_optional_theme_label(heuristic_draft.get("possible_new_theme"))
    if merged.get("possible_new_theme") and not merged.get("possible_new_theme_category"):
        merged["possible_new_theme_category"] = legacy._proposed_new_theme_category(
            profile,
            candidate,
            merged.get("possible_new_theme"),
        )

    ai_role_context = [
        ai_draft.get("short_company_description"),
        ai_draft.get("rationale"),
    ]
    heuristic_prefers_new_theme = (
        normalize_action(heuristic_draft.get("recommended_action")) == "consider_new_theme"
        and legacy._normalize_text(heuristic_draft.get("possible_new_theme"))
    )
    strong_role_evidence = has_strong_role_evidence(profile, candidate, *ai_role_context)
    candidate_new_theme = legacy._candidate_new_theme_label(profile, candidate, *ai_role_context)
    supports_distinct_new_theme = legacy._supports_distinct_new_theme_label(profile, candidate, *ai_role_context)
    ai_rationale_signals_gap = rationale_signals_precision_gap(ai_rationale)
    merged_rationale_signals_gap = rationale_signals_precision_gap(str(merged.get("rationale") or ""))
    best_ai_existing_fit = best_suggested_theme_fit_details(
        list(merged.get("suggested_existing_themes") or []),
        catalog,
        profile,
        candidate,
    )
    adjacency_only_existing_fit = existing_theme_fit_is_adjacent_only(best_ai_existing_fit)
    inferred_candidate_roles = candidate_roles(profile, candidate, *ai_role_context)
    role_specific_context_supports_new_theme = bool(inferred_candidate_roles) and (
        profile_has_research_value(profile) or any(legacy._normalize_text(part) for part in ai_role_context)
    )
    should_promote_new_theme = (
        bool(candidate_new_theme)
        and supports_distinct_new_theme
        and (
            heuristic_prefers_new_theme
            or ai_rationale_signals_gap
            or merged_rationale_signals_gap
            or (
                role_specific_context_supports_new_theme
                and list(merged.get("suggested_existing_themes") or [])
                and adjacency_only_existing_fit
            )
        )
        and (not merged.get("suggested_existing_themes") or adjacency_only_existing_fit)
    )
    top_existing_is_generic_factor = False
    existing_suggestions = list(merged.get("suggested_existing_themes") or [])
    if existing_suggestions:
        top_existing = existing_suggestions[0]
        top_existing_is_generic_factor = is_generic_factor_theme(
            {
                "theme_name": top_existing.get("theme_name"),
                "category": top_existing.get("category"),
                "theme_description": top_existing.get("why_it_might_fit"),
            }
        )
    if bool(candidate_new_theme) and strong_role_evidence and top_existing_is_generic_factor:
        should_promote_new_theme = True
    merged["research_decision_trace"] = {
        "candidate_new_theme": candidate_new_theme,
        "candidate_roles_detected": sorted(inferred_candidate_roles),
        "supports_distinct_new_theme": supports_distinct_new_theme,
        "ai_rationale_signals_gap": ai_rationale_signals_gap,
        "merged_rationale_signals_gap": merged_rationale_signals_gap,
        "best_existing_fit_score": int(best_ai_existing_fit.get("score") or 0),
        "best_existing_fit_direct_role": bool(best_ai_existing_fit.get("direct_role_fit")),
        "best_existing_fit_indirect_only": bool(best_ai_existing_fit.get("indirect_only_fit")),
        "adjacency_only_existing_fit": adjacency_only_existing_fit,
        "heuristic_prefers_new_theme": heuristic_prefers_new_theme,
        "strong_role_evidence": strong_role_evidence,
        "top_existing_is_generic_factor": top_existing_is_generic_factor,
        "should_promote_new_theme": should_promote_new_theme,
    }
    if should_promote_new_theme:
        merged["possible_new_theme"] = (
            legacy._normalize_optional_theme_label(ai_draft.get("possible_new_theme"))
            or candidate_new_theme
            or legacy._normalize_optional_theme_label(heuristic_draft.get("possible_new_theme"))
        )
        merged["possible_new_theme_category"] = legacy._normalize_text(ai_draft.get("possible_new_theme_category")) or legacy._proposed_new_theme_category(
            profile,
            candidate,
            merged["possible_new_theme"],
        )
        merged["recommended_action"] = "consider_new_theme"
        if legacy._normalize_text(merged.get("confidence")) in {"high", ""}:
            merged["confidence"] = "medium"
        precision_sentence = precision_override_reason(
            str(merged["possible_new_theme"]),
            list(merged.get("suggested_existing_themes") or []),
        )
        if precision_sentence not in merged["rationale"]:
            merged["rationale"] = f"{merged['rationale']} {precision_sentence}".strip()
        caveats = [str(value).strip() for value in merged.get("caveats") or [] if str(value).strip()]
        adjacency_caveat = "Existing governed themes look adjacent rather than direct fits for the company's narrow business role."
        if list(merged.get("suggested_existing_themes") or []) and adjacency_caveat not in caveats:
            caveats.append(adjacency_caveat)
        merged["caveats"] = caveats
    elif not supports_distinct_new_theme and normalize_action(ai_draft.get("recommended_action"), "watch_only") == "watch_only":
        merged["possible_new_theme"] = None
        merged["possible_new_theme_category"] = None

    if not legacy._normalize_text(merged.get("rationale")):
        merged["rationale"] = heuristic_rationale or "No strong governed-theme fit was identified; review the business role manually."

    merged["suggested_existing_themes"] = legacy._annotate_existing_theme_suggestions(
        list(merged.get("suggested_existing_themes") or []),
        catalog,
        profile,
        candidate,
    )
    merged["suggested_existing_themes"] = legacy._prioritize_operating_role_suggestions(
        list(merged.get("suggested_existing_themes") or []),
        strong_role_evidence=strong_role_evidence,
    )

    return merged


def normalize_ai_theme_suggestions(raw_items: object, catalog: list[dict[str, object]]) -> list[dict[str, object]]:
    from . import scanner_research as legacy

    if not isinstance(raw_items, list):
        return []
    by_id = {int(item["theme_id"]): item for item in catalog}
    by_name = {str(item["theme_name"]).strip().lower(): item for item in catalog}
    normalized: list[dict[str, object]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        catalog_entry = None
        theme_id = item.get("theme_id")
        if theme_id not in (None, ""):
            try:
                catalog_entry = by_id.get(int(theme_id))
            except Exception:
                catalog_entry = None
        if catalog_entry is None:
            theme_name = str(item.get("theme_name") or "").strip().lower()
            catalog_entry = by_name.get(theme_name)
        if catalog_entry is None:
            continue
        normalized.append(
            {
                "theme_id": int(catalog_entry["theme_id"]),
                "theme_name": str(catalog_entry["theme_name"]),
                "category": str(catalog_entry["category"]),
                "why_it_might_fit": legacy._normalize_text(item.get("why_it_might_fit")) or "AI suggested this as a possible governed-theme fit.",
                "representative_tickers": list(catalog_entry.get("representative_tickers") or []),
                "fit_label": legacy._normalize_text(item.get("fit_label")),
            }
        )
    return legacy._truncate_existing_theme_suggestions(normalized)
