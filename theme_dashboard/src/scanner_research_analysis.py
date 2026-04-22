from __future__ import annotations

"""Pure and cache-aware heuristic analysis helpers for scanner research.

This module owns the description-first preprocessing and heuristic analysis
stages. It stays side-effect free except for explicit use of the lightweight
in-memory caches defined in scanner_research_cache.py. Cache misses still route
through selected legacy wrappers where tests intentionally patch those names.
"""

from .scanner_research_cache import _DESCRIPTION_ANALYSIS_CACHE, _THEME_PREPROCESS_CACHE
from .scanner_research_heuristics import (
    annotate_suggestion_fit,
    candidate_archetypes,
    candidate_concepts,
    candidate_economic_roles,
    candidate_end_markets,
    candidate_roles,
    candidate_theme_ideas_from_description,
    description_generated_match_is_actionable,
    description_match_support_text,
    domain_anchor,
    dominant_economic_role,
    fit_label_from_details,
    format_signal_names,
    is_generic_factor_theme,
    looks_generic_theme,
    theme_archetypes,
    theme_cluster_key,
    theme_concepts,
    theme_economic_roles,
    theme_end_markets,
    theme_fit_details,
    theme_match_from_generated_idea,
    theme_roles,
    truncate_existing_theme_suggestions,
    value_chain_summary,
)


def theme_preprocess_cache_key(theme_entry: dict[str, object]) -> tuple[object, ...]:
    from . import scanner_research as legacy

    return (
        int(theme_entry["theme_id"]),
        legacy._normalize_text(theme_entry.get("theme_name")),
        legacy._normalize_text(theme_entry.get("category")),
        legacy._normalize_text(theme_entry.get("theme_description")),
        tuple(str(value or "").strip().upper() for value in list(theme_entry.get("representative_tickers") or [])),
    )


def build_preprocessed_theme_entry(theme_entry: dict[str, object]) -> dict[str, object]:
    from . import scanner_research as legacy

    prepared = dict(theme_entry)
    prepared["_theme_tokens"] = legacy._tokenize(theme_entry.get("theme_name"), theme_entry.get("category"), theme_entry.get("theme_description"))
    prepared["_theme_concepts"] = theme_concepts(theme_entry)
    prepared["_theme_roles"] = theme_roles(theme_entry)
    prepared["_theme_markets"] = theme_end_markets(theme_entry)
    prepared["_theme_archetypes"] = theme_archetypes(theme_entry)
    prepared["_theme_economic_roles"] = theme_economic_roles(theme_entry)
    prepared["_looks_generic_theme"] = looks_generic_theme(theme_entry)
    prepared["_is_generic_factor_theme"] = is_generic_factor_theme(theme_entry)
    return prepared


def preprocessed_theme_entry(theme_entry: dict[str, object]) -> dict[str, object]:
    from . import scanner_research as legacy

    key = theme_preprocess_cache_key(theme_entry)
    cached = _THEME_PREPROCESS_CACHE.get(key)
    if cached is not None:
        return dict(cached)
    # Preserve the legacy patch point on cache misses while the compatibility
    # facade remains part of the supported test surface.
    prepared = legacy._build_preprocessed_theme_entry(theme_entry)
    _THEME_PREPROCESS_CACHE[key] = prepared
    return dict(prepared)


def preprocessed_catalog(catalog: list[dict[str, object]]) -> list[dict[str, object]]:
    return [preprocessed_theme_entry(entry) for entry in list(catalog or [])]


def concise_theme_context(theme_entry: dict[str, object], representative_limit: int = 3) -> dict[str, object]:
    from . import scanner_research as legacy

    representative_tickers = [str(value).strip().upper() for value in list(theme_entry.get("representative_tickers") or []) if str(value).strip()][:representative_limit]
    description = legacy._normalize_text(theme_entry.get("theme_description"))
    if description:
        description = description[:180]
    elif representative_tickers:
        description = f"Representative tickers: {', '.join(representative_tickers)}"
    return {
        "theme_id": int(theme_entry["theme_id"]),
        "theme_name": str(theme_entry["theme_name"]),
        "category": str(theme_entry.get("category") or "Uncategorized"),
        "theme_description": description,
        "representative_tickers": representative_tickers,
    }


def description_analysis_cache_key(
    profile: dict[str, object],
    candidate: dict[str, object],
    extra_parts: tuple[object, ...],
) -> tuple[object, ...]:
    from . import scanner_research as legacy

    return (
        legacy._normalize_text(candidate.get("ticker")).upper(),
        legacy._normalize_text(profile.get("company_name")),
        legacy._normalize_text(profile.get("description")),
        legacy._normalize_text(profile.get("sic_description")),
        legacy._normalize_text(candidate.get("recommendation_reason")),
        tuple(legacy._normalize_text(part) for part in extra_parts),
    )


def build_candidate_analysis(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> dict[str, object]:
    from . import scanner_research as legacy

    description_text = " ".join(
        [
            legacy._normalize_text(profile.get("company_name")),
            legacy._normalize_text(profile.get("description")),
            legacy._normalize_text(profile.get("sic_description")),
            legacy._normalize_text(candidate.get("recommendation_reason")),
            *[legacy._normalize_text(part) for part in extra_parts],
        ]
    )
    profile_tokens = legacy._tokenize(
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    concepts = candidate_concepts(profile, candidate)
    roles = candidate_roles(profile, candidate, *extra_parts)
    markets = candidate_end_markets(profile, candidate, *extra_parts)
    archetypes = candidate_archetypes(profile, candidate, *extra_parts)
    economic_roles = candidate_economic_roles(profile, candidate, *extra_parts)
    dominant_role = dominant_economic_role(profile, candidate, *extra_parts)
    description_only_concepts = legacy._infer_concepts(
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    specific_domain_signal = legacy._has_specific_domain_signal(
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    descriptor_bundle = legacy._description_business_descriptor_bundle(description_text)
    business_descriptors = list(descriptor_bundle.get("descriptors") or [])
    value_chain_layers = set(descriptor_bundle.get("value_chain_layers") or set())
    descriptor_families = set(descriptor_bundle.get("descriptor_families") or set())
    merchant_input_evidence = legacy._merchant_input_evidence(description_text)
    if not merchant_input_evidence:
        archetypes.discard("semiconductor_materials_electronics_materials")
        economic_roles.discard("materials_supplier")
    analysis = {
        "profile_tokens": profile_tokens,
        "candidate_concepts": concepts,
        "candidate_roles": roles,
        "candidate_markets": markets,
        "candidate_archetypes": archetypes,
        "candidate_economic_roles": economic_roles,
        "dominant_economic_role": dominant_role,
        "business_descriptors": business_descriptors,
        "value_chain_layers": value_chain_layers,
        "descriptor_families": descriptor_families,
        "merchant_input_evidence": merchant_input_evidence,
        "umbrella_signals": legacy._umbrella_signals(description_text),
    }
    generic_only_probe = dict(analysis)
    generic_only_probe["candidate_concepts"] = description_only_concepts
    analysis["generic_business_model_only"] = legacy._analysis_is_generic_business_model_only(generic_only_probe) and not specific_domain_signal
    analysis["strong_role_evidence"] = bool(
        not analysis["generic_business_model_only"]
        and (len(roles) >= 1 or len(economic_roles) >= 1 or len(business_descriptors) >= 1)
        and (
            len(archetypes) >= 1
            or len(concepts - legacy.GENERIC_CONCEPTS) >= 1
            or len(economic_roles) >= 1
            or len(descriptor_families) >= 1
        )
    )
    analysis["clear_business_descriptor"] = bool(business_descriptors)
    return analysis


def candidate_analysis(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> dict[str, object]:
    from . import scanner_research as legacy

    key = description_analysis_cache_key(profile, candidate, tuple(extra_parts))
    cached = _DESCRIPTION_ANALYSIS_CACHE.get(key)
    if cached is not None:
        return cached
    # Preserve the legacy patch point on cache misses while the compatibility
    # facade remains part of the supported test surface.
    analysis = legacy._build_candidate_analysis(profile, candidate, *extra_parts)
    _DESCRIPTION_ANALYSIS_CACHE[key] = analysis
    return analysis


def prefilter_ai_theme_catalog(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    max_themes: int = 12,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    from . import scanner_research as legacy

    combined_text = " ".join(
        [
            legacy._normalize_text(profile.get("company_name")),
            legacy._normalize_text(profile.get("description")),
            legacy._normalize_text(profile.get("sic_description")),
            legacy._normalize_text(candidate.get("recommendation_reason")),
        ]
    ).lower()
    strong_energy_storage_grounding = any(
        legacy._contains_phrase(combined_text, term)
        for term in (
            "energy storage",
            "battery storage",
            "grid-scale battery",
            "battery-based energy storage",
            "grid services",
            "renewables optimization",
            "grid optimization",
            "power optimization",
            "dispatch",
        )
    )
    candidate_analysis_result = candidate_analysis(profile, candidate)
    strong_role_evidence = bool(candidate_analysis_result.get("strong_role_evidence"))
    grounded_ranked: list[tuple[int, int, dict[str, object]]] = []
    weak_ranked: list[tuple[int, int, dict[str, object]]] = []
    for entry in preprocessed_catalog(catalog):
        fit_details = theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis_result)
        score = int(fit_details["score"])
        direct_bonus = 1 if fit_details.get("direct_role_fit") else 0
        grounded_support = bool(
            fit_details.get("direct_role_fit")
            or fit_details.get("role_overlap")
            or fit_details.get("specific_overlap")
            or fit_details.get("archetype_overlap")
            or fit_details.get("economic_role_overlap")
            or fit_details.get("market_overlap")
            or fit_details.get("indirect_only_fit")
        )
        item = (score, direct_bonus, entry)
        if score >= 3 and grounded_support:
            grounded_ranked.append(item)
        elif score > 0 or grounded_support:
            weak_ranked.append(item)
    grounded_ranked.sort(key=lambda item: (-item[0], -item[1], str(item[2].get("theme_name") or "")))
    weak_ranked.sort(key=lambda item: (-item[0], -item[1], str(item[2].get("theme_name") or "")))

    selected: list[dict[str, object]] = []
    seen_theme_ids: set[int] = set()
    weak_backfill_count = 0
    for _, _, entry in grounded_ranked[:max_themes]:
        theme_id = int(entry["theme_id"])
        if theme_id in seen_theme_ids:
            continue
        selected.append(concise_theme_context(entry))
        seen_theme_ids.add(theme_id)

    weak_backfill_target = min(4 if strong_role_evidence else 6, max_themes)
    if len(selected) < weak_backfill_target and not (strong_energy_storage_grounding and selected):
        for _, _, entry in weak_ranked:
            theme_id = int(entry["theme_id"])
            if theme_id in seen_theme_ids:
                continue
            weak_item = concise_theme_context(entry)
            weak_item["weak_prefilter_backfill"] = True
            selected.append(weak_item)
            seen_theme_ids.add(theme_id)
            weak_backfill_count += 1
            if len(selected) >= weak_backfill_target:
                break

    if not selected:
        for entry in catalog[:max_themes]:
            theme_id = int(entry["theme_id"])
            if theme_id in seen_theme_ids:
                continue
            selected.append(concise_theme_context(entry))
            seen_theme_ids.add(theme_id)

    meta = {
        "full_catalog_theme_count": len(catalog),
        "filtered_theme_count": len(selected),
        "catalog_was_prefiltered": len(selected) < len(catalog),
        "max_themes": max_themes,
        "grounded_theme_count": len(grounded_ranked),
        "weak_backfill_count": weak_backfill_count,
    }
    return selected[:max_themes], meta


def heuristic_research_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    from . import scanner_research as legacy

    scored: list[tuple[int, dict[str, object], dict[str, object]]] = []
    adjacent_scored: list[tuple[int, dict[str, object], dict[str, object]]] = []
    broad_alternatives: list[str] = []
    candidate_analysis_result = candidate_analysis(profile, candidate)
    candidate_concept_set = set(candidate_analysis_result.get("candidate_concepts") or set())
    candidate_role_set = set(candidate_analysis_result.get("candidate_roles") or set())
    candidate_market_set = set(candidate_analysis_result.get("candidate_markets") or set())
    strong_role_evidence = bool(candidate_analysis_result.get("strong_role_evidence"))
    for entry in preprocessed_catalog(catalog):
        theme_concept_set = set(entry.get("_theme_concepts") or set())
        theme_role_set = set(entry.get("_theme_roles") or set())
        theme_market_set = set(entry.get("_theme_markets") or set())
        if (
            not (candidate_role_set & theme_role_set)
            and not ((candidate_concept_set & theme_concept_set) - legacy.GENERIC_CONCEPTS)
            and ((candidate_concept_set & theme_concept_set) & legacy.GENERIC_CONCEPTS)
            and str(entry.get("theme_name") or "") not in broad_alternatives
        ):
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        elif (looks_generic_theme(entry) or is_generic_factor_theme(entry)) and str(entry.get("theme_name") or "") not in broad_alternatives:
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        fit_details = theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis_result)
        score = int(fit_details["score"])
        why = str(fit_details["why"])
        if not why and not fit_details.get("direct_role_fit"):
            if fit_details.get("market_overlap"):
                why = "Indirect end-market adjacency through " + format_signal_names(set(fit_details["market_overlap"]), legacy.END_MARKET_DISPLAY_NAMES)
            elif fit_details.get("specific_overlap"):
                why = "Partial conceptual overlap on " + ", ".join(list(fit_details["specific_overlap"])[:2])
        suggestion_payload = {
            "theme_id": int(entry["theme_id"]),
            "theme_name": str(entry["theme_name"]),
            "category": str(entry["category"]),
            "why_it_might_fit": why,
            "representative_tickers": list(entry.get("representative_tickers") or []),
        }
        suggestion_payload = annotate_suggestion_fit(suggestion_payload, fit_details)
        weak_economic_only_adjacent = bool(
            strong_role_evidence
            and not fit_details.get("direct_role_fit")
            and fit_details.get("indirect_only_fit")
            and not fit_details.get("market_overlap")
            and not fit_details.get("role_overlap")
            and not fit_details.get("archetype_overlap")
            and fit_details.get("economic_role_overlap")
        )
        if (
            not weak_economic_only_adjacent
            and (
                not fit_details.get("direct_role_fit")
                and (
                    fit_details.get("market_overlap")
                    or ((candidate_market_set & theme_market_set) and not (candidate_role_set & theme_role_set))
                    or (((candidate_concept_set & theme_concept_set) - legacy.GENERIC_CONCEPTS) and not (candidate_role_set & theme_role_set))
                )
            )
        ):
            adjacent_scored.append((score, suggestion_payload, fit_details))
        if weak_economic_only_adjacent:
            continue
        if score < 3:
            continue
        scored.append((score, suggestion_payload, fit_details))
    scored.sort(key=lambda item: (-item[0], item[1]["theme_name"]))
    adjacent_scored.sort(key=lambda item: (-item[0], item[1]["theme_name"]))
    strongest_score = scored[0][0] if scored else 0
    score_floor = max(8, strongest_score - 2) if strongest_score else 999
    suggested_existing = [item[1] for item in scored if item[0] >= score_floor][:3]
    if strong_role_evidence:
        suggested_existing = [
            item
            for item in suggested_existing
            if item.get("fit_label") != "broad_fit"
            or not legacy._is_generic_factor_theme(
                {
                    "theme_name": item.get("theme_name"),
                    "category": item.get("category"),
                    "theme_description": item.get("why_it_might_fit"),
                }
            )
        ][:3]
    strongest_details = scored[0][2] if scored else {}
    strongest_direct_role_fit = bool(strongest_details.get("direct_role_fit"))
    strongest_indirect_only_fit = bool(strongest_details.get("indirect_only_fit"))
    secondary_existing = [item[1] for item in scored if item[0] >= max(5, strongest_score - 5)][:2]
    if strongest_indirect_only_fit and secondary_existing:
        suggested_existing = secondary_existing

    possible_similar: list[str] = []
    similar_seed = [item for item in scored if item[2].get("direct_role_fit")] or scored
    if strongest_score >= 8:
        for _, suggestion, fit_details in similar_seed[:3]:
            if not fit_details.get("direct_role_fit") and candidate_role_set:
                continue
            for ticker in suggestion.get("representative_tickers") or []:
                symbol = str(ticker).strip().upper()
                if symbol and symbol != candidate["ticker"] and symbol not in possible_similar:
                    possible_similar.append(symbol)
    possible_similar = possible_similar[:3 if strongest_score >= 12 else 2]

    confidence = "low"
    recommended_action = "watch_only"
    possible_new_theme = None
    possible_new_theme_category = None
    caveats: list[str] = []
    new_theme_label = legacy._candidate_new_theme_label(profile, candidate)
    should_prioritize_new_theme = legacy._should_prioritize_new_theme(
        candidate_roles,
        suggested_existing,
        strongest_score,
        strongest_direct_role_fit,
    )
    if should_prioritize_new_theme and new_theme_label:
        if not suggested_existing and adjacent_scored:
            suggested_existing = [item[1] for item in adjacent_scored[:2]]
        possible_new_theme = new_theme_label
        possible_new_theme_category = legacy._proposed_new_theme_category(profile, candidate, possible_new_theme)
        recommended_action = "consider_new_theme"
        confidence = "medium" if candidate_role_set else "low"
        if suggested_existing:
            caveats.append("Existing governed themes look adjacent rather than direct fits for the company's narrow business role.")
        else:
            caveats.append("No strong existing governed theme match was found from current catalog context.")
    elif suggested_existing and strongest_score >= 10:
        confidence = "high" if strongest_score >= 12 and strongest_direct_role_fit else "medium"
        recommended_action = "add_to_existing_theme_review"
        if strong_role_evidence and new_theme_label and suggested_existing:
            top_existing_name = str(suggested_existing[0].get("theme_name") or "")
            if is_generic_factor_theme(
                {
                    "theme_name": top_existing_name,
                    "category": suggested_existing[0].get("category"),
                    "theme_description": suggested_existing[0].get("why_it_might_fit"),
                }
            ):
                possible_new_theme = new_theme_label
                possible_new_theme_category = legacy._proposed_new_theme_category(profile, candidate, possible_new_theme)
                recommended_action = "consider_new_theme"
                confidence = "medium"
                caveats.append("Generic factor/style themes are less useful than the company's operating-role framing for thematic review.")
    elif new_theme_label:
        confidence = "low"
        possible_new_theme = new_theme_label
        possible_new_theme_category = legacy._proposed_new_theme_category(profile, candidate, possible_new_theme)
        recommended_action = "consider_new_theme"
        caveats.append("No strong existing governed theme match was found from current catalog context.")
    elif candidate["recommendation"] in {"high-persistence uncovered", "review for addition"}:
        caveats.append("No external company profile was available, so the draft is based on internal scanner evidence only.")
    else:
        recommended_action = "reject_for_now"
        caveats.append("Internal evidence is weak and no grounded theme fit was found.")

    if not legacy._normalize_text(profile.get("description")):
        caveats.append("Company description is unavailable or unverified in the current environment.")

    rationale_parts = [
        f"The company {legacy._value_chain_summary(profile, candidate) }.",
        f"Scanner Audit shows {candidate['recommendation']} with persistence_score={candidate['persistence_score']}, observed_days={candidate['observed_days']}, last_10={candidate['observations_last_10d']}, streak={candidate['current_streak']}.",
        "Theme ranking prioritizes the company's actual role in the stack over broad end-market adjacency.",
    ]
    if possible_new_theme:
        role_text = format_signal_names(candidate_role_set, legacy.ROLE_DISPLAY_NAMES) if candidate_role_set else "its apparent role"
        if suggested_existing:
            rationale_parts.append(
                f"The narrow business-role framing points more precisely to {possible_new_theme} than the best governed-theme matches, which remain useful but adjacent."
            )
            rationale_parts.append(
                "Best governed-theme fit: "
                + "; ".join(f"{item['theme_name']} ({item['why_it_might_fit']})" for item in suggested_existing)
            )
        else:
            rationale_parts.append(f"No strong governed-theme match stood out, so a tentative new-theme direction is {possible_new_theme}, which better reflects {role_text}.")
        weaker_alternatives = [name for name in broad_alternatives if name not in {item['theme_name'] for item in suggested_existing}][:2]
        if weaker_alternatives:
            rationale_parts.append("Broader alternatives such as " + ", ".join(weaker_alternatives) + " look weaker because they map more to end-market adjacency than to the company's actual role in the stack.")
        rationale_parts.append(f"A tentative new-theme label is being surfaced because {possible_new_theme} is a more precise description of the company's direct role than the current governed taxonomy.")
    elif suggested_existing and strongest_score >= 10:
        rationale_parts.append(
            "Best governed-theme fit: "
            + "; ".join(f"{item['theme_name']} ({item['why_it_might_fit']})" for item in suggested_existing)
        )
        weaker_alternatives = [item[1]["theme_name"] for item in scored[1:3] if item[0] < strongest_score and item[0] < 10]
        if not weaker_alternatives:
            weaker_alternatives = [name for name in broad_alternatives if name not in {item['theme_name'] for item in suggested_existing}][:2]
        if weaker_alternatives:
            rationale_parts.append("Broader alternatives such as " + ", ".join(weaker_alternatives) + " look weaker because they map more to end-market adjacency than to the company's actual role in the stack.")
    else:
        rationale_parts.append("No strong governed-theme fit was identified from the available profile and taxonomy context.")

    return {
        "ticker": candidate["ticker"],
        "company_name": legacy._normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": legacy._normalize_text(profile.get("description")) or legacy._normalize_text(profile.get("sic_description")) or "No verified company description available.",
        "possible_similar_tickers": possible_similar,
        "suggested_existing_themes": legacy._prioritize_operating_role_suggestions(
            truncate_existing_theme_suggestions(suggested_existing),
            strong_role_evidence=strong_role_evidence,
        ),
        "possible_new_theme": possible_new_theme,
        "possible_new_theme_category": possible_new_theme_category,
        "confidence": confidence,
        "rationale": " ".join(rationale_parts),
        "caveats": caveats,
        "recommended_action": recommended_action,
    }


def direct_theme_native_matches(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    candidate_analysis_result: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    from . import scanner_research as legacy

    analysis = candidate_analysis_result or candidate_analysis(profile, candidate)
    strong_role_evidence = bool(analysis.get("strong_role_evidence"))
    ranked: list[tuple[int, int, dict[str, object]]] = []
    for entry in preprocessed_catalog(catalog):
        fit_details = theme_fit_details(entry, profile, candidate, candidate_analysis=analysis)
        score = int(fit_details.get("score") or 0)
        if score < 3:
            continue
        suggestion = annotate_suggestion_fit(
            {
                "theme_id": int(entry["theme_id"]),
                "theme_name": str(entry["theme_name"]),
                "category": str(entry["category"]),
                "why_it_might_fit": str(fit_details.get("why") or ""),
                "representative_tickers": list(entry.get("representative_tickers") or []),
            },
            fit_details,
        )
        suggestion["_match_score"] = score
        suggestion["_match_source"] = "theme_native"
        ranked.append((score, 1 if fit_details.get("direct_role_fit") else 0, suggestion))

    ranked.sort(key=lambda item: (-item[0], -item[1], str(item[2].get("theme_name") or "")))
    if not ranked:
        return []

    strongest_score = ranked[0][0]
    score_floor = max(8, strongest_score - 2) if strongest_score else 999
    selected = [item[2] for item in ranked if item[0] >= score_floor][:5]
    return truncate_existing_theme_suggestions(
        legacy._prioritize_operating_role_suggestions(
            selected,
            strong_role_evidence=strong_role_evidence,
        )
    )


def description_theme_generation_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    from . import scanner_research as legacy

    draft_start = legacy._now_perf()
    description = legacy._normalize_text(profile.get("description")) or legacy._normalize_text(profile.get("sic_description"))
    candidate_analysis_result = candidate_analysis(profile, candidate)
    business_descriptors = list(candidate_analysis_result.get("business_descriptors") or [])
    value_chain_layers = sorted(candidate_analysis_result.get("value_chain_layers") or [])
    domain_start = legacy._now_perf()
    domain_anchor_value = domain_anchor(profile, candidate)
    domain_ms = legacy._elapsed_ms(domain_start)
    role_start = legacy._now_perf()
    dominant_business_role = dominant_economic_role(profile, candidate)
    role_ms = legacy._elapsed_ms(role_start)
    idea_start = legacy._now_perf()
    candidate_theme_ideas = candidate_theme_ideas_from_description(profile, candidate)
    idea_ms = legacy._elapsed_ms(idea_start)
    matched_theme_candidates: list[dict[str, object]] = []
    best_by_theme_id: dict[int, dict[str, object]] = {}
    match_start = legacy._now_perf()
    for idea in candidate_theme_ideas:
        for entry in preprocessed_catalog(catalog):
            match = theme_match_from_generated_idea(
                idea,
                entry,
                profile,
                candidate,
                domain_anchor=domain_anchor_value,
                dominant_business_role=dominant_business_role,
            )
            theme_id = int(entry["theme_id"])
            if theme_id not in best_by_theme_id or int(match["score"]) > int(best_by_theme_id[theme_id]["score"]):
                best_by_theme_id[theme_id] = match
    match_ms = legacy._elapsed_ms(match_start)
    direct_match_start = legacy._now_perf()
    theme_native_matches = direct_theme_native_matches(
        candidate,
        catalog,
        profile,
        candidate_analysis_result=candidate_analysis_result,
    )
    direct_match_ms = legacy._elapsed_ms(direct_match_start)
    finalize_start = legacy._now_perf()
    ranked_matches = sorted(
        best_by_theme_id.values(),
        key=lambda item: (-int(item["score"]), str(item["theme_entry"].get("theme_name") or "")),
    )
    strong_role_evidence = bool(candidate_analysis_result.get("strong_role_evidence"))
    validation_matches = [legacy._description_match_debug_entry(match) for match in ranked_matches[:8]]
    suggested_existing: list[dict[str, object]] = []
    seen_generic_clusters: set[str] = set()
    for match in ranked_matches:
        entry = match["theme_entry"]
        if strong_role_evidence and is_generic_factor_theme(entry):
            continue
        if not description_generated_match_is_actionable(match):
            continue
        cluster_key = theme_cluster_key(entry)
        if cluster_key.startswith("umbrella:") and cluster_key in seen_generic_clusters:
            continue
        fit_details = dict(match["fit_details"])
        fit_label = fit_label_from_details(fit_details)
        fit_label = str(match.get("fit_label") or fit_label or "broad_fit")
        why = description_match_support_text(match)
        suggestion = annotate_suggestion_fit(
            {
                "theme_id": int(entry["theme_id"]),
                "theme_name": str(entry["theme_name"]),
                "category": str(entry["category"]),
                "why_it_might_fit": why,
                "representative_tickers": list(entry.get("representative_tickers") or []),
            },
            fit_details,
        )
        suggestion["fit_label"] = fit_label
        suggestion["_match_score"] = int(match["score"])
        suggested_existing.append(suggestion)
        matched_theme_candidates.append(
            {
                "idea": match["idea"],
                "theme_name": suggestion["theme_name"],
                "score": int(match["score"]),
                "fit_label": suggestion["fit_label"],
            }
        )
        seen_generic_clusters.add(cluster_key)
        if len(suggested_existing) >= 3:
            break
    theme_native_debug = [
        {
            "theme_name": str(item.get("theme_name") or ""),
            "score": int(item.get("_match_score") or 0),
            "fit_label": str(item.get("fit_label") or "broad_fit"),
            "source": "theme_native",
        }
        for item in list(theme_native_matches or [])
    ]
    merged_suggestions_by_id = {int(item["theme_id"]): item for item in list(suggested_existing or [])}
    for suggestion in list(theme_native_matches or []):
        theme_id = int(suggestion["theme_id"])
        existing = merged_suggestions_by_id.get(theme_id)
        if existing is None or int(suggestion.get("_match_score") or 0) > int(existing.get("_match_score") or 0):
            merged_suggestions_by_id[theme_id] = suggestion
    suggested_existing = sorted(
        merged_suggestions_by_id.values(),
        key=lambda item: (-int(item.get("_match_score") or 0), str(item.get("theme_name") or "")),
    )
    suggested_existing = legacy._prioritize_operating_role_suggestions(
        suggested_existing,
        strong_role_evidence=strong_role_evidence,
    )
    strongest_unmatched_idea = None
    matched_ideas = {
        item["idea"]
        for item in matched_theme_candidates
        if int(item["score"]) >= 15 and str(item.get("fit_label") or "") == "direct_fit"
    }
    for idea in candidate_theme_ideas:
        if idea not in matched_ideas:
            strongest_unmatched_idea = idea
            break
    if business_descriptors:
        matched_descriptor_ideas = {item["idea"] for item in matched_theme_candidates if int(item["score"]) >= 12}
        for descriptor in business_descriptors:
            if descriptor not in matched_descriptor_ideas:
                strongest_unmatched_idea = descriptor
                break
    possible_new_theme = None
    possible_new_theme_category = None
    recommended_action = "watch_only"
    confidence = "low"
    descriptor_confidence = strong_role_evidence or bool(business_descriptors)
    top_existing_fit = suggested_existing[0] if suggested_existing else {}
    top_existing_is_direct = str(top_existing_fit.get("fit_label") or "") == "direct_fit"
    top_existing_is_adjacent = str(top_existing_fit.get("fit_label") or "") == "adjacent_fit"
    if strongest_unmatched_idea and len(candidate_theme_ideas) >= 1 and descriptor_confidence and not top_existing_is_direct:
        possible_new_theme = strongest_unmatched_idea
        possible_new_theme_category = legacy._proposed_new_theme_category(
            profile,
            candidate,
            possible_new_theme,
            business_descriptors=business_descriptors,
            value_chain_layers=value_chain_layers,
        )
        recommended_action = "consider_new_theme"
        confidence = "medium" if top_existing_is_adjacent or strong_role_evidence else "low"
    elif suggested_existing:
        recommended_action = "add_to_existing_theme_review" if top_existing_is_direct else "watch_only"
        confidence = "medium" if top_existing_is_direct else "low"
    caveats: list[str] = []
    if not description:
        caveats.append("Company description is unavailable or unverified in the current environment.")
    if suggested_existing and not top_existing_is_direct:
        caveats.append("Generated governed-theme matches look adjacent rather than direct fits for the company's dominant role.")
    if not suggested_existing and not possible_new_theme:
        caveats.append("Description-first generation did not find a strong operating-role theme cluster.")
    if business_descriptors and not suggested_existing and possible_new_theme:
        caveats.append("The business description supports a coherent theme bucket, but current governed coverage remains weak.")
    rationale_parts = [
        f"The company {value_chain_summary(profile, candidate) }.",
        f"Description-first generation anchored on domain `{domain_anchor_value}` and dominant role `{dominant_business_role or 'unclear'}`.",
    ]
    if business_descriptors:
        descriptor_summary = ", ".join(business_descriptors[:3])
        rationale_parts.append(f"Plain-language business descriptors: {descriptor_summary}.")
    if value_chain_layers:
        rationale_parts.append("Detected business layer: " + ", ".join(value_chain_layers[:3]).replace("_", " ") + ".")
    if suggested_existing:
        rationale_parts.append(
            "Best governed-theme matches: "
            + "; ".join(f"{item['theme_name']} [{item['fit_label']}: {item['why_it_might_fit']}]" for item in suggested_existing)
        )
        if theme_native_debug:
            rationale_parts.append(
                "Theme-native retrieval also compared the company description directly against governed theme identities from the database, so existing-theme suggestions do not depend only on generated phrase labels."
            )
    if possible_new_theme:
        if suggested_existing:
            rationale_parts.append(
                f"The strongest unmatched role idea is {possible_new_theme}, which remains a tentative new-theme suggestion because the best governed matches are still adjacent rather than direct."
            )
        else:
            rationale_parts.append(f"The strongest unmatched role idea is {possible_new_theme}, which is being preserved as a tentative new-theme suggestion.")
    elif not suggested_existing:
        rationale_parts.append("No strong governed-theme or reusable new-theme idea was identified from the description.")
    draft = {
        "ticker": candidate["ticker"],
        "company_name": legacy._normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": description or "No verified company description available.",
        "possible_similar_tickers": [],
        "suggested_existing_themes": suggested_existing[:3],
        "possible_new_theme": possible_new_theme,
        "possible_new_theme_category": possible_new_theme_category,
        "confidence": confidence,
        "rationale": " ".join(rationale_parts),
        "caveats": caveats,
        "recommended_action": recommended_action,
        "theme_generation_strategy": "description_theme_generation",
        "domain_anchor": domain_anchor_value,
        "dominant_business_role": dominant_business_role or "unclear",
        "candidate_theme_ideas": candidate_theme_ideas,
        "business_descriptors": business_descriptors,
        "matched_theme_candidates": matched_theme_candidates[:5],
    }
    if possible_new_theme:
        new_theme_status = "kept_tentative" if suggested_existing else "selected_no_actionable_governed_match"
        new_theme_reason = (
            "Best governed matches remained adjacent rather than direct."
            if suggested_existing
            else "No actionable governed-theme match cleared the description-first gate."
        )
    elif strongest_unmatched_idea and top_existing_is_direct:
        new_theme_status = "suppressed_by_direct_governed_match"
        new_theme_reason = "A direct governed-theme fit won, so the narrower idea stayed suppressed."
    elif strongest_unmatched_idea:
        new_theme_status = "suppressed_without_strong_role_evidence"
        new_theme_reason = "An unmatched idea existed but the description did not provide enough business-layer evidence to elevate it."
    else:
        new_theme_status = "none_generated"
        new_theme_reason = "No unmatched generated idea stood out after ranking."
    draft["validation_debug"] = {
        "strategy": "description_theme_generation",
        "domain_anchor": domain_anchor_value,
        "dominant_business_role": dominant_business_role or "unclear",
        "strong_role_evidence": strong_role_evidence,
        "business_descriptors": business_descriptors[:5],
        "value_chain_layers": value_chain_layers,
        "generated_theme_ideas": candidate_theme_ideas[:5],
        "evaluated_matches": validation_matches,
        "theme_native_retrieval_top_hits": theme_native_debug[:5],
        "possible_new_theme_decision": {
            "candidate": strongest_unmatched_idea,
            "selected": possible_new_theme,
            "selected_category": possible_new_theme_category,
            "status": new_theme_status,
            "reason": new_theme_reason,
        },
    }
    draft["research_timing_summary"] = {
        "strategy": "description_theme_generation",
        "domain_anchor_ms": domain_ms,
        "dominant_business_role_ms": role_ms,
        "candidate_theme_ideas_ms": idea_ms,
        "governed_theme_matching_ms": match_ms,
        "theme_native_retrieval_ms": direct_match_ms,
        "finalize_ms": legacy._elapsed_ms(finalize_start),
        "strategy_total_ms": legacy._elapsed_ms(draft_start),
    }
    return draft
