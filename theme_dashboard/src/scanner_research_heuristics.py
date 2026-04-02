from __future__ import annotations

"""Shared heuristic primitives for scanner research.

This module owns the reusable side-effect-light reasoning helpers used by the
analysis and merge stages. Some primitives now live here directly, while others
still delegate to the legacy module so compatibility patch points stay intact
during the staged cleanup.
"""


def fit_label_from_details(fit_details: dict[str, object]) -> str:
    from . import scanner_research as legacy

    score = int(fit_details.get("score") or 0)
    direct_role_fit = bool(fit_details.get("direct_role_fit"))
    indirect_only_fit = bool(fit_details.get("indirect_only_fit"))
    economic_role_overlap = bool(fit_details.get("economic_role_overlap"))
    weak_generic_business_model_overlap = bool(fit_details.get("generic_business_model_only")) and not bool(fit_details.get("specific_overlap")) and set(fit_details.get("role_overlap") or []) <= legacy.WEAK_ROLE_SIGNALS and set(fit_details.get("economic_role_overlap") or []) <= legacy.WEAK_ECONOMIC_ROLE_SIGNALS
    archetype_relation = str(fit_details.get("archetype_relation") or "")
    specific_overlap = bool(fit_details.get("specific_overlap"))
    market_overlap = bool(fit_details.get("market_overlap"))
    generic_overlap = bool(fit_details.get("generic_overlap"))
    broad_economic_only_direct = economic_role_overlap and not direct_role_fit and archetype_relation == "direct" and not specific_overlap
    if weak_generic_business_model_overlap:
        direct_role_fit = False
        economic_role_overlap = False
        broad_economic_only_direct = False
    if direct_role_fit and (specific_overlap or economic_role_overlap or archetype_relation == "direct" or score >= 18):
        return "direct_fit"
    if economic_role_overlap and not broad_economic_only_direct and archetype_relation == "direct" and not indirect_only_fit and (specific_overlap or score >= 18):
        return "direct_fit"
    if (
        bool(fit_details.get("indirect_only_fit"))
        or market_overlap
        or specific_overlap
        or economic_role_overlap
        or archetype_relation == "direct"
        or archetype_relation == "adjacent"
        or generic_overlap
    ):
        return "adjacent_fit"
    return "broad_fit"


def annotate_suggestion_fit(
    suggestion: dict[str, object],
    fit_details: dict[str, object],
) -> dict[str, object]:
    annotated = dict(suggestion)
    annotated["fit_label"] = fit_label_from_details(fit_details)
    return annotated


def truncate_existing_theme_suggestions(suggestions: list[dict[str, object]], *, limit: int = 3) -> list[dict[str, object]]:
    return list(suggestions or [])[:limit]


def theme_concepts(theme_entry: dict[str, object]) -> set[str]:
    from . import scanner_research as legacy

    return legacy._theme_concepts(theme_entry)


def candidate_roles(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    from . import scanner_research as legacy

    return legacy._candidate_roles(profile, candidate, *extra_parts)


def candidate_end_markets(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    from . import scanner_research as legacy

    return legacy._candidate_end_markets(profile, candidate, *extra_parts)


def theme_roles(theme_entry: dict[str, object]) -> set[str]:
    from . import scanner_research as legacy

    return legacy._theme_roles(theme_entry)


def theme_end_markets(theme_entry: dict[str, object]) -> set[str]:
    from . import scanner_research as legacy

    return legacy._theme_end_markets(theme_entry)


def candidate_archetypes(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    from . import scanner_research as legacy

    return legacy._candidate_archetypes(profile, candidate, *extra_parts)


def theme_archetypes(theme_entry: dict[str, object]) -> set[str]:
    from . import scanner_research as legacy

    return legacy._theme_archetypes(theme_entry)


def candidate_economic_roles(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    from . import scanner_research as legacy

    return legacy._candidate_economic_roles(profile, candidate, *extra_parts)


def theme_economic_roles(theme_entry: dict[str, object]) -> set[str]:
    from . import scanner_research as legacy

    return legacy._theme_economic_roles(theme_entry)


def dominant_economic_role(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    from . import scanner_research as legacy

    return legacy._dominant_economic_role(profile, candidate, *extra_parts)


def domain_anchor(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    from . import scanner_research as legacy

    return legacy._domain_anchor(profile, candidate, *extra_parts)


def candidate_theme_ideas_from_description(profile: dict[str, object], candidate: dict[str, object]) -> list[str]:
    from . import scanner_research as legacy

    return legacy._candidate_theme_ideas_from_description(profile, candidate)


def theme_cluster_key(theme_entry: dict[str, object]) -> str:
    from . import scanner_research as legacy

    return legacy._theme_cluster_key(theme_entry)


def theme_match_from_generated_idea(
    idea: str,
    theme_entry: dict[str, object],
    profile: dict[str, object],
    candidate: dict[str, object],
    *,
    domain_anchor: str,
    dominant_business_role: str,
) -> dict[str, object]:
    from . import scanner_research as legacy

    return legacy._theme_match_from_generated_idea(
        idea,
        theme_entry,
        profile,
        candidate,
        domain_anchor=domain_anchor,
        dominant_business_role=dominant_business_role,
    )


def description_generated_match_is_actionable(match: dict[str, object]) -> bool:
    from . import scanner_research as legacy

    return legacy._description_generated_match_is_actionable(match)


def description_match_support_text(match: dict[str, object]) -> str:
    from . import scanner_research as legacy

    return legacy._description_match_support_text(match)


def looks_generic_theme(theme_entry: dict[str, object]) -> bool:
    from . import scanner_research as legacy

    return legacy._looks_generic_theme(theme_entry)


def is_generic_factor_theme(theme_entry: dict[str, object]) -> bool:
    from . import scanner_research as legacy

    return legacy._is_generic_factor_theme(theme_entry)


def has_strong_role_evidence(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> bool:
    from . import scanner_research as legacy

    return legacy._has_strong_role_evidence(profile, candidate, *extra_parts)


def candidate_concepts(profile: dict[str, object], candidate: dict[str, object]) -> set[str]:
    from . import scanner_research as legacy

    return legacy._candidate_concepts(profile, candidate)


def format_signal_names(values: set[str], display_map: dict[str, str], limit: int = 2) -> str:
    from . import scanner_research as legacy

    return legacy._format_signal_names(values, display_map, limit=limit)


def theme_fit_details(
    theme_entry: dict[str, object],
    profile: dict[str, object],
    candidate: dict[str, object],
    *,
    candidate_analysis: dict[str, object] | None = None,
) -> dict[str, object]:
    from . import scanner_research as legacy

    return legacy._theme_fit_details(
        theme_entry,
        profile,
        candidate,
        candidate_analysis=candidate_analysis,
    )


def theme_fit_score(theme_entry: dict[str, object], profile: dict[str, object], candidate: dict[str, object]) -> tuple[int, str]:
    details = theme_fit_details(theme_entry, profile, candidate)
    return int(details["score"]), str(details["why"])


def value_chain_summary(profile: dict[str, object], candidate: dict[str, object]) -> str:
    from . import scanner_research as legacy

    return legacy._value_chain_summary(profile, candidate)
