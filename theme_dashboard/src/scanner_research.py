from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime

import requests

from .ai_proposals import sanitize_context
from .config import AI_MODEL, OPENAI_API_KEY_ENV, openai_api_key
from .provider_live import LiveProvider
from .scanner_audit import scanner_candidate_summary


RESEARCH_DRAFT_SYSTEM_PROMPT = """You are an advisory equity theme research assistant.
Return STRICT JSON with fields:
- ticker
- company_name
- short_company_description
- possible_similar_tickers (array of strings)
- suggested_existing_themes (array of objects with theme_id, theme_name, category, why_it_might_fit, fit_label)
- possible_new_theme
- confidence
- rationale
- caveats (array of strings)
- recommended_action

Rules:
- Advisory only. Never imply governed theme membership should be auto-applied.
- Ground suggested_existing_themes only in the provided governed theme catalog context.
- If evidence is weak, say so explicitly.
- Always provide a non-empty rationale of 2-4 concise sentences.
- The rationale must explain the company's business role/value-chain position and whether the best governed-theme fit is direct, adjacent, or weak.
- If no strong governed-theme fit exists, explicitly say that and still provide a useful rationale.
- If the company's role is more precise than the current governed themes, provide a concise possible_new_theme label when practical.
- Explicitly compare the best existing governed-theme fit versus the best narrow business-role / possible_new_theme label.
- Prefer the more precise classification over broad adjacency. If existing themes are only adjacent, keep them secondary and choose consider_new_theme when appropriate.
- Prefer concise, economically meaningful new-theme labels such as Optical Networking, Data Center Optics, Optical Interconnects, AI Fiber Optics, Semiconductor Materials, Semiconductor Substrates, or Compound Semiconductor Materials.
- Avoid vague labels like Advanced Infrastructure, Next-Gen Connectivity, Future Technology Platforms, or High Growth Materials.
- Keep outputs compact and operational. Do not leave required fields blank.
- recommended_action must be one of: add_to_existing_theme_review, consider_new_theme, watch_only, reject_for_now.
"""

STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "group",
    "holdings",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "company",
    "technologies",
    "technology",
    "systems",
    "common",
    "stock",
    "class",
    "global",
    "international",
    "services",
}

CONCEPT_KEYWORDS: dict[str, set[str]] = {
    "cybersecurity": {"cybersecurity", "cyber", "security", "identity", "endpoint", "threat", "zero", "trust"},
    "cloud": {"cloud", "saas", "platform", "infrastructure", "observability", "devops"},
    "ai_compute": {"ai", "artificial", "gpu", "accelerated", "inference", "training", "compute", "datacenter", "data center"},
    "semiconductor": {"semiconductor", "chip", "chips", "fab", "wafer", "silicon", "processor"},
    "data_analytics": {"analytics", "data", "integration", "decision", "intelligence", "ontology"},
    "defense": {"defense", "military", "government", "mission", "battlefield", "aerospace"},
    "robotics": {"robotics", "automation", "autonomous", "industrial", "factory"},
    "space": {"space", "satellite", "launch", "orbital", "rocket"},
    "payments": {"payments", "payment", "merchant", "fintech", "card", "transaction"},
    "biotech": {"biotech", "therapeutic", "drug", "pharma", "clinical", "biology"},
    "energy": {"energy", "solar", "battery", "nuclear", "power", "grid"},
    "software": {"software", "application", "enterprise", "workflow"},
}

GENERIC_CONCEPTS = {"cloud", "software"}

ROLE_KEYWORDS: dict[str, set[str]] = {
    "optical_networking": {
        "optical",
        "optics",
        "transceiver",
        "transceivers",
        "fiber",
        "photonic",
        "photonics",
        "interconnect",
        "coherent",
        "broadband",
        "networking",
        "network",
    },
    "semiconductor_materials": {
        "substrate",
        "substates",
        "substrates",
        "wafer",
        "wafers",
        "epitaxy",
        "ingot",
        "gallium arsenide",
        "indium phosphide",
        "compound semiconductor",
        "compound semiconductors",
        "semiconductor materials",
        "semiconductor substrate",
    },
    "semiconductor_equipment": {
        "lithography",
        "etch",
        "deposition",
        "metrology",
        "inspection",
        "fab equipment",
        "packaging equipment",
        "process equipment",
    },
    "chip_designer": {"fabless", "asic", "gpu", "cpu", "processor", "chip designer"},
    "server_systems": {"server", "servers", "rack-scale", "rack", "system", "systems", "accelerated computing"},
    "power_generation": {"utility", "utilities", "generation", "electricity", "power plant", "nuclear plant", "renewable generation"},
    "power_equipment": {"transformer", "inverter", "switchgear", "power conversion", "grid equipment", "electrical equipment"},
    "software_tooling": {"software", "platform", "tooling", "workflow", "analytics", "integration", "observability"},
    "robotics_automation": {"robotics", "automation", "autonomous", "factory", "industrial automation"},
    "devices_endpoints": {"device", "devices", "endpoint", "consumer electronics", "handset"},
    "healthcare_equipment": {"imaging system", "diagnostic equipment", "medical device", "medical equipment", "surgical system"},
}

END_MARKET_KEYWORDS: dict[str, set[str]] = {
    "ai": {"ai", "artificial intelligence", "inference", "training", "accelerated"},
    "data_center": {"data center", "data-center", "datacenter", "hyperscale", "server", "rack-scale"},
    "telecom": {"telecom", "telecommunications", "carrier", "broadband", "network"},
    "semiconductor_market": {"semiconductor", "chip", "fab", "wafer"},
    "healthcare": {"healthcare", "medical", "clinical", "hospital"},
    "industrial": {"industrial", "factory", "manufacturing"},
    "defense_market": {"defense", "military", "government", "mission"},
    "energy_market": {"energy", "power", "grid", "utility"},
}

THEME_NEW_LABELS = {
    "cybersecurity": "Cybersecurity",
    "ai_compute": "AI Infrastructure",
    "data_analytics": "Data Analytics Platforms",
    "defense": "Defense Tech",
    "robotics": "Robotics & Automation",
    "space": "Space Infrastructure",
    "payments": "Digital Payments",
    "biotech": "Biotech Platforms",
    "energy": "Energy Transition",
    "semiconductor": "Semiconductors",
}

ROLE_NEW_LABELS = {
    "optical_networking": "Optical Networking",
    "semiconductor_materials": "Semiconductor Materials",
    "semiconductor_equipment": "Semiconductor Equipment",
    "chip_designer": "Chip Designers",
    "server_systems": "AI Server Systems",
    "power_generation": "Power Generation",
    "power_equipment": "Power Equipment",
    "software_tooling": "Enterprise Software Tooling",
    "robotics_automation": "Robotics & Automation",
    "devices_endpoints": "Connected Devices",
    "healthcare_equipment": "Healthcare Equipment",
}

ROLE_DISPLAY_NAMES = {
    "optical_networking": "optical networking and interconnect",
    "semiconductor_materials": "semiconductor materials and substrates",
    "semiconductor_equipment": "semiconductor equipment",
    "chip_designer": "chip design",
    "server_systems": "server systems",
    "power_generation": "power generation",
    "power_equipment": "power equipment",
    "software_tooling": "software tooling",
    "robotics_automation": "robotics and automation",
    "devices_endpoints": "devices and endpoints",
    "healthcare_equipment": "healthcare equipment",
}

END_MARKET_DISPLAY_NAMES = {
    "ai": "AI",
    "data_center": "data centers",
    "telecom": "telecom",
    "semiconductor_market": "semiconductors",
    "healthcare": "healthcare",
    "industrial": "industrial markets",
    "defense_market": "defense/government",
    "energy_market": "energy and power",
}

ROLE_FAMILY = {
    "optical_networking": "communications_hardware",
    "semiconductor_materials": "semiconductor_supply_chain",
    "semiconductor_equipment": "semiconductor_supply_chain",
    "chip_designer": "semiconductor_products",
    "server_systems": "compute_hardware",
    "power_generation": "energy",
    "power_equipment": "energy",
    "software_tooling": "software",
    "robotics_automation": "industrial_automation",
    "devices_endpoints": "devices",
    "healthcare_equipment": "healthcare_devices",
}

ARCHETYPE_KEYWORDS: dict[str, set[str]] = {
    "fintech_payments_lending": {
        "payments",
        "payment",
        "merchant",
        "checkout",
        "transaction",
        "transactions",
        "fintech",
        "lending",
        "loan",
        "loans",
        "installment",
        "installments",
        "buy now pay later",
        "bnpl",
        "consumer finance",
    },
    "digital_identity_security": {
        "identity",
        "verification",
        "verify",
        "verified",
        "credential",
        "credentials",
        "biometric",
        "biometrics",
        "authentication",
        "identity platform",
        "access control",
        "secure identity",
    },
    "semiconductor_materials_electronics_materials": {
        "semiconductor materials",
        "compound semiconductor",
        "compound semiconductors",
        "electronics materials",
        "electronic materials",
        "substrate",
        "substrates",
        "wafer",
        "wafers",
        "epitaxy",
        "ingot",
        "gallium arsenide",
        "indium phosphide",
        "packaging materials",
        "specialty materials",
    },
    "ai_infrastructure_data_centers": {
        "data center",
        "data centers",
        "data-center",
        "datacenter",
        "datacenters",
        "hyperscale",
        "ai infrastructure",
        "gpu cluster",
        "compute cluster",
        "data-center capacity",
        "colocation",
        "server infrastructure",
    },
    "aerospace_defense_space_systems": {
        "defense",
        "mission",
        "satellite",
        "payload",
        "space systems",
        "defense systems",
        "aerospace",
        "government programs",
        "orbital",
    },
    "networking_interconnect": {
        "optical",
        "optics",
        "transceiver",
        "transceivers",
        "fiber",
        "interconnect",
        "coherent",
        "networking",
        "communications infrastructure",
    },
    "software_devops_cloud": {
        "software",
        "cloud",
        "devops",
        "observability",
        "saas",
        "platform",
        "workflow",
        "enterprise software",
        "tooling",
    },
    "healthcare_devices_services": {
        "animal health",
        "cro",
        "cmo",
        "dental",
        "medical",
        "healthcare",
        "clinical",
        "diagnostic",
        "hospital",
        "medical device",
    },
    "industrial_materials_chemicals": {
        "chemicals",
        "chemical",
        "coatings",
        "paints",
        "specialty chemicals",
        "materials",
        "industrial materials",
        "specialty materials",
    },
}

ARCHETYPE_DISPLAY_NAMES = {
    "fintech_payments_lending": "fintech/payments/lending",
    "digital_identity_security": "digital identity/security",
    "semiconductor_materials_electronics_materials": "semiconductor and electronics materials",
    "ai_infrastructure_data_centers": "AI infrastructure/data centers",
    "aerospace_defense_space_systems": "aerospace/defense/space systems",
    "networking_interconnect": "networking/interconnect",
    "software_devops_cloud": "software/devops/cloud",
    "healthcare_devices_services": "healthcare devices/services",
    "industrial_materials_chemicals": "industrial materials/chemicals",
}

ARCHETYPE_FAMILY = {
    "fintech_payments_lending": "finance",
    "digital_identity_security": "identity_security",
    "semiconductor_materials_electronics_materials": "semiconductor_materials",
    "ai_infrastructure_data_centers": "ai_infrastructure",
    "aerospace_defense_space_systems": "defense_space",
    "networking_interconnect": "communications_hardware",
    "software_devops_cloud": "software",
    "healthcare_devices_services": "healthcare",
    "industrial_materials_chemicals": "materials",
}

ARCHETYPE_ADJACENCY = {
    ("ai_infrastructure", "communications_hardware"),
    ("ai_infrastructure", "semiconductor_materials"),
    ("communications_hardware", "semiconductor_materials"),
    ("finance", "software"),
    ("identity_security", "software"),
    ("materials", "semiconductor_materials"),
}

ECONOMIC_ROLE_KEYWORDS: dict[str, set[str]] = {
    "component_supplier": {
        "component",
        "components",
        "module",
        "modules",
        "engine",
        "engines",
        "transceiver",
        "transceivers",
        "interposer",
        "optical engine",
        "light source",
        "supplier",
        "supplies",
    },
    "materials_supplier": {
        "materials",
        "substrate",
        "substrates",
        "wafer",
        "wafers",
        "compound semiconductor",
        "electronic materials",
        "electronics materials",
        "specialty chemicals",
        "packaging materials",
    },
    "end_platform_operator": {
        "operates",
        "operator",
        "platform operator",
        "marketplace",
        "network operator",
        "runs",
        "operates and builds",
        "campus",
        "campuses",
        "capacity",
    },
    "infrastructure_operator": {
        "builds and operates",
        "owns and operates",
        "data-center capacity",
        "hyperscale campus",
        "server campus",
        "infrastructure operator",
        "colocation",
    },
    "software_service_provider": {
        "software",
        "platform",
        "service",
        "services",
        "saas",
        "workflow",
        "observability",
        "analytics platform",
    },
    "financial_platform": {
        "payments platform",
        "digital checkout",
        "consumer lending",
        "merchant",
        "fintech platform",
        "installment payments",
        "financial platform",
    },
    "identity_verification_platform": {
        "identity verification",
        "biometric",
        "authentication",
        "credential",
        "member authentication",
        "identity platform",
    },
    "defense_systems_manufacturer": {
        "defense systems",
        "mission systems",
        "satellite systems",
        "space systems",
        "payload",
        "aircraft systems",
        "defense manufacturer",
        "aerospace systems",
        "mission hardware",
        "aerospace and government",
    },
}

ECONOMIC_ROLE_DISPLAY_NAMES = {
    "component_supplier": "component supplier",
    "materials_supplier": "materials supplier",
    "end_platform_operator": "end platform/operator",
    "infrastructure_operator": "infrastructure/operator",
    "software_service_provider": "software/service provider",
    "financial_platform": "financial platform",
    "identity_verification_platform": "identity verification platform",
    "defense_systems_manufacturer": "defense/space systems manufacturer",
}

ROLE_ALIGNMENT = {
    "optical_networking": {"component_supplier"},
    "semiconductor_materials": {"materials_supplier"},
    "semiconductor_equipment": {"component_supplier"},
    "chip_designer": {"component_supplier"},
    "server_systems": {"end_platform_operator", "infrastructure_operator", "component_supplier"},
    "software_tooling": {"software_service_provider"},
    "power_generation": {"infrastructure_operator"},
    "power_equipment": {"component_supplier"},
    "healthcare_equipment": {"component_supplier"},
}

ARCHETYPE_ALIGNMENT = {
    "fintech_payments_lending": {"financial_platform"},
    "digital_identity_security": {"identity_verification_platform", "software_service_provider"},
    "semiconductor_materials_electronics_materials": {"materials_supplier", "component_supplier"},
    "ai_infrastructure_data_centers": {"infrastructure_operator", "end_platform_operator"},
    "networking_interconnect": {"component_supplier"},
    "software_devops_cloud": {"software_service_provider"},
    "healthcare_devices_services": {"component_supplier", "software_service_provider"},
    "aerospace_defense_space_systems": {"defense_systems_manufacturer"},
}

_PROFILE_CACHE: dict[str, dict[str, object]] = {}
_DESCRIPTION_ANALYSIS_CACHE: dict[tuple[object, ...], dict[str, object]] = {}
_THEME_PREPROCESS_CACHE: dict[tuple[object, ...], dict[str, object]] = {}
VAGUE_NEW_THEME_LABEL_TOKENS = {
    "advanced",
    "business services",
    "future",
    "next-gen",
    "next generation",
    "platform",
    "platforms",
    "services",
    "solutions",
    "technology services",
    "infrastructure services",
    "high growth",
}

GENERIC_FACTOR_THEME_TOKENS = {
    "growth",
    "high growth",
    "momentum",
    "quality",
    "value",
    "large cap",
    "small cap",
    "mid cap",
    "factor",
    "style",
    "leaders",
}

RESEARCH_STRATEGIES = {"legacy_direct_match", "description_theme_generation"}

DOMAIN_ANCHOR_LABELS = {
    "fintech_payments_lending": "fintech",
    "digital_identity_security": "digital identity/security",
    "semiconductor_materials_electronics_materials": "semiconductor/electronics",
    "ai_infrastructure_data_centers": "AI infrastructure/data centers",
    "aerospace_defense_space_systems": "aerospace/defense/space",
    "networking_interconnect": "networking/communications",
    "software_devops_cloud": "software/cloud",
    "healthcare_devices_services": "healthcare",
    "industrial_materials_chemicals": "industrials/materials",
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _now_perf() -> float:
    return time.perf_counter()


def _elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 1)


def _normalize_research_strategy(value: object, fallback: str = "legacy_direct_match") -> str:
    normalized = _normalize_text(value) or fallback
    return normalized if normalized in RESEARCH_STRATEGIES else fallback


def _compact_error_reason(exc: Exception) -> str:
    text = _normalize_text(exc)
    if not text:
        return "Research generation error."
    return _sanitize_error_text(text, limit=140)


def _sanitize_error_text(text: object, *, limit: int = 200) -> str:
    text = _normalize_text(text)
    if not text:
        return ""
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"(?i)bearer\s+[a-z0-9_\-\.]+", "bearer [redacted]", text)
    text = re.sub(r"(?i)api[_ -]?key[=:]\s*[^ ,;]+", "api_key=[redacted]", text)
    return text[:limit]


def _normalize_optional_theme_label(value: object) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    normalized = text.lower()
    empty_markers = {
        "none",
        "none suggested",
        "no suggestion",
        "no strong fit",
        "no strong existing fit",
        "n/a",
        "na",
        "null",
    }
    if normalized in empty_markers:
        return None
    if any(token in normalized for token in VAGUE_NEW_THEME_LABEL_TOKENS):
        return None
    return text


def _fit_label_from_details(fit_details: dict[str, object]) -> str:
    score = int(fit_details.get("score") or 0)
    direct_role_fit = bool(fit_details.get("direct_role_fit"))
    indirect_only_fit = bool(fit_details.get("indirect_only_fit"))
    economic_role_overlap = bool(fit_details.get("economic_role_overlap"))
    archetype_relation = str(fit_details.get("archetype_relation") or "")
    specific_overlap = bool(fit_details.get("specific_overlap"))
    market_overlap = bool(fit_details.get("market_overlap"))
    generic_overlap = bool(fit_details.get("generic_overlap"))
    if direct_role_fit and (specific_overlap or economic_role_overlap or archetype_relation == "direct" or score >= 18):
        return "direct_fit"
    if economic_role_overlap and archetype_relation == "direct" and not indirect_only_fit and (specific_overlap or score >= 18):
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


def _annotate_suggestion_fit(
    suggestion: dict[str, object],
    fit_details: dict[str, object],
) -> dict[str, object]:
    annotated = dict(suggestion)
    annotated["fit_label"] = _fit_label_from_details(fit_details)
    return annotated


def _truncate_existing_theme_suggestions(suggestions: list[dict[str, object]], *, limit: int = 3) -> list[dict[str, object]]:
    return list(suggestions or [])[:limit]


def _extract_openai_error_details(exc: Exception) -> dict[str, object]:
    details: dict[str, object] = {
        "error_class": exc.__class__.__name__,
        "model": AI_MODEL,
    }
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            details["status_code"] = int(status_code)
        try:
            body = response.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            error_body = body.get("error")
            if isinstance(error_body, dict):
                error_type = _sanitize_error_text(error_body.get("type"), limit=80)
                error_message = _sanitize_error_text(error_body.get("message"), limit=200)
                if error_type:
                    details["error_type"] = error_type
                if error_message:
                    details["error_message"] = error_message
        if "error_message" not in details:
            response_text = _sanitize_error_text(getattr(response, "text", ""), limit=200)
            if response_text:
                details["error_message"] = response_text
    if "error_message" not in details:
        details["error_message"] = _compact_error_reason(exc)
    return details


def _format_openai_error_summary(details: dict[str, object]) -> str:
    status = details.get("status_code")
    error_type = _normalize_text(details.get("error_type"))
    message = _sanitize_error_text(details.get("error_message"), limit=140)
    model = _normalize_text(details.get("model")) or AI_MODEL
    parts = ["OpenAI request failed"]
    if status:
        parts.append(f"HTTP {status}")
    if error_type:
        parts.append(error_type)
    summary = ": ".join([parts[0], " ".join(parts[1:])]) if len(parts) > 1 else parts[0]
    if message:
        summary += f" - {message}"
    if model:
        summary += f" (model: {model})"
    return summary[:220]


def _tokenize(*parts: object) -> set[str]:
    tokens: set[str] = set()
    for part in parts:
        for token in re.findall(r"[a-z0-9]+", _normalize_text(part).lower()):
            if len(token) >= 3 and token not in STOPWORDS:
                tokens.add(token)
    return tokens


def _contains_phrase(text: str, keyword: str) -> bool:
    if " " in keyword:
        return keyword in text
    return bool(re.search(rf"\b{re.escape(keyword)}\b", text))


def _infer_concepts(*parts: object) -> set[str]:
    text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    concepts: set[str] = set()
    for concept, keywords in CONCEPT_KEYWORDS.items():
        if any(_contains_phrase(text, keyword) for keyword in keywords):
            concepts.add(concept)
    return concepts


def _infer_signals(keyword_map: dict[str, set[str]], *parts: object) -> set[str]:
    text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    signals: set[str] = set()
    for signal, keywords in keyword_map.items():
        if any(_contains_phrase(text, keyword) for keyword in keywords):
            signals.add(signal)
    return signals


def _count_signal_hits(keyword_map: dict[str, set[str]], *parts: object) -> dict[str, int]:
    text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    counts: dict[str, int] = {}
    for signal, keywords in keyword_map.items():
        count = sum(1 for keyword in keywords if _contains_phrase(text, keyword))
        if count:
            counts[signal] = count
    return counts


def _representative_ticker_role_hints(tickers: list[object]) -> set[str]:
    hints: set[str] = set()
    joined = " ".join(str(value or "").strip().upper() for value in tickers if str(value or "").strip())
    if any(symbol in joined for symbol in ["AAOI", "CIEN", "LITE", "FN", "INFN"]):
        hints.add("optical_networking")
    if any(symbol in joined for symbol in ["AXT", "WOLF", "COHR", "ONTO"]):
        hints.add("semiconductor_materials")
    if any(symbol in joined for symbol in ["ASML", "AMAT", "LRCX", "KLAC", "ONTO"]):
        hints.add("semiconductor_equipment")
    if any(symbol in joined for symbol in ["NVDA", "AMD", "AVGO", "MRVL", "MCHP"]):
        hints.add("chip_designer")
    if any(symbol in joined for symbol in ["SMCI", "DELL", "HPE"]):
        hints.add("server_systems")
    if any(symbol in joined for symbol in ["CRWD", "PANW", "ZS", "FTNT", "PLTR", "SNOW", "DDOG"]):
        hints.add("software_tooling")
    if any(symbol in joined for symbol in ["GEV", "ETN", "HUBB", "VRT"]):
        hints.add("power_equipment")
    if any(symbol in joined for symbol in ["CEG", "VST", "NEE"]):
        hints.add("power_generation")
    return hints


def _representative_ticker_market_hints(tickers: list[object]) -> set[str]:
    hints: set[str] = set()
    joined = " ".join(str(value or "").strip().upper() for value in tickers if str(value or "").strip())
    if any(symbol in joined for symbol in ["CRWD", "PANW", "ZS", "FTNT"]):
        hints.update({"cybersecurity", "data_center"})
    if any(symbol in joined for symbol in ["NVDA", "AMD", "AVGO", "SMCI", "MU"]):
        hints.update({"ai_compute", "semiconductor", "ai", "data_center", "semiconductor_market"})
    if any(symbol in joined for symbol in ["PLTR", "SNOW", "DDOG"]):
        hints.add("data_analytics")
    if any(symbol in joined for symbol in ["LMT", "NOC", "KTOS", "PLTR"]):
        hints.update({"defense", "defense_market"})
    if any(symbol in joined for symbol in ["RKLB", "ASTS", "LUNR"]):
        hints.add("space")
    if any(symbol in joined for symbol in ["AAOI", "LITE", "CIEN", "INFN", "FN"]):
        hints.update({"telecom", "data_center"})
    if any(symbol in joined for symbol in ["AXT", "WOLF", "ONTO", "AMAT", "LRCX", "ASML"]):
        hints.add("semiconductor_market")
    if any(symbol in joined for symbol in ["CEG", "VST", "GEV", "ETN", "VRT"]):
        hints.add("energy_market")
    return hints


def _theme_concepts(theme_entry: dict[str, object]) -> set[str]:
    return _infer_concepts(
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    ) | _representative_ticker_market_hints(list(theme_entry.get("representative_tickers") or []))


def _candidate_roles(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    parts = (
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    roles = _infer_signals(ROLE_KEYWORDS, *parts)
    role_counts = _count_signal_hits(ROLE_KEYWORDS, *parts)
    combined_text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    strong_optical_terms = {"optical", "optics", "fiber", "transceiver", "transceivers", "photonic", "photonics", "coherent"}
    if "optical_networking" in roles:
        optical_hits = int(role_counts.get("optical_networking") or 0)
        has_strong_optical_term = any(_contains_phrase(combined_text, term) for term in strong_optical_terms)
        if optical_hits < 2 and not has_strong_optical_term:
            roles.discard("optical_networking")
    strong_device_terms = {"endpoint", "endpoints", "consumer electronics", "handset"}
    if "devices_endpoints" in roles:
        device_hits = int(role_counts.get("devices_endpoints") or 0)
        has_strong_device_term = any(_contains_phrase(combined_text, term) for term in strong_device_terms)
        if device_hits < 2 and not has_strong_device_term:
            roles.discard("devices_endpoints")
    return roles


def _candidate_end_markets(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    return _infer_signals(
        END_MARKET_KEYWORDS,
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )


def _theme_roles(theme_entry: dict[str, object]) -> set[str]:
    parts = (
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    )
    roles = _infer_signals(
        ROLE_KEYWORDS,
        *parts,
    ) | _representative_ticker_role_hints(list(theme_entry.get("representative_tickers") or []))
    role_counts = _count_signal_hits(ROLE_KEYWORDS, *parts)
    combined_text = " ".join(_normalize_text(part).lower() for part in parts if _normalize_text(part))
    strong_optical_terms = {"optical", "optics", "fiber", "transceiver", "transceivers", "photonic", "photonics", "coherent"}
    if "optical_networking" in roles:
        optical_hits = int(role_counts.get("optical_networking") or 0)
        has_strong_optical_term = any(_contains_phrase(combined_text, term) for term in strong_optical_terms)
        representative_roles = _representative_ticker_role_hints(list(theme_entry.get("representative_tickers") or []))
        if optical_hits < 2 and not has_strong_optical_term and "optical_networking" not in representative_roles:
            roles.discard("optical_networking")
    strong_device_terms = {"endpoint", "endpoints", "consumer electronics", "handset"}
    if "devices_endpoints" in roles:
        device_hits = int(role_counts.get("devices_endpoints") or 0)
        has_strong_device_term = any(_contains_phrase(combined_text, term) for term in strong_device_terms)
        representative_roles = _representative_ticker_role_hints(list(theme_entry.get("representative_tickers") or []))
        if device_hits < 2 and not has_strong_device_term and "devices_endpoints" not in representative_roles:
            roles.discard("devices_endpoints")
    return roles


def _theme_end_markets(theme_entry: dict[str, object]) -> set[str]:
    return _infer_signals(
        END_MARKET_KEYWORDS,
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    ) | _representative_ticker_market_hints(list(theme_entry.get("representative_tickers") or []))


def _ranked_archetypes(scores: dict[str, int], *, threshold: int) -> set[str]:
    if not scores:
        return set()
    strongest = max(scores.values())
    return {
        archetype
        for archetype, score in scores.items()
        if score >= threshold and score >= strongest - 1
    }


def _infer_archetype_scores(
    *parts: object,
    roles: set[str] | None = None,
    concepts: set[str] | None = None,
    markets: set[str] | None = None,
) -> dict[str, int]:
    scores = _count_signal_hits(ARCHETYPE_KEYWORDS, *parts)
    roles = roles or set()
    concepts = concepts or set()
    markets = markets or set()
    if "semiconductor_materials" in roles:
        scores["semiconductor_materials_electronics_materials"] = scores.get("semiconductor_materials_electronics_materials", 0) + 5
    if "optical_networking" in roles:
        scores["networking_interconnect"] = scores.get("networking_interconnect", 0) + 4
    if "software_tooling" in roles:
        scores["software_devops_cloud"] = scores.get("software_devops_cloud", 0) + 3
    if "healthcare_equipment" in roles:
        scores["healthcare_devices_services"] = scores.get("healthcare_devices_services", 0) + 4
    if "payments" in concepts:
        scores["fintech_payments_lending"] = scores.get("fintech_payments_lending", 0) + 4
    if "cybersecurity" in concepts and any(
        _contains_phrase(" ".join(_normalize_text(part).lower() for part in parts), token)
        for token in {"identity", "verification", "biometric", "authentication"}
    ):
        scores["digital_identity_security"] = scores.get("digital_identity_security", 0) + 4
    if "ai_compute" in concepts:
        scores["ai_infrastructure_data_centers"] = scores.get("ai_infrastructure_data_centers", 0) + 3
    elif "data_center" in markets and roles & {"optical_networking", "server_systems", "power_equipment"}:
        scores["ai_infrastructure_data_centers"] = scores.get("ai_infrastructure_data_centers", 0) + 2
    if "defense" in concepts or "space" in concepts or "defense_market" in markets:
        scores["aerospace_defense_space_systems"] = scores.get("aerospace_defense_space_systems", 0) + 4
    if "healthcare" in markets or "biotech" in concepts:
        scores["healthcare_devices_services"] = scores.get("healthcare_devices_services", 0) + 2
    if "telecom" in markets:
        scores["networking_interconnect"] = scores.get("networking_interconnect", 0) + 1
    if "semiconductor_market" in markets or "semiconductor" in concepts:
        scores["semiconductor_materials_electronics_materials"] = scores.get("semiconductor_materials_electronics_materials", 0) + 1
    if "cloud" in concepts or "software" in concepts:
        scores["software_devops_cloud"] = scores.get("software_devops_cloud", 0) + 1
    if "energy_market" in markets and "networking_interconnect" in scores:
        scores["networking_interconnect"] -= 1
    return {key: value for key, value in scores.items() if value > 0}


def _candidate_archetypes(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    parts = (
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    return _ranked_archetypes(
        _infer_archetype_scores(
            *parts,
            roles=_candidate_roles(profile, candidate, *extra_parts),
            concepts=_candidate_concepts(profile, candidate),
            markets=_candidate_end_markets(profile, candidate, *extra_parts),
        ),
        threshold=3,
    )


def _theme_archetypes(theme_entry: dict[str, object]) -> set[str]:
    parts = (
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
        " ".join(str(value or "") for value in list(theme_entry.get("representative_tickers") or [])),
    )
    return _ranked_archetypes(
        _infer_archetype_scores(
            *parts,
            roles=_theme_roles(theme_entry),
            concepts=_theme_concepts(theme_entry),
            markets=_theme_end_markets(theme_entry),
        ),
        threshold=2,
    )


def _archetype_relation(candidate_archetypes: set[str], theme_archetypes: set[str]) -> str:
    if not candidate_archetypes or not theme_archetypes:
        return "unknown"
    if candidate_archetypes & theme_archetypes:
        return "direct"
    candidate_families = {ARCHETYPE_FAMILY.get(value) for value in candidate_archetypes if ARCHETYPE_FAMILY.get(value)}
    theme_families = {ARCHETYPE_FAMILY.get(value) for value in theme_archetypes if ARCHETYPE_FAMILY.get(value)}
    if candidate_families & theme_families:
        return "adjacent"
    for left in candidate_families:
        for right in theme_families:
            pair = tuple(sorted((left, right)))
            if pair in ARCHETYPE_ADJACENCY:
                return "adjacent"
    return "incompatible"


def _infer_economic_role_scores(
    *parts: object,
    roles: set[str] | None = None,
    archetypes: set[str] | None = None,
) -> dict[str, int]:
    scores = _count_signal_hits(ECONOMIC_ROLE_KEYWORDS, *parts)
    roles = roles or set()
    archetypes = archetypes or set()
    for role in roles:
        for economic_role in ROLE_ALIGNMENT.get(role, set()):
            scores[economic_role] = scores.get(economic_role, 0) + 3
    for archetype in archetypes:
        for economic_role in ARCHETYPE_ALIGNMENT.get(archetype, set()):
            scores[economic_role] = scores.get(economic_role, 0) + 2
    return {key: value for key, value in scores.items() if value > 0}


def _candidate_economic_roles(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> set[str]:
    parts = (
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    scores = _infer_economic_role_scores(
        *parts,
        roles=_candidate_roles(profile, candidate, *extra_parts),
        archetypes=_candidate_archetypes(profile, candidate, *extra_parts),
    )
    return _ranked_archetypes(scores, threshold=3)


def _theme_economic_roles(theme_entry: dict[str, object]) -> set[str]:
    parts = (
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
        " ".join(str(value or "") for value in list(theme_entry.get("representative_tickers") or [])),
    )
    scores = _infer_economic_role_scores(
        *parts,
        roles=_theme_roles(theme_entry),
        archetypes=_theme_archetypes(theme_entry),
    )
    return _ranked_archetypes(scores, threshold=2)


def _dominant_economic_role(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    roles = sorted(_candidate_economic_roles(profile, candidate, *extra_parts))
    return roles[0] if roles else ""


def _domain_anchor(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    archetypes = sorted(_candidate_archetypes(profile, candidate, *extra_parts))
    if archetypes:
        return DOMAIN_ANCHOR_LABELS.get(archetypes[0], archetypes[0].replace("_", "/"))
    concepts = _candidate_concepts(profile, candidate)
    if "defense" in concepts or "space" in concepts:
        return "aerospace/defense/space"
    if "payments" in concepts:
        return "fintech"
    if "semiconductor" in concepts:
        return "semiconductor/electronics"
    return "unclear"


def _candidate_theme_ideas_from_description(profile: dict[str, object], candidate: dict[str, object]) -> list[str]:
    description = " ".join(
        [
            _normalize_text(profile.get("company_name")),
            _normalize_text(profile.get("description")),
            _normalize_text(profile.get("sic_description")),
            _normalize_text(candidate.get("recommendation_reason")),
        ]
    ).lower()
    archetypes = _candidate_archetypes(profile, candidate)
    dominant_role = _dominant_economic_role(profile, candidate)
    ideas: list[str] = []
    memory_storage_signals = any(token in description for token in ["memory", "nand", "flash", "storage controller", "storage controllers"])

    def add(label: str | None) -> None:
        normalized = _normalize_optional_theme_label(label)
        if normalized and normalized not in ideas:
            ideas.append(normalized)

    if memory_storage_signals:
        add("Semiconductor Memory")
        add("Data Storage")
    if "semiconductor_materials_electronics_materials" in archetypes:
        add("Semiconductor Materials")
        if "substrate" in description or "substrates" in description:
            add("Semiconductor Substrates")
        if "electronics materials" in description or "packaging materials" in description:
            add("Electronics Materials")
    if "digital_identity_security" in archetypes:
        add("Digital Identity")
        add("Identity Verification")
    if "fintech_payments_lending" in archetypes:
        add("Fintech Payments")
        add("Digital Payments")
        if "lending" in description or "loan" in description or "installment" in description:
            add("Consumer Lending")
    if "ai_infrastructure_data_centers" in archetypes:
        add("AI Data Centers")
        add("AI Infrastructure")
        if dominant_role in {"infrastructure_operator", "end_platform_operator"}:
            add("Data Center Infrastructure")
    if "aerospace_defense_space_systems" in archetypes:
        if "missile" in description:
            add("Missile Defense Systems")
        if "propulsion" in description or "engine" in description:
            add("Aerospace Propulsion")
        add("Defense Systems")
        add("Space Systems")
    if "networking_interconnect" in archetypes:
        if dominant_role != "materials_supplier":
            add("Optical Interconnects")
            if "data center" in description or "hyperscale" in description:
                add("Data Center Optics")
            add("Optical Networking")
    if "software_devops_cloud" in archetypes and dominant_role == "software_service_provider":
        add("Cloud Software")
    if not ideas:
        fallback = _candidate_new_theme_label(profile, candidate)
        add(fallback)
    return ideas[:5]


def _generated_idea_analysis(idea: str) -> dict[str, object]:
    normalized = _normalize_text(idea)
    concepts = _infer_concepts(normalized)
    roles = _infer_signals(ROLE_KEYWORDS, normalized)
    markets = _infer_signals(END_MARKET_KEYWORDS, normalized)
    archetypes = _ranked_archetypes(
        _infer_archetype_scores(normalized, roles=roles, concepts=concepts, markets=markets),
        threshold=1,
    )
    economic_roles = _ranked_archetypes(
        _infer_economic_role_scores(normalized, roles=roles, archetypes=archetypes),
        threshold=1,
    )
    return {
        "idea_tokens": _tokenize(normalized),
        "idea_concepts": concepts,
        "idea_roles": roles,
        "idea_markets": markets,
        "idea_archetypes": archetypes,
        "idea_economic_roles": economic_roles,
    }


def _theme_match_from_generated_idea(
    idea: str,
    theme_entry: dict[str, object],
    profile: dict[str, object],
    candidate: dict[str, object],
    *,
    domain_anchor: str,
    dominant_business_role: str,
) -> dict[str, object]:
    prepared_theme = theme_entry if "_theme_tokens" in theme_entry else _preprocessed_theme_entry(theme_entry)
    candidate_analysis = _candidate_analysis(profile, candidate)
    idea_analysis = _generated_idea_analysis(idea)
    idea_tokens = set(idea_analysis.get("idea_tokens") or set())
    idea_concepts = set(idea_analysis.get("idea_concepts") or set())
    idea_roles = set(idea_analysis.get("idea_roles") or set())
    idea_markets = set(idea_analysis.get("idea_markets") or set())
    idea_archetypes = set(idea_analysis.get("idea_archetypes") or set())
    idea_economic_roles = set(idea_analysis.get("idea_economic_roles") or set())
    theme_tokens = set(prepared_theme.get("_theme_tokens") or set())
    theme_concepts = set(prepared_theme.get("_theme_concepts") or set())
    theme_roles = set(prepared_theme.get("_theme_roles") or set())
    theme_markets = set(prepared_theme.get("_theme_markets") or set())
    theme_archetypes = set(prepared_theme.get("_theme_archetypes") or set())
    theme_economic_roles = set(prepared_theme.get("_theme_economic_roles") or set())
    token_overlap = sorted(idea_tokens & theme_tokens)
    specific_overlap = sorted((idea_concepts & theme_concepts) - GENERIC_CONCEPTS)
    generic_overlap = sorted((idea_concepts & theme_concepts) & GENERIC_CONCEPTS)
    role_overlap = sorted(idea_roles & theme_roles)
    market_overlap = sorted(idea_markets & theme_markets)
    archetype_overlap = sorted(idea_archetypes & theme_archetypes)
    economic_role_overlap = sorted(idea_economic_roles & theme_economic_roles)
    archetype_relation = _archetype_relation(idea_archetypes, theme_archetypes)
    fit_details = _theme_fit_details(prepared_theme, profile, candidate, candidate_analysis=candidate_analysis)
    score = (
        len(role_overlap) * 18
        + len(economic_role_overlap) * 14
        + len(archetype_overlap) * 12
        + len(specific_overlap) * 9
        + len(market_overlap) * 4
        + len(generic_overlap) * 2
        + min(2, len(token_overlap)) * 2
        + min(10, max(0, int(fit_details.get("score") or 0)) // 2)
    )
    if archetype_relation == "direct":
        score += 6
    elif archetype_relation == "adjacent":
        score += 2
    elif archetype_relation == "incompatible":
        score -= 18
    if idea_roles and theme_roles and not role_overlap:
        idea_families = {ROLE_FAMILY.get(role) for role in idea_roles if ROLE_FAMILY.get(role)}
        theme_families = {ROLE_FAMILY.get(role) for role in theme_roles if ROLE_FAMILY.get(role)}
        if idea_families and theme_families:
            score -= 12 if idea_families.isdisjoint(theme_families) else 4
    if idea_economic_roles and theme_economic_roles and not economic_role_overlap:
        score -= 12
    if idea_archetypes and theme_archetypes and archetype_relation == "incompatible":
        score -= 8
    if len(token_overlap) <= 1 and not role_overlap and not specific_overlap and not economic_role_overlap and not archetype_overlap:
        score -= 10
    if token_overlap and len(token_overlap) == 1 and token_overlap[0] in {"network", "networks", "materials", "systems", "devices", "device"}:
        score -= 6
    if _is_generic_factor_theme(prepared_theme) and bool(candidate_analysis.get("strong_role_evidence")):
        score -= 12
    if domain_anchor != "unclear":
        theme_anchor = _domain_anchor(
            {"company_name": "", "description": _normalize_text(theme_entry.get("theme_description")), "sic_description": _normalize_text(theme_entry.get("category"))},
            {"recommendation_reason": _normalize_text(theme_entry.get("theme_name"))},
        )
        if theme_anchor == domain_anchor:
            score += 4
        elif theme_anchor != "unclear":
            score -= 8
    if dominant_business_role and dominant_business_role not in list(fit_details.get("theme_economic_roles") or []):
        if list(fit_details.get("theme_economic_roles") or []):
            score -= 6
    if archetype_relation == "incompatible" and not role_overlap and not specific_overlap and not economic_role_overlap:
        score = min(score, 2)
    if not role_overlap and not specific_overlap and not archetype_overlap and not economic_role_overlap and len(token_overlap) <= 1:
        score = min(score, 4)
    if role_overlap:
        fit_label = "direct_fit"
    elif economic_role_overlap or archetype_relation in {"direct", "adjacent"} or specific_overlap or market_overlap:
        fit_label = "adjacent_fit"
    else:
        fit_label = "broad_fit"
    return {
        "score": score,
        "fit_details": fit_details,
        "token_overlap": token_overlap,
        "specific_overlap": specific_overlap,
        "role_overlap": role_overlap,
        "market_overlap": market_overlap,
        "archetype_overlap": archetype_overlap,
        "economic_role_overlap": economic_role_overlap,
        "archetype_relation": archetype_relation,
        "fit_label": fit_label,
        "theme_entry": prepared_theme,
        "idea": idea,
    }


def _looks_generic_theme(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(token in theme_text for token in ["software", "cloud", "technology", "tech", "platform"])


def _is_generic_factor_theme(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(token in theme_text for token in GENERIC_FACTOR_THEME_TOKENS)


def _has_strong_role_evidence(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> bool:
    roles = _candidate_roles(profile, candidate, *extra_parts)
    archetypes = _candidate_archetypes(profile, candidate, *extra_parts)
    economic_roles = _candidate_economic_roles(profile, candidate, *extra_parts)
    concepts = _candidate_concepts(profile, candidate)
    specific_concepts = concepts - GENERIC_CONCEPTS
    return bool(
        (len(roles) >= 1 or len(economic_roles) >= 1)
        and (len(archetypes) >= 1 or len(specific_concepts) >= 1 or len(economic_roles) >= 1)
    )


def _candidate_concepts(profile: dict[str, object], candidate: dict[str, object]) -> set[str]:
    return _infer_concepts(
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
    )


def _format_signal_names(values: set[str], display_map: dict[str, str], limit: int = 2) -> str:
    return ", ".join(display_map.get(value, value.replace("_", " ")) for value in sorted(values)[:limit])


def _dominant_role(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str:
    roles = _candidate_roles(profile, candidate, *extra_parts)
    if roles:
        return sorted(roles)[0]
    return ""


def _theme_fit_details(
    theme_entry: dict[str, object],
    profile: dict[str, object],
    candidate: dict[str, object],
    *,
    candidate_analysis: dict[str, object] | None = None,
) -> dict[str, object]:
    prepared_theme = theme_entry if "_theme_tokens" in theme_entry else _preprocessed_theme_entry(theme_entry)
    analysis = candidate_analysis or _candidate_analysis(profile, candidate)
    theme_tokens = set(prepared_theme.get("_theme_tokens") or set())
    profile_tokens = set(analysis.get("profile_tokens") or set())
    candidate_concepts = set(analysis.get("candidate_concepts") or set())
    theme_concepts = set(prepared_theme.get("_theme_concepts") or set())
    candidate_roles = set(analysis.get("candidate_roles") or set())
    candidate_markets = set(analysis.get("candidate_markets") or set())
    candidate_archetypes = set(analysis.get("candidate_archetypes") or set())
    candidate_economic_roles = set(analysis.get("candidate_economic_roles") or set())
    strong_role_evidence = bool(analysis.get("strong_role_evidence"))
    theme_roles = set(prepared_theme.get("_theme_roles") or set())
    theme_markets = set(prepared_theme.get("_theme_markets") or set())
    theme_archetypes = set(prepared_theme.get("_theme_archetypes") or set())
    theme_economic_roles = set(prepared_theme.get("_theme_economic_roles") or set())
    specific_overlap = sorted((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS)
    generic_overlap = sorted((candidate_concepts & theme_concepts) & GENERIC_CONCEPTS)
    role_overlap = sorted(candidate_roles & theme_roles)
    market_overlap = sorted(candidate_markets & theme_markets)
    token_overlap = sorted((theme_tokens & profile_tokens) - STOPWORDS)
    archetype_overlap = sorted(candidate_archetypes & theme_archetypes)
    economic_role_overlap = sorted(candidate_economic_roles & theme_economic_roles)
    archetype_relation = _archetype_relation(candidate_archetypes, theme_archetypes)
    dominant_economic_role = str(analysis.get("dominant_economic_role") or "")

    score = (
        len(role_overlap) * 14
        + len(economic_role_overlap) * 12
        + len(archetype_overlap) * 10
        + len(specific_overlap) * 7
        + len(market_overlap) * 3
        + len(generic_overlap)
        + min(2, len(token_overlap))
    )
    direct_role_fit = bool(role_overlap)
    indirect_only_fit = not direct_role_fit and bool(market_overlap or generic_overlap or specific_overlap or archetype_relation == "adjacent")
    if theme_roles and candidate_roles and not role_overlap:
        candidate_families = {ROLE_FAMILY.get(role) for role in candidate_roles}
        theme_families = {ROLE_FAMILY.get(role) for role in theme_roles}
        if candidate_families.isdisjoint(theme_families):
            score -= 4 if market_overlap else 10
        else:
            score -= 4
    if archetype_relation == "direct":
        score += 4
    elif archetype_relation == "adjacent":
        score += 1
    elif archetype_relation == "incompatible":
        score -= 18 if candidate_archetypes and theme_archetypes else 8
    if candidate_economic_roles and theme_economic_roles and not economic_role_overlap:
        score -= 10 if dominant_economic_role else 6
    if not role_overlap and market_overlap:
        score -= 4
    if bool(prepared_theme.get("_looks_generic_theme")) and not role_overlap:
        score -= 4
    if bool(prepared_theme.get("_is_generic_factor_theme")):
        if strong_role_evidence:
            score -= 16
        elif not role_overlap and not archetype_overlap:
            score -= 8
    if (
        "networking_interconnect" in candidate_archetypes
        and candidate_archetypes - {"networking_interconnect"}
        and not archetype_overlap
        and archetype_relation != "adjacent"
    ):
        score -= 6
    if not role_overlap and not specific_overlap and generic_overlap and len(token_overlap) <= 1:
        score -= 3
    if not role_overlap and not specific_overlap and not market_overlap:
        score = min(score, 1)
    if archetype_relation == "incompatible" and not role_overlap and not specific_overlap:
        score = min(score, 0)
    if bool(prepared_theme.get("_is_generic_factor_theme")) and strong_role_evidence and (role_overlap or archetype_overlap or specific_overlap):
        score = min(score, 6)
    if (
        strong_role_evidence
        and dominant_economic_role in {"component_supplier", "materials_supplier", "defense_systems_manufacturer"}
        and "end_platform_operator" in theme_economic_roles
        and not economic_role_overlap
    ):
        score -= 10
    if (
        strong_role_evidence
        and dominant_economic_role in {"financial_platform", "identity_verification_platform"}
        and {"component_supplier", "materials_supplier"} & theme_economic_roles
        and not economic_role_overlap
    ):
        score -= 10

    if score <= 0:
        why = ""
    elif role_overlap:
        why = "Direct business-role fit on " + _format_signal_names(set(role_overlap), ROLE_DISPLAY_NAMES)
    elif economic_role_overlap:
        why = "Compatible economic-role fit on " + _format_signal_names(set(economic_role_overlap), ECONOMIC_ROLE_DISPLAY_NAMES)
    elif archetype_overlap:
        why = "Compatible business archetype fit on " + _format_signal_names(set(archetype_overlap), ARCHETYPE_DISPLAY_NAMES)
    elif specific_overlap:
        why = "Conceptual fit on " + ", ".join(specific_overlap[:2])
    elif market_overlap:
        why = "Indirect end-market adjacency through " + _format_signal_names(set(market_overlap), END_MARKET_DISPLAY_NAMES)
    elif generic_overlap:
        why = "Broad fit through " + ", ".join(generic_overlap[:2])
    else:
        why = "Weak text-only fit; treat as tentative."

    return {
        "score": score,
        "why": why,
        "role_overlap": role_overlap,
        "market_overlap": market_overlap,
        "specific_overlap": specific_overlap,
        "generic_overlap": generic_overlap,
        "direct_role_fit": direct_role_fit,
        "indirect_only_fit": indirect_only_fit,
        "archetype_overlap": archetype_overlap,
        "archetype_relation": archetype_relation,
        "candidate_archetypes": sorted(candidate_archetypes),
        "theme_archetypes": sorted(theme_archetypes),
        "economic_role_overlap": economic_role_overlap,
        "candidate_economic_roles": sorted(candidate_economic_roles),
        "theme_economic_roles": sorted(theme_economic_roles),
        "dominant_economic_role": dominant_economic_role,
        "generic_factor_theme": bool(prepared_theme.get("_is_generic_factor_theme")),
        "strong_role_evidence": strong_role_evidence,
    }


def _concept_strength(concepts: set[str]) -> str:
    specific = [concept for concept in concepts if concept not in GENERIC_CONCEPTS]
    if specific:
        return specific[0]
    return next(iter(concepts), "")


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
        catalog.append(
            {
                "theme_id": int(theme_id),
                "theme_name": str(theme_name),
                "category": str(category or "Uncategorized"),
                "representative_tickers": members[:representative_limit],
                "theme_description": (
                    f"{theme_name} ({category or 'Uncategorized'}) with representative tickers "
                    + (", ".join(members[:representative_limit]) if members else "none")
                ),
            }
        )
    return catalog


def _theme_preprocess_cache_key(theme_entry: dict[str, object]) -> tuple[object, ...]:
    return (
        int(theme_entry["theme_id"]),
        _normalize_text(theme_entry.get("theme_name")),
        _normalize_text(theme_entry.get("category")),
        _normalize_text(theme_entry.get("theme_description")),
        tuple(str(value or "").strip().upper() for value in list(theme_entry.get("representative_tickers") or [])),
    )


def _build_preprocessed_theme_entry(theme_entry: dict[str, object]) -> dict[str, object]:
    prepared = dict(theme_entry)
    prepared["_theme_tokens"] = _tokenize(theme_entry.get("theme_name"), theme_entry.get("category"), theme_entry.get("theme_description"))
    prepared["_theme_concepts"] = _theme_concepts(theme_entry)
    prepared["_theme_roles"] = _theme_roles(theme_entry)
    prepared["_theme_markets"] = _theme_end_markets(theme_entry)
    prepared["_theme_archetypes"] = _theme_archetypes(theme_entry)
    prepared["_theme_economic_roles"] = _theme_economic_roles(theme_entry)
    prepared["_looks_generic_theme"] = _looks_generic_theme(theme_entry)
    prepared["_is_generic_factor_theme"] = _is_generic_factor_theme(theme_entry)
    return prepared


def _preprocessed_theme_entry(theme_entry: dict[str, object]) -> dict[str, object]:
    key = _theme_preprocess_cache_key(theme_entry)
    cached = _THEME_PREPROCESS_CACHE.get(key)
    if cached is not None:
        return dict(cached)
    prepared = _build_preprocessed_theme_entry(theme_entry)
    _THEME_PREPROCESS_CACHE[key] = prepared
    return dict(prepared)


def _preprocessed_catalog(catalog: list[dict[str, object]]) -> list[dict[str, object]]:
    return [_preprocessed_theme_entry(entry) for entry in list(catalog or [])]


def _concise_theme_context(theme_entry: dict[str, object], representative_limit: int = 3) -> dict[str, object]:
    representative_tickers = [str(value).strip().upper() for value in list(theme_entry.get("representative_tickers") or []) if str(value).strip()][:representative_limit]
    description = _normalize_text(theme_entry.get("theme_description"))
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


def _load_company_profile(ticker: str) -> dict[str, object]:
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
        "company_name": _normalize_text(ref.get("name")),
        "description": _normalize_text(ref.get("description")),
        "sic_description": _normalize_text(ref.get("sic_description")),
        "primary_exchange": _normalize_text(ref.get("primary_exchange")),
        "market_cap": ref.get("market_cap"),
    }


def _profile_has_research_value(profile: dict[str, object] | None) -> bool:
    if not isinstance(profile, dict):
        return False
    return bool(
        _normalize_text(profile.get("company_name"))
        or _normalize_text(profile.get("description"))
        or _normalize_text(profile.get("sic_description"))
    )


def _load_company_profile_with_cache(ticker: str) -> dict[str, object]:
    normalized_ticker = str(ticker or "").strip().upper()
    cached = _PROFILE_CACHE.get(normalized_ticker)
    fresh = _load_company_profile(normalized_ticker)
    if _profile_has_research_value(fresh):
        profile = dict(fresh)
        profile["_profile_source"] = "live_lookup"
        _PROFILE_CACHE[normalized_ticker] = profile
        return profile
    if _profile_has_research_value(cached):
        profile = dict(cached)
        profile["_profile_source"] = "cached_live_lookup"
        return profile
    profile = dict(fresh) if isinstance(fresh, dict) else {}
    if profile:
        profile["_profile_source"] = "live_lookup_empty"
    return profile


def _candidate_context(conn, ticker: str) -> dict[str, object]:
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


def _description_analysis_cache_key(profile: dict[str, object], candidate: dict[str, object], extra_parts: tuple[object, ...]) -> tuple[object, ...]:
    return (
        _normalize_text(candidate.get("ticker")).upper(),
        _normalize_text(profile.get("company_name")),
        _normalize_text(profile.get("description")),
        _normalize_text(profile.get("sic_description")),
        _normalize_text(candidate.get("recommendation_reason")),
        tuple(_normalize_text(part) for part in extra_parts),
    )


def _build_candidate_analysis(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> dict[str, object]:
    profile_tokens = _tokenize(
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
        *extra_parts,
    )
    concepts = _candidate_concepts(profile, candidate)
    roles = _candidate_roles(profile, candidate, *extra_parts)
    markets = _candidate_end_markets(profile, candidate, *extra_parts)
    archetypes = _candidate_archetypes(profile, candidate, *extra_parts)
    economic_roles = _candidate_economic_roles(profile, candidate, *extra_parts)
    dominant_economic_role = _dominant_economic_role(profile, candidate, *extra_parts)
    return {
        "profile_tokens": profile_tokens,
        "candidate_concepts": concepts,
        "candidate_roles": roles,
        "candidate_markets": markets,
        "candidate_archetypes": archetypes,
        "candidate_economic_roles": economic_roles,
        "dominant_economic_role": dominant_economic_role,
        "strong_role_evidence": bool(
            (len(roles) >= 1 or len(economic_roles) >= 1)
            and (len(archetypes) >= 1 or len(concepts - GENERIC_CONCEPTS) >= 1 or len(economic_roles) >= 1)
        ),
    }


def _candidate_analysis(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> dict[str, object]:
    key = _description_analysis_cache_key(profile, candidate, tuple(extra_parts))
    cached = _DESCRIPTION_ANALYSIS_CACHE.get(key)
    if cached is not None:
        return cached
    analysis = _build_candidate_analysis(profile, candidate, *extra_parts)
    _DESCRIPTION_ANALYSIS_CACHE[key] = analysis
    return analysis


def _theme_fit_score(theme_entry: dict[str, object], profile: dict[str, object], candidate: dict[str, object]) -> tuple[int, str]:
    details = _theme_fit_details(theme_entry, profile, candidate)
    return int(details["score"]), str(details["why"])


def _candidate_new_theme_label(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> str | None:
    roles = _candidate_roles(profile, candidate, *extra_parts)
    markets = _candidate_end_markets(profile, candidate, *extra_parts)
    archetypes = _candidate_archetypes(profile, candidate, *extra_parts)
    economic_role = _dominant_economic_role(profile, candidate, *extra_parts)
    description = " ".join(
        [
            _normalize_text(profile.get("company_name")).lower(),
            _normalize_text(profile.get("description")).lower(),
            _normalize_text(profile.get("sic_description")).lower(),
            _normalize_text(candidate.get("recommendation_reason")).lower(),
            *[_normalize_text(part).lower() for part in extra_parts],
        ]
    )
    if economic_role == "financial_platform" and "fintech_payments_lending" in archetypes:
        return "Digital Payments"
    if economic_role == "identity_verification_platform" and "digital_identity_security" in archetypes:
        return "Identity Verification"
    if "fintech_payments_lending" in archetypes:
        return "Digital Payments"
    if "digital_identity_security" in archetypes:
        return "Identity Verification"
    if "semiconductor_materials_electronics_materials" in archetypes:
        if "compound semiconductor" in description or "gallium arsenide" in description or "indium phosphide" in description:
            return "Compound Semiconductor Materials"
        if "substrate" in description or "substrates" in description:
            return "Semiconductor Substrates"
        if "electronics materials" in description or "packaging materials" in description:
            return "Electronics Materials"
        if "specialty" in description:
            return "Specialty Semiconductor Materials"
        return "Semiconductor Materials"
    if "aerospace_defense_space_systems" in archetypes and economic_role == "defense_systems_manufacturer":
        return "Defense Systems"
    if "optical_networking" in roles:
        if "ai" in markets and "fiber" in description:
            return "AI Fiber Optics"
        if "data_center" in markets:
            return "Data Center Optics"
        if "interconnect" in description:
            return "Optical Interconnects"
        return "Optical Networking"
    if "networking_interconnect" in archetypes:
        if economic_role == "materials_supplier":
            return None
        if "ai" in markets and "fiber" in description:
            return "AI Fiber Optics"
        if "data_center" in markets:
            return "Data Center Optics"
        if "interconnect" in description:
            return "Optical Interconnects"
        return "Optical Networking"
    if "ai_infrastructure_data_centers" in archetypes:
        return "AI Data Centers"
    if "semiconductor_materials" in roles:
        if "compound semiconductor" in description or "gallium arsenide" in description or "indium phosphide" in description:
            return "Compound Semiconductor Materials"
        if "substrate" in description or "substrates" in description:
            return "Semiconductor Substrates"
        if "specialty" in description:
            return "Specialty Semiconductor Materials"
        return "Semiconductor Materials"
    dominant_role = _dominant_role(profile, candidate, *extra_parts)
    if dominant_role:
        if dominant_role in {"software_tooling", "power_generation"}:
            return None
        return _normalize_optional_theme_label(ROLE_NEW_LABELS.get(dominant_role))
    concept = _concept_strength(_candidate_concepts(profile, candidate))
    if concept:
        return _normalize_optional_theme_label(THEME_NEW_LABELS.get(concept) or _normalize_text(profile.get("sic_description")).title())
    return None


def _supports_distinct_new_theme_label(profile: dict[str, object], candidate: dict[str, object], *extra_parts: object) -> bool:
    archetypes = _candidate_archetypes(profile, candidate, *extra_parts)
    if archetypes & {
        "fintech_payments_lending",
        "digital_identity_security",
        "semiconductor_materials_electronics_materials",
        "ai_infrastructure_data_centers",
        "networking_interconnect",
    }:
        return True
    role = _dominant_role(profile, candidate, *extra_parts)
    if role in {"", "software_tooling", "power_generation"}:
        return False
    return True


def _should_prioritize_new_theme(
    candidate_roles: set[str],
    suggested_existing: list[dict[str, object]],
    strongest_score: int,
    strongest_direct_role_fit: bool,
) -> bool:
    if not candidate_roles:
        return False
    if not suggested_existing:
        return True
    if not strongest_direct_role_fit:
        return True
    return strongest_score < 12


def _value_chain_summary(profile: dict[str, object], candidate: dict[str, object]) -> str:
    roles = _candidate_roles(profile, candidate)
    markets = _candidate_end_markets(profile, candidate)
    economic_role = _dominant_economic_role(profile, candidate)
    if roles:
        if "server_systems" in roles and ({"ai", "data_center"} & markets):
            return "appears to serve AI compute infrastructure and data-center end markets through server systems"
        role_text = _format_signal_names(roles, ROLE_DISPLAY_NAMES)
        economic_role_text = ECONOMIC_ROLE_DISPLAY_NAMES.get(economic_role, "")
        market_text = _format_signal_names(markets, END_MARKET_DISPLAY_NAMES) if markets else ""
        if market_text and economic_role_text:
            return f"appears to serve {market_text} end markets as a {economic_role_text} focused on {role_text}"
        if market_text:
            return f"appears to serve {market_text} end markets through {role_text}"
        return f"appears to operate primarily in {role_text}"
    concepts = _candidate_concepts(profile, candidate)
    if "cybersecurity" in concepts:
        return "appears to operate in cybersecurity software and security operations tooling"
    if "ai_compute" in concepts and "semiconductor" in concepts:
        return "appears to sit in AI compute infrastructure through chips, servers, or accelerated data-center hardware"
    if "ai_compute" in concepts:
        return "appears to sit in AI compute infrastructure and data-center enablement"
    if "data_analytics" in concepts and "defense" in concepts:
        return "appears to provide data/decision platforms with government or defense adjacency"
    if "data_analytics" in concepts:
        return "appears to provide data integration, analytics, or decision software"
    if "space" in concepts:
        return "appears to operate in the space/satellite infrastructure value chain"
    if "defense" in concepts:
        return "appears to have defense or mission-oriented technology exposure"
    if "payments" in concepts:
        return "appears to operate in digital payments or transaction infrastructure"
    if "biotech" in concepts:
        return "appears to operate in biotech or therapeutics"
    if "energy" in concepts:
        return "appears to operate in energy generation, storage, or grid infrastructure"
    description = _normalize_text(profile.get("description")) or _normalize_text(profile.get("sic_description"))
    return description[:140] if description else "has limited profile context available"


def _prefilter_ai_theme_catalog(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    max_themes: int = 12,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    candidate_analysis = _candidate_analysis(profile, candidate)
    ranked: list[tuple[int, int, dict[str, object]]] = []
    adjacent: list[tuple[int, int, dict[str, object]]] = []
    for entry in _preprocessed_catalog(catalog):
        fit_details = _theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis)
        score = int(fit_details["score"])
        direct_bonus = 1 if fit_details.get("direct_role_fit") else 0
        ranked.append((score, direct_bonus, entry))
        if fit_details.get("indirect_only_fit") or fit_details.get("market_overlap") or fit_details.get("specific_overlap"):
            adjacent.append((score, direct_bonus, entry))
    ranked.sort(key=lambda item: (-item[0], -item[1], str(item[2].get("theme_name") or "")))
    adjacent.sort(key=lambda item: (-item[0], -item[1], str(item[2].get("theme_name") or "")))

    selected: list[dict[str, object]] = []
    seen_theme_ids: set[int] = set()
    for _, _, entry in ranked[:max_themes]:
        theme_id = int(entry["theme_id"])
        if theme_id in seen_theme_ids:
            continue
        selected.append(_concise_theme_context(entry))
        seen_theme_ids.add(theme_id)

    if len(selected) < min(6, max_themes):
        for _, _, entry in adjacent:
            theme_id = int(entry["theme_id"])
            if theme_id in seen_theme_ids:
                continue
            selected.append(_concise_theme_context(entry))
            seen_theme_ids.add(theme_id)
            if len(selected) >= min(6, max_themes):
                break

    if not selected:
        for entry in catalog[:max_themes]:
            theme_id = int(entry["theme_id"])
            if theme_id in seen_theme_ids:
                continue
            selected.append(_concise_theme_context(entry))
            seen_theme_ids.add(theme_id)

    meta = {
        "full_catalog_theme_count": len(catalog),
        "filtered_theme_count": len(selected),
        "catalog_was_prefiltered": len(selected) < len(catalog),
        "max_themes": max_themes,
    }
    return selected[:max_themes], meta


def _heuristic_research_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    scored: list[tuple[int, dict[str, object], dict[str, object]]] = []
    adjacent_scored: list[tuple[int, dict[str, object], dict[str, object]]] = []
    broad_alternatives: list[str] = []
    candidate_analysis = _candidate_analysis(profile, candidate)
    candidate_concepts = set(candidate_analysis.get("candidate_concepts") or set())
    candidate_roles = set(candidate_analysis.get("candidate_roles") or set())
    candidate_markets = set(candidate_analysis.get("candidate_markets") or set())
    strong_role_evidence = bool(candidate_analysis.get("strong_role_evidence"))
    for entry in _preprocessed_catalog(catalog):
        theme_concepts = set(entry.get("_theme_concepts") or set())
        theme_roles = set(entry.get("_theme_roles") or set())
        theme_markets = set(entry.get("_theme_markets") or set())
        if (
            not (candidate_roles & theme_roles)
            and not ((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS)
            and ((candidate_concepts & theme_concepts) & GENERIC_CONCEPTS)
            and str(entry.get("theme_name") or "") not in broad_alternatives
        ):
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        elif (_looks_generic_theme(entry) or _is_generic_factor_theme(entry)) and str(entry.get("theme_name") or "") not in broad_alternatives:
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        fit_details = _theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis)
        score = int(fit_details["score"])
        why = str(fit_details["why"])
        if not why and not fit_details.get("direct_role_fit"):
            if fit_details.get("market_overlap"):
                why = "Indirect end-market adjacency through " + _format_signal_names(set(fit_details["market_overlap"]), END_MARKET_DISPLAY_NAMES)
            elif fit_details.get("specific_overlap"):
                why = "Partial conceptual overlap on " + ", ".join(list(fit_details["specific_overlap"])[:2])
        suggestion_payload = {
            "theme_id": int(entry["theme_id"]),
            "theme_name": str(entry["theme_name"]),
            "category": str(entry["category"]),
            "why_it_might_fit": why,
            "representative_tickers": list(entry.get("representative_tickers") or []),
        }
        suggestion_payload = _annotate_suggestion_fit(suggestion_payload, fit_details)
        if (
            not fit_details.get("direct_role_fit")
            and (
                fit_details.get("market_overlap")
                or ((candidate_markets & theme_markets) and not (candidate_roles & theme_roles))
                or (((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS) and not (candidate_roles & theme_roles))
            )
        ):
            adjacent_scored.append((score, suggestion_payload, fit_details))
        if score < 3:
            continue
        scored.append(
            (
                score,
                suggestion_payload,
                fit_details,
            )
        )
    scored.sort(key=lambda item: (-item[0], item[1]["theme_name"]))
    adjacent_scored.sort(key=lambda item: (-item[0], item[1]["theme_name"]))
    strongest_score = scored[0][0] if scored else 0
    score_floor = max(8, strongest_score - 2) if strongest_score else 999
    suggested_existing = [item[1] for item in scored if item[0] >= score_floor][:3]
    if strong_role_evidence:
        suggested_existing = [item for item in suggested_existing if item.get("fit_label") != "broad_fit" or not _is_generic_factor_theme({
            "theme_name": item.get("theme_name"),
            "category": item.get("category"),
            "theme_description": item.get("why_it_might_fit"),
        })][:3]
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
            if not fit_details.get("direct_role_fit") and candidate_roles:
                continue
            for ticker in suggestion.get("representative_tickers") or []:
                symbol = str(ticker).strip().upper()
                if symbol and symbol != candidate["ticker"] and symbol not in possible_similar:
                    possible_similar.append(symbol)
    possible_similar = possible_similar[:3 if strongest_score >= 12 else 2]

    confidence = "low"
    recommended_action = "watch_only"
    possible_new_theme = None
    caveats: list[str] = []
    new_theme_label = _candidate_new_theme_label(profile, candidate)
    should_prioritize_new_theme = _should_prioritize_new_theme(
        candidate_roles,
        suggested_existing,
        strongest_score,
        strongest_direct_role_fit,
    )
    if should_prioritize_new_theme and new_theme_label:
        if not suggested_existing and adjacent_scored:
            suggested_existing = [item[1] for item in adjacent_scored[:2]]
        possible_new_theme = new_theme_label
        recommended_action = "consider_new_theme"
        confidence = "medium" if candidate_roles else "low"
        if suggested_existing:
            caveats.append("Existing governed themes look adjacent rather than direct fits for the company's narrow business role.")
        else:
            caveats.append("No strong existing governed theme match was found from current catalog context.")
    elif suggested_existing and strongest_score >= 10:
        confidence = "high" if strongest_score >= 12 and strongest_direct_role_fit else "medium"
        recommended_action = "add_to_existing_theme_review"
        if strong_role_evidence and new_theme_label and suggested_existing:
            top_existing_name = str(suggested_existing[0].get("theme_name") or "")
            if _is_generic_factor_theme(
                {
                    "theme_name": top_existing_name,
                    "category": suggested_existing[0].get("category"),
                    "theme_description": suggested_existing[0].get("why_it_might_fit"),
                }
            ):
                possible_new_theme = new_theme_label
                recommended_action = "consider_new_theme"
                confidence = "medium"
                caveats.append("Generic factor/style themes are less useful than the company's operating-role framing for thematic review.")
    elif new_theme_label:
        confidence = "low"
        possible_new_theme = new_theme_label
        recommended_action = "consider_new_theme"
        caveats.append("No strong existing governed theme match was found from current catalog context.")
    elif candidate["recommendation"] in {"high-persistence uncovered", "review for addition"}:
        caveats.append("No external company profile was available, so the draft is based on internal scanner evidence only.")
    else:
        recommended_action = "reject_for_now"
        caveats.append("Internal evidence is weak and no grounded theme fit was found.")

    if not _normalize_text(profile.get("description")):
        caveats.append("Company description is unavailable or unverified in the current environment.")

    rationale_parts = [
        f"The company { _value_chain_summary(profile, candidate) }.",
        f"Scanner Audit shows {candidate['recommendation']} with persistence_score={candidate['persistence_score']}, observed_days={candidate['observed_days']}, last_10={candidate['observations_last_10d']}, streak={candidate['current_streak']}.",
        "Theme ranking prioritizes the company's actual role in the stack over broad end-market adjacency.",
    ]
    if possible_new_theme:
        role_text = _format_signal_names(candidate_roles, ROLE_DISPLAY_NAMES) if candidate_roles else "its apparent role"
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
        weaker_alternatives = [name for name in broad_alternatives if name not in {item["theme_name"] for item in suggested_existing}][:2]
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
            weaker_alternatives = [name for name in broad_alternatives if name not in {item["theme_name"] for item in suggested_existing}][:2]
        if weaker_alternatives:
            rationale_parts.append("Broader alternatives such as " + ", ".join(weaker_alternatives) + " look weaker because they map more to end-market adjacency than to the company's actual role in the stack.")
    else:
        rationale_parts.append("No strong governed-theme fit was identified from the available profile and taxonomy context.")

    return {
        "ticker": candidate["ticker"],
        "company_name": _normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": _normalize_text(profile.get("description")) or _normalize_text(profile.get("sic_description")) or "No verified company description available.",
        "possible_similar_tickers": possible_similar,
        "suggested_existing_themes": _prioritize_operating_role_suggestions(
            _truncate_existing_theme_suggestions(suggested_existing),
            strong_role_evidence=strong_role_evidence,
        ),
        "possible_new_theme": possible_new_theme,
        "confidence": confidence,
        "rationale": " ".join(rationale_parts),
        "caveats": caveats,
        "recommended_action": recommended_action,
    }


def _description_theme_generation_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    draft_start = _now_perf()
    description = _normalize_text(profile.get("description")) or _normalize_text(profile.get("sic_description"))
    domain_start = _now_perf()
    domain_anchor = _domain_anchor(profile, candidate)
    domain_ms = _elapsed_ms(domain_start)
    role_start = _now_perf()
    dominant_business_role = _dominant_economic_role(profile, candidate)
    role_ms = _elapsed_ms(role_start)
    idea_start = _now_perf()
    candidate_theme_ideas = _candidate_theme_ideas_from_description(profile, candidate)
    idea_ms = _elapsed_ms(idea_start)
    matched_theme_candidates: list[dict[str, object]] = []
    best_by_theme_id: dict[int, dict[str, object]] = {}
    match_start = _now_perf()
    for idea in candidate_theme_ideas:
        for entry in _preprocessed_catalog(catalog):
            match = _theme_match_from_generated_idea(
                idea,
                entry,
                profile,
                candidate,
                domain_anchor=domain_anchor,
                dominant_business_role=dominant_business_role,
            )
            theme_id = int(entry["theme_id"])
            if theme_id not in best_by_theme_id or int(match["score"]) > int(best_by_theme_id[theme_id]["score"]):
                best_by_theme_id[theme_id] = match
    match_ms = _elapsed_ms(match_start)
    finalize_start = _now_perf()
    ranked_matches = sorted(
        best_by_theme_id.values(),
        key=lambda item: (-int(item["score"]), str(item["theme_entry"].get("theme_name") or "")),
    )
    strong_role_evidence = _has_strong_role_evidence(profile, candidate)
    suggested_existing: list[dict[str, object]] = []
    for match in ranked_matches:
        entry = match["theme_entry"]
        if strong_role_evidence and _is_generic_factor_theme(entry):
            continue
        fit_details = dict(match["fit_details"])
        fit_label = _fit_label_from_details(fit_details)
        fit_label = str(match.get("fit_label") or fit_label or "broad_fit")
        minimum_score = {"direct_fit": 15, "adjacent_fit": 12, "broad_fit": 14}.get(fit_label, 14)
        if int(match["score"]) < minimum_score:
            continue
        why = (
            f"Matches generated idea '{match['idea']}'"
            + (f" via {', '.join(match['token_overlap'][:3])}" if match["token_overlap"] else "")
        )
        suggestion = _annotate_suggestion_fit(
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
        if len(suggested_existing) >= 3:
            break
    suggested_existing = _prioritize_operating_role_suggestions(
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
    possible_new_theme = None
    recommended_action = "watch_only"
    confidence = "low"
    if strongest_unmatched_idea and len(candidate_theme_ideas) >= 1 and strong_role_evidence:
        possible_new_theme = strongest_unmatched_idea
        recommended_action = "consider_new_theme"
        confidence = "medium"
    elif suggested_existing:
        recommended_action = "add_to_existing_theme_review" if suggested_existing[0].get("fit_label") == "direct_fit" else "watch_only"
        confidence = "medium" if suggested_existing[0].get("fit_label") in {"direct_fit", "adjacent_fit"} else "low"
    caveats: list[str] = []
    if not description:
        caveats.append("Company description is unavailable or unverified in the current environment.")
    if not suggested_existing and not possible_new_theme:
        caveats.append("Description-first generation did not find a strong operating-role theme cluster.")
    rationale_parts = [
        f"The company { _value_chain_summary(profile, candidate) }.",
        f"Description-first generation anchored on domain `{domain_anchor}` and dominant role `{dominant_business_role or 'unclear'}`.",
    ]
    if suggested_existing:
        rationale_parts.append(
            "Best governed-theme matches: "
            + "; ".join(f"{item['theme_name']} [{item['fit_label']}]" for item in suggested_existing)
        )
    if possible_new_theme:
        rationale_parts.append(f"The strongest unmatched role idea is {possible_new_theme}, which is being preserved as a tentative new-theme suggestion.")
    elif not suggested_existing:
        rationale_parts.append("No strong governed-theme or reusable new-theme idea was identified from the description.")
    draft = {
        "ticker": candidate["ticker"],
        "company_name": _normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": description or "No verified company description available.",
        "possible_similar_tickers": [],
        "suggested_existing_themes": suggested_existing[:3],
        "possible_new_theme": possible_new_theme,
        "confidence": confidence,
        "rationale": " ".join(rationale_parts),
        "caveats": caveats,
        "recommended_action": recommended_action,
        "theme_generation_strategy": "description_theme_generation",
        "domain_anchor": domain_anchor,
        "dominant_business_role": dominant_business_role or "unclear",
        "candidate_theme_ideas": candidate_theme_ideas,
        "matched_theme_candidates": matched_theme_candidates[:5],
    }
    draft["research_timing_summary"] = {
        "strategy": "description_theme_generation",
        "domain_anchor_ms": domain_ms,
        "dominant_business_role_ms": role_ms,
        "candidate_theme_ideas_ms": idea_ms,
        "governed_theme_matching_ms": match_ms,
        "finalize_ms": _elapsed_ms(finalize_start),
        "strategy_total_ms": _elapsed_ms(draft_start),
    }
    return draft


def _call_openai_research(api_key: str, context: dict[str, object], *, max_output_tokens: int = 550) -> dict[str, object]:
    payload = {
        "model": AI_MODEL,
        "max_output_tokens": max_output_tokens,
        "input": [
            {"role": "system", "content": RESEARCH_DRAFT_SYSTEM_PROMPT},
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


def _estimate_context_size_chars(context: dict[str, object]) -> int:
    return len(json.dumps(sanitize_context(context)))


def _normalize_action(value: object, fallback: str = "watch_only") -> str:
    normalized = _normalize_text(value) or fallback
    allowed = {"add_to_existing_theme_review", "consider_new_theme", "watch_only", "reject_for_now"}
    return normalized if normalized in allowed else fallback


def _best_suggested_theme_fit_details(
    suggested_existing: list[dict[str, object]],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, object]:
    candidate_analysis = _candidate_analysis(profile, candidate)
    by_id = {int(item["theme_id"]): item for item in _preprocessed_catalog(catalog)}
    best: dict[str, object] = {"score": 0, "direct_role_fit": False, "indirect_only_fit": False}
    for suggestion in suggested_existing:
        try:
            theme_id = int(suggestion.get("theme_id"))
        except Exception:
            continue
        entry = by_id.get(theme_id)
        if entry is None:
            continue
        fit_details = _theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis)
        if int(fit_details.get("score") or 0) > int(best.get("score") or 0):
            best = fit_details
    return best


def _annotate_existing_theme_suggestions(
    suggestions: list[dict[str, object]],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> list[dict[str, object]]:
    candidate_analysis = _candidate_analysis(profile, candidate)
    by_id = {int(item["theme_id"]): item for item in _preprocessed_catalog(catalog)}
    annotated: list[dict[str, object]] = []
    for suggestion in list(suggestions or []):
        try:
            theme_id = int(suggestion.get("theme_id"))
        except Exception:
            continue
        entry = by_id.get(theme_id)
        if entry is None:
            continue
        fit_details = _theme_fit_details(entry, profile, candidate, candidate_analysis=candidate_analysis)
        annotated.append(_annotate_suggestion_fit(suggestion, fit_details))
    return _truncate_existing_theme_suggestions(annotated)


def _prioritize_operating_role_suggestions(
    suggestions: list[dict[str, object]],
    *,
    strong_role_evidence: bool,
) -> list[dict[str, object]]:
    if not strong_role_evidence:
        return _truncate_existing_theme_suggestions(suggestions)
    ranked = sorted(
        list(suggestions or []),
        key=lambda item: (
            _is_generic_factor_theme(
                {
                    "theme_name": item.get("theme_name"),
                    "category": item.get("category"),
                    "theme_description": item.get("why_it_might_fit"),
                }
            ),
            item.get("fit_label") == "broad_fit",
            -int(item.get("_match_score") or 0),
            str(item.get("theme_name") or ""),
        ),
    )
    return _truncate_existing_theme_suggestions(ranked)


def _precision_override_reason(
    possible_new_theme: str,
    suggested_existing: list[dict[str, object]],
) -> str:
    if suggested_existing:
        theme_names = ", ".join(item["theme_name"] for item in suggested_existing[:2])
        return (
            f"{possible_new_theme} is a more precise description of the company's direct role than adjacent governed themes such as {theme_names}."
        )
    return f"{possible_new_theme} is a more precise description of the company's direct role than the current governed taxonomy."


def _rationale_signals_precision_gap(rationale: str) -> bool:
    normalized = _normalize_text(rationale).lower()
    if not normalized:
        return False
    precision_markers = [
        "more precise",
        "more specific",
        "narrow business-role",
        "current governed taxonomy",
        "direct role",
        "actual role in the stack",
        "actual role",
        "role in the stack",
        "value-chain position",
        "what the company actually provides",
        "what the company provides",
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


def _existing_theme_fit_is_adjacent_only(best_fit: dict[str, object]) -> bool:
    if not isinstance(best_fit, dict):
        return False
    if bool(best_fit.get("direct_role_fit")):
        return False
    if bool(best_fit.get("indirect_only_fit")):
        return True
    return int(best_fit.get("score") or 0) < 12


def _merge_ai_with_heuristic_draft(
    ai_draft: dict[str, object],
    heuristic_draft: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, object]:
    merged = dict(heuristic_draft)
    merged.update({k: v for k, v in ai_draft.items() if v not in (None, "", [], {})})

    merged["company_name"] = _normalize_text(ai_draft.get("company_name")) or heuristic_draft.get("company_name")
    merged["short_company_description"] = _normalize_text(ai_draft.get("short_company_description")) or heuristic_draft.get("short_company_description")

    ai_similar = [str(value).strip().upper() for value in ai_draft.get("possible_similar_tickers") or [] if str(value).strip()]
    merged["possible_similar_tickers"] = ai_similar[:5] if ai_similar else list(heuristic_draft.get("possible_similar_tickers") or [])

    ai_suggested = list(ai_draft.get("suggested_existing_themes") or [])
    merged["suggested_existing_themes"] = ai_suggested if ai_suggested else list(heuristic_draft.get("suggested_existing_themes") or [])

    merged["possible_new_theme"] = _normalize_optional_theme_label(ai_draft.get("possible_new_theme")) or _normalize_optional_theme_label(heuristic_draft.get("possible_new_theme"))
    merged["confidence"] = _normalize_text(ai_draft.get("confidence")) or heuristic_draft.get("confidence") or "low"
    merged["recommended_action"] = _normalize_action(ai_draft.get("recommended_action"), heuristic_draft.get("recommended_action") or "watch_only")

    ai_rationale = _normalize_text(ai_draft.get("rationale"))
    heuristic_rationale = _normalize_text(heuristic_draft.get("rationale"))
    merged["rationale"] = ai_rationale or heuristic_rationale or "No grounded rationale was available."

    ai_caveats = [str(value).strip() for value in ai_draft.get("caveats") or [] if str(value).strip()]
    heuristic_caveats = [str(value).strip() for value in heuristic_draft.get("caveats") or [] if str(value).strip()]
    merged["caveats"] = ai_caveats or heuristic_caveats

    if not merged["possible_new_theme"] and merged["recommended_action"] == "consider_new_theme":
        merged["possible_new_theme"] = _normalize_optional_theme_label(heuristic_draft.get("possible_new_theme"))

    ai_role_context = [
        ai_draft.get("short_company_description"),
        ai_draft.get("rationale"),
    ]
    heuristic_prefers_new_theme = (
        _normalize_action(heuristic_draft.get("recommended_action")) == "consider_new_theme"
        and _normalize_text(heuristic_draft.get("possible_new_theme"))
    )
    strong_role_evidence = _has_strong_role_evidence(profile, candidate, *ai_role_context)
    candidate_new_theme = _candidate_new_theme_label(profile, candidate, *ai_role_context)
    supports_distinct_new_theme = _supports_distinct_new_theme_label(profile, candidate, *ai_role_context)
    ai_rationale_signals_gap = _rationale_signals_precision_gap(ai_rationale)
    merged_rationale_signals_gap = _rationale_signals_precision_gap(str(merged.get("rationale") or ""))
    best_ai_existing_fit = _best_suggested_theme_fit_details(
        list(merged.get("suggested_existing_themes") or []),
        catalog,
        profile,
        candidate,
    )
    adjacency_only_existing_fit = _existing_theme_fit_is_adjacent_only(best_ai_existing_fit)
    inferred_candidate_roles = _candidate_roles(profile, candidate, *ai_role_context)
    role_specific_context_supports_new_theme = bool(inferred_candidate_roles) and (
        _profile_has_research_value(profile) or any(_normalize_text(part) for part in ai_role_context)
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
        and (
            not merged.get("suggested_existing_themes")
            or adjacency_only_existing_fit
        )
    )
    top_existing_is_generic_factor = False
    existing_suggestions = list(merged.get("suggested_existing_themes") or [])
    if existing_suggestions:
        top_existing = existing_suggestions[0]
        top_existing_is_generic_factor = _is_generic_factor_theme(
            {
                "theme_name": top_existing.get("theme_name"),
                "category": top_existing.get("category"),
                "theme_description": top_existing.get("why_it_might_fit"),
            }
        )
    if (
        bool(candidate_new_theme)
        and strong_role_evidence
        and top_existing_is_generic_factor
    ):
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
            _normalize_optional_theme_label(ai_draft.get("possible_new_theme"))
            or candidate_new_theme
            or _normalize_optional_theme_label(heuristic_draft.get("possible_new_theme"))
        )
        merged["recommended_action"] = "consider_new_theme"
        if _normalize_text(merged.get("confidence")) in {"high", ""}:
            merged["confidence"] = "medium"
        precision_sentence = _precision_override_reason(
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
    elif not supports_distinct_new_theme and _normalize_action(ai_draft.get("recommended_action"), "watch_only") == "watch_only":
        merged["possible_new_theme"] = None

    if not _normalize_text(merged.get("rationale")):
        merged["rationale"] = heuristic_rationale or "No strong governed-theme fit was identified; review the business role manually."

    merged["suggested_existing_themes"] = _annotate_existing_theme_suggestions(
        list(merged.get("suggested_existing_themes") or []),
        catalog,
        profile,
        candidate,
    )
    merged["suggested_existing_themes"] = _prioritize_operating_role_suggestions(
        list(merged.get("suggested_existing_themes") or []),
        strong_role_evidence=strong_role_evidence,
    )

    return merged


def _normalize_ai_theme_suggestions(raw_items: object, catalog: list[dict[str, object]]) -> list[dict[str, object]]:
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
                "why_it_might_fit": _normalize_text(item.get("why_it_might_fit")) or "AI suggested this as a possible governed-theme fit.",
                "representative_tickers": list(catalog_entry.get("representative_tickers") or []),
                "fit_label": _normalize_text(item.get("fit_label")),
            }
        )
    return _truncate_existing_theme_suggestions(normalized)


def _ai_research_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    return _ai_research_draft_for_strategy(candidate, catalog, profile, strategy="legacy_direct_match")


def _baseline_research_draft_for_strategy(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    strategy: str,
) -> dict[str, object]:
    normalized_strategy = _normalize_research_strategy(strategy)
    baseline_start = _now_perf()
    if normalized_strategy == "description_theme_generation":
        draft = _description_theme_generation_draft(candidate, catalog, profile)
        timing = dict(draft.get("research_timing_summary") or {})
        timing["baseline_total_ms"] = _elapsed_ms(baseline_start)
        draft["research_timing_summary"] = timing
        return draft
    draft = _heuristic_research_draft(candidate, catalog, profile)
    draft["theme_generation_strategy"] = "legacy_direct_match"
    draft["domain_anchor"] = _domain_anchor(profile, candidate)
    draft["dominant_business_role"] = _dominant_economic_role(profile, candidate) or "unclear"
    draft["candidate_theme_ideas"] = []
    draft["matched_theme_candidates"] = []
    draft["research_timing_summary"] = {
        "strategy": "legacy_direct_match",
        "baseline_total_ms": _elapsed_ms(baseline_start),
    }
    return draft


def _ai_research_draft_for_strategy(
    candidate: dict[str, object],
    catalog: list[dict[str, object]],
    profile: dict[str, object],
    *,
    strategy: str,
) -> dict[str, object]:
    ai_total_start = _now_perf()
    api_key = openai_api_key()
    if not api_key:
        raise ValueError(f"{OPENAI_API_KEY_ENV} is not set.")
    baseline_start = _now_perf()
    heuristic_baseline = _baseline_research_draft_for_strategy(candidate, catalog, profile, strategy=strategy)
    baseline_ms = _elapsed_ms(baseline_start)
    prefilter_start = _now_perf()
    filtered_catalog, context_meta = _prefilter_ai_theme_catalog(
        candidate,
        catalog,
        profile,
        max_themes=8 if _normalize_research_strategy(strategy) == "description_theme_generation" else 12,
    )
    prefilter_ms = _elapsed_ms(prefilter_start)
    context = {
        "candidate": candidate,
        "company_profile": profile,
        "governed_theme_catalog": filtered_catalog,
        "heuristic_baseline": {
            "suggested_existing_themes": heuristic_baseline.get("suggested_existing_themes") or [],
            "possible_new_theme": heuristic_baseline.get("possible_new_theme"),
            "recommended_action": heuristic_baseline.get("recommended_action"),
            "rationale_summary": heuristic_baseline.get("rationale"),
            "domain_anchor": heuristic_baseline.get("domain_anchor"),
            "dominant_business_role": heuristic_baseline.get("dominant_business_role"),
            "candidate_theme_ideas": list(heuristic_baseline.get("candidate_theme_ideas") or [])[:5],
            "matched_theme_candidates": list(heuristic_baseline.get("matched_theme_candidates") or [])[:5],
        },
    }
    context_meta["estimated_context_chars"] = _estimate_context_size_chars(context)
    request_start = _now_perf()
    raw = _call_openai_research(
        api_key,
        context,
        max_output_tokens=400 if _normalize_research_strategy(strategy) == "description_theme_generation" else 550,
    )
    ai_request_ms = _elapsed_ms(request_start)
    normalize_start = _now_perf()
    suggested_existing = _normalize_ai_theme_suggestions(raw.get("suggested_existing_themes"), catalog)
    ai_normalize_ms = _elapsed_ms(normalize_start)
    ai_draft = {
        "ticker": candidate["ticker"],
        "company_name": _normalize_text(raw.get("company_name")) or _normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": _normalize_text(raw.get("short_company_description")) or _normalize_text(profile.get("description")) or "No verified company description available.",
        "possible_similar_tickers": [str(value).strip().upper() for value in raw.get("possible_similar_tickers") or [] if str(value).strip()][:5],
        "suggested_existing_themes": suggested_existing,
        "possible_new_theme": _normalize_optional_theme_label(raw.get("possible_new_theme")),
        "confidence": _normalize_text(raw.get("confidence")) or "low",
        "rationale": _normalize_text(raw.get("rationale")),
        "caveats": [str(value).strip() for value in raw.get("caveats") or [] if str(value).strip()],
        "recommended_action": _normalize_action(raw.get("recommended_action"), "watch_only"),
        "research_context_meta": context_meta,
    }
    merge_start = _now_perf()
    draft = _merge_ai_with_heuristic_draft(ai_draft, heuristic_baseline, catalog, profile, candidate)
    merge_ms = _elapsed_ms(merge_start)
    draft["research_context_meta"] = context_meta
    draft["theme_generation_strategy"] = _normalize_research_strategy(strategy)
    draft["domain_anchor"] = heuristic_baseline.get("domain_anchor") or _domain_anchor(profile, candidate)
    draft["dominant_business_role"] = heuristic_baseline.get("dominant_business_role") or (_dominant_economic_role(profile, candidate) or "unclear")
    draft["candidate_theme_ideas"] = list(heuristic_baseline.get("candidate_theme_ideas") or [])
    draft["matched_theme_candidates"] = list(heuristic_baseline.get("matched_theme_candidates") or [])
    timing = dict(heuristic_baseline.get("research_timing_summary") or {})
    timing.update(
        {
            "baseline_total_ms": baseline_ms,
            "catalog_prefilter_ms": prefilter_ms,
            "ai_request_ms": ai_request_ms,
            "ai_normalize_ms": ai_normalize_ms,
            "merge_ms": merge_ms,
            "model_attempts": 1,
            "strategy_total_ms": _elapsed_ms(ai_total_start),
        }
    )
    draft["research_timing_summary"] = timing
    if not draft["suggested_existing_themes"] and not draft["possible_new_theme"]:
        draft["caveats"] = list(draft.get("caveats") or [])
        draft["caveats"].append("AI did not find a strong grounded theme fit in the current governed catalog.")
    return draft


def generate_scanner_research_draft(conn, ticker: str, *, strategy: str = "legacy_direct_match") -> dict[str, object]:
    total_start = _now_perf()
    candidate_start = _now_perf()
    candidate = _candidate_context(conn, ticker)
    candidate_ms = _elapsed_ms(candidate_start)
    catalog_start = _now_perf()
    catalog = theme_catalog_context(conn)
    catalog_ms = _elapsed_ms(catalog_start)
    preprocess_start = _now_perf()
    preprocessed_catalog = _preprocessed_catalog(catalog)
    catalog_preprocess_ms = _elapsed_ms(preprocess_start)
    profile_start = _now_perf()
    profile = _load_company_profile_with_cache(candidate["ticker"])
    profile_ms = _elapsed_ms(profile_start)
    generated_at = datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ")
    normalized_strategy = _normalize_research_strategy(strategy)

    research_mode = "heuristic_fallback"
    fallback_reason = None
    research_error = None
    try:
        draft = _ai_research_draft_for_strategy(candidate, preprocessed_catalog, profile, strategy=normalized_strategy)
        research_mode = "openai"
    except Exception as exc:
        draft = _baseline_research_draft_for_strategy(candidate, preprocessed_catalog, profile, strategy=normalized_strategy)
        research_error = _extract_openai_error_details(exc)
        fallback_reason = _format_openai_error_summary(research_error)

    draft["ticker"] = candidate["ticker"]
    draft["generated_at"] = generated_at
    draft["source"] = "scanner_audit"
    draft["research_mode"] = research_mode
    draft["theme_generation_strategy"] = normalized_strategy
    timing = dict(draft.get("research_timing_summary") or {})
    timing.update(
        {
            "candidate_context_ms": candidate_ms,
            "catalog_query_ms": catalog_ms,
            "catalog_preprocess_ms": catalog_preprocess_ms,
            "profile_lookup_ms": profile_ms,
            "total_ms": _elapsed_ms(total_start),
        }
    )
    draft["research_timing_summary"] = timing
    if fallback_reason:
        draft["fallback_reason"] = fallback_reason
    if research_error:
        draft["research_error"] = research_error
    return draft


def get_or_create_scanner_research_draft(
    conn,
    ticker: str,
    existing_draft: dict[str, object] | None = None,
    *,
    force_refresh: bool = False,
    strategy: str = "legacy_direct_match",
) -> tuple[dict[str, object], bool]:
    normalized_ticker = str(ticker or "").strip().upper()
    normalized_strategy = _normalize_research_strategy(strategy)
    if (
        not force_refresh
        and isinstance(existing_draft, dict)
        and str(existing_draft.get("ticker") or "").strip().upper() == normalized_ticker
        and _normalize_research_strategy(existing_draft.get("theme_generation_strategy")) == normalized_strategy
    ):
        return existing_draft, True
    return generate_scanner_research_draft(conn, normalized_ticker, strategy=normalized_strategy), False
