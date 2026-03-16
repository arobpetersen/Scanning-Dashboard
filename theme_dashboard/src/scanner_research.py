from __future__ import annotations

import json
import re
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
- suggested_existing_themes (array of objects with theme_id, theme_name, category, why_it_might_fit)
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

_PROFILE_CACHE: dict[str, dict[str, object]] = {}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


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
    return None if normalized in empty_markers else text


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


def _representative_ticker_role_hints(tickers: list[object]) -> set[str]:
    hints: set[str] = set()
    joined = " ".join(str(value or "").strip().upper() for value in tickers if str(value or "").strip())
    if any(symbol in joined for symbol in ["AAOI", "CIEN", "LITE", "FN", "INFN"]):
        hints.add("optical_networking")
    if any(symbol in joined for symbol in ["AXT", "CCJ", "WOLF", "COHR", "ONTO"]):
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


def _candidate_roles(profile: dict[str, object], candidate: dict[str, object]) -> set[str]:
    return _infer_signals(
        ROLE_KEYWORDS,
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
    )


def _candidate_end_markets(profile: dict[str, object], candidate: dict[str, object]) -> set[str]:
    return _infer_signals(
        END_MARKET_KEYWORDS,
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
    )


def _theme_roles(theme_entry: dict[str, object]) -> set[str]:
    return _infer_signals(
        ROLE_KEYWORDS,
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    ) | _representative_ticker_role_hints(list(theme_entry.get("representative_tickers") or []))


def _theme_end_markets(theme_entry: dict[str, object]) -> set[str]:
    return _infer_signals(
        END_MARKET_KEYWORDS,
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    ) | _representative_ticker_market_hints(list(theme_entry.get("representative_tickers") or []))


def _looks_generic_theme(theme_entry: dict[str, object]) -> bool:
    theme_text = " ".join(
        [
            _normalize_text(theme_entry.get("theme_name")).lower(),
            _normalize_text(theme_entry.get("category")).lower(),
            _normalize_text(theme_entry.get("theme_description")).lower(),
        ]
    )
    return any(token in theme_text for token in ["software", "cloud", "technology", "tech", "platform"])


def _candidate_concepts(profile: dict[str, object], candidate: dict[str, object]) -> set[str]:
    return _infer_concepts(
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
    )


def _format_signal_names(values: set[str], display_map: dict[str, str], limit: int = 2) -> str:
    return ", ".join(display_map.get(value, value.replace("_", " ")) for value in sorted(values)[:limit])


def _dominant_role(profile: dict[str, object], candidate: dict[str, object]) -> str:
    roles = _candidate_roles(profile, candidate)
    if roles:
        return sorted(roles)[0]
    return ""


def _theme_fit_details(theme_entry: dict[str, object], profile: dict[str, object], candidate: dict[str, object]) -> dict[str, object]:
    theme_tokens = _tokenize(theme_entry.get("theme_name"), theme_entry.get("category"), theme_entry.get("theme_description"))
    profile_tokens = _tokenize(
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
    )
    candidate_concepts = _candidate_concepts(profile, candidate)
    theme_concepts = _theme_concepts(theme_entry)
    candidate_roles = _candidate_roles(profile, candidate)
    candidate_markets = _candidate_end_markets(profile, candidate)
    theme_roles = _theme_roles(theme_entry)
    theme_markets = _theme_end_markets(theme_entry)
    specific_overlap = sorted((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS)
    generic_overlap = sorted((candidate_concepts & theme_concepts) & GENERIC_CONCEPTS)
    role_overlap = sorted(candidate_roles & theme_roles)
    market_overlap = sorted(candidate_markets & theme_markets)
    token_overlap = sorted((theme_tokens & profile_tokens) - STOPWORDS)

    score = len(role_overlap) * 14 + len(specific_overlap) * 7 + len(market_overlap) * 3 + len(generic_overlap) + min(2, len(token_overlap))
    direct_role_fit = bool(role_overlap)
    indirect_only_fit = not direct_role_fit and bool(market_overlap or generic_overlap or specific_overlap)
    if theme_roles and candidate_roles and not role_overlap:
        candidate_families = {ROLE_FAMILY.get(role) for role in candidate_roles}
        theme_families = {ROLE_FAMILY.get(role) for role in theme_roles}
        if candidate_families.isdisjoint(theme_families):
            score -= 4 if market_overlap else 10
        else:
            score -= 4
    if not role_overlap and market_overlap:
        score -= 4
    if _looks_generic_theme(theme_entry) and not role_overlap:
        score -= 4
    if not role_overlap and not specific_overlap and generic_overlap and len(token_overlap) <= 1:
        score -= 3
    if not role_overlap and not specific_overlap and not market_overlap:
        score = min(score, 1)

    if score <= 0:
        why = ""
    elif role_overlap:
        why = "Direct business-role fit on " + _format_signal_names(set(role_overlap), ROLE_DISPLAY_NAMES)
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


def _theme_fit_score(theme_entry: dict[str, object], profile: dict[str, object], candidate: dict[str, object]) -> tuple[int, str]:
    details = _theme_fit_details(theme_entry, profile, candidate)
    return int(details["score"]), str(details["why"])


def _candidate_new_theme_label(profile: dict[str, object], candidate: dict[str, object]) -> str | None:
    roles = _candidate_roles(profile, candidate)
    markets = _candidate_end_markets(profile, candidate)
    description = " ".join(
        [
            _normalize_text(profile.get("company_name")).lower(),
            _normalize_text(profile.get("description")).lower(),
            _normalize_text(profile.get("sic_description")).lower(),
            _normalize_text(candidate.get("recommendation_reason")).lower(),
        ]
    )
    if "optical_networking" in roles:
        if "ai" in markets and "fiber" in description:
            return "AI Fiber Optics"
        if "data_center" in markets:
            return "Data Center Optics"
        if "interconnect" in description:
            return "Optical Interconnects"
        return "Optical Networking"
    if "semiconductor_materials" in roles:
        if "compound semiconductor" in description or "gallium arsenide" in description or "indium phosphide" in description:
            return "Compound Semiconductor Materials"
        if "substrate" in description or "substrates" in description:
            return "Semiconductor Substrates"
        if "specialty" in description:
            return "Specialty Semiconductor Materials"
        return "Semiconductor Materials"
    dominant_role = _dominant_role(profile, candidate)
    if dominant_role:
        return ROLE_NEW_LABELS.get(dominant_role)
    concept = _concept_strength(_candidate_concepts(profile, candidate))
    if concept:
        return THEME_NEW_LABELS.get(concept) or _normalize_text(profile.get("sic_description")).title() or None
    return None


def _supports_distinct_new_theme_label(profile: dict[str, object], candidate: dict[str, object]) -> bool:
    role = _dominant_role(profile, candidate)
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
    if roles:
        if "server_systems" in roles and ({"ai", "data_center"} & markets):
            return "appears to serve AI compute infrastructure and data-center end markets through server systems"
        role_text = _format_signal_names(roles, ROLE_DISPLAY_NAMES)
        market_text = _format_signal_names(markets, END_MARKET_DISPLAY_NAMES) if markets else ""
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
    ranked: list[tuple[int, int, dict[str, object]]] = []
    adjacent: list[tuple[int, int, dict[str, object]]] = []
    for entry in catalog:
        fit_details = _theme_fit_details(entry, profile, candidate)
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
    candidate_concepts = _candidate_concepts(profile, candidate)
    candidate_roles = _candidate_roles(profile, candidate)
    candidate_markets = _candidate_end_markets(profile, candidate)
    for entry in catalog:
        theme_concepts = _theme_concepts(entry)
        theme_roles = _theme_roles(entry)
        theme_markets = _theme_end_markets(entry)
        if (
            not (candidate_roles & theme_roles)
            and not ((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS)
            and ((candidate_concepts & theme_concepts) & GENERIC_CONCEPTS)
            and str(entry.get("theme_name") or "") not in broad_alternatives
        ):
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        elif _looks_generic_theme(entry) and str(entry.get("theme_name") or "") not in broad_alternatives:
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        fit_details = _theme_fit_details(entry, profile, candidate)
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
        "suggested_existing_themes": suggested_existing,
        "possible_new_theme": possible_new_theme,
        "confidence": confidence,
        "rationale": " ".join(rationale_parts),
        "caveats": caveats,
        "recommended_action": recommended_action,
    }


def _call_openai_research(api_key: str, context: dict[str, object]) -> dict[str, object]:
    payload = {
        "model": AI_MODEL,
        "max_output_tokens": 550,
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
    by_id = {int(item["theme_id"]): item for item in catalog}
    best: dict[str, object] = {"score": 0, "direct_role_fit": False, "indirect_only_fit": False}
    for suggestion in suggested_existing:
        try:
            theme_id = int(suggestion.get("theme_id"))
        except Exception:
            continue
        entry = by_id.get(theme_id)
        if entry is None:
            continue
        fit_details = _theme_fit_details(entry, profile, candidate)
        if int(fit_details.get("score") or 0) > int(best.get("score") or 0):
            best = fit_details
    return best


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
        "value-chain position",
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
    ]
    return any(marker in normalized for marker in precision_markers) and any(marker in normalized for marker in adjacency_markers)


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

    heuristic_prefers_new_theme = (
        _normalize_action(heuristic_draft.get("recommended_action")) == "consider_new_theme"
        and _normalize_text(heuristic_draft.get("possible_new_theme"))
    )
    candidate_new_theme = _candidate_new_theme_label(profile, candidate)
    supports_distinct_new_theme = _supports_distinct_new_theme_label(profile, candidate)
    ai_rationale_signals_gap = _rationale_signals_precision_gap(ai_rationale)
    merged_rationale_signals_gap = _rationale_signals_precision_gap(str(merged.get("rationale") or ""))
    best_ai_existing_fit = _best_suggested_theme_fit_details(
        list(merged.get("suggested_existing_themes") or []),
        catalog,
        profile,
        candidate,
    )
    should_promote_new_theme = (
        bool(candidate_new_theme)
        and supports_distinct_new_theme
        and (
            heuristic_prefers_new_theme
            or ai_rationale_signals_gap
            or merged_rationale_signals_gap
        )
        and (
            not merged.get("suggested_existing_themes")
            or not bool(best_ai_existing_fit.get("direct_role_fit"))
            or int(best_ai_existing_fit.get("score") or 0) < 12
        )
    )
    if should_promote_new_theme:
        merged["possible_new_theme"] = (
            _normalize_optional_theme_label(ai_draft.get("possible_new_theme"))
            or _normalize_optional_theme_label(heuristic_draft.get("possible_new_theme"))
            or candidate_new_theme
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
            }
        )
    return normalized[:3]


def _ai_research_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    api_key = openai_api_key()
    if not api_key:
        raise ValueError(f"{OPENAI_API_KEY_ENV} is not set.")
    heuristic_baseline = _heuristic_research_draft(candidate, catalog, profile)
    filtered_catalog, context_meta = _prefilter_ai_theme_catalog(candidate, catalog, profile)
    context = {
        "candidate": candidate,
        "company_profile": profile,
        "governed_theme_catalog": filtered_catalog,
        "heuristic_baseline": {
            "suggested_existing_themes": heuristic_baseline.get("suggested_existing_themes") or [],
            "possible_new_theme": heuristic_baseline.get("possible_new_theme"),
            "recommended_action": heuristic_baseline.get("recommended_action"),
            "rationale_summary": heuristic_baseline.get("rationale"),
        },
    }
    context_meta["estimated_context_chars"] = _estimate_context_size_chars(context)
    raw = _call_openai_research(api_key, context)
    suggested_existing = _normalize_ai_theme_suggestions(raw.get("suggested_existing_themes"), catalog)
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
    draft = _merge_ai_with_heuristic_draft(ai_draft, heuristic_baseline, catalog, profile, candidate)
    draft["research_context_meta"] = context_meta
    if not draft["suggested_existing_themes"] and not draft["possible_new_theme"]:
        draft["caveats"] = list(draft.get("caveats") or [])
        draft["caveats"].append("AI did not find a strong grounded theme fit in the current governed catalog.")
    return draft


def generate_scanner_research_draft(conn, ticker: str) -> dict[str, object]:
    candidate = _candidate_context(conn, ticker)
    catalog = theme_catalog_context(conn)
    profile = _load_company_profile_with_cache(candidate["ticker"])
    generated_at = datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ")

    research_mode = "heuristic_fallback"
    fallback_reason = None
    research_error = None
    try:
        draft = _ai_research_draft(candidate, catalog, profile)
        research_mode = "openai"
    except Exception as exc:
        draft = _heuristic_research_draft(candidate, catalog, profile)
        research_error = _extract_openai_error_details(exc)
        fallback_reason = _format_openai_error_summary(research_error)

    draft["ticker"] = candidate["ticker"]
    draft["generated_at"] = generated_at
    draft["source"] = "scanner_audit"
    draft["research_mode"] = research_mode
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
) -> tuple[dict[str, object], bool]:
    normalized_ticker = str(ticker or "").strip().upper()
    if (
        not force_refresh
        and isinstance(existing_draft, dict)
        and str(existing_draft.get("ticker") or "").strip().upper() == normalized_ticker
    ):
        return existing_draft, True
    return generate_scanner_research_draft(conn, normalized_ticker), False
