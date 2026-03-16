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


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _compact_error_reason(exc: Exception) -> str:
    text = _normalize_text(exc)
    if not text:
        return "Research generation error."
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"(?i)bearer\s+[a-z0-9_\-\.]+", "bearer [redacted]", text)
    text = re.sub(r"(?i)api[_ -]?key[=:]\s*[^ ,;]+", "api_key=[redacted]", text)
    return text[:140]


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


def _representative_ticker_hints(tickers: list[object]) -> set[str]:
    hints: set[str] = set()
    joined = " ".join(str(value or "").strip().upper() for value in tickers if str(value or "").strip())
    if any(symbol in joined for symbol in ["CRWD", "PANW", "ZS", "FTNT"]):
        hints.add("cybersecurity")
    if any(symbol in joined for symbol in ["NVDA", "AMD", "AVGO", "SMCI", "MU"]):
        hints.update({"ai_compute", "semiconductor"})
    if any(symbol in joined for symbol in ["PLTR", "SNOW", "DDOG"]):
        hints.add("data_analytics")
    if any(symbol in joined for symbol in ["LMT", "NOC", "KTOS", "PLTR"]):
        hints.add("defense")
    if any(symbol in joined for symbol in ["RKLB", "ASTS", "LUNR"]):
        hints.add("space")
    return hints


def _theme_concepts(theme_entry: dict[str, object]) -> set[str]:
    return _infer_concepts(
        theme_entry.get("theme_name"),
        theme_entry.get("category"),
        theme_entry.get("theme_description"),
    ) | _representative_ticker_hints(list(theme_entry.get("representative_tickers") or []))


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
    theme_tokens = _tokenize(theme_entry.get("theme_name"), theme_entry.get("category"), theme_entry.get("theme_description"))
    profile_tokens = _tokenize(
        profile.get("company_name"),
        profile.get("description"),
        profile.get("sic_description"),
        candidate.get("recommendation_reason"),
    )
    candidate_concepts = _candidate_concepts(profile, candidate)
    theme_concepts = _theme_concepts(theme_entry)
    specific_overlap = sorted((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS)
    generic_overlap = sorted((candidate_concepts & theme_concepts) & GENERIC_CONCEPTS)
    token_overlap = sorted((theme_tokens & profile_tokens) - STOPWORDS)

    score = len(specific_overlap) * 8 + len(generic_overlap) * 2 + min(2, len(token_overlap))
    if not specific_overlap and generic_overlap and len(token_overlap) <= 1:
        score -= 2
    if not specific_overlap and not generic_overlap:
        score = min(score, 1)
    if score <= 0:
        return 0, ""
    if specific_overlap:
        why = "Conceptual fit on " + ", ".join(specific_overlap[:2])
    elif generic_overlap:
        why = "Broad fit through " + ", ".join(generic_overlap[:2])
    else:
        why = "Weak text-only fit; treat as tentative."
    return score, why


def _value_chain_summary(profile: dict[str, object], candidate: dict[str, object]) -> str:
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


def _heuristic_research_draft(candidate: dict[str, object], catalog: list[dict[str, object]], profile: dict[str, object]) -> dict[str, object]:
    scored: list[tuple[int, dict[str, object]]] = []
    broad_alternatives: list[str] = []
    candidate_concepts = _candidate_concepts(profile, candidate)
    for entry in catalog:
        theme_concepts = _theme_concepts(entry)
        if (
            not ((candidate_concepts & theme_concepts) - GENERIC_CONCEPTS)
            and ((candidate_concepts & theme_concepts) & GENERIC_CONCEPTS)
            and str(entry.get("theme_name") or "") not in broad_alternatives
        ):
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        elif _looks_generic_theme(entry) and str(entry.get("theme_name") or "") not in broad_alternatives:
            broad_alternatives.append(str(entry.get("theme_name") or ""))
        score, why = _theme_fit_score(entry, profile, candidate)
        if score < 3:
            continue
        scored.append(
            (
                score,
                {
                    "theme_id": int(entry["theme_id"]),
                    "theme_name": str(entry["theme_name"]),
                    "category": str(entry["category"]),
                    "why_it_might_fit": why,
                    "representative_tickers": list(entry.get("representative_tickers") or []),
                },
            )
        )
    scored.sort(key=lambda item: (-item[0], item[1]["theme_name"]))
    strongest_score = scored[0][0] if scored else 0
    score_floor = max(6, strongest_score - 3) if strongest_score else 999
    suggested_existing = [item[1] for item in scored if item[0] >= score_floor][:2]

    possible_similar: list[str] = []
    if strongest_score >= 6:
        for suggestion in suggested_existing:
            for ticker in suggestion.get("representative_tickers") or []:
                symbol = str(ticker).strip().upper()
                if symbol and symbol != candidate["ticker"] and symbol not in possible_similar:
                    possible_similar.append(symbol)
    possible_similar = possible_similar[:3 if strongest_score >= 8 else 2]

    confidence = "low"
    recommended_action = "watch_only"
    possible_new_theme = None
    caveats: list[str] = []
    if suggested_existing and strongest_score >= 6:
        confidence = "medium" if strongest_score < 10 else "high"
        recommended_action = "add_to_existing_theme_review"
    elif _concept_strength(candidate_concepts):
        confidence = "low"
        possible_new_theme = THEME_NEW_LABELS.get(_concept_strength(candidate_concepts)) or _normalize_text(profile.get("sic_description")).title()
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
    ]
    if suggested_existing and strongest_score >= 6:
        rationale_parts.append(
            "Best governed-theme fit: "
            + "; ".join(f"{item['theme_name']} ({item['why_it_might_fit']})" for item in suggested_existing)
        )
        weaker_alternatives = [item[1]["theme_name"] for item in scored[1:3] if item[0] < strongest_score and item[0] < 6]
        if not weaker_alternatives:
            weaker_alternatives = [name for name in broad_alternatives if name not in {item["theme_name"] for item in suggested_existing}][:2]
        if weaker_alternatives:
            rationale_parts.append("Broader alternatives such as " + ", ".join(weaker_alternatives) + " look weaker and remain tentative.")
    elif possible_new_theme:
        rationale_parts.append(f"No strong governed-theme match stood out, so a tentative new-theme direction is {possible_new_theme}.")
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
        "input": [
            {"role": "system", "content": RESEARCH_DRAFT_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Generate one advisory research draft for the scanner candidate using the provided context only. "
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
    context = {
        "candidate": candidate,
        "company_profile": profile,
        "governed_theme_catalog": catalog,
    }
    raw = _call_openai_research(api_key, context)
    suggested_existing = _normalize_ai_theme_suggestions(raw.get("suggested_existing_themes"), catalog)
    draft = {
        "ticker": candidate["ticker"],
        "company_name": _normalize_text(raw.get("company_name")) or _normalize_text(profile.get("company_name")) or candidate["ticker"],
        "short_company_description": _normalize_text(raw.get("short_company_description")) or _normalize_text(profile.get("description")) or "No verified company description available.",
        "possible_similar_tickers": [str(value).strip().upper() for value in raw.get("possible_similar_tickers") or [] if str(value).strip()][:5],
        "suggested_existing_themes": suggested_existing,
        "possible_new_theme": _normalize_text(raw.get("possible_new_theme")) or None,
        "confidence": _normalize_text(raw.get("confidence")) or "low",
        "rationale": _normalize_text(raw.get("rationale")) or "AI draft did not provide a rationale.",
        "caveats": [str(value).strip() for value in raw.get("caveats") or [] if str(value).strip()],
        "recommended_action": _normalize_text(raw.get("recommended_action")) or "watch_only",
    }
    if not draft["suggested_existing_themes"] and not draft["possible_new_theme"]:
        draft["caveats"].append("AI did not find a strong grounded theme fit in the current governed catalog.")
    return draft


def generate_scanner_research_draft(conn, ticker: str) -> dict[str, object]:
    candidate = _candidate_context(conn, ticker)
    catalog = theme_catalog_context(conn)
    profile = _load_company_profile(candidate["ticker"])
    generated_at = datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ")

    research_mode = "heuristic_fallback"
    fallback_reason = None
    try:
        draft = _ai_research_draft(candidate, catalog, profile)
        research_mode = "openai"
    except Exception as exc:
        draft = _heuristic_research_draft(candidate, catalog, profile)
        fallback_reason = _compact_error_reason(exc)

    draft["ticker"] = candidate["ticker"]
    draft["generated_at"] = generated_at
    draft["source"] = "scanner_audit"
    draft["research_mode"] = research_mode
    if fallback_reason:
        draft["fallback_reason"] = fallback_reason
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
