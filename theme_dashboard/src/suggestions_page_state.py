from __future__ import annotations


def resolve_active_suggestions_tab(current: object, options: list[str], default: str) -> str:
    normalized_options = [str(option) for option in options]
    if str(current or "") in normalized_options:
        return str(current)
    if default in normalized_options:
        return default
    return normalized_options[0] if normalized_options else ""


def resolve_scanner_audit_ticker(current: object, options: list[str]) -> str:
    normalized_options = [str(option) for option in options]
    current_value = str(current or "")
    if current_value in normalized_options:
        return current_value
    return normalized_options[0] if normalized_options else ""


def normalize_scanner_research_draft_source(value: object) -> str:
    normalized = str(value or "").strip()
    allowed = {"fresh_generation", "forced_regeneration", "reused_session_draft"}
    return normalized if normalized in allowed else "reused_session_draft"


def build_scanner_research_debug_entry(
    ticker: object,
    draft: dict[str, object] | None,
    *,
    prior_debug: dict[str, object] | None = None,
    draft_source: object | None = None,
) -> dict[str, object]:
    draft = draft if isinstance(draft, dict) else {}
    entry = dict(prior_debug or {})
    resolved_source = normalize_scanner_research_draft_source(
        draft_source if draft_source is not None else entry.get("draft_source") or draft.get("draft_source")
    )
    entry.update(
        {
            "ticker": str(ticker or "").strip().upper() or str(draft.get("ticker") or "").strip().upper(),
            "generated_at": draft.get("generated_at") or entry.get("generated_at"),
            "research_mode": draft.get("research_mode") or entry.get("research_mode"),
            "draft_source": resolved_source,
        }
    )
    return entry
