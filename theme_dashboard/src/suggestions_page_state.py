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
