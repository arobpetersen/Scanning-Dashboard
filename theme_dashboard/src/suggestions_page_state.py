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


def normalize_theme_id_list(values: object, valid_ids: set[int] | None = None) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for value in list(values or []):
        try:
            theme_id = int(value)
        except Exception:
            continue
        if valid_ids is not None and theme_id not in valid_ids:
            continue
        if theme_id in seen:
            continue
        normalized.append(theme_id)
        seen.add(theme_id)
    return normalized


def add_theme_to_selected_existing(current_ids: object, theme_id: object, valid_ids: set[int] | None = None) -> list[int]:
    normalized = normalize_theme_id_list(current_ids, valid_ids)
    try:
        selected_theme_id = int(theme_id)
    except Exception:
        return normalized
    if valid_ids is not None and selected_theme_id not in valid_ids:
        return normalized
    if selected_theme_id in normalized:
        return normalized
    return normalized + [selected_theme_id]


def split_selected_existing_theme_ids(selected_ids: object, suggested_ids: object) -> tuple[list[int], list[int]]:
    normalized_selected = normalize_theme_id_list(selected_ids)
    suggested_set = set(normalize_theme_id_list(suggested_ids))
    selected_suggested = [theme_id for theme_id in normalized_selected if theme_id in suggested_set]
    custom_existing = [theme_id for theme_id in normalized_selected if theme_id not in suggested_set]
    return selected_suggested, custom_existing


def merge_suggested_and_custom_theme_ids(
    checked_suggested_ids: object,
    selected_existing_ids: object,
    suggested_ids: object,
    valid_ids: set[int] | None = None,
) -> list[int]:
    normalized_checked = normalize_theme_id_list(checked_suggested_ids, valid_ids)
    _, custom_existing = split_selected_existing_theme_ids(
        normalize_theme_id_list(selected_existing_ids, valid_ids),
        suggested_ids,
    )
    return normalize_theme_id_list(custom_existing + normalized_checked, valid_ids)


def sync_suggested_theme_checkbox_state(
    selected_existing_ids: object,
    suggested_ids: object,
) -> dict[int, bool]:
    normalized_selected = set(normalize_theme_id_list(selected_existing_ids))
    normalized_suggested = normalize_theme_id_list(suggested_ids)
    return {theme_id: theme_id in normalized_selected for theme_id in normalized_suggested}


def normalize_possible_new_theme_input(value: object) -> str:
    return str(value or "").strip()


def normalize_possible_new_theme_category_input(value: object) -> str:
    return str(value or "").strip()


def split_possible_new_theme_ideas(value: object) -> list[str]:
    raw = normalize_possible_new_theme_input(value)
    if not raw:
        return []
    ideas: list[str] = []
    seen: set[str] = set()
    for part in raw.replace(";", ",").split(","):
        idea = normalize_possible_new_theme_input(part)
        normalized_key = idea.casefold()
        if not idea or normalized_key in seen:
            continue
        ideas.append(idea)
        seen.add(normalized_key)
    return ideas


def join_possible_new_theme_ideas(values: object) -> str:
    ideas: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        idea = normalize_possible_new_theme_input(value)
        normalized_key = idea.casefold()
        if not idea or normalized_key in seen:
            continue
        ideas.append(idea)
        seen.add(normalized_key)
    return ", ".join(ideas)


def sync_generated_theme_idea_checkbox_state(
    current_value: object,
    generated_ideas: object,
) -> dict[str, bool]:
    selected = {idea.casefold() for idea in split_possible_new_theme_ideas(current_value)}
    synced: dict[str, bool] = {}
    for idea in list(generated_ideas or []):
        normalized = normalize_possible_new_theme_input(idea)
        if not normalized or normalized.casefold() in {key.casefold() for key in synced}:
            continue
        synced[normalized] = normalized.casefold() in selected
    return synced


def merge_generated_theme_ideas_with_custom(
    current_value: object,
    checked_generated_ideas: object,
    generated_ideas: object,
) -> str:
    current = split_possible_new_theme_ideas(current_value)
    generated_lookup = {
        normalize_possible_new_theme_input(idea).casefold(): normalize_possible_new_theme_input(idea)
        for idea in list(generated_ideas or [])
        if normalize_possible_new_theme_input(idea)
    }
    checked_lookup = {
        normalize_possible_new_theme_input(idea).casefold(): normalize_possible_new_theme_input(idea)
        for idea in list(checked_generated_ideas or [])
        if normalize_possible_new_theme_input(idea)
    }
    custom = [idea for idea in current if idea.casefold() not in generated_lookup]
    merged_generated = [
        generated_lookup[idea.casefold()]
        for idea in list(generated_ideas or [])
        if normalize_possible_new_theme_input(idea)
        and normalize_possible_new_theme_input(idea).casefold() in checked_lookup
    ]
    return join_possible_new_theme_ideas(merged_generated + custom)


def prepare_possible_new_theme_prefill(
    current_value: object,
    suggested_value: object,
    prior_state: dict[str, object] | None = None,
) -> tuple[str, dict[str, object]]:
    suggested = normalize_possible_new_theme_input(suggested_value)
    current = None if current_value is None else str(current_value)
    state = dict(prior_state or {})
    auto_value = str(state.get("auto_value") or "")
    user_edited = bool(state.get("user_edited"))

    if current is None:
        return suggested, {"auto_value": suggested, "user_edited": False}

    if current != auto_value:
        user_edited = True

    if not user_edited and suggested != auto_value:
        current = suggested
        auto_value = suggested

    if current is None:
        current = suggested
        auto_value = suggested

    return str(current), {"auto_value": auto_value, "user_edited": user_edited}


def prepare_possible_new_theme_category_prefill(
    current_value: object,
    suggested_value: object,
    prior_state: dict[str, object] | None = None,
) -> tuple[str, dict[str, object]]:
    suggested = normalize_possible_new_theme_category_input(suggested_value)
    current = None if current_value is None else str(current_value)
    state = dict(prior_state or {})
    auto_value = str(state.get("auto_value") or "")
    user_edited = bool(state.get("user_edited"))

    if current is None:
        return suggested, {"auto_value": suggested, "user_edited": False}

    if current != auto_value:
        user_edited = True

    if not user_edited and suggested != auto_value:
        current = suggested
        auto_value = suggested

    if current is None:
        current = suggested
        auto_value = suggested

    return str(current), {"auto_value": auto_value, "user_edited": user_edited}


def finalize_possible_new_theme_state(
    current_value: object,
    state: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized = normalize_possible_new_theme_input(current_value)
    next_state = dict(state or {})
    auto_value = str(next_state.get("auto_value") or "")
    next_state["user_edited"] = normalized != auto_value or bool(next_state.get("forced_user_edited"))
    return next_state


def finalize_possible_new_theme_category_state(
    current_value: object,
    state: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized = normalize_possible_new_theme_category_input(current_value)
    next_state = dict(state or {})
    auto_value = str(next_state.get("auto_value") or "")
    next_state["user_edited"] = normalized != auto_value or bool(next_state.get("forced_user_edited"))
    return next_state


def apply_generated_theme_idea_selection(
    current_value: object,
    selected_idea: object,
    state: dict[str, object] | None = None,
) -> tuple[str, dict[str, object]]:
    normalized = normalize_possible_new_theme_input(selected_idea)
    current = split_possible_new_theme_ideas(current_value)
    normalized_keys = {idea.casefold() for idea in current}
    if normalized:
        if normalized.casefold() in normalized_keys:
            current = [idea for idea in current if idea.casefold() != normalized.casefold()]
        else:
            current.append(normalized)
    updated_value = join_possible_new_theme_ideas(current)
    next_state = dict(state or {})
    next_state["forced_user_edited"] = True
    next_state["user_edited"] = True
    return updated_value, next_state


def apply_generated_theme_idea_checkbox_selection(
    current_value: object,
    checked_generated_ideas: object,
    generated_ideas: object,
    state: dict[str, object] | None = None,
) -> tuple[str, dict[str, object]]:
    updated_value = merge_generated_theme_ideas_with_custom(
        current_value,
        checked_generated_ideas,
        generated_ideas,
    )
    next_state = dict(state or {})
    next_state["forced_user_edited"] = True
    next_state["user_edited"] = True
    return updated_value, next_state


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
            "theme_generation_strategy": draft.get("theme_generation_strategy") or entry.get("theme_generation_strategy"),
            "domain_anchor": draft.get("domain_anchor") or entry.get("domain_anchor"),
            "dominant_business_role": draft.get("dominant_business_role") or entry.get("dominant_business_role"),
        }
    )
    return entry
