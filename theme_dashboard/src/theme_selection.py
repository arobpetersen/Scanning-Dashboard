from __future__ import annotations

import re


SELECTED_THEME_ID_KEY = "selected_theme_id"
SELECTED_THEME_LABEL_KEY = "explore_theme"
SELECTED_THEME_SOURCE_KEY = "selected_theme_source"


SELECTION_SOURCE_LABELS = {
    "current_leadership": "Current Market Leadership",
    "current_top_1w": "Current Top Themes 1W",
    "current_top_1m": "Current Top Themes 1M",
    "top_1w": "Top 10 1W",
    "top_1m": "Top 10 1M",
    "manual_dropdown": "Manual dropdown",
    "default": "Default theme",
    "historical_overview": "Historical overview",
    "historical_top_momentum": "Historical top momentum",
    "historical_signal": "Historical signal",
    "historical_table": "Historical table",
    "historical_detail": "Historical detail",
    "health_theme": "Health theme",
}


def resolve_theme_selection(
    selected_theme_id: int | None,
    selected_theme_label: str | None,
    label_by_id: dict[int, str],
    id_by_label: dict[str, int],
    fallback_theme_id: int,
) -> tuple[int, str]:
    if selected_theme_id in label_by_id:
        return int(selected_theme_id), label_by_id[int(selected_theme_id)]
    if selected_theme_label in id_by_label:
        label = str(selected_theme_label)
        return int(id_by_label[label]), label
    return int(fallback_theme_id), label_by_id[int(fallback_theme_id)]


def describe_selection_source(source: str | None) -> str:
    return SELECTION_SOURCE_LABELS.get(str(source or ""), "Theme selector")


def should_apply_selection_token(selection_token: str | None, last_applied_token: str | None) -> bool:
    token = str(selection_token or "").strip()
    if not token:
        return False
    return token != str(last_applied_token or "").strip()


def set_theme_selection_state(session_state, theme_id: int, label: str, source: str) -> None:
    session_state[SELECTED_THEME_ID_KEY] = int(theme_id)
    session_state[SELECTED_THEME_LABEL_KEY] = str(label)
    session_state[SELECTED_THEME_SOURCE_KEY] = str(source)


def _normalized_search_text(value: object) -> str:
    return str(value or "").strip().casefold()


def _significant_label_tokens(label: str) -> list[str]:
    return [token for token in re.findall(r"[A-Za-z0-9]+", str(label).casefold()) if len(token) >= 3]


def _primary_label_text(label: str) -> str:
    return re.split(r"\s*[\(\[]", str(label), maxsplit=1)[0].strip().casefold()


def theme_label_search_rank(label: str, query: str) -> tuple[int, str]:
    normalized_label = _normalized_search_text(label)
    primary_label = _primary_label_text(label)
    normalized_query = _normalized_search_text(query)
    if not normalized_query:
        return 0, normalized_label
    if normalized_label == normalized_query or primary_label == normalized_query:
        return 0, normalized_label
    if normalized_label.startswith(normalized_query) or primary_label.startswith(normalized_query):
        return 1, normalized_label
    if any(token.startswith(normalized_query) for token in _significant_label_tokens(label)):
        return 2, normalized_label
    if normalized_query in normalized_label:
        return 3, normalized_label
    return 4, normalized_label


def ranked_theme_labels_for_search(labels: list[str], query: str) -> list[str]:
    normalized_query = _normalized_search_text(query)
    ranked_labels = sorted(labels, key=lambda label: theme_label_search_rank(label, normalized_query))
    if not normalized_query:
        return ranked_labels
    return [label for label in ranked_labels if theme_label_search_rank(label, normalized_query)[0] < 4]


def prepare_replaceable_selectbox_widget_key(session_state, base_key: str, options: list[str], current_value: str | None) -> str:
    version_key = f"{base_key}__widget_version"
    widget_key = f"{base_key}__widget__{int(session_state.get(version_key, 0))}"
    current_label = str(current_value or "")
    if current_label in options and session_state.get(widget_key) != current_label:
        session_state[widget_key] = current_label
    elif session_state.get(widget_key) not in {None, *options}:
        session_state.pop(widget_key, None)
    return widget_key


def rotate_replaceable_selectbox_widget(session_state, base_key: str) -> None:
    version_key = f"{base_key}__widget_version"
    session_state[version_key] = int(session_state.get(version_key, 0)) + 1
