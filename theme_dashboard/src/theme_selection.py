from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable


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


@dataclass(frozen=True)
class ThemePickerOption:
    theme_id: int
    name: str
    category: str
    label: str


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


def _theme_sort_key(option: ThemePickerOption) -> tuple[str, str, int]:
    return option.name.casefold(), option.category.casefold(), int(option.theme_id)


def _iter_theme_records(theme_rows) -> Iterable[tuple[int, str, str]]:
    if hasattr(theme_rows, "iterrows"):
        for _, row in theme_rows.iterrows():
            yield int(row["id"]), str(row["name"]), str(row["category"])
        return

    for row in theme_rows:
        yield int(row["id"]), str(row["name"]), str(row["category"])


def build_theme_picker_options(theme_rows) -> list[ThemePickerOption]:
    records = sorted(
        _iter_theme_records(theme_rows),
        key=lambda item: (item[1].casefold(), item[2].casefold(), int(item[0])),
    )
    base_label_by_id: dict[int, str] = {}
    base_counts: dict[str, int] = {}
    for theme_id, name, category in records:
        base_label = f"{name} ({category})"
        base_label_by_id[int(theme_id)] = base_label
        base_counts[base_label] = base_counts.get(base_label, 0) + 1

    return [
        ThemePickerOption(
            theme_id=int(theme_id),
            name=name,
            category=category,
            label=(
                f"{base_label_by_id[int(theme_id)]} [#{int(theme_id)}]"
                if base_counts.get(base_label_by_id[int(theme_id)], 0) > 1
                else base_label_by_id[int(theme_id)]
            ),
        )
        for theme_id, name, category in records
    ]


def build_theme_option_maps(theme_rows) -> tuple[dict[str, int], dict[int, str], dict[str, int]]:
    picker_options = build_theme_picker_options(theme_rows)
    label_by_id = {option.theme_id: option.label for option in picker_options}
    options = {option.label: option.theme_id for option in picker_options}
    return options, label_by_id, dict(options)


def theme_picker_option_search_rank(option: ThemePickerOption, query: str) -> tuple[int, str, str, int]:
    normalized_query = _normalized_search_text(query)
    if not normalized_query:
        return 0, option.name.casefold(), option.category.casefold(), int(option.theme_id)

    name = option.name.casefold()
    label = option.label.casefold()
    theme_id = str(int(option.theme_id))
    if name == normalized_query or label == normalized_query or theme_id == normalized_query.removeprefix("#"):
        rank = 0
    elif name.startswith(normalized_query) or label.startswith(normalized_query):
        rank = 1
    elif normalized_query in name or normalized_query in label or normalized_query in theme_id:
        rank = 2
    else:
        rank = 3
    return rank, option.name.casefold(), option.category.casefold(), int(option.theme_id)


def ranked_theme_picker_options(options: list[ThemePickerOption], query: str) -> list[ThemePickerOption]:
    normalized_query = _normalized_search_text(query)
    ranked_options = sorted(options, key=lambda option: theme_picker_option_search_rank(option, normalized_query))
    if not normalized_query:
        return ranked_options
    return [option for option in ranked_options if theme_picker_option_search_rank(option, normalized_query)[0] < 3]


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
    if normalized_query in normalized_label:
        return 2, normalized_label
    return 3, normalized_label


def ranked_theme_labels_for_search(labels: list[str], query: str) -> list[str]:
    normalized_query = _normalized_search_text(query)
    ranked_labels = sorted(labels, key=lambda label: theme_label_search_rank(label, normalized_query))
    if not normalized_query:
        return ranked_labels
    return [label for label in ranked_labels if theme_label_search_rank(label, normalized_query)[0] < 3]


def prepare_replaceable_selectbox_widget_key(
    session_state,
    base_key: str,
    options: list[str],
    current_value: str | None,
    *,
    preserve_valid_widget_value: bool = False,
) -> str:
    version_key = f"{base_key}__widget_version"
    widget_key = f"{base_key}__widget__{int(session_state.get(version_key, 0))}"
    current_label = str(current_value or "")
    widget_value = session_state.get(widget_key)
    if preserve_valid_widget_value and widget_value in options and widget_value != current_label:
        return widget_key
    if current_label in options and widget_value != current_label:
        session_state[widget_key] = current_label
    elif session_state.get(widget_key) not in {None, *options}:
        session_state.pop(widget_key, None)
    return widget_key


def rotate_replaceable_selectbox_widget(session_state, base_key: str) -> None:
    version_key = f"{base_key}__widget_version"
    session_state[version_key] = int(session_state.get(version_key, 0)) + 1


def render_searchable_theme_picker(
    st_runtime,
    session_state,
    *,
    label: str,
    options: list[ThemePickerOption],
    base_key: str,
    current_label: str | None = None,
    search_label: str = "Search themes",
    search_placeholder: str = "Type theme name, category, or id",
    select_placeholder: str = "Select a theme",
) -> str | None:
    search_key = f"{base_key}__search"
    last_search_key = f"{base_key}__last_search"
    select_base_key = f"{base_key}__select"

    query = st_runtime.text_input(
        search_label,
        key=search_key,
        placeholder=search_placeholder,
    )
    if str(session_state.get(last_search_key, "")) != str(query or ""):
        session_state[last_search_key] = str(query or "")
        rotate_replaceable_selectbox_widget(session_state, select_base_key)

    visible_options = ranked_theme_picker_options(options, str(query or ""))
    visible_labels = [option.label for option in visible_options]
    if not visible_labels:
        st_runtime.caption("No matching themes.")
        return None

    widget_current_label = current_label if not str(query or "").strip() else None
    widget_key = prepare_replaceable_selectbox_widget_key(
        session_state,
        select_base_key,
        visible_labels,
        widget_current_label,
        preserve_valid_widget_value=False,
    )
    return st_runtime.selectbox(
        label,
        visible_labels,
        index=None,
        placeholder=f"{select_placeholder} ({len(visible_labels)} shown)",
        key=widget_key,
    )
