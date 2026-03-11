from __future__ import annotations


SELECTION_SOURCE_LABELS = {
    "top_1w": "Top 10 1W",
    "top_1m": "Top 10 1M",
    "manual_dropdown": "Manual dropdown",
    "default": "Default theme",
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
