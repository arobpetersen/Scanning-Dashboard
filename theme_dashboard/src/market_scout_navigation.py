from __future__ import annotations

from .theme_selection import build_theme_picker_options


def build_market_scout_theme_lookup(theme_rows) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    for option in build_theme_picker_options(theme_rows):
        payload = {"theme_id": int(option.theme_id), "label": option.label, "name": option.name}
        lookup.setdefault(option.name.casefold(), payload)
        lookup.setdefault(option.label.casefold(), payload)
    return lookup


def market_scout_theme_candidates(
    item: dict[str, object],
    theme_lookup: dict[str, dict[str, object]],
    *,
    limit: int = 4,
) -> list[dict[str, object]]:
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    theme_names: list[str] = []
    primary = str(metadata.get("theme") or "").strip()
    if primary:
        theme_names.append(primary)
    theme_names.extend(str(theme).strip() for theme in metadata.get("themes") or [] if str(theme).strip())

    candidates: list[dict[str, object]] = []
    seen_ids: set[int] = set()
    for theme_name in theme_names:
        candidate = theme_lookup.get(theme_name.casefold())
        if not candidate:
            continue
        theme_id = int(candidate["theme_id"])
        if theme_id in seen_ids:
            continue
        seen_ids.add(theme_id)
        candidates.append(candidate)
        if len(candidates) >= int(limit):
            break
    return candidates
