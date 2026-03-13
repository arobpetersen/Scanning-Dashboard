from __future__ import annotations


def extract_selected_row(event) -> int | None:
    """Best-effort extraction of a selected row index across Streamlit event payload shapes."""
    selection = {}
    if isinstance(event, dict):
        selection = event.get("selection", {}) or {}
    elif hasattr(event, "selection"):
        selection = event.selection

    rows = selection.get("rows", []) if isinstance(selection, dict) else getattr(selection, "rows", [])
    for row in rows or []:
        if row is not None:
            try:
                return int(row)
            except (TypeError, ValueError):
                continue

    cells = selection.get("cells", []) if isinstance(selection, dict) else getattr(selection, "cells", [])
    for cell in cells or []:
        row_value = None
        if isinstance(cell, dict):
            row_value = cell.get("row")
        elif isinstance(cell, (tuple, list)) and cell:
            row_value = cell[0]
        elif hasattr(cell, "row"):
            row_value = getattr(cell, "row")
        if row_value is None:
            continue
        try:
            return int(row_value)
        except (TypeError, ValueError):
            continue
    return None
