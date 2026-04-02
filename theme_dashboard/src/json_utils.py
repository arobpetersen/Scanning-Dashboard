from __future__ import annotations

import json


def parse_json_object(raw_value: object) -> dict[str, object]:
    text = str(raw_value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}
