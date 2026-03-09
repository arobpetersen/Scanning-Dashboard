from __future__ import annotations

import json
from dataclasses import asdict

import requests

from .config import AI_MAX_PROPOSALS, AI_MODEL, OPENAI_API_KEY_ENV
from .suggestions_service import SuggestionPayload, create_suggestion


SYSTEM_PROMPT = """You are an equity-theme taxonomy assistant.
Return STRICT JSON list only (no markdown) with fields:
- suggestion_type (one of add_ticker_to_theme, remove_ticker_from_theme, create_theme, rename_theme, review_theme)
- rationale (required, concise evidence-based)
- priority (low|medium|high)
- proposed_theme_name (optional)
- proposed_ticker (optional)
- existing_theme_id (optional integer)

Rules:
- Provide only actionable proposals with clear rationale.
- Avoid duplicate or weak suggestions.
- No direct apply; suggestions will be reviewed by humans.
"""


def _call_openai(api_key: str, prompt: str, context: dict, max_proposals: int) -> list[dict]:
    url = "https://api.openai.com/v1/responses"
    payload = {
        "model": AI_MODEL,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Generate up to {max_proposals} proposals.\n"
                    f"User instruction: {prompt}\n"
                    f"Context JSON: {json.dumps(context)[:12000]}"
                ),
            },
        ],
        "text": {"format": {"type": "json_object"}},
    }
    r = requests.post(url, headers={"Authorization": f"Bearer {api_key}"}, json=payload, timeout=45)
    r.raise_for_status()
    data = r.json()
    text = data.get("output_text", "")
    parsed = json.loads(text)
    items = parsed.get("proposals") if isinstance(parsed, dict) else parsed
    if not isinstance(items, list):
        return []
    return items


def generate_ai_suggestions(conn, prompt: str, context: dict, max_proposals: int = AI_MAX_PROPOSALS) -> dict:
    import os

    api_key = os.getenv(OPENAI_API_KEY_ENV, "").strip()
    if not api_key:
        raise ValueError(f"{OPENAI_API_KEY_ENV} is not set. Configure it to run AI proposal generation.")

    raw = _call_openai(api_key, prompt, context, max_proposals=max_proposals)

    created = 0
    duplicates = 0
    invalid = 0
    errors: list[str] = []

    for item in raw[:max_proposals]:
        try:
            payload = SuggestionPayload(
                suggestion_type=str(item.get("suggestion_type", "review_theme")),
                source="ai_proposal",
                rationale=str(item.get("rationale", "")).strip(),
                proposed_theme_name=item.get("proposed_theme_name"),
                proposed_ticker=item.get("proposed_ticker"),
                existing_theme_id=int(item["existing_theme_id"]) if item.get("existing_theme_id") not in (None, "") else None,
                priority=str(item.get("priority", "medium")),
            )
            create_suggestion(conn, payload)
            created += 1
        except Exception as exc:
            msg = str(exc).lower()
            if "equivalent pending suggestion" in msg:
                duplicates += 1
            else:
                invalid += 1
                errors.append(str(exc))

    return {
        "attempted": len(raw[:max_proposals]),
        "created": created,
        "duplicates": duplicates,
        "invalid": invalid,
        "errors": errors[:10],
    }
