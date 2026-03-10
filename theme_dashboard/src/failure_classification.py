from __future__ import annotations

FAILURE_CATEGORIES = {
    "NO_CANDLES",
    "SYMBOL_NOT_FOUND",
    "RATE_LIMIT",
    "TIMEOUT",
    "AUTH",
    "OTHER",
}


def categorize_failure_message(message: str | None) -> str:
    """Normalize provider/raw error text to deterministic refresh failure categories."""
    msg = (message or "").strip().lower()
    if not msg:
        return "OTHER"

    if "no_candles" in msg or "no candles" in msg or "no daily aggregates" in msg:
        return "NO_CANDLES"
    if "not found" in msg or "unknown symbol" in msg or "invalid symbol" in msg or "does not exist" in msg:
        return "SYMBOL_NOT_FOUND"
    if "rate_limit" in msg or "429" in msg or "too many requests" in msg:
        return "RATE_LIMIT"
    if "timeout" in msg or "timed out" in msg or "connection aborted" in msg:
        return "TIMEOUT"
    if "auth" in msg or "forbidden" in msg or "unauthorized" in msg or "permission" in msg or "api key" in msg or "403" in msg:
        return "AUTH"
    return "OTHER"
