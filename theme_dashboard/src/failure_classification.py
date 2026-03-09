from __future__ import annotations


def categorize_failure_message(message: str | None) -> str:
    msg = (message or "").strip().lower()
    if not msg:
        return "unknown"

    if "rate_limit" in msg or "429" in msg or "too many requests" in msg:
        return "provider_limit"
    if "auth" in msg or "forbidden" in msg or "permission" in msg or "unauthorized" in msg or "api key" in msg:
        return "provider_auth"
    if "timeout" in msg or "service unavailable" in msg or "bad gateway" in msg or "gateway" in msg or "connection" in msg:
        return "provider_outage"
    if "no_candles" in msg or "no candles" in msg:
        return "no_candles"
    if "not found" in msg or "unknown symbol" in msg or "invalid symbol" in msg:
        return "ticker_symbol_issue"
    if "no data" in msg or "missing" in msg or "empty" in msg:
        return "ticker_data_missing"
    return "unknown"
