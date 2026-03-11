import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "theme_dashboard.duckdb"
SEED_PATH = BASE_DIR / "themes_seed_structured.json"

DEFAULT_PROVIDER = "mock"
STALE_DATA_HOURS = 24
REFRESH_STALE_TIMEOUT_MINUTES = 30
LIVE_RATE_LIMIT_STOP_THRESHOLD = 5
FINNHUB_API_KEY_ENV = "FINNHUB_API_KEY"

# Compatibility constants used by pages/engines (no schema dependency)
LIVE_QUOTE_PROFILE_SOURCE = "finnhub"
LIVE_HISTORICAL_SOURCE = "finnhub"
MASSIVE_API_KEY_ENV = FINNHUB_API_KEY_ENV
AI_MODEL = "gpt-5-mini"
AI_MAX_PROPOSALS = 8
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
RULE_LOW_CONSTITUENT_THRESHOLD = 3
RULE_MAX_SUGGESTIONS_PER_RULE = 15
RULE_LIVE_FAILURE_MIN_COUNT = 3
RULE_LIVE_FAILURE_WINDOW_DAYS = 14

COMPOSITE_WEIGHTS = {
    "perf_1w": 0.25,
    "perf_1m": 0.50,
    "perf_3m": 0.25,
}


def finnhub_api_key() -> str | None:
    value = os.getenv(FINNHUB_API_KEY_ENV, "").strip()
    return value or None


def massive_api_key() -> str | None:
    # Backward-compatible alias while provider remains Finnhub-based in this recovery patch.
    return finnhub_api_key()


def openai_api_key() -> str | None:
    value = os.getenv(OPENAI_API_KEY_ENV, "").strip()
    return value or None
