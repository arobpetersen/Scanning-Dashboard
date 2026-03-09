import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "theme_dashboard.duckdb"
SEED_PATH = BASE_DIR / "themes_seed_structured.json"

DEFAULT_PROVIDER = "mock"
STALE_DATA_HOURS = 24
REFRESH_STALE_TIMEOUT_MINUTES = 30
LIVE_RATE_LIMIT_STOP_THRESHOLD = 5
MASSIVE_API_KEY_ENV = "MASSIVE_API_KEY"
LIVE_QUOTE_PROFILE_SOURCE = "massive"
LIVE_HISTORICAL_SOURCE = "massive"
RULE_LOW_CONSTITUENT_THRESHOLD = 3

COMPOSITE_WEIGHTS = {
    "perf_1w": 0.25,
    "perf_1m": 0.50,
    "perf_3m": 0.25,
}


def massive_api_key() -> str | None:
    value = os.getenv(MASSIVE_API_KEY_ENV, "").strip()
    return value or None
