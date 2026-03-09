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

COMPOSITE_WEIGHTS = {
    "perf_1w": 0.25,
    "perf_1m": 0.50,
    "perf_3m": 0.25,
}


def finnhub_api_key() -> str | None:
    value = os.getenv(FINNHUB_API_KEY_ENV, "").strip()
    return value or None
