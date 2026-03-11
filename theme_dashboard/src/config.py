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
LIVE_FETCH_REFERENCE_ON_REFRESH = os.getenv("LIVE_FETCH_REFERENCE_ON_REFRESH", "1").strip().lower() not in {"0", "false", "no"}
AIRTABLE_API_KEY_ENV = "AIRTABLE_API_KEY"
AIRTABLE_BASE_ID_ENV = "AIRTABLE_BASE_ID"
AIRTABLE_TABLE_THEMES = os.getenv("AIRTABLE_TABLE_THEMES", "Themes").strip() or "Themes"
AIRTABLE_TABLE_THEME_SNAPSHOT_HISTORY = os.getenv("AIRTABLE_TABLE_THEME_SNAPSHOT_HISTORY", "Theme Snapshot History").strip() or "Theme Snapshot History"
AIRTABLE_TABLE_TICKERS = os.getenv("AIRTABLE_TABLE_TICKERS", "Tickers").strip() or "Tickers"
AIRTABLE_TABLE_TICKER_SNAPSHOT_HISTORY = os.getenv("AIRTABLE_TABLE_TICKER_SNAPSHOT_HISTORY", "Ticker Snapshot History").strip() or "Ticker Snapshot History"
AIRTABLE_EXPORT_SNAPSHOT_LIMIT = int(os.getenv("AIRTABLE_EXPORT_SNAPSHOT_LIMIT", "14").strip() or "14")
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
AI_MODEL = "gpt-5-mini"
AI_MAX_PROPOSALS = 8

# Rules engine configuration
RULE_LOW_CONSTITUENT_THRESHOLD = 3
RULE_MAX_SUGGESTIONS_PER_RULE = 15
RULE_LIVE_FAILURE_MIN_COUNT = 3
RULE_LIVE_FAILURE_WINDOW_DAYS = 14

COMPOSITE_WEIGHTS = {
    "perf_1w": 0.25,
    "perf_1m": 0.50,
    "perf_3m": 0.25,
}


def massive_api_key() -> str | None:
    value = os.getenv(MASSIVE_API_KEY_ENV, "").strip()
    return value or None


def airtable_api_key() -> str | None:
    value = os.getenv(AIRTABLE_API_KEY_ENV, "").strip()
    return value or None


def airtable_base_id() -> str | None:
    value = os.getenv(AIRTABLE_BASE_ID_ENV, "").strip()
    return value or None


def openai_api_key() -> str | None:
    value = os.getenv(OPENAI_API_KEY_ENV, "").strip()
    return value or None
