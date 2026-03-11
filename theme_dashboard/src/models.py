from dataclasses import dataclass
from datetime import datetime


@dataclass
class Theme:
    id: int
    name: str
    category: str
    is_active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class TickerSnapshot:
    ticker: str
    price: float
    perf_1w: float
    perf_1m: float
    perf_3m: float
    market_cap: float
    avg_volume: float
    short_interest_pct: float
    float_shares: float
    adr_pct: float
    last_updated: datetime
