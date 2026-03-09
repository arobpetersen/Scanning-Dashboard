from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Iterable
import pandas as pd

from .provider_base import ProviderBase


class MockProvider(ProviderBase):
    name = "mock"

    def _rng_from_ticker(self, ticker: str) -> int:
        return int(hashlib.sha256(ticker.encode("utf-8")).hexdigest()[:8], 16)

    def fetch_ticker_data(self, tickers: Iterable[str]) -> tuple[pd.DataFrame, list[dict]]:
        rows = []
        failures: list[dict] = []
        now = datetime.now(timezone.utc)

        for raw in sorted(set(tickers)):
            ticker = raw.strip().upper()
            if not ticker:
                continue
            seed = self._rng_from_ticker(ticker)

            price = 5 + (seed % 45000) / 100
            perf_1w = ((seed // 3) % 4000) / 100 - 20
            perf_1m = ((seed // 5) % 6000) / 100 - 30
            perf_3m = ((seed // 7) % 12000) / 100 - 60
            market_cap = 1e9 + ((seed // 11) % 2000) * 1e8
            avg_volume = 1e5 + ((seed // 13) % 20000) * 1e3
            short_interest_pct = ((seed // 17) % 3000) / 100
            float_shares = 1e7 + ((seed // 19) % 2000) * 1e6
            adr_pct = ((seed // 23) % 1000) / 100

            rows.append(
                {
                    "ticker": ticker,
                    "price": round(price, 2),
                    "perf_1w": round(perf_1w, 2),
                    "perf_1m": round(perf_1m, 2),
                    "perf_3m": round(perf_3m, 2),
                    "market_cap": float(market_cap),
                    "avg_volume": float(avg_volume),
                    "short_interest_pct": round(short_interest_pct, 2),
                    "float_shares": float(float_shares),
                    "adr_pct": round(adr_pct, 2),
                    "last_updated": now,
                }
            )

        return pd.DataFrame(rows), failures
