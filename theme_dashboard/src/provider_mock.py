from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Iterable
import pandas as pd

from .provider_base import ProviderBase


class MockProvider(ProviderBase):
    name = "mock"

    def get_call_accounting(self) -> dict:
        return {"api_call_count": 0, "endpoint_counts": {}}

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

    def fetch_ticker_history_range(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        normalized = (ticker or "").strip().upper()
        if not normalized:
            return pd.DataFrame(columns=["ticker", "snapshot_date", "close", "volume"])

        dates = pd.bdate_range(start=start_date, end=end_date)
        if len(dates) == 0:
            return pd.DataFrame(columns=["ticker", "snapshot_date", "close", "volume"])

        seed = self._rng_from_ticker(normalized)
        base_price = 10 + (seed % 12000) / 100
        drift = ((seed // 29) % 40 - 20) / 1000.0
        wave_scale = ((seed // 31) % 25 + 5) / 1000.0
        volume_base = 5e5 + ((seed // 37) % 30000) * 100

        rows = []
        price = float(base_price)
        for idx, ts in enumerate(dates):
            cycle = ((idx % 10) - 5) / 5.0
            price = max(1.0, price * (1.0 + drift + wave_scale * cycle))
            rows.append(
                {
                    "ticker": normalized,
                    "snapshot_date": ts.date(),
                    "close": round(price, 4),
                    "volume": float(volume_base + (idx % 15) * 5000),
                }
            )

        return pd.DataFrame(rows)
