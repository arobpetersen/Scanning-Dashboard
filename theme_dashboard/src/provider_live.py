from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
import requests

from .config import finnhub_api_key
from .provider_base import ProviderBase


class LiveProvider(ProviderBase):
    name = "live"
    base_url = "https://finnhub.io/api/v1"

    def __init__(self, api_key: str | None = None, timeout_s: int = 20):
        self.api_key = api_key or finnhub_api_key()
        self.timeout_s = timeout_s

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _get(self, path: str, **params) -> dict:
        if not self.api_key:
            raise RuntimeError("Finnhub API key not configured")
        if "from_" in params:
            params["from"] = params.pop("from_")
        params["token"] = self.api_key
        resp = requests.get(f"{self.base_url}{path}", params=params, timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _calc_return(closes: list[float], lookback_days: int) -> float | None:
        if len(closes) <= lookback_days:
            return None
        current = closes[-1]
        past = closes[-(lookback_days + 1)]
        if past in (0, None):
            return None
        return round(((current - past) / past) * 100, 2)

    @staticmethod
    def _avg_volume(volumes: list[float], lookback_days: int = 21) -> float | None:
        if not volumes:
            return None
        sample = volumes[-lookback_days:]
        if not sample:
            return None
        return float(sum(sample) / len(sample))

    def fetch_ticker_data(self, tickers: Iterable[str]) -> tuple[pd.DataFrame, list[dict]]:
        rows: list[dict] = []
        failures: list[dict] = []

        if not self.is_configured:
            return pd.DataFrame(), [
                {
                    "ticker": t,
                    "error_message": "Finnhub API key missing. Set FINNHUB_API_KEY environment variable.",
                }
                for t in sorted(set(tickers))
            ]

        now = datetime.now(timezone.utc)
        to_ts = int(now.timestamp())
        from_ts = to_ts - 200 * 24 * 60 * 60

        for raw in sorted(set(tickers)):
            ticker = raw.strip().upper()
            if not ticker:
                continue
            try:
                quote = self._get("/quote", symbol=ticker)
                candles = self._get("/stock/candle", symbol=ticker, resolution="D", from_=from_ts, to=to_ts)
                profile = self._get("/stock/profile2", symbol=ticker)

                closes = candles.get("c", []) or []
                volumes = candles.get("v", []) or []
                if not closes:
                    raise RuntimeError("No historical close data returned by Finnhub")

                perf_1w = self._calc_return(closes, 5)
                perf_1m = self._calc_return(closes, 21)
                perf_3m = self._calc_return(closes, 63)

                price = quote.get("c")
                if price in (None, 0):
                    price = closes[-1]

                market_cap_m = profile.get("marketCapitalization")
                market_cap = float(market_cap_m) * 1_000_000 if market_cap_m is not None else None

                rows.append(
                    {
                        "ticker": ticker,
                        "price": float(price) if price is not None else None,
                        "perf_1w": perf_1w,
                        "perf_1m": perf_1m,
                        "perf_3m": perf_3m,
                        "market_cap": market_cap,
                        "avg_volume": self._avg_volume(volumes, 21),
                        "short_interest_pct": None,
                        "float_shares": None,
                        "adr_pct": None,
                        "last_updated": now,
                    }
                )
            except Exception as exc:
                failures.append({"ticker": ticker, "error_message": f"Finnhub fetch failed: {exc}"})

        return pd.DataFrame(rows), failures
