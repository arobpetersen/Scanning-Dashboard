from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import pandas as pd
import requests

from .config import LIVE_HISTORICAL_SOURCE, LIVE_QUOTE_PROFILE_SOURCE, massive_api_key
from .provider_base import ProviderBase


class LiveProvider(ProviderBase):
    """Massive (Polygon) proof-of-concept live provider.

    In this version both historical prices and reference/profile are sourced from Massive.
    """

    name = "live"
    base_url = "https://api.polygon.io"

    def __init__(self, api_key: str | None = None, timeout_s: int = 20, include_reference: bool = True):
        self.api_key = api_key or massive_api_key()
        self.timeout_s = timeout_s
        self._include_reference = include_reference
        self.session = requests.Session()
        self._ref_cache: dict[str, dict] = {}
        self._api_calls_total = 0
        self._endpoint_counts: dict[str, int] = defaultdict(int)

    def get_call_accounting(self) -> dict:
        return {
            "api_call_count": int(self._api_calls_total),
            "endpoint_counts": dict(self._endpoint_counts),
        }

    def _categorize_endpoint(self, path: str) -> str:
        if path.startswith("/v2/aggs/ticker/"):
            return "aggs_daily"
        if path.startswith("/v3/reference/tickers/"):
            return "reference_ticker"
        return "other"

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)

    @property
    def quote_profile_source(self) -> str:
        return LIVE_QUOTE_PROFILE_SOURCE

    @property
    def historical_source(self) -> str:
        return LIVE_HISTORICAL_SOURCE

    @property
    def historical_source_available(self) -> bool:
        return True

    def _get(self, path: str, **params) -> dict:
        if not self.api_key:
            raise RuntimeError("CONFIG: Massive API key not configured")

        params["apiKey"] = self.api_key
        self._api_calls_total += 1
        self._endpoint_counts[self._categorize_endpoint(path)] += 1
        response = self.session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout_s)

        if response.status_code == 429:
            raise RuntimeError("RATE_LIMIT: Massive returned HTTP 429")
        if response.status_code == 403:
            raise RuntimeError("AUTH: Massive returned HTTP 403 (check plan/permissions or API key)")
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, dict) and payload.get("error"):
            error_text = str(payload["error"])
            if "limit" in error_text.lower() or "429" in error_text:
                raise RuntimeError(f"RATE_LIMIT: {error_text}")
            raise RuntimeError(error_text)
        return payload

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
        sample = [v for v in volumes[-lookback_days:] if v is not None]
        if not sample:
            return None
        return float(sum(sample) / len(sample))

    def _fetch_history(self, ticker: str) -> tuple[list[float], list[float], datetime]:
        end_date = date.today()
        start_date = end_date - timedelta(days=365)
        payload = self._get(
            f"/v2/aggs/ticker/{ticker}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}",
            adjusted="true",
            sort="asc",
            limit=5000,
        )

        results = payload.get("results") or []
        if not results:
            raise RuntimeError("NO_CANDLES: Massive returned no daily aggregates")

        closes = [float(r["c"]) for r in results if r.get("c") is not None]
        volumes = [float(r["v"]) for r in results if r.get("v") is not None]
        if not closes:
            raise RuntimeError("NO_CANDLES: Massive aggregate closes missing")

        ts_ms = results[-1].get("t")
        if ts_ms is None:
            last_updated = datetime.now(timezone.utc)
        else:
            last_updated = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)

        return closes, volumes, last_updated

    def _fetch_reference(self, ticker: str) -> dict:
        cached = self._ref_cache.get(ticker)
        if cached is not None:
            return cached
        payload = self._get(f"/v3/reference/tickers/{ticker}")
        result = payload.get("results") or {}
        self._ref_cache[ticker] = result
        return result

    def fetch_ticker_data(self, tickers: Iterable[str]) -> tuple[pd.DataFrame, list[dict]]:
        rows: list[dict] = []
        failures: list[dict] = []

        normalized = sorted({(t or "").strip().upper() for t in tickers if (t or "").strip()})
        if not normalized:
            return pd.DataFrame(), []

        if not self.is_configured:
            return pd.DataFrame(), [
                {
                    "ticker": t,
                    "error_message": "CONFIG: Massive API key missing. Set MASSIVE_API_KEY environment variable.",
                }
                for t in normalized
            ]

        for ticker in normalized:
            try:
                closes, volumes, last_updated = self._fetch_history(ticker)
                perf_1w = self._calc_return(closes, 5)
                perf_1m = self._calc_return(closes, 21)
                perf_3m = self._calc_return(closes, 63)
                price = closes[-1]

                market_cap = None
                if self._include_reference:
                    try:
                        ref = self._fetch_reference(ticker)
                        market_cap_value = ref.get("market_cap")
                        market_cap = float(market_cap_value) if market_cap_value is not None else None
                    except Exception:
                        market_cap = None

                rows.append(
                    {
                        "ticker": ticker,
                        "price": float(price),
                        "perf_1w": perf_1w,
                        "perf_1m": perf_1m,
                        "perf_3m": perf_3m,
                        "market_cap": market_cap,
                        "avg_volume": self._avg_volume(volumes, 21),
                        "short_interest_pct": None,
                        "float_shares": None,
                        "adr_pct": None,
                        "last_updated": last_updated,
                    }
                )
            except Exception as exc:
                msg = str(exc)
                if "RATE_LIMIT" in msg or "429" in msg:
                    msg = f"RATE_LIMIT: {msg}"
                elif "AUTH:" in msg or "403" in msg:
                    msg = f"AUTH: {msg}"
                elif "NO_CANDLES" in msg:
                    msg = f"NO_CANDLES: {msg}"
                else:
                    msg = f"REQUEST_ERROR: {msg}"
                failures.append({"ticker": ticker, "error_message": f"Massive fetch failed: {msg}"})

        return pd.DataFrame(rows), failures
