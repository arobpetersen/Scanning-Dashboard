from __future__ import annotations

from typing import Iterable
import pandas as pd

from .provider_base import ProviderBase


class LiveProvider(ProviderBase):
    name = "live"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key

    def fetch_ticker_data(self, tickers: Iterable[str]) -> tuple[pd.DataFrame, list[dict]]:
        failures = [
            {
                "ticker": t,
                "error_message": "Live provider not configured. Add API integration in provider_live.py",
            }
            for t in set(tickers)
        ]
        return pd.DataFrame(), failures
