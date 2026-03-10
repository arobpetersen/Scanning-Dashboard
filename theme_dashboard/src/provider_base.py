from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable
import pandas as pd


class ProviderBase(ABC):
    name = "base"

    @abstractmethod
    def fetch_ticker_data(self, tickers: Iterable[str]) -> tuple[pd.DataFrame, list[dict]]:
        """Return (successful_rows_df, failures)."""
