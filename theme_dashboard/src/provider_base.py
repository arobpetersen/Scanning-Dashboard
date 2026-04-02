from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Iterable
import pandas as pd


class ProviderBase(ABC):
    name = "base"

    @abstractmethod
    def fetch_ticker_data(self, tickers: Iterable[str]) -> tuple[pd.DataFrame, list[dict]]:
        """Return (successful_rows_df, failures)."""

    def fetch_ticker_history_range(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        raise NotImplementedError("Historical range fetch is not implemented for this provider.")
