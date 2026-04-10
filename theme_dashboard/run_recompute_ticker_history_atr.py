from __future__ import annotations

import argparse

from src.database import get_conn, init_db
from src.ticker_history import recompute_ticker_daily_history_atr


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recompute ATR fields for ticker_daily_history in resumable batches.")
    parser.add_argument("--ticker", action="append", default=[], help="Restrict recompute to one ticker. Repeatable.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N ticker/source series in this run.")
    parser.add_argument("--start-after", default=None, help="Resume after this ticker symbol (lexicographic ticker order).")
    parser.add_argument("--market-data-source", default=None, help="Restrict recompute to one market_data_source.")
    parser.add_argument("--progress-every", type=int, default=25, help="Print progress every N processed series.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    init_db()

    def _progress(update: dict[str, int | str]) -> None:
        interval = max(int(args.progress_every or 1), 1)
        index = int(update.get("index", 0))
        total = int(update.get("total_series", 0))
        if index == 1 or index == total or (index % interval) == 0:
            print(
                "ATR recompute progress | "
                f"series={index}/{total} | "
                f"ticker={update.get('ticker')} | "
                f"source={update.get('market_data_source')} | "
                f"rows_updated_for_series={int(update.get('rows_updated_for_series', 0))} | "
                f"rows_updated_total={int(update.get('rows_updated', 0))}"
            )

    with get_conn() as conn:
        result = recompute_ticker_daily_history_atr(
            conn,
            tickers=list(args.ticker or []),
            market_data_source=args.market_data_source,
            start_after_ticker=args.start_after,
            limit=args.limit,
            progress_callback=_progress,
        )
    print(
        "ATR recompute complete | "
        f"series_recomputed={int(result.get('series_recomputed', 0))} | "
        f"rows_updated={int(result.get('rows_updated', 0))}"
    )


if __name__ == "__main__":
    main()
