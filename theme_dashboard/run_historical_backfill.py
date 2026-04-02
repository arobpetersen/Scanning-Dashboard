from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta

from src.database import get_conn, init_db
from src.historical_backfill import reconstruct_theme_history_range
from src.theme_service import seed_if_needed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill reconstructed daily historical theme snapshots.")
    parser.add_argument("--provider", default="live", choices=["live", "mock"], help="Historical market-data provider")
    parser.add_argument("--days", type=int, default=30, help="Look back this many calendar days when start/end are not supplied")
    parser.add_argument("--start-date", default=None, help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="Inclusive end date (YYYY-MM-DD)")
    parser.add_argument("--tickers", default="", help="Optional comma-separated ticker subset")
    parser.add_argument("--theme-ids", default="", help="Optional comma-separated theme id subset")
    parser.add_argument("--source-label", default="historical_backfill", help="Provenance source label to persist")
    parser.add_argument("--replace-existing", action="store_true", help="Replace existing rows for the same date/theme/source label")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    end_date = args.end_date or datetime.now(UTC).date().isoformat()
    start_date = args.start_date or (datetime.now(UTC).date() - timedelta(days=max(1, args.days))).isoformat()
    tickers = [token.strip().upper() for token in args.tickers.split(",") if token.strip()]
    theme_ids = [int(token.strip()) for token in args.theme_ids.split(",") if token.strip()]

    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        result = reconstruct_theme_history_range(
            conn,
            provider_name=args.provider,
            start_date=start_date,
            end_date=end_date,
            tickers=tickers or None,
            theme_ids=theme_ids or None,
            provenance_source_label=args.source_label,
            run_kind=args.source_label,
            replace_existing=args.replace_existing,
        )

    print(
        "historical_backfill",
        f"run_id={result.get('run_id')}",
        f"status={result.get('status')}",
        f"ticker_rows_written={result.get('ticker_history_rows_written', 0)}",
        f"ticker_rows_skipped={result.get('ticker_history_rows_skipped', 0)}",
        f"written={result.get('snapshot_rows_written', 0)}",
        f"skipped={result.get('snapshot_rows_skipped', 0)}",
        f"failed_tickers={','.join(result.get('failed_tickers', [])) if result.get('failed_tickers') else 'none'}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
