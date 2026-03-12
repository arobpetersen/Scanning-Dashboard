from __future__ import annotations

import argparse

from src.database import get_conn, init_db
from src.eod_refresh import run_scheduled_historical_append
from src.theme_service import seed_if_needed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append one reconstructed daily historical snapshot set safely.")
    parser.add_argument("--provider", default="live", choices=["live", "mock"], help="Historical market-data provider")
    parser.add_argument("--force", action="store_true", help="Run regardless of weekday/time and duplicate guard")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        result = run_scheduled_historical_append(conn, provider_name=args.provider, force=args.force)

    if result is None:
        print("No historical append executed (outside schedule, weekend, duplicate day, or no scoped history to write).")
        return 0

    print(
        f"Historical append completed with run_id={result.get('run_id')} "
        f"status={result.get('status')} written={result.get('snapshot_rows_written', 0)} "
        f"skipped={result.get('snapshot_rows_skipped', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
