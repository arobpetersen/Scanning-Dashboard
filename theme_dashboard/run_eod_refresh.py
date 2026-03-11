from __future__ import annotations

import argparse

from src.database import get_conn, init_db
from src.eod_refresh import run_scheduled_eod_refresh
from src.theme_service import seed_if_needed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run 6:00 PM ET end-of-day refresh safely.")
    parser.add_argument("--provider", default="live", choices=["live", "mock"], help="Refresh provider")
    parser.add_argument("--force", action="store_true", help="Run regardless of weekday/time and duplicate daily guard")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        run_id = run_scheduled_eod_refresh(conn, provider_name=args.provider, force=args.force)

    if run_id is None:
        print("No EOD refresh run executed (outside schedule, weekend, duplicate day, or no active tickers).")
        return 0

    print(f"EOD refresh completed with run_id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
