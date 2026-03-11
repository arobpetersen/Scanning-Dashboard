from __future__ import annotations

import argparse

from src.airtable_export import (
    AirtableClient,
    build_airtable_export_payloads,
    ensure_airtable_schema,
    export_to_airtable,
    print_plan_summary,
    validate_airtable_config,
)
from src.config import AIRTABLE_EXPORT_SNAPSHOT_LIMIT, airtable_api_key, airtable_base_id
from src.database import get_conn, init_db
from src.theme_service import seed_if_needed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export bounded recent theme/ticker history from DuckDB to Airtable.")
    parser.add_argument("--snapshot-limit", type=int, default=AIRTABLE_EXPORT_SNAPSHOT_LIMIT, help="Recent snapshot rows per theme/ticker to export")
    parser.add_argument("--dry-run", action="store_true", help="Build export payloads and upsert plan without writing to Airtable")
    parser.add_argument("--preview", type=int, default=0, help="Print up to N sample records per dataset")
    parser.add_argument("--validate-only", action="store_true", help="Validate Airtable credentials plus expected tables/fields without exporting records")
    parser.add_argument("--write", action="store_true", help="Write to Airtable using configured credentials")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = not args.write or args.dry_run or args.validate_only

    if args.write and args.validate_only:
        raise RuntimeError("Choose either --write or --validate-only, not both.")

    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)
        payloads = build_airtable_export_payloads(conn, snapshot_limit=args.snapshot_limit)

    client = None
    if args.write or args.validate_only:
        validate_airtable_config(airtable_api_key(), airtable_base_id())
        client = AirtableClient(api_key=airtable_api_key(), base_id=airtable_base_id())
        schema_validation = ensure_airtable_schema(client)
        if args.validate_only:
            print("airtable_schema_validation=ok")
            print_plan_summary(export_to_airtable(payloads, client=client, dry_run=True), preview_rows=max(0, args.preview))
            print(f"mode=validate-only snapshot_limit={args.snapshot_limit}")
            return 0

    plan = export_to_airtable(payloads, client=client, dry_run=dry_run)
    print_plan_summary(plan, preview_rows=max(0, args.preview))
    print(f"mode={'dry-run' if dry_run else 'write'} snapshot_limit={args.snapshot_limit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
