from __future__ import annotations

from pathlib import Path

from src.database import get_conn, init_db
from src.theme_model_comparison import build_and_write_theme_model_comparison
from src.theme_service import seed_if_needed


def main() -> None:
    init_db()
    docs_dir = Path(__file__).resolve().parent / "docs"
    with get_conn() as conn:
        seed_if_needed(conn)
        meta = build_and_write_theme_model_comparison(conn, docs_dir)

    print("Theme model comparison complete.")
    print(f"Preferred source: {meta.source}")
    print(f"Run id: {meta.run_id}")
    print(f"Snapshot time: {meta.snapshot_time}")
    print(f"Markdown: {meta.output_markdown}")
    print(f"CSV: {meta.output_csv}")


if __name__ == "__main__":
    main()
