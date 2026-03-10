from __future__ import annotations

import argparse

from theme_dashboard.src.database import get_conn, init_db
from theme_dashboard.src.fetch_data import run_refresh
from theme_dashboard.src.rankings import compute_theme_rankings
from theme_dashboard.src.theme_service import active_ticker_universe, get_theme_members, list_themes, seed_if_needed


def _resolve_tickers(conn, scope: str, theme_name: str | None, tickers_csv: str | None) -> tuple[list[str], str | None]:
    if scope == "active_themes":
        return active_ticker_universe(conn), None

    if scope == "selected_theme":
        if not theme_name:
            raise ValueError("--theme-name is required when --scope selected_theme")
        themes = list_themes(conn, active_only=False)
        matched = themes[themes["name"].str.lower() == theme_name.strip().lower()]
        if matched.empty:
            raise ValueError(f"Theme not found: {theme_name}")
        theme_id = int(matched.iloc[0]["id"])
        return get_theme_members(conn, theme_id)["ticker"].tolist(), str(matched.iloc[0]["name"])

    if scope == "custom_tickers":
        if not tickers_csv:
            raise ValueError("--tickers is required when --scope custom_tickers")
        tickers = sorted({t.strip().upper() for t in tickers_csv.replace("\n", ",").split(",") if t.strip()})
        return tickers, None

    raise ValueError(f"Unsupported scope: {scope}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild Theme Dashboard DuckDB + snapshots deterministically.")
    parser.add_argument("--provider", choices=["mock", "live"], default="mock")
    parser.add_argument("--scope", choices=["active_themes", "selected_theme", "custom_tickers"], default="active_themes")
    parser.add_argument("--theme-name", default=None)
    parser.add_argument("--tickers", default=None, help="Comma-separated tickers for custom_tickers scope")
    args = parser.parse_args()

    init_db()
    with get_conn() as conn:
        seeded = seed_if_needed(conn)
        tickers, resolved_theme = _resolve_tickers(conn, args.scope, args.theme_name, args.tickers)
        run_id = run_refresh(
            conn,
            args.provider,
            tickers=tickers,
            scope_type=args.scope,
            scope_theme_name=resolved_theme,
        )
        rankings = compute_theme_rankings(conn)

    print(f"seeded={seeded} run_id={run_id} tickers={len(tickers)} themes_ranked={len(rankings)}")


if __name__ == "__main__":
    main()
