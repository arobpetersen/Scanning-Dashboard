#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "theme_dashboard"))

from src.database import get_conn, init_db
from src.momentum_engine import compute_theme_momentum
from src.theme_service import list_themes, seed_if_needed

ARCHETYPES = [
    "persistent_leader",
    "emerging_theme",
    "weakening_theme",
    "choppy_theme",
    "dead_theme",
]
VOL_MULT = {"low": 0.5, "medium": 1.0, "high": 1.7}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic historical snapshots for theme dashboard.")
    p.add_argument("--days", type=int, default=120)
    p.add_argument("--frequency", choices=["daily", "weekly", "monthly"], default="weekly")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--volatility", choices=["low", "medium", "high"], default="medium")
    p.add_argument("--reset", action="store_true")
    p.add_argument("--shocks", default="", help="Comma-separated: ai_boom,energy_crash,defense_rally")
    return p.parse_args()


def step_days(freq: str) -> int:
    return {"daily": 1, "weekly": 7, "monthly": 30}[freq]


def choose_archetype(theme_id: int) -> str:
    return ARCHETYPES[theme_id % len(ARCHETYPES)]


def phase_boost(theme_name: str, phase: int) -> float:
    n = theme_name.lower()
    if phase == 0 and any(k in n for k in ["ai", "semiconductor", "chip", "cloud", "software", "robot"]):
        return 2.8
    if phase == 1 and any(k in n for k in ["defense", "aerospace", "health", "utility", "insurance"]):
        return 2.4
    if phase == 2 and any(k in n for k in ["energy", "oil", "gas", "uranium", "commodity", "materials"]):
        return 2.8
    return 0.0


def archetype_signal(archetype: str, t: float) -> tuple[float, float]:
    if archetype == "persistent_leader":
        return 1.8 + 1.4 * t, 62 + 8 * t
    if archetype == "emerging_theme":
        return -0.6 + 3.3 * t, 42 + 18 * t
    if archetype == "weakening_theme":
        return 2.6 - 3.0 * t, 66 - 20 * t
    if archetype == "choppy_theme":
        return 0.6 * math.sin(6.2 * t), 50 + 10 * math.sin(7.5 * t)
    return -0.4 + 0.2 * math.sin(4.8 * t), 36 + 4 * math.sin(5.0 * t)


def apply_shocks(name: str, t: float, shocks: set[str]) -> float:
    n = name.lower()
    bump = 0.0
    if "ai_boom" in shocks and 0.15 < t < 0.4 and any(k in n for k in ["ai", "chip", "semiconductor", "cloud"]):
        bump += 4.5
    if "energy_crash" in shocks and 0.5 < t < 0.75 and any(k in n for k in ["energy", "oil", "gas", "uranium"]):
        bump -= 4.2
    if "defense_rally" in shocks and 0.45 < t < 0.75 and any(k in n for k in ["defense", "aerospace", "security"]):
        bump += 3.8
    return bump


def generate() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    shocks = {s.strip() for s in args.shocks.split(",") if s.strip()}

    init_db()
    with get_conn() as conn:
        seed_if_needed(conn)

        if args.reset:
            conn.execute("DELETE FROM theme_snapshots")
            conn.execute("DELETE FROM ticker_snapshots")
            conn.execute("DELETE FROM refresh_failures")
            conn.execute("DELETE FROM refresh_run_tickers")
            conn.execute("DELETE FROM refresh_runs WHERE provider='synthetic_backfill'")

        themes = list_themes(conn, active_only=False)
        if themes.empty:
            print("No themes found.")
            return

        theme_members = conn.execute(
            """
            SELECT t.id AS theme_id, t.name AS theme_name, m.ticker
            FROM themes t
            LEFT JOIN theme_membership m ON m.theme_id = t.id
            ORDER BY t.id, m.ticker
            """
        ).df()

        # date schedule
        now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start = now - timedelta(days=args.days)
        step = timedelta(days=step_days(args.frequency))
        dates = []
        cur = start
        while cur <= now:
            dates.append(cur)
            cur += step
        if len(dates) < 3:
            dates = [start, start + step, now]

        vol = VOL_MULT[args.volatility]

        ticker_universe = sorted(set([x for x in theme_members["ticker"].dropna().tolist() if x]))
        ticker_theme_map = theme_members.dropna(subset=["ticker"]).groupby("ticker")["theme_id"].apply(list).to_dict()

        for i, dt in enumerate(dates):
            t = i / max(1, (len(dates) - 1))
            phase = 0 if t < 1 / 3 else (1 if t < 2 / 3 else 2)

            conn.execute(
                """
                INSERT INTO refresh_runs(provider, started_at, finished_at, status, ticker_count, success_count, failure_count, scope_type)
                VALUES ('synthetic_backfill', ?, ?, 'success', ?, ?, 0, 'synthetic_history')
                """,
                [dt, dt, len(ticker_universe), len(ticker_universe)],
            )
            run_id = int(conn.execute("SELECT max(run_id) FROM refresh_runs").fetchone()[0])

            # theme level signals
            theme_rows = []
            theme_perf = {}
            for _, tr in themes.iterrows():
                tid = int(tr["id"])
                tname = str(tr["name"])
                arche = choose_archetype(tid)
                sig, breadth_base = archetype_signal(arche, t)
                rot = phase_boost(tname, phase)
                shock = apply_shocks(tname, t, shocks)
                noise = rng.gauss(0, 0.8 * vol)

                avg_1m = sig + rot + shock + noise
                avg_1w = avg_1m * 0.55 + rng.gauss(0, 0.7 * vol)
                avg_3m = avg_1m * 1.15 + rng.gauss(0, 0.7 * vol)
                breadth = max(5.0, min(95.0, breadth_base + rot * 2.0 + shock * 1.1 + rng.gauss(0, 4.5 * vol)))

                members = theme_members[theme_members["theme_id"] == tid]["ticker"].dropna().tolist()
                base_count = len(members)
                if arche == "emerging_theme":
                    tcount = max(0, int(round(base_count + 0.12 * base_count * t)))
                elif arche == "weakening_theme":
                    tcount = max(0, int(round(base_count - 0.10 * base_count * t)))
                else:
                    tcount = base_count

                composite = 0.25 * avg_1w + 0.50 * avg_1m + 0.25 * avg_3m
                theme_perf[tid] = (avg_1w, avg_1m, avg_3m)
                theme_rows.append(
                    [run_id, dt, tid, tcount, round(avg_1w, 2), round(avg_1m, 2), round(avg_3m, 2),
                     round(max(0.0, min(100.0, breadth - 2.5)), 2), round(breadth, 2), round(max(0.0, min(100.0, breadth + 2.5)), 2), round(composite, 2), "synthetic_backfill"]
                )

            # ticker rows derived from theme trend + noise
            ticker_rows = []
            for ticker in ticker_universe:
                tids = ticker_theme_map.get(ticker, [])
                if tids:
                    vals = [theme_perf[tid] for tid in tids if tid in theme_perf]
                    if vals:
                        p1w = sum(v[0] for v in vals) / len(vals)
                        p1m = sum(v[1] for v in vals) / len(vals)
                        p3m = sum(v[2] for v in vals) / len(vals)
                    else:
                        p1w = p1m = p3m = 0.0
                else:
                    p1w = p1m = p3m = 0.0
                p1w += rng.gauss(0, 1.2 * vol)
                p1m += rng.gauss(0, 1.5 * vol)
                p3m += rng.gauss(0, 1.8 * vol)
                price = max(2.0, 80 + p1m * 2 + rng.gauss(0, 4 * vol))
                ticker_rows.append([run_id, ticker, round(price, 2), round(p1w, 2), round(p1m, 2), round(p3m, 2), dt, "synthetic_backfill"])

            conn.executemany(
                """
                INSERT INTO ticker_snapshots(
                    run_id, ticker, price, perf_1w, perf_1m, perf_3m, last_updated, snapshot_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ticker_rows,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO theme_snapshots(
                    run_id, snapshot_time, theme_id, ticker_count,
                    avg_1w, avg_1m, avg_3m,
                    positive_1w_breadth_pct, positive_1m_breadth_pct, positive_3m_breadth_pct,
                    composite_score, snapshot_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                theme_rows,
            )
            conn.executemany(
                "INSERT INTO refresh_run_tickers(run_id, ticker) VALUES (?, ?)",
                [(run_id, tkr) for tkr in ticker_universe],
            )

        # summary
        summary = compute_theme_momentum(conn, lookback_days=args.days, top_n=20)
        print("\nSynthetic history generation complete")
        print(f"window_days={args.days} frequency={args.frequency} seed={args.seed} volatility={args.volatility}")
        print(f"runs_generated={len(dates)}")
        print("\nTop themes (momentum):")
        if not summary["top_momentum"].empty:
            print(summary["top_momentum"][["theme", "momentum_score", "delta_composite"]].head(10).to_string(index=False))
        print("\nBiggest risers:")
        if not summary["biggest_risers"].empty:
            print(summary["biggest_risers"][["theme", "rank_change", "delta_composite"]].head(10).to_string(index=False))
        print("\nBiggest fallers:")
        if not summary["biggest_fallers"].empty:
            print(summary["biggest_fallers"][["theme", "rank_change", "delta_composite"]].head(10).to_string(index=False))
        print("\nEmerging themes (new leaders):")
        print(", ".join(summary["new_leaders"]) if summary["new_leaders"] else "None")


if __name__ == "__main__":
    generate()
