import unittest
from pathlib import Path

import duckdb
import pandas as pd

from src.leaderboard_utils import (
    MARKET_THEME_DRIVER_LABELS,
    THEME_ANOMALY_LABELS,
    TICKER_LEADERSHIP_SORT_COLUMNS,
    TICKER_VS_THEME_LABELS,
    build_top_governed_ticker_leaders,
    build_market_theme_snapshot_table,
    build_theme_anomaly_snapshot_table,
    build_theme_ticker_standout_table,
    build_top_theme_baseline_snapshot,
)


class TestMarketThemeSnapshot(unittest.TestCase):
    def test_ticker_leadership_page_defaults_to_1w_and_bypasses_cached_sort_loader(self):
        page_path = Path(__file__).resolve().parents[1] / "pages" / "5_Ticker_Leadership.py"
        content = page_path.read_text(encoding="utf-8")

        self.assertIn('DEFAULT_SORT = "1W %"', content)
        self.assertIn('SORT_STATE_KEY = "ticker_leadership_sort_by"', content)
        self.assertIn("key=SORT_STATE_KEY", content)
        self.assertIn("leaders = _load_ticker_leaders(selected_sort, int(top_n))", content)
        self.assertNotIn("load_ticker_leadership_cached", content)

    def test_snapshot_marks_broad_advance_and_rank_delta(self):
        current = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "theme_id": 101,
                    "theme": "AI",
                    "category": "Tech",
                    "avg_1d": 2.0,
                    "avg_1w": 5.0,
                    "avg_1m": 12.0,
                    "avg_6m": 30.0,
                    "composite_score": 11.5,
                    "eligible_contributor_count": 4,
                }
            ]
        )
        prior = pd.DataFrame(
            [
                {
                    "theme_id": 101,
                    "theme": "AI",
                    "category": "Tech",
                    "snapshot_date": "2026-04-24",
                    "rank": 3,
                    "standardized_composite_score": 10.0,
                },
            ]
        )
        members = pd.DataFrame(
            [
                {"theme_id": 101, "ticker": "AAA", "perf_1d": 2.0},
                {"theme_id": 101, "ticker": "BBB", "perf_1d": 1.5},
                {"theme_id": 101, "ticker": "CCC", "perf_1d": 0.5},
                {"theme_id": 101, "ticker": "DDD", "perf_1d": -0.1},
            ]
        )

        out = build_market_theme_snapshot_table(current, members, prior, top_k=12)

        self.assertEqual(int(out.iloc[0]["prior_rank"]), 3)
        self.assertEqual(int(out.iloc[0]["rank_delta_1d"]), 2)
        self.assertEqual(float(out.iloc[0]["delta_composite_score"]), 1.5)
        self.assertEqual(out.iloc[0]["driver_flag"], MARKET_THEME_DRIVER_LABELS["broad_advance"])
        self.assertEqual(float(out.iloc[0]["positive_1d_pct"]), 75.0)

    def test_snapshot_marks_single_name_led_and_new_entrant(self):
        current = pd.DataFrame(
            [
                {
                    "rank": 12,
                    "theme_id": 202,
                    "theme": "Power",
                    "category": "Energy",
                    "avg_1d": 1.1,
                    "eligible_contributor_count": 3,
                }
            ]
        )
        prior = pd.DataFrame(
            [
                {"theme_id": 202, "theme": "Power", "category": "Energy", "snapshot_date": "2026-04-24", "rank": 18},
            ]
        )
        members = pd.DataFrame(
            [
                {"theme_id": 202, "ticker": "BIG", "perf_1d": 6.0},
                {"theme_id": 202, "ticker": "MID", "perf_1d": 0.5},
                {"theme_id": 202, "ticker": "LOW", "perf_1d": -0.2},
            ]
        )

        out = build_market_theme_snapshot_table(current, members, prior, top_k=12)

        self.assertIn(MARKET_THEME_DRIVER_LABELS["single_name_led"], out.iloc[0]["driver_flag"])
        self.assertIn(MARKET_THEME_DRIVER_LABELS["new_top_12_entrant"], out.iloc[0]["driver_flag"])
        self.assertEqual(out.iloc[0]["top_1d_driver"], "BIG")
        self.assertGreater(float(out.iloc[0]["top_1d_driver_share"]), 90.0)

    def test_anomaly_snapshot_flags_only_material_standouts(self):
        snapshot = pd.DataFrame(
            [
                {
                    "current_rank": 1,
                    "theme": "Data Center",
                    "category": "Optics",
                    "avg_1d": -4.0,
                    "positive_1d_pct": 20.0,
                    "top_1d_driver": "OPT",
                    "top_1d_driver_move": 1.0,
                    "top_1d_driver_share": 40.0,
                },
                {
                    "current_rank": 2,
                    "theme": "Semis",
                    "category": "Power",
                    "avg_1d": -1.0,
                    "positive_1d_pct": 80.0,
                    "top_1d_driver": "PWR",
                    "top_1d_driver_move": 2.0,
                    "top_1d_driver_share": 35.0,
                },
                {
                    "current_rank": 3,
                    "theme": "Software",
                    "category": "AI",
                    "avg_1d": -2.0,
                    "positive_1d_pct": 50.0,
                    "top_1d_driver": "APP",
                    "top_1d_driver_move": 1.5,
                    "top_1d_driver_share": 35.0,
                },
                {
                    "current_rank": 4,
                    "theme": "Energy",
                    "category": "Grid",
                    "avg_1d": -8.0,
                    "positive_1d_pct": 10.0,
                    "top_1d_driver": "GRID",
                    "top_1d_driver_move": 0.5,
                    "top_1d_driver_share": 25.0,
                },
                {
                    "current_rank": 5,
                    "theme": "Semis",
                    "category": "Substrates",
                    "avg_1d": -3.0,
                    "positive_1d_pct": 20.0,
                    "top_1d_driver": "AXTI",
                    "top_1d_driver_move": 10.0,
                    "top_1d_driver_share": 75.0,
                },
            ]
        )

        out = build_theme_anomaly_snapshot_table(snapshot)

        standout_text = " | ".join(out["standout"].tolist())
        self.assertIn(THEME_ANOMALY_LABELS["relative_weakness"], standout_text)
        self.assertIn(THEME_ANOMALY_LABELS["internal_divergence"], standout_text)
        self.assertIn(THEME_ANOMALY_LABELS["single_name_concentration"], standout_text)
        self.assertTrue(any("AXTI 10.0%" in evidence for evidence in out["evidence"].tolist()))
        self.assertIn("basis", out.columns)
        self.assertNotIn("why_it_matters", out.columns)
        self.assertEqual(out["theme"].nunique(), len(out))

    def test_anomaly_snapshot_returns_empty_for_normal_clustered_action(self):
        snapshot = pd.DataFrame(
            [
                {
                    "current_rank": idx + 1,
                    "theme": f"Theme {idx}",
                    "category": "Group",
                    "avg_1d": 0.2 + (idx * 0.05),
                    "positive_1d_pct": 55.0,
                    "top_1d_driver": f"T{idx}",
                    "top_1d_driver_move": 0.4,
                    "top_1d_driver_share": 30.0,
                }
                for idx in range(8)
            ]
        )

        out = build_theme_anomaly_snapshot_table(snapshot)

        self.assertTrue(out.empty)

    def test_positive_breadth_above_baseline_requires_supportive_performance(self):
        snapshot = pd.DataFrame(
            [
                {
                    "current_rank": 1,
                    "theme": "Less Bad",
                    "category": "Group",
                    "avg_1d": -2.0,
                    "positive_1d_pct": 80.0,
                    "top_1d_driver": "LB",
                    "top_1d_driver_move": 1.0,
                    "top_1d_driver_share": 25.0,
                },
                {
                    "current_rank": 2,
                    "theme": "Baseline A",
                    "category": "Group",
                    "avg_1d": -0.4,
                    "positive_1d_pct": 30.0,
                    "top_1d_driver": "BA",
                    "top_1d_driver_move": 1.0,
                    "top_1d_driver_share": 25.0,
                },
                {
                    "current_rank": 3,
                    "theme": "Baseline B",
                    "category": "Group",
                    "avg_1d": -0.2,
                    "positive_1d_pct": 30.0,
                    "top_1d_driver": "BB",
                    "top_1d_driver_move": 1.0,
                    "top_1d_driver_share": 25.0,
                },
            ]
        )

        out = build_theme_anomaly_snapshot_table(snapshot)

        less_bad = out[out["theme"] == "Less Bad - Group"]
        if not less_bad.empty:
            self.assertNotIn(THEME_ANOMALY_LABELS["breadth_strength"], less_bad.iloc[0]["standout"])

    def test_top_theme_baseline_snapshot_reports_neutral_metrics(self):
        snapshot = pd.DataFrame(
            [
                {
                    "avg_1d": -2.0,
                    "positive_1d_pct": 25.0,
                    "delta_composite_score": -0.5,
                    "current_momentum_score": 10.0,
                },
                {
                    "avg_1d": 1.0,
                    "positive_1d_pct": 75.0,
                    "delta_composite_score": 0.25,
                    "current_momentum_score": 12.0,
                },
            ]
        )

        out = build_top_theme_baseline_snapshot(snapshot)

        observed = dict(zip(out["metric"], out["value"]))
        self.assertEqual(observed["Top-12 avg 1D"], "-0.5%")
        self.assertEqual(observed["Top-12 median 1D"], "-0.5%")
        self.assertEqual(observed["Top-12 avg composite Δ 1D"], "-0.12")
        self.assertEqual(observed["Top-12 avg momentum score"], "11.00")
        self.assertEqual(observed["Top-12 avg positive 1D breadth"], "50.0%")

    def test_top_theme_baseline_snapshot_shows_unavailable_composite_delta(self):
        snapshot = pd.DataFrame(
            [
                {
                    "avg_1d": 0.5,
                    "positive_1d_pct": 60.0,
                    "current_momentum_score": 9.0,
                }
            ]
        )

        out = build_top_theme_baseline_snapshot(snapshot)
        observed = dict(zip(out["metric"], out["value"]))

        self.assertEqual(observed["Top-12 avg composite Δ 1D"], "-")

    def test_ticker_standouts_flag_material_moves_vs_theme(self):
        snapshot = pd.DataFrame(
            [
                {"theme_id": 1, "theme": "Semis", "category": "Substrates", "avg_1d": -4.0},
                {"theme_id": 2, "theme": "Power", "category": "Grid", "avg_1d": 2.0},
            ]
        )
        members = pd.DataFrame(
            [
                {"theme_id": 1, "ticker": "AXTI", "perf_1d": 8.0},
                {"theme_id": 1, "ticker": "BASE", "perf_1d": -4.0},
                {"theme_id": 2, "ticker": "LOW", "perf_1d": -4.0},
                {"theme_id": 2, "ticker": "OK", "perf_1d": 2.5},
            ]
        )

        out = build_theme_ticker_standout_table(snapshot, members)

        self.assertIn("AXTI", out["ticker"].tolist())
        self.assertIn("LOW", out["ticker"].tolist())
        axti = out[out["ticker"] == "AXTI"].iloc[0]
        self.assertEqual(float(axti["ticker_1d"]), 8.0)
        self.assertEqual(float(axti["theme_avg_1d"]), -4.0)
        self.assertEqual(float(axti["diff_vs_theme"]), 12.0)
        self.assertEqual(TICKER_VS_THEME_LABELS["ticker_holding_up"], axti["standout"])
        self.assertIn("ticker up while theme avg is down", axti["basis"])

        low = out[out["ticker"] == "LOW"].iloc[0]
        self.assertEqual(TICKER_VS_THEME_LABELS["ticker_weak_theme_strong"], low["standout"])
        self.assertIn("ticker down while theme avg is up", low["basis"])

    def test_ticker_standouts_are_capped_to_extreme_rows(self):
        snapshot = pd.DataFrame(
            [
                {"theme_id": 1, "theme": "Theme", "category": "Group", "avg_1d": -1.0},
            ]
        )
        members = pd.DataFrame(
            [
                {"theme_id": 1, "ticker": f"T{idx:02d}", "perf_1d": float(idx + 5)}
                for idx in range(12)
            ]
        )

        out = build_theme_ticker_standout_table(snapshot, members)

        self.assertLessEqual(len(out), 8)
        self.assertEqual(out.iloc[0]["ticker"], "T11")

    def test_ticker_leadership_uses_active_governed_current_eligibility(self):
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
            conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
            conn.execute("create table refresh_runs(run_id bigint, provider varchar, status varchar, finished_at timestamp)")
            conn.execute(
                """
                create table ticker_snapshots(
                    run_id bigint,
                    ticker varchar,
                    price double,
                    perf_1d double,
                    perf_1w double,
                    perf_1m double,
                    perf_3m double,
                    perf_6m double,
                    avg_volume double,
                    snapshot_source varchar
                )
                """
            )
            conn.execute("create table symbol_refresh_status(ticker varchar, status varchar, manual_suppressed boolean)")
            conn.execute(
                """
                insert into themes values
                    (1, 'AI', 'Tech', true),
                    (2, 'Power', 'Energy', true),
                    (3, 'Inactive', 'Legacy', false)
                """
            )
            conn.execute(
                """
                insert into theme_membership values
                    (1, 'AAA'),
                    (2, 'AAA'),
                    (1, 'BBB'),
                    (1, 'LOW'),
                    (1, 'SUP'),
                    (3, 'OLD')
                """
            )
            conn.execute("insert into refresh_runs values (10, 'live', 'success', '2026-04-28 16:00:00')")
            conn.execute(
                """
                insert into ticker_snapshots values
                    (10, 'AAA', 20.0, 1.0, 12.0, 20.0, 30.0, 40.0, 2000000.0, 'live'),
                    (10, 'BBB', 10.0, 2.0, 15.0, 18.0, 25.0, 35.0, 2000000.0, 'live'),
                    (10, 'LOW', 0.5, 3.0, 99.0, 99.0, 99.0, 99.0, 2000000.0, 'live'),
                    (10, 'SUP', 20.0, 4.0, 88.0, 88.0, 88.0, 88.0, 2000000.0, 'live'),
                    (10, 'OLD', 30.0, 5.0, 77.0, 77.0, 77.0, 77.0, 2000000.0, 'live')
                """
            )
            conn.execute("insert into symbol_refresh_status values ('SUP', 'refresh_suppressed', false)")

            out = build_top_governed_ticker_leaders(conn, window="1W", top_k=25)
        finally:
            conn.close()

        self.assertEqual(out["ticker"].tolist(), ["BBB", "AAA"])
        self.assertEqual(out.loc[out["ticker"] == "AAA", "theme_count"].iloc[0], 2)
        self.assertEqual(out.loc[out["ticker"] == "AAA", "leadership_note"].iloc[0], "Multi-theme leader")
        self.assertIn("AI", out.loc[out["ticker"] == "AAA", "themes"].iloc[0])
        self.assertIn("Power", out.loc[out["ticker"] == "AAA", "themes"].iloc[0])
        self.assertIn("ticker_composite_score", out.columns)
        self.assertIn("ticker_momentum_score", out.columns)

    def test_ticker_leadership_can_sort_by_ticker_composite_score(self):
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
            conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
            conn.execute("create table refresh_runs(run_id bigint, provider varchar, status varchar, finished_at timestamp)")
            conn.execute(
                """
                create table ticker_snapshots(
                    run_id bigint,
                    ticker varchar,
                    price double,
                    perf_1d double,
                    perf_1w double,
                    perf_1m double,
                    perf_3m double,
                    perf_6m double,
                    avg_volume double,
                    snapshot_source varchar
                )
                """
            )
            conn.execute("create table symbol_refresh_status(ticker varchar, status varchar, manual_suppressed boolean)")
            conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
            conn.execute("insert into theme_membership values (1, 'AAA'), (1, 'BBB')")
            conn.execute("insert into refresh_runs values (10, 'live', 'success', '2026-04-28 16:00:00')")
            conn.execute(
                """
                insert into ticker_snapshots values
                    (10, 'AAA', 20.0, 1.0, 12.0, 20.0, 30.0, 40.0, 2000000.0, 'live'),
                    (10, 'BBB', 10.0, 2.0, 15.0, 18.0, 25.0, 35.0, 2000000.0, 'live')
                """
            )

            raw_1w = build_top_governed_ticker_leaders(conn, window="1W", sort_by="1W %", top_k=25)
            composite = build_top_governed_ticker_leaders(
                conn,
                window="1W",
                sort_by="Ticker Composite Score",
                top_k=25,
            )
        finally:
            conn.close()

        self.assertEqual(raw_1w["ticker"].tolist(), ["BBB", "AAA"])
        self.assertEqual(composite["ticker"].tolist(), ["AAA", "BBB"])
        self.assertGreater(
            float(composite.loc[composite["ticker"] == "AAA", "ticker_composite_score"].iloc[0]),
            float(composite.loc[composite["ticker"] == "BBB", "ticker_composite_score"].iloc[0]),
        )

    def test_ticker_leadership_sort_labels_cover_ui_modes_and_momentum_sort(self):
        self.assertEqual(
            set(TICKER_LEADERSHIP_SORT_COLUMNS),
            {
                "1D %",
                "1W %",
                "1M %",
                "3M %",
                "6M %",
                "Ticker Composite Score",
                "Ticker Momentum Score",
            },
        )

        conn = duckdb.connect(":memory:")
        try:
            conn.execute("create table themes(id bigint, name varchar, category varchar, is_active boolean)")
            conn.execute("create table theme_membership(theme_id bigint, ticker varchar)")
            conn.execute("create table refresh_runs(run_id bigint, provider varchar, status varchar, finished_at timestamp)")
            conn.execute(
                """
                create table ticker_snapshots(
                    run_id bigint,
                    ticker varchar,
                    price double,
                    perf_1d double,
                    perf_1w double,
                    perf_1m double,
                    perf_3m double,
                    perf_6m double,
                    avg_volume double,
                    snapshot_source varchar
                )
                """
            )
            conn.execute("create table symbol_refresh_status(ticker varchar, status varchar, manual_suppressed boolean)")
            conn.execute("insert into themes values (1, 'AI', 'Tech', true)")
            conn.execute("insert into theme_membership values (1, 'AAA'), (1, 'BBB')")
            conn.execute("insert into refresh_runs values (10, 'live', 'success', '2026-04-28 16:00:00')")
            conn.execute(
                """
                insert into ticker_snapshots values
                    (10, 'AAA', 20.0, 1.0, 10.0, 50.0, 50.0, 40.0, 2000000.0, 'live'),
                    (10, 'BBB', 10.0, 2.0, 15.0, 15.0, 15.0, 35.0, 2000000.0, 'live')
                """
            )

            raw_1w = build_top_governed_ticker_leaders(conn, window="1W", sort_by="1W %", top_k=25)
            momentum = build_top_governed_ticker_leaders(
                conn,
                window="1W",
                sort_by="Ticker Momentum Score",
                top_k=25,
            )
        finally:
            conn.close()

        self.assertEqual(raw_1w["ticker"].tolist(), ["BBB", "AAA"])
        self.assertEqual(momentum["ticker"].tolist(), ["AAA", "BBB"])
        self.assertGreater(
            float(momentum.loc[momentum["ticker"] == "AAA", "ticker_momentum_score"].iloc[0]),
            float(momentum.loc[momentum["ticker"] == "BBB", "ticker_momentum_score"].iloc[0]),
        )


if __name__ == "__main__":
    unittest.main()
