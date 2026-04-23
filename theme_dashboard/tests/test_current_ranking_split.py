from unittest.mock import patch

import pandas as pd

from src.rankings import (
    _build_current_ranking_metrics,
    _load_current_ranking_constituents,
    compute_current_ranking_operating_snapshot,
    compute_current_ranking_snapshot,
    compute_current_ranking_validation_snapshot,
)


def test_compute_current_ranking_operating_snapshot_returns_only_core_frames():
    current = pd.DataFrame(
        [
            {
                "theme_id": 1,
                "theme": "Alpha",
                "category": "Growth",
                "avg_1w": 1.0,
                "avg_1m": 2.0,
                "avg_3m": 3.0,
                "positive_1m_breadth_pct": 60.0,
                "legacy_composite_score": 1.5,
            }
        ]
    )
    legacy = current.copy()
    standardized = current.copy()

    with patch("src.rankings._build_current_ranking_base_snapshot", return_value=(current, legacy, standardized)), patch(
        "src.rankings.preferred_theme_snapshot_source",
        return_value=None,
    ):
        out = compute_current_ranking_operating_snapshot(conn=None)

    assert set(out.keys()) == {"theme_metrics", "rankings", "standardized_rankings"}
    assert out["theme_metrics"].equals(current)
    assert "delta_avg_1w" in out["rankings"].columns
    assert out["rankings"]["delta_avg_1w"].isna().all()
    assert "standardized_comparison" not in out


def test_compute_current_ranking_validation_snapshot_returns_only_validation_frames():
    current = pd.DataFrame([{"theme_id": 1, "avg_1w": 1.0, "avg_1m": 2.0}])
    legacy = pd.DataFrame([{"theme_id": 1}])
    standardized = pd.DataFrame([{"theme_id": 1}])
    momentum_rankings = pd.DataFrame([{"theme_id": 1}])
    standardized_comparison = pd.DataFrame([{"theme_id": 1, "rank_shift_vs_legacy": 0}])
    momentum_comparison = pd.DataFrame([{"theme_id": 1, "rank_shift_vs_1w": 0}])

    with patch("src.rankings._build_current_ranking_base_snapshot", return_value=(current, legacy, standardized)), patch(
        "src.rankings._finalize_current_window_rankings",
        return_value=pd.DataFrame([{"theme_id": 1}]),
    ), patch(
        "src.rankings._finalize_current_rankings",
        return_value=momentum_rankings,
    ), patch(
        "src.rankings._build_standardized_composite_validation",
        return_value=standardized_comparison,
    ), patch(
        "src.rankings._build_current_momentum_validation",
        return_value=momentum_comparison,
    ):
        out = compute_current_ranking_validation_snapshot(conn=None)

    assert set(out.keys()) == {
        "standardized_comparison",
        "current_momentum_rankings",
        "current_momentum_comparison",
    }
    assert out["standardized_comparison"].equals(standardized_comparison)
    assert out["current_momentum_rankings"].equals(momentum_rankings)
    assert out["current_momentum_comparison"].equals(momentum_comparison)


def test_compute_current_ranking_snapshot_merges_operating_and_validation_views():
    current = pd.DataFrame([{"theme_id": 1}])
    legacy = pd.DataFrame([{"theme_id": 1}])
    standardized = pd.DataFrame([{"theme_id": 1}])
    operating = {"theme_metrics": current, "rankings": legacy, "standardized_rankings": standardized}
    validation = {
        "standardized_comparison": pd.DataFrame([{"theme_id": 1}]),
        "current_momentum_rankings": pd.DataFrame([{"theme_id": 1}]),
        "current_momentum_comparison": pd.DataFrame([{"theme_id": 1}]),
    }

    with patch("src.rankings._build_current_ranking_base_snapshot", return_value=(current, legacy, standardized)), patch(
        "src.rankings._build_current_ranking_operating_snapshot_from_base",
        return_value=operating,
    ), patch(
        "src.rankings._build_current_ranking_validation_snapshot_from_base",
        return_value=validation,
    ):
        out = compute_current_ranking_snapshot(conn=None)

    assert out == {**operating, **validation}


def test_load_current_ranking_constituents_uses_operating_atr_helper():
    class FakeConn:
        def execute(self, sql):
            class Result:
                def __init__(self, frame):
                    self._frame = frame

                def df(self):
                    return self._frame

            if "FROM themes t" in sql:
                return Result(pd.DataFrame([{"theme_id": 1, "theme": "Alpha", "category": "Growth", "is_active": True, "ticker": "AAA"}]))
            if "FROM symbol_refresh_status" in sql:
                return Result(pd.DataFrame([{"ticker": "AAA", "status": "active"}]))
            raise AssertionError(sql)

    latest = pd.DataFrame(
        [{"ticker": "AAA", "run_id": 1, "snapshot_time": "2026-04-15 16:00:00", "price": 10.0, "avg_volume": 1000.0, "perf_1d": 0.5, "perf_1w": 1.0, "perf_1m": 2.0, "perf_3m": 3.0, "perf_6m": 4.0}]
    )
    atr = pd.DataFrame([{"ticker": "AAA", "perf_1w_atr_units": 0.5, "perf_1m_atr_units": 1.5}])

    with patch("src.rankings.latest_ticker_snapshots", return_value=latest), patch(
        "src.rankings.latest_ticker_history_atr_companion_fields",
        return_value=atr,
    ) as operating_helper, patch(
        "src.rankings.table_exists",
        return_value=True,
    ), patch(
        "src.rankings.table_has_column",
        return_value=True,
    ):
        out = _load_current_ranking_constituents(FakeConn())

    operating_helper.assert_called_once()
    assert "perf_1d" in out.columns
    assert "perf_6m" in out.columns
    assert "perf_1w_atr_units" in out.columns
    assert "perf_1m_atr_units" in out.columns


def test_build_current_ranking_metrics_carries_avg_1d_without_changing_other_metrics():
    raw = pd.DataFrame(
        [
            {
                "theme_id": 1,
                "theme": "Alpha",
                "category": "Growth",
                "is_active": True,
                "ticker": "AAA",
                "run_id": 1,
                "snapshot_time": "2026-04-15 16:00:00",
                "price": 10.0,
                "avg_volume": 5_000_000.0,
                "perf_1d": 1.0,
                "perf_1w": 2.0,
                "perf_1m": 3.0,
                "perf_3m": 4.0,
                "perf_6m": 5.0,
                "status": "active",
            },
            {
                "theme_id": 1,
                "theme": "Alpha",
                "category": "Growth",
                "is_active": True,
                "ticker": "BBB",
                "run_id": 1,
                "snapshot_time": "2026-04-15 16:00:00",
                "price": 20.0,
                "avg_volume": 5_000_000.0,
                "perf_1d": 3.0,
                "perf_1w": 4.0,
                "perf_1m": 5.0,
                "perf_3m": 6.0,
                "perf_6m": 7.0,
                "status": "active",
            },
        ]
    )

    out = _build_current_ranking_metrics(raw)

    assert float(out.iloc[0]["avg_1d"]) == 2.0
    assert float(out.iloc[0]["avg_1w"]) == 3.0
    assert float(out.iloc[0]["avg_1m"]) == 4.0
    assert float(out.iloc[0]["avg_6m"]) == 6.0


def test_build_current_ranking_metrics_uses_new_standardized_base_mix_without_changing_atr_or_momentum_structure():
    raw = pd.DataFrame(
        [
            {
                "theme_id": 1,
                "theme": "Alpha",
                "category": "Growth",
                "is_active": True,
                "ticker": "AAA",
                "run_id": 1,
                "snapshot_time": "2026-04-15 16:00:00",
                "price": 10.0,
                "avg_volume": 5_000_000.0,
                "perf_1d": 1.0,
                "perf_1w": 2.0,
                "perf_1m": 6.0,
                "perf_3m": 10.0,
                "perf_1w_atr_units": 1.0,
                "perf_1m_atr_units": 3.0,
                "status": "active",
            },
            {
                "theme_id": 1,
                "theme": "Alpha",
                "category": "Growth",
                "is_active": True,
                "ticker": "BBB",
                "run_id": 1,
                "snapshot_time": "2026-04-15 16:00:00",
                "price": 20.0,
                "avg_volume": 5_000_000.0,
                "perf_1d": 2.0,
                "perf_1w": 4.0,
                "perf_1m": 8.0,
                "perf_3m": 12.0,
                "perf_1w_atr_units": 2.0,
                "perf_1m_atr_units": 4.0,
                "status": "active",
            },
        ]
    )

    out = _build_current_ranking_metrics(raw)

    assert float(out.iloc[0]["avg_1w"]) == 3.0
    assert float(out.iloc[0]["avg_1m"]) == 7.0
    assert float(out.iloc[0]["avg_3m"]) == 11.0
    assert float(out.iloc[0]["standardized_base_strength_score"]) == 7.2
    assert float(out.iloc[0]["composite_atr_base_strength_score"]) == 2.9
    assert float(out.iloc[0]["current_momentum_raw_score"]) == 4.2
