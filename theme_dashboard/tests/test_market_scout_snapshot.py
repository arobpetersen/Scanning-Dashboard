import pandas as pd

from src.market_scout_snapshot import (
    market_backdrop_read_line,
    top_database_ticker_snapshot,
    top_theme_snapshot,
)


def test_top_database_ticker_snapshot_prefers_eligible_standouts_without_fake_1d():
    raw = pd.DataFrame(
        [
            {
                "run_id": 1,
                "ticker": "AAA",
                "theme": "AI - Software",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 15.0,
                "perf_1m": 35.0,
                "perf_3m": 40.0,
            },
            {
                "run_id": 1,
                "ticker": "BBB",
                "theme": "Clean Energy",
                "is_active": True,
                "status": "refresh_suppressed",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 50.0,
                "perf_1m": 80.0,
                "perf_3m": 90.0,
            },
        ]
    )

    out = top_database_ticker_snapshot(raw, limit=10)

    assert out["Ticker"].tolist() == ["AAA"]
    assert "1D" not in out.columns
    assert "Momentum" not in out.columns
    assert out.loc[0, "Theme"] == "AI - Software"
    assert out.loc[0, "1W"] == "15.0%"
    assert out.loc[0, "1M"] == "35.0%"
    assert out.loc[0, "Read"] == "Extreme 1M leader"


def test_top_database_ticker_snapshot_uses_varied_short_read_labels():
    raw = pd.DataFrame(
        [
            {
                "run_id": 1,
                "ticker": "AAA",
                "theme": "AI - Software",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 15.0,
                "perf_1m": 35.0,
                "perf_3m": 40.0,
            },
            {
                "run_id": 1,
                "ticker": "BBB",
                "theme": "Clean Energy",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 4.0,
                "perf_1m": 30.0,
                "perf_3m": 20.0,
            },
            {
                "run_id": 1,
                "ticker": "CCC",
                "theme": "Cloud",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 13.0,
                "perf_1m": 18.0,
                "perf_3m": 18.0,
            },
            {
                "run_id": 1,
                "ticker": "DDD",
                "theme": "AI - Software",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 3.0,
                "perf_1m": 9.0,
                "perf_3m": 35.0,
            },
            {
                "run_id": 1,
                "ticker": "EEE",
                "theme": "AI - Software",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 8.0,
                "perf_1m": 18.0,
                "perf_3m": 20.0,
            },
            {
                "run_id": 1,
                "ticker": "EEE",
                "theme": "Cloud",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1w": 8.0,
                "perf_1m": 18.0,
                "perf_3m": 20.0,
            },
        ]
    )

    out = top_database_ticker_snapshot(raw, limit=10)
    reads = dict(zip(out["Ticker"], out["Read"], strict=False))

    assert reads["AAA"] == "Extreme 1M leader"
    assert reads["BBB"] == "Outlier-led; verify group"
    assert reads["CCC"] == "Theme confirmation name"
    assert reads["DDD"] == "High momentum; check extension"
    assert reads["EEE"] == "Repeated scout name"


def test_top_database_ticker_snapshot_includes_1d_only_when_available():
    raw = pd.DataFrame(
        [
            {
                "run_id": 1,
                "ticker": "AAA",
                "theme": "AI - Software",
                "is_active": True,
                "status": "active",
                "price": 20.0,
                "avg_volume": 1_000_000,
                "perf_1d": 6.0,
                "perf_1w": 15.0,
                "perf_1m": 35.0,
                "perf_3m": 40.0,
            }
        ]
    )

    out = top_database_ticker_snapshot(raw, limit=10)

    assert out.loc[0, "1D"] == "6.0%"


def test_top_theme_snapshot_formats_compact_ranked_themes():
    metrics = pd.DataFrame(
        [
            {
                "theme_id": 2,
                "theme": "Clean Energy",
                "is_active": True,
                "avg_1w": 8.5,
                "avg_1m": 22.0,
                "positive_1w_breadth_pct": 72.0,
                "standardized_composite_score": 55.0,
            },
            {
                "theme_id": 1,
                "theme": "AI - Software",
                "is_active": True,
                "avg_1w": 4.0,
                "avg_1m": 12.0,
                "positive_1w_breadth_pct": 45.0,
                "standardized_composite_score": 40.0,
            },
        ]
    )
    rankings = pd.DataFrame({"theme_id": [2, 1]})

    out = top_theme_snapshot(metrics, rankings, limit=1)

    assert out.to_dict("records") == [
        {
            "Theme": "Clean Energy",
            "Rank": "#1",
            "1W": "8.5%",
            "1M": "22.0%",
            "Breadth": "72%",
            "Quality / Read": "Broad, strong 1M",
        }
    ]


def test_top_theme_snapshot_uses_varied_trader_read_labels():
    metrics = pd.DataFrame(
        [
            {
                "theme_id": 1,
                "theme": "Broad Theme",
                "is_active": True,
                "avg_1w": 3.0,
                "avg_1m": 10.0,
                "positive_1w_breadth_pct": 70.0,
                "standardized_composite_score": 50.0,
            },
            {
                "theme_id": 2,
                "theme": "Trend Theme",
                "is_active": True,
                "avg_1w": 2.0,
                "avg_1m": 5.0,
                "positive_1w_breadth_pct": 50.0,
                "standardized_composite_score": 45.0,
            },
            {
                "theme_id": 3,
                "theme": "Thin Theme",
                "is_active": True,
                "avg_1w": -1.0,
                "avg_1m": 3.0,
                "positive_1w_breadth_pct": 30.0,
                "eligible_breadth_pct": 35.0,
                "standardized_composite_score": 40.0,
            },
            {
                "theme_id": 4,
                "theme": "Watch Theme",
                "is_active": True,
                "avg_1w": -1.0,
                "avg_1m": 3.0,
                "positive_1w_breadth_pct": 50.0,
                "standardized_composite_score": 35.0,
            },
        ]
    )
    rankings = pd.DataFrame({"theme_id": [1, 2, 3, 4]})

    out = top_theme_snapshot(metrics, rankings, limit=4)

    assert out["Quality / Read"].tolist() == [
        "Broad leader",
        "Positive current trend.",
        "Outlier risk",
        "Watch breadth",
    ]


def test_market_backdrop_read_line_is_template_based():
    assert (
        market_backdrop_read_line({"move_label": "Strong Down Day", "character_tag": "Mixed"})
        == "Weak QQQ tape means positive clusters deserve more attention."
    )
    assert (
        market_backdrop_read_line({"move_label": "Strong Up Day", "character_tag": "Trend Up"})
        == "Strong QQQ tape means broad strength may be more market-beta driven."
    )
    assert (
        market_backdrop_read_line({"move_label": "Flat / Mixed", "character_tag": "Quiet"})
        == "Quiet QQQ tape means theme clusters may be more group-specific."
    )
    assert (
        market_backdrop_read_line({"move_label": "Flat / Mixed", "character_tag": "Volatile Fade"})
        == "Choppy or fading QQQ tape adds caution; require cleaner follow-through."
    )
