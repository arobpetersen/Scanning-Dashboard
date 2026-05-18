import pandas as pd

from src.theme_pattern_audit import (
    build_market_scout_items,
    build_theme_pattern_signals,
    format_theme_pattern_signal_evidence,
    no_market_scout_items_message,
)


def _constituent(theme_id, theme, ticker, perf_1d, perf_1w, perf_1m, *, run_id=1):
    return {
        "theme_id": theme_id,
        "theme": theme,
        "category": "Test",
        "is_active": True,
        "ticker": ticker,
        "run_id": run_id,
        "price": 10.0,
        "avg_volume": 2_000_000.0,
        "perf_1d": perf_1d,
        "perf_1w": perf_1w,
        "perf_1m": perf_1m,
        "perf_3m": 5.0,
        "status": "active",
    }


def _metric(theme_id, theme, rank, avg_1w, avg_1m, breadth=70.0, eligible=4):
    return {
        "theme_id": theme_id,
        "theme": theme,
        "category": "Test",
        "is_active": True,
        "ticker_count": eligible,
        "eligible_ticker_count": eligible,
        "eligible_standardized_count": eligible,
        "avg_1w": avg_1w,
        "avg_1m": avg_1m,
        "avg_3m": 5.0,
        "positive_1w_breadth_pct": breadth,
        "positive_1m_breadth_pct": breadth,
        "standardized_composite_score": 10.0,
        "current_momentum_score": 8.0,
        "rank": rank,
    }


def _rankings(metrics):
    return pd.DataFrame(
        [
            {
                "theme_id": row["theme_id"],
                "theme": row["theme"],
                "standardized_composite_score": row["standardized_composite_score"],
                "rank": row["rank"],
            }
            for row in metrics
        ]
    )


def _signals(constituents, metrics):
    return build_theme_pattern_signals(
        pd.DataFrame(constituents),
        pd.DataFrame(metrics),
        _rankings(metrics),
        limit=20,
    )


def _signals_with_context(constituents, metrics, qqq_market_context):
    return build_theme_pattern_signals(
        pd.DataFrame(constituents),
        pd.DataFrame(metrics),
        _rankings(metrics),
        limit=20,
        qqq_market_context=qqq_market_context,
    )


def test_broad_same_theme_thrust_flags_multiple_strong_current_movers():
    metrics = [_metric(1, "AI Infrastructure", 4, 14.0, 18.0)]
    constituents = [
        _constituent(1, "AI Infrastructure", "AAA", 6.0, 8.0, 12.0),
        _constituent(1, "AI Infrastructure", "BBB", 1.0, 13.0, 16.0),
        _constituent(1, "AI Infrastructure", "CCC", 2.0, 15.0, 18.0),
        _constituent(1, "AI Infrastructure", "DDD", 0.0, 2.0, 4.0),
    ]

    out = _signals(constituents, metrics)

    signal = next(signal for signal in out if signal.signal_type == "Broad Same-Theme Thrust")
    assert signal.theme == "AI Infrastructure"
    assert set(signal.tickers) == {"AAA", "BBB", "CCC"}
    assert "3 eligible members" in signal.why_notable


def test_extreme_ticker_standout_flags_material_theme_outperformance():
    metrics = [_metric(2, "Nuclear Power", 8, 8.0, 10.0)]
    constituents = [
        _constituent(2, "Nuclear Power", "NUKE", 2.0, 16.0, 45.0),
        _constituent(2, "Nuclear Power", "BASE", 1.0, 7.0, 8.0),
        _constituent(2, "Nuclear Power", "GRID", 1.0, 6.0, 7.0),
    ]

    out = _signals(constituents, metrics)

    signal = next(signal for signal in out if signal.signal_type == "Extreme Ticker Standout")
    assert signal.theme == "Nuclear Power"
    assert signal.tickers == ("NUKE",)
    assert "versus theme avg" in signal.why_notable


def test_outlier_led_theme_distinguishes_lack_of_broad_confirmation():
    metrics = [_metric(3, "Quantum", 5, 10.0, 25.0)]
    constituents = [
        _constituent(3, "Quantum", "QWIN", 1.0, 10.0, 80.0),
        _constituent(3, "Quantum", "QLAG", 0.0, 2.0, 5.0),
        _constituent(3, "Quantum", "QSOFT", 0.0, 1.0, 4.0),
    ]

    out = _signals(constituents, metrics)

    signal = next(signal for signal in out if signal.signal_type == "Outlier-Led Theme (Narrow/Fragile)")
    assert signal.theme == "Quantum"
    assert signal.tickers == ("QWIN",)
    assert signal.read == "Outlier is carrying a narrow, fragile theme read."
    assert signal.metadata["broad_confirmation"] is False


def test_outlier_led_theme_marks_broad_confirmation_when_other_members_are_moving():
    metrics = [_metric(7, "Space Infrastructure", 6, 18.0, 28.0)]
    constituents = [
        _constituent(7, "Space Infrastructure", "MOON", 1.0, 16.0, 85.0),
        _constituent(7, "Space Infrastructure", "ORBT", 1.0, 14.0, 18.0),
        _constituent(7, "Space Infrastructure", "LIFT", 6.0, 8.0, 17.0),
        _constituent(7, "Space Infrastructure", "SAT", 0.0, 3.0, 4.0),
    ]

    out = _signals(constituents, metrics)

    signal = next(signal for signal in out if signal.signal_type == "Outlier-Led Theme (Broadly Confirmed)")
    assert signal.theme == "Space Infrastructure"
    assert signal.tickers == ("MOON",)
    assert signal.read == "Outlier is leading, but breadth still confirms the theme."
    assert signal.metadata["broad_confirmation"] is True


def test_emerging_cluster_flags_lower_ranked_multi_ticker_strength():
    metrics = [_metric(4, "Water Tech", 18, 11.0, 13.0)]
    constituents = [
        _constituent(4, "Water Tech", "HHO", 6.0, 9.0, 12.0),
        _constituent(4, "Water Tech", "PIPE", 2.0, 14.0, 15.0),
        _constituent(4, "Water Tech", "FLOW", 0.0, 3.0, 5.0),
    ]

    out = _signals(constituents, metrics)

    signal = next(signal for signal in out if signal.signal_type == "Emerging Cluster")
    assert signal.theme == "Water Tech"
    assert set(signal.tickers) == {"HHO", "PIPE"}
    assert "Rank 18 theme" in signal.why_notable
    assert "across 4 eligible contributors" in signal.why_notable


def test_emerging_cluster_requires_enough_eligible_coverage():
    metrics = [_metric(8, "Tiny Biotech", 22, 18.0, 22.0, eligible=2)]
    constituents = [
        _constituent(8, "Tiny Biotech", "BIOA", 6.0, 18.0, 24.0),
        _constituent(8, "Tiny Biotech", "BIOB", 1.0, 15.0, 20.0),
    ]

    out = _signals(constituents, metrics)

    assert all(signal.signal_type != "Emerging Cluster" for signal in out)


def test_weakening_narrowing_flags_top_theme_with_thin_breadth():
    metrics = [_metric(5, "Robotics", 2, -1.0, 9.0, breadth=25.0, eligible=5)]
    constituents = [
        _constituent(5, "Robotics", "BOT", 0.0, -2.0, 8.0),
        _constituent(5, "Robotics", "ARM", 0.0, -1.0, 9.0),
        _constituent(5, "Robotics", "AUTO", 0.0, 0.0, 10.0),
    ]

    out = _signals(constituents, metrics)

    signal = next(signal for signal in out if signal.signal_type == "Weakening / Narrowing Theme")
    assert signal.theme == "Robotics"
    assert "Top-12 theme" in signal.why_notable
    assert "Leadership quality is narrowing" in signal.read


def test_unchanged_high_rank_theme_without_pattern_is_not_reported():
    metrics = [_metric(6, "Mega Cap Software", 1, 6.0, 8.0, breadth=80.0, eligible=6)]
    constituents = [
        _constituent(6, "Mega Cap Software", "SOFT1", 1.0, 6.0, 8.0),
        _constituent(6, "Mega Cap Software", "SOFT2", 1.0, 7.0, 8.0),
        _constituent(6, "Mega Cap Software", "SOFT3", 1.0, 5.0, 8.0),
    ]

    out = _signals(constituents, metrics)

    assert out == []


def test_broad_thrust_is_prioritized_above_extreme_single_name_standout():
    metrics = [
        _metric(9, "Copper Miners", 7, 16.0, 20.0),
        _metric(10, "Single Name Mania", 9, 2.0, 4.0),
    ]
    constituents = [
        _constituent(9, "Copper Miners", "CUA", 6.0, 14.0, 16.0),
        _constituent(9, "Copper Miners", "CUB", 2.0, 15.0, 17.0),
        _constituent(9, "Copper Miners", "CUC", 1.0, 16.0, 18.0),
        _constituent(10, "Single Name Mania", "ONE", 1.0, 90.0, 100.0),
        _constituent(10, "Single Name Mania", "TWO", 1.0, 1.0, 2.0),
        _constituent(10, "Single Name Mania", "THR", 1.0, 1.0, 2.0),
    ]

    out = build_theme_pattern_signals(
        pd.DataFrame(constituents),
        pd.DataFrame(metrics),
        _rankings(metrics),
        limit=1,
    )

    assert len(out) == 1
    assert out[0].signal_type == "Broad Same-Theme Thrust"
    assert out[0].theme == "Copper Miners"


def test_redundant_extreme_standout_is_suppressed_when_theme_has_broader_pattern():
    metrics = [_metric(11, "Advanced Materials", 10, 20.0, 25.0)]
    constituents = [
        _constituent(11, "Advanced Materials", "MATX", 6.0, 55.0, 75.0),
        _constituent(11, "Advanced Materials", "FIBR", 2.0, 15.0, 18.0),
        _constituent(11, "Advanced Materials", "ALLO", 1.0, 14.0, 17.0),
    ]

    out = _signals(constituents, metrics)

    assert any(signal.signal_type == "Broad Same-Theme Thrust" for signal in out)
    assert all(signal.signal_type != "Extreme Ticker Standout" for signal in out)


def test_signal_evidence_formatter_includes_broad_thrust_thresholds_and_tickers():
    metrics = [_metric(12, "Power Semis", 4, 14.0, 18.0)]
    constituents = [
        _constituent(12, "Power Semis", "PWR1", 6.0, 8.0, 12.0),
        _constituent(12, "Power Semis", "PWR2", 1.0, 13.0, 16.0),
        _constituent(12, "Power Semis", "PWR3", 2.0, 15.0, 18.0),
    ]

    signal = next(signal for signal in _signals(constituents, metrics) if signal.signal_type == "Broad Same-Theme Thrust")
    evidence = format_theme_pattern_signal_evidence(signal)

    assert any("Top involved tickers: PWR3, PWR2, PWR1." == item for item in evidence)
    assert any(">= 5.0% 1D or >= 12.0% 1W" in item for item in evidence)
    assert any("Theme 1W average: 14.0%." == item for item in evidence)


def test_signal_evidence_formatter_includes_standout_theme_average_comparison():
    metrics = [_metric(13, "Clean Fuels", 8, 8.0, 10.0)]
    constituents = [
        _constituent(13, "Clean Fuels", "FUEL", 2.0, 16.0, 45.0),
        _constituent(13, "Clean Fuels", "BASE", 1.0, 7.0, 8.0),
        _constituent(13, "Clean Fuels", "GRID", 1.0, 6.0, 7.0),
    ]

    signal = next(signal for signal in _signals(constituents, metrics) if signal.signal_type == "Extreme Ticker Standout")
    evidence = format_theme_pattern_signal_evidence(signal)

    assert any("1M excess 35.0%" in item for item in evidence)
    assert any("Ticker move: 45.0%; theme average: 10.0%." == item for item in evidence)


def test_signal_evidence_formatter_includes_outlier_breadth_status():
    metrics = [_metric(14, "Frontier Compute", 5, 10.0, 25.0)]
    constituents = [
        _constituent(14, "Frontier Compute", "FAST", 1.0, 10.0, 80.0),
        _constituent(14, "Frontier Compute", "SLOW", 0.0, 2.0, 5.0),
        _constituent(14, "Frontier Compute", "MID", 0.0, 1.0, 4.0),
    ]

    signal = next(signal for signal in _signals(constituents, metrics) if signal.signal_type == "Outlier-Led Theme (Narrow/Fragile)")
    evidence = format_theme_pattern_signal_evidence(signal)

    assert any("leader excess 55.0%" in item for item in evidence)
    assert any("Breadth status: narrow/fragile" in item for item in evidence)


def test_signal_evidence_formatter_includes_emerging_cluster_coverage():
    metrics = [_metric(15, "Grid Storage", 18, 11.0, 13.0)]
    constituents = [
        _constituent(15, "Grid Storage", "BATT", 6.0, 9.0, 12.0),
        _constituent(15, "Grid Storage", "CELL", 2.0, 14.0, 15.0),
        _constituent(15, "Grid Storage", "CAP", 0.0, 3.0, 5.0),
    ]

    signal = next(signal for signal in _signals(constituents, metrics) if signal.signal_type == "Emerging Cluster")
    evidence = format_theme_pattern_signal_evidence(signal)

    assert any("rank #18 >= 13" in item for item in evidence)
    assert any("eligible coverage 100%" in item and "strong coverage 50%" in item for item in evidence)


def test_weak_qqq_and_strong_broad_theme_thrust_becomes_more_notable():
    metrics = [_metric(16, "Relative Strength Theme", 6, 14.0, 18.0)]
    constituents = [
        _constituent(16, "Relative Strength Theme", "RSA", 6.0, 14.0, 16.0),
        _constituent(16, "Relative Strength Theme", "RSB", 1.0, 15.0, 17.0),
        _constituent(16, "Relative Strength Theme", "RSC", 1.0, 16.0, 18.0),
    ]

    base = next(signal for signal in _signals(constituents, metrics) if signal.signal_type == "Broad Same-Theme Thrust")
    contextual = next(
        signal
        for signal in _signals_with_context(
            constituents,
            metrics,
            {"move_label": "Down Day", "character_tag": "Trend Down", "qqq_pct_change": -1.0},
        )
        if signal.signal_type == "Broad Same-Theme Thrust"
    )

    assert contextual.priority > base.priority
    assert "relative strength" in contextual.read
    assert contextual.metadata["theme_move_vs_qqq_pct"] == 15.0


def test_quiet_qqq_and_theme_cluster_becomes_more_notable():
    metrics = [_metric(17, "Quiet Tape Cluster", 18, 11.0, 13.0)]
    constituents = [
        _constituent(17, "Quiet Tape Cluster", "QTA", 6.0, 9.0, 12.0),
        _constituent(17, "Quiet Tape Cluster", "QTB", 2.0, 14.0, 15.0),
        _constituent(17, "Quiet Tape Cluster", "QTC", 0.0, 3.0, 5.0),
    ]

    base = next(signal for signal in _signals(constituents, metrics) if signal.signal_type == "Emerging Cluster")
    contextual = next(
        signal
        for signal in _signals_with_context(
            constituents,
            metrics,
            {"move_label": "Flat / Mixed", "character_tag": "Quiet", "qqq_pct_change": 0.1},
        )
        if signal.signal_type == "Emerging Cluster"
    )

    assert contextual.priority > base.priority
    assert "theme-specific" in contextual.read
    assert "group-specific activity" not in contextual.read


def test_strong_qqq_downgrades_generic_broad_thrust_priority_unless_theme_strongly_outperforms():
    generic_metrics = [_metric(18, "Generic Beta", 7, 7.0, 9.0)]
    outperform_metrics = [_metric(19, "Real Leader", 7, 16.0, 20.0)]
    generic_constituents = [
        _constituent(18, "Generic Beta", "BTA", 6.0, 12.0, 13.0),
        _constituent(18, "Generic Beta", "BTB", 1.0, 13.0, 13.0),
        _constituent(18, "Generic Beta", "BTC", 1.0, 12.5, 13.0),
    ]
    outperform_constituents = [
        _constituent(19, "Real Leader", "LDA", 6.0, 17.0, 20.0),
        _constituent(19, "Real Leader", "LDB", 1.0, 16.0, 20.0),
        _constituent(19, "Real Leader", "LDC", 1.0, 15.0, 20.0),
    ]
    qqq_context = {"move_label": "Up Day", "character_tag": "Trend Up", "qqq_pct_change": 1.0}

    generic_base = next(signal for signal in _signals(generic_constituents, generic_metrics) if signal.signal_type == "Broad Same-Theme Thrust")
    generic_contextual = next(signal for signal in _signals_with_context(generic_constituents, generic_metrics, qqq_context) if signal.signal_type == "Broad Same-Theme Thrust")
    outperform_base = next(signal for signal in _signals(outperform_constituents, outperform_metrics) if signal.signal_type == "Broad Same-Theme Thrust")
    outperform_contextual = next(signal for signal in _signals_with_context(outperform_constituents, outperform_metrics, qqq_context) if signal.signal_type == "Broad Same-Theme Thrust")

    assert generic_contextual.priority < generic_base.priority
    assert "Nasdaq tailwind" in generic_contextual.read
    assert outperform_contextual.priority == outperform_base.priority
    assert "stronger than the broad Nasdaq bid" in outperform_contextual.read


def test_wide_choppy_qqq_adds_caution_context_language():
    metrics = [_metric(20, "Choppy Tape Theme", 6, 14.0, 18.0)]
    constituents = [
        _constituent(20, "Choppy Tape Theme", "CTA", 6.0, 14.0, 16.0),
        _constituent(20, "Choppy Tape Theme", "CTB", 1.0, 15.0, 17.0),
        _constituent(20, "Choppy Tape Theme", "CTC", 1.0, 16.0, 18.0),
    ]

    contextual = next(
        signal
        for signal in _signals_with_context(
            constituents,
            metrics,
            {"move_label": "Flat / Mixed", "character_tag": "Volatile Chop", "qqq_pct_change": 0.2},
        )
        if signal.signal_type == "Broad Same-Theme Thrust"
    )
    evidence = format_theme_pattern_signal_evidence(contextual)

    assert "Use extra caution" in contextual.read
    assert any("QQQ is Wide Choppy Day" in item for item in evidence)


def test_market_scout_groups_repeated_broad_thrust_signals_into_one_item():
    signals = [
        _raw_signal("Broad Same-Theme Thrust", "Alpha", ["AAA", "AAB"], priority=120.0),
        _raw_signal("Broad Same-Theme Thrust", "Beta", ["BBB", "BBC"], priority=118.0),
        _raw_signal("Broad Same-Theme Thrust", "Gamma", ["CCC", "CCD"], priority=116.0),
    ]

    items = build_market_scout_items(signals, limit=5)

    assert len(items) == 1
    assert items[0]["pattern"] == "Broad Same-Theme Thrust"
    assert "3 themes" in items[0]["headline"]
    assert len(items[0]["signal_indices"]) == 3


def test_market_scout_coherent_overlapping_tickers_across_themes_produce_cluster_item():
    signals = [
        _raw_signal("Broad Same-Theme Thrust", "LIDAR", ["AEVA", "OUST", "MBLY"], priority=120.0),
        _raw_signal("Emerging Cluster", "Autonomous Vehicles", ["AEVA", "MBLY", "OUST"], priority=110.0),
        _raw_signal("Extreme Ticker Standout", "Computer Vision", ["OUST", "AEVA", "MBLY"], priority=100.0),
        _raw_signal("Extreme Ticker Standout", "Gamma", ["CCC"], priority=100.0),
    ]

    items = build_market_scout_items(signals, limit=5)

    assert items[0]["pattern"] == "Coherent Overlap Cluster"
    assert items[0]["headline"] == "Mobility / Vision overlap"
    assert items[0]["tickers_to_inspect"] == ["AEVA", "OUST", "MBLY"]
    assert "Treat this as one underlying cluster" in items[0]["why_it_matters"]
    assert "inflating related theme reads" not in items[0]["next_look"]


def test_market_scout_mixed_overlap_produces_quality_warning_without_qqq_language():
    signals = [
        _raw_signal("Broad Same-Theme Thrust", "Space Launch", ["RKLB", "AAOI"], priority=120.0),
        _raw_signal("Emerging Cluster", "Enterprise SaaS", ["RKLB", "NOW"], priority=110.0),
        _raw_signal("Extreme Ticker Standout", "Cybersecurity", ["AAOI", "PANW"], priority=100.0),
        _raw_signal("Extreme Ticker Standout", "Gamma", ["CCC"], priority=100.0),
    ]

    items = build_market_scout_items(
        signals,
        qqq_market_context={"move_label": "Strong Down Day", "character_tag": "Trend Down", "qqq_pct_change": -1.51},
        limit=5,
    )
    warning = next(item for item in items if item["pattern"] == "Overlap Warning")

    assert warning["metadata"]["quality_note"] is True
    assert warning["priority"] < 160.0
    assert warning["headline"] == "Mixed repeated leaders"
    assert "do not form one clean theme cluster" in warning["why_it_matters"]
    assert "proper theme contexts" in warning["why_it_matters"]
    assert "Against a -1.5% QQQ tape" not in warning["why_it_matters"]
    assert "inflating related theme reads" not in warning["next_look"]


def test_market_scout_generic_ai_cloud_overlap_stays_neutral_quality_warning():
    signals = [
        _raw_signal("Broad Same-Theme Thrust", "Space - Launch", ["RKLB", "AAOI"], priority=122.0),
        _raw_signal("Broad Same-Theme Thrust", "AI - Agentic", ["RKLB", "NOW"], priority=120.0),
        _raw_signal("Emerging Cluster", "AI - Enterprise Apps", ["NOW", "HUBS"], priority=118.0),
        _raw_signal("Extreme Ticker Standout", "AI - Infrastructure", ["RKLB", "AAOI"], priority=116.0),
        _raw_signal("Extreme Ticker Standout", "Cloud - DevOps", ["HUBS", "WDAY"], priority=114.0),
    ]

    items = build_market_scout_items(signals, limit=5)
    warning = next(item for item in items if item["pattern"] == "Overlap Warning")

    assert warning["metadata"]["quality_note"] is True
    assert warning["metadata"]["coherence_confidence"] == "weak"
    assert warning["metadata"]["overlap_family"] == "Mixed"
    assert "Cloud cluster" not in warning["headline"]
    assert "signal" not in warning["headline"].lower()


def test_market_scout_mixed_space_and_ai_repeated_tickers_do_not_form_one_ai_cluster():
    signals = [
        _raw_signal("Broad Same-Theme Thrust", "Space - Launch", ["RKLB", "AAOI"], priority=125.0),
        _raw_signal("Emerging Cluster", "AI - Infrastructure", ["RKLB", "PANW", "NOW", "HUBS", "WDAY"], priority=123.0),
        _raw_signal("Extreme Ticker Standout", "AI - Software", ["PANW", "NOW", "HUBS", "WDAY"], priority=121.0),
        _raw_signal("Broad Same-Theme Thrust", "AI - Enterprise Apps", ["PANW", "NOW", "HUBS", "WDAY"], priority=119.0),
        _raw_signal("Extreme Ticker Standout", "Data Center Optics", ["AAOI"], priority=118.0),
    ]

    items = build_market_scout_items(signals, limit=5)
    cluster = next(item for item in items if item["pattern"] == "Coherent Overlap Cluster")
    warning = next(item for item in items if item["pattern"] == "Overlap Warning")

    assert cluster["metadata"]["overlap_family"] == "AI / Software"
    assert cluster["tickers_to_inspect"] == ["PANW", "NOW", "HUBS", "WDAY"]
    assert "RKLB" not in cluster["tickers_to_inspect"]
    assert "AAOI" not in cluster["tickers_to_inspect"]
    assert warning["headline"] == "Mixed repeated leaders"
    assert set(warning["tickers_to_inspect"]) == {"RKLB", "AAOI"}


def test_market_scout_ai_software_repeated_tickers_can_form_cluster_without_rklb():
    signals = [
        _raw_signal("Emerging Cluster", "AI - Infrastructure", ["PANW", "NOW", "HUBS", "WDAY"], priority=123.0),
        _raw_signal("Extreme Ticker Standout", "AI - Software", ["PANW", "NOW", "HUBS", "WDAY"], priority=121.0),
        _raw_signal("Broad Same-Theme Thrust", "AI - Enterprise Apps", ["PANW", "NOW", "HUBS", "WDAY"], priority=119.0),
        _raw_signal("Broad Same-Theme Thrust", "Space - Launch", ["RKLB"], priority=117.0),
    ]

    items = build_market_scout_items(signals, limit=5)
    cluster = next(item for item in items if item["pattern"] == "Coherent Overlap Cluster")

    assert cluster["headline"] == "AI / Software overlap"
    assert cluster["tickers_to_inspect"] == ["PANW", "NOW", "HUBS", "WDAY"]
    assert "RKLB" not in cluster["tickers_to_inspect"]
    assert cluster["metadata"]["quality_note"] is False


def test_market_scout_mixed_repeated_leaders_are_lower_priority_quality_note_without_qqq_language():
    signals = [
        _raw_signal("Broad Same-Theme Thrust", "Space - Launch", ["RKLB", "AAA"], priority=125.0),
        _raw_signal("Emerging Cluster", "AI - Infrastructure", ["RKLB", "PANW"], priority=123.0),
        _raw_signal("Extreme Ticker Standout", "Cybersecurity", ["PANW", "BBB"], priority=121.0),
        _raw_signal("Broad Same-Theme Thrust", "Clean Energy", ["AAA", "CCC"], priority=119.0),
    ]

    items = build_market_scout_items(
        signals,
        qqq_market_context={"move_label": "Strong Down Day", "character_tag": "Trend Down", "qqq_pct_change": -1.51},
        limit=5,
    )
    warning = next(item for item in items if item["pattern"] == "Overlap Warning")

    assert warning["metadata"]["quality_note"] is True
    assert warning["priority"] < 160.0
    assert warning["headline"] == "Mixed repeated leaders"
    assert "do not form one clean theme cluster" in warning["why_it_matters"]
    assert "Against a -1.5% QQQ tape" not in warning["why_it_matters"]


def test_market_scout_items_include_required_triage_fields():
    signals = [_raw_signal("Emerging Cluster", "Alpha", ["AAA", "AAB"], priority=110.0)]

    items = build_market_scout_items(signals, limit=5)

    assert items
    for item in items:
        assert item["why_it_matters"]
        assert item["tickers_to_inspect"]
        assert item["next_look"]


def test_market_scout_weak_qqq_upgrades_positive_cluster_wording():
    signals = [_raw_signal("Emerging Cluster", "Alpha", ["AAA", "AAB"], priority=110.0)]

    items = build_market_scout_items(
        signals,
        qqq_market_context={"move_label": "Down Day", "character_tag": "Trend Down", "qqq_pct_change": -1.0},
        limit=5,
    )

    assert "Against a -1.0% QQQ tape, this strength stands out." in items[0]["why_it_matters"]
    assert "QQQ is weak" not in items[0]["why_it_matters"]


def test_market_scout_quiet_qqq_upgrades_group_specific_cluster_wording():
    signals = [_raw_signal("Emerging Cluster", "Alpha", ["AAA", "AAB"], priority=110.0)]

    items = build_market_scout_items(
        signals,
        qqq_market_context={"move_label": "Flat / Mixed", "character_tag": "Quiet", "qqq_pct_change": 0.1},
        limit=5,
    )

    assert "Quiet QQQ tape makes this clustered strength look more group-specific." in items[0]["why_it_matters"]


def test_market_scout_strong_trend_up_tape_downgrades_generic_broad_thrust_wording():
    signals = [_raw_signal("Broad Same-Theme Thrust", "Alpha", ["AAA", "AAB", "AAC"], priority=120.0)]

    items = build_market_scout_items(
        signals,
        qqq_market_context={"move_label": "Strong Up Day", "character_tag": "Trend Up", "qqq_pct_change": 1.8},
        limit=5,
    )

    assert "Strong QQQ tape raises the bar" in items[0]["why_it_matters"]


def test_market_scout_no_signal_fallback_message():
    assert build_market_scout_items([], limit=5) == []
    assert no_market_scout_items_message() == "No high-value scout items detected from the latest stored data."


def test_market_scout_widespread_broad_thrust_does_not_dominate_higher_quality_item():
    signals = [
        _raw_signal("Broad Same-Theme Thrust", f"Theme {idx}", [f"T{idx}A", f"T{idx}B", f"T{idx}C"], priority=130.0)
        for idx in range(10)
    ]
    signals.append(_raw_signal("Outlier-Led Theme (Broadly Confirmed)", "Focused Outlier", ["LEAD"], priority=125.0))

    items = build_market_scout_items(signals, limit=5)

    assert items[0]["pattern"] == "Outlier-Led Theme (Broadly Confirmed)"
    broad_context = next(item for item in items if item["pattern"] == "Broad Same-Theme Thrust")
    assert broad_context["headline"] == "Broad thrust is widespread"
    assert "market-wide breadth context" in broad_context["why_it_matters"]


def test_market_scout_outlier_next_look_includes_supporting_tickers_when_available():
    signal = _raw_signal("Outlier-Led Theme (Narrow/Fragile)", "Outlier Theme", ["LEAD"], priority=125.0)
    signal["metadata"]["supporting_tickers"] = ["SUP1", "SUP2"]

    items = build_market_scout_items([signal], limit=5)

    assert "SUP1, SUP2" in items[0]["next_look"]
    assert "supporting tickers" in items[0]["next_look"]


def test_market_scout_overlap_outranks_narrow_fragile_outlier():
    narrow = _raw_signal("Outlier-Led Theme (Narrow/Fragile)", "Narrow Theme", ["ONE"], priority=220.0)
    narrow["metadata"].update({"leader": "ONE", "excess_vs_avg_pct": 40.0, "strong_ticker_count": 1})
    signals = [
        narrow,
        _raw_signal("Broad Same-Theme Thrust", "LIDAR", ["AEVA", "OUST", "MBLY"], priority=90.0),
        _raw_signal("Emerging Cluster", "Autonomous Vehicles", ["AEVA", "OUST", "MBLY"], priority=88.0),
        _raw_signal("Extreme Ticker Standout", "Computer Vision", ["AEVA", "OUST", "MBLY"], priority=86.0),
    ]

    items = build_market_scout_items(signals, limit=5)

    assert items[0]["pattern"] == "Coherent Overlap Cluster"
    narrow_item = next(item for item in items if item["pattern"] == "Outlier-Led Theme (Narrow/Fragile)")
    assert narrow_item["priority"] < items[0]["priority"]
    assert "may be distorting the theme read" in narrow_item["why_it_matters"]


def test_market_scout_broadly_confirmed_outlier_outranks_narrow_outlier():
    narrow = _raw_signal("Outlier-Led Theme (Narrow/Fragile)", "Narrow Theme", ["ONE"], priority=220.0)
    narrow["metadata"].update({"leader": "ONE", "excess_vs_avg_pct": 40.0, "strong_ticker_count": 1})
    broad = _raw_signal("Outlier-Led Theme (Broadly Confirmed)", "Confirmed Theme", ["LEAD", "SUP"], priority=120.0)
    broad["metadata"].update({"leader": "LEAD", "strong_ticker_count": 4, "supporting_tickers": ["SUP1", "SUP2", "SUP3"]})

    items = build_market_scout_items([narrow, broad], limit=5)

    assert items[0]["pattern"] == "Outlier-Led Theme (Broadly Confirmed)"
    assert "LEAD is the extreme leader" in items[0]["why_it_matters"]
    assert "SUP1, SUP2, and SUP3 are also participating" in items[0]["why_it_matters"]


def _raw_signal(signal_type, theme, tickers, *, priority):
    return {
        "signal_type": signal_type,
        "theme": theme,
        "tickers": tickers,
        "why_notable": f"{theme} notable setup.",
        "read": f"{theme} read.",
        "priority": priority,
        "metadata": {"rank": 10, "strong_ticker_count": len(tickers), "theme_avg_1w": 12.0},
    }
