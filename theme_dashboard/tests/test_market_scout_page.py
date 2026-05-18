from pathlib import Path


def test_market_scout_page_uses_report_style_snapshot_layout():
    content = (Path(__file__).resolve().parents[1] / "pages" / "6_Market_Scout.py").read_text(encoding="utf-8")

    assert "DEFAULT_VISIBLE_SCOUT_ITEMS = 3" in content
    assert "SCOUT_ITEM_LIMIT = 8" in content
    assert "market-scout-backdrop" in content
    assert "Market backdrop details" in content
    assert "market_backdrop_read_line" in content
    assert "_market_backdrop_line" in content
    assert "Gap {_format_pct_metric(context.get('gap_pct'))}" in content
    assert "market-scout-card" in content
    assert "market-scout-read" in content
    assert "market-scout-bullet" in content
    assert "market-scout-chip" in content
    assert "market-scout-ticker-chip" in content
    assert "Show more scout items" in content
    assert 'st.subheader("Scout Read")' in content
    assert 'st.subheader("Opening Brief")' in content
    assert "Ticker Standouts" in content
    assert "Theme Leadership" in content
    assert "5-Minute Snapshot" not in content
    assert "Top Database Tickers" not in content
    assert "Top Theme Snapshot" not in content
    assert "_render_five_minute_snapshot(top_database_tickers, top_theme_snapshot, theme_lookup)" in content
    assert content.index("_render_qqq_market_tape_strip(") < content.index("_render_five_minute_snapshot(top_database_tickers, top_theme_snapshot, theme_lookup)")
    assert content.index("_render_five_minute_snapshot(top_database_tickers, top_theme_snapshot, theme_lookup)") < content.index('st.subheader("Scout Read")')
    assert content.index('st.subheader("Scout Read")') < content.index('with st.expander("Advanced filters", expanded=False)')
    assert content.index('with st.expander("Advanced filters", expanded=False)') < content.index('with st.expander("Underlying Evidence", expanded=False)')
    assert 'key="market_scout_signal_types"' in content
    assert "build_market_scout_theme_lookup(theme_catalog)" in content
    assert "set_theme_selection_state(st.session_state, int(theme_id), str(label), \"market_scout\")" in content
    assert "st.switch_page(\"pages/1_Themes.py\")" in content
    assert "market_scout_theme_candidates(item, theme_lookup)" in content
    assert "Related themes" in content
    assert "Open theme" in content
    assert 'CLUSTER_SECTION_LABEL = "Theme / Cluster Reads"' in content
    assert "Theme Clusters Worth Opening" not in content
    assert "Outlier-Led Themes To Verify" in content
    assert "Scout Quality Notes" in content
    assert "review_items = [item for item in scout_items if not _is_quality_note(item)]" in content
    assert "<strong>Review action:</strong>" in content
    assert "&bull;" in content
    assert "Close position {_format_pct_brief(context.get('close_position_pct'), decimals=0)}" in content
    assert "{character_tag} character" in content
    assert "with st.expander(\"Trigger evidence\", expanded=False)" in content
    assert "with st.expander(\"Underlying Evidence\", expanded=False)" in content
    assert "not as a replacement for manual review" in content
    assert "Max signals shown" not in content
    assert "Scout Summary" not in content
    assert "What To Review First" not in content
    assert "Next look:" not in content
    assert "Why it matters:" not in content
    assert "Tickers to inspect:" not in content


def test_market_scout_page_has_briefing_interpretation_helpers():
    content = (Path(__file__).resolve().parents[1] / "pages" / "6_Market_Scout.py").read_text(encoding="utf-8")

    assert "Strength is concentrated in standout leaders, so breadth confirmation matters." in content
    assert "Weak QQQ tape makes positive clusters more notable." in content
    assert "str(bullet).strip().lstrip(\". \").strip()" in content
    assert "overlapping AI/software/infrastructure cluster" in content
    assert "then verify " in content
    assert "Shared high-momentum names are driving multiple theme alerts" in content
    assert "Several alerts overlap through the same standout tickers" in content
    assert "Overlapping AI/software/infrastructure strength needs breadth confirmation" in content
    assert "def _section_for_item(" in content
    assert "def _overlap_review_label(" in content
    assert "def _review_action(" in content
    assert "def _ticker_chips(" in content
