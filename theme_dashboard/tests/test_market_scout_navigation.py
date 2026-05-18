import pandas as pd

from src.market_scout_navigation import build_market_scout_theme_lookup, market_scout_theme_candidates
from src.theme_selection import describe_selection_source


def test_market_scout_theme_candidates_resolve_outlier_theme():
    themes = pd.DataFrame(
        [
            {"id": 1, "name": "Clean Energy", "category": "Energy"},
            {"id": 2, "name": "AI - Software", "category": "AI"},
        ]
    )
    lookup = build_market_scout_theme_lookup(themes)
    item = {"pattern": "Outlier-Led Theme (Broadly Confirmed)", "metadata": {"theme": "Clean Energy"}}

    assert market_scout_theme_candidates(item, lookup) == [
        {"theme_id": 1, "label": "Clean Energy (Energy)", "name": "Clean Energy"}
    ]


def test_market_scout_theme_candidates_resolve_related_cluster_themes_in_order():
    themes = pd.DataFrame(
        [
            {"id": 1, "name": "LIDAR", "category": "Mobility"},
            {"id": 2, "name": "Autonomous Vehicles", "category": "Mobility"},
            {"id": 3, "name": "Computer Vision", "category": "AI"},
        ]
    )
    lookup = build_market_scout_theme_lookup(themes)
    item = {
        "pattern": "Coherent Overlap Cluster",
        "metadata": {"themes": ["Autonomous Vehicles", "LIDAR", "Missing Theme", "Computer Vision"]},
    }

    assert [candidate["name"] for candidate in market_scout_theme_candidates(item, lookup)] == [
        "Autonomous Vehicles",
        "LIDAR",
        "Computer Vision",
    ]


def test_market_scout_selection_source_label_is_registered():
    assert describe_selection_source("market_scout") == "Market Scout"
