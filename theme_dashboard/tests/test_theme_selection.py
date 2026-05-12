import unittest

from src.theme_selection import (
    SELECTED_THEME_ID_KEY,
    SELECTED_THEME_LABEL_KEY,
    SELECTED_THEME_SOURCE_KEY,
    build_theme_option_maps,
    build_theme_picker_options,
    describe_selection_source,
    prepare_replaceable_selectbox_widget_key,
    ranked_theme_picker_options,
    ranked_theme_labels_for_search,
    resolve_theme_selection,
    rotate_replaceable_selectbox_widget,
    set_theme_selection_state,
    theme_label_search_rank,
    should_apply_selection_token,
)


class TestThemeSelection(unittest.TestCase):
    def test_resolve_theme_selection_prefers_valid_selected_id(self):
        label_by_id = {1: "AI (Tech)", 2: "Energy (Macro)"}
        id_by_label = {v: k for k, v in label_by_id.items()}

        theme_id, label = resolve_theme_selection(2, "AI (Tech)", label_by_id, id_by_label, fallback_theme_id=1)

        self.assertEqual(theme_id, 2)
        self.assertEqual(label, "Energy (Macro)")

    def test_resolve_theme_selection_falls_back_to_dropdown_label(self):
        label_by_id = {1: "AI (Tech)", 2: "Energy (Macro)"}
        id_by_label = {v: k for k, v in label_by_id.items()}

        theme_id, label = resolve_theme_selection(None, "Energy (Macro)", label_by_id, id_by_label, fallback_theme_id=1)

        self.assertEqual(theme_id, 2)
        self.assertEqual(label, "Energy (Macro)")

    def test_describe_selection_source(self):
        self.assertEqual(describe_selection_source("top_1w"), "Top 10 1W")
        self.assertEqual(describe_selection_source("manual_dropdown"), "Manual dropdown")

    def test_should_apply_selection_token_only_for_new_token(self):
        self.assertTrue(should_apply_selection_token("top_1w:12", None))
        self.assertFalse(should_apply_selection_token("top_1w:12", "top_1w:12"))
        self.assertFalse(should_apply_selection_token(None, "top_1w:12"))

    def test_set_theme_selection_state_updates_shared_keys(self):
        session_state = {}

        set_theme_selection_state(session_state, 7, "AI (Tech)", "historical_table")

        self.assertEqual(session_state[SELECTED_THEME_ID_KEY], 7)
        self.assertEqual(session_state[SELECTED_THEME_LABEL_KEY], "AI (Tech)")
        self.assertEqual(session_state[SELECTED_THEME_SOURCE_KEY], "historical_table")

    def test_theme_label_search_rank_orders_expected_match_types(self):
        labels = [
            "Battery Materials (Lithium)",
            "Lithium (Materials)",
            "Alternative Fuels (Energy)",
            "Lit (Exact)",
        ]

        ranked = sorted(labels, key=lambda label: theme_label_search_rank(label, "lit"))

        self.assertEqual(ranked[0], "Lit (Exact)")
        self.assertEqual(ranked[1], "Lithium (Materials)")
        self.assertEqual(ranked[2], "Battery Materials (Lithium)")
        self.assertLess(theme_label_search_rank("Battery Materials (Lithium)", "lit"), theme_label_search_rank("Alternative Fuels (Energy)", "lit"))

    def test_ranked_theme_labels_for_search_filters_and_uses_alphabetical_tiebreaks(self):
        labels = [
            "Battery Materials (Lithium)",
            "Lithium (Materials)",
            "Alternative Fuels (Energy)",
            "Lithium Miners (Materials)",
        ]

        ranked = ranked_theme_labels_for_search(labels, "lit")

        self.assertEqual(ranked, ["Lithium (Materials)", "Lithium Miners (Materials)", "Battery Materials (Lithium)"])
        self.assertNotIn("Alternative Fuels (Energy)", ranked)

    def test_build_theme_picker_options_sorts_by_name_category_id_and_disambiguates(self):
        themes = [
            {"id": 7, "name": "AI", "category": "Software"},
            {"id": 3, "name": "AI", "category": "Hardware"},
            {"id": 2, "name": "Energy", "category": "Macro"},
            {"id": 9, "name": "AI", "category": "Hardware"},
        ]

        options = build_theme_picker_options(themes)

        self.assertEqual(
            [(option.theme_id, option.label) for option in options],
            [
                (3, "AI (Hardware) [#3]"),
                (9, "AI (Hardware) [#9]"),
                (7, "AI (Software)"),
                (2, "Energy (Macro)"),
            ],
        )

    def test_build_theme_option_maps_preserves_label_identity(self):
        themes = [
            {"id": 2, "name": "Energy", "category": "Macro"},
            {"id": 1, "name": "AI", "category": "Software"},
        ]

        options, label_by_id, id_by_label = build_theme_option_maps(themes)

        self.assertEqual(list(options.keys()), ["AI (Software)", "Energy (Macro)"])
        self.assertEqual(label_by_id[1], "AI (Software)")
        self.assertEqual(id_by_label["Energy (Macro)"], 2)

    def test_ranked_theme_picker_options_orders_exact_starts_contains_then_alpha(self):
        options = build_theme_picker_options(
            [
                {"id": 4, "name": "Battery Materials", "category": "Lithium"},
                {"id": 3, "name": "Lithium", "category": "Materials"},
                {"id": 2, "name": "Lithium Miners", "category": "Materials"},
                {"id": 1, "name": "Alternative Fuels", "category": "Energy"},
            ]
        )

        ranked = ranked_theme_picker_options(options, "lithium")

        self.assertEqual(
            [option.label for option in ranked],
            ["Lithium (Materials)", "Lithium Miners (Materials)", "Battery Materials (Lithium)"],
        )
        self.assertNotIn("Alternative Fuels (Energy)", [option.label for option in ranked])

    def test_prepare_replaceable_selectbox_widget_key_seeds_current_selection(self):
        session_state = {}

        widget_key = prepare_replaceable_selectbox_widget_key(
            session_state,
            "historical_selected_theme",
            ["AI (Tech)", "Energy (Macro)"],
            "Energy (Macro)",
        )

        self.assertEqual(widget_key, "historical_selected_theme__widget__0")
        self.assertEqual(session_state[widget_key], "Energy (Macro)")

    def test_prepare_replaceable_selectbox_widget_key_resyncs_stale_widget_value(self):
        session_state = {"historical_selected_theme__widget__0": "AI (Tech)"}

        widget_key = prepare_replaceable_selectbox_widget_key(
            session_state,
            "historical_selected_theme",
            ["AI (Tech)", "Energy (Macro)"],
            "Energy (Macro)",
        )

        self.assertEqual(widget_key, "historical_selected_theme__widget__0")
        self.assertEqual(session_state[widget_key], "Energy (Macro)")

    def test_prepare_replaceable_selectbox_widget_key_can_preserve_valid_user_widget_value(self):
        session_state = {"historical_selected_theme__widget__0": "AI (Tech)"}

        widget_key = prepare_replaceable_selectbox_widget_key(
            session_state,
            "historical_selected_theme",
            ["AI (Tech)", "Energy (Macro)"],
            "Energy (Macro)",
            preserve_valid_widget_value=True,
        )

        self.assertEqual(widget_key, "historical_selected_theme__widget__0")
        self.assertEqual(session_state[widget_key], "AI (Tech)")

    def test_prepare_replaceable_selectbox_widget_key_clears_stale_filtered_value(self):
        session_state = {"historical_selected_theme__widget__0": "AI (Tech)"}

        widget_key = prepare_replaceable_selectbox_widget_key(
            session_state,
            "historical_selected_theme",
            ["Energy (Macro)"],
            None,
        )

        self.assertEqual(widget_key, "historical_selected_theme__widget__0")
        self.assertNotIn(widget_key, session_state)

    def test_rotate_replaceable_selectbox_widget_advances_widget_version(self):
        session_state = {}

        rotate_replaceable_selectbox_widget(session_state, "historical_selected_theme")
        rotate_replaceable_selectbox_widget(session_state, "historical_selected_theme")

        self.assertEqual(session_state["historical_selected_theme__widget_version"], 2)


if __name__ == "__main__":
    unittest.main()
