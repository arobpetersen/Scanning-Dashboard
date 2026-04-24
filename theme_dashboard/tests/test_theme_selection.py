import unittest

from src.theme_selection import (
    SELECTED_THEME_ID_KEY,
    SELECTED_THEME_LABEL_KEY,
    SELECTED_THEME_SOURCE_KEY,
    describe_selection_source,
    prepare_replaceable_selectbox_widget_key,
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
