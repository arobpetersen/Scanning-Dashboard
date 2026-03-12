import unittest

from src.theme_selection import (
    SELECTED_THEME_ID_KEY,
    SELECTED_THEME_LABEL_KEY,
    SELECTED_THEME_SOURCE_KEY,
    describe_selection_source,
    resolve_theme_selection,
    set_theme_selection_state,
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


if __name__ == "__main__":
    unittest.main()
