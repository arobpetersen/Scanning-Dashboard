import unittest

from src.theme_selection import describe_selection_source, resolve_theme_selection


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


if __name__ == "__main__":
    unittest.main()
