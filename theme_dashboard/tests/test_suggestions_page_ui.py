from pathlib import Path
import unittest


class SuggestionsPageUiTests(unittest.TestCase):
    def test_suggestions_page_is_simplified_to_scanner_audit_ui(self):
        content = (Path(__file__).resolve().parents[1] / "pages" / "3_Suggestions.py").read_text(encoding="utf-8")
        self.assertIn('st.set_page_config(page_title="Scanner Audit", layout="wide")', content)
        self.assertIn('st.title("Scanner Audit")', content)
        self.assertIn('active_suggestions_tab = "Scanner Audit"', content)
        self.assertIn("Legacy suggestion queue, manual, rules, and AI proposal tools are hidden from the normal UI.", content)
        self.assertNotIn('st.radio(\n    "Suggestions section"', content)
