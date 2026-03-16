import os
import shutil
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import requests

from src.config import AI_MODEL
from src.database import SCHEMA_SQL
from src.scanner_research import generate_scanner_research_draft, get_or_create_scanner_research_draft, theme_catalog_context
from src.scanner_audit import (
    import_tc2000_exports,
    parse_tc2000_export_file,
    promote_scanner_candidate_to_theme_review,
    recent_scanner_import_runs,
    reset_scanner_audit_data,
    scanner_candidate_summary,
    scanner_import_overview,
    set_scanner_candidate_review_state,
)


class TestScannerAudit(unittest.TestCase):
    class _FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object] | None = None, text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text

        def json(self):
            if self._payload is None:
                raise ValueError("No JSON payload")
            return self._payload

    def _tmpdir(self) -> Path:
        root = Path(__file__).resolve().parent / "_tmp_scanner_audit"
        root.mkdir(exist_ok=True)
        path = root / f"case_{uuid.uuid4().hex}"
        path.mkdir()
        self.addCleanup(lambda: shutil.rmtree(path, ignore_errors=True))
        return path

    def _conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(SCHEMA_SQL)
        return conn

    def test_parse_tc2000_export_file_handles_basic_csv(self):
        tmp = self._tmpdir()
        path = tmp / "Momentum Leaders.csv"
        path.write_text("Ticker,Company,Price\nAAPL,Apple,210\nmsft,Microsoft,420\n,\n", encoding="utf-8")

        parsed, meta = parse_tc2000_export_file(path)

        self.assertEqual(int(meta["rows_read"]), 3)
        self.assertEqual(int(meta["rows_parsed"]), 2)
        self.assertEqual(parsed["normalized_ticker"].tolist(), ["AAPL", "MSFT"])
        self.assertTrue((parsed["scanner_name"] == "Momentum Leaders").all())
        self.assertEqual(str(parsed.iloc[0]["scanner_name_basis"]), "filename_parse")
        self.assertEqual(str(parsed.iloc[0]["observed_date_basis"]), "modified_timestamp_fallback")

    def test_parse_tc2000_export_file_uses_filename_date_when_no_date_column(self):
        tmp = self._tmpdir()
        path = tmp / "Momentum Leaders 2026-03-02.csv"
        path.write_text("Ticker,Note\nAAPL,one\nMSFT,two\n", encoding="utf-8")

        parsed, meta = parse_tc2000_export_file(path, default_source_label="tc2000_export")

        self.assertTrue((parsed["observed_date"].astype(str) == "2026-03-02").all())
        self.assertTrue((parsed["scanner_name"] == "Momentum Leaders").all())
        self.assertTrue((parsed["observed_date_basis"] == "filename_parse").all())
        self.assertTrue((parsed["scanner_name_basis"] == "filename_parse").all())
        self.assertEqual(str(meta["observed_date_basis"]), "filename_parse")
        self.assertEqual(str(meta["scanner_name_basis"]), "filename_parse")

    def test_parse_tc2000_export_file_uses_default_source_for_generic_dated_export(self):
        tmp = self._tmpdir()
        path = tmp / "Export_20260302.csv"
        path.write_text("Ticker,Note\nAAPL,one\n", encoding="utf-8")

        parsed, meta = parse_tc2000_export_file(path, default_source_label="tc2000_export")

        self.assertEqual(str(parsed.iloc[0]["scanner_name"]), "tc2000_export")
        self.assertEqual(str(parsed.iloc[0]["scanner_name_basis"]), "default_source_label_fallback")
        self.assertEqual(str(parsed.iloc[0]["observed_date"]), "2026-03-02")
        self.assertEqual(str(meta["scanner_name_basis"]), "default_source_label_fallback")

    def test_parse_tc2000_export_file_falls_back_to_modified_time_only_when_filename_has_no_date(self):
        tmp = self._tmpdir()
        path = tmp / "Momentum Leaders.csv"
        path.write_text("Ticker,Note\nAAPL,one\n", encoding="utf-8")
        modified_at = datetime(2026, 3, 9, 14, 30, 0).timestamp()
        os.utime(path, (modified_at, modified_at))

        parsed, meta = parse_tc2000_export_file(path, default_source_label="tc2000_export")

        self.assertEqual(str(parsed.iloc[0]["observed_date"]), "2026-03-09")
        self.assertEqual(str(parsed.iloc[0]["observed_date_basis"]), "modified_timestamp_fallback")
        self.assertEqual(str(meta["observed_date_basis"]), "modified_timestamp_fallback")

    def test_import_tc2000_exports_prevents_duplicate_logical_hits(self):
        conn = self._conn()
        tmp = self._tmpdir()
        path = tmp / "Breakouts.csv"
        path.write_text("Symbol,Date\nAAPL,2026-03-10\nMSFT,2026-03-10\n", encoding="utf-8")

        first = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000")
        second = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000")

        stored = conn.execute("select normalized_ticker, observed_date, scanner_name from scanner_hit_history order by normalized_ticker").df()
        self.assertEqual(first["rows_imported"], 2)
        self.assertEqual(second["rows_imported"], 0)
        self.assertEqual(first["files_processed"], 1)
        self.assertEqual(second["files_processed"], 0)
        self.assertEqual(second["files_skipped"], 1)
        self.assertEqual(second["files_failed"], 0)
        self.assertEqual(second["file_results"][0]["status"], "skipped_already_imported")
        self.assertEqual(len(stored), 2)
        self.assertEqual(stored["normalized_ticker"].tolist(), ["AAPL", "MSFT"])
        ledger = conn.execute(
            """
            select import_status, first_import_run_id, last_seen_run_id
            from scanner_imported_files
            """
        ).fetchone()
        self.assertEqual(ledger[0], "success")
        self.assertEqual(int(ledger[1]), int(first["import_run_id"]))
        self.assertEqual(int(ledger[2]), int(second["import_run_id"]))
        conn.close()

    def test_import_tc2000_exports_reprocesses_changed_file(self):
        conn = self._conn()
        tmp = self._tmpdir()
        path = tmp / "Breakouts.csv"
        path.write_text("Symbol,Date\nAAPL,2026-03-10\n", encoding="utf-8")

        first = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000")
        path.write_text("Symbol,Date\nAAPL,2026-03-10\nNVDA,2026-03-11\n", encoding="utf-8")
        second = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000")

        stored = conn.execute("select normalized_ticker from scanner_hit_history order by normalized_ticker, observed_date").df()
        self.assertEqual(first["files_processed"], 1)
        self.assertEqual(first["files_skipped"], 0)
        self.assertEqual(second["files_processed"], 1)
        self.assertEqual(second["files_skipped"], 0)
        self.assertEqual(second["rows_imported"], 1)
        self.assertEqual(stored["normalized_ticker"].tolist(), ["AAPL", "NVDA"])
        self.assertEqual(
            conn.execute("select count(*) from scanner_imported_files where import_status = 'success'").fetchone()[0],
            2,
        )
        conn.close()

    def test_scanner_candidate_summary_computes_recurrence_and_governed_coverage(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAPL')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AAPL', 'AAPL', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000', 'Scanner A', 'h1'),
            (1, 'tc2000', 'AAPL', 'AAPL', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000', 'Scanner A', 'h2'),
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000', 'Scanner A', 'h3'),
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000', 'Scanner B', 'h4'),
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-12', '2026-03-12 08:00:00', 'f3.csv', 'tc2000', 'Scanner B', 'h5')
            """
        )

        out = scanner_candidate_summary(conn)
        aapl = out[out["ticker"] == "AAPL"].iloc[0]
        pltr = out[out["ticker"] == "PLTR"].iloc[0]

        self.assertTrue(bool(aapl["is_governed"]))
        self.assertEqual(int(aapl["active_theme_count"]), 1)
        self.assertEqual(str(aapl["recommendation"]), "already covered")
        self.assertFalse(bool(pltr["is_governed"]))
        self.assertEqual(int(pltr["observations_last_10d"]), 3)
        self.assertEqual(int(pltr["current_streak"]), 3)
        self.assertIn(str(pltr["recommendation"]), {"review for addition", "high-persistence uncovered"})
        self.assertEqual(str(pltr["review_state"]), "active")
        conn.close()

    def test_scanner_candidate_review_state_persists_and_surfaces(self):
        conn = self._conn()
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, scanner_name_inferred, scanner_name_basis,
                observed_date_inferred, observed_date_basis, row_hash
            ) values
            (1, 'tc2000', 'IONQ', 'IONQ', '2026-03-12', '2026-03-12 08:00:00', 'symbols.xlsx', 'tc2000', 'symbols', true, 'filename_parse', true, 'modified_timestamp_fallback', 'ionq-1')
            """
        )

        result = set_scanner_candidate_review_state(conn, "IONQ", "ignored", "known noise")
        out = scanner_candidate_summary(conn)
        row = out[out["ticker"] == "IONQ"].iloc[0]

        self.assertEqual(result["review_state"], "ignored")
        self.assertEqual(str(row["review_state"]), "ignored")
        self.assertEqual(str(row["review_note"]), "known noise")
        self.assertIn("scanner from filename parse", str(row["metadata_basis"]))
        self.assertIn("date from modified timestamp fallback", str(row["metadata_basis"]))
        conn.close()

    def test_import_historical_dated_exports_do_not_inflate_distinct_scanner_count(self):
        conn = self._conn()
        tmp = self._tmpdir()
        for name in ["Export_2026-03-02.csv", "Export_2026-03-03.csv", "Export_2026-03-04.csv"]:
            (tmp / name).write_text("Ticker,Note\nAAPL,one\n", encoding="utf-8")

        result = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000_export")
        summary = scanner_candidate_summary(conn)
        row = summary[summary["ticker"] == "AAPL"].iloc[0]

        self.assertEqual(result["files_processed"], 3)
        self.assertEqual(int(row["distinct_scanner_count"]), 1)
        self.assertEqual(int(row["observed_days"]), 3)
        self.assertEqual(int(row["current_streak"]), 3)
        self.assertEqual(str(row["first_seen"]).split(" ")[0], "2026-03-02")
        self.assertEqual(str(row["last_seen"]).split(" ")[0], "2026-03-04")
        self.assertIn("scanner from default source label fallback", str(row["metadata_basis"]))
        self.assertIn("date from filename parse", str(row["metadata_basis"]))
        conn.close()

    def test_import_tc2000_exports_skips_dirty_or_missing_ticker_files_safely(self):
        conn = self._conn()
        tmp = self._tmpdir()
        valid = tmp / "Valid.csv"
        invalid = tmp / "Invalid.csv"
        valid.write_text("Ticker,Date\nNVDA,2026-03-12\n,\n", encoding="utf-8")
        invalid.write_text("Name,Date\nNVIDIA,2026-03-12\n", encoding="utf-8")

        result = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000")

        stored = conn.execute("select normalized_ticker from scanner_hit_history").df()
        self.assertEqual(result["files_seen"], 2)
        self.assertEqual(result["files_processed"], 1)
        self.assertEqual(result["files_skipped"], 0)
        self.assertEqual(result["files_failed"], 1)
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored.iloc[0]["normalized_ticker"], "NVDA")
        self.assertTrue(any(row["status"] == "failed" for row in result["file_results"]))
        conn.close()

    def test_import_tc2000_exports_reports_no_files_cleanly(self):
        conn = self._conn()
        tmp = self._tmpdir()
        result = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000")

        runs = recent_scanner_import_runs(conn, limit=1)
        overview = scanner_import_overview(conn)
        self.assertEqual(result["status"], "no_files")
        self.assertEqual(str(runs.iloc[0]["status"]), "no_files")
        self.assertEqual(int(overview["files_seen"]), 0)
        self.assertEqual(int(overview["files_processed"]), 0)
        self.assertEqual(int(overview["files_skipped"]), 0)
        self.assertEqual(int(overview["files_failed"]), 0)
        conn.close()

    def test_reset_scanner_audit_data_clears_only_scanner_audit_tables(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAPL')")
        conn.execute(
            """
            insert into scanner_import_runs(import_run_id, import_source, folder_path, file_pattern, started_at, status)
            values (1, 'tc2000', 'c:/imports', '*.csv', '2026-03-12 08:00:00', 'success')
            """
        )
        conn.execute(
            """
            insert into scanner_hit_history(
                hit_id, import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, scanner_name_inferred, scanner_name_basis,
                observed_date_inferred, observed_date_basis, row_hash
            ) values
            (1, 1, 'tc2000', 'AAPL', 'AAPL', '2026-03-12', '2026-03-12 08:00:00', 'f.csv', 'tc2000', 'Scanner A', false, 'file_column', false, 'file_column', 'h1')
            """
        )
        conn.execute(
            """
            insert into scanner_candidate_review_state(normalized_ticker, review_state, review_note, updated_at)
            values ('AAPL', 'ignored', 'noise', '2026-03-12 09:00:00')
            """
        )
        conn.execute(
            """
            insert into scanner_imported_files(
                file_fingerprint, source_file, file_name, file_size, modified_at,
                first_import_run_id, last_seen_run_id, import_status, processed_at
            ) values
            ('fp1', 'c:/imports/f.csv', 'f.csv', 10, '2026-03-12 08:00:00', 1, 1, 'success', '2026-03-12 08:05:00')
            """
        )

        result = reset_scanner_audit_data(conn)

        self.assertEqual(int(result["tables_cleared"]["scanner_hit_history"]), 1)
        self.assertEqual(int(result["tables_cleared"]["scanner_import_runs"]), 1)
        self.assertEqual(int(result["tables_cleared"]["scanner_candidate_review_state"]), 1)
        self.assertEqual(int(result["tables_cleared"]["scanner_imported_files"]), 1)
        self.assertEqual(conn.execute("select count(*) from scanner_hit_history").fetchone()[0], 0)
        self.assertEqual(conn.execute("select count(*) from scanner_import_runs").fetchone()[0], 0)
        self.assertEqual(conn.execute("select count(*) from scanner_candidate_review_state").fetchone()[0], 0)
        self.assertEqual(conn.execute("select count(*) from scanner_imported_files").fetchone()[0], 0)
        self.assertEqual(conn.execute("select count(*) from themes").fetchone()[0], 1)
        self.assertEqual(conn.execute("select count(*) from theme_membership").fetchone()[0], 1)
        conn.close()

    def test_reset_scanner_audit_data_handles_missing_optional_table(self):
        conn = self._conn()
        conn.execute("drop table scanner_imported_files")
        conn.execute(
            """
            insert into scanner_import_runs(import_run_id, import_source, folder_path, file_pattern, started_at, status)
            values (1, 'tc2000', 'c:/imports', '*.csv', '2026-03-12 08:00:00', 'success')
            """
        )

        result = reset_scanner_audit_data(conn)

        self.assertEqual(int(result["tables_cleared"]["scanner_imported_files"]), 0)
        self.assertEqual(conn.execute("select count(*) from scanner_import_runs").fetchone()[0], 0)
        conn.close()

    def test_reset_scanner_audit_data_allows_fresh_import_after_clear(self):
        conn = self._conn()
        tmp = self._tmpdir()
        path = tmp / "Export_2026-03-02.csv"
        path.write_text("Ticker,Note\nAAPL,one\n", encoding="utf-8")

        first = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000_export")
        reset_scanner_audit_data(conn)
        second = import_tc2000_exports(conn, folder=tmp, pattern="*.csv", default_source_label="tc2000_export")

        self.assertEqual(first["rows_imported"], 1)
        self.assertEqual(second["rows_imported"], 1)
        self.assertEqual(second["files_processed"], 1)
        self.assertEqual(second["files_skipped"], 0)
        self.assertEqual(conn.execute("select count(*) from scanner_hit_history").fetchone()[0], 1)
        conn.close()

    def test_promote_scanner_candidate_creates_new_review_candidate_without_touching_membership(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AAPL')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'pltr-1'),
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000', 'Momentum', 'pltr-2'),
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-12', '2026-03-12 08:00:00', 'f3.csv', 'tc2000', 'Breakout', 'pltr-3')
            """
        )

        result = promote_scanner_candidate_to_theme_review(conn, "PLTR", "Possible AI infrastructure fit")

        stored = conn.execute(
            """
            select suggestion_type, status, source, proposed_ticker, reviewer_notes, source_context_json
            from theme_suggestions
            where suggestion_id = ?
            """,
            [result["suggestion_id"]],
        ).fetchone()
        self.assertEqual(result["action"], "created")
        self.assertEqual(stored[0], "review_theme")
        self.assertEqual(stored[1], "pending")
        self.assertEqual(stored[2], "scanner_audit")
        self.assertEqual(stored[3], "PLTR")
        self.assertIsNone(stored[4])
        self.assertEqual(conn.execute("select count(*) from theme_membership").fetchone()[0], 1)
        context = stored[5]
        self.assertIn('"candidate_source": "scanner_audit"', context)
        self.assertIn('"promotion_note": "Possible AI infrastructure fit"', context)
        conn.close()

    def test_promote_scanner_candidate_updates_existing_pending_review_instead_of_duplicating(self):
        conn = self._conn()
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'IONQ', 'IONQ', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'ionq-1'),
            (1, 'tc2000', 'IONQ', 'IONQ', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000', 'Breakout', 'ionq-2')
            """
        )
        first = promote_scanner_candidate_to_theme_review(conn, "IONQ", "First note")
        conn.execute(
            "update theme_suggestions set reviewer_notes = 'human note' where suggestion_id = ?",
            [first["suggestion_id"]],
        )
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (2, 'tc2000', 'IONQ', 'IONQ', '2026-03-12', '2026-03-12 08:00:00', 'f3.csv', 'tc2000', 'Trend', 'ionq-3')
            """
        )

        second = promote_scanner_candidate_to_theme_review(conn, "IONQ", "")

        stored = conn.execute(
            """
            select count(*), max(source_context_json), max(reviewer_notes)
            from theme_suggestions
            where upper(proposed_ticker) = 'IONQ' and suggestion_type = 'review_theme'
            """
        ).fetchone()
        self.assertEqual(first["suggestion_id"], second["suggestion_id"])
        self.assertEqual(second["action"], "updated")
        self.assertEqual(int(stored[0]), 1)
        self.assertEqual(stored[2], "human note")
        self.assertIn('"observed_days": 3', stored[1])
        self.assertIn('"promotion_note": "First note"', stored[1])
        conn.close()

    def test_promote_scanner_candidate_carries_forward_scanner_evidence_fields(self):
        conn = self._conn()
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, scanner_name_inferred, scanner_name_basis,
                observed_date_inferred, observed_date_basis, row_hash
            ) values
            (1, 'tc2000', 'RKLB', 'RKLB', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000_export', 'tc2000_export', true, 'default_source_label_fallback', true, 'filename_parse', 'rklb-1'),
            (1, 'tc2000', 'RKLB', 'RKLB', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000_export', 'tc2000_export', true, 'default_source_label_fallback', true, 'filename_parse', 'rklb-2'),
            (1, 'tc2000', 'RKLB', 'RKLB', '2026-03-12', '2026-03-12 08:00:00', 'f3.csv', 'tc2000_export', 'tc2000_export', true, 'default_source_label_fallback', true, 'filename_parse', 'rklb-3')
            """
        )

        result = promote_scanner_candidate_to_theme_review(conn, "RKLB", "Space infra watch")
        stored = conn.execute(
            "select rationale, source_context_json, source_updated_at from theme_suggestions where suggestion_id = ?",
            [result["suggestion_id"]],
        ).fetchone()

        self.assertIn("Scanner Audit evidence", stored[0])
        self.assertIn('"ticker": "RKLB"', stored[1])
        self.assertIn('"recommendation_reason"', stored[1])
        self.assertIn('"persistence_score"', stored[1])
        self.assertIn('"observations_last_5d"', stored[1])
        self.assertIn('"observations_last_10d"', stored[1])
        self.assertIn('"current_streak"', stored[1])
        self.assertIn('"distinct_scanner_count"', stored[1])
        self.assertIn('"first_seen": "2026-03-10"', stored[1])
        self.assertIn('"last_seen": "2026-03-12"', stored[1])
        self.assertIn('"metadata_basis"', stored[1])
        self.assertIn('"promotion_note": "Space infra watch"', stored[1])
        self.assertIsNotNone(stored[2])
        conn.close()

    def test_promote_scanner_candidate_creates_new_pending_row_after_rejected_history(self):
        conn = self._conn()
        conn.execute(
            """
            insert into theme_suggestions(
                suggestion_type, status, source, rationale, priority, proposed_ticker
            ) values
            ('review_theme', 'rejected', 'manual', 'old review', 'medium', 'ASTS')
            """
        )
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'ASTS', 'ASTS', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'asts-1')
            """
        )

        result = promote_scanner_candidate_to_theme_review(conn, "ASTS", "Retry after fresh scanner activity")

        rows = conn.execute(
            """
            select suggestion_id, status, source
            from theme_suggestions
            where upper(proposed_ticker) = 'ASTS'
            order by suggestion_id
            """
        ).fetchall()
        self.assertEqual(result["action"], "created")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1:], ("rejected", "manual"))
        self.assertEqual(rows[1][1], "pending")
        self.assertEqual(rows[1][2], "scanner_audit")
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "NVDA",
            "company_name": "NVIDIA Corporation",
            "description": "Designs graphics processors and AI computing platforms for data center and accelerated computing workloads.",
            "sic_description": "Semiconductor Devices",
        },
    )
    def test_generate_scanner_research_draft_returns_structured_shape(self, _mock_profile, _mock_api_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AMD')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AVGO')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'NVDA', 'NVDA', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'nvda-1'),
            (1, 'tc2000', 'NVDA', 'NVDA', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000', 'Breakout', 'nvda-2'),
            (1, 'tc2000', 'NVDA', 'NVDA', '2026-03-12', '2026-03-12 08:00:00', 'f3.csv', 'tc2000', 'Breakout', 'nvda-3')
            """
        )

        draft = generate_scanner_research_draft(conn, "NVDA")

        expected_keys = {
            "ticker",
            "company_name",
            "short_company_description",
            "possible_similar_tickers",
            "suggested_existing_themes",
            "possible_new_theme",
            "confidence",
            "rationale",
            "caveats",
            "recommended_action",
            "generated_at",
            "source",
        }
        self.assertTrue(expected_keys.issubset(set(draft.keys())))
        self.assertEqual(draft["ticker"], "NVDA")
        self.assertEqual(draft["source"], "scanner_audit")
        self.assertEqual(draft["research_mode"], "heuristic_fallback")
        self.assertIn("fallback_reason", draft)
        self.assertIsInstance(draft["possible_similar_tickers"], list)
        self.assertIsInstance(draft["suggested_existing_themes"], list)
        self.assertIsInstance(draft["caveats"], list)
        conn.close()

    def test_theme_catalog_context_uses_current_governed_catalog(self):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cybersecurity', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AMD')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'CRWD')")

        catalog = theme_catalog_context(conn, representative_limit=2)

        self.assertEqual(len(catalog), 2)
        ai_theme = next(item for item in catalog if item["theme_name"] == "AI Infrastructure")
        self.assertEqual(ai_theme["representative_tickers"], ["AMD", "NVDA"])
        self.assertIn("AI Infrastructure", ai_theme["theme_description"])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "NVDA",
            "company_name": "NVIDIA Corporation",
            "description": "Designs graphics processors and AI computing platforms for data center and accelerated computing workloads.",
            "sic_description": "Semiconductor Devices",
        },
    )
    def test_promote_generated_research_draft_creates_and_updates_review_candidate(self, _mock_profile, _mock_api_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AMD')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'NVDA', 'NVDA', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'nvda-r1'),
            (1, 'tc2000', 'NVDA', 'NVDA', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000', 'Breakout', 'nvda-r2'),
            (1, 'tc2000', 'NVDA', 'NVDA', '2026-03-12', '2026-03-12 08:00:00', 'f3.csv', 'tc2000', 'Breakout', 'nvda-r3')
            """
        )

        draft = generate_scanner_research_draft(conn, "NVDA")
        first = promote_scanner_candidate_to_theme_review(conn, "NVDA", "Looks like AI infra", research_draft=draft)
        second = promote_scanner_candidate_to_theme_review(conn, "NVDA", "", research_draft=draft)

        stored = conn.execute(
            """
            select count(*), max(source_context_json)
            from theme_suggestions
            where proposed_ticker = 'NVDA' and suggestion_type = 'review_theme'
            """
        ).fetchone()
        self.assertEqual(first["action"], "created")
        self.assertEqual(second["action"], "updated")
        self.assertEqual(int(stored[0]), 1)
        self.assertIn('"research_draft"', stored[1])
        self.assertIn('"suggested_existing_themes"', stored[1])
        self.assertEqual(conn.execute("select count(*) from theme_membership").fetchone()[0], 1)
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "CRWD",
            "company_name": "CrowdStrike Holdings",
            "description": "Provides cloud-native endpoint and cybersecurity software.",
            "sic_description": "Computer Software",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "CrowdStrike Holdings",
            "short_company_description": "Cloud-native cybersecurity software company.",
            "possible_similar_tickers": ["PANW", "ZS"],
            "suggested_existing_themes": [
                {"theme_id": 2, "theme_name": "Cybersecurity", "why_it_might_fit": "Security software peer fit."}
            ],
            "possible_new_theme": "",
            "confidence": "high",
            "rationale": "OpenAI matched the ticker to the governed Cybersecurity theme using company and catalog context.",
            "caveats": ["Advisory only."],
            "recommended_action": "add_to_existing_theme_review",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_promote_research_draft_captures_selected_and_custom_theme_ideas(self, _mock_key, _mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cybersecurity', 'Software', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (3, 'Cloud Security', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'PANW')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (3, 'ZS')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'CRWD', 'CRWD', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'crwd-3')
            """
        )

        draft = generate_scanner_research_draft(conn, "CRWD")
        result = promote_scanner_candidate_to_theme_review(
            conn,
            "CRWD",
            "Strong security setup",
            research_draft=draft,
            selected_suggested_theme_ids=[2],
            custom_existing_theme_ids=[3],
            custom_new_themes=["Endpoint Platform"],
        )

        stored = conn.execute(
            "select source_context_json from theme_suggestions where suggestion_id = ?",
            [result["suggestion_id"]],
        ).fetchone()[0]
        self.assertIn('"selected_suggested_themes"', stored)
        self.assertIn('"theme_name": "Cybersecurity"', stored)
        self.assertIn('"custom_existing_themes"', stored)
        self.assertIn('"theme_name": "Cloud Security"', stored)
        self.assertIn('"custom_new_themes"', stored)
        self.assertIn('"Endpoint Platform"', stored)
        self.assertEqual(conn.execute("select count(*) from theme_membership").fetchone()[0], 2)
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "PLTR",
            "company_name": "Palantir Technologies",
            "description": "Provides software platforms for data integration and analytics.",
            "sic_description": "Computer Software",
        },
    )
    def test_promote_research_draft_updates_existing_pending_with_latest_theme_selections(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (4, 'Defense Tech', 'Industrial', true)")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-10', '2026-03-10 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'pltr-s1'),
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-11', '2026-03-11 08:00:00', 'f2.csv', 'tc2000', 'Breakout', 'pltr-s2')
            """
        )

        draft = generate_scanner_research_draft(conn, "PLTR")
        first = promote_scanner_candidate_to_theme_review(
            conn,
            "PLTR",
            "First note",
            research_draft=draft,
            custom_existing_theme_ids=[1],
            custom_new_themes=["Data Fusion"],
        )
        second = promote_scanner_candidate_to_theme_review(
            conn,
            "PLTR",
            "",
            research_draft=draft,
            custom_existing_theme_ids=[4],
            custom_new_themes=["Defense Analytics"],
        )

        stored = conn.execute(
            """
            select count(*), max(source_context_json)
            from theme_suggestions
            where proposed_ticker = 'PLTR' and suggestion_type = 'review_theme'
            """
        ).fetchone()
        self.assertEqual(first["suggestion_id"], second["suggestion_id"])
        self.assertEqual(int(stored[0]), 1)
        self.assertIn('"theme_name": "Defense Tech"', stored[1])
        self.assertIn('"Defense Analytics"', stored[1])
        self.assertNotIn('"theme_name": "AI Infrastructure"', stored[1])
        self.assertEqual(conn.execute("select count(*) from theme_membership").fetchone()[0], 0)
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "CRWD",
            "company_name": "CrowdStrike Holdings",
            "description": "Provides cloud-native endpoint and cybersecurity software.",
            "sic_description": "Computer Software",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "CrowdStrike Holdings",
            "short_company_description": "Cloud-native cybersecurity software company.",
            "possible_similar_tickers": ["PANW", "ZS"],
            "suggested_existing_themes": [
                {"theme_id": 2, "theme_name": "Cybersecurity", "why_it_might_fit": "Security software peer fit."}
            ],
            "possible_new_theme": "",
            "confidence": "high",
            "rationale": "OpenAI matched the ticker to the governed Cybersecurity theme using company and catalog context.",
            "caveats": ["Advisory only."],
            "recommended_action": "add_to_existing_theme_review",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_generate_scanner_research_draft_openai_mode_is_visible(self, _mock_key, _mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cybersecurity', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'PANW')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'CRWD', 'CRWD', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'crwd-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "CRWD")

        self.assertEqual(draft["research_mode"], "openai")
        self.assertNotIn("fallback_reason", draft)
        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Cybersecurity")
        self.assertIn("research_context_meta", draft)
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "CRWD",
            "company_name": "CrowdStrike Holdings",
            "description": "Provides cloud-native endpoint and cybersecurity software.",
            "sic_description": "Computer Software",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_openai_research_context_is_prefiltered_to_relevant_theme_subset(self, _mock_key, _mock_profile):
        conn = self._conn()
        for theme_id, theme_name, category in [
            (1, "Cybersecurity", "Software"),
            (2, "Cloud Software", "Software"),
            (3, "Biotech", "Healthcare"),
            (4, "Defense Tech", "Industrial"),
            (5, "AI Infrastructure", "Tech"),
            (6, "Europe Luxury", "Consumer"),
            (7, "Digital Payments", "Fintech"),
            (8, "Space Infrastructure", "Industrial"),
            (9, "Data Analytics", "Software"),
            (10, "Energy Transition", "Energy"),
            (11, "Industrial Automation", "Industrial"),
            (12, "Healthcare Equipment", "Healthcare"),
            (13, "Semiconductor Materials", "Semiconductors"),
            (14, "Cloud Infrastructure", "Software"),
            (15, "Identity Security", "Software"),
        ]:
            conn.execute(
                "insert into themes(id, name, category, is_active) values (?, ?, ?, true)",
                [theme_id, theme_name, category],
            )
        for theme_id, ticker in [
            (1, "PANW"),
            (1, "ZS"),
            (2, "NOW"),
            (5, "NVDA"),
            (9, "DDOG"),
            (15, "OKTA"),
        ]:
            conn.execute("insert into theme_membership(theme_id, ticker) values (?, ?)", [theme_id, ticker])
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'CRWD', 'CRWD', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'crwd-catalog-1')
            """
        )

        captured: dict[str, object] = {}

        def _fake_call(_api_key, context):
            captured["context"] = context
            return {
                "company_name": "CrowdStrike Holdings",
                "short_company_description": "Cloud-native cybersecurity software company.",
                "possible_similar_tickers": ["PANW", "ZS"],
                "suggested_existing_themes": [
                    {"theme_id": 1, "theme_name": "Cybersecurity", "why_it_might_fit": "Security software peer fit."}
                ],
                "possible_new_theme": "",
                "confidence": "high",
                "rationale": "OpenAI matched the ticker to the governed Cybersecurity theme using company and catalog context.",
                "caveats": ["Advisory only."],
                "recommended_action": "add_to_existing_theme_review",
            }

        with patch("src.scanner_research._call_openai_research", side_effect=_fake_call):
            draft = generate_scanner_research_draft(conn, "CRWD")

        sent_catalog = captured["context"]["governed_theme_catalog"]
        self.assertLess(len(sent_catalog), 15)
        self.assertLessEqual(len(sent_catalog), 12)
        self.assertTrue(all(len(item.get("representative_tickers") or []) <= 3 for item in sent_catalog))
        self.assertEqual(draft["research_context_meta"]["full_catalog_theme_count"], 15)
        self.assertEqual(draft["research_context_meta"]["filtered_theme_count"], len(sent_catalog))
        self.assertTrue(draft["research_context_meta"]["catalog_was_prefiltered"])
        self.assertGreater(draft["research_context_meta"]["estimated_context_chars"], 0)
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "CRWD",
            "company_name": "CrowdStrike Holdings",
            "description": "Provides cloud-native endpoint and cybersecurity software for threat detection and response.",
            "sic_description": "Computer Software",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "CrowdStrike Holdings",
            "short_company_description": "Cybersecurity software company.",
            "possible_similar_tickers": ["PANW"],
            "suggested_existing_themes": [
                {"theme_id": 2, "theme_name": "Cybersecurity", "why_it_might_fit": "Security software peer fit."}
            ],
            "possible_new_theme": "",
            "confidence": "high",
            "rationale": "",
            "caveats": [],
            "recommended_action": "add_to_existing_theme_review",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_openai_path_backfills_non_empty_rationale_when_model_returns_blank(self, _mock_key, _mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cybersecurity', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'PANW')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'CRWD', 'CRWD', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'crwd-openai-blank')
            """
        )

        draft = generate_scanner_research_draft(conn, "CRWD")

        self.assertEqual(draft["research_mode"], "openai")
        self.assertTrue(str(draft["rationale"]).strip())
        self.assertIn("actual role in the stack", draft["rationale"])
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "ROLEX",
            "company_name": "Role Example",
            "description": "Designs optical transceivers and fiber interconnect products for data-center and telecom deployments.",
            "sic_description": "Communications Equipment",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "Role Example",
            "short_company_description": "Optical networking component supplier.",
            "possible_similar_tickers": [],
            "suggested_existing_themes": [],
            "possible_new_theme": "",
            "confidence": "low",
            "rationale": "No strong governed-theme fit exists; the role is more specific than the current taxonomy.",
            "caveats": ["Advisory only."],
            "recommended_action": "consider_new_theme",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_openai_path_can_surface_possible_new_theme_from_heuristic_when_model_omits_it(self, _mock_key, _mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'ROLEX', 'ROLEX', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'rolex-openai-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "ROLEX")

        self.assertEqual(draft["research_mode"], "openai")
        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIsNotNone(draft["possible_new_theme"])
        self.assertTrue(str(draft["rationale"]).strip())
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "ROLEY",
            "company_name": "Role Yield",
            "description": "Produces compound semiconductor substrates and wafer materials for communications and sensing applications.",
            "sic_description": "Semiconductor Materials",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "Role Yield",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_openai_parser_handles_partial_model_output_safely(self, _mock_key, _mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Semiconductor Leaders', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'ROLEY', 'ROLEY', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'roley-openai-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "ROLEY")

        self.assertEqual(draft["research_mode"], "openai")
        self.assertTrue(str(draft["rationale"]).strip())
        self.assertIsInstance(draft["suggested_existing_themes"], list)
        self.assertIn(draft["recommended_action"], {"add_to_existing_theme_review", "consider_new_theme", "watch_only", "reject_for_now"})
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "ROLENET",
            "company_name": "Role Network",
            "description": "Designs optical transceivers and fiber interconnect products for data-center communications infrastructure.",
            "sic_description": "Communications Equipment",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "Role Network",
            "short_company_description": "Optical networking component supplier.",
            "possible_similar_tickers": ["NVDA", "VRT"],
            "suggested_existing_themes": [
                {"theme_id": 1, "theme_name": "AI Infrastructure", "why_it_might_fit": "Data center adjacency."},
                {"theme_id": 2, "theme_name": "Edge Computing", "why_it_might_fit": "Networking adjacency."},
            ],
            "possible_new_theme": "",
            "confidence": "high",
            "rationale": "The company serves AI and data-center infrastructure markets.",
            "caveats": ["Advisory only."],
            "recommended_action": "add_to_existing_theme_review",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_openai_path_prefers_narrow_optics_new_theme_over_adjacent_ai_themes(self, _mock_key, _mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Edge Computing', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'VRT')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'ROLENET', 'ROLENET', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'rolenet-openai-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "ROLENET")

        self.assertEqual(draft["research_mode"], "openai")
        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIn(draft["possible_new_theme"], {"Optical Networking", "Data Center Optics", "Optical Interconnects", "AI Fiber Optics"})
        self.assertTrue(draft["suggested_existing_themes"])
        self.assertIn("more precise", draft["rationale"])
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "ROLESUB",
            "company_name": "Role Substrate",
            "description": "Produces compound semiconductor substrates and wafer materials used in communications and sensing applications.",
            "sic_description": "Semiconductor Materials",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "Role Substrate",
            "short_company_description": "Materials supplier to semiconductor and communications markets.",
            "possible_similar_tickers": ["SHW"],
            "suggested_existing_themes": [
                {"theme_id": 3, "theme_name": "Chemicals - Diversified", "why_it_might_fit": "Materials exposure."}
            ],
            "possible_new_theme": "",
            "confidence": "high",
            "rationale": "The company has materials exposure serving semiconductor markets.",
            "caveats": ["Advisory only."],
            "recommended_action": "add_to_existing_theme_review",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_openai_path_prefers_semiconductor_materials_new_theme_over_broad_materials_theme(self, _mock_key, _mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (3, 'Chemicals - Diversified', 'Materials', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (3, 'SHW')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'ROLESUB', 'ROLESUB', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'rolesub-openai-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "ROLESUB")

        self.assertEqual(draft["research_mode"], "openai")
        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertIn(
            draft["possible_new_theme"],
            {"Semiconductor Materials", "Semiconductor Substrates", "Compound Semiconductor Materials", "Specialty Semiconductor Materials"},
        )
        self.assertTrue(draft["suggested_existing_themes"])
        self.assertIn("adjacent", " ".join(draft["caveats"]).lower())
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    @patch("src.scanner_research._call_openai_research", side_effect=RuntimeError("HTTP 429 rate limit while calling provider with api_key=secret123"))
    @patch("src.scanner_research._load_company_profile", return_value={})
    def test_generate_scanner_research_draft_records_compact_fallback_reason(self, _mock_profile, _mock_call, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'SMCI', 'SMCI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'smci-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "SMCI")

        self.assertEqual(draft["research_mode"], "heuristic_fallback")
        self.assertIn("fallback_reason", draft)
        self.assertIn("HTTP 429 rate limit", draft["fallback_reason"])
        self.assertNotIn("secret123", draft["fallback_reason"])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    @patch("src.scanner_research._load_company_profile", return_value={})
    def test_generate_scanner_research_draft_captures_sanitized_openai_http_error_details(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'SMCI', 'SMCI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'smci-http-1')
            """
        )
        response = self._FakeResponse(
            429,
            payload={
                "error": {
                    "type": "rate_limit_exceeded",
                    "message": "Rate limit reached for model call with api_key=secret123",
                }
            },
        )
        error = requests.HTTPError("429 Client Error", response=response)

        with patch("src.scanner_research._call_openai_research", side_effect=error):
            draft = generate_scanner_research_draft(conn, "SMCI")

        self.assertEqual(draft["research_mode"], "heuristic_fallback")
        self.assertIn("research_error", draft)
        self.assertEqual(draft["research_error"]["status_code"], 429)
        self.assertEqual(draft["research_error"]["error_type"], "rate_limit_exceeded")
        self.assertEqual(draft["research_error"]["model"], AI_MODEL)
        self.assertIn("Rate limit reached", draft["research_error"]["error_message"])
        self.assertNotIn("secret123", draft["research_error"]["error_message"])
        self.assertIn("HTTP 429", draft["fallback_reason"])
        self.assertIn("rate_limit_exceeded", draft["fallback_reason"])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    @patch("src.scanner_research._load_company_profile", return_value={})
    def test_generate_scanner_research_draft_distinguishes_model_access_style_failure(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'SMCI', 'SMCI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'smci-http-2')
            """
        )
        response = self._FakeResponse(
            403,
            payload={
                "error": {
                    "type": "insufficient_permissions",
                    "message": "The model does not exist or you do not have access to it.",
                }
            },
        )
        error = requests.HTTPError("403 Client Error", response=response)

        with patch("src.scanner_research._call_openai_research", side_effect=error):
            draft = generate_scanner_research_draft(conn, "SMCI")

        self.assertEqual(draft["research_error"]["status_code"], 403)
        self.assertEqual(draft["research_error"]["error_type"], "insufficient_permissions")
        self.assertIn("do not have access", draft["research_error"]["error_message"])
        self.assertIn("HTTP 403", draft["fallback_reason"])
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "CRWD",
            "company_name": "CrowdStrike Holdings",
            "description": "Provides cloud-native endpoint and cybersecurity software.",
            "sic_description": "Computer Software",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        return_value={
            "company_name": "CrowdStrike Holdings",
            "short_company_description": "Cloud-native cybersecurity software company.",
            "possible_similar_tickers": ["PANW", "ZS"],
            "suggested_existing_themes": [
                {"theme_id": 2, "theme_name": "Cybersecurity", "why_it_might_fit": "Security software peer fit."}
            ],
            "possible_new_theme": "",
            "confidence": "high",
            "rationale": "OpenAI matched the ticker to the governed Cybersecurity theme using company and catalog context.",
            "caveats": ["Advisory only."],
            "recommended_action": "add_to_existing_theme_review",
        },
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_get_or_create_scanner_research_draft_reuses_existing_without_new_openai_call(self, _mock_key, mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cybersecurity', 'Software', true)")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'CRWD', 'CRWD', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'crwd-2')
            """
        )

        first, reused_first = get_or_create_scanner_research_draft(conn, "CRWD")
        second, reused_second = get_or_create_scanner_research_draft(conn, "CRWD", existing_draft=first)

        self.assertFalse(reused_first)
        self.assertTrue(reused_second)
        self.assertEqual(mock_call.call_count, 1)
        self.assertEqual(first, second)
        conn.close()

    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "CRWD",
            "company_name": "CrowdStrike Holdings",
            "description": "Provides cloud-native endpoint and cybersecurity software.",
            "sic_description": "Computer Software",
        },
    )
    @patch(
        "src.scanner_research._call_openai_research",
        side_effect=[
            {
                "company_name": "CrowdStrike Holdings",
                "short_company_description": "Cloud-native cybersecurity software company.",
                "possible_similar_tickers": ["PANW"],
                "suggested_existing_themes": [
                    {"theme_id": 2, "theme_name": "Cybersecurity", "why_it_might_fit": "Security software peer fit."}
                ],
                "possible_new_theme": "",
                "confidence": "high",
                "rationale": "First draft.",
                "caveats": ["Advisory only."],
                "recommended_action": "add_to_existing_theme_review",
            },
            {
                "company_name": "CrowdStrike Holdings",
                "short_company_description": "Cloud-native cybersecurity software company.",
                "possible_similar_tickers": ["PANW", "ZS"],
                "suggested_existing_themes": [
                    {"theme_id": 2, "theme_name": "Cybersecurity", "why_it_might_fit": "Refreshed security software peer fit."}
                ],
                "possible_new_theme": "",
                "confidence": "high",
                "rationale": "Refreshed draft.",
                "caveats": ["Advisory only."],
                "recommended_action": "add_to_existing_theme_review",
            },
        ],
    )
    @patch("src.scanner_research.openai_api_key", return_value="test-key")
    def test_get_or_create_scanner_research_draft_force_refresh_bypasses_existing_session_draft(self, _mock_key, mock_call, _mock_profile):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cybersecurity', 'Software', true)")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'CRWD', 'CRWD', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'crwd-2b')
            """
        )

        first, reused_first = get_or_create_scanner_research_draft(conn, "CRWD")
        refreshed, reused_second = get_or_create_scanner_research_draft(
            conn,
            "CRWD",
            existing_draft=first,
            force_refresh=True,
        )

        self.assertFalse(reused_first)
        self.assertFalse(reused_second)
        self.assertEqual(mock_call.call_count, 2)
        self.assertEqual(first["rationale"], "First draft.")
        self.assertEqual(refreshed["rationale"], "Refreshed draft.")
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "CRWD",
            "company_name": "CrowdStrike Holdings",
            "description": "Provides cloud-native endpoint and cybersecurity software for threat detection and security operations.",
            "sic_description": "Computer Software",
        },
    )
    def test_fallback_prefers_conceptual_fit_over_broad_token_overlap(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Cloud Software', 'Software', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cybersecurity', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NOW')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'PANW')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'ZS')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'CRWD', 'CRWD', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'crwd-fit-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "CRWD")

        self.assertTrue(draft["suggested_existing_themes"])
        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Cybersecurity")
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "WULF",
            "company_name": "TeraWulf",
            "description": "Operates digital asset mining facilities and power infrastructure.",
            "sic_description": "Electric Services",
        },
    )
    def test_fallback_can_return_no_strong_fit(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Cybersecurity', 'Software', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Biotech', 'Healthcare', true)")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'WULF', 'WULF', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'wulf-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "WULF")

        self.assertEqual(draft["suggested_existing_themes"], [])
        self.assertIn(draft["recommended_action"], {"consider_new_theme", "watch_only"})
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "PLTR",
            "company_name": "Palantir Technologies",
            "description": "Provides data integration, ontology, and decision software for government and enterprise customers.",
            "sic_description": "Computer Software",
        },
    )
    def test_fallback_similar_tickers_are_conservative_when_evidence_is_weak(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Data Analytics', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'SNOW')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'DDOG')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'MDB')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'PLTR', 'PLTR', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'pltr-peers-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "PLTR")

        self.assertLessEqual(len(draft["possible_similar_tickers"]), 3)
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "SMCI",
            "company_name": "Super Micro Computer",
            "description": "Provides servers and rack-scale systems for AI data-center deployments and accelerated computing workloads.",
            "sic_description": "Computer Hardware",
        },
    )
    def test_fallback_rationale_explains_conceptual_fit(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cloud Software', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'AMD')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'NOW')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'SMCI', 'SMCI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'smci-rationale-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "SMCI")

        self.assertIn("AI compute infrastructure", draft["rationale"])
        self.assertIn("Broader alternatives", draft["rationale"])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AAOI",
            "company_name": "Applied Optoelectronics",
            "description": "Designs and supplies optical transceivers, lasers, and fiber-optic networking products for data centers, telecom, and broadband access networks.",
            "sic_description": "Communications Equipment",
        },
    )
    def test_fallback_prefers_optical_networking_role_over_power_adjacency(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Power Infrastructure', 'Energy', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Optical Networking', 'Communications', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'VRT')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'ETN')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'LITE')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'CIEN')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AAOI', 'AAOI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'aaoi-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "AAOI")

        self.assertTrue(draft["suggested_existing_themes"])
        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Optical Networking")
        self.assertNotIn("Power Infrastructure", [item["theme_name"] for item in draft["suggested_existing_themes"][:1]])
        self.assertIn("optical networking", draft["rationale"].lower())
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AXT",
            "company_name": "AXT Inc.",
            "description": "Produces compound semiconductor substrates including gallium arsenide, indium phosphide, and germanium wafers used across communications and sensing applications.",
            "sic_description": "Semiconductor Materials",
        },
    )
    def test_fallback_prefers_semiconductor_materials_over_broad_ai_adjacency(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Semiconductor Materials', 'Semiconductors', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'SMCI')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'WOLF')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'ONTO')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AXT', 'AXT', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'axt-1')
            """
        )

        draft = generate_scanner_research_draft(conn, "AXT")

        self.assertTrue(draft["suggested_existing_themes"])
        self.assertEqual(draft["suggested_existing_themes"][0]["theme_name"], "Semiconductor Materials")
        self.assertNotIn("AI Infrastructure", [item["theme_name"] for item in draft["suggested_existing_themes"][:1]])
        self.assertIn("materials", draft["rationale"].lower())
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AXT",
            "company_name": "AXT Inc.",
            "description": "Produces compound semiconductor substrates and wafer materials for communications and sensing applications.",
            "sic_description": "Semiconductor Materials",
        },
    )
    def test_fallback_filters_obviously_irrelevant_themes(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Europe Luxury', 'Consumer', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Semiconductor Materials', 'Semiconductors', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'LVMUY')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'WOLF')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AXT', 'AXT', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'axt-2')
            """
        )

        draft = generate_scanner_research_draft(conn, "AXT")

        self.assertNotIn("Europe Luxury", [item["theme_name"] for item in draft["suggested_existing_themes"]])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AAOI",
            "company_name": "Applied Optoelectronics",
            "description": "Designs optical transceivers and fiber-optic interconnect products for data-center and telecom networks.",
            "sic_description": "Communications Equipment",
        },
    )
    def test_fallback_can_suggest_new_theme_for_narrow_uncovered_role(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Power', 'Energy', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Cloud Software', 'Software', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'VRT')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'NOW')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AAOI', 'AAOI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'aaoi-2')
            """
        )

        draft = generate_scanner_research_draft(conn, "AAOI")

        self.assertEqual(draft["suggested_existing_themes"], [])
        self.assertIn(draft["possible_new_theme"], {"Optical Networking", "Data Center Optics"})
        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AAOI",
            "company_name": "Applied Optoelectronics",
            "description": "Designs optical transceivers, fiber interconnect modules, and related networking products for data-center and telecom deployments.",
            "sic_description": "Communications Equipment",
        },
    )
    def test_fallback_can_propose_optics_new_theme_even_with_indirect_existing_matches(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'Data Center Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'VRT')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AAOI', 'AAOI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'aaoi-4')
            """
        )

        draft = generate_scanner_research_draft(conn, "AAOI")

        self.assertIn(draft["possible_new_theme"], {"Optical Networking", "Data Center Optics", "Optical Interconnects"})
        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertTrue(draft["suggested_existing_themes"])
        self.assertIn("more precise", draft["rationale"])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AXT",
            "company_name": "AXT Inc.",
            "description": "Produces compound semiconductor substrates and wafer materials used in communications and sensing applications.",
            "sic_description": "Semiconductor Materials",
        },
    )
    def test_fallback_can_propose_semiconductor_materials_new_theme_even_with_broad_adjacent_matches(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Semiconductor Leaders', 'Tech', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'NVDA')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'SMCI')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AXT', 'AXT', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'axt-4')
            """
        )

        draft = generate_scanner_research_draft(conn, "AXT")

        self.assertIn(
            draft["possible_new_theme"],
            {"Semiconductor Materials", "Semiconductor Substrates", "Compound Semiconductor Materials"},
        )
        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertTrue(draft["suggested_existing_themes"])
        self.assertIn("current governed taxonomy", draft["rationale"])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AAOI",
            "company_name": "Applied Optoelectronics",
            "description": "Designs optical networking and fiber interconnect products used in data-center deployments.",
            "sic_description": "Communications Equipment",
        },
    )
    def test_possible_new_theme_is_not_suppressed_by_broad_indirect_governed_fit(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Data Center Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'VRT')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AAOI', 'AAOI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'aaoi-5')
            """
        )

        draft = generate_scanner_research_draft(conn, "AAOI")

        self.assertEqual(draft["recommended_action"], "consider_new_theme")
        self.assertTrue(draft["suggested_existing_themes"])
        self.assertIsNotNone(draft["possible_new_theme"])
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AAOI",
            "company_name": "Applied Optoelectronics",
            "description": "Designs optical transceivers and fiber interconnect products for cloud and telecom networks.",
            "sic_description": "Communications Equipment",
        },
    )
    def test_fallback_similar_tickers_are_role_aligned(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Optical Networking', 'Communications', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'LITE')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'CIEN')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'INFN')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'NVDA')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AAOI', 'AAOI', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'aaoi-3')
            """
        )

        draft = generate_scanner_research_draft(conn, "AAOI")

        self.assertLessEqual(len(draft["possible_similar_tickers"]), 3)
        self.assertTrue(set(draft["possible_similar_tickers"]).issubset({"LITE", "CIEN", "INFN"}))
        conn.close()

    @patch("src.scanner_research.openai_api_key", return_value=None)
    @patch(
        "src.scanner_research._load_company_profile",
        return_value={
            "ticker": "AXT",
            "company_name": "AXT Inc.",
            "description": "Produces compound semiconductor substrates for communications and sensing markets rather than designing finished chips.",
            "sic_description": "Semiconductor Materials",
        },
    )
    def test_fallback_rationale_reflects_role_in_stack_reasoning(self, _mock_profile, _mock_key):
        conn = self._conn()
        conn.execute("insert into themes(id, name, category, is_active) values (1, 'Semiconductor Materials', 'Semiconductors', true)")
        conn.execute("insert into themes(id, name, category, is_active) values (2, 'AI Infrastructure', 'Tech', true)")
        conn.execute("insert into theme_membership(theme_id, ticker) values (1, 'WOLF')")
        conn.execute("insert into theme_membership(theme_id, ticker) values (2, 'NVDA')")
        conn.execute(
            """
            insert into scanner_hit_history(
                import_run_id, import_source, normalized_ticker, raw_ticker, observed_date, observed_at,
                source_file, source_label, scanner_name, row_hash
            ) values
            (1, 'tc2000', 'AXT', 'AXT', '2026-03-12', '2026-03-12 08:00:00', 'f1.csv', 'tc2000', 'Momentum', 'axt-3')
            """
        )

        draft = generate_scanner_research_draft(conn, "AXT")

        self.assertIn("serve", draft["rationale"])
        self.assertIn("materials", draft["rationale"].lower())
        self.assertIn("actual role in the stack", draft["rationale"])
        conn.close()


if __name__ == "__main__":
    unittest.main()
