import os
import shutil
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import duckdb

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


if __name__ == "__main__":
    unittest.main()
