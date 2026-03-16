import os
import shutil
import unittest
import uuid
from datetime import datetime
from pathlib import Path

import duckdb

from src.database import SCHEMA_SQL
from src.scanner_audit import (
    import_tc2000_exports,
    parse_tc2000_export_file,
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


if __name__ == "__main__":
    unittest.main()
