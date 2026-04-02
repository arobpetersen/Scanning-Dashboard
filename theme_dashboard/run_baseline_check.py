from __future__ import annotations

from pathlib import Path

from src.config import DB_PATH
from src.database import get_conn
from src.queries import baseline_status, core_table_status, source_audit_status


def collect_baseline_check() -> tuple[dict, int]:
    report: dict = {
        "db_path": str(DB_PATH),
        "db_exists": Path(DB_PATH).exists(),
        "db_connection_ok": False,
        "missing_tables": [],
        "warnings": [],
    }
    exit_code = 0

    if not report["db_exists"]:
        report["warnings"].append("DuckDB file does not exist yet. Run the app or rebuild/bootstrap flow first.")
        return report, 1

    with get_conn() as conn:
        report["db_connection_ok"] = True
        table_status = core_table_status(conn)
        missing_tables = table_status[table_status["exists"] == False]["table_name"].tolist()
        report["missing_tables"] = missing_tables
        if missing_tables:
            exit_code = 1

        overview = baseline_status(conn)
        row = overview.iloc[0].to_dict() if not overview.empty else {}
        report.update(row)
        audit = source_audit_status(conn)
        audit_row = audit.iloc[0].to_dict() if not audit.empty else {}
        report.update(audit_row)

    if int(report.get("themes_count") or 0) <= 0:
        report["warnings"].append("No themes present in DuckDB. Bootstrap/seed may not have run yet.")
        exit_code = 1

    if int(report.get("theme_snapshot_sets") or 0) <= 1:
        report["warnings"].append(
            f"Only {int(report.get('theme_snapshot_sets') or 0)} theme snapshot set(s) available. "
            "At least 2 boundary snapshots are needed for comparisons."
        )
    if int(report.get("ticker_snapshot_sets") or 0) <= 1:
        report["warnings"].append(
            f"Only {int(report.get('ticker_snapshot_sets') or 0)} ticker snapshot set(s) available. "
            "History views become more useful after another refresh."
        )
    if bool(report.get("active_contamination")):
        report["warnings"].append("Active source contamination detected in current views. Latest live-facing views are not source-pure.")
    elif bool(report.get("historical_residue_only")):
        report["warnings"].append("Mixed source history exists, but current live-facing views are using live-preferred selection.")

    return report, exit_code


def format_baseline_check(report: dict) -> str:
    lines = [
        "Scanning Dashboard baseline/source check",
        f"DB path: {report['db_path']}",
        f"DB exists: {'yes' if report.get('db_exists') else 'no'}",
        f"DB connection: {'ok' if report.get('db_connection_ok') else 'failed'}",
    ]
    missing_tables = report.get("missing_tables", [])
    lines.append(f"Core tables: {'ok' if not missing_tables else 'missing -> ' + ', '.join(missing_tables)}")
    if report.get("db_connection_ok"):
        lines.extend(
            [
                f"Themes present: {int(report.get('themes_count') or 0)}",
                "Latest refresh: "
                + (
                    f"run_id={int(report['latest_run_id'])} provider={report.get('latest_run_provider')} "
                    f"status={report.get('latest_run_status')} finished_at={report.get('latest_run_finished_at')}"
                    if report.get("latest_run_id") is not None
                    else "none"
                ),
                f"Snapshot rows: ticker={int(report.get('ticker_snapshot_rows') or 0)} theme={int(report.get('theme_snapshot_rows') or 0)}",
                f"Runs with theme snapshots: {int(report.get('runs_with_theme_snapshots') or 0)}",
                f"Latest theme snapshot time: {report.get('latest_theme_snapshot_time') or 'none'}",
                f"Latest ticker snapshot time: {report.get('latest_ticker_snapshot_time') or 'none'}",
                f"Preferred current sources: theme={report.get('preferred_theme_source') or 'none'} ticker={report.get('preferred_ticker_source') or 'none'}",
                f"Latest current-view sources: theme={report.get('latest_theme_view_sources') or 'none'} ticker={report.get('latest_ticker_view_sources') or 'none'}",
                f"Recent theme snapshot sources: {report.get('recent_theme_sources') or 'none'}",
                f"Recent ticker snapshot sources: {report.get('recent_ticker_sources') or 'none'}",
                f"Current views source-pure: {'yes' if not report.get('active_contamination') else 'no'}",
            ]
        )
    if report.get("warnings"):
        lines.append("Warnings:")
        lines.extend([f"- {warning}" for warning in report["warnings"]])
    return "\n".join(lines)


def main() -> int:
    report, exit_code = collect_baseline_check()
    print(format_baseline_check(report))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
