from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .config import TC2000_DEFAULT_SOURCE_LABEL, TC2000_FILE_GLOB, TC2000_IMPORT_DIR

TICKER_COLUMN_ALIASES = {"ticker", "symbol", "sym", "stock_symbol"}
SCANNER_COLUMN_ALIASES = {"scanner", "scanner_name", "scan", "watchlist", "list_name"}
DATE_COLUMN_ALIASES = {"observed_date", "date", "as_of", "asof", "scan_date"}
EXCLUDED_SUPPORTING_FIELDS = TICKER_COLUMN_ALIASES | SCANNER_COLUMN_ALIASES | DATE_COLUMN_ALIASES
GENERIC_EXPORT_NAME_TOKENS = {"export", "exports", "scan", "scanner", "audit", "report", "tc2000"}
FILENAME_DATE_PATTERN = re.compile(
    r"(?<!\d)(?P<year>20\d{2})[\s_-]?(?P<month>0[1-9]|1[0-2])[\s_-]?(?P<day>0[1-9]|[12]\d|3[01])(?!\d)"
)


def _normalize_column_name(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def normalize_ticker(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value or "").strip().upper()
    if text in {"", "NAN", "NONE", "NULL"}:
        return ""
    text = text.replace("/", ".")
    text = re.sub(r"[^A-Z0-9.\-]", "", text)
    return text


def _scanner_name_from_path(path: Path) -> str:
    return re.sub(r"[_\-.]+", " ", path.stem).strip() or path.stem


def _parsed_filename_date(path: Path):
    match = FILENAME_DATE_PATTERN.search(path.stem)
    if not match:
        return None
    try:
        return datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        ).date()
    except ValueError:
        return None


def _scanner_name_from_filename(path: Path, default_source_label: str) -> tuple[str, str]:
    stem = path.stem
    cleaned = FILENAME_DATE_PATTERN.sub(" ", stem)
    cleaned = re.sub(r"[_\-.]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_")
    if not cleaned:
        return default_source_label, "default_source_label_fallback"

    tokens = {piece.lower() for piece in cleaned.split() if piece.strip()}
    if tokens and tokens.issubset(GENERIC_EXPORT_NAME_TOKENS):
        return default_source_label, "default_source_label_fallback"
    return cleaned, "filename_parse"


def _read_export_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if suffix == ".tsv":
        return pd.read_csv(path, sep="\t")
    if suffix == ".txt":
        try:
            return pd.read_csv(path, sep=None, engine="python")
        except Exception:
            return pd.read_csv(path, sep="\t")
    return pd.read_csv(path, sep=None, engine="python")


def _find_first_matching_column(df: pd.DataFrame, aliases: set[str]) -> str | None:
    for col in df.columns:
        if _normalize_column_name(col) in aliases:
            return str(col)
    return None


def _coerce_observed_dates(df: pd.DataFrame, path: Path) -> tuple[pd.Series, pd.Series]:
    date_col = _find_first_matching_column(df, DATE_COLUMN_ALIASES)
    filename_date = _parsed_filename_date(path)
    modified_date = datetime.fromtimestamp(path.stat().st_mtime).date()
    if date_col:
        parsed = pd.to_datetime(df[date_col], errors="coerce")
        dates = parsed.dt.date
        basis = pd.Series(["file_column"] * len(df), index=df.index, dtype="object")
        if filename_date is not None:
            missing = dates.isna()
            dates = dates.where(~missing, filename_date)
            basis = basis.where(~missing, "filename_parse")
        missing = dates.isna()
        if missing.any():
            dates = dates.where(~missing, modified_date)
            basis = basis.where(~missing, "modified_timestamp_fallback")
        return dates, basis
    if filename_date is not None:
        return (
            pd.Series([filename_date] * len(df), index=df.index),
            pd.Series(["filename_parse"] * len(df), index=df.index, dtype="object"),
        )
    return (
        pd.Series([modified_date] * len(df), index=df.index),
        pd.Series(["modified_timestamp_fallback"] * len(df), index=df.index, dtype="object"),
    )


def _coerce_scanner_names(df: pd.DataFrame, path: Path, default_source_label: str) -> tuple[pd.Series, pd.Series]:
    scanner_col = _find_first_matching_column(df, SCANNER_COLUMN_ALIASES)
    default_name, fallback_basis = _scanner_name_from_filename(path, default_source_label)
    if not scanner_col:
        return (
            pd.Series([default_name] * len(df), index=df.index),
            pd.Series([fallback_basis] * len(df), index=df.index, dtype="object"),
        )
    names = df[scanner_col].astype(str).str.strip()
    names = names.where(names.ne(""), None)
    names = names.where(~names.isna(), default_name).fillna(default_name)
    basis = pd.Series(["file_column"] * len(df), index=df.index, dtype="object")
    basis = basis.where(df[scanner_col].astype(str).str.strip().ne(""), fallback_basis)
    return names, basis


def _supporting_fields_json(df: pd.DataFrame) -> pd.Series:
    normalized = {_normalize_column_name(col): str(col) for col in df.columns}
    supporting_cols = [orig for norm, orig in normalized.items() if norm not in EXCLUDED_SUPPORTING_FIELDS]
    if not supporting_cols:
        return pd.Series([None] * len(df), index=df.index)

    payloads: list[str | None] = []
    for _, row in df[supporting_cols].iterrows():
        payload: dict[str, object] = {}
        for col in supporting_cols:
            value = row[col]
            if value is None or pd.isna(value):
                continue
            if isinstance(value, pd.Timestamp):
                payload[str(col)] = value.isoformat()
            elif isinstance(value, (str, int, float, bool)):
                if isinstance(value, float) and not math.isfinite(value):
                    continue
                payload[str(col)] = value
            else:
                payload[str(col)] = str(value)
        payloads.append(json.dumps(payload, sort_keys=True) if payload else None)
    return pd.Series(payloads, index=df.index)


def parse_tc2000_export_file(
    path: Path,
    *,
    imported_at: datetime | None = None,
    default_source_label: str | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    imported_at = imported_at or datetime.now(UTC).replace(tzinfo=None)
    source_label = (default_source_label or TC2000_DEFAULT_SOURCE_LABEL).strip() or TC2000_DEFAULT_SOURCE_LABEL
    raw = _read_export_file(path)
    inferred_scanner_name, scanner_name_basis = _scanner_name_from_filename(path, source_label)
    filename_date = _parsed_filename_date(path)
    observed_dates, observed_date_basis = _coerce_observed_dates(raw, path)
    scanner_names, scanner_name_basis_series = _coerce_scanner_names(raw, path, source_label)
    meta = {
        "source_file": path.name,
        "rows_read": int(len(raw)),
        "rows_parsed": 0,
        "rows_skipped": 0,
        "unique_tickers": 0,
        "scanner_name": inferred_scanner_name,
        "scanner_name_inferred": _find_first_matching_column(raw, SCANNER_COLUMN_ALIASES) is None,
        "scanner_name_basis": scanner_name_basis,
        "observed_date_inferred": _find_first_matching_column(raw, DATE_COLUMN_ALIASES) is None,
        "observed_date_basis": "filename_parse" if filename_date is not None else "modified_timestamp_fallback",
    }
    if raw.empty:
        return pd.DataFrame(), meta

    ticker_col = _find_first_matching_column(raw, TICKER_COLUMN_ALIASES)
    if not ticker_col:
        raise ValueError(f"No ticker column found in {path.name}. Expected one of {sorted(TICKER_COLUMN_ALIASES)}.")

    parsed = pd.DataFrame(
        {
            "normalized_ticker": raw[ticker_col].map(normalize_ticker),
            "raw_ticker": raw[ticker_col].astype(str).str.strip(),
            "observed_date": observed_dates,
            "observed_at": imported_at,
            "source_file": path.name,
            "source_label": source_label,
            "scanner_name": scanner_names.astype(str).str.strip(),
            "file_modified_at": datetime.fromtimestamp(path.stat().st_mtime),
            "supporting_fields_json": _supporting_fields_json(raw),
            "scanner_name_inferred": scanner_name_basis_series.ne("file_column"),
            "scanner_name_basis": scanner_name_basis_series,
            "observed_date_inferred": observed_date_basis.ne("file_column"),
            "observed_date_basis": observed_date_basis,
        }
    )
    parsed["scanner_name"] = parsed["scanner_name"].replace("", inferred_scanner_name)
    parsed = parsed[parsed["normalized_ticker"].astype(str).str.len() > 0].copy()
    parsed = parsed[pd.notna(parsed["observed_date"])].copy()
    if parsed.empty:
        meta["rows_skipped"] = meta["rows_read"]
        return pd.DataFrame(), meta

    parsed["row_hash"] = parsed.apply(
        lambda row: hashlib.sha1(
            "|".join(
                [
                    str(row["normalized_ticker"]),
                    str(row["observed_date"]),
                    str(row["scanner_name"]),
                    str(row["source_label"]),
                ]
            ).encode("utf-8")
        ).hexdigest(),
        axis=1,
    )
    parsed = parsed.drop_duplicates(subset=["row_hash"]).reset_index(drop=True)
    meta["rows_parsed"] = int(len(parsed))
    meta["rows_skipped"] = int(meta["rows_read"] - meta["rows_parsed"])
    meta["unique_tickers"] = int(parsed["normalized_ticker"].nunique())
    return parsed, meta


def _collect_candidate_files(folder: Path, pattern: str) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for part in [piece.strip() for piece in str(pattern or TC2000_FILE_GLOB).split(",") if piece.strip()]:
        for path in sorted(folder.glob(part)):
            if path.is_file() and path not in seen:
                seen.add(path)
                files.append(path)
    return sorted(files)


def _normalize_source_file(path: Path) -> str:
    return str(path.resolve()).lower()


def _file_fingerprint(path: Path) -> tuple[str, int, datetime]:
    stat = path.stat()
    modified_at = datetime.fromtimestamp(stat.st_mtime)
    payload = "|".join(
        [
            _normalize_source_file(path),
            str(stat.st_size),
            str(int(stat.st_mtime_ns)),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest(), int(stat.st_size), modified_at


def _observed_date_diagnostics(parsed: pd.DataFrame, path: Path) -> tuple[str | None, str | None, int]:
    if not parsed.empty and "observed_date" in parsed.columns:
        values = sorted({str(value) for value in parsed["observed_date"].astype(str).tolist() if str(value).strip()})
        if values:
            return values[0], values[-1], len(values)

    fallback_date = _parsed_filename_date(path)
    if fallback_date is not None:
        text = str(fallback_date)
        return text, text, 1

    modified_text = str(datetime.fromtimestamp(path.stat().st_mtime).date())
    return modified_text, modified_text, 1


def import_tc2000_exports(
    conn,
    *,
    folder: Path | None = None,
    pattern: str | None = None,
    default_source_label: str | None = None,
) -> dict[str, object]:
    folder = Path(folder or TC2000_IMPORT_DIR)
    pattern = str(pattern or TC2000_FILE_GLOB)
    source_label = (default_source_label or TC2000_DEFAULT_SOURCE_LABEL).strip() or TC2000_DEFAULT_SOURCE_LABEL
    started_at = datetime.now(UTC).replace(tzinfo=None)
    import_run_id = int(
        conn.execute(
            """
            INSERT INTO scanner_import_runs(import_source, folder_path, file_pattern, started_at, status)
            VALUES ('tc2000', ?, ?, ?, 'running')
            RETURNING import_run_id
            """,
            [str(folder), pattern, started_at],
        ).fetchone()[0]
    )

    if not folder.exists():
        conn.execute(
            """
            UPDATE scanner_import_runs
            SET finished_at = ?, status = 'failed', error_message = ?
            WHERE import_run_id = ?
            """,
            [datetime.now(UTC).replace(tzinfo=None), f"Folder does not exist: {folder}", import_run_id],
        )
        return {
            "import_run_id": import_run_id,
            "status": "failed",
            "files_seen": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "rows_read": 0,
            "rows_imported": 0,
            "rows_skipped": 0,
            "unique_tickers_observed": 0,
            "file_results": [],
            "message": f"Folder does not exist: {folder}",
        }

    file_results: list[dict[str, object]] = []
    rows_read = 0
    rows_imported = 0
    rows_skipped = 0
    files_processed = 0
    files_skipped = 0
    files_failed = 0
    unique_tickers: set[str] = set()
    files = _collect_candidate_files(folder, pattern)
    if not files:
        conn.execute(
            """
            UPDATE scanner_import_runs
            SET finished_at = ?, status = 'no_files', files_seen = 0, notes = ?
            WHERE import_run_id = ?
            """,
            [datetime.now(UTC).replace(tzinfo=None), "No matching TC2000 export files found.", import_run_id],
        )
        return {
            "import_run_id": import_run_id,
            "status": "no_files",
            "files_seen": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "rows_read": 0,
            "rows_imported": 0,
            "rows_skipped": 0,
            "unique_tickers_observed": 0,
            "file_results": [],
            "message": "No matching TC2000 export files found.",
        }

    for path in files:
        fingerprint, file_size, modified_at = _file_fingerprint(path)
        observed_date_min, observed_date_max, observed_dates_count = _observed_date_diagnostics(pd.DataFrame(), path)
        try:
            already_imported = conn.execute(
                """
                SELECT first_import_run_id
                FROM scanner_imported_files
                WHERE file_fingerprint = ?
                  AND import_status = 'success'
                LIMIT 1
                """,
                [fingerprint],
            ).fetchone()
            if already_imported:
                conn.execute(
                    """
                    UPDATE scanner_imported_files
                    SET last_seen_run_id = ?
                    WHERE file_fingerprint = ?
                    """,
                    [import_run_id, fingerprint],
                )
                files_skipped += 1
                file_results.append(
                    {
                        "source_file": path.name,
                        "rows_read": 0,
                        "rows_parsed": 0,
                        "rows_imported": 0,
                        "rows_skipped": 0,
                        "unique_tickers": 0,
                        "status": "skipped_already_imported",
                        "scanner_name": _scanner_name_from_filename(path, source_label)[0],
                        "scanner_name_inferred": True,
                        "observed_date_inferred": True,
                        "scanner_name_basis": _scanner_name_from_filename(path, source_label)[1],
                        "observed_date_basis": "filename_parse" if _parsed_filename_date(path) is not None else "modified_timestamp_fallback",
                        "observed_date_min": observed_date_min,
                        "observed_date_max": observed_date_max,
                        "observed_dates_count": observed_dates_count,
                    }
                )
                continue

            parsed, meta = parse_tc2000_export_file(path, imported_at=started_at, default_source_label=source_label)
            observed_date_min, observed_date_max, observed_dates_count = _observed_date_diagnostics(parsed, path)
            file_result = {
                "source_file": path.name,
                "rows_read": int(meta["rows_read"]),
                "rows_parsed": int(meta["rows_parsed"]),
                "rows_imported": 0,
                "rows_skipped": int(meta["rows_skipped"]),
                "unique_tickers": int(meta["unique_tickers"]),
                "status": "processed",
                "scanner_name": str(meta["scanner_name"]),
                "scanner_name_inferred": bool(meta["scanner_name_inferred"]),
                "scanner_name_basis": str(meta["scanner_name_basis"]),
                "observed_date_inferred": bool(meta["observed_date_inferred"]),
                "observed_date_basis": str(meta["observed_date_basis"]),
                "observed_date_min": observed_date_min,
                "observed_date_max": observed_date_max,
                "observed_dates_count": observed_dates_count,
            }
            rows_read += int(meta["rows_read"])
            unique_tickers.update(parsed["normalized_ticker"].astype(str).tolist())

            to_insert = 0
            if not parsed.empty:
                staged = parsed.copy()
                staged["import_run_id"] = import_run_id
                staged["import_source"] = "tc2000"
                conn.register("scanner_hits_stage", staged)
                try:
                    to_insert = int(
                        conn.execute(
                            """
                            SELECT COUNT(*)
                            FROM scanner_hits_stage s
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM scanner_hit_history h
                                WHERE h.row_hash = s.row_hash
                            )
                            """
                        ).fetchone()[0]
                    )
                    conn.execute(
                        """
                        INSERT INTO scanner_hit_history(
                            import_run_id,
                            import_source,
                            normalized_ticker,
                            raw_ticker,
                            observed_date,
                            observed_at,
                            source_file,
                            source_label,
                            scanner_name,
                            file_modified_at,
                            scanner_name_inferred,
                            scanner_name_basis,
                            observed_date_inferred,
                            observed_date_basis,
                            row_hash,
                            supporting_fields_json
                        )
                        SELECT
                            import_run_id,
                            import_source,
                            normalized_ticker,
                            raw_ticker,
                            observed_date,
                            observed_at,
                            source_file,
                            source_label,
                            scanner_name,
                            file_modified_at,
                            scanner_name_inferred,
                            scanner_name_basis,
                            observed_date_inferred,
                            observed_date_basis,
                            row_hash,
                            supporting_fields_json
                        FROM scanner_hits_stage s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM scanner_hit_history h
                            WHERE h.row_hash = s.row_hash
                        )
                        """
                    )
                finally:
                    conn.unregister("scanner_hits_stage")
            conn.execute(
                """
                INSERT INTO scanner_imported_files(
                    file_fingerprint,
                    source_file,
                    file_name,
                    file_size,
                    modified_at,
                    first_import_run_id,
                    last_seen_run_id,
                    import_status,
                    processed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'success', ?)
                ON CONFLICT(file_fingerprint) DO UPDATE
                SET source_file = excluded.source_file,
                    file_name = excluded.file_name,
                    file_size = excluded.file_size,
                    modified_at = excluded.modified_at,
                    last_seen_run_id = excluded.last_seen_run_id,
                    import_status = excluded.import_status,
                    processed_at = excluded.processed_at
                """,
                [
                    fingerprint,
                    _normalize_source_file(path),
                    path.name,
                    file_size,
                    modified_at,
                    import_run_id,
                    import_run_id,
                    datetime.now(UTC).replace(tzinfo=None),
                ],
            )
            file_result["rows_imported"] = to_insert
            file_result["rows_skipped"] = int(file_result["rows_skipped"]) + int(len(parsed) - to_insert)
            rows_imported += to_insert
            rows_skipped += int(len(parsed) - to_insert) + int(meta["rows_skipped"])
            files_processed += 1
            file_results.append(file_result)
        except Exception as exc:
            files_failed += 1
            conn.execute(
                """
                INSERT INTO scanner_imported_files(
                    file_fingerprint,
                    source_file,
                    file_name,
                    file_size,
                    modified_at,
                    first_import_run_id,
                    last_seen_run_id,
                    import_status,
                    processed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'failed', ?)
                ON CONFLICT(file_fingerprint) DO UPDATE
                SET source_file = excluded.source_file,
                    file_name = excluded.file_name,
                    file_size = excluded.file_size,
                    modified_at = excluded.modified_at,
                    last_seen_run_id = excluded.last_seen_run_id,
                    import_status = excluded.import_status,
                    processed_at = excluded.processed_at
                """,
                [
                    fingerprint,
                    _normalize_source_file(path),
                    path.name,
                    file_size,
                    modified_at,
                    import_run_id,
                    import_run_id,
                    datetime.now(UTC).replace(tzinfo=None),
                ],
            )
            file_results.append(
                {
                    "source_file": path.name,
                    "rows_read": 0,
                    "rows_parsed": 0,
                    "rows_imported": 0,
                    "rows_skipped": 0,
                    "unique_tickers": 0,
                    "status": "failed",
                    "scanner_name": _scanner_name_from_filename(path, source_label)[0],
                    "scanner_name_inferred": True,
                    "scanner_name_basis": _scanner_name_from_filename(path, source_label)[1],
                    "observed_date_inferred": True,
                    "observed_date_basis": "filename_parse" if _parsed_filename_date(path) is not None else "modified_timestamp_fallback",
                    "observed_date_min": observed_date_min,
                    "observed_date_max": observed_date_max,
                    "observed_dates_count": observed_dates_count,
                    "error": str(exc),
                }
            )

    status = "success"
    if files_failed > 0:
        status = "partial"
    conn.execute(
        """
        UPDATE scanner_import_runs
        SET finished_at = ?,
            status = ?,
            files_seen = ?,
            files_processed = ?,
            files_skipped = ?,
            files_failed = ?,
            rows_read = ?,
            rows_imported = ?,
            rows_skipped = ?,
            unique_tickers_observed = ?,
            notes = ?
        WHERE import_run_id = ?
        """,
        [
            datetime.now(UTC).replace(tzinfo=None),
            status,
            len(files),
            files_processed,
            files_skipped,
            files_failed,
            rows_read,
            rows_imported,
            rows_skipped,
            len(unique_tickers),
            f"Seen {len(files)} file(s) in {folder}; processed {files_processed}, skipped {files_skipped}, failed {files_failed}.",
            import_run_id,
        ],
    )
    return {
        "import_run_id": import_run_id,
        "status": status,
        "files_seen": len(files),
        "files_processed": files_processed,
        "files_skipped": files_skipped,
        "files_failed": files_failed,
        "rows_read": rows_read,
        "rows_imported": rows_imported,
        "rows_skipped": rows_skipped,
        "unique_tickers_observed": len(unique_tickers),
        "file_results": file_results,
        "message": (
            f"Seen {len(files)} file(s); processed {files_processed}, "
            f"skipped {files_skipped}, failed {files_failed}; imported {rows_imported} new scanner hits."
        ),
    }


def recent_scanner_import_runs(conn, limit: int = 20) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            import_run_id,
            status,
            folder_path,
            file_pattern,
            started_at,
            finished_at,
            files_seen,
            files_processed,
            files_skipped,
            files_failed,
            rows_read,
            rows_imported,
            rows_skipped,
            unique_tickers_observed,
            notes,
            error_message
        FROM scanner_import_runs
        ORDER BY import_run_id DESC
        LIMIT ?
        """,
        [int(limit)],
    ).df()


def scanner_import_overview(conn) -> dict[str, object]:
    latest = recent_scanner_import_runs(conn, limit=1)
    candidates = scanner_candidate_summary(conn)
    if latest.empty:
        return {
            "last_import_time": None,
            "files_seen": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "rows_imported": 0,
            "unique_tickers_seen": 0,
            "uncovered_candidates": 0,
            "ignored_candidates": 0,
        }
    last = latest.iloc[0]
    return {
        "last_import_time": last.get("finished_at") or last.get("started_at"),
        "files_seen": int(last.get("files_seen") or 0),
        "files_processed": int(last.get("files_processed") or 0),
        "files_skipped": int(last.get("files_skipped") or 0),
        "files_failed": int(last.get("files_failed") or 0),
        "rows_imported": int(last.get("rows_imported") or 0),
        "unique_tickers_seen": int(last.get("unique_tickers_observed") or 0),
        "uncovered_candidates": int((candidates["is_governed"] == False).sum()) if not candidates.empty else 0,
        "ignored_candidates": int((candidates["review_state"] == "ignored").sum()) if not candidates.empty else 0,
    }


def _current_streak(global_dates: list[pd.Timestamp], ticker_dates: set[pd.Timestamp]) -> int:
    streak = 0
    for dt in global_dates:
        if dt in ticker_dates:
            streak += 1
        elif streak > 0:
            break
    return streak


def scanner_candidate_summary(conn) -> pd.DataFrame:
    hits = conn.execute(
        """
        SELECT
            normalized_ticker,
            observed_date,
            scanner_name,
            source_label,
            scanner_name_inferred,
            scanner_name_basis,
            observed_date_inferred,
            observed_date_basis
        FROM scanner_hit_history
        ORDER BY observed_date DESC, normalized_ticker
        """
    ).df()
    if hits.empty:
        return hits

    hits["observed_date"] = pd.to_datetime(hits["observed_date"]).dt.normalize()
    unique_dates = sorted(hits["observed_date"].drop_duplicates().tolist(), reverse=True)
    last_5 = set(unique_dates[:5])
    last_10 = set(unique_dates[:10])

    grouped = (
        hits.groupby("normalized_ticker", as_index=False)
        .agg(
            first_seen=("observed_date", "min"),
            last_seen=("observed_date", "max"),
            total_observations=("normalized_ticker", "count"),
            observed_days=("observed_date", "nunique"),
            distinct_scanner_count=("scanner_name", "nunique"),
            scanners=("scanner_name", lambda s: ", ".join(sorted({str(v) for v in s if str(v).strip()}))),
            source_labels=("source_label", lambda s: ", ".join(sorted({str(v) for v in s if str(v).strip()}))),
            inferred_scanner_name_used=("scanner_name_inferred", "max"),
            scanner_name_basis_values=("scanner_name_basis", lambda s: sorted({str(v) for v in s if str(v).strip()})),
            inferred_observed_date_used=("observed_date_inferred", "max"),
            observed_date_basis_values=("observed_date_basis", lambda s: sorted({str(v) for v in s if str(v).strip()})),
        )
    )
    days_last_5 = hits[hits["observed_date"].isin(last_5)].groupby("normalized_ticker")["observed_date"].nunique()
    days_last_10 = hits[hits["observed_date"].isin(last_10)].groupby("normalized_ticker")["observed_date"].nunique()
    grouped["observations_last_5d"] = grouped["normalized_ticker"].map(days_last_5).fillna(0).astype(int)
    grouped["observations_last_10d"] = grouped["normalized_ticker"].map(days_last_10).fillna(0).astype(int)
    ticker_date_sets = hits.groupby("normalized_ticker")["observed_date"].agg(lambda s: set(s.tolist()))
    grouped["current_streak"] = grouped["normalized_ticker"].map(
        lambda ticker: _current_streak(unique_dates, ticker_date_sets.get(ticker, set()))
    ).fillna(0).astype(int)
    grouped["persistence_score"] = (
        grouped["observed_days"]
        + grouped["observations_last_5d"] * 2
        + grouped["observations_last_10d"]
        + grouped["current_streak"] * 2
        + grouped["distinct_scanner_count"]
    ).astype(int)

    governed = conn.execute(
        """
        SELECT
            upper(m.ticker) AS normalized_ticker,
            COUNT(DISTINCT m.theme_id) AS active_theme_count,
            STRING_AGG(DISTINCT t.name, ', ' ORDER BY t.name) AS current_theme_names,
            STRING_AGG(DISTINCT t.category, ', ' ORDER BY t.category) AS current_categories
        FROM theme_membership m
        JOIN themes t ON t.id = m.theme_id
        WHERE t.is_active = TRUE
        GROUP BY upper(m.ticker)
        """
    ).df()
    if governed.empty:
        governed = pd.DataFrame(columns=["normalized_ticker", "active_theme_count", "current_theme_names", "current_categories"])
    out = grouped.merge(governed, on="normalized_ticker", how="left")
    out["active_theme_count"] = pd.to_numeric(out["active_theme_count"], errors="coerce").fillna(0).astype(int)
    out["is_governed"] = out["active_theme_count"] > 0
    out["governed_status"] = out["is_governed"].map({True: "already governed", False: "uncovered"})

    def _recommend(row: pd.Series) -> str:
        if bool(row["is_governed"]):
            return "already covered"
        if int(row["current_streak"]) >= 3 and int(row["observations_last_10d"]) >= 4:
            return "high-persistence uncovered"
        if int(row["observations_last_10d"]) >= 3 or int(row["distinct_scanner_count"]) >= 2:
            return "review for addition"
        return "monitor"

    out["recommendation"] = out.apply(_recommend, axis=1)

    review_state = conn.execute(
        """
        SELECT normalized_ticker, review_state, review_note, updated_at
        FROM scanner_candidate_review_state
        """
    ).df()
    if review_state.empty:
        review_state = pd.DataFrame(columns=["normalized_ticker", "review_state", "review_note", "updated_at"])
    out = out.merge(review_state, on="normalized_ticker", how="left")
    out["review_state"] = out["review_state"].fillna("active")
    out["review_note"] = out["review_note"].fillna("")

    out["recommendation_reason"] = out.apply(
        lambda row: (
            "already in active theme coverage"
            if bool(row["is_governed"])
            else (
                "uncovered + high persistence + recent streak"
                if str(row["recommendation"]) == "high-persistence uncovered"
                else (
                    "uncovered + recurring recent observations"
                    if str(row["recommendation"]) == "review for addition"
                    else "uncovered + low persistence, monitor"
                )
            )
        ),
        axis=1,
    )
    out["metadata_basis"] = out.apply(
        lambda row: ", ".join(
            [
                label
                for label in (
                    [
                        {
                            "file_column": "scanner from file column",
                            "filename_parse": "scanner from filename parse",
                            "default_source_label_fallback": "scanner from default source label fallback",
                        }.get(value, str(value))
                        for value in row["scanner_name_basis_values"]
                    ]
                    + [
                        {
                            "file_column": "date from file column",
                            "filename_parse": "date from filename parse",
                            "modified_timestamp_fallback": "date from modified timestamp fallback",
                        }.get(value, str(value))
                        for value in row["observed_date_basis_values"]
                    ]
                )
                if label
            ]
        )
        or "explicit export metadata",
        axis=1,
    )
    return (
        out.rename(columns={"normalized_ticker": "ticker"})
        .sort_values(
            ["is_governed", "persistence_score", "observations_last_10d", "current_streak", "ticker"],
            ascending=[True, False, False, False, True],
        )
        .reset_index(drop=True)
    )


def set_scanner_candidate_review_state(
    conn,
    ticker: str,
    review_state: str,
    review_note: str = "",
) -> dict[str, object]:
    normalized = normalize_ticker(ticker)
    if not normalized:
        raise ValueError("Ticker cannot be blank.")
    state = str(review_state or "active").strip().lower()
    if state not in {"active", "ignored", "reviewed"}:
        raise ValueError(f"Invalid scanner candidate review state: {review_state}")
    conn.execute(
        """
        INSERT INTO scanner_candidate_review_state(normalized_ticker, review_state, review_note, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(normalized_ticker) DO UPDATE
        SET review_state = excluded.review_state,
            review_note = excluded.review_note,
            updated_at = excluded.updated_at
        """,
        [normalized, state, review_note.strip(), datetime.now(UTC).replace(tzinfo=None)],
    )
    return {"ticker": normalized, "review_state": state, "review_note": review_note.strip()}


def _scanner_audit_priority(recommendation: str) -> str:
    return "high" if str(recommendation or "").strip().lower() == "high-persistence uncovered" else "medium"


def _scanner_audit_rationale(row: pd.Series) -> str:
    return (
        f"Scanner Audit evidence: recommendation={row['recommendation']}; "
        f"reason={row['recommendation_reason']}; "
        f"persistence_score={int(row['persistence_score'])}; "
        f"observed_days={int(row['observed_days'])}; "
        f"last_5={int(row['observations_last_5d'])}; "
        f"last_10={int(row['observations_last_10d'])}; "
        f"streak={int(row['current_streak'])}; "
        f"distinct_scanners={int(row['distinct_scanner_count'])}; "
        f"scanners={row['scanners']}."
    )


def _parse_source_context(raw_value: object) -> dict[str, object]:
    text = str(raw_value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _merge_priority(existing_priority: object, next_priority: str) -> str:
    current = str(existing_priority or "medium").strip().lower()
    ranking = {"low": 0, "medium": 1, "high": 2}
    return current if ranking.get(current, 1) >= ranking.get(next_priority, 1) else next_priority


def _normalize_theme_selection_ids(values: list[object] | None) -> list[int]:
    out: list[int] = []
    for value in values or []:
        try:
            normalized = int(value)
        except Exception:
            continue
        if normalized not in out:
            out.append(normalized)
    return out


def _resolve_theme_entries(conn, theme_ids: list[int]) -> list[dict[str, object]]:
    if not theme_ids:
        return []
    placeholders = ", ".join(["?"] * len(theme_ids))
    rows = conn.execute(
        f"""
        SELECT id AS theme_id, name AS theme_name, category
        FROM themes
        WHERE id IN ({placeholders})
        ORDER BY name
        """,
        theme_ids,
    ).df()
    if rows.empty:
        return []
    return [
        {
            "theme_id": int(row["theme_id"]),
            "theme_name": str(row["theme_name"]),
            "category": str(row["category"] or "Uncategorized"),
        }
        for _, row in rows.iterrows()
    ]


def _normalize_new_theme_labels(values: list[str] | None) -> list[str]:
    out: list[str] = []
    for value in values or []:
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in out:
            out.append(cleaned)
    return out


def _joined_new_theme_labels(values: list[str] | None) -> str | None:
    labels = _normalize_new_theme_labels(values)
    return ", ".join(labels) if labels else None


def _normalize_new_theme_category(value: object) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def promote_scanner_candidate_to_theme_review(
    conn,
    ticker: str,
    promotion_note: str = "",
    research_draft: dict[str, object] | None = None,
    selected_suggested_theme_ids: list[object] | None = None,
    custom_existing_theme_ids: list[object] | None = None,
    custom_new_themes: list[str] | None = None,
    proposed_new_theme_category: str | None = None,
) -> dict[str, object]:
    normalized = normalize_ticker(ticker)
    if not normalized:
        raise ValueError("Ticker cannot be blank.")

    candidates = scanner_candidate_summary(conn)
    if candidates.empty:
        raise ValueError("No Scanner Audit candidates are available.")

    matches = candidates[candidates["ticker"] == normalized]
    if matches.empty:
        raise ValueError(f"Scanner Audit candidate not found for {normalized}.")

    row = matches.iloc[0]
    promoted_at = datetime.now(UTC).replace(tzinfo=None)
    next_priority = _scanner_audit_priority(str(row["recommendation"]))
    existing = conn.execute(
        """
        SELECT suggestion_id, source, status, rationale, reviewer_notes, priority, source_context_json
        FROM theme_suggestions
        WHERE suggestion_type = 'review_theme'
          AND upper(COALESCE(proposed_ticker, '')) = ?
          AND status = 'pending'
        ORDER BY suggestion_id DESC
        LIMIT 1
        """,
        [normalized],
    ).fetchone()

    existing_context = _parse_source_context(existing[6]) if existing else {}
    previous_note = str(existing_context.get("promotion_note") or "").strip()
    note_text = promotion_note.strip() or previous_note
    evidence = {
        "ticker": normalized,
        "candidate_source": "scanner_audit",
        "source_labels": str(row["source_labels"] or ""),
        "recommendation": str(row["recommendation"] or ""),
        "recommendation_reason": str(row["recommendation_reason"] or ""),
        "persistence_score": int(row["persistence_score"]),
        "observed_days": int(row["observed_days"]),
        "observations_last_5d": int(row["observations_last_5d"]),
        "observations_last_10d": int(row["observations_last_10d"]),
        "current_streak": int(row["current_streak"]),
        "distinct_scanner_count": int(row["distinct_scanner_count"]),
        "first_seen": str(pd.to_datetime(row["first_seen"]).date()),
        "last_seen": str(pd.to_datetime(row["last_seen"]).date()),
        "scanners": str(row["scanners"] or ""),
        "scanner_summary": str(row["scanners"] or ""),
        "metadata_basis": str(row["metadata_basis"] or ""),
        "promoted_at": promoted_at.isoformat(sep=" "),
    }
    suggested_ids = _normalize_theme_selection_ids(selected_suggested_theme_ids)
    custom_existing_ids = _normalize_theme_selection_ids(custom_existing_theme_ids)
    suggested_theme_entries: list[dict[str, object]] = []
    if research_draft:
        for item in research_draft.get("suggested_existing_themes") or []:
            try:
                theme_id = int(item.get("theme_id"))
            except Exception:
                continue
            if theme_id in suggested_ids:
                suggested_theme_entries.append(
                    {
                        "theme_id": theme_id,
                        "theme_name": str(item.get("theme_name") or ""),
                        "category": str(item.get("category") or ""),
                        "why_it_might_fit": str(item.get("why_it_might_fit") or ""),
                    }
                )
    custom_existing_entries = _resolve_theme_entries(conn, custom_existing_ids)
    custom_new_theme_labels = _normalize_new_theme_labels(custom_new_themes)
    proposed_new_theme_text = _joined_new_theme_labels(custom_new_theme_labels)
    proposed_new_theme_category_value = _normalize_new_theme_category(proposed_new_theme_category)
    context = dict(existing_context)
    context.update(
        {
            "candidate_source": "scanner_audit",
            "promotion_note": note_text,
            "promoted_at": promoted_at.isoformat(sep=" "),
            "scanner_audit_evidence": evidence,
            "selected_suggested_themes": suggested_theme_entries,
            "custom_existing_themes": custom_existing_entries,
            "custom_new_themes": custom_new_theme_labels,
            "proposed_new_theme_category": proposed_new_theme_category_value,
        }
    )
    if research_draft:
        context["research_draft"] = research_draft
    context_json = json.dumps(context, sort_keys=True)
    rationale = _scanner_audit_rationale(row)
    if suggested_theme_entries or custom_existing_entries or custom_new_theme_labels:
        selected_names = [item["theme_name"] for item in suggested_theme_entries if item.get("theme_name")]
        custom_existing_names = [item["theme_name"] for item in custom_existing_entries if item.get("theme_name")]
        selection_summary = []
        if selected_names:
            selection_summary.append("selected themes=" + ", ".join(selected_names))
        if custom_existing_names:
            selection_summary.append("custom existing themes=" + ", ".join(custom_existing_names))
        if custom_new_theme_labels:
            selection_summary.append("custom new themes=" + ", ".join(custom_new_theme_labels))
        if proposed_new_theme_category_value:
            selection_summary.append("proposed category=" + proposed_new_theme_category_value)
        rationale = rationale + " Review selections: " + " | ".join(selection_summary) + "."

    if existing is None:
        from .suggestions_service import SuggestionPayload, create_suggestion

        suggestion_id = create_suggestion(
            conn,
            SuggestionPayload(
                suggestion_type="review_theme",
                source="scanner_audit",
                priority=next_priority,
                rationale=rationale,
                proposed_ticker=normalized,
                proposed_theme_name=proposed_new_theme_text,
                proposed_theme_category=proposed_new_theme_category_value,
            ),
        )
        conn.execute(
            """
            UPDATE theme_suggestions
            SET source_context_json = ?,
                source_updated_at = ?
            WHERE suggestion_id = ?
            """,
            [context_json, promoted_at, suggestion_id],
        )
        return {
            "action": "created",
            "suggestion_id": int(suggestion_id),
            "ticker": normalized,
            "message": f"Created new review candidate for {normalized}.",
        }

    suggestion_id = int(existing[0])
    existing_source = str(existing[1] or "").strip().lower()
    existing_rationale = str(existing[3] or "").strip()
    merged_priority = _merge_priority(existing[5], next_priority)
    updated_rationale = rationale if existing_source in {"scanner_audit", "imported"} or not existing_rationale else existing_rationale
    updated_source = "scanner_audit" if existing_source in {"", "scanner_audit", "imported"} else str(existing[1])
    conn.execute(
        """
        UPDATE theme_suggestions
        SET source = ?,
            rationale = ?,
            priority = ?,
            proposed_theme_name = ?,
            proposed_theme_category = ?,
            source_context_json = ?,
            source_updated_at = ?
        WHERE suggestion_id = ?
        """,
        [updated_source, updated_rationale, merged_priority, proposed_new_theme_text, proposed_new_theme_category_value, context_json, promoted_at, suggestion_id],
    )
    return {
        "action": "updated",
        "suggestion_id": suggestion_id,
        "ticker": normalized,
        "message": f"Updated existing review candidate for {normalized}.",
    }


def apply_scanner_candidate_selected_themes(
    conn,
    ticker: str,
    promotion_note: str = "",
    research_draft: dict[str, object] | None = None,
    selected_suggested_theme_ids: list[object] | None = None,
    custom_existing_theme_ids: list[object] | None = None,
    custom_new_themes: list[str] | None = None,
    proposed_new_theme_category: str | None = None,
) -> dict[str, object]:
    suggested_ids = _normalize_theme_selection_ids(selected_suggested_theme_ids)
    custom_existing_ids = _normalize_theme_selection_ids(custom_existing_theme_ids)
    if not suggested_ids and not custom_existing_ids:
        raise ValueError("Select at least one existing theme to apply now.")

    staged = promote_scanner_candidate_to_theme_review(
        conn,
        ticker,
        promotion_note,
        research_draft=research_draft,
        selected_suggested_theme_ids=suggested_ids,
        custom_existing_theme_ids=custom_existing_ids,
        custom_new_themes=custom_new_themes,
        proposed_new_theme_category=proposed_new_theme_category,
    )
    suggestion_id = int(staged["suggestion_id"])
    row = conn.execute(
        """
        SELECT status, source_context_json
        FROM theme_suggestions
        WHERE suggestion_id = ?
        """,
        [suggestion_id],
    ).fetchone()
    if row is None:
        raise ValueError("Direct-apply audit record was not found.")

    status = str(row[0] or "").strip().lower()
    context = {}
    try:
        parsed = json.loads(str(row[1] or "{}"))
        if isinstance(parsed, dict):
            context = parsed
    except Exception:
        context = {}

    selected_existing = list(context.get("selected_suggested_themes") or []) + list(context.get("custom_existing_themes") or [])
    selected_existing = [item for item in selected_existing if isinstance(item, dict) and item.get("theme_id")]
    custom_new_theme_labels = _normalize_new_theme_labels(list(context.get("custom_new_themes") or []))
    proposed_new_theme_category_value = _normalize_new_theme_category(context.get("proposed_new_theme_category"))
    if not selected_existing:
        raise ValueError("Direct apply requires at least one selected existing theme.")

    from .suggestions_service import apply_suggestion, review_suggestion

    audit_note = "Approved and applied directly from Scanner Audit."
    if status == "pending":
        review_suggestion(conn, suggestion_id, "approved", audit_note)
    elif status != "approved":
        raise ValueError(f"Scanner Audit direct apply expected a pending/approved review item, found `{status or 'unknown'}`.")

    apply_suggestion(conn, suggestion_id, audit_note)
    applied_theme_names = [
        str(item.get("theme_name") or "").strip()
        for item in selected_existing
        if str(item.get("theme_name") or "").strip()
    ]
    onboarding_row = conn.execute(
        """
        SELECT history_readiness_status, backfill_status, downstream_refresh_needed
        FROM governed_ticker_onboarding
        WHERE ticker = ?
        """,
        [normalize_ticker(ticker)],
    ).fetchone()
    onboarding_state = (
        {
            "history_readiness_status": str(onboarding_row[0] or ""),
            "backfill_status": str(onboarding_row[1] or ""),
            "downstream_refresh_needed": bool(onboarding_row[2]),
        }
        if onboarding_row
        else None
    )
    return {
        "action": "applied",
        "suggestion_id": suggestion_id,
        "ticker": normalize_ticker(ticker),
        "applied_theme_names": applied_theme_names,
        "proposed_new_theme_names": custom_new_theme_labels,
        "proposed_new_theme_category": proposed_new_theme_category_value,
        "onboarding_state": onboarding_state,
        "message": (
            f"Applied selected themes for {normalize_ticker(ticker)} and started onboarding."
        ),
    }


def reset_scanner_audit_data(conn) -> dict[str, object]:
    targets = [
        "scanner_hit_history",
        "scanner_import_runs",
        "scanner_candidate_review_state",
        "scanner_imported_files",
    ]
    existing = {
        str(row[0]).lower()
        for row in conn.execute(
            """
            SELECT table_name
            FROM duckdb_tables()
            WHERE lower(table_name) IN (?, ?, ?, ?)
            """,
            targets,
        ).fetchall()
    }
    cleared: dict[str, int] = {}
    for table_name in targets:
        if table_name.lower() not in existing:
            cleared[table_name] = 0
            continue
        count = int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
        conn.execute(f"DELETE FROM {table_name}")
        cleared[table_name] = count
    return {
        "tables_cleared": cleared,
        "total_rows_cleared": int(sum(cleared.values())),
        "message": (
            "Scanner Audit data reset complete. Cleared "
            + ", ".join(f"{name}={count}" for name, count in cleared.items())
            + "."
        ),
    }
