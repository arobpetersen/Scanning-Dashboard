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
                    }
                )
                continue

            parsed, meta = parse_tc2000_export_file(path, imported_at=started_at, default_source_label=source_label)
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
