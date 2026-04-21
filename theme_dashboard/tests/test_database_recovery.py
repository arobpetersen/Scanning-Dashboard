from pathlib import Path
import shutil
import uuid
from unittest.mock import patch

import duckdb

import src.database as database


class _FakeConn:
    def execute(self, sql):
        return [(1,)]


def test_connect_with_retry_quarantines_unreplayable_wal_and_retries():
    tmp_path = Path(".tmp") / f"codex_db_recovery_{uuid.uuid4().hex[:8]}"
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "test.duckdb"
    wal_path = tmp_path / "test.duckdb.wal"
    db_path.write_bytes(b"db")
    wal_path.write_bytes(b"wal")

    replay_exc = duckdb.InternalException(
        'INTERNAL Error: Failure while replaying WAL file "x": Calling DatabaseManager::GetDefaultDatabase with no default database set'
    )

    database._LAST_RECOVERY_NOTE = None
    try:
        with patch("src.database.duckdb.connect", side_effect=[replay_exc, _FakeConn()]):
            conn = database._connect_with_retry(str(db_path))

        assert isinstance(conn, _FakeConn)
        assert not wal_path.exists()
        backups = list(tmp_path.glob("test.duckdb.wal.unreplayable_*.bak"))
        assert len(backups) == 1
        assert database.latest_database_recovery_note() is not None
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)
