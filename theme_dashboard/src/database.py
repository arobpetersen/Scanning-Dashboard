from __future__ import annotations

from contextlib import contextmanager

import duckdb

from .config import DB_PATH

SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS themes_id_seq;
CREATE SEQUENCE IF NOT EXISTS snapshots_id_seq;
CREATE SEQUENCE IF NOT EXISTS refresh_run_id_seq;
CREATE SEQUENCE IF NOT EXISTS suggestion_id_seq;
CREATE SEQUENCE IF NOT EXISTS historical_reconstruction_run_id_seq;
CREATE SEQUENCE IF NOT EXISTS scanner_import_run_id_seq;
CREATE SEQUENCE IF NOT EXISTS scanner_hit_id_seq;

CREATE TABLE IF NOT EXISTS themes (
    id BIGINT PRIMARY KEY DEFAULT nextval('themes_id_seq'),
    name VARCHAR NOT NULL UNIQUE,
    category VARCHAR NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS theme_membership (
    theme_id BIGINT NOT NULL,
    ticker VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(theme_id, ticker),
    CHECK (length(trim(ticker)) > 0)
);

CREATE TABLE IF NOT EXISTS refresh_runs (
    run_id BIGINT PRIMARY KEY DEFAULT nextval('refresh_run_id_seq'),
    provider VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR NOT NULL,
    ticker_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,
    scope_type VARCHAR,
    scope_theme_name VARCHAR,
    error_message VARCHAR,
    api_call_count BIGINT NOT NULL DEFAULT 0,
    api_endpoint_counts VARCHAR,
    skipped_tickers VARCHAR,
    failure_category_counts VARCHAR,
    flagged_symbol_count BIGINT NOT NULL DEFAULT 0,
    suppressed_symbol_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS refresh_run_tickers (
    run_id BIGINT NOT NULL,
    ticker VARCHAR NOT NULL,
    PRIMARY KEY(run_id, ticker)
);

CREATE TABLE IF NOT EXISTS ticker_snapshots (
    snapshot_id BIGINT PRIMARY KEY DEFAULT nextval('snapshots_id_seq'),
    run_id BIGINT NOT NULL,
    ticker VARCHAR NOT NULL,
    price DOUBLE,
    perf_1w DOUBLE,
    perf_1m DOUBLE,
    perf_3m DOUBLE,
    market_cap DOUBLE,
    avg_volume DOUBLE,
    short_interest_pct DOUBLE,
    float_shares DOUBLE,
    adr_pct DOUBLE,
    last_updated TIMESTAMP,
    snapshot_source VARCHAR NOT NULL DEFAULT 'live',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (length(trim(ticker)) > 0)
);

CREATE TABLE IF NOT EXISTS theme_snapshots (
    run_id BIGINT NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    theme_id BIGINT NOT NULL,
    ticker_count BIGINT NOT NULL,
    avg_1w DOUBLE,
    avg_1m DOUBLE,
    avg_3m DOUBLE,
    positive_1w_breadth_pct DOUBLE,
    positive_1m_breadth_pct DOUBLE,
    positive_3m_breadth_pct DOUBLE,
    composite_score DOUBLE,
    snapshot_source VARCHAR NOT NULL DEFAULT 'live',
    PRIMARY KEY (run_id, theme_id)
);

CREATE TABLE IF NOT EXISTS historical_reconstruction_runs (
    run_id BIGINT PRIMARY KEY DEFAULT nextval('historical_reconstruction_run_id_seq'),
    run_kind VARCHAR NOT NULL,
    provenance_class VARCHAR NOT NULL DEFAULT 'reconstructed',
    provenance_source_label VARCHAR NOT NULL,
    market_data_source VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR NOT NULL,
    start_date DATE,
    end_date DATE,
    target_tickers VARCHAR,
    target_theme_ids VARCHAR,
    ticker_count BIGINT NOT NULL DEFAULT 0,
    theme_count BIGINT NOT NULL DEFAULT 0,
    ticker_history_rows_written BIGINT NOT NULL DEFAULT 0,
    ticker_history_rows_skipped BIGINT NOT NULL DEFAULT 0,
    snapshot_rows_written BIGINT NOT NULL DEFAULT 0,
    snapshot_rows_skipped BIGINT NOT NULL DEFAULT 0,
    failed_tickers VARCHAR,
    error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS ticker_daily_history (
    run_id BIGINT,
    ticker VARCHAR NOT NULL,
    trading_date DATE NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    vwap DOUBLE,
    trade_count BIGINT,
    provenance_class VARCHAR NOT NULL DEFAULT 'reconstructed',
    provenance_source_label VARCHAR NOT NULL,
    market_data_source VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, trading_date, market_data_source, provenance_source_label),
    CHECK (length(trim(ticker)) > 0)
);

CREATE TABLE IF NOT EXISTS reconstructed_theme_snapshots (
    run_id BIGINT NOT NULL,
    snapshot_date DATE NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    theme_id BIGINT NOT NULL,
    ticker_count BIGINT NOT NULL,
    avg_1w DOUBLE,
    avg_1m DOUBLE,
    avg_3m DOUBLE,
    positive_1w_breadth_pct DOUBLE,
    positive_1m_breadth_pct DOUBLE,
    positive_3m_breadth_pct DOUBLE,
    composite_score DOUBLE,
    provenance_class VARCHAR NOT NULL DEFAULT 'reconstructed',
    provenance_source_label VARCHAR NOT NULL,
    market_data_source VARCHAR NOT NULL,
    membership_basis VARCHAR NOT NULL DEFAULT 'current_governed_membership',
    PRIMARY KEY (snapshot_date, theme_id, provenance_source_label)
);

CREATE TABLE IF NOT EXISTS theme_suggestions (
    suggestion_id BIGINT PRIMARY KEY DEFAULT nextval('suggestion_id_seq'),
    suggestion_type VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP,
    source VARCHAR NOT NULL,
    rationale VARCHAR,
    proposed_theme_name VARCHAR,
    proposed_ticker VARCHAR,
    existing_theme_id BIGINT,
    proposed_target_theme_id BIGINT,
    reviewer_notes VARCHAR,
    priority VARCHAR NOT NULL DEFAULT 'medium',
    CHECK (status IN ('pending','approved','rejected','applied','obsolete')),
    CHECK (suggestion_type IN (
        'add_ticker_to_theme',
        'remove_ticker_from_theme',
        'create_theme',
        'rename_theme',
        'move_ticker_between_themes',
        'review_theme'
    )),
    CHECK (source IN ('manual','rules_engine','ai_proposal','imported'))
);

CREATE TABLE IF NOT EXISTS scanner_import_runs (
    import_run_id BIGINT PRIMARY KEY DEFAULT nextval('scanner_import_run_id_seq'),
    import_source VARCHAR NOT NULL,
    folder_path VARCHAR NOT NULL,
    file_pattern VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR NOT NULL,
    files_seen BIGINT NOT NULL DEFAULT 0,
    files_processed BIGINT NOT NULL DEFAULT 0,
    files_skipped BIGINT NOT NULL DEFAULT 0,
    files_failed BIGINT NOT NULL DEFAULT 0,
    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_imported BIGINT NOT NULL DEFAULT 0,
    rows_skipped BIGINT NOT NULL DEFAULT 0,
    unique_tickers_observed BIGINT NOT NULL DEFAULT 0,
    notes VARCHAR,
    error_message VARCHAR,
    CHECK (status IN ('running','success','partial','no_files','failed'))
);

CREATE TABLE IF NOT EXISTS scanner_hit_history (
    hit_id BIGINT PRIMARY KEY DEFAULT nextval('scanner_hit_id_seq'),
    import_run_id BIGINT,
    import_source VARCHAR NOT NULL,
    normalized_ticker VARCHAR NOT NULL,
    raw_ticker VARCHAR,
    observed_date DATE NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    source_file VARCHAR NOT NULL,
    source_label VARCHAR NOT NULL,
    scanner_name VARCHAR NOT NULL,
    file_modified_at TIMESTAMP,
    scanner_name_inferred BOOLEAN NOT NULL DEFAULT FALSE,
    scanner_name_basis VARCHAR NOT NULL DEFAULT 'file_column',
    observed_date_inferred BOOLEAN NOT NULL DEFAULT FALSE,
    observed_date_basis VARCHAR NOT NULL DEFAULT 'file_column',
    row_hash VARCHAR NOT NULL UNIQUE,
    supporting_fields_json VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (length(trim(normalized_ticker)) > 0),
    CHECK (length(trim(source_file)) > 0),
    CHECK (length(trim(scanner_name)) > 0),
    CHECK (scanner_name_basis IN ('file_column','filename_parse','default_source_label_fallback')),
    CHECK (observed_date_basis IN ('file_column','filename_parse','modified_timestamp_fallback'))
);

CREATE TABLE IF NOT EXISTS scanner_imported_files (
    file_fingerprint VARCHAR PRIMARY KEY,
    source_file VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    file_size BIGINT,
    modified_at TIMESTAMP,
    first_import_run_id BIGINT,
    last_seen_run_id BIGINT,
    import_status VARCHAR NOT NULL DEFAULT 'success',
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (length(trim(file_fingerprint)) > 0),
    CHECK (length(trim(source_file)) > 0),
    CHECK (length(trim(file_name)) > 0),
    CHECK (import_status IN ('success','failed'))
);

CREATE TABLE IF NOT EXISTS scanner_candidate_review_state (
    normalized_ticker VARCHAR PRIMARY KEY,
    review_state VARCHAR NOT NULL DEFAULT 'active',
    review_note VARCHAR,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (length(trim(normalized_ticker)) > 0),
    CHECK (review_state IN ('active','ignored','reviewed'))
);

CREATE TABLE IF NOT EXISTS symbol_refresh_status (
    ticker VARCHAR PRIMARY KEY,
    status VARCHAR NOT NULL DEFAULT 'active',
    suggested_status VARCHAR,
    suggested_reason VARCHAR,
    suppression_reason VARCHAR,
    last_failure_category VARCHAR,
    consecutive_failure_count BIGINT NOT NULL DEFAULT 0,
    rolling_failure_count BIGINT NOT NULL DEFAULT 0,
    last_failure_at TIMESTAMP,
    last_success_at TIMESTAMP,
    last_run_id BIGINT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (status IN ('active','watch','refresh_suppressed','inactive_candidate'))
);

CREATE TABLE IF NOT EXISTS refresh_failures (
    run_id BIGINT NOT NULL,
    ticker VARCHAR,
    error_message VARCHAR NOT NULL,
    failure_category VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_theme_membership_theme_id ON theme_membership(theme_id);
CREATE INDEX IF NOT EXISTS idx_theme_membership_ticker ON theme_membership(ticker);
CREATE INDEX IF NOT EXISTS idx_snapshots_run_id ON ticker_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_ticker ON ticker_snapshots(ticker);
CREATE INDEX IF NOT EXISTS idx_theme_snapshots_run_id ON theme_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_theme_snapshots_theme_id ON theme_snapshots(theme_id);
CREATE INDEX IF NOT EXISTS idx_reconstructed_theme_snapshots_theme_id ON reconstructed_theme_snapshots(theme_id);
CREATE INDEX IF NOT EXISTS idx_reconstructed_theme_snapshots_date ON reconstructed_theme_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_historical_reconstruction_runs_status ON historical_reconstruction_runs(status);
CREATE INDEX IF NOT EXISTS idx_ticker_daily_history_ticker ON ticker_daily_history(ticker);
CREATE INDEX IF NOT EXISTS idx_ticker_daily_history_date ON ticker_daily_history(trading_date);
CREATE INDEX IF NOT EXISTS idx_refresh_failures_run_id ON refresh_failures(run_id);
CREATE INDEX IF NOT EXISTS idx_refresh_failures_category ON refresh_failures(failure_category);
CREATE INDEX IF NOT EXISTS idx_refresh_run_tickers_run_id ON refresh_run_tickers(run_id);
CREATE INDEX IF NOT EXISTS idx_symbol_refresh_status_status ON symbol_refresh_status(status);
CREATE INDEX IF NOT EXISTS idx_theme_suggestions_status ON theme_suggestions(status);
CREATE INDEX IF NOT EXISTS idx_theme_suggestions_type ON theme_suggestions(suggestion_type);
CREATE INDEX IF NOT EXISTS idx_scanner_import_runs_status ON scanner_import_runs(status);
CREATE INDEX IF NOT EXISTS idx_scanner_imported_files_status ON scanner_imported_files(import_status);
CREATE INDEX IF NOT EXISTS idx_scanner_hit_history_ticker ON scanner_hit_history(normalized_ticker);
CREATE INDEX IF NOT EXISTS idx_scanner_hit_history_observed_date ON scanner_hit_history(observed_date);
CREATE INDEX IF NOT EXISTS idx_scanner_hit_history_scanner_name ON scanner_hit_history(scanner_name);
CREATE INDEX IF NOT EXISTS idx_scanner_candidate_review_state_state ON scanner_candidate_review_state(review_state);
"""


@contextmanager
def get_conn():
    conn = duckdb.connect(str(DB_PATH))
    try:
        yield conn
    finally:
        conn.close()


def _rebuild_theme_suggestions(conn) -> None:
    conn.execute(
        """
        CREATE TABLE theme_suggestions_migrated (
            suggestion_id BIGINT PRIMARY KEY DEFAULT nextval('suggestion_id_seq'),
            suggestion_type VARCHAR NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP,
            source VARCHAR NOT NULL,
            rationale VARCHAR,
            proposed_theme_name VARCHAR,
            proposed_ticker VARCHAR,
            existing_theme_id BIGINT,
            proposed_target_theme_id BIGINT,
            reviewer_notes VARCHAR,
            priority VARCHAR NOT NULL DEFAULT 'medium',
            CHECK (status IN ('pending','approved','rejected','applied','obsolete')),
            CHECK (suggestion_type IN (
                'add_ticker_to_theme',
                'remove_ticker_from_theme',
                'create_theme',
                'rename_theme',
                'move_ticker_between_themes',
                'review_theme'
            )),
            CHECK (source IN ('manual','rules_engine','ai_proposal','imported'))
        )
        """
    )
    conn.execute(
        """
        INSERT INTO theme_suggestions_migrated(
            suggestion_id, suggestion_type, status, created_at, reviewed_at, source,
            rationale, proposed_theme_name, proposed_ticker, existing_theme_id,
            proposed_target_theme_id, reviewer_notes, priority
        )
        SELECT suggestion_id, suggestion_type, status, created_at, reviewed_at, source,
               rationale, proposed_theme_name, proposed_ticker, existing_theme_id,
               proposed_target_theme_id, reviewer_notes, COALESCE(priority, 'medium')
        FROM theme_suggestions
        """
    )
    conn.execute("DROP TABLE theme_suggestions")
    conn.execute("ALTER TABLE theme_suggestions_migrated RENAME TO theme_suggestions")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_theme_suggestions_status ON theme_suggestions(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_theme_suggestions_type ON theme_suggestions(suggestion_type)")


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(SCHEMA_SQL)
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS scope_type VARCHAR")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS scope_theme_name VARCHAR")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS api_call_count BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS api_endpoint_counts VARCHAR")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS skipped_tickers VARCHAR")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS failure_category_counts VARCHAR")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS flagged_symbol_count BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS suppressed_symbol_count BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE refresh_failures ADD COLUMN IF NOT EXISTS failure_category VARCHAR")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS suggested_status VARCHAR")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS suggested_reason VARCHAR")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS suppression_reason VARCHAR")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS last_failure_category VARCHAR")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS consecutive_failure_count BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS rolling_failure_count BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS last_failure_at TIMESTAMP")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMP")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS last_run_id BIGINT")
        conn.execute("ALTER TABLE symbol_refresh_status ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
        conn.execute("UPDATE symbol_refresh_status SET updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)")
        conn.execute("ALTER TABLE theme_suggestions ADD COLUMN IF NOT EXISTS priority VARCHAR DEFAULT 'medium'")
        conn.execute("ALTER TABLE ticker_snapshots ADD COLUMN IF NOT EXISTS snapshot_source VARCHAR DEFAULT 'live'")
        conn.execute("ALTER TABLE theme_snapshots ADD COLUMN IF NOT EXISTS snapshot_source VARCHAR DEFAULT 'live'")
        conn.execute("ALTER TABLE historical_reconstruction_runs ADD COLUMN IF NOT EXISTS ticker_history_rows_written BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE historical_reconstruction_runs ADD COLUMN IF NOT EXISTS ticker_history_rows_skipped BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE scanner_import_runs ADD COLUMN IF NOT EXISTS files_failed BIGINT DEFAULT 0")
        conn.execute("ALTER TABLE scanner_hit_history ADD COLUMN IF NOT EXISTS scanner_name_inferred BOOLEAN DEFAULT FALSE")
        conn.execute("ALTER TABLE scanner_hit_history ADD COLUMN IF NOT EXISTS scanner_name_basis VARCHAR DEFAULT 'file_column'")
        conn.execute("ALTER TABLE scanner_hit_history ADD COLUMN IF NOT EXISTS observed_date_inferred BOOLEAN DEFAULT FALSE")
        conn.execute("ALTER TABLE scanner_hit_history ADD COLUMN IF NOT EXISTS observed_date_basis VARCHAR DEFAULT 'file_column'")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scanner_imported_files (
                file_fingerprint VARCHAR PRIMARY KEY,
                source_file VARCHAR NOT NULL,
                file_name VARCHAR NOT NULL,
                file_size BIGINT,
                modified_at TIMESTAMP,
                first_import_run_id BIGINT,
                last_seen_run_id BIGINT,
                import_status VARCHAR NOT NULL DEFAULT 'success',
                processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CHECK (length(trim(file_fingerprint)) > 0),
                CHECK (length(trim(source_file)) > 0),
                CHECK (length(trim(file_name)) > 0),
                CHECK (import_status IN ('success','failed'))
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scanner_imported_files_status ON scanner_imported_files(import_status)")
        conn.execute("UPDATE ticker_snapshots ts SET snapshot_source = COALESCE((SELECT rr.provider FROM refresh_runs rr WHERE rr.run_id = ts.run_id), 'live') WHERE snapshot_source IS NULL OR trim(snapshot_source)=''")
        conn.execute("UPDATE theme_snapshots ts SET snapshot_source = COALESCE((SELECT rr.provider FROM refresh_runs rr WHERE rr.run_id = ts.run_id), 'live') WHERE snapshot_source IS NULL OR trim(snapshot_source)=''")
        conn.execute("UPDATE theme_suggestions SET priority='medium' WHERE priority IS NULL OR trim(priority)=''")
        conn.execute("UPDATE scanner_hit_history SET scanner_name_inferred = COALESCE(scanner_name_inferred, FALSE)")
        conn.execute("UPDATE scanner_hit_history SET scanner_name_basis = COALESCE(NULLIF(trim(scanner_name_basis), ''), CASE WHEN COALESCE(scanner_name_inferred, FALSE) THEN 'filename_parse' ELSE 'file_column' END)")
        conn.execute("UPDATE scanner_hit_history SET observed_date_inferred = COALESCE(observed_date_inferred, FALSE)")
        conn.execute("UPDATE scanner_hit_history SET observed_date_basis = COALESCE(NULLIF(trim(observed_date_basis), ''), CASE WHEN COALESCE(observed_date_inferred, FALSE) THEN 'modified_timestamp_fallback' ELSE 'file_column' END)")

        ddl = conn.execute("SELECT sql FROM duckdb_tables() WHERE table_name='theme_suggestions' LIMIT 1").fetchone()
        ddl_text = ddl[0].lower() if ddl and ddl[0] else ""
        needs_rebuild = any(token not in ddl_text for token in ["review_theme", "obsolete", "priority"])
        if needs_rebuild:
            _rebuild_theme_suggestions(conn)

        from .theme_service import seed_if_needed

        seed_if_needed(conn)
