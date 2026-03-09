from __future__ import annotations

from contextlib import contextmanager

import duckdb

from .config import DB_PATH

SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS themes_id_seq;
CREATE SEQUENCE IF NOT EXISTS snapshots_id_seq;
CREATE SEQUENCE IF NOT EXISTS refresh_run_id_seq;
CREATE SEQUENCE IF NOT EXISTS suggestion_id_seq;

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
    error_message VARCHAR
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
    PRIMARY KEY (run_id, theme_id)
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
    CHECK (status IN ('pending','approved','rejected','applied')),
    CHECK (suggestion_type IN (
        'add_ticker_to_theme',
        'remove_ticker_from_theme',
        'create_theme',
        'rename_theme',
        'move_ticker_between_themes'
    )),
    CHECK (source IN ('manual','rules_engine','ai_proposal','imported'))
);

CREATE TABLE IF NOT EXISTS refresh_failures (
    run_id BIGINT NOT NULL,
    ticker VARCHAR,
    error_message VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_theme_membership_theme_id ON theme_membership(theme_id);
CREATE INDEX IF NOT EXISTS idx_theme_membership_ticker ON theme_membership(ticker);
CREATE INDEX IF NOT EXISTS idx_snapshots_run_id ON ticker_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_ticker ON ticker_snapshots(ticker);
CREATE INDEX IF NOT EXISTS idx_theme_snapshots_run_id ON theme_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_theme_snapshots_theme_id ON theme_snapshots(theme_id);
CREATE INDEX IF NOT EXISTS idx_refresh_failures_run_id ON refresh_failures(run_id);
CREATE INDEX IF NOT EXISTS idx_refresh_run_tickers_run_id ON refresh_run_tickers(run_id);
CREATE INDEX IF NOT EXISTS idx_theme_suggestions_status ON theme_suggestions(status);
CREATE INDEX IF NOT EXISTS idx_theme_suggestions_type ON theme_suggestions(suggestion_type);
"""


@contextmanager
def get_conn():
    conn = duckdb.connect(str(DB_PATH))
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(SCHEMA_SQL)
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS scope_type VARCHAR")
        conn.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS scope_theme_name VARCHAR")
