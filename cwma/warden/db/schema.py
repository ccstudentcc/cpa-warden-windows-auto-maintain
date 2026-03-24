from __future__ import annotations

import sqlite3


AUTH_ACCOUNTS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS auth_accounts (
    name TEXT PRIMARY KEY,
    disabled INTEGER NOT NULL,
    id_token_json TEXT,
    email TEXT,
    provider TEXT,
    source TEXT,
    unavailable INTEGER NOT NULL,
    auth_index TEXT,
    account TEXT,
    type TEXT,
    runtime_only INTEGER NOT NULL,
    status TEXT,
    status_message TEXT,
    chatgpt_account_id TEXT,
    id_token_plan_type TEXT,
    auth_updated_at TEXT,
    auth_modtime TEXT,
    auth_last_refresh TEXT,
    api_http_status INTEGER,
    api_status_code INTEGER,
    usage_allowed INTEGER,
    usage_limit_reached INTEGER,
    usage_plan_type TEXT,
    usage_email TEXT,
    usage_reset_at INTEGER,
    usage_reset_after_seconds INTEGER,
    usage_spark_allowed INTEGER,
    usage_spark_limit_reached INTEGER,
    usage_spark_reset_at INTEGER,
    usage_spark_reset_after_seconds INTEGER,
    quota_signal_source TEXT,
    is_invalid_401 INTEGER NOT NULL DEFAULT 0,
    is_quota_limited INTEGER NOT NULL DEFAULT 0,
    is_recovered INTEGER NOT NULL DEFAULT 0,
    probe_error_kind TEXT,
    probe_error_text TEXT,
    managed_reason TEXT,
    last_action TEXT,
    last_action_status TEXT,
    last_action_error TEXT,
    last_seen_at TEXT NOT NULL,
    last_probed_at TEXT,
    updated_at TEXT NOT NULL
);
"""


SCAN_RUNS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scan_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    total_files INTEGER NOT NULL,
    filtered_files INTEGER NOT NULL,
    probed_files INTEGER NOT NULL,
    invalid_401_count INTEGER NOT NULL,
    quota_limited_count INTEGER NOT NULL,
    recovered_count INTEGER NOT NULL,
    delete_401 INTEGER NOT NULL,
    quota_action TEXT NOT NULL,
    probe_workers INTEGER NOT NULL,
    action_workers INTEGER NOT NULL,
    timeout_seconds INTEGER NOT NULL,
    retries INTEGER NOT NULL
);
"""


UPLOAD_TABLE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS auth_file_uploads (
    base_url TEXT NOT NULL,
    file_name TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT NOT NULL,
    last_attempt_at TEXT,
    uploaded_at TEXT,
    last_http_status INTEGER,
    last_error TEXT,
    last_response TEXT,
    PRIMARY KEY (base_url, file_name, content_sha256)
);

CREATE INDEX IF NOT EXISTS idx_auth_file_uploads_status
    ON auth_file_uploads(status);

CREATE INDEX IF NOT EXISTS idx_auth_file_uploads_file_name
    ON auth_file_uploads(file_name);
"""


def ensure_auth_accounts_schema(
    conn: sqlite3.Connection,
    *,
    required_columns: dict[str, str] | None = None,
) -> None:
    normalized_required_columns = required_columns or {
        "usage_spark_allowed": "INTEGER",
        "usage_spark_limit_reached": "INTEGER",
        "usage_spark_reset_at": "INTEGER",
        "usage_spark_reset_after_seconds": "INTEGER",
        "quota_signal_source": "TEXT",
    }
    rows = conn.execute("PRAGMA table_info(auth_accounts)").fetchall()
    existing_columns = {str(row[1]) for row in rows}
    added = False
    for column, column_type in normalized_required_columns.items():
        if column in existing_columns:
            continue
        conn.execute(f"ALTER TABLE auth_accounts ADD COLUMN {column} {column_type}")
        added = True
    if added:
        conn.commit()


def init_upload_db(conn: sqlite3.Connection) -> None:
    conn.executescript(UPLOAD_TABLE_SCHEMA_SQL)
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(AUTH_ACCOUNTS_SCHEMA_SQL + SCAN_RUNS_SCHEMA_SQL)
    ensure_auth_accounts_schema(conn)
    conn.commit()
    init_upload_db(conn)
