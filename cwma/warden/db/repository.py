from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from ..models import AUTH_ACCOUNT_COLUMNS


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_existing_state(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute("SELECT * FROM auth_accounts").fetchall()
    return {str(row["name"]): dict(row) for row in rows}


def start_scan_run(
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    *,
    now_iso: Callable[[], str] = utc_now_iso,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO scan_runs (
            mode, started_at, finished_at, status, total_files, filtered_files, probed_files,
            invalid_401_count, quota_limited_count, recovered_count, delete_401, quota_action,
            probe_workers, action_workers, timeout_seconds, retries
        ) VALUES (?, ?, NULL, 'running', 0, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings["mode"],
            now_iso(),
            int(bool(settings["delete_401"])),
            settings["quota_action"],
            settings["probe_workers"],
            settings["action_workers"],
            settings["timeout"],
            settings["retries"],
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_scan_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    total_files: int,
    filtered_files: int,
    probed_files: int,
    invalid_401_count: int,
    quota_limited_count: int,
    recovered_count: int,
    now_iso: Callable[[], str] = utc_now_iso,
) -> None:
    conn.execute(
        """
        UPDATE scan_runs
        SET finished_at = ?, status = ?, total_files = ?, filtered_files = ?, probed_files = ?,
            invalid_401_count = ?, quota_limited_count = ?, recovered_count = ?
        WHERE run_id = ?
        """,
        (
            now_iso(),
            status,
            int(total_files),
            int(filtered_files),
            int(probed_files),
            int(invalid_401_count),
            int(quota_limited_count),
            int(recovered_count),
            int(run_id),
        ),
    )
    conn.commit()


def upsert_auth_accounts(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    auth_account_columns: list[str] | None = None,
) -> None:
    if not rows:
        return
    normalized_columns = auth_account_columns or AUTH_ACCOUNT_COLUMNS
    columns_sql = ", ".join(normalized_columns)
    placeholders = ", ".join(f":{column}" for column in normalized_columns)
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in normalized_columns
        if column != "name"
    )
    conn.executemany(
        f"""
        INSERT INTO auth_accounts ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT(name) DO UPDATE SET
            {updates}
        """,
        rows,
    )
    conn.commit()


def mark_upload_skipped_remote_exists(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    file_path: str,
    file_size: int,
    now_iso: Callable[[], str] = utc_now_iso,
) -> None:
    current_time = now_iso()
    conn.execute(
        """
        INSERT INTO auth_file_uploads (
            base_url, file_name, content_sha256, file_path, file_size, status,
            attempt_count, first_seen_at, last_attempt_at, uploaded_at, last_http_status, last_error, last_response
        ) VALUES (?, ?, ?, ?, ?, 'skipped_remote_exists', 0, ?, ?, ?, NULL, NULL, NULL)
        ON CONFLICT(base_url, file_name, content_sha256) DO UPDATE SET
            file_path = excluded.file_path,
            file_size = excluded.file_size,
            status = 'skipped_remote_exists',
            last_attempt_at = excluded.last_attempt_at,
            uploaded_at = excluded.uploaded_at,
            last_http_status = NULL,
            last_error = NULL,
            last_response = NULL
        """,
        (
            base_url,
            file_name,
            content_sha256,
            file_path,
            int(file_size),
            current_time,
            current_time,
            current_time,
        ),
    )
    conn.commit()


def claim_upload_slot(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    file_path: str,
    file_size: int,
    now_iso: Callable[[], str] = utc_now_iso,
) -> str:
    current_time = now_iso()
    cur = conn.execute(
        """
        INSERT INTO auth_file_uploads (
            base_url, file_name, content_sha256, file_path, file_size, status,
            attempt_count, first_seen_at, last_attempt_at, uploaded_at, last_http_status, last_error, last_response
        ) VALUES (?, ?, ?, ?, ?, 'uploading', 0, ?, NULL, NULL, NULL, NULL, NULL)
        ON CONFLICT(base_url, file_name, content_sha256) DO NOTHING
        """,
        (base_url, file_name, content_sha256, file_path, int(file_size), current_time),
    )
    conn.commit()
    if cur.rowcount == 1:
        return "claimed_new"

    row = conn.execute(
        """
        SELECT status
        FROM auth_file_uploads
        WHERE base_url = ? AND file_name = ? AND content_sha256 = ?
        """,
        (base_url, file_name, content_sha256),
    ).fetchone()
    if row is None:
        return "claimed_new"

    status = str(row["status"] or "").strip()
    if status in {"success", "skipped_remote_exists"}:
        return "skipped_done"
    if status == "uploading":
        return "skipped_in_progress"
    if status == "failed":
        takeover = conn.execute(
            """
            UPDATE auth_file_uploads
            SET status = 'uploading',
                file_path = ?,
                file_size = ?,
                last_error = NULL,
                last_response = NULL,
                last_http_status = NULL
            WHERE base_url = ? AND file_name = ? AND content_sha256 = ? AND status = 'failed'
            """,
            (file_path, int(file_size), base_url, file_name, content_sha256),
        )
        conn.commit()
        if takeover.rowcount == 1:
            return "claimed_retry"
        return "skipped_in_progress"
    return "skipped_done"


def mark_upload_attempt(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    now_iso: Callable[[], str] = utc_now_iso,
) -> None:
    conn.execute(
        """
        UPDATE auth_file_uploads
        SET attempt_count = attempt_count + 1,
            last_attempt_at = ?,
            status = 'uploading'
        WHERE base_url = ? AND file_name = ? AND content_sha256 = ?
        """,
        (now_iso(), base_url, file_name, content_sha256),
    )
    conn.commit()


def mark_upload_success(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    http_status: int | None,
    response_text: str | None,
    compact_text: Callable[[Any, int], str | None],
    now_iso: Callable[[], str] = utc_now_iso,
) -> None:
    conn.execute(
        """
        UPDATE auth_file_uploads
        SET status = 'success',
            uploaded_at = ?,
            last_http_status = ?,
            last_error = NULL,
            last_response = ?
        WHERE base_url = ? AND file_name = ? AND content_sha256 = ?
        """,
        (
            now_iso(),
            http_status,
            compact_text(response_text, 2000),
            base_url,
            file_name,
            content_sha256,
        ),
    )
    conn.commit()


def mark_upload_failure(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    http_status: int | None,
    error_text: str,
    response_text: str | None,
    compact_text: Callable[[Any, int], str | None],
) -> None:
    conn.execute(
        """
        UPDATE auth_file_uploads
        SET status = 'failed',
            uploaded_at = NULL,
            last_http_status = ?,
            last_error = ?,
            last_response = ?
        WHERE base_url = ? AND file_name = ? AND content_sha256 = ?
        """,
        (
            http_status,
            compact_text(error_text, 600),
            compact_text(response_text, 2000),
            base_url,
            file_name,
            content_sha256,
        ),
    )
    conn.commit()
