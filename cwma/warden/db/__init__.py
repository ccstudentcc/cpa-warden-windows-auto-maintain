"""DB boundary helpers for CPA warden."""

from .repository import (
    claim_upload_slot,
    connect_db,
    finish_scan_run,
    load_existing_state,
    mark_upload_attempt,
    mark_upload_failure,
    mark_upload_skipped_remote_exists,
    mark_upload_success,
    start_scan_run,
    upsert_auth_accounts,
)
from .schema import ensure_auth_accounts_schema, init_db, init_upload_db

__all__ = [
    "claim_upload_slot",
    "connect_db",
    "ensure_auth_accounts_schema",
    "finish_scan_run",
    "init_db",
    "init_upload_db",
    "load_existing_state",
    "mark_upload_attempt",
    "mark_upload_failure",
    "mark_upload_skipped_remote_exists",
    "mark_upload_success",
    "start_scan_run",
    "upsert_auth_accounts",
]
