from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from cwma.warden.db.repository import (
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
from cwma.warden.db.schema import init_db
from cwma.warden.models import AUTH_ACCOUNT_COLUMNS
from tests.temp_sandbox import TempSandboxState, setup_tempfile_sandbox, teardown_tempfile_sandbox

_TEMP_SANDBOX_STATE: TempSandboxState | None = None


def setUpModule() -> None:
    global _TEMP_SANDBOX_STATE
    _TEMP_SANDBOX_STATE = setup_tempfile_sandbox()


def tearDownModule() -> None:
    teardown_tempfile_sandbox(_TEMP_SANDBOX_STATE)


class WardenDbRepositoryModuleTests(unittest.TestCase):
    def test_connect_db_uses_row_factory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.sqlite3"
            conn = connect_db(str(db_path))
            try:
                self.assertIs(conn.row_factory, sqlite3.Row)
            finally:
                conn.close()

    def test_upsert_and_load_existing_state(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            init_db(conn)
            rows = [
                {
                    "name": "a.json",
                    "disabled": 0,
                    "id_token_json": None,
                    "email": "a@example.com",
                    "provider": "openai",
                    "source": "local",
                    "unavailable": 0,
                    "auth_index": "idx-1",
                    "account": "a@example.com",
                    "type": "codex",
                    "runtime_only": 0,
                    "status": None,
                    "status_message": None,
                    "chatgpt_account_id": None,
                    "id_token_plan_type": None,
                    "auth_updated_at": None,
                    "auth_modtime": None,
                    "auth_last_refresh": None,
                    "api_http_status": None,
                    "api_status_code": None,
                    "usage_allowed": None,
                    "usage_limit_reached": None,
                    "usage_plan_type": None,
                    "usage_email": None,
                    "usage_reset_at": None,
                    "usage_reset_after_seconds": None,
                    "usage_spark_allowed": None,
                    "usage_spark_limit_reached": None,
                    "usage_spark_reset_at": None,
                    "usage_spark_reset_after_seconds": None,
                    "quota_signal_source": None,
                    "is_invalid_401": 0,
                    "is_quota_limited": 0,
                    "is_recovered": 0,
                    "probe_error_kind": None,
                    "probe_error_text": None,
                    "managed_reason": None,
                    "last_action": None,
                    "last_action_status": None,
                    "last_action_error": None,
                    "last_seen_at": "2026-03-24T00:00:00+00:00",
                    "last_probed_at": None,
                    "updated_at": "2026-03-24T00:00:00+00:00",
                }
            ]
            upsert_auth_accounts(conn, rows, auth_account_columns=AUTH_ACCOUNT_COLUMNS)
            state = load_existing_state(conn)
            self.assertIn("a.json", state)
            self.assertEqual(state["a.json"]["provider"], "openai")
        finally:
            conn.close()

    def test_start_and_finish_scan_run(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            init_db(conn)
            settings = {
                "mode": "scan",
                "delete_401": True,
                "quota_action": "disable",
                "probe_workers": 10,
                "action_workers": 20,
                "timeout": 15,
                "retries": 3,
            }
            run_id = start_scan_run(conn, settings, now_iso=lambda: "2026-03-24T00:00:00+00:00")
            finish_scan_run(
                conn,
                run_id,
                status="success",
                total_files=7,
                filtered_files=6,
                probed_files=5,
                invalid_401_count=1,
                quota_limited_count=2,
                recovered_count=1,
                now_iso=lambda: "2026-03-24T01:00:00+00:00",
            )
            row = conn.execute("SELECT * FROM scan_runs WHERE run_id = ?", (run_id,)).fetchone()
            self.assertEqual(str(row["status"]), "success")
            self.assertEqual(int(row["total_files"]), 7)
            self.assertEqual(str(row["finished_at"]), "2026-03-24T01:00:00+00:00")
        finally:
            conn.close()

    def test_upload_slot_and_status_lifecycle(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            init_db(conn)
            slot = claim_upload_slot(
                conn,
                base_url="https://x.example",
                file_name="a.json",
                content_sha256="sha-1",
                file_path="C:/a.json",
                file_size=12,
                now_iso=lambda: "2026-03-24T00:00:00+00:00",
            )
            self.assertEqual(slot, "claimed_new")
            mark_upload_attempt(
                conn,
                base_url="https://x.example",
                file_name="a.json",
                content_sha256="sha-1",
                now_iso=lambda: "2026-03-24T00:01:00+00:00",
            )
            mark_upload_success(
                conn,
                base_url="https://x.example",
                file_name="a.json",
                content_sha256="sha-1",
                http_status=200,
                response_text="ok",
                now_iso=lambda: "2026-03-24T00:02:00+00:00",
                compact_text=lambda text, limit: str(text)[:limit] if text is not None else None,
            )
            row = conn.execute(
                "SELECT status, last_http_status FROM auth_file_uploads WHERE file_name = 'a.json'"
            ).fetchone()
            self.assertEqual(str(row["status"]), "success")
            self.assertEqual(int(row["last_http_status"]), 200)

            mark_upload_failure(
                conn,
                base_url="https://x.example",
                file_name="a.json",
                content_sha256="sha-1",
                http_status=500,
                error_text="err",
                response_text="bad",
                compact_text=lambda text, limit: str(text)[:limit] if text is not None else None,
            )
            failed = conn.execute(
                "SELECT status, last_error FROM auth_file_uploads WHERE file_name = 'a.json'"
            ).fetchone()
            self.assertEqual(str(failed["status"]), "failed")
            self.assertEqual(str(failed["last_error"]), "err")
        finally:
            conn.close()

    def test_mark_upload_skipped_remote_exists(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            init_db(conn)
            mark_upload_skipped_remote_exists(
                conn,
                base_url="https://x.example",
                file_name="a.json",
                content_sha256="sha-1",
                file_path="C:/a.json",
                file_size=12,
                now_iso=lambda: "2026-03-24T00:00:00+00:00",
            )
            row = conn.execute("SELECT status FROM auth_file_uploads WHERE file_name = 'a.json'").fetchone()
            self.assertEqual(str(row["status"]), "skipped_remote_exists")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
