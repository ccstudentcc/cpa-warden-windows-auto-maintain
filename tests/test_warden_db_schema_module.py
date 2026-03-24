from __future__ import annotations

import sqlite3
import unittest

from cwma.warden.db.schema import ensure_auth_accounts_schema, init_db, init_upload_db


class WardenDbSchemaModuleTests(unittest.TestCase):
    def test_init_db_creates_required_tables(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            init_db(conn)
            tables = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            self.assertIn("auth_accounts", tables)
            self.assertIn("scan_runs", tables)
            self.assertIn("auth_file_uploads", tables)
        finally:
            conn.close()

    def test_ensure_auth_accounts_schema_adds_missing_columns(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE auth_accounts (name TEXT PRIMARY KEY)")
            ensure_auth_accounts_schema(conn, required_columns={"quota_signal_source": "TEXT"})
            cols = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(auth_accounts)").fetchall()
            }
            self.assertIn("quota_signal_source", cols)
        finally:
            conn.close()

    def test_init_upload_db_is_idempotent(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            init_upload_db(conn)
            init_upload_db(conn)
            indexes = {
                str(row[1])
                for row in conn.execute("PRAGMA index_list(auth_file_uploads)").fetchall()
            }
            self.assertIn("idx_auth_file_uploads_status", indexes)
            self.assertIn("idx_auth_file_uploads_file_name", indexes)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
