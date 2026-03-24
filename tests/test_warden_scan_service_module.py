from __future__ import annotations

import asyncio
import unittest
from typing import Any

from cwma.warden.services.scan import run_scan_async


class _DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        return None


class WardenScanServiceModuleTests(unittest.IsolatedAsyncioTestCase):
    def _base_settings(self) -> dict[str, Any]:
        return {
            "mode": "scan",
            "db_path": "state.sqlite3",
            "log_file": "scan.log",
            "quota_disable_threshold": 0.2,
            "base_url": "https://x.example",
            "token": "t",
            "timeout": 10,
            "target_type": "codex",
            "provider": "",
            "retries": 1,
            "user_agent": "ua",
            "probe_workers": 4,
            "debug": False,
            "invalid_output": "invalid.json",
            "quota_output": "quota.json",
        }

    async def test_run_scan_async_happy_path(self) -> None:
        settings = self._base_settings()
        finish_calls: list[dict[str, Any]] = []
        summary_calls: list[dict[str, Any]] = []
        export_calls: list[tuple[str, str, list[dict[str, Any]], list[dict[str, Any]]]] = []
        upserts: list[list[dict[str, Any]]] = []

        files = [
            {"name": "a.json", "type": "codex"},
            {"name": "b.json", "type": "codex"},
        ]

        async def _probe(records: list[dict[str, Any]], **kwargs: Any) -> list[dict[str, Any]]:
            return [
                {
                    "name": "a.json",
                    "is_invalid_401": 1,
                    "is_quota_limited": 0,
                    "is_recovered": 0,
                    "last_probed_at": "2026-03-24T00:00:01+00:00",
                    "probe_error_kind": None,
                },
                {
                    "name": "b.json",
                    "is_invalid_401": 0,
                    "is_quota_limited": 1,
                    "is_recovered": 0,
                    "last_probed_at": "2026-03-24T00:00:01+00:00",
                    "probe_error_kind": None,
                },
            ]

        def _finish(
            conn: Any,
            run_id: int,
            *,
            status: str,
            total_files: int,
            filtered_files: int,
            probed_files: int,
            invalid_401_count: int,
            quota_limited_count: int,
            recovered_count: int,
        ) -> None:
            finish_calls.append(
                {
                    "run_id": run_id,
                    "status": status,
                    "total_files": total_files,
                    "filtered_files": filtered_files,
                    "probed_files": probed_files,
                    "invalid_401_count": invalid_401_count,
                    "quota_limited_count": quota_limited_count,
                    "recovered_count": recovered_count,
                }
            )

        result = await run_scan_async(
            object(),
            settings,
            maintain_name_scope=None,
            start_scan_run=lambda conn, s: 7,
            finish_scan_run=_finish,
            utc_now_iso=lambda: "2026-03-24T00:00:00+00:00",
            fetch_auth_files=lambda base_url, token, timeout: files,
            load_existing_state=lambda conn: {},
            get_item_name=lambda item: str(item.get("name") or ""),
            build_auth_record=lambda item, existing, now_iso: {
                "name": item["name"],
                "type": item.get("type"),
            },
            upsert_auth_accounts=lambda conn, rows: upserts.append(list(rows)),
            matches_filters=lambda record, target_type, provider: str(record.get("type")) == target_type,
            probe_accounts_async=_probe,
            print_scan_summary=lambda **kwargs: summary_calls.append(kwargs),
            summarize_failures=lambda rows: None,
            export_current_results=lambda invalid_output, quota_output, invalid_records, quota_records: export_calls.append(
                (invalid_output, quota_output, list(invalid_records), list(quota_records))
            ),
            logger=_DummyLogger(),
        )

        self.assertEqual(result["run_id"], 7)
        self.assertEqual(len(result["invalid_records"]), 1)
        self.assertEqual(len(result["quota_records"]), 1)
        self.assertEqual(len(upserts), 2)
        self.assertEqual(len(summary_calls), 1)
        self.assertEqual(len(export_calls), 1)
        self.assertEqual(finish_calls[0]["status"], "success")
        self.assertEqual(finish_calls[0]["probed_files"], 2)

    async def test_run_scan_async_respects_maintain_scope(self) -> None:
        settings = self._base_settings()
        probe_inputs: list[list[str]] = []

        async def _probe(records: list[dict[str, Any]], **kwargs: Any) -> list[dict[str, Any]]:
            probe_inputs.append([str(row.get("name")) for row in records])
            return []

        await run_scan_async(
            object(),
            settings,
            maintain_name_scope={"b.json"},
            start_scan_run=lambda conn, s: 1,
            finish_scan_run=lambda conn, run_id, **kwargs: None,
            utc_now_iso=lambda: "2026-03-24T00:00:00+00:00",
            fetch_auth_files=lambda base_url, token, timeout: [
                {"name": "a.json", "type": "codex"},
                {"name": "b.json", "type": "codex"},
            ],
            load_existing_state=lambda conn: {},
            get_item_name=lambda item: str(item.get("name") or ""),
            build_auth_record=lambda item, existing, now_iso: {"name": item["name"], "type": "codex"},
            upsert_auth_accounts=lambda conn, rows: None,
            matches_filters=lambda record, target_type, provider: True,
            probe_accounts_async=_probe,
            print_scan_summary=lambda **kwargs: None,
            summarize_failures=lambda rows: None,
            export_current_results=lambda invalid_output, quota_output, invalid_records, quota_records: None,
            logger=_DummyLogger(),
        )

        self.assertEqual(probe_inputs, [["b.json"]])

    async def test_run_scan_async_marks_failed_when_exception(self) -> None:
        settings = self._base_settings()
        finish_calls: list[dict[str, Any]] = []

        with self.assertRaisesRegex(RuntimeError, "boom"):
            await run_scan_async(
                object(),
                settings,
                maintain_name_scope=None,
                start_scan_run=lambda conn, s: 3,
                finish_scan_run=lambda conn, run_id, **kwargs: finish_calls.append(dict(kwargs)),
                utc_now_iso=lambda: "2026-03-24T00:00:00+00:00",
                fetch_auth_files=lambda base_url, token, timeout: (_ for _ in ()).throw(RuntimeError("boom")),
                load_existing_state=lambda conn: {},
                get_item_name=lambda item: "",
                build_auth_record=lambda item, existing, now_iso: {},
                upsert_auth_accounts=lambda conn, rows: None,
                matches_filters=lambda record, target_type, provider: True,
                probe_accounts_async=lambda *args, **kwargs: asyncio.sleep(0.0),
                print_scan_summary=lambda **kwargs: None,
                summarize_failures=lambda rows: None,
                export_current_results=lambda invalid_output, quota_output, invalid_records, quota_records: None,
                logger=_DummyLogger(),
            )

        self.assertEqual(len(finish_calls), 1)
        self.assertEqual(finish_calls[0]["status"], "failed")
        self.assertEqual(finish_calls[0]["total_files"], 0)


if __name__ == "__main__":
    unittest.main()
