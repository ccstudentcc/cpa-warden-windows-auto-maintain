from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from cwma.warden.exports import (
    build_invalid_export_record,
    build_quota_export_record,
    export_current_results,
    export_records,
    summarize_failures,
)
from tests.temp_sandbox import TempSandboxState, setup_tempfile_sandbox, teardown_tempfile_sandbox

_TEMP_SANDBOX_STATE: TempSandboxState | None = None


def setUpModule() -> None:
    global _TEMP_SANDBOX_STATE
    _TEMP_SANDBOX_STATE = setup_tempfile_sandbox()


def tearDownModule() -> None:
    teardown_tempfile_sandbox(_TEMP_SANDBOX_STATE)


class _FakeLogger:
    def __init__(self) -> None:
        self.info_calls: list[tuple[Any, ...]] = []
        self.debug_calls: list[tuple[Any, ...]] = []

    def info(self, *args: Any) -> None:
        self.info_calls.append(args)

    def debug(self, *args: Any) -> None:
        self.debug_calls.append(args)


class WardenExportsModuleTests(unittest.TestCase):
    def test_build_invalid_export_record_keeps_expected_fields(self) -> None:
        row = {
            "name": "a.json",
            "email": "a@example.com",
            "disabled": 1,
            "unavailable": 0,
            "auth_index": "idx-1",
            "chatgpt_account_id": "acct-1",
            "api_http_status": 200,
            "api_status_code": 401,
            "status": "bad",
            "status_message": "msg",
            "probe_error_kind": "other",
            "probe_error_text": "err",
        }
        exported = build_invalid_export_record(row)
        self.assertEqual(exported["name"], "a.json")
        self.assertEqual(exported["account"], "a@example.com")
        self.assertTrue(exported["disabled"])
        self.assertFalse(exported["unavailable"])
        self.assertEqual(exported["probe_error_kind"], "other")

    def test_build_quota_export_record_uses_injected_resolvers(self) -> None:
        row = {
            "name": "q.json",
            "email": "q@example.com",
            "usage_remaining_ratio": 0.4,
            "usage_spark_remaining_ratio": 1.2,
            "usage_limit_reached": 1,
            "usage_allowed": 0,
            "usage_spark_limit_reached": 0,
            "usage_spark_allowed": 1,
            "quota_threshold_triggered": 1,
            "usage_plan_type": "pro",
        }
        exported = build_quota_export_record(
            row,
            resolve_quota_signal=lambda r: (1, 0, "primary"),
            resolve_quota_remaining_ratio=lambda r: (0.4, "primary"),
            normalize_optional_ratio=lambda value: max(0.0, min(1.0, float(value))) if value is not None else None,
        )
        self.assertEqual(exported["quota_signal_source"], "primary")
        self.assertEqual(exported["remaining_ratio"], 0.4)
        self.assertEqual(exported["spark_remaining_ratio"], 1.0)
        self.assertTrue(exported["threshold_triggered"])

    def test_summarize_failures_logs_summary_and_samples(self) -> None:
        logger = _FakeLogger()
        rows = [
            {
                "name": "a.json",
                "account": "a@example.com",
                "auth_index": "idx-1",
                "chatgpt_account_id": "acct-1",
                "api_http_status": 429,
                "api_status_code": 200,
                "probe_error_kind": "management_api_http_429",
                "probe_error_text": "too many requests",
            },
            {
                "name": "b.json",
                "email": "b@example.com",
                "auth_index": "idx-2",
                "chatgpt_account_id": "",
                "api_http_status": 500,
                "api_status_code": None,
                "probe_error_kind": "other",
                "probe_error_text": "boom",
            },
        ]
        summarize_failures(
            rows,
            sample_limit=1,
            compact_text=lambda text, limit: str(text)[:limit] if text is not None else None,
            logger=logger,
        )
        self.assertTrue(any("失败原因统计:" in str(call[0]) for call in logger.info_calls))
        self.assertTrue(any("失败样例:" in str(call[0]) for call in logger.debug_calls))

    def test_export_records_and_export_current_results_write_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            invalid_path = Path(tmp) / "invalid.json"
            quota_path = Path(tmp) / "quota.json"
            logger = _FakeLogger()

            export_current_results(
                str(invalid_path),
                str(quota_path),
                [{"name": "a.json"}],
                [{"name": "q.json"}],
                export_records_fn=export_records,
                build_invalid_export_record_fn=lambda row: {"kind": "invalid", **row},
                build_quota_export_record_fn=lambda row: {"kind": "quota", **row},
                logger=logger,
            )

            invalid_rows = json.loads(invalid_path.read_text(encoding="utf-8"))
            quota_rows = json.loads(quota_path.read_text(encoding="utf-8"))
            self.assertEqual(invalid_rows[0]["kind"], "invalid")
            self.assertEqual(quota_rows[0]["kind"], "quota")
            self.assertTrue(any("已导出 401 列表" in str(call[0]) for call in logger.info_calls))


if __name__ == "__main__":
    unittest.main()
