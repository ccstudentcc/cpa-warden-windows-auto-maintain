from __future__ import annotations

import unittest
from typing import Any

from cwma.warden.services.runtime_ops import (
    apply_action_results,
    classify_account_state,
    confirm_action,
    run_register_hook_async,
    summarize_upload_results,
)


class _CaptureLogger:
    def __init__(self) -> None:
        self.info_calls: list[tuple[Any, ...]] = []
        self.warning_calls: list[tuple[Any, ...]] = []
        self.debug_calls: list[tuple[Any, ...]] = []

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.info_calls.append(args)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.warning_calls.append(args)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.debug_calls.append(args)


class _NonTty:
    def isatty(self) -> bool:
        return False


class WardenRuntimeOpsModuleTests(unittest.IsolatedAsyncioTestCase):
    def test_classify_account_state_threshold(self) -> None:
        logger = _CaptureLogger()
        record = {
            "name": "acc-1",
            "api_status_code": 200,
            "unavailable": 0,
            "disabled": 0,
        }

        updated = classify_account_state(
            record,
            quota_disable_threshold=0.2,
            resolve_quota_signal=lambda row: (0, 1, "usage"),
            resolve_quota_remaining_ratio=lambda row: (0.1, "rate_limit"),
            utc_now_iso=lambda: "2026-03-24T00:00:00+00:00",
            logger=logger,
        )

        self.assertEqual(updated["is_invalid_401"], 0)
        self.assertEqual(updated["is_quota_limited"], 1)
        self.assertEqual(updated["quota_threshold_triggered"], 1)
        self.assertEqual(updated["is_recovered"], 0)
        self.assertEqual(updated["updated_at"], "2026-03-24T00:00:00+00:00")
        self.assertEqual(len(logger.info_calls), 1)

    def test_apply_action_results_updates_records(self) -> None:
        logger = _CaptureLogger()
        records_by_name = {"a.json": {"name": "a.json", "disabled": 0}}
        results = [{"name": "a.json", "ok": True, "error": None}]

        updated = apply_action_results(
            records_by_name,
            results,
            action="disable_quota",
            managed_reason_on_success="quota_disabled",
            disabled_value=1,
            utc_now_iso=lambda: "2026-03-24T00:00:00+00:00",
            compact_text=lambda text, limit: str(text)[:limit],
            logger=logger,
        )

        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["last_action"], "disable_quota")
        self.assertEqual(updated[0]["last_action_status"], "success")
        self.assertEqual(updated[0]["managed_reason"], "quota_disabled")
        self.assertEqual(updated[0]["disabled"], 1)
        self.assertEqual(updated[0]["updated_at"], "2026-03-24T00:00:00+00:00")

    def test_confirm_action_non_tty_returns_false(self) -> None:
        logger = _CaptureLogger()
        ok = confirm_action(
            "即将删除 1 个账号",
            False,
            stdin=_NonTty(),
            input_fn=lambda prompt: "DELETE",
            logger=logger,
        )
        self.assertFalse(ok)
        self.assertEqual(len(logger.warning_calls), 1)

    async def test_run_register_hook_async_zero_count_short_circuit(self) -> None:
        logger = _CaptureLogger()
        result = await run_register_hook_async(
            {
                "upload_dir": ".",
                "upload_recursive": False,
                "register_timeout": 1,
                "register_command": "",
            },
            count=0,
            discover_upload_files=lambda path, recursive: [],
            compact_text=lambda text, limit: str(text)[:limit],
            logger=logger,
        )
        self.assertEqual(result["executed"], False)
        self.assertEqual(result["requested_count"], 0)
        self.assertEqual(result["new_files"], 0)

    async def test_run_register_hook_async_requires_command(self) -> None:
        logger = _CaptureLogger()
        with self.assertRaisesRegex(RuntimeError, "register_command"):
            await run_register_hook_async(
                {
                    "upload_dir": ".",
                    "upload_recursive": False,
                    "register_timeout": 1,
                    "register_command": "",
                },
                count=1,
                discover_upload_files=lambda path, recursive: [],
                compact_text=lambda text, limit: str(text)[:limit],
                logger=logger,
            )

    def test_summarize_upload_results_logs_failed_rows(self) -> None:
        logger = _CaptureLogger()
        summarize_upload_results(
            [
                {
                    "file_name": "x.json",
                    "status_code": 500,
                    "outcome": "upload_failed",
                    "error": "boom",
                }
            ],
            discovered_count=1,
            selected_count=1,
            to_upload_count=1,
            compact_text=lambda text, limit: str(text)[:limit],
            logger=logger,
        )
        self.assertGreaterEqual(len(logger.info_calls), 1)
        self.assertEqual(len(logger.warning_calls), 1)


if __name__ == "__main__":
    unittest.main()

