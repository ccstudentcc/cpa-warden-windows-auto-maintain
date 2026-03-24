from __future__ import annotations

import asyncio
import unittest
from typing import Any

from cwma.warden.services.runtime_ops import (
    apply_action_results,
    classify_account_state,
    confirm_action,
    probe_accounts_async,
    run_action_group_async,
    run_register_hook_async,
    summarize_upload_results,
    upload_auth_file_async,
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


class _NoopProgress:
    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled
        self.advanced = 0

    def __enter__(self) -> "_NoopProgress":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def advance(self, step: int = 1) -> None:
        self.advanced += int(step)


class _DummyAiohttpModule:
    class TCPConnector:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    class ClientTimeout:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    class ClientSession:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

        async def __aenter__(self) -> "_DummyAiohttpModule.ClientSession":
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
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

    async def test_probe_accounts_async_collects_results(self) -> None:
        logger = _CaptureLogger()

        async def _probe(
            session: Any,
            semaphore: Any,
            base_url: str,
            token: str,
            record: dict[str, Any],
            timeout: int,
            retries: int,
            user_agent: str,
        ) -> dict[str, Any]:
            return {
                "name": record["name"],
                "api_status_code": 200,
                "disabled": 0,
                "unavailable": 0,
            }

        def _classify(record: dict[str, Any], *, quota_disable_threshold: float) -> dict[str, Any]:
            row = dict(record)
            row["quota_disable_threshold"] = quota_disable_threshold
            return row

        results = await probe_accounts_async(
            [{"name": "a.json"}, {"name": "b.json"}],
            base_url="https://x.example",
            token="t",
            timeout=10,
            retries=1,
            user_agent="ua",
            probe_workers=2,
            quota_disable_threshold=0.25,
            debug=False,
            probe_wham_usage_async=_probe,
            classify_account_state=_classify,
            progress_log_step=lambda total: 1,
            progress_reporter_factory=lambda *args, **kwargs: _NoopProgress(enabled=False),
            logger=logger,
            aiohttp_module=_DummyAiohttpModule,
        )

        self.assertEqual({row["name"] for row in results}, {"a.json", "b.json"})
        self.assertTrue(all(float(row["quota_disable_threshold"]) == 0.25 for row in results))

    async def test_run_action_group_async_delete_path(self) -> None:
        logger = _CaptureLogger()

        async def _delete(
            session: Any,
            semaphore: Any,
            base_url: str,
            token: str,
            name: str,
            timeout: int,
            delete_retries: int,
        ) -> dict[str, Any]:
            return {"name": name, "ok": True}

        async def _toggle(*args: Any, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("toggle path should not run in delete test")

        results = await run_action_group_async(
            base_url="https://x.example",
            token="t",
            timeout=10,
            workers=2,
            items=["a.json", "b.json"],
            fn_name="delete",
            disabled=None,
            delete_retries=2,
            debug=False,
            delete_account_async=_delete,
            set_account_disabled_async=_toggle,
            progress_log_step=lambda total: 1,
            progress_reporter_factory=lambda *args, **kwargs: _NoopProgress(enabled=False),
            logger=logger,
            aiohttp_module=_DummyAiohttpModule,
        )

        self.assertEqual(len(results), 2)
        self.assertEqual({row["name"] for row in results}, {"a.json", "b.json"})
        self.assertTrue(all(bool(row["ok"]) for row in results))

    async def test_upload_auth_file_async_stop_event_short_circuit(self) -> None:
        stop_event = asyncio.Event()
        stop_event.set()
        result = await upload_auth_file_async(
            session=object(),
            semaphore=asyncio.Semaphore(1),
            conn=object(),
            settings={
                "base_url": "https://x.example",
                "token": "t",
                "timeout": 10,
                "upload_retries": 1,
                "upload_method": "json",
            },
            candidate={
                "file_name": "a.json",
                "file_path": "a.json",
                "content_sha256": "sha",
                "file_size": 1,
            },
            stop_event=stop_event,
            claim_upload_slot=lambda *args, **kwargs: "claimed",
            mark_upload_attempt=lambda *args, **kwargs: None,
            mark_upload_success=lambda *args, **kwargs: None,
            mark_upload_failure=lambda *args, **kwargs: None,
            mgmt_headers=lambda *args, **kwargs: {},
            maybe_json_loads=lambda value: value,
            compact_text=lambda text, limit: str(text)[:limit],
            backoff_seconds=lambda attempt: 0.0,
            aiohttp_module=_DummyAiohttpModule,
        )
        self.assertEqual(result["outcome"], "skipped_due_to_stop")
        self.assertEqual(result["error_kind"], "core_auth_manager_unavailable")

    async def test_upload_auth_file_async_claim_skipped_done(self) -> None:
        stop_event = asyncio.Event()
        result = await upload_auth_file_async(
            session=object(),
            semaphore=asyncio.Semaphore(1),
            conn=object(),
            settings={
                "base_url": "https://x.example",
                "token": "t",
                "timeout": 10,
                "upload_retries": 1,
                "upload_method": "json",
            },
            candidate={
                "file_name": "a.json",
                "file_path": "a.json",
                "content_sha256": "sha",
                "file_size": 1,
            },
            stop_event=stop_event,
            claim_upload_slot=lambda *args, **kwargs: "skipped_done",
            mark_upload_attempt=lambda *args, **kwargs: None,
            mark_upload_success=lambda *args, **kwargs: None,
            mark_upload_failure=lambda *args, **kwargs: None,
            mgmt_headers=lambda *args, **kwargs: {},
            maybe_json_loads=lambda value: value,
            compact_text=lambda text, limit: str(text)[:limit],
            backoff_seconds=lambda attempt: 0.0,
            aiohttp_module=_DummyAiohttpModule,
        )
        self.assertEqual(result["outcome"], "skipped_already_uploaded")
        self.assertTrue(result["ok"])

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
