from __future__ import annotations

import unittest
from typing import Any

from cwma.warden.services.refill import (
    compute_refill_upload_count,
    count_valid_accounts,
    run_maintain_refill_async,
)


class _DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        return None


class WardenRefillServiceModuleTests(unittest.IsolatedAsyncioTestCase):
    def test_count_valid_accounts_filters_invalid_cases(self) -> None:
        records = [
            {"disabled": 0, "is_invalid_401": 0, "is_quota_limited": 0, "probe_error_kind": None},
            {"disabled": 1, "is_invalid_401": 0, "is_quota_limited": 0, "probe_error_kind": None},
            {"disabled": 0, "is_invalid_401": 1, "is_quota_limited": 0, "probe_error_kind": None},
            {"disabled": 0, "is_invalid_401": 0, "is_quota_limited": 1, "probe_error_kind": None},
            {"disabled": 0, "is_invalid_401": 0, "is_quota_limited": 0, "probe_error_kind": "timeout"},
        ]
        self.assertEqual(
            count_valid_accounts(records, row_to_bool=lambda value: bool(int(value or 0))),
            1,
        )

    def test_compute_refill_upload_count(self) -> None:
        self.assertEqual(compute_refill_upload_count(5, 5, "to-threshold"), 0)
        self.assertEqual(compute_refill_upload_count(3, 5, "to-threshold"), 2)
        self.assertEqual(compute_refill_upload_count(3, 5, "fixed"), 5)

    async def test_run_maintain_refill_async_refill_then_success(self) -> None:
        settings = {
            "min_valid_accounts": 3,
            "refill_strategy": "to-threshold",
            "auto_register": False,
        }
        upload_limits: list[int | None] = []

        scans = [
            {"candidate_records": [{"disabled": 0}, {"disabled": 0}]},
            {"candidate_records": [{"disabled": 0}, {"disabled": 0}, {"disabled": 0}]},
        ]

        async def _run_scan(conn: Any, cfg: dict[str, Any]) -> dict[str, Any]:
            return scans.pop(0)

        async def _run_upload(conn: Any, cfg: dict[str, Any], limit: int | None) -> dict[str, Any]:
            upload_limits.append(limit)
            return {"results": [{"ok": True}]}

        result = await run_maintain_refill_async(
            object(),
            settings,
            run_maintain_async=lambda conn, cfg: _async_value({"ok": True}),
            run_scan_async=_run_scan,
            run_upload_async=_run_upload,
            run_register_hook_async=lambda cfg, count: _async_value({"executed": False}),
            row_to_bool=lambda value: bool(int(value or 0)),
            logger=_DummyLogger(),
        )

        self.assertEqual(upload_limits, [1])
        self.assertEqual(result["final_valid_count"], 3)
        self.assertIsNone(result["register_result"])

    async def test_run_maintain_refill_async_register_path(self) -> None:
        settings = {
            "min_valid_accounts": 4,
            "refill_strategy": "to-threshold",
            "auto_register": True,
        }
        upload_limits: list[int | None] = []
        register_counts: list[int] = []
        scans = [
            {"candidate_records": [{"disabled": 0}]},  # after maintain
            {"candidate_records": [{"disabled": 0}, {"disabled": 0}]},  # after refill upload
            {
                "candidate_records": [
                    {"disabled": 0},
                    {"disabled": 0},
                    {"disabled": 0},
                    {"disabled": 0},
                ]
            },  # final
        ]

        async def _run_scan(conn: Any, cfg: dict[str, Any]) -> dict[str, Any]:
            return scans.pop(0)

        async def _run_upload(conn: Any, cfg: dict[str, Any], limit: int | None) -> dict[str, Any]:
            upload_limits.append(limit)
            return {"results": []}

        async def _run_register(cfg: dict[str, Any], count: int) -> dict[str, Any]:
            register_counts.append(count)
            return {"executed": True, "requested_count": count}

        result = await run_maintain_refill_async(
            object(),
            settings,
            run_maintain_async=lambda conn, cfg: _async_value({"ok": True}),
            run_scan_async=_run_scan,
            run_upload_async=_run_upload,
            run_register_hook_async=_run_register,
            row_to_bool=lambda value: bool(int(value or 0)),
            logger=_DummyLogger(),
        )

        self.assertEqual(upload_limits, [3, 2])
        self.assertEqual(register_counts, [2])
        self.assertEqual(result["final_valid_count"], 4)


async def _async_value(value: Any) -> Any:
    return value


if __name__ == "__main__":
    unittest.main()
