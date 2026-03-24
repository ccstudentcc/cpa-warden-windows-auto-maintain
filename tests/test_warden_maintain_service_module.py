from __future__ import annotations

import unittest
from typing import Any

from cwma.warden.services.maintain import (
    MAINTAIN_STEP_DELETE_401,
    MAINTAIN_STEP_FINALIZE,
    MAINTAIN_STEP_QUOTA,
    MAINTAIN_STEP_SCAN,
    normalize_maintain_steps,
    run_maintain_async,
    run_maintain_steps_async,
)


class _DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        return None

    def debug(self, *args: Any, **kwargs: Any) -> None:
        return None


class WardenMaintainServiceModuleTests(unittest.IsolatedAsyncioTestCase):
    def _base_settings(self) -> dict[str, Any]:
        return {
            "delete_401": True,
            "quota_action": "disable",
            "quota_disable_threshold": 0.2,
            "auto_reenable": True,
            "reenable_scope": "signal",
            "delete_retries": 2,
            "assume_yes": True,
            "base_url": "https://x.example",
            "token": "t",
            "timeout": 10,
            "action_workers": 8,
            "debug": False,
        }

    async def test_run_maintain_async_disable_and_reenable_flow(self) -> None:
        settings = self._base_settings()
        call_seq: list[str] = []
        upserts: list[list[dict[str, Any]]] = []
        scan_scopes: list[set[str] | None] = []

        async def _run_scan(conn: Any, cfg: dict[str, Any], scope: set[str] | None) -> dict[str, Any]:
            scan_scopes.append(scope)
            return {
                "candidate_records": [
                    {"name": "a.json"},
                    {"name": "b.json", "disabled": 0},
                    {"name": "c.json", "disabled": 1, "managed_reason": "quota_disabled"},
                ],
                "invalid_records": [{"name": "a.json"}],
                "quota_records": [{"name": "b.json", "disabled": 0, "is_invalid_401": 0}],
                "recovered_records": [{"name": "c.json", "managed_reason": "quota_disabled"}],
            }

        async def _run_action(**kwargs: Any) -> list[dict[str, Any]]:
            fn_name = str(kwargs["fn_name"])
            disabled = kwargs.get("disabled")
            if fn_name == "delete":
                call_seq.append("delete_401")
                return [{"name": "a.json", "ok": True}]
            if disabled is True:
                call_seq.append("disable_quota")
                return [{"name": "b.json", "ok": True}]
            call_seq.append("reenable_quota")
            return [{"name": "c.json", "ok": True}]

        def _apply(records_by_name: dict[str, dict[str, Any]], results: list[dict[str, Any]], **kwargs: Any) -> list[dict[str, Any]]:
            return [records_by_name[row["name"]] for row in results if row.get("name") in records_by_name]

        result = await run_maintain_async(
            object(),
            settings,
            resolve_maintain_name_scope=lambda cfg: {"a.json", "b.json", "c.json"},
            run_scan_async=_run_scan,
            run_action_group_async=_run_action,
            confirm_action=lambda message, assume_yes: True,
            apply_action_results=_apply,
            upsert_auth_accounts=lambda conn, rows: upserts.append(list(rows)),
            mark_quota_already_disabled=lambda records: records,
            summarize_action_results=lambda label, rows: None,
            logger=_DummyLogger(),
        )

        self.assertEqual(scan_scopes, [{"a.json", "b.json", "c.json"}])
        self.assertEqual(call_seq, ["delete_401", "disable_quota", "reenable_quota"])
        self.assertEqual(len(upserts), 3)
        self.assertEqual(len(result["delete_401_results"]), 1)
        self.assertEqual(len(result["quota_action_results"]), 1)
        self.assertEqual(len(result["reenable_results"]), 1)

    async def test_run_maintain_async_skip_delete_when_not_confirmed(self) -> None:
        settings = self._base_settings()
        settings["assume_yes"] = False
        calls: list[str] = []

        async def _run_scan(conn: Any, cfg: dict[str, Any], scope: set[str] | None) -> dict[str, Any]:
            return {
                "candidate_records": [{"name": "a.json"}],
                "invalid_records": [{"name": "a.json"}],
                "quota_records": [],
                "recovered_records": [],
            }

        async def _run_action(**kwargs: Any) -> list[dict[str, Any]]:
            calls.append("called")
            return []

        result = await run_maintain_async(
            object(),
            settings,
            resolve_maintain_name_scope=lambda cfg: None,
            run_scan_async=_run_scan,
            run_action_group_async=_run_action,
            confirm_action=lambda message, assume_yes: False,
            apply_action_results=lambda records_by_name, results, **kwargs: [],
            upsert_auth_accounts=lambda conn, rows: None,
            mark_quota_already_disabled=lambda records: records,
            summarize_action_results=lambda label, rows: None,
            logger=_DummyLogger(),
        )

        self.assertEqual(calls, [])
        self.assertEqual(result["delete_401_results"], [])

    async def test_run_maintain_async_delete_quota_requires_confirm(self) -> None:
        settings = self._base_settings()
        settings["quota_action"] = "delete"
        action_calls: list[dict[str, Any]] = []

        async def _run_scan(conn: Any, cfg: dict[str, Any], scope: set[str] | None) -> dict[str, Any]:
            return {
                "candidate_records": [{"name": "q.json"}],
                "invalid_records": [],
                "quota_records": [{"name": "q.json", "is_invalid_401": 0}],
                "recovered_records": [],
            }

        async def _run_action(**kwargs: Any) -> list[dict[str, Any]]:
            action_calls.append(kwargs)
            return [{"name": "q.json", "ok": True}]

        await run_maintain_async(
            object(),
            settings,
            resolve_maintain_name_scope=lambda cfg: None,
            run_scan_async=_run_scan,
            run_action_group_async=_run_action,
            confirm_action=lambda message, assume_yes: False,
            apply_action_results=lambda records_by_name, results, **kwargs: [],
            upsert_auth_accounts=lambda conn, rows: None,
            mark_quota_already_disabled=lambda records: records,
            summarize_action_results=lambda label, rows: None,
            logger=_DummyLogger(),
        )

        self.assertEqual(action_calls, [])

    def test_normalize_maintain_steps_rejects_out_of_order(self) -> None:
        with self.assertRaises(ValueError):
            _ = normalize_maintain_steps([MAINTAIN_STEP_QUOTA, MAINTAIN_STEP_SCAN])

    async def test_run_maintain_steps_async_supports_partial_step_selection(self) -> None:
        settings = self._base_settings()
        settings["delete_401"] = True
        settings["auto_reenable"] = True
        call_seq: list[str] = []

        async def _run_scan(conn: Any, cfg: dict[str, Any], scope: set[str] | None) -> dict[str, Any]:
            return {
                "candidate_records": [
                    {"name": "a.json"},
                    {"name": "b.json", "disabled": 0},
                    {"name": "c.json", "disabled": 1, "managed_reason": "quota_disabled"},
                ],
                "invalid_records": [{"name": "a.json"}],
                "quota_records": [{"name": "b.json", "disabled": 0, "is_invalid_401": 0}],
                "recovered_records": [{"name": "c.json", "managed_reason": "quota_disabled"}],
            }

        async def _run_action(**kwargs: Any) -> list[dict[str, Any]]:
            fn_name = str(kwargs["fn_name"])
            if fn_name == "delete":
                call_seq.append("delete_401")
                return [{"name": "a.json", "ok": True}]
            call_seq.append("quota")
            return [{"name": "b.json", "ok": True}]

        result = await run_maintain_steps_async(
            object(),
            settings,
            resolve_maintain_name_scope=lambda cfg: {"a.json", "b.json", "c.json"},
            run_scan_async=_run_scan,
            run_action_group_async=_run_action,
            confirm_action=lambda message, assume_yes: True,
            apply_action_results=lambda records_by_name, results, **kwargs: [],
            upsert_auth_accounts=lambda conn, rows: None,
            mark_quota_already_disabled=lambda records: records,
            summarize_action_results=lambda label, rows: None,
            logger=_DummyLogger(),
            steps=[MAINTAIN_STEP_SCAN, MAINTAIN_STEP_QUOTA, MAINTAIN_STEP_FINALIZE],
        )

        self.assertEqual(call_seq, ["quota"])
        self.assertEqual(result["delete_401_results"], [])
        self.assertEqual(len(result["quota_action_results"]), 1)
        self.assertEqual(result["reenable_results"], [])


if __name__ == "__main__":
    unittest.main()
