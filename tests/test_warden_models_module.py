from __future__ import annotations

import unittest

from cwma.warden.models import (
    build_auth_record,
    extract_chatgpt_account_id_from_item,
    maybe_json_loads,
    normalize_optional_ratio,
    resolve_quota_remaining_ratio,
    resolve_quota_signal,
)


class WardenModelsModuleTests(unittest.TestCase):
    def test_maybe_json_loads_supports_json_string_and_invalid(self) -> None:
        self.assertEqual(maybe_json_loads('{"a": 1}'), {"a": 1})
        self.assertIsNone(maybe_json_loads("{"))
        self.assertEqual(maybe_json_loads({"a": 1}), {"a": 1})

    def test_normalize_optional_ratio_clamps(self) -> None:
        self.assertEqual(normalize_optional_ratio(-1), 0.0)
        self.assertEqual(normalize_optional_ratio(2), 1.0)
        self.assertAlmostEqual(normalize_optional_ratio("0.25") or -1.0, 0.25)
        self.assertIsNone(normalize_optional_ratio("x"))

    def test_resolve_quota_signal_prefers_spark_for_pro(self) -> None:
        limit_reached, allowed, source = resolve_quota_signal(
            {
                "usage_plan_type": "pro",
                "usage_spark_limit_reached": 1,
                "usage_spark_allowed": 0,
                "usage_limit_reached": 0,
                "usage_allowed": 1,
            }
        )
        self.assertEqual((limit_reached, allowed, source), (1, 0, "spark"))

    def test_resolve_quota_remaining_ratio_fallback_sources(self) -> None:
        ratio, source = resolve_quota_remaining_ratio(
            {
                "usage_plan_type": "pro",
                "usage_spark_limit_reached": 1,
                "usage_spark_remaining_ratio": None,
                "usage_remaining_ratio": 0.4,
            }
        )
        self.assertEqual(source, "primary_fallback")
        self.assertAlmostEqual(ratio or -1.0, 0.4)

    def test_extract_chatgpt_account_id_from_item_prefers_id_token_then_item(self) -> None:
        from_token = extract_chatgpt_account_id_from_item(
            {"id_token": '{"chatgpt_account_id":"acct-1"}', "chatgpt_account_id": "acct-2"}
        )
        from_item = extract_chatgpt_account_id_from_item({"chatgptAccountId": "acct-3"})
        self.assertEqual(from_token, "acct-1")
        self.assertEqual(from_item, "acct-3")

    def test_build_auth_record_keeps_managed_fields(self) -> None:
        item = {
            "name": "alpha.json",
            "disabled": True,
            "id_token": '{"chatgpt_account_id":"acct-1","plan_type":"pro"}',
            "email": "a@example.com",
            "provider": "openai",
            "source": "local",
            "auth_index": "idx-1",
            "type": "codex",
            "status_message": "line1\nline2",
            "updated_at": "2026-03-24T00:00:00+00:00",
        }
        existing = {
            "managed_reason": "quota_disabled",
            "last_action": "disable",
            "last_action_status": "success",
            "last_action_error": None,
        }

        record = build_auth_record(item, existing, "2026-03-24T01:00:00+00:00")
        self.assertEqual(record["name"], "alpha.json")
        self.assertEqual(record["chatgpt_account_id"], "acct-1")
        self.assertEqual(record["id_token_plan_type"], "pro")
        self.assertEqual(record["managed_reason"], "quota_disabled")
        self.assertEqual(record["last_action"], "disable")
        self.assertEqual(record["status_message"], "line1 line2")


if __name__ == "__main__":
    unittest.main()
