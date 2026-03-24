from __future__ import annotations

import asyncio
import unittest
from typing import Any

from cwma.warden.api.usage_probe import (
    DEFAULT_WHAM_USAGE_URL,
    build_wham_usage_payload,
    extract_remaining_ratio,
    find_spark_rate_limit,
    probe_wham_usage_async,
)


class _DummyLogger:
    def debug(self, *args: Any, **kwargs: Any) -> None:
        return None


class _FakeResponse:
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self._responses:
            raise RuntimeError("no fake response available")
        return self._responses.pop(0)


def _headers_builder(token: str, include_json: bool) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {token}"}
    if include_json:
        headers["Content-Type"] = "application/json"
    return headers


def _quota_signal_resolver(record: dict[str, Any]) -> tuple[int | None, int | None, str]:
    if record.get("usage_spark_limit_reached") is not None:
        return record.get("usage_spark_limit_reached"), record.get("usage_spark_allowed"), "spark"
    return record.get("usage_limit_reached"), record.get("usage_allowed"), "primary"


async def _no_sleep(_: float) -> None:
    return None


class WardenUsageProbeApiModuleTests(unittest.IsolatedAsyncioTestCase):
    def test_build_wham_usage_payload(self) -> None:
        payload = build_wham_usage_payload("idx-1", "ua-1", "acct-1")
        self.assertEqual(payload["authIndex"], "idx-1")
        self.assertEqual(payload["method"], "GET")
        self.assertEqual(payload["url"], DEFAULT_WHAM_USAGE_URL)
        self.assertEqual(payload["header"]["Chatgpt-Account-Id"], "acct-1")

    def test_extract_remaining_ratio_from_remaining_and_used(self) -> None:
        ratio_remaining = extract_remaining_ratio({"total": 100, "remaining": 25})
        ratio_used = extract_remaining_ratio({"primary_window": {"limit": 100, "used": 20}})
        self.assertAlmostEqual(ratio_remaining or -1.0, 0.25)
        self.assertAlmostEqual(ratio_used or -1.0, 0.8)

    def test_find_spark_rate_limit_prefers_metered_feature_then_limit_name(self) -> None:
        parsed = {
            "additional_rate_limits": [
                {"metered_feature": "other", "rate_limit": {"name": "other"}},
                {"metered_feature": "codex_bengalfox", "rate_limit": {"name": "spark-by-feature"}},
                {"limit_name": "spark pool", "rate_limit": {"name": "spark-by-name"}},
            ]
        }
        picked = find_spark_rate_limit(parsed)
        self.assertEqual((picked or {}).get("name"), "spark-by-feature")

        fallback = find_spark_rate_limit(
            {
                "additional_rate_limits": [
                    {"metered_feature": "other", "rate_limit": {"name": "other"}},
                    {"limit_name": "Spark pool", "rate_limit": {"name": "spark-by-name"}},
                ]
            }
        )
        self.assertEqual((fallback or {}).get("name"), "spark-by-name")

    async def test_probe_missing_auth_index_returns_early(self) -> None:
        session = _FakeSession([])
        result = await probe_wham_usage_async(
            session,
            asyncio.Semaphore(1),
            "https://example.com",
            "token",
            {"name": "a.json", "chatgpt_account_id": "acct-1"},
            timeout=3,
            retries=1,
            user_agent="ua",
            headers_builder=_headers_builder,
            backoff_seconds=lambda attempt: 0.0,
            now_iso=lambda: "2026-03-24T00:00:00+00:00",
            quota_signal_resolver=_quota_signal_resolver,
            logger=_DummyLogger(),
            sleep=_no_sleep,
        )
        self.assertEqual(result["probe_error_kind"], "missing_auth_index")
        self.assertEqual(result["probe_error_text"], "missing auth_index")
        self.assertEqual(len(session.calls), 0)

    async def test_probe_missing_account_id_returns_early(self) -> None:
        session = _FakeSession([])
        result = await probe_wham_usage_async(
            session,
            asyncio.Semaphore(1),
            "https://example.com",
            "token",
            {"name": "a.json", "auth_index": "idx-1"},
            timeout=3,
            retries=1,
            user_agent="ua",
            headers_builder=_headers_builder,
            backoff_seconds=lambda attempt: 0.0,
            now_iso=lambda: "2026-03-24T00:00:00+00:00",
            quota_signal_resolver=_quota_signal_resolver,
            logger=_DummyLogger(),
            sleep=_no_sleep,
        )
        self.assertEqual(result["probe_error_kind"], "missing_chatgpt_account_id")
        self.assertEqual(len(session.calls), 0)

    async def test_probe_429_retries_then_success(self) -> None:
        session = _FakeSession(
            [
                _FakeResponse(429, "{}"),
                _FakeResponse(
                    200,
                    '{"status_code":200,"body":{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"total":100,"remaining":40}}}}',
                ),
            ]
        )
        result = await probe_wham_usage_async(
            session,
            asyncio.Semaphore(1),
            "https://example.com",
            "token",
            {"name": "a.json", "auth_index": "idx-1", "chatgpt_account_id": "acct-1"},
            timeout=3,
            retries=1,
            user_agent="ua",
            headers_builder=_headers_builder,
            backoff_seconds=lambda attempt: 0.0,
            now_iso=lambda: "2026-03-24T00:00:00+00:00",
            quota_signal_resolver=_quota_signal_resolver,
            logger=_DummyLogger(),
            sleep=_no_sleep,
        )
        self.assertEqual(result["api_http_status"], 200)
        self.assertEqual(result["api_status_code"], 200)
        self.assertIsNone(result["probe_error_kind"])
        self.assertEqual(result.get("quota_signal_source"), "primary")
        self.assertEqual(len(session.calls), 2)

    async def test_probe_429_retries_exhausted(self) -> None:
        session = _FakeSession([_FakeResponse(429, "{}"), _FakeResponse(429, "{}")])
        result = await probe_wham_usage_async(
            session,
            asyncio.Semaphore(1),
            "https://example.com",
            "token",
            {"name": "a.json", "auth_index": "idx-1", "chatgpt_account_id": "acct-1"},
            timeout=3,
            retries=1,
            user_agent="ua",
            headers_builder=_headers_builder,
            backoff_seconds=lambda attempt: 0.0,
            now_iso=lambda: "2026-03-24T00:00:00+00:00",
            quota_signal_resolver=_quota_signal_resolver,
            logger=_DummyLogger(),
            sleep=_no_sleep,
        )
        self.assertEqual(result["api_http_status"], 429)
        self.assertEqual(result["probe_error_kind"], "management_api_http_429")
        self.assertEqual(result["probe_error_text"], "management api-call http 429")
        self.assertEqual(len(session.calls), 2)


if __name__ == "__main__":
    unittest.main()
