from __future__ import annotations

import asyncio
import unittest
from typing import Any

from cwma.warden.api.management import (
    build_management_headers,
    delete_account_async,
    fetch_auth_files,
    fetch_remote_auth_file_names,
    safe_json_response,
    set_account_disabled_async,
)


class _FakeSyncResponse:
    def __init__(self, payload: Any, *, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http status={self.status_code}")


class _FakeAsyncResponse:
    def __init__(self, status: int, text_payload: str) -> None:
        self.status = status
        self._text_payload = text_payload

    async def text(self) -> str:
        return self._text_payload

    async def __aenter__(self) -> "_FakeAsyncResponse":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _FakeAsyncSession:
    def __init__(self, *, delete_plan: list[Any] | None = None, patch_plan: list[Any] | None = None) -> None:
        self.delete_plan = list(delete_plan or [])
        self.patch_plan = list(patch_plan or [])
        self.delete_calls = 0
        self.patch_calls = 0

    def delete(self, url: str, **kwargs: Any) -> _FakeAsyncResponse:
        self.delete_calls += 1
        if not self.delete_plan:
            raise RuntimeError("unexpected delete call")
        item = self.delete_plan.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def patch(self, url: str, **kwargs: Any) -> _FakeAsyncResponse:
        self.patch_calls += 1
        if not self.patch_plan:
            raise RuntimeError("unexpected patch call")
        item = self.patch_plan.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class WardenManagementApiModuleTests(unittest.IsolatedAsyncioTestCase):
    def test_build_management_headers_include_json(self) -> None:
        headers = build_management_headers("tok", include_json=True)
        self.assertIn("Authorization", headers)
        self.assertIn("Content-Type", headers)

    def test_safe_json_response_guards_invalid(self) -> None:
        self.assertEqual(safe_json_response(_FakeSyncResponse({"ok": True})), {"ok": True})
        self.assertEqual(safe_json_response(_FakeSyncResponse(["x"])), {})
        self.assertEqual(safe_json_response(_FakeSyncResponse(ValueError("bad json"))), {})

    def test_fetch_auth_files_uses_injected_get(self) -> None:
        captured: dict[str, Any] = {}

        def _fake_get(url: str, **kwargs: Any) -> _FakeSyncResponse:
            captured["url"] = url
            captured["kwargs"] = kwargs
            return _FakeSyncResponse({"files": [{"name": "a.json"}]})

        files = fetch_auth_files("https://x.example", "token", 10, request_get=_fake_get)
        self.assertEqual(files, [{"name": "a.json"}])
        self.assertIn("/v0/management/auth-files", str(captured["url"]))
        self.assertEqual(captured["kwargs"]["timeout"], 10)

    def test_fetch_remote_auth_file_names_supports_extractor(self) -> None:
        def _fetcher(base_url: str, token: str, timeout: int) -> list[dict[str, Any]]:
            return [{"id": "x"}, {"id": "x"}, {"id": "y"}, {"name": "z"}]

        names = fetch_remote_auth_file_names(
            "https://x.example",
            "token",
            10,
            fetcher=_fetcher,
            name_extractor=lambda row: str(row.get("id") or "").strip(),
        )
        self.assertEqual(names, {"x", "y"})

    async def test_delete_account_async_retries_then_success(self) -> None:
        session = _FakeAsyncSession(
            delete_plan=[
                _FakeAsyncResponse(429, '{"status":"busy"}'),
                _FakeAsyncResponse(200, '{"status":"ok"}'),
            ]
        )
        result = await delete_account_async(
            session,
            asyncio.Semaphore(1),
            "https://x.example",
            "token",
            "acc.json",
            10,
            2,
            maybe_json_loads=lambda text: {"status": "ok"} if "ok" in text else {"status": "busy"},
            compact_text=lambda text, limit: str(text)[:limit],
            backoff=lambda attempt: 0.0,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(session.delete_calls, 2)

    async def test_set_account_disabled_async_failure_payload(self) -> None:
        session = _FakeAsyncSession(patch_plan=[_FakeAsyncResponse(500, "server error")])
        result = await set_account_disabled_async(
            session,
            asyncio.Semaphore(1),
            "https://x.example",
            "token",
            "acc.json",
            True,
            10,
            maybe_json_loads=lambda text: None,
            compact_text=lambda text, limit: str(text)[:limit],
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status_code"], 500)


if __name__ == "__main__":
    unittest.main()
