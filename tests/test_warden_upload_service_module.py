from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

from cwma.warden.services.upload import run_upload_async


class _DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        return None


class WardenUploadServiceModuleTests(unittest.IsolatedAsyncioTestCase):
    def _base_settings(self) -> dict[str, Any]:
        return {
            "upload_dir": "auth_files",
            "upload_recursive": False,
            "upload_workers": 4,
            "upload_retries": 1,
            "upload_method": "json",
            "upload_force": False,
            "base_url": "https://x.example",
            "token": "t",
            "timeout": 10,
            "debug": False,
        }

    async def test_run_upload_async_returns_empty_when_no_files(self) -> None:
        settings = self._base_settings()
        result = await run_upload_async(
            object(),
            settings,
            limit=None,
            resolve_upload_name_scope=lambda cfg: None,
            discover_upload_files=lambda upload_dir, recursive: [],
            validate_and_digest_json_file=lambda path: {},
            select_upload_candidates=lambda rows: ([], [], {}),
            summarize_upload_results=lambda *args, **kwargs: None,
            fetch_remote_auth_file_names=lambda base_url, token, timeout: set(),
            mark_upload_skipped_remote_exists=lambda conn, **kwargs: None,
            upload_auth_file_async=lambda *args, **kwargs: None,
            progress_log_step=lambda total: 1,
            progress_reporter_factory=lambda *args, **kwargs: None,
            logger=_DummyLogger(),
            dispatch_uploads=lambda *args, **kwargs: None,
        )
        self.assertEqual(result, {"results": []})

    async def test_run_upload_async_name_scope_and_remote_skip(self) -> None:
        settings = self._base_settings()
        summary_calls: list[dict[str, Any]] = []
        skipped_calls: list[dict[str, Any]] = []

        discovered = [Path("a.json"), Path("b.json")]
        validated = {
            "a.json": {
                "file_name": "a.json",
                "file_path": "a.json",
                "content_sha256": "sha-a",
                "file_size": 1,
                "content_bytes": b"{}",
                "content_text": "{}",
            },
            "b.json": {
                "file_name": "b.json",
                "file_path": "b.json",
                "content_sha256": "sha-b",
                "file_size": 1,
                "content_bytes": b"{}",
                "content_text": "{}",
            },
        }

        async def _dispatch(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        result = await run_upload_async(
            object(),
            settings,
            limit=None,
            resolve_upload_name_scope=lambda cfg: {"a.json"},
            discover_upload_files=lambda upload_dir, recursive: discovered,
            validate_and_digest_json_file=lambda path: dict(validated[path.name]),
            select_upload_candidates=lambda rows: (list(rows), [], {}),
            summarize_upload_results=lambda rows, **kwargs: summary_calls.append({"rows": list(rows), **kwargs}),
            fetch_remote_auth_file_names=lambda base_url, token, timeout: {"a.json"},
            mark_upload_skipped_remote_exists=lambda conn, **kwargs: skipped_calls.append(dict(kwargs)),
            upload_auth_file_async=lambda *args, **kwargs: None,
            progress_log_step=lambda total: 1,
            progress_reporter_factory=lambda *args, **kwargs: None,
            logger=_DummyLogger(),
            dispatch_uploads=_dispatch,
        )

        self.assertEqual(len(skipped_calls), 1)
        self.assertEqual(skipped_calls[0]["file_name"], "a.json")
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["outcome"], "skipped_remote_exists")
        self.assertEqual(len(summary_calls), 1)
        self.assertEqual(summary_calls[0]["to_upload_count"], 0)

    async def test_run_upload_async_raises_when_upload_failed_exists(self) -> None:
        settings = self._base_settings()
        settings["upload_force"] = True

        candidate = {
            "file_name": "a.json",
            "file_path": "a.json",
            "content_sha256": "sha-a",
            "file_size": 1,
            "content_bytes": b"{}",
            "content_text": "{}",
        }

        async def _dispatch(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return [
                {
                    "file_name": "a.json",
                    "file_path": "a.json",
                    "status_code": 500,
                    "ok": False,
                    "outcome": "upload_failed",
                    "error": "server error",
                    "error_kind": None,
                }
            ]

        with self.assertRaisesRegex(RuntimeError, "上传完成但存在失败文件"):
            await run_upload_async(
                object(),
                settings,
                limit=None,
                resolve_upload_name_scope=lambda cfg: None,
                discover_upload_files=lambda upload_dir, recursive: [Path("a.json")],
                validate_and_digest_json_file=lambda path: dict(candidate),
                select_upload_candidates=lambda rows: (list(rows), [], {}),
                summarize_upload_results=lambda *args, **kwargs: None,
                fetch_remote_auth_file_names=lambda base_url, token, timeout: set(),
                mark_upload_skipped_remote_exists=lambda conn, **kwargs: None,
                upload_auth_file_async=lambda *args, **kwargs: None,
                progress_log_step=lambda total: 1,
                progress_reporter_factory=lambda *args, **kwargs: None,
                logger=_DummyLogger(),
                dispatch_uploads=_dispatch,
            )


if __name__ == "__main__":
    unittest.main()
