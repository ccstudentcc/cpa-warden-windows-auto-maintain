from __future__ import annotations

import argparse
import json
import os
import time
import unittest
from pathlib import Path
from unittest import mock

from auto_maintain import AutoMaintainer, Settings, load_settings
from cwma.auto.infra.inprocess_supervisor import (
    poll_channel_exit as poll_inprocess_channel_exit,
    start_channel as start_inprocess_channel,
)
from cwma.auto.state.upload_queue import UploadQueueState

_ORIGINAL_TEMP_ENV: dict[str, str | None] = {}
_SANDBOX_TEMP_KEYS = ("TMPDIR", "TEMP", "TMP")


def setUpModule() -> None:
    sandbox_temp = (Path.cwd() / ".tmp_unittest_temp").resolve()
    sandbox_temp.mkdir(parents=True, exist_ok=True)
    global _ORIGINAL_TEMP_ENV
    _ORIGINAL_TEMP_ENV = {key: os.environ.get(key) for key in _SANDBOX_TEMP_KEYS}
    for key in _SANDBOX_TEMP_KEYS:
        os.environ[key] = str(sandbox_temp)


def tearDownModule() -> None:
    for key, value in _ORIGINAL_TEMP_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _build_settings(base_dir: Path, auth_dir: Path, *, inprocess: bool) -> Settings:
    state_dir = base_dir / "state"
    return Settings(
        base_dir=base_dir,
        watch_config_path=None,
        auth_dir=auth_dir,
        state_dir=state_dir,
        config_path=None,
        maintain_db_path=state_dir / "maintain.sqlite3",
        upload_db_path=state_dir / "upload.sqlite3",
        maintain_log_file=state_dir / "maintain.log",
        upload_log_file=state_dir / "upload.log",
        maintain_interval_seconds=3600,
        watch_interval_seconds=5,
        upload_stable_wait_seconds=0,
        upload_batch_size=100,
        smart_schedule_enabled=True,
        adaptive_upload_batching=True,
        upload_high_backlog_threshold=400,
        upload_high_backlog_batch_size=300,
        adaptive_maintain_batching=True,
        incremental_maintain_batch_size=120,
        maintain_high_backlog_threshold=300,
        maintain_high_backlog_batch_size=220,
        incremental_maintain_min_interval_seconds=20,
        incremental_maintain_full_guard_seconds=90,
        deep_scan_interval_loops=10,
        active_probe_interval_seconds=2,
        active_upload_deep_scan_interval_seconds=2,
        maintain_retry_count=0,
        upload_retry_count=0,
        command_retry_delay_seconds=1,
        run_maintain_on_start=False,
        run_upload_on_start=False,
        run_maintain_after_upload=False,
        maintain_assume_yes=False,
        delete_uploaded_files_after_upload=False,
        inspect_zip_files=False,
        auto_extract_zip_json=False,
        delete_zip_after_extract=False,
        bandizip_path="",
        bandizip_timeout_seconds=10,
        use_windows_zip_fallback=False,
        archive_extensions=(".zip", ".7z", ".rar"),
        bandizip_prefer_console=True,
        bandizip_hide_window=True,
        continue_on_command_failure=False,
        allow_multi_instance=True,
        run_once=False,
        inprocess_execution_enabled=inprocess,
    )


class AutoInprocessChannelTests(unittest.TestCase):
    def _workspace_temp_dir(self, prefix: str) -> Path:
        path = (Path.cwd() / ".tmp_unittest_temp" / f"{prefix}_{time.time_ns()}").resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    def test_start_inprocess_channel_and_poll_success(self) -> None:
        marks: list[str] = []
        start_result = start_inprocess_channel(
            channel="upload",
            command=["python", "cpa_warden.py", "--mode", "upload"],
            cwd=Path("."),
            env={},
            mark_channel_running=marks.append,
            command_runner=lambda **_kwargs: 0,
        )
        self.assertEqual(start_result.return_code, 0)
        self.assertEqual(marks, ["upload"])
        self.assertIsNotNone(start_result.process)

        poll_result = None
        for _ in range(50):
            poll_result = poll_inprocess_channel_exit(process=start_result.process)
            if poll_result.exited:
                break
            time.sleep(0.01)
        self.assertIsNotNone(poll_result)
        if poll_result is None:
            self.fail("expected poll result")
        self.assertTrue(poll_result.exited)
        self.assertEqual(poll_result.code, 0)
        self.assertIsNone(poll_result.process)

    def test_terminate_inprocess_channel_forces_shutdown_code(self) -> None:
        def _blocking_runner(**kwargs: object) -> int:
            cancel_requested = kwargs["cancel_requested"]
            if not hasattr(cancel_requested, "is_set"):
                self.fail("missing cancel_requested event")
            while not cancel_requested.is_set():
                time.sleep(0.01)
            return 0

        start_result = start_inprocess_channel(
            channel="maintain",
            command=["python", "cpa_warden.py", "--mode", "maintain"],
            cwd=Path("."),
            env={},
            command_runner=_blocking_runner,
        )
        self.assertIsNotNone(start_result.process)
        if start_result.process is None:
            self.fail("missing process handle")
        start_result.process.terminate()
        self.assertEqual(start_result.process.wait(timeout=1), 130)

    def test_load_settings_reads_inprocess_execution_enabled(self) -> None:
        tmp = self._workspace_temp_dir("inprocess_settings")
        watch_cfg = tmp / "watch.json"
        watch_cfg.write_text(
            json.dumps(
                {
                    "inprocess_execution_enabled": True,
                }
            ),
            encoding="utf-8",
        )
        args = argparse.Namespace(once=False, watch_config=str(watch_cfg))
        settings = load_settings(args)
        self.assertTrue(settings.inprocess_execution_enabled)

    def test_auto_maintainer_uses_inprocess_backend_when_enabled(self) -> None:
        base = self._workspace_temp_dir("inprocess_auto_maintainer")
        auth_dir = base / "auth"
        auth_dir.mkdir(parents=True, exist_ok=True)
        maintainer = AutoMaintainer(_build_settings(base, auth_dir, inprocess=True))
        start_fn = maintainer.channel_runtime_adapter.get_start_upload_channel()

        with mock.patch("auto_maintain.start_upload_channel", return_value=object()) as start_upload_mock:
            _ = start_fn(
                command=["python", "cpa_warden.py", "--mode", "upload"],
                cwd=base,
                state=UploadQueueState(
                    pending_snapshot=["a|1|1"],
                    pending_reason="queued",
                    pending_retry=False,
                    inflight_snapshot=None,
                    attempt=0,
                    retry_due_at=0.0,
                ),
                retry_count=0,
                retry_delay_seconds=1,
                env={},
                output_file=None,
                on_output_line=None,
                log=lambda _msg: None,
                mark_channel_running=lambda _channel: None,
                now_monotonic=0.0,
                popen_factory=mock.Mock(),
            )

        self.assertEqual(start_upload_mock.call_count, 1)
        kwargs = start_upload_mock.call_args.kwargs
        self.assertIs(kwargs["start_channel_impl"], start_inprocess_channel)


if __name__ == "__main__":
    unittest.main()
