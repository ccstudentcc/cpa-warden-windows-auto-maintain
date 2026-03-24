from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from auto_maintain import AutoMaintainer, Settings, load_settings
from cwma.auto.channel_status import CHANNEL_UPLOAD
from cwma.auto.runtime.startup_runtime import StartupRuntimeResult, StartupRuntimeState
from cwma.auto.runtime.watch_runtime import WatchRuntimeResult, WatchRuntimeState
from cwma.auto.snapshots import compute_uploaded_baseline as compute_uploaded_baseline_rows


class _DoneProcess:
    def __init__(self, code: int = 0) -> None:
        self._code = code
        self.pid = 99999

    def poll(self) -> int:
        return self._code


def _build_settings(base_dir: Path, auth_dir: Path) -> Settings:
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
        continue_on_command_failure=False,
        allow_multi_instance=True,
        run_once=False,
    )


class AutoMaintainTests(unittest.TestCase):
    def test_run_startup_phase_applies_runtime_result_state_writeback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            maintainer = AutoMaintainer(_build_settings(base, auth_dir))
            maintainer.last_json_count = 7
            maintainer.last_zip_signature = ("before.zip|1|1",)
            maintainer.next_maintain_due_at = 33.0

            runtime_result = StartupRuntimeResult(
                exit_code=5,
                state=StartupRuntimeState(
                    last_json_count=21,
                    last_zip_signature=("after.zip|2|2",),
                    next_maintain_due_at=44.0,
                ),
            )

            with mock.patch("auto_maintain.run_startup_cycle", return_value=runtime_result) as runtime_call:
                code = maintainer._run_startup_phase()

            self.assertEqual(code, 5)
            call_state = runtime_call.call_args.kwargs["state"]
            self.assertEqual(call_state.last_json_count, 7)
            self.assertEqual(call_state.last_zip_signature, ("before.zip|1|1",))
            self.assertEqual(call_state.next_maintain_due_at, 33.0)
            self.assertEqual(maintainer.last_json_count, 21)
            self.assertEqual(maintainer.last_zip_signature, ("after.zip|2|2",))
            self.assertEqual(maintainer.next_maintain_due_at, 44.0)
            self.assertEqual(maintainer.runtime.snapshot.last_json_count, 21)
            self.assertEqual(maintainer.runtime.snapshot.last_zip_signature, ("after.zip|2|2",))
            self.assertEqual(maintainer.runtime.lifecycle.next_maintain_due_at, 44.0)

    def test_run_watch_iteration_applies_runtime_result_state_writeback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            maintainer = AutoMaintainer(_build_settings(base, auth_dir))
            maintainer.next_maintain_due_at = 99.0

            runtime_result = WatchRuntimeResult(
                exit_code=8,
                state=WatchRuntimeState(next_maintain_due_at=123.0),
            )
            with mock.patch("auto_maintain.run_watch_iteration", return_value=runtime_result) as runtime_call:
                code = maintainer._run_watch_iteration(now=77.0)

            self.assertEqual(code, 8)
            self.assertEqual(runtime_call.call_args.kwargs["now_monotonic"], 77.0)
            call_state = runtime_call.call_args.kwargs["state"]
            self.assertEqual(call_state.next_maintain_due_at, 99.0)
            self.assertEqual(maintainer.next_maintain_due_at, 123.0)
            self.assertEqual(maintainer.runtime.lifecycle.next_maintain_due_at, 123.0)

    def test_check_and_maybe_upload_delegates_to_upload_scan_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            maintainer = AutoMaintainer(_build_settings(base, auth_dir))

            with mock.patch("auto_maintain.run_upload_scan_cycle", return_value=6) as runtime_call:
                code = maintainer.check_and_maybe_upload(
                    force_deep_scan=True,
                    preserve_retry_state=True,
                    skip_stability_wait=True,
                    queue_reason="guardrail-check",
                )

            self.assertEqual(code, 6)
            kwargs = runtime_call.call_args.kwargs
            self.assertTrue(kwargs["force_deep_scan"])
            self.assertTrue(kwargs["preserve_retry_state"])
            self.assertTrue(kwargs["skip_stability_wait"])
            self.assertEqual(kwargs["queue_reason"], "guardrail-check")
            for key in (
                "current_upload_scan_inputs",
                "should_run_upload_deep_scan",
                "refresh_upload_scan_inputs_after_zip_if_needed",
                "build_current_snapshot",
                "read_last_uploaded_snapshot",
                "handle_upload_no_changes_detected",
                "resolve_stable_upload_snapshot",
                "compute_pending_upload_snapshot",
                "handle_upload_no_pending_discovered",
                "queue_pending_upload_snapshot",
            ):
                self.assertTrue(callable(kwargs[key]), key)

    def test_probe_changes_during_active_upload_delegates_to_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            maintainer = AutoMaintainer(_build_settings(base, auth_dir))

            maintainer.upload_process = _DoneProcess(0)
            with mock.patch("auto_maintain.run_active_upload_probe_cycle", return_value=9) as runtime_call:
                code = maintainer.probe_changes_during_active_upload()

            self.assertEqual(code, 9)
            kwargs = runtime_call.call_args.kwargs
            self.assertTrue(kwargs["upload_running"])
            for key in (
                "collect_active_upload_probe_inputs",
                "decide_active_upload_probe",
                "apply_active_upload_probe_state",
                "log_active_upload_source_change_if_needed",
                "refresh_upload_queue_during_active_upload",
            ):
                self.assertTrue(callable(kwargs[key]), key)

    def test_upload_queue_state_adapter_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            maintainer = AutoMaintainer(_build_settings(base, auth_dir))

            maintainer.pending_upload_snapshot = ["a|1|1"]
            maintainer.pending_upload_reason = "queued"
            maintainer.pending_upload_retry = True
            maintainer.inflight_upload_snapshot = ["a|1|1"]
            maintainer.upload_attempt = 2
            maintainer.upload_retry_due_at = 123.0

            state = maintainer._upload_queue_state()
            self.assertEqual(state.pending_snapshot, ["a|1|1"])
            self.assertTrue(state.pending_retry)
            self.assertEqual(state.attempt, 2)

            state.pending_snapshot = ["b|2|2"]
            state.pending_reason = "next"
            state.pending_retry = False
            state.inflight_snapshot = None
            state.attempt = 0
            state.retry_due_at = 0.0
            maintainer._apply_upload_queue_state(state)

            self.assertEqual(maintainer.pending_upload_snapshot, ["b|2|2"])
            self.assertEqual(maintainer.pending_upload_reason, "next")
            self.assertFalse(maintainer.pending_upload_retry)
            self.assertIsNone(maintainer.inflight_upload_snapshot)
            self.assertEqual(maintainer.upload_attempt, 0)
            self.assertEqual(maintainer.upload_retry_due_at, 0.0)
            self.assertEqual(maintainer.runtime.upload.queue.pending_snapshot, ["b|2|2"])
            self.assertEqual(maintainer.runtime.upload.queue.pending_reason, "next")
            self.assertFalse(maintainer.runtime.upload.queue.pending_retry)
            self.assertIsNone(maintainer.runtime.upload.queue.inflight_snapshot)
            self.assertEqual(maintainer.runtime.upload.queue.attempt, 0)
            self.assertEqual(maintainer.runtime.upload.queue.retry_due_at, 0.0)

    def test_maintain_runtime_state_adapter_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            maintainer = AutoMaintainer(_build_settings(base, auth_dir))

            maintainer.pending_maintain = True
            maintainer.pending_maintain_reason = "post-upload"
            maintainer.pending_maintain_names = {"a.json"}
            maintainer.inflight_maintain_names = {"b.json"}
            maintainer.maintain_attempt = 3
            maintainer.maintain_retry_due_at = 77.0

            runtime = maintainer._maintain_runtime_state()
            self.assertTrue(runtime.queue.pending)
            self.assertEqual(runtime.queue.names, {"a.json"})
            self.assertEqual(runtime.inflight_names, {"b.json"})
            self.assertEqual(runtime.attempt, 3)

            runtime.queue.pending = False
            runtime.queue.reason = None
            runtime.queue.names = None
            runtime.inflight_names = None
            runtime.attempt = 0
            runtime.retry_due_at = 0.0
            maintainer._apply_maintain_runtime_state(runtime)

            self.assertFalse(maintainer.pending_maintain)
            self.assertIsNone(maintainer.pending_maintain_reason)
            self.assertIsNone(maintainer.pending_maintain_names)
            self.assertIsNone(maintainer.inflight_maintain_names)
            self.assertEqual(maintainer.maintain_attempt, 0)
            self.assertEqual(maintainer.maintain_retry_due_at, 0.0)
            self.assertFalse(maintainer.runtime.maintain.queue.pending)
            self.assertIsNone(maintainer.runtime.maintain.queue.reason)
            self.assertIsNone(maintainer.runtime.maintain.queue.names)
            self.assertIsNone(maintainer.runtime.maintain.inflight_names)
            self.assertEqual(maintainer.runtime.maintain.attempt, 0)
            self.assertEqual(maintainer.runtime.maintain.retry_due_at, 0.0)

    def test_compute_uploaded_baseline_keeps_only_uploaded_and_still_existing(self) -> None:
        existing_baseline = ["legacy|0|0", "a|1|1"]
        uploaded_snapshot = ["a|1|1", "b|2|2"]
        current_snapshot = ["a|1|1", "c|3|3", "legacy|0|0"]
        baseline = compute_uploaded_baseline_rows(existing_baseline, uploaded_snapshot, current_snapshot)
        self.assertEqual(baseline, ["a|1|1", "legacy|0|0"])

    def test_poll_upload_process_queues_next_batch_for_new_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)

            old_file = auth_dir / "old.json"
            old_file.write_text('{"v": 1}', encoding="utf-8")

            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)
            settings.state_dir.mkdir(parents=True, exist_ok=True)
            settings.maintain_db_path.parent.mkdir(parents=True, exist_ok=True)
            settings.upload_db_path.parent.mkdir(parents=True, exist_ok=True)

            uploaded_snapshot = maintainer.snapshot_lines()
            self.assertEqual(len(uploaded_snapshot), 1)

            new_file = auth_dir / "new.json"
            new_file.write_text('{"v": 2}', encoding="utf-8")

            maintainer.upload_process = _DoneProcess(0)
            maintainer.inflight_upload_snapshot = uploaded_snapshot
            result = maintainer.poll_upload_process()

            self.assertEqual(result, 0)
            self.assertIsNotNone(maintainer.pending_upload_snapshot)
            queued_snapshot = maintainer.pending_upload_snapshot or []
            self.assertTrue(any(str(new_file) in row for row in queued_snapshot))

            uploaded_file_snapshot = maintainer.read_snapshot(maintainer.last_uploaded_snapshot_file)
            self.assertEqual(uploaded_file_snapshot, uploaded_snapshot)

    def test_maybe_start_upload_uses_batch_size_and_scope_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.upload_batch_size = 2
            maintainer = AutoMaintainer(settings)

            batch_snapshot = [
                f"{auth_dir / 'a.json'}|1|1",
                f"{auth_dir / 'b.json'}|1|1",
                f"{auth_dir / 'c.json'}|1|1",
            ]
            maintainer.pending_upload_snapshot = list(batch_snapshot)
            maintainer.pending_upload_reason = "detected JSON changes"

            with mock.patch("auto_maintain.subprocess.Popen", return_value=_DoneProcess(0)) as popen:
                result = maintainer.maybe_start_upload()

            self.assertEqual(result, 0)
            self.assertIsNotNone(maintainer.upload_process)
            self.assertEqual(maintainer.inflight_upload_snapshot, batch_snapshot[:2])
            cmd = popen.call_args.args[0]
            self.assertIn("--upload-names-file", cmd)
            scope_path = Path(cmd[cmd.index("--upload-names-file") + 1])
            self.assertTrue(scope_path.exists())
            scoped_names = set(scope_path.read_text(encoding="utf-8").splitlines())
            self.assertEqual(scoped_names, {"a.json", "b.json"})

    def test_maybe_start_upload_scales_batch_size_for_high_backlog(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.upload_batch_size = 2
            settings.upload_high_backlog_threshold = 6
            settings.upload_high_backlog_batch_size = 8
            maintainer = AutoMaintainer(settings)

            maintainer.pending_maintain = True
            maintainer.pending_upload_snapshot = [
                f"{auth_dir / f'{idx}.json'}|1|1"
                for idx in range(10)
            ]
            maintainer.pending_upload_reason = "detected JSON changes"

            with mock.patch("auto_maintain.subprocess.Popen", return_value=_DoneProcess(0)):
                result = maintainer.maybe_start_upload()

            self.assertEqual(result, 0)
            self.assertIsNotNone(maintainer.inflight_upload_snapshot)
            self.assertEqual(len(maintainer.inflight_upload_snapshot or []), 8)

    def test_maybe_start_upload_passes_env_and_monotonic_to_runtime_channel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            maintainer.pending_upload_snapshot = [f"{auth_dir / 'a.json'}|1|1"]
            maintainer.pending_upload_reason = "detected JSON changes"
            captured: dict[str, object] = {}
            flow = mock.Mock(
                state=maintainer._upload_queue_state(),
                process=_DoneProcess(0),
                status="success",
                start_exception=None,
                return_code=0,
            )

            def _stub_start_upload_channel(**kwargs: object) -> object:
                captured.update(kwargs)
                return flow

            with mock.patch(
                "auto_maintain.start_upload_channel",
                side_effect=_stub_start_upload_channel,
            ) as runtime_call, mock.patch(
                "auto_maintain.build_child_process_env",
                return_value={"AUTO_TEST_ENV": "1"},
            ) as env_builder, mock.patch(
                "auto_maintain.time.monotonic",
                side_effect=[100.0, 101.5],
            ):
                result = maintainer.maybe_start_upload()

            self.assertEqual(result, 0)
            self.assertEqual(runtime_call.call_count, 1)
            env_builder.assert_called_once()
            self.assertEqual(captured["env"], {"AUTO_TEST_ENV": "1"})
            self.assertEqual(captured["now_monotonic"], 101.5)
            self.assertEqual(captured["cwd"], base)
            self.assertIs(maintainer.upload_process, flow.process)

    def test_poll_upload_process_keeps_existing_uploaded_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)

            file_a = auth_dir / "a.json"
            file_b = auth_dir / "b.json"
            file_c = auth_dir / "c.json"
            file_a.write_text("{}", encoding="utf-8")
            file_b.write_text("{}", encoding="utf-8")
            file_c.write_text("{}", encoding="utf-8")

            settings = _build_settings(base, auth_dir)
            settings.upload_batch_size = 2
            maintainer = AutoMaintainer(settings)
            settings.state_dir.mkdir(parents=True, exist_ok=True)

            snapshot = maintainer.snapshot_lines()
            baseline_before = [snapshot[0]]
            maintainer.write_snapshot(maintainer.last_uploaded_snapshot_file, baseline_before)

            maintainer.pending_upload_snapshot = list(snapshot)
            maintainer.upload_process = _DoneProcess(0)
            maintainer.inflight_upload_snapshot = snapshot[1:]

            result = maintainer.poll_upload_process()

            self.assertEqual(result, 0)
            baseline_after = maintainer.read_snapshot(maintainer.last_uploaded_snapshot_file)
            self.assertEqual(baseline_after, snapshot)

    def test_maybe_start_maintain_defers_incremental_by_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.incremental_maintain_min_interval_seconds = 60
            settings.incremental_maintain_full_guard_seconds = 0
            maintainer = AutoMaintainer(settings)

            maintainer.pending_maintain = True
            maintainer.pending_maintain_reason = "post-upload maintain"
            maintainer.pending_maintain_names = {"a.json"}
            maintainer.last_incremental_maintain_started_at = 995.0

            with mock.patch("auto_maintain.subprocess.Popen") as popen, mock.patch(
                "auto_maintain.time.monotonic",
                return_value=1000.0,
            ):
                result = maintainer.maybe_start_maintain()

            self.assertEqual(result, 0)
            self.assertIsNone(maintainer.maintain_process)
            self.assertTrue(maintainer.pending_maintain)
            self.assertEqual(popen.call_count, 0)

    def test_maybe_start_maintain_defers_incremental_when_full_due_soon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.incremental_maintain_min_interval_seconds = 0
            settings.incremental_maintain_full_guard_seconds = 120
            maintainer = AutoMaintainer(settings)

            maintainer.pending_maintain = True
            maintainer.pending_maintain_reason = "post-upload maintain"
            maintainer.pending_maintain_names = {"a.json"}
            maintainer.next_maintain_due_at = 1030.0

            with mock.patch("auto_maintain.subprocess.Popen") as popen, mock.patch(
                "auto_maintain.time.monotonic",
                return_value=1000.0,
            ):
                result = maintainer.maybe_start_maintain()

            self.assertEqual(result, 0)
            self.assertIsNone(maintainer.maintain_process)
            self.assertTrue(maintainer.pending_maintain)
            self.assertEqual(popen.call_count, 0)

    def test_check_and_maybe_upload_invokes_snapshot_seams(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            maintainer = AutoMaintainer(_build_settings(base, auth_dir))
            observed: dict[str, list[str]] = {}

            def _stub_run_upload_scan_cycle(**kwargs: object) -> int:
                build_current_snapshot = kwargs["build_current_snapshot"]
                read_last_uploaded_snapshot = kwargs["read_last_uploaded_snapshot"]
                if not callable(build_current_snapshot) or not callable(read_last_uploaded_snapshot):
                    self.fail("snapshot seam callbacks must be callable")
                observed["current"] = build_current_snapshot()  # type: ignore[misc]
                observed["uploaded"] = read_last_uploaded_snapshot()  # type: ignore[misc]
                return 0

            with mock.patch.object(maintainer, "build_snapshot", return_value=["cur|1|1"]) as build_snapshot, mock.patch.object(
                maintainer, "read_snapshot", return_value=["base|1|1"]
            ) as read_snapshot, mock.patch(
                "auto_maintain.run_upload_scan_cycle",
                side_effect=_stub_run_upload_scan_cycle,
            ) as runtime_call:
                result = maintainer.check_and_maybe_upload(force_deep_scan=True)

            self.assertEqual(result, 0)
            self.assertEqual(runtime_call.call_count, 1)
            build_snapshot.assert_called_once_with(maintainer.current_snapshot_file)
            read_snapshot.assert_called_once_with(maintainer.last_uploaded_snapshot_file)
            self.assertEqual(observed["current"], ["cur|1|1"])
            self.assertEqual(observed["uploaded"], ["base|1|1"])

    def test_snapshot_lines_tolerates_transient_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)

            existing = auth_dir / "ok.json"
            existing.write_text("{}", encoding="utf-8")
            missing = auth_dir / "missing.json"

            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            with mock.patch.object(maintainer, "get_json_paths", return_value=[existing, missing]):
                lines = maintainer.snapshot_lines()

            self.assertEqual(len(lines), 1)
            self.assertIn(str(existing), lines[0])

    def test_inspect_zip_archives_detects_nested_json_recursively(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            zip_path = auth_dir / "nested.zip"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("nested/dir/a.json", "{}")
                archive.writestr("nested/dir/readme.txt", "x")

            settings = _build_settings(base, auth_dir)
            settings.inspect_zip_files = True
            settings.auto_extract_zip_json = True
            settings.delete_zip_after_extract = False
            maintainer = AutoMaintainer(settings)

            with mock.patch.object(maintainer, "extract_zip_with_bandizip", return_value=0) as extract:
                changed_first = maintainer.inspect_zip_archives()
                changed_second = maintainer.inspect_zip_archives()

            self.assertTrue(changed_first)
            self.assertFalse(changed_second)
            self.assertEqual(extract.call_count, 1)

    def test_inspect_zip_archives_reprocesses_when_zip_signature_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            zip_path = auth_dir / "nested.zip"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("nested/dir/a.json", "{}")

            settings = _build_settings(base, auth_dir)
            settings.inspect_zip_files = True
            settings.auto_extract_zip_json = True
            settings.delete_zip_after_extract = False
            maintainer = AutoMaintainer(settings)

            with mock.patch.object(maintainer, "extract_zip_with_bandizip", return_value=0) as extract:
                first = maintainer.inspect_zip_archives()
                with zipfile.ZipFile(zip_path, "w") as archive:
                    archive.writestr("nested/dir/a.json", "{}")
                    archive.writestr("nested/dir/b.json", '{"v":2}')
                second = maintainer.inspect_zip_archives()

            self.assertTrue(first)
            self.assertTrue(second)
            self.assertEqual(extract.call_count, 2)

    def test_build_maintain_command_respects_assume_yes_switch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            without_yes = maintainer.build_maintain_command()
            self.assertNotIn("--yes", without_yes)

            settings.maintain_assume_yes = True
            with_yes = maintainer.build_maintain_command()
            self.assertIn("--yes", with_yes)

            scoped = maintainer.build_maintain_command(base / "scope.txt")
            self.assertIn("--maintain-names-file", scoped)
            self.assertIn(str(base / "scope.txt"), scoped)

    def test_delete_uploaded_files_prunes_empty_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            nested = auth_dir / "x" / "y"
            nested.mkdir(parents=True, exist_ok=True)
            target = nested / "delete_me.json"
            target.write_text("{}", encoding="utf-8")

            keep_dir = auth_dir / "keep"
            keep_dir.mkdir(parents=True, exist_ok=True)
            (keep_dir / "keep.json").write_text("{}", encoding="utf-8")

            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            stat = target.stat()
            snapshot_lines = [f"{target}|{stat.st_size}|{stat.st_mtime_ns}"]
            maintainer.delete_uploaded_files_from_snapshot(snapshot_lines)

            self.assertFalse(target.exists())
            self.assertFalse(nested.exists())
            self.assertFalse((auth_dir / "x").exists())
            self.assertTrue(keep_dir.exists())
            self.assertTrue(auth_dir.exists())

    def test_queue_maintain_merges_incremental_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            maintainer.queue_maintain("post-upload maintain", names={"a.json", "b.json"})
            maintainer.queue_maintain("post-upload maintain", names={"b.json", "c.json"})

            self.assertTrue(maintainer.pending_maintain)
            self.assertEqual(maintainer.pending_maintain_names, {"a.json", "b.json", "c.json"})

    def test_queue_full_maintain_overrides_incremental_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            maintainer.queue_maintain("post-upload maintain", names={"a.json"})
            maintainer.queue_maintain("scheduled maintain")

            self.assertTrue(maintainer.pending_maintain)
            self.assertIsNone(maintainer.pending_maintain_names)

    def test_poll_upload_process_queues_incremental_maintain_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            (auth_dir / "a.json").write_text("{}", encoding="utf-8")
            (auth_dir / "b.json").write_text("{}", encoding="utf-8")

            settings = _build_settings(base, auth_dir)
            settings.run_maintain_after_upload = True
            maintainer = AutoMaintainer(settings)
            settings.state_dir.mkdir(parents=True, exist_ok=True)

            uploaded_snapshot = maintainer.snapshot_lines()
            maintainer.upload_process = _DoneProcess(0)
            maintainer.inflight_upload_snapshot = uploaded_snapshot

            result = maintainer.poll_upload_process()

            self.assertEqual(result, 0)
            self.assertTrue(maintainer.pending_maintain)
            self.assertEqual(maintainer.pending_maintain_names, {"a.json", "b.json"})

    def test_probe_changes_during_active_upload_marks_follow_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.inspect_zip_files = True
            maintainer = AutoMaintainer(settings)

            maintainer.upload_process = _DoneProcess(0)
            maintainer.last_json_count = 10
            maintainer.last_zip_signature = ("old.zip|1|1",)

            with mock.patch.object(maintainer, "get_json_count", return_value=12), mock.patch.object(
                maintainer, "get_zip_signature", return_value=("new.zip|2|2",)
            ), mock.patch.object(maintainer, "check_and_maybe_upload", return_value=0) as refresh:
                result = maintainer.probe_changes_during_active_upload()

            self.assertEqual(result, 0)
            self.assertTrue(maintainer.pending_source_changes_during_upload)
            self.assertEqual(maintainer.last_json_count, 12)
            self.assertEqual(maintainer.last_zip_signature, ("new.zip|2|2",))
            refresh.assert_called_once_with(
                force_deep_scan=True,
                preserve_retry_state=True,
                skip_stability_wait=True,
                queue_reason="active-upload source changes",
            )

    def test_poll_upload_process_runs_forced_check_after_active_upload_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            (auth_dir / "a.json").write_text("{}", encoding="utf-8")

            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)
            settings.state_dir.mkdir(parents=True, exist_ok=True)

            maintainer.upload_process = _DoneProcess(0)
            maintainer.inflight_upload_snapshot = maintainer.snapshot_lines()
            maintainer.pending_source_changes_during_upload = True

            with mock.patch.object(maintainer, "check_and_maybe_upload", return_value=0) as follow_up:
                result = maintainer.poll_upload_process()

            self.assertEqual(result, 0)
            self.assertFalse(maintainer.pending_source_changes_during_upload)
            follow_up.assert_called_once_with(force_deep_scan=True)

    def test_maybe_start_maintain_slices_incremental_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.incremental_maintain_batch_size = 2
            settings.maintain_high_backlog_threshold = 4
            settings.maintain_high_backlog_batch_size = 2
            maintainer = AutoMaintainer(settings)

            maintainer.pending_maintain = True
            maintainer.pending_maintain_reason = "post-upload maintain"
            maintainer.pending_maintain_names = {"a.json", "b.json", "c.json", "d.json"}

            with mock.patch("auto_maintain.subprocess.Popen", return_value=_DoneProcess(0)):
                result = maintainer.maybe_start_maintain()

            self.assertEqual(result, 0)
            self.assertIsNotNone(maintainer.maintain_process)
            self.assertEqual(len(maintainer.inflight_maintain_names or set()), 2)
            self.assertTrue(maintainer.pending_maintain)
            self.assertEqual(len(maintainer.pending_maintain_names or set()), 2)

    def test_can_start_maintain_while_upload_channel_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            maintainer.upload_process = _DoneProcess(0)
            maintainer.queue_maintain("scheduled maintain")

            with mock.patch("auto_maintain.subprocess.Popen", return_value=_DoneProcess(0)) as popen:
                result = maintainer.maybe_start_maintain()

            self.assertEqual(result, 0)
            self.assertIsNotNone(maintainer.maintain_process)
            self.assertEqual(popen.call_count, 1)
            cmd = popen.call_args.args[0]
            self.assertIn("--mode", cmd)
            self.assertIn("maintain", cmd)
            self.assertIn("stdout", popen.call_args.kwargs)
            self.assertIn("stderr", popen.call_args.kwargs)

    def test_can_start_upload_while_maintain_channel_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            maintainer.maintain_process = _DoneProcess(0)
            maintainer.pending_upload_snapshot = ["dummy|1|1"]
            maintainer.pending_upload_reason = "detected JSON changes"

            with mock.patch("auto_maintain.subprocess.Popen", return_value=_DoneProcess(0)) as popen:
                result = maintainer.maybe_start_upload()

            self.assertEqual(result, 0)
            self.assertIsNotNone(maintainer.upload_process)
            self.assertEqual(popen.call_count, 1)
            cmd = popen.call_args.args[0]
            self.assertIn("--mode", cmd)
            self.assertIn("upload", cmd)
            self.assertIn("stdout", popen.call_args.kwargs)
            self.assertIn("stderr", popen.call_args.kwargs)

    def test_load_settings_reads_watch_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            watch_cfg = Path(tmp) / "watch.json"
            watch_cfg.write_text(
                json.dumps(
                    {
                        "watch_interval_seconds": 21,
                        "upload_stable_wait_seconds": 3,
                        "upload_batch_size": 7,
                        "smart_schedule_enabled": False,
                        "adaptive_upload_batching": False,
                        "upload_high_backlog_threshold": 33,
                        "upload_high_backlog_batch_size": 22,
                        "incremental_maintain_min_interval_seconds": 9,
                        "incremental_maintain_full_guard_seconds": 8,
                        "run_upload_on_start": False,
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(once=False, watch_config=str(watch_cfg))
            with mock.patch.dict(os.environ, {}, clear=True):
                settings = load_settings(args)
            self.assertEqual(settings.watch_interval_seconds, 21)
            self.assertEqual(settings.upload_stable_wait_seconds, 3)
            self.assertEqual(settings.upload_batch_size, 7)
            self.assertFalse(settings.smart_schedule_enabled)
            self.assertFalse(settings.adaptive_upload_batching)
            self.assertEqual(settings.upload_high_backlog_threshold, 33)
            self.assertEqual(settings.upload_high_backlog_batch_size, 22)
            self.assertEqual(settings.incremental_maintain_min_interval_seconds, 9)
            self.assertEqual(settings.incremental_maintain_full_guard_seconds, 8)
            self.assertFalse(settings.run_upload_on_start)
            self.assertEqual(settings.watch_config_path, watch_cfg)

    def test_load_settings_env_overrides_watch_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            watch_cfg = Path(tmp) / "watch.json"
            watch_cfg.write_text(
                json.dumps(
                    {
                        "watch_interval_seconds": 30,
                        "upload_batch_size": 5,
                        "smart_schedule_enabled": False,
                        "adaptive_upload_batching": False,
                        "upload_high_backlog_threshold": 10,
                        "upload_high_backlog_batch_size": 10,
                        "incremental_maintain_min_interval_seconds": 10,
                        "incremental_maintain_full_guard_seconds": 10,
                        "run_upload_on_start": False,
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(once=False, watch_config=str(watch_cfg))
            with mock.patch.dict(
                os.environ,
                {
                    "WATCH_INTERVAL_SECONDS": "11",
                    "UPLOAD_BATCH_SIZE": "13",
                    "SMART_SCHEDULE_ENABLED": "1",
                    "ADAPTIVE_UPLOAD_BATCHING": "1",
                    "UPLOAD_HIGH_BACKLOG_THRESHOLD": "44",
                    "UPLOAD_HIGH_BACKLOG_BATCH_SIZE": "55",
                    "INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS": "77",
                    "INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS": "88",
                    "RUN_UPLOAD_ON_START": "1",
                },
                clear=True,
            ):
                settings = load_settings(args)
            self.assertEqual(settings.watch_interval_seconds, 11)
            self.assertEqual(settings.upload_batch_size, 13)
            self.assertTrue(settings.smart_schedule_enabled)
            self.assertTrue(settings.adaptive_upload_batching)
            self.assertEqual(settings.upload_high_backlog_threshold, 44)
            self.assertEqual(settings.upload_high_backlog_batch_size, 55)
            self.assertEqual(settings.incremental_maintain_min_interval_seconds, 77)
            self.assertEqual(settings.incremental_maintain_full_guard_seconds, 88)
            self.assertTrue(settings.run_upload_on_start)

    def test_render_progress_snapshot_includes_queue_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            maintainer.progress_render_interval_seconds = 0
            maintainer.pending_upload_snapshot = [
                f"{auth_dir / 'a.json'}|1|1",
                f"{auth_dir / 'b.json'}|1|1",
                f"{auth_dir / 'c.json'}|1|1",
            ]
            maintainer.inflight_upload_snapshot = [
                f"{auth_dir / 'a.json'}|1|1",
            ]
            maintainer.pending_maintain = True
            maintainer.pending_maintain_reason = "post-upload maintain"
            maintainer.pending_maintain_names = {"a.json", "b.json"}

            with mock.patch("auto_maintain.log") as log_mock:
                maintainer.render_progress_snapshot(force=True)

            lines = [str(call.args[0]) for call in log_mock.call_args_list]
            upload_lines = [line for line in lines if line.startswith("UPLOAD   ")]
            maintain_lines = [line for line in lines if line.startswith("MAINTAIN ")]
            upload_queue_lines = [line for line in lines if "queue_files=" in line]
            maintain_queue_lines = [line for line in lines if "queue_full=" in line]
            header_lines = [line for line in lines if line.startswith("CPA Warden Auto Dashboard")]
            self.assertEqual(len(header_lines), 1)
            self.assertEqual(len(upload_lines), 1)
            self.assertEqual(len(maintain_lines), 1)
            self.assertEqual(len(upload_queue_lines), 1)
            self.assertEqual(len(maintain_queue_lines), 1)
            self.assertIn("queue_files=3", upload_queue_lines[0])
            self.assertIn("queue_batches=1", upload_queue_lines[0])
            self.assertIn("next_batch=3", upload_queue_lines[0])
            self.assertIn("inflight=1", upload_queue_lines[0])
            self.assertIn("queue_full=0", maintain_queue_lines[0])
            self.assertIn("queue_incremental=2", maintain_queue_lines[0])

    def test_render_progress_snapshot_skips_duplicate_panel_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)
            maintainer.progress_render_interval_seconds = 0
            maintainer.progress_render_heartbeat_seconds = 999

            with mock.patch("auto_maintain.log") as log_mock:
                maintainer.render_progress_snapshot(force=True)
                first_count = log_mock.call_count
                maintainer.render_progress_snapshot()

            self.assertEqual(first_count, maintainer.panel_height)
            self.assertEqual(log_mock.call_count, maintainer.panel_height)

    def test_update_channel_progress_triggers_render_and_syncs_runtime_ui(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            with mock.patch.object(
                maintainer.ui_runtime,
                "on_stage_update",
                wraps=maintainer.ui_runtime.on_stage_update,
            ) as on_stage_update:
                maintainer.update_channel_progress(
                    CHANNEL_UPLOAD,
                    stage="upload",
                    done=5,
                    total=12,
                    force_render=True,
                )

            on_stage_update.assert_called_once_with(
                CHANNEL_UPLOAD,
                stage="upload",
                done=5,
                total=12,
                force_render=True,
            )
            self.assertEqual(maintainer.upload_progress_state["stage"], "upload")
            self.assertEqual(maintainer.upload_progress_state["done"], 5)
            self.assertEqual(maintainer.upload_progress_state["total"], 12)
            self.assertEqual(maintainer.runtime.ui.upload_progress_state["stage"], "upload")
            self.assertEqual(maintainer.runtime.ui.upload_progress_state["done"], 5)
            self.assertEqual(maintainer.runtime.ui.upload_progress_state["total"], 12)

    def test_render_progress_snapshot_syncs_runtime_ui_signature_and_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)
            maintainer.progress_render_interval_seconds = 0

            with mock.patch("auto_maintain.log"), mock.patch(
                "auto_maintain.time.monotonic",
                return_value=321.0,
            ), mock.patch.object(
                maintainer.ui_runtime,
                "render_if_needed",
                wraps=maintainer.ui_runtime.render_if_needed,
            ) as render_if_needed:
                maintainer.render_progress_snapshot(force=True)

            render_if_needed.assert_called_once_with(force=True)
            self.assertEqual(maintainer.last_progress_render_at, 321.0)
            self.assertEqual(maintainer.runtime.ui.last_progress_render_at, 321.0)
            self.assertEqual(
                maintainer.runtime.ui.last_progress_signature,
                maintainer.last_progress_signature,
            )
            self.assertNotEqual(maintainer.last_progress_signature, "")

    def test_parse_child_progress_line_upload_candidate_and_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            with mock.patch("auto_maintain.log"):
                maintainer.parse_child_progress_line("upload", "上传候选文件数: 50")
                maintainer.parse_child_progress_line("upload", "上传成功: 12")

            self.assertEqual(maintainer.upload_progress_state.get("stage"), "upload")
            self.assertEqual(maintainer.upload_progress_state.get("total"), 50)
            self.assertEqual(maintainer.upload_progress_state.get("done"), 12)

    def test_should_log_child_alert_line_ignores_timeout_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            self.assertFalse(
                maintainer.should_log_child_alert_line(
                    "2026-03-23 15:40:23,637 | INFO | 开始并发探测 wham/usage: candidates=118 workers=50 timeout=15s retries=3"
                )
            )
            self.assertTrue(
                maintainer.should_log_child_alert_line(
                    "2026-03-23 15:40:23,637 | ERROR | request failed"
                )
            )

    def test_decode_child_output_line_supports_gb18030(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            raw = "开始并发探测".encode("gb18030")
            decoded = maintainer.decode_child_output_line(raw)
            self.assertEqual(decoded, "开始并发探测")

    def test_is_run_once_cycle_complete_requires_no_running_and_no_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            self.assertTrue(maintainer._is_run_once_cycle_complete())
            maintainer.pending_upload_snapshot = ["a|1|1"]
            self.assertFalse(maintainer._is_run_once_cycle_complete())
            maintainer.pending_upload_snapshot = None
            maintainer.maintain_process = _DoneProcess(0)
            self.assertFalse(maintainer._is_run_once_cycle_complete())

    def test_sleep_between_watch_cycles_run_once_complete_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.run_once = True
            maintainer = AutoMaintainer(settings)

            with mock.patch("auto_maintain.log") as log_mock:
                result = maintainer._sleep_between_watch_cycles()

            self.assertEqual(result, 0)
            self.assertTrue(
                any("Run-once cycle finished." in call.args[0] for call in log_mock.call_args_list if call.args)
            )

    def test_sleep_between_watch_cycles_run_once_waits_when_not_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            settings.run_once = True
            maintainer = AutoMaintainer(settings)
            maintainer.pending_upload_retry = True

            with mock.patch.object(maintainer, "sleep_with_shutdown", return_value=True) as sleep_mock:
                result = maintainer._sleep_between_watch_cycles()

            self.assertIsNone(result)
            sleep_mock.assert_called_once_with(1)

    def test_run_watch_cycle_and_maybe_sleep_returns_watch_exit_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            with mock.patch.object(
                maintainer,
                "_run_watch_iteration",
                return_value=7,
            ) as watch_mock, mock.patch.object(
                maintainer,
                "_sleep_between_watch_cycles",
                return_value=0,
            ) as sleep_mock:
                result = maintainer._run_watch_cycle_and_maybe_sleep()

            self.assertEqual(result, 7)
            watch_mock.assert_called_once()
            sleep_mock.assert_not_called()

    def test_run_watch_cycle_and_maybe_sleep_delegates_to_sleep_when_watch_ok(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            with mock.patch.object(
                maintainer,
                "_run_watch_iteration",
                return_value=0,
            ) as watch_mock, mock.patch.object(
                maintainer,
                "_sleep_between_watch_cycles",
                return_value=None,
            ) as sleep_mock:
                result = maintainer._run_watch_cycle_and_maybe_sleep()

            self.assertIsNone(result)
            watch_mock.assert_called_once()
            sleep_mock.assert_called_once()

    def test_resolve_stage_failure_respects_handle_failure_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            auth_dir = base / "auth"
            auth_dir.mkdir(parents=True, exist_ok=True)
            settings = _build_settings(base, auth_dir)
            maintainer = AutoMaintainer(settings)

            self.assertEqual(maintainer._resolve_stage_failure("watch", 0), 0)
            with mock.patch.object(maintainer, "handle_failure", return_value=True):
                self.assertEqual(maintainer._resolve_stage_failure("watch", 9), 0)
            with mock.patch.object(maintainer, "handle_failure", return_value=False):
                self.assertEqual(maintainer._resolve_stage_failure("watch", 9), 9)


if __name__ == "__main__":
    unittest.main()
