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
        incremental_maintain_min_interval_seconds=20,
        incremental_maintain_full_guard_seconds=90,
        deep_scan_interval_loops=10,
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
    def test_compute_uploaded_baseline_keeps_only_uploaded_and_still_existing(self) -> None:
        existing_baseline = ["legacy|0|0", "a|1|1"]
        uploaded_snapshot = ["a|1|1", "b|2|2"]
        current_snapshot = ["a|1|1", "c|3|3", "legacy|0|0"]
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            settings = _build_settings(base, base / "auth")
            maintainer = AutoMaintainer(settings)
            baseline = maintainer.compute_uploaded_baseline(existing_baseline, uploaded_snapshot, current_snapshot)
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
            maintainer.last_incremental_maintain_started_at = time.monotonic()

            with mock.patch("auto_maintain.subprocess.Popen") as popen:
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
            maintainer.next_maintain_due_at = time.monotonic() + 30

            with mock.patch("auto_maintain.subprocess.Popen") as popen:
                result = maintainer.maybe_start_maintain()

            self.assertEqual(result, 0)
            self.assertIsNone(maintainer.maintain_process)
            self.assertTrue(maintainer.pending_maintain)
            self.assertEqual(popen.call_count, 0)

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
            ):
                maintainer.probe_changes_during_active_upload()

            self.assertTrue(maintainer.pending_source_changes_during_upload)
            self.assertEqual(maintainer.last_json_count, 12)
            self.assertEqual(maintainer.last_zip_signature, ("new.zip|2|2",))

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


if __name__ == "__main__":
    unittest.main()
