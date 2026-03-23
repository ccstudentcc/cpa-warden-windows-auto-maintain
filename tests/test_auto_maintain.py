from __future__ import annotations

import argparse
import json
import os
import tempfile
import unittest
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

    def test_load_settings_reads_watch_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            watch_cfg = Path(tmp) / "watch.json"
            watch_cfg.write_text(
                json.dumps(
                    {
                        "watch_interval_seconds": 21,
                        "upload_stable_wait_seconds": 3,
                        "upload_batch_size": 7,
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
                        "run_upload_on_start": False,
                    }
                ),
                encoding="utf-8",
            )
            args = argparse.Namespace(once=False, watch_config=str(watch_cfg))
            with mock.patch.dict(
                os.environ,
                {"WATCH_INTERVAL_SECONDS": "11", "UPLOAD_BATCH_SIZE": "13", "RUN_UPLOAD_ON_START": "1"},
                clear=True,
            ):
                settings = load_settings(args)
            self.assertEqual(settings.watch_interval_seconds, 11)
            self.assertEqual(settings.upload_batch_size, 13)
            self.assertTrue(settings.run_upload_on_start)


if __name__ == "__main__":
    unittest.main()
