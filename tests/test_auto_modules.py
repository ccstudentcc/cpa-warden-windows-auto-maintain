from __future__ import annotations

import json
import os
import tempfile
import unittest
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from unittest import mock

from cwma.auto.active_probe import ActiveUploadProbeState, decide_active_upload_probe
from cwma.auto.channel_commands import (
    build_maintain_command,
    build_upload_command,
    format_maintain_start_message,
    format_upload_start_message,
)
from cwma.auto.channel_lifecycle import (
    decide_maintain_process_exit,
    decide_maintain_start_error,
    decide_upload_process_exit,
    decide_upload_start_error,
)
from cwma.auto.channel_start_prep import prepare_maintain_start, prepare_upload_start
from cwma.auto.channel_feedback import (
    build_non_success_exit_feedback,
    format_command_completed_message,
    format_command_failed_message,
    format_command_retry_message,
    format_command_start_failed_message,
    format_command_start_retry_message,
    maintain_pending_progress_stage,
    non_success_exit_progress_stage,
)
from cwma.auto.config import load_watch_config
from cwma.auto.locking import InstanceLockState, is_pid_running, read_lock_pid, release_instance_lock
from cwma.auto.maintain_queue import (
    MaintainRuntimeState,
    MaintainQueueState,
    clear_maintain_queue_state,
    decide_maintain_start_scope,
    mark_maintain_runtime_retry,
    mark_maintain_retry,
    mark_maintain_success,
    mark_maintain_terminal_failure,
    merge_incremental_maintain_names,
    queue_maintain_request,
)
from cwma.auto.output_pump import append_child_output_line, start_output_pump_thread
from cwma.auto.process_output import (
    build_child_process_env,
    decode_child_output_line,
    should_log_child_alert_line,
)
from cwma.auto.progress_parser import parse_progress_line
from cwma.auto.runtime_state import (
    build_auto_runtime_state,
    build_composed_maintain_runtime_state,
    build_lifecycle_runtime_state,
    build_maintain_queue_state,
    build_maintain_runtime_state,
    build_snapshot_runtime_state,
    build_ui_runtime_state,
    build_upload_queue_state,
    build_upload_runtime_state,
    unpack_auto_runtime_state,
    unpack_composed_maintain_runtime_state,
    unpack_lifecycle_runtime_state,
    unpack_maintain_queue_state,
    unpack_maintain_runtime_state,
    unpack_snapshot_runtime_state,
    unpack_ui_runtime_state,
    unpack_upload_queue_state,
    unpack_upload_runtime_state,
)
from cwma.auto.state_models import (
    AutoRuntimeState,
    LifecycleRuntimeState,
    MaintainRuntimeState as ComposedMaintainRuntimeState,
    SnapshotRuntimeState,
    UiRuntimeState,
    UploadRuntimeState,
)
from cwma.auto.scope_files import write_scope_names
from cwma.auto.snapshots import (
    build_snapshot_file,
    build_snapshot_lines,
    read_snapshot_lines,
    write_snapshot_lines,
)
from cwma.auto.upload_queue import (
    UploadQueueState,
    clear_upload_queue_state,
    decide_upload_start,
    mark_upload_no_changes,
    mark_upload_no_pending_discovered,
    mark_upload_retry,
    mark_upload_success,
    mark_upload_terminal_failure,
    merge_pending_upload_snapshot,
)
from cwma.auto.upload_postprocess import (
    POST_UPLOAD_PENDING_REASON,
    build_upload_success_postprocess,
)
from cwma.auto.upload_cleanup import cleanup_uploaded_files, prune_empty_dirs_under
from cwma.auto.upload_scan_cadence import decide_upload_deep_scan
from cwma.auto.channel_status import (
    CHANNEL_MAINTAIN,
    CHANNEL_UPLOAD,
    STAGE_FAILED,
    STAGE_IDLE,
    STAGE_PENDING_FULL,
    STAGE_PENDING_INCREMENTAL,
    STAGE_RETRY_WAIT,
    STATUS_FAILED,
    STATUS_RETRY,
    STATUS_SUCCESS,
)
from cwma.auto.zip_intake import (
    compute_zip_signature,
    extract_zip_with_bandizip,
    extract_zip_with_windows_builtin,
    inspect_zip_archives,
    list_zip_json_entries,
    list_zip_paths,
    ps_quote,
)


class AutoModuleTests(unittest.TestCase):
    def test_cleanup_uploaded_files_tracks_expected_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            ok_file = base / "ok.json"
            changed_file = base / "changed.json"
            ok_file.write_text("{}", encoding="utf-8")
            changed_file.write_text("{}", encoding="utf-8")

            ok_stat = ok_file.stat()
            changed_stat = changed_file.stat()
            failed_path = base / "cannot_unlink"
            failed_path.mkdir(parents=True, exist_ok=True)
            failed_stat = failed_path.stat()

            changed_file.write_text('{"changed": true}', encoding="utf-8")
            missing = base / "missing.json"

            result = cleanup_uploaded_files(
                [
                    f"{ok_file}|{ok_stat.st_size}|{ok_stat.st_mtime_ns}",
                    f"{changed_file}|{changed_stat.st_size}|{changed_stat.st_mtime_ns}",
                    f"{missing}|1|1",
                    "invalid",
                    f"{failed_path}|{failed_stat.st_size}|{failed_stat.st_mtime_ns}",
                ]
            )

            self.assertEqual(result.deleted, 1)
            self.assertEqual(result.skipped_changed, 1)
            self.assertEqual(result.skipped_missing, 1)
            self.assertEqual(result.failed, 1)
            self.assertFalse(ok_file.exists())
            self.assertTrue(changed_file.exists())

    def test_prune_empty_dirs_under_tracks_expected_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            auth_dir = Path(tmpdir) / "auth"
            deep_empty = auth_dir / "x" / "y"
            non_empty = auth_dir / "keep"
            deep_empty.mkdir(parents=True, exist_ok=True)
            non_empty.mkdir(parents=True, exist_ok=True)
            (non_empty / "token.json").write_text("{}", encoding="utf-8")

            result = prune_empty_dirs_under(auth_dir)

            self.assertEqual(result.removed, 2)
            self.assertEqual(result.skipped_non_empty, 1)
            self.assertEqual(result.skipped_missing, 0)
            self.assertEqual(result.failed, 0)
            self.assertTrue(auth_dir.exists())
            self.assertTrue(non_empty.exists())
            self.assertFalse((auth_dir / "x").exists())

    def test_decide_active_upload_probe_no_change(self) -> None:
        state = ActiveUploadProbeState(
            pending_source_changes=False,
            last_json_count=10,
            last_zip_signature=("z|1|1",),
            last_deep_scan_at=0.0,
        )
        decision = decide_active_upload_probe(
            state=state,
            upload_running=True,
            current_json_count=10,
            inspect_zip_files=True,
            current_zip_signature=("z|1|1",),
            now_monotonic=5.0,
            deep_scan_interval_seconds=2,
        )
        self.assertFalse(decision.source_changed)
        self.assertFalse(decision.should_log_detection)
        self.assertFalse(decision.should_refresh_upload_queue)
        self.assertEqual(decision.state, state)

    def test_decide_active_upload_probe_change_waiting_interval(self) -> None:
        state = ActiveUploadProbeState(
            pending_source_changes=False,
            last_json_count=10,
            last_zip_signature=("old|1|1",),
            last_deep_scan_at=10.0,
        )
        decision = decide_active_upload_probe(
            state=state,
            upload_running=True,
            current_json_count=12,
            inspect_zip_files=True,
            current_zip_signature=("new|2|2",),
            now_monotonic=11.0,
            deep_scan_interval_seconds=5,
        )
        self.assertTrue(decision.source_changed)
        self.assertEqual(set(decision.changed_reasons), {"json", "zip"})
        self.assertTrue(decision.should_log_detection)
        self.assertFalse(decision.should_refresh_upload_queue)
        self.assertTrue(decision.state.pending_source_changes)
        self.assertEqual(decision.state.last_json_count, 12)
        self.assertEqual(decision.state.last_zip_signature, ("new|2|2",))
        self.assertEqual(decision.state.last_deep_scan_at, 10.0)

    def test_decide_active_upload_probe_change_refresh(self) -> None:
        state = ActiveUploadProbeState(
            pending_source_changes=True,
            last_json_count=10,
            last_zip_signature=("old|1|1",),
            last_deep_scan_at=3.0,
        )
        decision = decide_active_upload_probe(
            state=state,
            upload_running=True,
            current_json_count=11,
            inspect_zip_files=False,
            current_zip_signature=None,
            now_monotonic=10.0,
            deep_scan_interval_seconds=2,
        )
        self.assertTrue(decision.source_changed)
        self.assertEqual(decision.changed_reasons, ("json",))
        self.assertFalse(decision.should_log_detection)
        self.assertTrue(decision.should_refresh_upload_queue)
        self.assertEqual(decision.state.last_deep_scan_at, 10.0)

    def test_runtime_state_upload_build_unpack_roundtrip(self) -> None:
        state = build_upload_queue_state(
            pending_snapshot=["a|1|1"],
            pending_reason="queued",
            pending_retry=True,
            inflight_snapshot=["b|2|2"],
            attempt=3,
            retry_due_at=12.5,
        )
        values = unpack_upload_queue_state(state)
        self.assertEqual(values[0], ["a|1|1"])
        self.assertEqual(values[1], "queued")
        self.assertTrue(values[2])
        self.assertEqual(values[3], ["b|2|2"])
        self.assertEqual(values[4], 3)
        self.assertEqual(values[5], 12.5)

    def test_runtime_state_maintain_build_unpack_roundtrip(self) -> None:
        queue = build_maintain_queue_state(
            pending=True,
            reason="maintain retry",
            names={"a.json"},
        )
        runtime = build_maintain_runtime_state(
            queue=queue,
            inflight_names={"b.json"},
            attempt=2,
            retry_due_at=9.0,
        )

        queue_values = unpack_maintain_queue_state(queue)
        runtime_values = unpack_maintain_runtime_state(runtime)

        self.assertEqual(queue_values, (True, "maintain retry", {"a.json"}))
        self.assertEqual(runtime_values[0], queue)
        self.assertEqual(runtime_values[1], {"b.json"})
        self.assertEqual(runtime_values[2], 2)
        self.assertEqual(runtime_values[3], 9.0)

    def test_state_models_auto_runtime_defaults(self) -> None:
        runtime = AutoRuntimeState()
        self.assertIsInstance(runtime.upload, UploadRuntimeState)
        self.assertIsInstance(runtime.maintain, ComposedMaintainRuntimeState)
        self.assertIsInstance(runtime.snapshot, SnapshotRuntimeState)
        self.assertIsInstance(runtime.ui, UiRuntimeState)
        self.assertIsInstance(runtime.lifecycle, LifecycleRuntimeState)
        self.assertEqual(runtime.upload.deep_scan_counter, 0)
        self.assertFalse(runtime.lifecycle.shutdown_requested)

    def test_runtime_state_composed_upload_build_unpack_roundtrip(self) -> None:
        queue = build_upload_queue_state(
            pending_snapshot=["u|1|1"],
            pending_reason="detected",
            pending_retry=False,
            inflight_snapshot=["u|1|1"],
            attempt=1,
            retry_due_at=3.0,
        )
        runtime = build_upload_runtime_state(
            queue=queue,
            deep_scan_counter=11,
            pending_source_changes_during_upload=True,
            last_active_upload_deep_scan_at=14.0,
        )
        values = unpack_upload_runtime_state(runtime)
        self.assertEqual(values[0], queue)
        self.assertEqual(values[1], 11)
        self.assertTrue(values[2])
        self.assertEqual(values[3], 14.0)

    def test_runtime_state_composed_maintain_build_unpack_roundtrip(self) -> None:
        queue = build_maintain_queue_state(
            pending=True,
            reason="queued incremental maintain",
            names={"x.json"},
        )
        runtime = build_composed_maintain_runtime_state(
            queue=queue,
            inflight_names={"y.json"},
            attempt=2,
            retry_due_at=22.0,
            last_incremental_started_at=18.0,
            last_incremental_defer_reason="cooldown",
        )
        values = unpack_composed_maintain_runtime_state(runtime)
        self.assertEqual(values[0], queue)
        self.assertEqual(values[1], {"y.json"})
        self.assertEqual(values[2], 2)
        self.assertEqual(values[3], 22.0)
        self.assertEqual(values[4], 18.0)
        self.assertEqual(values[5], "cooldown")

    def test_runtime_state_snapshot_build_unpack_roundtrip(self) -> None:
        state = build_snapshot_runtime_state(
            last_uploaded_snapshot_file=Path("last.txt"),
            current_snapshot_file=Path("current.txt"),
            stable_snapshot_file=Path("stable.txt"),
            last_json_count=7,
            last_zip_signature=("a.zip|1|2",),
            zip_extract_processed_signatures={"a.zip": "sig"},
        )
        values = unpack_snapshot_runtime_state(state)
        self.assertEqual(values[0], Path("last.txt"))
        self.assertEqual(values[1], Path("current.txt"))
        self.assertEqual(values[2], Path("stable.txt"))
        self.assertEqual(values[3], 7)
        self.assertEqual(values[4], ("a.zip|1|2",))
        self.assertEqual(values[5], {"a.zip": "sig"})

    def test_runtime_state_snapshot_build_unpack_defensive_copy(self) -> None:
        source = {"a.zip": "sig-a"}
        state = build_snapshot_runtime_state(
            last_uploaded_snapshot_file=Path("last.txt"),
            current_snapshot_file=Path("current.txt"),
            stable_snapshot_file=Path("stable.txt"),
            last_json_count=1,
            last_zip_signature=("a.zip|1|1",),
            zip_extract_processed_signatures=source,
        )
        source["b.zip"] = "sig-b"
        self.assertEqual(state.zip_extract_processed_signatures, {"a.zip": "sig-a"})

        unpacked = unpack_snapshot_runtime_state(state)[5]
        unpacked["c.zip"] = "sig-c"
        self.assertEqual(state.zip_extract_processed_signatures, {"a.zip": "sig-a"})

    def test_runtime_state_ui_build_unpack_roundtrip(self) -> None:
        state = build_ui_runtime_state(
            upload_progress_state={"stage": "running", "done": 1, "total": 5},
            maintain_progress_state={"stage": "idle", "done": 0, "total": 0},
            last_progress_render_at=10.0,
            progress_render_interval_seconds=0.5,
            progress_render_heartbeat_seconds=9.0,
            last_progress_signature="sig",
            panel_height=9,
            panel_title="Panel",
            panel_enabled=True,
            panel_color_enabled=False,
            panel_initialized=True,
        )
        values = unpack_ui_runtime_state(state)
        self.assertEqual(values[0], {"stage": "running", "done": 1, "total": 5})
        self.assertEqual(values[1], {"stage": "idle", "done": 0, "total": 0})
        self.assertEqual(values[2], 10.0)
        self.assertEqual(values[3], 0.5)
        self.assertEqual(values[4], 9.0)
        self.assertEqual(values[5], "sig")
        self.assertEqual(values[6], 9)
        self.assertEqual(values[7], "Panel")
        self.assertTrue(values[8])
        self.assertFalse(values[9])
        self.assertTrue(values[10])

    def test_runtime_state_ui_build_unpack_defensive_copy(self) -> None:
        upload_progress = {"stage": "running", "done": 1, "total": 5}
        maintain_progress = {"stage": "idle", "done": 0, "total": 0}
        state = build_ui_runtime_state(
            upload_progress_state=upload_progress,
            maintain_progress_state=maintain_progress,
            last_progress_render_at=1.0,
            progress_render_interval_seconds=0.5,
            progress_render_heartbeat_seconds=9.0,
            last_progress_signature="sig",
            panel_height=9,
            panel_title="Panel",
            panel_enabled=True,
            panel_color_enabled=True,
            panel_initialized=False,
        )
        upload_progress["done"] = 99
        maintain_progress["stage"] = "pending"
        self.assertEqual(state.upload_progress_state["done"], 1)
        self.assertEqual(state.maintain_progress_state["stage"], "idle")

        unpacked_upload, unpacked_maintain = unpack_ui_runtime_state(state)[:2]
        unpacked_upload["done"] = 7
        unpacked_maintain["stage"] = "running"
        self.assertEqual(state.upload_progress_state["done"], 1)
        self.assertEqual(state.maintain_progress_state["stage"], "idle")

    def test_runtime_state_lifecycle_and_auto_build_unpack_roundtrip(self) -> None:
        started_at = datetime(2026, 3, 24, 10, 0, 0)
        lifecycle = build_lifecycle_runtime_state(
            instance_started_at=started_at,
            shutdown_requested=True,
            shutdown_reason="test",
            instance_lock_token="token",
            instance_lock_handle=None,
            upload_process=None,
            maintain_process=None,
            upload_output_thread=None,
            maintain_output_thread=None,
            windows_console_handler=None,
            next_maintain_due_at=88.0,
        )
        lifecycle_values = unpack_lifecycle_runtime_state(lifecycle)
        self.assertEqual(lifecycle_values[0], started_at)
        self.assertTrue(lifecycle_values[1])
        self.assertEqual(lifecycle_values[2], "test")
        self.assertEqual(lifecycle_values[3], "token")
        self.assertEqual(lifecycle_values[10], 88.0)

        upload = build_upload_runtime_state(
            queue=build_upload_queue_state(
                pending_snapshot=None,
                pending_reason=None,
                pending_retry=False,
                inflight_snapshot=None,
                attempt=0,
                retry_due_at=0.0,
            ),
            deep_scan_counter=1,
            pending_source_changes_during_upload=False,
            last_active_upload_deep_scan_at=0.0,
        )
        maintain = build_composed_maintain_runtime_state(
            queue=build_maintain_queue_state(
                pending=False,
                reason=None,
                names=None,
            ),
            inflight_names=None,
            attempt=0,
            retry_due_at=0.0,
            last_incremental_started_at=0.0,
            last_incremental_defer_reason=None,
        )
        snapshot = build_snapshot_runtime_state(
            last_uploaded_snapshot_file=None,
            current_snapshot_file=None,
            stable_snapshot_file=None,
            last_json_count=0,
            last_zip_signature=tuple(),
            zip_extract_processed_signatures={},
        )
        ui = build_ui_runtime_state(
            upload_progress_state={"stage": "idle", "done": 0, "total": 0},
            maintain_progress_state={"stage": "idle", "done": 0, "total": 0},
            last_progress_render_at=0.0,
            progress_render_interval_seconds=0.4,
            progress_render_heartbeat_seconds=8.0,
            last_progress_signature="",
            panel_height=8,
            panel_title="CPA Warden Auto Dashboard",
            panel_enabled=False,
            panel_color_enabled=False,
            panel_initialized=False,
        )
        auto_state = build_auto_runtime_state(
            upload=upload,
            maintain=maintain,
            snapshot=snapshot,
            ui=ui,
            lifecycle=lifecycle,
        )
        auto_values = unpack_auto_runtime_state(auto_state)
        self.assertEqual(auto_values[0], upload)
        self.assertEqual(auto_values[1], maintain)
        self.assertEqual(auto_values[2], snapshot)
        self.assertEqual(auto_values[3], ui)
        self.assertEqual(auto_values[4], lifecycle)

    def test_build_auto_runtime_state_defaults_are_not_shared_between_instances(self) -> None:
        first = build_auto_runtime_state()
        second = build_auto_runtime_state()

        self.assertIsNot(first.upload, second.upload)
        self.assertIsNot(first.maintain, second.maintain)
        self.assertIsNot(first.snapshot, second.snapshot)
        self.assertIsNot(first.ui, second.ui)
        self.assertIsNot(first.lifecycle, second.lifecycle)

        first.upload.deep_scan_counter = 9
        first.maintain.queue.pending = True
        first.snapshot.last_json_count = 11
        first.ui.panel_initialized = True
        first.lifecycle.shutdown_requested = True

        self.assertEqual(second.upload.deep_scan_counter, 0)
        self.assertFalse(second.maintain.queue.pending)
        self.assertEqual(second.snapshot.last_json_count, 0)
        self.assertFalse(second.ui.panel_initialized)
        self.assertFalse(second.lifecycle.shutdown_requested)

    def test_build_maintain_command_with_scope_and_yes(self) -> None:
        cmd = build_maintain_command(
            command_base=["python", "cpa_warden.py"],
            maintain_db_path=Path("m.sqlite3"),
            maintain_log_file=Path("m.log"),
            maintain_names_file=Path("names.txt"),
            assume_yes=True,
        )
        self.assertIn("--mode", cmd)
        self.assertIn("maintain", cmd)
        self.assertIn("--maintain-names-file", cmd)
        self.assertIn("names.txt", cmd)
        self.assertIn("--yes", cmd)

    def test_build_upload_command_with_scope(self) -> None:
        cmd = build_upload_command(
            command_base=["python", "cpa_warden.py"],
            auth_dir=Path("auth_files"),
            upload_db_path=Path("u.sqlite3"),
            upload_log_file=Path("u.log"),
            upload_names_file=Path("upload_names.txt"),
        )
        self.assertIn("--mode", cmd)
        self.assertIn("upload", cmd)
        self.assertIn("--upload-dir", cmd)
        self.assertIn("auth_files", cmd)
        self.assertIn("--upload-names-file", cmd)
        self.assertIn("upload_names.txt", cmd)

    def test_format_start_messages(self) -> None:
        maintain_msg = format_maintain_start_message(
            attempt=1,
            max_attempts=3,
            reason="post-upload maintain",
            maintain_scope_names={"a.json", "b.json"},
        )
        upload_msg = format_upload_start_message(
            attempt=2,
            max_attempts=3,
            reason="detected JSON changes",
            batch_size=20,
            pending_total=45,
        )
        self.assertIn("scope=incremental names=2", maintain_msg)
        self.assertIn("attempt 2 of 3", upload_msg)
        self.assertIn("batch_size=20", upload_msg)
        self.assertIn("pending_total=45", upload_msg)

    def test_channel_feedback_message_formatting(self) -> None:
        self.assertEqual(
            format_command_start_failed_message(CHANNEL_MAINTAIN, RuntimeError("boom")),
            "Maintain command failed to start: boom",
        )
        self.assertEqual(
            format_command_start_retry_message(CHANNEL_UPLOAD, 20),
            "Will retry upload in 20s.",
        )
        self.assertEqual(
            format_command_completed_message(CHANNEL_MAINTAIN),
            "Maintain command completed.",
        )
        self.assertEqual(
            format_command_retry_message(CHANNEL_UPLOAD, 7, 15),
            "Upload command failed with exit 7. Retrying in 15s...",
        )
        self.assertEqual(
            format_command_failed_message(CHANNEL_MAINTAIN, 9),
            "Maintain command failed after retries. Exit code 9.",
        )

    def test_channel_feedback_stage_mappings(self) -> None:
        self.assertEqual(
            maintain_pending_progress_stage(has_pending=False, pending_names=None),
            STAGE_IDLE,
        )
        self.assertEqual(
            maintain_pending_progress_stage(has_pending=True, pending_names=None),
            STAGE_PENDING_FULL,
        )
        self.assertEqual(
            maintain_pending_progress_stage(has_pending=True, pending_names={"a.json"}),
            STAGE_PENDING_INCREMENTAL,
        )
        self.assertEqual(non_success_exit_progress_stage(STATUS_RETRY), STAGE_RETRY_WAIT)
        self.assertEqual(non_success_exit_progress_stage(STATUS_FAILED), STAGE_FAILED)
        self.assertIsNone(non_success_exit_progress_stage(STATUS_SUCCESS))

    def test_build_non_success_exit_feedback(self) -> None:
        retry_feedback = build_non_success_exit_feedback(
            channel=CHANNEL_UPLOAD,
            status=STATUS_RETRY,
            code=7,
            retry_delay_seconds=15,
        )
        self.assertEqual(retry_feedback.stage, STAGE_RETRY_WAIT)
        self.assertEqual(
            retry_feedback.message,
            "Upload command failed with exit 7. Retrying in 15s...",
        )

        failed_feedback = build_non_success_exit_feedback(
            channel=CHANNEL_MAINTAIN,
            status=STATUS_FAILED,
            code=9,
            retry_delay_seconds=20,
        )
        self.assertEqual(failed_feedback.stage, STAGE_FAILED)
        self.assertEqual(
            failed_feedback.message,
            "Maintain command failed after retries. Exit code 9.",
        )

        neutral_feedback = build_non_success_exit_feedback(
            channel=CHANNEL_UPLOAD,
            status=STATUS_SUCCESS,
            code=0,
            retry_delay_seconds=20,
        )
        self.assertIsNone(neutral_feedback.stage)
        self.assertIsNone(neutral_feedback.message)

    def test_prepare_maintain_start_with_incremental_scope(self) -> None:
        prep = prepare_maintain_start(
            reason="post-upload maintain",
            attempt=1,
            max_attempts=3,
            scope_names={"a.json"},
            write_scope_file=lambda names: Path(f"scope-{len(names)}.txt"),
            build_command=lambda scope_file: ["maintain", str(scope_file) if scope_file else "-"],
            format_start_message=lambda attempt, max_attempts, reason, scope_names: (
                f"a={attempt}/{max_attempts}|r={reason}|n={len(scope_names or set())}"
            ),
        )
        self.assertEqual(prep.scope_file, Path("scope-1.txt"))
        self.assertTrue(prep.started_incremental)
        self.assertEqual(prep.command[0], "maintain")
        self.assertIn("n=1", prep.log_message)

    def test_prepare_upload_start_without_scope_file(self) -> None:
        prep = prepare_upload_start(
            reason="detected JSON changes",
            attempt=2,
            max_attempts=3,
            batch=["a|1|1", "b|1|1"],
            pending_total=4,
            extract_scope_names=lambda _batch: set(),
            write_scope_file=lambda names: Path(f"scope-{len(names)}.txt"),
            build_command=lambda scope_file: ["upload", str(scope_file) if scope_file else "-"],
            format_start_message=lambda attempt, max_attempts, reason, batch_size, pending_total: (
                f"a={attempt}/{max_attempts}|r={reason}|b={batch_size}|p={pending_total}"
            ),
        )
        self.assertIsNone(prep.scope_file)
        self.assertEqual(prep.batch, ["a|1|1", "b|1|1"])
        self.assertEqual(prep.command[0], "upload")
        self.assertIn("b=2", prep.log_message)

    def test_load_watch_config_parses_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch.json"
            path.write_text(json.dumps({"watch_interval_seconds": 15}), encoding="utf-8")
            cfg = load_watch_config(path)
        self.assertEqual(cfg["watch_interval_seconds"], 15)

    def test_read_lock_pid_parses_valid_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            lock_file.write_text("12345|token|1700000000", encoding="utf-8")
            self.assertEqual(read_lock_pid(lock_file), 12345)

    def test_read_lock_pid_handles_invalid_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            lock_file.write_text("not-a-pid", encoding="utf-8")
            self.assertIsNone(read_lock_pid(lock_file))

    def test_release_instance_lock_removes_matching_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            token = "token-1"
            lock_file.write_text(f"100|{token}|1700000000", encoding="utf-8")
            state = InstanceLockState(token=token, handle=None)
            next_state = release_instance_lock(
                lock_file=lock_file,
                allow_multi_instance=False,
                state=state,
                log=lambda _msg: None,
            )
            self.assertIsNone(next_state.token)
            self.assertFalse(lock_file.exists())

    def test_release_instance_lock_keeps_mismatched_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            lock_file.write_text("100|token-a|1700000000", encoding="utf-8")
            state = InstanceLockState(token="token-b", handle=None)
            _ = release_instance_lock(
                lock_file=lock_file,
                allow_multi_instance=False,
                state=state,
                log=lambda _msg: None,
            )
            self.assertTrue(lock_file.exists())

    def test_is_pid_running_accepts_current_pid(self) -> None:
        self.assertTrue(is_pid_running(os.getpid()))

    def test_decide_upload_deep_scan_force_true_resets_counter(self) -> None:
        decision = decide_upload_deep_scan(
            force_deep_scan=True,
            pending_upload_retry=False,
            current_json_count=1,
            last_json_count=1,
            inspect_zip_files=True,
            current_zip_signature=("a",),
            last_zip_signature=("a",),
            deep_scan_counter=7,
            deep_scan_interval_loops=10,
        )
        self.assertTrue(decision.should_deep_scan)
        self.assertEqual(decision.next_deep_scan_counter, 0)

    def test_decide_upload_deep_scan_interval_trigger(self) -> None:
        decision = decide_upload_deep_scan(
            force_deep_scan=False,
            pending_upload_retry=False,
            current_json_count=1,
            last_json_count=1,
            inspect_zip_files=False,
            current_zip_signature=tuple(),
            last_zip_signature=tuple(),
            deep_scan_counter=4,
            deep_scan_interval_loops=5,
        )
        self.assertTrue(decision.should_deep_scan)
        self.assertEqual(decision.next_deep_scan_counter, 0)

    def test_decide_upload_deep_scan_no_trigger_increments_counter(self) -> None:
        decision = decide_upload_deep_scan(
            force_deep_scan=False,
            pending_upload_retry=False,
            current_json_count=3,
            last_json_count=3,
            inspect_zip_files=True,
            current_zip_signature=("x",),
            last_zip_signature=("x",),
            deep_scan_counter=1,
            deep_scan_interval_loops=5,
        )
        self.assertFalse(decision.should_deep_scan)
        self.assertEqual(decision.next_deep_scan_counter, 2)

    def test_decode_child_output_line_supports_gb18030(self) -> None:
        raw = "维护完成".encode("gb18030")
        self.assertEqual(decode_child_output_line(raw), "维护完成")

    def test_build_child_process_env_sets_utf8_defaults(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            env = build_child_process_env()
        self.assertEqual(env["PYTHONUTF8"], "1")
        self.assertEqual(env["PYTHONIOENCODING"], "utf-8")

    def test_should_log_child_alert_line_filters_timeout_marker(self) -> None:
        self.assertFalse(should_log_child_alert_line("foo timeout=15s bar"))
        self.assertTrue(should_log_child_alert_line("request failed with status 500"))

    def test_queue_maintain_request_full_scope_overrides_incremental(self) -> None:
        state = MaintainQueueState(pending=True, reason="inc", names={"a.json"})
        result = queue_maintain_request(state=state, reason="full", names=None)
        self.assertTrue(result.state.pending)
        self.assertEqual(result.state.reason, "full")
        self.assertIsNone(result.state.names)
        self.assertEqual(result.progress_stage, "pending_full")

    def test_queue_maintain_request_incremental_merges_names(self) -> None:
        state = MaintainQueueState(pending=True, reason="old", names={"a.json"})
        result = queue_maintain_request(state=state, reason="inc", names={"b.json", " "})
        self.assertTrue(result.state.pending)
        self.assertEqual(result.state.reason, "inc")
        self.assertEqual(result.state.names, {"a.json", "b.json"})
        self.assertEqual(result.progress_stage, "pending_incremental")

    def test_merge_incremental_maintain_names_noop_when_full_queued(self) -> None:
        state = MaintainQueueState(pending=True, reason="full", names=None)
        next_state = merge_incremental_maintain_names(state=state, names={"a.json"})
        self.assertTrue(next_state.pending)
        self.assertEqual(next_state.reason, "full")
        self.assertIsNone(next_state.names)

    def test_clear_maintain_queue_state_resets_fields(self) -> None:
        cleared = clear_maintain_queue_state()
        self.assertFalse(cleared.pending)
        self.assertIsNone(cleared.reason)
        self.assertIsNone(cleared.names)

    def test_mark_maintain_retry_sets_full_when_inflight_missing(self) -> None:
        state = MaintainQueueState(pending=False, reason=None, names=None)
        retried = mark_maintain_retry(state=state, inflight_names=None)
        self.assertTrue(retried.pending)
        self.assertEqual(retried.reason, "maintain retry")
        self.assertIsNone(retried.names)

    def test_mark_maintain_retry_merges_incremental_names(self) -> None:
        state = MaintainQueueState(pending=True, reason="x", names={"a.json"})
        retried = mark_maintain_retry(state=state, inflight_names={"b.json"})
        self.assertTrue(retried.pending)
        self.assertEqual(retried.reason, "maintain retry")
        self.assertEqual(retried.names, {"a.json", "b.json"})

    def test_decide_maintain_start_scope_full_mode_clears_queue(self) -> None:
        state = MaintainQueueState(pending=True, reason="scheduled maintain", names=None)
        decision = decide_maintain_start_scope(state=state, batch_size=0)
        self.assertTrue(decision.should_start)
        self.assertIsNone(decision.scope_names)
        self.assertFalse(decision.state.pending)
        self.assertIsNone(decision.state.reason)
        self.assertIsNone(decision.state.names)

    def test_decide_maintain_start_scope_incremental_slices_remaining(self) -> None:
        state = MaintainQueueState(
            pending=True,
            reason="post-upload maintain",
            names={"a.json", "b.json", "c.json"},
        )
        decision = decide_maintain_start_scope(state=state, batch_size=2)
        self.assertTrue(decision.should_start)
        self.assertEqual(len(decision.scope_names or set()), 2)
        self.assertTrue(decision.state.pending)
        self.assertEqual(len(decision.state.names or set()), 1)
        self.assertEqual(decision.state.reason, "queued incremental maintain")

    def test_decide_maintain_start_scope_empty_incremental_skips(self) -> None:
        state = MaintainQueueState(pending=True, reason="x", names=set())
        decision = decide_maintain_start_scope(state=state, batch_size=2)
        self.assertFalse(decision.should_start)
        self.assertEqual(decision.skip_reason, "incremental scope is empty")
        self.assertFalse(decision.state.pending)

    def test_mark_maintain_runtime_retry_updates_due_and_clears_inflight(self) -> None:
        runtime = MaintainRuntimeState(
            queue=MaintainQueueState(pending=False, reason=None, names=None),
            inflight_names={"a.json"},
            attempt=1,
            retry_due_at=0.0,
        )
        next_runtime = mark_maintain_runtime_retry(
            state=runtime,
            now_monotonic=10.0,
            retry_delay_seconds=7,
        )
        self.assertTrue(next_runtime.queue.pending)
        self.assertEqual(next_runtime.queue.reason, "maintain retry")
        self.assertEqual(next_runtime.queue.names, {"a.json"})
        self.assertIsNone(next_runtime.inflight_names)
        self.assertEqual(next_runtime.retry_due_at, 17.0)

    def test_mark_maintain_success_resets_attempt_and_retry_due(self) -> None:
        runtime = MaintainRuntimeState(
            queue=MaintainQueueState(pending=True, reason="x", names={"a.json"}),
            inflight_names={"a.json"},
            attempt=2,
            retry_due_at=8.0,
        )
        done = mark_maintain_success(runtime)
        self.assertEqual(done.queue, runtime.queue)
        self.assertIsNone(done.inflight_names)
        self.assertEqual(done.attempt, 0)
        self.assertEqual(done.retry_due_at, 0.0)

    def test_mark_maintain_terminal_failure_clears_queue(self) -> None:
        runtime = MaintainRuntimeState(
            queue=MaintainQueueState(pending=True, reason="x", names={"a.json"}),
            inflight_names={"a.json"},
            attempt=3,
            retry_due_at=9.0,
        )
        failed = mark_maintain_terminal_failure(runtime)
        self.assertFalse(failed.queue.pending)
        self.assertIsNone(failed.queue.reason)
        self.assertIsNone(failed.queue.names)
        self.assertIsNone(failed.inflight_names)
        self.assertEqual(failed.attempt, 0)
        self.assertEqual(failed.retry_due_at, 9.0)

    def test_decide_maintain_start_error_retry_then_terminal(self) -> None:
        runtime = MaintainRuntimeState(
            queue=MaintainQueueState(pending=False, reason=None, names=None),
            inflight_names={"a.json"},
            attempt=1,
            retry_due_at=0.0,
        )
        retry_decision = decide_maintain_start_error(
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertTrue(retry_decision.should_retry)
        self.assertFalse(retry_decision.terminal_failure)
        self.assertTrue(retry_decision.state.queue.pending)
        self.assertEqual(retry_decision.state.retry_due_at, 15.0)

        terminal_decision = decide_maintain_start_error(
            state=runtime,
            retry_count=0,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertFalse(terminal_decision.should_retry)
        self.assertTrue(terminal_decision.terminal_failure)
        self.assertFalse(terminal_decision.state.queue.pending)

    def test_decide_upload_start_error_retry_then_terminal(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="queued",
            pending_retry=False,
            inflight_snapshot=["a|1|1"],
            attempt=1,
            retry_due_at=0.0,
        )
        retry_decision = decide_upload_start_error(
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertTrue(retry_decision.should_retry)
        self.assertFalse(retry_decision.terminal_failure)
        self.assertTrue(retry_decision.state.pending_retry)
        self.assertEqual(retry_decision.state.retry_due_at, 24.0)

        terminal_decision = decide_upload_start_error(
            state=state,
            retry_count=0,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertFalse(terminal_decision.should_retry)
        self.assertTrue(terminal_decision.terminal_failure)
        self.assertIsNone(terminal_decision.state.pending_snapshot)

    def test_decide_maintain_process_exit_statuses(self) -> None:
        runtime = MaintainRuntimeState(
            queue=MaintainQueueState(pending=False, reason=None, names=None),
            inflight_names={"a.json"},
            attempt=1,
            retry_due_at=0.0,
        )

        success = decide_maintain_process_exit(
            code=0,
            shutdown_requested=False,
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(success.status, "success")
        self.assertEqual(success.return_code, 0)
        self.assertEqual(success.state.attempt, 0)

        shutdown = decide_maintain_process_exit(
            code=2,
            shutdown_requested=True,
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(shutdown.status, "shutdown")
        self.assertEqual(shutdown.return_code, 130)
        self.assertEqual(shutdown.state, runtime)

        retry = decide_maintain_process_exit(
            code=2,
            shutdown_requested=False,
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(retry.status, "retry")
        self.assertEqual(retry.return_code, 0)
        self.assertTrue(retry.state.queue.pending)
        self.assertEqual(retry.state.retry_due_at, 15.0)

        failed = decide_maintain_process_exit(
            code=2,
            shutdown_requested=False,
            state=runtime,
            retry_count=0,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.return_code, 2)
        self.assertFalse(failed.state.queue.pending)

    def test_decide_upload_process_exit_statuses(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="queued",
            pending_retry=False,
            inflight_snapshot=["a|1|1"],
            attempt=1,
            retry_due_at=0.0,
        )

        success = decide_upload_process_exit(
            code=0,
            shutdown_requested=False,
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(success.status, "success")
        self.assertEqual(success.return_code, 0)
        self.assertIsNone(success.state.inflight_snapshot)
        self.assertEqual(success.state.attempt, 0)

        shutdown = decide_upload_process_exit(
            code=3,
            shutdown_requested=True,
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(shutdown.status, "shutdown")
        self.assertEqual(shutdown.return_code, 130)
        self.assertEqual(shutdown.state, state)

        retry = decide_upload_process_exit(
            code=3,
            shutdown_requested=False,
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(retry.status, "retry")
        self.assertEqual(retry.return_code, 0)
        self.assertTrue(retry.state.pending_retry)
        self.assertEqual(retry.state.retry_due_at, 24.0)

        failed = decide_upload_process_exit(
            code=3,
            shutdown_requested=False,
            state=state,
            retry_count=0,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.return_code, 3)
        self.assertIsNone(failed.state.pending_snapshot)

    def test_parse_progress_line_upload_candidate_total(self) -> None:
        result = parse_progress_line(
            channel="upload",
            text="上传候选文件数: 19",
            current_upload_total=0,
            should_log_alert_line=lambda _text: False,
        )
        self.assertEqual(len(result.updates), 1)
        update = result.updates[0]
        self.assertEqual(update.channel, "upload")
        self.assertEqual(update.stage, "scan")
        self.assertEqual(update.total, 19)
        self.assertFalse(result.should_log_alert)

    def test_parse_progress_line_maintain_probe_candidates(self) -> None:
        result = parse_progress_line(
            channel="maintain",
            text="开始并发探测 wham/usage: candidates=88 workers=50",
            current_upload_total=0,
            should_log_alert_line=lambda _text: False,
        )
        self.assertEqual(len(result.updates), 1)
        update = result.updates[0]
        self.assertEqual(update.channel, "maintain")
        self.assertEqual(update.stage, "probe")
        self.assertEqual(update.total, 88)
        self.assertTrue(update.force_render)

    def test_parse_progress_line_alert_passthrough(self) -> None:
        result = parse_progress_line(
            channel="maintain",
            text="request failed with status 500",
            current_upload_total=0,
            should_log_alert_line=lambda text: "failed" in text,
        )
        self.assertEqual(result.updates, ())
        self.assertTrue(result.should_log_alert)

    def test_append_child_output_line_writes_line_with_newline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "state" / "upload_command_output.log"
            append_child_output_line(target=target, line="hello")
            self.assertEqual(target.read_text(encoding="utf-8"), "hello\n")

    def test_start_output_pump_thread_decodes_and_forwards_lines(self) -> None:
        class _Proc:
            def __init__(self) -> None:
                self.stdout = BytesIO("维护完成\n".encode("utf-8"))

        class _FakeThread:
            def __init__(self, *, target: object, name: str, daemon: bool) -> None:
                self._target = target
                self.name = name
                self.daemon = daemon
                self.started = False

            def start(self) -> None:
                self.started = True
                if callable(self._target):
                    self._target()

            def join(self, timeout: float | None = None) -> None:
                return None

        received: list[str] = []
        warnings: list[str] = []
        fake_threads: list[_FakeThread] = []

        def _thread_factory(*, target: object, name: str, daemon: bool) -> _FakeThread:
            thread = _FakeThread(target=target, name=name, daemon=daemon)
            fake_threads.append(thread)
            return thread

        proc = _Proc()
        thread = start_output_pump_thread(
            channel="maintain",
            proc=proc,  # type: ignore[arg-type]
            decode_line=lambda raw: raw.decode("utf-8") if isinstance(raw, bytes) else raw,
            on_line=lambda line: received.append(line),
            warn=lambda msg: warnings.append(msg),
            thread_factory=_thread_factory,  # type: ignore[arg-type]
        )
        self.assertIsNotNone(thread)
        if thread is None:
            self.fail("expected output pump thread")
        self.assertEqual(len(fake_threads), 1)
        self.assertTrue(fake_threads[0].started)
        self.assertEqual(fake_threads[0].name, "maintain-output-pump")
        self.assertTrue(fake_threads[0].daemon)
        self.assertEqual(received, ["维护完成\n"])
        self.assertEqual(warnings, [])

    def test_list_zip_paths_and_signature(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            zip_path = auth_dir / "a.zip"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            paths = list_zip_paths(auth_dir)
            sig = compute_zip_signature(auth_dir, log=lambda _msg: None)
        self.assertEqual(paths, [zip_path])
        self.assertEqual(len(sig), 1)
        self.assertIn(str(zip_path), sig[0])

    def test_list_zip_json_entries_supports_nested_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "nested.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("a/b/c.json", "{}")
                zf.writestr("a/d.txt", "x")
            with zipfile.ZipFile(zip_path, "r") as zf:
                entries = list_zip_json_entries(zf)
        self.assertEqual(entries, ["a/b/c.json"])

    def test_extract_zip_with_bandizip_returns_one_when_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            zip_path = base_dir / "x.zip"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            logs: list[str] = []
            with mock.patch("cwma.auto.zip_intake.shutil.which", return_value=None):
                code = extract_zip_with_bandizip(
                    zip_path=zip_path,
                    output_dir=base_dir,
                    base_dir=base_dir,
                    bandizip_path="",
                    timeout_seconds=5,
                    log=logs.append,
                )
        self.assertEqual(code, 1)
        self.assertTrue(any("Bandizip not found" in item for item in logs))

    def test_extract_zip_with_windows_builtin_returns_one_when_shell_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            zip_path = base_dir / "x.zip"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            logs: list[str] = []
            with mock.patch("cwma.auto.zip_intake.shutil.which", return_value=None):
                code = extract_zip_with_windows_builtin(
                    zip_path=zip_path,
                    output_dir=base_dir,
                    base_dir=base_dir,
                    timeout_seconds=5,
                    log=logs.append,
                )
        self.assertEqual(code, 1)
        self.assertTrue(any("shell not found" in item for item in logs))

    def test_ps_quote_escapes_single_quote(self) -> None:
        self.assertEqual(ps_quote("a'b"), "a''b")

    def test_snapshot_file_helpers_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            a = base / "a.json"
            b = base / "b.json"
            a.write_text("{}", encoding="utf-8")
            b.write_text("{}", encoding="utf-8")
            target = base / "snapshot.txt"

            lines = build_snapshot_file(
                target=target,
                paths=[a, b],
                log=lambda _msg: None,
            )
            loaded = read_snapshot_lines(target)

        self.assertEqual(lines, loaded)
        self.assertEqual(len(lines), 2)

    def test_build_snapshot_lines_skips_missing_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            exists = base / "ok.json"
            missing = base / "missing.json"
            exists.write_text("{}", encoding="utf-8")
            warnings: list[str] = []

            lines = build_snapshot_lines([exists, missing], log=warnings.append)

        self.assertEqual(len(lines), 1)
        self.assertIn("skipped transient files", warnings[0])

    def test_write_snapshot_lines_overwrites_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "snapshot.txt"
            write_snapshot_lines(target, ["a", "b"])
            write_snapshot_lines(target, ["c"])
            loaded = read_snapshot_lines(target)
        self.assertEqual(loaded, ["c"])

    def test_write_scope_names_sorts_and_filters_empty_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "state" / "scope.txt"
            result = write_scope_names(target, {"b.json", "", "a.json", " "})
            content = target.read_text(encoding="utf-8").splitlines()
        self.assertEqual(result, target)
        self.assertEqual(content, ["a.json", "b.json"])

    def test_clear_upload_queue_state_resets_fields(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="detected",
            pending_retry=True,
            inflight_snapshot=["a|1|1"],
            attempt=2,
            retry_due_at=9.0,
        )
        cleared = clear_upload_queue_state(state)
        self.assertIsNone(cleared.pending_snapshot)
        self.assertIsNone(cleared.pending_reason)
        self.assertFalse(cleared.pending_retry)
        self.assertIsNone(cleared.inflight_snapshot)
        self.assertEqual(cleared.attempt, 0)
        self.assertEqual(cleared.retry_due_at, 0.0)

    def test_decide_upload_start_waits_for_retry_window(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="detected",
            pending_retry=False,
            inflight_snapshot=None,
            attempt=1,
            retry_due_at=10.0,
        )
        decision = decide_upload_start(
            state=state,
            now_monotonic=9.0,
            batch_size=1,
        )
        self.assertFalse(decision.can_start)
        self.assertTrue(decision.waiting_retry)
        self.assertEqual(decision.batch, [])

    def test_decide_upload_start_reuses_inflight_batch_on_retry(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1", "b|1|1"],
            pending_reason="retry",
            pending_retry=True,
            inflight_snapshot=["b|1|1"],
            attempt=1,
            retry_due_at=0.0,
        )
        decision = decide_upload_start(
            state=state,
            now_monotonic=1.0,
            batch_size=2,
        )
        self.assertTrue(decision.can_start)
        self.assertEqual(decision.batch, ["b|1|1"])
        self.assertEqual(decision.state.inflight_snapshot, ["b|1|1"])

    def test_mark_upload_retry_sets_retry_due(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="detected",
            pending_retry=False,
            inflight_snapshot=["a|1|1"],
            attempt=1,
            retry_due_at=0.0,
        )
        retried = mark_upload_retry(
            state=state,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertTrue(retried.pending_retry)
        self.assertEqual(retried.retry_due_at, 15.0)
        self.assertEqual(retried.pending_snapshot, ["a|1|1"])

    def test_mark_upload_success_resets_retry_fields(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="detected",
            pending_retry=True,
            inflight_snapshot=["a|1|1"],
            attempt=2,
            retry_due_at=3.0,
        )
        done = mark_upload_success(state)
        self.assertEqual(done.pending_snapshot, ["a|1|1"])
        self.assertFalse(done.pending_retry)
        self.assertIsNone(done.inflight_snapshot)
        self.assertEqual(done.attempt, 0)
        self.assertEqual(done.retry_due_at, 0.0)

    def test_mark_upload_terminal_failure_clears_queue(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="detected",
            pending_retry=True,
            inflight_snapshot=["a|1|1"],
            attempt=2,
            retry_due_at=3.0,
        )
        failed = mark_upload_terminal_failure(state)
        self.assertIsNone(failed.pending_snapshot)
        self.assertIsNone(failed.pending_reason)
        self.assertFalse(failed.pending_retry)
        self.assertIsNone(failed.inflight_snapshot)
        self.assertEqual(failed.attempt, 0)
        self.assertEqual(failed.retry_due_at, 0.0)

    def test_mark_upload_no_changes_clears_retry_flag_when_not_preserved(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="detected",
            pending_retry=True,
            inflight_snapshot=["a|1|1"],
            attempt=2,
            retry_due_at=5.0,
        )
        next_state = mark_upload_no_changes(state=state, preserve_retry_state=False)
        self.assertFalse(next_state.pending_retry)
        self.assertEqual(next_state.attempt, 2)
        self.assertEqual(next_state.retry_due_at, 5.0)

    def test_mark_upload_no_pending_discovered_resets_retry_runtime(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="detected",
            pending_retry=True,
            inflight_snapshot=None,
            attempt=2,
            retry_due_at=5.0,
        )
        next_state = mark_upload_no_pending_discovered(state=state, preserve_retry_state=False)
        self.assertFalse(next_state.pending_retry)
        self.assertEqual(next_state.attempt, 0)
        self.assertEqual(next_state.retry_due_at, 0.0)
        self.assertEqual(next_state.pending_reason, "detected")

    def test_merge_pending_upload_snapshot_merges_and_sets_reason(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="old",
            pending_retry=False,
            inflight_snapshot=None,
            attempt=3,
            retry_due_at=9.0,
        )
        result = merge_pending_upload_snapshot(
            state=state,
            discovered_pending_snapshot=["b|1|1", "a|1|1"],
            queue_reason="detected JSON changes",
            preserve_retry_state=False,
        )
        self.assertEqual(result.merged_pending_snapshot, ["a|1|1", "b|1|1"])
        self.assertEqual(result.state.pending_reason, "detected JSON changes")
        self.assertEqual(result.state.attempt, 0)
        self.assertEqual(result.state.retry_due_at, 0.0)

    def test_build_upload_success_postprocess_with_pending(self) -> None:
        row_a = f"{Path('C:/auth/a.json')}|1|1"
        row_b = f"{Path('C:/auth/b.json')}|2|2"
        row_c = f"{Path('C:/auth/c.json')}|3|3"
        result = build_upload_success_postprocess(
            previous_uploaded_baseline=[row_a],
            uploaded_snapshot=[row_a, row_b],
            current_snapshot=[row_a, row_c],
        )
        self.assertEqual(result.uploaded_baseline, [row_a])
        self.assertEqual(result.pending_snapshot, [row_c])
        self.assertEqual(result.queue_snapshot, [row_c])
        self.assertEqual(result.queue_reason, POST_UPLOAD_PENDING_REASON)
        self.assertEqual(result.progress_stage, "pending")
        self.assertEqual(result.uploaded_names, {"a.json", "b.json"})

    def test_build_upload_success_postprocess_without_pending(self) -> None:
        row_a = f"{Path('C:/auth/a.json')}|1|1"
        row_b = f"{Path('C:/auth/b.json')}|2|2"
        result = build_upload_success_postprocess(
            previous_uploaded_baseline=[row_a],
            uploaded_snapshot=[row_b],
            current_snapshot=[row_a, row_b],
        )
        self.assertEqual(result.pending_snapshot, [])
        self.assertIsNone(result.queue_snapshot)
        self.assertIsNone(result.queue_reason)
        self.assertEqual(result.progress_stage, "idle")
        self.assertEqual(result.uploaded_names, {"b.json"})

    def test_inspect_zip_archives_tracks_processed_signature(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            zip_path = auth_dir / "nested.zip"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("nested/a.json", "{}")

            processed: dict[str, str] = {}
            extracted: list[str] = []

            def _extract(path: Path, output_dir: Path) -> int:
                extracted.append(f"{path.name}->{output_dir}")
                return 0

            changed_first = inspect_zip_archives(
                auth_dir=auth_dir,
                inspect_zip_files=True,
                auto_extract_zip_json=True,
                delete_zip_after_extract=False,
                processed_signatures=processed,
                extract_zip=_extract,
                log=lambda _msg: None,
            )
            changed_second = inspect_zip_archives(
                auth_dir=auth_dir,
                inspect_zip_files=True,
                auto_extract_zip_json=True,
                delete_zip_after_extract=False,
                processed_signatures=processed,
                extract_zip=_extract,
                log=lambda _msg: None,
            )

        self.assertTrue(changed_first)
        self.assertFalse(changed_second)
        self.assertEqual(len(extracted), 1)

    def test_inspect_zip_archives_delete_mode_removes_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            zip_path = auth_dir / "delete-me.zip"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("a.json", "{}")

            changed = inspect_zip_archives(
                auth_dir=auth_dir,
                inspect_zip_files=True,
                auto_extract_zip_json=True,
                delete_zip_after_extract=True,
                processed_signatures={},
                extract_zip=lambda _path, _output: 0,
                log=lambda _msg: None,
            )

        self.assertTrue(changed)
        self.assertFalse(zip_path.exists())
if __name__ == "__main__":
    unittest.main()

