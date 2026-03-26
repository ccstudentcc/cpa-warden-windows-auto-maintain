from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
import tempfile

from cwma.auto.state.active_probe import ActiveUploadProbeState, decide_active_upload_probe
from cwma.auto.state.maintain_queue import (
    MAINTAIN_SCOPE_FULL,
    MAINTAIN_SCOPE_INCREMENTAL,
    MaintainQueueState,
    MaintainRuntimeState,
    clear_maintain_queue_state,
    decide_maintain_start_scope,
    mark_maintain_runtime_retry,
    mark_maintain_retry,
    mark_maintain_success,
    mark_maintain_terminal_failure,
    merge_incremental_maintain_names,
    queue_maintain_request,
)
from cwma.auto.state.runtime_state import (
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
from cwma.auto.state.state_models import (
    AutoRuntimeState,
    LifecycleRuntimeState,
    MaintainRuntimeState as ComposedMaintainRuntimeState,
    SnapshotRuntimeState,
    UiRuntimeState,
    UploadRuntimeState,
)
from cwma.auto.state.scope_files import write_scope_names
from cwma.auto.state.snapshots import (
    build_snapshot_file,
    build_snapshot_lines,
    read_snapshot_lines,
    write_snapshot_lines,
)
from cwma.auto.state.upload_postprocess import (
    POST_UPLOAD_PENDING_REASON,
    build_upload_success_postprocess,
)
from cwma.auto.state.upload_queue import (
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
from cwma.auto.state.upload_scan_cadence import decide_upload_deep_scan
from cwma.scheduler.smart_scheduler import SmartSchedulerConfig, SmartSchedulerPolicy
from tests.temp_sandbox import TempSandboxState, setup_tempfile_sandbox, teardown_tempfile_sandbox

_TEMP_SANDBOX_STATE: TempSandboxState | None = None


def setUpModule() -> None:
    global _TEMP_SANDBOX_STATE
    _TEMP_SANDBOX_STATE = setup_tempfile_sandbox()


def tearDownModule() -> None:
    teardown_tempfile_sandbox(_TEMP_SANDBOX_STATE)


class AutoModuleStateTests(unittest.TestCase):
    @staticmethod
    def _build_scheduler_policy() -> SmartSchedulerPolicy:
        return SmartSchedulerPolicy(
            SmartSchedulerConfig(
                enabled=True,
                adaptive_upload_batching=True,
                base_upload_batch_size=100,
                upload_high_backlog_threshold=400,
                upload_high_backlog_batch_size=300,
                adaptive_maintain_batching=True,
                base_incremental_maintain_batch_size=120,
                maintain_high_backlog_threshold=300,
                maintain_high_backlog_batch_size=220,
                incremental_maintain_min_interval_seconds=20,
                incremental_maintain_full_guard_seconds=90,
            )
        )

    def test_smart_scheduler_upload_realtime_mode_under_maintain_pressure(self) -> None:
        policy = self._build_scheduler_policy()
        batch_size = policy.choose_upload_batch_size(
            pending_count=150,
            maintain_pressure=True,
        )
        self.assertEqual(batch_size, 50)

    def test_smart_scheduler_upload_throughput_mode_on_high_backlog(self) -> None:
        policy = self._build_scheduler_policy()
        batch_size = policy.choose_upload_batch_size(
            pending_count=600,
            maintain_pressure=True,
        )
        self.assertEqual(batch_size, 300)

    def test_smart_scheduler_incremental_realtime_mode_under_upload_pressure(self) -> None:
        policy = self._build_scheduler_policy()
        batch_size = policy.choose_incremental_maintain_batch_size(
            pending_count=200,
            upload_pressure=True,
        )
        self.assertEqual(batch_size, 60)

    def test_smart_scheduler_upload_batch_uses_total_backlog_signal(self) -> None:
        policy = self._build_scheduler_policy()
        batch_size = policy.choose_upload_batch_size(
            pending_count=250,
            maintain_pressure=True,
            total_backlog=520,
        )
        self.assertEqual(batch_size, 250)

    def test_smart_scheduler_incremental_batch_uses_total_backlog_signal(self) -> None:
        policy = self._build_scheduler_policy()
        batch_size = policy.choose_incremental_maintain_batch_size(
            pending_count=180,
            upload_pressure=False,
            total_backlog=400,
        )
        self.assertEqual(batch_size, 180)

    def test_smart_scheduler_defers_incremental_only_for_small_fill(self) -> None:
        policy = self._build_scheduler_policy()
        deferred, reason = policy.should_defer_incremental_maintain(
            pending_incremental_count=1,
            planned_batch_size=4,
            pending_upload_count=60,
            upload_running=True,
        )
        self.assertTrue(deferred)
        self.assertEqual(reason, "batch_too_small_waiting_fill")

    def test_smart_scheduler_does_not_defer_incremental_when_fill_is_enough(self) -> None:
        policy = self._build_scheduler_policy()
        deferred, reason = policy.should_defer_incremental_maintain(
            pending_incremental_count=4,
            planned_batch_size=4,
            pending_upload_count=60,
            upload_running=True,
        )
        self.assertFalse(deferred)
        self.assertEqual(reason, "")

    def test_smart_scheduler_does_not_defer_incremental_without_upload_fill_source(self) -> None:
        policy = self._build_scheduler_policy()
        deferred, reason = policy.should_defer_incremental_maintain(
            pending_incremental_count=1,
            planned_batch_size=4,
            pending_upload_count=0,
            upload_running=False,
        )
        self.assertFalse(deferred)
        self.assertEqual(reason, "")

    def test_smart_scheduler_does_not_defer_when_upload_fill_is_too_small(self) -> None:
        policy = self._build_scheduler_policy()
        deferred, reason = policy.should_defer_incremental_maintain(
            pending_incremental_count=8,
            planned_batch_size=30,
            pending_upload_count=4,
            upload_running=False,
        )
        self.assertFalse(deferred)
        self.assertEqual(reason, "")

    def test_smart_scheduler_upload_ewma_smoothing_keeps_throughput_after_spike(self) -> None:
        policy = SmartSchedulerPolicy(
            SmartSchedulerConfig(
                enabled=True,
                adaptive_upload_batching=True,
                base_upload_batch_size=100,
                upload_high_backlog_threshold=400,
                upload_high_backlog_batch_size=300,
                adaptive_maintain_batching=True,
                base_incremental_maintain_batch_size=120,
                maintain_high_backlog_threshold=300,
                maintain_high_backlog_batch_size=220,
                incremental_maintain_min_interval_seconds=20,
                incremental_maintain_full_guard_seconds=90,
                backlog_ewma_alpha=0.2,
            )
        )
        first = policy.choose_upload_batch_size(
            pending_count=300,
            maintain_pressure=True,
            total_backlog=500,
        )
        second = policy.choose_upload_batch_size(
            pending_count=300,
            maintain_pressure=True,
            total_backlog=100,
        )
        self.assertEqual(first, 300)
        self.assertEqual(second, 300)

    def test_smart_scheduler_upload_hysteresis_uses_enter_and_exit_thresholds(self) -> None:
        policy = SmartSchedulerPolicy(
            SmartSchedulerConfig(
                enabled=True,
                adaptive_upload_batching=True,
                base_upload_batch_size=100,
                upload_high_backlog_threshold=400,
                upload_high_backlog_batch_size=300,
                adaptive_maintain_batching=True,
                base_incremental_maintain_batch_size=120,
                maintain_high_backlog_threshold=300,
                maintain_high_backlog_batch_size=220,
                incremental_maintain_min_interval_seconds=20,
                incremental_maintain_full_guard_seconds=90,
                scheduler_hysteresis_enabled=True,
                upload_high_backlog_enter_threshold=400,
                upload_high_backlog_exit_threshold=260,
            )
        )
        entered = policy.choose_upload_batch_size(
            pending_count=180,
            maintain_pressure=True,
            total_backlog=450,
        )
        held = policy.choose_upload_batch_size(
            pending_count=180,
            maintain_pressure=True,
            total_backlog=300,
        )
        exited = policy.choose_upload_batch_size(
            pending_count=180,
            maintain_pressure=True,
            total_backlog=240,
        )
        self.assertEqual(entered, 180)
        self.assertEqual(held, 180)
        self.assertEqual(exited, 100)

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
        self.assertEqual(len(result.state.pipeline.maintain_scan_queue), 1)
        job_id = result.state.pipeline.maintain_scan_queue[0]
        self.assertEqual(result.state.pipeline.jobs[job_id].scope_type, MAINTAIN_SCOPE_INCREMENTAL)

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
        self.assertEqual(decision.step, "scan")
        self.assertIsNone(decision.scope_names)
        self.assertFalse(decision.state.pending)
        self.assertIsNone(decision.state.reason)
        self.assertIsNone(decision.state.names)
        self.assertEqual(len(decision.state.pipeline.jobs), 1)
        self.assertEqual(decision.state.pipeline.maintain_scan_queue, [])

    def test_queue_maintain_request_full_overrides_incremental_pipeline_jobs(self) -> None:
        state = clear_maintain_queue_state()
        state = queue_maintain_request(
            state=state,
            reason="post-upload maintain",
            names={"a.json", "b.json"},
        ).state
        state = queue_maintain_request(state=state, reason="scheduled full", names=None).state

        self.assertTrue(state.pending)
        self.assertIsNone(state.names)
        self.assertEqual(len(state.pipeline.jobs), 1)
        full_job_id = state.pipeline.maintain_scan_queue[0]
        self.assertEqual(state.pipeline.jobs[full_job_id].scope_type, MAINTAIN_SCOPE_FULL)

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

    def test_merge_pending_upload_snapshot_replaces_same_path_with_latest_row(self) -> None:
        old_row = "C:/auth/a.json|10|100"
        new_row = "C:/auth/a.json|11|120"
        state = UploadQueueState(
            pending_snapshot=[old_row],
            pending_reason="old",
            pending_retry=False,
            inflight_snapshot=None,
            attempt=0,
            retry_due_at=0.0,
        )
        result = merge_pending_upload_snapshot(
            state=state,
            discovered_pending_snapshot=[new_row],
            queue_reason="active-upload source changes",
            preserve_retry_state=True,
        )
        self.assertEqual(result.merged_pending_snapshot, [new_row])
        self.assertEqual(result.state.pending_reason, "active-upload source changes")

    def test_merge_pending_upload_snapshot_applies_buffer_limit(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="old",
            pending_retry=False,
            inflight_snapshot=None,
            attempt=0,
            retry_due_at=0.0,
        )
        result = merge_pending_upload_snapshot(
            state=state,
            discovered_pending_snapshot=["b|1|1", "c|1|1"],
            queue_reason="detected JSON changes",
            preserve_retry_state=False,
            buffer_limit=2,
        )
        self.assertEqual(result.merged_pending_snapshot, ["a|1|1", "b|1|1"])
        self.assertEqual(result.state.pending_snapshot, ["a|1|1", "b|1|1"])
        self.assertEqual(result.overflow_count, 1)

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


if __name__ == "__main__":
    unittest.main()

