from __future__ import annotations

import argparse
import atexit
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, TextIO

from .config import Settings, load_settings as load_auto_settings
from .channel_status import (
    CHANNEL_MAINTAIN,
    CHANNEL_UPLOAD,
    STAGE_DEFERRED,
    STAGE_FAILED,
    STAGE_IDLE,
    STAGE_PENDING,
    STAGE_PENDING_FULL,
    STAGE_PENDING_INCREMENTAL,
    STAGE_RETRY_WAIT,
    STAGE_RUNNING,
    STAGE_START_FAILED,
    STATE_IDLE,
    STATE_PENDING,
    STATE_RUNNING,
    STATUS_FAILED,
    STATUS_RETRY,
    STATUS_SHUTDOWN,
    STATUS_SUCCESS,
)
from .channel_commands import (
    build_maintain_command as build_maintain_command_rows,
    build_upload_command as build_upload_command_rows,
    format_maintain_start_message as format_maintain_start_message_rows,
    format_upload_start_message as format_upload_start_message_rows,
)
from .channel_feedback import (
    build_non_success_exit_feedback,
    format_command_completed_message,
    format_command_start_failed_message,
    format_command_start_retry_message,
    maintain_pending_progress_stage,
)
from .channel_lifecycle import (
    decide_maintain_start_error,
    decide_upload_start_error,
)
from .channel_start_prep import (
    prepare_maintain_start,
    prepare_upload_start,
)
from .process_supervisor import (
    terminate_channel,
)
from .dashboard import (
    apply_panel_colors as apply_dashboard_panel_colors,
    fit_panel_line as fit_dashboard_panel_line,
    panel_border_line as dashboard_border_line,
)
from .locking import (
    InstanceLockState,
    acquire_instance_lock as acquire_lock_state,
    release_instance_lock as release_lock_state,
)
from .maintain_queue import (
    MaintainRuntimeState,
    MaintainQueueState,
    decide_maintain_start_scope,
    merge_incremental_maintain_names,
    queue_maintain_request,
)
from .output_pump import (
    append_child_output_line as append_output_line,
    start_output_pump_thread,
)
from .panel_render import (
    PanelLinesContext,
    build_plain_panel_lines,
)
from .panel_snapshot import PanelSnapshot, build_panel_snapshot
from .process_output import (
    build_child_process_env,
    decode_child_output_line as decode_process_output_line,
    should_log_child_alert_line as should_log_process_alert_line,
)
from .progress_parser import parse_progress_line
from .runtime_state import (
    build_auto_runtime_state,
    build_composed_maintain_runtime_state,
    build_lifecycle_runtime_state,
    build_maintain_queue_state,
    build_maintain_runtime_state,
    build_snapshot_runtime_state,
    build_ui_runtime_state,
    build_upload_runtime_state,
    build_upload_queue_state,
    unpack_maintain_queue_state,
    unpack_maintain_runtime_state,
    unpack_upload_queue_state,
)
from .runtime.shutdown_runtime import (
    ShutdownRuntimeState,
    current_loop_sleep_seconds as current_loop_sleep_seconds_runtime,
    request_shutdown as request_shutdown_runtime,
    sleep_with_shutdown as sleep_with_shutdown_runtime,
    sleep_between_watch_cycles as sleep_between_watch_cycles_runtime,
)
from .runtime.channel_runtime import (
    poll_maintain_channel,
    poll_upload_channel,
    start_maintain_channel,
    start_upload_channel,
)
from .runtime.channel_runtime_adapter import ChannelRuntimeAdapter
from .runtime.startup_runtime import (
    StartupRuntimeDeps,
    StartupRuntimeState,
    run_startup_cycle,
)
from .runtime.upload_scan_runtime import (
    run_active_upload_probe_cycle,
    run_upload_scan_cycle,
)
from .runtime.upload_runtime_adapter import UploadRuntimeAdapter
from .runtime.watch_runtime import (
    WatchRuntimeDeps,
    WatchRuntimeState,
    run_watch_iteration,
)
from .ui_runtime import UiRuntime
from .scope_files import write_scope_names
from .snapshots import (
    build_snapshot_file as build_snapshot_file_rows,
    build_snapshot_lines as build_snapshot_lines_rows,
    compute_pending_upload_snapshot as compute_pending_upload_snapshot_rows,
    compute_uploaded_baseline as compute_uploaded_baseline_rows,
    extract_names_from_snapshot as extract_names_from_snapshot_rows,
    read_snapshot_lines as read_snapshot_lines_rows,
    write_snapshot_lines as write_snapshot_lines_rows,
)
from .zip_intake import (
    compute_zip_signature as compute_zip_signature_rows,
    extract_zip_with_bandizip as extract_zip_with_bandizip_rows,
    extract_zip_with_windows_builtin as extract_zip_with_windows_builtin_rows,
    inspect_zip_archives as inspect_zip_archives_rows,
)
from .upload_queue import (
    UploadQueueState,
    decide_upload_start,
)
from .upload_postprocess import (
    UploadSuccessPostProcessResult,
    build_upload_success_postprocess,
)
from .upload_cleanup import cleanup_uploaded_files, prune_empty_dirs_under
from ..scheduler.smart_scheduler import SmartSchedulerConfig, SmartSchedulerPolicy


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


class AutoMaintainer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.cpa_script = self.settings.base_dir / "cpa_warden.py"
        self.last_uploaded_snapshot_file = self.settings.state_dir / "last_uploaded_snapshot.txt"
        self.current_snapshot_file = self.settings.state_dir / "current_snapshot.txt"
        self.stable_snapshot_file = self.settings.state_dir / "stable_snapshot.txt"
        self.instance_lock_file = self.settings.state_dir / "auto_maintain.lock"
        self.instance_lock_token: str | None = None
        self.instance_lock_handle: TextIO | None = None
        self.instance_started_at = datetime.now()
        self.shutdown_requested = False
        self.shutdown_reason: str | None = None
        self.upload_process: subprocess.Popen | None = None
        self.maintain_process: subprocess.Popen | None = None
        self._windows_console_handler = None
        self.deep_scan_counter = 0
        self.pending_upload_retry = False
        self.pending_source_changes_during_upload = False
        self.last_active_upload_deep_scan_at = 0.0
        self.last_json_count = 0
        self.last_zip_signature: tuple[str, ...] = tuple()
        self.pending_upload_snapshot: list[str] | None = None
        self.pending_upload_reason: str | None = None
        self.inflight_upload_snapshot: list[str] | None = None
        self.upload_attempt = 0
        self.maintain_attempt = 0
        self.upload_retry_due_at = 0.0
        self.maintain_retry_due_at = 0.0
        self.pending_maintain = False
        self.pending_maintain_reason: str | None = None
        self.pending_maintain_names: set[str] | None = None
        self.inflight_maintain_names: set[str] | None = None
        self.maintain_names_file = self.settings.state_dir / "maintain_names_scope.txt"
        self.upload_names_file = self.settings.state_dir / "upload_names_scope.txt"
        self.maintain_cmd_output_file = self.settings.state_dir / "maintain_command_output.log"
        self.upload_cmd_output_file = self.settings.state_dir / "upload_command_output.log"
        self.output_lock = threading.Lock()
        self.console_lock = threading.Lock()
        self.upload_progress_state: dict[str, int | str] = {"stage": STAGE_IDLE, "done": 0, "total": 0}
        self.maintain_progress_state: dict[str, int | str] = {"stage": STAGE_IDLE, "done": 0, "total": 0}
        self.last_progress_render_at = 0.0
        self.progress_render_interval_seconds = 0.4
        self.progress_render_heartbeat_seconds = 8.0
        self.last_progress_signature = ""
        self.panel_height = 8
        self.panel_title = "CPA Warden Auto Dashboard"
        self.panel_enabled = self._detect_panel_capability()
        self.panel_color_enabled = (
            self.panel_enabled
            and os.getenv("AUTO_MAINTAIN_PANEL_COLOR", "1").strip() not in {"0", "false", "False"}
        )
        self.panel_initialized = False
        self.upload_output_thread: threading.Thread | None = None
        self.maintain_output_thread: threading.Thread | None = None
        self.zip_extract_processed_signatures: dict[str, str] = {}
        self.last_incremental_maintain_started_at = 0.0
        self.last_incremental_defer_reason: str | None = None
        self.next_maintain_due_at: float | None = None
        self.scheduler_policy = SmartSchedulerPolicy(
            SmartSchedulerConfig(
                enabled=self.settings.smart_schedule_enabled,
                adaptive_upload_batching=self.settings.adaptive_upload_batching,
                base_upload_batch_size=self.settings.upload_batch_size,
                upload_high_backlog_threshold=self.settings.upload_high_backlog_threshold,
                upload_high_backlog_batch_size=self.settings.upload_high_backlog_batch_size,
                adaptive_maintain_batching=self.settings.adaptive_maintain_batching,
                base_incremental_maintain_batch_size=self.settings.incremental_maintain_batch_size,
                maintain_high_backlog_threshold=self.settings.maintain_high_backlog_threshold,
                maintain_high_backlog_batch_size=self.settings.maintain_high_backlog_batch_size,
                incremental_maintain_min_interval_seconds=(
                    self.settings.incremental_maintain_min_interval_seconds
                ),
                incremental_maintain_full_guard_seconds=(
                    self.settings.incremental_maintain_full_guard_seconds
                ),
            )
        )
        self.runtime = build_auto_runtime_state(
            upload=build_upload_runtime_state(
                queue=build_upload_queue_state(
                    pending_snapshot=self.pending_upload_snapshot,
                    pending_reason=self.pending_upload_reason,
                    pending_retry=self.pending_upload_retry,
                    inflight_snapshot=self.inflight_upload_snapshot,
                    attempt=self.upload_attempt,
                    retry_due_at=self.upload_retry_due_at,
                ),
                deep_scan_counter=self.deep_scan_counter,
                pending_source_changes_during_upload=self.pending_source_changes_during_upload,
                last_active_upload_deep_scan_at=self.last_active_upload_deep_scan_at,
            ),
            maintain=build_composed_maintain_runtime_state(
                queue=build_maintain_queue_state(
                    pending=self.pending_maintain,
                    reason=self.pending_maintain_reason,
                    names=self.pending_maintain_names,
                ),
                inflight_names=self.inflight_maintain_names,
                attempt=self.maintain_attempt,
                retry_due_at=self.maintain_retry_due_at,
                last_incremental_started_at=self.last_incremental_maintain_started_at,
                last_incremental_defer_reason=self.last_incremental_defer_reason,
            ),
            snapshot=build_snapshot_runtime_state(
                last_uploaded_snapshot_file=self.last_uploaded_snapshot_file,
                current_snapshot_file=self.current_snapshot_file,
                stable_snapshot_file=self.stable_snapshot_file,
                last_json_count=self.last_json_count,
                last_zip_signature=self.last_zip_signature,
                zip_extract_processed_signatures=self.zip_extract_processed_signatures,
            ),
            ui=build_ui_runtime_state(
                upload_progress_state=self.upload_progress_state,
                maintain_progress_state=self.maintain_progress_state,
                last_progress_render_at=self.last_progress_render_at,
                progress_render_interval_seconds=self.progress_render_interval_seconds,
                progress_render_heartbeat_seconds=self.progress_render_heartbeat_seconds,
                last_progress_signature=self.last_progress_signature,
                panel_height=self.panel_height,
                panel_title=self.panel_title,
                panel_enabled=self.panel_enabled,
                panel_color_enabled=self.panel_color_enabled,
                panel_initialized=self.panel_initialized,
            ),
            lifecycle=build_lifecycle_runtime_state(
                instance_started_at=self.instance_started_at,
                shutdown_requested=self.shutdown_requested,
                shutdown_reason=self.shutdown_reason,
                instance_lock_token=self.instance_lock_token,
                instance_lock_handle=self.instance_lock_handle,
                upload_process=self.upload_process,
                maintain_process=self.maintain_process,
                upload_output_thread=self.upload_output_thread,
                maintain_output_thread=self.maintain_output_thread,
                windows_console_handler=self._windows_console_handler,
                next_maintain_due_at=self.next_maintain_due_at,
            ),
        )
        self.ui_runtime = UiRuntime(
            state=self.runtime.ui,
            monotonic=lambda: time.monotonic(),
            build_panel_snapshot=self._build_progress_panel_snapshot,
            build_panel_lines=self._build_progress_panel_lines,
            apply_panel_colors=lambda lines, upload_state, maintain_state, upload_stage, maintain_stage: (
                apply_dashboard_panel_colors(
                    lines,
                    enabled=self.panel_color_enabled,
                    upload_state=upload_state,
                    maintain_state=maintain_state,
                    upload_stage=upload_stage,
                    maintain_stage=maintain_stage,
                )
            ),
            render_panel=self.render_fixed_panel,
            output_lock_factory=lambda: self.output_lock,
        )
        self.upload_runtime_adapter = UploadRuntimeAdapter(
            host=self,
            compute_pending_upload_snapshot=compute_pending_upload_snapshot_rows,
            get_run_upload_scan_cycle=lambda: run_upload_scan_cycle,
            get_run_active_upload_probe_cycle=lambda: run_active_upload_probe_cycle,
            log=log,
        )
        self.channel_runtime_adapter = ChannelRuntimeAdapter(
            host=self,
            get_start_maintain_channel=lambda: start_maintain_channel,
            get_start_upload_channel=lambda: start_upload_channel,
            get_poll_maintain_channel=lambda: poll_maintain_channel,
            get_poll_upload_channel=lambda: poll_upload_channel,
            get_build_child_process_env=lambda: build_child_process_env,
            get_monotonic=lambda: time.monotonic,
            get_popen_factory=lambda: subprocess.Popen,
            log=log,
        )

    def ensure_paths(self) -> None:
        if not self.cpa_script.exists():
            raise RuntimeError(f"cpa_warden.py not found: {self.cpa_script}")

        self.settings.auth_dir.mkdir(parents=True, exist_ok=True)
        if not self.settings.auth_dir.is_dir():
            raise RuntimeError(f"AUTH_DIR is not a directory: {self.settings.auth_dir}")

        self.settings.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.settings.state_dir.is_dir():
            raise RuntimeError(f"STATE_DIR is not a directory: {self.settings.state_dir}")

        self.settings.maintain_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.upload_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.maintain_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.settings.upload_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.maintain_cmd_output_file.parent.mkdir(parents=True, exist_ok=True)
        self.upload_cmd_output_file.parent.mkdir(parents=True, exist_ok=True)

    def _apply_startup_runtime_state(self, state: StartupRuntimeState) -> None:
        self.last_json_count = state.last_json_count
        self.last_zip_signature = state.last_zip_signature
        self.next_maintain_due_at = state.next_maintain_due_at
        self.runtime.snapshot.last_json_count = self.last_json_count
        self.runtime.snapshot.last_zip_signature = self.last_zip_signature
        self.runtime.lifecycle.next_maintain_due_at = self.next_maintain_due_at

    def _apply_watch_runtime_state(self, state: WatchRuntimeState) -> None:
        self.next_maintain_due_at = state.next_maintain_due_at
        self.runtime.lifecycle.next_maintain_due_at = self.next_maintain_due_at

    def _build_startup_runtime_state(self) -> StartupRuntimeState:
        return StartupRuntimeState(
            last_json_count=self.last_json_count,
            last_zip_signature=self.last_zip_signature,
            next_maintain_due_at=self.next_maintain_due_at,
        )

    def _build_startup_runtime_deps(self) -> StartupRuntimeDeps:
        return StartupRuntimeDeps(
            build_snapshot=lambda: self.build_snapshot(self.current_snapshot_file),
            uploaded_snapshot_exists=self.last_uploaded_snapshot_file.exists,
            write_uploaded_snapshot=lambda rows: self.write_snapshot(self.last_uploaded_snapshot_file, rows),
            get_json_count=self.get_json_count,
            inspect_zip_files=self.settings.inspect_zip_files,
            get_zip_signature=self.get_zip_signature,
            inspect_zip_archives=self.inspect_zip_archives,
            run_stage=self._run_stage,
            run_stage_sequence=self._run_stage_sequence,
            check_and_maybe_upload=self.check_and_maybe_upload,
            queue_maintain=self.queue_maintain,
            run_maintain_on_start=self.settings.run_maintain_on_start,
            run_upload_on_start=self.settings.run_upload_on_start,
            maybe_start_maintain=self.maybe_start_maintain,
            maybe_start_upload=self.maybe_start_upload,
            render_progress_snapshot=self.render_progress_snapshot,
            monotonic=time.monotonic,
            maintain_interval_seconds=self.settings.maintain_interval_seconds,
            log=log,
        )

    def _build_watch_runtime_state(self) -> WatchRuntimeState:
        return WatchRuntimeState(next_maintain_due_at=self.next_maintain_due_at)

    def _build_watch_runtime_deps(self) -> WatchRuntimeDeps:
        return WatchRuntimeDeps(
            maintain_interval_seconds=self.settings.maintain_interval_seconds,
            upload_running=lambda: self.upload_process is not None,
            has_pending_upload_snapshot=lambda: self.pending_upload_snapshot is not None,
            queue_maintain=self.queue_maintain,
            run_stage=self._run_stage,
            run_stage_sequence=self._run_stage_sequence,
            poll_upload_process=self.poll_upload_process,
            poll_maintain_process=self.poll_maintain_process,
            probe_changes_during_active_upload=self.probe_changes_during_active_upload,
            check_and_maybe_upload=self.check_and_maybe_upload,
            maybe_start_upload=self.maybe_start_upload,
            maybe_start_maintain=self.maybe_start_maintain,
            render_progress_snapshot=self.render_progress_snapshot,
        )

    def _run_runtime_cycle(
        self,
        *,
        run_cycle: Callable[[], object],
        apply_state: Callable[[object], None],
    ) -> int:
        result = run_cycle()
        apply_state(getattr(result, "state"))
        return int(getattr(result, "exit_code"))

    def _run_startup_phase(self) -> int:
        return self._run_runtime_cycle(
            run_cycle=lambda: run_startup_cycle(
                state=self._build_startup_runtime_state(),
                deps=self._build_startup_runtime_deps(),
            ),
            apply_state=lambda state: self._apply_startup_runtime_state(state),
        )

    def _run_watch_iteration(self, now: float) -> int:
        return self._run_runtime_cycle(
            run_cycle=lambda: run_watch_iteration(
                now_monotonic=now,
                state=self._build_watch_runtime_state(),
                deps=self._build_watch_runtime_deps(),
            ),
            apply_state=lambda state: self._apply_watch_runtime_state(state),
        )

    def _run_stage_sequence(self, stages: Iterable[tuple[str, Callable[[], int]]]) -> int:
        for stage, runner in stages:
            handled_exit = self._run_stage(stage, runner)
            if handled_exit != 0:
                return handled_exit
        return 0

    def _run_stage(self, stage: str, runner: Callable[[], int]) -> int:
        return self._resolve_stage_failure(stage, runner())

    def _resolve_stage_failure(self, stage: str, code: int) -> int:
        if code == 0:
            return 0
        if self.handle_failure(stage, code):
            return 0
        return code

    def _is_run_once_cycle_complete(self) -> bool:
        nothing_running = self.upload_process is None and self.maintain_process is None
        nothing_pending = (
            self.pending_upload_snapshot is None
            and not self.pending_maintain
            and (not self.pending_upload_retry)
        )
        return nothing_running and nothing_pending

    def _sleep_between_watch_cycles(self) -> int | None:
        return sleep_between_watch_cycles_runtime(
            run_once=self.settings.run_once,
            is_run_once_cycle_complete=self._is_run_once_cycle_complete,
            sleep_with_shutdown_fn=self.sleep_with_shutdown,
            current_loop_sleep_seconds_fn=self.current_loop_sleep_seconds,
            log=log,
        )

    def _run_watch_cycle_and_maybe_sleep(self) -> int | None:
        now = time.monotonic()
        watch_exit = self._run_watch_iteration(now)
        if watch_exit != 0:
            return watch_exit
        return self._sleep_between_watch_cycles()

    def run(self) -> int:
        self.ensure_paths()
        self.register_shutdown_handlers()
        self.acquire_instance_lock()
        atexit.register(self.release_instance_lock)
        self.set_console_title()
        self.init_fixed_panel_area()
        self.log_settings()
        try:
            startup_exit = self._run_startup_phase()
            if startup_exit != 0:
                return startup_exit

            while True:
                cycle_exit = self._run_watch_cycle_and_maybe_sleep()
                if cycle_exit is not None:
                    return cycle_exit
        finally:
            self.release_instance_lock()

    def _settings_log_rows(self) -> list[tuple[str, str | int | Path]]:
        return [
            ("WATCH_CONFIG_PATH", self.settings.watch_config_path or "(none)"),
            ("AUTH_DIR", self.settings.auth_dir),
            ("STATE_DIR", self.settings.state_dir),
            ("MAINTAIN_DB_PATH", self.settings.maintain_db_path),
            ("UPLOAD_DB_PATH", self.settings.upload_db_path),
            ("MAINTAIN_LOG_FILE", self.settings.maintain_log_file),
            ("UPLOAD_LOG_FILE", self.settings.upload_log_file),
            ("MAINTAIN_COMMAND_OUTPUT_FILE", self.maintain_cmd_output_file),
            ("UPLOAD_COMMAND_OUTPUT_FILE", self.upload_cmd_output_file),
            ("MAINTAIN_NAMES_SCOPE_FILE", self.maintain_names_file),
            ("UPLOAD_NAMES_SCOPE_FILE", self.upload_names_file),
            ("MAINTAIN_INTERVAL_SECONDS", self.settings.maintain_interval_seconds),
            ("WATCH_INTERVAL_SECONDS", self.settings.watch_interval_seconds),
            ("UPLOAD_STABLE_WAIT_SECONDS", self.settings.upload_stable_wait_seconds),
            ("UPLOAD_BATCH_SIZE", self.settings.upload_batch_size),
            ("SMART_SCHEDULE_ENABLED", int(self.settings.smart_schedule_enabled)),
            ("ADAPTIVE_UPLOAD_BATCHING", int(self.settings.adaptive_upload_batching)),
            ("UPLOAD_HIGH_BACKLOG_THRESHOLD", self.settings.upload_high_backlog_threshold),
            ("UPLOAD_HIGH_BACKLOG_BATCH_SIZE", self.settings.upload_high_backlog_batch_size),
            ("ADAPTIVE_MAINTAIN_BATCHING", int(self.settings.adaptive_maintain_batching)),
            ("INCREMENTAL_MAINTAIN_BATCH_SIZE", self.settings.incremental_maintain_batch_size),
            ("MAINTAIN_HIGH_BACKLOG_THRESHOLD", self.settings.maintain_high_backlog_threshold),
            ("MAINTAIN_HIGH_BACKLOG_BATCH_SIZE", self.settings.maintain_high_backlog_batch_size),
            (
                "INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS",
                self.settings.incremental_maintain_min_interval_seconds,
            ),
            (
                "INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS",
                self.settings.incremental_maintain_full_guard_seconds,
            ),
            ("RUN_MAINTAIN_ON_START", int(self.settings.run_maintain_on_start)),
            ("RUN_UPLOAD_ON_START", int(self.settings.run_upload_on_start)),
            ("RUN_MAINTAIN_AFTER_UPLOAD", int(self.settings.run_maintain_after_upload)),
            ("MAINTAIN_ASSUME_YES", int(self.settings.maintain_assume_yes)),
            (
                "DELETE_UPLOADED_FILES_AFTER_UPLOAD",
                int(self.settings.delete_uploaded_files_after_upload),
            ),
            ("INSPECT_ZIP_FILES", int(self.settings.inspect_zip_files)),
            ("AUTO_EXTRACT_ZIP_JSON", int(self.settings.auto_extract_zip_json)),
            ("DELETE_ZIP_AFTER_EXTRACT", int(self.settings.delete_zip_after_extract)),
            ("BANDIZIP_PATH", self.settings.bandizip_path),
            ("BANDIZIP_TIMEOUT_SECONDS", self.settings.bandizip_timeout_seconds),
            ("USE_WINDOWS_ZIP_FALLBACK", int(self.settings.use_windows_zip_fallback)),
            ("DEEP_SCAN_INTERVAL_LOOPS", self.settings.deep_scan_interval_loops),
            ("ACTIVE_PROBE_INTERVAL_SECONDS", self.settings.active_probe_interval_seconds),
            (
                "ACTIVE_UPLOAD_DEEP_SCAN_INTERVAL_SECONDS",
                self.settings.active_upload_deep_scan_interval_seconds,
            ),
            ("MAINTAIN_RETRY_COUNT", self.settings.maintain_retry_count),
            ("UPLOAD_RETRY_COUNT", self.settings.upload_retry_count),
            ("COMMAND_RETRY_DELAY_SECONDS", self.settings.command_retry_delay_seconds),
            ("CONTINUE_ON_COMMAND_FAILURE", int(self.settings.continue_on_command_failure)),
            ("ALLOW_MULTI_INSTANCE", int(self.settings.allow_multi_instance)),
            ("INSTANCE_LABEL", self.instance_label()),
            ("INSTANCE_LOCK_FILE", self.instance_lock_file),
        ]

    def log_settings(self) -> None:
        log("Started auto maintenance loop.")
        for key, value in self._settings_log_rows():
            log(f"{key}={value}")
        if self.instance_lock_token:
            log(f"INSTANCE_LOCK_TOKEN={self.instance_lock_token}")

    def instance_label(self) -> str:
        started = self.instance_started_at.strftime("%Y-%m-%d %H:%M:%S")
        return f"cpa-auto-maintain | {started} | {os.getpid()}"

    def set_console_title(self) -> None:
        if os.name != "nt":
            return
        try:
            import ctypes

            ctypes.windll.kernel32.SetConsoleTitleW(self.instance_label())
        except Exception as exc:
            log(f"[WARN] Failed to set console title: {exc}")

    def register_shutdown_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._signal_handler)

        if os.name == "nt":
            try:
                import ctypes

                CTRL_C_EVENT = 0
                CTRL_BREAK_EVENT = 1
                CTRL_CLOSE_EVENT = 2
                CTRL_LOGOFF_EVENT = 5
                CTRL_SHUTDOWN_EVENT = 6

                event_names = {
                    CTRL_C_EVENT: "CTRL_C_EVENT",
                    CTRL_BREAK_EVENT: "CTRL_BREAK_EVENT",
                    CTRL_CLOSE_EVENT: "CTRL_CLOSE_EVENT",
                    CTRL_LOGOFF_EVENT: "CTRL_LOGOFF_EVENT",
                    CTRL_SHUTDOWN_EVENT: "CTRL_SHUTDOWN_EVENT",
                }

                handler_type = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)

                def _handler(event_type: int) -> bool:
                    reason = event_names.get(event_type, f"WIN_EVENT_{event_type}")
                    self.request_shutdown(reason)
                    return True

                self._windows_console_handler = handler_type(_handler)
                ctypes.windll.kernel32.SetConsoleCtrlHandler(self._windows_console_handler, True)
            except Exception as exc:
                log(f"[WARN] Failed to register Windows console handler: {exc}")

    def _signal_handler(self, signum: int, _frame: object) -> None:
        self.request_shutdown(f"SIGNAL_{signum}")

    def request_shutdown(self, reason: str) -> None:
        state = request_shutdown_runtime(
            state=ShutdownRuntimeState(
                shutdown_requested=self.shutdown_requested,
                shutdown_reason=self.shutdown_reason,
            ),
            reason=reason,
            log=log,
            terminate_active_processes=self.terminate_active_processes,
        )
        self.shutdown_requested = state.shutdown_requested
        self.shutdown_reason = state.shutdown_reason
        self.runtime.lifecycle.shutdown_requested = self.shutdown_requested
        self.runtime.lifecycle.shutdown_reason = self.shutdown_reason

    def terminate_process(self, proc: subprocess.Popen | None, *, name: str) -> None:
        terminate_channel(
            process=proc,
            channel=name,
            log=log,
            terminate_timeout_seconds=8,
            kill_wait_seconds=5,
        )

    def terminate_active_processes(self) -> None:
        self.terminate_process(self.upload_process, name=CHANNEL_UPLOAD)
        self.terminate_process(self.maintain_process, name=CHANNEL_MAINTAIN)

    def sleep_with_shutdown(self, total_seconds: int) -> bool:
        return sleep_with_shutdown_runtime(
            shutdown_requested=self.shutdown_requested,
            total_seconds=total_seconds,
        )

    def current_loop_sleep_seconds(self) -> int:
        return current_loop_sleep_seconds_runtime(
            upload_running=self.upload_process is not None,
            maintain_running=self.maintain_process is not None,
            watch_interval_seconds=self.settings.watch_interval_seconds,
            active_probe_interval_seconds=self.settings.active_probe_interval_seconds,
        )

    def wait_for_stable_snapshot(self, baseline_snapshot: list[str]) -> tuple[int, list[str] | None]:
        stable_seconds = self.settings.upload_stable_wait_seconds
        if stable_seconds <= 0:
            return 0, baseline_snapshot

        poll_seconds = min(2.0, max(0.5, stable_seconds / 4.0))
        last_change_at = time.time()
        stable_snapshot = baseline_snapshot

        while True:
            elapsed = time.time() - last_change_at
            remaining = stable_seconds - elapsed
            if remaining <= 0:
                return 0, stable_snapshot

            if not self.sleep_with_shutdown(min(poll_seconds, remaining)):
                return 130, None

            latest_snapshot = self.build_snapshot(self.stable_snapshot_file)
            if latest_snapshot != stable_snapshot:
                stable_snapshot = latest_snapshot
                last_change_at = time.time()
                log(
                    "Detected additional JSON changes during stability wait. "
                    f"Reset timer to {stable_seconds}s."
                )

    def acquire_instance_lock(self) -> None:
        state = acquire_lock_state(
            lock_file=self.instance_lock_file,
            state_dir=self.settings.state_dir,
            allow_multi_instance=self.settings.allow_multi_instance,
            log=log,
        )
        self.instance_lock_token = state.token
        self.instance_lock_handle = state.handle
        self.runtime.lifecycle.instance_lock_token = self.instance_lock_token
        self.runtime.lifecycle.instance_lock_handle = self.instance_lock_handle

    def acquire_instance_lock_windows(self) -> None:
        if os.name != "nt":
            raise RuntimeError("Windows lock backend unavailable (msvcrt).")
        state = acquire_lock_state(
            lock_file=self.instance_lock_file,
            state_dir=self.settings.state_dir,
            allow_multi_instance=False,
            log=log,
        )
        self.instance_lock_token = state.token
        self.instance_lock_handle = state.handle
        self.runtime.lifecycle.instance_lock_token = self.instance_lock_token
        self.runtime.lifecycle.instance_lock_handle = self.instance_lock_handle

    def release_instance_lock(self) -> None:
        state = InstanceLockState(token=self.instance_lock_token, handle=self.instance_lock_handle)
        next_state = release_lock_state(
            lock_file=self.instance_lock_file,
            allow_multi_instance=self.settings.allow_multi_instance,
            state=state,
            log=log,
        )
        self.instance_lock_token = next_state.token
        self.instance_lock_handle = next_state.handle
        self.runtime.lifecycle.instance_lock_token = self.instance_lock_token
        self.runtime.lifecycle.instance_lock_handle = self.instance_lock_handle

    def get_json_paths(self) -> list[Path]:
        return sorted(
            (p for p in self.settings.auth_dir.rglob("*.json") if p.is_file()),
            key=lambda p: str(p).lower(),
        )

    def get_json_count(self) -> int:
        return len(self.get_json_paths())

    def get_zip_signature(self) -> tuple[str, ...]:
        return compute_zip_signature_rows(self.settings.auth_dir, log=log)

    def inspect_zip_archives(self) -> bool:
        return inspect_zip_archives_rows(
            auth_dir=self.settings.auth_dir,
            inspect_zip_files=self.settings.inspect_zip_files,
            auto_extract_zip_json=self.settings.auto_extract_zip_json,
            delete_zip_after_extract=self.settings.delete_zip_after_extract,
            processed_signatures=self.zip_extract_processed_signatures,
            extract_zip=self.extract_zip_with_bandizip,
            log=log,
        )

    def extract_zip_with_bandizip(self, zip_path: Path, output_dir: Path) -> int:
        exit_code = extract_zip_with_bandizip_rows(
            zip_path=zip_path,
            output_dir=output_dir,
            base_dir=self.settings.base_dir,
            bandizip_path=self.settings.bandizip_path,
            timeout_seconds=self.settings.bandizip_timeout_seconds,
            log=log,
        )
        if exit_code == 0:
            return 0

        if self.settings.use_windows_zip_fallback:
            log(f"Trying Windows built-in unzip fallback: {zip_path.name}")
            return extract_zip_with_windows_builtin_rows(
                zip_path=zip_path,
                output_dir=output_dir,
                base_dir=self.settings.base_dir,
                timeout_seconds=self.settings.bandizip_timeout_seconds,
                log=log,
            )

        return exit_code

    def snapshot_lines(self) -> list[str]:
        return build_snapshot_lines_rows(self.get_json_paths(), log=log)

    def write_snapshot(self, target: Path, lines: Iterable[str]) -> None:
        write_snapshot_lines_rows(target, lines)

    def read_snapshot(self, source: Path) -> list[str]:
        return read_snapshot_lines_rows(source)

    def build_snapshot(self, target: Path) -> list[str]:
        return build_snapshot_file_rows(
            target=target,
            paths=self.get_json_paths(),
            log=log,
        )

    def delete_uploaded_files_from_snapshot(self, snapshot_lines: list[str]) -> None:
        result = cleanup_uploaded_files(snapshot_lines)
        log(
            "Upload cleanup summary: "
            f"deleted={result.deleted}, skipped_changed={result.skipped_changed}, "
            f"skipped_missing={result.skipped_missing}, failed={result.failed}"
        )
        self.prune_empty_dirs_under_auth_dir()

    def prune_empty_dirs_under_auth_dir(self) -> None:
        result = prune_empty_dirs_under(self.settings.auth_dir)
        log(
            "Upload empty-dir cleanup summary: "
            f"removed={result.removed}, skipped_non_empty={result.skipped_non_empty}, "
            f"skipped_missing={result.skipped_missing}, failed={result.failed}"
        )

    def command_base(self) -> list[str]:
        cmd = [sys.executable, str(self.cpa_script)]
        if self.settings.config_path is not None:
            cmd.extend(["--config", str(self.settings.config_path)])
        return cmd

    def update_channel_progress(
        self,
        name: str,
        *,
        stage: str | None = None,
        done: int | None = None,
        total: int | None = None,
        force_render: bool = False,
    ) -> None:
        self._sync_ui_runtime_inputs()
        self.ui_runtime.on_stage_update(
            name,
            stage=stage,
            done=done,
            total=total,
            force_render=force_render,
        )
        self._sync_ui_runtime_outputs()

    def mark_channel_running(self, name: str) -> None:
        self.update_channel_progress(name, stage=STAGE_RUNNING, force_render=True)

    def _format_bar(self, done: int, total: int, width: int = 18) -> str:
        if total <= 0:
            return "[" + ("-" * width) + "]"
        ratio = min(1.0, max(0.0, float(done) / float(total)))
        filled = int(round(ratio * width))
        return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

    def _compute_upload_queue_batches(self, pending_count: int) -> tuple[int, int]:
        if pending_count <= 0:
            return 0, 0
        maintain_pressure = self.maintain_process is not None or self.pending_maintain
        next_batch_size = self.scheduler_policy.choose_upload_batch_size(
            pending_count=pending_count,
            maintain_pressure=maintain_pressure,
        )
        if next_batch_size <= 0:
            return 0, 0
        queue_batches = (pending_count + next_batch_size - 1) // next_batch_size
        return next_batch_size, queue_batches

    def _short_reason(self, reason: str, limit: int = 36) -> str:
        text = (reason or "-").strip()
        if len(text) <= limit:
            return text
        return text[: max(1, limit - 3)] + "..."

    def decode_child_output_line(self, raw: bytes | str) -> str:
        return decode_process_output_line(raw)

    def should_log_child_alert_line(self, text: str) -> bool:
        return should_log_process_alert_line(text)

    def _build_progress_panel_snapshot(self, *, now_monotonic: float) -> PanelSnapshot:
        return build_panel_snapshot(
            upload_progress_state=self.upload_progress_state,
            maintain_progress_state=self.maintain_progress_state,
            pending_upload_snapshot=self.pending_upload_snapshot,
            inflight_upload_snapshot=self.inflight_upload_snapshot,
            pending_upload_reason=self.pending_upload_reason,
            upload_running=self.upload_process is not None,
            upload_retry_due_at=self.upload_retry_due_at,
            pending_maintain=self.pending_maintain,
            pending_maintain_names=self.pending_maintain_names,
            pending_maintain_reason=self.pending_maintain_reason,
            inflight_maintain_names=self.inflight_maintain_names,
            maintain_running=self.maintain_process is not None,
            maintain_retry_due_at=self.maintain_retry_due_at,
            next_maintain_due_at=self.next_maintain_due_at,
            last_incremental_defer_reason=self.last_incremental_defer_reason,
            now_monotonic=now_monotonic,
            compute_upload_queue_batches=self._compute_upload_queue_batches,
            choose_incremental_maintain_batch_size=(
                lambda pending_count, upload_pressure: self.scheduler_policy.choose_incremental_maintain_batch_size(
                    pending_count=pending_count,
                    upload_pressure=upload_pressure,
                )
            ),
        )

    def _build_progress_panel_lines(self, *, panel_snapshot: PanelSnapshot) -> list[str]:
        context = PanelLinesContext(
            panel_title=self.panel_title,
            now_text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            panel_mode="fixed" if self.panel_enabled else "log",
            watch_interval_seconds=self.settings.watch_interval_seconds,
            upload_bar=self._format_bar(panel_snapshot.upload_done, panel_snapshot.upload_total),
            maintain_bar=self._format_bar(panel_snapshot.maintain_done, panel_snapshot.maintain_total),
            upload_reason_text=self._short_reason(panel_snapshot.upload_reason, limit=40),
            maintain_reason_text=self._short_reason(panel_snapshot.maintain_reason, limit=40),
            maintain_defer_text=self._short_reason(panel_snapshot.maintain_defer_reason, limit=28),
        )
        return build_plain_panel_lines(
            snapshot=panel_snapshot,
            context=context,
            fit_line=fit_dashboard_panel_line,
            border_line=dashboard_border_line,
        )

    def render_progress_snapshot(self, *, force: bool = False) -> None:
        self._sync_ui_runtime_inputs()
        self.ui_runtime.render_if_needed(force=force)
        self._sync_ui_runtime_outputs()

    def _sync_ui_runtime_inputs(self) -> None:
        self.runtime.ui.upload_progress_state = self.upload_progress_state
        self.runtime.ui.maintain_progress_state = self.maintain_progress_state
        self.runtime.ui.last_progress_render_at = self.last_progress_render_at
        self.runtime.ui.progress_render_interval_seconds = self.progress_render_interval_seconds
        self.runtime.ui.progress_render_heartbeat_seconds = self.progress_render_heartbeat_seconds
        self.runtime.ui.last_progress_signature = self.last_progress_signature

    def _sync_ui_runtime_outputs(self) -> None:
        self.upload_progress_state = self.runtime.ui.upload_progress_state
        self.maintain_progress_state = self.runtime.ui.maintain_progress_state
        self.last_progress_render_at = self.runtime.ui.last_progress_render_at
        self.last_progress_signature = self.runtime.ui.last_progress_signature

    def parse_child_progress_line(self, name: str, line: str) -> None:
        text = line.strip()
        if not text:
            return
        current_upload_total = int(self.upload_progress_state.get("total") or 0)
        parsed = parse_progress_line(
            channel=name,
            text=text,
            current_upload_total=current_upload_total,
            should_log_alert_line=self.should_log_child_alert_line,
        )
        for update in parsed.updates:
            self.update_channel_progress(
                update.channel,
                stage=update.stage,
                done=update.done,
                total=update.total,
                force_render=update.force_render,
            )
            return

        if parsed.should_log_alert:
            log(f"[{name}] {text}")

    def _write_child_output_line(self, name: str, line: str) -> None:
        target = self.upload_cmd_output_file if name == CHANNEL_UPLOAD else self.maintain_cmd_output_file
        append_output_line(target=target, line=line)

    def start_output_pump(self, name: str, proc: subprocess.Popen) -> None:
        def _on_line(line: str) -> None:
            self._write_child_output_line(name, line)
            self.parse_child_progress_line(name, line)

        thread = start_output_pump_thread(
            channel=name,
            proc=proc,
            decode_line=self.decode_child_output_line,
            on_line=_on_line,
            warn=log,
        )
        if thread is None:
            return
        if name == CHANNEL_UPLOAD:
            self.upload_output_thread = thread
        else:
            self.maintain_output_thread = thread

    def _detect_panel_capability(self) -> bool:
        if not sys.stdout.isatty():
            return False
        if os.getenv("AUTO_MAINTAIN_FIXED_PANEL", "1").strip() in {"0", "false", "False"}:
            return False
        if os.name != "nt":
            return True
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            if handle in (0, -1):
                return False

            mode = ctypes.c_uint()
            if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                return False

            enable_vt = 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
            if mode.value & enable_vt:
                return True
            if not kernel32.SetConsoleMode(handle, mode.value | enable_vt):
                return False
            return True
        except Exception:
            return False

    def init_fixed_panel_area(self) -> None:
        if (not self.panel_enabled) or self.panel_initialized:
            return
        with self.console_lock:
            if self.panel_initialized:
                return
            sys.stdout.write("\n" * self.panel_height)
            sys.stdout.flush()
            self.panel_initialized = True
            self.runtime.ui.panel_initialized = self.panel_initialized

    def render_fixed_panel(self, lines: list[str]) -> None:
        if not self.panel_enabled:
            for line in lines:
                log(line)
            return

        self.init_fixed_panel_area()
        with self.console_lock:
            # Save cursor, jump to top-left, overwrite reserved panel rows, then restore cursor.
            sys.stdout.write("\x1b[s")
            sys.stdout.write("\x1b[H")
            for idx in range(self.panel_height):
                row_text = lines[idx] if idx < len(lines) else ""
                sys.stdout.write("\x1b[2K")
                sys.stdout.write(row_text)
                if idx < self.panel_height - 1:
                    sys.stdout.write("\n")
            sys.stdout.write("\x1b[u")
            sys.stdout.flush()

    def build_maintain_command(self, maintain_names_file: Path | None = None) -> list[str]:
        return build_maintain_command_rows(
            command_base=self.command_base(),
            maintain_db_path=self.settings.maintain_db_path,
            maintain_log_file=self.settings.maintain_log_file,
            maintain_names_file=maintain_names_file,
            assume_yes=self.settings.maintain_assume_yes,
        )

    def build_upload_command(self, upload_names_file: Path | None = None) -> list[str]:
        return build_upload_command_rows(
            command_base=self.command_base(),
            auth_dir=self.settings.auth_dir,
            upload_db_path=self.settings.upload_db_path,
            upload_log_file=self.settings.upload_log_file,
            upload_names_file=upload_names_file,
        )

    def _upload_queue_state(self) -> UploadQueueState:
        return build_upload_queue_state(
            pending_snapshot=self.pending_upload_snapshot,
            pending_reason=self.pending_upload_reason,
            pending_retry=self.pending_upload_retry,
            inflight_snapshot=self.inflight_upload_snapshot,
            attempt=self.upload_attempt,
            retry_due_at=self.upload_retry_due_at,
        )

    def _apply_upload_queue_state(self, state: UploadQueueState) -> None:
        (
            self.pending_upload_snapshot,
            self.pending_upload_reason,
            self.pending_upload_retry,
            self.inflight_upload_snapshot,
            self.upload_attempt,
            self.upload_retry_due_at,
        ) = unpack_upload_queue_state(state)
        self.runtime.upload.queue = build_upload_queue_state(
            pending_snapshot=self.pending_upload_snapshot,
            pending_reason=self.pending_upload_reason,
            pending_retry=self.pending_upload_retry,
            inflight_snapshot=self.inflight_upload_snapshot,
            attempt=self.upload_attempt,
            retry_due_at=self.upload_retry_due_at,
        )

    def _maintain_queue_state(self) -> MaintainQueueState:
        return build_maintain_queue_state(
            pending=self.pending_maintain,
            reason=self.pending_maintain_reason,
            names=self.pending_maintain_names,
        )

    def _apply_maintain_queue_state(self, state: MaintainQueueState) -> None:
        (
            self.pending_maintain,
            self.pending_maintain_reason,
            self.pending_maintain_names,
        ) = unpack_maintain_queue_state(state)
        self.runtime.maintain.queue = build_maintain_queue_state(
            pending=self.pending_maintain,
            reason=self.pending_maintain_reason,
            names=self.pending_maintain_names,
        )

    def _maintain_runtime_state(self) -> MaintainRuntimeState:
        return build_maintain_runtime_state(
            queue=self._maintain_queue_state(),
            inflight_names=self.inflight_maintain_names,
            attempt=self.maintain_attempt,
            retry_due_at=self.maintain_retry_due_at,
        )

    def _apply_maintain_runtime_state(self, state: MaintainRuntimeState) -> None:
        queue_state, inflight_names, attempt, retry_due_at = unpack_maintain_runtime_state(state)
        self._apply_maintain_queue_state(queue_state)
        self.inflight_maintain_names = inflight_names
        self.maintain_attempt = attempt
        self.maintain_retry_due_at = retry_due_at
        self.runtime.maintain = build_composed_maintain_runtime_state(
            queue=build_maintain_queue_state(
                pending=self.pending_maintain,
                reason=self.pending_maintain_reason,
                names=self.pending_maintain_names,
            ),
            inflight_names=self.inflight_maintain_names,
            attempt=self.maintain_attempt,
            retry_due_at=self.maintain_retry_due_at,
            last_incremental_started_at=self.last_incremental_maintain_started_at,
            last_incremental_defer_reason=self.last_incremental_defer_reason,
        )

    def queue_maintain(self, reason: str, names: set[str] | None = None) -> None:
        result = queue_maintain_request(
            state=self._maintain_queue_state(),
            reason=reason,
            names=names,
        )
        self._apply_maintain_queue_state(result.state)
        if result.progress_stage:
            self.update_channel_progress(CHANNEL_MAINTAIN, stage=result.progress_stage, force_render=True)

    def merge_pending_incremental_maintain_names(self, names: set[str]) -> None:
        state = merge_incremental_maintain_names(
            state=self._maintain_queue_state(),
            names=names,
        )
        self._apply_maintain_queue_state(state)

    def _defer_incremental_maintain_if_needed(self, now: float) -> bool:
        if self.pending_maintain_names is None:
            return False
        defer_incremental, defer_reason = self.scheduler_policy.should_defer_incremental_maintain(
            now_monotonic=now,
            last_incremental_started_at=self.last_incremental_maintain_started_at,
            next_full_maintain_due_at=self.next_maintain_due_at,
            has_pending_full_maintain=(
                self.pending_maintain and self.pending_maintain_names is None
            ),
        )
        if not defer_incremental:
            return False
        if defer_reason != self.last_incremental_defer_reason:
            log(f"Deferred incremental maintain start: {defer_reason}.")
            self.last_incremental_defer_reason = defer_reason
            self.update_channel_progress(CHANNEL_MAINTAIN, stage=STAGE_DEFERRED, force_render=True)
        return True

    def _set_maintain_process(self, process: subprocess.Popen | None) -> None:
        self.maintain_process = process
        self.runtime.lifecycle.maintain_process = self.maintain_process

    def _set_upload_process(self, process: subprocess.Popen | None) -> None:
        self.upload_process = process
        self.runtime.lifecycle.upload_process = self.upload_process

    def maybe_start_maintain(self) -> int:
        return self.channel_runtime_adapter.maybe_start_maintain()

    def maybe_start_upload(self) -> int:
        return self.channel_runtime_adapter.maybe_start_upload()

    def handle_command_start_error(self, name: str, exc: Exception) -> int:
        # Kept for compatibility with tests/hooks that may invoke this path directly.
        return self.channel_runtime_adapter.handle_command_start_error(name, exc)

    def poll_maintain_process(self) -> int:
        return self.channel_runtime_adapter.poll_maintain_process()

    def poll_upload_process(self) -> int:
        return self.channel_runtime_adapter.poll_upload_process()

    def probe_changes_during_active_upload(self) -> int:
        return self.upload_runtime_adapter.probe_changes_during_active_upload()

    def handle_failure(self, stage: str, code: int) -> bool:
        log(f"{stage} failed with exit {code}.")
        if self.settings.run_once:
            log("Run-once mode enabled. Exiting on failure.")
            return False
        if self.settings.continue_on_command_failure:
            log("Continue mode enabled. Keep loop alive.")
            return True
        return False

    def check_and_maybe_upload(
        self,
        force_deep_scan: bool = False,
        *,
        preserve_retry_state: bool = False,
        skip_stability_wait: bool = False,
        queue_reason: str = "detected JSON changes",
    ) -> int:
        return self.upload_runtime_adapter.check_and_maybe_upload(
            force_deep_scan=force_deep_scan,
            preserve_retry_state=preserve_retry_state,
            skip_stability_wait=skip_stability_wait,
            queue_reason=queue_reason,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto maintain + folder watch uploader for cpa-warden.",
    )
    parser.add_argument(
        "--watch-config",
        help="Watcher config JSON path (env WATCH_CONFIG_PATH also supported).",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    return parser.parse_args()


def load_settings(args: argparse.Namespace) -> Settings:
    base_dir = Path(__file__).resolve().parents[2]
    return load_auto_settings(
        args=args,
        base_dir=base_dir,
        log=log,
    )


def main() -> int:
    args = parse_args()
    try:
        settings = load_settings(args)
        maintainer = AutoMaintainer(settings)
        return maintainer.run()
    except KeyboardInterrupt:
        log("Interrupted by user.")
        return 130
    except Exception as exc:
        log(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

