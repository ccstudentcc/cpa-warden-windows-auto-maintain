"""Host bootstrap adapter for AutoMaintainer initialization wiring."""

from __future__ import annotations

import os
import subprocess
import threading
from datetime import datetime
from typing import Any, Callable

from ..channel.channel_status import STAGE_IDLE
from ..state.runtime_state import (
    build_auto_runtime_state,
    build_composed_maintain_runtime_state,
    build_lifecycle_runtime_state,
    build_maintain_queue_state,
    build_snapshot_runtime_state,
    build_ui_runtime_state,
    build_upload_runtime_state,
    build_upload_queue_state,
)
from ...scheduler.smart_scheduler import SmartSchedulerConfig, SmartSchedulerPolicy


def initialize_host_state(
    *,
    host: Any,
    settings: Any,
    detect_panel_capability: Callable[[], bool],
) -> None:
    host.settings = settings
    host.cpa_script = host.settings.base_dir / "cpa_warden.py"
    host.last_uploaded_snapshot_file = host.settings.state_dir / "last_uploaded_snapshot.txt"
    host.current_snapshot_file = host.settings.state_dir / "current_snapshot.txt"
    host.stable_snapshot_file = host.settings.state_dir / "stable_snapshot.txt"
    host.instance_lock_file = host.settings.state_dir / "auto_maintain.lock"
    host.instance_lock_token = None
    host.instance_lock_handle = None
    host.instance_started_at = datetime.now()
    host.shutdown_requested = False
    host.shutdown_reason = None
    host.upload_process = None
    host.maintain_process = None
    host._windows_console_handler = None
    host.deep_scan_counter = 0
    host.pending_upload_retry = False
    host.pending_source_changes_during_upload = False
    host.deferred_upload_snapshot_after_stability_wait = []
    host.last_active_upload_deep_scan_at = 0.0
    host.last_json_count = 0
    host.last_zip_signature = tuple()
    host.pending_upload_snapshot = None
    host.pending_upload_reason = None
    host.inflight_upload_snapshot = None
    host.upload_attempt = 0
    host.maintain_attempt = 0
    host.upload_retry_due_at = 0.0
    host.maintain_retry_due_at = 0.0
    host.pending_maintain = False
    host.pending_maintain_reason = None
    host.pending_maintain_names = None
    host.inflight_maintain_names = None
    host.maintain_names_file = host.settings.state_dir / "maintain_names_scope.txt"
    host.upload_names_file = host.settings.state_dir / "upload_names_scope.txt"
    host.maintain_cmd_output_file = host.settings.state_dir / "maintain_command_output.log"
    host.upload_cmd_output_file = host.settings.state_dir / "upload_command_output.log"
    host.output_lock = threading.Lock()
    host.console_lock = threading.Lock()
    host.upload_progress_state = {"stage": STAGE_IDLE, "done": 0, "total": 0}
    host.maintain_progress_state = {"stage": STAGE_IDLE, "done": 0, "total": 0}
    host.last_progress_render_at = 0.0
    host.progress_render_interval_seconds = 0.4
    host.progress_render_heartbeat_seconds = 8.0
    host.last_progress_signature = ""
    host.panel_height = 8
    host.panel_title = "CPA Warden Auto Dashboard"
    host.panel_enabled = detect_panel_capability()
    host.panel_color_enabled = (
        host.panel_enabled
        and os.getenv("AUTO_MAINTAIN_PANEL_COLOR", "1").strip() not in {"0", "false", "False"}
    )
    host.panel_initialized = False
    host.upload_output_thread = None
    host.maintain_output_thread = None
    host.zip_extract_processed_signatures = {}
    host.last_incremental_maintain_started_at = 0.0
    host.last_incremental_defer_reason = None
    host.next_maintain_due_at = None
    host.scheduler_policy = SmartSchedulerPolicy(
        SmartSchedulerConfig(
            enabled=host.settings.smart_schedule_enabled,
            adaptive_upload_batching=host.settings.adaptive_upload_batching,
            base_upload_batch_size=host.settings.upload_batch_size,
            upload_high_backlog_threshold=host.settings.upload_high_backlog_threshold,
            upload_high_backlog_batch_size=host.settings.upload_high_backlog_batch_size,
            adaptive_maintain_batching=host.settings.adaptive_maintain_batching,
            base_incremental_maintain_batch_size=host.settings.incremental_maintain_batch_size,
            maintain_high_backlog_threshold=host.settings.maintain_high_backlog_threshold,
            maintain_high_backlog_batch_size=host.settings.maintain_high_backlog_batch_size,
            incremental_maintain_min_interval_seconds=(
                host.settings.incremental_maintain_min_interval_seconds
            ),
            incremental_maintain_full_guard_seconds=(
                host.settings.incremental_maintain_full_guard_seconds
            ),
        )
    )
    host.runtime = build_auto_runtime_state(
        upload=build_upload_runtime_state(
            queue=build_upload_queue_state(
                pending_snapshot=host.pending_upload_snapshot,
                pending_reason=host.pending_upload_reason,
                pending_retry=host.pending_upload_retry,
                inflight_snapshot=host.inflight_upload_snapshot,
                attempt=host.upload_attempt,
                retry_due_at=host.upload_retry_due_at,
            ),
            deep_scan_counter=host.deep_scan_counter,
            pending_source_changes_during_upload=host.pending_source_changes_during_upload,
            last_active_upload_deep_scan_at=host.last_active_upload_deep_scan_at,
        ),
        maintain=build_composed_maintain_runtime_state(
            queue=build_maintain_queue_state(
                pending=host.pending_maintain,
                reason=host.pending_maintain_reason,
                names=host.pending_maintain_names,
            ),
            inflight_names=host.inflight_maintain_names,
            attempt=host.maintain_attempt,
            retry_due_at=host.maintain_retry_due_at,
            last_incremental_started_at=host.last_incremental_maintain_started_at,
            last_incremental_defer_reason=host.last_incremental_defer_reason,
        ),
        snapshot=build_snapshot_runtime_state(
            last_uploaded_snapshot_file=host.last_uploaded_snapshot_file,
            current_snapshot_file=host.current_snapshot_file,
            stable_snapshot_file=host.stable_snapshot_file,
            last_json_count=host.last_json_count,
            last_zip_signature=host.last_zip_signature,
            zip_extract_processed_signatures=host.zip_extract_processed_signatures,
        ),
        ui=build_ui_runtime_state(
            upload_progress_state=host.upload_progress_state,
            maintain_progress_state=host.maintain_progress_state,
            last_progress_render_at=host.last_progress_render_at,
            progress_render_interval_seconds=host.progress_render_interval_seconds,
            progress_render_heartbeat_seconds=host.progress_render_heartbeat_seconds,
            last_progress_signature=host.last_progress_signature,
            panel_height=host.panel_height,
            panel_title=host.panel_title,
            panel_enabled=host.panel_enabled,
            panel_color_enabled=host.panel_color_enabled,
            panel_initialized=host.panel_initialized,
        ),
        lifecycle=build_lifecycle_runtime_state(
            instance_started_at=host.instance_started_at,
            shutdown_requested=host.shutdown_requested,
            shutdown_reason=host.shutdown_reason,
            instance_lock_token=host.instance_lock_token,
            instance_lock_handle=host.instance_lock_handle,
            upload_process=host.upload_process,
            maintain_process=host.maintain_process,
            upload_output_thread=host.upload_output_thread,
            maintain_output_thread=host.maintain_output_thread,
            windows_console_handler=host._windows_console_handler,
            next_maintain_due_at=host.next_maintain_due_at,
        ),
    )
