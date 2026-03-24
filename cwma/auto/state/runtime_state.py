from __future__ import annotations

import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import TextIO

from .maintain_queue import MaintainQueueState, MaintainRuntimeState as LegacyMaintainRuntimeState
from .state_models import (
    AutoRuntimeState,
    LifecycleRuntimeState,
    MaintainRuntimeState,
    SnapshotRuntimeState,
    UiRuntimeState,
    UploadRuntimeState,
)
from .upload_queue import UploadQueueState


def build_upload_queue_state(
    *,
    pending_snapshot: list[str] | None,
    pending_reason: str | None,
    pending_retry: bool,
    inflight_snapshot: list[str] | None,
    attempt: int,
    retry_due_at: float,
) -> UploadQueueState:
    return UploadQueueState(
        pending_snapshot=pending_snapshot,
        pending_reason=pending_reason,
        pending_retry=pending_retry,
        inflight_snapshot=inflight_snapshot,
        attempt=attempt,
        retry_due_at=retry_due_at,
    )


def unpack_upload_queue_state(
    state: UploadQueueState,
) -> tuple[list[str] | None, str | None, bool, list[str] | None, int, float]:
    return (
        state.pending_snapshot,
        state.pending_reason,
        state.pending_retry,
        state.inflight_snapshot,
        state.attempt,
        state.retry_due_at,
    )


def build_maintain_queue_state(
    *,
    pending: bool,
    reason: str | None,
    names: set[str] | None,
) -> MaintainQueueState:
    return MaintainQueueState(
        pending=pending,
        reason=reason,
        names=names,
    )


def unpack_maintain_queue_state(
    state: MaintainQueueState,
) -> tuple[bool, str | None, set[str] | None]:
    return (state.pending, state.reason, state.names)


def build_maintain_runtime_state(
    *,
    queue: MaintainQueueState,
    inflight_names: set[str] | None,
    attempt: int,
    retry_due_at: float,
) -> LegacyMaintainRuntimeState:
    return LegacyMaintainRuntimeState(
        queue=queue,
        inflight_names=inflight_names,
        attempt=attempt,
        retry_due_at=retry_due_at,
    )


def unpack_maintain_runtime_state(
    state: LegacyMaintainRuntimeState,
) -> tuple[MaintainQueueState, set[str] | None, int, float]:
    return (
        state.queue,
        state.inflight_names,
        state.attempt,
        state.retry_due_at,
    )


def build_upload_runtime_state(
    *,
    queue: UploadQueueState,
    deep_scan_counter: int,
    pending_source_changes_during_upload: bool,
    last_active_upload_deep_scan_at: float,
) -> UploadRuntimeState:
    return UploadRuntimeState(
        queue=queue,
        deep_scan_counter=deep_scan_counter,
        pending_source_changes_during_upload=pending_source_changes_during_upload,
        last_active_upload_deep_scan_at=last_active_upload_deep_scan_at,
    )


def unpack_upload_runtime_state(
    state: UploadRuntimeState,
) -> tuple[UploadQueueState, int, bool, float]:
    return (
        state.queue,
        state.deep_scan_counter,
        state.pending_source_changes_during_upload,
        state.last_active_upload_deep_scan_at,
    )


def build_composed_maintain_runtime_state(
    *,
    queue: MaintainQueueState,
    inflight_names: set[str] | None,
    attempt: int,
    retry_due_at: float,
    last_incremental_started_at: float,
    last_incremental_defer_reason: str | None,
) -> MaintainRuntimeState:
    return MaintainRuntimeState(
        queue=queue,
        inflight_names=inflight_names,
        attempt=attempt,
        retry_due_at=retry_due_at,
        last_incremental_started_at=last_incremental_started_at,
        last_incremental_defer_reason=last_incremental_defer_reason,
    )


def unpack_composed_maintain_runtime_state(
    state: MaintainRuntimeState,
) -> tuple[MaintainQueueState, set[str] | None, int, float, float, str | None]:
    return (
        state.queue,
        state.inflight_names,
        state.attempt,
        state.retry_due_at,
        state.last_incremental_started_at,
        state.last_incremental_defer_reason,
    )


def build_snapshot_runtime_state(
    *,
    last_uploaded_snapshot_file: Path | None,
    current_snapshot_file: Path | None,
    stable_snapshot_file: Path | None,
    last_json_count: int,
    last_zip_signature: tuple[str, ...],
    zip_extract_processed_signatures: dict[str, str] | None,
) -> SnapshotRuntimeState:
    return SnapshotRuntimeState(
        last_uploaded_snapshot_file=last_uploaded_snapshot_file,
        current_snapshot_file=current_snapshot_file,
        stable_snapshot_file=stable_snapshot_file,
        last_json_count=last_json_count,
        last_zip_signature=last_zip_signature,
        zip_extract_processed_signatures=dict(zip_extract_processed_signatures or {}),
    )


def unpack_snapshot_runtime_state(
    state: SnapshotRuntimeState,
) -> tuple[Path | None, Path | None, Path | None, int, tuple[str, ...], dict[str, str]]:
    return (
        state.last_uploaded_snapshot_file,
        state.current_snapshot_file,
        state.stable_snapshot_file,
        state.last_json_count,
        state.last_zip_signature,
        dict(state.zip_extract_processed_signatures),
    )


def build_ui_runtime_state(
    *,
    upload_progress_state: dict[str, int | str],
    maintain_progress_state: dict[str, int | str],
    last_progress_render_at: float,
    progress_render_interval_seconds: float,
    progress_render_heartbeat_seconds: float,
    last_progress_signature: str,
    panel_height: int,
    panel_title: str,
    panel_enabled: bool,
    panel_color_enabled: bool,
    panel_initialized: bool,
) -> UiRuntimeState:
    return UiRuntimeState(
        upload_progress_state=dict(upload_progress_state),
        maintain_progress_state=dict(maintain_progress_state),
        last_progress_render_at=last_progress_render_at,
        progress_render_interval_seconds=progress_render_interval_seconds,
        progress_render_heartbeat_seconds=progress_render_heartbeat_seconds,
        last_progress_signature=last_progress_signature,
        panel_height=panel_height,
        panel_title=panel_title,
        panel_enabled=panel_enabled,
        panel_color_enabled=panel_color_enabled,
        panel_initialized=panel_initialized,
    )


def unpack_ui_runtime_state(
    state: UiRuntimeState,
) -> tuple[
    dict[str, int | str],
    dict[str, int | str],
    float,
    float,
    float,
    str,
    int,
    str,
    bool,
    bool,
    bool,
]:
    return (
        dict(state.upload_progress_state),
        dict(state.maintain_progress_state),
        state.last_progress_render_at,
        state.progress_render_interval_seconds,
        state.progress_render_heartbeat_seconds,
        state.last_progress_signature,
        state.panel_height,
        state.panel_title,
        state.panel_enabled,
        state.panel_color_enabled,
        state.panel_initialized,
    )


def build_lifecycle_runtime_state(
    *,
    instance_started_at: datetime,
    shutdown_requested: bool,
    shutdown_reason: str | None,
    instance_lock_token: str | None,
    instance_lock_handle: TextIO | None,
    upload_process: subprocess.Popen | None,
    maintain_process: subprocess.Popen | None,
    upload_output_thread: threading.Thread | None,
    maintain_output_thread: threading.Thread | None,
    windows_console_handler: object | None,
    next_maintain_due_at: float | None,
) -> LifecycleRuntimeState:
    return LifecycleRuntimeState(
        instance_started_at=instance_started_at,
        shutdown_requested=shutdown_requested,
        shutdown_reason=shutdown_reason,
        instance_lock_token=instance_lock_token,
        instance_lock_handle=instance_lock_handle,
        upload_process=upload_process,
        maintain_process=maintain_process,
        upload_output_thread=upload_output_thread,
        maintain_output_thread=maintain_output_thread,
        windows_console_handler=windows_console_handler,
        next_maintain_due_at=next_maintain_due_at,
    )


def unpack_lifecycle_runtime_state(
    state: LifecycleRuntimeState,
) -> tuple[
    datetime,
    bool,
    str | None,
    str | None,
    TextIO | None,
    subprocess.Popen | None,
    subprocess.Popen | None,
    threading.Thread | None,
    threading.Thread | None,
    object | None,
    float | None,
]:
    return (
        state.instance_started_at,
        state.shutdown_requested,
        state.shutdown_reason,
        state.instance_lock_token,
        state.instance_lock_handle,
        state.upload_process,
        state.maintain_process,
        state.upload_output_thread,
        state.maintain_output_thread,
        state.windows_console_handler,
        state.next_maintain_due_at,
    )


def build_auto_runtime_state(
    *,
    upload: UploadRuntimeState | None = None,
    maintain: MaintainRuntimeState | None = None,
    snapshot: SnapshotRuntimeState | None = None,
    ui: UiRuntimeState | None = None,
    lifecycle: LifecycleRuntimeState | None = None,
) -> AutoRuntimeState:
    return AutoRuntimeState(
        upload=upload or UploadRuntimeState(),
        maintain=maintain or MaintainRuntimeState(),
        snapshot=snapshot or SnapshotRuntimeState(),
        ui=ui or UiRuntimeState(),
        lifecycle=lifecycle or LifecycleRuntimeState(),
    )


def unpack_auto_runtime_state(
    state: AutoRuntimeState,
) -> tuple[
    UploadRuntimeState,
    MaintainRuntimeState,
    SnapshotRuntimeState,
    UiRuntimeState,
    LifecycleRuntimeState,
]:
    return (
        state.upload,
        state.maintain,
        state.snapshot,
        state.ui,
        state.lifecycle,
    )
