from __future__ import annotations

import subprocess
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TextIO

from .maintain_queue import MaintainQueueState
from .upload_queue import UploadQueueState


def _default_upload_queue_state() -> UploadQueueState:
    return UploadQueueState(
        pending_snapshot=None,
        pending_reason=None,
        pending_retry=False,
        inflight_snapshot=None,
        attempt=0,
        retry_due_at=0.0,
    )


def _default_maintain_queue_state() -> MaintainQueueState:
    return MaintainQueueState(
        pending=False,
        reason=None,
        names=None,
    )


@dataclass
class UploadRuntimeState:
    queue: UploadQueueState = field(default_factory=_default_upload_queue_state)
    deep_scan_counter: int = 0
    pending_source_changes_during_upload: bool = False
    last_active_upload_deep_scan_at: float = 0.0


@dataclass
class MaintainRuntimeState:
    queue: MaintainQueueState = field(default_factory=_default_maintain_queue_state)
    inflight_names: set[str] | None = None
    attempt: int = 0
    retry_due_at: float = 0.0
    last_incremental_started_at: float = 0.0
    last_incremental_defer_reason: str | None = None


@dataclass
class SnapshotRuntimeState:
    last_uploaded_snapshot_file: Path | None = None
    current_snapshot_file: Path | None = None
    stable_snapshot_file: Path | None = None
    last_json_count: int = 0
    last_zip_signature: tuple[str, ...] = tuple()
    zip_extract_processed_signatures: dict[str, str] = field(default_factory=dict)


@dataclass
class UiRuntimeState:
    upload_progress_state: dict[str, int | str] = field(
        default_factory=lambda: {"stage": "idle", "done": 0, "total": 0}
    )
    maintain_progress_state: dict[str, int | str] = field(
        default_factory=lambda: {"stage": "idle", "done": 0, "total": 0}
    )
    last_progress_render_at: float = 0.0
    progress_render_interval_seconds: float = 0.4
    progress_render_heartbeat_seconds: float = 8.0
    last_progress_signature: str = ""
    panel_height: int = 10
    panel_title: str = "CPA Warden Auto Dashboard"
    panel_enabled: bool = False
    panel_color_enabled: bool = False
    panel_initialized: bool = False


@dataclass
class LifecycleRuntimeState:
    instance_started_at: datetime = field(default_factory=datetime.now)
    shutdown_requested: bool = False
    shutdown_reason: str | None = None
    instance_lock_token: str | None = None
    instance_lock_handle: TextIO | None = None
    upload_process: subprocess.Popen | None = None
    maintain_process: subprocess.Popen | None = None
    upload_output_thread: threading.Thread | None = None
    maintain_output_thread: threading.Thread | None = None
    windows_console_handler: object | None = None
    next_maintain_due_at: float | None = None


@dataclass
class AutoRuntimeState:
    upload: UploadRuntimeState = field(default_factory=UploadRuntimeState)
    maintain: MaintainRuntimeState = field(default_factory=MaintainRuntimeState)
    snapshot: SnapshotRuntimeState = field(default_factory=SnapshotRuntimeState)
    ui: UiRuntimeState = field(default_factory=UiRuntimeState)
    lifecycle: LifecycleRuntimeState = field(default_factory=LifecycleRuntimeState)
