"""Host adapter for upload scan and active-upload probe runtime wiring."""

from __future__ import annotations

import time
from typing import Any, Callable, Protocol

from ..active_probe import ActiveUploadProbeState, decide_active_upload_probe
from ..channel_status import CHANNEL_UPLOAD, STAGE_IDLE, STAGE_PENDING
from ..upload_queue import (
    mark_upload_no_changes,
    mark_upload_no_pending_discovered,
    merge_pending_upload_snapshot,
)
from ..upload_scan_cadence import decide_upload_deep_scan

UploadSnapshot = list[str]
ZipSignature = tuple[str, ...]


class UploadRuntimeHost(Protocol):
    settings: Any
    runtime: Any
    upload_process: Any
    current_snapshot_file: Any
    last_uploaded_snapshot_file: Any
    stable_snapshot_file: Any
    pending_upload_retry: bool
    pending_upload_snapshot: UploadSnapshot | None
    pending_source_changes_during_upload: bool
    pending_upload_reason: str | None
    deep_scan_counter: int
    last_json_count: int
    last_zip_signature: ZipSignature
    last_active_upload_deep_scan_at: float

    def get_json_count(self) -> int: ...
    def get_zip_signature(self) -> ZipSignature: ...
    def inspect_zip_archives(self) -> bool: ...
    def build_snapshot(self, target: Any) -> UploadSnapshot: ...
    def read_snapshot(self, source: Any) -> UploadSnapshot: ...
    def write_snapshot(self, target: Any, lines: list[str]) -> None: ...
    def wait_for_stable_snapshot(self, baseline_snapshot: UploadSnapshot) -> tuple[int, UploadSnapshot | None]: ...
    def _upload_queue_state(self) -> Any: ...
    def _apply_upload_queue_state(self, state: Any) -> None: ...
    def update_channel_progress(
        self,
        name: str,
        *,
        stage: str | None = None,
        done: int | None = None,
        total: int | None = None,
        force_render: bool = False,
    ) -> None: ...
    def check_and_maybe_upload(
        self,
        force_deep_scan: bool = False,
        preserve_retry_state: bool = False,
        skip_stability_wait: bool = False,
        queue_reason: str = "detected JSON changes",
    ) -> int: ...


class UploadRuntimeAdapter:
    def __init__(
        self,
        *,
        host: UploadRuntimeHost,
        compute_pending_upload_snapshot: Callable[[UploadSnapshot, UploadSnapshot], UploadSnapshot],
        get_run_upload_scan_cycle: Callable[[], Callable[..., int]],
        get_run_active_upload_probe_cycle: Callable[[], Callable[..., int]],
        log: Callable[[str], None],
    ) -> None:
        self.host = host
        self.compute_pending_upload_snapshot = compute_pending_upload_snapshot
        self.get_run_upload_scan_cycle = get_run_upload_scan_cycle
        self.get_run_active_upload_probe_cycle = get_run_active_upload_probe_cycle
        self.log = log

    def _current_upload_scan_inputs(self) -> tuple[int, ZipSignature]:
        current_json_count = self.host.get_json_count()
        current_zip_signature = self.host.get_zip_signature() if self.host.settings.inspect_zip_files else tuple()
        return current_json_count, current_zip_signature

    def _should_run_upload_deep_scan(
        self,
        *,
        force_deep_scan: bool,
        current_json_count: int,
        current_zip_signature: ZipSignature,
    ) -> bool:
        cadence = decide_upload_deep_scan(
            force_deep_scan=force_deep_scan,
            pending_upload_retry=self.host.pending_upload_retry,
            current_json_count=current_json_count,
            last_json_count=self.host.last_json_count,
            inspect_zip_files=self.host.settings.inspect_zip_files,
            current_zip_signature=current_zip_signature,
            last_zip_signature=self.host.last_zip_signature,
            deep_scan_counter=self.host.deep_scan_counter,
            deep_scan_interval_loops=self.host.settings.deep_scan_interval_loops,
        )
        self.host.deep_scan_counter = cadence.next_deep_scan_counter
        self.host.runtime.upload.deep_scan_counter = self.host.deep_scan_counter
        return cadence.should_deep_scan

    def _refresh_upload_scan_inputs_after_zip_if_needed(
        self,
        *,
        current_json_count: int,
        current_zip_signature: ZipSignature,
    ) -> tuple[int, ZipSignature]:
        if not self.host.settings.inspect_zip_files:
            return current_json_count, current_zip_signature
        zip_changed = self.host.inspect_zip_archives()
        if not zip_changed:
            return current_json_count, current_zip_signature
        return self.host.get_json_count(), self.host.get_zip_signature()

    def _resolve_stable_upload_snapshot(
        self,
        *,
        current_snapshot: UploadSnapshot,
        skip_stability_wait: bool,
    ) -> tuple[int, UploadSnapshot | None]:
        if skip_stability_wait:
            return 0, current_snapshot
        self.log(f"Detected JSON changes. Waiting {self.host.settings.upload_stable_wait_seconds}s for stability...")
        stable_wait_exit, stable_snapshot = self.host.wait_for_stable_snapshot(current_snapshot)
        if stable_wait_exit != 0:
            return stable_wait_exit, None
        if stable_snapshot is None:
            return 130, None
        return 0, stable_snapshot

    def _record_upload_scan_baseline(
        self,
        *,
        snapshot: UploadSnapshot,
        json_count: int,
        zip_signature: ZipSignature,
    ) -> None:
        self.host.write_snapshot(self.host.stable_snapshot_file, snapshot)
        self.host.last_json_count = json_count
        if self.host.settings.inspect_zip_files:
            self.host.last_zip_signature = zip_signature
        self.host.runtime.snapshot.last_json_count = self.host.last_json_count
        self.host.runtime.snapshot.last_zip_signature = self.host.last_zip_signature

    def _handle_upload_no_changes_detected(
        self,
        *,
        current_snapshot: UploadSnapshot,
        current_json_count: int,
        current_zip_signature: ZipSignature,
        preserve_retry_state: bool,
    ) -> int:
        self._record_upload_scan_baseline(
            snapshot=current_snapshot,
            json_count=current_json_count,
            zip_signature=current_zip_signature,
        )
        state = mark_upload_no_changes(
            state=self.host._upload_queue_state(),
            preserve_retry_state=preserve_retry_state,
        )
        self.host._apply_upload_queue_state(state)
        return 0

    def _handle_upload_no_pending_discovered(
        self,
        *,
        stable_snapshot: UploadSnapshot,
        current_zip_signature: ZipSignature,
        preserve_retry_state: bool,
    ) -> int:
        state = mark_upload_no_pending_discovered(
            state=self.host._upload_queue_state(),
            preserve_retry_state=preserve_retry_state,
        )
        self.host._apply_upload_queue_state(state)
        self._record_upload_scan_baseline(
            snapshot=stable_snapshot,
            json_count=len(stable_snapshot),
            zip_signature=current_zip_signature,
        )
        if self.host.pending_upload_snapshot is None:
            self.host.update_channel_progress(CHANNEL_UPLOAD, stage=STAGE_IDLE, done=0, total=0, force_render=True)
        return 0

    def _queue_pending_upload_snapshot(
        self,
        *,
        pending_snapshot: UploadSnapshot,
        queue_reason: str,
        preserve_retry_state: bool,
    ) -> int:
        merge_result = merge_pending_upload_snapshot(
            state=self.host._upload_queue_state(),
            discovered_pending_snapshot=pending_snapshot,
            queue_reason=queue_reason,
            preserve_retry_state=preserve_retry_state,
        )
        self.host._apply_upload_queue_state(merge_result.state)
        merged_pending = merge_result.merged_pending_snapshot
        self.log(f"Upload batch queued. pending={len(merged_pending)}")
        self.host.update_channel_progress(CHANNEL_UPLOAD, stage=STAGE_PENDING, force_render=True)
        return 0

    def check_and_maybe_upload(
        self,
        *,
        force_deep_scan: bool = False,
        preserve_retry_state: bool = False,
        skip_stability_wait: bool = False,
        queue_reason: str = "detected JSON changes",
    ) -> int:
        run_upload_scan_cycle = self.get_run_upload_scan_cycle()
        return run_upload_scan_cycle(
            force_deep_scan=force_deep_scan,
            preserve_retry_state=preserve_retry_state,
            skip_stability_wait=skip_stability_wait,
            queue_reason=queue_reason,
            current_upload_scan_inputs=self._current_upload_scan_inputs,
            should_run_upload_deep_scan=self._should_run_upload_deep_scan,
            refresh_upload_scan_inputs_after_zip_if_needed=self._refresh_upload_scan_inputs_after_zip_if_needed,
            build_current_snapshot=lambda: self.host.build_snapshot(self.host.current_snapshot_file),
            read_last_uploaded_snapshot=lambda: self.host.read_snapshot(self.host.last_uploaded_snapshot_file),
            handle_upload_no_changes_detected=self._handle_upload_no_changes_detected,
            resolve_stable_upload_snapshot=self._resolve_stable_upload_snapshot,
            compute_pending_upload_snapshot=self.compute_pending_upload_snapshot,
            handle_upload_no_pending_discovered=self._handle_upload_no_pending_discovered,
            queue_pending_upload_snapshot=self._queue_pending_upload_snapshot,
        )

    def _collect_active_upload_probe_inputs(self) -> tuple[int, ZipSignature | None, float]:
        current_json_count = self.host.get_json_count()
        current_zip_signature: ZipSignature | None = None
        if self.host.settings.inspect_zip_files:
            current_zip_signature = self.host.get_zip_signature()
        return current_json_count, current_zip_signature, time.monotonic()

    def _decide_active_upload_probe(
        self,
        *,
        current_json_count: int,
        current_zip_signature: ZipSignature | None,
        now_monotonic: float,
    ) -> Any:
        return decide_active_upload_probe(
            state=ActiveUploadProbeState(
                pending_source_changes=self.host.pending_source_changes_during_upload,
                last_json_count=self.host.last_json_count,
                last_zip_signature=self.host.last_zip_signature,
                last_deep_scan_at=self.host.last_active_upload_deep_scan_at,
            ),
            upload_running=True,
            current_json_count=current_json_count,
            inspect_zip_files=self.host.settings.inspect_zip_files,
            current_zip_signature=current_zip_signature,
            now_monotonic=now_monotonic,
            deep_scan_interval_seconds=self.host.settings.active_upload_deep_scan_interval_seconds,
        )

    def _apply_active_upload_probe_state(self, *, decision: Any) -> None:
        self.host.pending_source_changes_during_upload = decision.state.pending_source_changes
        self.host.last_json_count = decision.state.last_json_count
        self.host.last_zip_signature = decision.state.last_zip_signature
        self.host.last_active_upload_deep_scan_at = decision.state.last_deep_scan_at
        self.host.runtime.upload.pending_source_changes_during_upload = self.host.pending_source_changes_during_upload
        self.host.runtime.upload.last_active_upload_deep_scan_at = self.host.last_active_upload_deep_scan_at
        self.host.runtime.snapshot.last_json_count = self.host.last_json_count
        self.host.runtime.snapshot.last_zip_signature = self.host.last_zip_signature

    def _log_active_upload_source_change_if_needed(self, *, decision: Any) -> None:
        if not decision.should_log_detection:
            return
        self.log(
            "Detected source changes during active upload ("
            + ",".join(decision.changed_reasons)
            + "). Will trigger immediate deep upload check after batch completion."
        )

    def _refresh_upload_queue_during_active_upload(self) -> int:
        self.log("Refreshing upload queue immediately during active upload.")
        return self.host.check_and_maybe_upload(
            force_deep_scan=True,
            preserve_retry_state=True,
            skip_stability_wait=True,
            queue_reason="active-upload source changes",
        )

    def probe_changes_during_active_upload(self) -> int:
        run_active_upload_probe_cycle = self.get_run_active_upload_probe_cycle()
        return run_active_upload_probe_cycle(
            upload_running=self.host.upload_process is not None,
            collect_active_upload_probe_inputs=self._collect_active_upload_probe_inputs,
            decide_active_upload_probe=self._decide_active_upload_probe,
            apply_active_upload_probe_state=self._apply_active_upload_probe_state,
            log_active_upload_source_change_if_needed=self._log_active_upload_source_change_if_needed,
            refresh_upload_queue_during_active_upload=self._refresh_upload_queue_during_active_upload,
        )
