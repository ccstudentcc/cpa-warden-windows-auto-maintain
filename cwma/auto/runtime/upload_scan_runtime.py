"""Upload-scan runtime adapters (deep scan + active-upload probe)."""

from __future__ import annotations

from typing import Protocol

from ..active_probe import ActiveUploadProbeDecision

UploadSnapshot = list[str]
ZipSignature = tuple[str, ...]


class CurrentUploadScanInputs(Protocol):
    def __call__(self) -> tuple[int, ZipSignature]: ...


class ShouldRunUploadDeepScan(Protocol):
    def __call__(
        self,
        *,
        force_deep_scan: bool,
        current_json_count: int,
        current_zip_signature: ZipSignature,
    ) -> bool: ...


class RefreshUploadScanInputsAfterZipIfNeeded(Protocol):
    def __call__(
        self,
        *,
        current_json_count: int,
        current_zip_signature: ZipSignature,
    ) -> tuple[int, ZipSignature]: ...


class BuildCurrentSnapshot(Protocol):
    def __call__(self) -> UploadSnapshot: ...


class ReadLastUploadedSnapshot(Protocol):
    def __call__(self) -> UploadSnapshot: ...


class HandleUploadNoChangesDetected(Protocol):
    def __call__(
        self,
        *,
        current_snapshot: UploadSnapshot,
        current_json_count: int,
        current_zip_signature: ZipSignature,
        preserve_retry_state: bool,
    ) -> int: ...


class ResolveStableUploadSnapshot(Protocol):
    def __call__(
        self,
        *,
        current_snapshot: UploadSnapshot,
        skip_stability_wait: bool,
    ) -> tuple[int, UploadSnapshot | None]: ...


class ComputePendingUploadSnapshot(Protocol):
    def __call__(
        self,
        current_snapshot: UploadSnapshot,
        uploaded_baseline: UploadSnapshot,
    ) -> UploadSnapshot: ...


class HandleUploadNoPendingDiscovered(Protocol):
    def __call__(
        self,
        *,
        stable_snapshot: UploadSnapshot,
        current_zip_signature: ZipSignature,
        preserve_retry_state: bool,
    ) -> int: ...


class QueuePendingUploadSnapshot(Protocol):
    def __call__(
        self,
        *,
        pending_snapshot: UploadSnapshot,
        queue_reason: str,
        preserve_retry_state: bool,
    ) -> int: ...


class CollectActiveUploadProbeInputs(Protocol):
    def __call__(self) -> tuple[int, ZipSignature | None, float]: ...


class DecideActiveUploadProbe(Protocol):
    def __call__(
        self,
        *,
        current_json_count: int,
        current_zip_signature: ZipSignature | None,
        now_monotonic: float,
    ) -> ActiveUploadProbeDecision: ...


class ApplyActiveUploadProbeState(Protocol):
    def __call__(self, *, decision: ActiveUploadProbeDecision) -> None: ...


class LogActiveUploadSourceChangeIfNeeded(Protocol):
    def __call__(self, *, decision: ActiveUploadProbeDecision) -> None: ...


class RefreshUploadQueueDuringActiveUpload(Protocol):
    def __call__(self) -> int: ...


def run_upload_scan_cycle(
    *,
    force_deep_scan: bool = False,
    preserve_retry_state: bool = False,
    skip_stability_wait: bool = False,
    queue_reason: str = "detected JSON changes",
    current_upload_scan_inputs: CurrentUploadScanInputs,
    should_run_upload_deep_scan: ShouldRunUploadDeepScan,
    refresh_upload_scan_inputs_after_zip_if_needed: RefreshUploadScanInputsAfterZipIfNeeded,
    build_current_snapshot: BuildCurrentSnapshot,
    read_last_uploaded_snapshot: ReadLastUploadedSnapshot,
    handle_upload_no_changes_detected: HandleUploadNoChangesDetected,
    resolve_stable_upload_snapshot: ResolveStableUploadSnapshot,
    compute_pending_upload_snapshot: ComputePendingUploadSnapshot,
    handle_upload_no_pending_discovered: HandleUploadNoPendingDiscovered,
    queue_pending_upload_snapshot: QueuePendingUploadSnapshot,
) -> int:
    """Run one upload deep-scan cycle with callback-injected side effects."""

    current_json_count, current_zip_signature = current_upload_scan_inputs()
    if not should_run_upload_deep_scan(
        force_deep_scan=force_deep_scan,
        current_json_count=current_json_count,
        current_zip_signature=current_zip_signature,
    ):
        return 0

    current_json_count, current_zip_signature = refresh_upload_scan_inputs_after_zip_if_needed(
        current_json_count=current_json_count,
        current_zip_signature=current_zip_signature,
    )
    current_snapshot = build_current_snapshot()
    last_uploaded_snapshot = read_last_uploaded_snapshot()
    if current_snapshot == last_uploaded_snapshot:
        return handle_upload_no_changes_detected(
            current_snapshot=current_snapshot,
            current_json_count=current_json_count,
            current_zip_signature=current_zip_signature,
            preserve_retry_state=preserve_retry_state,
        )

    stable_wait_exit, stable_snapshot = resolve_stable_upload_snapshot(
        current_snapshot=current_snapshot,
        skip_stability_wait=skip_stability_wait,
    )
    if stable_wait_exit != 0:
        return stable_wait_exit
    if stable_snapshot is None:
        return 130

    pending_snapshot = compute_pending_upload_snapshot(stable_snapshot, last_uploaded_snapshot)
    if not pending_snapshot:
        return handle_upload_no_pending_discovered(
            stable_snapshot=stable_snapshot,
            current_zip_signature=current_zip_signature,
            preserve_retry_state=preserve_retry_state,
        )

    return queue_pending_upload_snapshot(
        pending_snapshot=pending_snapshot,
        queue_reason=queue_reason,
        preserve_retry_state=preserve_retry_state,
    )


def run_active_upload_probe_cycle(
    *,
    upload_running: bool,
    collect_active_upload_probe_inputs: CollectActiveUploadProbeInputs,
    decide_active_upload_probe: DecideActiveUploadProbe,
    apply_active_upload_probe_state: ApplyActiveUploadProbeState,
    log_active_upload_source_change_if_needed: LogActiveUploadSourceChangeIfNeeded,
    refresh_upload_queue_during_active_upload: RefreshUploadQueueDuringActiveUpload,
) -> int:
    """Run active-upload source-change probe and optional queue refresh."""

    if not upload_running:
        return 0

    current_json_count, current_zip_signature, now_monotonic = collect_active_upload_probe_inputs()
    decision = decide_active_upload_probe(
        current_json_count=current_json_count,
        current_zip_signature=current_zip_signature,
        now_monotonic=now_monotonic,
    )
    if not decision.source_changed:
        return 0

    apply_active_upload_probe_state(decision=decision)
    log_active_upload_source_change_if_needed(decision=decision)
    if not decision.should_refresh_upload_queue:
        return 0

    return refresh_upload_queue_during_active_upload()


__all__ = [
    "run_upload_scan_cycle",
    "run_active_upload_probe_cycle",
]
