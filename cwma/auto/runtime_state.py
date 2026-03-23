from __future__ import annotations

from .maintain_queue import MaintainQueueState, MaintainRuntimeState
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
) -> MaintainRuntimeState:
    return MaintainRuntimeState(
        queue=queue,
        inflight_names=inflight_names,
        attempt=attempt,
        retry_due_at=retry_due_at,
    )


def unpack_maintain_runtime_state(
    state: MaintainRuntimeState,
) -> tuple[MaintainQueueState, set[str] | None, int, float]:
    return (
        state.queue,
        state.inflight_names,
        state.attempt,
        state.retry_due_at,
    )
