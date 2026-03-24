from __future__ import annotations

from dataclasses import dataclass

from ..channel.channel_status import STAGE_PENDING_FULL, STAGE_PENDING_INCREMENTAL


@dataclass
class MaintainQueueState:
    pending: bool
    reason: str | None
    names: set[str] | None


@dataclass
class QueueMaintainResult:
    state: MaintainQueueState
    progress_stage: str | None


@dataclass
class MaintainStartDecision:
    state: MaintainQueueState
    scope_names: set[str] | None
    should_start: bool
    skip_reason: str | None


@dataclass
class MaintainRuntimeState:
    queue: MaintainQueueState
    inflight_names: set[str] | None
    attempt: int
    retry_due_at: float


def clear_maintain_queue_state() -> MaintainQueueState:
    return MaintainQueueState(
        pending=False,
        reason=None,
        names=None,
    )


def queue_maintain_request(
    *,
    state: MaintainQueueState,
    reason: str,
    names: set[str] | None,
) -> QueueMaintainResult:
    if names is None:
        return QueueMaintainResult(
            state=MaintainQueueState(pending=True, reason=reason, names=None),
            progress_stage=STAGE_PENDING_FULL,
        )

    clean_names = {name.strip() for name in names if name and name.strip()}
    if not clean_names:
        return QueueMaintainResult(state=state, progress_stage=None)

    if state.pending and state.names is None:
        return QueueMaintainResult(state=state, progress_stage=None)

    if state.names is None:
        merged_names = set(clean_names)
    else:
        merged_names = set(state.names)
        merged_names.update(clean_names)

    return QueueMaintainResult(
        state=MaintainQueueState(pending=True, reason=reason, names=merged_names),
        progress_stage=STAGE_PENDING_INCREMENTAL,
    )


def decide_maintain_start_scope(
    *,
    state: MaintainQueueState,
    batch_size: int,
) -> MaintainStartDecision:
    if not state.pending:
        return MaintainStartDecision(
            state=state,
            scope_names=None,
            should_start=False,
            skip_reason=None,
        )

    if state.names is None:
        return MaintainStartDecision(
            state=clear_maintain_queue_state(),
            scope_names=None,
            should_start=True,
            skip_reason=None,
        )

    pending_names = sorted(state.names)
    selected_names = set(pending_names[:batch_size])
    remaining_names = set(pending_names[batch_size:])
    if not selected_names:
        return MaintainStartDecision(
            state=clear_maintain_queue_state(),
            scope_names=None,
            should_start=False,
            skip_reason="incremental scope is empty",
        )

    queue_after = MaintainQueueState(
        pending=bool(remaining_names),
        reason="queued incremental maintain" if remaining_names else None,
        names=remaining_names if remaining_names else None,
    )
    return MaintainStartDecision(
        state=queue_after,
        scope_names=selected_names,
        should_start=True,
        skip_reason=None,
    )


def merge_incremental_maintain_names(
    *,
    state: MaintainQueueState,
    names: set[str],
) -> MaintainQueueState:
    if not names:
        return state
    if state.pending and state.names is None:
        return state
    if state.names is None:
        merged_names = set(names)
    else:
        merged_names = set(state.names)
        merged_names.update(names)
    return MaintainQueueState(
        pending=True,
        reason=state.reason,
        names=merged_names,
    )


def mark_maintain_retry(
    *,
    state: MaintainQueueState,
    inflight_names: set[str] | None,
    retry_reason: str = "maintain retry",
) -> MaintainQueueState:
    if inflight_names is None:
        return MaintainQueueState(
            pending=True,
            reason=retry_reason,
            names=None,
        )

    merged = merge_incremental_maintain_names(
        state=state,
        names=set(inflight_names),
    )
    return MaintainQueueState(
        pending=True,
        reason=retry_reason,
        names=merged.names,
    )


def mark_maintain_success(state: MaintainRuntimeState) -> MaintainRuntimeState:
    return MaintainRuntimeState(
        queue=state.queue,
        inflight_names=None,
        attempt=0,
        retry_due_at=0.0,
    )


def mark_maintain_runtime_retry(
    *,
    state: MaintainRuntimeState,
    now_monotonic: float,
    retry_delay_seconds: int,
    retry_reason: str = "maintain retry",
) -> MaintainRuntimeState:
    queue = mark_maintain_retry(
        state=state.queue,
        inflight_names=state.inflight_names,
        retry_reason=retry_reason,
    )
    return MaintainRuntimeState(
        queue=queue,
        inflight_names=None,
        attempt=state.attempt,
        retry_due_at=now_monotonic + retry_delay_seconds,
    )


def mark_maintain_terminal_failure(
    state: MaintainRuntimeState,
    *,
    clear_retry_due_at: bool = False,
) -> MaintainRuntimeState:
    return MaintainRuntimeState(
        queue=clear_maintain_queue_state(),
        inflight_names=None,
        attempt=0,
        retry_due_at=0.0 if clear_retry_due_at else state.retry_due_at,
    )
