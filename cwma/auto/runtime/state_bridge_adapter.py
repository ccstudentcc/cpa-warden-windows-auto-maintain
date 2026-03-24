"""Host state-bridge adapter for queue/runtime state packing and maintain queue policy."""

from __future__ import annotations

from typing import Any, Callable, Protocol

from ..channel_status import CHANNEL_MAINTAIN, STAGE_DEFERRED
from ..maintain_queue import (
    MaintainQueueState,
    MaintainRuntimeState,
    merge_incremental_maintain_names,
    queue_maintain_request,
)
from ..runtime_state import (
    build_composed_maintain_runtime_state,
    build_maintain_queue_state,
    build_maintain_runtime_state,
    build_upload_queue_state,
    unpack_maintain_queue_state,
    unpack_maintain_runtime_state,
    unpack_upload_queue_state,
)
from ..upload_queue import UploadQueueState


class StateBridgeHost(Protocol):
    runtime: Any
    scheduler_policy: Any
    pending_upload_snapshot: list[str] | None
    pending_upload_reason: str | None
    pending_upload_retry: bool
    inflight_upload_snapshot: list[str] | None
    upload_attempt: int
    upload_retry_due_at: float
    pending_maintain: bool
    pending_maintain_reason: str | None
    pending_maintain_names: set[str] | None
    inflight_maintain_names: set[str] | None
    maintain_attempt: int
    maintain_retry_due_at: float
    last_incremental_maintain_started_at: float
    last_incremental_defer_reason: str | None
    next_maintain_due_at: float | None

    def update_channel_progress(
        self,
        name: str,
        *,
        stage: str | None = None,
        done: int | None = None,
        total: int | None = None,
        force_render: bool = False,
    ) -> None: ...


class StateBridgeAdapter:
    def __init__(
        self,
        *,
        host: StateBridgeHost,
        log: Callable[[str], None],
    ) -> None:
        self.host = host
        self.log = log

    def upload_queue_state(self) -> UploadQueueState:
        return build_upload_queue_state(
            pending_snapshot=self.host.pending_upload_snapshot,
            pending_reason=self.host.pending_upload_reason,
            pending_retry=self.host.pending_upload_retry,
            inflight_snapshot=self.host.inflight_upload_snapshot,
            attempt=self.host.upload_attempt,
            retry_due_at=self.host.upload_retry_due_at,
        )

    def apply_upload_queue_state(self, state: UploadQueueState) -> None:
        (
            self.host.pending_upload_snapshot,
            self.host.pending_upload_reason,
            self.host.pending_upload_retry,
            self.host.inflight_upload_snapshot,
            self.host.upload_attempt,
            self.host.upload_retry_due_at,
        ) = unpack_upload_queue_state(state)
        self.host.runtime.upload.queue = build_upload_queue_state(
            pending_snapshot=self.host.pending_upload_snapshot,
            pending_reason=self.host.pending_upload_reason,
            pending_retry=self.host.pending_upload_retry,
            inflight_snapshot=self.host.inflight_upload_snapshot,
            attempt=self.host.upload_attempt,
            retry_due_at=self.host.upload_retry_due_at,
        )

    def maintain_queue_state(self) -> MaintainQueueState:
        return build_maintain_queue_state(
            pending=self.host.pending_maintain,
            reason=self.host.pending_maintain_reason,
            names=self.host.pending_maintain_names,
        )

    def apply_maintain_queue_state(self, state: MaintainQueueState) -> None:
        (
            self.host.pending_maintain,
            self.host.pending_maintain_reason,
            self.host.pending_maintain_names,
        ) = unpack_maintain_queue_state(state)
        self.host.runtime.maintain.queue = build_maintain_queue_state(
            pending=self.host.pending_maintain,
            reason=self.host.pending_maintain_reason,
            names=self.host.pending_maintain_names,
        )

    def maintain_runtime_state(self) -> MaintainRuntimeState:
        return build_maintain_runtime_state(
            queue=self.maintain_queue_state(),
            inflight_names=self.host.inflight_maintain_names,
            attempt=self.host.maintain_attempt,
            retry_due_at=self.host.maintain_retry_due_at,
        )

    def apply_maintain_runtime_state(self, state: MaintainRuntimeState) -> None:
        queue_state, inflight_names, attempt, retry_due_at = unpack_maintain_runtime_state(state)
        self.apply_maintain_queue_state(queue_state)
        self.host.inflight_maintain_names = inflight_names
        self.host.maintain_attempt = attempt
        self.host.maintain_retry_due_at = retry_due_at
        self.host.runtime.maintain = build_composed_maintain_runtime_state(
            queue=build_maintain_queue_state(
                pending=self.host.pending_maintain,
                reason=self.host.pending_maintain_reason,
                names=self.host.pending_maintain_names,
            ),
            inflight_names=self.host.inflight_maintain_names,
            attempt=self.host.maintain_attempt,
            retry_due_at=self.host.maintain_retry_due_at,
            last_incremental_started_at=self.host.last_incremental_maintain_started_at,
            last_incremental_defer_reason=self.host.last_incremental_defer_reason,
        )

    def queue_maintain(self, reason: str, names: set[str] | None = None) -> None:
        result = queue_maintain_request(
            state=self.maintain_queue_state(),
            reason=reason,
            names=names,
        )
        self.apply_maintain_queue_state(result.state)
        if result.progress_stage:
            self.host.update_channel_progress(
                CHANNEL_MAINTAIN,
                stage=result.progress_stage,
                force_render=True,
            )

    def merge_pending_incremental_maintain_names(self, names: set[str]) -> None:
        state = merge_incremental_maintain_names(
            state=self.maintain_queue_state(),
            names=names,
        )
        self.apply_maintain_queue_state(state)

    def defer_incremental_maintain_if_needed(self, now: float) -> bool:
        if self.host.pending_maintain_names is None:
            return False
        defer_incremental, defer_reason = self.host.scheduler_policy.should_defer_incremental_maintain(
            now_monotonic=now,
            last_incremental_started_at=self.host.last_incremental_maintain_started_at,
            next_full_maintain_due_at=self.host.next_maintain_due_at,
            has_pending_full_maintain=(
                self.host.pending_maintain and self.host.pending_maintain_names is None
            ),
        )
        if not defer_incremental:
            return False
        if defer_reason != self.host.last_incremental_defer_reason:
            self.log(f"Deferred incremental maintain start: {defer_reason}.")
            self.host.last_incremental_defer_reason = defer_reason
            self.host.update_channel_progress(CHANNEL_MAINTAIN, stage=STAGE_DEFERRED, force_render=True)
        return True
