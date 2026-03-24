from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Sequence

from ..channel.channel_status import STAGE_IDLE, STATE_IDLE, STATE_PENDING, STATE_RUNNING


@dataclass(frozen=True)
class PanelSnapshot:
    upload_stage: str
    upload_done: int
    upload_total: int
    maintain_stage: str
    maintain_done: int
    maintain_total: int
    pending_upload: int
    inflight_upload: int
    upload_reason: str
    upload_state: str
    upload_retry_wait: int
    upload_next_batch_size: int
    upload_queue_batches: int
    pending_full: bool
    pending_incremental: int
    maintain_next_batch: int
    maintain_state: str
    maintain_reason: str
    maintain_inflight_scope: str
    maintain_retry_wait: int
    next_full_wait: int
    maintain_defer_reason: str


def _seconds_until(target: float | None, now: float) -> int:
    if target is None:
        return 0
    return max(0, int(target - now))


def build_panel_snapshot(
    *,
    upload_progress_state: Mapping[str, int | str],
    maintain_progress_state: Mapping[str, int | str],
    pending_upload_snapshot: Sequence[str] | None,
    inflight_upload_snapshot: Sequence[str] | None,
    pending_upload_reason: str | None,
    upload_running: bool,
    upload_retry_due_at: float | None,
    pending_maintain: bool,
    pending_maintain_names: set[str] | None,
    pending_maintain_reason: str | None,
    inflight_maintain_names: set[str] | None,
    maintain_running: bool,
    maintain_retry_due_at: float | None,
    next_maintain_due_at: float | None,
    last_incremental_defer_reason: str | None,
    now_monotonic: float,
    compute_upload_queue_batches: Callable[[int], tuple[int, int]],
    choose_incremental_maintain_batch_size: Callable[[int, bool], int],
) -> PanelSnapshot:
    upload_stage = str(upload_progress_state.get("stage") or STAGE_IDLE)
    upload_done = int(upload_progress_state.get("done") or 0)
    upload_total = int(upload_progress_state.get("total") or 0)
    maintain_stage = str(maintain_progress_state.get("stage") or STAGE_IDLE)
    maintain_done = int(maintain_progress_state.get("done") or 0)
    maintain_total = int(maintain_progress_state.get("total") or 0)

    pending_upload = len(pending_upload_snapshot or [])
    inflight_upload = len(inflight_upload_snapshot or [])
    upload_reason = pending_upload_reason or "-"
    upload_state = STATE_RUNNING if upload_running else (STATE_PENDING if pending_upload > 0 else STATE_IDLE)
    upload_retry_wait = _seconds_until(upload_retry_due_at, now_monotonic)
    upload_next_batch_size, upload_queue_batches = compute_upload_queue_batches(pending_upload)

    pending_full = pending_maintain and pending_maintain_names is None
    pending_incremental = 0 if pending_full else len(pending_maintain_names or set())
    maintain_next_batch = 0
    if pending_incremental > 0:
        upload_pressure = upload_running or bool(pending_upload_snapshot)
        maintain_next_batch = choose_incremental_maintain_batch_size(pending_incremental, upload_pressure)

    maintain_state = STATE_RUNNING if maintain_running else (STATE_PENDING if pending_maintain else STATE_IDLE)
    maintain_reason = pending_maintain_reason or "-"
    maintain_inflight_scope = (
        "full" if inflight_maintain_names is None and maintain_running else str(len(inflight_maintain_names or set()))
    )
    maintain_retry_wait = _seconds_until(maintain_retry_due_at, now_monotonic)
    next_full_wait = _seconds_until(next_maintain_due_at, now_monotonic)
    maintain_defer_reason = last_incremental_defer_reason or "-"

    return PanelSnapshot(
        upload_stage=upload_stage,
        upload_done=upload_done,
        upload_total=upload_total,
        maintain_stage=maintain_stage,
        maintain_done=maintain_done,
        maintain_total=maintain_total,
        pending_upload=pending_upload,
        inflight_upload=inflight_upload,
        upload_reason=upload_reason,
        upload_state=upload_state,
        upload_retry_wait=upload_retry_wait,
        upload_next_batch_size=upload_next_batch_size,
        upload_queue_batches=upload_queue_batches,
        pending_full=pending_full,
        pending_incremental=pending_incremental,
        maintain_next_batch=maintain_next_batch,
        maintain_state=maintain_state,
        maintain_reason=maintain_reason,
        maintain_inflight_scope=maintain_inflight_scope,
        maintain_retry_wait=maintain_retry_wait,
        next_full_wait=next_full_wait,
        maintain_defer_reason=maintain_defer_reason,
    )
