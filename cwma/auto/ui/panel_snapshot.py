from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Sequence

from ..channel.channel_status import STAGE_IDLE, STATE_IDLE, STATE_PENDING, STATE_RUNNING
from ..state.maintain_queue import (
    MAINTAIN_SCOPE_FULL,
    MAINTAIN_SCOPE_INCREMENTAL,
    MAINTAIN_STEP_DELETE_401,
    MAINTAIN_STEP_FINALIZE,
    MAINTAIN_STEP_QUOTA,
    MAINTAIN_STEP_REENABLE,
    MAINTAIN_STEP_SCAN,
    MaintainQueueState,
)

_MAINTAIN_STEP_ORDER = (
    MAINTAIN_STEP_SCAN,
    MAINTAIN_STEP_DELETE_401,
    MAINTAIN_STEP_QUOTA,
    MAINTAIN_STEP_REENABLE,
    MAINTAIN_STEP_FINALIZE,
)

_ACTION_STEPS = (
    MAINTAIN_STEP_DELETE_401,
    MAINTAIN_STEP_QUOTA,
    MAINTAIN_STEP_REENABLE,
    MAINTAIN_STEP_FINALIZE,
)


@dataclass(frozen=True)
class MaintainStepSnapshot:
    step: str
    queued: int
    running: int
    retry: int


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
    maintain_queue_full_jobs: int
    maintain_queue_incremental_jobs: int
    maintain_running_full_jobs: int
    maintain_running_incremental_jobs: int
    maintain_retry_jobs: int
    maintain_parallel_state: str
    maintain_steps: tuple[MaintainStepSnapshot, ...]


def _seconds_until(target: float | None, now: float) -> int:
    if target is None:
        return 0
    return max(0, int(target - now))


def _is_retry_reason(reason: str | None) -> bool:
    if not reason:
        return False
    return "retry" in reason.lower()


def _resolve_stage_step(stage: str, *, fallback: str = MAINTAIN_STEP_SCAN) -> str:
    normalized = stage.strip().lower()
    if normalized in _MAINTAIN_STEP_ORDER:
        return normalized
    if normalized in {"probe", "prepare", "scan", "running", "pending"}:
        return MAINTAIN_STEP_SCAN
    if normalized == "delete":
        return MAINTAIN_STEP_DELETE_401
    if normalized == "disable":
        return MAINTAIN_STEP_QUOTA
    if normalized == "enable":
        return MAINTAIN_STEP_REENABLE
    if normalized == "done":
        return MAINTAIN_STEP_FINALIZE
    return fallback


def _empty_step_snapshots() -> dict[str, MaintainStepSnapshot]:
    return {
        step: MaintainStepSnapshot(step=step, queued=0, running=0, retry=0)
        for step in _MAINTAIN_STEP_ORDER
    }


def _with_step_value(
    snapshots: dict[str, MaintainStepSnapshot],
    *,
    step: str,
    queued: int | None = None,
    running: int | None = None,
    retry: int | None = None,
) -> None:
    current = snapshots[step]
    snapshots[step] = MaintainStepSnapshot(
        step=current.step,
        queued=current.queued if queued is None else queued,
        running=current.running if running is None else running,
        retry=current.retry if retry is None else retry,
    )


def _build_pipeline_step_snapshots(
    *,
    maintain_queue_state: MaintainQueueState,
) -> tuple[tuple[MaintainStepSnapshot, ...], int, int, int, int]:
    pipeline = maintain_queue_state.pipeline
    queue_map = {
        MAINTAIN_STEP_SCAN: list(pipeline.maintain_scan_queue),
        MAINTAIN_STEP_DELETE_401: list(pipeline.maintain_delete_401_queue),
        MAINTAIN_STEP_QUOTA: list(pipeline.maintain_quota_queue),
        MAINTAIN_STEP_REENABLE: list(pipeline.maintain_reenable_queue),
        MAINTAIN_STEP_FINALIZE: list(pipeline.maintain_finalize_queue),
    }
    queued_job_ids = {
        job_id
        for queue in queue_map.values()
        for job_id in queue
        if job_id in pipeline.jobs
    }
    running_job_ids = {job_id for job_id in pipeline.jobs if job_id not in queued_job_ids}
    snapshots = _empty_step_snapshots()

    for step in _MAINTAIN_STEP_ORDER:
        queued_ids = [job_id for job_id in queue_map[step] if job_id in pipeline.jobs]
        running_ids = [
            job_id
            for job_id in running_job_ids
            if pipeline.jobs[job_id].stage_cursor == step
        ]
        retry_count = 0
        for job_id in queued_ids + running_ids:
            if _is_retry_reason(pipeline.jobs[job_id].reason):
                retry_count += 1
        _with_step_value(
            snapshots,
            step=step,
            queued=len(queued_ids),
            running=len(running_ids),
            retry=retry_count,
        )

    full_job_count = sum(
        1
        for job in pipeline.jobs.values()
        if job.scope_type == MAINTAIN_SCOPE_FULL
    )
    incremental_job_count = sum(
        1
        for job in pipeline.jobs.values()
        if job.scope_type == MAINTAIN_SCOPE_INCREMENTAL
    )
    running_full_job_count = sum(
        1
        for job_id in running_job_ids
        if pipeline.jobs[job_id].scope_type == MAINTAIN_SCOPE_FULL
    )
    running_incremental_job_count = sum(
        1
        for job_id in running_job_ids
        if pipeline.jobs[job_id].scope_type == MAINTAIN_SCOPE_INCREMENTAL
    )

    return (
        tuple(snapshots[step] for step in _MAINTAIN_STEP_ORDER),
        full_job_count,
        incremental_job_count,
        running_full_job_count,
        running_incremental_job_count,
    )


def _build_fallback_step_snapshots(
    *,
    maintain_stage: str,
    pending_maintain: bool,
    pending_full: bool,
    pending_incremental: int,
    maintain_running: bool,
    inflight_maintain_names: set[str] | None,
) -> tuple[tuple[MaintainStepSnapshot, ...], int, int, int, int]:
    snapshots = _empty_step_snapshots()
    pending_job_count = 1 if pending_maintain else 0
    if pending_job_count > 0:
        _with_step_value(snapshots, step=MAINTAIN_STEP_SCAN, queued=pending_job_count)
    if maintain_running:
        running_step = _resolve_stage_step(maintain_stage)
        current_running = snapshots[running_step].running
        _with_step_value(
            snapshots,
            step=running_step,
            running=current_running + 1,
        )

    queue_full_jobs = 1 if pending_full else 0
    queue_incremental_jobs = 1 if (pending_incremental > 0) else 0
    running_full_jobs = 1 if (maintain_running and inflight_maintain_names is None) else 0
    running_incremental_jobs = 1 if (maintain_running and inflight_maintain_names is not None) else 0
    return (
        tuple(snapshots[step] for step in _MAINTAIN_STEP_ORDER),
        queue_full_jobs,
        queue_incremental_jobs,
        running_full_jobs,
        running_incremental_jobs,
    )


def _derive_parallel_state(
    *,
    upload_running: bool,
    maintain_running: bool,
    maintain_steps: tuple[MaintainStepSnapshot, ...],
) -> str:
    channel_parallel = upload_running and maintain_running
    scan_running = 0
    action_running = 0
    for step_snapshot in maintain_steps:
        if step_snapshot.step == MAINTAIN_STEP_SCAN:
            scan_running += step_snapshot.running
        elif step_snapshot.step in _ACTION_STEPS:
            action_running += step_snapshot.running
    pipeline_parallel = scan_running > 0 and action_running > 0
    if channel_parallel and pipeline_parallel:
        return "channels+pipeline"
    if channel_parallel:
        return "channels"
    if pipeline_parallel:
        return "pipeline"
    if maintain_running:
        return "single"
    return "-"


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
    maintain_queue_state: MaintainQueueState | None = None,
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
    has_pipeline_state = bool(
        maintain_queue_state
        and (
            maintain_queue_state.pipeline.jobs
            or maintain_queue_state.pipeline.maintain_scan_queue
            or maintain_queue_state.pipeline.maintain_delete_401_queue
            or maintain_queue_state.pipeline.maintain_quota_queue
            or maintain_queue_state.pipeline.maintain_reenable_queue
            or maintain_queue_state.pipeline.maintain_finalize_queue
        )
    )
    if has_pipeline_state and maintain_queue_state is not None:
        (
            maintain_steps,
            maintain_queue_full_jobs,
            maintain_queue_incremental_jobs,
            maintain_running_full_jobs,
            maintain_running_incremental_jobs,
        ) = _build_pipeline_step_snapshots(maintain_queue_state=maintain_queue_state)
    else:
        (
            maintain_steps,
            maintain_queue_full_jobs,
            maintain_queue_incremental_jobs,
            maintain_running_full_jobs,
            maintain_running_incremental_jobs,
        ) = _build_fallback_step_snapshots(
            maintain_stage=maintain_stage,
            pending_maintain=pending_maintain,
            pending_full=pending_full,
            pending_incremental=pending_incremental,
            maintain_running=maintain_running,
            inflight_maintain_names=inflight_maintain_names,
        )
    maintain_retry_jobs = sum(step_snapshot.retry for step_snapshot in maintain_steps)
    if maintain_retry_jobs == 0 and maintain_retry_wait > 0:
        maintain_retry_jobs = 1
    maintain_parallel_state = _derive_parallel_state(
        upload_running=upload_running,
        maintain_running=maintain_running,
        maintain_steps=maintain_steps,
    )

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
        maintain_queue_full_jobs=maintain_queue_full_jobs,
        maintain_queue_incremental_jobs=maintain_queue_incremental_jobs,
        maintain_running_full_jobs=maintain_running_full_jobs,
        maintain_running_incremental_jobs=maintain_running_incremental_jobs,
        maintain_retry_jobs=maintain_retry_jobs,
        maintain_parallel_state=maintain_parallel_state,
        maintain_steps=maintain_steps,
    )
