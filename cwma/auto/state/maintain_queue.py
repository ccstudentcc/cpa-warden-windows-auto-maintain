from __future__ import annotations

from dataclasses import dataclass, field

from ..channel.channel_status import STAGE_PENDING_FULL, STAGE_PENDING_INCREMENTAL


MAINTAIN_SCOPE_FULL = "full"
MAINTAIN_SCOPE_INCREMENTAL = "incremental"

MAINTAIN_STEP_SCAN = "scan"
MAINTAIN_STEP_DELETE_401 = "delete_401"
MAINTAIN_STEP_QUOTA = "quota"
MAINTAIN_STEP_REENABLE = "reenable"
MAINTAIN_STEP_FINALIZE = "finalize"


@dataclass
class MaintainJob:
    job_id: str
    scope_type: str
    names: set[str] | None
    reason: str | None
    created_at: float
    stage_cursor: str = MAINTAIN_STEP_SCAN


@dataclass
class MaintainPipelineState:
    next_job_seq: int = 1
    jobs: dict[str, MaintainJob] = field(default_factory=dict)
    maintain_scan_queue: list[str] = field(default_factory=list)
    maintain_delete_401_queue: list[str] = field(default_factory=list)
    maintain_quota_queue: list[str] = field(default_factory=list)
    maintain_reenable_queue: list[str] = field(default_factory=list)
    maintain_finalize_queue: list[str] = field(default_factory=list)


@dataclass
class MaintainQueueState:
    pending: bool
    reason: str | None
    names: set[str] | None
    pipeline: MaintainPipelineState = field(default_factory=MaintainPipelineState)


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
        pipeline=MaintainPipelineState(),
    )


def _clean_names(names: set[str] | None) -> set[str]:
    return {name.strip() for name in (names or set()) if name and name.strip()}


def _clone_pipeline(pipeline: MaintainPipelineState) -> MaintainPipelineState:
    return MaintainPipelineState(
        next_job_seq=pipeline.next_job_seq,
        jobs={
            job_id: MaintainJob(
                job_id=job.job_id,
                scope_type=job.scope_type,
                names=None if job.names is None else set(job.names),
                reason=job.reason,
                created_at=job.created_at,
                stage_cursor=job.stage_cursor,
            )
            for job_id, job in pipeline.jobs.items()
        },
        maintain_scan_queue=list(pipeline.maintain_scan_queue),
        maintain_delete_401_queue=list(pipeline.maintain_delete_401_queue),
        maintain_quota_queue=list(pipeline.maintain_quota_queue),
        maintain_reenable_queue=list(pipeline.maintain_reenable_queue),
        maintain_finalize_queue=list(pipeline.maintain_finalize_queue),
    )


def _pipeline_step_queues(pipeline: MaintainPipelineState) -> list[list[str]]:
    return [
        pipeline.maintain_scan_queue,
        pipeline.maintain_delete_401_queue,
        pipeline.maintain_quota_queue,
        pipeline.maintain_reenable_queue,
        pipeline.maintain_finalize_queue,
    ]


def _queued_job_ids_in_order(pipeline: MaintainPipelineState) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for queue in _pipeline_step_queues(pipeline):
        for job_id in queue:
            if job_id in pipeline.jobs and job_id not in seen:
                seen.add(job_id)
                ordered.append(job_id)
    if not ordered and pipeline.jobs:
        ordered.extend(sorted(pipeline.jobs.keys()))
    return ordered


def _allocate_job_id(pipeline: MaintainPipelineState) -> str:
    job_id = f"maintain-job-{pipeline.next_job_seq:06d}"
    pipeline.next_job_seq += 1
    return job_id


def _drop_job_from_step_queues(pipeline: MaintainPipelineState, job_id: str) -> None:
    for queue in _pipeline_step_queues(pipeline):
        queue[:] = [queued_job_id for queued_job_id in queue if queued_job_id != job_id]


def _first_full_job_id(pipeline: MaintainPipelineState) -> str | None:
    for job_id in _queued_job_ids_in_order(pipeline):
        job = pipeline.jobs.get(job_id)
        if job and job.scope_type == MAINTAIN_SCOPE_FULL:
            return job_id
    return None


def _incremental_job_ids_in_order(pipeline: MaintainPipelineState) -> list[str]:
    incremental_job_ids: list[str] = []
    for job_id in _queued_job_ids_in_order(pipeline):
        job = pipeline.jobs.get(job_id)
        if job and job.scope_type == MAINTAIN_SCOPE_INCREMENTAL:
            incremental_job_ids.append(job_id)
    return incremental_job_ids


def _seed_pipeline_from_legacy_state(
    *,
    pending: bool,
    reason: str | None,
    names: set[str] | None,
) -> MaintainPipelineState:
    pipeline = MaintainPipelineState()
    if not pending:
        return pipeline
    scope_type = MAINTAIN_SCOPE_FULL if names is None else MAINTAIN_SCOPE_INCREMENTAL
    clean_names = None if names is None else _clean_names(names)
    job_id = _allocate_job_id(pipeline)
    pipeline.jobs[job_id] = MaintainJob(
        job_id=job_id,
        scope_type=scope_type,
        names=clean_names,
        reason=reason,
        created_at=0.0,
        stage_cursor=MAINTAIN_STEP_SCAN,
    )
    pipeline.maintain_scan_queue.append(job_id)
    return pipeline


def _build_state_from_pipeline(
    pipeline: MaintainPipelineState,
    *,
    fallback_reason: str | None = None,
) -> MaintainQueueState:
    ordered_job_ids = _queued_job_ids_in_order(pipeline)
    if not ordered_job_ids:
        return MaintainQueueState(
            pending=False,
            reason=None,
            names=None,
            pipeline=pipeline,
        )

    first_full_job_id = _first_full_job_id(pipeline)
    if first_full_job_id is not None:
        full_job = pipeline.jobs[first_full_job_id]
        return MaintainQueueState(
            pending=True,
            reason=full_job.reason or fallback_reason,
            names=None,
            pipeline=pipeline,
        )

    merged_names: set[str] = set()
    first_reason: str | None = None
    for job_id in ordered_job_ids:
        job = pipeline.jobs[job_id]
        if job.scope_type != MAINTAIN_SCOPE_INCREMENTAL:
            continue
        merged_names.update(_clean_names(job.names))
        if first_reason is None:
            first_reason = job.reason
    if not merged_names:
        return MaintainQueueState(
            pending=True,
            reason=first_reason or fallback_reason,
            names=set(),
            pipeline=pipeline,
        )
    return MaintainQueueState(
        pending=True,
        reason=first_reason or fallback_reason,
        names=merged_names,
        pipeline=pipeline,
    )


def _normalize_queue_state(state: MaintainQueueState) -> MaintainQueueState:
    if state.pipeline.jobs:
        return _build_state_from_pipeline(_clone_pipeline(state.pipeline), fallback_reason=state.reason)
    seeded_pipeline = _seed_pipeline_from_legacy_state(
        pending=state.pending,
        reason=state.reason,
        names=state.names,
    )
    return _build_state_from_pipeline(seeded_pipeline, fallback_reason=state.reason)


def _set_primary_job_reason(
    pipeline: MaintainPipelineState,
    *,
    reason: str | None,
) -> MaintainPipelineState:
    ordered_job_ids = _queued_job_ids_in_order(pipeline)
    if not ordered_job_ids:
        return pipeline
    first_job_id = ordered_job_ids[0]
    first_job = pipeline.jobs[first_job_id]
    pipeline.jobs[first_job_id] = MaintainJob(
        job_id=first_job.job_id,
        scope_type=first_job.scope_type,
        names=None if first_job.names is None else set(first_job.names),
        reason=reason,
        created_at=first_job.created_at,
        stage_cursor=first_job.stage_cursor,
    )
    return pipeline


def queue_maintain_request(
    *,
    state: MaintainQueueState,
    reason: str,
    names: set[str] | None,
) -> QueueMaintainResult:
    normalized_state = _normalize_queue_state(state)
    pipeline = _clone_pipeline(normalized_state.pipeline)

    if names is None:
        for incremental_job_id in _incremental_job_ids_in_order(pipeline):
            _drop_job_from_step_queues(pipeline, incremental_job_id)
            pipeline.jobs.pop(incremental_job_id, None)

        existing_full_job_id = _first_full_job_id(pipeline)
        if existing_full_job_id is None:
            full_job_id = _allocate_job_id(pipeline)
            pipeline.jobs[full_job_id] = MaintainJob(
                job_id=full_job_id,
                scope_type=MAINTAIN_SCOPE_FULL,
                names=None,
                reason=reason,
                created_at=0.0,
                stage_cursor=MAINTAIN_STEP_SCAN,
            )
            pipeline.maintain_scan_queue.append(full_job_id)
        else:
            existing_full_job = pipeline.jobs[existing_full_job_id]
            pipeline.jobs[existing_full_job_id] = MaintainJob(
                job_id=existing_full_job.job_id,
                scope_type=existing_full_job.scope_type,
                names=None,
                reason=reason,
                created_at=existing_full_job.created_at,
                stage_cursor=existing_full_job.stage_cursor,
            )
            if existing_full_job_id not in pipeline.maintain_scan_queue:
                pipeline.maintain_scan_queue.insert(0, existing_full_job_id)

        return QueueMaintainResult(
            state=_build_state_from_pipeline(pipeline, fallback_reason=reason),
            progress_stage=STAGE_PENDING_FULL,
        )

    clean_names = _clean_names(names)
    if not clean_names:
        return QueueMaintainResult(state=normalized_state, progress_stage=None)

    if _first_full_job_id(pipeline) is not None:
        return QueueMaintainResult(state=_build_state_from_pipeline(pipeline), progress_stage=None)

    incremental_job_ids = _incremental_job_ids_in_order(pipeline)
    if not incremental_job_ids:
        incremental_job_id = _allocate_job_id(pipeline)
        pipeline.jobs[incremental_job_id] = MaintainJob(
            job_id=incremental_job_id,
            scope_type=MAINTAIN_SCOPE_INCREMENTAL,
            names=set(clean_names),
            reason=reason,
            created_at=0.0,
            stage_cursor=MAINTAIN_STEP_SCAN,
        )
        pipeline.maintain_scan_queue.append(incremental_job_id)
    else:
        primary_incremental_job_id = incremental_job_ids[0]
        merged_names = set(clean_names)
        for incremental_job_id in incremental_job_ids:
            incremental_job = pipeline.jobs[incremental_job_id]
            merged_names.update(_clean_names(incremental_job.names))
        for stale_incremental_job_id in incremental_job_ids[1:]:
            _drop_job_from_step_queues(pipeline, stale_incremental_job_id)
            pipeline.jobs.pop(stale_incremental_job_id, None)

        primary_job = pipeline.jobs[primary_incremental_job_id]
        pipeline.jobs[primary_incremental_job_id] = MaintainJob(
            job_id=primary_job.job_id,
            scope_type=primary_job.scope_type,
            names=merged_names,
            reason=reason,
            created_at=primary_job.created_at,
            stage_cursor=primary_job.stage_cursor,
        )
        if primary_incremental_job_id not in pipeline.maintain_scan_queue:
            pipeline.maintain_scan_queue.append(primary_incremental_job_id)

    return QueueMaintainResult(
        state=_build_state_from_pipeline(pipeline, fallback_reason=reason),
        progress_stage=STAGE_PENDING_INCREMENTAL,
    )


def decide_maintain_start_scope(
    *,
    state: MaintainQueueState,
    batch_size: int,
) -> MaintainStartDecision:
    normalized_state = _normalize_queue_state(state)
    if not normalized_state.pending:
        return MaintainStartDecision(
            state=normalized_state,
            scope_names=None,
            should_start=False,
            skip_reason=None,
        )

    pipeline = _clone_pipeline(normalized_state.pipeline)
    selected_job_id: str | None = None
    for queued_job_id in pipeline.maintain_scan_queue:
        if queued_job_id in pipeline.jobs:
            selected_job_id = queued_job_id
            break

    if selected_job_id is None:
        return MaintainStartDecision(
            state=_build_state_from_pipeline(pipeline),
            scope_names=None,
            should_start=False,
            skip_reason=None,
        )

    selected_job = pipeline.jobs[selected_job_id]
    if selected_job.scope_type == MAINTAIN_SCOPE_FULL:
        _drop_job_from_step_queues(pipeline, selected_job_id)
        pipeline.jobs.pop(selected_job_id, None)
        return MaintainStartDecision(
            state=_build_state_from_pipeline(pipeline),
            scope_names=None,
            should_start=True,
            skip_reason=None,
        )

    pending_names = sorted(_clean_names(selected_job.names))
    selected_names = set(pending_names[:batch_size])
    remaining_names = set(pending_names[batch_size:])
    if not selected_names:
        _drop_job_from_step_queues(pipeline, selected_job_id)
        pipeline.jobs.pop(selected_job_id, None)
        return MaintainStartDecision(
            state=_build_state_from_pipeline(pipeline),
            scope_names=None,
            should_start=False,
            skip_reason="incremental scope is empty",
        )

    if remaining_names:
        pipeline.jobs[selected_job_id] = MaintainJob(
            job_id=selected_job.job_id,
            scope_type=selected_job.scope_type,
            names=remaining_names,
            reason="queued incremental maintain",
            created_at=selected_job.created_at,
            stage_cursor=selected_job.stage_cursor,
        )
    else:
        _drop_job_from_step_queues(pipeline, selected_job_id)
        pipeline.jobs.pop(selected_job_id, None)

    return MaintainStartDecision(
        state=_build_state_from_pipeline(pipeline),
        scope_names=selected_names,
        should_start=True,
        skip_reason=None,
    )


def merge_incremental_maintain_names(
    *,
    state: MaintainQueueState,
    names: set[str],
) -> MaintainQueueState:
    normalized_state = _normalize_queue_state(state)
    clean_names = _clean_names(names)
    if not clean_names:
        return normalized_state

    pipeline = _clone_pipeline(normalized_state.pipeline)
    if _first_full_job_id(pipeline) is not None:
        return _build_state_from_pipeline(pipeline, fallback_reason=normalized_state.reason)

    incremental_job_ids = _incremental_job_ids_in_order(pipeline)
    if not incremental_job_ids:
        incremental_job_id = _allocate_job_id(pipeline)
        pipeline.jobs[incremental_job_id] = MaintainJob(
            job_id=incremental_job_id,
            scope_type=MAINTAIN_SCOPE_INCREMENTAL,
            names=set(clean_names),
            reason=normalized_state.reason,
            created_at=0.0,
            stage_cursor=MAINTAIN_STEP_SCAN,
        )
        pipeline.maintain_scan_queue.append(incremental_job_id)
        return _build_state_from_pipeline(pipeline, fallback_reason=normalized_state.reason)

    primary_incremental_job_id = incremental_job_ids[0]
    merged_names = set(clean_names)
    for incremental_job_id in incremental_job_ids:
        incremental_job = pipeline.jobs[incremental_job_id]
        merged_names.update(_clean_names(incremental_job.names))
    for stale_incremental_job_id in incremental_job_ids[1:]:
        _drop_job_from_step_queues(pipeline, stale_incremental_job_id)
        pipeline.jobs.pop(stale_incremental_job_id, None)

    primary_job = pipeline.jobs[primary_incremental_job_id]
    pipeline.jobs[primary_incremental_job_id] = MaintainJob(
        job_id=primary_job.job_id,
        scope_type=primary_job.scope_type,
        names=merged_names,
        reason=normalized_state.reason,
        created_at=primary_job.created_at,
        stage_cursor=primary_job.stage_cursor,
    )
    if primary_incremental_job_id not in pipeline.maintain_scan_queue:
        pipeline.maintain_scan_queue.append(primary_incremental_job_id)
    return _build_state_from_pipeline(pipeline, fallback_reason=normalized_state.reason)


def mark_maintain_retry(
    *,
    state: MaintainQueueState,
    inflight_names: set[str] | None,
    retry_reason: str = "maintain retry",
) -> MaintainQueueState:
    if inflight_names is None:
        return queue_maintain_request(
            state=state,
            reason=retry_reason,
            names=None,
        ).state

    merged = merge_incremental_maintain_names(
        state=state,
        names=set(inflight_names),
    )
    pipeline = _set_primary_job_reason(_clone_pipeline(merged.pipeline), reason=retry_reason)
    projected = _build_state_from_pipeline(pipeline, fallback_reason=retry_reason)
    return MaintainQueueState(
        pending=True,
        reason=retry_reason,
        names=projected.names,
        pipeline=projected.pipeline,
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
