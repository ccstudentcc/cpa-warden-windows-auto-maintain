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
MAINTAIN_STEP_ORDER = (
    MAINTAIN_STEP_SCAN,
    MAINTAIN_STEP_DELETE_401,
    MAINTAIN_STEP_QUOTA,
    MAINTAIN_STEP_REENABLE,
    MAINTAIN_STEP_FINALIZE,
)
MAINTAIN_ACTION_STEPS = (
    MAINTAIN_STEP_DELETE_401,
    MAINTAIN_STEP_QUOTA,
    MAINTAIN_STEP_REENABLE,
)


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
    step: str | None
    should_start: bool
    skip_reason: str | None


@dataclass(frozen=True)
class MaintainStepWorkItem:
    job_id: str
    step: str
    scope_type: str
    scope_names: set[str] | None
    reason: str | None


@dataclass(frozen=True)
class MaintainPipelineClaimResult:
    state: MaintainQueueState
    work_items: list[MaintainStepWorkItem]
    blocked_job_ids: list[str]


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


def _queued_job_ids_from_queues(pipeline: MaintainPipelineState) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for queue in _pipeline_step_queues(pipeline):
        for job_id in queue:
            if job_id in pipeline.jobs and job_id not in seen:
                seen.add(job_id)
                ordered.append(job_id)
    return ordered


def _step_queue_for(pipeline: MaintainPipelineState, *, step: str) -> list[str]:
    if step == MAINTAIN_STEP_SCAN:
        return pipeline.maintain_scan_queue
    if step == MAINTAIN_STEP_DELETE_401:
        return pipeline.maintain_delete_401_queue
    if step == MAINTAIN_STEP_QUOTA:
        return pipeline.maintain_quota_queue
    if step == MAINTAIN_STEP_REENABLE:
        return pipeline.maintain_reenable_queue
    if step == MAINTAIN_STEP_FINALIZE:
        return pipeline.maintain_finalize_queue
    raise ValueError(f"Unknown maintain step: {step}")


def _is_action_step(step: str) -> bool:
    return step in MAINTAIN_ACTION_STEPS


def _next_step(step: str) -> str | None:
    try:
        index = MAINTAIN_STEP_ORDER.index(step)
    except ValueError as exc:
        raise ValueError(f"Unknown maintain step: {step}") from exc
    if index + 1 >= len(MAINTAIN_STEP_ORDER):
        return None
    return MAINTAIN_STEP_ORDER[index + 1]


def _build_work_item(job: MaintainJob) -> MaintainStepWorkItem:
    return MaintainStepWorkItem(
        job_id=job.job_id,
        step=job.stage_cursor,
        scope_type=job.scope_type,
        scope_names=None if job.names is None else _clean_names(job.names),
        reason=job.reason,
    )


def _running_pipeline_work_item(state: MaintainQueueState) -> MaintainStepWorkItem | None:
    pipeline = state.pipeline
    queued_ids = {
        job_id
        for queue in _pipeline_step_queues(pipeline)
        for job_id in queue
        if job_id in pipeline.jobs
    }
    running_jobs = [
        job
        for job_id, job in pipeline.jobs.items()
        if job_id not in queued_ids
    ]
    if not running_jobs:
        return None
    running_jobs.sort(key=lambda job: (job.created_at, job.job_id))
    return _build_work_item(running_jobs[0])


def _has_account_lock_conflict(
    *,
    job: MaintainJob,
    account_locks: dict[str, str],
) -> bool:
    if not _is_action_step(job.stage_cursor):
        return False

    current_locks = {name: owner for name, owner in account_locks.items() if owner != job.job_id}
    if not current_locks:
        return False
    if job.names is None:
        return True

    job_names = _clean_names(job.names)
    if not job_names:
        return False
    return any(name in current_locks for name in job_names)


def _claim_step_work_item(
    *,
    pipeline: MaintainPipelineState,
    step: str,
    skip_job_ids: set[str],
    account_locks: dict[str, str],
) -> tuple[MaintainStepWorkItem | None, list[str]]:
    queue = _step_queue_for(pipeline, step=step)
    blocked_job_ids: list[str] = []
    stale_job_ids: list[str] = []

    for job_id in queue:
        if job_id in skip_job_ids:
            continue
        job = pipeline.jobs.get(job_id)
        if job is None:
            stale_job_ids.append(job_id)
            continue
        if _has_account_lock_conflict(job=job, account_locks=account_locks):
            blocked_job_ids.append(job_id)
            continue

        if stale_job_ids:
            queue[:] = [queued_job_id for queued_job_id in queue if queued_job_id not in stale_job_ids]

        queue[:] = [queued_job_id for queued_job_id in queue if queued_job_id != job_id]
        pipeline.jobs[job_id] = MaintainJob(
            job_id=job.job_id,
            scope_type=job.scope_type,
            names=None if job.names is None else set(job.names),
            reason=job.reason,
            created_at=job.created_at,
            stage_cursor=step,
        )
        return _build_work_item(pipeline.jobs[job_id]), blocked_job_ids

    if stale_job_ids:
        queue[:] = [queued_job_id for queued_job_id in queue if queued_job_id not in stale_job_ids]
    return None, blocked_job_ids


def claim_maintain_pipeline_work(
    *,
    state: MaintainQueueState,
    account_locks: dict[str, str] | None = None,
    allow_scan_parallel: bool = True,
) -> MaintainPipelineClaimResult:
    normalized_state = _normalize_queue_state(state)
    pipeline = _clone_pipeline(normalized_state.pipeline)
    locks = {} if account_locks is None else dict(account_locks)

    work_items: list[MaintainStepWorkItem] = []
    blocked_job_ids: list[str] = []
    claimed_job_ids: set[str] = set()

    action_item: MaintainStepWorkItem | None = None
    for step in (MAINTAIN_STEP_DELETE_401, MAINTAIN_STEP_QUOTA, MAINTAIN_STEP_REENABLE, MAINTAIN_STEP_FINALIZE):
        action_item, blocked = _claim_step_work_item(
            pipeline=pipeline,
            step=step,
            skip_job_ids=claimed_job_ids,
            account_locks=locks,
        )
        blocked_job_ids.extend(blocked)
        if action_item is not None:
            work_items.append(action_item)
            claimed_job_ids.add(action_item.job_id)
            break

    if action_item is None or allow_scan_parallel:
        scan_item, blocked = _claim_step_work_item(
            pipeline=pipeline,
            step=MAINTAIN_STEP_SCAN,
            skip_job_ids=claimed_job_ids,
            account_locks=locks,
        )
        blocked_job_ids.extend(blocked)
        if scan_item is not None:
            work_items.append(scan_item)

    return MaintainPipelineClaimResult(
        state=_build_state_from_pipeline(pipeline, fallback_reason=normalized_state.reason),
        work_items=work_items,
        blocked_job_ids=blocked_job_ids,
    )


def advance_maintain_pipeline_after_success(
    *,
    state: MaintainQueueState,
    work_item: MaintainStepWorkItem,
) -> MaintainQueueState:
    normalized_state = _normalize_queue_state(state)
    pipeline = _clone_pipeline(normalized_state.pipeline)
    job = pipeline.jobs.get(work_item.job_id)
    if job is None:
        return _build_state_from_pipeline(pipeline, fallback_reason=normalized_state.reason)

    completed_step = work_item.step
    next_step = _next_step(completed_step)
    if next_step is None:
        _drop_job_from_step_queues(pipeline, work_item.job_id)
        pipeline.jobs.pop(work_item.job_id, None)
        return _build_state_from_pipeline(pipeline, fallback_reason=normalized_state.reason)

    pipeline.jobs[work_item.job_id] = MaintainJob(
        job_id=job.job_id,
        scope_type=job.scope_type,
        names=None if job.names is None else set(job.names),
        reason=job.reason,
        created_at=job.created_at,
        stage_cursor=next_step,
    )
    next_queue = _step_queue_for(pipeline, step=next_step)
    if work_item.job_id not in next_queue:
        next_queue.append(work_item.job_id)
    return _build_state_from_pipeline(pipeline, fallback_reason=normalized_state.reason)


def requeue_maintain_pipeline_after_retry(
    *,
    state: MaintainQueueState,
    work_item: MaintainStepWorkItem,
    retry_reason: str = "maintain step retry",
) -> MaintainQueueState:
    normalized_state = _normalize_queue_state(state)
    pipeline = _clone_pipeline(normalized_state.pipeline)
    job = pipeline.jobs.get(work_item.job_id)
    if job is None:
        return _build_state_from_pipeline(pipeline, fallback_reason=retry_reason)

    retry_step = work_item.step
    pipeline.jobs[work_item.job_id] = MaintainJob(
        job_id=job.job_id,
        scope_type=job.scope_type,
        names=None if job.names is None else set(job.names),
        reason=retry_reason,
        created_at=job.created_at,
        stage_cursor=retry_step,
    )
    retry_queue = _step_queue_for(pipeline, step=retry_step)
    if work_item.job_id not in retry_queue:
        retry_queue.insert(0, work_item.job_id)
    return _build_state_from_pipeline(pipeline, fallback_reason=retry_reason)


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
    ordered_job_ids = _queued_job_ids_from_queues(pipeline)
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
    claim = claim_maintain_pipeline_work(
        state=normalized_state,
        allow_scan_parallel=False,
    )
    if not claim.work_items:
        return MaintainStartDecision(
            state=claim.state,
            scope_names=None,
            step=None,
            should_start=False,
            skip_reason=None,
        )

    selected_item = claim.work_items[0]
    next_state = claim.state
    scope_names = selected_item.scope_names

    if (
        selected_item.step == MAINTAIN_STEP_SCAN
        and selected_item.scope_type == MAINTAIN_SCOPE_INCREMENTAL
        and scope_names is not None
    ):
        pending_names = sorted(_clean_names(scope_names))
        if not pending_names:
            pipeline = _clone_pipeline(claim.state.pipeline)
            pipeline.jobs.pop(selected_item.job_id, None)
            next_state = _build_state_from_pipeline(pipeline)
            return MaintainStartDecision(
                state=next_state,
                scope_names=None,
                step=None,
                should_start=False,
                skip_reason="incremental scope is empty",
            )

        normalized_batch_size = max(1, int(batch_size))
        selected_names = set(pending_names[:normalized_batch_size])
        remaining_names = set(pending_names[normalized_batch_size:])
        if remaining_names:
            pipeline = _clone_pipeline(claim.state.pipeline)
            selected_job = pipeline.jobs.get(selected_item.job_id)
            if selected_job is not None:
                pipeline.jobs[selected_item.job_id] = MaintainJob(
                    job_id=selected_job.job_id,
                    scope_type=selected_job.scope_type,
                    names=selected_names,
                    reason=selected_job.reason,
                    created_at=selected_job.created_at,
                    stage_cursor=selected_job.stage_cursor,
                )
                next_job_id = _allocate_job_id(pipeline)
                pipeline.jobs[next_job_id] = MaintainJob(
                    job_id=next_job_id,
                    scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                    names=remaining_names,
                    reason="queued incremental maintain",
                    created_at=selected_job.created_at,
                    stage_cursor=MAINTAIN_STEP_SCAN,
                )
                pipeline.maintain_scan_queue.append(next_job_id)
                next_state = _build_state_from_pipeline(
                    pipeline,
                    fallback_reason=normalized_state.reason,
                )
        scope_names = selected_names

    return MaintainStartDecision(
        state=next_state,
        scope_names=scope_names,
        step=selected_item.step,
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
    running_item = _running_pipeline_work_item(state.queue)
    next_queue = state.queue
    if running_item is not None:
        next_queue = advance_maintain_pipeline_after_success(
            state=state.queue,
            work_item=running_item,
        )
    return MaintainRuntimeState(
        queue=next_queue,
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
    running_item = _running_pipeline_work_item(state.queue)
    if running_item is not None:
        queue = requeue_maintain_pipeline_after_retry(
            state=state.queue,
            work_item=running_item,
            retry_reason=retry_reason,
        )
    else:
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
