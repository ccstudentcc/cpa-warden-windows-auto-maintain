"""Maintain pipeline step executor for Stage 3 serial/parallel policy."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..state.maintain_queue import (
    MAINTAIN_ACTION_STEPS,
    MaintainPipelineClaimResult,
    MaintainQueueState,
    MaintainStepWorkItem,
    advance_maintain_pipeline_after_success,
    claim_maintain_pipeline_work,
    requeue_maintain_pipeline_after_retry,
)

STEP_STATUS_SUCCESS = "success"
STEP_STATUS_RETRY = "retry"
STEP_STATUS_FAILED = "failed"


@dataclass(frozen=True)
class MaintainPipelineStepOutcome:
    status: str
    retry_reason: str | None = None


@dataclass(frozen=True)
class MaintainPipelineCycleResult:
    state: MaintainQueueState
    claim: MaintainPipelineClaimResult
    had_failure: bool


def _reserved_lock_names_for_item(item: MaintainStepWorkItem) -> set[str]:
    if item.step not in MAINTAIN_ACTION_STEPS:
        return set()
    if item.scope_names is None:
        return {"*"}
    return set(item.scope_names)


async def _run_step_safely(
    *,
    item: MaintainStepWorkItem,
    run_step: Callable[[MaintainStepWorkItem], Awaitable[MaintainPipelineStepOutcome]],
) -> MaintainPipelineStepOutcome:
    try:
        return await run_step(item)
    except Exception as exc:
        return MaintainPipelineStepOutcome(
            status=STEP_STATUS_FAILED,
            retry_reason=str(exc) or "maintain step execution failed",
        )


async def run_maintain_pipeline_cycle(
    *,
    state: MaintainQueueState,
    run_step: Callable[[MaintainStepWorkItem], Awaitable[MaintainPipelineStepOutcome]],
    account_locks: dict[str, str] | None = None,
    allow_scan_parallel: bool = True,
    fail_fast: bool = True,
) -> MaintainPipelineCycleResult:
    """Run one maintain pipeline cycle and apply step transitions.

    Policy:
    - A job is always advanced in strict step order.
    - In one cycle, we may execute at most one action-step and one scan-step.
    - Action-step execution honors account-level locks.
    """

    lock_table = {} if account_locks is None else account_locks
    claim = claim_maintain_pipeline_work(
        state=state,
        account_locks=lock_table,
        allow_scan_parallel=allow_scan_parallel,
    )
    if not claim.work_items:
        return MaintainPipelineCycleResult(
            state=claim.state,
            claim=claim,
            had_failure=False,
        )

    reserved_locks_by_job: dict[str, set[str]] = {}
    for item in claim.work_items:
        lock_names = _reserved_lock_names_for_item(item)
        if not lock_names:
            continue
        reserved_locks_by_job[item.job_id] = lock_names
        for name in lock_names:
            lock_table[name] = item.job_id

    queue_state = claim.state
    had_failure = False
    try:
        outcomes = await asyncio.gather(
            *[
                _run_step_safely(item=item, run_step=run_step)
                for item in claim.work_items
            ]
        )
        for item, outcome in zip(claim.work_items, outcomes):
            if outcome.status == STEP_STATUS_SUCCESS:
                queue_state = advance_maintain_pipeline_after_success(
                    state=queue_state,
                    work_item=item,
                )
                continue

            retry_reason = outcome.retry_reason or "maintain step retry"
            queue_state = requeue_maintain_pipeline_after_retry(
                state=queue_state,
                work_item=item,
                retry_reason=retry_reason,
            )
            if outcome.status == STEP_STATUS_FAILED:
                had_failure = True
                if fail_fast:
                    break
    finally:
        for job_id, lock_names in reserved_locks_by_job.items():
            for name in lock_names:
                if lock_table.get(name) == job_id:
                    lock_table.pop(name, None)

    return MaintainPipelineCycleResult(
        state=queue_state,
        claim=claim,
        had_failure=had_failure,
    )


__all__ = [
    "STEP_STATUS_SUCCESS",
    "STEP_STATUS_RETRY",
    "STEP_STATUS_FAILED",
    "MaintainPipelineStepOutcome",
    "MaintainPipelineCycleResult",
    "run_maintain_pipeline_cycle",
]
