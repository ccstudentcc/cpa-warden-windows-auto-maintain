"""Maintain pipeline step executor for Stage 3 serial/parallel policy."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

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


@dataclass(frozen=True)
class AccountLockLease:
    owner_job_id: str
    expires_at_monotonic: float


def _reserved_lock_names_for_item(item: MaintainStepWorkItem) -> set[str]:
    if item.step not in MAINTAIN_ACTION_STEPS:
        return set()
    if item.scope_names is None:
        return {"*"}
    return set(item.scope_names)


def _decode_lock_owner(lock_value: Any, *, now_monotonic: float) -> str | None:
    if isinstance(lock_value, str):
        return lock_value
    if isinstance(lock_value, AccountLockLease):
        if lock_value.expires_at_monotonic <= now_monotonic:
            return None
        return lock_value.owner_job_id
    if isinstance(lock_value, dict):
        raw_owner = lock_value.get("owner_job_id", lock_value.get("owner"))
        if not isinstance(raw_owner, str) or not raw_owner.strip():
            return None
        raw_expire = lock_value.get("expires_at_monotonic", lock_value.get("expires_at"))
        if raw_expire is None:
            return raw_owner
        try:
            expires_at = float(raw_expire)
        except (TypeError, ValueError):
            return None
        if expires_at <= now_monotonic:
            return None
        return raw_owner
    return None


def _active_lock_owners(
    *,
    lock_table: dict[str, Any],
    now_monotonic: float,
) -> dict[str, str]:
    owners: dict[str, str] = {}
    for name in list(lock_table.keys()):
        owner = _decode_lock_owner(lock_table.get(name), now_monotonic=now_monotonic)
        if owner is None:
            lock_table.pop(name, None)
            continue
        owners[name] = owner
    return owners


def _write_lock_entry(
    *,
    lock_table: dict[str, Any],
    lock_name: str,
    owner_job_id: str,
    now_monotonic: float,
    lease_seconds: int,
) -> None:
    if lease_seconds <= 0:
        lock_table[lock_name] = owner_job_id
        return
    lock_table[lock_name] = AccountLockLease(
        owner_job_id=owner_job_id,
        expires_at_monotonic=now_monotonic + float(lease_seconds),
    )


def _is_lock_owned_by(lock_value: Any, *, owner_job_id: str, now_monotonic: float) -> bool:
    owner = _decode_lock_owner(lock_value, now_monotonic=now_monotonic)
    return owner == owner_job_id


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
    account_locks: dict[str, Any] | None = None,
    allow_scan_parallel: bool = True,
    fail_fast: bool = True,
    account_lock_lease_seconds: int = 0,
    now_monotonic: Callable[[], float] = time.monotonic,
) -> MaintainPipelineCycleResult:
    """Run one maintain pipeline cycle and apply step transitions.

    Policy:
    - A job is always advanced in strict step order.
    - In one cycle, we may execute at most one action-step and one scan-step.
    - Action-step execution honors account-level locks.
    """

    current_now = now_monotonic()
    lock_table: dict[str, Any] = {} if account_locks is None else account_locks
    lock_owners_for_claim = _active_lock_owners(lock_table=lock_table, now_monotonic=current_now)
    claim = claim_maintain_pipeline_work(
        state=state,
        account_locks=lock_owners_for_claim,
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
            _write_lock_entry(
                lock_table=lock_table,
                lock_name=name,
                owner_job_id=item.job_id,
                now_monotonic=current_now,
                lease_seconds=account_lock_lease_seconds,
            )

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
                if _is_lock_owned_by(lock_table.get(name), owner_job_id=job_id, now_monotonic=current_now):
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
    "AccountLockLease",
    "run_maintain_pipeline_cycle",
]
