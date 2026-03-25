from __future__ import annotations

import unittest
from typing import Any

from cwma.auto.runtime.maintain_pipeline_runtime import (
    STEP_STATUS_SUCCESS,
    AccountLockLease,
    MaintainPipelineStepOutcome,
    run_maintain_pipeline_cycle,
)
from cwma.auto.state.maintain_queue import (
    MAINTAIN_SCOPE_FULL,
    MAINTAIN_SCOPE_INCREMENTAL,
    MAINTAIN_STEP_DELETE_401,
    MAINTAIN_STEP_QUOTA,
    MAINTAIN_STEP_REENABLE,
    MAINTAIN_STEP_FINALIZE,
    MAINTAIN_STEP_SCAN,
    MaintainJob,
    MaintainPipelineState,
    MaintainQueueState,
    advance_maintain_pipeline_after_success,
    claim_maintain_pipeline_work,
    clear_maintain_queue_state,
    queue_maintain_request,
    requeue_maintain_pipeline_after_retry,
)


class AutoMaintainPipelineRuntimeModuleTests(unittest.IsolatedAsyncioTestCase):
    def test_maintain_step_order_and_retry(self) -> None:
        state = clear_maintain_queue_state()
        state = queue_maintain_request(
            state=state,
            reason="post-upload maintain",
            names={"a.json"},
        ).state

        claim = claim_maintain_pipeline_work(state=state)
        self.assertEqual([item.step for item in claim.work_items], [MAINTAIN_STEP_SCAN])
        state = advance_maintain_pipeline_after_success(
            state=claim.state,
            work_item=claim.work_items[0],
        )

        claim = claim_maintain_pipeline_work(state=state)
        self.assertEqual([item.step for item in claim.work_items], [MAINTAIN_STEP_DELETE_401])
        state = requeue_maintain_pipeline_after_retry(
            state=claim.state,
            work_item=claim.work_items[0],
            retry_reason="retry delete",
        )
        claim = claim_maintain_pipeline_work(state=state)
        self.assertEqual([item.step for item in claim.work_items], [MAINTAIN_STEP_DELETE_401])
        state = advance_maintain_pipeline_after_success(
            state=claim.state,
            work_item=claim.work_items[0],
        )

        for expected in (MAINTAIN_STEP_QUOTA, MAINTAIN_STEP_REENABLE, MAINTAIN_STEP_FINALIZE):
            claim = claim_maintain_pipeline_work(state=state)
            self.assertEqual([item.step for item in claim.work_items], [expected])
            state = advance_maintain_pipeline_after_success(
                state=claim.state,
                work_item=claim.work_items[0],
            )

        self.assertFalse(state.pending)
        self.assertEqual(state.pipeline.jobs, {})

    async def test_pipeline_cycle_allows_action_and_scan_parallel_claim(self) -> None:
        state = MaintainQueueState(
            pending=True,
            reason="pipeline",
            names={"a.json", "b.json"},
            pipeline=MaintainPipelineState(
                next_job_seq=3,
                jobs={
                    "job-action": MaintainJob(
                        job_id="job-action",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"a.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_DELETE_401,
                    ),
                    "job-scan": MaintainJob(
                        job_id="job-scan",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"b.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_SCAN,
                    ),
                },
                maintain_scan_queue=["job-scan"],
                maintain_delete_401_queue=["job-action"],
                maintain_quota_queue=[],
                maintain_reenable_queue=[],
                maintain_finalize_queue=[],
            ),
        )

        seen_steps: list[str] = []

        async def _run_step(item: Any) -> MaintainPipelineStepOutcome:
            seen_steps.append(item.step)
            return MaintainPipelineStepOutcome(status=STEP_STATUS_SUCCESS)

        result = await run_maintain_pipeline_cycle(
            state=state,
            run_step=_run_step,
            account_locks={},
            allow_scan_parallel=True,
        )

        self.assertEqual(len(result.claim.work_items), 2)
        self.assertEqual(set(seen_steps), {MAINTAIN_STEP_DELETE_401, MAINTAIN_STEP_SCAN})
        self.assertIn("job-action", result.state.pipeline.maintain_quota_queue)
        self.assertIn("job-scan", result.state.pipeline.maintain_delete_401_queue)

    async def test_account_lock_prevents_conflicting_action(self) -> None:
        state = MaintainQueueState(
            pending=True,
            reason="pipeline",
            names={"a.json", "b.json"},
            pipeline=MaintainPipelineState(
                next_job_seq=3,
                jobs={
                    "job-action": MaintainJob(
                        job_id="job-action",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"a.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_DELETE_401,
                    ),
                    "job-scan": MaintainJob(
                        job_id="job-scan",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"b.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_SCAN,
                    ),
                },
                maintain_scan_queue=["job-scan"],
                maintain_delete_401_queue=["job-action"],
                maintain_quota_queue=[],
                maintain_reenable_queue=[],
                maintain_finalize_queue=[],
            ),
        )

        async def _run_step(_item: Any) -> MaintainPipelineStepOutcome:
            return MaintainPipelineStepOutcome(status=STEP_STATUS_SUCCESS)

        locks = {"a.json": "other-job"}
        result = await run_maintain_pipeline_cycle(
            state=state,
            run_step=_run_step,
            account_locks=locks,
            allow_scan_parallel=True,
        )

        self.assertEqual([item.step for item in result.claim.work_items], [MAINTAIN_STEP_SCAN])
        self.assertIn("job-action", result.claim.blocked_job_ids)
        self.assertIn("job-action", result.state.pipeline.maintain_delete_401_queue)

    async def test_account_lock_lease_blocks_until_expired(self) -> None:
        state = MaintainQueueState(
            pending=True,
            reason="pipeline",
            names={"a.json", "b.json"},
            pipeline=MaintainPipelineState(
                next_job_seq=3,
                jobs={
                    "job-action": MaintainJob(
                        job_id="job-action",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"a.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_DELETE_401,
                    ),
                    "job-scan": MaintainJob(
                        job_id="job-scan",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"b.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_SCAN,
                    ),
                },
                maintain_scan_queue=["job-scan"],
                maintain_delete_401_queue=["job-action"],
                maintain_quota_queue=[],
                maintain_reenable_queue=[],
                maintain_finalize_queue=[],
            ),
        )

        async def _run_step(_item: Any) -> MaintainPipelineStepOutcome:
            return MaintainPipelineStepOutcome(status=STEP_STATUS_SUCCESS)

        locks = {
            "a.json": AccountLockLease(
                owner_job_id="other-job",
                expires_at_monotonic=120.0,
            )
        }
        result = await run_maintain_pipeline_cycle(
            state=state,
            run_step=_run_step,
            account_locks=locks,
            allow_scan_parallel=True,
            now_monotonic=lambda: 100.0,
        )

        self.assertEqual([item.step for item in result.claim.work_items], [MAINTAIN_STEP_SCAN])
        self.assertIn("job-action", result.claim.blocked_job_ids)
        self.assertIn("a.json", locks)

    async def test_expired_account_lock_lease_is_cleaned_before_claim(self) -> None:
        state = MaintainQueueState(
            pending=True,
            reason="pipeline",
            names={"a.json", "b.json"},
            pipeline=MaintainPipelineState(
                next_job_seq=3,
                jobs={
                    "job-action": MaintainJob(
                        job_id="job-action",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"a.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_DELETE_401,
                    ),
                    "job-scan": MaintainJob(
                        job_id="job-scan",
                        scope_type=MAINTAIN_SCOPE_INCREMENTAL,
                        names={"b.json"},
                        reason="queued incremental maintain",
                        created_at=0.0,
                        stage_cursor=MAINTAIN_STEP_SCAN,
                    ),
                },
                maintain_scan_queue=["job-scan"],
                maintain_delete_401_queue=["job-action"],
                maintain_quota_queue=[],
                maintain_reenable_queue=[],
                maintain_finalize_queue=[],
            ),
        )

        async def _run_step(_item: Any) -> MaintainPipelineStepOutcome:
            return MaintainPipelineStepOutcome(status=STEP_STATUS_SUCCESS)

        locks = {
            "a.json": AccountLockLease(
                owner_job_id="other-job",
                expires_at_monotonic=80.0,
            )
        }
        result = await run_maintain_pipeline_cycle(
            state=state,
            run_step=_run_step,
            account_locks=locks,
            allow_scan_parallel=True,
            now_monotonic=lambda: 100.0,
        )

        claimed_steps = [item.step for item in result.claim.work_items]
        self.assertIn(MAINTAIN_STEP_DELETE_401, claimed_steps)
        self.assertNotIn("job-action", result.claim.blocked_job_ids)
        self.assertNotIn("a.json", locks)

    async def test_full_and_incremental_jobs_share_same_executor(self) -> None:
        seen_scope_types: list[str] = []

        async def _run_step(item: Any) -> MaintainPipelineStepOutcome:
            seen_scope_types.append(item.scope_type)
            return MaintainPipelineStepOutcome(status=STEP_STATUS_SUCCESS)

        full_state = queue_maintain_request(
            state=clear_maintain_queue_state(),
            reason="scheduled full",
            names=None,
        ).state
        incremental_state = queue_maintain_request(
            state=clear_maintain_queue_state(),
            reason="post-upload maintain",
            names={"x.json"},
        ).state

        full_result = await run_maintain_pipeline_cycle(
            state=full_state,
            run_step=_run_step,
            account_locks={},
        )
        incremental_result = await run_maintain_pipeline_cycle(
            state=incremental_state,
            run_step=_run_step,
            account_locks={},
        )

        self.assertEqual(len(full_result.claim.work_items), 1)
        self.assertEqual(len(incremental_result.claim.work_items), 1)
        self.assertEqual(seen_scope_types, [MAINTAIN_SCOPE_FULL, MAINTAIN_SCOPE_INCREMENTAL])


if __name__ == "__main__":
    unittest.main()
