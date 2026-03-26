from __future__ import annotations

import unittest

from cwma.auto.state.maintain_queue import (
    MAINTAIN_SCOPE_FULL,
    MAINTAIN_SCOPE_INCREMENTAL,
    clear_maintain_queue_state,
    decide_maintain_start_scope,
    queue_maintain_request,
)


class AutoMaintainPipelineStateTests(unittest.TestCase):
    def test_queue_incremental_creates_scan_step_job(self) -> None:
        state = clear_maintain_queue_state()
        result = queue_maintain_request(
            state=state,
            reason="post-upload maintain",
            names={"a.json", "b.json"},
        )
        self.assertTrue(result.state.pending)
        self.assertEqual(result.state.names, {"a.json", "b.json"})
        self.assertEqual(len(result.state.pipeline.jobs), 1)
        self.assertEqual(len(result.state.pipeline.maintain_scan_queue), 1)
        self.assertEqual(result.state.pipeline.maintain_delete_401_queue, [])
        self.assertEqual(result.state.pipeline.maintain_quota_queue, [])
        self.assertEqual(result.state.pipeline.maintain_reenable_queue, [])
        self.assertEqual(result.state.pipeline.maintain_finalize_queue, [])
        job_id = result.state.pipeline.maintain_scan_queue[0]
        job = result.state.pipeline.jobs[job_id]
        self.assertEqual(job.scope_type, MAINTAIN_SCOPE_INCREMENTAL)
        self.assertEqual(job.names, {"a.json", "b.json"})

    def test_queue_incremental_merges_into_single_job(self) -> None:
        state = clear_maintain_queue_state()
        state = queue_maintain_request(state=state, reason="inc-1", names={"a.json"}).state
        state = queue_maintain_request(state=state, reason="inc-2", names={"b.json"}).state
        self.assertEqual(len(state.pipeline.jobs), 1)
        job_id = state.pipeline.maintain_scan_queue[0]
        job = state.pipeline.jobs[job_id]
        self.assertEqual(job.scope_type, MAINTAIN_SCOPE_INCREMENTAL)
        self.assertEqual(job.names, {"a.json", "b.json"})
        self.assertEqual(state.reason, "inc-2")

    def test_queue_full_clears_incremental_jobs(self) -> None:
        state = clear_maintain_queue_state()
        state = queue_maintain_request(state=state, reason="inc", names={"a.json", "b.json"}).state
        state = queue_maintain_request(state=state, reason="scheduled full", names=None).state

        self.assertTrue(state.pending)
        self.assertIsNone(state.names)
        self.assertEqual(len(state.pipeline.jobs), 1)
        full_job_id = state.pipeline.maintain_scan_queue[0]
        self.assertEqual(state.pipeline.jobs[full_job_id].scope_type, MAINTAIN_SCOPE_FULL)

    def test_decide_start_scope_slices_incremental_job(self) -> None:
        state = clear_maintain_queue_state()
        state = queue_maintain_request(
            state=state,
            reason="post-upload maintain",
            names={"a.json", "b.json", "c.json"},
        ).state
        decision = decide_maintain_start_scope(state=state, batch_size=2)

        self.assertTrue(decision.should_start)
        self.assertEqual(decision.step, "scan")
        self.assertEqual(len(decision.scope_names or set()), 2)
        self.assertTrue(decision.state.pending)
        self.assertEqual(len(decision.state.names or set()), 1)
        self.assertEqual(len(decision.state.pipeline.jobs), 2)
        self.assertEqual(len(decision.state.pipeline.maintain_scan_queue), 1)
        remaining_job_id = decision.state.pipeline.maintain_scan_queue[0]
        self.assertEqual(
            decision.state.pipeline.jobs[remaining_job_id].scope_type,
            MAINTAIN_SCOPE_INCREMENTAL,
        )
        self.assertEqual(decision.state.pipeline.jobs[remaining_job_id].names, decision.state.names)
        running_job_ids = set(decision.state.pipeline.jobs.keys()) - set(
            decision.state.pipeline.maintain_scan_queue
        )
        self.assertEqual(len(running_job_ids), 1)
        running_job_id = running_job_ids.pop()
        self.assertEqual(
            decision.state.pipeline.jobs[running_job_id].scope_type,
            MAINTAIN_SCOPE_INCREMENTAL,
        )
        self.assertEqual(decision.state.pipeline.jobs[running_job_id].names, decision.scope_names)

    def test_decide_start_scope_consumes_full_job(self) -> None:
        state = clear_maintain_queue_state()
        state = queue_maintain_request(state=state, reason="scheduled full", names=None).state
        decision = decide_maintain_start_scope(state=state, batch_size=1)

        self.assertTrue(decision.should_start)
        self.assertEqual(decision.step, "scan")
        self.assertIsNone(decision.scope_names)
        self.assertFalse(decision.state.pending)
        self.assertEqual(len(decision.state.pipeline.jobs), 1)
        self.assertEqual(decision.state.pipeline.maintain_scan_queue, [])


if __name__ == "__main__":
    unittest.main()
