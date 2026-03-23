from __future__ import annotations

import unittest

from cwma.auto.watch_cycle import (
    decide_scheduled_maintain_enqueues,
    decide_watch_upload_check_gate,
)


class WatchCycleModuleTests(unittest.TestCase):
    def test_decide_scheduled_maintain_enqueues_with_none_due(self) -> None:
        decision = decide_scheduled_maintain_enqueues(
            next_maintain_due_at=None,
            now_monotonic=100.0,
            maintain_interval_seconds=60,
        )
        self.assertEqual(decision.enqueue_count, 0)
        self.assertIsNone(decision.next_maintain_due_at)

    def test_decide_scheduled_maintain_enqueues_not_due(self) -> None:
        decision = decide_scheduled_maintain_enqueues(
            next_maintain_due_at=120.0,
            now_monotonic=100.0,
            maintain_interval_seconds=60,
        )
        self.assertEqual(decision.enqueue_count, 0)
        self.assertEqual(decision.next_maintain_due_at, 120.0)

    def test_decide_scheduled_maintain_enqueues_due_once(self) -> None:
        decision = decide_scheduled_maintain_enqueues(
            next_maintain_due_at=100.0,
            now_monotonic=100.0,
            maintain_interval_seconds=60,
        )
        self.assertEqual(decision.enqueue_count, 1)
        self.assertEqual(decision.next_maintain_due_at, 160.0)

    def test_decide_scheduled_maintain_enqueues_due_multiple(self) -> None:
        decision = decide_scheduled_maintain_enqueues(
            next_maintain_due_at=10.0,
            now_monotonic=35.0,
            maintain_interval_seconds=10,
        )
        self.assertEqual(decision.enqueue_count, 3)
        self.assertEqual(decision.next_maintain_due_at, 40.0)

    def test_decide_scheduled_maintain_enqueues_rejects_non_positive_interval(self) -> None:
        with self.assertRaises(ValueError):
            decide_scheduled_maintain_enqueues(
                next_maintain_due_at=10.0,
                now_monotonic=20.0,
                maintain_interval_seconds=0,
            )

    def test_decide_watch_upload_check_gate_allows_when_idle_without_pending(self) -> None:
        decision = decide_watch_upload_check_gate(
            upload_running=False,
            has_pending_upload_snapshot=False,
        )
        self.assertTrue(decision.should_run_upload_check)
        self.assertEqual(decision.reason, "upload idle and no pending snapshot")

    def test_decide_watch_upload_check_gate_blocks_when_upload_running(self) -> None:
        decision = decide_watch_upload_check_gate(
            upload_running=True,
            has_pending_upload_snapshot=False,
        )
        self.assertFalse(decision.should_run_upload_check)
        self.assertEqual(decision.reason, "upload process running")

    def test_decide_watch_upload_check_gate_blocks_when_pending_snapshot_exists(self) -> None:
        decision = decide_watch_upload_check_gate(
            upload_running=False,
            has_pending_upload_snapshot=True,
        )
        self.assertFalse(decision.should_run_upload_check)
        self.assertEqual(decision.reason, "pending upload snapshot exists")


if __name__ == "__main__":
    unittest.main()
