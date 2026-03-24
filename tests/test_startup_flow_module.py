from __future__ import annotations

import unittest

from cwma.auto.orchestration.startup_flow import (
    build_startup_action_plan,
    decide_startup_seed,
    decide_startup_zip_follow_up,
)


class StartupFlowModuleTests(unittest.TestCase):
    def test_decide_startup_seed_when_snapshot_missing(self) -> None:
        decision = decide_startup_seed(last_uploaded_snapshot_exists=False)
        self.assertTrue(decision.should_seed_uploaded_snapshot)

    def test_decide_startup_seed_when_snapshot_exists(self) -> None:
        decision = decide_startup_seed(last_uploaded_snapshot_exists=True)
        self.assertFalse(decision.should_seed_uploaded_snapshot)

    def test_decide_startup_zip_follow_up_enabled_and_changed(self) -> None:
        decision = decide_startup_zip_follow_up(
            inspect_zip_files=True,
            startup_zip_changed=True,
        )
        self.assertTrue(decision.should_run_upload_check)
        self.assertEqual(
            decision.log_message,
            "Startup ZIP scan produced changes. Queue immediate upload check.",
        )

    def test_decide_startup_zip_follow_up_disabled_or_unchanged(self) -> None:
        disabled = decide_startup_zip_follow_up(
            inspect_zip_files=False,
            startup_zip_changed=True,
        )
        unchanged = decide_startup_zip_follow_up(
            inspect_zip_files=True,
            startup_zip_changed=False,
        )
        self.assertFalse(disabled.should_run_upload_check)
        self.assertIsNone(disabled.log_message)
        self.assertFalse(unchanged.should_run_upload_check)
        self.assertIsNone(unchanged.log_message)

    def test_build_startup_action_plan_passthrough(self) -> None:
        plan = build_startup_action_plan(
            run_maintain_on_start=True,
            run_upload_on_start=False,
        )
        self.assertTrue(plan.run_startup_maintain)
        self.assertFalse(plan.run_startup_upload_check)


if __name__ == "__main__":
    unittest.main()

