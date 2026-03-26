from __future__ import annotations

import unittest

from cwma.auto.runtime.shutdown_runtime import current_loop_sleep_seconds


class AutoShutdownRuntimeModuleTests(unittest.TestCase):
    def test_current_loop_sleep_seconds_uses_watch_interval_when_no_work(self) -> None:
        seconds = current_loop_sleep_seconds(
            upload_running=False,
            maintain_running=False,
            has_pending_upload=False,
            has_pending_maintain=False,
            has_pending_upload_retry=False,
            watch_interval_seconds=15,
            active_probe_interval_seconds=2,
        )
        self.assertEqual(seconds, 15)

    def test_current_loop_sleep_seconds_uses_active_probe_when_channels_running(self) -> None:
        seconds = current_loop_sleep_seconds(
            upload_running=True,
            maintain_running=False,
            has_pending_upload=False,
            has_pending_maintain=False,
            has_pending_upload_retry=False,
            watch_interval_seconds=15,
            active_probe_interval_seconds=2,
        )
        self.assertEqual(seconds, 2)

    def test_current_loop_sleep_seconds_uses_active_probe_when_pending_queue_exists(self) -> None:
        for pending_upload, pending_maintain, pending_retry in (
            (True, False, False),
            (False, True, False),
            (False, False, True),
        ):
            seconds = current_loop_sleep_seconds(
                upload_running=False,
                maintain_running=False,
                has_pending_upload=pending_upload,
                has_pending_maintain=pending_maintain,
                has_pending_upload_retry=pending_retry,
                watch_interval_seconds=15,
                active_probe_interval_seconds=2,
            )
            self.assertEqual(seconds, 2)


if __name__ == "__main__":
    unittest.main()

