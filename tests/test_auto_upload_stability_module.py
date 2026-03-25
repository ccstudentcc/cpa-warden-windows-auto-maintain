from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from cwma.auto.runtime.lifecycle_runtime_adapter import LifecycleRuntimeAdapter
from tests.temp_sandbox import TempSandboxState, setup_tempfile_sandbox, teardown_tempfile_sandbox

_TEMP_SANDBOX_STATE: TempSandboxState | None = None


def setUpModule() -> None:
    global _TEMP_SANDBOX_STATE
    _TEMP_SANDBOX_STATE = setup_tempfile_sandbox()


def tearDownModule() -> None:
    teardown_tempfile_sandbox(_TEMP_SANDBOX_STATE)


class _LifecycleHost:
    def __init__(self, stable_seconds: int) -> None:
        self.settings = SimpleNamespace(
            upload_stable_wait_seconds=stable_seconds,
            watch_interval_seconds=1,
            active_probe_interval_seconds=1,
        )
        self.runtime = SimpleNamespace(
            lifecycle=SimpleNamespace(
                shutdown_requested=False,
                shutdown_reason=None,
            )
        )
        self.shutdown_requested = False
        self.shutdown_reason = None
        self.upload_process = None
        self.maintain_process = None
        self.stable_snapshot_file = Path("stable_snapshot.txt")
        self.deferred_upload_snapshot_after_stability_wait: list[str] = []
        self._windows_console_handler = None

    def instance_label(self) -> str:
        return "test-host"

    def request_shutdown(self, reason: str) -> None:
        self.shutdown_requested = True
        self.shutdown_reason = reason

    def terminate_active_processes(self) -> None:
        return

    def sleep_with_shutdown(self, total_seconds: int) -> bool:
        return total_seconds >= 0

    def build_snapshot(self, target: Path) -> list[str]:
        _ = target
        return []


class UploadStabilityTests(unittest.TestCase):
    def test_stability_wait_freeze_current_batch(self) -> None:
        host = _LifecycleHost(stable_seconds=5)
        logs: list[str] = []
        adapter = LifecycleRuntimeAdapter(host=host, log=logs.append)
        snapshots = iter([["C:/auth/a.json|1|1", "C:/auth/b.json|1|1"]])

        with mock.patch("cwma.auto.runtime.lifecycle_runtime_adapter.time.time", side_effect=[0.0, 1.0, 6.0]):
            exit_code, stable_snapshot = adapter.wait_for_stable_snapshot(
                ["C:/auth/a.json|1|1"],
                sleep_with_shutdown=lambda _seconds: True,
                build_stable_snapshot=lambda: next(snapshots),
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stable_snapshot, ["C:/auth/a.json|1|1"])
        self.assertEqual(
            host.deferred_upload_snapshot_after_stability_wait,
            ["C:/auth/b.json|1|1"],
        )
        self.assertTrue(any("Frozen current batch" in line for line in logs))

    def test_stability_wait_moves_changed_path_to_deferred_queue(self) -> None:
        host = _LifecycleHost(stable_seconds=5)
        adapter = LifecycleRuntimeAdapter(host=host, log=lambda _line: None)
        snapshots = iter([["C:/auth/a.json|2|2"]])

        with mock.patch("cwma.auto.runtime.lifecycle_runtime_adapter.time.time", side_effect=[0.0, 1.0, 6.0]):
            exit_code, stable_snapshot = adapter.wait_for_stable_snapshot(
                ["C:/auth/a.json|1|1"],
                sleep_with_shutdown=lambda _seconds: True,
                build_stable_snapshot=lambda: next(snapshots),
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stable_snapshot, [])
        self.assertEqual(
            host.deferred_upload_snapshot_after_stability_wait,
            ["C:/auth/a.json|2|2"],
        )

    def test_stability_wait_interrupt_returns_130_and_clears_deferred_rows(self) -> None:
        host = _LifecycleHost(stable_seconds=5)
        host.deferred_upload_snapshot_after_stability_wait = ["stale|1|1"]
        adapter = LifecycleRuntimeAdapter(host=host, log=lambda _line: None)

        with mock.patch("cwma.auto.runtime.lifecycle_runtime_adapter.time.time", side_effect=[0.0, 1.0]):
            exit_code, stable_snapshot = adapter.wait_for_stable_snapshot(
                ["C:/auth/a.json|1|1"],
                sleep_with_shutdown=lambda _seconds: False,
                build_stable_snapshot=lambda: ["C:/auth/a.json|1|1"],
            )

        self.assertEqual(exit_code, 130)
        self.assertIsNone(stable_snapshot)
        self.assertEqual(host.deferred_upload_snapshot_after_stability_wait, [])


if __name__ == "__main__":
    unittest.main()
