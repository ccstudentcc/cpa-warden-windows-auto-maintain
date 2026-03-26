"""Host lifecycle adapter for console/shutdown/sleep/stability wait orchestration."""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path
from typing import Any, Callable, Protocol

from ..channel.channel_status import CHANNEL_MAINTAIN, CHANNEL_UPLOAD
from ..infra.process_supervisor import terminate_channel
from ..state.upload_queue import coalesce_upload_snapshot_rows, parse_upload_snapshot_row
from .shutdown_runtime import (
    ShutdownRuntimeState,
    current_loop_sleep_seconds as current_loop_sleep_seconds_runtime,
    request_shutdown as request_shutdown_runtime,
    sleep_with_shutdown as sleep_with_shutdown_runtime,
)


class LifecycleRuntimeHost(Protocol):
    settings: Any
    runtime: Any
    shutdown_requested: bool
    shutdown_reason: str | None
    upload_process: Any
    maintain_process: Any
    pending_upload_snapshot: list[str] | None
    pending_upload_retry: bool
    pending_maintain: bool
    stable_snapshot_file: Path
    deferred_upload_snapshot_after_stability_wait: list[str]
    _windows_console_handler: Any

    def instance_label(self) -> str: ...
    def request_shutdown(self, reason: str) -> None: ...
    def terminate_active_processes(self) -> None: ...
    def sleep_with_shutdown(self, total_seconds: int) -> bool: ...
    def build_snapshot(self, target: Path) -> list[str]: ...


class LifecycleRuntimeAdapter:
    def __init__(self, *, host: LifecycleRuntimeHost, log: Callable[[str], None]) -> None:
        self.host = host
        self.log = log

    def set_console_title(self) -> None:
        if os.name != "nt":
            return
        try:
            import ctypes

            ctypes.windll.kernel32.SetConsoleTitleW(self.host.instance_label())
        except Exception as exc:
            self.log(f"[WARN] Failed to set console title: {exc}")

    def register_shutdown_handlers(self, signal_handler: Callable[[int, object], None]) -> None:
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, signal_handler)

        if os.name != "nt":
            return
        try:
            import ctypes

            CTRL_C_EVENT = 0
            CTRL_BREAK_EVENT = 1
            CTRL_CLOSE_EVENT = 2
            CTRL_LOGOFF_EVENT = 5
            CTRL_SHUTDOWN_EVENT = 6

            event_names = {
                CTRL_C_EVENT: "CTRL_C_EVENT",
                CTRL_BREAK_EVENT: "CTRL_BREAK_EVENT",
                CTRL_CLOSE_EVENT: "CTRL_CLOSE_EVENT",
                CTRL_LOGOFF_EVENT: "CTRL_LOGOFF_EVENT",
                CTRL_SHUTDOWN_EVENT: "CTRL_SHUTDOWN_EVENT",
            }

            handler_type = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)

            def _handler(event_type: int) -> bool:
                reason = event_names.get(event_type, f"WIN_EVENT_{event_type}")
                self.host.request_shutdown(reason)
                return True

            self.host._windows_console_handler = handler_type(_handler)
            ctypes.windll.kernel32.SetConsoleCtrlHandler(self.host._windows_console_handler, True)
        except Exception as exc:
            self.log(f"[WARN] Failed to register Windows console handler: {exc}")

    def request_shutdown(self, reason: str) -> None:
        state = request_shutdown_runtime(
            state=ShutdownRuntimeState(
                shutdown_requested=self.host.shutdown_requested,
                shutdown_reason=self.host.shutdown_reason,
            ),
            reason=reason,
            log=self.log,
            terminate_active_processes=self.host.terminate_active_processes,
        )
        self.host.shutdown_requested = state.shutdown_requested
        self.host.shutdown_reason = state.shutdown_reason
        self.host.runtime.lifecycle.shutdown_requested = self.host.shutdown_requested
        self.host.runtime.lifecycle.shutdown_reason = self.host.shutdown_reason

    def terminate_process(self, proc: Any, *, name: str) -> None:
        terminate_channel(
            process=proc,
            channel=name,
            log=self.log,
            terminate_timeout_seconds=8,
            kill_wait_seconds=5,
        )

    def terminate_active_processes(self) -> None:
        self.terminate_process(self.host.upload_process, name=CHANNEL_UPLOAD)
        self.terminate_process(self.host.maintain_process, name=CHANNEL_MAINTAIN)

    def sleep_with_shutdown(self, total_seconds: int) -> bool:
        return sleep_with_shutdown_runtime(
            shutdown_requested=self.host.shutdown_requested,
            total_seconds=total_seconds,
        )

    def current_loop_sleep_seconds(self) -> int:
        return current_loop_sleep_seconds_runtime(
            upload_running=self.host.upload_process is not None,
            maintain_running=self.host.maintain_process is not None,
            has_pending_upload=self.host.pending_upload_snapshot is not None,
            has_pending_maintain=bool(self.host.pending_maintain),
            has_pending_upload_retry=bool(self.host.pending_upload_retry),
            watch_interval_seconds=self.host.settings.watch_interval_seconds,
            active_probe_interval_seconds=self.host.settings.active_probe_interval_seconds,
        )

    def _set_deferred_upload_snapshot_after_stability_wait(self, rows: list[str]) -> None:
        setattr(self.host, "deferred_upload_snapshot_after_stability_wait", list(rows))

    def wait_for_stable_snapshot(
        self,
        baseline_snapshot: list[str],
        *,
        sleep_with_shutdown: Callable[[int], bool],
        build_stable_snapshot: Callable[[], list[str]],
    ) -> tuple[int, list[str] | None]:
        stable_seconds = self.host.settings.upload_stable_wait_seconds
        self._set_deferred_upload_snapshot_after_stability_wait([])
        if stable_seconds <= 0:
            return 0, baseline_snapshot

        poll_seconds = min(2.0, max(0.5, stable_seconds / 4.0))
        stable_started_at = time.time()
        frozen_snapshot = coalesce_upload_snapshot_rows(list(baseline_snapshot))
        frozen_by_path: dict[str, str] = {}
        for row in frozen_snapshot:
            parsed = parse_upload_snapshot_row(row)
            if parsed is None:
                continue
            frozen_by_path[parsed[0]] = row
        frozen_paths = set(frozen_by_path.keys())
        deferred_by_path: dict[str, str] = {}
        deferred_log_emitted = False

        while True:
            elapsed = time.time() - stable_started_at
            remaining = stable_seconds - elapsed
            if remaining <= 0:
                stable_rows: list[str] = []
                for row in frozen_snapshot:
                    parsed = parse_upload_snapshot_row(row)
                    if parsed is None:
                        stable_rows.append(row)
                        continue
                    if parsed[0] in frozen_by_path:
                        stable_rows.append(frozen_by_path[parsed[0]])
                self._set_deferred_upload_snapshot_after_stability_wait(list(deferred_by_path.values()))
                return 0, stable_rows

            if not sleep_with_shutdown(min(poll_seconds, remaining)):
                self._set_deferred_upload_snapshot_after_stability_wait([])
                return 130, None

            latest_snapshot = coalesce_upload_snapshot_rows(build_stable_snapshot())
            latest_by_path: dict[str, str] = {}
            for row in latest_snapshot:
                parsed = parse_upload_snapshot_row(row)
                if parsed is None:
                    continue
                latest_by_path[parsed[0]] = row

            for path in list(frozen_by_path.keys()):
                latest_row = latest_by_path.get(path)
                if latest_row == frozen_by_path[path]:
                    continue
                frozen_by_path.pop(path, None)
                if latest_row is not None:
                    deferred_by_path[path] = latest_row

            for path, latest_row in latest_by_path.items():
                if path in frozen_paths:
                    continue
                deferred_by_path[path] = latest_row

            if deferred_by_path and not deferred_log_emitted:
                deferred_log_emitted = True
                self.log(
                    "Detected additional JSON changes during stability wait. "
                    "Frozen current batch; deferred new versions to next queue intake."
                )
