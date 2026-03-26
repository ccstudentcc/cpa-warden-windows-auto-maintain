"""Shutdown and watch-loop sleep runtime adapters."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class ShutdownRuntimeState:
    shutdown_requested: bool = False
    shutdown_reason: str | None = None


def _sleep_step_seconds(*, remaining_seconds: float) -> float:
    return min(0.5, max(0.05, remaining_seconds))


def request_shutdown(
    *,
    state: ShutdownRuntimeState,
    reason: str,
    log: Callable[[str], None],
    terminate_active_processes: Callable[[], None],
) -> ShutdownRuntimeState:
    if state.shutdown_requested:
        return state
    log(f"Shutdown requested: {reason}")
    terminate_active_processes()
    return ShutdownRuntimeState(shutdown_requested=True, shutdown_reason=reason)


def sleep_with_shutdown(
    *,
    shutdown_requested: bool,
    total_seconds: int,
    now_time: Callable[[], float] = time.time,
    sleeper: Callable[[float], None] = time.sleep,
) -> bool:
    """Sleep in small increments and abort early if shutdown is requested."""

    if total_seconds <= 0:
        return not shutdown_requested
    deadline = now_time() + total_seconds
    while now_time() < deadline:
        if shutdown_requested:
            return False
        remaining = deadline - now_time()
        sleeper(_sleep_step_seconds(remaining_seconds=remaining))
    return not shutdown_requested


def current_loop_sleep_seconds(
    *,
    upload_running: bool,
    maintain_running: bool,
    has_pending_upload: bool = False,
    has_pending_maintain: bool = False,
    has_pending_upload_retry: bool = False,
    watch_interval_seconds: int,
    active_probe_interval_seconds: int,
) -> int:
    """Resolve current loop sleep seconds based on channel activity."""

    has_active_or_pending_work = (
        upload_running
        or maintain_running
        or has_pending_upload
        or has_pending_maintain
        or has_pending_upload_retry
    )
    if has_active_or_pending_work:
        return min(watch_interval_seconds, active_probe_interval_seconds)
    return watch_interval_seconds


def sleep_between_watch_cycles(
    *,
    run_once: bool,
    is_run_once_cycle_complete: Callable[[], bool],
    sleep_with_shutdown_fn: Callable[[int], bool],
    current_loop_sleep_seconds_fn: Callable[[], int],
    log: Callable[[str], None],
) -> int | None:
    """Sleep between watch cycles while preserving run-once and shutdown semantics."""

    if run_once:
        if is_run_once_cycle_complete():
            log("Run-once cycle finished.")
            return 0
        if not sleep_with_shutdown_fn(1):
            return 130
        return None

    if not sleep_with_shutdown_fn(current_loop_sleep_seconds_fn()):
        return 130
    return None


__all__ = [
    "ShutdownRuntimeState",
    "request_shutdown",
    "sleep_with_shutdown",
    "current_loop_sleep_seconds",
    "sleep_between_watch_cycles",
]
