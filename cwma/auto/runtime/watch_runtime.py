"""Watch-iteration runtime adapter for scheduler orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from ..watch_cycle import decide_scheduled_maintain_enqueues, decide_watch_upload_check_gate

StageRunner = Callable[[], int]


@dataclass(frozen=True)
class WatchRuntimeState:
    next_maintain_due_at: float | None = None


@dataclass(frozen=True)
class WatchRuntimeResult:
    exit_code: int
    state: WatchRuntimeState


@dataclass(frozen=True)
class WatchRuntimeDeps:
    maintain_interval_seconds: int
    upload_running: Callable[[], bool]
    has_pending_upload_snapshot: Callable[[], bool]
    queue_maintain: Callable[[str], None]
    run_stage: Callable[[str, StageRunner], int]
    run_stage_sequence: Callable[[Iterable[tuple[str, StageRunner]]], int]
    poll_upload_process: StageRunner
    poll_maintain_process: StageRunner
    probe_changes_during_active_upload: StageRunner
    check_and_maybe_upload: StageRunner
    maybe_start_upload: StageRunner
    maybe_start_maintain: StageRunner
    render_progress_snapshot: Callable[..., None]


def _result(*, exit_code: int, next_maintain_due_at: float | None) -> WatchRuntimeResult:
    return WatchRuntimeResult(
        exit_code=exit_code,
        state=WatchRuntimeState(next_maintain_due_at=next_maintain_due_at),
    )


def run_watch_iteration(
    *,
    now_monotonic: float,
    state: WatchRuntimeState,
    deps: WatchRuntimeDeps,
) -> WatchRuntimeResult:
    scheduled_decision = decide_scheduled_maintain_enqueues(
        next_maintain_due_at=state.next_maintain_due_at,
        now_monotonic=now_monotonic,
        maintain_interval_seconds=deps.maintain_interval_seconds,
    )
    for _ in range(scheduled_decision.enqueue_count):
        deps.queue_maintain("scheduled maintain")
    next_maintain_due_at = scheduled_decision.next_maintain_due_at

    handled_exit = deps.run_stage_sequence(
        (
            ("upload command", deps.poll_upload_process),
            ("maintain command", deps.poll_maintain_process),
        )
    )
    if handled_exit != 0:
        return _result(exit_code=handled_exit, next_maintain_due_at=next_maintain_due_at)

    upload_running = deps.upload_running()
    if upload_running:
        handled_exit = deps.run_stage("active-upload source probe", deps.probe_changes_during_active_upload)
        if handled_exit != 0:
            return _result(exit_code=handled_exit, next_maintain_due_at=next_maintain_due_at)

    upload_check_gate = decide_watch_upload_check_gate(
        upload_running=upload_running,
        has_pending_upload_snapshot=deps.has_pending_upload_snapshot(),
    )
    if upload_check_gate.should_run_upload_check:
        handled_exit = deps.run_stage("watch upload-check", deps.check_and_maybe_upload)
        if handled_exit != 0:
            return _result(exit_code=handled_exit, next_maintain_due_at=next_maintain_due_at)

    handled_exit = deps.run_stage_sequence(
        (
            ("watch upload command", deps.maybe_start_upload),
            ("watch maintain command", deps.maybe_start_maintain),
        )
    )
    if handled_exit != 0:
        return _result(exit_code=handled_exit, next_maintain_due_at=next_maintain_due_at)

    deps.render_progress_snapshot()
    return _result(exit_code=0, next_maintain_due_at=next_maintain_due_at)


__all__ = [
    "WatchRuntimeState",
    "WatchRuntimeDeps",
    "WatchRuntimeResult",
    "run_watch_iteration",
]
