"""Startup-phase runtime adapter for watcher orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from ..startup_flow import build_startup_action_plan, decide_startup_seed, decide_startup_zip_follow_up

StageRunner = Callable[[], int]


@dataclass(frozen=True)
class StartupRuntimeState:
    last_json_count: int = 0
    last_zip_signature: tuple[str, ...] = tuple()
    next_maintain_due_at: float | None = None


@dataclass(frozen=True)
class StartupRuntimeResult:
    exit_code: int
    state: StartupRuntimeState


@dataclass(frozen=True)
class StartupRuntimeDeps:
    build_snapshot: Callable[[], list[str]]
    uploaded_snapshot_exists: Callable[[], bool]
    write_uploaded_snapshot: Callable[[list[str]], None]
    get_json_count: Callable[[], int]
    inspect_zip_files: bool
    get_zip_signature: Callable[[], tuple[str, ...]]
    inspect_zip_archives: Callable[[], bool]
    run_stage: Callable[[str, StageRunner], int]
    run_stage_sequence: Callable[[Iterable[tuple[str, StageRunner]]], int]
    check_and_maybe_upload: Callable[..., int]
    queue_maintain: Callable[[str], None]
    run_maintain_on_start: bool
    run_upload_on_start: bool
    maybe_start_maintain: StageRunner
    maybe_start_upload: StageRunner
    render_progress_snapshot: Callable[..., None]
    monotonic: Callable[[], float]
    maintain_interval_seconds: int
    log: Callable[[str], None]


def _build_runtime_state(
    *,
    last_json_count: int,
    last_zip_signature: tuple[str, ...],
    next_maintain_due_at: float | None,
) -> StartupRuntimeState:
    return StartupRuntimeState(
        last_json_count=last_json_count,
        last_zip_signature=last_zip_signature,
        next_maintain_due_at=next_maintain_due_at,
    )


def run_startup_cycle(
    *,
    state: StartupRuntimeState,
    deps: StartupRuntimeDeps,
) -> StartupRuntimeResult:
    initial_snapshot = deps.build_snapshot()
    seed_decision = decide_startup_seed(
        last_uploaded_snapshot_exists=deps.uploaded_snapshot_exists(),
    )
    if seed_decision.should_seed_uploaded_snapshot:
        deps.write_uploaded_snapshot(initial_snapshot)

    last_json_count = deps.get_json_count()
    last_zip_signature = state.last_zip_signature
    if deps.inspect_zip_files:
        last_zip_signature = deps.get_zip_signature()

    startup_zip_changed = False
    if deps.inspect_zip_files:
        startup_zip_changed = deps.inspect_zip_archives()
        last_json_count = deps.get_json_count()
        last_zip_signature = deps.get_zip_signature()

    current_state = _build_runtime_state(
        last_json_count=last_json_count,
        last_zip_signature=last_zip_signature,
        next_maintain_due_at=state.next_maintain_due_at,
    )
    zip_follow_up = decide_startup_zip_follow_up(
        inspect_zip_files=deps.inspect_zip_files,
        startup_zip_changed=startup_zip_changed,
    )
    if zip_follow_up.should_run_upload_check:
        if zip_follow_up.log_message:
            deps.log(zip_follow_up.log_message)
        handled_exit = deps.run_stage(
            "startup zip-upload-check",
            lambda: deps.check_and_maybe_upload(force_deep_scan=True),
        )
        if handled_exit != 0:
            return StartupRuntimeResult(exit_code=handled_exit, state=current_state)

    startup_action_plan = build_startup_action_plan(
        run_maintain_on_start=deps.run_maintain_on_start,
        run_upload_on_start=deps.run_upload_on_start,
    )
    if startup_action_plan.run_startup_maintain:
        deps.queue_maintain("startup maintain")

    if startup_action_plan.run_startup_upload_check:
        handled_exit = deps.run_stage("startup upload-check", deps.check_and_maybe_upload)
        if handled_exit != 0:
            return StartupRuntimeResult(exit_code=handled_exit, state=current_state)

    next_maintain_due_at = deps.monotonic() + deps.maintain_interval_seconds
    handled_exit = deps.run_stage_sequence(
        (
            ("startup maintain command", deps.maybe_start_maintain),
            ("startup upload command", deps.maybe_start_upload),
        )
    )
    if handled_exit != 0:
        return StartupRuntimeResult(
            exit_code=handled_exit,
            state=_build_runtime_state(
                last_json_count=last_json_count,
                last_zip_signature=last_zip_signature,
                next_maintain_due_at=next_maintain_due_at,
            ),
        )

    deps.render_progress_snapshot(force=True)
    return StartupRuntimeResult(
        exit_code=0,
        state=_build_runtime_state(
            last_json_count=last_json_count,
            last_zip_signature=last_zip_signature,
            next_maintain_due_at=next_maintain_due_at,
        ),
    )


__all__ = [
    "StartupRuntimeState",
    "StartupRuntimeDeps",
    "StartupRuntimeResult",
    "run_startup_cycle",
]
