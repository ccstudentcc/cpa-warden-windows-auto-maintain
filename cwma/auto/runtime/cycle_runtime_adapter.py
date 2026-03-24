"""Host adapter for startup/watch runtime state+deps assembly and cycle execution."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Protocol

from .startup_runtime import StartupRuntimeDeps, StartupRuntimeState
from .watch_runtime import WatchRuntimeDeps, WatchRuntimeState


class CycleRuntimeHost(Protocol):
    settings: Any
    runtime: Any
    last_uploaded_snapshot_file: Any
    current_snapshot_file: Any
    last_json_count: int
    last_zip_signature: tuple[str, ...]
    next_maintain_due_at: float | None
    upload_process: Any
    pending_upload_snapshot: list[str] | None

    def build_snapshot(self, target: Any) -> list[str]: ...
    def write_snapshot(self, target: Any, lines: list[str]) -> None: ...
    def get_json_count(self) -> int: ...
    def get_zip_signature(self) -> tuple[str, ...]: ...
    def inspect_zip_archives(self) -> bool: ...
    def _run_stage(self, stage: str, runner: Callable[[], int]) -> int: ...
    def _run_stage_sequence(self, stages: Iterable[tuple[str, Callable[[], int]]]) -> int: ...
    def check_and_maybe_upload(
        self,
        force_deep_scan: bool = False,
        *,
        preserve_retry_state: bool = False,
        skip_stability_wait: bool = False,
        queue_reason: str = "detected JSON changes",
    ) -> int: ...
    def queue_maintain(self, reason: str, names: set[str] | None = None) -> None: ...
    def maybe_start_maintain(self) -> int: ...
    def maybe_start_upload(self) -> int: ...
    def render_progress_snapshot(self, *, force: bool = False) -> None: ...
    def poll_upload_process(self) -> int: ...
    def poll_maintain_process(self) -> int: ...
    def probe_changes_during_active_upload(self) -> int: ...
    def handle_failure(self, stage: str, code: int) -> bool: ...


class CycleRuntimeAdapter:
    def __init__(
        self,
        *,
        host: CycleRuntimeHost,
        get_run_startup_cycle: Callable[[], Callable[..., Any]],
        get_run_watch_iteration: Callable[[], Callable[..., Any]],
        monotonic: Callable[[], float],
        log: Callable[[str], None],
    ) -> None:
        self.host = host
        self.get_run_startup_cycle = get_run_startup_cycle
        self.get_run_watch_iteration = get_run_watch_iteration
        self.monotonic = monotonic
        self.log = log

    def apply_startup_runtime_state(self, state: StartupRuntimeState) -> None:
        self.host.last_json_count = state.last_json_count
        self.host.last_zip_signature = state.last_zip_signature
        self.host.next_maintain_due_at = state.next_maintain_due_at
        self.host.runtime.snapshot.last_json_count = self.host.last_json_count
        self.host.runtime.snapshot.last_zip_signature = self.host.last_zip_signature
        self.host.runtime.lifecycle.next_maintain_due_at = self.host.next_maintain_due_at

    def apply_watch_runtime_state(self, state: WatchRuntimeState) -> None:
        self.host.next_maintain_due_at = state.next_maintain_due_at
        self.host.runtime.lifecycle.next_maintain_due_at = self.host.next_maintain_due_at

    def build_startup_runtime_state(self) -> StartupRuntimeState:
        return StartupRuntimeState(
            last_json_count=self.host.last_json_count,
            last_zip_signature=self.host.last_zip_signature,
            next_maintain_due_at=self.host.next_maintain_due_at,
        )

    def build_startup_runtime_deps(self) -> StartupRuntimeDeps:
        return StartupRuntimeDeps(
            build_snapshot=lambda: self.host.build_snapshot(self.host.current_snapshot_file),
            uploaded_snapshot_exists=self.host.last_uploaded_snapshot_file.exists,
            write_uploaded_snapshot=lambda rows: self.host.write_snapshot(
                self.host.last_uploaded_snapshot_file,
                rows,
            ),
            get_json_count=self.host.get_json_count,
            inspect_zip_files=self.host.settings.inspect_zip_files,
            get_zip_signature=self.host.get_zip_signature,
            inspect_zip_archives=self.host.inspect_zip_archives,
            run_stage=self.host._run_stage,
            run_stage_sequence=self.host._run_stage_sequence,
            check_and_maybe_upload=self.host.check_and_maybe_upload,
            queue_maintain=self.host.queue_maintain,
            run_maintain_on_start=self.host.settings.run_maintain_on_start,
            run_upload_on_start=self.host.settings.run_upload_on_start,
            maybe_start_maintain=self.host.maybe_start_maintain,
            maybe_start_upload=self.host.maybe_start_upload,
            render_progress_snapshot=self.host.render_progress_snapshot,
            monotonic=self.monotonic,
            maintain_interval_seconds=self.host.settings.maintain_interval_seconds,
            log=self.log,
        )

    def build_watch_runtime_state(self) -> WatchRuntimeState:
        return WatchRuntimeState(next_maintain_due_at=self.host.next_maintain_due_at)

    def build_watch_runtime_deps(self) -> WatchRuntimeDeps:
        return WatchRuntimeDeps(
            maintain_interval_seconds=self.host.settings.maintain_interval_seconds,
            upload_running=lambda: self.host.upload_process is not None,
            has_pending_upload_snapshot=lambda: self.host.pending_upload_snapshot is not None,
            queue_maintain=self.host.queue_maintain,
            run_stage=self.host._run_stage,
            run_stage_sequence=self.host._run_stage_sequence,
            poll_upload_process=self.host.poll_upload_process,
            poll_maintain_process=self.host.poll_maintain_process,
            probe_changes_during_active_upload=self.host.probe_changes_during_active_upload,
            check_and_maybe_upload=self.host.check_and_maybe_upload,
            maybe_start_upload=self.host.maybe_start_upload,
            maybe_start_maintain=self.host.maybe_start_maintain,
            render_progress_snapshot=self.host.render_progress_snapshot,
        )

    @staticmethod
    def run_runtime_cycle(
        *,
        run_cycle: Callable[[], object],
        apply_state: Callable[[object], None],
    ) -> int:
        result = run_cycle()
        apply_state(getattr(result, "state"))
        return int(getattr(result, "exit_code"))

    def run_startup_phase(self) -> int:
        run_startup_cycle = self.get_run_startup_cycle()
        return self.run_runtime_cycle(
            run_cycle=lambda: run_startup_cycle(
                state=self.build_startup_runtime_state(),
                deps=self.build_startup_runtime_deps(),
            ),
            apply_state=lambda state: self.apply_startup_runtime_state(state),
        )

    def run_watch_iteration(self, now: float) -> int:
        run_watch_iteration = self.get_run_watch_iteration()
        return self.run_runtime_cycle(
            run_cycle=lambda: run_watch_iteration(
                now_monotonic=now,
                state=self.build_watch_runtime_state(),
                deps=self.build_watch_runtime_deps(),
            ),
            apply_state=lambda state: self.apply_watch_runtime_state(state),
        )

    def run_stage_sequence(self, stages: Iterable[tuple[str, Callable[[], int]]]) -> int:
        for stage, runner in stages:
            handled_exit = self.run_stage(stage, runner)
            if handled_exit != 0:
                return handled_exit
        return 0

    def run_stage(self, stage: str, runner: Callable[[], int]) -> int:
        return self.resolve_stage_failure(stage, runner())

    def resolve_stage_failure(self, stage: str, code: int) -> int:
        if code == 0:
            return 0
        if self.host.handle_failure(stage, code):
            return 0
        return code
