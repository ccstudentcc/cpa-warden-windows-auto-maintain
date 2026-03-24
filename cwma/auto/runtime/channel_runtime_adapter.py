"""Host adapter for maintain/upload channel start and poll orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from ..channel_commands import (
    format_maintain_start_message as format_maintain_start_message_rows,
    format_upload_start_message as format_upload_start_message_rows,
)
from ..channel_feedback import (
    build_non_success_exit_feedback,
    format_command_completed_message,
    format_command_start_failed_message,
    format_command_start_retry_message,
    maintain_pending_progress_stage,
)
from ..channel_lifecycle import decide_maintain_start_error, decide_upload_start_error
from ..channel_start_prep import prepare_maintain_start, prepare_upload_start
from ..channel_status import (
    CHANNEL_MAINTAIN,
    CHANNEL_UPLOAD,
    STAGE_DEFERRED,
    STAGE_RETRY_WAIT,
    STAGE_RUNNING,
    STAGE_START_FAILED,
    STATUS_FAILED,
    STATUS_RETRY,
    STATUS_SHUTDOWN,
    STATUS_SUCCESS,
)
from ..maintain_queue import decide_maintain_start_scope
from ..runtime.channel_runtime import (
    poll_maintain_channel,
    poll_upload_channel,
    start_maintain_channel,
    start_upload_channel,
)
from ..scope_files import write_scope_names
from ..snapshots import extract_names_from_snapshot as extract_names_from_snapshot_rows
from ..upload_postprocess import build_upload_success_postprocess
from ..upload_queue import decide_upload_start


class ChannelRuntimeHost(Protocol):
    settings: Any
    runtime: Any
    scheduler_policy: Any
    maintain_process: Any
    upload_process: Any
    pending_maintain: bool
    pending_maintain_reason: str | None
    pending_maintain_names: set[str] | None
    inflight_maintain_names: set[str] | None
    maintain_retry_due_at: float
    maintain_attempt: int
    last_incremental_maintain_started_at: float
    last_incremental_defer_reason: str | None
    pending_upload_snapshot: list[str] | None
    pending_upload_reason: str | None
    pending_upload_retry: bool
    inflight_upload_snapshot: list[str] | None
    upload_attempt: int
    upload_retry_due_at: float
    pending_source_changes_during_upload: bool
    last_json_count: int
    last_zip_signature: tuple[str, ...]
    maintain_names_file: Path
    upload_names_file: Path
    maintain_cmd_output_file: Path
    upload_cmd_output_file: Path
    last_uploaded_snapshot_file: Path
    current_snapshot_file: Path
    stable_snapshot_file: Path
    zip_extract_processed_signatures: dict[str, str]

    def _defer_incremental_maintain_if_needed(self, now: float) -> bool: ...
    def _maintain_queue_state(self) -> Any: ...
    def _apply_maintain_queue_state(self, state: Any) -> None: ...
    def _maintain_runtime_state(self) -> Any: ...
    def _apply_maintain_runtime_state(self, state: Any) -> None: ...
    def _upload_queue_state(self) -> Any: ...
    def _apply_upload_queue_state(self, state: Any) -> None: ...
    def _set_maintain_process(self, process: Any) -> None: ...
    def _set_upload_process(self, process: Any) -> None: ...
    def build_maintain_command(self, maintain_names_file: Path | None = None) -> list[str]: ...
    def build_upload_command(self, upload_names_file: Path | None = None) -> list[str]: ...
    def parse_child_progress_line(self, name: str, line: str) -> None: ...
    def mark_channel_running(self, name: str) -> None: ...
    def update_channel_progress(
        self,
        name: str,
        *,
        stage: str | None = None,
        done: int | None = None,
        total: int | None = None,
        force_render: bool = False,
    ) -> None: ...
    def read_snapshot(self, source: Path) -> list[str]: ...
    def write_snapshot(self, target: Path, lines: list[str]) -> None: ...
    def build_snapshot(self, target: Path) -> list[str]: ...
    def get_zip_signature(self) -> tuple[str, ...]: ...
    def delete_uploaded_files_from_snapshot(self, snapshot_lines: list[str]) -> None: ...
    def check_and_maybe_upload(
        self,
        force_deep_scan: bool = False,
        *,
        preserve_retry_state: bool = False,
        skip_stability_wait: bool = False,
        queue_reason: str = "detected JSON changes",
    ) -> int: ...
    def queue_maintain(self, reason: str, names: set[str] | None = None) -> None: ...


class ChannelRuntimeAdapter:
    def __init__(
        self,
        *,
        host: ChannelRuntimeHost,
        get_start_maintain_channel: Callable[[], Callable[..., Any]] = lambda: start_maintain_channel,
        get_start_upload_channel: Callable[[], Callable[..., Any]] = lambda: start_upload_channel,
        get_poll_maintain_channel: Callable[[], Callable[..., Any]] = lambda: poll_maintain_channel,
        get_poll_upload_channel: Callable[[], Callable[..., Any]] = lambda: poll_upload_channel,
        get_build_child_process_env: Callable[[], Callable[[], Mapping[str, str]]] | None = None,
        get_monotonic: Callable[[], Callable[[], float]] | None = None,
        get_popen_factory: Callable[[], Any] | None = None,
        log: Callable[[str], None] = print,
    ) -> None:
        self.host = host
        self.get_start_maintain_channel = get_start_maintain_channel
        self.get_start_upload_channel = get_start_upload_channel
        self.get_poll_maintain_channel = get_poll_maintain_channel
        self.get_poll_upload_channel = get_poll_upload_channel
        self.get_build_child_process_env = get_build_child_process_env or (lambda: (lambda: {}))
        self.get_monotonic = get_monotonic or (lambda: (lambda: 0.0))
        self.get_popen_factory = get_popen_factory or (lambda: None)
        self.log = log

    def _apply_channel_start_flow_feedback(
        self,
        *,
        channel: str,
        status: str,
        start_exception: Exception | None,
        return_code: int,
    ) -> int:
        if status == STATUS_SUCCESS:
            return return_code
        if start_exception is not None:
            self.log(format_command_start_failed_message(channel, start_exception))
        self.host.update_channel_progress(channel, stage=STAGE_START_FAILED, force_render=True)
        if status == STATUS_RETRY:
            self.log(
                format_command_start_retry_message(
                    channel,
                    self.host.settings.command_retry_delay_seconds,
                )
            )
            self.host.update_channel_progress(channel, stage=STAGE_RETRY_WAIT, force_render=True)
        return return_code

    def _finalize_channel_start_flow(
        self,
        *,
        channel: str,
        process: Any,
        status: str,
        start_exception: Exception | None,
        return_code: int,
        apply_state: Callable[[], None],
    ) -> int:
        apply_state()
        if channel == CHANNEL_MAINTAIN:
            self.host._set_maintain_process(process)
        else:
            self.host._set_upload_process(process)
        return self._apply_channel_start_flow_feedback(
            channel=channel,
            status=status,
            start_exception=start_exception,
            return_code=return_code,
        )

    def _start_and_finalize_channel_flow(
        self,
        *,
        channel: str,
        command: list[str],
        state: object,
        retry_count: int,
        output_file: Path,
        start_channel: Callable[..., object],
        apply_state_from_flow: Callable[[object], None],
    ) -> int:
        start_flow = start_channel(
            command=command,
            cwd=self.host.settings.base_dir,
            state=state,
            retry_count=retry_count,
            retry_delay_seconds=self.host.settings.command_retry_delay_seconds,
            env=self.get_build_child_process_env()(),
            output_file=output_file,
            on_output_line=lambda line: self.host.parse_child_progress_line(channel, line),
            log=self.log,
            mark_channel_running=self.host.mark_channel_running,
            now_monotonic=self.get_monotonic()(),
            popen_factory=self.get_popen_factory(),
        )
        return self._finalize_channel_start_flow(
            channel=channel,
            process=getattr(start_flow, "process"),
            status=getattr(start_flow, "status"),
            start_exception=getattr(start_flow, "start_exception"),
            return_code=getattr(start_flow, "return_code"),
            apply_state=lambda: apply_state_from_flow(start_flow),
        )

    def maybe_start_maintain(self) -> int:
        now = self.get_monotonic()()
        if self.host.maintain_process is not None:
            return 0
        if not self.host.pending_maintain:
            return 0
        if now < self.host.maintain_retry_due_at:
            return 0
        if self.host._defer_incremental_maintain_if_needed(now):
            return 0
        self.host.last_incremental_defer_reason = None

        self.host.maintain_attempt += 1
        max_attempts = self.host.settings.maintain_retry_count + 1
        reason = self.host.pending_maintain_reason or "unspecified"
        if self.host.pending_maintain_names is None:
            batch_size = 0
        else:
            batch_size = self.host.scheduler_policy.choose_incremental_maintain_batch_size(
                pending_count=len(self.host.pending_maintain_names),
                upload_pressure=(self.host.upload_process is not None)
                or bool(self.host.pending_upload_snapshot),
            )
        decision = decide_maintain_start_scope(
            state=self.host._maintain_queue_state(),
            batch_size=batch_size,
        )
        self.host._apply_maintain_queue_state(decision.state)
        if decision.skip_reason:
            self.host.maintain_attempt = 0
            self.host.maintain_retry_due_at = 0.0
            self.log(f"Skipped maintain start: {decision.skip_reason}.")
            return 0
        if not decision.should_start:
            return 0
        prep = prepare_maintain_start(
            reason=reason,
            attempt=self.host.maintain_attempt,
            max_attempts=max_attempts,
            scope_names=decision.scope_names,
            write_scope_file=lambda names: write_scope_names(self.host.maintain_names_file, names),
            build_command=self.host.build_maintain_command,
            format_start_message=lambda attempt, max_attempts, reason, scope_names: (
                format_maintain_start_message_rows(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=reason,
                    maintain_scope_names=scope_names,
                )
            ),
        )
        self.log(prep.log_message)
        if prep.started_incremental:
            self.host.last_incremental_maintain_started_at = now
        self.host.inflight_maintain_names = prep.scope_names
        return self._start_and_finalize_channel_flow(
            channel=CHANNEL_MAINTAIN,
            command=prep.command,
            state=self.host._maintain_runtime_state(),
            retry_count=self.host.settings.maintain_retry_count,
            output_file=self.host.maintain_cmd_output_file,
            start_channel=self.get_start_maintain_channel(),
            apply_state_from_flow=lambda flow: self.host._apply_maintain_runtime_state(
                getattr(flow, "state")
            ),
        )

    def maybe_start_upload(self) -> int:
        if self.host.upload_process is not None:
            return 0

        state = self.host._upload_queue_state()
        if state.pending_snapshot is None:
            return 0

        now = self.get_monotonic()()
        if state.pending_retry and state.inflight_snapshot is not None:
            batch_size = 1
        else:
            pending_total = len(state.pending_snapshot or [])
            maintain_pressure = self.host.maintain_process is not None or self.host.pending_maintain
            batch_size = self.host.scheduler_policy.choose_upload_batch_size(
                pending_count=pending_total,
                maintain_pressure=maintain_pressure,
            )

        decision = decide_upload_start(
            state=state,
            now_monotonic=now,
            batch_size=batch_size,
        )
        self.host._apply_upload_queue_state(decision.state)
        if decision.waiting_retry:
            return 0
        if not decision.can_start:
            return 0

        self.host.upload_attempt += 1
        max_attempts = self.host.settings.upload_retry_count + 1
        reason = self.host.pending_upload_reason or "detected changes"
        prep = prepare_upload_start(
            reason=reason,
            attempt=self.host.upload_attempt,
            max_attempts=max_attempts,
            batch=decision.batch,
            pending_total=len(self.host.pending_upload_snapshot or []),
            extract_scope_names=extract_names_from_snapshot_rows,
            write_scope_file=lambda names: write_scope_names(self.host.upload_names_file, names),
            build_command=self.host.build_upload_command,
            format_start_message=lambda attempt, max_attempts, reason, batch_size, pending_total: (
                format_upload_start_message_rows(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=reason,
                    batch_size=batch_size,
                    pending_total=pending_total,
                )
            ),
        )
        self.log(prep.log_message)
        self.host.inflight_upload_snapshot = list(prep.batch)
        return self._start_and_finalize_channel_flow(
            channel=CHANNEL_UPLOAD,
            command=prep.command,
            state=self.host._upload_queue_state(),
            retry_count=self.host.settings.upload_retry_count,
            output_file=self.host.upload_cmd_output_file,
            start_channel=self.get_start_upload_channel(),
            apply_state_from_flow=lambda flow: self.host._apply_upload_queue_state(getattr(flow, "state")),
        )

    def handle_command_start_error(self, name: str, exc: Exception) -> int:
        if name == CHANNEL_MAINTAIN:
            decision = decide_maintain_start_error(
                state=self.host._maintain_runtime_state(),
                retry_count=self.host.settings.maintain_retry_count,
                now_monotonic=self.get_monotonic()(),
                retry_delay_seconds=self.host.settings.command_retry_delay_seconds,
            )
            self.host._apply_maintain_runtime_state(decision.state)
            should_retry = decision.should_retry
        else:
            decision = decide_upload_start_error(
                state=self.host._upload_queue_state(),
                retry_count=self.host.settings.upload_retry_count,
                now_monotonic=self.get_monotonic()(),
                retry_delay_seconds=self.host.settings.command_retry_delay_seconds,
            )
            self.host._apply_upload_queue_state(decision.state)
            should_retry = decision.should_retry
        return self._apply_channel_start_flow_feedback(
            channel=name,
            status=STATUS_RETRY if should_retry else STATUS_FAILED,
            start_exception=exc,
            return_code=0 if should_retry else 1,
        )

    def _handle_maintain_success(self) -> None:
        self.log(format_command_completed_message(CHANNEL_MAINTAIN))
        stage = maintain_pending_progress_stage(
            has_pending=self.host.pending_maintain,
            pending_names=self.host.pending_maintain_names,
        )
        self.host.update_channel_progress(CHANNEL_MAINTAIN, stage=stage, done=0, total=0, force_render=True)

    def _sync_post_upload_snapshots(self, *, uploaded_snapshot: list[str]) -> Any:
        previous_uploaded_baseline = self.host.read_snapshot(self.host.last_uploaded_snapshot_file)

        if self.host.settings.delete_uploaded_files_after_upload:
            self.host.delete_uploaded_files_from_snapshot(uploaded_snapshot)

        current_snapshot = self.host.build_snapshot(self.host.current_snapshot_file)
        self.host.write_snapshot(self.host.stable_snapshot_file, current_snapshot)
        postprocess = build_upload_success_postprocess(
            previous_uploaded_baseline=previous_uploaded_baseline,
            uploaded_snapshot=uploaded_snapshot,
            current_snapshot=current_snapshot,
        )
        self.host.write_snapshot(self.host.last_uploaded_snapshot_file, postprocess.uploaded_baseline)
        self.host.last_json_count = len(current_snapshot)
        if self.host.settings.inspect_zip_files:
            self.host.last_zip_signature = self.host.get_zip_signature()
        self.host.runtime.snapshot.last_json_count = self.host.last_json_count
        self.host.runtime.snapshot.last_zip_signature = self.host.last_zip_signature
        self.host.runtime.snapshot.zip_extract_processed_signatures = dict(
            self.host.zip_extract_processed_signatures
        )
        return postprocess

    def _apply_post_upload_queue_state(self, *, postprocess: Any) -> None:
        self.host.pending_upload_snapshot = postprocess.queue_snapshot
        self.host.pending_upload_reason = postprocess.queue_reason
        if postprocess.pending_snapshot:
            self.log(
                "Detected files outside uploaded baseline after upload. "
                f"Queued next upload batch ({len(postprocess.pending_snapshot)} pending)."
            )
            self.host.update_channel_progress(
                CHANNEL_UPLOAD,
                stage=postprocess.progress_stage,
                force_render=True,
            )
            return
        self.host.update_channel_progress(
            CHANNEL_UPLOAD,
            stage=postprocess.progress_stage,
            done=0,
            total=0,
            force_render=True,
        )

    def _run_post_upload_follow_up_check_if_needed(self) -> int:
        if not self.host.pending_source_changes_during_upload:
            return 0
        self.host.pending_source_changes_during_upload = False
        self.log("Running immediate deep upload check after active-upload source changes.")
        return self.host.check_and_maybe_upload(force_deep_scan=True)

    def _queue_post_upload_maintain_if_enabled(self, *, uploaded_names: set[str]) -> None:
        if not self.host.settings.run_maintain_after_upload:
            return
        if uploaded_names:
            self.host.queue_maintain("post-upload maintain", names=uploaded_names)
            return
        self.log("Skipped post-upload maintain: no uploaded names detected.")

    def _handle_upload_success(self, *, uploaded_snapshot: list[str], return_code: int) -> int:
        self.log(format_command_completed_message(CHANNEL_UPLOAD))
        postprocess = self._sync_post_upload_snapshots(uploaded_snapshot=uploaded_snapshot)
        self._apply_post_upload_queue_state(postprocess=postprocess)
        follow_up_exit = self._run_post_upload_follow_up_check_if_needed()
        if follow_up_exit != 0:
            return follow_up_exit
        self._queue_post_upload_maintain_if_enabled(uploaded_names=postprocess.uploaded_names)
        return return_code

    def _finalize_polled_channel_flow(
        self,
        *,
        channel: str,
        exited: bool,
        status: str | None,
        exit_code: int | None,
        return_code: int,
        apply_state: Callable[[], None],
        on_success: Callable[[], int],
        on_non_success: Callable[[], None] | None = None,
    ) -> int:
        if not exited:
            return 0
        apply_state()
        if status == STATUS_SUCCESS:
            return on_success()
        if status == STATUS_SHUTDOWN:
            return return_code
        feedback = build_non_success_exit_feedback(
            channel=channel,
            status=str(status),
            code=int(exit_code or 0),
            retry_delay_seconds=self.host.settings.command_retry_delay_seconds,
        )
        if feedback.message:
            self.log(feedback.message)
        if feedback.stage is not None:
            self.host.update_channel_progress(channel, stage=feedback.stage, force_render=True)
        if on_non_success is not None:
            on_non_success()
        return return_code

    def _poll_and_finalize_channel_flow(
        self,
        *,
        channel: str,
        process: Any,
        state: object,
        retry_count: int,
        poll_channel: Callable[..., object],
        apply_state_from_flow: Callable[[object], None],
        set_process: Callable[[Any], None],
        on_success_from_flow: Callable[[object], int],
        on_non_success: Callable[[], None] | None = None,
    ) -> int:
        flow = poll_channel(
            process=process,
            state=state,
            shutdown_requested=self.host.shutdown_requested,
            retry_count=retry_count,
            retry_delay_seconds=self.host.settings.command_retry_delay_seconds,
            now_monotonic=self.get_monotonic()(),
        )
        set_process(getattr(flow, "process"))
        return self._finalize_polled_channel_flow(
            channel=channel,
            exited=getattr(flow, "exited"),
            status=getattr(flow, "status"),
            exit_code=getattr(flow, "exit_code"),
            return_code=getattr(flow, "return_code"),
            apply_state=lambda: apply_state_from_flow(flow),
            on_success=lambda: on_success_from_flow(flow),
            on_non_success=on_non_success,
        )

    def poll_maintain_process(self) -> int:
        return self._poll_and_finalize_channel_flow(
            channel=CHANNEL_MAINTAIN,
            process=self.host.maintain_process,
            state=self.host._maintain_runtime_state(),
            retry_count=self.host.settings.maintain_retry_count,
            poll_channel=self.get_poll_maintain_channel(),
            apply_state_from_flow=lambda flow: self.host._apply_maintain_runtime_state(
                getattr(flow, "state")
            ),
            set_process=self.host._set_maintain_process,
            on_success_from_flow=lambda flow: (
                self._handle_maintain_success() or getattr(flow, "return_code")
            ),
        )

    def poll_upload_process(self) -> int:
        uploaded_snapshot = self.host.inflight_upload_snapshot or []
        return self._poll_and_finalize_channel_flow(
            channel=CHANNEL_UPLOAD,
            process=self.host.upload_process,
            state=self.host._upload_queue_state(),
            retry_count=self.host.settings.upload_retry_count,
            poll_channel=self.get_poll_upload_channel(),
            apply_state_from_flow=lambda flow: self.host._apply_upload_queue_state(getattr(flow, "state")),
            set_process=self.host._set_upload_process,
            on_success_from_flow=lambda flow: self._handle_upload_success(
                uploaded_snapshot=uploaded_snapshot,
                return_code=getattr(flow, "return_code"),
            ),
            on_non_success=lambda: setattr(self.host, "pending_source_changes_during_upload", False),
        )

