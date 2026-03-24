"""Runtime adapters for channel start/poll lifecycle delegation."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeVar

from ..channel.channel_lifecycle import (
    decide_maintain_process_exit,
    decide_maintain_start_error,
    decide_upload_process_exit,
    decide_upload_start_error,
)
from ..channel.channel_status import CHANNEL_MAINTAIN, CHANNEL_UPLOAD, STATUS_FAILED, STATUS_RETRY, STATUS_SUCCESS
from ..state.maintain_queue import MaintainRuntimeState
from ..infra.process_supervisor import poll_channel_exit as poll_subprocess_channel_exit
from ..infra.process_supervisor import start_channel as start_subprocess_channel
from ..state.upload_queue import UploadQueueState

StateT = TypeVar("StateT")
StartResultT = TypeVar("StartResultT")
PollResultT = TypeVar("PollResultT")


class _StartErrorDecision(Protocol[StateT]):
    state: StateT
    should_retry: bool


class _ProcessExitDecision(Protocol[StateT]):
    state: StateT
    status: str
    return_code: int


@dataclass(frozen=True)
class MaintainStartFlowResult:
    """Result of maintain channel start flow."""

    process: subprocess.Popen | None
    state: MaintainRuntimeState
    status: str
    return_code: int
    start_exception: Exception | None


@dataclass(frozen=True)
class UploadStartFlowResult:
    """Result of upload channel start flow."""

    process: subprocess.Popen | None
    state: UploadQueueState
    status: str
    return_code: int
    start_exception: Exception | None


@dataclass(frozen=True)
class MaintainPollFlowResult:
    """Result of maintain channel poll and lifecycle decision flow."""

    process: subprocess.Popen | None
    state: MaintainRuntimeState
    exited: bool
    exit_code: int | None
    status: str | None
    return_code: int


@dataclass(frozen=True)
class UploadPollFlowResult:
    """Result of upload channel poll and lifecycle decision flow."""

    process: subprocess.Popen | None
    state: UploadQueueState
    exited: bool
    exit_code: int | None
    status: str | None
    return_code: int


def _start_terminal_return_code(*, should_retry: bool) -> int:
    return 0 if should_retry else 1


def _resolve_monotonic(now_monotonic: float | None) -> float:
    return time.monotonic() if now_monotonic is None else now_monotonic


def _start_channel_flow(
    *,
    channel: str,
    command: list[str],
    cwd: str | Path | None,
    state: StateT,
    retry_count: int,
    retry_delay_seconds: int,
    decide_start_error: Callable[..., _StartErrorDecision[StateT]],
    build_result: Callable[[subprocess.Popen | None, StateT, str, int, Exception | None], StartResultT],
    env: Mapping[str, str] | None = None,
    output_file: Path | None = None,
    on_output_line: Callable[[str], None] | None = None,
    log: Callable[[str], None] | None = None,
    mark_channel_running: Callable[[str], None] | None = None,
    now_monotonic: float | None = None,
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
    start_channel_impl: Callable[..., object] = start_subprocess_channel,
) -> StartResultT:
    captured_exc: Exception | None = None

    def _capture_start_error(_channel: str, exc: Exception) -> int:
        nonlocal captured_exc
        captured_exc = exc
        return 1

    start_result = start_channel_impl(
        channel=channel,
        command=command,
        cwd=cwd,
        env=env,
        output_file=output_file,
        on_output_line=on_output_line,
        log=log,
        mark_channel_running=mark_channel_running,
        handle_start_error=_capture_start_error,
        popen_factory=popen_factory,
    )
    if start_result.return_code == 0:
        return build_result(start_result.process, state, STATUS_SUCCESS, 0, None)

    now_value = _resolve_monotonic(now_monotonic)
    decision = decide_start_error(
        state=state,
        retry_count=retry_count,
        now_monotonic=now_value,
        retry_delay_seconds=retry_delay_seconds,
    )
    status = STATUS_RETRY if decision.should_retry else STATUS_FAILED
    return build_result(
        None,
        decision.state,
        status,
        _start_terminal_return_code(should_retry=decision.should_retry),
        captured_exc,
    )


def _poll_channel_flow(
    *,
    process: subprocess.Popen | None,
    state: StateT,
    shutdown_requested: bool,
    retry_count: int,
    retry_delay_seconds: int,
    decide_process_exit: Callable[..., _ProcessExitDecision[StateT]],
    build_result: Callable[[subprocess.Popen | None, StateT, bool, int | None, str | None, int], PollResultT],
    now_monotonic: float | None = None,
    poll_channel_exit_impl: Callable[..., object] = poll_subprocess_channel_exit,
) -> PollResultT:
    poll_result = poll_channel_exit_impl(process=process)
    if not poll_result.exited:
        return build_result(poll_result.process, state, False, None, None, 0)

    exit_code = int(poll_result.code or 0)
    now_value = _resolve_monotonic(now_monotonic)
    decision = decide_process_exit(
        code=exit_code,
        shutdown_requested=shutdown_requested,
        state=state,
        retry_count=retry_count,
        now_monotonic=now_value,
        retry_delay_seconds=retry_delay_seconds,
    )
    return build_result(
        poll_result.process,
        decision.state,
        True,
        exit_code,
        decision.status,
        decision.return_code,
    )


def start_maintain_channel(
    *,
    command: list[str],
    cwd: str | Path | None,
    state: MaintainRuntimeState,
    retry_count: int,
    retry_delay_seconds: int,
    env: Mapping[str, str] | None = None,
    output_file: Path | None = None,
    on_output_line: Callable[[str], None] | None = None,
    log: Callable[[str], None] | None = None,
    mark_channel_running: Callable[[str], None] | None = None,
    now_monotonic: float | None = None,
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
    start_channel_impl: Callable[..., object] = start_subprocess_channel,
) -> MaintainStartFlowResult:
    """Start maintain channel and apply start-error retry policy on failure."""

    return _start_channel_flow(
        channel=CHANNEL_MAINTAIN,
        command=command,
        cwd=cwd,
        state=state,
        retry_count=retry_count,
        retry_delay_seconds=retry_delay_seconds,
        decide_start_error=decide_maintain_start_error,
        build_result=lambda process, state, status, return_code, start_exception: (
            MaintainStartFlowResult(
                process=process,
                state=state,
                status=status,
                return_code=return_code,
                start_exception=start_exception,
            )
        ),
        env=env,
        output_file=output_file,
        on_output_line=on_output_line,
        log=log,
        mark_channel_running=mark_channel_running,
        now_monotonic=now_monotonic,
        popen_factory=popen_factory,
        start_channel_impl=start_channel_impl,
    )


def start_upload_channel(
    *,
    command: list[str],
    cwd: str | Path | None,
    state: UploadQueueState,
    retry_count: int,
    retry_delay_seconds: int,
    env: Mapping[str, str] | None = None,
    output_file: Path | None = None,
    on_output_line: Callable[[str], None] | None = None,
    log: Callable[[str], None] | None = None,
    mark_channel_running: Callable[[str], None] | None = None,
    now_monotonic: float | None = None,
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
    start_channel_impl: Callable[..., object] = start_subprocess_channel,
) -> UploadStartFlowResult:
    """Start upload channel and apply start-error retry policy on failure."""

    return _start_channel_flow(
        channel=CHANNEL_UPLOAD,
        command=command,
        cwd=cwd,
        state=state,
        retry_count=retry_count,
        retry_delay_seconds=retry_delay_seconds,
        decide_start_error=decide_upload_start_error,
        build_result=lambda process, state, status, return_code, start_exception: (
            UploadStartFlowResult(
                process=process,
                state=state,
                status=status,
                return_code=return_code,
                start_exception=start_exception,
            )
        ),
        env=env,
        output_file=output_file,
        on_output_line=on_output_line,
        log=log,
        mark_channel_running=mark_channel_running,
        now_monotonic=now_monotonic,
        popen_factory=popen_factory,
        start_channel_impl=start_channel_impl,
    )


def poll_maintain_channel(
    *,
    process: subprocess.Popen | None,
    state: MaintainRuntimeState,
    shutdown_requested: bool,
    retry_count: int,
    retry_delay_seconds: int,
    now_monotonic: float | None = None,
    poll_channel_exit_impl: Callable[..., object] = poll_subprocess_channel_exit,
) -> MaintainPollFlowResult:
    """Poll maintain process and apply maintain exit-lifecycle decision logic."""

    return _poll_channel_flow(
        process=process,
        state=state,
        shutdown_requested=shutdown_requested,
        retry_count=retry_count,
        retry_delay_seconds=retry_delay_seconds,
        decide_process_exit=decide_maintain_process_exit,
        build_result=lambda process, state, exited, exit_code, status, return_code: (
            MaintainPollFlowResult(
                process=process,
                state=state,
                exited=exited,
                exit_code=exit_code,
                status=status,
                return_code=return_code,
            )
        ),
        now_monotonic=now_monotonic,
        poll_channel_exit_impl=poll_channel_exit_impl,
    )


def poll_upload_channel(
    *,
    process: subprocess.Popen | None,
    state: UploadQueueState,
    shutdown_requested: bool,
    retry_count: int,
    retry_delay_seconds: int,
    now_monotonic: float | None = None,
    poll_channel_exit_impl: Callable[..., object] = poll_subprocess_channel_exit,
) -> UploadPollFlowResult:
    """Poll upload process and apply upload exit-lifecycle decision logic."""

    return _poll_channel_flow(
        process=process,
        state=state,
        shutdown_requested=shutdown_requested,
        retry_count=retry_count,
        retry_delay_seconds=retry_delay_seconds,
        decide_process_exit=decide_upload_process_exit,
        build_result=lambda process, state, exited, exit_code, status, return_code: (
            UploadPollFlowResult(
                process=process,
                state=state,
                exited=exited,
                exit_code=exit_code,
                status=status,
                return_code=return_code,
            )
        ),
        now_monotonic=now_monotonic,
        poll_channel_exit_impl=poll_channel_exit_impl,
    )


__all__ = [
    "MaintainStartFlowResult",
    "UploadStartFlowResult",
    "MaintainPollFlowResult",
    "UploadPollFlowResult",
    "start_maintain_channel",
    "start_upload_channel",
    "poll_maintain_channel",
    "poll_upload_channel",
]
