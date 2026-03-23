from __future__ import annotations

from dataclasses import dataclass

from .channel_status import STATUS_FAILED, STATUS_RETRY, STATUS_SHUTDOWN, STATUS_SUCCESS
from .maintain_queue import (
    MaintainRuntimeState,
    mark_maintain_runtime_retry,
    mark_maintain_success,
    mark_maintain_terminal_failure,
)
from .upload_queue import (
    UploadQueueState,
    mark_upload_retry,
    mark_upload_success,
    mark_upload_terminal_failure,
)


@dataclass
class MaintainStartErrorDecision:
    state: MaintainRuntimeState
    should_retry: bool
    terminal_failure: bool


@dataclass
class UploadStartErrorDecision:
    state: UploadQueueState
    should_retry: bool
    terminal_failure: bool


@dataclass
class MaintainProcessExitDecision:
    state: MaintainRuntimeState
    status: str
    return_code: int


@dataclass
class UploadProcessExitDecision:
    state: UploadQueueState
    status: str
    return_code: int


def decide_maintain_start_error(
    *,
    state: MaintainRuntimeState,
    retry_count: int,
    now_monotonic: float,
    retry_delay_seconds: int,
) -> MaintainStartErrorDecision:
    max_attempts = retry_count + 1
    if state.attempt < max_attempts:
        return MaintainStartErrorDecision(
            state=mark_maintain_runtime_retry(
                state=state,
                now_monotonic=now_monotonic,
                retry_delay_seconds=retry_delay_seconds,
            ),
            should_retry=True,
            terminal_failure=False,
        )
    return MaintainStartErrorDecision(
        state=mark_maintain_terminal_failure(state),
        should_retry=False,
        terminal_failure=True,
    )


def decide_upload_start_error(
    *,
    state: UploadQueueState,
    retry_count: int,
    now_monotonic: float,
    retry_delay_seconds: int,
) -> UploadStartErrorDecision:
    max_attempts = retry_count + 1
    if state.attempt < max_attempts:
        return UploadStartErrorDecision(
            state=mark_upload_retry(
                state=state,
                now_monotonic=now_monotonic,
                retry_delay_seconds=retry_delay_seconds,
            ),
            should_retry=True,
            terminal_failure=False,
        )
    return UploadStartErrorDecision(
        state=mark_upload_terminal_failure(state),
        should_retry=False,
        terminal_failure=True,
    )


def decide_maintain_process_exit(
    *,
    code: int,
    shutdown_requested: bool,
    state: MaintainRuntimeState,
    retry_count: int,
    now_monotonic: float,
    retry_delay_seconds: int,
) -> MaintainProcessExitDecision:
    if code == 0:
        return MaintainProcessExitDecision(
            state=mark_maintain_success(state),
            status=STATUS_SUCCESS,
            return_code=0,
        )
    if shutdown_requested:
        return MaintainProcessExitDecision(
            state=state,
            status=STATUS_SHUTDOWN,
            return_code=130,
        )

    max_attempts = retry_count + 1
    if state.attempt < max_attempts:
        return MaintainProcessExitDecision(
            state=mark_maintain_runtime_retry(
                state=state,
                now_monotonic=now_monotonic,
                retry_delay_seconds=retry_delay_seconds,
            ),
            status=STATUS_RETRY,
            return_code=0,
        )
    return MaintainProcessExitDecision(
        state=mark_maintain_terminal_failure(state),
        status=STATUS_FAILED,
        return_code=code,
    )


def decide_upload_process_exit(
    *,
    code: int,
    shutdown_requested: bool,
    state: UploadQueueState,
    retry_count: int,
    now_monotonic: float,
    retry_delay_seconds: int,
) -> UploadProcessExitDecision:
    if code == 0:
        return UploadProcessExitDecision(
            state=mark_upload_success(state),
            status=STATUS_SUCCESS,
            return_code=0,
        )
    if shutdown_requested:
        return UploadProcessExitDecision(
            state=state,
            status=STATUS_SHUTDOWN,
            return_code=130,
        )

    max_attempts = retry_count + 1
    if state.attempt < max_attempts:
        return UploadProcessExitDecision(
            state=mark_upload_retry(
                state=state,
                now_monotonic=now_monotonic,
                retry_delay_seconds=retry_delay_seconds,
            ),
            status=STATUS_RETRY,
            return_code=0,
        )
    return UploadProcessExitDecision(
        state=mark_upload_terminal_failure(state),
        status=STATUS_FAILED,
        return_code=code,
    )
