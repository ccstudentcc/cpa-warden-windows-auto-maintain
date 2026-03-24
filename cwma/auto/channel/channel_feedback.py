from __future__ import annotations

from dataclasses import dataclass

from .channel_status import (
    CHANNEL_MAINTAIN,
    CHANNEL_UPLOAD,
    STAGE_FAILED,
    STAGE_IDLE,
    STAGE_PENDING_FULL,
    STAGE_PENDING_INCREMENTAL,
    STAGE_RETRY_WAIT,
    STATUS_FAILED,
    STATUS_RETRY,
)


@dataclass(frozen=True)
class NonSuccessExitFeedback:
    stage: str | None
    message: str | None


def _channel_name_lower(channel: str) -> str:
    if channel == CHANNEL_MAINTAIN:
        return "maintain"
    if channel == CHANNEL_UPLOAD:
        return "upload"
    return channel


def _channel_name_title(channel: str) -> str:
    return _channel_name_lower(channel).capitalize()


def format_command_start_failed_message(channel: str, exc: Exception) -> str:
    return f"{_channel_name_title(channel)} command failed to start: {exc}"


def format_command_start_retry_message(channel: str, retry_delay_seconds: int) -> str:
    return f"Will retry {_channel_name_lower(channel)} in {retry_delay_seconds}s."


def format_command_completed_message(channel: str) -> str:
    return f"{_channel_name_title(channel)} command completed."


def format_command_retry_message(channel: str, code: int, retry_delay_seconds: int) -> str:
    return (
        f"{_channel_name_title(channel)} command failed with exit {code}. "
        f"Retrying in {retry_delay_seconds}s..."
    )


def format_command_failed_message(channel: str, code: int) -> str:
    return f"{_channel_name_title(channel)} command failed after retries. Exit code {code}."


def maintain_pending_progress_stage(*, has_pending: bool, pending_names: set[str] | None) -> str:
    if not has_pending:
        return STAGE_IDLE
    return STAGE_PENDING_FULL if pending_names is None else STAGE_PENDING_INCREMENTAL


def non_success_exit_progress_stage(status: str) -> str | None:
    if status == STATUS_RETRY:
        return STAGE_RETRY_WAIT
    if status == STATUS_FAILED:
        return STAGE_FAILED
    return None


def build_non_success_exit_feedback(
    *,
    channel: str,
    status: str,
    code: int,
    retry_delay_seconds: int,
) -> NonSuccessExitFeedback:
    stage = non_success_exit_progress_stage(status)
    if status == STATUS_RETRY:
        message = format_command_retry_message(channel, code, retry_delay_seconds)
    elif status == STATUS_FAILED:
        message = format_command_failed_message(channel, code)
    else:
        message = None
    return NonSuccessExitFeedback(stage=stage, message=message)
