"""Process supervision adapters used by runtime orchestration modules."""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from .channel_runner import ChannelStartResult, poll_process_exit, start_channel_with_handler
from .output_pump import append_child_output_line, start_output_pump_thread
from .process_output import build_child_process_env, decode_child_output_line
from .process_runner import terminate_running_process

LogCallback = Callable[[str], None]
ChannelCallback = Callable[[str], None]
OutputLineCallback = Callable[[str], None]
StartErrorHandler = Callable[[str, Exception], int]
PopenFactory = Callable[..., subprocess.Popen]


def _noop_log(_message: str) -> None:
    return


def _noop_channel(_channel: str) -> None:
    return


def _default_start_error_handler(_channel: str, _exc: Exception) -> int:
    return 1


@dataclass(frozen=True)
class ChannelExitResult:
    """Result of polling a running channel process."""

    process: subprocess.Popen | None
    exited: bool
    code: int | None


def _build_output_line_callback(
    *,
    output_file: Path | None,
    on_output_line: OutputLineCallback | None,
) -> OutputLineCallback:
    def _handle_output_line(line: str) -> None:
        if output_file is not None:
            append_child_output_line(target=output_file, line=line)
        if on_output_line is not None:
            on_output_line(line)

    return _handle_output_line


def start_channel(
    *,
    channel: str,
    command: list[str],
    cwd: str | Path | None,
    env: Mapping[str, str] | None = None,
    output_file: Path | None = None,
    on_output_line: OutputLineCallback | None = None,
    log: LogCallback | None = None,
    mark_channel_running: ChannelCallback | None = None,
    handle_start_error: StartErrorHandler | None = None,
    popen_factory: PopenFactory = subprocess.Popen,
) -> ChannelStartResult:
    """Start a channel process with optional output pumping and error handling."""

    env_map = build_child_process_env() if env is None else dict(env)
    warn = log or _noop_log
    mark_running = mark_channel_running or _noop_channel
    on_start_error = handle_start_error or _default_start_error_handler
    cwd_text = None if cwd is None else str(cwd)
    handle_output_line = _build_output_line_callback(
        output_file=output_file,
        on_output_line=on_output_line,
    )

    def _start_output_pump(channel_name: str, proc: subprocess.Popen) -> None:
        start_output_pump_thread(
            channel=channel_name,
            proc=proc,
            decode_line=decode_child_output_line,
            on_line=handle_output_line,
            warn=warn,
        )

    return start_channel_with_handler(
        channel=channel,
        cmd=list(command),
        env=env_map,
        cwd=cwd_text,
        start_output_pump=_start_output_pump,
        mark_channel_running=mark_running,
        handle_start_error=on_start_error,
        popen_factory=popen_factory,
    )


def poll_channel_exit(*, process: subprocess.Popen | None) -> ChannelExitResult:
    """Poll channel process exit state and clear handle when exited."""

    poll_result = poll_process_exit(process)
    if not poll_result.exited:
        return ChannelExitResult(process=process, exited=False, code=None)
    return ChannelExitResult(process=None, exited=True, code=int(poll_result.code or 0))


def terminate_channel(
    *,
    process: subprocess.Popen | None,
    channel: str,
    log: LogCallback,
    terminate_timeout_seconds: int = 20,
    kill_wait_seconds: int = 5,
) -> None:
    """Terminate a running channel process with graceful-then-kill fallback."""

    terminate_running_process(
        proc=process,
        name=channel,
        log=log,
        terminate_timeout_seconds=terminate_timeout_seconds,
        kill_wait_seconds=kill_wait_seconds,
    )


__all__ = [
    "ChannelExitResult",
    "start_channel",
    "poll_channel_exit",
    "terminate_channel",
]
