from __future__ import annotations

from dataclasses import dataclass
from subprocess import Popen
from typing import Callable

from ..infra.process_runner import start_channel_command


@dataclass(frozen=True)
class ChannelStartResult:
    return_code: int
    process: Popen | None


@dataclass(frozen=True)
class ProcessPollResult:
    exited: bool
    code: int | None


def start_channel_with_handler(
    *,
    channel: str,
    cmd: list[str],
    env: dict[str, str],
    cwd: str,
    start_output_pump: Callable[[str, Popen], None],
    mark_channel_running: Callable[[str], None],
    handle_start_error: Callable[[str, Exception], int],
    popen_factory: Callable[..., Popen],
) -> ChannelStartResult:
    try:
        proc = start_channel_command(
            channel=channel,
            cmd=cmd,
            env=env,
            cwd=cwd,
            start_output_pump=start_output_pump,
            mark_channel_running=mark_channel_running,
            popen_factory=popen_factory,
        )
    except Exception as exc:
        return ChannelStartResult(return_code=handle_start_error(channel, exc), process=None)
    return ChannelStartResult(return_code=0, process=proc)


def poll_process_exit(proc: Popen | None) -> ProcessPollResult:
    if proc is None:
        return ProcessPollResult(exited=False, code=None)
    code = proc.poll()
    if code is None:
        return ProcessPollResult(exited=False, code=None)
    return ProcessPollResult(exited=True, code=code)
