from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping


def launch_child_command(
    *,
    cmd: list[str],
    env: Mapping[str, str],
    cwd: str | None = None,
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
) -> subprocess.Popen:
    return popen_factory(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=1,
        env=dict(env),
    )


def terminate_running_process(
    *,
    proc: subprocess.Popen | None,
    name: str,
    log: Callable[[str], None],
    terminate_timeout_seconds: int = 20,
    kill_wait_seconds: int = 5,
) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        log(f"Terminating active {name} process (pid={proc.pid})...")
        proc.terminate()
        proc.wait(timeout=terminate_timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=kill_wait_seconds)
        except subprocess.TimeoutExpired:
            log(f"[WARN] Force-killed {name} process but wait timed out (pid={proc.pid}).")
    except Exception as exc:
        log(f"[WARN] Failed to terminate {name} process (pid={proc.pid}): {exc}")


def start_channel_command(
    *,
    channel: str,
    cmd: list[str],
    env: Mapping[str, str],
    cwd: str | None,
    start_output_pump: Callable[[str, subprocess.Popen], None],
    mark_channel_running: Callable[[str], None],
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
) -> subprocess.Popen:
    proc = launch_child_command(
        cmd=cmd,
        env=env,
        cwd=cwd,
        popen_factory=popen_factory,
    )
    start_output_pump(channel, proc)
    mark_channel_running(channel)
    return proc
