"""In-process channel supervision helpers (Stage 1 skeleton)."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
import threading
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..channel.channel_runner import ChannelStartResult
from .output_pump import append_child_output_line
from .process_supervisor import ChannelExitResult

LogCallback = Callable[[str], None]
OutputLineCallback = Callable[[str], None]
StartErrorHandler = Callable[[str, Exception], int]
ThreadFactory = Callable[..., threading.Thread]
ChannelCommandRunner = Callable[..., int]

_CPA_INPROCESS_RUN_LOCK = threading.Lock()
_DISABLE_RICH_PROGRESS_ENV_KEY = "CPA_WARDEN_DISABLE_RICH_PROGRESS"
_DISABLE_RICH_PROGRESS_ENV_VALUE = "1"


def _noop_log(_message: str) -> None:
    return


def _noop_channel(_channel: str) -> None:
    return


def _noop_output_line(_line: str) -> None:
    return


def _default_start_error_handler(_channel: str, _exc: Exception) -> int:
    return 1


def _normalize_system_exit_code(exc: SystemExit) -> int:
    code = exc.code
    if isinstance(code, int):
        return code
    return 1


def _extract_cli_argv(command: list[str]) -> list[str]:
    if not command:
        raise ValueError("In-process channel command cannot be empty.")
    if command[0].startswith("-"):
        return list(command)
    if len(command) >= 2 and command[1].lower().endswith((".py", ".pyw")):
        return list(command[2:])
    if len(command) >= 3 and command[1] == "-m":
        return list(command[3:])
    if command[0].lower().endswith((".py", ".pyw")):
        return list(command[1:])
    if len(command) >= 2:
        return list(command[1:])
    return []


@contextmanager
def _temporary_env(env: Mapping[str, str] | None) -> Any:
    if env is None:
        yield
        return
    previous: dict[str, str | None] = {}
    for key, value in env.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def _temporary_cwd(cwd: str | Path | None) -> Any:
    if cwd is None:
        yield
        return
    previous = Path.cwd()
    os.chdir(cwd)
    try:
        yield
    finally:
        os.chdir(previous)


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


class _InProcessLogForwardHandler(logging.Handler):
    def __init__(self, emit_line: OutputLineCallback) -> None:
        super().__init__(level=logging.NOTSET)
        self._emit_line = emit_line

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            line = record.getMessage()
        self._emit_line(line)


@contextmanager
def _temporary_cpa_logger_bridge(*, logger: logging.Logger, on_output_line: OutputLineCallback) -> Any:
    removed_console_handlers: list[logging.Handler] = []
    for handler in list(logger.handlers):
        stream = getattr(handler, "stream", None)
        if stream in {sys.stdout, sys.stderr}:
            logger.removeHandler(handler)
            removed_console_handlers.append(handler)

    bridge_handler = _InProcessLogForwardHandler(on_output_line)
    bridge_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(bridge_handler)
    try:
        yield
    finally:
        logger.removeHandler(bridge_handler)
        for handler in removed_console_handlers:
            logger.addHandler(handler)


def run_cpa_command_inprocess(
    *,
    command: list[str],
    cwd: str | Path | None,
    env: Mapping[str, str] | None,
    log: LogCallback,
    on_output_line: OutputLineCallback | None = None,
) -> int:
    """Execute cpa_warden maintain/upload command in-process and return exit code."""

    from ...apps import cpa_warden as cpa_app
    from ...warden.cli import parse_cli_args as parse_cli_args_warden

    argv = _extract_cli_argv(command)
    if "--mode" not in argv:
        raise ValueError("In-process command requires --mode (maintain/upload).")

    try:
        mode_index = argv.index("--mode") + 1
        mode = argv[mode_index]
    except Exception as exc:
        raise ValueError("Invalid --mode argument for in-process command.") from exc
    if mode not in {"maintain", "upload"}:
        raise ValueError(f"In-process command mode not supported: {mode}")

    with _CPA_INPROCESS_RUN_LOCK, _temporary_env(env), _temporary_cwd(cwd):
        try:
            cpa_app.ensure_aiohttp()
        except SystemExit as exc:
            return _normalize_system_exit_code(exc)

        try:
            args = parse_cli_args_warden(cpa_app.DEFAULT_CONFIG_PATH, argv=argv)
            config_required = "--config" in argv
            conf = cpa_app.load_config_json(args.config, required=config_required)
            settings = cpa_app.build_settings(args, conf)
            cpa_app.configure_logging(settings["log_file"], settings["debug"])
            bridge_output = on_output_line or _noop_output_line
            with _temporary_cpa_logger_bridge(logger=cpa_app.LOGGER, on_output_line=bridge_output):
                cpa_app.ensure_credentials(settings, interactive=False)
                conn = cpa_app.connect_db(settings["db_path"])
                try:
                    cpa_app.init_db(conn)
                    if mode == "maintain":
                        asyncio.run(cpa_app.run_maintain_async(conn, settings))
                    else:
                        asyncio.run(cpa_app.run_upload_async(conn, settings))
                    return 0
                finally:
                    conn.close()
        except SystemExit as exc:
            return _normalize_system_exit_code(exc)
        except KeyboardInterrupt:
            return 130
        except Exception as exc:  # pragma: no cover - defensive branch
            log(f"[WARN] In-process {mode} channel execution failed: {exc}")
            return 1


class InProcessChannelHandle:
    """Popen-like handle for in-process channel execution."""

    def __init__(
        self,
        *,
        channel: str,
        command: list[str],
        run: ChannelCommandRunner,
        log: LogCallback,
        thread_factory: ThreadFactory = threading.Thread,
    ) -> None:
        self.channel = channel
        self.command = list(command)
        self.pid = os.getpid()
        self._run = run
        self._log = log
        self._thread_factory = thread_factory
        self._done_event = threading.Event()
        self._cancel_requested = threading.Event()
        self._lock = threading.Lock()
        self._exit_code: int | None = None
        self._forced_exit_code: int | None = None

        self._thread = self._thread_factory(
            target=self._run_thread_main,
            name=f"{channel}-inprocess-runner",
            daemon=True,
        )
        self._thread.start()

    def _run_thread_main(self) -> None:
        code = 1
        try:
            code = int(self._run(cancel_requested=self._cancel_requested))
        except Exception as exc:  # pragma: no cover - defensive branch
            self._log(f"[WARN] In-process {self.channel} runner crashed: {exc}")
            code = 1
        self._set_exit_code(code)

    def _set_exit_code(self, code: int) -> None:
        with self._lock:
            if self._forced_exit_code is not None:
                return
            self._exit_code = int(code)
            self._done_event.set()

    def poll(self) -> int | None:
        with self._lock:
            if self._forced_exit_code is not None:
                return self._forced_exit_code
            if not self._done_event.is_set():
                return None
            return int(self._exit_code if self._exit_code is not None else 1)

    def terminate(self) -> None:
        with self._lock:
            self._cancel_requested.set()
            if self._forced_exit_code is None:
                self._forced_exit_code = 130
                self._done_event.set()

    def kill(self) -> None:
        self.terminate()

    def wait(self, timeout: float | None = None) -> int:
        code = self.poll()
        if code is not None:
            return int(code)
        if timeout is None:
            self._done_event.wait()
        elif not self._done_event.wait(timeout):
            raise subprocess.TimeoutExpired(cmd=f"inprocess:{self.channel}", timeout=timeout)
        code = self.poll()
        return int(code if code is not None else 1)


def start_channel(
    *,
    channel: str,
    command: list[str],
    cwd: str | Path | None,
    env: Mapping[str, str] | None = None,
    output_file: Path | None = None,
    on_output_line: OutputLineCallback | None = None,
    log: LogCallback | None = None,
    mark_channel_running: Callable[[str], None] | None = None,
    handle_start_error: StartErrorHandler | None = None,
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
    command_runner: ChannelCommandRunner | None = None,
    thread_factory: ThreadFactory = threading.Thread,
) -> ChannelStartResult:
    """Start in-process channel execution with process-like lifecycle semantics."""

    del popen_factory
    warn = log or _noop_log
    mark_running = mark_channel_running or _noop_channel
    on_start_error = handle_start_error or _default_start_error_handler
    handle_output_line = _build_output_line_callback(
        output_file=output_file,
        on_output_line=on_output_line,
    )
    runner = command_runner or (
        lambda **kwargs: run_cpa_command_inprocess(
            command=list(kwargs["command"]),
            cwd=kwargs["cwd"],
            env=kwargs["env"],
            log=kwargs["log"],
            on_output_line=kwargs["on_output_line"],
        )
    )
    runner_env = dict(env or {})
    runner_env[_DISABLE_RICH_PROGRESS_ENV_KEY] = _DISABLE_RICH_PROGRESS_ENV_VALUE

    try:
        process = InProcessChannelHandle(
            channel=channel,
            command=command,
            run=lambda *, cancel_requested: runner(
                channel=channel,
                command=list(command),
                cwd=cwd,
                env=runner_env,
                log=warn,
                on_output_line=handle_output_line,
                cancel_requested=cancel_requested,
            ),
            log=warn,
            thread_factory=thread_factory,
        )
    except Exception as exc:
        return ChannelStartResult(return_code=on_start_error(channel, exc), process=None)

    mark_running(channel)
    return ChannelStartResult(return_code=0, process=process)


def poll_channel_exit(*, process: InProcessChannelHandle | None) -> ChannelExitResult:
    """Poll in-process channel exit state and clear handle when exited."""

    if process is None:
        return ChannelExitResult(process=None, exited=False, code=None)
    code = process.poll()
    if code is None:
        return ChannelExitResult(process=process, exited=False, code=None)
    return ChannelExitResult(process=None, exited=True, code=int(code))


__all__ = [
    "InProcessChannelHandle",
    "run_cpa_command_inprocess",
    "start_channel",
    "poll_channel_exit",
]
