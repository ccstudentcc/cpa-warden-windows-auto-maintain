from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cwma.auto.channel_runner import (
    ChannelStartResult,
    ProcessPollResult,
    poll_process_exit,
    start_channel_with_handler,
)
from cwma.auto.process_runner import (
    launch_child_command,
    start_channel_command,
    terminate_running_process,
)
from cwma.auto.process_supervisor import poll_channel_exit, start_channel


class AutoModuleProcessChannelTests(unittest.TestCase):
    def test_launch_child_command_uses_factory_and_env(self) -> None:
        captured: dict[str, object] = {}

        class _Proc:
            pass

        def _factory(*args: object, **kwargs: object) -> _Proc:
            captured["args"] = args
            captured["kwargs"] = kwargs
            return _Proc()

        proc = launch_child_command(
            cmd=["python", "--version"],
            env={"A": "1"},
            cwd="C:/tmp",
            popen_factory=_factory,  # type: ignore[arg-type]
        )
        self.assertIsInstance(proc, _Proc)
        self.assertEqual(captured["args"], (["python", "--version"],))
        kwargs = captured["kwargs"]
        self.assertIsInstance(kwargs, dict)
        if not isinstance(kwargs, dict):
            self.fail("expected kwargs dict")
        self.assertEqual(kwargs["cwd"], "C:/tmp")
        self.assertEqual(kwargs["env"], {"A": "1"})

    def test_terminate_running_process_terminates_active_proc(self) -> None:
        proc = mock.Mock()
        proc.poll.return_value = None
        proc.pid = 123
        logs: list[str] = []
        terminate_running_process(proc=proc, name="upload", log=logs.append)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once()
        self.assertTrue(any("Terminating active upload process" in item for item in logs))

    def test_start_channel_command_starts_pump_and_marks_running(self) -> None:
        class _Proc:
            pass

        calls: list[tuple[str, object]] = []

        def _factory(*args: object, **kwargs: object) -> _Proc:
            return _Proc()

        proc = start_channel_command(
            channel="upload",
            cmd=["python", "--version"],
            env={"A": "1"},
            cwd="C:/tmp",
            start_output_pump=lambda name, p: calls.append((name, p)),
            mark_channel_running=lambda name: calls.append((name, "running")),
            popen_factory=_factory,  # type: ignore[arg-type]
        )
        self.assertIsInstance(proc, _Proc)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], "upload")
        self.assertEqual(calls[1], ("upload", "running"))

    def test_start_channel_with_handler_returns_process_on_success(self) -> None:
        class _Proc:
            pass

        result = start_channel_with_handler(
            channel="upload",
            cmd=["python", "--version"],
            env={"A": "1"},
            cwd="C:/tmp",
            start_output_pump=lambda _name, _proc: None,
            mark_channel_running=lambda _name: None,
            handle_start_error=lambda _name, _exc: 9,
            popen_factory=lambda *args, **kwargs: _Proc(),  # type: ignore[arg-type]
        )
        self.assertEqual(result.return_code, 0)
        self.assertIsInstance(result.process, _Proc)

    def test_start_channel_with_handler_uses_error_handler_on_failure(self) -> None:
        seen: dict[str, object] = {}

        def _handle_start_error(name: str, exc: Exception) -> int:
            seen["name"] = name
            seen["exc"] = exc
            return 7

        def _failing_factory(*args: object, **kwargs: object) -> object:
            raise RuntimeError("boom")

        result = start_channel_with_handler(
            channel="maintain",
            cmd=["python", "--version"],
            env={},
            cwd="C:/tmp",
            start_output_pump=lambda _name, _proc: None,
            mark_channel_running=lambda _name: None,
            handle_start_error=_handle_start_error,
            popen_factory=_failing_factory,  # type: ignore[arg-type]
        )
        self.assertEqual(result.return_code, 7)
        self.assertIsNone(result.process)
        self.assertEqual(seen["name"], "maintain")
        self.assertIsInstance(seen["exc"], RuntimeError)

    def test_poll_process_exit_states(self) -> None:
        class _Proc:
            def __init__(self, code: int | None) -> None:
                self._code = code

            def poll(self) -> int | None:
                return self._code

        self.assertFalse(poll_process_exit(None).exited)
        self.assertFalse(poll_process_exit(_Proc(None)).exited)
        exited = poll_process_exit(_Proc(3))
        self.assertTrue(exited.exited)
        self.assertEqual(exited.code, 3)

    def test_start_channel_uses_env_builder_and_output_callbacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_target = Path(tmp) / "upload.log"
            captured: dict[str, object] = {}
            forwarded_lines: list[str] = []

            class _Proc:
                pass

            def _stub_start_channel_with_handler(**kwargs: object) -> ChannelStartResult:
                captured.update(kwargs)
                proc = _Proc()
                start_output_pump = kwargs["start_output_pump"]
                if not callable(start_output_pump):
                    self.fail("start_output_pump should be callable")
                start_output_pump("upload", proc)  # type: ignore[misc]
                return ChannelStartResult(return_code=0, process=proc)  # type: ignore[arg-type]

            with mock.patch(
                "cwma.auto.process_supervisor.build_child_process_env",
                return_value={"AUTO": "1"},
            ) as env_builder, mock.patch(
                "cwma.auto.process_supervisor.start_channel_with_handler",
                side_effect=_stub_start_channel_with_handler,
            ) as start_with_handler, mock.patch(
                "cwma.auto.process_supervisor.start_output_pump_thread",
                side_effect=lambda channel, proc, decode_line, on_line, warn: on_line("hello"),
            ) as pump_thread, mock.patch(
                "cwma.auto.process_supervisor.append_child_output_line"
            ) as append_line:
                result = start_channel(
                    channel="upload",
                    command=["python", "--version"],
                    cwd=Path("C:/tmp"),
                    output_file=output_target,
                    on_output_line=forwarded_lines.append,
                    log=lambda _msg: None,
                    popen_factory=mock.Mock(),
                )

            self.assertEqual(result.return_code, 0)
            self.assertEqual(forwarded_lines, ["hello"])
            env_builder.assert_called_once()
            self.assertEqual(start_with_handler.call_count, 1)
            self.assertEqual(pump_thread.call_count, 1)
            append_line.assert_called_once_with(target=output_target, line="hello")
            self.assertEqual(captured["channel"], "upload")
            self.assertEqual(captured["cmd"], ["python", "--version"])
            self.assertEqual(captured["env"], {"AUTO": "1"})
            self.assertEqual(captured["cwd"], str(Path("C:/tmp")))

    def test_start_channel_prefers_explicit_env_over_builder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_target = Path(tmp) / "maintain.log"
            captured: dict[str, object] = {}

            class _Proc:
                pass

            def _stub_start_channel_with_handler(**kwargs: object) -> ChannelStartResult:
                captured.update(kwargs)
                return ChannelStartResult(return_code=0, process=_Proc())  # type: ignore[arg-type]

            with mock.patch("cwma.auto.process_supervisor.build_child_process_env") as env_builder, mock.patch(
                "cwma.auto.process_supervisor.start_channel_with_handler",
                side_effect=_stub_start_channel_with_handler,
            ) as start_with_handler, mock.patch(
                "cwma.auto.process_supervisor.start_output_pump_thread"
            ) as pump_thread:
                result = start_channel(
                    channel="maintain",
                    command=["python", "--version"],
                    cwd=Path("C:/tmp"),
                    env={"MANUAL_ENV": "1"},
                    output_file=output_target,
                    on_output_line=lambda _line: None,
                    log=lambda _msg: None,
                    popen_factory=mock.Mock(),
                )

            self.assertEqual(result.return_code, 0)
            self.assertEqual(start_with_handler.call_count, 1)
            self.assertEqual(pump_thread.call_count, 0)
            env_builder.assert_not_called()
            self.assertEqual(captured["env"], {"MANUAL_ENV": "1"})
            self.assertEqual(captured["cwd"], str(Path("C:/tmp")))

    def test_poll_channel_exit_preserves_running_process(self) -> None:
        proc = object()
        with mock.patch(
            "cwma.auto.process_supervisor.poll_process_exit",
            return_value=ProcessPollResult(exited=False, code=None),
        ) as poll_mock:
            result = poll_channel_exit(process=proc)  # type: ignore[arg-type]

        poll_mock.assert_called_once_with(proc)
        self.assertFalse(result.exited)
        self.assertIsNone(result.code)
        self.assertIs(result.process, proc)

    def test_poll_channel_exit_normalizes_none_code_to_zero(self) -> None:
        proc = object()
        with mock.patch(
            "cwma.auto.process_supervisor.poll_process_exit",
            return_value=ProcessPollResult(exited=True, code=None),
        ) as poll_mock:
            result = poll_channel_exit(process=proc)  # type: ignore[arg-type]

        poll_mock.assert_called_once_with(proc)
        self.assertTrue(result.exited)
        self.assertEqual(result.code, 0)
        self.assertIsNone(result.process)


if __name__ == "__main__":
    unittest.main()
