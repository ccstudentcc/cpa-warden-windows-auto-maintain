from __future__ import annotations

import json
import os
import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest import mock

from cwma.auto.channel.channel_commands import (
    build_maintain_command,
    build_upload_command,
    format_maintain_start_message,
    format_upload_start_message,
)
from cwma.auto.channel.channel_feedback import (
    build_non_success_exit_feedback,
    format_command_completed_message,
    format_command_failed_message,
    format_command_retry_message,
    format_command_start_failed_message,
    format_command_start_retry_message,
    maintain_pending_progress_stage,
    non_success_exit_progress_stage,
)
from cwma.auto.channel.channel_lifecycle import (
    decide_maintain_process_exit,
    decide_maintain_start_error,
    decide_upload_process_exit,
    decide_upload_start_error,
)
from cwma.auto.channel.channel_runner import (
    ChannelStartResult,
    ProcessPollResult,
    poll_process_exit,
    start_channel_with_handler,
)
from cwma.auto.channel.channel_start_prep import prepare_maintain_start, prepare_upload_start
from cwma.auto.channel.channel_status import (
    CHANNEL_MAINTAIN,
    CHANNEL_UPLOAD,
    STAGE_FAILED,
    STAGE_IDLE,
    STAGE_PENDING_FULL,
    STAGE_PENDING_INCREMENTAL,
    STAGE_RETRY_WAIT,
    STATUS_FAILED,
    STATUS_RETRY,
    STATUS_SUCCESS,
)
from cwma.auto.infra.config import load_watch_config
from cwma.auto.infra.locking import InstanceLockState, is_pid_running, read_lock_pid, release_instance_lock
from cwma.auto.state.maintain_queue import MaintainQueueState, MaintainRuntimeState
from cwma.auto.infra.output_pump import append_child_output_line, start_output_pump_thread
from cwma.auto.infra.process_output import (
    build_child_process_env,
    decode_child_output_line,
    should_log_child_alert_line,
)
from cwma.auto.infra.process_runner import (
    launch_child_command,
    start_channel_command,
    terminate_running_process,
)
from cwma.auto.infra.process_supervisor import poll_channel_exit, start_channel
from cwma.auto.infra.upload_cleanup import cleanup_uploaded_files, prune_empty_dirs_under
from cwma.auto.state.upload_queue import UploadQueueState
from cwma.auto.infra.zip_intake import (
    compute_zip_signature,
    extract_zip_with_bandizip,
    extract_zip_with_windows_builtin,
    inspect_zip_archives,
    list_zip_json_entries,
    list_zip_paths,
    ps_quote,
)
from tests.temp_sandbox import TempSandboxState, setup_tempfile_sandbox, teardown_tempfile_sandbox

_TEMP_SANDBOX_STATE: TempSandboxState | None = None


def setUpModule() -> None:
    global _TEMP_SANDBOX_STATE
    _TEMP_SANDBOX_STATE = setup_tempfile_sandbox()


def tearDownModule() -> None:
    teardown_tempfile_sandbox(_TEMP_SANDBOX_STATE)


class AutoModuleProcessChannelTests(unittest.TestCase):
    def test_cleanup_uploaded_files_tracks_expected_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            ok_file = base / "ok.json"
            changed_file = base / "changed.json"
            ok_file.write_text("{}", encoding="utf-8")
            changed_file.write_text("{}", encoding="utf-8")

            ok_stat = ok_file.stat()
            changed_stat = changed_file.stat()
            failed_path = base / "cannot_unlink"
            failed_path.mkdir(parents=True, exist_ok=True)
            failed_stat = failed_path.stat()

            changed_file.write_text('{"changed": true}', encoding="utf-8")
            missing = base / "missing.json"

            result = cleanup_uploaded_files(
                [
                    f"{ok_file}|{ok_stat.st_size}|{ok_stat.st_mtime_ns}",
                    f"{changed_file}|{changed_stat.st_size}|{changed_stat.st_mtime_ns}",
                    f"{missing}|1|1",
                    "invalid",
                    f"{failed_path}|{failed_stat.st_size}|{failed_stat.st_mtime_ns}",
                ]
            )

            self.assertEqual(result.deleted, 1)
            self.assertEqual(result.skipped_changed, 1)
            self.assertEqual(result.skipped_missing, 1)
            self.assertEqual(result.failed, 1)
            self.assertFalse(ok_file.exists())
            self.assertTrue(changed_file.exists())

    def test_prune_empty_dirs_under_tracks_expected_counters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            auth_dir = Path(tmpdir) / "auth"
            deep_empty = auth_dir / "x" / "y"
            non_empty = auth_dir / "keep"
            deep_empty.mkdir(parents=True, exist_ok=True)
            non_empty.mkdir(parents=True, exist_ok=True)
            (non_empty / "token.json").write_text("{}", encoding="utf-8")

            result = prune_empty_dirs_under(auth_dir)

            self.assertEqual(result.removed, 2)
            self.assertEqual(result.skipped_non_empty, 1)
            self.assertEqual(result.skipped_missing, 0)
            self.assertEqual(result.failed, 0)
            self.assertTrue(auth_dir.exists())
            self.assertTrue(non_empty.exists())
            self.assertFalse((auth_dir / "x").exists())

    def test_build_maintain_command_with_scope_and_yes(self) -> None:
        cmd = build_maintain_command(
            command_base=["python", "cpa_warden.py"],
            maintain_db_path=Path("m.sqlite3"),
            maintain_log_file=Path("m.log"),
            maintain_names_file=Path("names.txt"),
            assume_yes=True,
        )
        self.assertIn("--mode", cmd)
        self.assertIn("maintain", cmd)
        self.assertIn("--maintain-names-file", cmd)
        self.assertIn("names.txt", cmd)
        self.assertIn("--yes", cmd)

    def test_build_upload_command_with_scope(self) -> None:
        cmd = build_upload_command(
            command_base=["python", "cpa_warden.py"],
            auth_dir=Path("auth_files"),
            upload_db_path=Path("u.sqlite3"),
            upload_log_file=Path("u.log"),
            upload_names_file=Path("upload_names.txt"),
        )
        self.assertIn("--mode", cmd)
        self.assertIn("upload", cmd)
        self.assertIn("--upload-dir", cmd)
        self.assertIn("auth_files", cmd)
        self.assertIn("--upload-names-file", cmd)
        self.assertIn("upload_names.txt", cmd)

    def test_format_start_messages(self) -> None:
        maintain_msg = format_maintain_start_message(
            attempt=1,
            max_attempts=3,
            reason="post-upload maintain",
            maintain_scope_names={"a.json", "b.json"},
        )
        upload_msg = format_upload_start_message(
            attempt=2,
            max_attempts=3,
            reason="detected JSON changes",
            batch_size=20,
            pending_total=45,
        )
        self.assertIn("scope=incremental names=2", maintain_msg)
        self.assertIn("attempt 2 of 3", upload_msg)
        self.assertIn("batch_size=20", upload_msg)
        self.assertIn("pending_total=45", upload_msg)

    def test_channel_feedback_message_formatting(self) -> None:
        self.assertEqual(
            format_command_start_failed_message(CHANNEL_MAINTAIN, RuntimeError("boom")),
            "Maintain command failed to start: boom",
        )
        self.assertEqual(
            format_command_start_retry_message(CHANNEL_UPLOAD, 20),
            "Will retry upload in 20s.",
        )
        self.assertEqual(
            format_command_completed_message(CHANNEL_MAINTAIN),
            "Maintain command completed.",
        )
        self.assertEqual(
            format_command_retry_message(CHANNEL_UPLOAD, 7, 15),
            "Upload command failed with exit 7. Retrying in 15s...",
        )
        self.assertEqual(
            format_command_failed_message(CHANNEL_MAINTAIN, 9),
            "Maintain command failed after retries. Exit code 9.",
        )

    def test_channel_feedback_stage_mappings(self) -> None:
        self.assertEqual(
            maintain_pending_progress_stage(has_pending=False, pending_names=None),
            STAGE_IDLE,
        )
        self.assertEqual(
            maintain_pending_progress_stage(has_pending=True, pending_names=None),
            STAGE_PENDING_FULL,
        )
        self.assertEqual(
            maintain_pending_progress_stage(has_pending=True, pending_names={"a.json"}),
            STAGE_PENDING_INCREMENTAL,
        )
        self.assertEqual(non_success_exit_progress_stage(STATUS_RETRY), STAGE_RETRY_WAIT)
        self.assertEqual(non_success_exit_progress_stage(STATUS_FAILED), STAGE_FAILED)
        self.assertIsNone(non_success_exit_progress_stage(STATUS_SUCCESS))

    def test_build_non_success_exit_feedback(self) -> None:
        retry_feedback = build_non_success_exit_feedback(
            channel=CHANNEL_UPLOAD,
            status=STATUS_RETRY,
            code=7,
            retry_delay_seconds=15,
        )
        self.assertEqual(retry_feedback.stage, STAGE_RETRY_WAIT)
        self.assertEqual(
            retry_feedback.message,
            "Upload command failed with exit 7. Retrying in 15s...",
        )

        failed_feedback = build_non_success_exit_feedback(
            channel=CHANNEL_MAINTAIN,
            status=STATUS_FAILED,
            code=9,
            retry_delay_seconds=20,
        )
        self.assertEqual(failed_feedback.stage, STAGE_FAILED)
        self.assertEqual(
            failed_feedback.message,
            "Maintain command failed after retries. Exit code 9.",
        )

        neutral_feedback = build_non_success_exit_feedback(
            channel=CHANNEL_UPLOAD,
            status=STATUS_SUCCESS,
            code=0,
            retry_delay_seconds=20,
        )
        self.assertIsNone(neutral_feedback.stage)
        self.assertIsNone(neutral_feedback.message)

    def test_prepare_maintain_start_with_incremental_scope(self) -> None:
        prep = prepare_maintain_start(
            reason="post-upload maintain",
            attempt=1,
            max_attempts=3,
            scope_names={"a.json"},
            write_scope_file=lambda names: Path(f"scope-{len(names)}.txt"),
            build_command=lambda scope_file: ["maintain", str(scope_file) if scope_file else "-"],
            format_start_message=lambda attempt, max_attempts, reason, scope_names: (
                f"a={attempt}/{max_attempts}|r={reason}|n={len(scope_names or set())}"
            ),
        )
        self.assertEqual(prep.scope_file, Path("scope-1.txt"))
        self.assertTrue(prep.started_incremental)
        self.assertEqual(prep.command[0], "maintain")
        self.assertIn("n=1", prep.log_message)

    def test_prepare_upload_start_without_scope_file(self) -> None:
        prep = prepare_upload_start(
            reason="detected JSON changes",
            attempt=2,
            max_attempts=3,
            batch=["a|1|1", "b|1|1"],
            pending_total=4,
            extract_scope_names=lambda _batch: set(),
            write_scope_file=lambda names: Path(f"scope-{len(names)}.txt"),
            build_command=lambda scope_file: ["upload", str(scope_file) if scope_file else "-"],
            format_start_message=lambda attempt, max_attempts, reason, batch_size, pending_total: (
                f"a={attempt}/{max_attempts}|r={reason}|b={batch_size}|p={pending_total}"
            ),
        )
        self.assertIsNone(prep.scope_file)
        self.assertEqual(prep.batch, ["a|1|1", "b|1|1"])
        self.assertEqual(prep.command[0], "upload")
        self.assertIn("b=2", prep.log_message)

    def test_load_watch_config_parses_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch.json"
            path.write_text(json.dumps({"watch_interval_seconds": 15}), encoding="utf-8")
            cfg = load_watch_config(path)
        self.assertEqual(cfg["watch_interval_seconds"], 15)

    def test_read_lock_pid_parses_valid_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            lock_file.write_text("12345|token|1700000000", encoding="utf-8")
            self.assertEqual(read_lock_pid(lock_file), 12345)

    def test_read_lock_pid_handles_invalid_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            lock_file.write_text("not-a-pid", encoding="utf-8")
            self.assertIsNone(read_lock_pid(lock_file))

    def test_release_instance_lock_removes_matching_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            token = "token-1"
            lock_file.write_text(f"100|{token}|1700000000", encoding="utf-8")
            state = InstanceLockState(token=token, handle=None)
            next_state = release_instance_lock(
                lock_file=lock_file,
                allow_multi_instance=False,
                state=state,
                log=lambda _msg: None,
            )
            self.assertIsNone(next_state.token)
            self.assertFalse(lock_file.exists())

    def test_release_instance_lock_keeps_mismatched_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_file = Path(tmp) / "auto_maintain.lock"
            lock_file.write_text("100|token-a|1700000000", encoding="utf-8")
            state = InstanceLockState(token="token-b", handle=None)
            _ = release_instance_lock(
                lock_file=lock_file,
                allow_multi_instance=False,
                state=state,
                log=lambda _msg: None,
            )
            self.assertTrue(lock_file.exists())

    def test_is_pid_running_accepts_current_pid(self) -> None:
        self.assertTrue(is_pid_running(os.getpid()))

    def test_decode_child_output_line_supports_gb18030(self) -> None:
        raw = "维护完成".encode("gb18030")
        self.assertEqual(decode_child_output_line(raw), "维护完成")

    def test_build_child_process_env_sets_utf8_defaults(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            env = build_child_process_env()
        self.assertEqual(env["PYTHONUTF8"], "1")
        self.assertEqual(env["PYTHONIOENCODING"], "utf-8")

    def test_should_log_child_alert_line_filters_timeout_marker(self) -> None:
        self.assertFalse(should_log_child_alert_line("foo timeout=15s bar"))
        self.assertTrue(should_log_child_alert_line("request failed with status 500"))

    def test_decide_maintain_start_error_retry_then_terminal(self) -> None:
        runtime = MaintainRuntimeState(
            queue=MaintainQueueState(pending=False, reason=None, names=None),
            inflight_names={"a.json"},
            attempt=1,
            retry_due_at=0.0,
        )
        retry_decision = decide_maintain_start_error(
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertTrue(retry_decision.should_retry)
        self.assertFalse(retry_decision.terminal_failure)
        self.assertTrue(retry_decision.state.queue.pending)
        self.assertEqual(retry_decision.state.retry_due_at, 15.0)

        terminal_decision = decide_maintain_start_error(
            state=runtime,
            retry_count=0,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertFalse(terminal_decision.should_retry)
        self.assertTrue(terminal_decision.terminal_failure)
        self.assertFalse(terminal_decision.state.queue.pending)

    def test_decide_upload_start_error_retry_then_terminal(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="queued",
            pending_retry=False,
            inflight_snapshot=["a|1|1"],
            attempt=1,
            retry_due_at=0.0,
        )
        retry_decision = decide_upload_start_error(
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertTrue(retry_decision.should_retry)
        self.assertFalse(retry_decision.terminal_failure)
        self.assertTrue(retry_decision.state.pending_retry)
        self.assertEqual(retry_decision.state.retry_due_at, 24.0)

        terminal_decision = decide_upload_start_error(
            state=state,
            retry_count=0,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertFalse(terminal_decision.should_retry)
        self.assertTrue(terminal_decision.terminal_failure)
        self.assertIsNone(terminal_decision.state.pending_snapshot)

    def test_decide_maintain_process_exit_statuses(self) -> None:
        runtime = MaintainRuntimeState(
            queue=MaintainQueueState(pending=False, reason=None, names=None),
            inflight_names={"a.json"},
            attempt=1,
            retry_due_at=0.0,
        )

        success = decide_maintain_process_exit(
            code=0,
            shutdown_requested=False,
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(success.status, "success")
        self.assertEqual(success.return_code, 0)
        self.assertEqual(success.state.attempt, 0)

        shutdown = decide_maintain_process_exit(
            code=2,
            shutdown_requested=True,
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(shutdown.status, "shutdown")
        self.assertEqual(shutdown.return_code, 130)
        self.assertEqual(shutdown.state, runtime)

        retry = decide_maintain_process_exit(
            code=2,
            shutdown_requested=False,
            state=runtime,
            retry_count=1,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(retry.status, "retry")
        self.assertEqual(retry.return_code, 0)
        self.assertTrue(retry.state.queue.pending)
        self.assertEqual(retry.state.retry_due_at, 15.0)

        failed = decide_maintain_process_exit(
            code=2,
            shutdown_requested=False,
            state=runtime,
            retry_count=0,
            now_monotonic=10.0,
            retry_delay_seconds=5,
        )
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.return_code, 2)
        self.assertFalse(failed.state.queue.pending)

    def test_decide_upload_process_exit_statuses(self) -> None:
        state = UploadQueueState(
            pending_snapshot=["a|1|1"],
            pending_reason="queued",
            pending_retry=False,
            inflight_snapshot=["a|1|1"],
            attempt=1,
            retry_due_at=0.0,
        )

        success = decide_upload_process_exit(
            code=0,
            shutdown_requested=False,
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(success.status, "success")
        self.assertEqual(success.return_code, 0)
        self.assertIsNone(success.state.inflight_snapshot)
        self.assertEqual(success.state.attempt, 0)

        shutdown = decide_upload_process_exit(
            code=3,
            shutdown_requested=True,
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(shutdown.status, "shutdown")
        self.assertEqual(shutdown.return_code, 130)
        self.assertEqual(shutdown.state, state)

        retry = decide_upload_process_exit(
            code=3,
            shutdown_requested=False,
            state=state,
            retry_count=1,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(retry.status, "retry")
        self.assertEqual(retry.return_code, 0)
        self.assertTrue(retry.state.pending_retry)
        self.assertEqual(retry.state.retry_due_at, 24.0)

        failed = decide_upload_process_exit(
            code=3,
            shutdown_requested=False,
            state=state,
            retry_count=0,
            now_monotonic=20.0,
            retry_delay_seconds=4,
        )
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.return_code, 3)
        self.assertIsNone(failed.state.pending_snapshot)

    def test_append_child_output_line_writes_line_with_newline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "state" / "upload_command_output.log"
            append_child_output_line(target=target, line="hello")
            self.assertEqual(target.read_text(encoding="utf-8"), "hello\n")

    def test_start_output_pump_thread_decodes_and_forwards_lines(self) -> None:
        class _Proc:
            def __init__(self) -> None:
                self.stdout = BytesIO("维护完成\n".encode("utf-8"))

        class _FakeThread:
            def __init__(self, *, target: object, name: str, daemon: bool) -> None:
                self._target = target
                self.name = name
                self.daemon = daemon
                self.started = False

            def start(self) -> None:
                self.started = True
                if callable(self._target):
                    self._target()

            def join(self, timeout: float | None = None) -> None:
                return None

        received: list[str] = []
        warnings: list[str] = []
        fake_threads: list[_FakeThread] = []

        def _thread_factory(*, target: object, name: str, daemon: bool) -> _FakeThread:
            thread = _FakeThread(target=target, name=name, daemon=daemon)
            fake_threads.append(thread)
            return thread

        proc = _Proc()
        thread = start_output_pump_thread(
            channel="maintain",
            proc=proc,  # type: ignore[arg-type]
            decode_line=lambda raw: raw.decode("utf-8") if isinstance(raw, bytes) else raw,
            on_line=lambda line: received.append(line),
            warn=lambda msg: warnings.append(msg),
            thread_factory=_thread_factory,  # type: ignore[arg-type]
        )
        self.assertIsNotNone(thread)
        if thread is None:
            self.fail("expected output pump thread")
        self.assertEqual(len(fake_threads), 1)
        self.assertTrue(fake_threads[0].started)
        self.assertEqual(fake_threads[0].name, "maintain-output-pump")
        self.assertTrue(fake_threads[0].daemon)
        self.assertEqual(received, ["维护完成\n"])
        self.assertEqual(warnings, [])

    def test_list_zip_paths_and_signature(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            zip_path = auth_dir / "a.zip"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            paths = list_zip_paths(auth_dir)
            sig = compute_zip_signature(auth_dir, log=lambda _msg: None)
        self.assertEqual(paths, [zip_path])
        self.assertEqual(len(sig), 1)
        self.assertIn(str(zip_path), sig[0])

    def test_list_zip_paths_supports_multi_archive_extensions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            zip_path = auth_dir / "a.zip"
            seven_z_path = auth_dir / "b.7z"
            rar_path = auth_dir / "c.rar"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            seven_z_path.write_bytes(b"7z")
            rar_path.write_bytes(b"Rar!")
            paths = list_zip_paths(auth_dir)
            sig = compute_zip_signature(auth_dir, log=lambda _msg: None)
        self.assertEqual(paths, [zip_path, seven_z_path, rar_path])
        self.assertEqual(len(sig), 3)

    def test_list_zip_json_entries_supports_nested_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "nested.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("a/b/c.json", "{}")
                zf.writestr("a/d.txt", "x")
            with zipfile.ZipFile(zip_path, "r") as zf:
                entries = list_zip_json_entries(zf)
        self.assertEqual(entries, ["a/b/c.json"])

    def test_extract_zip_with_bandizip_returns_one_when_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            zip_path = base_dir / "x.zip"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            logs: list[str] = []
            with mock.patch("cwma.auto.infra.zip_intake.shutil.which", return_value=None):
                code = extract_zip_with_bandizip(
                    zip_path=zip_path,
                    output_dir=base_dir,
                    base_dir=base_dir,
                    bandizip_path="",
                    timeout_seconds=5,
                    log=logs.append,
                )
        self.assertEqual(code, 1)
        self.assertTrue(any("Bandizip not found" in item for item in logs))

    def test_extract_zip_with_bandizip_prefers_bz_console_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            zip_path = base_dir / "x.zip"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            commands: list[list[str]] = []

            def _which(name: str) -> str | None:
                mapping = {
                    "bz.exe": r"C:\\Program Files\\Bandizip\\bz.exe",
                    "Bandizip.exe": r"C:\\Program Files\\Bandizip\\Bandizip.exe",
                }
                return mapping.get(name)

            def _run(cmd: list[str], **kwargs: object) -> mock.Mock:
                commands.append(cmd)
                result = mock.Mock()
                result.returncode = 0
                result.stdout = ""
                result.stderr = ""
                return result

            with mock.patch("cwma.auto.infra.zip_intake.shutil.which", side_effect=_which), mock.patch(
                "cwma.auto.infra.zip_intake.subprocess.run",
                side_effect=_run,
            ):
                code = extract_zip_with_bandizip(
                    zip_path=zip_path,
                    output_dir=base_dir,
                    base_dir=base_dir,
                    bandizip_path="",
                    timeout_seconds=5,
                    prefer_console=True,
                    hide_window=False,
                    log=lambda _msg: None,
                )
        self.assertEqual(code, 0)
        self.assertGreaterEqual(len(commands), 1)
        self.assertTrue(commands[0][0].lower().endswith("bz.exe"))

    def test_extract_zip_with_windows_builtin_returns_one_when_shell_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            zip_path = base_dir / "x.zip"
            zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
            logs: list[str] = []
            with mock.patch("cwma.auto.infra.zip_intake.shutil.which", return_value=None):
                code = extract_zip_with_windows_builtin(
                    zip_path=zip_path,
                    output_dir=base_dir,
                    base_dir=base_dir,
                    timeout_seconds=5,
                    log=logs.append,
                )
        self.assertEqual(code, 1)
        self.assertTrue(any("shell not found" in item for item in logs))

    def test_ps_quote_escapes_single_quote(self) -> None:
        self.assertEqual(ps_quote("a'b"), "a''b")

    def test_inspect_zip_archives_tracks_processed_signature(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            zip_path = auth_dir / "nested.zip"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("nested/a.json", "{}")

            processed: dict[str, str] = {}
            extracted: list[str] = []

            def _extract(path: Path, output_dir: Path) -> int:
                extracted.append(f"{path.name}->{output_dir}")
                return 0

            changed_first = inspect_zip_archives(
                auth_dir=auth_dir,
                inspect_zip_files=True,
                auto_extract_zip_json=True,
                delete_zip_after_extract=False,
                processed_signatures=processed,
                extract_zip=_extract,
                log=lambda _msg: None,
            )
            changed_second = inspect_zip_archives(
                auth_dir=auth_dir,
                inspect_zip_files=True,
                auto_extract_zip_json=True,
                delete_zip_after_extract=False,
                processed_signatures=processed,
                extract_zip=_extract,
                log=lambda _msg: None,
            )

        self.assertTrue(changed_first)
        self.assertFalse(changed_second)
        self.assertEqual(len(extracted), 1)

    def test_inspect_zip_archives_delete_mode_removes_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            zip_path = auth_dir / "delete-me.zip"
            with zipfile.ZipFile(zip_path, "w") as archive:
                archive.writestr("a.json", "{}")

            changed = inspect_zip_archives(
                auth_dir=auth_dir,
                inspect_zip_files=True,
                auto_extract_zip_json=True,
                delete_zip_after_extract=True,
                processed_signatures={},
                extract_zip=lambda _path, _output: 0,
                log=lambda _msg: None,
            )

        self.assertTrue(changed)
        self.assertFalse(zip_path.exists())

    def test_inspect_zip_archives_supports_non_zip_archive_with_idempotency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            auth_dir = Path(tmp)
            seven_z_path = auth_dir / "bundle.7z"
            seven_z_path.write_bytes(b"7z")

            processed: dict[str, str] = {}
            extracted: list[str] = []

            def _extract(path: Path, output_dir: Path) -> int:
                extracted.append(f"{path.name}->{output_dir}")
                return 0

            with mock.patch(
                "cwma.auto.infra.zip_intake.list_non_zip_json_entries_with_bandizip",
                return_value=None,
            ):
                changed_first = inspect_zip_archives(
                    auth_dir=auth_dir,
                    inspect_zip_files=True,
                    auto_extract_zip_json=True,
                    delete_zip_after_extract=False,
                    processed_signatures=processed,
                    extract_zip=_extract,
                    log=lambda _msg: None,
                )
                changed_second = inspect_zip_archives(
                    auth_dir=auth_dir,
                    inspect_zip_files=True,
                    auto_extract_zip_json=True,
                    delete_zip_after_extract=False,
                    processed_signatures=processed,
                    extract_zip=_extract,
                    log=lambda _msg: None,
                )

        self.assertTrue(changed_first)
        self.assertFalse(changed_second)
        self.assertEqual(len(extracted), 1)

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
                "cwma.auto.infra.process_supervisor.build_child_process_env",
                return_value={"AUTO": "1"},
            ) as env_builder, mock.patch(
                "cwma.auto.infra.process_supervisor.start_channel_with_handler",
                side_effect=_stub_start_channel_with_handler,
            ) as start_with_handler, mock.patch(
                "cwma.auto.infra.process_supervisor.start_output_pump_thread",
                side_effect=lambda channel, proc, decode_line, on_line, warn: on_line("hello"),
            ) as pump_thread, mock.patch(
                "cwma.auto.infra.process_supervisor.append_child_output_line"
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

            with mock.patch("cwma.auto.infra.process_supervisor.build_child_process_env") as env_builder, mock.patch(
                "cwma.auto.infra.process_supervisor.start_channel_with_handler",
                side_effect=_stub_start_channel_with_handler,
            ) as start_with_handler, mock.patch(
                "cwma.auto.infra.process_supervisor.start_output_pump_thread"
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
            "cwma.auto.infra.process_supervisor.poll_process_exit",
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
            "cwma.auto.infra.process_supervisor.poll_process_exit",
            return_value=ProcessPollResult(exited=True, code=None),
        ) as poll_mock:
            result = poll_channel_exit(process=proc)  # type: ignore[arg-type]

        poll_mock.assert_called_once_with(proc)
        self.assertTrue(result.exited)
        self.assertEqual(result.code, 0)
        self.assertIsNone(result.process)


if __name__ == "__main__":
    unittest.main()

