from __future__ import annotations

import argparse
import atexit
import json
import locale
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, TextIO

from ..scheduler.smart_scheduler import SmartSchedulerConfig, SmartSchedulerPolicy


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def parse_bool_value(name: str, raw: object) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        if raw in {0, 1}:
            return bool(raw)
        raise ValueError(f"{name} must be 0/1/true/false/yes/no/on/off, got: {raw}")
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{name} must be 0/1/true/false/yes/no/on/off, got: {raw}")


def parse_int_value(name: str, raw: object, minimum: int) -> int:
    if isinstance(raw, bool):
        raise ValueError(f"{name} must be an integer, got: {raw}")
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str):
        try:
            value = int(raw.strip())
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer, got: {raw}") from exc
    else:
        raise ValueError(f"{name} must be an integer, got: {raw}")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got: {value}")
    return value


def resolve_path(base_dir: Path, raw: str) -> Path:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p


def load_watch_config(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid watch config JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Watch config root must be an object: {path}")
    return payload


def pick_setting(
    env_name: str,
    config_data: dict[str, object],
    config_key: str,
    default: object,
) -> object:
    env_raw = os.getenv(env_name)
    if env_raw is not None and env_raw.strip() != "":
        return env_raw.strip()
    cfg_raw = config_data.get(config_key)
    if cfg_raw is not None and not (isinstance(cfg_raw, str) and cfg_raw.strip() == ""):
        return cfg_raw
    return default


@dataclass
class Settings:
    base_dir: Path
    watch_config_path: Path | None
    auth_dir: Path
    state_dir: Path
    config_path: Path | None
    maintain_db_path: Path
    upload_db_path: Path
    maintain_log_file: Path
    upload_log_file: Path
    maintain_interval_seconds: int
    watch_interval_seconds: int
    upload_stable_wait_seconds: int
    upload_batch_size: int
    smart_schedule_enabled: bool
    adaptive_upload_batching: bool
    upload_high_backlog_threshold: int
    upload_high_backlog_batch_size: int
    adaptive_maintain_batching: bool
    incremental_maintain_batch_size: int
    maintain_high_backlog_threshold: int
    maintain_high_backlog_batch_size: int
    incremental_maintain_min_interval_seconds: int
    incremental_maintain_full_guard_seconds: int
    deep_scan_interval_loops: int
    active_probe_interval_seconds: int
    active_upload_deep_scan_interval_seconds: int
    maintain_retry_count: int
    upload_retry_count: int
    command_retry_delay_seconds: int
    run_maintain_on_start: bool
    run_upload_on_start: bool
    run_maintain_after_upload: bool
    maintain_assume_yes: bool
    delete_uploaded_files_after_upload: bool
    inspect_zip_files: bool
    auto_extract_zip_json: bool
    delete_zip_after_extract: bool
    bandizip_path: str
    bandizip_timeout_seconds: int
    use_windows_zip_fallback: bool
    continue_on_command_failure: bool
    allow_multi_instance: bool
    run_once: bool


class AutoMaintainer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.cpa_script = self.settings.base_dir / "cpa_warden.py"
        self.last_uploaded_snapshot_file = self.settings.state_dir / "last_uploaded_snapshot.txt"
        self.current_snapshot_file = self.settings.state_dir / "current_snapshot.txt"
        self.stable_snapshot_file = self.settings.state_dir / "stable_snapshot.txt"
        self.instance_lock_file = self.settings.state_dir / "auto_maintain.lock"
        self.instance_lock_token: str | None = None
        self.instance_lock_handle: TextIO | None = None
        self.instance_started_at = datetime.now()
        self.shutdown_requested = False
        self.shutdown_reason: str | None = None
        self.upload_process: subprocess.Popen | None = None
        self.maintain_process: subprocess.Popen | None = None
        self._windows_console_handler = None
        self.deep_scan_counter = 0
        self.pending_upload_retry = False
        self.pending_source_changes_during_upload = False
        self.last_active_upload_deep_scan_at = 0.0
        self.last_json_count = 0
        self.last_zip_signature: tuple[str, ...] = tuple()
        self.pending_upload_snapshot: list[str] | None = None
        self.pending_upload_reason: str | None = None
        self.inflight_upload_snapshot: list[str] | None = None
        self.upload_attempt = 0
        self.maintain_attempt = 0
        self.upload_retry_due_at = 0.0
        self.maintain_retry_due_at = 0.0
        self.pending_maintain = False
        self.pending_maintain_reason: str | None = None
        self.pending_maintain_names: set[str] | None = None
        self.inflight_maintain_names: set[str] | None = None
        self.maintain_names_file = self.settings.state_dir / "maintain_names_scope.txt"
        self.upload_names_file = self.settings.state_dir / "upload_names_scope.txt"
        self.maintain_cmd_output_file = self.settings.state_dir / "maintain_command_output.log"
        self.upload_cmd_output_file = self.settings.state_dir / "upload_command_output.log"
        self.output_lock = threading.Lock()
        self.console_lock = threading.Lock()
        self.upload_progress_state: dict[str, int | str] = {"stage": "idle", "done": 0, "total": 0}
        self.maintain_progress_state: dict[str, int | str] = {"stage": "idle", "done": 0, "total": 0}
        self.last_progress_render_at = 0.0
        self.progress_render_interval_seconds = 0.4
        self.progress_render_heartbeat_seconds = 8.0
        self.last_progress_signature = ""
        self.panel_height = 8
        self.panel_title = "CPA Warden Auto Dashboard"
        self.panel_enabled = self._detect_panel_capability()
        self.panel_color_enabled = (
            self.panel_enabled
            and os.getenv("AUTO_MAINTAIN_PANEL_COLOR", "1").strip() not in {"0", "false", "False"}
        )
        self.panel_initialized = False
        self.upload_progress_regex = re.compile(r"上传进度:\s*(\d+)\s*/\s*(\d+)")
        self.upload_actual_total_regex = re.compile(r"需要实际上传数:\s*(\d+)")
        self.upload_candidate_total_regex = re.compile(r"上传候选文件数:\s*(\d+)")
        self.upload_success_regex = re.compile(r"上传成功:\s*(\d+)")
        self.maintain_progress_regex = re.compile(r"(探测|删除|禁用|启用)进度:\s*(\d+)\s*/\s*(\d+)")
        self.maintain_probe_candidates_regex = re.compile(r"开始并发探测.*candidates=(\d+)")
        self.maintain_filtered_accounts_regex = re.compile(r"符合过滤条件账号数:\s*(\d+)")
        self.maintain_pending_delete_regex = re.compile(r"待删除 401 账号:\s*(\d+)")
        self.maintain_pending_disable_regex = re.compile(r"待禁用限额账号:\s*(\d+)")
        self.maintain_pending_enable_regex = re.compile(r"待恢复启用账号:\s*(\d+)")
        self.upload_output_thread: threading.Thread | None = None
        self.maintain_output_thread: threading.Thread | None = None
        self.zip_extract_processed_signatures: dict[str, str] = {}
        self.last_incremental_maintain_started_at = 0.0
        self.last_incremental_defer_reason: str | None = None
        self.next_maintain_due_at: float | None = None
        self.scheduler_policy = SmartSchedulerPolicy(
            SmartSchedulerConfig(
                enabled=self.settings.smart_schedule_enabled,
                adaptive_upload_batching=self.settings.adaptive_upload_batching,
                base_upload_batch_size=self.settings.upload_batch_size,
                upload_high_backlog_threshold=self.settings.upload_high_backlog_threshold,
                upload_high_backlog_batch_size=self.settings.upload_high_backlog_batch_size,
                adaptive_maintain_batching=self.settings.adaptive_maintain_batching,
                base_incremental_maintain_batch_size=self.settings.incremental_maintain_batch_size,
                maintain_high_backlog_threshold=self.settings.maintain_high_backlog_threshold,
                maintain_high_backlog_batch_size=self.settings.maintain_high_backlog_batch_size,
                incremental_maintain_min_interval_seconds=(
                    self.settings.incremental_maintain_min_interval_seconds
                ),
                incremental_maintain_full_guard_seconds=(
                    self.settings.incremental_maintain_full_guard_seconds
                ),
            )
        )

    def ensure_paths(self) -> None:
        if not self.cpa_script.exists():
            raise RuntimeError(f"cpa_warden.py not found: {self.cpa_script}")

        self.settings.auth_dir.mkdir(parents=True, exist_ok=True)
        if not self.settings.auth_dir.is_dir():
            raise RuntimeError(f"AUTH_DIR is not a directory: {self.settings.auth_dir}")

        self.settings.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.settings.state_dir.is_dir():
            raise RuntimeError(f"STATE_DIR is not a directory: {self.settings.state_dir}")

        self.settings.maintain_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.upload_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.maintain_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.settings.upload_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.maintain_cmd_output_file.parent.mkdir(parents=True, exist_ok=True)
        self.upload_cmd_output_file.parent.mkdir(parents=True, exist_ok=True)

    def run(self) -> int:
        self.ensure_paths()
        self.register_shutdown_handlers()
        self.acquire_instance_lock()
        atexit.register(self.release_instance_lock)
        self.set_console_title()
        self.init_fixed_panel_area()
        self.log_settings()
        try:
            initial_snapshot = self.build_snapshot(self.current_snapshot_file)
            if not self.last_uploaded_snapshot_file.exists():
                self.write_snapshot(self.last_uploaded_snapshot_file, initial_snapshot)

            self.last_json_count = self.get_json_count()
            if self.settings.inspect_zip_files:
                self.last_zip_signature = self.get_zip_signature()

            if self.settings.inspect_zip_files:
                startup_zip_changed = self.inspect_zip_archives()
                self.last_json_count = self.get_json_count()
                self.last_zip_signature = self.get_zip_signature()
                if startup_zip_changed:
                    log("Startup ZIP scan produced changes. Queue immediate upload check.")
                    startup_zip_upload_exit = self.check_and_maybe_upload(force_deep_scan=True)
                    if startup_zip_upload_exit != 0 and not self.handle_failure(
                        "startup zip-upload-check",
                        startup_zip_upload_exit,
                    ):
                        return startup_zip_upload_exit

            if self.settings.run_maintain_on_start:
                self.queue_maintain("startup maintain")

            if self.settings.run_upload_on_start:
                upload_check_exit = self.check_and_maybe_upload()
                if upload_check_exit != 0 and not self.handle_failure("startup upload-check", upload_check_exit):
                    return upload_check_exit

            now = time.monotonic()
            self.next_maintain_due_at = now + self.settings.maintain_interval_seconds

            start_maintain_exit = self.maybe_start_maintain()
            if start_maintain_exit != 0 and not self.handle_failure("startup maintain command", start_maintain_exit):
                return start_maintain_exit

            start_upload_exit = self.maybe_start_upload()
            if start_upload_exit != 0 and not self.handle_failure("startup upload command", start_upload_exit):
                return start_upload_exit
            self.render_progress_snapshot(force=True)

            while True:
                now = time.monotonic()

                while self.next_maintain_due_at is not None and self.next_maintain_due_at <= now:
                    self.queue_maintain("scheduled maintain")
                    self.next_maintain_due_at += self.settings.maintain_interval_seconds

                upload_done_exit = self.poll_upload_process()
                if upload_done_exit != 0 and not self.handle_failure("upload command", upload_done_exit):
                    return upload_done_exit

                maintain_done_exit = self.poll_maintain_process()
                if maintain_done_exit != 0 and not self.handle_failure("maintain command", maintain_done_exit):
                    return maintain_done_exit

                if self.upload_process is not None:
                    probe_exit = self.probe_changes_during_active_upload()
                    if probe_exit != 0 and not self.handle_failure("active-upload source probe", probe_exit):
                        return probe_exit

                # Build new upload batch only when current batch is not running.
                if self.upload_process is None and self.pending_upload_snapshot is None:
                    upload_check_exit = self.check_and_maybe_upload()
                    if upload_check_exit != 0 and not self.handle_failure("watch upload-check", upload_check_exit):
                        return upload_check_exit

                start_upload_exit = self.maybe_start_upload()
                if start_upload_exit != 0 and not self.handle_failure("watch upload command", start_upload_exit):
                    return start_upload_exit

                start_maintain_exit = self.maybe_start_maintain()
                if start_maintain_exit != 0 and not self.handle_failure("watch maintain command", start_maintain_exit):
                    return start_maintain_exit

                self.render_progress_snapshot()

                if self.settings.run_once:
                    nothing_running = self.upload_process is None and self.maintain_process is None
                    nothing_pending = (
                        self.pending_upload_snapshot is None
                        and not self.pending_maintain
                        and (not self.pending_upload_retry)
                    )
                    if not (nothing_running and nothing_pending):
                        if not self.sleep_with_shutdown(1):
                            return 130
                        continue
                    log("Run-once cycle finished.")
                    return 0

                if not self.sleep_with_shutdown(self.current_loop_sleep_seconds()):
                    return 130
        finally:
            self.release_instance_lock()

    def log_settings(self) -> None:
        log("Started auto maintenance loop.")
        log(f"WATCH_CONFIG_PATH={self.settings.watch_config_path or '(none)'}")
        log(f"AUTH_DIR={self.settings.auth_dir}")
        log(f"STATE_DIR={self.settings.state_dir}")
        log(f"MAINTAIN_DB_PATH={self.settings.maintain_db_path}")
        log(f"UPLOAD_DB_PATH={self.settings.upload_db_path}")
        log(f"MAINTAIN_LOG_FILE={self.settings.maintain_log_file}")
        log(f"UPLOAD_LOG_FILE={self.settings.upload_log_file}")
        log(f"MAINTAIN_COMMAND_OUTPUT_FILE={self.maintain_cmd_output_file}")
        log(f"UPLOAD_COMMAND_OUTPUT_FILE={self.upload_cmd_output_file}")
        log(f"MAINTAIN_NAMES_SCOPE_FILE={self.maintain_names_file}")
        log(f"UPLOAD_NAMES_SCOPE_FILE={self.upload_names_file}")
        log(f"MAINTAIN_INTERVAL_SECONDS={self.settings.maintain_interval_seconds}")
        log(f"WATCH_INTERVAL_SECONDS={self.settings.watch_interval_seconds}")
        log(f"UPLOAD_STABLE_WAIT_SECONDS={self.settings.upload_stable_wait_seconds}")
        log(f"UPLOAD_BATCH_SIZE={self.settings.upload_batch_size}")
        log(f"SMART_SCHEDULE_ENABLED={int(self.settings.smart_schedule_enabled)}")
        log(f"ADAPTIVE_UPLOAD_BATCHING={int(self.settings.adaptive_upload_batching)}")
        log(f"UPLOAD_HIGH_BACKLOG_THRESHOLD={self.settings.upload_high_backlog_threshold}")
        log(f"UPLOAD_HIGH_BACKLOG_BATCH_SIZE={self.settings.upload_high_backlog_batch_size}")
        log(f"ADAPTIVE_MAINTAIN_BATCHING={int(self.settings.adaptive_maintain_batching)}")
        log(f"INCREMENTAL_MAINTAIN_BATCH_SIZE={self.settings.incremental_maintain_batch_size}")
        log(f"MAINTAIN_HIGH_BACKLOG_THRESHOLD={self.settings.maintain_high_backlog_threshold}")
        log(f"MAINTAIN_HIGH_BACKLOG_BATCH_SIZE={self.settings.maintain_high_backlog_batch_size}")
        log(
            "INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS="
            f"{self.settings.incremental_maintain_min_interval_seconds}"
        )
        log(
            "INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS="
            f"{self.settings.incremental_maintain_full_guard_seconds}"
        )
        log(f"RUN_MAINTAIN_ON_START={int(self.settings.run_maintain_on_start)}")
        log(f"RUN_UPLOAD_ON_START={int(self.settings.run_upload_on_start)}")
        log(f"RUN_MAINTAIN_AFTER_UPLOAD={int(self.settings.run_maintain_after_upload)}")
        log(f"MAINTAIN_ASSUME_YES={int(self.settings.maintain_assume_yes)}")
        log(f"DELETE_UPLOADED_FILES_AFTER_UPLOAD={int(self.settings.delete_uploaded_files_after_upload)}")
        log(f"INSPECT_ZIP_FILES={int(self.settings.inspect_zip_files)}")
        log(f"AUTO_EXTRACT_ZIP_JSON={int(self.settings.auto_extract_zip_json)}")
        log(f"DELETE_ZIP_AFTER_EXTRACT={int(self.settings.delete_zip_after_extract)}")
        log(f"BANDIZIP_PATH={self.settings.bandizip_path}")
        log(f"BANDIZIP_TIMEOUT_SECONDS={self.settings.bandizip_timeout_seconds}")
        log(f"USE_WINDOWS_ZIP_FALLBACK={int(self.settings.use_windows_zip_fallback)}")
        log(f"DEEP_SCAN_INTERVAL_LOOPS={self.settings.deep_scan_interval_loops}")
        log(f"ACTIVE_PROBE_INTERVAL_SECONDS={self.settings.active_probe_interval_seconds}")
        log(
            "ACTIVE_UPLOAD_DEEP_SCAN_INTERVAL_SECONDS="
            f"{self.settings.active_upload_deep_scan_interval_seconds}"
        )
        log(f"MAINTAIN_RETRY_COUNT={self.settings.maintain_retry_count}")
        log(f"UPLOAD_RETRY_COUNT={self.settings.upload_retry_count}")
        log(f"COMMAND_RETRY_DELAY_SECONDS={self.settings.command_retry_delay_seconds}")
        log(f"CONTINUE_ON_COMMAND_FAILURE={int(self.settings.continue_on_command_failure)}")
        log(f"ALLOW_MULTI_INSTANCE={int(self.settings.allow_multi_instance)}")
        log(f"INSTANCE_LABEL={self.instance_label()}")
        log(f"INSTANCE_LOCK_FILE={self.instance_lock_file}")
        if self.instance_lock_token:
            log(f"INSTANCE_LOCK_TOKEN={self.instance_lock_token}")

    def instance_label(self) -> str:
        started = self.instance_started_at.strftime("%Y-%m-%d %H:%M:%S")
        return f"cpa-auto-maintain | {started} | {os.getpid()}"

    def set_console_title(self) -> None:
        if os.name != "nt":
            return
        try:
            import ctypes

            ctypes.windll.kernel32.SetConsoleTitleW(self.instance_label())
        except Exception as exc:
            log(f"[WARN] Failed to set console title: {exc}")

    def register_shutdown_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._signal_handler)

        if os.name == "nt":
            try:
                import ctypes

                CTRL_C_EVENT = 0
                CTRL_BREAK_EVENT = 1
                CTRL_CLOSE_EVENT = 2
                CTRL_LOGOFF_EVENT = 5
                CTRL_SHUTDOWN_EVENT = 6

                event_names = {
                    CTRL_C_EVENT: "CTRL_C_EVENT",
                    CTRL_BREAK_EVENT: "CTRL_BREAK_EVENT",
                    CTRL_CLOSE_EVENT: "CTRL_CLOSE_EVENT",
                    CTRL_LOGOFF_EVENT: "CTRL_LOGOFF_EVENT",
                    CTRL_SHUTDOWN_EVENT: "CTRL_SHUTDOWN_EVENT",
                }

                handler_type = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)

                def _handler(event_type: int) -> bool:
                    reason = event_names.get(event_type, f"WIN_EVENT_{event_type}")
                    self.request_shutdown(reason)
                    return True

                self._windows_console_handler = handler_type(_handler)
                ctypes.windll.kernel32.SetConsoleCtrlHandler(self._windows_console_handler, True)
            except Exception as exc:
                log(f"[WARN] Failed to register Windows console handler: {exc}")

    def _signal_handler(self, signum: int, _frame: object) -> None:
        self.request_shutdown(f"SIGNAL_{signum}")

    def request_shutdown(self, reason: str) -> None:
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.shutdown_reason = reason
        log(f"Shutdown requested: {reason}")
        self.terminate_active_processes()

    def terminate_process(self, proc: subprocess.Popen | None, *, name: str) -> None:
        if proc is None:
            return
        if proc.poll() is not None:
            return
        try:
            log(f"Terminating active {name} process (pid={proc.pid})...")
            proc.terminate()
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log(f"[WARN] Force-killed {name} process but wait timed out (pid={proc.pid}).")
        except Exception as exc:
            log(f"[WARN] Failed to terminate {name} process (pid={proc.pid}): {exc}")

    def terminate_active_processes(self) -> None:
        self.terminate_process(self.upload_process, name="upload")
        self.terminate_process(self.maintain_process, name="maintain")

    def sleep_with_shutdown(self, total_seconds: int) -> bool:
        if total_seconds <= 0:
            return not self.shutdown_requested
        deadline = time.time() + total_seconds
        while time.time() < deadline:
            if self.shutdown_requested:
                return False
            remaining = deadline - time.time()
            time.sleep(min(0.5, max(0.05, remaining)))
        return not self.shutdown_requested

    def current_loop_sleep_seconds(self) -> int:
        if self.upload_process is None and self.maintain_process is None:
            return self.settings.watch_interval_seconds
        return min(self.settings.watch_interval_seconds, self.settings.active_probe_interval_seconds)

    def wait_for_stable_snapshot(self, baseline_snapshot: list[str]) -> tuple[int, list[str] | None]:
        stable_seconds = self.settings.upload_stable_wait_seconds
        if stable_seconds <= 0:
            return 0, baseline_snapshot

        poll_seconds = min(2.0, max(0.5, stable_seconds / 4.0))
        last_change_at = time.time()
        stable_snapshot = baseline_snapshot

        while True:
            elapsed = time.time() - last_change_at
            remaining = stable_seconds - elapsed
            if remaining <= 0:
                return 0, stable_snapshot

            if not self.sleep_with_shutdown(min(poll_seconds, remaining)):
                return 130, None

            latest_snapshot = self.build_snapshot(self.stable_snapshot_file)
            if latest_snapshot != stable_snapshot:
                stable_snapshot = latest_snapshot
                last_change_at = time.time()
                log(
                    "Detected additional JSON changes during stability wait. "
                    f"Reset timer to {stable_seconds}s."
                )

    def is_pid_running(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def read_lock_pid(self) -> int | None:
        if not self.instance_lock_file.exists():
            return None
        try:
            raw = self.instance_lock_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if not raw:
            return None
        pid_text = raw.split("|", 1)[0].strip()
        if not pid_text.isdigit():
            return None
        return int(pid_text)

    def acquire_instance_lock(self) -> None:
        if self.settings.allow_multi_instance:
            return
        if os.name == "nt":
            self.acquire_instance_lock_windows()
            return

        for _ in range(2):
            token = str(uuid.uuid4())
            payload = f"{os.getpid()}|{token}|{int(time.time())}"
            try:
                fd = os.open(self.instance_lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as fp:
                    fp.write(payload)
                self.instance_lock_token = token
                return
            except FileExistsError:
                pid = self.read_lock_pid()
                if pid is not None and not self.is_pid_running(pid):
                    try:
                        self.instance_lock_file.unlink(missing_ok=True)
                    except OSError as exc:
                        log(f"[WARN] Failed to remove stale lock file {self.instance_lock_file}: {exc}")
                    continue
                detail = f"pid={pid}" if pid is not None else "unknown pid"
                raise RuntimeError(f"Another auto_maintain instance is already running ({detail}).")
        raise RuntimeError("Unable to acquire instance lock.")

    def acquire_instance_lock_windows(self) -> None:
        try:
            import msvcrt
        except Exception as exc:
            raise RuntimeError("Windows lock backend unavailable (msvcrt).") from exc

        token = str(uuid.uuid4())
        payload = f"{os.getpid()}|{token}|{int(time.time())}"
        handle: TextIO | None = None
        try:
            self.settings.state_dir.mkdir(parents=True, exist_ok=True)
            handle = self.instance_lock_file.open("a+", encoding="utf-8")
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write("\n")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            handle.seek(0)
            handle.truncate()
            handle.write(payload)
            handle.flush()
            self.instance_lock_token = token
            self.instance_lock_handle = handle
            return
        except OSError as exc:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
            pid = self.read_lock_pid()
            detail = f"pid={pid}" if pid is not None else "unknown pid"
            raise RuntimeError(f"Another auto_maintain instance is already running ({detail}).") from exc
        except Exception:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
            raise

    def release_instance_lock(self) -> None:
        if self.settings.allow_multi_instance:
            return
        if os.name == "nt" and self.instance_lock_handle is not None:
            try:
                import msvcrt

                self.instance_lock_handle.seek(0)
                msvcrt.locking(self.instance_lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError as exc:
                log(f"[WARN] Failed to unlock lock file {self.instance_lock_file}: {exc}")
            finally:
                try:
                    self.instance_lock_handle.close()
                except OSError:
                    pass
                self.instance_lock_handle = None
                self.instance_lock_token = None
            try:
                self.instance_lock_file.unlink(missing_ok=True)
            except OSError as exc:
                log(f"[WARN] Failed to remove lock file {self.instance_lock_file}: {exc}")
            return

        if self.instance_lock_token is None:
            return
        try:
            if not self.instance_lock_file.exists():
                return
            raw = self.instance_lock_file.read_text(encoding="utf-8").strip()
            parts = raw.split("|")
            if len(parts) >= 2 and parts[1] == self.instance_lock_token:
                self.instance_lock_file.unlink(missing_ok=True)
        except OSError as exc:
            log(f"[WARN] Failed to release lock file {self.instance_lock_file}: {exc}")
        finally:
            self.instance_lock_token = None

    def get_json_paths(self) -> list[Path]:
        return sorted(
            (p for p in self.settings.auth_dir.rglob("*.json") if p.is_file()),
            key=lambda p: str(p).lower(),
        )

    def get_json_count(self) -> int:
        return len(self.get_json_paths())

    def get_zip_paths(self) -> list[Path]:
        return sorted(
            (p for p in self.settings.auth_dir.rglob("*.zip") if p.is_file()),
            key=lambda p: str(p).lower(),
        )

    def get_zip_signature(self) -> tuple[str, ...]:
        signature: list[str] = []
        skipped = 0
        for path in self.get_zip_paths():
            try:
                stat = path.stat()
            except (FileNotFoundError, OSError):
                skipped += 1
                continue
            signature.append(f"{path}|{stat.st_size}|{stat.st_mtime_ns}")
        signature.sort()
        if skipped > 0:
            log(f"[WARN] ZIP signature skipped transient files: {skipped}")
        return tuple(signature)

    def get_zip_count(self) -> int:
        return len(self.get_zip_paths())

    def zip_json_entries(self, archive: zipfile.ZipFile) -> list[str]:
        json_entries: list[str] = []
        for info in archive.infolist():
            if info.is_dir():
                continue
            # ZipInfo names include nested paths, so this naturally supports recursive folder layouts in zip.
            if Path(info.filename).suffix.lower() == ".json":
                json_entries.append(info.filename)
        return json_entries

    def inspect_zip_archives(self) -> bool:
        if not self.settings.inspect_zip_files:
            return False

        zip_paths = self.get_zip_paths()
        if not zip_paths:
            return False

        current_zip_keys = {str(path).lower() for path in zip_paths}
        stale_keys = [
            key
            for key in self.zip_extract_processed_signatures
            if key not in current_zip_keys
        ]
        for stale_key in stale_keys:
            self.zip_extract_processed_signatures.pop(stale_key, None)

        with_json = 0
        without_json = 0
        invalid = 0
        total_json_entries = 0
        extracted = 0
        extract_failed = 0
        deleted_zip = 0
        skipped_already_processed = 0
        any_changed = False
        detail_lines: list[str] = []

        for path in zip_paths:
            path_key = str(path).lower()
            try:
                stat = path.stat()
                archive_signature = f"{stat.st_size}|{stat.st_mtime_ns}"
            except (FileNotFoundError, OSError):
                continue

            try:
                with zipfile.ZipFile(path, "r") as zf:
                    json_entries = self.zip_json_entries(zf)
            except (zipfile.BadZipFile, OSError, RuntimeError):
                invalid += 1
                self.zip_extract_processed_signatures.pop(path_key, None)
                if len(detail_lines) < 10:
                    detail_lines.append(f"{path.name}: invalid zip")
                continue

            json_count = len(json_entries)
            total_json_entries += json_count
            if json_count > 0:
                with_json += 1
                if self.settings.auto_extract_zip_json:
                    already_processed = (
                        (not self.settings.delete_zip_after_extract)
                        and self.zip_extract_processed_signatures.get(path_key) == archive_signature
                    )
                    if already_processed:
                        skipped_already_processed += 1
                        if len(detail_lines) < 10:
                            detail_lines.append(f"{path.name}: already processed")
                    else:
                        extract_exit = self.extract_zip_with_bandizip(path, self.settings.auth_dir)
                        if extract_exit == 0:
                            extracted += 1
                            any_changed = True
                            if self.settings.delete_zip_after_extract:
                                try:
                                    path.unlink()
                                    deleted_zip += 1
                                    any_changed = True
                                    self.zip_extract_processed_signatures.pop(path_key, None)
                                except OSError:
                                    if len(detail_lines) < 10:
                                        detail_lines.append(f"{path.name}: extracted but failed to delete zip")
                            else:
                                self.zip_extract_processed_signatures[path_key] = archive_signature
                        else:
                            extract_failed += 1
                            self.zip_extract_processed_signatures.pop(path_key, None)
                if len(detail_lines) < 10:
                    detail_lines.append(f"{path.name}: json_entries={json_count}")
            else:
                without_json += 1
                self.zip_extract_processed_signatures.pop(path_key, None)
                if len(detail_lines) < 10:
                    detail_lines.append(f"{path.name}: no json")

        log(
            "ZIP scan summary: "
            f"total_zip={len(zip_paths)}, with_json={with_json}, "
            f"without_json={without_json}, invalid_zip={invalid}, "
            f"total_json_entries={total_json_entries}, extracted={extracted}, "
            f"extract_failed={extract_failed}, deleted_zip={deleted_zip}, "
            f"skipped_already_processed={skipped_already_processed}"
        )
        for line in detail_lines:
            log(f"ZIP detail: {line}")
        if with_json > 0:
            if self.settings.auto_extract_zip_json:
                log("ZIP note: JSON zip archives were processed via Bandizip.")
            else:
                log("ZIP note: uploader only handles extracted .json files in upload directory.")
        return any_changed

    def _ps_quote(self, value: str) -> str:
        return value.replace("'", "''")

    def extract_zip_with_windows_builtin(self, zip_path: Path, output_dir: Path) -> int:
        shell_candidates = ["powershell.exe", "pwsh.exe", "powershell", "pwsh"]
        shell_path = None
        for candidate in shell_candidates:
            resolved = shutil.which(candidate)
            if resolved:
                shell_path = resolved
                break
        if not shell_path:
            log(f"Windows built-in unzip shell not found for {zip_path.name}.")
            return 1

        cmd_script = (
            "$ErrorActionPreference='Stop';"
            f"Expand-Archive -LiteralPath '{self._ps_quote(str(zip_path))}' "
            f"-DestinationPath '{self._ps_quote(str(output_dir))}' -Force"
        )
        cmd = [shell_path, "-NoProfile", "-Command", cmd_script]
        try:
            completed = subprocess.run(
                cmd,
                cwd=str(self.settings.base_dir),
                timeout=self.settings.bandizip_timeout_seconds,
                capture_output=True,
                text=True,
            )
        except subprocess.TimeoutExpired:
            log(f"Windows unzip timeout: {zip_path.name}")
            return 1
        except OSError:
            return 1

        if completed.returncode == 0:
            log(f"Windows built-in extracted: {zip_path.name}")
            return 0

        stderr_text = (completed.stderr or "").strip()
        stdout_text = (completed.stdout or "").strip()
        if stderr_text:
            log(f"Windows unzip stderr ({zip_path.name}): {stderr_text[:200]}")
        elif stdout_text:
            log(f"Windows unzip output ({zip_path.name}): {stdout_text[:200]}")
        return 1

    def extract_zip_with_bandizip(self, zip_path: Path, output_dir: Path) -> int:
        bandizip_candidates: list[str] = []
        configured = (self.settings.bandizip_path or "").strip()
        if configured:
            bandizip_candidates.append(configured)
        if configured.lower() != "bandizip.exe":
            bandizip_candidates.append("Bandizip.exe")
        if configured.lower() != "bz.exe":
            bandizip_candidates.append("bz.exe")

        tried_commands: list[str] = []
        for candidate in bandizip_candidates:
            resolved = shutil.which(candidate) if not Path(candidate).exists() else candidate
            if not resolved:
                continue

            commands = [
                [resolved, "x", "-y", f"-o:{output_dir}", str(zip_path)],
                [resolved, "x", "-y", "-o", str(output_dir), str(zip_path)],
            ]
            for cmd in commands:
                tried_commands.append(" ".join(cmd))
                try:
                    completed = subprocess.run(
                        cmd,
                        cwd=str(self.settings.base_dir),
                        timeout=self.settings.bandizip_timeout_seconds,
                        capture_output=True,
                        text=True,
                    )
                except subprocess.TimeoutExpired:
                    log(f"Bandizip extract timeout: {zip_path.name}")
                    continue
                except OSError:
                    continue

                if completed.returncode == 0:
                    log(f"Bandizip extracted: {zip_path.name}")
                    return 0

                stderr_text = (completed.stderr or "").strip()
                stdout_text = (completed.stdout or "").strip()
                if stderr_text:
                    log(f"Bandizip stderr ({zip_path.name}): {stderr_text[:200]}")
                elif stdout_text:
                    log(f"Bandizip output ({zip_path.name}): {stdout_text[:200]}")

        if tried_commands:
            log(f"Bandizip extract failed: {zip_path.name}")
        else:
            log(
                f"Bandizip not found for {zip_path.name}. "
                "Set BANDIZIP_PATH or ensure Bandizip.exe/bz.exe is in PATH."
            )

        if self.settings.use_windows_zip_fallback:
            log(f"Trying Windows built-in unzip fallback: {zip_path.name}")
            return self.extract_zip_with_windows_builtin(zip_path, output_dir)

        return 1

    def snapshot_lines(self) -> list[str]:
        lines: list[str] = []
        skipped = 0
        for path in self.get_json_paths():
            try:
                stat = path.stat()
            except (FileNotFoundError, OSError):
                skipped += 1
                continue
            lines.append(f"{path}|{stat.st_size}|{stat.st_mtime_ns}")
        lines.sort()
        if skipped > 0:
            log(f"[WARN] Snapshot skipped transient files: {skipped}")
        return lines

    def write_snapshot(self, target: Path, lines: Iterable[str]) -> None:
        target.write_text("\n".join(lines), encoding="utf-8")

    def read_snapshot(self, source: Path) -> list[str]:
        if not source.exists():
            return []
        text = source.read_text(encoding="utf-8")
        if not text:
            return []
        return text.splitlines()

    def build_snapshot(self, target: Path) -> list[str]:
        lines = self.snapshot_lines()
        self.write_snapshot(target, lines)
        return lines

    def compute_uploaded_baseline(
        self,
        existing_baseline: list[str],
        uploaded_snapshot: list[str],
        current_snapshot: list[str],
    ) -> list[str]:
        current_set = set(current_snapshot)
        if not current_set:
            return []

        merged = set(existing_baseline).intersection(current_set)
        if uploaded_snapshot:
            merged.update(set(uploaded_snapshot).intersection(current_set))
        return sorted(merged)

    def compute_pending_upload_snapshot(
        self,
        current_snapshot: list[str],
        uploaded_baseline: list[str],
    ) -> list[str]:
        if not current_snapshot:
            return []
        uploaded_set = set(uploaded_baseline)
        return [row for row in current_snapshot if row not in uploaded_set]

    def extract_names_from_snapshot(self, snapshot_lines: list[str]) -> set[str]:
        names: set[str] = set()
        for row in snapshot_lines:
            parts = row.rsplit("|", 2)
            if len(parts) != 3:
                continue
            file_name = Path(parts[0]).name.strip()
            if file_name:
                names.add(file_name)
        return names

    def write_maintain_names_scope(self, names: set[str]) -> Path:
        sorted_names = sorted(name for name in names if name.strip())
        self.maintain_names_file.parent.mkdir(parents=True, exist_ok=True)
        self.maintain_names_file.write_text("\n".join(sorted_names), encoding="utf-8")
        return self.maintain_names_file

    def write_upload_names_scope(self, names: set[str]) -> Path:
        sorted_names = sorted(name for name in names if name.strip())
        self.upload_names_file.parent.mkdir(parents=True, exist_ok=True)
        self.upload_names_file.write_text("\n".join(sorted_names), encoding="utf-8")
        return self.upload_names_file

    def delete_uploaded_files_from_snapshot(self, snapshot_lines: list[str]) -> None:
        deleted = 0
        skipped_changed = 0
        skipped_missing = 0
        failed = 0

        for row in snapshot_lines:
            parts = row.rsplit("|", 2)
            if len(parts) != 3:
                continue
            path_text, size_text, mtime_text = parts
            path = Path(path_text)
            try:
                expected_size = int(size_text)
                expected_mtime_ns = int(mtime_text)
            except ValueError:
                continue

            if not path.exists():
                skipped_missing += 1
                continue

            try:
                stat = path.stat()
            except OSError:
                failed += 1
                continue

            if stat.st_size != expected_size or stat.st_mtime_ns != expected_mtime_ns:
                skipped_changed += 1
                continue

            try:
                path.unlink()
                deleted += 1
            except OSError:
                failed += 1

        log(
            "Upload cleanup summary: "
            f"deleted={deleted}, skipped_changed={skipped_changed}, "
            f"skipped_missing={skipped_missing}, failed={failed}"
        )
        self.prune_empty_dirs_under_auth_dir()

    def prune_empty_dirs_under_auth_dir(self) -> None:
        removed = 0
        skipped_non_empty = 0
        skipped_missing = 0
        failed = 0

        dirs = sorted(
            (p for p in self.settings.auth_dir.rglob("*") if p.is_dir()),
            key=lambda p: len(p.parts),
            reverse=True,
        )
        for path in dirs:
            if path == self.settings.auth_dir:
                continue
            try:
                path.rmdir()
                removed += 1
            except OSError:
                if not path.exists():
                    skipped_missing += 1
                    continue
                try:
                    next(path.iterdir())
                    skipped_non_empty += 1
                except StopIteration:
                    failed += 1
                except OSError:
                    failed += 1

        log(
            "Upload empty-dir cleanup summary: "
            f"removed={removed}, skipped_non_empty={skipped_non_empty}, "
            f"skipped_missing={skipped_missing}, failed={failed}"
        )

    def command_base(self) -> list[str]:
        cmd = [sys.executable, str(self.cpa_script)]
        if self.settings.config_path is not None:
            cmd.extend(["--config", str(self.settings.config_path)])
        return cmd

    def update_channel_progress(
        self,
        name: str,
        *,
        stage: str | None = None,
        done: int | None = None,
        total: int | None = None,
        force_render: bool = False,
    ) -> None:
        with self.output_lock:
            state = self.upload_progress_state if name == "upload" else self.maintain_progress_state
            if stage is not None:
                state["stage"] = stage
            if done is not None:
                state["done"] = max(0, int(done))
            if total is not None:
                state["total"] = max(0, int(total))
        self.render_progress_snapshot(force=force_render)

    def _format_bar(self, done: int, total: int, width: int = 18) -> str:
        if total <= 0:
            return "[" + ("-" * width) + "]"
        ratio = min(1.0, max(0.0, float(done) / float(total)))
        filled = int(round(ratio * width))
        return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

    def _seconds_until(self, due_at: float | None, now: float) -> int:
        if due_at is None:
            return 0
        return max(0, int(due_at - now))

    def _compute_upload_queue_batches(self, pending_count: int) -> tuple[int, int]:
        if pending_count <= 0:
            return 0, 0
        maintain_pressure = self.maintain_process is not None or self.pending_maintain
        next_batch_size = self.scheduler_policy.choose_upload_batch_size(
            pending_count=pending_count,
            maintain_pressure=maintain_pressure,
        )
        if next_batch_size <= 0:
            return 0, 0
        queue_batches = (pending_count + next_batch_size - 1) // next_batch_size
        return next_batch_size, queue_batches

    def _short_reason(self, reason: str, limit: int = 36) -> str:
        text = (reason or "-").strip()
        if len(text) <= limit:
            return text
        return text[: max(1, limit - 3)] + "..."

    def _fit_panel_line(self, text: str) -> str:
        width = shutil.get_terminal_size((160, 20)).columns
        if width <= 0:
            return text
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[: width - 3] + "..."

    def _panel_border_line(self, char: str = "=") -> str:
        width = max(40, shutil.get_terminal_size((120, 20)).columns)
        # Keep a small margin so borders don't hit the edge in narrow terminals.
        inner = max(10, width - 4)
        return f"+{char * inner}+"

    def _color_text(self, text: str, code: str) -> str:
        if not self.panel_color_enabled:
            return text
        return f"\x1b[{code}m{text}\x1b[0m"

    def _state_color_code(self, state: str) -> str:
        mapping = {
            "running": "32",   # green
            "pending": "33",   # yellow
            "idle": "37",      # gray/white
            "failed": "31",    # red
        }
        return mapping.get(state, "36")

    def _apply_panel_colors(
        self,
        lines: list[str],
        *,
        upload_state: str,
        maintain_state: str,
        upload_stage: str,
        maintain_stage: str,
    ) -> list[str]:
        if not self.panel_color_enabled:
            return lines

        colored = list(lines)
        if len(colored) >= 1:
            colored[0] = self._color_text(colored[0], "90")
        if len(colored) >= 2:
            colored[1] = self._color_text(colored[1], "1;36")
        if len(colored) >= 3:
            line = colored[2]
            line = line.replace("UPLOAD", self._color_text("UPLOAD", "1;34"), 1)
            line = line.replace(f"state={upload_state}", f"state={self._color_text(upload_state, self._state_color_code(upload_state))}", 1)
            line = line.replace(f"stage={upload_stage}", f"stage={self._color_text(upload_stage, '36')}", 1)
            colored[2] = line
        if len(colored) >= 4:
            colored[3] = self._color_text(colored[3], "90")
        if len(colored) >= 5:
            colored[4] = self._color_text(colored[4], "90")
        if len(colored) >= 6:
            line = colored[5]
            line = line.replace("MAINTAIN", self._color_text("MAINTAIN", "1;35"), 1)
            line = line.replace(f"state={maintain_state}", f"state={self._color_text(maintain_state, self._state_color_code(maintain_state))}", 1)
            line = line.replace(f"stage={maintain_stage}", f"stage={self._color_text(maintain_stage, '36')}", 1)
            colored[5] = line
        if len(colored) >= 7:
            colored[6] = self._color_text(colored[6], "90")
        if len(colored) >= 8:
            colored[7] = self._color_text(colored[7], "90")
        return colored

    def _preferred_decoding_order(self) -> list[str]:
        order = ["utf-8", "utf-8-sig", "gb18030", "cp936"]
        preferred = locale.getpreferredencoding(False).strip()
        if preferred and preferred.lower() not in {item.lower() for item in order}:
            order.append(preferred)
        return order

    def decode_child_output_line(self, raw: bytes | str) -> str:
        if isinstance(raw, str):
            return raw
        if not raw:
            return ""
        for enc in self._preferred_decoding_order():
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                continue
        return raw.decode("utf-8", errors="replace")

    def build_child_process_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.setdefault("PYTHONUTF8", "1")
        env.setdefault("PYTHONIOENCODING", "utf-8")
        return env

    def should_log_child_alert_line(self, text: str) -> bool:
        lower = text.lower()
        if "timeout=" in lower:
            return False
        if "| error |" in lower or "| warn |" in lower or "| warning |" in lower:
            return True
        if "[error]" in lower or "[warn]" in lower:
            return True
        if "traceback" in lower or "failed" in lower or "timed out" in lower:
            return True
        if " 错误" in text or "失败" in text or "超时" in text:
            return True
        return False

    def render_progress_snapshot(self, *, force: bool = False) -> None:
        now = time.monotonic()
        with self.output_lock:
            if (not force) and (now - self.last_progress_render_at < self.progress_render_interval_seconds):
                return
            upload_stage = str(self.upload_progress_state.get("stage") or "idle")
            upload_done = int(self.upload_progress_state.get("done") or 0)
            upload_total = int(self.upload_progress_state.get("total") or 0)
            maintain_stage = str(self.maintain_progress_state.get("stage") or "idle")
            maintain_done = int(self.maintain_progress_state.get("done") or 0)
            maintain_total = int(self.maintain_progress_state.get("total") or 0)
            pending_upload = len(self.pending_upload_snapshot or [])
            inflight_upload = len(self.inflight_upload_snapshot or [])
            upload_reason = self.pending_upload_reason or "-"
            upload_state = (
                "running"
                if self.upload_process is not None
                else ("pending" if pending_upload > 0 else "idle")
            )
            upload_retry_wait = self._seconds_until(self.upload_retry_due_at, now)
            next_batch_size, queue_batches = self._compute_upload_queue_batches(pending_upload)

            pending_full = self.pending_maintain and self.pending_maintain_names is None
            pending_incremental = 0 if pending_full else len(self.pending_maintain_names or set())
            maintain_next_batch = 0
            if pending_incremental > 0:
                maintain_next_batch = self.scheduler_policy.choose_incremental_maintain_batch_size(
                    pending_count=pending_incremental,
                    upload_pressure=(self.upload_process is not None) or bool(self.pending_upload_snapshot),
                )
            maintain_state = (
                "running"
                if self.maintain_process is not None
                else ("pending" if self.pending_maintain else "idle")
            )
            maintain_reason = self.pending_maintain_reason or "-"
            maintain_inflight_scope = (
                "full"
                if self.inflight_maintain_names is None and self.maintain_process is not None
                else str(len(self.inflight_maintain_names or set()))
            )
            maintain_retry_wait = self._seconds_until(self.maintain_retry_due_at, now)
            next_full_wait = self._seconds_until(self.next_maintain_due_at, now)
            maintain_defer_reason = self.last_incremental_defer_reason or "-"

            upload_bar = self._format_bar(upload_done, upload_total)
            maintain_bar = self._format_bar(maintain_done, maintain_total)
            upload_reason_text = self._short_reason(upload_reason, limit=40)
            maintain_reason_text = self._short_reason(maintain_reason, limit=40)
            maintain_defer_text = self._short_reason(maintain_defer_reason, limit=28)
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            panel_mode = "fixed" if self.panel_enabled else "log"
            panel_lines_plain = [
                self._fit_panel_line(
                    self._panel_border_line("=")
                ),
                self._fit_panel_line(
                    f"{self.panel_title} | {now_text} | panel={panel_mode} | watch={self.settings.watch_interval_seconds}s"
                ),
                self._fit_panel_line(
                    f"UPLOAD   {upload_bar} {upload_done}/{upload_total} state={upload_state} stage={upload_stage}"
                ),
                self._fit_panel_line(
                    "         "
                    f"queue_files={pending_upload} queue_batches={queue_batches} next_batch={next_batch_size} "
                    f"inflight={inflight_upload} retry={upload_retry_wait}s reason={upload_reason_text}"
                ),
                self._fit_panel_line(
                    self._panel_border_line("-")
                ),
                self._fit_panel_line(
                    f"MAINTAIN {maintain_bar} {maintain_done}/{maintain_total} state={maintain_state} stage={maintain_stage}"
                ),
                self._fit_panel_line(
                    "         "
                    f"queue_full={int(pending_full)} queue_incremental={pending_incremental} "
                    f"next_batch={maintain_next_batch} "
                    f"inflight_scope={maintain_inflight_scope} retry={maintain_retry_wait}s "
                    f"next_full={next_full_wait}s defer={maintain_defer_text} reason={maintain_reason_text}"
                ),
                self._fit_panel_line(
                    self._panel_border_line("=")
                ),
            ]
            signature = "\n".join(panel_lines_plain)
            signature_unchanged = signature == self.last_progress_signature
            if (
                (not force)
                and signature_unchanged
                and (now - self.last_progress_render_at < self.progress_render_heartbeat_seconds)
            ):
                return
            self.last_progress_render_at = now
            self.last_progress_signature = signature

        panel_lines = self._apply_panel_colors(
            panel_lines_plain,
            upload_state=upload_state,
            maintain_state=maintain_state,
            upload_stage=upload_stage,
            maintain_stage=maintain_stage,
        )
        self.render_fixed_panel(panel_lines)

    def parse_child_progress_line(self, name: str, line: str) -> None:
        text = line.strip()
        if not text:
            return

        if name == "upload":
            if "开始上传认证文件" in text:
                self.update_channel_progress("upload", stage="scan", done=0, total=0, force_render=True)
                return
            match = self.upload_progress_regex.search(text)
            if match:
                done = int(match.group(1))
                total = int(match.group(2))
                self.update_channel_progress("upload", stage="upload", done=done, total=total)
                return
            actual_total = self.upload_actual_total_regex.search(text)
            if actual_total:
                total = int(actual_total.group(1))
                self.update_channel_progress("upload", stage="upload", done=0, total=total, force_render=True)
                return
            candidate_total = self.upload_candidate_total_regex.search(text)
            if candidate_total:
                total = int(candidate_total.group(1))
                self.update_channel_progress("upload", stage="scan", done=0, total=total)
                return
            uploaded_count = self.upload_success_regex.search(text)
            if uploaded_count:
                done = int(uploaded_count.group(1))
                total = int(self.upload_progress_state.get("total") or 0)
                if total > 0:
                    self.update_channel_progress("upload", stage="upload", done=done, total=total)
                else:
                    self.update_channel_progress("upload", stage="upload", done=done)
                return
            if "上传流程完成" in text:
                self.update_channel_progress("upload", stage="done", force_render=True)
                return
        else:
            if "开始维护:" in text:
                self.update_channel_progress("maintain", stage="prepare", done=0, total=0, force_render=True)
                return
            probe_match = self.maintain_probe_candidates_regex.search(text)
            if probe_match:
                total = int(probe_match.group(1))
                self.update_channel_progress("maintain", stage="probe", done=0, total=total, force_render=True)
                return
            filtered_accounts = self.maintain_filtered_accounts_regex.search(text)
            if filtered_accounts:
                total = int(filtered_accounts.group(1))
                self.update_channel_progress("maintain", stage="probe", done=total, total=total)
                return

            maintain_match = self.maintain_progress_regex.search(text)
            if maintain_match:
                stage_map = {
                    "探测": "probe",
                    "删除": "delete",
                    "禁用": "disable",
                    "启用": "enable",
                }
                stage_cn = maintain_match.group(1)
                done = int(maintain_match.group(2))
                total = int(maintain_match.group(3))
                self.update_channel_progress(
                    "maintain",
                    stage=stage_map.get(stage_cn, stage_cn),
                    done=done,
                    total=total,
                )
                return

            pending_delete = self.maintain_pending_delete_regex.search(text)
            if pending_delete:
                total = int(pending_delete.group(1))
                self.update_channel_progress("maintain", stage="delete", done=0, total=total, force_render=True)
                return

            pending_disable = self.maintain_pending_disable_regex.search(text)
            if pending_disable:
                total = int(pending_disable.group(1))
                self.update_channel_progress("maintain", stage="disable", done=0, total=total, force_render=True)
                return

            pending_enable = self.maintain_pending_enable_regex.search(text)
            if pending_enable:
                total = int(pending_enable.group(1))
                self.update_channel_progress("maintain", stage="enable", done=0, total=total, force_render=True)
                return
            if "维护完成" in text:
                self.update_channel_progress("maintain", stage="done", force_render=True)
                return

        if self.should_log_child_alert_line(text):
            log(f"[{name}] {text}")

    def _write_child_output_line(self, name: str, line: str) -> None:
        target = self.upload_cmd_output_file if name == "upload" else self.maintain_cmd_output_file
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("a", encoding="utf-8", errors="replace") as fp:
                fp.write(line)
                if not line.endswith("\n"):
                    fp.write("\n")
        except OSError:
            pass

    def start_output_pump(self, name: str, proc: subprocess.Popen) -> None:
        stream = getattr(proc, "stdout", None)
        if stream is None:
            return

        def _pump() -> None:
            try:
                for raw_line in iter(stream.readline, b""):
                    line = self.decode_child_output_line(raw_line)
                    self._write_child_output_line(name, line)
                    self.parse_child_progress_line(name, line)
            except Exception as exc:
                log(f"[WARN] output pump failed ({name}): {exc}")
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

        thread = threading.Thread(target=_pump, name=f"{name}-output-pump", daemon=True)
        thread.start()
        if name == "upload":
            self.upload_output_thread = thread
        else:
            self.maintain_output_thread = thread

    def _detect_panel_capability(self) -> bool:
        if not sys.stdout.isatty():
            return False
        if os.getenv("AUTO_MAINTAIN_FIXED_PANEL", "1").strip() in {"0", "false", "False"}:
            return False
        if os.name != "nt":
            return True
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            if handle in (0, -1):
                return False

            mode = ctypes.c_uint()
            if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                return False

            enable_vt = 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
            if mode.value & enable_vt:
                return True
            if not kernel32.SetConsoleMode(handle, mode.value | enable_vt):
                return False
            return True
        except Exception:
            return False

    def init_fixed_panel_area(self) -> None:
        if (not self.panel_enabled) or self.panel_initialized:
            return
        with self.console_lock:
            if self.panel_initialized:
                return
            sys.stdout.write("\n" * self.panel_height)
            sys.stdout.flush()
            self.panel_initialized = True

    def render_fixed_panel(self, lines: list[str]) -> None:
        if not self.panel_enabled:
            for line in lines:
                log(line)
            return

        self.init_fixed_panel_area()
        with self.console_lock:
            # Save cursor, jump to top-left, overwrite reserved panel rows, then restore cursor.
            sys.stdout.write("\x1b[s")
            sys.stdout.write("\x1b[H")
            for idx in range(self.panel_height):
                row_text = lines[idx] if idx < len(lines) else ""
                sys.stdout.write("\x1b[2K")
                sys.stdout.write(row_text)
                if idx < self.panel_height - 1:
                    sys.stdout.write("\n")
            sys.stdout.write("\x1b[u")
            sys.stdout.flush()

    def build_maintain_command(self, maintain_names_file: Path | None = None) -> list[str]:
        cmd = self.command_base() + [
            "--mode",
            "maintain",
            "--db-path",
            str(self.settings.maintain_db_path),
            "--log-file",
            str(self.settings.maintain_log_file),
        ]
        if maintain_names_file is not None:
            cmd.extend(["--maintain-names-file", str(maintain_names_file)])
        if self.settings.maintain_assume_yes:
            cmd.append("--yes")
        return cmd

    def build_upload_command(self, upload_names_file: Path | None = None) -> list[str]:
        cmd = self.command_base() + [
            "--mode",
            "upload",
            "--upload-dir",
            str(self.settings.auth_dir),
            "--upload-recursive",
            "--db-path",
            str(self.settings.upload_db_path),
            "--log-file",
            str(self.settings.upload_log_file),
        ]
        if upload_names_file is not None:
            cmd.extend(["--upload-names-file", str(upload_names_file)])
        return cmd

    def queue_maintain(self, reason: str, names: set[str] | None = None) -> None:
        if names is None:
            self.pending_maintain = True
            self.pending_maintain_reason = reason
            self.pending_maintain_names = None
            self.update_channel_progress("maintain", stage="pending_full", force_render=True)
            return

        clean_names = {name.strip() for name in names if name and name.strip()}
        if not clean_names:
            return

        if self.pending_maintain and self.pending_maintain_names is None:
            return

        self.pending_maintain = True
        self.pending_maintain_reason = reason
        if self.pending_maintain_names is None:
            self.pending_maintain_names = set(clean_names)
        else:
            self.pending_maintain_names.update(clean_names)
        self.update_channel_progress("maintain", stage="pending_incremental", force_render=True)

    def merge_pending_incremental_maintain_names(self, names: set[str]) -> None:
        if not names:
            return
        if self.pending_maintain and self.pending_maintain_names is None:
            # Full maintain already queued, incremental queue is redundant.
            return
        self.pending_maintain = True
        if self.pending_maintain_names is None:
            self.pending_maintain_names = set(names)
        else:
            self.pending_maintain_names.update(names)

    def maybe_start_maintain(self) -> int:
        if self.maintain_process is not None:
            return 0
        if not self.pending_maintain:
            return 0
        now = time.monotonic()
        if now < self.maintain_retry_due_at:
            return 0
        if self.pending_maintain_names is not None:
            defer_incremental, defer_reason = self.scheduler_policy.should_defer_incremental_maintain(
                now_monotonic=now,
                last_incremental_started_at=self.last_incremental_maintain_started_at,
                next_full_maintain_due_at=self.next_maintain_due_at,
                has_pending_full_maintain=(
                    self.pending_maintain and self.pending_maintain_names is None
                ),
            )
            if defer_incremental:
                if defer_reason != self.last_incremental_defer_reason:
                    log(f"Deferred incremental maintain start: {defer_reason}.")
                    self.last_incremental_defer_reason = defer_reason
                    self.update_channel_progress("maintain", stage="deferred", force_render=True)
                return 0
        self.last_incremental_defer_reason = None
        self.maintain_attempt += 1
        max_attempts = self.settings.maintain_retry_count + 1
        reason = self.pending_maintain_reason or "unspecified"
        maintain_scope_file: Path | None = None
        maintain_scope_names: set[str] | None = None
        remaining_incremental_names: set[str] = set()
        if self.pending_maintain_names is not None:
            pending_names = sorted(self.pending_maintain_names)
            batch_size = self.scheduler_policy.choose_incremental_maintain_batch_size(
                pending_count=len(pending_names),
                upload_pressure=(self.upload_process is not None) or bool(self.pending_upload_snapshot),
            )
            selected_names = pending_names[:batch_size]
            maintain_scope_names = set(selected_names)
            remaining_incremental_names = set(pending_names[batch_size:])
            if not maintain_scope_names:
                self.pending_maintain = False
                self.pending_maintain_reason = None
                self.pending_maintain_names = None
                self.maintain_attempt = 0
                self.maintain_retry_due_at = 0.0
                log("Skipped maintain start: incremental scope is empty.")
                return 0
            maintain_scope_file = self.write_maintain_names_scope(maintain_scope_names)
        cmd = self.build_maintain_command(maintain_scope_file)
        scope_label = (
            f"incremental names={len(maintain_scope_names or set())}"
            if maintain_scope_names is not None
            else "full"
        )
        log(
            f"Starting maintain command attempt {self.maintain_attempt} of {max_attempts} "
            f"(reason={reason}, scope={scope_label})."
        )
        if maintain_scope_names is not None:
            self.last_incremental_maintain_started_at = now
        self.inflight_maintain_names = maintain_scope_names
        if maintain_scope_names is None:
            self.pending_maintain = False
            self.pending_maintain_reason = None
            self.pending_maintain_names = None
        else:
            self.pending_maintain = bool(remaining_incremental_names)
            self.pending_maintain_reason = "queued incremental maintain" if remaining_incremental_names else None
            self.pending_maintain_names = remaining_incremental_names if remaining_incremental_names else None
        try:
            self.maintain_process = subprocess.Popen(
                cmd,
                cwd=str(self.settings.base_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=False,
                env=self.build_child_process_env(),
            )
            self.start_output_pump("maintain", self.maintain_process)
            self.update_channel_progress("maintain", stage="running", force_render=True)
        except Exception as exc:
            return self.handle_command_start_error("maintain", exc)
        return 0

    def maybe_start_upload(self) -> int:
        if self.upload_process is not None:
            return 0
        if self.pending_upload_snapshot is None:
            return 0
        if not self.pending_upload_snapshot:
            self.pending_upload_snapshot = None
            self.pending_upload_reason = None
            self.pending_upload_retry = False
            self.inflight_upload_snapshot = None
            self.upload_attempt = 0
            self.upload_retry_due_at = 0.0
            return 0
        now = time.monotonic()
        if now < self.upload_retry_due_at:
            return 0

        upload_batch: list[str]
        if self.pending_upload_retry and self.inflight_upload_snapshot is not None:
            upload_batch = list(self.inflight_upload_snapshot)
        else:
            pending_total = len(self.pending_upload_snapshot)
            maintain_pressure = self.maintain_process is not None or self.pending_maintain
            batch_size = self.scheduler_policy.choose_upload_batch_size(
                pending_count=pending_total,
                maintain_pressure=maintain_pressure,
            )
            upload_batch = list(self.pending_upload_snapshot[:batch_size])
            self.inflight_upload_snapshot = list(upload_batch)

        if not upload_batch:
            self.pending_upload_snapshot = None
            self.pending_upload_reason = None
            self.pending_upload_retry = False
            self.inflight_upload_snapshot = None
            self.upload_attempt = 0
            self.upload_retry_due_at = 0.0
            return 0

        upload_scope_names = self.extract_names_from_snapshot(upload_batch)
        upload_scope_file: Path | None = None
        if upload_scope_names:
            upload_scope_file = self.write_upload_names_scope(upload_scope_names)

        self.upload_attempt += 1
        max_attempts = self.settings.upload_retry_count + 1
        reason = self.pending_upload_reason or "detected changes"
        cmd = self.build_upload_command(upload_scope_file)
        log(
            f"Starting upload command attempt {self.upload_attempt} of {max_attempts} "
            f"(reason={reason}, batch_size={len(upload_batch)}, pending_total={len(self.pending_upload_snapshot)})."
        )
        self.inflight_upload_snapshot = list(upload_batch)
        try:
            self.upload_process = subprocess.Popen(
                cmd,
                cwd=str(self.settings.base_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=False,
                env=self.build_child_process_env(),
            )
            self.start_output_pump("upload", self.upload_process)
            self.update_channel_progress("upload", stage="running", force_render=True)
        except Exception as exc:
            return self.handle_command_start_error("upload", exc)
        return 0

    def handle_command_start_error(self, name: str, exc: Exception) -> int:
        log(f"{name.capitalize()} command failed to start: {exc}")
        if name == "maintain":
            self.update_channel_progress("maintain", stage="start_failed", force_render=True)
            max_attempts = self.settings.maintain_retry_count + 1
            if self.maintain_attempt < max_attempts:
                if self.inflight_maintain_names is None:
                    self.pending_maintain = True
                    self.pending_maintain_reason = "maintain retry"
                    self.pending_maintain_names = None
                else:
                    self.merge_pending_incremental_maintain_names(set(self.inflight_maintain_names))
                    self.pending_maintain_reason = "maintain retry"
                self.maintain_retry_due_at = time.monotonic() + self.settings.command_retry_delay_seconds
                log(f"Will retry maintain in {self.settings.command_retry_delay_seconds}s.")
                self.inflight_maintain_names = None
                self.update_channel_progress("maintain", stage="retry_wait", force_render=True)
                return 0
            self.pending_maintain = False
            self.pending_maintain_reason = None
            self.pending_maintain_names = None
            self.inflight_maintain_names = None
            self.maintain_attempt = 0
            return 1

        max_attempts = self.settings.upload_retry_count + 1
        self.update_channel_progress("upload", stage="start_failed", force_render=True)
        if self.upload_attempt < max_attempts:
            self.upload_retry_due_at = time.monotonic() + self.settings.command_retry_delay_seconds
            self.pending_upload_retry = True
            log(f"Will retry upload in {self.settings.command_retry_delay_seconds}s.")
            self.update_channel_progress("upload", stage="retry_wait", force_render=True)
            return 0
        self.pending_upload_snapshot = None
        self.pending_upload_reason = None
        self.inflight_upload_snapshot = None
        self.pending_upload_retry = False
        self.upload_attempt = 0
        return 1

    def poll_maintain_process(self) -> int:
        proc = self.maintain_process
        if proc is None:
            return 0
        code = proc.poll()
        if code is None:
            return 0
        self.maintain_process = None
        if code == 0:
            log("Maintain command completed.")
            self.maintain_attempt = 0
            self.maintain_retry_due_at = 0.0
            self.inflight_maintain_names = None
            if self.pending_maintain:
                stage = "pending_full" if self.pending_maintain_names is None else "pending_incremental"
                self.update_channel_progress("maintain", stage=stage, done=0, total=0, force_render=True)
            else:
                self.update_channel_progress("maintain", stage="idle", done=0, total=0, force_render=True)
            return 0

        if self.shutdown_requested:
            return 130

        max_attempts = self.settings.maintain_retry_count + 1
        if self.maintain_attempt < max_attempts:
            if self.inflight_maintain_names is None:
                self.pending_maintain = True
                self.pending_maintain_reason = "maintain retry"
                self.pending_maintain_names = None
            else:
                self.merge_pending_incremental_maintain_names(set(self.inflight_maintain_names))
                self.pending_maintain_reason = "maintain retry"
            self.maintain_retry_due_at = time.monotonic() + self.settings.command_retry_delay_seconds
            log(
                f"Maintain command failed with exit {code}. "
                f"Retrying in {self.settings.command_retry_delay_seconds}s..."
            )
            self.inflight_maintain_names = None
            self.update_channel_progress("maintain", stage="retry_wait", force_render=True)
            return 0

        log(f"Maintain command failed after retries. Exit code {code}.")
        self.pending_maintain = False
        self.pending_maintain_reason = None
        self.pending_maintain_names = None
        self.inflight_maintain_names = None
        self.maintain_attempt = 0
        self.update_channel_progress("maintain", stage="failed", force_render=True)
        return code

    def probe_changes_during_active_upload(self) -> int:
        if self.upload_process is None:
            return 0

        current_json_count = self.get_json_count()
        json_changed = current_json_count != self.last_json_count
        current_zip_signature = self.last_zip_signature
        zip_changed = False
        if self.settings.inspect_zip_files:
            current_zip_signature = self.get_zip_signature()
            zip_changed = current_zip_signature != self.last_zip_signature

        if not (json_changed or zip_changed):
            return 0

        if not self.pending_source_changes_during_upload:
            reasons: list[str] = []
            if json_changed:
                reasons.append("json")
            if zip_changed:
                reasons.append("zip")
            log(
                "Detected source changes during active upload ("
                + ",".join(reasons)
                + "). Will trigger immediate deep upload check after batch completion."
            )
        self.pending_source_changes_during_upload = True
        self.last_json_count = current_json_count
        if self.settings.inspect_zip_files:
            self.last_zip_signature = current_zip_signature

        now = time.monotonic()
        if (
            self.settings.active_upload_deep_scan_interval_seconds > 0
            and (now - self.last_active_upload_deep_scan_at)
            < self.settings.active_upload_deep_scan_interval_seconds
        ):
            return 0

        self.last_active_upload_deep_scan_at = now
        log("Refreshing upload queue immediately during active upload.")
        return self.check_and_maybe_upload(
            force_deep_scan=True,
            preserve_retry_state=True,
            skip_stability_wait=True,
            queue_reason="active-upload source changes",
        )

    def poll_upload_process(self) -> int:
        proc = self.upload_process
        if proc is None:
            return 0
        code = proc.poll()
        if code is None:
            return 0
        self.upload_process = None

        if code == 0:
            log("Upload command completed.")
            uploaded_snapshot = self.inflight_upload_snapshot or []
            previous_uploaded_baseline = self.read_snapshot(self.last_uploaded_snapshot_file)
            self.pending_upload_retry = False
            self.inflight_upload_snapshot = None
            self.upload_attempt = 0
            self.upload_retry_due_at = 0.0

            if self.settings.delete_uploaded_files_after_upload:
                self.delete_uploaded_files_from_snapshot(uploaded_snapshot)

            current_snapshot = self.build_snapshot(self.current_snapshot_file)
            self.write_snapshot(self.stable_snapshot_file, current_snapshot)
            uploaded_baseline = self.compute_uploaded_baseline(
                previous_uploaded_baseline,
                uploaded_snapshot,
                current_snapshot,
            )
            self.write_snapshot(self.last_uploaded_snapshot_file, uploaded_baseline)
            self.last_json_count = len(current_snapshot)
            if self.settings.inspect_zip_files:
                self.last_zip_signature = self.get_zip_signature()

            pending_snapshot = self.compute_pending_upload_snapshot(current_snapshot, uploaded_baseline)
            if pending_snapshot:
                self.pending_upload_snapshot = pending_snapshot
                self.pending_upload_reason = "post-upload pending files"
                log(
                    "Detected files outside uploaded baseline after upload. "
                    f"Queued next upload batch ({len(pending_snapshot)} pending)."
                )
                self.update_channel_progress("upload", stage="pending", force_render=True)
            else:
                self.pending_upload_snapshot = None
                self.pending_upload_reason = None
                self.update_channel_progress("upload", stage="idle", done=0, total=0, force_render=True)

            if self.pending_source_changes_during_upload:
                self.pending_source_changes_during_upload = False
                log(
                    "Running immediate deep upload check after active-upload source changes."
                )
                follow_up_exit = self.check_and_maybe_upload(force_deep_scan=True)
                if follow_up_exit != 0:
                    return follow_up_exit

            if self.settings.run_maintain_after_upload:
                uploaded_names = self.extract_names_from_snapshot(uploaded_snapshot)
                if uploaded_names:
                    self.queue_maintain("post-upload maintain", names=uploaded_names)
                else:
                    log("Skipped post-upload maintain: no uploaded names detected.")
            return 0

        if self.shutdown_requested:
            return 130

        max_attempts = self.settings.upload_retry_count + 1
        if self.upload_attempt < max_attempts:
            self.pending_upload_retry = True
            self.upload_retry_due_at = time.monotonic() + self.settings.command_retry_delay_seconds
            log(
                f"Upload command failed with exit {code}. "
                f"Retrying in {self.settings.command_retry_delay_seconds}s..."
            )
            self.update_channel_progress("upload", stage="retry_wait", force_render=True)
            return 0

        log(f"Upload command failed after retries. Exit code {code}.")
        self.pending_upload_snapshot = None
        self.pending_upload_reason = None
        self.inflight_upload_snapshot = None
        self.pending_upload_retry = False
        self.pending_source_changes_during_upload = False
        self.upload_attempt = 0
        self.update_channel_progress("upload", stage="failed", force_render=True)
        return code

    def handle_failure(self, stage: str, code: int) -> bool:
        log(f"{stage} failed with exit {code}.")
        if self.settings.run_once:
            log("Run-once mode enabled. Exiting on failure.")
            return False
        if self.settings.continue_on_command_failure:
            log("Continue mode enabled. Keep loop alive.")
            return True
        return False

    def check_and_maybe_upload(
        self,
        force_deep_scan: bool = False,
        *,
        preserve_retry_state: bool = False,
        skip_stability_wait: bool = False,
        queue_reason: str = "detected JSON changes",
    ) -> int:
        current_json_count = self.get_json_count()
        current_zip_signature = self.get_zip_signature() if self.settings.inspect_zip_files else tuple()
        need_deep_scan = False

        if force_deep_scan:
            need_deep_scan = True
        elif self.pending_upload_retry:
            need_deep_scan = True
        elif current_json_count != self.last_json_count:
            need_deep_scan = True
        elif self.settings.inspect_zip_files and current_zip_signature != self.last_zip_signature:
            need_deep_scan = True
        else:
            self.deep_scan_counter += 1
            if self.deep_scan_counter >= self.settings.deep_scan_interval_loops:
                need_deep_scan = True

        if not need_deep_scan:
            return 0

        self.deep_scan_counter = 0
        if self.settings.inspect_zip_files:
            zip_changed = self.inspect_zip_archives()
            if zip_changed:
                current_json_count = self.get_json_count()
                current_zip_signature = self.get_zip_signature()
        current_snapshot = self.build_snapshot(self.current_snapshot_file)
        last_uploaded_snapshot = self.read_snapshot(self.last_uploaded_snapshot_file)
        if current_snapshot == last_uploaded_snapshot:
            self.write_snapshot(self.stable_snapshot_file, current_snapshot)
            self.last_json_count = current_json_count
            if self.settings.inspect_zip_files:
                self.last_zip_signature = current_zip_signature
            if not preserve_retry_state:
                self.pending_upload_retry = False
            return 0

        stable_snapshot = current_snapshot
        if not skip_stability_wait:
            log(f"Detected JSON changes. Waiting {self.settings.upload_stable_wait_seconds}s for stability...")
            stable_wait_exit, stable_snapshot = self.wait_for_stable_snapshot(current_snapshot)
            if stable_wait_exit != 0:
                return stable_wait_exit
            if stable_snapshot is None:
                return 130

        pending_snapshot = self.compute_pending_upload_snapshot(stable_snapshot, last_uploaded_snapshot)
        if not pending_snapshot:
            if self.pending_upload_snapshot is None:
                self.pending_upload_reason = None
            if not preserve_retry_state:
                self.pending_upload_retry = False
                self.upload_attempt = 0
                self.upload_retry_due_at = 0.0
            self.last_json_count = len(stable_snapshot)
            if self.settings.inspect_zip_files:
                self.last_zip_signature = current_zip_signature
            self.write_snapshot(self.stable_snapshot_file, stable_snapshot)
            if self.pending_upload_snapshot is None:
                self.update_channel_progress("upload", stage="idle", done=0, total=0, force_render=True)
            return 0

        existing_pending = set(self.pending_upload_snapshot or [])
        merged_pending = sorted(existing_pending.union(pending_snapshot))
        self.pending_upload_snapshot = merged_pending
        self.pending_upload_reason = queue_reason
        if not self.pending_upload_retry and not preserve_retry_state:
            self.upload_attempt = 0
            self.upload_retry_due_at = 0.0
        log(f"Upload batch queued. pending={len(merged_pending)}")
        self.update_channel_progress("upload", stage="pending", force_render=True)

        return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto maintain + folder watch uploader for cpa-warden.",
    )
    parser.add_argument(
        "--watch-config",
        help="Watcher config JSON path (env WATCH_CONFIG_PATH also supported).",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    return parser.parse_args()


def load_settings(args: argparse.Namespace) -> Settings:
    base_dir = Path(__file__).resolve().parent
    watch_config_data: dict[str, object] = {}
    watch_config_path: Path | None = None

    watch_config_raw = (args.watch_config or os.getenv("WATCH_CONFIG_PATH", "")).strip()
    if watch_config_raw:
        watch_config_path = resolve_path(base_dir, watch_config_raw)
        if not watch_config_path.exists():
            raise ValueError(f"WATCH_CONFIG_PATH not found: {watch_config_path}")
        if watch_config_path.is_dir():
            raise ValueError(f"WATCH_CONFIG_PATH must be a file path, not a directory: {watch_config_path}")
        watch_config_data = load_watch_config(watch_config_path)
    else:
        default_watch_config = base_dir / "auto_maintain.config.json"
        if default_watch_config.exists() and default_watch_config.is_file():
            watch_config_path = default_watch_config
            watch_config_data = load_watch_config(default_watch_config)

    auth_dir = resolve_path(
        base_dir,
        str(pick_setting("AUTH_DIR", watch_config_data, "auth_dir", str(base_dir / "auth_files"))),
    )
    state_dir = resolve_path(
        base_dir,
        str(pick_setting("STATE_DIR", watch_config_data, "state_dir", str(base_dir / ".auto_maintain_state"))),
    )

    maintain_db_path = resolve_path(
        base_dir,
        str(
            pick_setting(
                "MAINTAIN_DB_PATH",
                watch_config_data,
                "maintain_db_path",
                str(state_dir / "cpa_warden_maintain.sqlite3"),
            )
        ),
    )
    upload_db_path = resolve_path(
        base_dir,
        str(
            pick_setting(
                "UPLOAD_DB_PATH",
                watch_config_data,
                "upload_db_path",
                str(state_dir / "cpa_warden_upload.sqlite3"),
            )
        ),
    )
    maintain_log_file = resolve_path(
        base_dir,
        str(
            pick_setting(
                "MAINTAIN_LOG_FILE",
                watch_config_data,
                "maintain_log_file",
                str(state_dir / "cpa_warden_maintain.log"),
            )
        ),
    )
    upload_log_file = resolve_path(
        base_dir,
        str(
            pick_setting(
                "UPLOAD_LOG_FILE",
                watch_config_data,
                "upload_log_file",
                str(state_dir / "cpa_warden_upload.log"),
            )
        ),
    )

    config_raw = str(pick_setting("CONFIG_PATH", watch_config_data, "config_path", "")).strip()
    config_path: Path | None = None
    if config_raw:
        config_path = resolve_path(base_dir, config_raw)
        if config_path.exists() and config_path.is_dir():
            raise ValueError(f"CONFIG_PATH must be a file path, not a directory: {config_path}")
        if not config_path.exists():
            log(f"[WARN] CONFIG_PATH not found: {config_path}")
            log("[WARN] Fallback to default config resolution.")
            config_path = None

    return Settings(
        base_dir=base_dir,
        watch_config_path=watch_config_path,
        auth_dir=auth_dir,
        state_dir=state_dir,
        config_path=config_path,
        maintain_db_path=maintain_db_path,
        upload_db_path=upload_db_path,
        maintain_log_file=maintain_log_file,
        upload_log_file=upload_log_file,
        maintain_interval_seconds=parse_int_value(
            "MAINTAIN_INTERVAL_SECONDS",
            pick_setting("MAINTAIN_INTERVAL_SECONDS", watch_config_data, "maintain_interval_seconds", 3600),
            1,
        ),
        watch_interval_seconds=parse_int_value(
            "WATCH_INTERVAL_SECONDS",
            pick_setting("WATCH_INTERVAL_SECONDS", watch_config_data, "watch_interval_seconds", 15),
            1,
        ),
        upload_stable_wait_seconds=parse_int_value(
            "UPLOAD_STABLE_WAIT_SECONDS",
            pick_setting("UPLOAD_STABLE_WAIT_SECONDS", watch_config_data, "upload_stable_wait_seconds", 20),
            0,
        ),
        upload_batch_size=parse_int_value(
            "UPLOAD_BATCH_SIZE",
            pick_setting("UPLOAD_BATCH_SIZE", watch_config_data, "upload_batch_size", 100),
            1,
        ),
        smart_schedule_enabled=parse_bool_value(
            "SMART_SCHEDULE_ENABLED",
            pick_setting("SMART_SCHEDULE_ENABLED", watch_config_data, "smart_schedule_enabled", True),
        ),
        adaptive_upload_batching=parse_bool_value(
            "ADAPTIVE_UPLOAD_BATCHING",
            pick_setting("ADAPTIVE_UPLOAD_BATCHING", watch_config_data, "adaptive_upload_batching", True),
        ),
        upload_high_backlog_threshold=parse_int_value(
            "UPLOAD_HIGH_BACKLOG_THRESHOLD",
            pick_setting("UPLOAD_HIGH_BACKLOG_THRESHOLD", watch_config_data, "upload_high_backlog_threshold", 400),
            1,
        ),
        upload_high_backlog_batch_size=parse_int_value(
            "UPLOAD_HIGH_BACKLOG_BATCH_SIZE",
            pick_setting("UPLOAD_HIGH_BACKLOG_BATCH_SIZE", watch_config_data, "upload_high_backlog_batch_size", 300),
            1,
        ),
        adaptive_maintain_batching=parse_bool_value(
            "ADAPTIVE_MAINTAIN_BATCHING",
            pick_setting("ADAPTIVE_MAINTAIN_BATCHING", watch_config_data, "adaptive_maintain_batching", True),
        ),
        incremental_maintain_batch_size=parse_int_value(
            "INCREMENTAL_MAINTAIN_BATCH_SIZE",
            pick_setting(
                "INCREMENTAL_MAINTAIN_BATCH_SIZE",
                watch_config_data,
                "incremental_maintain_batch_size",
                120,
            ),
            1,
        ),
        maintain_high_backlog_threshold=parse_int_value(
            "MAINTAIN_HIGH_BACKLOG_THRESHOLD",
            pick_setting(
                "MAINTAIN_HIGH_BACKLOG_THRESHOLD",
                watch_config_data,
                "maintain_high_backlog_threshold",
                300,
            ),
            1,
        ),
        maintain_high_backlog_batch_size=parse_int_value(
            "MAINTAIN_HIGH_BACKLOG_BATCH_SIZE",
            pick_setting(
                "MAINTAIN_HIGH_BACKLOG_BATCH_SIZE",
                watch_config_data,
                "maintain_high_backlog_batch_size",
                220,
            ),
            1,
        ),
        incremental_maintain_min_interval_seconds=parse_int_value(
            "INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS",
            pick_setting(
                "INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS",
                watch_config_data,
                "incremental_maintain_min_interval_seconds",
                20,
            ),
            0,
        ),
        incremental_maintain_full_guard_seconds=parse_int_value(
            "INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS",
            pick_setting(
                "INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS",
                watch_config_data,
                "incremental_maintain_full_guard_seconds",
                90,
            ),
            0,
        ),
        deep_scan_interval_loops=parse_int_value(
            "DEEP_SCAN_INTERVAL_LOOPS",
            pick_setting("DEEP_SCAN_INTERVAL_LOOPS", watch_config_data, "deep_scan_interval_loops", 40),
            1,
        ),
        active_probe_interval_seconds=parse_int_value(
            "ACTIVE_PROBE_INTERVAL_SECONDS",
            pick_setting("ACTIVE_PROBE_INTERVAL_SECONDS", watch_config_data, "active_probe_interval_seconds", 2),
            1,
        ),
        active_upload_deep_scan_interval_seconds=parse_int_value(
            "ACTIVE_UPLOAD_DEEP_SCAN_INTERVAL_SECONDS",
            pick_setting(
                "ACTIVE_UPLOAD_DEEP_SCAN_INTERVAL_SECONDS",
                watch_config_data,
                "active_upload_deep_scan_interval_seconds",
                2,
            ),
            1,
        ),
        maintain_retry_count=parse_int_value(
            "MAINTAIN_RETRY_COUNT",
            pick_setting("MAINTAIN_RETRY_COUNT", watch_config_data, "maintain_retry_count", 1),
            0,
        ),
        upload_retry_count=parse_int_value(
            "UPLOAD_RETRY_COUNT",
            pick_setting("UPLOAD_RETRY_COUNT", watch_config_data, "upload_retry_count", 1),
            0,
        ),
        command_retry_delay_seconds=parse_int_value(
            "COMMAND_RETRY_DELAY_SECONDS",
            pick_setting("COMMAND_RETRY_DELAY_SECONDS", watch_config_data, "command_retry_delay_seconds", 20),
            1,
        ),
        run_maintain_on_start=parse_bool_value(
            "RUN_MAINTAIN_ON_START",
            pick_setting("RUN_MAINTAIN_ON_START", watch_config_data, "run_maintain_on_start", True),
        ),
        run_upload_on_start=parse_bool_value(
            "RUN_UPLOAD_ON_START",
            pick_setting("RUN_UPLOAD_ON_START", watch_config_data, "run_upload_on_start", True),
        ),
        run_maintain_after_upload=parse_bool_value(
            "RUN_MAINTAIN_AFTER_UPLOAD",
            pick_setting("RUN_MAINTAIN_AFTER_UPLOAD", watch_config_data, "run_maintain_after_upload", True),
        ),
        maintain_assume_yes=parse_bool_value(
            "MAINTAIN_ASSUME_YES",
            pick_setting("MAINTAIN_ASSUME_YES", watch_config_data, "maintain_assume_yes", False),
        ),
        delete_uploaded_files_after_upload=parse_bool_value(
            "DELETE_UPLOADED_FILES_AFTER_UPLOAD",
            pick_setting(
                "DELETE_UPLOADED_FILES_AFTER_UPLOAD",
                watch_config_data,
                "delete_uploaded_files_after_upload",
                True,
            ),
        ),
        inspect_zip_files=parse_bool_value(
            "INSPECT_ZIP_FILES",
            pick_setting("INSPECT_ZIP_FILES", watch_config_data, "inspect_zip_files", True),
        ),
        auto_extract_zip_json=parse_bool_value(
            "AUTO_EXTRACT_ZIP_JSON",
            pick_setting("AUTO_EXTRACT_ZIP_JSON", watch_config_data, "auto_extract_zip_json", True),
        ),
        delete_zip_after_extract=parse_bool_value(
            "DELETE_ZIP_AFTER_EXTRACT",
            pick_setting("DELETE_ZIP_AFTER_EXTRACT", watch_config_data, "delete_zip_after_extract", True),
        ),
        bandizip_path=str(
            pick_setting(
                "BANDIZIP_PATH",
                watch_config_data,
                "bandizip_path",
                r"C:\Program Files\Bandizip\Bandizip.exe",
            )
        ),
        bandizip_timeout_seconds=parse_int_value(
            "BANDIZIP_TIMEOUT_SECONDS",
            pick_setting("BANDIZIP_TIMEOUT_SECONDS", watch_config_data, "bandizip_timeout_seconds", 120),
            1,
        ),
        use_windows_zip_fallback=parse_bool_value(
            "USE_WINDOWS_ZIP_FALLBACK",
            pick_setting("USE_WINDOWS_ZIP_FALLBACK", watch_config_data, "use_windows_zip_fallback", True),
        ),
        continue_on_command_failure=parse_bool_value(
            "CONTINUE_ON_COMMAND_FAILURE",
            pick_setting(
                "CONTINUE_ON_COMMAND_FAILURE",
                watch_config_data,
                "continue_on_command_failure",
                False,
            ),
        ),
        allow_multi_instance=parse_bool_value(
            "ALLOW_MULTI_INSTANCE",
            pick_setting("ALLOW_MULTI_INSTANCE", watch_config_data, "allow_multi_instance", False),
        ),
        run_once=bool(args.once),
    )


def main() -> int:
    args = parse_args()
    try:
        settings = load_settings(args)
        maintainer = AutoMaintainer(settings)
        return maintainer.run()
    except KeyboardInterrupt:
        log("Interrupted by user.")
        return 130
    except Exception as exc:
        log(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
