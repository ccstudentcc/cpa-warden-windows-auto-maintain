from __future__ import annotations

import argparse
import atexit
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be 0/1/true/false/yes/no/on/off, got: {raw}")


def parse_int_env(name: str, default: int, minimum: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw.strip())
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer, got: {raw}") from exc
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got: {value}")
    return value


def resolve_path(base_dir: Path, raw: str) -> Path:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p


def parse_path_env(base_dir: Path, name: str, default: str) -> Path:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        raw = default
    return resolve_path(base_dir, raw)


@dataclass
class Settings:
    base_dir: Path
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
    deep_scan_interval_loops: int
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
        self.instance_started_at = datetime.now()
        self.shutdown_requested = False
        self.shutdown_reason: str | None = None
        self.upload_process: subprocess.Popen | None = None
        self.maintain_process: subprocess.Popen | None = None
        self._windows_console_handler = None
        self.deep_scan_counter = 0
        self.pending_upload_retry = False
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

    def run(self) -> int:
        self.ensure_paths()
        self.register_shutdown_handlers()
        self.acquire_instance_lock()
        atexit.register(self.release_instance_lock)
        self.set_console_title()
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
            next_maintain_due_at = now + self.settings.maintain_interval_seconds

            start_maintain_exit = self.maybe_start_maintain()
            if start_maintain_exit != 0 and not self.handle_failure("startup maintain command", start_maintain_exit):
                return start_maintain_exit

            start_upload_exit = self.maybe_start_upload()
            if start_upload_exit != 0 and not self.handle_failure("startup upload command", start_upload_exit):
                return start_upload_exit

            while True:
                now = time.monotonic()

                while next_maintain_due_at <= now:
                    self.queue_maintain("scheduled maintain")
                    next_maintain_due_at += self.settings.maintain_interval_seconds

                upload_done_exit = self.poll_upload_process()
                if upload_done_exit != 0 and not self.handle_failure("upload command", upload_done_exit):
                    return upload_done_exit

                maintain_done_exit = self.poll_maintain_process()
                if maintain_done_exit != 0 and not self.handle_failure("maintain command", maintain_done_exit):
                    return maintain_done_exit

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

                if not self.sleep_with_shutdown(self.settings.watch_interval_seconds):
                    return 130
        finally:
            self.release_instance_lock()

    def log_settings(self) -> None:
        log("Started auto maintenance loop.")
        log(f"AUTH_DIR={self.settings.auth_dir}")
        log(f"STATE_DIR={self.settings.state_dir}")
        log(f"MAINTAIN_DB_PATH={self.settings.maintain_db_path}")
        log(f"UPLOAD_DB_PATH={self.settings.upload_db_path}")
        log(f"MAINTAIN_LOG_FILE={self.settings.maintain_log_file}")
        log(f"UPLOAD_LOG_FILE={self.settings.upload_log_file}")
        log(f"MAINTAIN_INTERVAL_SECONDS={self.settings.maintain_interval_seconds}")
        log(f"WATCH_INTERVAL_SECONDS={self.settings.watch_interval_seconds}")
        log(f"UPLOAD_STABLE_WAIT_SECONDS={self.settings.upload_stable_wait_seconds}")
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
        raw = self.instance_lock_file.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        pid_text = raw.split("|", 1)[0].strip()
        if not pid_text.isdigit():
            return None
        return int(pid_text)

    def acquire_instance_lock(self) -> None:
        if self.settings.allow_multi_instance:
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

    def release_instance_lock(self) -> None:
        if self.settings.allow_multi_instance:
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

    def inspect_zip_archives(self) -> bool:
        if not self.settings.inspect_zip_files:
            return False

        zip_paths = self.get_zip_paths()
        if not zip_paths:
            return False

        with_json = 0
        without_json = 0
        invalid = 0
        total_json_entries = 0
        extracted = 0
        extract_failed = 0
        deleted_zip = 0
        any_changed = False
        detail_lines: list[str] = []

        for path in zip_paths:
            try:
                with zipfile.ZipFile(path, "r") as zf:
                    json_entries = [
                        info.filename
                        for info in zf.infolist()
                        if (not info.is_dir()) and info.filename.lower().endswith(".json")
                    ]
            except (zipfile.BadZipFile, OSError, RuntimeError):
                invalid += 1
                if len(detail_lines) < 10:
                    detail_lines.append(f"{path.name}: invalid zip")
                continue

            json_count = len(json_entries)
            total_json_entries += json_count
            if json_count > 0:
                with_json += 1
                if self.settings.auto_extract_zip_json:
                    extract_exit = self.extract_zip_with_bandizip(path, self.settings.auth_dir)
                    if extract_exit == 0:
                        extracted += 1
                        any_changed = True
                        if self.settings.delete_zip_after_extract:
                            try:
                                path.unlink()
                                deleted_zip += 1
                                any_changed = True
                            except OSError:
                                if len(detail_lines) < 10:
                                    detail_lines.append(f"{path.name}: extracted but failed to delete zip")
                    else:
                        extract_failed += 1
                if len(detail_lines) < 10:
                    detail_lines.append(f"{path.name}: json_entries={json_count}")
            else:
                without_json += 1
                if len(detail_lines) < 10:
                    detail_lines.append(f"{path.name}: no json")

        log(
            "ZIP scan summary: "
            f"total_zip={len(zip_paths)}, with_json={with_json}, "
            f"without_json={without_json}, invalid_zip={invalid}, "
            f"total_json_entries={total_json_entries}, extracted={extracted}, "
            f"extract_failed={extract_failed}, deleted_zip={deleted_zip}"
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
        uploaded_snapshot: list[str],
        current_snapshot: list[str],
    ) -> list[str]:
        uploaded_set = set(uploaded_snapshot)
        if not uploaded_set:
            return []
        current_set = set(current_snapshot)
        return sorted(uploaded_set.intersection(current_set))

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

    def build_maintain_command(self) -> list[str]:
        cmd = self.command_base() + [
            "--mode",
            "maintain",
            "--db-path",
            str(self.settings.maintain_db_path),
            "--log-file",
            str(self.settings.maintain_log_file),
        ]
        if self.settings.maintain_assume_yes:
            cmd.append("--yes")
        return cmd

    def build_upload_command(self) -> list[str]:
        return self.command_base() + [
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

    def queue_maintain(self, reason: str) -> None:
        self.pending_maintain = True
        self.pending_maintain_reason = reason

    def maybe_start_maintain(self) -> int:
        if self.maintain_process is not None:
            return 0
        if not self.pending_maintain:
            return 0
        now = time.monotonic()
        if now < self.maintain_retry_due_at:
            return 0
        self.maintain_attempt += 1
        max_attempts = self.settings.maintain_retry_count + 1
        reason = self.pending_maintain_reason or "unspecified"
        cmd = self.build_maintain_command()
        log(
            f"Starting maintain command attempt {self.maintain_attempt} of {max_attempts} "
            f"(reason={reason})."
        )
        self.pending_maintain = False
        self.pending_maintain_reason = None
        try:
            self.maintain_process = subprocess.Popen(cmd, cwd=str(self.settings.base_dir))
        except Exception as exc:
            return self.handle_command_start_error("maintain", exc)
        return 0

    def maybe_start_upload(self) -> int:
        if self.upload_process is not None:
            return 0
        if self.pending_upload_snapshot is None:
            return 0
        now = time.monotonic()
        if now < self.upload_retry_due_at:
            return 0
        self.upload_attempt += 1
        max_attempts = self.settings.upload_retry_count + 1
        reason = self.pending_upload_reason or "detected changes"
        cmd = self.build_upload_command()
        log(
            f"Starting upload command attempt {self.upload_attempt} of {max_attempts} "
            f"(reason={reason})."
        )
        self.inflight_upload_snapshot = list(self.pending_upload_snapshot)
        try:
            self.upload_process = subprocess.Popen(cmd, cwd=str(self.settings.base_dir))
        except Exception as exc:
            return self.handle_command_start_error("upload", exc)
        return 0

    def handle_command_start_error(self, name: str, exc: Exception) -> int:
        log(f"{name.capitalize()} command failed to start: {exc}")
        if name == "maintain":
            max_attempts = self.settings.maintain_retry_count + 1
            if self.maintain_attempt < max_attempts:
                self.pending_maintain = True
                self.pending_maintain_reason = "maintain retry"
                self.maintain_retry_due_at = time.monotonic() + self.settings.command_retry_delay_seconds
                log(f"Will retry maintain in {self.settings.command_retry_delay_seconds}s.")
                return 0
            self.pending_maintain = False
            self.pending_maintain_reason = None
            self.maintain_attempt = 0
            return 1

        max_attempts = self.settings.upload_retry_count + 1
        if self.upload_attempt < max_attempts:
            self.upload_retry_due_at = time.monotonic() + self.settings.command_retry_delay_seconds
            self.pending_upload_retry = True
            log(f"Will retry upload in {self.settings.command_retry_delay_seconds}s.")
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
            return 0

        if self.shutdown_requested:
            return 130

        max_attempts = self.settings.maintain_retry_count + 1
        if self.maintain_attempt < max_attempts:
            self.pending_maintain = True
            self.pending_maintain_reason = "maintain retry"
            self.maintain_retry_due_at = time.monotonic() + self.settings.command_retry_delay_seconds
            log(
                f"Maintain command failed with exit {code}. "
                f"Retrying in {self.settings.command_retry_delay_seconds}s..."
            )
            return 0

        log(f"Maintain command failed after retries. Exit code {code}.")
        self.pending_maintain = False
        self.pending_maintain_reason = None
        self.maintain_attempt = 0
        return code

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
            self.pending_upload_retry = False
            self.inflight_upload_snapshot = None
            self.upload_attempt = 0
            self.upload_retry_due_at = 0.0

            if self.settings.delete_uploaded_files_after_upload:
                self.delete_uploaded_files_from_snapshot(uploaded_snapshot)

            current_snapshot = self.build_snapshot(self.current_snapshot_file)
            self.write_snapshot(self.stable_snapshot_file, current_snapshot)
            uploaded_baseline = self.compute_uploaded_baseline(uploaded_snapshot, current_snapshot)
            self.write_snapshot(self.last_uploaded_snapshot_file, uploaded_baseline)
            self.last_json_count = len(current_snapshot)
            if self.settings.inspect_zip_files:
                self.last_zip_signature = self.get_zip_signature()

            if current_snapshot != uploaded_baseline:
                self.pending_upload_snapshot = list(current_snapshot)
                self.pending_upload_reason = "post-upload pending files"
                log(
                    "Detected files outside uploaded baseline after upload. "
                    "Queued next upload batch."
                )
            else:
                self.pending_upload_snapshot = None
                self.pending_upload_reason = None

            if self.settings.run_maintain_after_upload:
                self.queue_maintain("post-upload maintain")
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
            return 0

        log(f"Upload command failed after retries. Exit code {code}.")
        self.pending_upload_snapshot = None
        self.pending_upload_reason = None
        self.inflight_upload_snapshot = None
        self.pending_upload_retry = False
        self.upload_attempt = 0
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

    def check_and_maybe_upload(self, force_deep_scan: bool = False) -> int:
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
            self.pending_upload_retry = False
            return 0

        log(f"Detected JSON changes. Waiting {self.settings.upload_stable_wait_seconds}s for stability...")
        stable_wait_exit, stable_snapshot = self.wait_for_stable_snapshot(current_snapshot)
        if stable_wait_exit != 0:
            return stable_wait_exit
        if stable_snapshot is None:
            return 130

        self.pending_upload_snapshot = list(stable_snapshot)
        self.pending_upload_reason = "detected JSON changes"
        if not self.pending_upload_retry:
            self.upload_attempt = 0
            self.upload_retry_due_at = 0.0
        log("Upload batch queued.")

        return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto maintain + folder watch uploader for cpa-warden.",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    return parser.parse_args()


def load_settings(args: argparse.Namespace) -> Settings:
    base_dir = Path(__file__).resolve().parent
    auth_dir = resolve_path(base_dir, os.getenv("AUTH_DIR", str(base_dir / "auth_files")))
    state_dir = resolve_path(base_dir, os.getenv("STATE_DIR", str(base_dir / ".auto_maintain_state")))
    maintain_db_path = parse_path_env(base_dir, "MAINTAIN_DB_PATH", str(state_dir / "cpa_warden_maintain.sqlite3"))
    upload_db_path = parse_path_env(base_dir, "UPLOAD_DB_PATH", str(state_dir / "cpa_warden_upload.sqlite3"))
    maintain_log_file = parse_path_env(base_dir, "MAINTAIN_LOG_FILE", str(state_dir / "cpa_warden_maintain.log"))
    upload_log_file = parse_path_env(base_dir, "UPLOAD_LOG_FILE", str(state_dir / "cpa_warden_upload.log"))

    config_raw = os.getenv("CONFIG_PATH", "").strip()
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
        auth_dir=auth_dir,
        state_dir=state_dir,
        config_path=config_path,
        maintain_db_path=maintain_db_path,
        upload_db_path=upload_db_path,
        maintain_log_file=maintain_log_file,
        upload_log_file=upload_log_file,
        maintain_interval_seconds=parse_int_env("MAINTAIN_INTERVAL_SECONDS", 3600, 1),
        watch_interval_seconds=parse_int_env("WATCH_INTERVAL_SECONDS", 15, 1),
        upload_stable_wait_seconds=parse_int_env("UPLOAD_STABLE_WAIT_SECONDS", 20, 0),
        deep_scan_interval_loops=parse_int_env("DEEP_SCAN_INTERVAL_LOOPS", 40, 1),
        maintain_retry_count=parse_int_env("MAINTAIN_RETRY_COUNT", 1, 0),
        upload_retry_count=parse_int_env("UPLOAD_RETRY_COUNT", 1, 0),
        command_retry_delay_seconds=parse_int_env("COMMAND_RETRY_DELAY_SECONDS", 20, 1),
        run_maintain_on_start=parse_bool_env("RUN_MAINTAIN_ON_START", True),
        run_upload_on_start=parse_bool_env("RUN_UPLOAD_ON_START", True),
        run_maintain_after_upload=parse_bool_env("RUN_MAINTAIN_AFTER_UPLOAD", True),
        maintain_assume_yes=parse_bool_env("MAINTAIN_ASSUME_YES", False),
        delete_uploaded_files_after_upload=parse_bool_env("DELETE_UPLOADED_FILES_AFTER_UPLOAD", True),
        inspect_zip_files=parse_bool_env("INSPECT_ZIP_FILES", True),
        auto_extract_zip_json=parse_bool_env("AUTO_EXTRACT_ZIP_JSON", True),
        delete_zip_after_extract=parse_bool_env("DELETE_ZIP_AFTER_EXTRACT", True),
        bandizip_path=os.getenv("BANDIZIP_PATH", r"C:\Program Files\Bandizip\Bandizip.exe"),
        bandizip_timeout_seconds=parse_int_env("BANDIZIP_TIMEOUT_SECONDS", 120, 1),
        use_windows_zip_fallback=parse_bool_env("USE_WINDOWS_ZIP_FALLBACK", True),
        continue_on_command_failure=parse_bool_env("CONTINUE_ON_COMMAND_FAILURE", False),
        allow_multi_instance=parse_bool_env("ALLOW_MULTI_INSTANCE", False),
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
