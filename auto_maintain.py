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


@dataclass
class Settings:
    base_dir: Path
    auth_dir: Path
    state_dir: Path
    config_path: Path | None
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
        self.active_process: subprocess.Popen | None = None
        self._windows_console_handler = None
        self.deep_scan_counter = 0
        self.pending_upload_retry = False
        self.last_json_count = 0
        self.last_zip_count = 0

    def ensure_paths(self) -> None:
        if not self.cpa_script.exists():
            raise RuntimeError(f"cpa_warden.py not found: {self.cpa_script}")

        self.settings.auth_dir.mkdir(parents=True, exist_ok=True)
        if not self.settings.auth_dir.is_dir():
            raise RuntimeError(f"AUTH_DIR is not a directory: {self.settings.auth_dir}")

        self.settings.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.settings.state_dir.is_dir():
            raise RuntimeError(f"STATE_DIR is not a directory: {self.settings.state_dir}")

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
                self.last_zip_count = self.get_zip_count()

            if self.settings.inspect_zip_files:
                startup_zip_changed = self.inspect_zip_archives()
                self.last_json_count = self.get_json_count()
                self.last_zip_count = self.get_zip_count()
                if startup_zip_changed:
                    log("Startup ZIP scan produced changes. Trigger immediate upload check.")
                    startup_zip_upload_exit = self.check_and_maybe_upload(force_deep_scan=True)
                    if startup_zip_upload_exit != 0 and not self.handle_failure(
                        "startup zip-upload-check",
                        startup_zip_upload_exit,
                    ):
                        return startup_zip_upload_exit

            if self.settings.run_maintain_on_start:
                maintain_exit = self.run_maintain()
                if maintain_exit != 0 and not self.handle_failure("startup maintain", maintain_exit):
                    return maintain_exit

            if self.settings.run_upload_on_start:
                upload_check_exit = self.check_and_maybe_upload()
                if upload_check_exit != 0 and not self.handle_failure("startup upload-check", upload_check_exit):
                    return upload_check_exit

            elapsed_seconds = 0
            while True:
                if elapsed_seconds >= self.settings.maintain_interval_seconds:
                    maintain_exit = self.run_maintain()
                    if maintain_exit != 0 and not self.handle_failure("scheduled maintain", maintain_exit):
                        return maintain_exit
                    elapsed_seconds = 0

                upload_check_exit = self.check_and_maybe_upload()
                if upload_check_exit != 0 and not self.handle_failure("watch upload-check", upload_check_exit):
                    return upload_check_exit

                if self.settings.run_once:
                    log("Run-once cycle finished.")
                    return 0

                if not self.sleep_with_shutdown(self.settings.watch_interval_seconds):
                    return 130
                elapsed_seconds += self.settings.watch_interval_seconds
        finally:
            self.release_instance_lock()

    def log_settings(self) -> None:
        log("Started auto maintenance loop.")
        log(f"AUTH_DIR={self.settings.auth_dir}")
        log(f"STATE_DIR={self.settings.state_dir}")
        log(f"MAINTAIN_INTERVAL_SECONDS={self.settings.maintain_interval_seconds}")
        log(f"WATCH_INTERVAL_SECONDS={self.settings.watch_interval_seconds}")
        log(f"UPLOAD_STABLE_WAIT_SECONDS={self.settings.upload_stable_wait_seconds}")
        log(f"RUN_MAINTAIN_ON_START={int(self.settings.run_maintain_on_start)}")
        log(f"RUN_UPLOAD_ON_START={int(self.settings.run_upload_on_start)}")
        log(f"RUN_MAINTAIN_AFTER_UPLOAD={int(self.settings.run_maintain_after_upload)}")
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
        except Exception:
            pass

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
            except Exception:
                pass

    def _signal_handler(self, signum: int, _frame: object) -> None:
        self.request_shutdown(f"SIGNAL_{signum}")

    def request_shutdown(self, reason: str) -> None:
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        self.shutdown_reason = reason
        log(f"Shutdown requested: {reason}")
        self.terminate_active_process()

    def terminate_active_process(self) -> None:
        proc = self.active_process
        if proc is None:
            return
        if proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        except Exception:
            pass

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
                    except OSError:
                        pass
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
        except OSError:
            pass
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
        for path in self.get_json_paths():
            stat = path.stat()
            lines.append(f"{path}|{stat.st_size}|{stat.st_mtime_ns}")
        lines.sort()
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

    def command_base(self) -> list[str]:
        cmd = [sys.executable, str(self.cpa_script)]
        if self.settings.config_path is not None:
            cmd.extend(["--config", str(self.settings.config_path)])
        return cmd

    def run_with_retry(self, name: str, cmd: list[str], retry_count: int) -> int:
        max_attempts = retry_count + 1
        for attempt in range(1, max_attempts + 1):
            log(f"Running {name} command attempt {attempt} of {max_attempts}.")
            self.active_process = subprocess.Popen(cmd, cwd=str(self.settings.base_dir))
            try:
                while True:
                    code = self.active_process.poll()
                    if code is not None:
                        break
                    if self.shutdown_requested:
                        self.terminate_active_process()
                        return 130
                    time.sleep(0.2)
            finally:
                self.active_process = None

            if code == 0:
                log(f"{name.capitalize()} command completed.")
                return 0
            if self.shutdown_requested:
                return 130
            if attempt < max_attempts:
                log(
                    f"{name.capitalize()} command failed with exit {code}. "
                    f"Retrying in {self.settings.command_retry_delay_seconds}s..."
                )
                if not self.sleep_with_shutdown(self.settings.command_retry_delay_seconds):
                    return 130
            else:
                log(f"{name.capitalize()} command failed after retries. Exit code {code}.")
                return code
        return 1

    def run_maintain(self) -> int:
        cmd = self.command_base() + ["--mode", "maintain", "--yes"]
        return self.run_with_retry("maintain", cmd, self.settings.maintain_retry_count)

    def run_upload(self) -> int:
        cmd = self.command_base() + [
            "--mode",
            "upload",
            "--upload-dir",
            str(self.settings.auth_dir),
            "--upload-recursive",
        ]
        return self.run_with_retry("upload", cmd, self.settings.upload_retry_count)

    def handle_failure(self, stage: str, code: int) -> bool:
        log(f"{stage} failed with exit {code}.")
        if self.settings.continue_on_command_failure:
            log("Continue mode enabled. Keep loop alive.")
            return True
        return False

    def check_and_maybe_upload(self, force_deep_scan: bool = False) -> int:
        current_json_count = self.get_json_count()
        current_zip_count = self.get_zip_count() if self.settings.inspect_zip_files else 0
        need_deep_scan = False

        if force_deep_scan:
            need_deep_scan = True
        elif self.pending_upload_retry:
            need_deep_scan = True
        elif current_json_count != self.last_json_count:
            need_deep_scan = True
        elif self.settings.inspect_zip_files and current_zip_count != self.last_zip_count:
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
                current_zip_count = self.get_zip_count()
        current_snapshot = self.build_snapshot(self.current_snapshot_file)
        last_uploaded_snapshot = self.read_snapshot(self.last_uploaded_snapshot_file)
        if current_snapshot == last_uploaded_snapshot:
            self.last_json_count = current_json_count
            if self.settings.inspect_zip_files:
                self.last_zip_count = current_zip_count
            self.pending_upload_retry = False
            return 0

        log(f"Detected JSON changes. Waiting {self.settings.upload_stable_wait_seconds}s for stability...")
        stable_wait_exit, stable_snapshot = self.wait_for_stable_snapshot(current_snapshot)
        if stable_wait_exit != 0:
            return stable_wait_exit
        if stable_snapshot is None:
            return 130

        upload_exit = self.run_upload()
        if upload_exit != 0:
            self.pending_upload_retry = True
            if not self.handle_failure("upload command", upload_exit):
                return upload_exit
            return 0

        self.write_snapshot(self.last_uploaded_snapshot_file, stable_snapshot)
        self.last_json_count = self.get_json_count()
        if self.settings.inspect_zip_files:
            self.last_zip_count = self.get_zip_count()
        self.pending_upload_retry = False

        if self.settings.delete_uploaded_files_after_upload:
            self.delete_uploaded_files_from_snapshot(stable_snapshot)
            self.last_json_count = self.get_json_count()
            if self.settings.inspect_zip_files:
                self.last_zip_count = self.get_zip_count()
            refreshed_snapshot = self.build_snapshot(self.last_uploaded_snapshot_file)
            self.write_snapshot(self.current_snapshot_file, refreshed_snapshot)

        if self.settings.run_maintain_after_upload:
            log("Running maintain after successful upload...")
            maintain_exit = self.run_maintain()
            if maintain_exit != 0 and not self.handle_failure("post-upload maintain", maintain_exit):
                return maintain_exit

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
        delete_uploaded_files_after_upload=parse_bool_env("DELETE_UPLOADED_FILES_AFTER_UPLOAD", True),
        inspect_zip_files=parse_bool_env("INSPECT_ZIP_FILES", True),
        auto_extract_zip_json=parse_bool_env("AUTO_EXTRACT_ZIP_JSON", True),
        delete_zip_after_extract=parse_bool_env("DELETE_ZIP_AFTER_EXTRACT", True),
        bandizip_path=os.getenv("BANDIZIP_PATH", r"C:\Program Files\Bandizip\Bandizip.exe"),
        bandizip_timeout_seconds=parse_int_env("BANDIZIP_TIMEOUT_SECONDS", 120, 1),
        use_windows_zip_fallback=parse_bool_env("USE_WINDOWS_ZIP_FALLBACK", True),
        continue_on_command_failure=parse_bool_env("CONTINUE_ON_COMMAND_FAILURE", True),
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
