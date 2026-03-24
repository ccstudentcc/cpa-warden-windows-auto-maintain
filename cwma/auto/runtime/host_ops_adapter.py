"""Host utility adapter for file/lock/snapshot/zip/settings orchestration."""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Callable, Protocol

from ..infra.locking import InstanceLockState, acquire_instance_lock as acquire_lock_state, release_instance_lock
from ..state.snapshots import (
    build_snapshot_file as build_snapshot_file_rows,
    build_snapshot_lines as build_snapshot_lines_rows,
    read_snapshot_lines as read_snapshot_lines_rows,
    write_snapshot_lines as write_snapshot_lines_rows,
)
from ..infra.upload_cleanup import cleanup_uploaded_files, prune_empty_dirs_under
from ..infra.zip_intake import (
    compute_zip_signature as compute_zip_signature_rows,
    extract_zip_with_bandizip as extract_zip_with_bandizip_rows,
    extract_zip_with_windows_builtin as extract_zip_with_windows_builtin_rows,
    inspect_zip_archives as inspect_zip_archives_rows,
)


class HostOpsRuntimeHost(Protocol):
    settings: Any
    runtime: Any
    cpa_script: Path
    maintain_names_file: Path
    upload_names_file: Path
    maintain_cmd_output_file: Path
    upload_cmd_output_file: Path
    instance_lock_file: Path
    instance_started_at: Any
    instance_lock_token: str | None
    instance_lock_handle: Any
    zip_extract_processed_signatures: dict[str, str]

    def instance_label(self) -> str: ...


class HostOpsAdapter:
    def __init__(
        self,
        *,
        host: HostOpsRuntimeHost,
        get_log: Callable[[], Callable[[str], None]],
    ) -> None:
        self.host = host
        self.get_log = get_log

    def ensure_paths(self) -> None:
        if not self.host.cpa_script.exists():
            raise RuntimeError(f"cpa_warden.py not found: {self.host.cpa_script}")

        self.host.settings.auth_dir.mkdir(parents=True, exist_ok=True)
        if not self.host.settings.auth_dir.is_dir():
            raise RuntimeError(f"AUTH_DIR is not a directory: {self.host.settings.auth_dir}")

        self.host.settings.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.host.settings.state_dir.is_dir():
            raise RuntimeError(f"STATE_DIR is not a directory: {self.host.settings.state_dir}")

        self.host.settings.maintain_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.host.settings.upload_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.host.settings.maintain_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.host.settings.upload_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.host.maintain_cmd_output_file.parent.mkdir(parents=True, exist_ok=True)
        self.host.upload_cmd_output_file.parent.mkdir(parents=True, exist_ok=True)

    def settings_log_rows(self) -> list[tuple[str, str | int | Path]]:
        return [
            ("WATCH_CONFIG_PATH", self.host.settings.watch_config_path or "(none)"),
            ("AUTH_DIR", self.host.settings.auth_dir),
            ("STATE_DIR", self.host.settings.state_dir),
            ("MAINTAIN_DB_PATH", self.host.settings.maintain_db_path),
            ("UPLOAD_DB_PATH", self.host.settings.upload_db_path),
            ("MAINTAIN_LOG_FILE", self.host.settings.maintain_log_file),
            ("UPLOAD_LOG_FILE", self.host.settings.upload_log_file),
            ("MAINTAIN_COMMAND_OUTPUT_FILE", self.host.maintain_cmd_output_file),
            ("UPLOAD_COMMAND_OUTPUT_FILE", self.host.upload_cmd_output_file),
            ("MAINTAIN_NAMES_SCOPE_FILE", self.host.maintain_names_file),
            ("UPLOAD_NAMES_SCOPE_FILE", self.host.upload_names_file),
            ("MAINTAIN_INTERVAL_SECONDS", self.host.settings.maintain_interval_seconds),
            ("WATCH_INTERVAL_SECONDS", self.host.settings.watch_interval_seconds),
            ("UPLOAD_STABLE_WAIT_SECONDS", self.host.settings.upload_stable_wait_seconds),
            ("UPLOAD_BATCH_SIZE", self.host.settings.upload_batch_size),
            ("SMART_SCHEDULE_ENABLED", int(self.host.settings.smart_schedule_enabled)),
            ("ADAPTIVE_UPLOAD_BATCHING", int(self.host.settings.adaptive_upload_batching)),
            ("UPLOAD_HIGH_BACKLOG_THRESHOLD", self.host.settings.upload_high_backlog_threshold),
            ("UPLOAD_HIGH_BACKLOG_BATCH_SIZE", self.host.settings.upload_high_backlog_batch_size),
            ("ADAPTIVE_MAINTAIN_BATCHING", int(self.host.settings.adaptive_maintain_batching)),
            ("INCREMENTAL_MAINTAIN_BATCH_SIZE", self.host.settings.incremental_maintain_batch_size),
            ("MAINTAIN_HIGH_BACKLOG_THRESHOLD", self.host.settings.maintain_high_backlog_threshold),
            ("MAINTAIN_HIGH_BACKLOG_BATCH_SIZE", self.host.settings.maintain_high_backlog_batch_size),
            (
                "INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS",
                self.host.settings.incremental_maintain_min_interval_seconds,
            ),
            (
                "INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS",
                self.host.settings.incremental_maintain_full_guard_seconds,
            ),
            ("RUN_MAINTAIN_ON_START", int(self.host.settings.run_maintain_on_start)),
            ("RUN_UPLOAD_ON_START", int(self.host.settings.run_upload_on_start)),
            ("RUN_MAINTAIN_AFTER_UPLOAD", int(self.host.settings.run_maintain_after_upload)),
            ("MAINTAIN_ASSUME_YES", int(self.host.settings.maintain_assume_yes)),
            (
                "DELETE_UPLOADED_FILES_AFTER_UPLOAD",
                int(self.host.settings.delete_uploaded_files_after_upload),
            ),
            ("INSPECT_ZIP_FILES", int(self.host.settings.inspect_zip_files)),
            ("AUTO_EXTRACT_ZIP_JSON", int(self.host.settings.auto_extract_zip_json)),
            ("DELETE_ZIP_AFTER_EXTRACT", int(self.host.settings.delete_zip_after_extract)),
            ("BANDIZIP_PATH", self.host.settings.bandizip_path),
            ("BANDIZIP_TIMEOUT_SECONDS", self.host.settings.bandizip_timeout_seconds),
            ("USE_WINDOWS_ZIP_FALLBACK", int(self.host.settings.use_windows_zip_fallback)),
            ("ARCHIVE_EXTENSIONS", ",".join(self.host.settings.archive_extensions)),
            ("BANDIZIP_PREFER_CONSOLE", int(self.host.settings.bandizip_prefer_console)),
            ("BANDIZIP_HIDE_WINDOW", int(self.host.settings.bandizip_hide_window)),
            ("DEEP_SCAN_INTERVAL_LOOPS", self.host.settings.deep_scan_interval_loops),
            ("ACTIVE_PROBE_INTERVAL_SECONDS", self.host.settings.active_probe_interval_seconds),
            (
                "ACTIVE_UPLOAD_DEEP_SCAN_INTERVAL_SECONDS",
                self.host.settings.active_upload_deep_scan_interval_seconds,
            ),
            ("MAINTAIN_RETRY_COUNT", self.host.settings.maintain_retry_count),
            ("UPLOAD_RETRY_COUNT", self.host.settings.upload_retry_count),
            ("COMMAND_RETRY_DELAY_SECONDS", self.host.settings.command_retry_delay_seconds),
            ("CONTINUE_ON_COMMAND_FAILURE", int(self.host.settings.continue_on_command_failure)),
            ("ALLOW_MULTI_INSTANCE", int(self.host.settings.allow_multi_instance)),
            ("INSTANCE_LABEL", self.host.instance_label()),
            ("INSTANCE_LOCK_FILE", self.host.instance_lock_file),
        ]

    def log_settings(self) -> None:
        log = self.get_log()
        log("Started auto maintenance loop.")
        for key, value in self.settings_log_rows():
            log(f"{key}={value}")
        if self.host.instance_lock_token:
            log(f"INSTANCE_LOCK_TOKEN={self.host.instance_lock_token}")

    def acquire_instance_lock(self, *, allow_multi_instance: bool) -> None:
        state = acquire_lock_state(
            lock_file=self.host.instance_lock_file,
            state_dir=self.host.settings.state_dir,
            allow_multi_instance=allow_multi_instance,
            log=self.get_log(),
        )
        self.host.instance_lock_token = state.token
        self.host.instance_lock_handle = state.handle
        self.host.runtime.lifecycle.instance_lock_token = self.host.instance_lock_token
        self.host.runtime.lifecycle.instance_lock_handle = self.host.instance_lock_handle

    def release_instance_lock(self) -> None:
        state = InstanceLockState(token=self.host.instance_lock_token, handle=self.host.instance_lock_handle)
        next_state = release_instance_lock(
            lock_file=self.host.instance_lock_file,
            allow_multi_instance=self.host.settings.allow_multi_instance,
            state=state,
            log=self.get_log(),
        )
        self.host.instance_lock_token = next_state.token
        self.host.instance_lock_handle = next_state.handle
        self.host.runtime.lifecycle.instance_lock_token = self.host.instance_lock_token
        self.host.runtime.lifecycle.instance_lock_handle = self.host.instance_lock_handle

    def get_json_paths(self) -> list[Path]:
        return sorted(
            (p for p in self.host.settings.auth_dir.rglob("*.json") if p.is_file()),
            key=lambda p: str(p).lower(),
        )

    def get_json_count(self) -> int:
        return len(self.get_json_paths())

    def get_zip_signature(self) -> tuple[str, ...]:
        return compute_zip_signature_rows(
            self.host.settings.auth_dir,
            log=self.get_log(),
            archive_extensions=self.host.settings.archive_extensions,
        )

    def extract_zip_with_bandizip(self, zip_path: Path, output_dir: Path) -> int:
        exit_code = extract_zip_with_bandizip_rows(
            zip_path=zip_path,
            output_dir=output_dir,
            base_dir=self.host.settings.base_dir,
            bandizip_path=self.host.settings.bandizip_path,
            timeout_seconds=self.host.settings.bandizip_timeout_seconds,
            prefer_console=self.host.settings.bandizip_prefer_console,
            hide_window=self.host.settings.bandizip_hide_window,
            log=self.get_log(),
        )
        if exit_code == 0:
            return 0

        if self.host.settings.use_windows_zip_fallback:
            self.get_log()(f"Trying Windows built-in unzip fallback: {zip_path.name}")
            return extract_zip_with_windows_builtin_rows(
                zip_path=zip_path,
                output_dir=output_dir,
                base_dir=self.host.settings.base_dir,
                timeout_seconds=self.host.settings.bandizip_timeout_seconds,
                hide_window=self.host.settings.bandizip_hide_window,
                log=self.get_log(),
            )

        return exit_code

    def inspect_zip_archives(self) -> bool:
        return inspect_zip_archives_rows(
            auth_dir=self.host.settings.auth_dir,
            inspect_zip_files=self.host.settings.inspect_zip_files,
            auto_extract_zip_json=self.host.settings.auto_extract_zip_json,
            delete_zip_after_extract=self.host.settings.delete_zip_after_extract,
            processed_signatures=self.host.zip_extract_processed_signatures,
            extract_zip=self.host.extract_zip_with_bandizip,
            log=self.get_log(),
            archive_extensions=self.host.settings.archive_extensions,
            bandizip_path=self.host.settings.bandizip_path,
            bandizip_timeout_seconds=self.host.settings.bandizip_timeout_seconds,
            bandizip_prefer_console=self.host.settings.bandizip_prefer_console,
            bandizip_hide_window=self.host.settings.bandizip_hide_window,
        )

    def snapshot_lines(self) -> list[str]:
        return build_snapshot_lines_rows(self.get_json_paths(), log=self.get_log())

    def write_snapshot(self, target: Path, lines: Iterable[str]) -> None:
        write_snapshot_lines_rows(target, lines)

    def read_snapshot(self, source: Path) -> list[str]:
        return read_snapshot_lines_rows(source)

    def build_snapshot(self, target: Path) -> list[str]:
        return build_snapshot_file_rows(
            target=target,
            paths=self.get_json_paths(),
            log=self.get_log(),
        )

    def delete_uploaded_files_from_snapshot(self, snapshot_lines: list[str]) -> None:
        result = cleanup_uploaded_files(snapshot_lines)
        self.get_log()(
            "Upload cleanup summary: "
            f"deleted={result.deleted}, skipped_changed={result.skipped_changed}, "
            f"skipped_missing={result.skipped_missing}, failed={result.failed}"
        )
        self.prune_empty_dirs_under_auth_dir()

    def prune_empty_dirs_under_auth_dir(self) -> None:
        result = prune_empty_dirs_under(self.host.settings.auth_dir)
        self.get_log()(
            "Upload empty-dir cleanup summary: "
            f"removed={result.removed}, skipped_non_empty={result.skipped_non_empty}, "
            f"skipped_missing={result.skipped_missing}, failed={result.failed}"
        )
