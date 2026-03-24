from __future__ import annotations

import argparse
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ...common import load_json_object, parse_bool_value, parse_int_value, pick_setting, resolve_path


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
    archive_extensions: tuple[str, ...]
    bandizip_prefer_console: bool
    bandizip_hide_window: bool
    continue_on_command_failure: bool
    allow_multi_instance: bool
    run_once: bool


def load_watch_config(path: Path) -> dict[str, object]:
    try:
        return load_json_object(path, encoding="utf-8-sig")
    except ValueError as exc:
        raise ValueError(f"Invalid watch config JSON: {path}") from exc


def parse_archive_extensions(name: str, raw: object) -> tuple[str, ...]:
    def _normalize(value: object) -> str | None:
        text = str(value).strip().lower()
        if not text:
            return None
        if not text.startswith("."):
            text = f".{text}"
        if any(ch.isspace() for ch in text):
            raise ValueError(f"{name} contains invalid extension token: {value}")
        return text

    tokens: list[str] = []
    if isinstance(raw, str):
        for item in raw.replace(";", ",").split(","):
            normalized = _normalize(item)
            if normalized is not None:
                tokens.append(normalized)
    elif isinstance(raw, Iterable):
        for item in raw:
            normalized = _normalize(item)
            if normalized is not None:
                tokens.append(normalized)
    else:
        raise ValueError(f"{name} must be a comma-separated string or array, got: {raw}")

    deduped: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        deduped.append(token)
        seen.add(token)
    if not deduped:
        raise ValueError(f"{name} cannot be empty.")
    return tuple(deduped)


def load_settings(
    *,
    args: argparse.Namespace,
    base_dir: Path,
    log: Callable[[str], None],
) -> Settings:
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
        archive_extensions=parse_archive_extensions(
            "ARCHIVE_EXTENSIONS",
            pick_setting(
                "ARCHIVE_EXTENSIONS",
                watch_config_data,
                "archive_extensions",
                ".zip,.7z,.rar",
            ),
        ),
        bandizip_prefer_console=parse_bool_value(
            "BANDIZIP_PREFER_CONSOLE",
            pick_setting(
                "BANDIZIP_PREFER_CONSOLE",
                watch_config_data,
                "bandizip_prefer_console",
                True,
            ),
        ),
        bandizip_hide_window=parse_bool_value(
            "BANDIZIP_HIDE_WINDOW",
            pick_setting(
                "BANDIZIP_HIDE_WINDOW",
                watch_config_data,
                "bandizip_hide_window",
                True,
            ),
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
