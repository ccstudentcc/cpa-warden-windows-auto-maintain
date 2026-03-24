#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from ..warden.config import (
    build_settings as build_settings_warden,
    config_lookup as config_lookup_warden,
    load_config_json as load_config_json_warden,
    parse_bool_config as parse_bool_config_warden,
)
from ..warden.cli import parse_cli_args
from ..warden.interactive import (
    prompt_choice as prompt_choice_interactive,
    prompt_float as prompt_float_interactive,
    prompt_int as prompt_int_interactive,
    prompt_string as prompt_string_interactive,
    prompt_yes_no as prompt_yes_no_interactive,
)
from ..warden.api.management import (
    build_management_headers as mgmt_headers_warden,
    delete_account_async as delete_account_async_warden,
    fetch_auth_files as fetch_auth_files_warden,
    fetch_remote_auth_file_names as fetch_remote_auth_file_names_warden,
    set_account_disabled_async as set_account_disabled_async_warden,
)
from ..warden.api.usage_probe import (
    build_wham_usage_payload as build_wham_usage_payload_warden,
    extract_remaining_ratio as extract_remaining_ratio_warden,
    find_spark_rate_limit as find_spark_rate_limit_warden,
    probe_wham_usage_async as probe_wham_usage_async_warden,
)
from ..warden.db.repository import (
    claim_upload_slot as claim_upload_slot_warden,
    connect_db as connect_db_warden,
    finish_scan_run as finish_scan_run_warden,
    load_existing_state as load_existing_state_warden,
    mark_upload_attempt as mark_upload_attempt_warden,
    mark_upload_failure as mark_upload_failure_warden,
    mark_upload_skipped_remote_exists as mark_upload_skipped_remote_exists_warden,
    mark_upload_success as mark_upload_success_warden,
    start_scan_run as start_scan_run_warden,
    upsert_auth_accounts as upsert_auth_accounts_warden,
)
from ..warden.db.schema import (
    ensure_auth_accounts_schema as ensure_auth_accounts_schema_warden,
    init_db as init_db_warden,
    init_upload_db as init_upload_db_warden,
)
from ..warden.exports import (
    build_invalid_export_record as build_invalid_export_record_warden,
    build_quota_export_record as build_quota_export_record_warden,
    export_current_results as export_current_results_warden,
    export_records as export_records_warden,
    summarize_failures as summarize_failures_warden,
)
from ..warden.models import (
    AUTH_ACCOUNT_COLUMNS as AUTH_ACCOUNT_COLUMNS_WARDEN,
    build_auth_record as build_auth_record_warden,
    compact_text as compact_text_warden,
    extract_chatgpt_account_id_from_item as extract_chatgpt_account_id_from_item_warden,
    extract_id_token_plan_type as extract_id_token_plan_type_warden,
    get_id_token_object as get_id_token_object_warden,
    get_item_account as get_item_account_warden,
    get_item_name as get_item_name_warden,
    get_item_type as get_item_type_warden,
    matches_filters as matches_filters_warden,
    maybe_json_loads as maybe_json_loads_warden,
    normalize_optional_flag as normalize_optional_flag_warden,
    normalize_optional_number as normalize_optional_number_warden,
    normalize_optional_ratio as normalize_optional_ratio_warden,
    pick_first_number as pick_first_number_warden,
    resolve_quota_remaining_ratio as resolve_quota_remaining_ratio_warden,
    resolve_quota_signal as resolve_quota_signal_warden,
)
from ..warden.services.maintain_scope import (
    load_name_scope_file as load_name_scope_file_warden,
    resolve_maintain_name_scope as resolve_maintain_name_scope_warden,
    resolve_upload_name_scope as resolve_upload_name_scope_warden,
)
from ..warden.services.runtime_ops import (
    apply_action_results as apply_action_results_runtime_warden,
    classify_account_state as classify_account_state_runtime_warden,
    confirm_action as confirm_action_runtime_warden,
    mark_quota_already_disabled as mark_quota_already_disabled_runtime_warden,
    print_scan_summary as print_scan_summary_runtime_warden,
    probe_accounts_async as probe_accounts_async_runtime_warden,
    run_action_group_async as run_action_group_async_runtime_warden,
    run_register_hook_async as run_register_hook_async_runtime_warden,
    summarize_action_results as summarize_action_results_runtime_warden,
    summarize_upload_results as summarize_upload_results_runtime_warden,
    upload_auth_file_async as upload_auth_file_async_runtime_warden,
)
from ..warden.services.maintain import run_maintain_async as run_maintain_async_service_warden
from ..warden.services.refill import (
    compute_refill_upload_count as compute_refill_upload_count_service_warden,
    count_valid_accounts as count_valid_accounts_service_warden,
    run_maintain_refill_async as run_maintain_refill_async_service_warden,
)
from ..warden.services.scan import run_scan_async as run_scan_async_service_warden
from ..warden.services.upload import run_upload_async as run_upload_async_service_warden
from ..warden.services.upload_scope import (
    discover_upload_files as discover_upload_files_warden,
    select_upload_candidates as select_upload_candidates_warden,
    validate_and_digest_json_file as validate_and_digest_json_file_warden,
)

try:
    import aiohttp
except Exception:
    aiohttp = None

try:
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
except Exception:
    Progress = None


DEFAULT_CONFIG_PATH = "config.json"
DEFAULT_TARGET_TYPE = "codex"
DEFAULT_USER_AGENT = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
DEFAULT_PROBE_WORKERS = 100
DEFAULT_ACTION_WORKERS = 100
DEFAULT_TIMEOUT = 15
DEFAULT_RETRIES = 3
DEFAULT_DELETE_RETRIES = 2
DEFAULT_QUOTA_ACTION = "disable"
DEFAULT_DELETE_401 = True
DEFAULT_AUTO_REENABLE = True
DEFAULT_REENABLE_SCOPE = "signal"
DEFAULT_DB_PATH = "cpa_warden_state.sqlite3"
DEFAULT_INVALID_OUTPUT = "cpa_warden_401_accounts.json"
DEFAULT_QUOTA_OUTPUT = "cpa_warden_quota_accounts.json"
DEFAULT_LOG_FILE = "cpa_warden.log"
DEFAULT_UPLOAD_WORKERS = 20
DEFAULT_UPLOAD_RETRIES = 2
DEFAULT_UPLOAD_METHOD = "json"
DEFAULT_UPLOAD_RECURSIVE = False
DEFAULT_UPLOAD_FORCE = False
DEFAULT_REFILL_STRATEGY = "to-threshold"
DEFAULT_AUTO_REGISTER = False
DEFAULT_REGISTER_TIMEOUT = 300
DEFAULT_QUOTA_DISABLE_THRESHOLD = 0.0
WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
SPARK_METERED_FEATURE = "codex_bengalfox"

AUTH_ACCOUNT_COLUMNS = list(AUTH_ACCOUNT_COLUMNS_WARDEN)


LOGGER = logging.getLogger("cpa_warden")


def configure_logging(log_file: str, debug: bool) -> None:
    LOGGER.handlers.clear()
    LOGGER.setLevel(logging.DEBUG)
    LOGGER.propagate = False

    log_path = Path(log_file)
    if log_path.parent and str(log_path.parent) not in {"", "."}:
        log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def rich_progress_enabled(debug: bool) -> bool:
    return bool((Progress is not None) and (not debug) and hasattr(sys.stdout, "isatty") and sys.stdout.isatty())


class ProgressReporter:
    def __init__(self, description: str, total: int, *, debug: bool) -> None:
        self.description = description
        self.total = max(0, int(total))
        self.debug = debug
        self.enabled = rich_progress_enabled(debug)
        self._progress: Progress | None = None
        self._task_id: Any = None

    def __enter__(self) -> "ProgressReporter":
        if self.enabled:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                transient=False,
            )
            self._progress.start()
            self._task_id = self._progress.add_task(self.description, total=self.total)
        return self

    def advance(self, step: int = 1) -> None:
        if self._progress is not None and self._task_id is not None:
            self._progress.advance(self._task_id, step)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._progress is not None:
            self._progress.stop()


def progress_log_step(total: int, target_updates: int = 20) -> int:
    normalized_total = max(0, int(total))
    if normalized_total <= 0:
        return 1
    normalized_target = max(1, int(target_updates))
    return max(1, int(math.ceil(normalized_total / normalized_target)))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_json(resp: requests.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def maybe_json_loads(value: Any) -> Any:
    return maybe_json_loads_warden(value)


def compact_text(text: Any, limit: int = 240) -> str | None:
    return compact_text_warden(text, limit=limit)


def normalize_optional_flag(value: Any) -> int | None:
    return normalize_optional_flag_warden(value)


def normalize_optional_number(value: Any) -> float | None:
    return normalize_optional_number_warden(value)


def normalize_optional_ratio(value: Any) -> float | None:
    return normalize_optional_ratio_warden(value)


def retry_backoff_seconds(attempt: int) -> float:
    return min(3.0, 0.5 * (2 ** max(0, attempt)))


def pick_first_number(record: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    return pick_first_number_warden(record, keys)


def extract_remaining_ratio(rate_limit: dict[str, Any] | None) -> float | None:
    return extract_remaining_ratio_warden(rate_limit)


def find_spark_rate_limit(parsed_body: dict[str, Any]) -> dict[str, Any] | None:
    return find_spark_rate_limit_warden(parsed_body, spark_metered_feature=SPARK_METERED_FEATURE)


def resolve_quota_signal(record: dict[str, Any]) -> tuple[int | None, int | None, str]:
    return resolve_quota_signal_warden(record)


def resolve_quota_remaining_ratio(record: dict[str, Any]) -> tuple[float | None, str]:
    return resolve_quota_remaining_ratio_warden(record)


def ensure_aiohttp() -> None:
    if aiohttp is None:
        print("错误: 未安装 aiohttp。请先执行 `uv sync`。", file=sys.stderr)
        sys.exit(1)


def get_item_name(item: dict[str, Any]) -> str:
    return get_item_name_warden(item)


def get_item_type(item: dict[str, Any]) -> str:
    return get_item_type_warden(item)


def get_item_account(item: dict[str, Any]) -> str:
    return get_item_account_warden(item)


def get_id_token_object(item: dict[str, Any]) -> dict[str, Any]:
    return get_id_token_object_warden(item)


def extract_chatgpt_account_id_from_item(item: dict[str, Any]) -> str:
    return extract_chatgpt_account_id_from_item_warden(item)


def extract_id_token_plan_type(item: dict[str, Any]) -> str:
    return extract_id_token_plan_type_warden(item)


def mgmt_headers(token: str, include_json: bool = False) -> dict[str, str]:
    return mgmt_headers_warden(token, include_json=include_json)


def config_lookup(conf: dict[str, Any], *keys: str, default: Any = None) -> Any:
    return config_lookup_warden(conf, *keys, default=default)


def parse_bool_config(value: Any, *, key: str) -> bool:
    return parse_bool_config_warden(value, key=key)


def load_config_json(path: str, required: bool = False) -> dict[str, Any]:
    return load_config_json_warden(path, required=required)


def build_settings(args: argparse.Namespace, conf: dict[str, Any]) -> dict[str, Any]:
    return build_settings_warden(
        args,
        conf,
        defaults={
            "config_path": DEFAULT_CONFIG_PATH,
            "target_type": DEFAULT_TARGET_TYPE,
            "user_agent": DEFAULT_USER_AGENT,
            "probe_workers": DEFAULT_PROBE_WORKERS,
            "action_workers": DEFAULT_ACTION_WORKERS,
            "timeout": DEFAULT_TIMEOUT,
            "retries": DEFAULT_RETRIES,
            "delete_retries": DEFAULT_DELETE_RETRIES,
            "quota_action": DEFAULT_QUOTA_ACTION,
            "delete_401": DEFAULT_DELETE_401,
            "auto_reenable": DEFAULT_AUTO_REENABLE,
            "reenable_scope": DEFAULT_REENABLE_SCOPE,
            "db_path": DEFAULT_DB_PATH,
            "invalid_output": DEFAULT_INVALID_OUTPUT,
            "quota_output": DEFAULT_QUOTA_OUTPUT,
            "log_file": DEFAULT_LOG_FILE,
            "upload_workers": DEFAULT_UPLOAD_WORKERS,
            "upload_retries": DEFAULT_UPLOAD_RETRIES,
            "upload_method": DEFAULT_UPLOAD_METHOD,
            "upload_recursive": DEFAULT_UPLOAD_RECURSIVE,
            "upload_force": DEFAULT_UPLOAD_FORCE,
            "refill_strategy": DEFAULT_REFILL_STRATEGY,
            "auto_register": DEFAULT_AUTO_REGISTER,
            "register_timeout": DEFAULT_REGISTER_TIMEOUT,
            "quota_disable_threshold": DEFAULT_QUOTA_DISABLE_THRESHOLD,
        },
    )


def connect_db(db_path: str) -> sqlite3.Connection:
    return connect_db_warden(db_path)


def init_db(conn: sqlite3.Connection) -> None:
    init_db_warden(conn)


def ensure_auth_accounts_schema(conn: sqlite3.Connection) -> None:
    ensure_auth_accounts_schema_warden(conn)


def init_upload_db(conn: sqlite3.Connection) -> None:
    init_upload_db_warden(conn)


def load_existing_state(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    return load_existing_state_warden(conn)


def start_scan_run(conn: sqlite3.Connection, settings: dict[str, Any]) -> int:
    return start_scan_run_warden(conn, settings, now_iso=utc_now_iso)


def finish_scan_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    total_files: int,
    filtered_files: int,
    probed_files: int,
    invalid_401_count: int,
    quota_limited_count: int,
    recovered_count: int,
) -> None:
    finish_scan_run_warden(
        conn,
        run_id,
        status=status,
        total_files=total_files,
        filtered_files=filtered_files,
        probed_files=probed_files,
        invalid_401_count=invalid_401_count,
        quota_limited_count=quota_limited_count,
        recovered_count=recovered_count,
        now_iso=utc_now_iso,
    )


def upsert_auth_accounts(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    upsert_auth_accounts_warden(conn, rows, auth_account_columns=AUTH_ACCOUNT_COLUMNS)


def row_to_bool(value: Any) -> bool:
    return bool(int(value)) if value is not None else False


def build_auth_record(
    item: dict[str, Any],
    existing_row: dict[str, Any] | None,
    now_iso: str,
) -> dict[str, Any]:
    return build_auth_record_warden(item, existing_row, now_iso)


def matches_filters(record: dict[str, Any], target_type: str, provider: str) -> bool:
    return matches_filters_warden(record, target_type, provider)


def load_name_scope_file(path_text: str, *, option_label: str) -> set[str]:
    return load_name_scope_file_warden(path_text, option_label=option_label)


def resolve_maintain_name_scope(settings: dict[str, Any]) -> set[str] | None:
    return resolve_maintain_name_scope_warden(settings)


def resolve_upload_name_scope(settings: dict[str, Any]) -> set[str] | None:
    return resolve_upload_name_scope_warden(settings)


def fetch_auth_files(base_url: str, token: str, timeout: int) -> list[dict[str, Any]]:
    LOGGER.info("开始拉取 auth-files 列表")
    LOGGER.debug("GET %s/v0/management/auth-files", base_url.rstrip("/"))
    files = fetch_auth_files_warden(
        base_url,
        token,
        timeout,
        headers_builder=mgmt_headers,
        json_loader=safe_json,
    )
    LOGGER.info("auth-files 拉取完成: %s", len(files))
    return files


def discover_upload_files(upload_dir: str, recursive: bool) -> list[Path]:
    return discover_upload_files_warden(upload_dir, recursive)


def validate_and_digest_json_file(path: Path) -> dict[str, Any]:
    return validate_and_digest_json_file_warden(path)


def select_upload_candidates(
    validated_files: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    return select_upload_candidates_warden(validated_files)


def fetch_remote_auth_file_names(base_url: str, token: str, timeout: int) -> set[str]:
    return fetch_remote_auth_file_names_warden(
        base_url,
        token,
        timeout,
        fetcher=fetch_auth_files,
        name_extractor=get_item_name,
    )


def mark_upload_skipped_remote_exists(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    file_path: str,
    file_size: int,
) -> None:
    mark_upload_skipped_remote_exists_warden(
        conn,
        base_url=base_url,
        file_name=file_name,
        content_sha256=content_sha256,
        file_path=file_path,
        file_size=file_size,
        now_iso=utc_now_iso,
    )


def claim_upload_slot(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    file_path: str,
    file_size: int,
) -> str:
    return claim_upload_slot_warden(
        conn,
        base_url=base_url,
        file_name=file_name,
        content_sha256=content_sha256,
        file_path=file_path,
        file_size=file_size,
        now_iso=utc_now_iso,
    )


def mark_upload_attempt(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
) -> None:
    mark_upload_attempt_warden(
        conn,
        base_url=base_url,
        file_name=file_name,
        content_sha256=content_sha256,
        now_iso=utc_now_iso,
    )


def mark_upload_success(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    http_status: int | None,
    response_text: str | None,
) -> None:
    mark_upload_success_warden(
        conn,
        base_url=base_url,
        file_name=file_name,
        content_sha256=content_sha256,
        http_status=http_status,
        response_text=response_text,
        now_iso=utc_now_iso,
        compact_text=compact_text,
    )


def mark_upload_failure(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    file_name: str,
    content_sha256: str,
    http_status: int | None,
    error_text: str,
    response_text: str | None,
) -> None:
    mark_upload_failure_warden(
        conn,
        base_url=base_url,
        file_name=file_name,
        content_sha256=content_sha256,
        http_status=http_status,
        error_text=error_text,
        response_text=response_text,
        compact_text=compact_text,
    )


async def upload_auth_file_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    candidate: dict[str, Any],
    stop_event: asyncio.Event,
) -> dict[str, Any]:
    return await upload_auth_file_async_runtime_warden(
        session,
        semaphore,
        conn,
        settings,
        candidate,
        stop_event,
        claim_upload_slot=claim_upload_slot,
        mark_upload_attempt=mark_upload_attempt,
        mark_upload_success=mark_upload_success,
        mark_upload_failure=mark_upload_failure,
        mgmt_headers=mgmt_headers,
        maybe_json_loads=maybe_json_loads,
        compact_text=compact_text,
        backoff_seconds=retry_backoff_seconds,
        aiohttp_module=aiohttp,
    )


def summarize_upload_results(
    results: list[dict[str, Any]],
    *,
    discovered_count: int,
    selected_count: int,
    to_upload_count: int,
) -> None:
    summarize_upload_results_runtime_warden(
        results,
        discovered_count=discovered_count,
        selected_count=selected_count,
        to_upload_count=to_upload_count,
        compact_text=compact_text,
        logger=LOGGER,
    )


async def run_upload_async(
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    return await run_upload_async_service_warden(
        conn,
        settings,
        limit=limit,
        resolve_upload_name_scope=resolve_upload_name_scope,
        discover_upload_files=discover_upload_files,
        validate_and_digest_json_file=validate_and_digest_json_file,
        select_upload_candidates=select_upload_candidates,
        summarize_upload_results=summarize_upload_results,
        fetch_remote_auth_file_names=fetch_remote_auth_file_names,
        mark_upload_skipped_remote_exists=mark_upload_skipped_remote_exists,
        upload_auth_file_async=upload_auth_file_async,
        progress_log_step=progress_log_step,
        progress_reporter_factory=ProgressReporter,
        logger=LOGGER,
    )


def build_wham_usage_payload(auth_index: str, user_agent: str, chatgpt_account_id: str) -> dict[str, Any]:
    return build_wham_usage_payload_warden(
        auth_index,
        user_agent,
        chatgpt_account_id,
        usage_url=WHAM_USAGE_URL,
    )


async def probe_wham_usage_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    base_url: str,
    token: str,
    record: dict[str, Any],
    timeout: int,
    retries: int,
    user_agent: str,
) -> dict[str, Any]:
    return await probe_wham_usage_async_warden(
        session,
        semaphore,
        base_url,
        token,
        record,
        timeout,
        retries,
        user_agent,
        headers_builder=mgmt_headers,
        backoff_seconds=retry_backoff_seconds,
        now_iso=utc_now_iso,
        quota_signal_resolver=resolve_quota_signal,
        logger=LOGGER,
        usage_url=WHAM_USAGE_URL,
    )


def classify_account_state(record: dict[str, Any], *, quota_disable_threshold: float) -> dict[str, Any]:
    return classify_account_state_runtime_warden(
        record,
        quota_disable_threshold=quota_disable_threshold,
        resolve_quota_signal=resolve_quota_signal,
        resolve_quota_remaining_ratio=resolve_quota_remaining_ratio,
        utc_now_iso=utc_now_iso,
        logger=LOGGER,
    )


async def probe_accounts_async(
    records: list[dict[str, Any]],
    *,
    base_url: str,
    token: str,
    timeout: int,
    retries: int,
    user_agent: str,
    probe_workers: int,
    quota_disable_threshold: float,
    debug: bool,
) -> list[dict[str, Any]]:
    return await probe_accounts_async_runtime_warden(
        records,
        base_url=base_url,
        token=token,
        timeout=timeout,
        retries=retries,
        user_agent=user_agent,
        probe_workers=probe_workers,
        quota_disable_threshold=quota_disable_threshold,
        debug=debug,
        probe_wham_usage_async=probe_wham_usage_async,
        classify_account_state=classify_account_state,
        progress_log_step=progress_log_step,
        progress_reporter_factory=ProgressReporter,
        logger=LOGGER,
        aiohttp_module=aiohttp,
    )


def build_invalid_export_record(record: dict[str, Any]) -> dict[str, Any]:
    return build_invalid_export_record_warden(record)


def build_quota_export_record(record: dict[str, Any]) -> dict[str, Any]:
    return build_quota_export_record_warden(
        record,
        resolve_quota_signal=resolve_quota_signal,
        resolve_quota_remaining_ratio=resolve_quota_remaining_ratio,
        normalize_optional_ratio=normalize_optional_ratio,
    )


def export_records(path: str, rows: list[dict[str, Any]]) -> None:
    export_records_warden(path, rows)


def summarize_failures(records: list[dict[str, Any]], sample_limit: int = 3) -> None:
    summarize_failures_warden(
        records,
        sample_limit=sample_limit,
        compact_text=compact_text,
        logger=LOGGER,
    )


async def delete_account_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    base_url: str,
    token: str,
    name: str,
    timeout: int,
    delete_retries: int,
) -> dict[str, Any]:
    return await delete_account_async_warden(
        session,
        semaphore,
        base_url,
        token,
        name,
        timeout,
        delete_retries,
        headers_builder=mgmt_headers,
        maybe_json_loads=maybe_json_loads,
        compact_text=compact_text,
        logger=LOGGER,
    )


async def set_account_disabled_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    base_url: str,
    token: str,
    name: str,
    disabled: bool,
    timeout: int,
) -> dict[str, Any]:
    return await set_account_disabled_async_warden(
        session,
        semaphore,
        base_url,
        token,
        name,
        disabled,
        timeout,
        headers_builder=mgmt_headers,
        maybe_json_loads=maybe_json_loads,
        compact_text=compact_text,
    )


async def run_action_group_async(
    *,
    base_url: str,
    token: str,
    timeout: int,
    workers: int,
    items: list[str],
    fn_name: str,
    disabled: bool | None = None,
    delete_retries: int = 0,
    debug: bool = False,
) -> list[dict[str, Any]]:
    return await run_action_group_async_runtime_warden(
        base_url=base_url,
        token=token,
        timeout=timeout,
        workers=workers,
        items=items,
        fn_name=fn_name,
        disabled=disabled,
        delete_retries=delete_retries,
        debug=debug,
        delete_account_async=delete_account_async,
        set_account_disabled_async=set_account_disabled_async,
        progress_log_step=progress_log_step,
        progress_reporter_factory=ProgressReporter,
        logger=LOGGER,
        aiohttp_module=aiohttp,
    )


def apply_action_results(
    records_by_name: dict[str, dict[str, Any]],
    results: list[dict[str, Any]],
    *,
    action: str,
    managed_reason_on_success: str | None,
    disabled_value: int | None,
) -> list[dict[str, Any]]:
    return apply_action_results_runtime_warden(
        records_by_name,
        results,
        action=action,
        managed_reason_on_success=managed_reason_on_success,
        disabled_value=disabled_value,
        utc_now_iso=utc_now_iso,
        compact_text=compact_text,
        logger=LOGGER,
    )


def mark_quota_already_disabled(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return mark_quota_already_disabled_runtime_warden(
        records,
        utc_now_iso=utc_now_iso,
        logger=LOGGER,
    )


def print_scan_summary(
    *,
    total_files: int,
    candidate_records: list[dict[str, Any]],
    invalid_records: list[dict[str, Any]],
    quota_records: list[dict[str, Any]],
    recovered_records: list[dict[str, Any]],
) -> None:
    print_scan_summary_runtime_warden(
        total_files=total_files,
        candidate_records=candidate_records,
        invalid_records=invalid_records,
        quota_records=quota_records,
        recovered_records=recovered_records,
        logger=LOGGER,
    )


def summarize_action_results(label: str, results: list[dict[str, Any]]) -> None:
    summarize_action_results_runtime_warden(
        label,
        results,
        compact_text=compact_text,
        logger=LOGGER,
    )


def confirm_action(message: str, assume_yes: bool) -> bool:
    return confirm_action_runtime_warden(message, assume_yes, stdin=sys.stdin, input_fn=input, logger=LOGGER)


def export_current_results(
    invalid_output: str,
    quota_output: str,
    invalid_records: list[dict[str, Any]],
    quota_records: list[dict[str, Any]],
) -> None:
    export_current_results_warden(
        invalid_output,
        quota_output,
        invalid_records,
        quota_records,
        export_records_fn=export_records,
        build_invalid_export_record_fn=build_invalid_export_record,
        build_quota_export_record_fn=build_quota_export_record,
        logger=LOGGER,
    )


async def run_scan_async(
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    maintain_name_scope: set[str] | None = None,
) -> dict[str, Any]:
    return await run_scan_async_service_warden(
        conn,
        settings,
        maintain_name_scope=maintain_name_scope,
        start_scan_run=start_scan_run,
        finish_scan_run=finish_scan_run,
        utc_now_iso=utc_now_iso,
        fetch_auth_files=fetch_auth_files,
        load_existing_state=load_existing_state,
        get_item_name=get_item_name,
        build_auth_record=build_auth_record,
        upsert_auth_accounts=upsert_auth_accounts,
        matches_filters=matches_filters,
        probe_accounts_async=probe_accounts_async,
        print_scan_summary=print_scan_summary,
        summarize_failures=summarize_failures,
        export_current_results=export_current_results,
        logger=LOGGER,
    )


async def run_maintain_async(conn: sqlite3.Connection, settings: dict[str, Any]) -> dict[str, Any]:
    return await run_maintain_async_service_warden(
        conn,
        settings,
        resolve_maintain_name_scope=resolve_maintain_name_scope,
        run_scan_async=lambda _conn, _settings, _scope: run_scan_async(
            _conn,
            _settings,
            maintain_name_scope=_scope,
        ),
        run_action_group_async=run_action_group_async,
        confirm_action=confirm_action,
        apply_action_results=apply_action_results,
        upsert_auth_accounts=upsert_auth_accounts,
        mark_quota_already_disabled=mark_quota_already_disabled,
        summarize_action_results=summarize_action_results,
        logger=LOGGER,
    )


def count_valid_accounts(records: list[dict[str, Any]]) -> int:
    return count_valid_accounts_service_warden(records, row_to_bool=row_to_bool)


def compute_refill_upload_count(valid_count: int, min_valid_accounts: int, strategy: str) -> int:
    return compute_refill_upload_count_service_warden(valid_count, min_valid_accounts, strategy)


async def run_register_hook_async(settings: dict[str, Any], *, count: int) -> dict[str, Any]:
    return await run_register_hook_async_runtime_warden(
        settings,
        count=count,
        discover_upload_files=discover_upload_files,
        compact_text=compact_text,
        logger=LOGGER,
    )


async def run_maintain_refill_async(conn: sqlite3.Connection, settings: dict[str, Any]) -> dict[str, Any]:
    return await run_maintain_refill_async_service_warden(
        conn,
        settings,
        run_maintain_async=run_maintain_async,
        run_scan_async=run_scan_async,
        run_upload_async=lambda _conn, _settings, _limit: run_upload_async(
            _conn,
            _settings,
            limit=_limit,
        ),
        run_register_hook_async=lambda _settings, _count: run_register_hook_async(
            _settings,
            count=_count,
        ),
        row_to_bool=row_to_bool,
        logger=LOGGER,
    )


def prompt_string(label: str, default: str, *, secret: bool = False) -> str:
    return prompt_string_interactive(label, default, secret=secret)


def prompt_int(label: str, default: int, *, min_value: int = 0) -> int:
    return prompt_int_interactive(label, default, min_value=min_value)


def prompt_float(label: str, default: float, *, min_value: float = 0.0, max_value: float | None = None) -> float:
    return prompt_float_interactive(
        label,
        default,
        min_value=min_value,
        max_value=max_value,
    )


def prompt_yes_no(label: str, default: bool) -> bool:
    return prompt_yes_no_interactive(label, default)


def prompt_choice(label: str, options: list[str], default: str) -> str:
    return prompt_choice_interactive(label, options, default)


def choose_mode_interactive() -> str:
    print("\n请选择操作:")
    print("1) scan - 检测 401 和限额并导出")
    print("2) maintain - 删除 401、处理限额、恢复账号")
    print("3) upload - 从目录并发上传认证文件")
    print("4) maintain-refill - 维护后自动补充到最小有效账号数")
    print("0) exit")
    while True:
        choice = input("请输入选项编号: ").strip()
        if choice == "1":
            return "scan"
        if choice == "2":
            return "maintain"
        if choice == "3":
            return "upload"
        if choice == "4":
            return "maintain-refill"
        if choice == "0":
            return "exit"
        print("无效选项，请重新输入。")


def prompt_interactive_settings(settings: dict[str, Any]) -> dict[str, Any]:
    settings["timeout"] = prompt_int("请输入请求超时 timeout(秒)", settings["timeout"], min_value=1)
    settings["db_path"] = prompt_string("请输入 SQLite 路径 db_path", settings["db_path"])
    settings["log_file"] = prompt_string("请输入日志文件路径 log_file", settings["log_file"])
    settings["debug"] = prompt_yes_no("是否开启调试模式 debug", settings["debug"])

    if settings["mode"] in {"scan", "maintain", "maintain-refill"}:
        settings["probe_workers"] = prompt_int("请输入探测并发 probe_workers", settings["probe_workers"], min_value=1)
        settings["retries"] = prompt_int("请输入失败重试 retries", settings["retries"], min_value=0)
        settings["target_type"] = prompt_string("请输入 target_type", settings["target_type"])
        settings["provider"] = prompt_string("请输入 provider", settings["provider"])
        settings["quota_disable_threshold"] = prompt_float(
            "请输入限额禁用阈值 quota_disable_threshold(0~1)",
            settings["quota_disable_threshold"],
            min_value=0.0,
            max_value=1.0,
        )
        settings["invalid_output"] = prompt_string("请输入 401 导出路径 invalid_output", settings["invalid_output"])
        settings["quota_output"] = prompt_string("请输入限额导出路径 quota_output", settings["quota_output"])

    if settings["mode"] in {"maintain", "maintain-refill"}:
        settings["action_workers"] = prompt_int("请输入动作并发 action_workers", settings["action_workers"], min_value=1)
        settings["delete_retries"] = prompt_int("请输入删除失败重试 delete_retries", settings["delete_retries"], min_value=0)
        settings["delete_401"] = prompt_yes_no("是否自动删除 401 账号", settings["delete_401"])
        settings["quota_action"] = prompt_choice("限额账号动作 quota_action", ["disable", "delete"], settings["quota_action"])
        settings["auto_reenable"] = prompt_yes_no("是否自动重新启用恢复账号", settings["auto_reenable"])
        settings["reenable_scope"] = prompt_choice(
            "恢复范围 reenable_scope",
            ["signal", "managed"],
            settings["reenable_scope"],
        )
        settings["assume_yes"] = prompt_yes_no("是否跳过危险操作确认", settings["assume_yes"])

    if settings["mode"] in {"upload", "maintain-refill"}:
        settings["upload_dir"] = prompt_string("请输入上传目录 upload_dir", settings["upload_dir"])
        settings["upload_workers"] = prompt_int("请输入上传并发 upload_workers", settings["upload_workers"], min_value=1)
        settings["upload_retries"] = prompt_int("请输入上传重试 upload_retries", settings["upload_retries"], min_value=0)
        settings["upload_method"] = prompt_choice("上传方式 upload_method", ["json", "multipart"], settings["upload_method"])
        settings["upload_recursive"] = prompt_yes_no("是否递归上传子目录 upload_recursive", settings["upload_recursive"])
        settings["upload_force"] = prompt_yes_no("远端同名文件已存在时是否强制上传 upload_force", settings["upload_force"])

    if settings["mode"] == "maintain-refill":
        settings["min_valid_accounts"] = prompt_int("请输入最小有效账号数 min_valid_accounts", settings["min_valid_accounts"], min_value=1)
        settings["refill_strategy"] = prompt_choice("补充策略 refill_strategy", ["to-threshold", "fixed"], settings["refill_strategy"])
        settings["auto_register"] = prompt_yes_no("不足时是否启用外部注册钩子 auto_register", settings["auto_register"])
        settings["register_timeout"] = prompt_int("注册命令超时秒数 register_timeout", settings["register_timeout"], min_value=1)
        settings["register_workdir"] = prompt_string("注册命令工作目录 register_workdir", settings["register_workdir"])
        if settings["auto_register"]:
            settings["register_command"] = prompt_string("注册命令 register_command", settings["register_command"])

    return settings


def ensure_credentials(settings: dict[str, Any], interactive: bool) -> None:
    if not settings["base_url"] and interactive:
        settings["base_url"] = prompt_string("请输入 CPA base_url", "")
    if not settings["token"] and interactive:
        settings["token"] = prompt_string("请输入 CPA 管理 token", "", secret=True)

    settings["base_url"] = settings["base_url"].rstrip("/")

    if not settings["base_url"]:
        raise RuntimeError("缺少 base_url，请在配置文件中提供。")
    if not settings["token"]:
        raise RuntimeError("缺少 token，请在配置文件中提供。")


def parse_args() -> argparse.Namespace:
    return parse_cli_args(DEFAULT_CONFIG_PATH)


def run_async_or_exit(coro: Any) -> Any:
    try:
        return asyncio.run(coro)
    except KeyboardInterrupt:
        LOGGER.error("已中断。")
        sys.exit(130)
    except Exception as exc:
        LOGGER.error("错误: %s", exc)
        sys.exit(1)


def main() -> int:
    args = parse_args()
    ensure_aiohttp()

    interactive = False
    if args.mode is None:
        if not sys.stdin.isatty():
            print("错误: 未指定 --mode，且当前不是交互终端。", file=sys.stderr)
            return 1
        interactive = True
        mode = choose_mode_interactive()
        if mode == "exit":
            print("已退出。")
            return 0
        args.mode = mode
        args.config = prompt_string("配置文件路径", args.config)

    config_required = "--config" in sys.argv and not interactive
    conf = load_config_json(args.config, required=config_required)
    settings = build_settings(args, conf)

    if interactive:
        settings = prompt_interactive_settings(settings)

    configure_logging(settings["log_file"], settings["debug"])
    LOGGER.info("日志文件: %s", settings["log_file"])
    LOGGER.info("调试模式: %s", "on" if settings["debug"] else "off")

    ensure_credentials(settings, interactive)
    LOGGER.debug(
        "运行参数: mode=%s target_type=%s provider=%s probe_workers=%s action_workers=%s timeout=%s retries=%s delete_retries=%s quota_action=%s quota_disable_threshold=%s delete_401=%s auto_reenable=%s reenable_scope=%s maintain_names_file=%s upload_names_file=%s upload_dir=%s upload_workers=%s upload_retries=%s upload_method=%s upload_recursive=%s upload_force=%s min_valid_accounts=%s refill_strategy=%s auto_register=%s register_timeout=%s register_workdir=%s db_path=%s",
        settings["mode"],
        settings["target_type"],
        settings["provider"] or "",
        settings["probe_workers"],
        settings["action_workers"],
        settings["timeout"],
        settings["retries"],
        settings["delete_retries"],
        settings["quota_action"],
        settings["quota_disable_threshold"],
        settings["delete_401"],
        settings["auto_reenable"],
        settings["reenable_scope"],
        settings["maintain_names_file"],
        settings["upload_names_file"],
        settings["upload_dir"],
        settings["upload_workers"],
        settings["upload_retries"],
        settings["upload_method"],
        settings["upload_recursive"],
        settings["upload_force"],
        settings["min_valid_accounts"],
        settings["refill_strategy"],
        settings["auto_register"],
        settings["register_timeout"],
        settings["register_workdir"],
        settings["db_path"],
    )

    conn = connect_db(settings["db_path"])
    try:
        init_db(conn)
        if settings["mode"] == "scan":
            run_async_or_exit(run_scan_async(conn, settings))
            return 0
        if settings["mode"] == "maintain":
            run_async_or_exit(run_maintain_async(conn, settings))
            return 0
        if settings["mode"] == "maintain-refill":
            run_async_or_exit(run_maintain_refill_async(conn, settings))
            return 0
        run_async_or_exit(run_upload_async(conn, settings))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
