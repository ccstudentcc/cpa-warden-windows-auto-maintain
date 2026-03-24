from __future__ import annotations

import asyncio
import os
import shlex
import sys
import urllib.parse
from collections import Counter
from pathlib import Path
from typing import Any, Awaitable, Callable


async def upload_auth_file_async(
    session: Any,
    semaphore: asyncio.Semaphore,
    conn: Any,
    settings: dict[str, Any],
    candidate: dict[str, Any],
    stop_event: asyncio.Event,
    *,
    claim_upload_slot: Callable[..., str],
    mark_upload_attempt: Callable[..., None],
    mark_upload_success: Callable[..., None],
    mark_upload_failure: Callable[..., None],
    mgmt_headers: Callable[..., dict[str, str]],
    maybe_json_loads: Callable[[Any], Any],
    compact_text: Callable[[Any, int], str | None],
    backoff_seconds: Callable[[int], float],
    aiohttp_module: Any,
) -> dict[str, Any]:
    base_url = settings["base_url"]
    token = settings["token"]
    timeout = settings["timeout"]
    upload_retries = settings["upload_retries"]
    upload_method = settings["upload_method"]
    file_name = str(candidate["file_name"])
    file_path = str(candidate["file_path"])
    content_sha256 = str(candidate["content_sha256"])
    file_size = int(candidate["file_size"])

    if stop_event.is_set():
        return {
            "file_name": file_name,
            "file_path": file_path,
            "status_code": None,
            "ok": False,
            "outcome": "skipped_due_to_stop",
            "error": "stopped: core auth manager unavailable",
            "error_kind": "core_auth_manager_unavailable",
        }

    claim_state = claim_upload_slot(
        conn,
        base_url=base_url,
        file_name=file_name,
        content_sha256=content_sha256,
        file_path=file_path,
        file_size=file_size,
    )
    if claim_state == "skipped_done":
        return {
            "file_name": file_name,
            "file_path": file_path,
            "status_code": None,
            "ok": True,
            "outcome": "skipped_already_uploaded",
            "error": None,
            "error_kind": None,
        }
    if claim_state == "skipped_in_progress":
        return {
            "file_name": file_name,
            "file_path": file_path,
            "status_code": None,
            "ok": True,
            "outcome": "skipped_in_progress",
            "error": None,
            "error_kind": None,
        }

    if stop_event.is_set():
        mark_upload_failure(
            conn,
            base_url=base_url,
            file_name=file_name,
            content_sha256=content_sha256,
            http_status=None,
            error_text="stopped: core auth manager unavailable",
            response_text=None,
        )
        return {
            "file_name": file_name,
            "file_path": file_path,
            "status_code": None,
            "ok": False,
            "outcome": "skipped_due_to_stop",
            "error": "stopped: core auth manager unavailable",
            "error_kind": "core_auth_manager_unavailable",
        }

    encoded_name = urllib.parse.quote(file_name, safe="")
    json_upload_url = f"{base_url.rstrip('/')}/v0/management/auth-files?name={encoded_name}"
    multipart_upload_url = f"{base_url.rstrip('/')}/v0/management/auth-files"

    for attempt in range(upload_retries + 1):
        mark_upload_attempt(
            conn,
            base_url=base_url,
            file_name=file_name,
            content_sha256=content_sha256,
        )
        try:
            async with semaphore:
                if stop_event.is_set():
                    mark_upload_failure(
                        conn,
                        base_url=base_url,
                        file_name=file_name,
                        content_sha256=content_sha256,
                        http_status=None,
                        error_text="stopped: core auth manager unavailable",
                        response_text=None,
                    )
                    return {
                        "file_name": file_name,
                        "file_path": file_path,
                        "status_code": None,
                        "ok": False,
                        "outcome": "skipped_due_to_stop",
                        "error": "stopped: core auth manager unavailable",
                        "error_kind": "core_auth_manager_unavailable",
                    }

                if upload_method == "multipart":
                    form_data = aiohttp_module.FormData()
                    form_data.add_field(
                        "file",
                        candidate["content_bytes"],
                        filename=file_name,
                        content_type="application/json",
                    )
                    req_headers = mgmt_headers(token)
                    request_kwargs: dict[str, Any] = {"data": form_data}
                    request_url = multipart_upload_url
                else:
                    req_headers = mgmt_headers(token, include_json=True)
                    request_kwargs = {"data": candidate["content_text"]}
                    request_url = json_upload_url

                async with session.post(
                    request_url,
                    headers=req_headers,
                    timeout=timeout,
                    **request_kwargs,
                ) as resp:
                    text = await resp.text()
                    data = maybe_json_loads(text)
                    ok = resp.status == 200 and isinstance(data, dict) and data.get("status") == "ok"
                    if ok:
                        mark_upload_success(
                            conn,
                            base_url=base_url,
                            file_name=file_name,
                            content_sha256=content_sha256,
                            http_status=resp.status,
                            response_text=text,
                        )
                        return {
                            "file_name": file_name,
                            "file_path": file_path,
                            "status_code": resp.status,
                            "ok": True,
                            "outcome": "uploaded_success",
                            "error": None,
                            "error_kind": None,
                        }

                    resp_error = ""
                    if isinstance(data, dict):
                        resp_error = str(data.get("error") or "").strip()
                    is_core_unavailable = resp.status == 503 and (
                        resp_error == "core auth manager unavailable"
                        or "core auth manager unavailable" in str(text).lower()
                    )
                    if is_core_unavailable:
                        stop_event.set()
                        mark_upload_failure(
                            conn,
                            base_url=base_url,
                            file_name=file_name,
                            content_sha256=content_sha256,
                            http_status=resp.status,
                            error_text="core auth manager unavailable",
                            response_text=text,
                        )
                        return {
                            "file_name": file_name,
                            "file_path": file_path,
                            "status_code": resp.status,
                            "ok": False,
                            "outcome": "upload_failed",
                            "error": "core auth manager unavailable",
                            "error_kind": "core_auth_manager_unavailable",
                        }

                    should_retry = resp.status == 429 or resp.status >= 500
                    error_text = resp_error or compact_text(text, 240) or f"http {resp.status}"
                    if should_retry and attempt < upload_retries:
                        await asyncio.sleep(backoff_seconds(attempt))
                        continue

                    mark_upload_failure(
                        conn,
                        base_url=base_url,
                        file_name=file_name,
                        content_sha256=content_sha256,
                        http_status=resp.status,
                        error_text=error_text,
                        response_text=text,
                    )
                    return {
                        "file_name": file_name,
                        "file_path": file_path,
                        "status_code": resp.status,
                        "ok": False,
                        "outcome": "upload_failed",
                        "error": error_text,
                        "error_kind": None,
                    }
        except asyncio.TimeoutError:
            if attempt < upload_retries:
                await asyncio.sleep(backoff_seconds(attempt))
                continue
            mark_upload_failure(
                conn,
                base_url=base_url,
                file_name=file_name,
                content_sha256=content_sha256,
                http_status=None,
                error_text="timeout",
                response_text=None,
            )
            return {
                "file_name": file_name,
                "file_path": file_path,
                "status_code": None,
                "ok": False,
                "outcome": "upload_failed",
                "error": "timeout",
                "error_kind": "timeout",
            }
        except Exception as exc:
            if attempt < upload_retries:
                await asyncio.sleep(backoff_seconds(attempt))
                continue
            mark_upload_failure(
                conn,
                base_url=base_url,
                file_name=file_name,
                content_sha256=content_sha256,
                http_status=None,
                error_text=str(exc),
                response_text=None,
            )
            return {
                "file_name": file_name,
                "file_path": file_path,
                "status_code": None,
                "ok": False,
                "outcome": "upload_failed",
                "error": str(exc),
                "error_kind": "other",
            }

    mark_upload_failure(
        conn,
        base_url=base_url,
        file_name=file_name,
        content_sha256=content_sha256,
        http_status=None,
        error_text="upload failed after retries",
        response_text=None,
    )
    return {
        "file_name": file_name,
        "file_path": file_path,
        "status_code": None,
        "ok": False,
        "outcome": "upload_failed",
        "error": "upload failed after retries",
        "error_kind": None,
    }


def summarize_upload_results(
    results: list[dict[str, Any]],
    *,
    discovered_count: int,
    selected_count: int,
    to_upload_count: int,
    compact_text: Callable[[Any, int], str | None],
    logger: Any,
) -> None:
    counter = Counter(str(row.get("outcome") or "unknown") for row in results)
    logger.info("上传扫描文件数: %s", discovered_count)
    logger.info("上传候选文件数: %s", selected_count)
    logger.info("需要实际上传数: %s", to_upload_count)
    logger.info("上传成功: %s", counter.get("uploaded_success", 0))
    logger.info("跳过(已上传): %s", counter.get("skipped_already_uploaded", 0))
    logger.info("跳过(远端已存在): %s", counter.get("skipped_remote_exists", 0))
    logger.info("跳过(进行中): %s", counter.get("skipped_in_progress", 0))
    logger.info("跳过(本地重复): %s", counter.get("skipped_local_duplicate", 0))
    logger.info("校验失败: %s", counter.get("validation_failed", 0))
    logger.info("上传失败: %s", counter.get("upload_failed", 0))

    failed = [row for row in results if row.get("outcome") in {"validation_failed", "upload_failed"}]
    for row in failed[:10]:
        logger.warning(
            "[上传失败] %s | status=%s | %s",
            row.get("file_name") or row.get("file_path"),
            row.get("status_code"),
            compact_text(row.get("error"), 200) or "-",
        )


def classify_account_state(
    record: dict[str, Any],
    *,
    quota_disable_threshold: float,
    resolve_quota_signal: Callable[[dict[str, Any]], tuple[int | None, int | None, str]],
    resolve_quota_remaining_ratio: Callable[[dict[str, Any]], tuple[float | None, str]],
    utc_now_iso: Callable[[], str],
    logger: Any,
) -> dict[str, Any]:
    invalid_401 = bool(record.get("unavailable")) or record.get("api_status_code") == 401
    effective_limit_reached, effective_allowed, quota_signal_source = resolve_quota_signal(record)
    effective_remaining_ratio, remaining_ratio_source = resolve_quota_remaining_ratio(record)
    quota_limited_by_threshold = (
        quota_disable_threshold > 0
        and effective_remaining_ratio is not None
        and effective_remaining_ratio <= quota_disable_threshold
    )
    quota_limited = (
        not invalid_401
        and not bool(record.get("unavailable"))
        and record.get("api_status_code") == 200
        and (effective_limit_reached == 1 or quota_limited_by_threshold)
    )
    recovered = (
        not invalid_401
        and not quota_limited
        and bool(record.get("disabled"))
        and record.get("api_status_code") == 200
        and effective_allowed == 1
        and effective_limit_reached == 0
    )

    record["quota_signal_source"] = quota_signal_source
    record["quota_remaining_ratio"] = effective_remaining_ratio
    record["quota_remaining_ratio_source"] = remaining_ratio_source
    record["quota_threshold_triggered"] = int(quota_limited_by_threshold)
    record["is_invalid_401"] = int(invalid_401)
    record["is_quota_limited"] = int(quota_limited)
    record["is_recovered"] = int(recovered)
    record["updated_at"] = utc_now_iso()
    if quota_limited_by_threshold and effective_limit_reached != 1:
        logger.info(
            "限额阈值触发: name=%s remaining_ratio=%.4f threshold=%.4f signal_source=%s ratio_source=%s",
            record.get("name"),
            effective_remaining_ratio,
            quota_disable_threshold,
            quota_signal_source,
            remaining_ratio_source,
        )
    return record


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
    probe_wham_usage_async: Callable[..., Awaitable[dict[str, Any]]],
    classify_account_state: Callable[..., dict[str, Any]],
    progress_log_step: Callable[[int], int],
    progress_reporter_factory: Callable[..., Any],
    logger: Any,
    aiohttp_module: Any,
) -> list[dict[str, Any]]:
    if not records:
        return []

    logger.info(
        "开始并发探测 wham/usage: candidates=%s workers=%s timeout=%ss retries=%s",
        len(records),
        probe_workers,
        timeout,
        retries,
    )

    connector = aiohttp_module.TCPConnector(limit=max(1, probe_workers), limit_per_host=max(1, probe_workers))
    client_timeout = aiohttp_module.ClientTimeout(total=max(1, timeout))
    semaphore = asyncio.Semaphore(max(1, probe_workers))

    results: list[dict[str, Any]] = []
    async with aiohttp_module.ClientSession(connector=connector, timeout=client_timeout, trust_env=True) as session:
        tasks = [
            asyncio.create_task(
                probe_wham_usage_async(
                    session,
                    semaphore,
                    base_url,
                    token,
                    record,
                    timeout,
                    retries,
                    user_agent,
                )
            )
            for record in records
        ]

        done = 0
        total = len(tasks)
        report_step = progress_log_step(total)
        next_report = report_step
        with progress_reporter_factory("探测账号", total, debug=debug) as progress:
            for task in asyncio.as_completed(tasks):
                probed = classify_account_state(await task, quota_disable_threshold=quota_disable_threshold)
                results.append(probed)
                done += 1
                progress.advance()
                if (not progress.enabled) and (done >= next_report or done == total):
                    logger.info("探测进度: %s/%s", done, total)
                    next_report += report_step

    return results


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
    delete_account_async: Callable[..., Awaitable[dict[str, Any]]],
    set_account_disabled_async: Callable[..., Awaitable[dict[str, Any]]],
    progress_log_step: Callable[[int], int],
    progress_reporter_factory: Callable[..., Any],
    logger: Any,
    aiohttp_module: Any,
) -> list[dict[str, Any]]:
    if not items:
        return []

    connector = aiohttp_module.TCPConnector(limit=max(1, workers), limit_per_host=max(1, workers))
    client_timeout = aiohttp_module.ClientTimeout(total=max(1, timeout))
    semaphore = asyncio.Semaphore(max(1, workers))

    async with aiohttp_module.ClientSession(connector=connector, timeout=client_timeout, trust_env=True) as session:
        tasks = []
        for name in items:
            if fn_name == "delete":
                tasks.append(
                    asyncio.create_task(
                        delete_account_async(
                            session,
                            semaphore,
                            base_url,
                            token,
                            name,
                            timeout,
                            delete_retries=max(0, int(delete_retries)),
                        )
                    )
                )
            else:
                tasks.append(
                    asyncio.create_task(
                        set_account_disabled_async(
                            session,
                            semaphore,
                            base_url,
                            token,
                            name,
                            bool(disabled),
                            timeout,
                        )
                    )
                )

        results: list[dict[str, Any]] = []
        done = 0
        total = len(tasks)
        report_step = progress_log_step(total)
        next_report = report_step
        action_label = "删除" if fn_name == "delete" else ("禁用" if disabled else "启用")
        with progress_reporter_factory(f"{action_label}账号", total, debug=debug) as progress:
            for task in asyncio.as_completed(tasks):
                results.append(await task)
                done += 1
                progress.advance()
                if (not progress.enabled) and (done >= next_report or done == total):
                    logger.info("%s进度: %s/%s", action_label, done, total)
                    next_report += report_step
        return results


def apply_action_results(
    records_by_name: dict[str, dict[str, Any]],
    results: list[dict[str, Any]],
    *,
    action: str,
    managed_reason_on_success: str | None,
    disabled_value: int | None,
    utc_now_iso: Callable[[], str],
    compact_text: Callable[[Any, int], str | None],
    logger: Any,
) -> list[dict[str, Any]]:
    updated: list[dict[str, Any]] = []
    now_iso = utc_now_iso()
    for result in results:
        name = result.get("name")
        record = records_by_name.get(name)
        if not record:
            continue
        record["last_action"] = action
        record["last_action_status"] = "success" if result.get("ok") else "failed"
        record["last_action_error"] = result.get("error")
        record["updated_at"] = now_iso
        if result.get("ok"):
            if managed_reason_on_success is None:
                record["managed_reason"] = None
            else:
                record["managed_reason"] = managed_reason_on_success
            if disabled_value is not None:
                record["disabled"] = disabled_value
            logger.debug("动作成功: action=%s name=%s", action, name)
        else:
            logger.debug(
                "动作失败: action=%s name=%s status_code=%s error=%s",
                action,
                name,
                result.get("status_code"),
                compact_text(result.get("error"), 200),
            )
        updated.append(record)
    return updated


def mark_quota_already_disabled(
    records: list[dict[str, Any]],
    *,
    utc_now_iso: Callable[[], str],
    logger: Any,
) -> list[dict[str, Any]]:
    now_iso = utc_now_iso()
    updated = []
    for record in records:
        record["managed_reason"] = "quota_disabled"
        record["last_action"] = "mark_quota_disabled"
        record["last_action_status"] = "success"
        record["last_action_error"] = None
        record["updated_at"] = now_iso
        logger.debug("标记已禁用限额账号: name=%s", record.get("name"))
        updated.append(record)
    return updated


def print_scan_summary(
    *,
    total_files: int,
    candidate_records: list[dict[str, Any]],
    invalid_records: list[dict[str, Any]],
    quota_records: list[dict[str, Any]],
    recovered_records: list[dict[str, Any]],
    logger: Any,
) -> None:
    status_counter = Counter(str(row.get("status") or "") for row in candidate_records)
    logger.info("总认证文件数: %s", total_files)
    logger.info("符合过滤条件账号数: %s", len(candidate_records))
    logger.info("401 账号数: %s", len(invalid_records))
    logger.info("限额账号数: %s", len(quota_records))
    logger.info("恢复候选账号数: %s", len(recovered_records))
    logger.debug("状态分布: %s", dict(sorted(status_counter.items(), key=lambda item: item[0])))


def summarize_action_results(
    label: str,
    results: list[dict[str, Any]],
    *,
    compact_text: Callable[[Any, int], str | None],
    logger: Any,
) -> None:
    if not results:
        logger.info("%s: 0", label)
        return
    success = [row for row in results if row.get("ok")]
    failed = [row for row in results if not row.get("ok")]
    logger.info("%s: 成功=%s，失败=%s", label, len(success), len(failed))
    for row in failed[:10]:
        logger.warning("[%s失败] %s | %s", label, row.get("name"), compact_text(row.get("error"), 160) or "-")


def confirm_action(
    message: str,
    assume_yes: bool,
    *,
    stdin: Any = sys.stdin,
    input_fn: Callable[[str], str] = input,
    logger: Any,
) -> bool:
    if assume_yes:
        return True
    if not stdin.isatty():
        logger.warning("缺少交互终端，已取消: %s", message)
        return False
    answer = input_fn(f"{message}，输入 DELETE 确认: ").strip()
    return answer == "DELETE"


async def run_register_hook_async(
    settings: dict[str, Any],
    *,
    count: int,
    discover_upload_files: Callable[[str, bool], list[Path]],
    compact_text: Callable[[Any, int], str | None],
    logger: Any,
) -> dict[str, Any]:
    if count <= 0:
        return {"executed": False, "requested_count": 0, "new_files": 0}

    command = str(settings.get("register_command") or "").strip()
    if not command:
        raise RuntimeError("register_command 为空，无法执行 auto_register")

    upload_dir = Path(str(settings["upload_dir"])).expanduser()
    upload_dir.mkdir(parents=True, exist_ok=True)
    before_files = {
        str(path)
        for path in discover_upload_files(str(upload_dir), bool(settings["upload_recursive"]))
    }

    cwd = str(settings.get("register_workdir") or "").strip()
    cwd_path = Path(cwd).expanduser() if cwd else None
    if cwd_path is not None and (not cwd_path.exists() or not cwd_path.is_dir()):
        raise RuntimeError(f"register_workdir 不存在或不是目录: {cwd}")

    cmd = (
        f"{command} "
        f"--count {shlex.quote(str(count))} "
        f"--output-dir {shlex.quote(str(upload_dir))}"
    )
    env = os.environ.copy()
    env["CPA_REGISTER_COUNT"] = str(count)
    env["CPA_REGISTER_OUTPUT_DIR"] = str(upload_dir)

    logger.info("执行外部注册钩子: count=%s command=%s", count, command)
    process = await asyncio.create_subprocess_shell(
        cmd,
        cwd=str(cwd_path) if cwd_path is not None else None,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            process.communicate(),
            timeout=int(settings["register_timeout"]),
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError(f"register command timeout after {settings['register_timeout']}s")

    stdout_text = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
    stderr_text = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
    if process.returncode != 0:
        raise RuntimeError(
            f"register command failed: exit={process.returncode}, stderr={compact_text(stderr_text, 240) or '-'}"
        )

    after_files = {
        str(path)
        for path in discover_upload_files(str(upload_dir), bool(settings["upload_recursive"]))
    }
    new_files = max(0, len(after_files - before_files))
    if new_files < 1:
        raise RuntimeError("register command succeeded but produced no new .json files")

    logger.info("外部注册钩子完成: 新增文件=%s", new_files)
    if stdout_text.strip():
        logger.debug("register stdout: %s", compact_text(stdout_text, 800))
    if stderr_text.strip():
        logger.debug("register stderr: %s", compact_text(stderr_text, 800))

    return {
        "executed": True,
        "requested_count": int(count),
        "new_files": int(new_files),
        "return_code": int(process.returncode),
    }
