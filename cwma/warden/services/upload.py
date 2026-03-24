from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable


async def _default_dispatch_uploads(
    conn: Any,
    settings: dict[str, Any],
    upload_candidates: list[dict[str, Any]],
    *,
    upload_auth_file_async: Callable[..., Awaitable[dict[str, Any]]],
    progress_log_step: Callable[[int], int],
    progress_reporter_factory: Callable[..., Any],
    logger: Any,
) -> list[dict[str, Any]]:
    try:
        import aiohttp
    except Exception as exc:
        raise RuntimeError("未安装 aiohttp，无法执行上传任务") from exc

    connector = aiohttp.TCPConnector(
        limit=max(1, settings["upload_workers"]),
        limit_per_host=max(1, settings["upload_workers"]),
    )
    client_timeout = aiohttp.ClientTimeout(total=max(1, settings["timeout"]))
    semaphore = asyncio.Semaphore(max(1, settings["upload_workers"]))
    stop_event = asyncio.Event()
    results: list[dict[str, Any]] = []

    async with aiohttp.ClientSession(connector=connector, timeout=client_timeout, trust_env=True) as session:
        tasks = [
            asyncio.create_task(
                upload_auth_file_async(
                    session,
                    semaphore,
                    conn,
                    settings,
                    candidate,
                    stop_event,
                )
            )
            for candidate in upload_candidates
        ]
        done = 0
        total = len(tasks)
        report_step = progress_log_step(total)
        next_report = report_step
        with progress_reporter_factory("上传认证文件", total, debug=settings["debug"]) as progress:
            for task in asyncio.as_completed(tasks):
                results.append(await task)
                done += 1
                progress.advance()
                if (not progress.enabled) and (done >= next_report or done == total):
                    logger.info("上传进度: %s/%s", done, total)
                    next_report += report_step
    return results


async def run_upload_async(
    conn: Any,
    settings: dict[str, Any],
    *,
    limit: int | None,
    resolve_upload_name_scope: Callable[[dict[str, Any]], set[str] | None],
    discover_upload_files: Callable[[str, bool], list[Path]],
    validate_and_digest_json_file: Callable[[Path], dict[str, Any]],
    select_upload_candidates: Callable[
        [list[dict[str, Any]]],
        tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]],
    ],
    summarize_upload_results: Callable[..., None],
    fetch_remote_auth_file_names: Callable[[str, str, int], set[str]],
    mark_upload_skipped_remote_exists: Callable[..., None],
    upload_auth_file_async: Callable[..., Awaitable[dict[str, Any]]],
    progress_log_step: Callable[[int], int],
    progress_reporter_factory: Callable[..., Any],
    logger: Any,
    dispatch_uploads: Callable[..., Awaitable[list[dict[str, Any]]]] = _default_dispatch_uploads,
) -> dict[str, Any]:
    upload_name_scope = resolve_upload_name_scope(settings)
    logger.info(
        "开始上传认证文件: dir=%s recursive=%s workers=%s retries=%s method=%s force=%s limit=%s",
        settings["upload_dir"],
        settings["upload_recursive"],
        settings["upload_workers"],
        settings["upload_retries"],
        settings["upload_method"],
        settings["upload_force"],
        limit if limit is not None else "all",
    )
    if upload_name_scope is None:
        logger.info("上传范围: full")
    else:
        logger.info("上传范围: incremental names=%s", len(upload_name_scope))
    discovered_paths = discover_upload_files(settings["upload_dir"], settings["upload_recursive"])
    if not discovered_paths:
        logger.info("未发现可上传的 .json 文件")
        return {"results": []}

    validated_files: list[dict[str, Any]] = []
    pre_results: list[dict[str, Any]] = []
    for path in discovered_paths:
        try:
            validated_files.append(validate_and_digest_json_file(path))
        except Exception as exc:
            pre_results.append(
                {
                    "file_name": path.name,
                    "file_path": str(path),
                    "status_code": None,
                    "ok": False,
                    "outcome": "validation_failed",
                    "error": str(exc),
                    "error_kind": "validation",
                }
            )

    selected_candidates, skipped_local_duplicates, conflicting_names = select_upload_candidates(validated_files)
    pre_results.extend(skipped_local_duplicates)
    if upload_name_scope is not None:
        selected_candidates = [
            candidate
            for candidate in selected_candidates
            if candidate["file_name"] in upload_name_scope
        ]
        conflicting_names = {
            name: rows
            for name, rows in conflicting_names.items()
            if name in upload_name_scope
        }
    if conflicting_names:
        samples: list[str] = []
        for name, rows in list(sorted(conflicting_names.items(), key=lambda item: item[0]))[:10]:
            samples.append(f"{name}({len(rows)} paths)")
        raise RuntimeError("检测到同名不同内容的本地文件，已停止上传: " + ", ".join(samples))
    if not selected_candidates:
        summarize_upload_results(
            pre_results,
            discovered_count=len(discovered_paths),
            selected_count=0,
            to_upload_count=0,
        )
        failed_count = sum(1 for row in pre_results if row.get("outcome") in {"validation_failed", "upload_failed"})
        if failed_count > 0:
            raise RuntimeError(f"上传完成但存在失败文件: {failed_count}")
        logger.info("上传流程完成")
        return {"results": pre_results}

    remote_names: set[str] = set()
    if not settings["upload_force"]:
        remote_names = fetch_remote_auth_file_names(settings["base_url"], settings["token"], settings["timeout"])
    upload_candidates: list[dict[str, Any]] = []
    for candidate in selected_candidates:
        if (not settings["upload_force"]) and candidate["file_name"] in remote_names:
            mark_upload_skipped_remote_exists(
                conn,
                base_url=settings["base_url"],
                file_name=candidate["file_name"],
                content_sha256=candidate["content_sha256"],
                file_path=candidate["file_path"],
                file_size=candidate["file_size"],
            )
            pre_results.append(
                {
                    "file_name": candidate["file_name"],
                    "file_path": candidate["file_path"],
                    "status_code": None,
                    "ok": True,
                    "outcome": "skipped_remote_exists",
                    "error": None,
                    "error_kind": None,
                }
            )
            continue
        upload_candidates.append(candidate)

    if limit is not None:
        if limit < 0:
            raise RuntimeError("upload limit 不能小于 0")
        upload_candidates = upload_candidates[:limit]

    if not upload_candidates:
        results = list(pre_results)
        summarize_upload_results(
            results,
            discovered_count=len(discovered_paths),
            selected_count=len(selected_candidates),
            to_upload_count=0,
        )
        failed_count = sum(1 for row in results if row.get("outcome") in {"validation_failed", "upload_failed"})
        if failed_count > 0:
            raise RuntimeError(f"上传完成但存在失败文件: {failed_count}")
        logger.info("上传流程完成")
        return {"results": results}

    results = list(pre_results)
    results.extend(
        await dispatch_uploads(
            conn,
            settings,
            upload_candidates,
            upload_auth_file_async=upload_auth_file_async,
            progress_log_step=progress_log_step,
            progress_reporter_factory=progress_reporter_factory,
            logger=logger,
        )
    )

    summarize_upload_results(
        results,
        discovered_count=len(discovered_paths),
        selected_count=len(selected_candidates),
        to_upload_count=len(upload_candidates),
    )

    if any(row.get("error_kind") == "core_auth_manager_unavailable" for row in results):
        raise RuntimeError("core auth manager unavailable")
    failed_count = sum(1 for row in results if row.get("outcome") in {"validation_failed", "upload_failed"})
    if failed_count > 0:
        raise RuntimeError(f"上传完成但存在失败文件: {failed_count}")

    logger.info("上传流程完成")
    return {"results": results}
