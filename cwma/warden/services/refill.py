from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable
from typing import Any


def count_valid_accounts(records: list[dict[str, Any]], *, row_to_bool: Callable[[Any], bool]) -> int:
    valid = 0
    for row in records:
        if row_to_bool(row.get("disabled")):
            continue
        if int(row.get("is_invalid_401") or 0) == 1:
            continue
        if int(row.get("is_quota_limited") or 0) == 1:
            continue
        if row.get("probe_error_kind"):
            continue
        valid += 1
    return valid


def compute_refill_upload_count(valid_count: int, min_valid_accounts: int, strategy: str) -> int:
    if valid_count >= min_valid_accounts:
        return 0
    gap = min_valid_accounts - valid_count
    if strategy == "fixed":
        return min_valid_accounts
    return gap


async def run_maintain_refill_async(
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    *,
    run_maintain_async: Callable[[sqlite3.Connection, dict[str, Any]], Awaitable[dict[str, Any]]],
    run_scan_async: Callable[[sqlite3.Connection, dict[str, Any]], Awaitable[dict[str, Any]]],
    run_upload_async: Callable[[sqlite3.Connection, dict[str, Any], int | None], Awaitable[dict[str, Any]]],
    run_register_hook_async: Callable[[dict[str, Any], int], Awaitable[dict[str, Any]]],
    row_to_bool: Callable[[Any], bool],
    logger: Any,
) -> dict[str, Any]:
    min_valid_accounts = int(settings["min_valid_accounts"])
    refill_strategy = str(settings["refill_strategy"])

    logger.info(
        "开始维护补充: min_valid_accounts=%s refill_strategy=%s auto_register=%s",
        min_valid_accounts,
        refill_strategy,
        bool(settings["auto_register"]),
    )
    maintain_result = await run_maintain_async(conn, settings)

    after_maintain_scan = await run_scan_async(conn, settings)
    valid_after_maintain = count_valid_accounts(
        after_maintain_scan["candidate_records"],
        row_to_bool=row_to_bool,
    )
    logger.info("维护后有效账号数: %s (目标 >= %s)", valid_after_maintain, min_valid_accounts)

    refill_upload_result: dict[str, Any] | None = None
    after_refill_scan = after_maintain_scan
    valid_after_refill = valid_after_maintain
    refill_upload_count = compute_refill_upload_count(
        valid_after_maintain,
        min_valid_accounts,
        refill_strategy,
    )

    if refill_upload_count > 0:
        logger.info("触发补充上传: count=%s strategy=%s", refill_upload_count, refill_strategy)
        refill_upload_result = await run_upload_async(conn, settings, refill_upload_count)
        after_refill_scan = await run_scan_async(conn, settings)
        valid_after_refill = count_valid_accounts(
            after_refill_scan["candidate_records"],
            row_to_bool=row_to_bool,
        )
        logger.info("补充上传后有效账号数: %s (目标 >= %s)", valid_after_refill, min_valid_accounts)

    register_result: dict[str, Any] | None = None
    register_upload_result: dict[str, Any] | None = None
    final_scan = after_refill_scan
    final_valid_count = valid_after_refill
    if final_valid_count < min_valid_accounts:
        if not settings["auto_register"]:
            raise RuntimeError(
                f"维护补充后有效账号不足: valid={final_valid_count}, min_required={min_valid_accounts}"
            )

        register_count = min_valid_accounts - final_valid_count
        register_result = await run_register_hook_async(settings, register_count)
        register_upload_result = await run_upload_async(conn, settings, register_count)
        final_scan = await run_scan_async(conn, settings)
        final_valid_count = count_valid_accounts(
            final_scan["candidate_records"],
            row_to_bool=row_to_bool,
        )
        logger.info("注册并上传后有效账号数: %s (目标 >= %s)", final_valid_count, min_valid_accounts)

    if final_valid_count < min_valid_accounts:
        raise RuntimeError(
            f"最终有效账号仍不足: valid={final_valid_count}, min_required={min_valid_accounts}"
        )

    logger.info("维护补充完成: final_valid=%s min_required=%s", final_valid_count, min_valid_accounts)
    return {
        "maintain": maintain_result,
        "after_maintain_scan": after_maintain_scan,
        "refill_upload_count": int(refill_upload_count),
        "refill_upload_result": refill_upload_result,
        "after_refill_scan": after_refill_scan,
        "register_result": register_result,
        "register_upload_result": register_upload_result,
        "final_scan": final_scan,
        "final_valid_count": int(final_valid_count),
    }
