from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable
from typing import Any


async def run_maintain_async(
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    *,
    resolve_maintain_name_scope: Callable[[dict[str, Any]], set[str] | None],
    run_scan_async: Callable[
        [sqlite3.Connection, dict[str, Any], set[str] | None],
        Awaitable[dict[str, Any]],
    ],
    run_action_group_async: Callable[..., Awaitable[list[dict[str, Any]]]],
    confirm_action: Callable[[str, bool], bool],
    apply_action_results: Callable[..., list[dict[str, Any]]],
    upsert_auth_accounts: Callable[[sqlite3.Connection, list[dict[str, Any]]], None],
    mark_quota_already_disabled: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    summarize_action_results: Callable[[str, list[dict[str, Any]]], None],
    logger: Any,
) -> dict[str, Any]:
    maintain_name_scope = resolve_maintain_name_scope(settings)
    logger.info(
        "开始维护: delete_401=%s quota_action=%s quota_disable_threshold=%s auto_reenable=%s reenable_scope=%s delete_retries=%s",
        settings["delete_401"],
        settings["quota_action"],
        settings["quota_disable_threshold"],
        settings["auto_reenable"],
        settings["reenable_scope"],
        settings["delete_retries"],
    )
    if maintain_name_scope is None:
        logger.info("维护范围: full")
    else:
        logger.info("维护范围: incremental names=%s", len(maintain_name_scope))
    scan_result = await run_scan_async(conn, settings, maintain_name_scope)

    records_by_name = {
        row["name"]: row
        for row in scan_result["candidate_records"]
        if row.get("name")
    }
    invalid_records = scan_result["invalid_records"]
    quota_records = [row for row in scan_result["quota_records"] if row.get("is_invalid_401") != 1]
    recovered_records = scan_result["recovered_records"]

    delete_401_results: list[dict[str, Any]] = []
    quota_action_results: list[dict[str, Any]] = []
    reenable_results: list[dict[str, Any]] = []

    if settings["delete_401"] and invalid_records:
        names = [row["name"] for row in invalid_records if row.get("name")]
        logger.info("待删除 401 账号: %s", len(names))
        if confirm_action(f"即将删除 {len(names)} 个 401 账号", settings["assume_yes"]):
            delete_401_results = await run_action_group_async(
                base_url=settings["base_url"],
                token=settings["token"],
                timeout=settings["timeout"],
                workers=settings["action_workers"],
                items=names,
                fn_name="delete",
                delete_retries=settings["delete_retries"],
                debug=settings["debug"],
            )
            updated = apply_action_results(
                records_by_name,
                delete_401_results,
                action="delete_401",
                managed_reason_on_success="deleted_401",
                disabled_value=None,
            )
            upsert_auth_accounts(conn, updated)

    deleted_401_names = {row["name"] for row in delete_401_results if row.get("ok")}

    if settings["quota_action"] == "disable":
        already_disabled = [
            row
            for row in quota_records
            if row.get("name") not in deleted_401_names and row.get("disabled") == 1
        ]
        to_disable = [
            row
            for row in quota_records
            if row.get("name") not in deleted_401_names and row.get("disabled") != 1
        ]
        logger.info("待禁用限额账号: %s", len(to_disable))
        logger.debug("已处于禁用状态的限额账号: %s", len(already_disabled))

        if already_disabled:
            upsert_auth_accounts(conn, mark_quota_already_disabled(already_disabled))

        if to_disable:
            quota_action_results = await run_action_group_async(
                base_url=settings["base_url"],
                token=settings["token"],
                timeout=settings["timeout"],
                workers=settings["action_workers"],
                items=[row["name"] for row in to_disable if row.get("name")],
                fn_name="toggle",
                disabled=True,
                debug=settings["debug"],
            )
            updated = apply_action_results(
                records_by_name,
                quota_action_results,
                action="disable_quota",
                managed_reason_on_success="quota_disabled",
                disabled_value=1,
            )
            upsert_auth_accounts(conn, updated)
    else:
        quota_delete_targets = [
            row["name"]
            for row in quota_records
            if row.get("name") and row.get("name") not in deleted_401_names
        ]
        logger.info("待删除限额账号: %s", len(quota_delete_targets))
        if quota_delete_targets and confirm_action(
            f"即将删除 {len(quota_delete_targets)} 个限额账号",
            settings["assume_yes"],
        ):
            quota_action_results = await run_action_group_async(
                base_url=settings["base_url"],
                token=settings["token"],
                timeout=settings["timeout"],
                workers=settings["action_workers"],
                items=quota_delete_targets,
                fn_name="delete",
                delete_retries=settings["delete_retries"],
                debug=settings["debug"],
            )
            updated = apply_action_results(
                records_by_name,
                quota_action_results,
                action="delete_quota",
                managed_reason_on_success="quota_deleted",
                disabled_value=None,
            )
            upsert_auth_accounts(conn, updated)

    deleted_quota_names = {
        row["name"]
        for row in quota_action_results
        if row.get("ok") and settings["quota_action"] == "delete"
    }

    if settings["auto_reenable"]:
        recoverable_records = (
            recovered_records
            if settings["reenable_scope"] == "signal"
            else [
                row
                for row in recovered_records
                if str(row.get("managed_reason") or "") == "quota_disabled"
            ]
        )
        reenable_targets = [
            row["name"]
            for row in recoverable_records
            if row.get("name") not in deleted_401_names and row.get("name") not in deleted_quota_names
        ]
        logger.info("待恢复启用账号: %s（scope=%s）", len(reenable_targets), settings["reenable_scope"])
        if reenable_targets:
            reenable_results = await run_action_group_async(
                base_url=settings["base_url"],
                token=settings["token"],
                timeout=settings["timeout"],
                workers=settings["action_workers"],
                items=reenable_targets,
                fn_name="toggle",
                disabled=False,
                debug=settings["debug"],
            )
            updated = apply_action_results(
                records_by_name,
                reenable_results,
                action="reenable_quota",
                managed_reason_on_success=None,
                disabled_value=0,
            )
            upsert_auth_accounts(conn, updated)

    summarize_action_results("删除 401", delete_401_results)
    summarize_action_results("处理限额", quota_action_results)
    summarize_action_results("恢复启用", reenable_results)
    logger.info("维护完成")

    return {
        "scan": scan_result,
        "delete_401_results": delete_401_results,
        "quota_action_results": quota_action_results,
        "reenable_results": reenable_results,
    }
