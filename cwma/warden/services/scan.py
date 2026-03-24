from __future__ import annotations

import sqlite3
from typing import Any, Awaitable, Callable


async def run_scan_async(
    conn: sqlite3.Connection,
    settings: dict[str, Any],
    *,
    maintain_name_scope: set[str] | None,
    start_scan_run: Callable[[sqlite3.Connection, dict[str, Any]], int],
    finish_scan_run: Callable[..., None],
    utc_now_iso: Callable[[], str],
    fetch_auth_files: Callable[[str, str, int], list[dict[str, Any]]],
    load_existing_state: Callable[[sqlite3.Connection], dict[str, dict[str, Any]]],
    get_item_name: Callable[[dict[str, Any]], str],
    build_auth_record: Callable[[dict[str, Any], dict[str, Any] | None, str], dict[str, Any]],
    upsert_auth_accounts: Callable[[sqlite3.Connection, list[dict[str, Any]]], None],
    matches_filters: Callable[[dict[str, Any], str, str], bool],
    probe_accounts_async: Callable[..., Awaitable[list[dict[str, Any]]]],
    print_scan_summary: Callable[..., None],
    summarize_failures: Callable[[list[dict[str, Any]]], None],
    export_current_results: Callable[[str, str, list[dict[str, Any]], list[dict[str, Any]]], None],
    logger: Any,
) -> dict[str, Any]:
    run_id = start_scan_run(conn, settings)
    logger.info(
        "开始扫描: mode=%s db=%s log=%s quota_disable_threshold=%s",
        settings["mode"],
        settings["db_path"],
        settings["log_file"],
        settings["quota_disable_threshold"],
    )
    try:
        now_iso = utc_now_iso()
        files = fetch_auth_files(settings["base_url"], settings["token"], settings["timeout"])
        existing_state = load_existing_state(conn)

        inventory_records: list[dict[str, Any]] = []
        for item in files:
            name = get_item_name(item)
            if not name:
                continue
            inventory_records.append(build_auth_record(item, existing_state.get(name), now_iso))

        upsert_auth_accounts(conn, inventory_records)

        candidate_records = [
            record
            for record in inventory_records
            if matches_filters(record, settings["target_type"], settings["provider"])
        ]
        if maintain_name_scope is not None:
            candidate_records = [
                record
                for record in candidate_records
                if str(record.get("name") or "") in maintain_name_scope
            ]
        probed_records = await probe_accounts_async(
            candidate_records,
            base_url=settings["base_url"],
            token=settings["token"],
            timeout=settings["timeout"],
            retries=settings["retries"],
            user_agent=settings["user_agent"],
            probe_workers=settings["probe_workers"],
            quota_disable_threshold=settings["quota_disable_threshold"],
            debug=settings["debug"],
        )
        probed_map = {record["name"]: record for record in probed_records}

        for record in inventory_records:
            if record["name"] in probed_map:
                record.update(probed_map[record["name"]])
                record["updated_at"] = utc_now_iso()

        upsert_auth_accounts(conn, inventory_records)

        current_candidates = [
            record
            for record in inventory_records
            if matches_filters(record, settings["target_type"], settings["provider"])
        ]
        invalid_records = [row for row in current_candidates if row.get("is_invalid_401") == 1]
        quota_records = [row for row in current_candidates if row.get("is_quota_limited") == 1]
        recovered_records = [row for row in current_candidates if row.get("is_recovered") == 1]
        failure_records = [row for row in current_candidates if row.get("probe_error_kind")]
        probed_files = sum(1 for row in current_candidates if row.get("last_probed_at"))

        finish_scan_run(
            conn,
            run_id,
            status="success",
            total_files=len(files),
            filtered_files=len(current_candidates),
            probed_files=probed_files,
            invalid_401_count=len(invalid_records),
            quota_limited_count=len(quota_records),
            recovered_count=len(recovered_records),
        )

        print_scan_summary(
            total_files=len(files),
            candidate_records=current_candidates,
            invalid_records=invalid_records,
            quota_records=quota_records,
            recovered_records=recovered_records,
        )
        if failure_records:
            summarize_failures(failure_records)
        export_current_results(settings["invalid_output"], settings["quota_output"], invalid_records, quota_records)
        logger.info("扫描完成")

        return {
            "run_id": run_id,
            "all_records": inventory_records,
            "candidate_records": current_candidates,
            "invalid_records": invalid_records,
            "quota_records": quota_records,
            "recovered_records": recovered_records,
        }
    except Exception:
        finish_scan_run(
            conn,
            run_id,
            status="failed",
            total_files=0,
            filtered_files=0,
            probed_files=0,
            invalid_401_count=0,
            quota_limited_count=0,
            recovered_count=0,
        )
        raise
