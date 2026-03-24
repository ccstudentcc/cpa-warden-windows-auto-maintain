from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from typing import Any


def build_invalid_export_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": record.get("name"),
        "account": record.get("account") or record.get("email") or "",
        "email": record.get("email") or "",
        "provider": record.get("provider"),
        "source": record.get("source"),
        "disabled": bool(record.get("disabled")),
        "unavailable": bool(record.get("unavailable")),
        "auth_index": record.get("auth_index"),
        "chatgpt_account_id": record.get("chatgpt_account_id"),
        "api_http_status": record.get("api_http_status"),
        "api_status_code": record.get("api_status_code"),
        "status": record.get("status"),
        "status_message": record.get("status_message"),
        "probe_error_kind": record.get("probe_error_kind"),
        "probe_error_text": record.get("probe_error_text"),
    }


def build_quota_export_record(
    record: dict[str, Any],
    *,
    resolve_quota_signal: Callable[[dict[str, Any]], tuple[int | None, int | None, str]],
    resolve_quota_remaining_ratio: Callable[[dict[str, Any]], tuple[float | None, str]],
    normalize_optional_ratio: Callable[[Any], float | None],
) -> dict[str, Any]:
    effective_limit_reached, effective_allowed, quota_signal_source = resolve_quota_signal(record)
    effective_remaining_ratio, remaining_ratio_source = resolve_quota_remaining_ratio(record)
    return {
        "name": record.get("name"),
        "account": record.get("account") or record.get("email") or "",
        "email": record.get("usage_email") or record.get("email") or "",
        "provider": record.get("provider"),
        "source": record.get("source"),
        "disabled": bool(record.get("disabled")),
        "unavailable": bool(record.get("unavailable")),
        "auth_index": record.get("auth_index"),
        "chatgpt_account_id": record.get("chatgpt_account_id"),
        "api_http_status": record.get("api_http_status"),
        "api_status_code": record.get("api_status_code"),
        "limit_reached": bool(effective_limit_reached) if effective_limit_reached is not None else None,
        "allowed": bool(effective_allowed) if effective_allowed is not None else None,
        "quota_signal_source": record.get("quota_signal_source") or quota_signal_source,
        "remaining_ratio": effective_remaining_ratio,
        "remaining_ratio_source": record.get("quota_remaining_ratio_source") or remaining_ratio_source,
        "threshold_triggered": bool(record.get("quota_threshold_triggered")),
        "primary_remaining_ratio": normalize_optional_ratio(record.get("usage_remaining_ratio")),
        "spark_remaining_ratio": normalize_optional_ratio(record.get("usage_spark_remaining_ratio")),
        "primary_limit_reached": bool(record.get("usage_limit_reached")) if record.get("usage_limit_reached") is not None else None,
        "primary_allowed": bool(record.get("usage_allowed")) if record.get("usage_allowed") is not None else None,
        "spark_limit_reached": bool(record.get("usage_spark_limit_reached")) if record.get("usage_spark_limit_reached") is not None else None,
        "spark_allowed": bool(record.get("usage_spark_allowed")) if record.get("usage_spark_allowed") is not None else None,
        "plan_type": record.get("usage_plan_type") or record.get("id_token_plan_type"),
        "reset_at": record.get("usage_reset_at"),
        "reset_after_seconds": record.get("usage_reset_after_seconds"),
        "spark_reset_at": record.get("usage_spark_reset_at"),
        "spark_reset_after_seconds": record.get("usage_spark_reset_after_seconds"),
        "probe_error_kind": record.get("probe_error_kind"),
        "probe_error_text": record.get("probe_error_text"),
    }


def export_records(path: str, rows: list[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)


def summarize_failures(
    records: list[dict[str, Any]],
    *,
    sample_limit: int = 3,
    compact_text: Callable[[Any, int], str | None],
    logger: Any,
) -> None:
    failed = [row for row in records if row.get("probe_error_kind")]
    if not failed:
        return

    labels = {
        "missing_auth_index": "缺少 auth_index",
        "missing_chatgpt_account_id": "缺少 Chatgpt-Account-Id",
        "management_api_http_4xx": "管理接口 HTTP 4xx",
        "management_api_http_429": "管理接口 HTTP 429",
        "management_api_http_5xx": "管理接口 HTTP 5xx",
        "api_call_invalid_json": "api-call 返回不是 JSON",
        "api_call_not_object": "api-call 返回不是对象",
        "missing_status_code": "api-call 缺少 status_code",
        "body_invalid_json": "api-call body 不是合法 JSON",
        "body_not_object": "api-call body 不是对象",
        "timeout": "请求超时",
        "other": "其他异常",
    }
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "samples": []})

    for row in failed:
        key = str(row.get("probe_error_kind") or "other")
        bucket = buckets[key]
        bucket["count"] += 1
        if len(bucket["samples"]) < sample_limit:
            bucket["samples"].append(
                " | ".join(
                    [
                        row.get("name") or "-",
                        f"account={row.get('account') or row.get('email') or '-'}",
                        f"auth_index={row.get('auth_index') or '-'}",
                        f"has_account_id={'yes' if row.get('chatgpt_account_id') else 'no'}",
                        f"api_http={row.get('api_http_status') if row.get('api_http_status') is not None else '-'}",
                        f"status_code={row.get('api_status_code') if row.get('api_status_code') is not None else '-'}",
                        f"error_kind={key}",
                        f"error={compact_text(row.get('probe_error_text'), 100) or '-'}",
                    ]
                )
            )

    logger.info("失败原因统计:")
    for key, payload in sorted(buckets.items(), key=lambda item: (-item[1]["count"], item[0])):
        logger.info("  - %s: %s", labels.get(key, key), payload["count"])

    logger.debug("失败样例:")
    for key, payload in sorted(buckets.items(), key=lambda item: (-item[1]["count"], item[0])):
        logger.debug("  [%s]", labels.get(key, key))
        for sample in payload["samples"]:
            logger.debug("    %s", sample)


def export_current_results(
    invalid_output: str,
    quota_output: str,
    invalid_records: list[dict[str, Any]],
    quota_records: list[dict[str, Any]],
    *,
    export_records_fn: Callable[[str, list[dict[str, Any]]], None],
    build_invalid_export_record_fn: Callable[[dict[str, Any]], dict[str, Any]],
    build_quota_export_record_fn: Callable[[dict[str, Any]], dict[str, Any]],
    logger: Any,
) -> None:
    export_records_fn(invalid_output, [build_invalid_export_record_fn(row) for row in invalid_records])
    export_records_fn(quota_output, [build_quota_export_record_fn(row) for row in quota_records])
    logger.info("已导出 401 列表: %s", invalid_output)
    logger.info("已导出限额列表: %s", quota_output)
