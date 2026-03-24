from __future__ import annotations

import json
import math
from typing import Any, TypedDict


AUTH_ACCOUNT_COLUMNS = [
    "name",
    "disabled",
    "id_token_json",
    "email",
    "provider",
    "source",
    "unavailable",
    "auth_index",
    "account",
    "type",
    "runtime_only",
    "status",
    "status_message",
    "chatgpt_account_id",
    "id_token_plan_type",
    "auth_updated_at",
    "auth_modtime",
    "auth_last_refresh",
    "api_http_status",
    "api_status_code",
    "usage_allowed",
    "usage_limit_reached",
    "usage_plan_type",
    "usage_email",
    "usage_reset_at",
    "usage_reset_after_seconds",
    "usage_spark_allowed",
    "usage_spark_limit_reached",
    "usage_spark_reset_at",
    "usage_spark_reset_after_seconds",
    "quota_signal_source",
    "is_invalid_401",
    "is_quota_limited",
    "is_recovered",
    "probe_error_kind",
    "probe_error_text",
    "managed_reason",
    "last_action",
    "last_action_status",
    "last_action_error",
    "last_seen_at",
    "last_probed_at",
    "updated_at",
]


class AuthInventoryItem(TypedDict, total=False):
    name: str
    id: str
    disabled: bool | int
    id_token: str | dict[str, Any]
    email: str
    provider: str
    source: str
    unavailable: bool | int
    auth_index: str
    account: str
    type: str
    typo: str
    runtime_only: bool | int
    status: str
    status_message: str
    chatgpt_account_id: str
    chatgptAccountId: str
    account_id: str
    accountId: str
    updated_at: str
    modtime: str
    last_refresh: str


class AuthAccountRecord(TypedDict, total=False):
    name: str
    disabled: int
    id_token_json: str | None
    email: str | None
    provider: str | None
    source: str | None
    unavailable: int
    auth_index: str | None
    account: str | None
    type: str | None
    runtime_only: int
    status: str | None
    status_message: str | None
    chatgpt_account_id: str | None
    id_token_plan_type: str | None
    auth_updated_at: str | None
    auth_modtime: str | None
    auth_last_refresh: str | None
    api_http_status: int | None
    api_status_code: int | None
    usage_allowed: int | None
    usage_limit_reached: int | None
    usage_plan_type: str | None
    usage_email: str | None
    usage_reset_at: str | None
    usage_reset_after_seconds: float | None
    usage_spark_allowed: int | None
    usage_spark_limit_reached: int | None
    usage_spark_reset_at: str | None
    usage_spark_reset_after_seconds: float | None
    quota_signal_source: str | None
    quota_remaining_ratio: float | None
    quota_remaining_ratio_source: str | None
    quota_threshold_triggered: int
    is_invalid_401: int
    is_quota_limited: int
    is_recovered: int
    probe_error_kind: str | None
    probe_error_text: str | None
    managed_reason: str | None
    last_action: str | None
    last_action_status: str | None
    last_action_error: str | None
    last_seen_at: str
    last_probed_at: str | None
    updated_at: str


def maybe_json_loads(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except Exception:
        return None


def compact_text(text: Any, limit: int = 240) -> str | None:
    if text is None:
        return None
    normalized = str(text).replace("\r", " ").replace("\n", " ").strip()
    if not normalized:
        return None
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 3)] + "..."


def normalize_optional_flag(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if value is None:
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None
    return normalized if normalized in {0, 1} else None


def normalize_optional_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(normalized):
        return None
    return normalized


def normalize_optional_ratio(value: Any) -> float | None:
    normalized = normalize_optional_number(value)
    if normalized is None:
        return None
    if normalized < 0:
        return 0.0
    if normalized > 1:
        return 1.0
    return normalized


def pick_first_number(record: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key not in record:
            continue
        value = normalize_optional_number(record.get(key))
        if value is not None:
            return value
    return None


def get_item_name(item: AuthInventoryItem) -> str:
    return str(item.get("name") or item.get("id") or "").strip()


def get_item_type(item: AuthInventoryItem) -> str:
    return str(item.get("type") or item.get("typo") or "").strip()


def get_item_account(item: AuthInventoryItem) -> str:
    return str(item.get("account") or item.get("email") or "").strip()


def get_id_token_object(item: AuthInventoryItem) -> dict[str, Any]:
    parsed = maybe_json_loads(item.get("id_token"))
    return parsed if isinstance(parsed, dict) else {}


def extract_chatgpt_account_id_from_item(item: AuthInventoryItem) -> str:
    id_token = get_id_token_object(item)
    for source in (id_token, item):
        for key in ("chatgpt_account_id", "chatgptAccountId", "account_id", "accountId"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def extract_id_token_plan_type(item: AuthInventoryItem) -> str:
    id_token = get_id_token_object(item)
    value = id_token.get("plan_type")
    return value.strip() if isinstance(value, str) else ""


def resolve_quota_signal(record: dict[str, Any]) -> tuple[int | None, int | None, str]:
    plan_type = str(record.get("usage_plan_type") or record.get("id_token_plan_type") or "").strip().lower()
    spark_limit_reached = normalize_optional_flag(record.get("usage_spark_limit_reached"))
    spark_allowed = normalize_optional_flag(record.get("usage_spark_allowed"))
    primary_limit_reached = normalize_optional_flag(record.get("usage_limit_reached"))
    primary_allowed = normalize_optional_flag(record.get("usage_allowed"))

    if plan_type == "pro" and spark_limit_reached is not None:
        effective_allowed = spark_allowed if spark_allowed is not None else primary_allowed
        return spark_limit_reached, effective_allowed, "spark"
    return primary_limit_reached, primary_allowed, "primary"


def resolve_quota_remaining_ratio(record: dict[str, Any]) -> tuple[float | None, str]:
    _, _, quota_signal_source = resolve_quota_signal(record)
    primary_ratio = normalize_optional_ratio(record.get("usage_remaining_ratio"))
    spark_ratio = normalize_optional_ratio(record.get("usage_spark_remaining_ratio"))

    if quota_signal_source == "spark":
        if spark_ratio is not None:
            return spark_ratio, "spark"
        if primary_ratio is not None:
            return primary_ratio, "primary_fallback"
        return None, "spark"

    if primary_ratio is not None:
        return primary_ratio, "primary"
    if spark_ratio is not None:
        return spark_ratio, "spark_fallback"
    return None, "primary"


def build_auth_record(
    item: AuthInventoryItem,
    existing_row: dict[str, Any] | None,
    now_iso: str,
) -> AuthAccountRecord:
    id_token_obj = get_id_token_object(item)
    id_token_json = json.dumps(id_token_obj, ensure_ascii=False) if id_token_obj else None
    existing_row = existing_row or {}
    return {
        "name": get_item_name(item),
        "disabled": int(bool(item.get("disabled"))),
        "id_token_json": id_token_json,
        "email": str(item.get("email") or "").strip() or None,
        "provider": str(item.get("provider") or "").strip() or None,
        "source": str(item.get("source") or "").strip() or None,
        "unavailable": int(bool(item.get("unavailable"))),
        "auth_index": str(item.get("auth_index") or "").strip() or None,
        "account": get_item_account(item) or None,
        "type": get_item_type(item) or None,
        "runtime_only": int(bool(item.get("runtime_only"))),
        "status": str(item.get("status") or "").strip() or None,
        "status_message": compact_text(item.get("status_message"), 1200),
        "chatgpt_account_id": extract_chatgpt_account_id_from_item(item) or None,
        "id_token_plan_type": extract_id_token_plan_type(item) or None,
        "auth_updated_at": str(item.get("updated_at") or "").strip() or None,
        "auth_modtime": str(item.get("modtime") or "").strip() or None,
        "auth_last_refresh": str(item.get("last_refresh") or "").strip() or None,
        "api_http_status": None,
        "api_status_code": None,
        "usage_allowed": None,
        "usage_limit_reached": None,
        "usage_plan_type": None,
        "usage_email": None,
        "usage_reset_at": None,
        "usage_reset_after_seconds": None,
        "usage_spark_allowed": None,
        "usage_spark_limit_reached": None,
        "usage_spark_reset_at": None,
        "usage_spark_reset_after_seconds": None,
        "quota_signal_source": None,
        "is_invalid_401": 0,
        "is_quota_limited": 0,
        "is_recovered": 0,
        "probe_error_kind": None,
        "probe_error_text": None,
        "managed_reason": existing_row.get("managed_reason"),
        "last_action": existing_row.get("last_action"),
        "last_action_status": existing_row.get("last_action_status"),
        "last_action_error": existing_row.get("last_action_error"),
        "last_seen_at": now_iso,
        "last_probed_at": None,
        "updated_at": now_iso,
    }


def matches_filters(record: dict[str, Any], target_type: str, provider: str) -> bool:
    if str(record.get("type") or "").lower() != target_type.lower():
        return False
    if provider and str(record.get("provider") or "").lower() != provider.lower():
        return False
    return True
