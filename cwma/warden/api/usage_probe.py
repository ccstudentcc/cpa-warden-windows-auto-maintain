from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from logging import Logger
from typing import Any

from ..models import normalize_optional_ratio, pick_first_number


DEFAULT_WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
DEFAULT_SPARK_METERED_FEATURE = "codex_bengalfox"


def build_wham_usage_payload(
    auth_index: str,
    user_agent: str,
    chatgpt_account_id: str,
    *,
    usage_url: str = DEFAULT_WHAM_USAGE_URL,
) -> dict[str, Any]:
    return {
        "authIndex": auth_index,
        "method": "GET",
        "url": usage_url,
        "header": {
            "Authorization": "Bearer $TOKEN$",
            "Content-Type": "application/json",
            "User-Agent": user_agent,
            "Chatgpt-Account-Id": chatgpt_account_id,
        },
    }


def extract_remaining_ratio(rate_limit: dict[str, Any] | None) -> float | None:
    if not isinstance(rate_limit, dict):
        return None

    total_keys = (
        "total",
        "limit",
        "max",
        "maximum",
        "quota",
        "request_limit",
        "requests_limit",
        "total_requests",
    )
    remaining_keys = (
        "remaining",
        "remaining_requests",
        "requests_remaining",
        "available",
        "available_requests",
        "left",
    )
    used_keys = (
        "used",
        "consumed",
        "used_requests",
        "requests_used",
        "spent",
    )

    windows: list[dict[str, Any]] = [rate_limit]
    for window_key in ("primary_window", "window", "current_window"):
        window = rate_limit.get(window_key)
        if isinstance(window, dict):
            windows.append(window)

    for window in windows:
        total = pick_first_number(window, total_keys)
        if total is None or total <= 0:
            continue
        remaining = pick_first_number(window, remaining_keys)
        used = pick_first_number(window, used_keys)
        if remaining is None and used is None:
            continue
        if remaining is not None:
            ratio = remaining / total
        else:
            ratio = (total - used) / total
        return normalize_optional_ratio(ratio)
    return None


def find_spark_rate_limit(
    parsed_body: dict[str, Any],
    *,
    spark_metered_feature: str = DEFAULT_SPARK_METERED_FEATURE,
) -> dict[str, Any] | None:
    additional_limits = parsed_body.get("additional_rate_limits")
    if not isinstance(additional_limits, list):
        return None

    spark_candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for item in additional_limits:
        if not isinstance(item, dict):
            continue
        rate_limit = item.get("rate_limit")
        if not isinstance(rate_limit, dict):
            continue
        spark_candidates.append((item, rate_limit))

    for item, rate_limit in spark_candidates:
        metered_feature = str(item.get("metered_feature") or "").strip().lower()
        if metered_feature == spark_metered_feature:
            return rate_limit

    for item, rate_limit in spark_candidates:
        limit_name = str(item.get("limit_name") or "").strip().lower()
        if "spark" in limit_name:
            return rate_limit

    return None


async def probe_wham_usage_async(
    session: Any,
    semaphore: asyncio.Semaphore,
    base_url: str,
    token: str,
    record: dict[str, Any],
    timeout: int,
    retries: int,
    user_agent: str,
    *,
    headers_builder: Callable[[str, bool], dict[str, str]],
    backoff_seconds: Callable[[int], float],
    now_iso: Callable[[], str],
    quota_signal_resolver: Callable[[dict[str, Any]], tuple[int | None, int | None, str]],
    logger: Logger,
    usage_url: str = DEFAULT_WHAM_USAGE_URL,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    json_loads: Callable[[str], Any] = json.loads,
) -> dict[str, Any]:
    result = dict(record)
    result["last_probed_at"] = now_iso()
    logger.debug(
        "开始探测账号: name=%s auth_index=%s unavailable=%s disabled=%s has_account_id=%s",
        result.get("name"),
        result.get("auth_index"),
        bool(result.get("unavailable")),
        bool(result.get("disabled")),
        "yes" if result.get("chatgpt_account_id") else "no",
    )

    auth_index = str(result.get("auth_index") or "").strip()
    account_id = str(result.get("chatgpt_account_id") or "").strip()

    if not auth_index:
        result["probe_error_kind"] = "missing_auth_index"
        result["probe_error_text"] = "missing auth_index"
        logger.debug("跳过账号: name=%s reason=missing_auth_index", result.get("name"))
        return result

    if not account_id:
        result["probe_error_kind"] = "missing_chatgpt_account_id"
        result["probe_error_text"] = "missing Chatgpt-Account-Id"
        logger.debug("跳过账号: name=%s reason=missing_chatgpt_account_id", result.get("name"))
        return result

    payload = build_wham_usage_payload(auth_index, user_agent, account_id, usage_url=usage_url)
    url = f"{base_url.rstrip('/')}/v0/management/api-call"

    for attempt in range(retries + 1):
        try:
            async with semaphore:
                async with session.post(
                    url,
                    headers=headers_builder(token, True),
                    json=payload,
                    timeout=timeout,
                ) as resp:
                    text = await resp.text()
                    result["api_http_status"] = resp.status

                    if resp.status == 429:
                        result["probe_error_kind"] = "management_api_http_429"
                        result["probe_error_text"] = "management api-call http 429"
                        if attempt < retries:
                            delay = backoff_seconds(attempt)
                            logger.debug(
                                "探测受限重试: name=%s api_http=429 attempt=%s/%s delay=%.2fs",
                                result.get("name"),
                                attempt + 1,
                                retries + 1,
                                delay,
                            )
                            await sleep(delay)
                            continue
                        logger.debug("探测失败: name=%s api_http=429 retries_exhausted=true", result.get("name"))
                        return result
                    if resp.status >= 500:
                        result["probe_error_kind"] = "management_api_http_5xx"
                        result["probe_error_text"] = f"management api-call http {resp.status}"
                        if attempt < retries:
                            delay = backoff_seconds(attempt)
                            logger.debug(
                                "探测服务端错误重试: name=%s api_http=%s attempt=%s/%s delay=%.2fs",
                                result.get("name"),
                                resp.status,
                                attempt + 1,
                                retries + 1,
                                delay,
                            )
                            await sleep(delay)
                            continue
                        logger.debug(
                            "探测失败: name=%s api_http=%s retries_exhausted=true",
                            result.get("name"),
                            resp.status,
                        )
                        return result
                    if resp.status >= 400:
                        result["probe_error_kind"] = "management_api_http_4xx"
                        result["probe_error_text"] = f"management api-call http {resp.status}"
                        logger.debug("探测失败: name=%s api_http=%s", result.get("name"), resp.status)
                        return result

                    try:
                        outer = json_loads(text)
                    except Exception:
                        result["probe_error_kind"] = "api_call_invalid_json"
                        result["probe_error_text"] = "api-call response is not valid JSON"
                        logger.debug("探测失败: name=%s reason=api_call_invalid_json", result.get("name"))
                        return result

                    if not isinstance(outer, dict):
                        result["probe_error_kind"] = "api_call_not_object"
                        result["probe_error_text"] = (
                            f"api-call response is not JSON object: {type(outer).__name__}"
                        )
                        logger.debug("探测失败: name=%s reason=api_call_not_object", result.get("name"))
                        return result

                    status_code = outer.get("status_code")
                    result["api_status_code"] = status_code
                    if status_code is None:
                        result["probe_error_kind"] = "missing_status_code"
                        result["probe_error_text"] = "missing status_code in api-call response"
                        logger.debug("探测失败: name=%s reason=missing_status_code", result.get("name"))
                        return result

                    if status_code == 401:
                        result["probe_error_kind"] = None
                        result["probe_error_text"] = None
                        logger.debug("探测完成: name=%s status_code=401", result.get("name"))
                        return result

                    body = outer.get("body")
                    if isinstance(body, dict):
                        parsed_body = body
                    elif isinstance(body, str):
                        try:
                            parsed_body = json_loads(body)
                        except Exception:
                            result["probe_error_kind"] = "body_invalid_json"
                            result["probe_error_text"] = "api-call body is not valid JSON"
                            logger.debug("探测失败: name=%s reason=body_invalid_json", result.get("name"))
                            return result
                    elif body is None:
                        parsed_body = {}
                    else:
                        result["probe_error_kind"] = "body_not_object"
                        result["probe_error_text"] = f"api-call body is not JSON object: {type(body).__name__}"
                        logger.debug("探测失败: name=%s reason=body_not_object", result.get("name"))
                        return result

                    if parsed_body and not isinstance(parsed_body, dict):
                        result["probe_error_kind"] = "body_not_object"
                        result["probe_error_text"] = (
                            f"api-call body is not JSON object: {type(parsed_body).__name__}"
                        )
                        logger.debug("探测失败: name=%s reason=body_not_object", result.get("name"))
                        return result

                    rate_limit = parsed_body.get("rate_limit") if isinstance(parsed_body, dict) else None
                    primary_window = rate_limit.get("primary_window") if isinstance(rate_limit, dict) else None
                    result["usage_allowed"] = (
                        int(rate_limit.get("allowed"))
                        if isinstance(rate_limit, dict) and isinstance(rate_limit.get("allowed"), bool)
                        else None
                    )
                    result["usage_limit_reached"] = (
                        int(rate_limit.get("limit_reached"))
                        if isinstance(rate_limit, dict) and isinstance(rate_limit.get("limit_reached"), bool)
                        else None
                    )
                    result["usage_remaining_ratio"] = extract_remaining_ratio(rate_limit)
                    result["usage_plan_type"] = (
                        str(parsed_body.get("plan_type") or "").strip() or None
                        if isinstance(parsed_body, dict)
                        else None
                    )
                    result["usage_email"] = (
                        str(parsed_body.get("email") or "").strip() or None
                        if isinstance(parsed_body, dict)
                        else None
                    )
                    result["usage_reset_at"] = (
                        int(primary_window.get("reset_at"))
                        if isinstance(primary_window, dict) and primary_window.get("reset_at") is not None
                        else None
                    )
                    result["usage_reset_after_seconds"] = (
                        int(primary_window.get("reset_after_seconds"))
                        if isinstance(primary_window, dict)
                        and primary_window.get("reset_after_seconds") is not None
                        else None
                    )
                    spark_rate_limit = find_spark_rate_limit(parsed_body) if isinstance(parsed_body, dict) else None
                    spark_primary_window = (
                        spark_rate_limit.get("primary_window") if isinstance(spark_rate_limit, dict) else None
                    )
                    result["usage_spark_allowed"] = (
                        int(spark_rate_limit.get("allowed"))
                        if isinstance(spark_rate_limit, dict) and isinstance(spark_rate_limit.get("allowed"), bool)
                        else None
                    )
                    result["usage_spark_limit_reached"] = (
                        int(spark_rate_limit.get("limit_reached"))
                        if isinstance(spark_rate_limit, dict)
                        and isinstance(spark_rate_limit.get("limit_reached"), bool)
                        else None
                    )
                    result["usage_spark_remaining_ratio"] = extract_remaining_ratio(spark_rate_limit)
                    result["usage_spark_reset_at"] = (
                        int(spark_primary_window.get("reset_at"))
                        if isinstance(spark_primary_window, dict)
                        and spark_primary_window.get("reset_at") is not None
                        else None
                    )
                    result["usage_spark_reset_after_seconds"] = (
                        int(spark_primary_window.get("reset_after_seconds"))
                        if isinstance(spark_primary_window, dict)
                        and spark_primary_window.get("reset_after_seconds") is not None
                        else None
                    )
                    effective_limit_reached, effective_allowed, quota_signal_source = quota_signal_resolver(result)
                    result["quota_signal_source"] = quota_signal_source

                    if status_code == 200:
                        result["probe_error_kind"] = None
                        result["probe_error_text"] = None
                        logger.debug(
                            "探测完成: name=%s api_http=%s status_code=%s limit_reached=%s allowed=%s source=%s remaining_ratio=%s spark_limit_reached=%s spark_remaining_ratio=%s",
                            result.get("name"),
                            result.get("api_http_status"),
                            result.get("api_status_code"),
                            effective_limit_reached,
                            effective_allowed,
                            result.get("quota_signal_source"),
                            result.get("usage_remaining_ratio"),
                            result.get("usage_spark_limit_reached"),
                            result.get("usage_spark_remaining_ratio"),
                        )
                        return result

                    result["probe_error_kind"] = "other"
                    result["probe_error_text"] = f"unexpected upstream status_code={status_code}"
                    logger.debug(
                        "探测异常: name=%s api_http=%s status_code=%s",
                        result.get("name"),
                        result.get("api_http_status"),
                        status_code,
                    )
                    return result
        except asyncio.TimeoutError:
            result["probe_error_kind"] = "timeout"
            result["probe_error_text"] = "timeout"
            logger.debug("探测超时: name=%s attempt=%s", result.get("name"), attempt + 1)
        except Exception as exc:
            result["probe_error_kind"] = "other"
            result["probe_error_text"] = str(exc)
            logger.debug("探测异常: name=%s error=%s", result.get("name"), exc)

        if attempt >= retries:
            return result

    return result
