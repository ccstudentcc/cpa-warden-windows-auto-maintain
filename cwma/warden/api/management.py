from __future__ import annotations

import asyncio
import urllib.parse
from typing import Any, Callable

import requests


def build_management_headers(token: str, include_json: bool = False) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/plain, */*",
    }
    if include_json:
        headers["Content-Type"] = "application/json"
    return headers


def safe_json_response(resp: requests.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def fetch_auth_files(
    base_url: str,
    token: str,
    timeout: int,
    *,
    request_get: Callable[..., requests.Response] = requests.get,
    headers_builder: Callable[[str, bool], dict[str, str]] = build_management_headers,
    json_loader: Callable[[requests.Response], dict[str, Any]] = safe_json_response,
) -> list[dict[str, Any]]:
    resp = request_get(
        f"{base_url.rstrip('/')}/v0/management/auth-files",
        headers=headers_builder(token, False),
        timeout=timeout,
    )
    resp.raise_for_status()
    data = json_loader(resp)
    files = data.get("files")
    return files if isinstance(files, list) else []


def fetch_remote_auth_file_names(
    base_url: str,
    token: str,
    timeout: int,
    *,
    fetcher: Callable[[str, str, int], list[dict[str, Any]]] = fetch_auth_files,
    name_extractor: Callable[[dict[str, Any]], str] | None = None,
) -> set[str]:
    files = fetcher(base_url, token, timeout)
    names = set()
    for item in files:
        if name_extractor is None:
            name = str(item.get("name") or item.get("id") or "").strip()
        else:
            name = name_extractor(item)
        if name:
            names.add(name)
    return names


def retry_backoff_seconds(attempt: int) -> float:
    return min(5.0, 0.5 * (2**max(0, int(attempt))))


async def delete_account_async(
    session: Any,
    semaphore: asyncio.Semaphore,
    base_url: str,
    token: str,
    name: str,
    timeout: int,
    delete_retries: int,
    *,
    headers_builder: Callable[[str, bool], dict[str, str]] = build_management_headers,
    maybe_json_loads: Callable[[Any], Any],
    compact_text: Callable[[Any, int], str | None],
    backoff: Callable[[int], float] = retry_backoff_seconds,
    logger: Any | None = None,
) -> dict[str, Any]:
    encoded_name = urllib.parse.quote(name, safe="")
    url = f"{base_url.rstrip('/')}/v0/management/auth-files?name={encoded_name}"
    max_attempts = max(1, int(delete_retries) + 1)
    for attempt in range(max_attempts):
        try:
            async with semaphore:
                async with session.delete(url, headers=headers_builder(token, False), timeout=timeout) as resp:
                    text = await resp.text()
                    data = maybe_json_loads(text)
                    ok = resp.status == 200 and isinstance(data, dict) and data.get("status") == "ok"
                    if ok:
                        return {
                            "name": name,
                            "ok": True,
                            "status_code": resp.status,
                            "error": None,
                            "attempts": attempt + 1,
                        }

                    should_retry = resp.status in {408, 425, 429} or resp.status >= 500
                    if should_retry and attempt < (max_attempts - 1):
                        wait_seconds = backoff(attempt)
                        if logger is not None:
                            logger.debug(
                                "删除失败准备重试: name=%s status=%s attempt=%s/%s wait=%.1fs",
                                name,
                                resp.status,
                                attempt + 1,
                                max_attempts,
                                wait_seconds,
                            )
                        await asyncio.sleep(wait_seconds)
                        continue

                    return {
                        "name": name,
                        "ok": False,
                        "status_code": resp.status,
                        "error": compact_text(text, 200),
                        "attempts": attempt + 1,
                    }
        except Exception as exc:
            if attempt < (max_attempts - 1):
                wait_seconds = backoff(attempt)
                if logger is not None:
                    logger.debug(
                        "删除异常准备重试: name=%s attempt=%s/%s error=%s wait=%.1fs",
                        name,
                        attempt + 1,
                        max_attempts,
                        exc,
                        wait_seconds,
                    )
                await asyncio.sleep(wait_seconds)
                continue

            return {"name": name, "ok": False, "status_code": None, "error": str(exc), "attempts": attempt + 1}

    return {"name": name, "ok": False, "status_code": None, "error": "unreachable", "attempts": 0}


async def set_account_disabled_async(
    session: Any,
    semaphore: asyncio.Semaphore,
    base_url: str,
    token: str,
    name: str,
    disabled: bool,
    timeout: int,
    *,
    headers_builder: Callable[[str, bool], dict[str, str]] = build_management_headers,
    maybe_json_loads: Callable[[Any], Any],
    compact_text: Callable[[Any, int], str | None],
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/v0/management/auth-files/status"
    payload = {"name": name, "disabled": bool(disabled)}
    try:
        async with semaphore:
            async with session.patch(
                url,
                headers=headers_builder(token, True),
                json=payload,
                timeout=timeout,
            ) as resp:
                text = await resp.text()
                data = maybe_json_loads(text)
                ok = resp.status == 200 and isinstance(data, dict) and data.get("status") == "ok"
                return {
                    "name": name,
                    "ok": ok,
                    "disabled": bool(disabled),
                    "status_code": resp.status,
                    "error": None if ok else compact_text(text, 200),
                }
    except Exception as exc:
        return {
            "name": name,
            "ok": False,
            "disabled": bool(disabled),
            "status_code": None,
            "error": str(exc),
        }
