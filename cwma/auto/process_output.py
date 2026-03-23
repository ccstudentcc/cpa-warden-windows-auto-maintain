from __future__ import annotations

import locale
import os
from collections.abc import Mapping


def preferred_decoding_order() -> list[str]:
    order = ["utf-8", "utf-8-sig", "gb18030", "cp936"]
    preferred = locale.getpreferredencoding(False).strip()
    if preferred and preferred.lower() not in {item.lower() for item in order}:
        order.append(preferred)
    return order


def decode_child_output_line(raw: bytes | str) -> str:
    if isinstance(raw, str):
        return raw
    if not raw:
        return ""
    for enc in preferred_decoding_order():
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def build_child_process_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


def should_log_child_alert_line(text: str) -> bool:
    lower = text.lower()
    if "timeout=" in lower:
        return False
    if "| error |" in lower or "| warn |" in lower or "| warning |" in lower:
        return True
    if "[error]" in lower or "[warn]" in lower:
        return True
    if "traceback" in lower or "failed" in lower or "timed out" in lower:
        return True
    if " 错误" in text or "失败" in text or "超时" in text:
        return True
    return False
