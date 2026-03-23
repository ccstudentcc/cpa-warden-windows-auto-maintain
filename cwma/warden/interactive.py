from __future__ import annotations

import getpass


def prompt_string(label: str, default: str, *, secret: bool = False) -> str:
    shown_default = default or "空"
    prompt = f"{label}（默认 {shown_default}）: "
    raw = getpass.getpass(prompt) if secret else input(prompt)
    raw = raw.strip()
    return raw or default


def prompt_int(label: str, default: int, *, min_value: int = 0) -> int:
    raw = input(f"{label}（默认 {default}）: ").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        print("输入无效，使用默认值。")
        return default
    if value < min_value:
        print(f"输入过小，使用最小值 {min_value}。")
        return min_value
    return value


def prompt_float(label: str, default: float, *, min_value: float = 0.0, max_value: float | None = None) -> float:
    raw = input(f"{label}（默认 {default:g}）: ").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except Exception:
        print("输入无效，使用默认值。")
        return default
    if value < min_value:
        print(f"输入过小，使用最小值 {min_value:g}。")
        return min_value
    if max_value is not None and value > max_value:
        print(f"输入过大，使用最大值 {max_value:g}。")
        return max_value
    return value


def prompt_yes_no(label: str, default: bool) -> bool:
    default_text = "yes" if default else "no"
    raw = input(f"{label}（默认 {default_text}）[yes/no]: ").strip().lower()
    if not raw:
        return default
    if raw in {"y", "yes"}:
        return True
    if raw in {"n", "no"}:
        return False
    print("输入无效，使用默认值。")
    return default


def prompt_choice(label: str, options: list[str], default: str) -> str:
    raw = input(f"{label}（默认 {default}）[{ '/'.join(options) }]: ").strip().lower()
    if not raw:
        return default
    if raw in options:
        return raw
    print("输入无效，使用默认值。")
    return default

