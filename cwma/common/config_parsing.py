from __future__ import annotations

import json
from pathlib import Path


def parse_bool_value(
    name: str,
    raw: object,
    *,
    true_values: set[str] | None = None,
    false_values: set[str] | None = None,
) -> bool:
    true_tokens = true_values or {"1", "true", "yes", "on"}
    false_tokens = false_values or {"0", "false", "no", "off"}

    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        if raw in {0, 1}:
            return bool(raw)
        raise ValueError(f"{name} must be 0/1/true/false/yes/no/on/off, got: {raw}")
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in true_tokens:
            return True
        if normalized in false_tokens:
            return False
    raise ValueError(f"{name} must be 0/1/true/false/yes/no/on/off, got: {raw}")


def parse_int_value(name: str, raw: object, minimum: int) -> int:
    if isinstance(raw, bool):
        raise ValueError(f"{name} must be an integer, got: {raw}")
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str):
        try:
            value = int(raw.strip())
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer, got: {raw}") from exc
    else:
        raise ValueError(f"{name} must be an integer, got: {raw}")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got: {value}")
    return value


def resolve_path(base_dir: Path, raw: str) -> Path:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p


def load_json_object(path: Path, *, encoding: str = "utf-8-sig") -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding=encoding))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON config: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def pick_setting(
    env_name: str,
    config_data: dict[str, object],
    config_key: str,
    default: object,
) -> object:
    import os

    env_raw = os.getenv(env_name)
    if env_raw is not None and env_raw.strip() != "":
        return env_raw.strip()
    cfg_raw = config_data.get(config_key)
    if cfg_raw is not None and not (isinstance(cfg_raw, str) and cfg_raw.strip() == ""):
        return cfg_raw
    return default
