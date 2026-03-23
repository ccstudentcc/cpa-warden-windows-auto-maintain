from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from ..common import load_json_object as load_json_object_common
from ..common import parse_bool_value as parse_bool_common


DEFAULT_SETTINGS_VALUES: dict[str, Any] = {
    "config_path": "config.json",
    "target_type": "codex",
    "user_agent": "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal",
    "probe_workers": 100,
    "action_workers": 100,
    "timeout": 15,
    "retries": 3,
    "delete_retries": 2,
    "quota_action": "disable",
    "delete_401": True,
    "auto_reenable": True,
    "reenable_scope": "signal",
    "db_path": "cpa_warden_state.sqlite3",
    "invalid_output": "cpa_warden_401_accounts.json",
    "quota_output": "cpa_warden_quota_accounts.json",
    "log_file": "cpa_warden.log",
    "upload_workers": 20,
    "upload_retries": 2,
    "upload_method": "json",
    "upload_recursive": False,
    "upload_force": False,
    "refill_strategy": "to-threshold",
    "auto_register": False,
    "register_timeout": 300,
    "quota_disable_threshold": 0.0,
}


def build_default_settings_values(**overrides: Any) -> dict[str, Any]:
    values = dict(DEFAULT_SETTINGS_VALUES)
    values.update(overrides)
    return values


def config_lookup(conf: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in conf and conf.get(key) not in (None, ""):
            return conf.get(key)
    return default


def parse_bool_config(value: Any, *, key: str) -> bool:
    try:
        return parse_bool_common(
            key,
            value,
            true_values={"1", "true", "yes", "y", "on"},
            false_values={"0", "false", "no", "n", "off"},
        )
    except ValueError:
        if isinstance(value, (bool, int, str)):
            raise RuntimeError(f"{key} 必须是布尔值（true/false 或 1/0），当前={value!r}") from None
        raise RuntimeError(f"{key} 必须是布尔值（true/false 或 1/0），当前类型={type(value).__name__}") from None


def load_config_json(path: str, required: bool = False) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        if required:
            raise RuntimeError(f"配置文件不存在: {path}")
        return {}

    try:
        data = load_json_object_common(p, encoding="utf-8")
    except ValueError as exc:
        raise RuntimeError(f"读取配置文件失败: {exc}") from exc

    return data


def build_settings(
    args: argparse.Namespace,
    conf: dict[str, Any],
    *,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    values = build_default_settings_values(**(defaults or {}))
    quota_action = str(
        args.quota_action
        or config_lookup(conf, "quota_action", default=values["quota_action"])
    ).strip().lower()
    if quota_action not in {"disable", "delete"}:
        raise RuntimeError("quota_action 只能是 disable 或 delete")

    upload_method = str(
        args.upload_method
        or config_lookup(conf, "upload_method", default=values["upload_method"])
    ).strip().lower()
    if upload_method not in {"json", "multipart"}:
        raise RuntimeError("upload_method 只能是 json 或 multipart")

    refill_strategy = str(
        args.refill_strategy
        or config_lookup(conf, "refill_strategy", default=values["refill_strategy"])
    ).strip().lower()
    if refill_strategy not in {"to-threshold", "fixed"}:
        raise RuntimeError("refill_strategy 只能是 to-threshold 或 fixed")

    reenable_scope = str(
        args.reenable_scope
        or config_lookup(conf, "reenable_scope", default=values["reenable_scope"])
    ).strip().lower()
    if reenable_scope not in {"signal", "managed"}:
        raise RuntimeError("reenable_scope 只能是 signal 或 managed")

    settings = {
        "config_path": args.config,
        "base_url": str(config_lookup(conf, "base_url", default="")).strip(),
        "token": str(config_lookup(conf, "token", default="")).strip(),
        "mode": args.mode,
        "target_type": str(
            args.target_type
            or config_lookup(conf, "target_type", default=values["target_type"])
        ).strip(),
        "provider": str(
            args.provider if args.provider is not None else config_lookup(conf, "provider", default="")
        ).strip(),
        "probe_workers": int(
            args.probe_workers
            if args.probe_workers is not None
            else config_lookup(conf, "probe_workers", "workers", default=values["probe_workers"])
        ),
        "action_workers": int(
            args.action_workers
            if args.action_workers is not None
            else config_lookup(conf, "action_workers", "delete_workers", default=values["action_workers"])
        ),
        "timeout": int(
            args.timeout if args.timeout is not None else config_lookup(conf, "timeout", default=values["timeout"])
        ),
        "retries": int(
            args.retries if args.retries is not None else config_lookup(conf, "retries", default=values["retries"])
        ),
        "delete_retries": int(
            args.delete_retries
            if args.delete_retries is not None
            else config_lookup(conf, "delete_retries", "action_retries", default=values["delete_retries"])
        ),
        "quota_action": quota_action,
        "quota_disable_threshold": float(
            args.quota_disable_threshold
            if args.quota_disable_threshold is not None
            else config_lookup(conf, "quota_disable_threshold", default=values["quota_disable_threshold"])
        ),
        "delete_401": (
            args.delete_401
            if args.delete_401 is not None
            else parse_bool_config(config_lookup(conf, "delete_401", default=values["delete_401"]), key="delete_401")
        ),
        "auto_reenable": (
            args.auto_reenable
            if args.auto_reenable is not None
            else parse_bool_config(
                config_lookup(conf, "auto_reenable", default=values["auto_reenable"]),
                key="auto_reenable",
            )
        ),
        "reenable_scope": reenable_scope,
        "maintain_names_file": str(
            args.maintain_names_file
            if args.maintain_names_file is not None
            else config_lookup(conf, "maintain_names_file", default="")
        ).strip(),
        "upload_names_file": str(
            args.upload_names_file
            if args.upload_names_file is not None
            else config_lookup(conf, "upload_names_file", default="")
        ).strip(),
        "db_path": str(
            args.db_path
            or config_lookup(conf, "db_path", default=values["db_path"])
        ).strip(),
        "invalid_output": str(
            args.invalid_output
            or config_lookup(conf, "invalid_output", "output", default=values["invalid_output"])
        ).strip(),
        "quota_output": str(
            args.quota_output
            or config_lookup(conf, "quota_output", default=values["quota_output"])
        ).strip(),
        "log_file": str(
            args.log_file
            or config_lookup(conf, "log_file", default=values["log_file"])
        ).strip(),
        "user_agent": str(
            args.user_agent
            or config_lookup(conf, "user_agent", default=values["user_agent"])
        ).strip(),
        "debug": bool(
            args.debug
            if args.debug is not None
            else parse_bool_config(config_lookup(conf, "debug", default=False), key="debug")
        ),
        "assume_yes": bool(args.yes),
        "upload_dir": str(
            args.upload_dir
            if args.upload_dir is not None
            else config_lookup(conf, "upload_dir", default="")
        ).strip(),
        "upload_workers": int(
            args.upload_workers
            if args.upload_workers is not None
            else config_lookup(conf, "upload_workers", default=values["upload_workers"])
        ),
        "upload_retries": int(
            args.upload_retries
            if args.upload_retries is not None
            else config_lookup(conf, "upload_retries", default=values["upload_retries"])
        ),
        "upload_method": upload_method,
        "upload_recursive": (
            args.upload_recursive
            if args.upload_recursive is not None
            else parse_bool_config(
                config_lookup(conf, "upload_recursive", default=values["upload_recursive"]),
                key="upload_recursive",
            )
        ),
        "upload_force": (
            args.upload_force
            if args.upload_force is not None
            else parse_bool_config(config_lookup(conf, "upload_force", default=values["upload_force"]), key="upload_force")
        ),
        "min_valid_accounts": int(
            args.min_valid_accounts
            if args.min_valid_accounts is not None
            else config_lookup(conf, "min_valid_accounts", default=0)
        ),
        "refill_strategy": refill_strategy,
        "auto_register": (
            args.auto_register
            if args.auto_register is not None
            else parse_bool_config(config_lookup(conf, "auto_register", default=values["auto_register"]), key="auto_register")
        ),
        "register_command": str(
            args.register_command
            if args.register_command is not None
            else config_lookup(conf, "register_command", default="")
        ).strip(),
        "register_timeout": int(
            args.register_timeout
            if args.register_timeout is not None
            else config_lookup(conf, "register_timeout", default=values["register_timeout"])
        ),
        "register_workdir": str(
            args.register_workdir
            if args.register_workdir is not None
            else config_lookup(conf, "register_workdir", default="")
        ).strip(),
    }

    if settings["probe_workers"] < 1:
        raise RuntimeError("probe_workers 必须 >= 1")
    if settings["action_workers"] < 1:
        raise RuntimeError("action_workers 必须 >= 1")
    if settings["timeout"] < 1:
        raise RuntimeError("timeout 必须 >= 1")
    if settings["retries"] < 0:
        raise RuntimeError("retries 不能小于 0")
    if settings["delete_retries"] < 0:
        raise RuntimeError("delete_retries 不能小于 0")
    if settings["quota_disable_threshold"] < 0 or settings["quota_disable_threshold"] > 1:
        raise RuntimeError("quota_disable_threshold 必须在 0 到 1 之间")
    if settings["upload_workers"] < 1:
        raise RuntimeError("upload_workers 必须 >= 1")
    if settings["upload_retries"] < 0:
        raise RuntimeError("upload_retries 不能小于 0")
    if settings["register_timeout"] < 1:
        raise RuntimeError("register_timeout 必须 >= 1")
    if not settings["target_type"]:
        raise RuntimeError("target_type 不能为空")
    if not settings["log_file"]:
        raise RuntimeError("log_file 不能为空")
    if settings["mode"] in {"upload", "maintain-refill"} and not settings["upload_dir"]:
        raise RuntimeError("upload/maintain-refill 模式下必须提供 upload_dir")
    if settings["mode"] == "maintain-refill":
        if settings["min_valid_accounts"] < 1:
            raise RuntimeError("maintain-refill 模式下 min_valid_accounts 必须 >= 1")
        if settings["auto_register"] and not settings["register_command"]:
            raise RuntimeError("auto_register=true 时必须提供 register_command")

    return settings

