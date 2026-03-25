from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path

from cwma.warden.config import (
    build_default_settings_values,
    build_settings,
    config_lookup,
    load_config_json,
    parse_bool_config,
)
from tests.temp_sandbox import TempSandboxState, setup_tempfile_sandbox, teardown_tempfile_sandbox

_TEMP_SANDBOX_STATE: TempSandboxState | None = None


def setUpModule() -> None:
    global _TEMP_SANDBOX_STATE
    _TEMP_SANDBOX_STATE = setup_tempfile_sandbox()


def tearDownModule() -> None:
    teardown_tempfile_sandbox(_TEMP_SANDBOX_STATE)


def _build_args(**overrides: object) -> argparse.Namespace:
    base: dict[str, object] = {
        "config": "config.json",
        "mode": "scan",
        "target_type": None,
        "provider": None,
        "probe_workers": None,
        "action_workers": None,
        "timeout": None,
        "retries": None,
        "delete_retries": None,
        "user_agent": None,
        "quota_action": None,
        "quota_disable_threshold": None,
        "maintain_names_file": None,
        "upload_names_file": None,
        "db_path": None,
        "invalid_output": None,
        "quota_output": None,
        "log_file": None,
        "debug": None,
        "upload_dir": None,
        "upload_workers": None,
        "upload_retries": None,
        "upload_method": None,
        "min_valid_accounts": None,
        "refill_strategy": None,
        "register_command": None,
        "register_timeout": None,
        "register_workdir": None,
        "upload_force": None,
        "delete_401": None,
        "auto_reenable": None,
        "reenable_scope": None,
        "auto_register": None,
        "upload_recursive": None,
        "yes": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


class WardenConfigModuleTests(unittest.TestCase):
    def test_config_lookup_prefers_first_non_empty_key(self) -> None:
        conf = {"a": "", "b": "x", "c": "y"}
        self.assertEqual(config_lookup(conf, "a", "b", "c", default="z"), "x")
        self.assertEqual(config_lookup(conf, "missing", default="z"), "z")

    def test_parse_bool_config_supports_yes_no(self) -> None:
        self.assertTrue(parse_bool_config("yes", key="flag"))
        self.assertFalse(parse_bool_config("n", key="flag"))
        with self.assertRaises(RuntimeError):
            parse_bool_config("maybe", key="flag")

    def test_load_config_json_missing_required_and_optional(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = str(Path(tmp) / "missing.json")
            self.assertEqual(load_config_json(missing, required=False), {})
            with self.assertRaises(RuntimeError):
                load_config_json(missing, required=True)

    def test_load_config_json_reads_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"token": "abc"}), encoding="utf-8")
            self.assertEqual(load_config_json(str(path), required=True)["token"], "abc")

    def test_build_settings_arg_overrides_conf(self) -> None:
        args = _build_args(
            mode="upload",
            probe_workers=9,
            upload_dir="from-arg",
            upload_method="multipart",
            upload_recursive=True,
            upload_force=True,
        )
        conf = {
            "probe_workers": 77,
            "upload_dir": "from-conf",
            "upload_method": "json",
            "upload_recursive": False,
            "upload_force": False,
        }
        settings = build_settings(args, conf)
        self.assertEqual(settings["probe_workers"], 9)
        self.assertEqual(settings["upload_dir"], "from-arg")
        self.assertEqual(settings["upload_method"], "multipart")
        self.assertTrue(settings["upload_recursive"])
        self.assertTrue(settings["upload_force"])

    def test_build_settings_uses_conf_and_alias_defaults(self) -> None:
        args = _build_args()
        conf = {"workers": 66, "delete_workers": 55, "debug": "true"}
        settings = build_settings(args, conf)
        self.assertEqual(settings["probe_workers"], 66)
        self.assertEqual(settings["action_workers"], 55)
        self.assertTrue(settings["debug"])

    def test_build_settings_validates_upload_dir_for_upload_mode(self) -> None:
        args = _build_args(mode="upload")
        with self.assertRaises(RuntimeError):
            build_settings(args, {})

    def test_build_settings_validates_refill_constraints(self) -> None:
        args = _build_args(mode="maintain-refill", upload_dir="x", min_valid_accounts=1, auto_register=True)
        with self.assertRaises(RuntimeError):
            build_settings(args, {})

    def test_build_settings_accepts_custom_defaults(self) -> None:
        args = _build_args()
        defaults = build_default_settings_values(target_type="openai")
        settings = build_settings(args, {}, defaults=defaults)
        self.assertEqual(settings["target_type"], "openai")


if __name__ == "__main__":
    unittest.main()
