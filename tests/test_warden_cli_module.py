from __future__ import annotations

import unittest

from cwma.warden.cli import build_parser, parse_cli_args


class WardenCliModuleTests(unittest.TestCase):
    def test_parse_cli_args_uses_injected_default_config(self) -> None:
        args = parse_cli_args("custom.json", ["--mode", "scan"])
        self.assertEqual(args.config, "custom.json")
        self.assertEqual(args.mode, "scan")

    def test_parse_cli_args_parses_representative_flags(self) -> None:
        args = parse_cli_args(
            "config.json",
            [
                "--mode",
                "maintain-refill",
                "--target-type",
                "openai",
                "--provider",
                "foo",
                "--probe-workers",
                "55",
                "--quota-action",
                "disable",
                "--maintain-steps",
                "scan,quota,finalize",
                "--quota-disable-threshold",
                "0.3",
                "--upload-method",
                "multipart",
                "--auto-register",
                "--upload-recursive",
                "--yes",
            ],
        )
        self.assertEqual(args.mode, "maintain-refill")
        self.assertEqual(args.target_type, "openai")
        self.assertEqual(args.provider, "foo")
        self.assertEqual(args.probe_workers, 55)
        self.assertEqual(args.quota_action, "disable")
        self.assertEqual(args.maintain_steps, "scan,quota,finalize")
        self.assertAlmostEqual(args.quota_disable_threshold, 0.3)
        self.assertEqual(args.upload_method, "multipart")
        self.assertTrue(args.auto_register)
        self.assertTrue(args.upload_recursive)
        self.assertTrue(args.yes)

    def test_mutually_exclusive_boolean_switch_defaults_are_none(self) -> None:
        args = parse_cli_args("config.json", ["--mode", "upload"])
        self.assertIsNone(args.delete_401)
        self.assertIsNone(args.auto_reenable)
        self.assertIsNone(args.auto_register)
        self.assertIsNone(args.upload_recursive)
        self.assertIsNone(args.upload_force)
        self.assertIsNone(args.debug)

    def test_mutually_exclusive_switch_last_selected_value(self) -> None:
        args = parse_cli_args("config.json", ["--mode", "maintain", "--no-delete-401", "--upload-force"])
        self.assertFalse(args.delete_401)
        self.assertTrue(args.upload_force)

    def test_build_parser_keeps_help_description(self) -> None:
        parser = build_parser("config.json")
        self.assertIn("交互式 CPA 账号维护脚本", parser.description or "")


if __name__ == "__main__":
    unittest.main()
