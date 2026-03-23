from __future__ import annotations

import unittest
from unittest import mock

from cwma.warden.interactive import (
    prompt_choice,
    prompt_float,
    prompt_int,
    prompt_string,
    prompt_yes_no,
)


class WardenInteractiveModuleTests(unittest.TestCase):
    def test_prompt_string_returns_default_when_empty(self) -> None:
        with mock.patch("builtins.input", return_value="   "):
            self.assertEqual(prompt_string("label", "default"), "default")

    def test_prompt_string_uses_getpass_when_secret(self) -> None:
        with mock.patch("getpass.getpass", return_value="  token  ") as getpass_mock:
            self.assertEqual(prompt_string("token", "", secret=True), "token")
            getpass_mock.assert_called_once()

    def test_prompt_int_invalid_uses_default(self) -> None:
        with mock.patch("builtins.input", return_value="oops"), mock.patch("builtins.print") as print_mock:
            self.assertEqual(prompt_int("workers", 10), 10)
            print_mock.assert_called_once_with("输入无效，使用默认值。")

    def test_prompt_int_below_min_uses_min(self) -> None:
        with mock.patch("builtins.input", return_value="-1"), mock.patch("builtins.print") as print_mock:
            self.assertEqual(prompt_int("workers", 10, min_value=2), 2)
            print_mock.assert_called_once_with("输入过小，使用最小值 2。")

    def test_prompt_float_bounds_and_invalid(self) -> None:
        with mock.patch("builtins.input", return_value="abc"), mock.patch("builtins.print") as print_mock:
            self.assertEqual(prompt_float("ratio", 0.2), 0.2)
            print_mock.assert_called_once_with("输入无效，使用默认值。")

        with mock.patch("builtins.input", return_value="-1"), mock.patch("builtins.print") as print_mock:
            self.assertEqual(prompt_float("ratio", 0.2, min_value=0.0), 0.0)
            print_mock.assert_called_once_with("输入过小，使用最小值 0。")

        with mock.patch("builtins.input", return_value="9"), mock.patch("builtins.print") as print_mock:
            self.assertEqual(prompt_float("ratio", 0.2, min_value=0.0, max_value=1.0), 1.0)
            print_mock.assert_called_once_with("输入过大，使用最大值 1。")

    def test_prompt_yes_no_parses_and_defaults(self) -> None:
        with mock.patch("builtins.input", return_value="yes"):
            self.assertTrue(prompt_yes_no("flag", False))
        with mock.patch("builtins.input", return_value="n"):
            self.assertFalse(prompt_yes_no("flag", True))
        with mock.patch("builtins.input", return_value=""), mock.patch("builtins.print") as print_mock:
            self.assertTrue(prompt_yes_no("flag", True))
            print_mock.assert_not_called()
        with mock.patch("builtins.input", return_value="invalid"), mock.patch("builtins.print") as print_mock:
            self.assertFalse(prompt_yes_no("flag", False))
            print_mock.assert_called_once_with("输入无效，使用默认值。")

    def test_prompt_choice_parses_and_defaults(self) -> None:
        with mock.patch("builtins.input", return_value="b"):
            self.assertEqual(prompt_choice("mode", ["a", "b"], "a"), "b")
        with mock.patch("builtins.input", return_value=""), mock.patch("builtins.print") as print_mock:
            self.assertEqual(prompt_choice("mode", ["a", "b"], "a"), "a")
            print_mock.assert_not_called()
        with mock.patch("builtins.input", return_value="x"), mock.patch("builtins.print") as print_mock:
            self.assertEqual(prompt_choice("mode", ["a", "b"], "a"), "a")
            print_mock.assert_called_once_with("输入无效，使用默认值。")


if __name__ == "__main__":
    unittest.main()

