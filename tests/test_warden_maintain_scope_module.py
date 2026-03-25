from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cwma.warden.services.maintain_scope import (
    load_name_scope_file,
    resolve_maintain_name_scope,
    resolve_upload_name_scope,
)
from tests.temp_sandbox import TempSandboxState, setup_tempfile_sandbox, teardown_tempfile_sandbox

_TEMP_SANDBOX_STATE: TempSandboxState | None = None


def setUpModule() -> None:
    global _TEMP_SANDBOX_STATE
    _TEMP_SANDBOX_STATE = setup_tempfile_sandbox()


def tearDownModule() -> None:
    teardown_tempfile_sandbox(_TEMP_SANDBOX_STATE)


class WardenMaintainScopeModuleTests(unittest.TestCase):
    def test_load_name_scope_file_trims_and_dedups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scope_file = Path(tmp) / "scope.txt"
            scope_file.write_text("\n# comment\n  a.json \n\nb.json\na.json\n", encoding="utf-8")

            names = load_name_scope_file(str(scope_file), option_label="maintain_names_file")

            self.assertEqual(names, {"a.json", "b.json"})

    def test_load_name_scope_file_missing_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing.txt"
            with self.assertRaisesRegex(RuntimeError, "maintain_names_file 不存在"):
                load_name_scope_file(str(missing), option_label="maintain_names_file")

    def test_resolve_maintain_name_scope_mode_guard_and_empty_path(self) -> None:
        self.assertIsNone(resolve_maintain_name_scope({"mode": "scan", "maintain_names_file": "x.txt"}))
        self.assertIsNone(resolve_maintain_name_scope({"mode": "maintain", "maintain_names_file": "   "}))

    def test_resolve_maintain_name_scope_loads_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scope_file = Path(tmp) / "maintain_scope.txt"
            scope_file.write_text("one.json\ntwo.json\n", encoding="utf-8")
            names = resolve_maintain_name_scope(
                {"mode": "maintain", "maintain_names_file": str(scope_file)}
            )
            self.assertEqual(names, {"one.json", "two.json"})

    def test_resolve_upload_name_scope_mode_guard_and_missing(self) -> None:
        self.assertIsNone(resolve_upload_name_scope({"mode": "maintain", "upload_names_file": "x.txt"}))
        with self.assertRaisesRegex(RuntimeError, "upload_names_file 不存在"):
            resolve_upload_name_scope({"mode": "upload", "upload_names_file": "missing_scope.txt"})


if __name__ == "__main__":
    unittest.main()
