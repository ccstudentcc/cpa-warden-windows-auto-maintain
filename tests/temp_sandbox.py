from __future__ import annotations

import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SANDBOX_TEMP_KEYS = ("TMPDIR", "TEMP", "TMP")


@dataclass
class TempSandboxState:
    original_tempdir: str | None
    original_env: dict[str, str | None]
    original_temporary_directory: Any


class WorkspaceTemporaryDirectory:
    def __init__(
        self,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: str | os.PathLike[str] | None = None,
        ignore_cleanup_errors: bool = False,
    ) -> None:
        base_dir = Path(dir) if dir is not None else Path(tempfile.gettempdir())
        base_dir.mkdir(parents=True, exist_ok=True)
        resolved_prefix = prefix or "tmp"
        resolved_suffix = suffix or ""
        candidate = base_dir / f"{resolved_prefix}{time.time_ns()}{resolved_suffix}"
        candidate.mkdir(parents=True, exist_ok=False)
        self.name = str(candidate)
        self._ignore_cleanup_errors = ignore_cleanup_errors

    def __enter__(self) -> str:
        return self.name

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.cleanup()

    def cleanup(self) -> None:
        shutil.rmtree(self.name, ignore_errors=self._ignore_cleanup_errors)


def setup_tempfile_sandbox() -> TempSandboxState:
    sandbox_temp = (Path.cwd() / ".tmp_unittest_temp").resolve()
    sandbox_temp.mkdir(parents=True, exist_ok=True)
    state = TempSandboxState(
        original_tempdir=tempfile.tempdir,
        original_env={key: os.environ.get(key) for key in _SANDBOX_TEMP_KEYS},
        original_temporary_directory=tempfile.TemporaryDirectory,
    )
    for key in _SANDBOX_TEMP_KEYS:
        os.environ[key] = str(sandbox_temp)
    tempfile.tempdir = str(sandbox_temp)
    tempfile.TemporaryDirectory = WorkspaceTemporaryDirectory  # type: ignore[assignment]
    return state


def teardown_tempfile_sandbox(state: TempSandboxState | None) -> None:
    if state is None:
        return
    tempfile.tempdir = state.original_tempdir
    tempfile.TemporaryDirectory = state.original_temporary_directory
    for key, value in state.original_env.items():
        if value is None:
            os.environ.pop(key, None)
            continue
        os.environ[key] = value
