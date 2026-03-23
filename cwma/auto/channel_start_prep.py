from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class MaintainStartPrep:
    scope_names: set[str] | None
    scope_file: Path | None
    command: list[str]
    log_message: str
    started_incremental: bool


@dataclass(frozen=True)
class UploadStartPrep:
    batch: list[str]
    scope_names: set[str]
    scope_file: Path | None
    command: list[str]
    log_message: str


def prepare_maintain_start(
    *,
    reason: str,
    attempt: int,
    max_attempts: int,
    scope_names: set[str] | None,
    write_scope_file: Callable[[set[str]], Path],
    build_command: Callable[[Path | None], list[str]],
    format_start_message: Callable[[int, int, str, set[str] | None], str],
) -> MaintainStartPrep:
    scope_file: Path | None = None
    if scope_names is not None:
        scope_file = write_scope_file(scope_names)
    command = build_command(scope_file)
    log_message = format_start_message(attempt, max_attempts, reason, scope_names)
    return MaintainStartPrep(
        scope_names=scope_names,
        scope_file=scope_file,
        command=command,
        log_message=log_message,
        started_incremental=scope_names is not None,
    )


def prepare_upload_start(
    *,
    reason: str,
    attempt: int,
    max_attempts: int,
    batch: list[str],
    pending_total: int,
    extract_scope_names: Callable[[list[str]], set[str]],
    write_scope_file: Callable[[set[str]], Path],
    build_command: Callable[[Path | None], list[str]],
    format_start_message: Callable[[int, int, str, int, int], str],
) -> UploadStartPrep:
    scope_names = extract_scope_names(batch)
    scope_file: Path | None = None
    if scope_names:
        scope_file = write_scope_file(scope_names)
    command = build_command(scope_file)
    log_message = format_start_message(
        attempt,
        max_attempts,
        reason,
        len(batch),
        pending_total,
    )
    return UploadStartPrep(
        batch=list(batch),
        scope_names=scope_names,
        scope_file=scope_file,
        command=command,
        log_message=log_message,
    )
