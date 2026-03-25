from __future__ import annotations

from pathlib import Path


def build_maintain_command(
    *,
    command_base: list[str],
    maintain_db_path: Path,
    maintain_log_file: Path,
    maintain_names_file: Path | None = None,
    maintain_steps: tuple[str, ...] | None = None,
    assume_yes: bool = False,
) -> list[str]:
    cmd = list(command_base) + [
        "--mode",
        "maintain",
        "--db-path",
        str(maintain_db_path),
        "--log-file",
        str(maintain_log_file),
    ]
    if maintain_names_file is not None:
        cmd.extend(["--maintain-names-file", str(maintain_names_file)])
    if maintain_steps:
        cmd.extend(["--maintain-steps", ",".join(maintain_steps)])
    if assume_yes:
        cmd.append("--yes")
    return cmd


def build_upload_command(
    *,
    command_base: list[str],
    auth_dir: Path,
    upload_db_path: Path,
    upload_log_file: Path,
    upload_names_file: Path | None = None,
) -> list[str]:
    cmd = list(command_base) + [
        "--mode",
        "upload",
        "--upload-dir",
        str(auth_dir),
        "--upload-recursive",
        "--db-path",
        str(upload_db_path),
        "--log-file",
        str(upload_log_file),
    ]
    if upload_names_file is not None:
        cmd.extend(["--upload-names-file", str(upload_names_file)])
    return cmd


def format_maintain_start_message(
    *,
    attempt: int,
    max_attempts: int,
    reason: str,
    maintain_scope_names: set[str] | None,
) -> str:
    scope_label = (
        f"incremental names={len(maintain_scope_names or set())}"
        if maintain_scope_names is not None
        else "full"
    )
    return (
        f"Starting maintain command attempt {attempt} of {max_attempts} "
        f"(reason={reason}, scope={scope_label})."
    )


def format_upload_start_message(
    *,
    attempt: int,
    max_attempts: int,
    reason: str,
    batch_size: int,
    pending_total: int,
) -> str:
    return (
        f"Starting upload command attempt {attempt} of {max_attempts} "
        f"(reason={reason}, batch_size={batch_size}, pending_total={pending_total})."
    )
