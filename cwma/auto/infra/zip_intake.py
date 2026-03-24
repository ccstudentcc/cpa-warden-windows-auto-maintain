from __future__ import annotations

import shutil
import subprocess
import zipfile
from collections.abc import Callable, MutableMapping
from pathlib import Path


def list_zip_paths(auth_dir: Path) -> list[Path]:
    return sorted(
        (p for p in auth_dir.rglob("*.zip") if p.is_file()),
        key=lambda p: str(p).lower(),
    )


def compute_zip_signature(auth_dir: Path, *, log: Callable[[str], None]) -> tuple[str, ...]:
    signature: list[str] = []
    skipped = 0
    for path in list_zip_paths(auth_dir):
        try:
            stat = path.stat()
        except (FileNotFoundError, OSError):
            skipped += 1
            continue
        signature.append(f"{path}|{stat.st_size}|{stat.st_mtime_ns}")
    signature.sort()
    if skipped > 0:
        log(f"[WARN] ZIP signature skipped transient files: {skipped}")
    return tuple(signature)


def count_zip_files(auth_dir: Path) -> int:
    return len(list_zip_paths(auth_dir))


def list_zip_json_entries(archive: zipfile.ZipFile) -> list[str]:
    entries: list[str] = []
    for info in archive.infolist():
        if info.is_dir():
            continue
        # ZipInfo names include nested paths, so this naturally supports recursive folder layouts in zip.
        if Path(info.filename).suffix.lower() == ".json":
            entries.append(info.filename)
    return entries


def ps_quote(value: str) -> str:
    return value.replace("'", "''")


def extract_zip_with_windows_builtin(
    *,
    zip_path: Path,
    output_dir: Path,
    base_dir: Path,
    timeout_seconds: int,
    log: Callable[[str], None],
) -> int:
    shell_candidates = ["powershell.exe", "pwsh.exe", "powershell", "pwsh"]
    shell_path = None
    for candidate in shell_candidates:
        resolved = shutil.which(candidate)
        if resolved:
            shell_path = resolved
            break
    if not shell_path:
        log(f"Windows built-in unzip shell not found for {zip_path.name}.")
        return 1

    cmd_script = (
        "$ErrorActionPreference='Stop';"
        f"Expand-Archive -LiteralPath '{ps_quote(str(zip_path))}' "
        f"-DestinationPath '{ps_quote(str(output_dir))}' -Force"
    )
    command = [shell_path, "-NoProfile", "-Command", cmd_script]
    try:
        completed = subprocess.run(
            command,
            cwd=str(base_dir),
            timeout=timeout_seconds,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        log(f"Windows unzip timeout: {zip_path.name}")
        return 1
    except OSError:
        return 1

    if completed.returncode == 0:
        log(f"Windows built-in extracted: {zip_path.name}")
        return 0

    stderr_text = (completed.stderr or "").strip()
    stdout_text = (completed.stdout or "").strip()
    if stderr_text:
        log(f"Windows unzip stderr ({zip_path.name}): {stderr_text[:200]}")
    elif stdout_text:
        log(f"Windows unzip output ({zip_path.name}): {stdout_text[:200]}")
    return 1


def extract_zip_with_bandizip(
    *,
    zip_path: Path,
    output_dir: Path,
    base_dir: Path,
    bandizip_path: str,
    timeout_seconds: int,
    log: Callable[[str], None],
) -> int:
    bandizip_candidates: list[str] = []
    configured = (bandizip_path or "").strip()
    if configured:
        bandizip_candidates.append(configured)
    if configured.lower() != "bandizip.exe":
        bandizip_candidates.append("Bandizip.exe")
    if configured.lower() != "bz.exe":
        bandizip_candidates.append("bz.exe")

    tried_commands: list[str] = []
    for candidate in bandizip_candidates:
        resolved = shutil.which(candidate) if not Path(candidate).exists() else candidate
        if not resolved:
            continue

        commands = [
            [resolved, "x", "-y", f"-o:{output_dir}", str(zip_path)],
            [resolved, "x", "-y", "-o", str(output_dir), str(zip_path)],
        ]
        for cmd in commands:
            tried_commands.append(" ".join(cmd))
            try:
                completed = subprocess.run(
                    cmd,
                    cwd=str(base_dir),
                    timeout=timeout_seconds,
                    capture_output=True,
                    text=True,
                )
            except subprocess.TimeoutExpired:
                log(f"Bandizip extract timeout: {zip_path.name}")
                continue
            except OSError:
                continue

            if completed.returncode == 0:
                log(f"Bandizip extracted: {zip_path.name}")
                return 0

            stderr_text = (completed.stderr or "").strip()
            stdout_text = (completed.stdout or "").strip()
            if stderr_text:
                log(f"Bandizip stderr ({zip_path.name}): {stderr_text[:200]}")
            elif stdout_text:
                log(f"Bandizip output ({zip_path.name}): {stdout_text[:200]}")

    if tried_commands:
        log(f"Bandizip extract failed: {zip_path.name}")
    else:
        log(
            f"Bandizip not found for {zip_path.name}. "
            "Set BANDIZIP_PATH or ensure Bandizip.exe/bz.exe is in PATH."
        )
    return 1


def inspect_zip_archives(
    *,
    auth_dir: Path,
    inspect_zip_files: bool,
    auto_extract_zip_json: bool,
    delete_zip_after_extract: bool,
    processed_signatures: MutableMapping[str, str],
    extract_zip: Callable[[Path, Path], int],
    log: Callable[[str], None],
    detail_limit: int = 10,
) -> bool:
    if not inspect_zip_files:
        return False

    zip_paths = list_zip_paths(auth_dir)
    if not zip_paths:
        return False

    current_zip_keys = {str(path).lower() for path in zip_paths}
    stale_keys = [key for key in processed_signatures if key not in current_zip_keys]
    for stale_key in stale_keys:
        processed_signatures.pop(stale_key, None)

    with_json = 0
    without_json = 0
    invalid = 0
    total_json_entries = 0
    extracted = 0
    extract_failed = 0
    deleted_zip = 0
    skipped_already_processed = 0
    any_changed = False
    detail_lines: list[str] = []

    for path in zip_paths:
        path_key = str(path).lower()
        try:
            stat = path.stat()
            archive_signature = f"{stat.st_size}|{stat.st_mtime_ns}"
        except (FileNotFoundError, OSError):
            continue

        try:
            with zipfile.ZipFile(path, "r") as zf:
                json_entries = list_zip_json_entries(zf)
        except (zipfile.BadZipFile, OSError, RuntimeError):
            invalid += 1
            processed_signatures.pop(path_key, None)
            if len(detail_lines) < detail_limit:
                detail_lines.append(f"{path.name}: invalid zip")
            continue

        json_count = len(json_entries)
        total_json_entries += json_count
        if json_count > 0:
            with_json += 1
            if auto_extract_zip_json:
                already_processed = (
                    (not delete_zip_after_extract)
                    and processed_signatures.get(path_key) == archive_signature
                )
                if already_processed:
                    skipped_already_processed += 1
                    if len(detail_lines) < detail_limit:
                        detail_lines.append(f"{path.name}: already processed")
                else:
                    extract_exit = extract_zip(path, auth_dir)
                    if extract_exit == 0:
                        extracted += 1
                        any_changed = True
                        if delete_zip_after_extract:
                            try:
                                path.unlink()
                                deleted_zip += 1
                                any_changed = True
                                processed_signatures.pop(path_key, None)
                            except OSError:
                                if len(detail_lines) < detail_limit:
                                    detail_lines.append(
                                        f"{path.name}: extracted but failed to delete zip"
                                    )
                        else:
                            processed_signatures[path_key] = archive_signature
                    else:
                        extract_failed += 1
                        processed_signatures.pop(path_key, None)
            if len(detail_lines) < detail_limit:
                detail_lines.append(f"{path.name}: json_entries={json_count}")
        else:
            without_json += 1
            processed_signatures.pop(path_key, None)
            if len(detail_lines) < detail_limit:
                detail_lines.append(f"{path.name}: no json")

    log(
        "ZIP scan summary: "
        f"total_zip={len(zip_paths)}, with_json={with_json}, "
        f"without_json={without_json}, invalid_zip={invalid}, "
        f"total_json_entries={total_json_entries}, extracted={extracted}, "
        f"extract_failed={extract_failed}, deleted_zip={deleted_zip}, "
        f"skipped_already_processed={skipped_already_processed}"
    )
    for line in detail_lines:
        log(f"ZIP detail: {line}")
    if with_json > 0:
        if auto_extract_zip_json:
            log("ZIP note: JSON zip archives were processed via Bandizip.")
        else:
            log("ZIP note: uploader only handles extracted .json files in upload directory.")
    return any_changed
