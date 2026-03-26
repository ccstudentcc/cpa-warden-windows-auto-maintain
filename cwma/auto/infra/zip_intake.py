from __future__ import annotations

import os
import re
import shutil
import subprocess
import zipfile
from collections.abc import Callable, Iterable, MutableMapping, Sequence
from pathlib import Path

DEFAULT_ARCHIVE_EXTENSIONS: tuple[str, ...] = (".zip", ".7z", ".rar")


def normalize_archive_extensions(archive_extensions: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for ext in archive_extensions:
        value = ext.strip().lower()
        if not value:
            continue
        if not value.startswith("."):
            value = f".{value}"
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    if not normalized:
        return DEFAULT_ARCHIVE_EXTENSIONS
    return tuple(normalized)


def list_archive_paths(
    auth_dir: Path,
    *,
    archive_extensions: Sequence[str] = DEFAULT_ARCHIVE_EXTENSIONS,
) -> list[Path]:
    normalized_extensions = normalize_archive_extensions(archive_extensions)
    paths: list[Path] = []
    for extension in normalized_extensions:
        paths.extend(p for p in auth_dir.rglob(f"*{extension}") if p.is_file())
    return sorted(paths, key=lambda p: str(p).lower())


def list_zip_paths(
    auth_dir: Path,
    *,
    archive_extensions: Sequence[str] = DEFAULT_ARCHIVE_EXTENSIONS,
) -> list[Path]:
    # Kept for compatibility with existing imports/tests; now includes multi-format archive files.
    return list_archive_paths(auth_dir, archive_extensions=archive_extensions)


def compute_zip_signature(
    auth_dir: Path,
    *,
    log: Callable[[str], None],
    archive_extensions: Sequence[str] = DEFAULT_ARCHIVE_EXTENSIONS,
) -> tuple[str, ...]:
    signature: list[str] = []
    skipped = 0
    for path in list_archive_paths(auth_dir, archive_extensions=archive_extensions):
        try:
            stat = path.stat()
        except (FileNotFoundError, OSError):
            skipped += 1
            continue
        signature.append(f"{path}|{stat.st_size}|{stat.st_mtime_ns}")
    signature.sort()
    if skipped > 0:
        log(f"[WARN] Archive signature skipped transient files: {skipped}")
    return tuple(signature)


def count_zip_files(
    auth_dir: Path,
    *,
    archive_extensions: Sequence[str] = DEFAULT_ARCHIVE_EXTENSIONS,
) -> int:
    return len(list_archive_paths(auth_dir, archive_extensions=archive_extensions))


def list_zip_json_entries(archive: zipfile.ZipFile) -> list[str]:
    entries: list[str] = []
    for info in archive.infolist():
        if info.is_dir():
            continue
        # ZipInfo names include nested paths, so this naturally supports recursive folder layouts in zip.
        if Path(info.filename).suffix.lower() == ".json":
            entries.append(info.filename)
    return entries


def _iter_bandizip_candidates(
    *,
    bandizip_path: str,
    prefer_console: bool,
) -> list[str]:
    configured = (bandizip_path or "").strip()
    ordered: list[str] = []
    if configured:
        configured_path = Path(configured)
        configured_name = configured_path.name.lower()
        if prefer_console and configured_name == "bandizip.exe":
            # In console-preferred mode we skip GUI Bandizip.exe to avoid long hangs/timeouts.
            ordered.append(str(configured_path.with_name("bc.exe")))
            ordered.append(str(configured_path.with_name("bz.exe")))
        else:
            ordered.append(configured)
            if configured_name == "bandizip.exe":
                ordered.append(str(configured_path.with_name("bc.exe")))
                ordered.append(str(configured_path.with_name("bz.exe")))
            elif configured_name == "bz.exe":
                ordered.append(str(configured_path.with_name("bc.exe")))
                if not prefer_console:
                    ordered.append(str(configured_path.with_name("Bandizip.exe")))
            elif configured_name == "bc.exe":
                ordered.append(str(configured_path.with_name("bz.exe")))
                if not prefer_console:
                    ordered.append(str(configured_path.with_name("Bandizip.exe")))

    if prefer_console:
        ordered.extend(["bc.exe", "bz.exe"])
    else:
        ordered.extend(["Bandizip.exe", "bc.exe", "bz.exe"])

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in ordered:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _resolve_executable(candidate: str) -> str | None:
    candidate_path = Path(candidate)
    if candidate_path.is_absolute():
        return str(candidate_path) if candidate_path.exists() else None
    if candidate_path.exists():
        return str(candidate_path)
    return shutil.which(candidate)


def _subprocess_window_kwargs(*, hide_window: bool) -> dict[str, object]:
    if not hide_window or os.name != "nt":
        return {}
    kwargs: dict[str, object] = {}
    create_no_window = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if create_no_window:
        kwargs["creationflags"] = create_no_window
    startup_info_factory = getattr(subprocess, "STARTUPINFO", None)
    if startup_info_factory is not None:
        startup_info = startup_info_factory()
        startup_info.dwFlags |= int(getattr(subprocess, "STARTF_USESHOWWINDOW", 0))
        startup_info.wShowWindow = 0
        kwargs["startupinfo"] = startup_info
    return kwargs


def _run_command(
    *,
    cmd: list[str],
    base_dir: Path,
    timeout_seconds: int,
    hide_window: bool,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(base_dir),
        timeout=timeout_seconds,
        capture_output=True,
        text=True,
        **_subprocess_window_kwargs(hide_window=hide_window),
    )


def _parse_bandizip_json_entries(output: str) -> list[str]:
    entries: list[str] = []
    seen: set[str] = set()
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or ".json" not in line.lower():
            continue
        match = re.search(r"([^\r\n]*?\.json)", line, flags=re.IGNORECASE)
        if not match:
            continue
        value = match.group(1).strip().strip('"')
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        entries.append(value)
    if not entries and ".json" in output.lower():
        # Fallback marker when listing output format is unexpected but JSON evidence exists.
        return ["<json-detected>"]
    return entries


def list_non_zip_json_entries_with_bandizip(
    *,
    archive_path: Path,
    base_dir: Path,
    bandizip_path: str,
    timeout_seconds: int,
    prefer_console: bool,
    hide_window: bool,
    log: Callable[[str], None],
) -> list[str] | None:
    tried_commands = False
    for candidate in _iter_bandizip_candidates(
        bandizip_path=bandizip_path,
        prefer_console=prefer_console,
    ):
        resolved = _resolve_executable(candidate)
        if not resolved:
            continue

        commands = (
            [resolved, "l", str(archive_path)],
            [resolved, "l", "-y", str(archive_path)],
        )
        for cmd in commands:
            tried_commands = True
            try:
                completed = _run_command(
                    cmd=cmd,
                    base_dir=base_dir,
                    timeout_seconds=timeout_seconds,
                    hide_window=hide_window,
                )
            except subprocess.TimeoutExpired:
                log(f"Bandizip list timeout ({Path(resolved).name}): {archive_path.name}")
                continue
            except OSError:
                continue

            if completed.returncode == 0:
                return _parse_bandizip_json_entries(completed.stdout or "")

            stderr_text = (completed.stderr or "").strip()
            stdout_text = (completed.stdout or "").strip()
            if stderr_text:
                log(f"Bandizip list stderr ({archive_path.name}): {stderr_text[:200]}")
            elif stdout_text:
                log(f"Bandizip list output ({archive_path.name}): {stdout_text[:200]}")

    if not tried_commands:
        log(
            f"Bandizip list command unavailable for {archive_path.name}. "
            "Set BANDIZIP_PATH or ensure Bandizip.exe/bc.exe/bz.exe is in PATH."
        )
    return None


def ps_quote(value: str) -> str:
    return value.replace("'", "''")


def extract_zip_with_windows_builtin(
    *,
    zip_path: Path,
    output_dir: Path,
    base_dir: Path,
    timeout_seconds: int,
    hide_window: bool = True,
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
        completed = _run_command(
            cmd=command,
            base_dir=base_dir,
            timeout_seconds=timeout_seconds,
            hide_window=hide_window,
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
    prefer_console: bool = True,
    hide_window: bool = True,
    log: Callable[[str], None],
) -> int:
    tried_commands: list[str] = []
    for candidate in _iter_bandizip_candidates(
        bandizip_path=bandizip_path,
        prefer_console=prefer_console,
    ):
        resolved = _resolve_executable(candidate)
        if not resolved:
            continue

        commands = [
            [resolved, "x", "-y", f"-o:{output_dir}", str(zip_path)],
            [resolved, "x", "-y", "-o", str(output_dir), str(zip_path)],
        ]
        for cmd in commands:
            tried_commands.append(" ".join(cmd))
            try:
                completed = _run_command(
                    cmd=cmd,
                    base_dir=base_dir,
                    timeout_seconds=timeout_seconds,
                    hide_window=hide_window,
                )
            except subprocess.TimeoutExpired:
                log(f"Bandizip extract timeout ({Path(resolved).name}): {zip_path.name}")
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
            "Set BANDIZIP_PATH or ensure Bandizip.exe/bc.exe/bz.exe is in PATH."
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
    archive_extensions: Sequence[str] = DEFAULT_ARCHIVE_EXTENSIONS,
    bandizip_path: str = "",
    bandizip_timeout_seconds: int = 120,
    bandizip_prefer_console: bool = True,
    bandizip_hide_window: bool = True,
    detail_limit: int = 10,
) -> bool:
    if not inspect_zip_files:
        return False

    archive_paths = list_archive_paths(auth_dir, archive_extensions=archive_extensions)
    if not archive_paths:
        return False

    current_archive_keys = {str(path).lower() for path in archive_paths}
    stale_keys = [key for key in processed_signatures if key not in current_archive_keys]
    for stale_key in stale_keys:
        processed_signatures.pop(stale_key, None)

    with_json = 0
    without_json = 0
    invalid = 0
    unknown_content = 0
    total_json_entries = 0
    extracted = 0
    extract_failed = 0
    deleted_archive = 0
    skipped_already_processed = 0
    any_changed = False
    detail_lines: list[str] = []

    for path in archive_paths:
        path_key = str(path).lower()
        suffix = path.suffix.lower()
        try:
            stat = path.stat()
            archive_signature = f"{stat.st_size}|{stat.st_mtime_ns}"
        except (FileNotFoundError, OSError):
            continue

        json_entries: list[str] = []
        unknown_json = False
        if suffix == ".zip":
            try:
                with zipfile.ZipFile(path, "r") as zf:
                    json_entries = list_zip_json_entries(zf)
            except (zipfile.BadZipFile, OSError, RuntimeError):
                invalid += 1
                processed_signatures.pop(path_key, None)
                if len(detail_lines) < detail_limit:
                    detail_lines.append(f"{path.name}: invalid archive")
                continue
        else:
            listed_entries = list_non_zip_json_entries_with_bandizip(
                archive_path=path,
                base_dir=auth_dir,
                bandizip_path=bandizip_path,
                timeout_seconds=bandizip_timeout_seconds,
                prefer_console=bandizip_prefer_console,
                hide_window=bandizip_hide_window,
                log=log,
            )
            if listed_entries is None:
                unknown_json = True
            else:
                json_entries = listed_entries

        json_count = len(json_entries)
        should_attempt_extract = auto_extract_zip_json and (json_count > 0 or unknown_json)
        if json_count > 0:
            total_json_entries += json_count
            with_json += 1
        elif unknown_json:
            unknown_content += 1
        else:
            without_json += 1
            processed_signatures.pop(path_key, None)
            if len(detail_lines) < detail_limit:
                detail_lines.append(f"{path.name}: no json")
            continue

        if should_attempt_extract:
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
                            deleted_archive += 1
                            any_changed = True
                            processed_signatures.pop(path_key, None)
                        except OSError:
                            if len(detail_lines) < detail_limit:
                                detail_lines.append(
                                    f"{path.name}: extracted but failed to delete archive"
                                )
                    else:
                        processed_signatures[path_key] = archive_signature
                else:
                    extract_failed += 1
                    processed_signatures.pop(path_key, None)

        if len(detail_lines) < detail_limit:
            if json_count > 0:
                detail_lines.append(f"{path.name}: json_entries={json_count}")
            elif unknown_json:
                detail_lines.append(f"{path.name}: entry list unavailable, extraction attempted")

    log(
        "Archive scan summary: "
        f"total_archive={len(archive_paths)}, with_json={with_json}, "
        f"without_json={without_json}, invalid_archive={invalid}, "
        f"unknown_content={unknown_content}, total_json_entries={total_json_entries}, "
        f"extracted={extracted}, extract_failed={extract_failed}, deleted_archive={deleted_archive}, "
        f"skipped_already_processed={skipped_already_processed}"
    )
    for line in detail_lines:
        log(f"Archive detail: {line}")
    if with_json > 0 or unknown_content > 0:
        if auto_extract_zip_json:
            log("Archive note: archive JSON payloads were processed via Bandizip.")
        else:
            log("Archive note: uploader only handles extracted .json files in upload directory.")
    return any_changed
