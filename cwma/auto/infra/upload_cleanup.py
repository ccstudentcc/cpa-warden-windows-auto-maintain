from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class UploadedFileCleanupResult:
    deleted: int
    skipped_changed: int
    skipped_missing: int
    failed: int


@dataclass(frozen=True)
class EmptyDirPruneResult:
    removed: int
    skipped_non_empty: int
    skipped_missing: int
    failed: int


def cleanup_uploaded_files(snapshot_lines: Iterable[str]) -> UploadedFileCleanupResult:
    deleted = 0
    skipped_changed = 0
    skipped_missing = 0
    failed = 0

    for row in snapshot_lines:
        parts = row.rsplit("|", 2)
        if len(parts) != 3:
            continue
        path_text, size_text, mtime_text = parts
        path = Path(path_text)
        try:
            expected_size = int(size_text)
            expected_mtime_ns = int(mtime_text)
        except ValueError:
            continue

        if not path.exists():
            skipped_missing += 1
            continue

        try:
            stat = path.stat()
        except OSError:
            failed += 1
            continue

        if stat.st_size != expected_size or stat.st_mtime_ns != expected_mtime_ns:
            skipped_changed += 1
            continue

        try:
            path.unlink()
            deleted += 1
        except OSError:
            failed += 1

    return UploadedFileCleanupResult(
        deleted=deleted,
        skipped_changed=skipped_changed,
        skipped_missing=skipped_missing,
        failed=failed,
    )


def prune_empty_dirs_under(auth_dir: Path) -> EmptyDirPruneResult:
    removed = 0
    skipped_non_empty = 0
    skipped_missing = 0
    failed = 0

    dirs = sorted(
        (p for p in auth_dir.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    )
    for path in dirs:
        if path == auth_dir:
            continue
        try:
            path.rmdir()
            removed += 1
        except OSError:
            if not path.exists():
                skipped_missing += 1
                continue
            try:
                next(path.iterdir())
                skipped_non_empty += 1
            except StopIteration:
                failed += 1
            except OSError:
                failed += 1

    return EmptyDirPruneResult(
        removed=removed,
        skipped_non_empty=skipped_non_empty,
        skipped_missing=skipped_missing,
        failed=failed,
    )
