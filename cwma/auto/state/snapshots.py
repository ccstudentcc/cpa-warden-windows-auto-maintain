from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path


def build_snapshot_lines(
    paths: Iterable[Path],
    *,
    log: Callable[[str], None],
) -> list[str]:
    lines: list[str] = []
    skipped = 0
    for path in paths:
        try:
            stat = path.stat()
        except (FileNotFoundError, OSError):
            skipped += 1
            continue
        lines.append(f"{path}|{stat.st_size}|{stat.st_mtime_ns}")
    lines.sort()
    if skipped > 0:
        log(f"[WARN] Snapshot skipped transient files: {skipped}")
    return lines


def write_snapshot_lines(target: Path, lines: Iterable[str]) -> None:
    target.write_text("\n".join(lines), encoding="utf-8")


def read_snapshot_lines(source: Path) -> list[str]:
    if not source.exists():
        return []
    text = source.read_text(encoding="utf-8")
    if not text:
        return []
    return text.splitlines()


def build_snapshot_file(
    *,
    target: Path,
    paths: Iterable[Path],
    log: Callable[[str], None],
) -> list[str]:
    lines = build_snapshot_lines(paths, log=log)
    write_snapshot_lines(target, lines)
    return lines


def compute_uploaded_baseline(
    existing_baseline: list[str],
    uploaded_snapshot: list[str],
    current_snapshot: list[str],
) -> list[str]:
    current_set = set(current_snapshot)
    if not current_set:
        return []

    merged = set(existing_baseline).intersection(current_set)
    if uploaded_snapshot:
        merged.update(set(uploaded_snapshot).intersection(current_set))
    return sorted(merged)


def compute_pending_upload_snapshot(
    current_snapshot: list[str],
    uploaded_baseline: list[str],
) -> list[str]:
    if not current_snapshot:
        return []
    uploaded_set = set(uploaded_baseline)
    return [row for row in current_snapshot if row not in uploaded_set]


def extract_names_from_snapshot(snapshot_lines: list[str]) -> set[str]:
    names: set[str] = set()
    for row in snapshot_lines:
        parts = row.rsplit("|", 2)
        if len(parts) != 3:
            continue
        file_name = Path(parts[0]).name.strip()
        if file_name:
            names.add(file_name)
    return names
