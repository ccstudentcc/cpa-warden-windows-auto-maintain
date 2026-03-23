from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def discover_upload_files(upload_dir: str, recursive: bool) -> list[Path]:
    root = Path(upload_dir).expanduser()
    if not root.exists():
        raise RuntimeError(f"upload_dir 不存在: {upload_dir}")
    if not root.is_dir():
        raise RuntimeError(f"upload_dir 不是目录: {upload_dir}")

    iterator = root.rglob("*.json") if recursive else root.glob("*.json")
    files = [path for path in iterator if path.is_file() and path.name.endswith(".json")]
    return sorted(files, key=lambda p: str(p))


def validate_and_digest_json_file(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except Exception as exc:
        raise RuntimeError(f"文件不是 UTF-8 编码: {path}") from exc

    try:
        json.loads(text)
    except Exception as exc:
        raise RuntimeError(f"JSON 格式无效: {path}") from exc

    return {
        "file_name": path.name,
        "file_path": str(path),
        "content_text": text,
        "content_bytes": raw,
        "content_sha256": hashlib.sha256(raw).hexdigest(),
        "file_size": len(raw),
    }


def select_upload_candidates(
    validated_files: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in validated_files:
        grouped[str(item["file_name"])].append(item)

    selected: list[dict[str, Any]] = []
    skipped_local_duplicates: list[dict[str, Any]] = []
    conflicting_names: dict[str, list[dict[str, Any]]] = {}

    for file_name in sorted(grouped.keys()):
        items = sorted(grouped[file_name], key=lambda row: str(row["file_path"]))
        hashes = {str(row["content_sha256"]) for row in items}
        if len(hashes) > 1:
            conflicting_names[file_name] = items
            continue
        selected.append(items[0])
        for duplicated in items[1:]:
            skipped_local_duplicates.append(
                {
                    "file_name": duplicated["file_name"],
                    "file_path": duplicated["file_path"],
                    "status_code": None,
                    "ok": True,
                    "outcome": "skipped_local_duplicate",
                    "error": "local duplicate with same file name and content",
                    "error_kind": None,
                }
            )

    return selected, skipped_local_duplicates, conflicting_names
