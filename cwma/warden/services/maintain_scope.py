from __future__ import annotations

from pathlib import Path
from typing import Any


def load_name_scope_file(path_text: str, *, option_label: str) -> set[str]:
    path = Path(path_text).expanduser()
    if not path.exists():
        raise RuntimeError(f"{option_label} 不存在: {path}")
    if not path.is_file():
        raise RuntimeError(f"{option_label} 不是文件: {path}")

    names: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        names.add(line)
    return names


def resolve_maintain_name_scope(settings: dict[str, Any]) -> set[str] | None:
    if str(settings.get("mode") or "").strip().lower() != "maintain":
        return None
    path_text = str(settings.get("maintain_names_file") or "").strip()
    if not path_text:
        return None
    return load_name_scope_file(path_text, option_label="maintain_names_file")


def resolve_upload_name_scope(settings: dict[str, Any]) -> set[str] | None:
    if str(settings.get("mode") or "").strip().lower() != "upload":
        return None
    path_text = str(settings.get("upload_names_file") or "").strip()
    if not path_text:
        return None
    return load_name_scope_file(path_text, option_label="upload_names_file")

