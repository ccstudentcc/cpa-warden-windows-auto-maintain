from __future__ import annotations

from pathlib import Path


def write_scope_names(target: Path, names: set[str]) -> Path:
    sorted_names = sorted(name for name in names if name.strip())
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(sorted_names), encoding="utf-8")
    return target
