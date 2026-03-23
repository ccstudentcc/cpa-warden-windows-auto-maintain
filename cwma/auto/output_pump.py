from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from subprocess import Popen


def append_child_output_line(*, target: Path, line: str) -> None:
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8", errors="replace") as fp:
            fp.write(line)
            if not line.endswith("\n"):
                fp.write("\n")
    except OSError:
        return


def start_output_pump_thread(
    *,
    channel: str,
    proc: Popen,
    decode_line: Callable[[bytes | str], str],
    on_line: Callable[[str], None],
    warn: Callable[[str], None],
) -> threading.Thread | None:
    stream = getattr(proc, "stdout", None)
    if stream is None:
        return None

    def _pump() -> None:
        try:
            for raw_line in iter(stream.readline, b""):
                line = decode_line(raw_line)
                on_line(line)
        except Exception as exc:
            warn(f"[WARN] output pump failed ({channel}): {exc}")
        finally:
            try:
                stream.close()
            except Exception:
                pass

    thread = threading.Thread(target=_pump, name=f"{channel}-output-pump", daemon=True)
    thread.start()
    return thread
