from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TextIO


@dataclass
class InstanceLockState:
    token: str | None = None
    handle: TextIO | None = None


def is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def read_lock_pid(lock_file: Path) -> int | None:
    if not lock_file.exists():
        return None
    try:
        raw = lock_file.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    pid_text = raw.split("|", 1)[0].strip()
    if not pid_text.isdigit():
        return None
    return int(pid_text)


def acquire_instance_lock(
    *,
    lock_file: Path,
    state_dir: Path,
    allow_multi_instance: bool,
    log: Callable[[str], None],
) -> InstanceLockState:
    if allow_multi_instance:
        return InstanceLockState()
    if os.name == "nt":
        return _acquire_instance_lock_windows(lock_file=lock_file, state_dir=state_dir)

    for _ in range(2):
        token = str(uuid.uuid4())
        payload = f"{os.getpid()}|{token}|{int(time.time())}"
        try:
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fp:
                fp.write(payload)
            return InstanceLockState(token=token, handle=None)
        except FileExistsError:
            pid = read_lock_pid(lock_file)
            if pid is not None and not is_pid_running(pid):
                try:
                    lock_file.unlink(missing_ok=True)
                except OSError as exc:
                    log(f"[WARN] Failed to remove stale lock file {lock_file}: {exc}")
                continue
            detail = f"pid={pid}" if pid is not None else "unknown pid"
            raise RuntimeError(f"Another auto_maintain instance is already running ({detail}).")
    raise RuntimeError("Unable to acquire instance lock.")


def _acquire_instance_lock_windows(*, lock_file: Path, state_dir: Path) -> InstanceLockState:
    try:
        import msvcrt
    except Exception as exc:
        raise RuntimeError("Windows lock backend unavailable (msvcrt).") from exc

    token = str(uuid.uuid4())
    payload = f"{os.getpid()}|{token}|{int(time.time())}"
    handle: TextIO | None = None
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        handle = lock_file.open("a+", encoding="utf-8")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write("\n")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        handle.seek(0)
        handle.truncate()
        handle.write(payload)
        handle.flush()
        return InstanceLockState(token=token, handle=handle)
    except OSError as exc:
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        pid = read_lock_pid(lock_file)
        detail = f"pid={pid}" if pid is not None else "unknown pid"
        raise RuntimeError(f"Another auto_maintain instance is already running ({detail}).") from exc
    except Exception:
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        raise


def release_instance_lock(
    *,
    lock_file: Path,
    allow_multi_instance: bool,
    state: InstanceLockState,
    log: Callable[[str], None],
) -> InstanceLockState:
    if allow_multi_instance:
        return InstanceLockState()
    if os.name == "nt" and state.handle is not None:
        try:
            import msvcrt

            state.handle.seek(0)
            msvcrt.locking(state.handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError as exc:
            log(f"[WARN] Failed to unlock lock file {lock_file}: {exc}")
        finally:
            try:
                state.handle.close()
            except OSError:
                pass
        try:
            lock_file.unlink(missing_ok=True)
        except OSError as exc:
            log(f"[WARN] Failed to remove lock file {lock_file}: {exc}")
        return InstanceLockState()

    if state.token is None:
        return InstanceLockState()
    try:
        if not lock_file.exists():
            return InstanceLockState()
        raw = lock_file.read_text(encoding="utf-8").strip()
        parts = raw.split("|")
        if len(parts) >= 2 and parts[1] == state.token:
            lock_file.unlink(missing_ok=True)
    except OSError as exc:
        log(f"[WARN] Failed to release lock file {lock_file}: {exc}")
    return InstanceLockState()
