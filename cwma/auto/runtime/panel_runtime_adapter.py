"""Panel runtime adapter for snapshot composition and fixed-panel rendering."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Callable, Protocol

from ..ui.panel_render import PanelLinesContext, build_plain_panel_lines
from ..ui.panel_snapshot import build_panel_snapshot
from ..ui.dashboard import fit_panel_line as fit_dashboard_panel_line
from ..ui.dashboard import panel_border_line as dashboard_border_line


def detect_panel_capability() -> bool:
    if not sys.stdout.isatty():
        return False
    if os.getenv("AUTO_MAINTAIN_FIXED_PANEL", "1").strip() in {"0", "false", "False"}:
        return False
    if os.name != "nt":
        return True
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        if handle in (0, -1):
            return False

        mode = ctypes.c_uint()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False

        enable_vt = 0x0004  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        if mode.value & enable_vt:
            return True
        if not kernel32.SetConsoleMode(handle, mode.value | enable_vt):
            return False
        return True
    except Exception:
        return False


class PanelRuntimeHost(Protocol):
    settings: Any
    scheduler_policy: Any
    runtime: Any
    panel_title: str
    panel_height: int
    panel_enabled: bool
    panel_initialized: bool
    upload_progress_state: dict[str, int | str]
    maintain_progress_state: dict[str, int | str]
    pending_upload_snapshot: list[str] | None
    inflight_upload_snapshot: list[str] | None
    pending_upload_reason: str | None
    upload_process: Any
    upload_retry_due_at: float
    pending_maintain: bool
    pending_maintain_names: set[str] | None
    pending_maintain_reason: str | None
    inflight_maintain_names: set[str] | None
    maintain_process: Any
    maintain_retry_due_at: float
    next_maintain_due_at: float | None
    last_incremental_defer_reason: str | None
    output_lock: Any
    console_lock: Any


class PanelRuntimeAdapter:
    def __init__(self, *, host: PanelRuntimeHost, log: Callable[[str], None]) -> None:
        self.host = host
        self.log = log

    @staticmethod
    def _format_bar(done: int, total: int, width: int = 18) -> str:
        if total <= 0:
            return "[" + ("-" * width) + "]"
        ratio = min(1.0, max(0.0, float(done) / float(total)))
        filled = int(round(ratio * width))
        return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

    @staticmethod
    def _short_reason(reason: str, limit: int = 36) -> str:
        text = (reason or "-").strip()
        if len(text) <= limit:
            return text
        return text[: max(1, limit - 3)] + "..."

    def _compute_upload_queue_batches(self, pending_count: int) -> tuple[int, int]:
        if pending_count <= 0:
            return 0, 0
        maintain_pressure = self.host.maintain_process is not None or self.host.pending_maintain
        total_backlog = self.host.scheduler_policy.estimate_total_backlog(
            pending_upload_count=pending_count,
            pending_incremental_maintain_count=len(self.host.pending_maintain_names or set()),
            has_pending_full_maintain=(self.host.pending_maintain and self.host.pending_maintain_names is None),
        )
        next_batch_size = self.host.scheduler_policy.choose_upload_batch_size(
            pending_count=pending_count,
            maintain_pressure=maintain_pressure,
            total_backlog=total_backlog,
        )
        if next_batch_size <= 0:
            return 0, 0
        queue_batches = (pending_count + next_batch_size - 1) // next_batch_size
        return next_batch_size, queue_batches

    def build_progress_panel_snapshot(self, *, now_monotonic: float) -> Any:
        return build_panel_snapshot(
            upload_progress_state=self.host.upload_progress_state,
            maintain_progress_state=self.host.maintain_progress_state,
            pending_upload_snapshot=self.host.pending_upload_snapshot,
            inflight_upload_snapshot=self.host.inflight_upload_snapshot,
            pending_upload_reason=self.host.pending_upload_reason,
            upload_running=self.host.upload_process is not None,
            upload_retry_due_at=self.host.upload_retry_due_at,
            pending_maintain=self.host.pending_maintain,
            pending_maintain_names=self.host.pending_maintain_names,
            pending_maintain_reason=self.host.pending_maintain_reason,
            inflight_maintain_names=self.host.inflight_maintain_names,
            maintain_running=self.host.maintain_process is not None,
            maintain_retry_due_at=self.host.maintain_retry_due_at,
            next_maintain_due_at=self.host.next_maintain_due_at,
            last_incremental_defer_reason=self.host.last_incremental_defer_reason,
            maintain_queue_state=self.host.runtime.maintain.queue,
            now_monotonic=now_monotonic,
            compute_upload_queue_batches=self._compute_upload_queue_batches,
            choose_incremental_maintain_batch_size=(
                lambda pending_count, upload_pressure: self.host.scheduler_policy.choose_incremental_maintain_batch_size(
                    pending_count=pending_count,
                    upload_pressure=upload_pressure,
                    total_backlog=self.host.scheduler_policy.estimate_total_backlog(
                        pending_upload_count=len(self.host.pending_upload_snapshot or []),
                        pending_incremental_maintain_count=pending_count,
                        has_pending_full_maintain=(
                            self.host.pending_maintain and self.host.pending_maintain_names is None
                        ),
                    ),
                )
            ),
        )

    def build_progress_panel_lines(self, *, panel_snapshot: Any) -> list[str]:
        context = PanelLinesContext(
            panel_title=self.host.panel_title,
            now_text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            panel_mode="fixed" if self.host.panel_enabled else "log",
            watch_interval_seconds=self.host.settings.watch_interval_seconds,
            upload_bar=self._format_bar(panel_snapshot.upload_done, panel_snapshot.upload_total),
            maintain_bar=self._format_bar(panel_snapshot.maintain_done, panel_snapshot.maintain_total),
            upload_reason_text=self._short_reason(panel_snapshot.upload_reason, limit=40),
            maintain_reason_text=self._short_reason(panel_snapshot.maintain_reason, limit=40),
            maintain_defer_text=self._short_reason(panel_snapshot.maintain_defer_reason, limit=28),
        )
        return build_plain_panel_lines(
            snapshot=panel_snapshot,
            context=context,
            fit_line=fit_dashboard_panel_line,
            border_line=dashboard_border_line,
        )

    def init_fixed_panel_area(self) -> None:
        if (not self.host.panel_enabled) or self.host.panel_initialized:
            return
        with self.host.console_lock:
            if self.host.panel_initialized:
                return
            sys.stdout.write("\n" * self.host.panel_height)
            sys.stdout.flush()
            self.host.panel_initialized = True
            self.host.runtime.ui.panel_initialized = self.host.panel_initialized

    def render_fixed_panel(self, lines: list[str]) -> None:
        if not self.host.panel_enabled:
            for line in lines:
                self.log(line)
            return

        self.init_fixed_panel_area()
        with self.host.console_lock:
            # Save cursor, jump to top-left, overwrite reserved panel rows, then restore cursor.
            sys.stdout.write("\x1b[s")
            sys.stdout.write("\x1b[H")
            for idx in range(self.host.panel_height):
                row_text = lines[idx] if idx < len(lines) else ""
                sys.stdout.write("\x1b[2K")
                sys.stdout.write(row_text)
                if idx < self.host.panel_height - 1:
                    sys.stdout.write("\n")
            sys.stdout.write("\x1b[u")
            sys.stdout.flush()
