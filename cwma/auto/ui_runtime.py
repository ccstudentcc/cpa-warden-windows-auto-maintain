"""UI runtime wrapper for panel render cadence and signature gating."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Callable, ContextManager, Protocol, Sequence

from .channel_status import CHANNEL_MAINTAIN, CHANNEL_UPLOAD, STAGE_IDLE
from .panel_render import SignatureHeartbeatGate, panel_signature, should_skip_render_by_signature_gate


class PanelSnapshotLike(Protocol):
    upload_state: str
    maintain_state: str
    upload_stage: str
    maintain_stage: str


class BuildPanelSnapshot(Protocol):
    def __call__(self, *, now_monotonic: float) -> PanelSnapshotLike: ...


class BuildPanelLines(Protocol):
    def __call__(self, *, panel_snapshot: PanelSnapshotLike) -> list[str]: ...


class ApplyPanelColors(Protocol):
    def __call__(
        self,
        lines: list[str],
        *,
        upload_state: str,
        maintain_state: str,
        upload_stage: str,
        maintain_stage: str,
    ) -> list[str]: ...


class UiStateLike(Protocol):
    upload_progress_state: ProgressState
    maintain_progress_state: ProgressState
    last_progress_render_at: float
    progress_render_interval_seconds: float
    progress_render_heartbeat_seconds: float
    last_progress_signature: str


ProgressState = dict[str, int | str]


@dataclass
class UiRuntimeState:
    upload_progress_state: ProgressState = field(
        default_factory=lambda: {"stage": STAGE_IDLE, "done": 0, "total": 0}
    )
    maintain_progress_state: ProgressState = field(
        default_factory=lambda: {"stage": STAGE_IDLE, "done": 0, "total": 0}
    )
    last_progress_render_at: float = 0.0
    progress_render_interval_seconds: float = 0.4
    progress_render_heartbeat_seconds: float = 8.0
    last_progress_signature: str = ""


class UiRuntime:
    def __init__(
        self,
        *,
        state: UiStateLike,
        monotonic: Callable[[], float],
        build_panel_snapshot: BuildPanelSnapshot,
        build_panel_lines: BuildPanelLines,
        apply_panel_colors: ApplyPanelColors,
        render_panel: Callable[[list[str]], None],
        output_lock_factory: Callable[[], ContextManager[object]] | None = None,
        panel_signature_fn: Callable[[Sequence[str]], str] = panel_signature,
    ) -> None:
        self.state = state
        self._monotonic = monotonic
        self._build_panel_snapshot = build_panel_snapshot
        self._build_panel_lines = build_panel_lines
        self._apply_panel_colors = apply_panel_colors
        self._render_panel = render_panel
        self._output_lock_factory = output_lock_factory or (lambda: nullcontext())
        self._panel_signature_fn = panel_signature_fn

    @staticmethod
    def _normalize_progress_value(value: int) -> int:
        return max(0, int(value))

    def on_stage_update(
        self,
        channel: str,
        *,
        stage: str | None = None,
        done: int | None = None,
        total: int | None = None,
        force_render: bool = False,
    ) -> None:
        with self._output_lock_factory():
            progress = self._resolve_channel_progress(channel)
            if stage is not None:
                progress["stage"] = stage
            if done is not None:
                progress["done"] = self._normalize_progress_value(done)
            if total is not None:
                progress["total"] = self._normalize_progress_value(total)
        self.render_if_needed(force=force_render)

    def on_tick(self, *, force: bool = False) -> None:
        self.render_if_needed(force=force)

    def render_if_needed(self, *, force: bool = False) -> None:
        now = self._monotonic()
        with self._output_lock_factory():
            if (not force) and (
                now - self.state.last_progress_render_at < self.state.progress_render_interval_seconds
            ):
                return
            panel_snapshot = self._build_panel_snapshot(now_monotonic=now)
            panel_lines_plain = self._build_panel_lines(panel_snapshot=panel_snapshot)
            signature = self._panel_signature_fn(panel_lines_plain)
            if should_skip_render_by_signature_gate(
                SignatureHeartbeatGate(
                    force=force,
                    signature_unchanged=(signature == self.state.last_progress_signature),
                    now_monotonic=now,
                    last_render_at=self.state.last_progress_render_at,
                    heartbeat_seconds=self.state.progress_render_heartbeat_seconds,
                )
            ):
                return
            self.state.last_progress_render_at = now
            self.state.last_progress_signature = signature

        panel_lines = self._apply_panel_colors(
            panel_lines_plain,
            upload_state=panel_snapshot.upload_state,
            maintain_state=panel_snapshot.maintain_state,
            upload_stage=panel_snapshot.upload_stage,
            maintain_stage=panel_snapshot.maintain_stage,
        )
        self._render_panel(panel_lines)

    def _resolve_channel_progress(self, channel: str) -> ProgressState:
        if channel == CHANNEL_UPLOAD:
            return self.state.upload_progress_state
        if channel == CHANNEL_MAINTAIN:
            return self.state.maintain_progress_state
        raise ValueError(f"Unsupported channel: {channel}")


__all__ = [
    "UiRuntimeState",
    "UiRuntime",
]
