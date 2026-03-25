from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

from .panel_snapshot import PanelSnapshot


@dataclass(frozen=True)
class PanelLinesContext:
    panel_title: str
    now_text: str
    panel_mode: str
    watch_interval_seconds: int
    upload_bar: str
    maintain_bar: str
    upload_reason_text: str
    maintain_reason_text: str
    maintain_defer_text: str


@dataclass(frozen=True)
class SignatureHeartbeatGate:
    force: bool
    signature_unchanged: bool
    now_monotonic: float
    last_render_at: float
    heartbeat_seconds: float


def _format_step_qr_tokens(snapshot: PanelSnapshot) -> str:
    tokens: list[str] = []
    for step_snapshot in snapshot.maintain_steps:
        tokens.append(f"{step_snapshot.step}:q{step_snapshot.queued}/r{step_snapshot.running}")
    return " ".join(tokens) if tokens else "-"


def _format_step_retry_tokens(snapshot: PanelSnapshot) -> str:
    tokens: list[str] = []
    for step_snapshot in snapshot.maintain_steps:
        tokens.append(f"{step_snapshot.step}:{step_snapshot.retry}")
    return " ".join(tokens) if tokens else "-"


def build_plain_panel_lines(
    *,
    snapshot: PanelSnapshot,
    context: PanelLinesContext,
    fit_line: Callable[[str], str],
    border_line: Callable[[str], str],
) -> list[str]:
    return [
        fit_line(border_line("=")),
        fit_line(
            f"{context.panel_title} | {context.now_text} | panel={context.panel_mode} "
            f"| watch={context.watch_interval_seconds}s"
        ),
        fit_line(
            f"UPLOAD   {context.upload_bar} {snapshot.upload_done}/{snapshot.upload_total} "
            f"state={snapshot.upload_state} stage={snapshot.upload_stage}"
        ),
        fit_line(
            "         "
            f"queue_files={snapshot.pending_upload} "
            f"queue_batches={snapshot.upload_queue_batches} "
            f"next_batch={snapshot.upload_next_batch_size} "
            f"inflight={snapshot.inflight_upload} retry={snapshot.upload_retry_wait}s "
            f"reason={context.upload_reason_text}"
        ),
        fit_line(border_line("-")),
        fit_line(
            f"MAINTAIN {context.maintain_bar} {snapshot.maintain_done}/{snapshot.maintain_total} "
            f"state={snapshot.maintain_state} stage={snapshot.maintain_stage}"
        ),
        fit_line(
            "         "
            f"queue_full={int(snapshot.pending_full)} "
            f"queue_incremental={snapshot.pending_incremental} "
            f"jobs_full={snapshot.maintain_queue_full_jobs} "
            f"jobs_incremental={snapshot.maintain_queue_incremental_jobs} "
            f"next_batch={snapshot.maintain_next_batch} "
            f"inflight_scope={snapshot.maintain_inflight_scope} "
            f"running_full={snapshot.maintain_running_full_jobs} "
            f"running_incremental={snapshot.maintain_running_incremental_jobs} "
            f"retry={snapshot.maintain_retry_wait}s "
            f"next_full={snapshot.next_full_wait}s "
            f"defer={context.maintain_defer_text} reason={context.maintain_reason_text}"
        ),
        fit_line(
            "         "
            f"steps_qr={_format_step_qr_tokens(snapshot)}"
        ),
        fit_line(
            "         "
            f"steps_retry={_format_step_retry_tokens(snapshot)} "
            f"retry_jobs={snapshot.maintain_retry_jobs} "
            f"parallel={snapshot.maintain_parallel_state}"
        ),
        fit_line(border_line("=")),
    ]


def panel_signature(lines: Sequence[str]) -> str:
    return "\n".join(lines)


def should_skip_render_by_signature_gate(gate: SignatureHeartbeatGate) -> bool:
    return (
        (not gate.force)
        and gate.signature_unchanged
        and (gate.now_monotonic - gate.last_render_at < gate.heartbeat_seconds)
    )
