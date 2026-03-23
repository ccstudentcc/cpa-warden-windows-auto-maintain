from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScheduledMaintainDecision:
    enqueue_count: int
    next_maintain_due_at: float | None


@dataclass(frozen=True)
class UploadCheckGateDecision:
    should_run_upload_check: bool
    reason: str


def decide_scheduled_maintain_enqueues(
    *,
    next_maintain_due_at: float | None,
    now_monotonic: float,
    maintain_interval_seconds: int,
) -> ScheduledMaintainDecision:
    if next_maintain_due_at is None:
        return ScheduledMaintainDecision(enqueue_count=0, next_maintain_due_at=None)
    if maintain_interval_seconds <= 0:
        raise ValueError("maintain_interval_seconds must be > 0")

    enqueue_count = 0
    next_due_at = float(next_maintain_due_at)
    while next_due_at <= now_monotonic:
        enqueue_count += 1
        next_due_at += maintain_interval_seconds
    return ScheduledMaintainDecision(
        enqueue_count=enqueue_count,
        next_maintain_due_at=next_due_at,
    )


def decide_watch_upload_check_gate(
    *,
    upload_running: bool,
    has_pending_upload_snapshot: bool,
) -> UploadCheckGateDecision:
    if upload_running:
        return UploadCheckGateDecision(
            should_run_upload_check=False,
            reason="upload process running",
        )
    if has_pending_upload_snapshot:
        return UploadCheckGateDecision(
            should_run_upload_check=False,
            reason="pending upload snapshot exists",
        )
    return UploadCheckGateDecision(
        should_run_upload_check=True,
        reason="upload idle and no pending snapshot",
    )
