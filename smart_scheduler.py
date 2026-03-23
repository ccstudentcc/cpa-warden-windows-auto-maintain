from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SmartSchedulerConfig:
    enabled: bool
    adaptive_upload_batching: bool
    base_upload_batch_size: int
    upload_high_backlog_threshold: int
    upload_high_backlog_batch_size: int
    incremental_maintain_min_interval_seconds: int
    incremental_maintain_full_guard_seconds: int


class SmartSchedulerPolicy:
    def __init__(self, config: SmartSchedulerConfig) -> None:
        self.config = config

    def choose_upload_batch_size(
        self,
        *,
        pending_count: int,
        maintain_pressure: bool,
    ) -> int:
        if pending_count <= 0:
            return 0

        base_size = min(self.config.base_upload_batch_size, pending_count)
        if not self.config.enabled or not self.config.adaptive_upload_batching:
            return max(1, base_size)

        if pending_count >= self.config.upload_high_backlog_threshold:
            high_size = max(
                self.config.base_upload_batch_size,
                self.config.upload_high_backlog_batch_size,
            )
            if not maintain_pressure:
                # Keep some headroom for incremental maintain chances in low-pressure periods.
                high_size = max(
                    self.config.base_upload_batch_size,
                    high_size // 2,
                )
            return min(max(1, high_size), pending_count)

        if maintain_pressure and pending_count > self.config.base_upload_batch_size:
            expanded = max(
                self.config.base_upload_batch_size,
                int(self.config.base_upload_batch_size * 1.5),
            )
            return min(max(1, expanded), pending_count)

        return max(1, base_size)

    def should_defer_incremental_maintain(
        self,
        *,
        now_monotonic: float,
        last_incremental_started_at: float,
        next_full_maintain_due_at: float | None,
        has_pending_full_maintain: bool,
    ) -> tuple[bool, str]:
        if not self.config.enabled:
            return False, ""

        if has_pending_full_maintain:
            return True, "full maintain already pending"

        guard_seconds = self.config.incremental_maintain_full_guard_seconds
        if (
            guard_seconds > 0
            and next_full_maintain_due_at is not None
            and (next_full_maintain_due_at - now_monotonic) <= guard_seconds
        ):
            return True, "scheduled full maintain is due soon"

        min_interval = self.config.incremental_maintain_min_interval_seconds
        if (
            min_interval > 0
            and last_incremental_started_at > 0
            and (now_monotonic - last_incremental_started_at) < min_interval
        ):
            return True, "incremental maintain cooldown"

        return False, ""
