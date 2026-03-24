from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SmartSchedulerConfig:
    enabled: bool
    adaptive_upload_batching: bool
    base_upload_batch_size: int
    upload_high_backlog_threshold: int
    upload_high_backlog_batch_size: int
    adaptive_maintain_batching: bool
    base_incremental_maintain_batch_size: int
    maintain_high_backlog_threshold: int
    maintain_high_backlog_batch_size: int
    incremental_maintain_min_interval_seconds: int
    incremental_maintain_full_guard_seconds: int


class SmartSchedulerPolicy:
    def __init__(self, config: SmartSchedulerConfig) -> None:
        self.config = config

    def _upload_realtime_backlog_threshold(self) -> int:
        return max(1, self.config.base_upload_batch_size * 2)

    def _maintain_realtime_backlog_threshold(self) -> int:
        return max(1, self.config.base_incremental_maintain_batch_size * 2)

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

        realtime_threshold = self._upload_realtime_backlog_threshold()
        throughput_threshold = max(
            self.config.upload_high_backlog_threshold,
            realtime_threshold + 1,
        )
        high_size = max(
            self.config.base_upload_batch_size,
            self.config.upload_high_backlog_batch_size,
        )

        if pending_count >= throughput_threshold:
            return min(max(1, high_size), pending_count)

        if maintain_pressure and pending_count <= realtime_threshold:
            # Favor faster upload/maintain interleaving when backlog is still manageable.
            realtime_size = max(1, (self.config.base_upload_batch_size + 1) // 2)
            return min(realtime_size, pending_count)

        if not maintain_pressure and pending_count > self.config.base_upload_batch_size:
            # With maintain idle, opportunistically increase throughput a bit.
            burst_size = max(
                self.config.base_upload_batch_size,
                int(self.config.base_upload_batch_size * 1.25),
            )
            return min(max(1, min(burst_size, high_size)), pending_count)

        return max(1, base_size)

    def choose_incremental_maintain_batch_size(
        self,
        *,
        pending_count: int,
        upload_pressure: bool,
    ) -> int:
        if pending_count <= 0:
            return 0

        base_size = min(self.config.base_incremental_maintain_batch_size, pending_count)
        if not self.config.enabled or not self.config.adaptive_maintain_batching:
            return max(1, base_size)

        realtime_threshold = self._maintain_realtime_backlog_threshold()
        throughput_threshold = max(
            self.config.maintain_high_backlog_threshold,
            realtime_threshold + 1,
        )
        high_size = max(
            self.config.base_incremental_maintain_batch_size,
            self.config.maintain_high_backlog_batch_size,
        )

        if pending_count >= throughput_threshold and not upload_pressure:
            return min(max(1, high_size), pending_count)

        if upload_pressure and pending_count <= realtime_threshold:
            # Keep maintain responsive under upload pressure with smaller slices.
            realtime_size = max(1, (self.config.base_incremental_maintain_batch_size + 1) // 2)
            return min(realtime_size, pending_count)

        if not upload_pressure and pending_count > self.config.base_incremental_maintain_batch_size:
            burst_size = max(
                self.config.base_incremental_maintain_batch_size,
                int(self.config.base_incremental_maintain_batch_size * 1.25),
            )
            return min(max(1, min(burst_size, high_size)), pending_count)

        return max(1, base_size)

    def should_defer_incremental_maintain(
        self,
        *,
        now_monotonic: float,
        last_incremental_started_at: float,
        next_full_maintain_due_at: float | None,
        has_pending_full_maintain: bool,
        pending_upload_count: int = 0,
        upload_running: bool = False,
    ) -> tuple[bool, str]:
        if not self.config.enabled:
            return False, ""

        if has_pending_full_maintain:
            return True, "full maintain already pending"

        # Under high upload backlog, keep incremental maintain from starting too frequently.
        if upload_running and pending_upload_count >= self.config.upload_high_backlog_threshold:
            backlog_cooldown = max(self.config.incremental_maintain_min_interval_seconds, 15)
            if (
                backlog_cooldown > 0
                and last_incremental_started_at > 0
                and (now_monotonic - last_incremental_started_at) < backlog_cooldown
            ):
                return True, "upload backlog priority mode"

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
