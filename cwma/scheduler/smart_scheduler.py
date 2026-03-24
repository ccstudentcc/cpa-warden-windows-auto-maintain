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

    def _full_maintain_backlog_equivalent(self) -> int:
        # A full maintain has no explicit incremental-name cardinality, so map it to
        # at least one normal incremental batch when estimating total backlog.
        return max(1, self.config.base_incremental_maintain_batch_size)

    def _upload_realtime_backlog_threshold(self) -> int:
        return max(1, self.config.base_upload_batch_size * 2)

    def _maintain_realtime_backlog_threshold(self) -> int:
        return max(1, self.config.base_incremental_maintain_batch_size * 2)

    def estimate_total_backlog(
        self,
        *,
        pending_upload_count: int,
        pending_incremental_maintain_count: int,
        has_pending_full_maintain: bool,
    ) -> int:
        upload_backlog = max(0, pending_upload_count)
        maintain_backlog = max(0, pending_incremental_maintain_count)
        if has_pending_full_maintain:
            maintain_backlog += self._full_maintain_backlog_equivalent()
        return upload_backlog + maintain_backlog

    def choose_upload_batch_size(
        self,
        *,
        pending_count: int,
        maintain_pressure: bool,
        total_backlog: int | None = None,
    ) -> int:
        if pending_count <= 0:
            return 0

        effective_total_backlog = max(pending_count, int(total_backlog or pending_count))
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

        if effective_total_backlog >= throughput_threshold:
            return min(max(1, high_size), pending_count)

        if maintain_pressure and effective_total_backlog <= realtime_threshold:
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
        total_backlog: int | None = None,
    ) -> int:
        if pending_count <= 0:
            return 0

        target_size = self.estimate_incremental_target_batch_size(
            pending_count=pending_count,
            upload_pressure=upload_pressure,
            total_backlog=total_backlog,
        )
        return min(max(1, target_size), pending_count)

    def estimate_incremental_target_batch_size(
        self,
        *,
        pending_count: int,
        upload_pressure: bool,
        total_backlog: int | None = None,
    ) -> int:
        if pending_count <= 0:
            return 0
        effective_total_backlog = max(pending_count, int(total_backlog or pending_count))
        base_size = max(1, self.config.base_incremental_maintain_batch_size)
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

        if effective_total_backlog >= throughput_threshold and not upload_pressure:
            return max(1, high_size)

        if upload_pressure and effective_total_backlog <= realtime_threshold:
            # Keep maintain responsive under upload pressure with smaller slices.
            realtime_size = max(1, (self.config.base_incremental_maintain_batch_size + 1) // 2)
            return realtime_size

        if not upload_pressure:
            burst_size = max(
                self.config.base_incremental_maintain_batch_size,
                int(self.config.base_incremental_maintain_batch_size * 1.25),
            )
            return max(1, min(burst_size, high_size))

        return max(1, base_size)

    def should_defer_incremental_maintain(
        self,
        *,
        pending_incremental_count: int,
        planned_batch_size: int,
        pending_upload_count: int = 0,
        upload_running: bool = False,
    ) -> tuple[bool, str]:
        if not self.config.enabled:
            return False, ""
        if pending_incremental_count <= 0:
            return False, ""
        if planned_batch_size <= 0:
            return False, ""
        if not upload_running and pending_upload_count <= 0:
            return False, ""
        if pending_incremental_count >= planned_batch_size:
            return False, ""

        min_fill_count = max(1, (planned_batch_size + 1) // 2)
        if pending_incremental_count < min_fill_count:
            return True, "batch_too_small_waiting_fill"

        return False, ""
