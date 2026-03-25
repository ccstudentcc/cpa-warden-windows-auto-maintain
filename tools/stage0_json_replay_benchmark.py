from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cwma.scheduler.smart_scheduler import SmartSchedulerConfig, SmartSchedulerPolicy


@dataclass(frozen=True)
class ReplayScenario:
    name: str
    maintain_pressure: bool
    event_times: tuple[float, ...]


@dataclass(frozen=True)
class ReplayMetrics:
    scenario: str
    events_total: int
    files_processed: int
    simulation_seconds: float
    throughput_files_per_second: float
    upload_queue_wait_p95_seconds: float
    stability_wait_reset_count: int
    retry_rate_percent: float
    batches_started: int
    batches_retried: int


def _build_scheduler_config(config_data: dict[str, object]) -> SmartSchedulerConfig:
    def _optional_int(name: str) -> int | None:
        raw = config_data.get(name, None)
        if raw is None:
            return None
        text = str(raw).strip()
        if not text:
            return None
        return int(text)

    return SmartSchedulerConfig(
        enabled=bool(config_data.get("smart_schedule_enabled", True)),
        adaptive_upload_batching=bool(config_data.get("adaptive_upload_batching", True)),
        base_upload_batch_size=max(1, int(config_data.get("upload_batch_size", 100))),
        upload_high_backlog_threshold=max(1, int(config_data.get("upload_high_backlog_threshold", 400))),
        upload_high_backlog_batch_size=max(1, int(config_data.get("upload_high_backlog_batch_size", 300))),
        adaptive_maintain_batching=bool(config_data.get("adaptive_maintain_batching", True)),
        base_incremental_maintain_batch_size=max(
            1,
            int(config_data.get("incremental_maintain_batch_size", 120)),
        ),
        maintain_high_backlog_threshold=max(1, int(config_data.get("maintain_high_backlog_threshold", 300))),
        maintain_high_backlog_batch_size=max(1, int(config_data.get("maintain_high_backlog_batch_size", 220))),
        incremental_maintain_min_interval_seconds=max(
            0,
            int(config_data.get("incremental_maintain_min_interval_seconds", 20)),
        ),
        incremental_maintain_full_guard_seconds=max(
            0,
            int(config_data.get("incremental_maintain_full_guard_seconds", 90)),
        ),
        backlog_ewma_alpha=float(config_data.get("backlog_ewma_alpha", 1.0)),
        scheduler_hysteresis_enabled=bool(config_data.get("scheduler_hysteresis_enabled", False)),
        upload_high_backlog_enter_threshold=_optional_int("upload_high_backlog_enter_threshold"),
        upload_high_backlog_exit_threshold=_optional_int("upload_high_backlog_exit_threshold"),
        maintain_high_backlog_enter_threshold=_optional_int("maintain_high_backlog_enter_threshold"),
        maintain_high_backlog_exit_threshold=_optional_int("maintain_high_backlog_exit_threshold"),
    )


def _uniform_events(*, total: int, start: float, duration: float) -> list[float]:
    if total <= 0:
        return []
    if total == 1:
        return [start]
    step = duration / float(total - 1)
    return [start + (idx * step) for idx in range(total)]


def build_default_scenarios() -> tuple[ReplayScenario, ...]:
    burst_events = tuple(_uniform_events(total=1200, start=0.0, duration=20.0))
    sustained_events = tuple(_uniform_events(total=1800, start=0.0, duration=600.0))

    mixed_events: list[float] = []
    cursor = 0.0
    for _ in range(6):
        mixed_events.extend(_uniform_events(total=160, start=cursor, duration=4.0))
        cursor += 10.0
    mixed_events.extend(_uniform_events(total=900, start=cursor, duration=360.0))

    return (
        ReplayScenario(name="burst", maintain_pressure=True, event_times=burst_events),
        ReplayScenario(name="sustained", maintain_pressure=False, event_times=sustained_events),
        ReplayScenario(name="mixed", maintain_pressure=True, event_times=tuple(mixed_events)),
    )


def _p95(values: Iterable[float]) -> float:
    data = sorted(values)
    if not data:
        return 0.0
    if len(data) == 1:
        return data[0]
    index = int(round((len(data) - 1) * 0.95))
    return data[index]


def simulate_replay(
    *,
    scenario: ReplayScenario,
    policy: SmartSchedulerPolicy,
    stable_wait_seconds: float,
    upload_batch_base_latency_seconds: float,
    retry_probability: float,
    seed: int,
) -> ReplayMetrics:
    rng = random.Random(seed)
    events = scenario.event_times

    pending_stability_buffer: deque[str] = deque()
    upload_queue: deque[str] = deque()
    arrival_at: dict[str, float] = {}
    queue_waits: list[float] = []

    stability_waiting = False
    last_stability_change_at = 0.0
    stability_wait_reset_count = 0

    inflight_batch_ids: list[str] | None = None
    inflight_batch_done_at = 0.0

    batch_count = 0
    batch_retry_count = 0
    processed_count = 0

    now = 0.0
    event_idx = 0

    while True:
        while event_idx < len(events) and events[event_idx] <= now + 1e-9:
            event_id = f"{scenario.name}-{event_idx}"
            event_time = events[event_idx]
            arrival_at[event_id] = event_time
            pending_stability_buffer.append(event_id)

            if stability_waiting:
                stability_wait_reset_count += 1
            else:
                stability_waiting = True
            last_stability_change_at = event_time
            event_idx += 1

        if stability_waiting and (now - last_stability_change_at) >= stable_wait_seconds:
            while pending_stability_buffer:
                upload_queue.append(pending_stability_buffer.popleft())
            stability_waiting = False

        if inflight_batch_ids is not None and now >= inflight_batch_done_at:
            batch_failed = rng.random() < retry_probability
            if batch_failed:
                batch_retry_count += 1
                for file_id in reversed(inflight_batch_ids):
                    upload_queue.appendleft(file_id)
            else:
                for file_id in inflight_batch_ids:
                    queue_waits.append(max(0.0, now - arrival_at[file_id]))
                processed_count += len(inflight_batch_ids)
            inflight_batch_ids = None

        if inflight_batch_ids is None and upload_queue:
            planned_batch_size = policy.choose_upload_batch_size(
                pending_count=len(upload_queue),
                maintain_pressure=scenario.maintain_pressure,
            )
            actual_batch_size = max(1, min(planned_batch_size, len(upload_queue)))
            inflight_batch_ids = [upload_queue.popleft() for _ in range(actual_batch_size)]
            batch_count += 1
            inflight_batch_done_at = now + upload_batch_base_latency_seconds + (0.002 * actual_batch_size)

        done = (
            event_idx >= len(events)
            and not pending_stability_buffer
            and not stability_waiting
            and not upload_queue
            and inflight_batch_ids is None
        )
        if done:
            break

        next_event_at = events[event_idx] if event_idx < len(events) else float("inf")
        next_stable_flush_at = (
            last_stability_change_at + stable_wait_seconds if stability_waiting else float("inf")
        )
        next_batch_done_at = inflight_batch_done_at if inflight_batch_ids is not None else float("inf")
        next_time = min(next_event_at, next_stable_flush_at, next_batch_done_at)
        if next_time <= now:
            next_time = now + 1e-6
        now = next_time

    sim_seconds = max(now, events[-1] if events else 0.0)
    throughput = (processed_count / sim_seconds) if sim_seconds > 0 else 0.0
    retry_rate = (batch_retry_count / batch_count * 100.0) if batch_count > 0 else 0.0

    return ReplayMetrics(
        scenario=scenario.name,
        events_total=len(events),
        files_processed=processed_count,
        simulation_seconds=sim_seconds,
        throughput_files_per_second=throughput,
        upload_queue_wait_p95_seconds=_p95(queue_waits),
        stability_wait_reset_count=stability_wait_reset_count,
        retry_rate_percent=retry_rate,
        batches_started=batch_count,
        batches_retried=batch_retry_count,
    )


def write_csv(*, output_path: Path, rows: tuple[ReplayMetrics, ...]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "scenario",
                "events_total",
                "files_processed",
                "simulation_seconds",
                "throughput_files_per_second",
                "upload_queue_wait_p95_seconds",
                "stability_wait_reset_count",
                "retry_rate_percent",
                "batches_started",
                "batches_retried",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.scenario,
                    row.events_total,
                    row.files_processed,
                    f"{row.simulation_seconds:.3f}",
                    f"{row.throughput_files_per_second:.3f}",
                    f"{row.upload_queue_wait_p95_seconds:.3f}",
                    row.stability_wait_reset_count,
                    f"{row.retry_rate_percent:.2f}",
                    row.batches_started,
                    row.batches_retried,
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage-0 benchmark: replay high-frequency JSON events and export baseline CSV metrics."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("auto_maintain.config.example.json"),
        help="Path to watcher config JSON used for scheduler/stability parameters.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/stage0_json_replay_baseline.csv"),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260324,
        help="Deterministic random seed for retry simulation.",
    )
    parser.add_argument(
        "--retry-probability",
        type=float,
        default=0.02,
        help="Batch-level retry probability (0~1).",
    )
    parser.add_argument(
        "--upload-batch-base-latency-seconds",
        type=float,
        default=0.35,
        help="Fixed per-batch latency used by the simulation.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_data = json.loads(args.config.read_text(encoding="utf-8"))

    scheduler_config = _build_scheduler_config(config_data)
    policy = SmartSchedulerPolicy(config=scheduler_config)
    stable_wait_seconds = float(config_data.get("upload_stable_wait_seconds", 5))

    metrics = tuple(
        simulate_replay(
            scenario=scenario,
            policy=policy,
            stable_wait_seconds=stable_wait_seconds,
            upload_batch_base_latency_seconds=args.upload_batch_base_latency_seconds,
            retry_probability=args.retry_probability,
            seed=args.seed + idx,
        )
        for idx, scenario in enumerate(build_default_scenarios())
    )

    write_csv(output_path=args.output, rows=metrics)

    print(f"Stage-0 replay baseline written to: {args.output}")
    for row in metrics:
        print(
            f"- {row.scenario}: events={row.events_total}, processed={row.files_processed}, "
            f"throughput={row.throughput_files_per_second:.2f}/s, "
            f"wait_p95={row.upload_queue_wait_p95_seconds:.2f}s, "
            f"stable_resets={row.stability_wait_reset_count}, "
            f"retry_rate={row.retry_rate_percent:.2f}%"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
