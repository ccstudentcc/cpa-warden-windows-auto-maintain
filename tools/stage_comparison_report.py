from __future__ import annotations

import argparse
import csv
import platform
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class ScenarioMetrics:
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


@dataclass(frozen=True)
class MetricDelta:
    baseline: float
    candidate: float
    delta_percent: float


@dataclass(frozen=True)
class GateBResult:
    applicable: bool
    resets_reduction_pass: bool
    wait_p95_reduction_pass: bool
    mixed_throughput_pass: bool
    resets_reduction_percent: float
    wait_p95_reduction_percent: float
    mixed_throughput_improve_percent: float

    @property
    def passed(self) -> bool:
        if not self.applicable:
            return True
        return self.resets_reduction_pass and self.wait_p95_reduction_pass and self.mixed_throughput_pass


def build_gate_b_not_applicable() -> GateBResult:
    return GateBResult(
        applicable=False,
        resets_reduction_pass=False,
        wait_p95_reduction_pass=False,
        mixed_throughput_pass=False,
        resets_reduction_percent=0.0,
        wait_p95_reduction_percent=0.0,
        mixed_throughput_improve_percent=0.0,
    )


def _to_float(value: str) -> float:
    return float(value.strip())


def _to_int(value: str) -> int:
    return int(value.strip())


def parse_metrics_csv(path: Path) -> dict[str, ScenarioMetrics]:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    rows: dict[str, ScenarioMetrics] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
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
        }
        header = set(reader.fieldnames or [])
        missing = required - header
        if missing:
            missing_fields = ", ".join(sorted(missing))
            raise ValueError(f"CSV missing required fields: {missing_fields}")

        for raw in reader:
            scenario = (raw.get("scenario") or "").strip()
            if not scenario:
                raise ValueError("CSV contains empty scenario field.")
            if scenario in rows:
                raise ValueError(f"CSV contains duplicated scenario: {scenario}")
            rows[scenario] = ScenarioMetrics(
                scenario=scenario,
                events_total=_to_int(raw["events_total"]),
                files_processed=_to_int(raw["files_processed"]),
                simulation_seconds=_to_float(raw["simulation_seconds"]),
                throughput_files_per_second=_to_float(raw["throughput_files_per_second"]),
                upload_queue_wait_p95_seconds=_to_float(raw["upload_queue_wait_p95_seconds"]),
                stability_wait_reset_count=_to_int(raw["stability_wait_reset_count"]),
                retry_rate_percent=_to_float(raw["retry_rate_percent"]),
                batches_started=_to_int(raw["batches_started"]),
                batches_retried=_to_int(raw["batches_retried"]),
            )
    return rows


def _percent_change(*, baseline: float, candidate: float) -> float:
    if baseline == 0.0:
        if candidate == 0.0:
            return 0.0
        return 100.0
    return ((candidate - baseline) / baseline) * 100.0


def _percent_reduction(*, baseline: float, candidate: float) -> float:
    return -_percent_change(baseline=baseline, candidate=candidate)


def build_metric_deltas(
    *,
    baseline_rows: dict[str, ScenarioMetrics],
    candidate_rows: dict[str, ScenarioMetrics],
) -> dict[str, dict[str, MetricDelta]]:
    scenarios = sorted(baseline_rows.keys())
    if sorted(candidate_rows.keys()) != scenarios:
        raise ValueError(
            "Scenario mismatch between baseline and candidate CSV. "
            f"baseline={sorted(baseline_rows.keys())}, candidate={sorted(candidate_rows.keys())}"
        )

    deltas: dict[str, dict[str, MetricDelta]] = {}
    for scenario in scenarios:
        base = baseline_rows[scenario]
        cand = candidate_rows[scenario]
        deltas[scenario] = {
            "throughput_files_per_second": MetricDelta(
                baseline=base.throughput_files_per_second,
                candidate=cand.throughput_files_per_second,
                delta_percent=_percent_change(
                    baseline=base.throughput_files_per_second,
                    candidate=cand.throughput_files_per_second,
                ),
            ),
            "upload_queue_wait_p95_seconds": MetricDelta(
                baseline=base.upload_queue_wait_p95_seconds,
                candidate=cand.upload_queue_wait_p95_seconds,
                delta_percent=_percent_reduction(
                    baseline=base.upload_queue_wait_p95_seconds,
                    candidate=cand.upload_queue_wait_p95_seconds,
                ),
            ),
            "stability_wait_reset_count": MetricDelta(
                baseline=float(base.stability_wait_reset_count),
                candidate=float(cand.stability_wait_reset_count),
                delta_percent=_percent_reduction(
                    baseline=float(base.stability_wait_reset_count),
                    candidate=float(cand.stability_wait_reset_count),
                ),
            ),
            "retry_rate_percent": MetricDelta(
                baseline=base.retry_rate_percent,
                candidate=cand.retry_rate_percent,
                delta_percent=_percent_reduction(
                    baseline=base.retry_rate_percent,
                    candidate=cand.retry_rate_percent,
                ),
            ),
        }
    return deltas


def evaluate_gate_b(
    *,
    baseline_rows: dict[str, ScenarioMetrics],
    candidate_rows: dict[str, ScenarioMetrics],
    decision_scenario: str = "mixed",
) -> GateBResult:
    if decision_scenario not in baseline_rows or decision_scenario not in candidate_rows:
        raise ValueError(f"Decision scenario not found in both CSV files: {decision_scenario}")

    base = baseline_rows[decision_scenario]
    cand = candidate_rows[decision_scenario]

    resets_reduction = _percent_reduction(
        baseline=float(base.stability_wait_reset_count),
        candidate=float(cand.stability_wait_reset_count),
    )
    wait_reduction = _percent_reduction(
        baseline=base.upload_queue_wait_p95_seconds,
        candidate=cand.upload_queue_wait_p95_seconds,
    )
    throughput_gain = _percent_change(
        baseline=base.throughput_files_per_second,
        candidate=cand.throughput_files_per_second,
    )

    return GateBResult(
        applicable=True,
        resets_reduction_pass=resets_reduction >= 70.0,
        wait_p95_reduction_pass=wait_reduction >= 25.0,
        mixed_throughput_pass=throughput_gain >= 15.0,
        resets_reduction_percent=resets_reduction,
        wait_p95_reduction_percent=wait_reduction,
        mixed_throughput_improve_percent=throughput_gain,
    )


def _status_label(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


def _gate_b_overall_label(gate_b: GateBResult) -> str:
    if not gate_b.applicable:
        return "N/A"
    return _status_label(gate_b.passed)


def render_report(
    *,
    stage_label: str,
    baseline_csv: Path,
    candidate_csv: Path,
    output_path: Path,
    commit_ref: str,
    baseline_rows: dict[str, ScenarioMetrics],
    candidate_rows: dict[str, ScenarioMetrics],
    deltas: dict[str, dict[str, MetricDelta]],
    gate_b: GateBResult,
) -> str:
    lines: list[str] = []
    lines.append(f"# {stage_label} vs Stage 0 Comparison Report")
    lines.append("")
    lines.append(f"Generated: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
    lines.append(f"Candidate Stage: `{stage_label}`")
    lines.append(f"Code ref / commit: `{commit_ref}`")
    lines.append(f"Python version: `{platform.python_version()}`")
    lines.append(f"OS: `{platform.platform()}`")
    lines.append("")
    lines.append("## 1) Artifacts")
    lines.append("")
    lines.append(f"- Baseline: `{baseline_csv.as_posix()}`")
    lines.append(f"- Candidate: `{candidate_csv.as_posix()}`")
    lines.append(f"- Report: `{output_path.as_posix()}`")
    lines.append("")
    lines.append("## 2) Scenario Metrics (Baseline vs Candidate)")
    lines.append("")
    lines.append("| Scenario | Metric | Stage 0 | Candidate | Delta | Pass/Fail |")
    lines.append("| --- | --- | ---: | ---: | ---: | --- |")

    metric_specs = (
        ("throughput_files_per_second", "Throughput (files/s)", True),
        ("upload_queue_wait_p95_seconds", "Upload Queue Wait P95 (s)", True),
        ("stability_wait_reset_count", "Stability Wait Resets", True),
        ("retry_rate_percent", "Retry Rate (%)", False),
    )

    for scenario in sorted(baseline_rows.keys()):
        for key, label, has_target in metric_specs:
            delta = deltas[scenario][key]
            base_display = f"{delta.baseline:.3f}" if key != "stability_wait_reset_count" else f"{int(delta.baseline)}"
            cand_display = f"{delta.candidate:.3f}" if key != "stability_wait_reset_count" else f"{int(delta.candidate)}"
            delta_display = f"{delta.delta_percent:+.2f}%"
            if has_target and scenario == "mixed":
                if not gate_b.applicable:
                    pass_fail = "N/A"
                elif key == "throughput_files_per_second":
                    pass_fail = _status_label(delta.delta_percent >= 15.0)
                elif key == "upload_queue_wait_p95_seconds":
                    pass_fail = _status_label(delta.delta_percent >= 25.0)
                else:
                    pass_fail = _status_label(delta.delta_percent >= 70.0)
            else:
                pass_fail = "-"
            lines.append(
                f"| {scenario} | {label} | {base_display} | {cand_display} | {delta_display} | {pass_fail} |"
            )

    lines.append("")
    lines.append("## 3) Gate B Target Check (Decision Scenario: mixed)")
    lines.append("")
    lines.append("- Stability Wait Resets reduction target: `>= 70%`")
    lines.append("- Upload Queue Wait P95 reduction target: `>= 25%`")
    lines.append("- Mixed Throughput improvement target: `>= 15%`")
    lines.append("")
    if gate_b.applicable:
        lines.append(
            f"- Stability Wait Resets: `{gate_b.resets_reduction_percent:.2f}%` -> `{_status_label(gate_b.resets_reduction_pass)}`"
        )
        lines.append(
            f"- Upload Queue Wait P95: `{gate_b.wait_p95_reduction_percent:.2f}%` -> `{_status_label(gate_b.wait_p95_reduction_pass)}`"
        )
        lines.append(
            f"- Mixed Throughput: `{gate_b.mixed_throughput_improve_percent:.2f}%` -> `{_status_label(gate_b.mixed_throughput_pass)}`"
        )
    else:
        lines.append("- Gate mode: `N/A (non-performance change)`")
    lines.append(f"- Overall Gate B: `{_gate_b_overall_label(gate_b)}`")
    lines.append("")
    lines.append("## 4) Decision")
    lines.append("")
    if not gate_b.applicable:
        lines.append("- Recommendation: `go-with-guardrails` (Gate B marked as non-performance N/A).")
    else:
        lines.append(
            "- Recommendation: "
            + ("`go`" if gate_b.passed else "`no-go`")
            + " (based on Gate B targets and decision scenario `mixed`)."
        )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Stage-X vs Stage-0 markdown comparison report.")
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("results/stage0_json_replay_baseline.csv"),
        help="Baseline CSV path.",
    )
    parser.add_argument(
        "--candidate",
        type=Path,
        required=True,
        help="Candidate CSV path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/stageX_vs_stage0_report.md"),
        help="Output markdown path.",
    )
    parser.add_argument(
        "--stage-label",
        default="Stage X",
        help="Stage label shown in report header.",
    )
    parser.add_argument(
        "--commit-ref",
        default="unknown",
        help="Commit sha or tag for this candidate run.",
    )
    parser.add_argument(
        "--strict-gate-b",
        action="store_true",
        help="Exit with code 2 when Gate B does not pass.",
    )
    parser.add_argument(
        "--gate-b-mode",
        choices=("strict", "advisory", "na"),
        default="strict",
        help="Gate-B evaluation mode: strict=fail on miss, advisory=report only, na=non-performance change.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    baseline_rows = parse_metrics_csv(args.baseline)
    candidate_rows = parse_metrics_csv(args.candidate)
    deltas = build_metric_deltas(baseline_rows=baseline_rows, candidate_rows=candidate_rows)
    if args.gate_b_mode == "na":
        gate_b = build_gate_b_not_applicable()
    else:
        gate_b = evaluate_gate_b(baseline_rows=baseline_rows, candidate_rows=candidate_rows)
    markdown = render_report(
        stage_label=args.stage_label,
        baseline_csv=args.baseline,
        candidate_csv=args.candidate,
        output_path=args.output,
        commit_ref=args.commit_ref,
        baseline_rows=baseline_rows,
        candidate_rows=candidate_rows,
        deltas=deltas,
        gate_b=gate_b,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(markdown + "\n", encoding="utf-8")

    print(f"Stage comparison report written: {args.output}")
    print(f"Gate B result: {_gate_b_overall_label(gate_b)}")
    enforce_strict = args.strict_gate_b or args.gate_b_mode == "strict"
    if enforce_strict and gate_b.applicable and not gate_b.passed:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
