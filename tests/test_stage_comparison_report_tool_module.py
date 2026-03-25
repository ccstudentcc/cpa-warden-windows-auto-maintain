from __future__ import annotations

import csv
import shutil
import time
import unittest
from pathlib import Path

from tools.stage_comparison_report import (
    build_gate_b_not_applicable,
    build_metric_deltas,
    evaluate_gate_b,
    parse_metrics_csv,
    render_report,
)


CSV_HEADER = [
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


def _row(
    *,
    scenario: str,
    throughput: float,
    wait_p95: float,
    resets: int,
    retry_rate: float = 0.0,
) -> list[str]:
    return [
        scenario,
        "1000",
        "1000",
        "100.0",
        f"{throughput:.3f}",
        f"{wait_p95:.3f}",
        str(resets),
        f"{retry_rate:.2f}",
        "10",
        "1",
    ]


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_HEADER)
        writer.writerows(rows)


class StageComparisonReportToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = (Path.cwd() / ".tmp_unittest_temp" / f"stage_report_{time.time_ns()}").resolve()
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parse_metrics_csv_reads_expected_fields(self) -> None:
        csv_path = self.temp_dir / "baseline.csv"
        _write_csv(
            csv_path,
            [
                _row(scenario="burst", throughput=10.0, wait_p95=20.0, resets=100),
                _row(scenario="sustained", throughput=5.0, wait_p95=50.0, resets=200),
                _row(scenario="mixed", throughput=4.333, wait_p95=328.709, resets=1853, retry_rate=3.12),
            ],
        )

        parsed = parse_metrics_csv(csv_path)
        self.assertEqual(set(parsed.keys()), {"burst", "sustained", "mixed"})
        self.assertEqual(parsed["mixed"].stability_wait_reset_count, 1853)
        self.assertAlmostEqual(parsed["mixed"].throughput_files_per_second, 4.333)
        self.assertAlmostEqual(parsed["mixed"].retry_rate_percent, 3.12)

    def test_evaluate_gate_b_passes_when_candidate_hits_targets(self) -> None:
        baseline_path = self.temp_dir / "baseline.csv"
        candidate_path = self.temp_dir / "candidate.csv"
        _write_csv(
            baseline_path,
            [
                _row(scenario="burst", throughput=39.735, wait_p95=24.949, resets=1199),
                _row(scenario="sustained", throughput=2.941, wait_p95=575.933, resets=1799),
                _row(scenario="mixed", throughput=4.333, wait_p95=328.709, resets=1853),
            ],
        )
        _write_csv(
            candidate_path,
            [
                _row(scenario="burst", throughput=45.0, wait_p95=18.0, resets=800),
                _row(scenario="sustained", throughput=3.5, wait_p95=430.0, resets=700),
                _row(scenario="mixed", throughput=5.5, wait_p95=220.0, resets=500),
            ],
        )

        baseline_rows = parse_metrics_csv(baseline_path)
        candidate_rows = parse_metrics_csv(candidate_path)
        gate = evaluate_gate_b(baseline_rows=baseline_rows, candidate_rows=candidate_rows)

        self.assertTrue(gate.resets_reduction_pass)
        self.assertTrue(gate.wait_p95_reduction_pass)
        self.assertTrue(gate.mixed_throughput_pass)
        self.assertTrue(gate.passed)

    def test_build_metric_deltas_rejects_scenario_mismatch(self) -> None:
        baseline_path = self.temp_dir / "baseline.csv"
        candidate_path = self.temp_dir / "candidate.csv"
        _write_csv(
            baseline_path,
            [
                _row(scenario="burst", throughput=1.0, wait_p95=1.0, resets=1),
                _row(scenario="mixed", throughput=1.0, wait_p95=1.0, resets=1),
            ],
        )
        _write_csv(
            candidate_path,
            [
                _row(scenario="burst", throughput=2.0, wait_p95=0.8, resets=1),
                _row(scenario="sustained", throughput=2.0, wait_p95=0.8, resets=1),
            ],
        )

        baseline_rows = parse_metrics_csv(baseline_path)
        candidate_rows = parse_metrics_csv(candidate_path)
        with self.assertRaises(ValueError):
            _ = build_metric_deltas(baseline_rows=baseline_rows, candidate_rows=candidate_rows)

    def test_render_report_contains_gate_summary(self) -> None:
        baseline_path = self.temp_dir / "baseline.csv"
        candidate_path = self.temp_dir / "candidate.csv"
        output_path = self.temp_dir / "report.md"
        _write_csv(
            baseline_path,
            [
                _row(scenario="burst", throughput=10.0, wait_p95=20.0, resets=100),
                _row(scenario="sustained", throughput=5.0, wait_p95=50.0, resets=200),
                _row(scenario="mixed", throughput=4.0, wait_p95=100.0, resets=1000),
            ],
        )
        _write_csv(
            candidate_path,
            [
                _row(scenario="burst", throughput=11.0, wait_p95=15.0, resets=80),
                _row(scenario="sustained", throughput=5.5, wait_p95=45.0, resets=180),
                _row(scenario="mixed", throughput=5.0, wait_p95=70.0, resets=200),
            ],
        )

        baseline_rows = parse_metrics_csv(baseline_path)
        candidate_rows = parse_metrics_csv(candidate_path)
        deltas = build_metric_deltas(baseline_rows=baseline_rows, candidate_rows=candidate_rows)
        gate = evaluate_gate_b(baseline_rows=baseline_rows, candidate_rows=candidate_rows)
        report = render_report(
            stage_label="Stage 8",
            baseline_csv=baseline_path,
            candidate_csv=candidate_path,
            output_path=output_path,
            commit_ref="abc123",
            baseline_rows=baseline_rows,
            candidate_rows=candidate_rows,
            deltas=deltas,
            gate_b=gate,
        )

        self.assertIn("# Stage 8 vs Stage 0 Comparison Report", report)
        self.assertIn("## 3) Gate B Target Check (Decision Scenario: mixed)", report)
        self.assertIn("Overall Gate B: `PASS`", report)

    def test_render_report_supports_non_performance_gate_na_mode(self) -> None:
        baseline_path = self.temp_dir / "baseline.csv"
        candidate_path = self.temp_dir / "candidate.csv"
        output_path = self.temp_dir / "report.md"
        _write_csv(
            baseline_path,
            [
                _row(scenario="burst", throughput=10.0, wait_p95=20.0, resets=100),
                _row(scenario="sustained", throughput=5.0, wait_p95=50.0, resets=200),
                _row(scenario="mixed", throughput=4.0, wait_p95=100.0, resets=1000),
            ],
        )
        _write_csv(
            candidate_path,
            [
                _row(scenario="burst", throughput=10.0, wait_p95=20.0, resets=100),
                _row(scenario="sustained", throughput=5.0, wait_p95=50.0, resets=200),
                _row(scenario="mixed", throughput=4.0, wait_p95=100.0, resets=1000),
            ],
        )

        baseline_rows = parse_metrics_csv(baseline_path)
        candidate_rows = parse_metrics_csv(candidate_path)
        deltas = build_metric_deltas(baseline_rows=baseline_rows, candidate_rows=candidate_rows)
        gate = build_gate_b_not_applicable()
        report = render_report(
            stage_label="Stage 8",
            baseline_csv=baseline_path,
            candidate_csv=candidate_path,
            output_path=output_path,
            commit_ref="abc123",
            baseline_rows=baseline_rows,
            candidate_rows=candidate_rows,
            deltas=deltas,
            gate_b=gate,
        )

        self.assertIn("Overall Gate B: `N/A`", report)
        self.assertIn("Gate mode: `N/A (non-performance change)`", report)
        self.assertIn("Recommendation: `go-with-guardrails`", report)


if __name__ == "__main__":
    unittest.main()
