from __future__ import annotations

import unittest
from pathlib import Path

from tools.quality_gate_runner import (
    CommandResult,
    GateCommand,
    build_gate_commands,
    render_report,
    summarize_by_gate,
)


class QualityGateRunnerToolTests(unittest.TestCase):
    def test_build_gate_commands_g1_only_skips_g2_suites(self) -> None:
        commands = build_gate_commands(python_executable="python", include_g2=False)
        command_ids = {command.id for command in commands}
        self.assertIn("compile_core", command_ids)
        self.assertIn("help_core_cli", command_ids)
        self.assertIn("help_auto_cli", command_ids)
        self.assertIn("g1_upload_stability", command_ids)
        self.assertNotIn("g2_host_integration", command_ids)
        self.assertNotIn("g2_inprocess_channel", command_ids)

    def test_summarize_by_gate_counts_failures(self) -> None:
        cmd_g1 = GateCommand(
            id="g1_case",
            gate="G1",
            description="desc",
            command=("python", "-m", "unittest"),
        )
        cmd_g2 = GateCommand(
            id="g2_case",
            gate="G2",
            description="desc",
            command=("python", "-m", "unittest"),
        )
        results = [
            CommandResult(command=cmd_g1, return_code=0, duration_seconds=1.0, stdout="", stderr=""),
            CommandResult(command=cmd_g1, return_code=1, duration_seconds=1.0, stdout="", stderr="err"),
            CommandResult(command=cmd_g2, return_code=0, duration_seconds=1.0, stdout="ok", stderr=""),
        ]
        summary = summarize_by_gate(results)
        self.assertEqual(summary["G1"].total, 2)
        self.assertEqual(summary["G1"].failed, 1)
        self.assertFalse(summary["G1"].passed)
        self.assertEqual(summary["G2"].total, 1)
        self.assertEqual(summary["G2"].failed, 0)
        self.assertTrue(summary["G2"].passed)

    def test_render_report_contains_summary_and_details(self) -> None:
        cmd = GateCommand(
            id="compile_core",
            gate="BASE",
            description="compile",
            command=("python", "-m", "py_compile", "a.py"),
        )
        results = [
            CommandResult(
                command=cmd,
                return_code=0,
                duration_seconds=0.12,
                stdout="ok",
                stderr="",
            )
        ]
        summaries = summarize_by_gate(results)
        report = render_report(
            results=results,
            summaries=summaries,
            output_path=Path("results/quality_gate_report.md"),
            include_g2=True,
            timeout_seconds=900,
            max_output_lines=2,
        )
        self.assertIn("# Quality Gate Report", report)
        self.assertIn("| BASE | 1 | 0 | PASS |", report)
        self.assertIn("compile_core", report)
        self.assertIn("Overall: `PASS`", report)

    def test_render_report_trims_long_output(self) -> None:
        cmd = GateCommand(
            id="g1_case",
            gate="G1",
            description="test",
            command=("python", "-m", "unittest"),
        )
        output_text = "\n".join(["l1", "l2", "l3", "l4", "l5"])
        results = [
            CommandResult(
                command=cmd,
                return_code=1,
                duration_seconds=0.5,
                stdout=output_text,
                stderr="",
            )
        ]
        summaries = summarize_by_gate(results)
        report = render_report(
            results=results,
            summaries=summaries,
            output_path=Path("results/quality_gate_report.md"),
            include_g2=False,
            timeout_seconds=60,
            max_output_lines=2,
        )
        self.assertIn("... (truncated)", report)
        self.assertIn("Overall: `FAIL`", report)


if __name__ == "__main__":
    unittest.main()
