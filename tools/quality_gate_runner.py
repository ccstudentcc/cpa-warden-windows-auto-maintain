from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class GateCommand:
    id: str
    gate: str
    description: str
    command: tuple[str, ...]


@dataclass(frozen=True)
class CommandResult:
    command: GateCommand
    return_code: int
    duration_seconds: float
    stdout: str
    stderr: str


@dataclass(frozen=True)
class GateSummary:
    gate: str
    total: int
    failed: int

    @property
    def passed(self) -> bool:
        return self.failed == 0


def build_gate_commands(*, python_executable: str, include_g2: bool = True) -> list[GateCommand]:
    commands: list[GateCommand] = [
        GateCommand(
            id="compile_core",
            gate="BASE",
            description="Compile check for core entrypoints and gate tools",
            command=(
                python_executable,
                "-m",
                "py_compile",
                "cpa_warden.py",
                "auto_maintain.py",
                "clean_codex_accounts.py",
                "tools/stage_comparison_report.py",
                "tools/quality_gate_runner.py",
            ),
        ),
        GateCommand(
            id="help_core_cli",
            gate="BASE",
            description="Core CLI help command should work",
            command=(python_executable, "cpa_warden.py", "--help"),
        ),
        GateCommand(
            id="help_auto_cli",
            gate="BASE",
            description="Auto maintain CLI help command should work",
            command=(python_executable, "auto_maintain.py", "--help"),
        ),
        GateCommand(
            id="g1_upload_stability",
            gate="G1",
            description="Upload stability freeze/deferred semantics",
            command=(python_executable, "-m", "unittest", "-q", "tests.test_auto_upload_stability_module"),
        ),
        GateCommand(
            id="g1_pipeline_state",
            gate="G1",
            description="Maintain pipeline state transitions",
            command=(python_executable, "-m", "unittest", "-q", "tests.test_auto_maintain_pipeline_state_module"),
        ),
        GateCommand(
            id="g1_pipeline_runtime",
            gate="G1",
            description="Maintain pipeline runtime step ordering/locks",
            command=(python_executable, "-m", "unittest", "-q", "tests.test_auto_maintain_pipeline_runtime_module"),
        ),
        GateCommand(
            id="g1_scheduler_and_state",
            gate="G1",
            description="Scheduler/defer/state contracts",
            command=(python_executable, "-m", "unittest", "-q", "tests.test_auto_modules_state"),
        ),
        GateCommand(
            id="g1_ui_snapshot",
            gate="G1",
            description="UI step-queue observability contracts",
            command=(python_executable, "-m", "unittest", "-q", "tests.test_auto_modules_ui"),
        ),
    ]

    if include_g2:
        commands.extend(
            [
                GateCommand(
                    id="g2_host_integration",
                    gate="G2",
                    description="Watcher integration regression suite",
                    command=(python_executable, "-m", "unittest", "-q", "tests.test_auto_maintain"),
                ),
                GateCommand(
                    id="g2_inprocess_channel",
                    gate="G2",
                    description="In-process channel lifecycle integration",
                    command=(python_executable, "-m", "unittest", "-q", "tests.test_auto_inprocess_channel_module"),
                ),
            ]
        )
    return commands


def run_gate_command(command: GateCommand, *, timeout_seconds: int) -> CommandResult:
    start = time.perf_counter()
    completed = subprocess.run(
        list(command.command),
        text=True,
        capture_output=True,
        timeout=max(1, int(timeout_seconds)),
        check=False,
    )
    duration = time.perf_counter() - start
    return CommandResult(
        command=command,
        return_code=int(completed.returncode),
        duration_seconds=duration,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


def summarize_by_gate(results: list[CommandResult]) -> dict[str, GateSummary]:
    counters: dict[str, tuple[int, int]] = {}
    for result in results:
        total, failed = counters.get(result.command.gate, (0, 0))
        total += 1
        if result.return_code != 0:
            failed += 1
        counters[result.command.gate] = (total, failed)
    return {
        gate: GateSummary(gate=gate, total=total, failed=failed)
        for gate, (total, failed) in counters.items()
    }


def _status_text(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


def _trim_output(text: str, *, max_lines: int) -> str:
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    if not lines:
        return "-"
    if len(lines) <= max_lines:
        return " | ".join(lines)
    kept = lines[:max_lines]
    return " | ".join(kept) + " | ... (truncated)"


def render_report(
    *,
    results: list[CommandResult],
    summaries: dict[str, GateSummary],
    output_path: Path,
    include_g2: bool,
    timeout_seconds: int,
    max_output_lines: int,
) -> str:
    lines: list[str] = []
    lines.append("# Quality Gate Report")
    lines.append("")
    lines.append(f"Generated: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
    lines.append(f"Python: `{sys.version.split()[0]}`")
    lines.append(f"Include G2: `{str(include_g2).lower()}`")
    lines.append(f"Command timeout: `{timeout_seconds}s`")
    lines.append(f"Output file: `{output_path.as_posix()}`")
    lines.append("")
    lines.append("## Gate Summary")
    lines.append("")
    lines.append("| Gate | Total | Failed | Status |")
    lines.append("| --- | ---: | ---: | --- |")
    for gate in sorted(summaries.keys()):
        summary = summaries[gate]
        lines.append(
            f"| {gate} | {summary.total} | {summary.failed} | {_status_text(summary.passed)} |"
        )

    overall_pass = all(summary.passed for summary in summaries.values())
    lines.append("")
    lines.append(f"Overall: `{_status_text(overall_pass)}`")
    lines.append("")
    lines.append("## Command Details")
    lines.append("")
    lines.append("| ID | Gate | Status | Duration (s) | Command | Output Summary |")
    lines.append("| --- | --- | --- | ---: | --- | --- |")
    for result in results:
        output_summary = _trim_output(
            (result.stderr.strip() + "\n" + result.stdout.strip()).strip(),
            max_lines=max_output_lines,
        )
        command_text = " ".join(result.command.command)
        lines.append(
            "| "
            + f"{result.command.id} | {result.command.gate} | {_status_text(result.return_code == 0)} | "
            + f"{result.duration_seconds:.2f} | `{command_text}` | {output_summary} |"
        )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run G1/G2 quality gates and emit markdown report.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/quality_gate_report.md"),
        help="Output markdown report path.",
    )
    parser.add_argument(
        "--python-executable",
        default=sys.executable,
        help="Python executable used for child commands.",
    )
    parser.add_argument(
        "--g1-only",
        action="store_true",
        help="Run G1 + compile checks only (skip G2 integration suites).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=900,
        help="Per-command timeout in seconds.",
    )
    parser.add_argument(
        "--max-output-lines",
        type=int,
        default=4,
        help="Max lines to retain in output summary per command.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 2 when any gate fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    include_g2 = not args.g1_only
    commands = build_gate_commands(
        python_executable=args.python_executable,
        include_g2=include_g2,
    )
    results = [run_gate_command(command, timeout_seconds=args.timeout_seconds) for command in commands]
    summaries = summarize_by_gate(results)
    report = render_report(
        results=results,
        summaries=summaries,
        output_path=args.output,
        include_g2=include_g2,
        timeout_seconds=args.timeout_seconds,
        max_output_lines=max(1, args.max_output_lines),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report + "\n", encoding="utf-8")

    overall_pass = all(summary.passed for summary in summaries.values())
    print(f"Quality gate report written: {args.output}")
    print(f"Overall gate status: {_status_text(overall_pass)}")
    if args.strict and not overall_pass:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
