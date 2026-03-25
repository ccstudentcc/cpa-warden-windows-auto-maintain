# Stage X vs Stage 0 Comparison Report (Template)

Generated: `YYYY-MM-DD`
Candidate Stage: `Stage X`
Baseline Artifacts:
- `results/stage0_json_replay_baseline.csv`
- `results/stage0_baseline_report.md`

Candidate Artifacts:
- `results/stageX_json_replay_candidate.csv`
- `results/stageX_vs_stage0_report.md` (this file)

## 1) Scope and Environment

- Code ref / commit: `<commit-sha>`
- Python version: `<python-version>`
- OS: `<windows-version>`
- Replay command:
  - `uv run python tools/stage0_json_replay_benchmark.py --output results/stageX_json_replay_candidate.csv`
- Notes:
  - `<notes>`

## 2) Scenario Metrics (Baseline vs Candidate)

| Scenario | Metric | Stage 0 | Stage X | Delta | Pass/Fail |
| --- | --- | ---: | ---: | ---: | --- |
| burst | Throughput (files/s) |  |  |  |  |
| burst | Upload Queue Wait P95 (s) |  |  |  |  |
| burst | Stability Wait Resets |  |  |  |  |
| burst | Retry Rate |  |  |  |  |
| sustained | Throughput (files/s) |  |  |  |  |
| sustained | Upload Queue Wait P95 (s) |  |  |  |  |
| sustained | Stability Wait Resets |  |  |  |  |
| sustained | Retry Rate |  |  |  |  |
| mixed | Throughput (files/s) |  |  |  |  |
| mixed | Upload Queue Wait P95 (s) |  |  |  |  |
| mixed | Stability Wait Resets |  |  |  |  |
| mixed | Retry Rate |  |  |  |  |

## 3) Gate B Target Check

Recommended targets:
- Stability Wait Resets: `>= 70%` reduction
- Upload Queue Wait P95: `>= 25%` reduction
- Mixed Throughput: `>= 15%` improvement

Evaluation:
- Stability Wait Resets: `<pass/fail + reason>`
- Upload Queue Wait P95: `<pass/fail + reason>`
- Mixed Throughput: `<pass/fail + reason>`

## 4) Regression & Safety Snapshot

- Correctness suite status: `<pass/fail>`
- Integration suite status: `<pass/fail>`
- Compile check status: `<pass/fail>`
- Rollback drill (`inprocess_execution_enabled=false`) status: `<pass/fail>`

## 5) Risk Readout

- Observed regressions:
  - `<item>`
- Suspected root causes:
  - `<item>`
- Mitigations / follow-ups:
  - `<item>`

## 6) Decision

- Release recommendation: `<go / no-go / go-with-guardrails>`
- Rationale:
  - `<summary>`
- Required follow-up tasks:
  - `<task-list>`
