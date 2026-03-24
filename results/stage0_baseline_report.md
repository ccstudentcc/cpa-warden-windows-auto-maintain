# Stage 0 Baseline Report

Generated: 2026-03-24

## 1) Contract Freeze

- Contract matrix C1-C10 is frozen in `IMPLEMENTATION_PLAN.md` (Section: `4. 契约矩阵`).
- Stage 0 execution evidence is recorded in the Stage 0 section of `IMPLEMENTATION_PLAN.md`.

## 2) Baseline Regression Commands

All Stage 0 baseline test commands passed:

1. `uv run python -m unittest -v tests/test_auto_maintain.py`
2. `uv run python -m unittest -v tests/test_auto_modules_state.py tests/test_auto_modules_ui.py tests/test_watch_cycle_module.py`
3. `uv run python -m unittest discover -s tests -p "test_warden_*_module.py"`

Result summary:

- `tests/test_auto_maintain.py`: 43 passed
- state/ui/watch suites: 71 passed
- `test_warden_*_module.py`: 83 passed

## 3) High-Frequency JSON Replay Baseline

Replay script:

- `python tools/stage0_json_replay_benchmark.py --output results/stage0_json_replay_baseline.csv`

Baseline CSV:

- `results/stage0_json_replay_baseline.csv`

Scenario metrics:

| Scenario | Events | Throughput (files/s) | Upload Queue Wait P95 (s) | Stability Wait Resets | Retry Rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| burst | 1200 | 39.735 | 24.949 | 1199 | 0.00% |
| sustained | 1800 | 2.941 | 575.933 | 1799 | 11.11% |
| mixed | 1860 | 4.333 | 328.709 | 1853 | 3.12% |

## 4) Baseline Readout

- Stability-wait timer reset behavior is clearly amplified under sustained/mixed high-frequency writes.
- Queue wait p95 grows sharply when stability reset count remains high, supporting Stage 4 optimization priority.
- This report is the Stage 0 reference baseline for subsequent Stage 1+ comparison.
