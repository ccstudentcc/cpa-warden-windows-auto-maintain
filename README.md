# cpa-warden-windows-auto-maintain

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![uv](https://img.shields.io/badge/deps-uv-6f42c1)

[简体中文](README.zh-CN.md)

`cpa-warden-windows-auto-maintain` is a Windows-first automation project for long-running CPA account maintenance.
It is based on [`fantasticjoe/cpa-warden`](https://github.com/fantasticjoe/cpa-warden) and keeps compatibility with the upstream `cpa_warden.py` workflow.

## Upstream Attribution

This repository is a derivative project based on `cpa-warden`.

- Upstream project: `fantasticjoe/cpa-warden`
- Derivative baseline in this repository: commit `f3778f4334f443fd822c25935c1d2a1ee26c144b`
- Follow-up commits focus on Windows automation, scheduler/channel safety, and runtime hardening

See [NOTICE](NOTICE) for attribution details.

## Latest Project Status (2026-03-25)

- `cwma/auto` is now capability-grouped: `orchestration`, `channel`, `state`, `infra`, `ui`, plus `runtime` adapters
- Stage-2.6 capability split test mapping is closed out:
  - `tests/test_auto_modules_process_channel.py`
  - `tests/test_auto_modules_state.py`
  - `tests/test_auto_modules_ui.py`
- Redundant top-level `cwma/auto/*.py` compatibility wrappers have been removed; canonical paths are subpackage paths
- Maintain queue is now modeled as staged jobs with explicit step-level transitions shared by full and incremental runs
- Maintain service now executes explicit ordered steps (`scan -> delete_401 -> quota -> reenable -> finalize`) through a step engine + pipeline runtime policy
- Upload stability wait now freezes the current candidate batch, defers in-window new/updated rows to next-round intake, and merges pending rows by path (`last-writer-wins`)
- Smart scheduler batch sizing is now driven by a shared total-backlog signal (upload pending + incremental pending + full-maintain equivalent backlog), and incremental defer is narrowed to `batch_too_small_waiting_fill` only
- Dashboard panel now exposes maintain step-queue observability (`steps_qr` / `steps_retry`), full-vs-incremental job counters, and channel/pipeline parallel-state hints
- Stage-7 hardening is complete: full unittest discovery is stable under Python 3.14 on Windows via workspace-temp sandboxing, and rollout/rollback guidance is now documented for in-process execution

## Documentation Architecture

To reduce drift and duplication, each document has one primary role:

| Document | Primary role | Update when |
| --- | --- | --- |
| `README.md` | operator quick start, runtime behavior summary, key commands | startup/runtime behavior or user-facing commands change |
| `README.zh-CN.md` | Chinese mirror of `README.md` (same meaning) | whenever `README.md` meaning changes |
| `ARCHITECTURE.md` | current module boundaries, dependency rules, data/concurrency/failure models | package/module boundaries, state model, or orchestration contract changes |
| `cwma/auto/BOUNDARY_MAP.md` | capability ownership map for `cwma/auto` and `cwma/auto/runtime` | ownership or dependency-direction rules change |
| `CONTRIBUTING.md` | contribution workflow, validation expectations, doc sync rules | contributor workflow, CI expectations, or doc policy changes |
| `CHANGELOG.md` | release-level change history | user-visible or maintenance-significant change lands |

## What This Project Solves

The goal is not replacing `cpa_warden.py`, but running it safely and continuously on Windows:

- watch `auth_files` and queue upload on stable changes
- run `upload` and `maintain` as independent channels
- run scheduled full maintain and post-upload incremental maintain
- auto-switch between real-time interleaving and backlog-drain throughput modes for upload/incremental-maintain scheduling
- drive upload/incremental batch sizing from total backlog rather than local single-queue pressure only
- execute maintain work as an explicit staged pipeline with deterministic step ordering and account-lock-aware action claims
- isolate maintain/upload runtime DB and log files
- keep upload stability wait bounded by freezing the current batch and deferring in-window changes to the next queue intake
- allow incremental maintain defer only when current incremental slice is too small and upload-side fill source is active (`batch_too_small_waiting_fill`)
- render maintain pipeline queue/running/retry/defer state directly in panel output, including full/incremental job split and parallel-state visibility
- support archive intake (`.zip/.7z/.rar`, Bandizip first, Windows fallback for `.zip`)
- clean uploaded files and prune empty directories
- keep single-instance safety (launcher lock + Python runtime lock)
- fail fast by default, with explicit retry controls

## Current Component Layout

- `cwma/apps/cpa_warden.py`: upstream-compatible CPA scanner / maintainer / uploader CLI host
- `cwma/auto/app.py`: watcher host and orchestration entrypoint
- `cwma/auto/orchestration/*`: startup/watch cycle sequencing
- `cwma/auto/channel/*`: maintain/upload channel command + lifecycle policy
- `cwma/auto/state/*`: queue/snapshot/state transitions and pure decisions
- `cwma/auto/infra/*`: process/archive/lock/config/cleanup side-effect boundaries
- `cwma/auto/ui/*`: progress parsing and dashboard rendering
- `cwma/auto/runtime/*`: host adapters wiring orchestration/state/infra/ui contracts
- `cwma/warden/*`: split domain modules for CLI/config/services/api/db/models/exports
- `cwma/scheduler/smart_scheduler.py`: adaptive scheduling policy
- `cwma/common/config_parsing.py`: shared strict config parsing/path helpers
- `auto_maintain.py` / `cpa_warden.py` / `smart_scheduler.py`: root compatibility entrypoints

Deep module ownership and dependency rules are maintained in [ARCHITECTURE.md](ARCHITECTURE.md) and [`cwma/auto/BOUNDARY_MAP.md`](cwma/auto/BOUNDARY_MAP.md).

## Watcher Execution Flow

`auto_maintain.py` loop behavior:

1. Load settings (`env > --watch-config JSON > defaults`) and initialize runtime state.
2. Build snapshots and uploaded baseline state.
3. Optionally inspect archives (`.zip/.7z/.rar`) and feed extracted JSON changes into the same upload pipeline.
4. Queue startup maintain/upload checks according to settings.
5. Start `upload` and `maintain` independently when each channel is idle.
6. While child processes run, keep active probes, channel-local poll/retry handling, and maintain step-engine cycle progression.
7. During upload stability wait, freeze the current candidate batch; defer in-window new/updated rows to the next queue intake.
8. On upload success, update baseline, apply cleanup, and optionally queue scoped post-upload maintain.
9. In `--once` mode, exit only after pending/running work resolves; unresolved failure exits non-zero.

## Requirements

- Windows 10/11
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended)
- Bandizip (optional, recommended for archive-heavy intake)

## Quick Start

1. Install dependencies.

```bash
uv sync
```

2. Prepare CPA config.

```bash
copy config.example.json config.json
```

Set at least:

- `base_url`
- `token`

3. Prepare watcher profile config.

```bash
copy auto_maintain.config.example.json auto_maintain.config.json
```

4. Keep `auth_files` as input placeholder.

- This repo tracks only `auth_files/.gitkeep`.
- Runtime JSON/archive files under `auth_files` are ignored by git.

5. Start optimized profile.

```bat
start_auto_maintain_optimized.bat
```

## Runtime State and Ignore Rules

- `.auto_maintain_state/` is runtime-only and ignored by git
- `auth_files/*` is ignored except `auth_files/.gitkeep`
- `auto_maintain.config.json` is local runtime profile and ignored by git
- runtime artifacts such as `*.sqlite3`, `*.log`, and export JSONs should not be committed

## Profile Defaults (From `auto_maintain.config.example.json`)

- maintain interval: `2400s`
- watch interval: `15s`
- upload stable wait: `5s`
- upload batch size: `100`
- smart scheduler and adaptive batching: enabled
- maintain-after-upload: enabled
- delete uploaded source JSON: enabled
- archive inspect + auto extract: enabled
- single-instance lock: enabled
- fail-fast on command failure: enabled

## Key Watcher Config Fields

Full schema is tracked in `auto_maintain.config.example.json`. Frequently adjusted keys:

- paths: `auth_dir`, `config_path`, `state_dir`, `maintain_db_path`, `upload_db_path`
- cadence: `maintain_interval_seconds`, `watch_interval_seconds`, `upload_stable_wait_seconds`
- upload scheduling: `upload_batch_size`, `adaptive_upload_batching`, `upload_high_backlog_*`
- maintain scheduling: `adaptive_maintain_batching`, `incremental_maintain_*`, `maintain_high_backlog_*`
  - scheduler mode switching rule: low backlog favors smaller slices for faster upload/maintain interleaving; high backlog favors larger slices for faster queue drain
  - total-backlog rule: upload/incremental batch selection consumes a shared backlog estimate (upload pending + incremental pending + full-maintain equivalent)
  - incremental defer rule: defer is used only for small-batch fill waiting (`batch_too_small_waiting_fill`), not cooldown/full-guard legacy reasons
  - optional smoothing/hysteresis knobs: `backlog_ewma_alpha`, `scheduler_hysteresis_enabled`, `*_high_backlog_enter_threshold`, `*_high_backlog_exit_threshold`
  - optional pressure/lock knobs: `next_batch_buffer_limit` (cap pending upload buffer growth), `account_lock_lease_seconds` (stale account-lock auto-expiry window)
- runtime behavior: `run_maintain_on_start`, `run_upload_on_start`, `run_maintain_after_upload`
- execution backend toggle: `inprocess_execution_enabled` (`false` = legacy subprocess mode; `true` = in-process channel execution)
- failure policy: `maintain_retry_count`, `upload_retry_count`, `command_retry_delay_seconds`, `continue_on_command_failure`
- safety: `allow_multi_instance`, `maintain_assume_yes`
- archive intake: `inspect_zip_files`, `auto_extract_zip_json`, `delete_zip_after_extract`, `archive_extensions`, `bandizip_*`, `use_windows_zip_fallback`

## Environment Variables

Main variables consumed by `auto_maintain.py` include:

- `WATCH_CONFIG_PATH`
- `AUTH_DIR`, `CONFIG_PATH`, `STATE_DIR`
- `MAINTAIN_DB_PATH`, `UPLOAD_DB_PATH`
- `MAINTAIN_LOG_FILE`, `UPLOAD_LOG_FILE`
- scheduler and cadence controls (`*_INTERVAL_*`, `*_BATCH_*`, `*_BACKLOG_*`)
- optional scheduler smoothing/hysteresis controls (`BACKLOG_EWMA_ALPHA`, `SCHEDULER_HYSTERESIS_ENABLED`, `*_HIGH_BACKLOG_ENTER_THRESHOLD`, `*_HIGH_BACKLOG_EXIT_THRESHOLD`)
- optional pressure/lock controls (`NEXT_BATCH_BUFFER_LIMIT`, `ACCOUNT_LOCK_LEASE_SECONDS`)
- behavior toggles (`RUN_*`, `ALLOW_MULTI_INSTANCE`, `CONTINUE_ON_COMMAND_FAILURE`, `MAINTAIN_ASSUME_YES`, `INPROCESS_EXECUTION_ENABLED`)
- archive controls (`INSPECT_ZIP_FILES`, `AUTO_EXTRACT_ZIP_JSON`, `ARCHIVE_EXTENSIONS`, `BANDIZIP_*`, `USE_WINDOWS_ZIP_FALLBACK`)
- dashboard toggles (`AUTO_MAINTAIN_FIXED_PANEL`, `AUTO_MAINTAIN_PANEL_COLOR`)

Precedence:

1. Environment variables
2. `--watch-config` / `WATCH_CONFIG_PATH` JSON
3. Built-in defaults

## Rollout and Rollback (Stage 7)

- Default remains rollback-safe: `inprocess_execution_enabled=false` (subprocess backend).
- Canary rollout path:
  1. enable `inprocess_execution_enabled=true` in `auto_maintain.config.json` (or set `INPROCESS_EXECUTION_ENABLED=1`)
  2. run short-window `--once` checks, then monitor normal watcher windows
  3. keep fail-fast enabled (`continue_on_command_failure=false`) during rollout
- Instant rollback path:
  - set `inprocess_execution_enabled=false` (or `INPROCESS_EXECUTION_ENABLED=0`) and restart watcher
  - this reverts to legacy subprocess lifecycle semantics without changing CLI entrypoints

## Core CLI Compatibility

`cpa_warden.py` remains upstream-compatible. Common commands:

```bash
uv run python cpa_warden.py --mode scan
uv run python cpa_warden.py --mode maintain --yes
uv run python cpa_warden.py --mode maintain --maintain-steps scan,quota,finalize --yes
uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive
uv run python cpa_warden.py --mode maintain-refill --min-valid-accounts 200 --upload-dir ./auth_files
```

Single-cycle dry run:

```bash
uv run python auto_maintain.py --watch-config ./auto_maintain.config.json --once
```

## Validation

CI baseline:

```bash
uv run python -m py_compile cpa_warden.py auto_maintain.py clean_codex_accounts.py
uv run python cpa_warden.py --help
uv run python auto_maintain.py --help
uv run python -m unittest -v tests/test_auto_maintain.py
```

Optional full local suite:

```bash
uv run python -m unittest discover -s tests -p "test_*.py"
```

Stage comparison note:

- For non-performance changes (for example reliability guardrails or config constraints), you can generate Stage-X comparison evidence with Gate-B marked as N/A:
  - `uv run python tools/stage_comparison_report.py --candidate results/stageX_json_replay_candidate.csv --output results/stageX_vs_stage0_report.md --stage-label "Stage X" --commit-ref <commit-sha> --gate-b-mode na`

Note: the test suite now patches `tempfile.TemporaryDirectory()` to a workspace-safe implementation (`tests/temp_sandbox.py`) so full discovery remains stable in constrained Windows/Python 3.14 environments.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
