# Architecture

Last updated: `2026-03-26`

Release line: `cwma v0.2.0` (`2026-03-26`)

## 1. Goal and Scope

This repository provides a Windows-first automation layer on top of upstream-compatible CPA tooling.

In scope:

- keep `cpa_warden.py` workflow compatibility
- run long-lived watcher orchestration on Windows
- isolate maintain/upload runtime state
- support archive intake (`.zip/.7z/.rar`) and file lifecycle cleanup
- provide deterministic channel lifecycle, retry, and failure semantics

Out of scope:

- replacing upstream CPA domain behavior with a new protocol
- breaking existing root entrypoint scripts (`auto_maintain.py`, `cpa_warden.py`, `smart_scheduler.py`)

## 2. Current Status Snapshot

As of `2026-03-25`, `cwma/auto` uses capability-oriented package boundaries:

- `orchestration`
- `channel`
- `state`
- `infra`
- `ui`
- `runtime` (host adapters)

The old redundant top-level `cwma/auto/*.py` compatibility wrappers were removed after migration.
Maintain pipeline Stage-3 runtime pieces are now in place:

- step-level claim/advance/requeue transitions are implemented in `cwma/auto/state/maintain_queue.py`
- in-process maintain pipeline cycle execution (`run_maintain_pipeline_cycle`) is provided by `cwma/auto/runtime/maintain_pipeline_runtime.py`
- maintain service execution is now modeled as explicit ordered steps (`scan -> delete_401 -> quota -> reenable -> finalize`) in `cwma/warden/services/maintain.py`
- subprocess maintain-channel start (`cwma/auto/runtime/channel_runtime_adapter.py`) now claims a single pipeline work item (`allow_scan_parallel=False`) and passes explicit `--maintain-steps` derived from claimed step (`scan` or `scan+<action-step>`)
- claimed/running jobs are preserved in pipeline state (not dropped on start), and are advanced/requeued by `mark_maintain_success` / `mark_maintain_runtime_retry` based on runtime outcome

Upload intake Stage-4 stability/queue hardening is now in place:

- stability wait freezes the current candidate batch and defers in-window new/updated rows to next-round intake
- pending upload queue merge is path-coalesced (`last-writer-wins`) to prevent stale duplicate row versions for the same file path
- Bandizip command resolution prefers console binaries (`bc.exe` then `bz.exe`) in console-preferred mode and avoids GUI fallback; Windows built-in unzip fallback is constrained to `.zip` archives only

Scheduling Stage-5 total-backlog policy is now in place:

- upload/incremental maintain batch sizing accepts a shared total-backlog signal (`pending upload + pending incremental + full-maintain equivalent backlog`)
- incremental maintain defer semantics now use smart small-fill prediction (`batch_too_small_waiting_fill`) based on maintain gap vs predicted upload fill capability
- watch-loop sleep cadence uses active-probe interval while either channel is running or pending queue/retry work exists (`pending upload`, `pending maintain`, `pending upload retry`)

UI Stage-6 step-queue observability is now in place:

- panel snapshot composition consumes maintain pipeline queue state and projects per-step `queued/running/retry` counters (`scan/delete_401/quota/reenable/finalize`)
- panel output now includes full/incremental maintain job counters and parallel-state hints (`channels`, `pipeline`, `channels+pipeline`)

Stage-7 hardening/docs/rollout is now in place:

- in-process execution is explicitly rollout-gated by `INPROCESS_EXECUTION_ENABLED` / `inprocess_execution_enabled` (default `false`, subprocess fallback preserved)
- test hardening now routes `tempfile.TemporaryDirectory()` through a workspace-safe sandbox helper (`tests/temp_sandbox.py`) for Python 3.14 + Windows stability in constrained environments
- README/README.zh-CN/ARCHITECTURE/BOUNDARY_MAP/CHANGELOG are synchronized for rollback and rollout guidance

## 3. Documentation Architecture

To avoid drift and duplicated maintenance:

- `README.md` / `README.zh-CN.md`: operator-facing usage and behavior summary
- `ARCHITECTURE.md` (this file): canonical system boundaries and models
- `cwma/auto/BOUNDARY_MAP.md`: capability ownership map and dependency direction policy
- `CONTRIBUTING.md`: contribution and validation workflow
- `CHANGELOG.md`: release-level history

## 4. Entrypoints and Public Surfaces

### 4.1 Root compatibility entrypoints

- `cpa_warden.py` -> forwards to `cwma/apps/cpa_warden.py`
- `auto_maintain.py` -> forwards to `cwma/auto/app.py`
- `smart_scheduler.py` -> forwards to `cwma/scheduler/smart_scheduler.py`

### 4.2 Windows launcher entrypoints

- `auto_maintain.bat`
- `start_auto_maintain_optimized.bat`

These launchers preserve existing operational command paths and bootstrap behavior.

## 5. Package Structure

### 5.1 CPA domain package (`cwma/warden`)

`cwma/apps/cpa_warden.py` is the app host. Domain logic is split into dedicated modules:

- `cwma/warden/cli.py`: parser construction and CLI argument handling
- `cwma/warden/config.py`: config parsing and settings build/validation
- `cwma/warden/interactive.py`: interactive prompt primitives
- `cwma/warden/models.py`: normalized model extraction/helpers
- `cwma/warden/exports.py`: export shaping and summary formatting
- `cwma/warden/api/*.py`: management and usage-probe HTTP boundaries
- `cwma/warden/db/*.py`: schema and repository persistence boundaries
- `cwma/warden/services/*.py`: scan/maintain/upload/refill/runtime operations and scope helpers

### 5.2 Auto orchestration package (`cwma/auto`)

Canonical ownership is grouped by capability.

- `orchestration`: startup/watch sequencing and stage flow
- `channel`: maintain/upload channel lifecycle and decision policy
- `state`: queue/runtime/snapshot transitions and pure state decisions
- `infra`: process, archive, cleanup, lock, config, and shutdown side effects
- `ui`: progress parsing and dashboard rendering
- `runtime`: adapter layer that wires host callbacks/dependencies while keeping `cwma/auto/app.py` thin

### 5.3 Shared and policy packages

- `cwma/scheduler/smart_scheduler.py`: adaptive batching/defer policy model
- `cwma/common/config_parsing.py`: shared strict config + JSON/path parsing helpers

## 6. Capability Boundaries and Dependency Rules

`cwma/auto/BOUNDARY_MAP.md` is the source of truth. Summary:

1. `orchestration -> channel|state|infra|ui` is allowed.
2. `channel -> state|infra` is allowed; channel must not import orchestration entrypoints.
3. `state` remains pure and must not import orchestration/channel runtime adapters or infra side-effect modules.
4. `infra` must not import orchestration/channel policy modules.
5. `ui -> state|ui-local helpers` is allowed; UI must not drive channel/orchestration policy.

Change ownership rule:

- Every change in `cwma/auto` should map to a primary capability owner before implementation.
- Cross-capability changes must explain why a single capability boundary is insufficient.

## 7. Runtime Flow Model

Watcher lifecycle (`cwma/auto/app.py` + runtime adapters):

1. Load settings (`env > --watch-config JSON > defaults`) and initialize host/runtime state.
2. Build baseline snapshots and bootstrap channel queues.
3. Optionally inspect/extract archives and merge resulting changes into upload discovery.
4. Run startup stage (startup queueing + initial channel starts).
5. Enter watch loop:
   - queue scheduled full maintain when due
   - poll running channels
   - run active probes while channels are running
   - run upload deep-scan cadence checks
   - start idle channels with pending work
   - if either channel has pending/retry work, use active-probe sleep cadence instead of full watch interval
6. On upload success:
   - update uploaded baseline and pending snapshots
   - apply cleanup policy
   - queue scoped post-upload maintain (incremental names set) when enabled
7. On failure:
   - apply channel-local retry policy
   - enforce fail-fast/non-zero semantics based on configuration and `--once`
8. On shutdown:
   - propagate shutdown signal
   - terminate child processes safely
   - release locks

## 8. State and Persistence Model

Default runtime state directory: `.auto_maintain_state`

Core runtime artifacts:

- maintain/upload sqlite db files
- maintain/upload logs
- maintain/upload scope files
- maintain pipeline state (`MaintainJob` + step queues: `scan/delete_401/quota/reenable/finalize`)
- current/stable/uploaded snapshots
- launcher/runtime lock files
- child command output logs

Snapshot consistency contracts:

- `current_snapshot`: current observed files
- `stable_snapshot`: stability-window-confirmed snapshot
- `last_uploaded_snapshot`: uploaded baseline

Pending upload queue is derived from snapshot delta relative to uploaded baseline.
Stability wait may emit a deferred snapshot buffer; deferred rows are merged back into pending intake after the frozen batch is queued.

## 9. Configuration Resolution Model

Watcher setting source precedence:

1. environment variables
2. watcher JSON (`--watch-config` / `WATCH_CONFIG_PATH`)
3. built-in defaults

Tracked default template:

- `auto_maintain.config.example.json`

Runtime-local copy:

- `auto_maintain.config.json` (ignored by git)

Rollout/rollback controls:

- `inprocess_execution_enabled` (`INPROCESS_EXECUTION_ENABLED`) gates in-process channel backend rollout
- default remains `false` to keep subprocess lifecycle as immediate rollback path

## 10. Concurrency and Scheduling Model

- Two independent process channels:
  - upload channel
  - maintain channel
- Channel lifecycle and retry counters are isolated.
- Upload runs in serial batches (`UPLOAD_BATCH_SIZE`) with scope files.
- Upload stability wait uses a frozen-batch model: in-window new/updated rows do not reset the current wait timer; they are deferred to subsequent queue intake.
- Upload queue intake supports optional backpressure cap via `next_batch_buffer_limit` (default `null` = uncapped).
- Scheduled maintain is full scope.
- Post-upload maintain is incremental and derived from completed upload batch names.
- Maintain queue state keeps a unified Job model for full/incremental and separate step queues (`scan/delete_401/quota/reenable/finalize`).
- UI snapshot derives step-level queue/running/retry telemetry from the same maintain pipeline state and preserves fallback rendering when pipeline fields are absent.
- Maintain step engine enforces serial order per job and supports cross-job pipeline concurrency:
  - one action-stage job can run while another job runs scan
  - action-stage claim respects account-name locks to avoid conflicting side effects
- Maintain account locks support optional lease expiry via `account_lock_lease_seconds` to prevent stale lock starvation.
- Full and incremental maintain jobs share the same step engine and transition rules.
- Smart scheduler adapts upload/maintain batching and incremental maintain deferral under backlog pressure.
- Smart scheduler batch planning now consumes total backlog, not only local queue length.
- Smart scheduler uses backlog-sensitive mode switching:
  - lower backlog: smaller slices to improve upload/maintain interleaving responsiveness
  - higher backlog: larger slices to improve queue-drain throughput
- Smart scheduler optionally supports EWMA backlog smoothing and enter/exit hysteresis thresholds:
  - `backlog_ewma_alpha` controls smoothing intensity (`1.0` keeps raw signal behavior)
  - `scheduler_hysteresis_enabled` + `*_high_backlog_enter_threshold` / `*_high_backlog_exit_threshold` can reduce mode flapping when backlog oscillates near thresholds
- Incremental maintain defer is constrained to a smart fill behavior:
  - defer only when current incremental pending is below planned batch and predicted upload fill can close most/all of the gap soon
  - no defer when fill signal is weak/absent, and no cooldown/full-guard legacy reasons

## 11. Failure and Safety Model

- Default policy is fail-fast (`continue_on_command_failure = false`).
- `--once` mode exits non-zero on unresolved failure.
- In-process execution rollout is reversible at config/env level:
  - enable: `inprocess_execution_enabled=true` (or `INPROCESS_EXECUTION_ENABLED=1`)
  - rollback: `inprocess_execution_enabled=false` (or `INPROCESS_EXECUTION_ENABLED=0`)
- Single-instance safety is enforced in two layers on Windows:
  - launcher lock (`auto_maintain_launcher.lock`)
  - runtime lock (`auto_maintain.lock`, Windows file lock via `msvcrt`)
- Cleanup removes only confirmed uploaded sources and then prunes empty directories.

## 12. Test Architecture

### 12.1 CI baseline

Current CI validates:

- bytecode compile checks
- CLI help for `cpa_warden.py` and `auto_maintain.py`
- watcher regression suite `tests/test_auto_maintain.py`

### 12.2 Capability-oriented module suites

Stage-2.6 split suites:

- `tests/test_auto_modules_process_channel.py`
- `tests/test_auto_modules_state.py`
- `tests/test_auto_modules_ui.py`

Maintain pipeline runtime suites:

- `tests/test_auto_maintain_pipeline_state_module.py`
- `tests/test_auto_maintain_pipeline_runtime_module.py`

Orchestration-specific suites:

- `tests/test_startup_flow_module.py`
- `tests/test_watch_cycle_module.py`

CPA domain split suites:

- `tests/test_warden_*_module.py` files (CLI/config/models/api/db/services/exports/interactive/runtime_ops)
- maintain service step-engine behavior is covered in `tests/test_warden_maintain_service_module.py`

Stage-7 hardening note:

- `tests/temp_sandbox.py` is the shared temp sandbox bootstrap for unittest modules that rely on `tempfile.TemporaryDirectory()`; it stabilizes test I/O on Python 3.14 Windows runners by using workspace-local temp dirs.

## 13. Architecture Change Checklist

When changing behavior that crosses module boundaries:

1. Update `cwma/auto/BOUNDARY_MAP.md` if ownership/dependency directions change.
2. Update this file if runtime flow, state contracts, or failure semantics change.
3. Update `README.md` and `README.zh-CN.md` only for operator-visible behavior changes.
4. Add/adjust tests in the suite mapped to the owning capability.
5. Record user-visible or operationally significant changes in `CHANGELOG.md`.
