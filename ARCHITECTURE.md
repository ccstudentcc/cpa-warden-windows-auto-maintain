# Architecture

Last updated: `2026-03-24`

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

As of `2026-03-24`, `cwma/auto` uses capability-oriented package boundaries:

- `orchestration`
- `channel`
- `state`
- `infra`
- `ui`
- `runtime` (host adapters)

The old redundant top-level `cwma/auto/*.py` compatibility wrappers were removed after migration.

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

## 9. Configuration Resolution Model

Watcher setting source precedence:

1. environment variables
2. watcher JSON (`--watch-config` / `WATCH_CONFIG_PATH`)
3. built-in defaults

Tracked default template:

- `auto_maintain.config.example.json`

Runtime-local copy:

- `auto_maintain.config.json` (ignored by git)

## 10. Concurrency and Scheduling Model

- Two independent process channels:
  - upload channel
  - maintain channel
- Channel lifecycle and retry counters are isolated.
- Upload runs in serial batches (`UPLOAD_BATCH_SIZE`) with scope files.
- Scheduled maintain is full scope.
- Post-upload maintain is incremental and derived from completed upload batch names.
- Maintain queue state now keeps a unified Job model for full/incremental and separate step queues;
  Stage-2 currently drives start decisions from the `scan` queue while preserving legacy pending projections.
- Smart scheduler adapts upload/maintain batching and incremental maintain deferral under backlog pressure.
- Smart scheduler uses backlog-sensitive mode switching:
  - lower backlog: smaller slices to improve upload/maintain interleaving responsiveness
  - higher backlog: larger slices to improve queue-drain throughput

## 11. Failure and Safety Model

- Default policy is fail-fast (`continue_on_command_failure = false`).
- `--once` mode exits non-zero on unresolved failure.
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

Orchestration-specific suites:

- `tests/test_startup_flow_module.py`
- `tests/test_watch_cycle_module.py`

CPA domain split suites:

- `tests/test_warden_*_module.py` files (CLI/config/models/api/db/services/exports/interactive/runtime_ops)

## 13. Architecture Change Checklist

When changing behavior that crosses module boundaries:

1. Update `cwma/auto/BOUNDARY_MAP.md` if ownership/dependency directions change.
2. Update this file if runtime flow, state contracts, or failure semantics change.
3. Update `README.md` and `README.zh-CN.md` only for operator-visible behavior changes.
4. Add/adjust tests in the suite mapped to the owning capability.
5. Record user-visible or operationally significant changes in `CHANGELOG.md`.
