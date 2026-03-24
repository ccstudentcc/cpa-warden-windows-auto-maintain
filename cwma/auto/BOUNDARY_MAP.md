# CWMA Auto Boundary Map (Stage 2.6)

## Purpose

This document defines capability-oriented ownership boundaries for `cwma/auto` and `cwma/auto/runtime`:

- `orchestration`
- `channel`
- `state`
- `infra`
- `ui`

The goal is to make change impact explicit and keep dependency directions stable.

Compatibility note:
- Top-level modules under `cwma/auto/*.py` are retained as compatibility wrappers that alias canonical implementations in subpackages.

## Capability Ownership

### Orchestration

Owner scope:
- `cwma/auto/app.py`
- `cwma/auto/orchestration/startup_flow.py`
- `cwma/auto/orchestration/watch_cycle.py`
- `cwma/auto/runtime/startup_runtime.py`
- `cwma/auto/runtime/watch_runtime.py`
- `cwma/auto/runtime/upload_scan_runtime.py`
- `cwma/auto/runtime/cycle_runtime_adapter.py`
- `cwma/auto/runtime/host_init_adapter.py`

Responsibilities:
- startup/watch cycle ordering
- runtime dependency wiring
- stage sequencing and failure propagation

### Channel

Owner scope:
- `cwma/auto/channel/channel_status.py`
- `cwma/auto/channel/channel_commands.py`
- `cwma/auto/channel/channel_start_prep.py`
- `cwma/auto/channel/channel_feedback.py`
- `cwma/auto/channel/channel_lifecycle.py`
- `cwma/auto/channel/channel_runner.py`
- `cwma/auto/runtime/channel_runtime.py`
- `cwma/auto/runtime/channel_runtime_adapter.py`
- `cwma/auto/runtime/upload_runtime_adapter.py`

Responsibilities:
- maintain/upload start + poll lifecycle
- channel-specific retry/success/failure semantics
- channel progress/status feedback shaping

### State

Owner scope:
- `cwma/auto/state/state_models.py`
- `cwma/auto/state/runtime_state.py`
- `cwma/auto/state/maintain_queue.py`
- `cwma/auto/state/upload_queue.py`
- `cwma/auto/state/upload_postprocess.py`
- `cwma/auto/state/upload_scan_cadence.py`
- `cwma/auto/state/active_probe.py`
- `cwma/auto/state/snapshots.py`
- `cwma/auto/state/scope_files.py`
- `cwma/auto/runtime/state_bridge_adapter.py`

Responsibilities:
- queue/runtime/snapshot state transformations
- pure decision/state-transition logic
- maintain/upload scope/baseline derivation

### Infra

Owner scope:
- `cwma/auto/infra/config.py`
- `cwma/auto/infra/process_output.py`
- `cwma/auto/infra/output_pump.py`
- `cwma/auto/infra/process_runner.py`
- `cwma/auto/infra/process_supervisor.py`
- `cwma/auto/infra/zip_intake.py`
- `cwma/auto/infra/locking.py`
- `cwma/auto/infra/upload_cleanup.py`
- `cwma/auto/runtime/host_ops_adapter.py`
- `cwma/auto/runtime/lifecycle_runtime_adapter.py`
- `cwma/auto/runtime/shutdown_runtime.py`

Responsibilities:
- process lifecycle + stdout/stderr pumping
- file-system and ZIP side effects
- lock lifecycle and shutdown/sleep cadence
- config loading/parsing boundary

### UI

Owner scope:
- `cwma/auto/ui/dashboard.py`
- `cwma/auto/ui/panel_snapshot.py`
- `cwma/auto/ui/panel_render.py`
- `cwma/auto/ui/progress_parser.py`
- `cwma/auto/ui/ui_runtime.py`
- `cwma/auto/runtime/panel_runtime_adapter.py`

Responsibilities:
- progress parsing and presentation state
- panel snapshot composition and render policy
- fixed-panel terminal I/O adapter behavior

### Package Surface

Owner scope:
- `cwma/auto/__init__.py`
- `cwma/auto/runtime/__init__.py`

Responsibilities:
- package import surface only
- no capability policy logic

## Stage 2.6 Test Modules Map (Closeout Completed)

1. `process/channel/infra` runtime and lifecycle seams are mapped to `tests/test_auto_modules_process_channel.py` as the active destination for new coverage in this capability group.
2. `ui` snapshot/render/progress seams are mapped to `tests/test_auto_modules_ui.py` as the active destination for new coverage in this capability.
3. `state` queue/runtime transition seams are mapped to `tests/test_auto_modules_state.py` as the active destination for new coverage in this capability.
4. Transitional residual suite `tests/test_auto_modules.py` has been retired after capability coverage drain; new stage-2.6 module coverage should continue in the three split suites above.

## Allowed Dependency Directions

Rules:
1. `orchestration` may depend on `channel`, `state`, `infra`, and `ui`.
2. `channel` may depend on `state` and `infra`; it may emit `ui` feedback payloads but must not import orchestration entrypoints.
3. `state` may depend on local value models and pure helpers; it must not import orchestration runtime adapters or infra side-effect modules.
4. `infra` must not import orchestration or channel policy modules.
5. `ui` may depend on `state` and pure formatting helpers; it must not drive channel or orchestration policy.

## Ownership Notes for Future Changes

1. If a change modifies stage order or run-loop control, start in `orchestration`.
2. If a change modifies maintain/upload lifecycle semantics, start in `channel`.
3. If a change modifies queue/snapshot transitions, start in `state`.
4. If a change modifies process/file/zip/lock side effects, start in `infra`.
5. If a change modifies panel/progress output behavior, start in `ui`.
