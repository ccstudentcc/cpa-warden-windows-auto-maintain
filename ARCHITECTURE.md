# Architecture

## Project Goal

This repository provides a Windows-first automation layer on top of the CPA maintenance capabilities from `cpa_warden.py`.

## Module Map

### `cwma/apps/cpa_warden.py`

- Entry point: `main()` via root compatibility script `cpa_warden.py`
- Public interface: CLI modes (`scan`, `maintain`, `upload`, `maintain-refill`) and related flags
- Responsibility: core CPA API interactions, account classification, actions, and exports
- Internal note: CLI argument parsing is delegated to `cwma/warden/cli.py` while keeping app-layer compatibility wrapper `parse_args(...)`
- Internal note: interactive prompt primitives are delegated to `cwma/warden/interactive.py` via app-layer wrappers
- Internal note: config loading + settings build are delegated to `cwma/warden/config.py` via app-layer compatibility wrappers
- Internal note: maintain/upload scope and upload discovery helpers are delegated to `cwma/warden/services/*` via app-layer compatibility wrappers
- Test coverage status: currently validated through CLI checks, production usage, and module tests (`tests/test_warden_cli_module.py`, `tests/test_warden_interactive_module.py`, `tests/test_warden_config_module.py`, `tests/test_warden_maintain_scope_module.py`, `tests/test_warden_upload_scope_module.py`)

### `cwma/warden/cli.py`

- Entry point: imported by `cwma/apps/cpa_warden.py`
- Public interface: `build_parser`, `parse_cli_args`
- Responsibility: centralized CPA warden CLI parser construction and argument parsing
- Test file: `tests/test_warden_cli_module.py`

### `cwma/warden/interactive.py`

- Entry point: imported by `cwma/apps/cpa_warden.py`
- Public interface: `prompt_string`, `prompt_int`, `prompt_float`, `prompt_yes_no`, `prompt_choice`
- Responsibility: reusable interactive prompt primitives for string/number/boolean/choice input handling
- Test file: `tests/test_warden_interactive_module.py`

### `cwma/warden/config.py`

- Entry point: imported by `cwma/apps/cpa_warden.py`
- Public interface: `build_default_settings_values`, `config_lookup`, `parse_bool_config`, `load_config_json`, `build_settings`
- Responsibility: centralized config parsing and settings build/validation for CPA warden modes
- Test file: `tests/test_warden_config_module.py`

### `cwma/warden/services/maintain_scope.py`

- Entry point: imported by `cwma/apps/cpa_warden.py` and `cwma/warden/services/upload_scope.py`
- Public interface: `load_name_scope_file`, `resolve_maintain_name_scope`, `resolve_upload_name_scope`
- Responsibility: scoped-name file loading and maintain/upload scope resolution helpers
- Test file: `tests/test_warden_maintain_scope_module.py`

### `cwma/warden/services/upload_scope.py`

- Entry point: imported by `cwma/apps/cpa_warden.py`
- Public interface: `discover_upload_files`, `validate_and_digest_json_file`, `select_upload_candidates`
- Responsibility: upload file discovery, JSON validation/digest, and local candidate conflict/duplicate selection
- Test file: `tests/test_warden_upload_scope_module.py`

### `cwma/auto/app.py`

- Entry point: `main()` via root compatibility script `auto_maintain.py`
- Public interface: `--watch-config`, environment-variable overrides, and `--once`
- Responsibility: orchestration loop, upload/maintain concurrency, snapshotting, lock control, ZIP intake, retry/fail-fast policy
- Internal note: runtime upload/maintain state transitions are now funneled through centralized adapter methods to reduce duplicated field wiring and keep lifecycle paths consistent
- Internal note: startup/watch-iteration orchestration branches were extracted into dedicated coordinator methods to reduce `run()` complexity
- Internal note: run-once completion checks, watch-cycle sleep policy, and stage-failure resolution are coordinated through dedicated helper methods
- Internal note: startup/watch stage execution now goes through a unified `_run_stage(...)` helper for consistent failure gating
- Internal note: channel-specific success side effects are routed through dedicated handlers (`_handle_maintain_success` / `_handle_upload_success`) to isolate post-success orchestration
- Internal note: startup/watch/upload-scan/shutdown core orchestration has begun delegating into `cwma/auto/runtime/*` adapters, and process launch/poll/terminate paths now route through `cwma/auto/process_supervisor.py`
- Internal note: low-value host pass-through wrappers continue to be removed in small batches (ZIP/scope/snapshot helper seams now prefer direct module wiring where compatibility hooks are not needed)
- Internal note: upload start path is decomposed into dedicated helper steps (batch sizing, start decision, start payload prep, process launch) to keep channel lifecycle branches small and testable
- Internal note: non-success process-exit feedback application is deduplicated through `_apply_non_success_process_exit_feedback(...)` for maintain/upload poll paths
- Internal note: maintain/upload poll prelude now uses `_collect_exited_process_code(...)` to standardize exited-process collection + process-handle clearing
- Internal note: upload-success postprocess path is decomposed into dedicated helper steps (snapshot sync, queue/progress apply, follow-up deep check, post-upload maintain queueing)
- Internal note: maintain/upload poll lifecycle decision apply paths are deduplicated through `_decide_maintain_process_exit(...)` and `_decide_upload_process_exit(...)`
- Internal note: upload deep-scan path (`check_and_maybe_upload`) is decomposed into dedicated helper steps for scan inputs, cadence gate, stability resolve, and queue/no-change state transitions
- Internal note: active-upload source probe path is decomposed into dedicated helper steps for probe input capture, decision, state apply, and refresh dispatch
- Internal note: maintain/upload poll decision dispatch is decomposed into dedicated helper methods (`_handle_maintain_poll_decision` / `_handle_upload_poll_decision`) for consistent success/shutdown/non-success routing
- Internal note: maintain/upload start-error lifecycle now routes through dedicated decision + retry-feedback helpers to keep channel failure paths symmetric
- Internal note: maintain/upload subprocess launch wiring is centralized via `_start_channel_process(...)`; maintain pre-start guards/skip handling are split into focused helpers
- Internal note: upload deep-scan no-change/no-pending branches now share `_record_upload_scan_baseline(...)` to keep snapshot/count/signature updates consistent
- Internal note: startup/watch multi-stage execution now reuses `_run_stage_sequence(...)` for consistent ordered execution and fail-fast short-circuiting
- Internal note: startup configuration log emission is centralized through `_settings_log_rows(...)` to reduce duplicated output wiring
- Internal note: upload cleanup core logic is extracted to `cwma/auto/upload_cleanup.py`; app-layer methods now focus on orchestration + logging
- Internal note: progress panel rendering now uses `cwma/auto/panel_render.py` pure helpers, with `render_progress_snapshot` split into snapshot build, line composition, and signature-gate steps
- Internal note: startup bootstrap decisions are extracted to `cwma/auto/startup_flow.py` (`seed`, `zip follow-up`, `startup action plan`) with `_run_startup_phase` focused on orchestration
- Internal note: watch-cycle due-maintain advancement and upload-check gating are extracted to `cwma/auto/watch_cycle.py` for pure decision logic reuse
- Test file: `tests/test_auto_maintain.py`

### `cwma/apps/auto_maintain.py`

- Entry point: package-compatibility adapter for legacy imports and script paths
- Public interface: compatibility alias to `cwma/auto/app.py`
- Responsibility: keep existing `cwma.apps.auto_maintain` import targets stable during/after Stage-2 refactor

### `cwma/scheduler/smart_scheduler.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `SmartSchedulerConfig`, `SmartSchedulerPolicy`
- Responsibility: centralized scheduling policy decisions for adaptive upload batching and incremental maintain deferral rules

### `cwma/common/config_parsing.py`

- Entry point: imported by `cwma/auto/app.py` and `cwma/apps/cpa_warden.py`
- Public interface: `parse_bool_value`, `parse_int_value`, `resolve_path`, `load_json_object`, `pick_setting`
- Responsibility: shared strict config parsing and JSON/path helpers for cross-app consistency

### `cwma/auto/config.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `Settings`, `load_watch_config`, `load_settings`
- Responsibility: watcher settings model + env/watch-config/default resolution

### `cwma/auto/channel_commands.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `build_maintain_command`, `build_upload_command`, `format_maintain_start_message`, `format_upload_start_message`
- Responsibility: channel command assembly and start-log message formatting helpers

### `cwma/auto/channel_lifecycle.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `decide_maintain_start_error`, `decide_upload_start_error`, `decide_maintain_process_exit`, `decide_upload_process_exit`
- Responsibility: channel lifecycle decisions (start-error + process-exit retry/success/failure/shutdown) as pure functions

### `cwma/auto/channel_runner.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `ChannelStartResult`, `ProcessPollResult`, `start_channel_with_handler`, `poll_process_exit`
- Responsibility: shared channel start/poll wrappers that bridge low-level process launch with start-error handling and process-exit probing

### `cwma/auto/channel_feedback.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: message/stage helpers such as `format_command_*`, `maintain_pending_progress_stage`, `non_success_exit_progress_stage`, `build_non_success_exit_feedback`
- Responsibility: centralize channel-facing lifecycle log text and status-to-progress-stage mapping to avoid duplicated branch text in maintain/upload orchestration

### `cwma/auto/channel_start_prep.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `MaintainStartPrep`, `UploadStartPrep`, `prepare_maintain_start`, `prepare_upload_start`
- Responsibility: centralize channel start preparation payloads (scope-file derivation, command assembly, start-log context) before process launch

### `cwma/auto/channel_status.py`

- Entry point: imported by auto-maintain modules
- Public interface: channel/stage/state/status constants (e.g. `CHANNEL_UPLOAD`, `STAGE_PENDING`, `STATUS_RETRY`)
- Responsibility: single source of truth for channel and progress-status string constants to avoid drift across parser/dashboard/lifecycle modules

### `cwma/auto/snapshots.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `build_snapshot_lines`, `write_snapshot_lines`, `read_snapshot_lines`, `build_snapshot_file`, `compute_uploaded_baseline`, `compute_pending_upload_snapshot`, `extract_names_from_snapshot`
- Responsibility: snapshot line/file helpers and pure baseline/name-scope transformations isolated from runtime orchestration

### `cwma/auto/locking.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `InstanceLockState`, `acquire_instance_lock`, `release_instance_lock`, `is_pid_running`, `read_lock_pid`
- Responsibility: single-instance lock acquisition/release and stale-lock detection, including Windows file-lock path

### `cwma/auto/dashboard.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `fit_panel_line`, `panel_border_line`, `apply_panel_colors`
- Responsibility: pure terminal panel layout/color helpers reused by watcher dashboard rendering

### `cwma/auto/panel_snapshot.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `PanelSnapshot`, `build_panel_snapshot`
- Responsibility: pure dashboard snapshot composition from runtime state (queue/progress/state/retry metrics), isolated from rendering side effects

### `cwma/auto/process_output.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `preferred_decoding_order`, `decode_child_output_line`, `build_child_process_env`, `should_log_child_alert_line`
- Responsibility: child-process output decoding order, subprocess env defaults, and alert-line filtering

### `cwma/auto/progress_parser.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `ProgressUpdate`, `ProgressParseResult`, `parse_progress_line`
- Responsibility: parse upload/maintain child log lines into normalized progress-state updates
- Internal note: maintain progress stage mapping is centralized as module-level constants to avoid inline map drift

### `cwma/auto/runtime_state.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: legacy queue/runtime adapters plus composed runtime adapters (`build_*_runtime_state`, `unpack_*_runtime_state`, `build_auto_runtime_state`, `unpack_auto_runtime_state`)
- Responsibility: pure conversion helpers between orchestrator runtime fields and queue/runtime dataclasses, including Stage 2.5 composed runtime root wiring

### `cwma/auto/state_models.py`

- Entry point: imported by `cwma/auto/runtime_state.py` and `cwma/auto/app.py`
- Public interface: `UploadRuntimeState`, `MaintainRuntimeState`, `SnapshotRuntimeState`, `UiRuntimeState`, `LifecycleRuntimeState`, `AutoRuntimeState`
- Responsibility: typed composed runtime state containers used to reduce mutable-field sprawl in host orchestrator

### `cwma/auto/process_supervisor.py`

- Entry point: imported by `cwma/auto/app.py` and `cwma/auto/runtime/channel_runtime.py`
- Public interface: `start_channel`, `poll_channel_exit`, `terminate_channel`, `ChannelExitResult`
- Responsibility: process lifecycle supervision boundary around launch/poll/terminate and output pumping composition

### `cwma/auto/runtime/channel_runtime.py`

- Entry point: imported by `cwma/auto/runtime/__init__.py` and host integration points
- Public interface: `start_maintain_channel`, `start_upload_channel`, `poll_maintain_channel`, `poll_upload_channel` (+ flow result dataclasses)
- Responsibility: channel start/poll orchestration adapter layer that reuses lifecycle policy modules without changing policy semantics

### `cwma/auto/runtime/startup_runtime.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `StartupRuntimeState`, `StartupRuntimeDeps`, `StartupRuntimeResult`, `run_startup_cycle`
- Responsibility: startup phase orchestration adapter (seed, ZIP follow-up, startup maintain/upload checks, initial command starts)

### `cwma/auto/runtime/watch_runtime.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `WatchRuntimeState`, `WatchRuntimeDeps`, `WatchRuntimeResult`, `run_watch_iteration`
- Responsibility: watch-iteration orchestration adapter (scheduled maintain enqueue, poll/start stage sequencing, upload-check gate)

### `cwma/auto/runtime/upload_scan_runtime.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `run_upload_scan_cycle`, `run_active_upload_probe_cycle`
- Responsibility: upload deep-scan and active-upload probe orchestration wrappers with callback-injected side effects

### `cwma/auto/runtime/shutdown_runtime.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `ShutdownRuntimeState`, `request_shutdown`, `sleep_with_shutdown`, `current_loop_sleep_seconds`, `sleep_between_watch_cycles`
- Responsibility: shutdown and sleep cadence orchestration adapters (host-facing, behavior-preserving)

### `cwma/auto/ui_runtime.py`

- Entry point: imported by runtime/host integration points
- Public interface: `UiRuntimeState`, `UiRuntime`
- Responsibility: encapsulate dashboard render cadence/signature-gate behavior behind a runtime API (`on_stage_update`, `on_tick`, `render_if_needed`)

### `cwma/auto/upload_postprocess.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `build_upload_success_postprocess`, `UploadSuccessPostProcessResult`
- Responsibility: post-upload success pipeline decisions (uploaded baseline merge, pending snapshot derivation, queue/progress state, uploaded-name extraction)

### `cwma/auto/active_probe.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `ActiveUploadProbeState`, `ActiveUploadProbeDecision`, `decide_active_upload_probe`
- Responsibility: active-upload source-change probe decision state machine (change detection, detection logging gate, deep-scan cooldown gate)

### `cwma/auto/upload_scan_cadence.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `UploadScanCadenceDecision`, `decide_upload_deep_scan`
- Responsibility: pure deep-scan cadence decision for upload watcher checks (force/retry/change/interval triggers and counter transitions)

### `cwma/auto/scope_files.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `write_scope_names`
- Responsibility: maintain/upload scope-file serialization shared by both channels

### `cwma/auto/maintain_queue.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `MaintainQueueState`, `MaintainStartDecision`, `MaintainRuntimeState`, `QueueMaintainResult`, `clear_maintain_queue_state`, `queue_maintain_request`, `decide_maintain_start_scope`, `merge_incremental_maintain_names`, `mark_maintain_retry`, `mark_maintain_runtime_retry`, `mark_maintain_success`, `mark_maintain_terminal_failure`
- Responsibility: maintain queue + lifecycle transitions for full/incremental requests, start-scope slicing, and retry/success/failure state handling

### `cwma/auto/upload_queue.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `UploadQueueState`, `UploadStartDecision`, `UploadMergeResult`, `clear_upload_queue_state`, `decide_upload_start`, `mark_upload_retry`, `mark_upload_success`, `mark_upload_terminal_failure`, `mark_upload_no_changes`, `mark_upload_no_pending_discovered`, `merge_pending_upload_snapshot`
- Responsibility: upload queue normalization plus start/retry/success/failure and deep-scan queue reconciliation transitions

### `cwma/auto/output_pump.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `append_child_output_line`, `start_output_pump_thread`
- Responsibility: append child output lines to channel log files and run pump threads over child stdout

### `cwma/auto/process_runner.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `launch_child_command`, `start_channel_command`, `terminate_running_process`
- Responsibility: generic child command launch/start and graceful terminate/kill fallback behavior

### `cwma/auto/zip_intake.py`

- Entry point: imported by `cwma/auto/app.py`
- Public interface: `list_zip_paths`, `compute_zip_signature`, `count_zip_files`, `list_zip_json_entries`, `extract_zip_with_bandizip`, `extract_zip_with_windows_builtin`, `inspect_zip_archives`
- Responsibility: ZIP path/signature discovery plus extraction/inspection orchestration for watcher intake flow

### Compatibility Entrypoints (`repo root`)

- `auto_maintain.py`
- `cpa_warden.py`
- `smart_scheduler.py`

These root files forward to package implementations so existing user scripts and launcher paths continue to work.

### `auto_maintain.bat`

- Entry point: double-click / shell execution on Windows
- Public interface: forwards arguments to `auto_maintain.py`
- Responsibility: runtime bootstrap with `uv` preferred and Python fallback

### `start_auto_maintain_optimized.bat`

- Entry point: operational profile launcher
- Public interface: `auto_maintain.config.json` profile + pass-through args
- Responsibility: bootstrap watcher profile file and launch `auto_maintain.py --watch-config ...`

### `auto_maintain.config.example.json`

- Entry point: copied to `auto_maintain.config.json` for local runtime usage
- Public interface: JSON watcher profile keys mirroring watcher env names in snake_case
- Responsibility: repository-tracked default profile for Windows automation runs

## Runtime Data Flow

1. Watcher scans `auth_files` and computes snapshots.
2. When stable changes are detected, upload batch is queued.
3. Upload and maintain commands are scheduled as separate channels.
4. Upload channel executes serial batches (size controlled by `UPLOAD_BATCH_SIZE`).
5. Each upload batch is scope-limited by `--upload-names-file`.
6. Upload completion updates baseline snapshot and optionally deletes uploaded files.
7. Optional post-upload maintain is queued using names from that completed upload batch.
8. Runtime state persists under `.auto_maintain_state`.

## Settings Resolution Model

- Watcher config JSON is loaded from:
  - `--watch-config`
  - or `WATCH_CONFIG_PATH`
  - or local `auto_maintain.config.json` (if present)
- Final setting precedence:
  - environment variables
  - watcher config JSON
  - built-in defaults

## Concurrency Model

- The scheduler has two independent execution channels:
  - upload channel (`self.upload_process`)
  - maintain channel (`self.maintain_process`)
- Each channel has independent pending flags, retry counters, and retry due times.
- Command launch conditions are channel-local, so maintain can continue while upload is still running.
- Upload channel supports serial batch slicing so one large snapshot does not block post-upload incremental maintain for early batches.
- During active upload execution, watcher still performs lightweight JSON-count/ZIP-signature probes; detected changes trigger an immediate forced deep upload check right after current batch completion.
- During active upload execution, watcher also performs periodic deep queue refresh scans so newly arrived files can enter pending upload queue before current batch ends.
- Smart scheduler can adapt upload batch size under backlog pressure (`UPLOAD_HIGH_BACKLOG_THRESHOLD` / `UPLOAD_HIGH_BACKLOG_BATCH_SIZE`).
- Smart scheduler can also adapt incremental-maintain batch size under contention/backlog (`INCREMENTAL_MAINTAIN_BATCH_SIZE` / `MAINTAIN_HIGH_BACKLOG_*`).
- Runtime panel snapshots expose per-channel queue state in terminal output (`queue_files`, `queue_batches`, `queue_full`, `queue_incremental`) so operators can observe scheduler backlog behavior directly.
- Fixed dashboard redraw can be toggled by `AUTO_MAINTAIN_FIXED_PANEL`; color can be toggled by `AUTO_MAINTAIN_PANEL_COLOR`.
- Incremental maintain can be deferred by cooldown and full-maintain-guard rules (`INCREMENTAL_MAINTAIN_*`).
- Post-upload maintain is queued as an additional maintain reason, not as an upload blocking step.
- Scheduled maintain (`MAINTAIN_INTERVAL_SECONDS`) is full-scope.
- Post-upload maintain is incremental-scope, constrained to uploaded auth names through `--maintain-names-file`.

## Snapshot Model

Watcher consistency is built around three snapshot files:

- `current_snapshot.txt`: current observed JSON files and metadata
- `stable_snapshot.txt`: stable snapshot after wait-window confirmation
- `last_uploaded_snapshot.txt`: uploaded baseline used for change detection

Key rules:

- On upload success, baseline is merged from:
  - previous uploaded baseline,
  - current batch uploaded snapshot,
  - and files that still exist in current snapshot.
- Pending upload queue is derived from `current_snapshot - uploaded_baseline` after each successful upload batch.

This keeps partial-batch progress while avoiding incorrect "already uploaded" marking for files created during upload.

## ZIP Intake Model

- ZIP scanning can run before upload checks (`INSPECT_ZIP_FILES=1`).
- ZIP change detection uses signature delta (path/size/mtime), not only ZIP count.
- If enabled, ZIP extraction prefers Bandizip and can fall back to Windows extraction.
- ZIP-derived JSON changes feed into the same stable-snapshot + upload queue pipeline.

## State Files

Default state directory: `.auto_maintain_state`

- `cpa_warden_maintain.sqlite3`
- `cpa_warden_upload.sqlite3`
- `cpa_warden_maintain.log`
- `cpa_warden_upload.log`
- `maintain_command_output.log`
- `upload_command_output.log`
- `maintain_names_scope.txt`
- `upload_names_scope.txt`
- `last_uploaded_snapshot.txt`
- `current_snapshot.txt`
- `stable_snapshot.txt`
- `auto_maintain_launcher.lock`
- `auto_maintain.lock`

## Failure Model

- Default behavior is fail-fast (`CONTINUE_ON_COMMAND_FAILURE=0`).
- Upload and maintain have independent retry counters.
- `--once` always exits on failure.
- Single-instance lock is enabled by default (`ALLOW_MULTI_INSTANCE=0`) and enforced in two layers on Windows:
  - launcher pre-lock (`auto_maintain_launcher.lock`) in `auto_maintain.bat`;
  - runtime file lock (`auto_maintain.lock`) in `auto_maintain.py` using `msvcrt`.

## Cleanup Model

- On successful upload, source JSON files can be deleted (`DELETE_UPLOADED_FILES_AFTER_UPLOAD=1`).
- After file deletion, empty subdirectories under `auth_dir` are pruned.
- Cleanup is best-effort with warning logs on skipped/failed paths.

## Git Hygiene Model

- Runtime state directory is ignored.
- `auth_files` keeps only `.gitkeep` tracked.
- Sensitive local config (`config.json`) is ignored.

