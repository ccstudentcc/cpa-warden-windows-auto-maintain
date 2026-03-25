# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Added Stage-0 baseline replay tooling and artifacts:
  - `tools/stage0_json_replay_benchmark.py` for burst/sustained/mixed high-frequency JSON event replay
  - `results/stage0_json_replay_baseline.csv` as baseline metrics output
  - `results/stage0_baseline_report.md` as the baseline validation/readout record
- Added maintain pipeline runtime module `cwma/auto/runtime/maintain_pipeline_runtime.py` for step-cycle orchestration (`1 action + 1 scan` pipeline claim policy per cycle).
- Added maintain pipeline runtime test suite `tests/test_auto_maintain_pipeline_runtime_module.py` covering step-order progression, retry requeue, account-lock conflict handling, and full/incremental shared executor behavior.
- Added upload stability suite `tests/test_auto_upload_stability_module.py` covering frozen-batch stability wait and deferred next-round intake behavior.
- Added shared test temp sandbox helper `tests/temp_sandbox.py` to stabilize `tempfile.TemporaryDirectory()` behavior on Python 3.14 Windows runners.
- Added Gate-B comparison report generator `tools/stage_comparison_report.py` and tool tests `tests/test_stage_comparison_report_tool_module.py` for baseline-vs-candidate KPI checks and markdown report output.
- Added G1/G2 quality gate runner `tools/quality_gate_runner.py` and tool tests `tests/test_quality_gate_runner_tool_module.py` for one-command compile/test gate execution and summary report output.

### Changed

- Reworked documentation architecture to reduce duplication and align with current package boundaries:
  - `README.md` / `README.zh-CN.md` now focus on operator-facing usage and runtime behavior summary
  - `ARCHITECTURE.md` rewritten as current-state boundary/model reference (instead of stage-history-heavy narrative)
  - `CONTRIBUTING.md` now defines doc ownership/update rules to keep cross-doc consistency
- Added CLI option `--upload-names-file` (upload mode) to constrain upload candidates to a provided name set
- Added `smart_scheduler.py` policy helper for adaptive upload batching and incremental-maintain pacing decisions
- Watcher upload scheduling now supports `UPLOAD_BATCH_SIZE` and executes serial scoped batches so post-upload incremental maintain can start earlier
- Watcher now supports smart scheduling knobs for low/high-frequency compatibility:
  - `SMART_SCHEDULE_ENABLED`
  - `ADAPTIVE_UPLOAD_BATCHING`
  - `UPLOAD_HIGH_BACKLOG_THRESHOLD`
  - `UPLOAD_HIGH_BACKLOG_BATCH_SIZE`
  - `INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS`
  - `INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS`
- Watcher now pumps child command output through parser threads, keeps command-output artifacts under `.auto_maintain_state/*_command_output.log`, and renders concise per-channel terminal panel snapshots to avoid parallel progress-bar flicker
- Panel snapshots now expose per-channel queue visibility (`queue_files`, `queue_batches`, `queue_full`, `queue_incremental`, retry waits, and next full-maintain wait)
- Panel snapshots now also expose maintain pipeline step-queue observability (`steps_qr`, `steps_retry`, `retry_jobs`), full/incremental job counters (`jobs_full`, `jobs_incremental`), and channel/pipeline parallel-state hints
- Added fixed dashboard redraw and optional color/channel separators for clearer upload/maintain panel readability (`AUTO_MAINTAIN_FIXED_PANEL`, `AUTO_MAINTAIN_PANEL_COLOR`)
- Child output decoding now uses UTF-8-first fallback chain (including GB18030/CP936) to reduce mojibake in Chinese logs
- While upload is running, watcher now performs lightweight JSON/ZIP change probing and triggers immediate deep upload-check after current batch completes
- While upload is running, watcher now also performs periodic deep queue refresh (`ACTIVE_UPLOAD_DEEP_SCAN_INTERVAL_SECONDS`) so newly arrived JSON/ZIP changes are queued earlier, not only at batch end
- Main loop now uses faster active probe cadence (`ACTIVE_PROBE_INTERVAL_SECONDS`) while upload/maintain processes are running
- Smart scheduler now supports adaptive incremental-maintain batch slicing (`ADAPTIVE_MAINTAIN_BATCHING`, `INCREMENTAL_MAINTAIN_BATCH_SIZE`, `MAINTAIN_HIGH_BACKLOG_*`) to improve upload/maintain interleaving under backlog pressure
- Smart scheduler now performs backlog-sensitive mode switching for upload/incremental-maintain:
  - lower backlog favors smaller slices for real-time interleaving
  - higher backlog favors larger slices for queue-drain throughput
  - incremental maintain start can be deferred briefly in upload backlog priority mode to avoid over-contending with active upload
- Refactored repository layout into package-first structure:
  - implementations moved to `cwma/apps/*` and `cwma/scheduler/*`;
  - root scripts (`auto_maintain.py`, `cpa_warden.py`, `smart_scheduler.py`) now act as compatibility entrypoints
- Upload baseline merge now preserves previous successful batches when current run only processes part of the queue
- `auto_maintain.bat` now adds launcher-level lock precheck (`auto_maintain_launcher.lock`) before process start
- Python runtime lock arbitration remains authoritative and now uses Windows file locking (`msvcrt`) on `auto_maintain.lock`
- Stage-2.6 capability test mapping closeout completed: retired `tests/test_auto_modules.py`, aligned architecture/boundary docs to split suites, and updated regression commands to run only `process_channel` / `state` / `ui` modules
- Auto module layout now groups canonical implementations into capability subpackages (`cwma/auto/orchestration`, `channel`, `state`, `infra`, `ui`) and removes redundant top-level compatibility wrappers after import/test migration
- Maintain queue state transitions now expose explicit step-level claim/advance/requeue operations in `cwma/auto/state/maintain_queue.py`, including account-lock-aware action-step conflict checks.
- `cwma/warden/services/maintain.py` now provides an explicit maintain step engine (`scan -> delete_401 -> quota -> reenable -> finalize`) with ordered-step validation via `run_maintain_steps_async`; `run_maintain_async` remains the compatible full-flow entrypoint.
- Extended `tests/test_warden_maintain_service_module.py` with maintain step-order validation and partial-step execution coverage.
- Upload stability wait now freezes the current candidate batch and defers in-window new/updated rows to the next queue intake instead of resetting the timer indefinitely.
- Upload pending queue merge is now path-coalesced (`last-writer-wins`) so active deep-scan intake replaces stale versions of the same file path.
- Smart scheduler batch selection now consumes a shared total-backlog estimate (`upload pending + incremental pending + full-maintain equivalent`) when deciding upload and incremental-maintain batch sizes.
- Smart scheduler now supports optional backlog signal smoothing and hysteresis thresholds:
  - `BACKLOG_EWMA_ALPHA` / `backlog_ewma_alpha`
  - `SCHEDULER_HYSTERESIS_ENABLED` / `scheduler_hysteresis_enabled`
  - `*_HIGH_BACKLOG_ENTER_THRESHOLD` / `*_HIGH_BACKLOG_EXIT_THRESHOLD` (upload + maintain)
- Incremental maintain defer semantics are narrowed to the small-fill case only (`batch_too_small_waiting_fill`) when upload-side fill source is active; cooldown/full-guard backlog-priority defer reasons were removed.
- `cwma/auto/runtime/channel_runtime_adapter.py` now computes total backlog at maintain/upload start boundaries and passes it to scheduler policy consistently (including panel next-batch projections via `panel_runtime_adapter`).
- Added Stage-5 scheduler/defer regression coverage in `tests/test_auto_modules_state.py` and host-level Stage-5 integration coverage in `tests/test_auto_maintain.py`.
- Stage-7 hardening now applies the shared temp sandbox bootstrap across unittest modules that rely on `tempfile`, making full discovery (`273` tests) pass consistently in constrained Windows/Python 3.14 environments.
- Stage-7 rollout/rollback documentation is now synchronized across `README.md`, `README.zh-CN.md`, `ARCHITECTURE.md`, and `cwma/auto/BOUNDARY_MAP.md` with explicit `INPROCESS_EXECUTION_ENABLED` guidance.
- In-process channel startup now forces `CPA_WARDEN_DISABLE_RICH_PROGRESS=1` so nested `cpa_warden` runs do not render Rich live progress bars that interfere with watcher fixed-panel terminal UI.
- Added operator command quick entries for Gate-B and G1/G2 runner workflows in `cmd.md`.
- CI workflow now runs unified gate command `tools/quality_gate_runner.py --strict` instead of scattered compile/help/test steps.
- CI now uploads `results/quality_gate_report_ci.md` as a build artifact for each Python matrix entry.
- CI now publishes `quality_gate_report_ci.md` summary (`Overall` + Gate Summary table) to GitHub Actions job summary.
- In-process channel logging now bridges through the watcher output callback path (instead of direct stdout), so progress parsing/panel state updates remain consistent and fixed-panel redraw is no longer torn by child logger output.

## [cwma 0.1.0] - 2026-03-23

Git baseline: `b8acb43956f5d99fefd1c2093c9fa32c13fd23da`

### Added

- Added `NOTICE` with explicit upstream attribution to `fantasticjoe/cpa-warden` and derivative baseline commit `f3778f4`
- Added `ARCHITECTURE.md` describing module responsibilities, runtime flow, and state files for the Windows automation stack
- Added `auto_maintain.config.example.json` as a tracked watcher profile template
- Added watcher CLI option `--watch-config` (`WATCH_CONFIG_PATH` supported) for loading watcher settings from JSON

### Changed

- Repositioned project documentation toward Windows-first automation orchestration while preserving `cpa_warden.py` compatibility
- Updated English and Simplified Chinese README files to document watcher operation, runtime state model, and ignore policy for `auth_files/.gitkeep` + `.auto_maintain_state`
- Expanded README/README.zh-CN with explicit "improvement highlights" and step-by-step watcher execution logic, including upload/maintain parallel scheduling details
- Updated `CONTRIBUTING.md` and `SECURITY.md` to align with derivative-project governance and runtime artifact handling rules
- Updated package metadata name/description to reflect the derivative Windows automation project identity
- CI now validates `auto_maintain.py` compilation/help and runs `tests/test_auto_maintain.py`
- Expanded `ARCHITECTURE.md` with explicit concurrency, snapshot, ZIP intake, and cleanup models
- `auto_maintain.py` now schedules `upload` and `maintain` commands in parallel channels so maintenance no longer blocks behind long upload batches
- Scheduled maintain keeps full scope, while post-upload maintain now runs incremental scope based on uploaded auth names
- Added CLI option `--maintain-names-file` (maintain mode) to constrain scan/actions to a provided name set
- Added split runtime paths for watcher-managed command state: `MAINTAIN_DB_PATH` / `UPLOAD_DB_PATH` and `MAINTAIN_LOG_FILE` / `UPLOAD_LOG_FILE`
- Updated `start_auto_maintain_optimized.bat` profile defaults to use dedicated maintain/upload SQLite files and log files under `.auto_maintain_state`
- Fixed upload snapshot baseline handling in watcher flow to avoid marking mid-upload new files as already uploaded
- Watcher now queues a follow-up upload batch when files are detected outside the completed upload baseline
- Hardened snapshot generation against transient file-system races (missing/replaced files during scans)
- Upload cleanup now also prunes empty subdirectories under `auth_dir` after file deletion
- ZIP change triggering now checks signature deltas (path/size/mtime) instead of only count changes
- Added `MAINTAIN_ASSUME_YES` to control whether watcher adds `--yes` for maintain command
- Watcher failure handling now fails fast by default (`CONTINUE_ON_COMMAND_FAILURE=false`) and forces fail-fast in `--once` mode
- Replaced silent exception swallowing in critical runtime paths with warning logs
- Launcher now relies on Python-side lock arbitration instead of pre-filtering by PID reuse in batch script
- Corrected default Bandizip path spelling in optimized launcher profile
- `start_auto_maintain_optimized.bat` now bootstraps and uses `auto_maintain.config.json` instead of hardcoding watcher env values
- Watcher setting resolution now follows: environment variables > watch config JSON > built-in defaults

## [0.2.0] - 2026-03-09

### Added

- Added `upload` mode to concurrently upload local auth JSON files via `POST /v0/management/auth-files`
- Added upload-related CLI/config options: `upload_dir`, `upload_workers`, `upload_retries`, `upload_method`, `upload_recursive`, `upload_force`
- Added SQLite table `auth_file_uploads` for upload status tracking and idempotent deduplication across concurrent runs
- Added `maintain-refill` mode to enforce a minimum valid-account threshold after maintenance
- Added refill and external register hook options: `min_valid_accounts`, `refill_strategy`, `auto_register`, `register_command`, `register_timeout`, `register_workdir`
- Added quota auto-disable threshold option: `quota_disable_threshold` / `--quota-disable-threshold` (`0~1`, default `0`)
- Added re-enable scope option: `reenable_scope` / `--reenable-scope` (`signal` or `managed`, default `signal`)

### Changed

- Raised default `probe_workers` from 40 to 100
- Raised default `action_workers` from 20 to 100
- Raised default `retries` from 1 to 3
- `upload` mode now exits non-zero when any file validation or upload fails
- `maintain-refill` now exits non-zero when post-maintenance valid accounts remain below threshold
- Updated docs to standardize on `cpa_warden.py` as the documented CLI entrypoint
- Quota classification now supports threshold-based disabling when remaining ratio is `<= quota_disable_threshold` (with `limit_reached` behavior unchanged as fallback)
- Pro-plan quota signal selection now falls back to primary `rate_limit` when Spark signal is incomplete (`limit_reached` unavailable)
- Probe retry behavior now retries `429` and `5xx` responses with backoff; other `4xx` fail fast
- Boolean config parsing is now strict (`true/false/1/0/yes/no/on/off`) to avoid accidental truthy-string misconfiguration
- Recovered-account classification now relies on live usage signals plus current disabled state, with `reenable_scope` to control whether auto-reenable targets all signal-recovered accounts or only tool-managed ones

## [0.1.0] - 2026-03-01

### Added

- Interactive `scan` and `maintain` workflows for local [CLIProxyAPI (CPA)](https://github.com/router-for-me/CLIProxyAPI) account operations
- External JSON configuration for CLIProxyAPI connection settings and runtime behavior
- Concurrent `wham/usage` probing through the CLIProxyAPI `api-call` endpoint
- SQLite state tracking for auth inventory and probe results
- JSON exports for invalid `401` accounts and quota-limited accounts
- Rich progress support for production runs in TTY environments
- Debug logging with full details written to a log file
- English and Simplified Chinese README files
- A contributor guide for open-source changes
- GitHub issue templates and a pull request template
- A CI workflow for dependency sync, bytecode compilation, and CLI help checks

### Changed

- Renamed the project identity from `cpa-clean` to `cpa-warden`
- Clarified account classification around `auth-files` inventory and `wham/usage` probing
- Kept production terminal output concise while preserving detailed logs in the log file
