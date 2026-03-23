# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
- Added CLI option `--upload-names-file` (upload mode) to constrain upload candidates to a provided name set
- Added `smart_scheduler.py` policy helper for adaptive upload batching and incremental-maintain pacing decisions
- Added split runtime paths for watcher-managed command state: `MAINTAIN_DB_PATH` / `UPLOAD_DB_PATH` and `MAINTAIN_LOG_FILE` / `UPLOAD_LOG_FILE`
- Updated `start_auto_maintain_optimized.bat` profile defaults to use dedicated maintain/upload SQLite files and log files under `.auto_maintain_state`
- Fixed upload snapshot baseline handling in watcher flow to avoid marking mid-upload new files as already uploaded
- Watcher now queues a follow-up upload batch when files are detected outside the completed upload baseline
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
- Added fixed dashboard redraw and optional color/channel separators for clearer upload/maintain panel readability (`AUTO_MAINTAIN_FIXED_PANEL`, `AUTO_MAINTAIN_PANEL_COLOR`)
- Child output decoding now uses UTF-8-first fallback chain (including GB18030/CP936) to reduce mojibake in Chinese logs
- While upload is running, watcher now performs lightweight JSON/ZIP change probing and triggers immediate deep upload-check after current batch completes
- Upload baseline merge now preserves previous successful batches when current run only processes part of the queue
- Hardened snapshot generation against transient file-system races (missing/replaced files during scans)
- Upload cleanup now also prunes empty subdirectories under `auth_dir` after file deletion
- ZIP change triggering now checks signature deltas (path/size/mtime) instead of only count changes
- Added `MAINTAIN_ASSUME_YES` to control whether watcher adds `--yes` for maintain command
- Watcher failure handling now fails fast by default (`CONTINUE_ON_COMMAND_FAILURE=false`) and forces fail-fast in `--once` mode
- Replaced silent exception swallowing in critical runtime paths with warning logs
- `auto_maintain.bat` now adds launcher-level lock precheck (`auto_maintain_launcher.lock`) before process start
- Python runtime lock arbitration remains authoritative and now uses Windows file locking (`msvcrt`) on `auto_maintain.lock`
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
