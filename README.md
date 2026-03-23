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
- Follow-up commits after that baseline are focused on Windows automation, scheduling, and runtime hardening.

See [NOTICE](NOTICE) for attribution details.

## Project Focus

The core value of this project is not replacing `cpa_warden.py`, but orchestrating it safely and continuously on Windows:

- watch `auth_files` and trigger uploads when files are stable
- run scheduled maintenance without waiting for upload batches to finish
- keep maintain/upload runtime state isolated with split DB/log files
- handle ZIP intake for JSON auth files (Bandizip + Windows fallback)
  - recursively detects JSON entries even when nested in zip subfolders
- clean uploaded files and prune empty subdirectories
- display terminal panel snapshots for upload/maintain channels, including queue/backlog scope
- run unattended with fail-fast defaults and explicit retries

## Improvement Highlights

Compared with the baseline derivative commit (`f3778f4`), current watcher behavior includes:

1. Parallel scheduler channels for `upload` and `maintain` so maintenance is no longer blocked by long upload batches.
   - Scheduled maintenance (`MAINTAIN_INTERVAL_SECONDS`) is full-scope.
   - Post-upload maintenance is incremental-scope based on uploaded auth names.
2. Upload queue now supports batch slicing (`UPLOAD_BATCH_SIZE`) and per-batch scoped upload via `--upload-names-file`.
   - Each upload batch completes faster.
   - Incremental maintain can start earlier for already uploaded batches.
3. Smart scheduling policy layer (`smart_scheduler.py`) now adapts runtime behavior across low/high-frequency workloads:
   - adaptive upload batch size under high backlog pressure;
   - adaptive incremental-maintain batch sizing under runtime contention/backlog;
   - incremental maintain cooldown to avoid over-frequent maintain churn;
   - full-maintain proximity guard to avoid starting incremental maintain right before scheduled full maintain.
4. Split runtime persistence paths for maintain/upload (`MAINTAIN_DB_PATH` + `UPLOAD_DB_PATH`) and split logs (`MAINTAIN_LOG_FILE` + `UPLOAD_LOG_FILE`).
5. Upload baseline correctness fix: partial-batch success merges with existing uploaded baseline instead of overwriting it.
6. Snapshot robustness improvements for transient file-system races (disappearing/replaced files while scanning).
7. Post-upload follow-up batch queueing when files are outside the completed upload baseline.
8. ZIP change detection by signature delta (path/size/mtime), not only ZIP file count.
9. Upload cleanup now prunes empty directories under `auth_dir`.
10. Fail-fast default policy for command failures, with explicit retry controls and stricter `--once` semantics.
11. `MAINTAIN_ASSUME_YES` support for unattended maintain executions.
12. Python-side single-instance lock arbitration to reduce duplicate watcher runs.
13. Terminal panel snapshots now expose per-channel queue visibility (`queue_files`, `queue_batches`, `queue_full`, `queue_incremental`, retries, and next full-maintain wait).
14. While an upload batch is running, watcher performs lightweight JSON/ZIP change probes and schedules an immediate deep upload check right after that batch completes.
15. Runtime dashboard supports fixed panel redraw with optional color and channel separators to avoid flicker from mixed parallel progress output.
16. Single-instance safety is now guarded in two layers on Windows:
   - launcher lock file (`auto_maintain_launcher.lock`) in `auto_maintain.bat`;
   - Python runtime lock (`auto_maintain.lock`) with Windows file lock (`msvcrt`) in `auto_maintain.py`.

## Execution Logic (Watcher)

`auto_maintain.py` loop behavior is:

1. Build initial snapshots and initialize `last_uploaded_snapshot` if absent.
2. Inspect ZIP files (optional) and enqueue upload-check if ZIP extraction produced JSON changes.
3. Queue startup maintain/upload based on profile flags.
4. Start maintain and upload commands independently when each channel is idle.
   - Upload channel consumes pending files in serial batches (`UPLOAD_BATCH_SIZE`).
   - Each upload batch uses `--upload-names-file` to scope command-side upload candidates.
5. Poll command exits independently:
   - while upload process is active, watcher runs lightweight JSON-count/ZIP-signature probes and periodic deep queue refresh;
   - upload success updates snapshots/baseline and optionally deletes uploaded source files;
   - maintain success clears maintain retry state.
6. On upload completion, optionally queue post-upload maintain.
   - if source changes were detected during active upload, watcher triggers an immediate forced deep upload check (`force_deep_scan=True`) before normal post-upload flow.
   - The queued post-upload maintain uses `--maintain-names-file` to scope actions to names from the completed upload batch.
7. Apply independent retry windows for maintain/upload failures.
8. In `--once` mode, exit only after all running/pending work finishes; exit non-zero on failures.

## Key Components

- `cwma/apps/cpa_warden.py`: upstream-compatible CPA scanner / maintainer / uploader CLI implementation
- `cwma/auto/app.py`: Windows-oriented scheduler and watcher orchestrator host
- `cwma/apps/auto_maintain.py`: compatibility adapter for legacy package import paths
- `cwma/scheduler/smart_scheduler.py`: scheduling policy model for upload/maintain batch decisions
- `cwma/common/config_parsing.py` + `cwma/auto/config.py`: shared config parsing utilities plus watcher settings loader/model
- `cwma/auto/snapshots.py`: pure snapshot helpers extracted from runtime orchestration
- `cwma/auto/locking.py`: single-instance lock backend (including Windows file-lock behavior) extracted from watcher runtime
- `cwma/auto/dashboard.py`: terminal panel formatting/color helpers extracted as pure rendering helpers
- `cwma/auto/process_output.py`: child-process output decoding/alert filtering/environment helpers extracted for process supervision reuse
- `cwma/auto/progress_parser.py`: child log-line to progress-state parsing rules extracted as reusable pure parser
- `cwma/auto/output_pump.py`: child-process output append/pump thread helpers extracted for process supervision reuse
- `cwma/auto/zip_intake.py`: ZIP path/signature discovery and extraction backend helpers extracted for intake pipeline reuse
- `cwma/auto/process_runner.py`: child process launch/terminate helpers extracted for command supervision reuse
- `auto_maintain.py` / `cpa_warden.py` / `smart_scheduler.py`: root-level compatibility entrypoints (existing scripts/commands keep working)
- `auto_maintain.bat`: launcher with `uv -> python` fallback
- `start_auto_maintain_optimized.bat`: tuned profile for production-like unattended runs
- `auto_maintain.config.example.json`: watcher profile template
- `tests/test_auto_maintain.py`: regression tests for scheduling and file lifecycle behavior
- `tests/test_auto_modules.py`: module-level tests for extracted `config` / `locking` / `dashboard` / `process_output` / `progress_parser` / `output_pump` / `zip_intake` / `process_runner` helpers

## Requirements

- Windows 10/11
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended)
- Bandizip (optional, recommended for ZIP-heavy intake)

## Quick Start

1. Install dependencies.

```bash
uv sync
```

2. Prepare configuration.

```bash
copy config.example.json config.json
```

Set at least:

- `base_url`
- `token`

3. Prepare watcher profile configuration.

```bash
copy auto_maintain.config.example.json auto_maintain.config.json
```

4. Keep `auth_files` as an input directory placeholder.

- This repo tracks only `auth_files/.gitkeep`.
- Runtime JSON/ZIP files under `auth_files` are ignored by git.

5. Start the optimized watcher profile.

```bat
start_auto_maintain_optimized.bat
```

## Runtime State And Ignore Rules

- `.auto_maintain_state/` is runtime-only and ignored by git.
- `auth_files/*` is ignored except `auth_files/.gitkeep`.
- `auto_maintain.config.json` is local runtime profile and ignored by git.
- Recommended runtime artifacts stay outside commits:
- `.auto_maintain_state/cpa_warden_maintain.sqlite3`
- `.auto_maintain_state/cpa_warden_upload.sqlite3`
- `.auto_maintain_state/cpa_warden_maintain.log`
- `.auto_maintain_state/cpa_warden_upload.log`
- `.auto_maintain_state/maintain_command_output.log`
- `.auto_maintain_state/upload_command_output.log`
- `.auto_maintain_state/maintain_names_scope.txt`
- `.auto_maintain_state/upload_names_scope.txt`
- `.auto_maintain_state/last_uploaded_snapshot.txt`
- `.auto_maintain_state/current_snapshot.txt`
- `.auto_maintain_state/stable_snapshot.txt`

## Operational Defaults In Optimized Launcher

`start_auto_maintain_optimized.bat` now reads `auto_maintain.config.json` (or creates it from `auto_maintain.config.example.json` on first run).

Current template defaults (`auto_maintain.config.example.json`):

- maintain interval: `2400s`
- watch interval: `15s`
- upload stable wait: `5s`
- upload batch size: `100`
- smart scheduler: enabled
- adaptive upload batching: enabled
- high backlog threshold: `400`
- high backlog batch size: `300`
- incremental maintain cooldown: `20s`
- full maintain guard: `90s`
- deep scan interval: `120` loops
- maintain after upload: enabled
- delete uploaded source JSON: enabled
- inspect and auto-extract ZIP: enabled
- single-instance lock: enabled
- fail-fast on command failure: enabled

It also keeps env override capability for all watcher settings.

## Watcher Config Parameters

`auto_maintain.config.json` keys and meanings:

| Key | Default | Description |
| --- | --- | --- |
| `auth_dir` | `./auth_files` | Upload watch directory. JSON/ZIP intake happens here. |
| `config_path` | `./config.json` | `cpa_warden.py` config file path. |
| `state_dir` | `./.auto_maintain_state` | Runtime state directory for lock/snapshots/log/db. |
| `maintain_db_path` | `./.auto_maintain_state/cpa_warden_maintain.sqlite3` | SQLite path for maintain channel. |
| `upload_db_path` | `./.auto_maintain_state/cpa_warden_upload.sqlite3` | SQLite path for upload channel. |
| `maintain_log_file` | `./.auto_maintain_state/cpa_warden_maintain.log` | Log file for maintain channel. |
| `upload_log_file` | `./.auto_maintain_state/cpa_warden_upload.log` | Log file for upload channel. |
| `maintain_interval_seconds` | `2400` | Full maintain schedule interval. |
| `watch_interval_seconds` | `15` | Main loop poll interval for watcher scheduling. |
| `upload_stable_wait_seconds` | `5` | Stability wait before enqueuing an upload batch. |
| `upload_batch_size` | `100` | Max JSON files per upload command batch; pending files continue in next serial upload batch. |
| `smart_schedule_enabled` | `true` | Enable smart scheduling policy layer for adaptive/balanced behavior. |
| `adaptive_upload_batching` | `true` | Allow upload batch size to expand under high backlog pressure. |
| `upload_high_backlog_threshold` | `400` | Pending-upload threshold that activates high-backlog upload batching rules. |
| `upload_high_backlog_batch_size` | `300` | Target upload batch size when high-backlog batching is active. |
| `adaptive_maintain_batching` | `true` | Enable adaptive incremental-maintain batch sizing. |
| `incremental_maintain_batch_size` | `120` | Base batch size for each incremental maintain run. |
| `maintain_high_backlog_threshold` | `300` | Incremental-maintain backlog threshold for high-backlog slicing rules. |
| `maintain_high_backlog_batch_size` | `220` | Target incremental-maintain batch size in high-backlog mode. |
| `incremental_maintain_min_interval_seconds` | `20` | Minimum interval between starting incremental maintain runs. |
| `incremental_maintain_full_guard_seconds` | `90` | Defer incremental maintain when scheduled full maintain is due within this window. |
| `deep_scan_interval_loops` | `120` | Force deep re-scan every N loops even without obvious change. |
| `active_probe_interval_seconds` | `2` | Probe interval while upload/maintain is running (faster than idle watch interval). |
| `active_upload_deep_scan_interval_seconds` | `2` | Minimum interval between active-upload deep queue refresh scans. |
| `run_maintain_on_start` | `true` | Whether to queue a maintain run on startup. |
| `run_upload_on_start` | `true` | Whether to evaluate upload changes on startup. |
| `run_maintain_after_upload` | `true` | Whether to queue post-upload maintain (incremental scope). |
| `maintain_assume_yes` | `true` | Whether maintain command adds `--yes` for unattended execution. |
| `delete_uploaded_files_after_upload` | `true` | Whether uploaded source JSON files are deleted afterward. |
| `maintain_retry_count` | `1` | Retry count for maintain command failures. |
| `upload_retry_count` | `1` | Retry count for upload command failures. |
| `command_retry_delay_seconds` | `20` | Delay between retry attempts. |
| `continue_on_command_failure` | `false` | Keep watcher alive on command failure when true; otherwise fail-fast. |
| `allow_multi_instance` | `false` | Allow multiple watcher instances on same state dir when true. |
| `inspect_zip_files` | `true` | Enable ZIP detection in watch directory. |
| `auto_extract_zip_json` | `true` | Auto-extract ZIP archives containing JSON. |
| `delete_zip_after_extract` | `true` | Delete ZIP source after successful extraction. |
| `bandizip_path` | `D:\\Bandizp\\Bandizip.exe` | Bandizip executable path for extraction. |
| `bandizip_timeout_seconds` | `120` | Timeout for each Bandizip extraction call. |
| `use_windows_zip_fallback` | `true` | Use Windows built-in extraction if Bandizip fails/unavailable. |

Notes:

- `maintain_interval_seconds` controls full maintain only.
- Post-upload maintain is incremental by uploaded names from each completed upload batch.
- Smart scheduler defaults are tuned to keep low-frequency responsiveness while reducing high-frequency churn.
- File/path settings can use relative paths (resolved from repo root).

## Useful Environment Variables

Main variables consumed by `auto_maintain.py`:

- `WATCH_CONFIG_PATH`
- `AUTH_DIR`, `CONFIG_PATH`, `STATE_DIR`
- `MAINTAIN_DB_PATH`, `UPLOAD_DB_PATH`
- `MAINTAIN_LOG_FILE`, `UPLOAD_LOG_FILE`
- `MAINTAIN_INTERVAL_SECONDS`, `WATCH_INTERVAL_SECONDS`
- `UPLOAD_STABLE_WAIT_SECONDS`, `UPLOAD_BATCH_SIZE`, `DEEP_SCAN_INTERVAL_LOOPS`
- `SMART_SCHEDULE_ENABLED`, `ADAPTIVE_UPLOAD_BATCHING`
- `UPLOAD_HIGH_BACKLOG_THRESHOLD`, `UPLOAD_HIGH_BACKLOG_BATCH_SIZE`
- `ADAPTIVE_MAINTAIN_BATCHING`
- `INCREMENTAL_MAINTAIN_BATCH_SIZE`, `MAINTAIN_HIGH_BACKLOG_THRESHOLD`, `MAINTAIN_HIGH_BACKLOG_BATCH_SIZE`
- `INCREMENTAL_MAINTAIN_MIN_INTERVAL_SECONDS`, `INCREMENTAL_MAINTAIN_FULL_GUARD_SECONDS`
- `ACTIVE_PROBE_INTERVAL_SECONDS`, `ACTIVE_UPLOAD_DEEP_SCAN_INTERVAL_SECONDS`
- `RUN_MAINTAIN_ON_START`, `RUN_UPLOAD_ON_START`, `RUN_MAINTAIN_AFTER_UPLOAD`
- `MAINTAIN_ASSUME_YES`
- `MAINTAIN_RETRY_COUNT`, `UPLOAD_RETRY_COUNT`, `COMMAND_RETRY_DELAY_SECONDS`
- `CONTINUE_ON_COMMAND_FAILURE`, `ALLOW_MULTI_INSTANCE`
- `INSPECT_ZIP_FILES`, `AUTO_EXTRACT_ZIP_JSON`, `DELETE_ZIP_AFTER_EXTRACT`
- `BANDIZIP_PATH`, `BANDIZIP_TIMEOUT_SECONDS`, `USE_WINDOWS_ZIP_FALLBACK`
- `AUTO_MAINTAIN_FIXED_PANEL`, `AUTO_MAINTAIN_PANEL_COLOR` (terminal dashboard rendering toggles)

Precedence:

- Environment variables
- `--watch-config` / `WATCH_CONFIG_PATH` JSON file
- Built-in defaults

One-cycle dry run:

```bash
uv run python auto_maintain.py --watch-config ./auto_maintain.config.json --once
```

## Core CLI Compatibility

`cpa_warden.py` behavior remains upstream-compatible. Common commands:

```bash
uv run python cpa_warden.py --mode scan
uv run python cpa_warden.py --mode maintain --yes
uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive
uv run python cpa_warden.py --mode maintain-refill --min-valid-accounts 200 --upload-dir ./auth_files
```

## Validation

```bash
uv run python -m py_compile cpa_warden.py auto_maintain.py clean_codex_accounts.py
uv run python cpa_warden.py --help
uv run python auto_maintain.py --help
uv run python -m unittest -v tests/test_auto_maintain.py
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
