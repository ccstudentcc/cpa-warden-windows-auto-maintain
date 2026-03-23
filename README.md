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
- clean uploaded files and prune empty subdirectories
- run unattended with fail-fast defaults and explicit retries

## Improvement Highlights

Compared with the baseline derivative commit (`f3778f4`), current watcher behavior includes:

1. Parallel scheduler channels for `upload` and `maintain` so maintenance is no longer blocked by long upload batches.
   - Scheduled maintenance (`MAINTAIN_INTERVAL_SECONDS`) is full-scope.
   - Post-upload maintenance is incremental-scope based on uploaded auth names.
2. Split runtime persistence paths for maintain/upload (`MAINTAIN_DB_PATH` + `UPLOAD_DB_PATH`) and split logs (`MAINTAIN_LOG_FILE` + `UPLOAD_LOG_FILE`).
3. Upload baseline correctness fix: files created during an in-flight upload are not incorrectly marked as already uploaded.
4. Snapshot robustness improvements for transient file-system races (disappearing/replaced files while scanning).
5. Post-upload follow-up batch queueing when files are outside the completed upload baseline.
6. ZIP change detection by signature delta (path/size/mtime), not only ZIP file count.
7. Upload cleanup now prunes empty directories under `auth_dir`.
8. Fail-fast default policy for command failures, with explicit retry controls and stricter `--once` semantics.
9. `MAINTAIN_ASSUME_YES` support for unattended maintain executions.
10. Python-side single-instance lock arbitration to reduce duplicate watcher runs.

## Execution Logic (Watcher)

`auto_maintain.py` loop behavior is:

1. Build initial snapshots and initialize `last_uploaded_snapshot` if absent.
2. Inspect ZIP files (optional) and enqueue upload-check if ZIP extraction produced JSON changes.
3. Queue startup maintain/upload based on profile flags.
4. Start maintain and upload commands independently when each channel is idle.
5. Poll command exits independently:
   - upload success updates snapshots/baseline and optionally deletes uploaded source files;
   - maintain success clears maintain retry state.
6. On upload completion, optionally queue post-upload maintain.
   - The queued post-upload maintain uses `--maintain-names-file` to scope actions to uploaded names.
7. Apply independent retry windows for maintain/upload failures.
8. In `--once` mode, exit only after all running/pending work finishes; exit non-zero on failures.

## Key Components

- `cpa_warden.py`: upstream-compatible CPA scanner / maintainer / uploader CLI
- `auto_maintain.py`: Windows-oriented scheduler and watcher
- `auto_maintain.bat`: launcher with `uv -> python` fallback
- `start_auto_maintain_optimized.bat`: tuned profile for production-like unattended runs
- `auto_maintain.config.example.json`: watcher profile template
- `tests/test_auto_maintain.py`: regression tests for scheduling and file lifecycle behavior

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
- `.auto_maintain_state/maintain_names_scope.txt`
- `.auto_maintain_state/last_uploaded_snapshot.txt`
- `.auto_maintain_state/current_snapshot.txt`
- `.auto_maintain_state/stable_snapshot.txt`

## Operational Defaults In Optimized Launcher

`start_auto_maintain_optimized.bat` now reads `auto_maintain.config.json` (or creates it from `auto_maintain.config.example.json` on first run).

Current template defaults (`auto_maintain.config.example.json`):

- maintain interval: `2400s`
- watch interval: `15s`
- upload stable wait: `5s`
- deep scan interval: `120` loops
- maintain after upload: enabled
- delete uploaded source JSON: enabled
- inspect and auto-extract ZIP: enabled
- single-instance lock: enabled
- fail-fast on command failure: enabled

It also keeps env override capability for all watcher settings.

## Useful Environment Variables

Main variables consumed by `auto_maintain.py`:

- `WATCH_CONFIG_PATH`
- `AUTH_DIR`, `CONFIG_PATH`, `STATE_DIR`
- `MAINTAIN_DB_PATH`, `UPLOAD_DB_PATH`
- `MAINTAIN_LOG_FILE`, `UPLOAD_LOG_FILE`
- `MAINTAIN_INTERVAL_SECONDS`, `WATCH_INTERVAL_SECONDS`
- `UPLOAD_STABLE_WAIT_SECONDS`, `DEEP_SCAN_INTERVAL_LOOPS`
- `RUN_MAINTAIN_ON_START`, `RUN_UPLOAD_ON_START`, `RUN_MAINTAIN_AFTER_UPLOAD`
- `MAINTAIN_ASSUME_YES`
- `MAINTAIN_RETRY_COUNT`, `UPLOAD_RETRY_COUNT`, `COMMAND_RETRY_DELAY_SECONDS`
- `CONTINUE_ON_COMMAND_FAILURE`, `ALLOW_MULTI_INSTANCE`
- `INSPECT_ZIP_FILES`, `AUTO_EXTRACT_ZIP_JSON`, `DELETE_ZIP_AFTER_EXTRACT`
- `BANDIZIP_PATH`, `BANDIZIP_TIMEOUT_SECONDS`, `USE_WINDOWS_ZIP_FALLBACK`

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
