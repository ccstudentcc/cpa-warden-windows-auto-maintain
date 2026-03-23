# Architecture

## Project Goal

This repository provides a Windows-first automation layer on top of the CPA maintenance capabilities from `cpa_warden.py`.

## Module Map

### `cpa_warden.py`

- Entry point: `main()` via `uv run python cpa_warden.py`
- Public interface: CLI modes (`scan`, `maintain`, `upload`, `maintain-refill`) and related flags
- Responsibility: core CPA API interactions, account classification, actions, and exports
- Test coverage status: currently validated through CLI checks and production usage; no dedicated unit-test module yet

### `auto_maintain.py`

- Entry point: `main()` via `uv run python auto_maintain.py`
- Public interface: environment-variable-driven scheduler/watcher plus `--once`
- Responsibility: orchestration loop, upload/maintain concurrency, snapshotting, lock control, ZIP intake, retry/fail-fast policy
- Test file: `tests/test_auto_maintain.py`

### `auto_maintain.bat`

- Entry point: double-click / shell execution on Windows
- Public interface: forwards arguments to `auto_maintain.py`
- Responsibility: runtime bootstrap with `uv` preferred and Python fallback

### `start_auto_maintain_optimized.bat`

- Entry point: operational profile launcher
- Public interface: environment profile values and pass-through args
- Responsibility: production-like defaults for interval, retry, state paths, ZIP handling, and lock behavior

## Runtime Data Flow

1. Watcher scans `auth_files` and computes snapshots.
2. When stable changes are detected, upload batch is queued.
3. Upload and maintain commands are scheduled as separate channels.
4. Upload completion updates baseline snapshot and optionally deletes uploaded files.
5. Optional post-upload maintain is queued.
6. Runtime state persists under `.auto_maintain_state`.

## State Files

Default state directory: `.auto_maintain_state`

- `cpa_warden_maintain.sqlite3`
- `cpa_warden_upload.sqlite3`
- `cpa_warden_maintain.log`
- `cpa_warden_upload.log`
- `last_uploaded_snapshot.txt`
- `current_snapshot.txt`
- `stable_snapshot.txt`
- `auto_maintain.lock`

## Failure Model

- Default behavior is fail-fast (`CONTINUE_ON_COMMAND_FAILURE=0`).
- Upload and maintain have independent retry counters.
- `--once` always exits on failure.
- Single-instance lock is enabled by default (`ALLOW_MULTI_INSTANCE=0`).

## Git Hygiene Model

- Runtime state directory is ignored.
- `auth_files` keeps only `.gitkeep` tracked.
- Sensitive local config (`config.json`) is ignored.
