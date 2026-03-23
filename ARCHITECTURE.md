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

## Concurrency Model

- The scheduler has two independent execution channels:
  - upload channel (`self.upload_process`)
  - maintain channel (`self.maintain_process`)
- Each channel has independent pending flags, retry counters, and retry due times.
- Command launch conditions are channel-local, so maintain can continue while upload is still running.
- Post-upload maintain is queued as an additional maintain reason, not as an upload blocking step.

## Snapshot Model

Watcher consistency is built around three snapshot files:

- `current_snapshot.txt`: current observed JSON files and metadata
- `stable_snapshot.txt`: stable snapshot after wait-window confirmation
- `last_uploaded_snapshot.txt`: uploaded baseline used for change detection

Key rule:

- On upload success, baseline is computed as the intersection of
  - uploaded in-flight snapshot, and
  - files that still exist in the current snapshot

This avoids incorrectly marking files created during upload as already uploaded.

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
- `last_uploaded_snapshot.txt`
- `current_snapshot.txt`
- `stable_snapshot.txt`
- `auto_maintain.lock`

## Failure Model

- Default behavior is fail-fast (`CONTINUE_ON_COMMAND_FAILURE=0`).
- Upload and maintain have independent retry counters.
- `--once` always exits on failure.
- Single-instance lock is enabled by default (`ALLOW_MULTI_INSTANCE=0`).

## Cleanup Model

- On successful upload, source JSON files can be deleted (`DELETE_UPLOADED_FILES_AFTER_UPLOAD=1`).
- After file deletion, empty subdirectories under `auth_dir` are pruned.
- Cleanup is best-effort with warning logs on skipped/failed paths.

## Git Hygiene Model

- Runtime state directory is ignored.
- `auth_files` keeps only `.gitkeep` tracked.
- Sensitive local config (`config.json`) is ignored.
