from __future__ import annotations

# Channels
CHANNEL_UPLOAD = "upload"
CHANNEL_MAINTAIN = "maintain"

# Dashboard/runtime states
STATE_IDLE = "idle"
STATE_PENDING = "pending"
STATE_RUNNING = "running"
STATE_FAILED = "failed"

# Process lifecycle statuses
STATUS_SUCCESS = "success"
STATUS_SHUTDOWN = "shutdown"
STATUS_RETRY = "retry"
STATUS_FAILED = "failed"

# Progress stages
STAGE_IDLE = "idle"
STAGE_RUNNING = "running"
STAGE_PENDING = "pending"
STAGE_PENDING_FULL = "pending_full"
STAGE_PENDING_INCREMENTAL = "pending_incremental"
STAGE_DEFERRED = "deferred"
STAGE_START_FAILED = "start_failed"
STAGE_RETRY_WAIT = "retry_wait"
STAGE_FAILED = "failed"
STAGE_SCAN = "scan"
STAGE_UPLOAD = "upload"
STAGE_DONE = "done"
STAGE_PREPARE = "prepare"
STAGE_PROBE = "probe"
STAGE_DELETE = "delete"
STAGE_DISABLE = "disable"
STAGE_ENABLE = "enable"
