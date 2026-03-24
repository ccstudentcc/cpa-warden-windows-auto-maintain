"""Public runtime adapter API for auto-maintain orchestration."""

from .channel_runtime import (
    MaintainPollFlowResult,
    MaintainStartFlowResult,
    UploadPollFlowResult,
    UploadStartFlowResult,
    poll_maintain_channel,
    poll_upload_channel,
    start_maintain_channel,
    start_upload_channel,
)
from .shutdown_runtime import (
    ShutdownRuntimeState,
    current_loop_sleep_seconds,
    request_shutdown,
    sleep_between_watch_cycles,
    sleep_with_shutdown,
)
from .startup_runtime import StartupRuntimeDeps, StartupRuntimeResult, StartupRuntimeState, run_startup_cycle
from .upload_scan_runtime import run_active_upload_probe_cycle, run_upload_scan_cycle
from .watch_runtime import WatchRuntimeDeps, WatchRuntimeResult, WatchRuntimeState, run_watch_iteration
from .maintain_pipeline_runtime import (
    STEP_STATUS_FAILED,
    STEP_STATUS_RETRY,
    STEP_STATUS_SUCCESS,
    MaintainPipelineCycleResult,
    MaintainPipelineStepOutcome,
    run_maintain_pipeline_cycle,
)

__all__ = [
    "MaintainStartFlowResult",
    "UploadStartFlowResult",
    "MaintainPollFlowResult",
    "UploadPollFlowResult",
    "start_maintain_channel",
    "start_upload_channel",
    "poll_maintain_channel",
    "poll_upload_channel",
    "StartupRuntimeState",
    "StartupRuntimeDeps",
    "StartupRuntimeResult",
    "run_startup_cycle",
    "WatchRuntimeState",
    "WatchRuntimeDeps",
    "WatchRuntimeResult",
    "run_watch_iteration",
    "ShutdownRuntimeState",
    "request_shutdown",
    "sleep_with_shutdown",
    "current_loop_sleep_seconds",
    "sleep_between_watch_cycles",
    "run_upload_scan_cycle",
    "run_active_upload_probe_cycle",
    "STEP_STATUS_SUCCESS",
    "STEP_STATUS_RETRY",
    "STEP_STATUS_FAILED",
    "MaintainPipelineStepOutcome",
    "MaintainPipelineCycleResult",
    "run_maintain_pipeline_cycle",
]
