from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StartupSeedDecision:
    should_seed_uploaded_snapshot: bool


@dataclass(frozen=True)
class StartupZipFollowUpDecision:
    should_run_upload_check: bool
    log_message: str | None


@dataclass(frozen=True)
class StartupActionPlan:
    run_startup_maintain: bool
    run_startup_upload_check: bool


def decide_startup_seed(*, last_uploaded_snapshot_exists: bool) -> StartupSeedDecision:
    return StartupSeedDecision(
        should_seed_uploaded_snapshot=not last_uploaded_snapshot_exists,
    )


def decide_startup_zip_follow_up(
    *,
    inspect_zip_files: bool,
    startup_zip_changed: bool,
) -> StartupZipFollowUpDecision:
    should_run = inspect_zip_files and startup_zip_changed
    if not should_run:
        return StartupZipFollowUpDecision(
            should_run_upload_check=False,
            log_message=None,
        )
    return StartupZipFollowUpDecision(
        should_run_upload_check=True,
        log_message="Startup ZIP scan produced changes. Queue immediate upload check.",
    )


def build_startup_action_plan(
    *,
    run_maintain_on_start: bool,
    run_upload_on_start: bool,
) -> StartupActionPlan:
    return StartupActionPlan(
        run_startup_maintain=run_maintain_on_start,
        run_startup_upload_check=run_upload_on_start,
    )
