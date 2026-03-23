from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ActiveUploadProbeState:
    pending_source_changes: bool
    last_json_count: int
    last_zip_signature: tuple[str, ...]
    last_deep_scan_at: float


@dataclass
class ActiveUploadProbeDecision:
    state: ActiveUploadProbeState
    source_changed: bool
    changed_reasons: tuple[str, ...]
    should_log_detection: bool
    should_refresh_upload_queue: bool


def decide_active_upload_probe(
    *,
    state: ActiveUploadProbeState,
    upload_running: bool,
    current_json_count: int,
    inspect_zip_files: bool,
    current_zip_signature: tuple[str, ...] | None,
    now_monotonic: float,
    deep_scan_interval_seconds: int,
) -> ActiveUploadProbeDecision:
    if not upload_running:
        return ActiveUploadProbeDecision(
            state=state,
            source_changed=False,
            changed_reasons=tuple(),
            should_log_detection=False,
            should_refresh_upload_queue=False,
        )

    json_changed = current_json_count != state.last_json_count
    zip_changed = False
    next_zip_signature = state.last_zip_signature
    if inspect_zip_files:
        next_zip_signature = current_zip_signature or tuple()
        zip_changed = next_zip_signature != state.last_zip_signature

    if not (json_changed or zip_changed):
        return ActiveUploadProbeDecision(
            state=state,
            source_changed=False,
            changed_reasons=tuple(),
            should_log_detection=False,
            should_refresh_upload_queue=False,
        )

    reasons: list[str] = []
    if json_changed:
        reasons.append("json")
    if zip_changed:
        reasons.append("zip")

    next_state = ActiveUploadProbeState(
        pending_source_changes=True,
        last_json_count=current_json_count,
        last_zip_signature=next_zip_signature,
        last_deep_scan_at=state.last_deep_scan_at,
    )
    should_refresh = True
    if (
        deep_scan_interval_seconds > 0
        and (now_monotonic - state.last_deep_scan_at) < deep_scan_interval_seconds
    ):
        should_refresh = False
    else:
        next_state.last_deep_scan_at = now_monotonic

    return ActiveUploadProbeDecision(
        state=next_state,
        source_changed=True,
        changed_reasons=tuple(reasons),
        should_log_detection=not state.pending_source_changes,
        should_refresh_upload_queue=should_refresh,
    )
