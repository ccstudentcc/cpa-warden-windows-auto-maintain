from __future__ import annotations

from dataclasses import dataclass


@dataclass
class UploadQueueState:
    pending_snapshot: list[str] | None
    pending_reason: str | None
    pending_retry: bool
    inflight_snapshot: list[str] | None
    attempt: int
    retry_due_at: float


@dataclass
class UploadStartDecision:
    state: UploadQueueState
    batch: list[str]
    can_start: bool
    waiting_retry: bool


@dataclass
class UploadMergeResult:
    state: UploadQueueState
    merged_pending_snapshot: list[str]


def parse_upload_snapshot_row(row: str) -> tuple[str, int, int] | None:
    parts = row.rsplit("|", 2)
    if len(parts) != 3:
        return None
    path, size_text, mtime_text = parts
    try:
        size = int(size_text)
        mtime_ns = int(mtime_text)
    except ValueError:
        return None
    return path, size, mtime_ns


def coalesce_upload_snapshot_rows(rows: list[str]) -> list[str]:
    merged_rows: list[str] = []
    path_to_index: dict[str, int] = {}
    raw_row_to_index: dict[str, int] = {}
    for row in rows:
        parsed = parse_upload_snapshot_row(row)
        if parsed is None:
            existing = raw_row_to_index.get(row)
            if existing is None:
                raw_row_to_index[row] = len(merged_rows)
                merged_rows.append(row)
                continue
            merged_rows[existing] = row
            continue

        path = parsed[0]
        existing = path_to_index.get(path)
        if existing is None:
            path_to_index[path] = len(merged_rows)
            merged_rows.append(row)
            continue
        # Last-writer-wins for the same path to avoid stale duplicate versions in pending queue.
        merged_rows[existing] = row
    return merged_rows


def clear_upload_queue_state(state: UploadQueueState) -> UploadQueueState:
    return UploadQueueState(
        pending_snapshot=None,
        pending_reason=None,
        pending_retry=False,
        inflight_snapshot=None,
        attempt=0,
        retry_due_at=0.0,
    )


def mark_upload_retry(
    *,
    state: UploadQueueState,
    now_monotonic: float,
    retry_delay_seconds: int,
) -> UploadQueueState:
    return UploadQueueState(
        pending_snapshot=state.pending_snapshot,
        pending_reason=state.pending_reason,
        pending_retry=True,
        inflight_snapshot=state.inflight_snapshot,
        attempt=state.attempt,
        retry_due_at=now_monotonic + retry_delay_seconds,
    )


def mark_upload_success(state: UploadQueueState) -> UploadQueueState:
    return UploadQueueState(
        pending_snapshot=state.pending_snapshot,
        pending_reason=state.pending_reason,
        pending_retry=False,
        inflight_snapshot=None,
        attempt=0,
        retry_due_at=0.0,
    )


def mark_upload_terminal_failure(state: UploadQueueState) -> UploadQueueState:
    return clear_upload_queue_state(state)


def mark_upload_no_changes(
    *,
    state: UploadQueueState,
    preserve_retry_state: bool,
) -> UploadQueueState:
    if preserve_retry_state:
        return state
    return UploadQueueState(
        pending_snapshot=state.pending_snapshot,
        pending_reason=state.pending_reason,
        pending_retry=False,
        inflight_snapshot=state.inflight_snapshot,
        attempt=state.attempt,
        retry_due_at=state.retry_due_at,
    )


def mark_upload_no_pending_discovered(
    *,
    state: UploadQueueState,
    preserve_retry_state: bool,
) -> UploadQueueState:
    next_reason = state.pending_reason
    if state.pending_snapshot is None:
        next_reason = None

    if preserve_retry_state:
        return UploadQueueState(
            pending_snapshot=state.pending_snapshot,
            pending_reason=next_reason,
            pending_retry=state.pending_retry,
            inflight_snapshot=state.inflight_snapshot,
            attempt=state.attempt,
            retry_due_at=state.retry_due_at,
        )

    return UploadQueueState(
        pending_snapshot=state.pending_snapshot,
        pending_reason=next_reason,
        pending_retry=False,
        inflight_snapshot=state.inflight_snapshot,
        attempt=0,
        retry_due_at=0.0,
    )


def merge_pending_upload_snapshot(
    *,
    state: UploadQueueState,
    discovered_pending_snapshot: list[str],
    queue_reason: str,
    preserve_retry_state: bool,
) -> UploadMergeResult:
    merged_pending = coalesce_upload_snapshot_rows(
        list(state.pending_snapshot or []) + list(discovered_pending_snapshot)
    )
    attempt = state.attempt
    retry_due_at = state.retry_due_at
    if (not state.pending_retry) and (not preserve_retry_state):
        attempt = 0
        retry_due_at = 0.0
    next_state = UploadQueueState(
        pending_snapshot=merged_pending,
        pending_reason=queue_reason,
        pending_retry=state.pending_retry,
        inflight_snapshot=state.inflight_snapshot,
        attempt=attempt,
        retry_due_at=retry_due_at,
    )
    return UploadMergeResult(
        state=next_state,
        merged_pending_snapshot=merged_pending,
    )


def decide_upload_start(
    *,
    state: UploadQueueState,
    now_monotonic: float,
    batch_size: int,
) -> UploadStartDecision:
    if state.pending_snapshot is None:
        return UploadStartDecision(
            state=state,
            batch=[],
            can_start=False,
            waiting_retry=False,
        )

    if not state.pending_snapshot:
        cleared = clear_upload_queue_state(state)
        return UploadStartDecision(
            state=cleared,
            batch=[],
            can_start=False,
            waiting_retry=False,
        )

    if now_monotonic < state.retry_due_at:
        return UploadStartDecision(
            state=state,
            batch=[],
            can_start=False,
            waiting_retry=True,
        )

    if state.pending_retry and state.inflight_snapshot is not None:
        batch = list(state.inflight_snapshot)
    else:
        batch = list(state.pending_snapshot[:batch_size])

    if not batch:
        cleared = clear_upload_queue_state(state)
        return UploadStartDecision(
            state=cleared,
            batch=[],
            can_start=False,
            waiting_retry=False,
        )

    next_state = UploadQueueState(
        pending_snapshot=state.pending_snapshot,
        pending_reason=state.pending_reason,
        pending_retry=state.pending_retry,
        inflight_snapshot=list(batch),
        attempt=state.attempt,
        retry_due_at=state.retry_due_at,
    )
    return UploadStartDecision(
        state=next_state,
        batch=batch,
        can_start=True,
        waiting_retry=False,
    )
