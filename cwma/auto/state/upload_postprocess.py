from __future__ import annotations

from dataclasses import dataclass

from ..channel.channel_status import STAGE_IDLE, STAGE_PENDING
from .snapshots import (
    compute_pending_upload_snapshot,
    compute_uploaded_baseline,
    extract_names_from_snapshot,
)

POST_UPLOAD_PENDING_REASON = "post-upload pending files"


@dataclass
class UploadSuccessPostProcessResult:
    uploaded_baseline: list[str]
    pending_snapshot: list[str]
    queue_snapshot: list[str] | None
    queue_reason: str | None
    progress_stage: str
    uploaded_names: set[str]


def build_upload_success_postprocess(
    *,
    previous_uploaded_baseline: list[str],
    uploaded_snapshot: list[str],
    current_snapshot: list[str],
) -> UploadSuccessPostProcessResult:
    uploaded_baseline = compute_uploaded_baseline(
        existing_baseline=previous_uploaded_baseline,
        uploaded_snapshot=uploaded_snapshot,
        current_snapshot=current_snapshot,
    )
    pending_snapshot = compute_pending_upload_snapshot(
        current_snapshot=current_snapshot,
        uploaded_baseline=uploaded_baseline,
    )
    queue_snapshot = pending_snapshot if pending_snapshot else None
    queue_reason = POST_UPLOAD_PENDING_REASON if pending_snapshot else None
    progress_stage = STAGE_PENDING if pending_snapshot else STAGE_IDLE
    uploaded_names = extract_names_from_snapshot(uploaded_snapshot)
    return UploadSuccessPostProcessResult(
        uploaded_baseline=uploaded_baseline,
        pending_snapshot=pending_snapshot,
        queue_snapshot=queue_snapshot,
        queue_reason=queue_reason,
        progress_stage=progress_stage,
        uploaded_names=uploaded_names,
    )
