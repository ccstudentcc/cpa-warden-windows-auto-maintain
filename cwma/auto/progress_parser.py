from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

from .channel_status import (
    CHANNEL_MAINTAIN,
    CHANNEL_UPLOAD,
    STAGE_DELETE,
    STAGE_DISABLE,
    STAGE_DONE,
    STAGE_ENABLE,
    STAGE_PREPARE,
    STAGE_PROBE,
    STAGE_SCAN,
    STAGE_UPLOAD,
)


@dataclass(frozen=True)
class ProgressUpdate:
    channel: str
    stage: str | None = None
    done: int | None = None
    total: int | None = None
    force_render: bool = False


@dataclass(frozen=True)
class ProgressParseResult:
    updates: tuple[ProgressUpdate, ...] = ()
    should_log_alert: bool = False


_UPLOAD_PROGRESS_REGEX = re.compile(r"上传进度:\s*(\d+)\s*/\s*(\d+)")
_UPLOAD_ACTUAL_TOTAL_REGEX = re.compile(r"需要实际上传数:\s*(\d+)")
_UPLOAD_CANDIDATE_TOTAL_REGEX = re.compile(r"上传候选文件数:\s*(\d+)")
_UPLOAD_SUCCESS_REGEX = re.compile(r"上传成功:\s*(\d+)")

_MAINTAIN_PROGRESS_REGEX = re.compile(r"(探测|删除|禁用|启用)进度:\s*(\d+)\s*/\s*(\d+)")
_MAINTAIN_PROBE_CANDIDATES_REGEX = re.compile(r"开始并发探测.*candidates=(\d+)")
_MAINTAIN_FILTERED_ACCOUNTS_REGEX = re.compile(r"符合过滤条件账号数:\s*(\d+)")
_MAINTAIN_PENDING_DELETE_REGEX = re.compile(r"待删除 401 账号:\s*(\d+)")
_MAINTAIN_PENDING_DISABLE_REGEX = re.compile(r"待禁用限额账号:\s*(\d+)")
_MAINTAIN_PENDING_ENABLE_REGEX = re.compile(r"待恢复启用账号:\s*(\d+)")

_MAINTAIN_PROGRESS_STAGE_MAP = {
    "探测": STAGE_PROBE,
    "删除": STAGE_DELETE,
    "禁用": STAGE_DISABLE,
    "启用": STAGE_ENABLE,
}


def parse_progress_line(
    *,
    channel: str,
    text: str,
    current_upload_total: int,
    should_log_alert_line: Callable[[str], bool],
) -> ProgressParseResult:
    if not text:
        return ProgressParseResult()

    if channel == CHANNEL_UPLOAD:
        if "开始上传认证文件" in text:
            return ProgressParseResult(
                updates=(ProgressUpdate(CHANNEL_UPLOAD, stage=STAGE_SCAN, done=0, total=0, force_render=True),)
            )
        match = _UPLOAD_PROGRESS_REGEX.search(text)
        if match:
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_UPLOAD,
                        stage=STAGE_UPLOAD,
                        done=int(match.group(1)),
                        total=int(match.group(2)),
                    ),
                )
            )
        actual_total = _UPLOAD_ACTUAL_TOTAL_REGEX.search(text)
        if actual_total:
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_UPLOAD,
                        stage=STAGE_UPLOAD,
                        done=0,
                        total=int(actual_total.group(1)),
                        force_render=True,
                    ),
                )
            )
        candidate_total = _UPLOAD_CANDIDATE_TOTAL_REGEX.search(text)
        if candidate_total:
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_UPLOAD,
                        stage=STAGE_SCAN,
                        done=0,
                        total=int(candidate_total.group(1)),
                    ),
                )
            )
        uploaded_count = _UPLOAD_SUCCESS_REGEX.search(text)
        if uploaded_count:
            done = int(uploaded_count.group(1))
            if current_upload_total > 0:
                return ProgressParseResult(
                    updates=(ProgressUpdate(CHANNEL_UPLOAD, stage=STAGE_UPLOAD, done=done, total=current_upload_total),)
                )
            return ProgressParseResult(updates=(ProgressUpdate(CHANNEL_UPLOAD, stage=STAGE_UPLOAD, done=done),))
        if "上传流程完成" in text:
            return ProgressParseResult(
                updates=(ProgressUpdate(CHANNEL_UPLOAD, stage=STAGE_DONE, force_render=True),)
            )
    else:
        if "开始维护:" in text:
            return ProgressParseResult(
                updates=(ProgressUpdate(CHANNEL_MAINTAIN, stage=STAGE_PREPARE, done=0, total=0, force_render=True),)
            )
        probe_match = _MAINTAIN_PROBE_CANDIDATES_REGEX.search(text)
        if probe_match:
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_MAINTAIN,
                        stage=STAGE_PROBE,
                        done=0,
                        total=int(probe_match.group(1)),
                        force_render=True,
                    ),
                )
            )
        filtered_accounts = _MAINTAIN_FILTERED_ACCOUNTS_REGEX.search(text)
        if filtered_accounts:
            total = int(filtered_accounts.group(1))
            return ProgressParseResult(
                updates=(ProgressUpdate(CHANNEL_MAINTAIN, stage=STAGE_PROBE, done=total, total=total),)
            )

        maintain_match = _MAINTAIN_PROGRESS_REGEX.search(text)
        if maintain_match:
            stage_cn = maintain_match.group(1)
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_MAINTAIN,
                        stage=_MAINTAIN_PROGRESS_STAGE_MAP.get(stage_cn, stage_cn),
                        done=int(maintain_match.group(2)),
                        total=int(maintain_match.group(3)),
                    ),
                )
            )

        pending_delete = _MAINTAIN_PENDING_DELETE_REGEX.search(text)
        if pending_delete:
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_MAINTAIN,
                        stage=STAGE_DELETE,
                        done=0,
                        total=int(pending_delete.group(1)),
                        force_render=True,
                    ),
                )
            )

        pending_disable = _MAINTAIN_PENDING_DISABLE_REGEX.search(text)
        if pending_disable:
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_MAINTAIN,
                        stage=STAGE_DISABLE,
                        done=0,
                        total=int(pending_disable.group(1)),
                        force_render=True,
                    ),
                )
            )

        pending_enable = _MAINTAIN_PENDING_ENABLE_REGEX.search(text)
        if pending_enable:
            return ProgressParseResult(
                updates=(
                    ProgressUpdate(
                        CHANNEL_MAINTAIN,
                        stage=STAGE_ENABLE,
                        done=0,
                        total=int(pending_enable.group(1)),
                        force_render=True,
                    ),
                )
            )
        if "维护完成" in text:
            return ProgressParseResult(
                updates=(ProgressUpdate(CHANNEL_MAINTAIN, stage=STAGE_DONE, force_render=True),)
            )

    return ProgressParseResult(should_log_alert=should_log_alert_line(text))
