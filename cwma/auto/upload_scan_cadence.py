from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UploadScanCadenceDecision:
    should_deep_scan: bool
    next_deep_scan_counter: int


def decide_upload_deep_scan(
    *,
    force_deep_scan: bool,
    pending_upload_retry: bool,
    current_json_count: int,
    last_json_count: int,
    inspect_zip_files: bool,
    current_zip_signature: tuple[str, ...],
    last_zip_signature: tuple[str, ...],
    deep_scan_counter: int,
    deep_scan_interval_loops: int,
) -> UploadScanCadenceDecision:
    if force_deep_scan:
        return UploadScanCadenceDecision(should_deep_scan=True, next_deep_scan_counter=0)
    if pending_upload_retry:
        return UploadScanCadenceDecision(should_deep_scan=True, next_deep_scan_counter=0)
    if current_json_count != last_json_count:
        return UploadScanCadenceDecision(should_deep_scan=True, next_deep_scan_counter=0)
    if inspect_zip_files and current_zip_signature != last_zip_signature:
        return UploadScanCadenceDecision(should_deep_scan=True, next_deep_scan_counter=0)

    next_counter = deep_scan_counter + 1
    if next_counter >= deep_scan_interval_loops:
        return UploadScanCadenceDecision(should_deep_scan=True, next_deep_scan_counter=0)
    return UploadScanCadenceDecision(should_deep_scan=False, next_deep_scan_counter=next_counter)
