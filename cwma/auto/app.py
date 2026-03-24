from __future__ import annotations

import argparse
import atexit
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from .infra.config import Settings, load_settings as load_auto_settings
from .channel.channel_status import (
    CHANNEL_UPLOAD,
    STAGE_RUNNING,
)
from .channel.channel_commands import (
    build_maintain_command as build_maintain_command_rows,
    build_upload_command as build_upload_command_rows,
)
from .ui.dashboard import (
    apply_panel_colors as apply_dashboard_panel_colors,
)
from .state.maintain_queue import (
    MaintainRuntimeState,
    MaintainQueueState,
)
from .infra.output_pump import (
    append_child_output_line as append_output_line,
    start_output_pump_thread,
)
from .infra.process_output import (
    build_child_process_env,
    decode_child_output_line as decode_process_output_line,
    should_log_child_alert_line as should_log_process_alert_line,
)
from .infra.inprocess_supervisor import (
    poll_channel_exit as poll_inprocess_channel_exit,
    start_channel as start_inprocess_channel,
)
from .ui.progress_parser import parse_progress_line
from .runtime.shutdown_runtime import sleep_between_watch_cycles as sleep_between_watch_cycles_runtime
from .runtime.channel_runtime import (
    poll_maintain_channel,
    poll_upload_channel,
    start_maintain_channel,
    start_upload_channel,
)
from .runtime.channel_runtime_adapter import ChannelRuntimeAdapter
from .runtime.cycle_runtime_adapter import CycleRuntimeAdapter
from .runtime.host_init_adapter import initialize_host_state
from .runtime.lifecycle_runtime_adapter import LifecycleRuntimeAdapter
from .runtime.host_ops_adapter import HostOpsAdapter
from .runtime.panel_runtime_adapter import PanelRuntimeAdapter, detect_panel_capability
from .runtime.state_bridge_adapter import StateBridgeAdapter
from .runtime.startup_runtime import (
    StartupRuntimeDeps,
    StartupRuntimeState,
    run_startup_cycle,
)
from .runtime.upload_scan_runtime import (
    run_active_upload_probe_cycle,
    run_upload_scan_cycle,
)
from .runtime.upload_runtime_adapter import UploadRuntimeAdapter
from .runtime.watch_runtime import (
    WatchRuntimeDeps,
    WatchRuntimeState,
    run_watch_iteration,
)
from .ui.ui_runtime import UiRuntime
from .state.snapshots import compute_pending_upload_snapshot as compute_pending_upload_snapshot_rows
from .state.upload_queue import UploadQueueState


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


class AutoMaintainer:
    def __init__(self, settings: Settings) -> None:
        initialize_host_state(
            host=self,
            settings=settings,
            detect_panel_capability=detect_panel_capability,
        )
        self.panel_runtime_adapter = PanelRuntimeAdapter(
            host=self,
            log=lambda message: log(message),
        )
        self.ui_runtime = UiRuntime(
            state=self.runtime.ui,
            monotonic=lambda: time.monotonic(),
            build_panel_snapshot=self.panel_runtime_adapter.build_progress_panel_snapshot,
            build_panel_lines=self.panel_runtime_adapter.build_progress_panel_lines,
            apply_panel_colors=lambda lines, upload_state, maintain_state, upload_stage, maintain_stage: (
                apply_dashboard_panel_colors(
                    lines,
                    enabled=self.panel_color_enabled,
                    upload_state=upload_state,
                    maintain_state=maintain_state,
                    upload_stage=upload_stage,
                    maintain_stage=maintain_stage,
                )
            ),
            render_panel=self.panel_runtime_adapter.render_fixed_panel,
            output_lock_factory=lambda: self.output_lock,
        )
        self.upload_runtime_adapter = UploadRuntimeAdapter(
            host=self,
            compute_pending_upload_snapshot=compute_pending_upload_snapshot_rows,
            get_run_upload_scan_cycle=lambda: run_upload_scan_cycle,
            get_run_active_upload_probe_cycle=lambda: run_active_upload_probe_cycle,
            log=log,
        )
        if settings.inprocess_execution_enabled:
            def start_maintain(**kwargs: object) -> object:
                return start_maintain_channel(
                    **kwargs,
                    start_channel_impl=start_inprocess_channel,
                )

            def start_upload(**kwargs: object) -> object:
                return start_upload_channel(
                    **kwargs,
                    start_channel_impl=start_inprocess_channel,
                )

            def poll_maintain(**kwargs: object) -> object:
                return poll_maintain_channel(
                    **kwargs,
                    poll_channel_exit_impl=poll_inprocess_channel_exit,
                )

            def poll_upload(**kwargs: object) -> object:
                return poll_upload_channel(
                    **kwargs,
                    poll_channel_exit_impl=poll_inprocess_channel_exit,
                )
            get_start_maintain_channel = lambda: start_maintain
            get_start_upload_channel = lambda: start_upload
            get_poll_maintain_channel = lambda: poll_maintain
            get_poll_upload_channel = lambda: poll_upload
        else:
            # Resolve these callables lazily so unittest patching of module symbols
            # (for example `auto_maintain.start_upload_channel`) keeps working.
            get_start_maintain_channel = lambda: start_maintain_channel
            get_start_upload_channel = lambda: start_upload_channel
            get_poll_maintain_channel = lambda: poll_maintain_channel
            get_poll_upload_channel = lambda: poll_upload_channel
        self.channel_runtime_adapter = ChannelRuntimeAdapter(
            host=self,
            get_start_maintain_channel=get_start_maintain_channel,
            get_start_upload_channel=get_start_upload_channel,
            get_poll_maintain_channel=get_poll_maintain_channel,
            get_poll_upload_channel=get_poll_upload_channel,
            get_build_child_process_env=lambda: build_child_process_env,
            get_monotonic=lambda: time.monotonic,
            get_popen_factory=lambda: subprocess.Popen,
            log=log,
        )
        self.host_ops_adapter = HostOpsAdapter(
            host=self,
            get_log=lambda: log,
        )
        self.lifecycle_runtime_adapter = LifecycleRuntimeAdapter(host=self, log=log)
        self.state_bridge_adapter = StateBridgeAdapter(host=self, log=log)
        self.cycle_runtime_adapter = CycleRuntimeAdapter(
            host=self,
            get_run_startup_cycle=lambda: run_startup_cycle,
            get_run_watch_iteration=lambda: run_watch_iteration,
            monotonic=time.monotonic,
            log=log,
        )

    def ensure_paths(self) -> None:
        self.host_ops_adapter.ensure_paths()

    def _apply_startup_runtime_state(self, state: StartupRuntimeState) -> None:
        self.cycle_runtime_adapter.apply_startup_runtime_state(state)

    def _apply_watch_runtime_state(self, state: WatchRuntimeState) -> None:
        self.cycle_runtime_adapter.apply_watch_runtime_state(state)

    def _build_startup_runtime_state(self) -> StartupRuntimeState:
        return self.cycle_runtime_adapter.build_startup_runtime_state()

    def _build_startup_runtime_deps(self) -> StartupRuntimeDeps:
        return self.cycle_runtime_adapter.build_startup_runtime_deps()

    def _build_watch_runtime_state(self) -> WatchRuntimeState:
        return self.cycle_runtime_adapter.build_watch_runtime_state()

    def _build_watch_runtime_deps(self) -> WatchRuntimeDeps:
        return self.cycle_runtime_adapter.build_watch_runtime_deps()

    def _run_runtime_cycle(
        self,
        *,
        run_cycle: Callable[[], object],
        apply_state: Callable[[object], None],
    ) -> int:
        return self.cycle_runtime_adapter.run_runtime_cycle(
            run_cycle=run_cycle,
            apply_state=apply_state,
        )

    def _run_startup_phase(self) -> int:
        return self.cycle_runtime_adapter.run_startup_phase()

    def _run_watch_iteration(self, now: float) -> int:
        return self.cycle_runtime_adapter.run_watch_iteration(now)

    def _run_stage_sequence(self, stages: Iterable[tuple[str, Callable[[], int]]]) -> int:
        return self.cycle_runtime_adapter.run_stage_sequence(stages)

    def _run_stage(self, stage: str, runner: Callable[[], int]) -> int:
        return self.cycle_runtime_adapter.run_stage(stage, runner)

    def _resolve_stage_failure(self, stage: str, code: int) -> int:
        return self.cycle_runtime_adapter.resolve_stage_failure(stage, code)

    def _is_run_once_cycle_complete(self) -> bool:
        nothing_running = self.upload_process is None and self.maintain_process is None
        nothing_pending = (
            self.pending_upload_snapshot is None
            and not self.pending_maintain
            and (not self.pending_upload_retry)
        )
        return nothing_running and nothing_pending

    def _sleep_between_watch_cycles(self) -> int | None:
        return sleep_between_watch_cycles_runtime(
            run_once=self.settings.run_once,
            is_run_once_cycle_complete=self._is_run_once_cycle_complete,
            sleep_with_shutdown_fn=self.sleep_with_shutdown,
            current_loop_sleep_seconds_fn=self.current_loop_sleep_seconds,
            log=log,
        )

    def _run_watch_cycle_and_maybe_sleep(self) -> int | None:
        now = time.monotonic()
        watch_exit = self._run_watch_iteration(now)
        if watch_exit != 0:
            return watch_exit
        return self._sleep_between_watch_cycles()

    def run(self) -> int:
        self.ensure_paths()
        self.register_shutdown_handlers()
        self.acquire_instance_lock()
        atexit.register(self.release_instance_lock)
        self.set_console_title()
        self.panel_runtime_adapter.init_fixed_panel_area()
        self.log_settings()
        try:
            startup_exit = self._run_startup_phase()
            if startup_exit != 0:
                return startup_exit

            while True:
                cycle_exit = self._run_watch_cycle_and_maybe_sleep()
                if cycle_exit is not None:
                    return cycle_exit
        finally:
            self.release_instance_lock()

    def _settings_log_rows(self) -> list[tuple[str, str | int | Path]]:
        return self.host_ops_adapter.settings_log_rows()

    def log_settings(self) -> None:
        self.host_ops_adapter.log_settings()

    def instance_label(self) -> str:
        started = self.instance_started_at.strftime("%Y-%m-%d %H:%M:%S")
        return f"cpa-auto-maintain | {started} | {os.getpid()}"

    def set_console_title(self) -> None:
        self.lifecycle_runtime_adapter.set_console_title()

    def register_shutdown_handlers(self) -> None:
        self.lifecycle_runtime_adapter.register_shutdown_handlers(self._signal_handler)

    def _signal_handler(self, signum: int, _frame: object) -> None:
        self.request_shutdown(f"SIGNAL_{signum}")

    def request_shutdown(self, reason: str) -> None:
        self.lifecycle_runtime_adapter.request_shutdown(reason)

    def terminate_process(self, proc: subprocess.Popen | None, *, name: str) -> None:
        self.lifecycle_runtime_adapter.terminate_process(proc, name=name)

    def terminate_active_processes(self) -> None:
        self.lifecycle_runtime_adapter.terminate_active_processes()

    def sleep_with_shutdown(self, total_seconds: int) -> bool:
        return self.lifecycle_runtime_adapter.sleep_with_shutdown(total_seconds)

    def current_loop_sleep_seconds(self) -> int:
        return self.lifecycle_runtime_adapter.current_loop_sleep_seconds()

    def wait_for_stable_snapshot(self, baseline_snapshot: list[str]) -> tuple[int, list[str] | None]:
        return self.lifecycle_runtime_adapter.wait_for_stable_snapshot(
            baseline_snapshot,
            sleep_with_shutdown=self.sleep_with_shutdown,
            build_stable_snapshot=lambda: self.build_snapshot(self.stable_snapshot_file),
        )

    def acquire_instance_lock(self) -> None:
        self.host_ops_adapter.acquire_instance_lock(
            allow_multi_instance=self.settings.allow_multi_instance,
        )

    def acquire_instance_lock_windows(self) -> None:
        if os.name != "nt":
            raise RuntimeError("Windows lock backend unavailable (msvcrt).")
        self.host_ops_adapter.acquire_instance_lock(
            allow_multi_instance=False,
        )

    def release_instance_lock(self) -> None:
        self.host_ops_adapter.release_instance_lock()

    def get_json_paths(self) -> list[Path]:
        return self.host_ops_adapter.get_json_paths()

    def get_json_count(self) -> int:
        return self.host_ops_adapter.get_json_count()

    def get_zip_signature(self) -> tuple[str, ...]:
        return self.host_ops_adapter.get_zip_signature()

    def inspect_zip_archives(self) -> bool:
        return self.host_ops_adapter.inspect_zip_archives()

    def extract_zip_with_bandizip(self, zip_path: Path, output_dir: Path) -> int:
        return self.host_ops_adapter.extract_zip_with_bandizip(zip_path, output_dir)

    def snapshot_lines(self) -> list[str]:
        return self.host_ops_adapter.snapshot_lines()

    def write_snapshot(self, target: Path, lines: Iterable[str]) -> None:
        self.host_ops_adapter.write_snapshot(target, lines)

    def read_snapshot(self, source: Path) -> list[str]:
        return self.host_ops_adapter.read_snapshot(source)

    def build_snapshot(self, target: Path) -> list[str]:
        return self.host_ops_adapter.build_snapshot(target)

    def delete_uploaded_files_from_snapshot(self, snapshot_lines: list[str]) -> None:
        self.host_ops_adapter.delete_uploaded_files_from_snapshot(snapshot_lines)

    def prune_empty_dirs_under_auth_dir(self) -> None:
        self.host_ops_adapter.prune_empty_dirs_under_auth_dir()

    def command_base(self) -> list[str]:
        cmd = [sys.executable, str(self.cpa_script)]
        if self.settings.config_path is not None:
            cmd.extend(["--config", str(self.settings.config_path)])
        return cmd

    def update_channel_progress(
        self,
        name: str,
        *,
        stage: str | None = None,
        done: int | None = None,
        total: int | None = None,
        force_render: bool = False,
    ) -> None:
        self._sync_ui_runtime_inputs()
        self.ui_runtime.on_stage_update(
            name,
            stage=stage,
            done=done,
            total=total,
            force_render=force_render,
        )
        self._sync_ui_runtime_outputs()

    def mark_channel_running(self, name: str) -> None:
        self.update_channel_progress(name, stage=STAGE_RUNNING, force_render=True)

    def decode_child_output_line(self, raw: bytes | str) -> str:
        return decode_process_output_line(raw)

    def should_log_child_alert_line(self, text: str) -> bool:
        return should_log_process_alert_line(text)

    def render_progress_snapshot(self, *, force: bool = False) -> None:
        self._sync_ui_runtime_inputs()
        self.ui_runtime.render_if_needed(force=force)
        self._sync_ui_runtime_outputs()

    def _sync_ui_runtime_inputs(self) -> None:
        self.runtime.ui.upload_progress_state = self.upload_progress_state
        self.runtime.ui.maintain_progress_state = self.maintain_progress_state
        self.runtime.ui.last_progress_render_at = self.last_progress_render_at
        self.runtime.ui.progress_render_interval_seconds = self.progress_render_interval_seconds
        self.runtime.ui.progress_render_heartbeat_seconds = self.progress_render_heartbeat_seconds
        self.runtime.ui.last_progress_signature = self.last_progress_signature

    def _sync_ui_runtime_outputs(self) -> None:
        self.upload_progress_state = self.runtime.ui.upload_progress_state
        self.maintain_progress_state = self.runtime.ui.maintain_progress_state
        self.last_progress_render_at = self.runtime.ui.last_progress_render_at
        self.last_progress_signature = self.runtime.ui.last_progress_signature

    def parse_child_progress_line(self, name: str, line: str) -> None:
        text = line.strip()
        if not text:
            return
        current_upload_total = int(self.upload_progress_state.get("total") or 0)
        parsed = parse_progress_line(
            channel=name,
            text=text,
            current_upload_total=current_upload_total,
            should_log_alert_line=self.should_log_child_alert_line,
        )
        for update in parsed.updates:
            self.update_channel_progress(
                update.channel,
                stage=update.stage,
                done=update.done,
                total=update.total,
                force_render=update.force_render,
            )
            return

        if parsed.should_log_alert:
            log(f"[{name}] {text}")

    def _write_child_output_line(self, name: str, line: str) -> None:
        target = self.upload_cmd_output_file if name == CHANNEL_UPLOAD else self.maintain_cmd_output_file
        append_output_line(target=target, line=line)

    def start_output_pump(self, name: str, proc: subprocess.Popen) -> None:
        def _on_line(line: str) -> None:
            self._write_child_output_line(name, line)
            self.parse_child_progress_line(name, line)

        thread = start_output_pump_thread(
            channel=name,
            proc=proc,
            decode_line=self.decode_child_output_line,
            on_line=_on_line,
            warn=log,
        )
        if thread is None:
            return
        if name == CHANNEL_UPLOAD:
            self.upload_output_thread = thread
        else:
            self.maintain_output_thread = thread

    def build_maintain_command(self, maintain_names_file: Path | None = None) -> list[str]:
        return build_maintain_command_rows(
            command_base=self.command_base(),
            maintain_db_path=self.settings.maintain_db_path,
            maintain_log_file=self.settings.maintain_log_file,
            maintain_names_file=maintain_names_file,
            assume_yes=self.settings.maintain_assume_yes,
        )

    def build_upload_command(self, upload_names_file: Path | None = None) -> list[str]:
        return build_upload_command_rows(
            command_base=self.command_base(),
            auth_dir=self.settings.auth_dir,
            upload_db_path=self.settings.upload_db_path,
            upload_log_file=self.settings.upload_log_file,
            upload_names_file=upload_names_file,
        )

    def _upload_queue_state(self) -> UploadQueueState:
        return self.state_bridge_adapter.upload_queue_state()

    def _apply_upload_queue_state(self, state: UploadQueueState) -> None:
        self.state_bridge_adapter.apply_upload_queue_state(state)

    def _maintain_queue_state(self) -> MaintainQueueState:
        return self.state_bridge_adapter.maintain_queue_state()

    def _apply_maintain_queue_state(self, state: MaintainQueueState) -> None:
        self.state_bridge_adapter.apply_maintain_queue_state(state)

    def _maintain_runtime_state(self) -> MaintainRuntimeState:
        return self.state_bridge_adapter.maintain_runtime_state()

    def _apply_maintain_runtime_state(self, state: MaintainRuntimeState) -> None:
        self.state_bridge_adapter.apply_maintain_runtime_state(state)

    def queue_maintain(self, reason: str, names: set[str] | None = None) -> None:
        self.state_bridge_adapter.queue_maintain(reason, names)

    def merge_pending_incremental_maintain_names(self, names: set[str]) -> None:
        self.state_bridge_adapter.merge_pending_incremental_maintain_names(names)

    def _defer_incremental_maintain_if_needed(self, now: float, *, planned_batch_size: int) -> bool:
        return self.state_bridge_adapter.defer_incremental_maintain_if_needed(
            now,
            planned_batch_size=planned_batch_size,
        )

    def _set_maintain_process(self, process: subprocess.Popen | None) -> None:
        self.maintain_process = process
        self.runtime.lifecycle.maintain_process = self.maintain_process

    def _set_upload_process(self, process: subprocess.Popen | None) -> None:
        self.upload_process = process
        self.runtime.lifecycle.upload_process = self.upload_process

    def maybe_start_maintain(self) -> int:
        return self.channel_runtime_adapter.maybe_start_maintain()

    def maybe_start_upload(self) -> int:
        return self.channel_runtime_adapter.maybe_start_upload()

    def handle_command_start_error(self, name: str, exc: Exception) -> int:
        # Kept for compatibility with tests/hooks that may invoke this path directly.
        return self.channel_runtime_adapter.handle_command_start_error(name, exc)

    def poll_maintain_process(self) -> int:
        return self.channel_runtime_adapter.poll_maintain_process()

    def poll_upload_process(self) -> int:
        return self.channel_runtime_adapter.poll_upload_process()

    def probe_changes_during_active_upload(self) -> int:
        return self.upload_runtime_adapter.probe_changes_during_active_upload()

    def handle_failure(self, stage: str, code: int) -> bool:
        log(f"{stage} failed with exit {code}.")
        if self.settings.run_once:
            log("Run-once mode enabled. Exiting on failure.")
            return False
        if self.settings.continue_on_command_failure:
            log("Continue mode enabled. Keep loop alive.")
            return True
        return False

    def check_and_maybe_upload(
        self,
        force_deep_scan: bool = False,
        *,
        preserve_retry_state: bool = False,
        skip_stability_wait: bool = False,
        queue_reason: str = "detected JSON changes",
    ) -> int:
        return self.upload_runtime_adapter.check_and_maybe_upload(
            force_deep_scan=force_deep_scan,
            preserve_retry_state=preserve_retry_state,
            skip_stability_wait=skip_stability_wait,
            queue_reason=queue_reason,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto maintain + folder watch uploader for cpa-warden.",
    )
    parser.add_argument(
        "--watch-config",
        help="Watcher config JSON path (env WATCH_CONFIG_PATH also supported).",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    return parser.parse_args()


def load_settings(args: argparse.Namespace) -> Settings:
    base_dir = Path(__file__).resolve().parents[2]
    return load_auto_settings(
        args=args,
        base_dir=base_dir,
        log=log,
    )


def main() -> int:
    args = parse_args()
    try:
        settings = load_settings(args)
        maintainer = AutoMaintainer(settings)
        return maintainer.run()
    except KeyboardInterrupt:
        log("Interrupted by user.")
        return 130
    except Exception as exc:
        log(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

