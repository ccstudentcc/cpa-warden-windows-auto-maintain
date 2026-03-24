from __future__ import annotations

import os
import unittest
from unittest import mock

from cwma.auto.channel.channel_status import CHANNEL_UPLOAD, STATE_PENDING, STATE_RUNNING
from cwma.auto.ui.dashboard import apply_panel_colors, fit_panel_line, panel_border_line
from cwma.auto.ui.panel_render import (
    PanelLinesContext,
    SignatureHeartbeatGate,
    build_plain_panel_lines,
    panel_signature,
    should_skip_render_by_signature_gate,
)
from cwma.auto.ui.panel_snapshot import build_panel_snapshot
from cwma.auto.ui.progress_parser import parse_progress_line
from cwma.auto.ui.ui_runtime import UiRuntime, UiRuntimeState as UiPanelRuntimeState


class AutoModuleUiTests(unittest.TestCase):
    def test_parse_progress_line_upload_candidate_total(self) -> None:
        result = parse_progress_line(
            channel="upload",
            text="上传候选文件数: 19",
            current_upload_total=0,
            should_log_alert_line=lambda _text: False,
        )
        self.assertEqual(len(result.updates), 1)
        update = result.updates[0]
        self.assertEqual(update.channel, "upload")
        self.assertEqual(update.stage, "scan")
        self.assertEqual(update.total, 19)
        self.assertFalse(result.should_log_alert)

    def test_parse_progress_line_maintain_probe_candidates(self) -> None:
        result = parse_progress_line(
            channel="maintain",
            text="开始并发探测 wham/usage: candidates=88 workers=50",
            current_upload_total=0,
            should_log_alert_line=lambda _text: False,
        )
        self.assertEqual(len(result.updates), 1)
        update = result.updates[0]
        self.assertEqual(update.channel, "maintain")
        self.assertEqual(update.stage, "probe")
        self.assertEqual(update.total, 88)
        self.assertTrue(update.force_render)

    def test_parse_progress_line_alert_passthrough(self) -> None:
        result = parse_progress_line(
            channel="maintain",
            text="request failed with status 500",
            current_upload_total=0,
            should_log_alert_line=lambda text: "failed" in text,
        )
        self.assertEqual(result.updates, ())
        self.assertTrue(result.should_log_alert)

    def test_ui_runtime_on_stage_update_updates_progress_and_renders(self) -> None:
        rendered: list[list[str]] = []
        state = UiPanelRuntimeState()
        ui = UiRuntime(
            state=state,
            monotonic=lambda: 100.0,
            build_panel_snapshot=lambda **_: mock.Mock(
                upload_state="idle",
                maintain_state="idle",
                upload_stage="idle",
                maintain_stage="idle",
            ),
            build_panel_lines=lambda **_: ["line-1", "line-2"],
            apply_panel_colors=lambda lines, **_: lines,
            render_panel=lambda lines: rendered.append(lines),
        )

        ui.on_stage_update(
            CHANNEL_UPLOAD,
            stage="upload",
            done=3,
            total=10,
            force_render=True,
        )

        self.assertEqual(state.upload_progress_state["stage"], "upload")
        self.assertEqual(state.upload_progress_state["done"], 3)
        self.assertEqual(state.upload_progress_state["total"], 10)
        self.assertEqual(len(rendered), 1)

    def test_ui_runtime_render_if_needed_respects_interval_and_force(self) -> None:
        rendered: list[list[str]] = []
        ticks = iter([0.0, 0.05, 0.10])
        state = UiPanelRuntimeState(
            progress_render_interval_seconds=0.2,
            progress_render_heartbeat_seconds=9.0,
        )
        ui = UiRuntime(
            state=state,
            monotonic=lambda: next(ticks),
            build_panel_snapshot=lambda **_: mock.Mock(
                upload_state="idle",
                maintain_state="idle",
                upload_stage="idle",
                maintain_stage="idle",
            ),
            build_panel_lines=lambda **_: ["same"],
            apply_panel_colors=lambda lines, **_: lines,
            render_panel=lambda lines: rendered.append(lines),
        )

        ui.render_if_needed(force=True)
        ui.render_if_needed(force=False)
        ui.render_if_needed(force=True)

        self.assertEqual(len(rendered), 2)
        self.assertEqual(state.last_progress_signature, "same")

    def test_build_plain_panel_lines_from_snapshot(self) -> None:
        snapshot = build_panel_snapshot(
            upload_progress_state={"stage": "running", "done": 3, "total": 10},
            maintain_progress_state={"stage": "pending_incremental", "done": 1, "total": 5},
            pending_upload_snapshot=["a", "b"],
            inflight_upload_snapshot=["x"],
            pending_upload_reason="detected changes",
            upload_running=False,
            upload_retry_due_at=12.0,
            pending_maintain=True,
            pending_maintain_names={"acc-a"},
            pending_maintain_reason="post-upload maintain",
            inflight_maintain_names=None,
            maintain_running=False,
            maintain_retry_due_at=9.0,
            next_maintain_due_at=21.0,
            last_incremental_defer_reason=None,
            now_monotonic=5.0,
            compute_upload_queue_batches=lambda pending: (pending, 1 if pending > 0 else 0),
            choose_incremental_maintain_batch_size=lambda pending, _pressure: pending,
        )
        context = PanelLinesContext(
            panel_title="CPA Warden Auto Dashboard",
            now_text="2026-03-23 22:40:00",
            panel_mode="log",
            watch_interval_seconds=5,
            upload_bar="[###---------------]",
            maintain_bar="[##----------------]",
            upload_reason_text="detected changes",
            maintain_reason_text="post-upload maintain",
            maintain_defer_text="-",
        )
        lines = build_plain_panel_lines(
            snapshot=snapshot,
            context=context,
            fit_line=lambda text: text,
            border_line=lambda char: char * 8,
        )
        self.assertEqual(len(lines), 8)
        self.assertEqual(lines[0], "========")
        self.assertEqual(lines[4], "--------")
        self.assertIn("panel=log", lines[1])
        self.assertIn("UPLOAD   [###---------------] 3/10", lines[2])
        self.assertIn("queue_files=2", lines[3])
        self.assertIn("MAINTAIN [##----------------] 1/5", lines[5])
        self.assertIn("queue_incremental=1", lines[6])

    def test_panel_signature_joins_lines_with_newlines(self) -> None:
        self.assertEqual(panel_signature(["a", "b", "c"]), "a\nb\nc")

    def test_should_skip_render_by_signature_gate(self) -> None:
        self.assertTrue(
            should_skip_render_by_signature_gate(
                SignatureHeartbeatGate(
                    force=False,
                    signature_unchanged=True,
                    now_monotonic=10.0,
                    last_render_at=5.0,
                    heartbeat_seconds=8.0,
                )
            )
        )
        self.assertFalse(
            should_skip_render_by_signature_gate(
                SignatureHeartbeatGate(
                    force=True,
                    signature_unchanged=True,
                    now_monotonic=10.0,
                    last_render_at=5.0,
                    heartbeat_seconds=8.0,
                )
            )
        )
        self.assertFalse(
            should_skip_render_by_signature_gate(
                SignatureHeartbeatGate(
                    force=False,
                    signature_unchanged=False,
                    now_monotonic=10.0,
                    last_render_at=5.0,
                    heartbeat_seconds=8.0,
                )
            )
        )
        self.assertFalse(
            should_skip_render_by_signature_gate(
                SignatureHeartbeatGate(
                    force=False,
                    signature_unchanged=True,
                    now_monotonic=20.0,
                    last_render_at=5.0,
                    heartbeat_seconds=8.0,
                )
            )
        )

    def test_fit_panel_line_truncates_with_ellipsis(self) -> None:
        with mock.patch("cwma.auto.ui.dashboard.shutil.get_terminal_size") as mocked_size:
            mocked_size.return_value = os.terminal_size((8, 20))
            self.assertEqual(fit_panel_line("1234567890"), "12345...")

    def test_panel_border_line_respects_minimum_width(self) -> None:
        with mock.patch("cwma.auto.ui.dashboard.shutil.get_terminal_size") as mocked_size:
            mocked_size.return_value = os.terminal_size((20, 20))
            border = panel_border_line("=")
        self.assertTrue(border.startswith("+"))
        self.assertTrue(border.endswith("+"))
        self.assertEqual(len(border), 38)

    def test_apply_panel_colors_disabled_returns_original_lines(self) -> None:
        lines = ["A", "B", "UPLOAD state=idle stage=idle", "-", "-", "MAINTAIN state=idle stage=idle"]
        self.assertEqual(
            apply_panel_colors(
                lines,
                enabled=False,
                upload_state="idle",
                maintain_state="idle",
                upload_stage="idle",
                maintain_stage="idle",
            ),
            lines,
        )

    def test_apply_panel_colors_enabled_injects_ansi_escape(self) -> None:
        lines = ["A", "B", "UPLOAD state=running stage=upload", "-", "-", "MAINTAIN state=pending stage=probe"]
        colored = apply_panel_colors(
            lines,
            enabled=True,
            upload_state="running",
            maintain_state="pending",
            upload_stage="upload",
            maintain_stage="probe",
        )
        self.assertNotEqual(colored, lines)
        self.assertIn("\x1b[", colored[2])
        self.assertIn("\x1b[", colored[5])

    def test_build_panel_snapshot_pending_incremental_scope(self) -> None:
        snapshot = build_panel_snapshot(
            upload_progress_state={"stage": "upload", "done": 2, "total": 5},
            maintain_progress_state={"stage": "probe", "done": 1, "total": 4},
            pending_upload_snapshot=["a", "b", "c"],
            inflight_upload_snapshot=["a"],
            pending_upload_reason=None,
            upload_running=False,
            upload_retry_due_at=15.0,
            pending_maintain=True,
            pending_maintain_names={"x.json", "y.json"},
            pending_maintain_reason="post-upload maintain",
            inflight_maintain_names=set(),
            maintain_running=False,
            maintain_retry_due_at=25.0,
            next_maintain_due_at=40.0,
            last_incremental_defer_reason=None,
            now_monotonic=10.0,
            compute_upload_queue_batches=lambda pending: (2, 2) if pending > 0 else (0, 0),
            choose_incremental_maintain_batch_size=lambda pending, upload_pressure: (
                1 if upload_pressure else pending
            ),
        )
        self.assertEqual(snapshot.upload_state, STATE_PENDING)
        self.assertEqual(snapshot.pending_upload, 3)
        self.assertEqual(snapshot.upload_queue_batches, 2)
        self.assertFalse(snapshot.pending_full)
        self.assertEqual(snapshot.pending_incremental, 2)
        self.assertEqual(snapshot.maintain_next_batch, 1)
        self.assertEqual(snapshot.upload_retry_wait, 5)
        self.assertEqual(snapshot.maintain_retry_wait, 15)
        self.assertEqual(snapshot.next_full_wait, 30)

    def test_build_panel_snapshot_running_full_scope(self) -> None:
        snapshot = build_panel_snapshot(
            upload_progress_state={"stage": "scan", "done": 0, "total": 0},
            maintain_progress_state={"stage": "running", "done": 0, "total": 0},
            pending_upload_snapshot=[],
            inflight_upload_snapshot=["a"],
            pending_upload_reason="detected JSON changes",
            upload_running=True,
            upload_retry_due_at=0.0,
            pending_maintain=True,
            pending_maintain_names=None,
            pending_maintain_reason="scheduled maintain",
            inflight_maintain_names=None,
            maintain_running=True,
            maintain_retry_due_at=0.0,
            next_maintain_due_at=None,
            last_incremental_defer_reason="cooldown",
            now_monotonic=10.0,
            compute_upload_queue_batches=lambda pending: (0, 0),
            choose_incremental_maintain_batch_size=lambda pending, upload_pressure: 0,
        )
        self.assertEqual(snapshot.upload_state, STATE_RUNNING)
        self.assertEqual(snapshot.maintain_state, STATE_RUNNING)
        self.assertTrue(snapshot.pending_full)
        self.assertEqual(snapshot.pending_incremental, 0)
        self.assertEqual(snapshot.maintain_next_batch, 0)
        self.assertEqual(snapshot.maintain_inflight_scope, "full")

