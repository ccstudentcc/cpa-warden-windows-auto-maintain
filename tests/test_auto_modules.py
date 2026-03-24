from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cwma.auto.progress_parser import parse_progress_line
from cwma.auto.scope_files import write_scope_names
from cwma.auto.snapshots import (
    build_snapshot_file,
    build_snapshot_lines,
    read_snapshot_lines,
    write_snapshot_lines,
)


class AutoModuleTests(unittest.TestCase):
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

    def test_snapshot_file_helpers_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            a = base / "a.json"
            b = base / "b.json"
            a.write_text("{}", encoding="utf-8")
            b.write_text("{}", encoding="utf-8")
            target = base / "snapshot.txt"

            lines = build_snapshot_file(
                target=target,
                paths=[a, b],
                log=lambda _msg: None,
            )
            loaded = read_snapshot_lines(target)

        self.assertEqual(lines, loaded)
        self.assertEqual(len(lines), 2)

    def test_build_snapshot_lines_skips_missing_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            exists = base / "ok.json"
            missing = base / "missing.json"
            exists.write_text("{}", encoding="utf-8")
            warnings: list[str] = []

            lines = build_snapshot_lines([exists, missing], log=warnings.append)

        self.assertEqual(len(lines), 1)
        self.assertIn("skipped transient files", warnings[0])

    def test_write_snapshot_lines_overwrites_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "snapshot.txt"
            write_snapshot_lines(target, ["a", "b"])
            write_snapshot_lines(target, ["c"])
            loaded = read_snapshot_lines(target)
        self.assertEqual(loaded, ["c"])

    def test_write_scope_names_sorts_and_filters_empty_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "state" / "scope.txt"
            result = write_scope_names(target, {"b.json", "", "a.json", " "})
            content = target.read_text(encoding="utf-8").splitlines()
        self.assertEqual(result, target)
        self.assertEqual(content, ["a.json", "b.json"])


if __name__ == "__main__":
    unittest.main()
