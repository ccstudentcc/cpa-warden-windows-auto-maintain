from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cwma.warden.services.upload_scope import (
    discover_upload_files,
    select_upload_candidates,
    validate_and_digest_json_file,
)


class WardenUploadScopeModuleTests(unittest.TestCase):
    def test_discover_upload_files_respects_recursive_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            nested = root / "nested"
            nested.mkdir(parents=True, exist_ok=True)
            (root / "top.json").write_text("{}", encoding="utf-8")
            (nested / "inner.json").write_text("{}", encoding="utf-8")

            top_only = discover_upload_files(str(root), recursive=False)
            recursive = discover_upload_files(str(root), recursive=True)

            self.assertEqual([p.name for p in top_only], ["top.json"])
            self.assertEqual(sorted(p.name for p in recursive), ["inner.json", "top.json"])

    def test_validate_and_digest_json_file_invalid_json_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "bad.json"
            bad.write_text("{", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "JSON 格式无效"):
                validate_and_digest_json_file(bad)

    def test_select_upload_candidates_handles_duplicates_and_conflicts(self) -> None:
        same_content_a = {
            "file_name": "same.json",
            "file_path": "a/same.json",
            "content_sha256": "sha-same",
        }
        same_content_b = {
            "file_name": "same.json",
            "file_path": "b/same.json",
            "content_sha256": "sha-same",
        }
        conflict_a = {
            "file_name": "conflict.json",
            "file_path": "a/conflict.json",
            "content_sha256": "sha-1",
        }
        conflict_b = {
            "file_name": "conflict.json",
            "file_path": "b/conflict.json",
            "content_sha256": "sha-2",
        }

        selected, skipped, conflicting = select_upload_candidates(
            [same_content_b, same_content_a, conflict_a, conflict_b]
        )

        self.assertEqual([item["file_name"] for item in selected], ["same.json"])
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["outcome"], "skipped_local_duplicate")
        self.assertIn("conflict.json", conflicting)
        self.assertEqual(len(conflicting["conflict.json"]), 2)


if __name__ == "__main__":
    unittest.main()

