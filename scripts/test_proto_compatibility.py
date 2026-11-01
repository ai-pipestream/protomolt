#!/usr/bin/env python3
"""Exercise the public compatibility script with real Buf and isolated Git fixtures."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
APPROVED = "f83b48fe1aad1e5aaafe4fc5007325b67c97e1d2"
BEFORE = "e78dd0e5c64ea38699a1002caa7e31ecb45cd430"
DOCUMENT = "repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/document_service.proto"
ARCHIVE = "repo/proto/src/main/proto/ai/protomolt/proto/repo/archive/v1/archive_service.proto"


class ProtoCompatibilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="protomolt-compat-test-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.repo = Path(cls.temp.name) / "repo"
        # Local objects only: no network, changes to the caller's checkout or
        # dependency on the live main branch. Historical snapshots are deliberate.
        subprocess.run(["git", "clone", "--quiet", "--shared", "--no-checkout",
                        str(ROOT), str(cls.repo)], check=True)
        cls.git("checkout", "--quiet", "--detach", BEFORE)
        schema = cls.repo / DOCUMENT
        schema.write_bytes(schema.read_bytes() + b"\n// Unapproved baseline identity.\n")
        cls.git("add", DOCUMENT)
        cls.git("-c", "user.name=Compatibility fixture", "-c", "user.email=fixture@example.invalid",
                "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Alter baseline identity")
        cls.changed_baseline = cls.git("rev-parse", "HEAD").strip()
        cls.git("checkout", "--quiet", "--detach", APPROVED)
        for name in ("check-proto-compatibility.sh", "check-approved-proto-migration.py"):
            shutil.copy2(ROOT / "scripts" / name, cls.repo / "scripts" / name)
        # Use the real current gate configuration, including every FILE rule.
        shutil.copy2(ROOT / "buf.yaml", cls.repo / "buf.yaml")

    @classmethod
    def git(cls, *args):
        return subprocess.run(["git", *args], cwd=cls.repo, text=True,
                              capture_output=True, check=True).stdout

    def tearDown(self):
        self.git("checkout", "--", DOCUMENT, ARCHIVE)

    def gate(self, baseline=BEFORE):
        return subprocess.run(["bash", "scripts/check-proto-compatibility.sh", baseline],
                              cwd=self.repo, text=True, capture_output=True, env=os.environ.copy())

    def change(self, path, old, new):
        file = self.repo / path
        content = file.read_text()
        self.assertEqual(content.count(old), 1, "Fixture must change exactly one declaration")
        file.write_text(content.replace(old, new))

    def refused(self, result, diagnostic):
        self.assertEqual(result.returncode, 100, result.stdout + result.stderr)
        self.assertIn(diagnostic, result.stdout + result.stderr)
        self.assertNotIn("Accepted ONLY", result.stdout)

    def test_approved_transition(self):
        result = self.gate()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Accepted ONLY the approved #675", result.stdout)

    def test_baseline_already_contains_transition(self):
        result = self.gate(APPROVED)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("Accepted ONLY", result.stdout)

    def test_unrelated_streaming_change_is_still_rejected(self):
        self.change(ARCHIVE, "returns (CreateArchiveResponse);", "returns (stream CreateArchiveResponse);")
        self.refused(self.gate(), 'RPC \\"CreateArchive\\"')

    def test_unrelated_oneof_change_is_still_rejected(self):
        self.change(ARCHIVE, "message CreateArchiveResponse {\n  // The stored archive.\n  Archive archive = 1;",
                    "message CreateArchiveResponse {\n  oneof result { Archive archive = 1; }")
        self.refused(self.gate(), 'message \\"CreateArchiveResponse\\"')

    def test_changed_current_source_cannot_reuse_approval(self):
        schema = self.repo / DOCUMENT
        schema.write_bytes(schema.read_bytes() + b"\n// Unapproved current identity.\n")
        self.refused(self.gate(), "schema source identity does not match approval")

    def test_changed_baseline_cannot_reuse_approval(self):
        self.refused(self.gate(self.changed_baseline), "schema source identity does not match approval")

    def test_unrelated_break_after_migration_is_still_rejected(self):
        self.change(ARCHIVE, "returns (CreateArchiveResponse);", "returns (stream CreateArchiveResponse);")
        self.refused(self.gate(APPROVED), 'RPC \\"CreateArchive\\"')

    def test_build_failure_is_not_accepted_as_migration(self):
        schema = self.repo / DOCUMENT
        schema.write_bytes(schema.read_bytes() + b"\ninvalid proto syntax\n")
        result = self.gate()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("syntax", result.stderr)
        self.assertNotIn("Accepted ONLY", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
