"""Integrity and safe-failure checks for offline restoration; no network calls."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "tools/offline_env.py"
SPEC = importlib.util.spec_from_file_location("offline_env", SCRIPT)
offline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(offline)


class OfflineBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="orbita-bundle-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        (self.bundle / "package.whl").write_bytes(b"wheel-fixture")
        self.manifest = {
            "schema": 1,
            "platform": "windows-x64",
            "source_hashes": {"lock": "expected"},
            "files": {"package.whl": offline.digest(self.bundle / "package.whl")},
        }
        self.write_manifest()

    def write_manifest(self):
        (self.bundle / "manifest.json").write_text(
            json.dumps(self.manifest), encoding="utf-8"
        )

    def test_valid_bundle(self):
        with patch.object(offline, "source_hashes", return_value={"lock": "expected"}):
            offline.validate_bundle(self.bundle)

    def test_corrupt_package_is_rejected(self):
        (self.bundle / "package.whl").write_bytes(b"corrupted")
        with self.assertRaisesRegex(offline.BundleError, "corrupted"):
            offline.validate_bundle(self.bundle, check_source=False)

    def test_missing_package_is_rejected(self):
        self.manifest["files"]["missing.whl"] = "unused"
        self.write_manifest()
        with self.assertRaisesRegex(offline.BundleError, "Missing"):
            offline.validate_bundle(self.bundle, check_source=False)

    def test_configuration_drift_is_rejected(self):
        with patch.object(offline, "source_hashes", return_value={"lock": "changed"}):
            with self.assertRaisesRegex(offline.BundleError, "does not match"):
                offline.validate_bundle(self.bundle)

    def test_manifest_cannot_escape_bundle(self):
        self.manifest["files"] = {"../outside": "unused"}
        self.write_manifest()
        with self.assertRaisesRegex(offline.BundleError, "Invalid path"):
            offline.validate_bundle(self.bundle, check_source=False)

    def test_existing_environment_is_preserved(self):
        target = self.root / ".venv"
        target.mkdir()
        sentinel = target / "keep.txt"
        sentinel.write_text("keep")
        with (
            patch.object(offline, "ROOT", self.root),
            patch.object(offline.sys, "platform", "win32"),
            patch.object(offline, "validate_bundle"),
            patch.object(offline, "run") as run,
        ):
            with self.assertRaisesRegex(offline.BundleError, "already exists"):
                offline.restore(self.bundle, target)
        self.assertEqual(sentinel.read_text(), "keep")
        run.assert_not_called()
