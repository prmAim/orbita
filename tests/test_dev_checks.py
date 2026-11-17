import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.dev_probe_process import ProbeFailure, capture

ROOT = Path(__file__).resolve().parents[1]


class DemoSchemaCheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for relative in (
            "deploy/demo/schema-compatibility.json",
            "deploy/demo/cleanup.sql",
            "deploy/demo-data.ps1",
            "backend/src/test/java/ru/orbita/broker/InterfaceDemoLedger.java",
        ):
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, destination)
        migrations = self.root / "backend/src/main/resources/db/migration"
        shutil.copytree(ROOT / "backend/src/main/resources/db/migration", migrations)

    def run_check(self):
        return subprocess.run(
            [
                sys.executable,
                str(ROOT / "tools/check_demo_schema.py"),
                "--root",
                str(self.root),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

    def mutate(self, path, old, new):
        target = self.root / path
        content = target.read_text(encoding="utf-8")
        self.assertIn(old, content)
        target.write_text(content.replace(old, new), encoding="utf-8")

    def test_current_and_legacy_pairs_pass(self):
        result = self.run_check()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("1:15; 2:16,17", result.stdout)

    def test_new_unqualified_migration_fails(self):
        path = (
            self.root / "backend/src/main/resources/db/migration/V18__unqualified.sql"
        )
        path.write_text("select 1;", encoding="utf-8")
        result = self.run_check()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("unqualified", result.stderr)

    def test_consumer_drift_fails(self):
        cases = (
            (
                "deploy/demo-data.ps1",
                "$baseline.schema -cne '17'",
                "$baseline.schema -cne '18'",
            ),
            (
                "backend/src/test/java/ru/orbita/broker/InterfaceDemoLedger.java",
                '!"17".equals',
                '!"18".equals',
            ),
            (
                "deploy/demo/cleanup.sql",
                "schema_version is distinct from '16' and",
                "schema_version is distinct from '18' and",
            ),
            ("deploy/demo/cleanup.sql", "b.version in (1,2)", "b.version in (1,2,3)"),
        )
        for path, old, new in cases:
            with self.subTest(path=path):
                self.mutate(path, old, new)
                self.assertNotEqual(0, self.run_check().returncode)
                self.mutate(path, new, old)

    def test_invalid_qualification_fails(self):
        path = self.root / "deploy/demo/schema-compatibility.json"
        original = path.read_text(encoding="utf-8")
        for content in (
            "{",
            "{}",
            json.dumps({"current": 18}),
            original.replace('"17"', '"bogus"'),
        ):
            with self.subTest(content=content):
                path.write_text(content, encoding="utf-8")
                self.assertNotEqual(0, self.run_check().returncode)
        path.unlink()
        self.assertNotEqual(0, self.run_check().returncode)

    def test_missing_and_malformed_consumers_fail(self):
        cases = (
            (
                "deploy/demo-data.ps1",
                "if($baseline.schema -cne '17'){throw",
                "if($baseline.schema -eq '17'){throw",
            ),
            (
                "backend/src/test/java/ru/orbita/broker/InterfaceDemoLedger.java",
                'if (!row.next() || !"17".equals(row.getString(1)))',
                'if (!row.next() && !"17".equals(row.getString(1)))',
            ),
            (
                "deploy/demo/cleanup.sql",
                "'16' and schema_version",
                "'16' or schema_version",
            ),
        )
        for path, original, malformed in cases:
            with self.subTest(path=path):
                self.mutate(path, original, malformed)
                self.assertNotEqual(0, self.run_check().returncode)
                self.mutate(path, malformed, original)
                target = self.root / path
                contents = target.read_text(encoding="utf-8")
                target.unlink()
                self.assertNotEqual(0, self.run_check().returncode)
                target.write_text(contents, encoding="utf-8")

    def test_comment_cannot_replace_actual_guard(self):
        path = "deploy/demo-data.ps1"
        original = "if($baseline.schema -cne '17'){throw"
        self.mutate(
            path,
            original,
            "<# " + original + " #> if($baseline.schema -cne '18'){throw",
        )
        self.assertNotEqual(0, self.run_check().returncode)

    def test_inline_comment_cannot_supply_a_guard(self):
        self.mutate(
            "deploy/demo-data.ps1",
            "$baseline.schema -cne '17'",
            "$baseline.schema -cne '18'",
        )
        path = self.root / "deploy/demo-data.ps1"
        with path.open("a", encoding="utf-8") as stream:
            stream.write(
                "\nWrite-Output 'something' # if($baseline.schema -cne '17'){throw\n"
            )
        self.assertNotEqual(0, self.run_check().returncode)

    def test_duplicate_guard_fails(self):
        path = self.root / "deploy/demo-data.ps1"
        guard = next(
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if "$baseline.schema -cne" in line
        )
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n" + guard)
        self.assertNotEqual(0, self.run_check().returncode)

    def test_missing_qualified_migration_fails(self):
        directory = self.root / "backend/src/main/resources/db/migration"
        next(directory.glob("V16__*.sql")).unlink()
        self.assertNotEqual(0, self.run_check().returncode)


class EnvironmentCheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.java_home = self.root / "JDK with spaces"
        self.java = self.tool(
            self.java_home / "bin", "java", 'openjdk version "25.0.1"', 0
        )
        self.tool(self.java_home / "bin", "javac", "javac 25", 0)
        self.docker = self.tool(self.root, "docker", '"29.0.0"', 0)
        props = dict(
            line.split("=", 1)
            for line in (ROOT / "backend/.mvn/wrapper/maven-wrapper.properties")
            .read_text()
            .splitlines()
        )
        url = props["distributionUrl"]
        self.distribution_url = url
        if os.name == "nt":
            digest = hashlib.sha256(url.encode("ascii")).hexdigest()
        else:
            digest = 0
            for char in url:
                digest = (digest * 31 + ord(char)) & 0xFFFFFFFF
            digest = format(digest, "x")
        self.maven_home = self.root / "m2/wrapper/dists/apache-maven-3.10.0" / digest
        self.tool(self.maven_home / "bin", "mvn", "SHOULD NEVER RUN", 99)
        (self.maven_home / "lib").mkdir()
        (self.maven_home / "lib/maven-core-3.10.0.jar").touch()

    def tool(self, directory, name, output, exit_code):
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (name + ".cmd" if os.name == "nt" else name)
        if os.name == "nt":
            content = f"@echo off\necho {output}\nexit /b {exit_code}\n"
        else:
            content = f"#!/bin/sh\nprintf '%s\\n' '{output}'\nexit {exit_code}\n"
        path.write_text(content, encoding="utf-8")
        path.chmod(0o755)
        return path

    def run_check(self, *extra, environment=None):
        env = os.environ.copy()
        env.pop("MVNW_REPOURL", None)
        env.update(environment or {})
        return subprocess.run(
            [
                sys.executable,
                str(ROOT / "tools/check_dev_environment.py"),
                "--java-home",
                str(self.java_home),
                "--maven-user-home",
                str(self.root / "m2"),
                "--docker",
                str(self.docker),
                "--build-directory",
                str(self.root / "build"),
                "--maven-repository",
                str(self.root / "repository"),
                *extra,
            ],
            capture_output=True,
            text=True,
            timeout=15,
            env=env,
        )

    def test_mirror_trailing_slash_uses_wrappers_verbatim_url_hash(self):
        mirror = "https://example.invalid/mirror/"
        url = (
            mirror
            + "/org/apache/maven/"
            + self.distribution_url.split("/org/apache/maven/")[1]
        )
        if os.name == "nt":
            digest = hashlib.sha256(url.encode("ascii")).hexdigest()
        else:
            digest = 0
            for char in url:
                digest = (digest * 31 + ord(char)) & 0xFFFFFFFF
            digest = format(digest, "x")
        destination = self.maven_home.parent / digest
        shutil.copytree(self.maven_home, destination)
        shutil.rmtree(self.maven_home)
        result = self.run_check(environment={"MVNW_REPOURL": mirror})
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn(str(destination), result.stdout)
        self.assertNotIn(mirror, result.stdout + result.stderr)

    def test_ready_reports_reproducible_offline_command_without_running_maven(self):
        result = self.run_check()
        self.assertEqual(0, result.returncode, result.stderr)
        for expected in (
            "JDK 25.0.1",
            "Maven Wrapper 3.3.4",
            "3.10.0",
            "Docker Engine 29.0.0",
            "--offline",
            "spotless:check",
            "verify",
            str(self.root / "repository"),
        ):
            self.assertIn(expected, result.stdout)
        self.assertNotIn("SHOULD NEVER RUN", result.stdout)
        self.assertFalse((self.root / "build").exists())
        self.assertFalse((self.root / "repository").exists())

    def test_missing_jdk_and_distribution_are_distinct_failures(self):
        self.java.unlink()
        shutil.rmtree(self.maven_home)
        result = self.run_check()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("JDK: MISSING", result.stdout)
        self.assertIn("Maven distribution: MISSING", result.stdout)
        self.assertNotIn("VERIFY", result.stdout)

    def test_wrong_java_major_fails(self):
        self.tool(self.java_home / "bin", "java", 'openjdk version "21.0.1"', 0)
        result = self.run_check()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("required 25", result.stdout)

    def test_invalid_timeout_rejected_before_probing(self):
        for value in ("nan", "inf", "0", "-1", "61"):
            with self.subTest(timeout=value):
                result = self.run_check("--timeout", value)
                self.assertEqual(2, result.returncode)
                self.assertIn("--timeout must be", result.stderr)
                self.assertNotIn("Selected Java", result.stdout)

    def test_docker_engine_unavailable_and_access_denied(self):
        for output, expected in (
            ("Cannot connect to Docker daemon", "ENGINE_UNAVAILABLE"),
            ("permission denied", "ACCESS_DENIED"),
        ):
            with self.subTest(output=output):
                self.tool(self.root, "docker", output, 1)
                result = self.run_check()
                self.assertNotEqual(0, result.returncode)
                self.assertIn(expected, result.stdout)

    def test_missing_docker(self):
        self.docker.unlink()
        result = self.run_check()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Docker: MISSING", result.stdout)

    def test_hanging_java_is_bounded(self):
        if os.name == "nt":
            self.java.write_text(
                "@echo off\nfor /L %%i in (1,1,1000000000) do @rem wait\n",
                encoding="utf-8",
            )
        else:
            self.java.write_text("#!/bin/sh\nexec sleep 5\n", encoding="utf-8")
        result = self.run_check("--timeout", "0.1")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("JDK: TIMEOUT", result.stdout)

    def test_descendant_holding_pipes_is_terminated_within_bound(self):
        pid_file = self.root / "child.pid"
        if os.name == "nt":
            self.java.write_text(
                f'@echo off\n"{sys.executable}" -c "import os,time,pathlib;pathlib.Path(r\'{pid_file}\').write_text(str(os.getpid()));time.sleep(10)"\n',
                encoding="utf-8",
            )
        else:
            self.java.write_text(
                f'#!/bin/sh\n"{sys.executable}" -c "import os,time,pathlib;pathlib.Path(\'{pid_file}\').write_text(str(os.getpid()));time.sleep(10)"\n',
                encoding="utf-8",
            )
        started = time.monotonic()
        result = self.run_check("--timeout", "1")
        self.assertLess(time.monotonic() - started, 4)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("JDK: TIMEOUT; probe process tree terminated", result.stdout)
        self.assertTrue(
            pid_file.exists(), "descendant must have started before timeout"
        )
        if os.name == "nt":
            import ctypes as ct
            from ctypes import wintypes as wt

            api = ct.WinDLL("kernel32", use_last_error=True)
            api.OpenProcess.argtypes = [wt.DWORD, wt.BOOL, wt.DWORD]
            api.OpenProcess.restype = wt.HANDLE
            api.GetExitCodeProcess.argtypes = [wt.HANDLE, ct.POINTER(wt.DWORD)]
            api.CloseHandle.argtypes = [wt.HANDLE]
            handle = api.OpenProcess(0x1000, False, int(pid_file.read_text()))
            if handle:
                try:
                    code = wt.DWORD()
                    self.assertTrue(api.GetExitCodeProcess(handle, ct.byref(code)))
                    self.assertNotEqual(
                        259, code.value, "probe descendant still active"
                    )
                finally:
                    api.CloseHandle(handle)

    @unittest.skipUnless(os.name == "nt", "Windows job assignment boundary")
    def test_job_assignment_failure_does_not_launch_target(self):
        marker = self.root / "must-not-exist"
        with patch("tools.dev_probe_process.WindowsJob") as job:
            job.return_value.assign.side_effect = ProbeFailure(
                "ACCESS_DENIED assigning private probe job"
            )
            with self.assertRaisesRegex(ProbeFailure, "ACCESS_DENIED"):
                capture(
                    [
                        sys.executable,
                        "-c",
                        f"from pathlib import Path; Path({str(marker)!r}).touch()",
                    ],
                    1,
                )
            job.return_value.close.assert_called_once()
        self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
