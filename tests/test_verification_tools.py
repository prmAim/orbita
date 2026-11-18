import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tools import check_ci_scope, watch_ci

# The stdlib command-line tools are also executable directly from tools/.
with patch.object(sys, "path", [str(check_ci_scope.ROOT / "tools"), *sys.path]):
    from tools import run_target_checks as runner


class TargetResultsTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.run = Path(tmp.name)
        self.env = Mock(java_home=self.run)
        self.env.command.return_value = ["mvn"]

    def report(self, name, tests=1, skipped=0, failures=0, errors=0):
        directory = self.run / "targets/surefire-reports"
        directory.mkdir(parents=True, exist_ok=True)
        outcomes = ["skipped"] * skipped + ["failure"] * failures + ["error"] * errors
        cases = "".join(
            f'<testcase name="case{i}">'
            f"{f'<{outcomes[i]}/>' if i < len(outcomes) else ''}</testcase>"
            for i in range(tests)
        )
        (directory / f"TEST-{name}.xml").write_text(
            f'<testsuite name="example.{name}" tests="{tests}" errors="{errors}" failures="{failures}" skipped="{skipped}">{cases}</testsuite>',
            encoding="utf-8",
        )

    def stage(self, tests, code=0, label="targets", log=b""):
        def run(*args, stdout, **kwargs):
            stdout.write(log)
            return subprocess.CompletedProcess(args, code)

        with patch.object(runner.subprocess, "run", side_effect=run):
            return runner.maven_stage(self.env, self.run / "targets", label, tests, [])

    def test_partial_selection_and_skipped_targets_do_not_pass(self):
        self.report("Good", 3, skipped=1)
        self.report("Skipped", skipped=1)
        result = self.stage(["Good", "Missing", "Skipped"])
        self.assertEqual("TARGETS_NOT_EXECUTED", result["outcome"])
        self.assertEqual(["Missing", "Skipped"], result["missing"])
        self.assertEqual(
            dict(executed=2, skipped=2, failures=0, errors=0), result["totals"]
        )
        self.assertEqual(0, result["maven_exit"])
        self.assertEqual(["case1", "case2"], result["suites"]["example.Good"]["cases"])

    def test_exit_zero_without_this_runs_xml_fails(self):
        self.report("Old")
        (self.run / "targets").rename(self.run / "previous")
        self.assertEqual("TARGETS_NOT_EXECUTED", self.stage(["Old"])["outcome"])

    def test_fixture_and_target_assertions_have_different_outcomes(self):
        self.report("Fixture", failures=1)
        self.assertEqual(
            "FIXTURE_FAILURE", self.stage(["Fixture"], 1, "fixtures")["outcome"]
        )
        self.assertEqual("TEST_FAILURE_REVIEW", self.stage(["Fixture"], 1)["outcome"])

    def test_failed_and_errored_cases_are_executed_and_listed(self):
        self.report("Failed", tests=3, failures=1, errors=1)
        result = self.stage(["Failed"])
        self.assertEqual("TEST_FAILURE_REVIEW", result["outcome"])
        self.assertEqual(
            dict(executed=3, failures=1, errors=1, skipped=0), result["totals"]
        )
        self.assertEqual(
            ["case0", "case1", "case2"], result["suites"]["example.Failed"]["cases"]
        )

    def test_compilation_and_build_failures_remain_failures(self):
        for log, outcome in [
            (
                b"Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.15.0:testCompile (default-testCompile)",
                "COMPILATION",
            ),
            (b"cannot access central in offline mode", "ENVIRONMENT"),
            (b"unidentified error", "MAVEN_FAILURE"),
        ]:
            with self.subTest(outcome=outcome):
                self.assertEqual(outcome, self.stage(["A"], 1, log=log)["outcome"])

    def test_malformed_and_green_reports(self):
        self.report("Good")
        self.assertEqual("PASS", self.stage(["example.Good"])["outcome"])
        next((self.run / "targets/surefire-reports").glob("*.xml")).write_text("<")
        self.assertEqual("REPORT_ERROR", self.stage(["Good"])["outcome"])

    def test_child_failure_and_error_cannot_hide_behind_green_suite_counts(self):
        for name, node in (("Failure", "failure"), ("Error", "error")):
            with self.subTest(node=node):
                self.report(name)
                report = self.run / "targets/surefire-reports" / f"TEST-{name}.xml"
                report.write_text(
                    report.read_text(encoding="utf-8").replace(
                        "</testcase>", f"<{node}/></testcase>"
                    ),
                    encoding="utf-8",
                )
                self.assertEqual("REPORT_ERROR", self.stage([name])["outcome"])
                report.unlink()

    def test_conflicting_testcase_outcomes_are_rejected(self):
        directory = self.run / "targets/surefire-reports"
        directory.mkdir(parents=True)
        (directory / "TEST-Conflict.xml").write_text(
            '<testsuite name="example.Conflict" tests="2" errors="0" failures="1" skipped="1">'
            '<testcase name="conflict"><failure/><skipped/></testcase>'
            '<testcase name="clean"/></testsuite>',
            encoding="utf-8",
        )
        self.assertEqual("REPORT_ERROR", self.stage(["Conflict"])["outcome"])


class CiScopeTests(unittest.TestCase):
    def test_allowlist_fails_closed(self):
        report = b"docs/acceptance/check.md"
        self.assertTrue(check_ci_scope.reports_only([report]))
        for other in (
            b".github/workflows/ci.yml",
            b"docs/specifications/file.md",
            b"backend/pom.xml",
            b"tools/check_ci_scope.py",
            b"backend/V20.sql",
            b"docs/acceptance/check.py",
        ):
            self.assertFalse(check_ci_scope.reports_only([report, other]))
        self.assertFalse(check_ci_scope.reports_only([]))

    def test_broken_report_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            reports = root / "docs/acceptance"
            reports.mkdir(parents=True)
            (reports / "test.md").write_text(
                "# Report\n\n[file](missing.md)", encoding="utf-8"
            )
            self.assertEqual(
                ["test.md: broken local link missing.md"],
                check_ci_scope.check_reports(root),
            )

    def test_report_links_support_titles_references_and_angle_destinations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            reports = root / "docs/acceptance"
            reports.mkdir(parents=True)
            (reports / "existing file.md").write_text("# Linked\n", encoding="utf-8")
            (reports / "test.md").write_text(
                "# Report\n\n"
                '[titled](missing-title.md "link title")\n'
                "[angle](<missing with spaces.md>)\n"
                "[existing](<existing file.md>)\n"
                '[remote](https://example.invalid/missing.md "remote")\n'
                "[fragment](#missing)\n"
                "`[inline example](missing-inline.md)`\n"
                "```markdown\n"
                "[block example](missing-block.md)\n"
                "[block reference]: missing-block-reference.md\n"
                "```\n"
                "[named][missing]\n"
                '[missing]: missing-reference.md "reference title"\n',
                encoding="utf-8",
            )
            self.assertEqual(
                [
                    "test.md: broken local link missing-title.md",
                    "test.md: broken local link missing with spaces.md",
                    "test.md: broken local link missing-reference.md",
                ],
                check_ci_scope.check_reports(root),
            )

    def test_git_range_rename_and_missing_base(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()

            def git(*args):
                return subprocess.check_output(
                    ["git", *args], cwd=root, text=True
                ).strip()

            def commit():
                git("add", ".")
                git(
                    "-c",
                    "user.name=test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "-qm",
                    "test",
                )
                return git("rev-parse", "HEAD")

            git("init", "-q")
            src = root / "code.py"
            src.write_text("# title\n")
            base = commit()
            reports = root / "docs/acceptance"
            reports.mkdir(parents=True)
            src.rename(reports / "test.md")
            renamed = commit()
            (reports / "test.md").write_text("# report\n")
            head = commit()
            output = root / "scope.txt"
            for event, start, end, full in [
                ("push", base, renamed, True),
                ("push", renamed, head, False),
                ("pull_request", base, head, True),
                ("push", "0" * 40, head, True),
            ]:
                with (
                    self.subTest(event=event, start=start, end=end),
                    patch.object(check_ci_scope, "ROOT", root),
                    patch.object(sys, "argv", ["scope", event, start, end]),
                    patch.dict(os.environ, GITHUB_OUTPUT=str(output)),
                ):
                    check_ci_scope.main()
                    self.assertEqual(
                        f"full={str(full).lower()}", output.read_text().splitlines()[-1]
                    )


class TargetRunnerContextTests(unittest.TestCase):
    def run_cli(self, root, *, environment, capture_result):
        root.mkdir(parents=True, exist_ok=True)
        argv = [
            "run_target_checks.py",
            "--build-directory",
            str(root / ".scratch"),
            "--it",
            "Example",
        ]
        with (
            patch.object(runner, "ROOT", root),
            patch.object(runner, "git", side_effect=["a" * 40, ""]),
            patch.object(runner, "inspect_environment", return_value=environment),
            patch.object(runner.shutil, "which", return_value="docker"),
            patch.object(runner, "capture", capture_result) as probe,
            patch.object(sys, "argv", argv),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            exit_code = runner.main()
        summaries = list((root / ".scratch").glob("run-*/summary.json"))
        self.assertEqual(1, len(summaries))
        return exit_code, json.loads(summaries[0].read_text(encoding="utf-8")), probe

    def test_docker_context_is_recorded_even_when_preflight_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            exit_code, summary, probe = self.run_cli(
                Path(tmp),
                environment=None,
                capture_result=Mock(return_value=(0, "selected-context\n", "")),
            )
        self.assertEqual(1, exit_code)
        self.assertEqual("ENVIRONMENT", summary["outcome"])
        self.assertEqual("selected-context", summary["docker_context"])
        probe.assert_called_once_with(["docker", "context", "show"], 5)

    def test_context_probe_failure_does_not_block_valid_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = Mock(java_home=Path(tmp), docker_version="27.0")
            stage = {
                "stage": "targets",
                "outcome": "PASS",
                "maven_exit": 0,
                "missing": [],
                "totals": {},
            }
            with patch.object(runner, "maven_stage", return_value=stage) as maven:
                exit_code, summary, probe = self.run_cli(
                    Path(tmp),
                    environment=env,
                    capture_result=Mock(side_effect=runner.ProbeFailure("TIMEOUT")),
                )
        self.assertEqual(0, exit_code)
        self.assertEqual("PASS", summary["outcome"])
        self.assertEqual("27.0", summary["docker_version"])
        probe.assert_called_once_with(["docker", "context", "show"], 5)
        maven.assert_called_once()


class WatchCiTests(unittest.TestCase):
    def test_waits_for_both_and_only_announces_changes(self):
        sha = "a" * 40
        pending = {
            "headSha": sha,
            "event": "pull_request",
            "status": "in_progress",
            "jobs": [{"steps": [{"name": "compile", "status": "in_progress"}]}],
        }

        def end(conclusion):
            return {
                **pending,
                "status": "completed",
                "conclusion": conclusion,
                "url": "run-url",
            }

        found = [{"headSha": sha, "databaseId": 1}]
        responses = [found, pending, [], pending, found, end("success"), end("failure")]
        with (
            patch.object(watch_ci, "gh", side_effect=responses),
            patch.object(watch_ci.time, "sleep"),
            contextlib.redirect_stdout(io.StringIO()) as out,
        ):
            self.assertEqual(1, watch_ci.watch(sha, "pull_request", None, 60, 100))
        self.assertEqual(1, out.getvalue().count("compile"))
        self.assertIn("ci.yml: success run-url", out.getvalue())
        self.assertIn("java.yml: failure run-url", out.getvalue())
