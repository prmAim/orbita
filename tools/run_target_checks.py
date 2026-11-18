"""Run selected Java test classes offline and verify this run's Maven reports."""

import argparse
import contextlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

from check_dev_environment import (
    ROOT,
    add_environment_arguments,
    environment_arguments,
    inspect_environment,
)

B1_UNIT = [
    "BuyResourceRejectionClassifierTest",
    "BuyResourceRejectionTlsTest",
    "OrderSdkEvidenceTest",
    "OrderSdkTlsTest",
    "MirrorOrderFeedTlsTest",
]
B1_IT = ["OrderEvidenceIT", "OrderEvidenceRestartIT"]


def test_class(value):
    if not re.fullmatch(r"(?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*", value, re.ASCII):
        raise argparse.ArgumentTypeError("use an exact Java test class name")
    return value


def verify_reports(directory, selected):
    """Check coverage per selector, including all-skipped and missing individual suites."""
    suites = {}
    for path in sorted(directory.glob("TEST-*.xml")):
        root = ET.parse(path).getroot()
        name = root.attrib["name"]
        if root.tag != "testsuite" or name in suites:
            raise ValueError(f"unexpected or duplicate suite in {path.name}")
        counts = {
            key: int(root.attrib[key])
            for key in ("tests", "failures", "errors", "skipped")
        }
        if (
            min(counts.values()) < 0
            or sum(counts[key] for key in ("failures", "errors", "skipped"))
            > counts["tests"]
        ):
            raise ValueError(f"invalid counts in {path.name}")
        cases = root.findall("testcase")
        executed = [
            case.attrib["name"] for case in cases if case.find("skipped") is None
        ]
        if (
            len(cases) != counts["tests"]
            or len(executed) != counts["tests"] - counts["skipped"]
        ):
            raise ValueError(f"test cases differ from suite totals in {path.name}")
        counts["executed"] = len(executed)
        counts["cases"] = executed
        counts["report"] = str(path)
        suites[name] = counts
    missing = [
        target
        for target in selected
        if not any(
            target in (name, name.rsplit(".", 1)[-1]) and counts["executed"]
            for name, counts in suites.items()
        )
    ]
    return suites, missing


def maven_stage(env, build, label, unit, integration):
    command = env.command(build)
    if unit:
        command.append("-Dtest=" + ",".join(unit))
    if integration:
        if not unit:
            command.extend(["-Dtest=!**/*", "-Dsurefire.failIfNoSpecifiedTests=false"])
        command.extend(["-Dit.test=" + ",".join(integration), "verify"])
    else:
        command.append("test")
    log = build.parent / f"{label}.log"
    result = {
        "stage": label,
        "command": command,
        "maven_exit": None,
        "log": str(log),
        "suites": {},
        "missing": [],
    }
    print(f"Running {label}: {log}", flush=True)
    with log.open("wb") as stream:
        code = subprocess.run(
            command,
            cwd=ROOT / "backend",
            stdout=stream,
            stderr=subprocess.STDOUT,
            env={**os.environ, "JAVA_HOME": str(env.java_home)},
        ).returncode
    result["maven_exit"] = code
    try:
        for plugin, targets in [("surefire", unit), ("failsafe", integration)]:
            suites, missing = verify_reports(build / f"{plugin}-reports", targets)
            if suites.keys() & result["suites"].keys():
                raise ValueError("suite reported by both plugins")
            result["suites"].update(suites)
            result["missing"].extend(missing)
            for name in suites:
                if name not in targets and name.rsplit(".", 1)[-1] not in targets:
                    raise ValueError(f"{plugin} ran unselected suite {name}")
    except (KeyError, ET.ParseError, ValueError) as error:
        return {**result, "outcome": "REPORT_ERROR", "detail": str(error)}
    totals = {
        key: sum(s[key] for s in result["suites"].values())
        for key in ("executed", "failures", "errors", "skipped")
    }
    result["totals"] = totals
    if totals["failures"] or totals["errors"]:
        outcome = "FIXTURE_FAILURE" if label == "fixtures" else "TEST_FAILURE_REVIEW"
    elif code:
        output = log.read_text(encoding="utf-8", errors="replace")
        if re.search(
            r"Failed to execute goal [^\n]*compiler[^:\n]*:[^:\n]+:(?:testCompile|compile)\b",
            output,
        ):
            outcome = "COMPILATION"
        elif "in offline mode" in output or "Could not resolve dependencies" in output:
            outcome = "ENVIRONMENT"
        else:
            outcome = "MAVEN_FAILURE"
    elif result["missing"]:
        outcome = "TARGETS_NOT_EXECUTED"
    else:
        outcome = "PASS"
    return {**result, "outcome": outcome}


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_environment_arguments(parser)
    parser.set_defaults(build_directory=ROOT / ".scratch/target-checks")
    parser.add_argument("--suite", choices=["b1"])
    parser.add_argument("--unit", action="append", type=test_class, default=[])
    parser.add_argument("--it", action="append", type=test_class, default=[])
    parser.add_argument("--fixture", action="append", type=test_class, default=[])
    parser.add_argument(
        "--access-context", choices=["shell", "sandbox", "approved"], default="shell"
    )
    args = environment_arguments(parser)
    if args.suite:
        if args.unit or args.it or args.fixture:
            parser.error("choose a named suite or individual classes")
        args.fixture, args.unit, args.it = ["OrderEvidenceFixturesTest"], B1_UNIT, B1_IT
    if not (args.fixture or args.unit or args.it):
        parser.error("select --suite, --fixture, --unit or --it")
    parent = args.build_directory.expanduser().resolve()
    parent.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="run-", dir=parent))
    summary = {
        "outcome": "ENVIRONMENT",
        "stages": [],
        "access_context": args.access_context,
        "maven_user_home": str(args.maven_user_home.expanduser().resolve()),
        "maven_repository": str(args.maven_repository.expanduser().resolve()),
        "java_home": str(args.java_home) if args.java_home else None,
        "docker_cli": shutil.which(args.docker or "docker"),
        "docker_context": os.environ.get("DOCKER_CONTEXT"),
        "docker_host_override": bool(os.environ.get("DOCKER_HOST")),
        "fixture": args.fixture,
        "unit": args.unit,
        "it": args.it,
    }
    try:
        summary["commit"] = git("rev-parse", "HEAD")
        summary["dirty"] = bool(git("status", "--porcelain"))
        with (
            (run / "preflight.log").open("w", encoding="utf-8") as stream,
            contextlib.redirect_stdout(stream),
        ):
            env = inspect_environment(args, require_docker=bool(args.it))
        if env:
            summary.update(
                java_home=str(env.java_home), docker_version=env.docker_version
            )
            if args.it:
                context = subprocess.run(
                    [summary["docker_cli"], "context", "show"],
                    capture_output=True,
                    text=True,
                    timeout=args.timeout,
                )
                if context.returncode == 0:
                    summary["docker_context"] = context.stdout.strip()
            for label, unit, integration in [
                ("fixtures", args.fixture, []),
                ("targets", args.unit, args.it),
            ]:
                if unit or integration:
                    stage = maven_stage(env, run / label, label, unit, integration)
                    summary["stages"].append(stage)
                    summary["outcome"] = stage["outcome"]
                    if stage["outcome"] != "PASS":
                        break
        else:
            print((run / "preflight.log").read_text(encoding="utf-8"))
    except (OSError, subprocess.SubprocessError) as error:
        summary.update(outcome="ENVIRONMENT", detail=str(error))
    summary["exit_code"] = int(summary["outcome"] != "PASS")
    path = run / "summary.json"
    path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        f"{summary['outcome']} {summary.get('commit', 'unknown')} dirty={summary.get('dirty')} | {path}"
    )
    for stage in summary["stages"]:
        print(
            f"{stage['stage']}: {stage['outcome']} Maven exit={stage['maven_exit']} {stage.get('totals', {})} missing={stage['missing']}"
        )
    return summary["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
