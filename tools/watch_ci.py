"""Watch both Orbita CI workflows for one full commit SHA and event."""

import argparse
import json
import re
import subprocess
import sys
import time

REPO = "prmAim/orbita"
WORKFLOWS = ("java.yml", "ci.yml")


def gh(*args):
    return json.loads(
        subprocess.check_output(["gh", "run", *args, "--repo", REPO], timeout=45)
    )


def stage(run):
    if run["status"] == "completed":
        return run["conclusion"]
    active = [
        step["name"]
        for job in run.get("jobs", [])
        for step in job.get("steps", [])
        if step["status"] == "in_progress"
    ]
    return ", ".join(active) or run["status"]


def watch(sha, event, branch, interval, timeout):
    deadline = time.monotonic() + timeout
    ids = {}
    states = {}
    completed = {}
    while True:
        for workflow in WORKFLOWS:
            if workflow in completed:
                continue
            if workflow not in ids:
                filters = ["--branch", branch] if branch else []
                runs = gh(
                    "list",
                    "--workflow",
                    workflow,
                    "--commit",
                    sha,
                    "--event",
                    event,
                    *filters,
                    "--limit",
                    "2",
                    "--json",
                    "databaseId,headSha",
                )
                if len(runs) > 1:
                    raise ValueError(
                        f"ambiguous runs for {workflow} at {sha}, specify --branch"
                    )
                if runs and runs[0]["headSha"] == sha:
                    ids[workflow] = str(runs[0]["databaseId"])
            if workflow in ids:
                run = gh(
                    "view",
                    ids[workflow],
                    "--json",
                    "headSha,event,status,conclusion,jobs,url",
                )
                if (run["headSha"], run["event"]) != (sha, event):
                    raise ValueError("run identity changed")
                current = stage(run)
                if run["status"] == "completed":
                    completed[workflow] = run["conclusion"]
                    current += " " + run["url"]
            else:
                current = "waiting for run"
            if states.get(workflow) != current:
                print(f"{workflow}: {current}", flush=True)
                states[workflow] = current
        if len(completed) == len(WORKFLOWS):
            return int(any(status != "success" for status in completed.values()))
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"CI did not finish for {sha} ({event})")
        time.sleep(min(interval, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sha")
    parser.add_argument("--event", choices=["push", "pull_request"], required=True)
    parser.add_argument("--branch", help="branch name; defaults to main for push")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--timeout", type=int, default=2400)
    args = parser.parse_args()
    if (
        not re.fullmatch(r"[0-9a-fA-F]{40}", args.sha)
        or min(args.interval, args.timeout) <= 0
    ):
        parser.error(
            "supply a full commit SHA and positive interval/timeout in seconds"
        )
    branch = args.branch or ("main" if args.event == "push" else None)
    print(f"{REPO} {args.event} {args.sha}", flush=True)
    try:
        return watch(args.sha.lower(), args.event, branch, args.interval, args.timeout)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
